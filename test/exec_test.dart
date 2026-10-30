import 'dart:io';
import 'dart:typed_data';

import 'package:libbigdata/libbigdata.dart' hide JoinType, RowFilter, FilterOp;
import 'package:libbigdata/src/core/failure.dart';
import 'package:libbigdata/src/core/keys.dart';
import 'package:libbigdata/src/core/limits.dart';
import 'package:libbigdata/src/formats/delta/roaring_bitmap.dart';
import 'package:libbigdata/src/sql/exec/kernels.dart';
import 'package:libbigdata/src/sql/exec/where_pushdown.dart';
import 'package:libbigdata/src/sql/operators/physical_operators.dart';
import 'package:libbigdata/src/sql/operators/spill.dart';
import 'package:libbigdata/src/sql/planner/logical_plan.dart' as lp;
import 'package:libbigdata/src/sql/planner/physical_plan.dart';
import 'package:test/test.dart';

import 'support/results.dart';

/// a throw in one cleanup step of an operator's close() must
/// not strand the remaining steps (other children, FDs, spill files, budget).
class _FakeIterator extends RecordBatchIterator {
  _FakeIterator(this.schema, {this.throwOnClose = false});
  @override
  final Schema schema;
  final bool throwOnClose;
  var closed = false;

  @override
  Future<RecordBatch?> next() async => null;

  @override
  Future<void> close() async {
    closed = true;
    if (throwOnClose) throw StateError('child close boom');
  }
}

/// Yields each batch after running its `before` callback, then null.
class _CallbackIterator extends RecordBatchIterator {
  _CallbackIterator(this.schema, this._steps);
  @override
  final Schema schema;
  final List<({RecordBatch batch, void Function() before})> _steps;
  var _next = 0;

  @override
  Future<RecordBatch?> next() async {
    if (_next >= _steps.length) return null;
    final step = _steps[_next++];
    step.before();
    return step.batch;
  }

  @override
  Future<void> close() async {}
}

/// Yields [batch], then throws on the next call.
class _FailingIterator extends RecordBatchIterator {
  _FailingIterator(this.schema, this.batch);
  @override
  final Schema schema;
  final RecordBatch batch;
  var _done = false;

  @override
  Future<RecordBatch?> next() async {
    if (_done) throw StateError('input failed');
    _done = true;
    return batch;
  }

  @override
  Future<void> close() async {}
}

/// Yields a fixed list of batches, then null.
class _ListIterator extends RecordBatchIterator {
  _ListIterator(this.schema, this._batches);
  @override
  final Schema schema;
  final List<RecordBatch> _batches;
  int _i = 0;

  @override
  Future<RecordBatch?> next() async =>
      _i < _batches.length ? _batches[_i++] : null;

  @override
  Future<void> close() async {}
}

/// A [MemoryManager] that records the high-water mark of [used] after every
/// grant, so a test can assert the true reserved peak.
final class _PeakManager extends MemoryManager {
  _PeakManager(super.budget) : super(hardCap: true);
  int peak = 0;

  /// Reserves refused, each of which made the caller spill.
  int denied = 0;

  @override
  bool reserve(int bytes) {
    final ok = super.reserve(bytes);
    if (used > peak) peak = used;
    if (!ok) denied++;
    return ok;
  }
}

void main() {
  group('kernels', () {
    NumericColumn ints(List<int?> v) =>
        NumericColumn.int64(name: 'x', values: v, nullable: v.contains(null));
    NumericColumn doubles(List<double?> v) =>
        NumericColumn.float64(name: 'x', values: v, nullable: v.contains(null));
    ListColumn<String> strs(List<String?> v) => ListColumn<String>(
      name: 'x',
      dataType: DataType.string,
      data: v,
      nullable: v.contains(null),
    );

    group('compareScalar', () {
      test('integer comparisons with all six operators', () {
        final c = ints([1, 5, 10]);
        expect(
          Kernels.compareScalar(c, lp.BinaryOperator.gt, 4).selected(),
          equals(Int32List.fromList([1, 2])),
        );
        expect(
          Kernels.compareScalar(c, lp.BinaryOperator.lte, 5).selected(),
          equals(Int32List.fromList([0, 1])),
        );
        expect(
          Kernels.compareScalar(c, lp.BinaryOperator.eq, 5).selected(),
          equals(Int32List.fromList([1])),
        );
        expect(
          Kernels.compareScalar(c, lp.BinaryOperator.neq, 5).selected(),
          equals(Int32List.fromList([0, 2])),
        );
      });

      test('double fast path and string slow path', () {
        expect(
          Kernels.compareScalar(
            doubles([1.5, 2.5]),
            lp.BinaryOperator.gte,
            2.5,
          ).selected(),
          equals(Int32List.fromList([1])),
        );
        expect(
          Kernels.compareScalar(
            strs(['apple', 'pear']),
            lp.BinaryOperator.lt,
            'b',
          ).selected(),
          equals(Int32List.fromList([0])),
        );
      });

      test('null operand yields UNKNOWN (not selected); 3VL flags', () {
        final c = ints([1, null, 10]);
        final m = Kernels.compareScalar(c, lp.BinaryOperator.gt, 0);
        expect(m.isTrue(0), isTrue);
        expect(m.isNull(1), isTrue);
        expect(m.isTrue(1), isFalse);
        expect(m.isTrue(2), isTrue);
        expect(m.selected(), equals(Int32List.fromList([0, 2])));
        // Null literal -> all UNKNOWN.
        expect(
          Kernels.compareScalar(c, lp.BinaryOperator.gt, null).selected(),
          isEmpty,
        );
      });
    });

    group('compareColumns', () {
      test('element-wise with nulls', () {
        final a = ints([1, 5, null]);
        final b = ints([2, 5, 9]);
        final m = Kernels.compareColumns(a, lp.BinaryOperator.lt, b);
        expect(m.isTrue(0), isTrue);
        expect(m.isFalse(1), isTrue);
        expect(m.isNull(2), isTrue);
      });
    });

    group('boolean combination (three-valued logic)', () {
      // a: T F N ; b: T F N T F N T F N  -> test the 3x3 grid.
      BoolMask mk(List<int> states) {
        // 1=true, 0=false, -1=null
        final m = BoolMask(states.length);
        for (int i = 0; i < states.length; i++) {
          if (states[i] == 1) {
            m.setTrue(i);
          } else if (states[i] == -1) {
            m.setNull(i);
          }
        }
        return m;
      }

      final a = mk([1, 1, 1, 0, 0, 0, -1, -1, -1]);
      final b = mk([1, 0, -1, 1, 0, -1, 1, 0, -1]);

      test('AND', () {
        final r = Kernels.and(a, b);
        // T&T=T, T&F=F, T&N=N, F&*=F, N&T=N, N&F=F, N&N=N
        final expectTrue = {0};
        final expectFalse = {1, 3, 4, 5, 7};
        final expectNull = {2, 6, 8};
        for (final i in expectTrue) {
          expect(r.isTrue(i), isTrue, reason: 'AND true @$i');
        }
        for (final i in expectFalse) {
          expect(r.isFalse(i), isTrue, reason: 'AND false @$i');
        }
        for (final i in expectNull) {
          expect(r.isNull(i), isTrue, reason: 'AND null @$i');
        }
      });

      test('OR', () {
        final r = Kernels.or(a, b);
        // T|*=T, F|T=T, F|F=F, F|N=N, N|T=T, N|F=N, N|N=N
        final expectTrue = {0, 1, 2, 3, 6};
        final expectFalse = {4};
        final expectNull = {5, 7, 8};
        for (final i in expectTrue) {
          expect(r.isTrue(i), isTrue, reason: 'OR true @$i');
        }
        for (final i in expectFalse) {
          expect(r.isFalse(i), isTrue, reason: 'OR false @$i');
        }
        for (final i in expectNull) {
          expect(r.isNull(i), isTrue, reason: 'OR null @$i');
        }
      });

      test('NOT', () {
        final r = Kernels.not(mk([1, 0, -1]));
        expect(r.isFalse(0), isTrue);
        expect(r.isTrue(1), isTrue);
        expect(r.isNull(2), isTrue);
      });
    });

    group('arith', () {
      test('int + int stays int64', () {
        final r = Kernels.arithmetic(
          ints([1, 2, 3]),
          lp.BinaryOperator.add,
          10,
          'r',
        );
        expect(r, isA<NumericColumn>());
        expect(r.dataType, DataType.int64);
        expect(r.toList(), equals([11, 12, 13]));
      });

      test('divide promotes to float64', () {
        final r = Kernels.arithmetic(
          ints([10, 5]),
          lp.BinaryOperator.divide,
          2,
          'r',
        );
        expect(r.dataType, DataType.float64);
        expect(r.toList(), equals([5.0, 2.5]));
      });

      test('column op column, nulls propagate', () {
        final r = Kernels.arithmetic(
          ints([1, null, 3]),
          lp.BinaryOperator.multiply,
          ints([2, 2, 2]),
          'r',
        );
        expect(r.toList(), equals([2, null, 6]));
      });
    });

    group('gather', () {
      test('numeric gather preserves typed buffer and nulls', () {
        final c = ints([10, 20, null, 40]);
        final g = Kernels.gather(c, Int32List.fromList([3, 2, 0]));
        expect(g, isA<NumericColumn>());
        expect(g.toList(), equals([40, null, 10]));
      });

      test('boxed gather for ListColumn', () {
        final g = Kernels.gather(
          strs(['a', 'b', 'c']),
          Int32List.fromList([2, 0]),
        );
        expect(g, isA<ListColumn>());
        expect(g.toList(), equals(['c', 'a']));
      });
    });

    group('concat', () {
      test('same-type numerics stay numeric', () {
        final r = Kernels.concat([
          ints([1, 2]),
          ints([3, 4]),
        ]);
        expect(r, isA<NumericColumn>());
        expect(r.toList(), equals([1, 2, 3, 4]));
      });
    });

    group('mask <-> column', () {
      test('round-trips through a boolean column', () {
        final m = Kernels.compareScalar(
          ints([1, 5, 10]),
          lp.BinaryOperator.gt,
          4,
        );
        final col = Kernels.maskToColumn(m, 'b');
        expect(col.toList(), equals([false, true, true]));
        final back = Kernels.columnToMask(col);
        expect(back.selected(), equals(m.selected()));
      });
    });

    group('typed columns survive filter -> project (end to end)', () {
      test('SELECT id FROM t WHERE id > 2 keeps NumericColumn', () async {
        final table = Table(
          name: 't',
          columns: [
            NumericColumn.int64(
              name: 'id',
              values: [1, 2, 3, 4, 5],
              nullable: false,
            ),
            NumericColumn.float64(
              name: 'v',
              values: [1.0, 2.0, 3.0, 4.0, 5.0],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
              Field(name: 'v', dataType: DataType.float64, nullable: false),
            ],
          ),
        );
        final engine = SqlEngine.open().ok;
        engine.registerTable('t', table);
        final df = await engine.sql('SELECT id, v FROM t WHERE id > 2').ok;
        final result = await df.execute().ok;
        expect(result.rowCount, 3);
        expect(
          result.column('id'),
          isA<NumericColumn>(),
          reason: 'numeric typing must survive filter+projection',
        );
        expect(result.column('id')!.toList(), equals([3, 4, 5]));
        expect(result.column('v')!.toList(), equals([3.0, 4.0, 5.0]));
      });
    });

    // Zero-copy typed concat/slice/filter (#1.1): data is bulk-copied and the
    // packed null bitmap is realigned/shifted bit-by-bit; these cases cross byte
    // boundaries where that logic is easy to get wrong.
    group('typed concat/slice/filter parity (#1.1)', () {
      test('concat across byte boundaries preserves values and nulls', () {
        final a = ints([0, null, 2]); // len 3
        final b = ints([3, 4, null, 6, 7]); // len 5
        final c = ints([null, 9, 10, 11, 12, null, 14]); // len 7
        final r = Kernels.concat([a, b, c]) as NumericColumn;
        final expected = [...a.toList(), ...b.toList(), ...c.toList()];
        expect(r.toList(), expected);
        for (int i = 0; i < expected.length; i++) {
          expect(r.isNullAt(i), expected[i] == null, reason: 'idx $i');
        }
      });

      test('slice with non-byte-aligned start realigns nulls', () {
        final col = ints([for (int i = 0; i < 20; i++) i % 3 == 0 ? null : i]);
        final s = col.slice(3, 11) as NumericColumn;
        final expected = [for (int i = 3; i < 11; i++) col[i]];
        expect(s.toList(), expected);
        for (int i = 0; i < expected.length; i++) {
          expect(s.isNullAt(i), expected[i] == null, reason: 'idx $i');
        }
        expect(s.length, 8);
      });

      test('filter gathers with mixed and duplicate indices', () {
        final col = ints([10, null, 30, 40, null]);
        const idx = [4, 0, 2, 1, 2];
        final f = col.filter(idx);
        expect(f.toList(), [for (final i in idx) col[i]]);
      });

      test('empty slice and empty filter', () {
        final col = ints([1, 2, 3]);
        expect(col.slice(1, 1).length, 0);
        expect(col.filter(const <int>[]).length, 0);
      });

      test('all-null concat piece', () {
        final r = Kernels.concat([
          ints([null, null]),
          ints([1, 2]),
        ]);
        expect(r.toList(), [null, null, 1, 2]);
      });

      test('int8 pieces preserve int8 on concat (widest actual width)', () {
        final a = NumericColumn.int8(
          name: 'x',
          values: [1, 2],
          nullable: false,
        );
        final b = NumericColumn.int8(
          name: 'x',
          values: [3, 4],
          nullable: false,
        );
        final r = Kernels.concat([a, b]) as NumericColumn;
        expect(r.dataType, DataType.int8);
        expect(r.toList(), [1, 2, 3, 4]);
      });

      test('mixed int widths concat to the widest actual width', () {
        final a = NumericColumn.int8(
          name: 'x',
          values: [1, 2],
          nullable: false,
        );
        final b = NumericColumn.int32(
          name: 'x',
          values: [3, 100000],
          nullable: false,
        );
        final r = Kernels.concat([a, b]) as NumericColumn;
        expect(r.dataType, DataType.int32);
        expect(r.toList(), [1, 2, 3, 100000]);
      });

      test('float concat preserves doubles and nulls', () {
        final r = Kernels.concat([
          doubles([1.5, null]),
          doubles([2.5, 3.5]),
        ]);
        expect(r.toList(), [1.5, null, 2.5, 3.5]);
      });
    });
  });

  group('operators', () {
    Table people() => Table(
      name: 'people',
      columns: [
        NumericColumn.int64(
          name: 'grp',
          values: [1, 1, 2, 2, 2],
          nullable: false,
        ),
        NumericColumn.int64(
          name: 'age',
          values: [20, 30, 40, 50, 60],
          nullable: false,
        ),
        ListColumn<String>(
          name: 'dept',
          dataType: DataType.string,
          data: ['x', 'x', 'y', null, 'y'],
          nullable: true,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'grp', dataType: DataType.int64, nullable: false),
          Field(name: 'age', dataType: DataType.int64, nullable: false),
          Field(name: 'dept', dataType: DataType.string, nullable: true),
        ],
      ),
    );

    Table t(
      String name,
      String kCol,
      List<int> ks,
      String vCol,
      List<String> vs,
    ) => Table(
      name: name,
      columns: [
        NumericColumn.int64(name: kCol, values: ks, nullable: false),
        ListColumn<String>(
          name: vCol,
          dataType: DataType.string,
          data: vs,
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: kCol, dataType: DataType.int64, nullable: false),
          Field(name: vCol, dataType: DataType.string, nullable: false),
        ],
      ),
    );

    PhysicalTableScan scan(Table t) =>
        PhysicalTableScan(tableName: t.name, table: t);

    PhysicalColumnExpression col(String n, DataType t) =>
        PhysicalColumnExpression(n, t);

    Future<RecordBatch> run(PhysicalPlan plan) async {
      final it = plan.execute();
      RecordBatch? merged;
      while (true) {
        final b = await it.next();
        if (b == null) break;
        merged = merged == null ? b : merged.concat(b);
      }
      await it.close();
      return merged ??
          RecordBatch(columns: const [], schema: plan.schema, rowCount: 0);
    }

    group('vectorized Aggregate', () {
      test('typed numeric grouping; group key stays NumericColumn', () async {
        final agg = PhysicalAggregate(
          input: scan(people()),
          groupBy: [col('grp', DataType.int64)],
          aggregates: [
            PhysicalCount(),
            PhysicalSum(argument: col('age', DataType.int64)),
          ],
          aliases: ['c', 's'],
        );
        final r = await run(agg);
        expect(r.rowCount, 2);
        expect(r.column('grp'), isA<NumericColumn>());
        final grp = r.column('grp')!.toList();
        final c = r.column('c')!.toList();
        final s = r.column('s')!.toList();
        final byGrp = {
          for (int i = 0; i < grp.length; i++) grp[i]: [c[i], s[i]],
        };
        expect(byGrp[1], [
          2,
          BigInt.from(50),
        ]); // integer SUM → lossless decimal
        expect(byGrp[2], [3, BigInt.from(150)]);
      });

      test('null group key is its own group', () async {
        final agg = PhysicalAggregate(
          input: scan(people()),
          groupBy: [col('dept', DataType.string)],
          aggregates: [PhysicalCount()],
          aliases: ['c'],
        );
        final r = await run(agg);
        expect(r.rowCount, 3);
        final dept = r.column('dept')!.toList();
        final c = r.column('c')!.toList();
        final byDept = {for (int i = 0; i < dept.length; i++) dept[i]: c[i]};
        expect(byDept['x'], 2);
        expect(byDept['y'], 2);
        expect(byDept[null], 1); // the null-dept row groups separately
      });

      test('global aggregates (no group by)', () async {
        final agg = PhysicalAggregate(
          input: scan(people()),
          groupBy: const [],
          aggregates: [
            PhysicalCount(),
            PhysicalAvg(argument: col('age', DataType.int64)),
            PhysicalMin(argument: col('age', DataType.int64)),
            PhysicalMax(argument: col('age', DataType.int64)),
          ],
          aliases: ['c', 'a', 'mn', 'mx'],
        );
        final r = await run(agg);
        expect(r.rowCount, 1);
        expect(r.column('c')!.toList(), [5]);
        expect(r.column('a')!.toList(), [40.0]);
        expect(r.column('mn')!.toList(), [20]);
        expect(r.column('mx')!.toList(), [60]);
      });
    });

    group('HashJoin (all join types)', () {
      final l = t('l', 'k', [1, 2, 3], 'lv', ['a', 'b', 'c']);
      final r = t('r', 'k', [2, 3, 4], 'rv', ['B', 'C', 'D']);

      Future<RecordBatch> join(lp.JoinType type) => run(
        PhysicalHashJoin(
          left: scan(l),
          right: scan(r),
          leftKeys: ['k'],
          rightKeys: ['k'],
          joinType: type,
        ),
      );

      test('inner keeps only matches', () async {
        final j = await join(lp.JoinType.inner);
        expect(j.rowCount, 2);
        expect((j.column('k')!.toList()..sort()), [2, 3]);
      });

      test('left keeps unmatched left with null right', () async {
        final j = await join(lp.JoinType.left);
        expect(j.rowCount, 3);
        // k=1 has no right match -> rv null
        final k = j.column('k')!.toList();
        final rv = j.column('rv')!.toList();
        expect(rv[k.indexOf(1)], isNull);
      });

      test('right keeps unmatched right with null left', () async {
        final j = await join(lp.JoinType.right);
        expect(j.rowCount, 3);
        final rv = j.column('rv')!.toList();
        final lv = j.column('lv')!.toList();
        expect(lv[rv.indexOf('D')], isNull); // k=4 only on right
      });

      test('full keeps all', () async {
        final j = await join(lp.JoinType.full);
        expect(j.rowCount, 4); // k in {1,2,3,4}
      });

      test('cross is the product', () async {
        final j = await join(lp.JoinType.cross);
        expect(j.rowCount, 9);
      });
    });

    group('semi/anti HashJoin (3VL)', () {
      // A key column that may be null, to exercise the null rules.
      Table keyed(String name, List<int?> ks, String vCol, List<String> vs) =>
          Table(
            name: name,
            columns: [
              NumericColumn.int64(name: 'k', values: ks, nullable: true),
              ListColumn<String>(
                name: vCol,
                dataType: DataType.string,
                data: vs,
                nullable: false,
              ),
            ],
            schema: Schema(
              fields: [
                Field(name: 'k', dataType: DataType.int64, nullable: true),
                Field(name: vCol, dataType: DataType.string, nullable: false),
              ],
            ),
          );

      final left = keyed('l', [1, null, 3], 'lv', ['a', 'n', 'c']);
      final right = keyed('r', [2, 3, 4], 'rv', ['B', 'C', 'D']);

      Future<List<Object?>> lv(Table l, Table r, lp.JoinType type) async {
        final j = await run(
          PhysicalHashJoin(
            left: scan(l),
            right: scan(r),
            leftKeys: ['k'],
            rightKeys: ['k'],
            joinType: type,
          ),
        );
        // A semi/anti join emits the LEFT schema only.
        expect(j.schema.fieldNames, ['k', 'lv']);
        if (j.rowCount == 0) return const [];
        final out = j.column('lv')!.toList()..sort();
        return out;
      }

      test(
        'SEMI keeps left rows with a match; a null key matches nothing',
        () async {
          // k=3 is in {2,3,4}; k=1 and k=null are not.
          expect(await lv(left, right, lp.JoinType.semi), ['c']);
        },
      );

      test(
        'ANTI (NOT EXISTS) keeps unmatched left, INCLUDING a null key',
        () async {
          // k=1 (no match) and k=null (matches nothing → unmatched) are kept; k=3
          // has a match and is dropped.
          expect(await lv(left, right, lp.JoinType.anti), ['a', 'n']);
        },
      );

      test('antiNullAware (NOT IN) drops a null-key left row', () async {
        // k=1 not in set → kept; k=null → UNKNOWN → dropped; k=3 in set → dropped.
        expect(await lv(left, right, lp.JoinType.antiNullAware), ['a']);
      });

      test('antiNullAware over an EMPTY build keeps every left row', () async {
        // x NOT IN (empty) is TRUE for every row, even a null key.
        final empty = keyed('r', [], 'rv', []);
        expect(await lv(left, empty, lp.JoinType.antiNullAware), [
          'a',
          'c',
          'n',
        ]);
      });

      test('antiNullAware over a build with a NULL keeps nothing', () async {
        // x NOT IN (S) is UNKNOWN for every row when S holds a null.
        final withNull = keyed('r', [2, null], 'rv', ['B', 'N']);
        expect(await lv(left, withNull, lp.JoinType.antiNullAware), isEmpty);
      });

      test('SEMI survives a spill', () async {
        final big = <int?>[for (int i = 0; i < 400; i++) i];
        final l = keyed('l', big, 'lv', [for (final i in big) 'l$i']);
        // right holds only even keys.
        final evens = [for (int i = 0; i < 400; i += 2) i];
        final r = keyed('r', evens, 'rv', [for (final i in evens) 'r$i']);
        final j = await run(
          PhysicalHashJoin(
            left: scan(l),
            right: scan(r),
            leftKeys: ['k'],
            rightKeys: ['k'],
            joinType: lp.JoinType.semi,
            // A tiny positive threshold forces the grace-partition path (0 would
            // DISABLE spilling: `spills` is `spillThresholdBytes > 0`).
            options: const EngineOptions(spillThresholdBytes: 256),
          ),
        );
        expect(j.rowCount, 200); // the even keys
      });

      // F3: a high-cardinality semi build must bound its distinct-key set and
      // repartition, not accumulate an unbounded Set. A tiny threshold forces the
      // set past budget so the repartition path runs; the answer must be exact.
      test('SEMI repartitions a high-cardinality build (F3)', () async {
        final all = <int?>[for (int i = 0; i < 2000; i++) i];
        final l = keyed('l', all, 'lv', [for (final i in all) 'l$i']);
        final thirds = [for (int i = 0; i < 2000; i += 3) i];
        final r = keyed('r', thirds, 'rv', [for (final i in thirds) 'r$i']);
        final j = await run(
          PhysicalHashJoin(
            left: scan(l),
            right: scan(r),
            leftKeys: ['k'],
            rightKeys: ['k'],
            joinType: lp.JoinType.semi,
            options: const EngineOptions(spillThresholdBytes: 512),
          ),
        );
        expect(j.rowCount, thirds.length);
      });
    });

    group('vectorized Sort', () {
      test('descending by age; typed column preserved', () async {
        final s = PhysicalSort(
          input: scan(people()),
          sortExpressions: [
            SortExpression(
              expression: col('age', DataType.int64),
              ascending: false,
            ),
          ],
        );
        final r = await run(s);
        expect(r.column('age'), isA<NumericColumn>());
        expect(r.column('age')!.toList(), [60, 50, 40, 30, 20]);
      });

      test('stable: ties keep original order', () async {
        // Sort only by grp -> within grp=1 the original age order (20,30) is kept.
        final s = PhysicalSort(
          input: scan(people()),
          sortExpressions: [
            SortExpression(
              expression: col('grp', DataType.int64),
              ascending: true,
            ),
          ],
        );
        final r = await run(s);
        expect(r.column('age')!.toList(), [20, 30, 40, 50, 60]);
      });
    });

    group('expression completion (CASE / CAST / functions)', () {
      Future<List<Object?>> project(PhysicalExpression e, String alias) async {
        final p = PhysicalProjection(
          input: scan(people()),
          expressions: [e],
          aliases: [alias],
        );
        return (await run(p)).column(alias)!.toList();
      }

      test('CASE WHEN', () async {
        final e = PhysicalCaseExpression(
          [
            PhysicalWhen(
              PhysicalBinaryExpression(
                left: col('age', DataType.int64),
                operator: lp.BinaryOperator.gte,
                right: PhysicalLiteralExpression(40, DataType.int64),
                resultType: DataType.boolean,
              ),
              PhysicalLiteralExpression('old', DataType.string),
            ),
          ],
          PhysicalLiteralExpression('young', DataType.string),
          DataType.string,
        );
        expect(await project(e, 'cat'), [
          'young',
          'young',
          'old',
          'old',
          'old',
        ]);
      });

      test('UPPER and COALESCE handle nulls', () async {
        expect(
          await project(
            PhysicalFunctionExpression('UPPER', [col('dept', DataType.string)]),
            'u',
          ),
          ['X', 'X', 'Y', null, 'Y'],
        );
        expect(
          await project(
            PhysicalFunctionExpression('COALESCE', [
              col('dept', DataType.string),
              PhysicalLiteralExpression('none', DataType.string),
            ]),
            'co',
          ),
          ['x', 'x', 'y', 'none', 'y'],
        );
      });

      test('CAST int to double', () async {
        final e = PhysicalCastExpression(
          col('age', DataType.int64),
          DataType.float64,
        );
        expect(await project(e, 'd'), [20.0, 30.0, 40.0, 50.0, 60.0]);
      });
    });
  });

  group('where pushdown', () {
    // A one-column row group spanning [lo, hi].
    Map<String, ColumnBounds> g(int lo, int hi) => {
      'id': ColumnBounds(
        min: lo,
        max: hi,
        nullCount: 0,
        rowCount: 100,
        hasStats: true,
      ),
    };

    group('logicalToRowFilter (#3.1)', () {
      lp.LogicalExpression lcol(String n) =>
          lp.ColumnExpression(name: n, columnType: DataType.int32);
      lp.LogicalExpression llit(Object? v) =>
          lp.LiteralExpression(v, DataType.int32);
      lp.LogicalExpression lbin(
        lp.LogicalExpression l,
        lp.BinaryOperator op,
        lp.LogicalExpression r,
      ) => lp.BinaryExpression(l, op, r);

      test('col OP literal translates and prunes', () {
        final f = logicalToRowFilter(
          lbin(lcol('id'), lp.BinaryOperator.gt, llit(5)),
        )!;
        expect(f.keep(g(0, 4)), isFalse);
        expect(f.keep(g(0, 10)), isTrue);
      });

      test('AND prunes on either side; cross-column conjunct is dropped', () {
        final f = logicalToRowFilter(
          lbin(
            lbin(lcol('id'), lp.BinaryOperator.gte, llit(10)),
            lp.BinaryOperator.and,
            lbin(lcol('a'), lp.BinaryOperator.eq, lcol('b')),
          ),
        )!;
        expect(f.keep(g(0, 9)), isFalse);
        expect(f.columns, {'id'});
      });
    });

    group('end-to-end SQL pushdown over Parquet', () {
      late Directory dir;
      late String path;
      setUp(() async {
        dir = Directory.systemTemp.createTempSync('sqlpush_');
        path = '${dir.path}/d.parquet';
        final table = Table(
          name: 't',
          columns: [
            NumericColumn.int32(
              name: 'id',
              values: List.generate(1000, (i) => i),
              nullable: false,
            ),
            ListColumn<String>(
              name: 'name',
              dataType: DataType.string,
              data: List.generate(1000, (i) => 'n$i'),
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int32, nullable: false),
              Field(name: 'name', dataType: DataType.string, nullable: false),
            ],
          ),
        );
        await ParquetWriter(
          path,
          options: const ParquetWriterOptions(rowGroupSize: 100),
        ).write(table).ok;
      });
      tearDown(() => dir.deleteSync(recursive: true));

      test('WHERE prunes row groups and returns exact rows', () async {
        ParquetReader.rowGroupsPruned = 0;
        final engine = SqlEngine.open(allowExternalPaths: true).ok;
        final result = await engine
            .execute("SELECT * FROM '$path' WHERE id >= 500")
            .ok;
        expect(result.rowCount, 500);
        final ids = result.column('id')!.toList().cast<int>();
        expect(ids.first, 500);
        expect(ids.last, 999);
        expect(
          ParquetReader.rowGroupsPruned,
          greaterThan(0),
          reason: 'pushdown should skip the leading row groups',
        );
      });

      test('multi-conjunct WHERE prunes both ends through the rule', () async {
        ParquetReader.rowGroupsPruned = 0;
        final engine = SqlEngine.open(allowExternalPaths: true).ok;
        final result = await engine
            .execute("SELECT * FROM '$path' WHERE id >= 500 AND id < 800")
            .ok;
        final ids = result.column('id')!.toList().cast<int>();
        expect(result.rowCount, 300);
        expect(ids.first, 500);
        expect(ids.last, 799);
        expect(ParquetReader.rowGroupsPruned, greaterThan(0));
      });

      test('a non-prunable predicate still returns correct rows', () async {
        ParquetReader.rowGroupsPruned = 0;
        final engine = SqlEngine.open(allowExternalPaths: true).ok;
        final result = await engine
            .execute("SELECT * FROM '$path' WHERE name = 'n7'")
            .ok;
        expect(result.rowCount, 1);
        expect(result.column('id')!.toList(), [7]);
      });
    });
  });

  group('operator close', () {
    final schema = Schema(
      fields: [Field(name: 'v', dataType: DataType.int64, nullable: false)],
    );

    test(
      'SetOpIterator.close closes the right child even if left throws',
      () async {
        final left = _FakeIterator(schema, throwOnClose: true);
        final right = _FakeIterator(schema);
        final op = SetOpIterator(
          left,
          right,
          schema,
          lp.SetOpType.union,
          false,
          const EngineOptions(),
          schema,
          schema,
        );

        await expectLater(op.close(), throwsA(isA<StateError>()));
        // The left throw must not prevent the right child from being closed.
        expect(right.closed, isTrue);
        expect(left.closed, isTrue);
      },
    );
  });

  // ---- Relocated: audit_round47 (#9 SpillBudget is a hard ceiling) ----
  group('#9 SpillBudget is a hard ceiling', () {
    test('a positive adjust over the cap throws (not silently crossed)', () {
      final b = SpillBudget(100);
      b.charge(90);
      expect(() => b.adjust(20), throwsFailure(Kind.limit)); // 90+20 > 100
      expect(b.used, 90, reason: 'a rejected reservation counts nothing');
      b.adjust(-40); // a refund always applies
      expect(b.used, 50);
    });
  });

  // ---- Relocated: audit_round47 (#11 numeric stats are bounded and stable) --
  group('#11 numeric stats are bounded and stable', () {
    test(
      'distinctCount saturates at the cap for a high-cardinality column',
      () {
        final col = NumericColumn.int64(
          name: 'v',
          values: [for (var i = 0; i < 70000; i++) i],
          nullable: false,
        );
        final s = col.stats!;
        // 70000 distinct values but the probe is capped at 65536.
        expect(s.distinctCount, lessThanOrEqualTo(1 << 16));
        expect(s.distinctCount, greaterThan(60000));
      },
    );

    test('variance is stable for large-magnitude values (Welford)', () {
      // sum-of-squares would catastrophically cancel here; Welford stays exact.
      final col = NumericColumn.float64(
        name: 'v',
        values: [1000000000.0, 1000000001.0, 1000000002.0],
        nullable: false,
      );
      final s = col.stats!;
      // population variance of {0,1,2} is 2/3.
      expect(s.stdDev, closeTo(0.8164965809, 1e-6));
    });
  });

  // ---- Relocated: audit_round48 (S2 DISTINCT accumulators are bounded) ----
  group('S2 DISTINCT accumulators are bounded', () {
    test('multi-COUNT(DISTINCT) over a low maxOutputRows fails loud', () async {
      final dir = Directory.systemTemp.createTempSync('r48s2_');
      final engine = SqlEngine.open(spillDir: dir.path, maxOutputRows: 4).ok;
      try {
        engine.registerTable(
          't',
          Table(
            name: 't',
            columns: [
              NumericColumn.int64(
                name: 'k',
                values: [for (var i = 0; i < 40; i++) i],
                nullable: false,
              ),
              NumericColumn.int64(
                name: 'a',
                values: [for (var i = 0; i < 40; i++) i],
                nullable: false,
              ),
              NumericColumn.int64(
                name: 'b',
                values: [for (var i = 0; i < 40; i++) i],
                nullable: false,
              ),
            ],
            schema: Schema(
              fields: [
                Field(name: 'k', dataType: DataType.int64, nullable: false),
                Field(name: 'a', dataType: DataType.int64, nullable: false),
                Field(name: 'b', dataType: DataType.int64, nullable: false),
              ],
            ),
          ),
        );
        // A global multi-distinct aggregate takes the plain accumulator path,
        // which must be bounded by the shared distinct budget (>4 distinct).
        await expectLater(
          engine.execute(
            'SELECT COUNT(DISTINCT a) AS ca, COUNT(DISTINCT b) AS cb FROM t',
          ),
          failsWith(Kind.limit),
        );
      } finally {
        await engine.dispose();
        dir.deleteSync(recursive: true);
      }
    });
  });

  // ---- Relocated: audit_round49 (#4 byte-accurate memory limits) ----
  group('#4 byte-accurate memory limits', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('r49_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('multi-DISTINCT over large values fails on the byte budget', () async {
      // maxOutputRows: null opts OUT of the count cap, so only the byte budget
      // (spillThresholdBytes) can arm: a few large distinct values must trip it.
      final engine = SqlEngine.open(
        spillDir: dir.path,
        maxOutputRows: null,
        spillThresholdBytes: 256,
      ).ok;
      final big = 'x' * 500;
      engine.registerTable(
        't',
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(
              name: 'k',
              values: [for (var i = 0; i < 8; i++) i],
              nullable: false,
            ),
            ListColumn<String>(
              name: 'a',
              dataType: DataType.string,
              data: [for (var i = 0; i < 8; i++) '$big$i'],
              nullable: false,
            ),
            ListColumn<String>(
              name: 'b',
              dataType: DataType.string,
              data: [for (var i = 0; i < 8; i++) '$big${i + 100}'],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'k', dataType: DataType.int64, nullable: false),
              Field(name: 'a', dataType: DataType.string, nullable: false),
              Field(name: 'b', dataType: DataType.string, nullable: false),
            ],
          ),
        ),
      );
      try {
        // GROUP BY <expr> + multiple COUNT(DISTINCT) bails the planner rewrites,
        // hitting the plain per-group accumulator path the budget guards.
        await expectLater(
          engine.execute(
            'SELECT k % 2 AS g, COUNT(DISTINCT a) AS ca, '
            'COUNT(DISTINCT b) AS cb FROM t GROUP BY k % 2',
          ),
          failsWith(Kind.limit),
        );
      } finally {
        await engine.dispose();
      }
    });
  });

  // ---- Relocated: review_round42 (#3 Roaring allocation bombs) ----
  group('#3 Roaring allocation bombs', () {
    test('an implausible bucket count throws instead of allocating', () {
      final payload =
          (BytesBuilder()..add(
                (ByteData(8)..setUint64(0, 0xFFFFFFFF, Endian.little)).buffer
                    .asUint8List(),
              ))
              .toBytes();
      expect(
        () => Roaring.deserializeToBitmap(payload),
        throwsA(isA<FormatException>()),
      );
    });

    test('an implausible container count throws instead of allocating', () {
      final b = BytesBuilder()
        ..add(
          (ByteData(8)..setUint64(0, 1, Endian.little)).buffer.asUint8List(),
        )
        ..add(
          (ByteData(4)..setUint32(0, 0, Endian.little)).buffer.asUint8List(),
        )
        ..add(
          (ByteData(4)..setUint32(0, Roaring.serialCookieNoRun, Endian.little))
              .buffer
              .asUint8List(),
        )
        ..add(
          (ByteData(
            4,
          )..setUint32(0, 0xFFFFFFFF, Endian.little)).buffer.asUint8List(),
        );
      expect(
        () => Roaring.deserializeToBitmap(b.toBytes()),
        throwsA(isA<FormatException>()),
      );
    });
  });

  // ---- Relocated: review_round43 (#5 Roaring cardinality) ----
  group('#5 Roaring cardinality', () {
    test('a run-container bomb is rejected before expansion', () {
      // 27 bytes: 1 bucket, 1 run container declaring one run [0, +65535] → would
      // expand to 65536 values, far over a small declared cardinality.
      final b = ByteData(27);
      b.setUint64(0, 1, Endian.little); // bucketCount
      b.setUint32(8, 0, Endian.little); // bucket key
      b.setUint32(12, 12347, Endian.little); // run cookie, count=1
      b.setUint8(16, 0x01); // runBitset: container 0 is a run
      b.setUint16(17, 0, Endian.little); // descriptor key
      b.setUint16(19, 0, Endian.little); // descriptor card
      b.setUint16(21, 1, Endian.little); // runCount = 1
      b.setUint16(23, 0, Endian.little); // run value = 0
      b.setUint16(25, 65535, Endian.little); // run length = 65535
      final bytes = b.buffer.asUint8List();
      expect(
        () => Roaring.deserializeToBitmap(bytes, 0, 10), // descriptor says 10
        throwsA(isA<FormatException>()),
      );
    });

    test('a decoded count disagreeing with the descriptor throws', () {
      final valid = Roaring.serializeTreemap([1, 2, 3]);
      expect(
        () => Roaring.deserializeToBitmap(valid, 0, 99), // really 3, not 99
        throwsA(isA<FormatException>()),
      );
      // The exact count still decodes fine.
      expect(Roaring.deserializeToBitmap(valid, 0, 3).length, 3);
    });
  });

  // ---- Relocated: review_round44 (#8 Roaring rejects duplicate buckets) ----
  group('#8 Roaring rejects duplicate buckets', () {
    test('two buckets with the same high key throw', () {
      // bucketCount=2; bucket0 key=5 + empty no-run container; bucket1 key=5 dup.
      final b = ByteData(24);
      b.setUint64(0, 2, Endian.little); // bucketCount
      b.setUint32(8, 5, Endian.little); // bucket0 key
      b.setUint32(12, 12346, Endian.little); // no-run cookie
      b.setUint32(16, 0, Endian.little); // container count 0
      b.setUint32(20, 5, Endian.little); // bucket1 key (duplicate)
      expect(
        () => Roaring.deserializeToBitmap(b.buffer.asUint8List()),
        throwsA(isA<FormatException>()),
      );
    });
  });

  // ---- Relocated: review_round45 (#6 key-byte estimator) ----
  group('#6 key-byte estimator', () {
    test('weighs payload, not a flat per-key slot', () {
      final scalar = rowKeyBytes(RowKey([1]));
      final wide = rowKeyBytes(RowKey(['a very long string key value here']));
      final binary = rowKeyBytes(
        RowKey([Uint8List.fromList(List.filled(100, 0))]),
      );
      expect(wide, greaterThan(scalar));
      expect(binary, greaterThan(scalar));
    });
  });

  // ---- Relocated: hardening (spill-run corruption + eager-read cap) ----
  group('spill-run corruption + eager-read cap', () {
    test(
      '#1 a finalized spill run whose file vanished fails loud (not empty)',
      () async {
        // hasFile:true but the file is gone → another session swept it → reading it
        // as zero rows would silently drop a partition. Must throw.
        final run = SpillRun(
          '/tmp/libbigdata-missing-spill-x.parquet',
          Schema(
            fields: [Field(name: 'v', dataType: DataType.int64)],
          ),
        );
        expect(run.read().toList(), throwsFailure(Kind.io));
      },
    );

    test('#1 a legitimately empty run (no file) reads as empty', () async {
      final run = SpillRun(
        '/tmp/whatever',
        Schema(
          fields: [Field(name: 'v', dataType: DataType.int64)],
        ),
        hasFile: false,
      );
      expect(await run.read().toList(), isEmpty);
    });

    test('#3 the eager-read byte cap applies the overhead factor', () {
      // 600 MiB raw × 2.0 factor > 1 GiB cap → over.
      const cap = 1 << 30;
      expect(overByteCap(600 << 20, cap), isTrue);
      expect(overByteCap(400 << 20, cap), isFalse);
      expect(overByteCap(1 << 40, null), isFalse); // null cap = unbounded
    });
  });

  group('equi-join honours maxOutputRows', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('joincap_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table ids(List<int> v) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: v, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    test(
      'a hot-key join over the cap throws instead of buffering it all',
      () async {
        final engine = SqlEngine.open(spillDir: dir.path, maxOutputRows: 5).ok;
        // 10 rows all sharing one key -> a self-join yields 100 pairs, over the cap.
        engine.registerTable('a', ids(List.filled(10, 1)));
        engine.registerTable('b', ids(List.filled(10, 1)));
        await expectLater(
          engine.execute('SELECT a.id FROM a JOIN b ON a.id = b.id'),
          failsWith(Kind.limit),
        );
        await engine.dispose();
      },
    );
  });

  // INTERSECT/EXCEPT dedup their output with an `emitted` RowKey set that grows
  // with the DISTINCT OUTPUT. Its budget check depended on `!canRecurse`, which
  // is false at every depth an ordinary query reaches, so the guard never
  // fired, the memory reservation's denial was computed and discarded, and the
  // set grew without limit.
  //
  // It is now built in a first pass and consumed (`remove`) while emitting, so
  // an overflow can repartition like the input key sets do: recursion is only a
  // recovery while nothing has been yielded. These pin the dedup that the
  // two-pass emit performs; a bug there drops or duplicates rows.
  group('set-op output dedup set is bounded', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('setopbud_'));
    tearDown(() => dir.deleteSync(recursive: true));

    /// [n] rows drawn from [variants] distinct values, so the output dedups
    /// heavily and duplicates cross batch boundaries.
    Table dupes(String name, int n, int variants, {int from = 0}) => Table(
      name: name,
      columns: [
        NumericColumn.int64(
          name: 'v',
          values: [for (var i = 0; i < n; i++) from + (i % variants)],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [Field(name: 'v', dataType: DataType.int64, nullable: false)],
      ),
    );

    /// A tiny budget forces the grace path, so the emitted set must repartition
    /// rather than grow: the whole point of the guard.
    SqlEngine tiny() =>
        SqlEngine.open(spillDir: dir.path, spillThresholdBytes: 2048).ok;

    test(
      'EXCEPT under a tiny budget is correct and emits each row once',
      () async {
        final e = tiny();
        e.registerTable('l', dupes('l', 12000, 4000));
        e.registerTable('r', dupes('r', 100, 50, from: 100000)); // disjoint
        final v = (await e.execute('SELECT v FROM l EXCEPT SELECT v FROM r').ok)
            .column('v')!
            .toList();
        expect(v.length, 4000, reason: 'one row per distinct left value');
        expect(v.toSet().length, 4000, reason: 'no duplicates emitted');
        await e.dispose();
      },
    );

    test('a hard memory cap accounts for the concat transient (review 4)', () async {
      final schema = Schema(
        fields: [Field(name: 'v', dataType: DataType.int64, nullable: false)],
      );
      // Many rows, FEW distinct values: the input buffers dominate memory while
      // the distinct-key set stays tiny, so the peak is attributable to the
      // concat transient (not the dedup set); that is what makes this test
      // sensitive to whether the transient is reserved.
      RecordBatch batch(int from, int n) => RecordBatch(
        columns: [
          NumericColumn.int64(
            name: 'v',
            values: [for (var i = 0; i < n; i++) from + (i % 10)],
            nullable: false,
          ),
        ],
        schema: schema,
        rowCount: n,
      );
      final left = batch(0, 8000); // values 0..9
      final right = batch(5, 8000); // values 5..14; union distinct 0..14
      final inputBytes = left.memoryUsage + right.memoryUsage;

      Future<({Set<Object?> values, int peak})> run(int budget) async {
        final manager = _PeakManager(budget);
        final op = SetOpIterator(
          _ListIterator(schema, [left]),
          _ListIterator(schema, [right]),
          schema,
          lp.SetOpType.union,
          false,
          EngineOptions(
            spillThresholdBytes: 1 << 30, // never trip fill on the threshold
            spillDir: dir.path,
            memory: manager,
          ),
          schema,
          schema,
        );
        final values = <Object?>{};
        RecordBatch? b;
        while ((b = await op.next()) != null) {
          values.addAll(b!.columns.single.toList());
        }
        await op.close();
        return (values: values, peak: manager.peak);
      }

      final expected = {for (var i = 0; i < 15; i++) i}; // 0..14

      // Generous cap: the concat copies fit, so their transient is RESERVED and
      // the reserved peak reflects inputs + copies. Before the fix the transient
      // was unaccounted and the peak never rose above the buffered inputs.
      final big = await run(inputBytes * 4);
      expect(big.values, expected);
      expect(
        big.peak,
        greaterThan(inputBytes),
        reason: 'concat transient is now reserved, not silently unaccounted',
      );

      // Tight cap (fits the inputs but not the 2x concat transient): the reserve
      // is denied, the operator spills, and the reserved peak stays at/below the
      // ceiling: strictMemory is a real bound. The SPILLED result must still be
      // the correct distinct value set, not just the right count.
      final tight = inputBytes + inputBytes ~/ 2;
      final small = await run(tight);
      expect(small.values, expected);
      expect(small.peak, lessThanOrEqualTo(tight));
    });

    test(
      'INTERSECT under a tiny budget is correct and emits each row once',
      () async {
        final e = tiny();
        e.registerTable('l', dupes('l', 12000, 4000));
        e.registerTable('r', dupes('r', 9000, 3000)); // overlaps 0..2999
        final v =
            (await e.execute('SELECT v FROM l INTERSECT SELECT v FROM r').ok)
                .column('v')!
                .toList();
        expect(v.length, 3000);
        expect(v.toSet(), {for (var i = 0; i < 3000; i++) i});
        await e.dispose();
      },
    );

    test('a tiny budget agrees with the unbounded path', () async {
      Future<List<Object?>> run(int budget, String sql) async {
        final e = budget == 0
            ? SqlEngine.open(spillThresholdBytes: 0).ok
            : SqlEngine.open(
                spillDir: dir.path,
                spillThresholdBytes: budget,
              ).ok;
        e.registerTable('l', dupes('l', 8000, 2500));
        e.registerTable('r', dupes('r', 6000, 1200));
        final out = (await e.execute(sql).ok).column('v')!.toList()
          ..sort((a, b) => (a as int).compareTo(b as int));
        await e.dispose();
        return out;
      }

      for (final sql in [
        'SELECT v FROM l EXCEPT SELECT v FROM r',
        'SELECT v FROM l INTERSECT SELECT v FROM r',
      ]) {
        expect(await run(2048, sql), await run(0, sql), reason: sql);
      }
    });

    test('a spilled set-op leaves no files behind', () async {
      final e = tiny();
      e.registerTable('l', dupes('l', 8000, 2000));
      e.registerTable('r', dupes('r', 4000, 900));
      await e.execute('SELECT v FROM l EXCEPT SELECT v FROM r').ok;
      await e.dispose();
      expect(
        dir
            .listSync(recursive: true)
            .whereType<File>()
            .where((f) => f.uri.pathSegments.last.startsWith('spill-')),
        isEmpty,
      );
    });
  });

  group('top-n', () {
    // 3000 rows in batches of 256: v and s repeat (ties) and hold nulls, id
    // is unique, so comparing ids checks the exact order ties come out in.
    const rows = 3000;
    final table = Table.fromRows('t', [
      for (var i = 0; i < rows; i++)
        {
          'id': i,
          'v': i % 11 == 0 ? null : (i * 7919) % 37,
          's': i % 13 == 0 ? null : 's${(i * 31) % 29}',
          'd': i % 17 == 0 ? null : ((i * 13) % 101) / 4,
        },
    ]);
    PhysicalTableScan scan() =>
        PhysicalTableScan(tableName: 't', table: table, batchSize: 256);
    PhysicalColumnExpression col(String name) => PhysicalColumnExpression(
      name,
      table.schema.fields.firstWhere((f) => f.name == name).dataType,
    );

    Future<List<Object?>> ids(PhysicalPlan plan) async {
      final it = plan.execute();
      final out = <Object?>[];
      while (true) {
        final b = await it.next();
        if (b == null) break;
        out.addAll(b.column('id')!.toList());
      }
      await it.close();
      return out;
    }

    final orders = <String, List<SortExpression> Function()>{
      'v': () => [SortExpression(expression: col('v'))],
      'v desc nulls first': () => [
        SortExpression(
          expression: col('v'),
          ascending: false,
          nullsFirst: true,
        ),
      ],
      'v nulls first': () => [
        SortExpression(expression: col('v'), nullsFirst: true),
      ],
      's, v desc': () => [
        SortExpression(expression: col('s')),
        SortExpression(expression: col('v'), ascending: false),
      ],
      'd desc': () => [SortExpression(expression: col('d'), ascending: false)],
    };

    for (final MapEntry(key: label, value: order) in orders.entries) {
      for (final count in [1, 10, 255, 2999, 5000]) {
        test('$label, $count rows match the full sort', () async {
          final top = PhysicalTopN(
            input: scan(),
            sortExpressions: order(),
            count: count,
          );
          final full = PhysicalLimit(
            input: PhysicalSort(input: scan(), sortExpressions: order()),
            limit: count,
          );
          expect(await ids(top), await ids(full));
        });
      }
    }

    test('holds at most count rows between batches', () async {
      final it =
          PhysicalTopN(
                input: scan(),
                sortExpressions: orders['v']!(),
                count: 10,
              ).execute()
              as TopNIterator;
      while (await it.next() != null) {}
      await it.close();
      expect(it.peak, 10);
    });

    test('SQL ORDER BY with LIMIT and OFFSET matches the full sort', () async {
      final engine = SqlEngine.open().ok;
      addTearDown(engine.dispose);
      engine.registerTable('t', table);
      Future<List<Object?>> sql(String query) async => [
        for (final row in (await engine.execute(query).ok).toRows()) row['id'],
      ];
      final full = await sql('SELECT id FROM t ORDER BY s DESC, d');
      expect(
        await sql('SELECT id FROM t ORDER BY s DESC, d LIMIT 25 OFFSET 7'),
        full.sublist(7, 32),
      );
    });
  });

  group('grouped aggregate and the shared memory budget', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('agg_memory_'));
    tearDown(() => dir.deleteSync(recursive: true));

    final schema = Schema(
      fields: [Field(name: 'k', dataType: DataType.int64, nullable: false)],
    );

    // 20000 distinct keys, each twice, in batches of 4000 rows.
    final batches = [
      for (var from = 0; from < 40000; from += 4000)
        RecordBatch(
          columns: [
            NumericColumn.int64(
              name: 'k',
              values: [for (var i = from; i < from + 4000; i++) i % 20000],
              nullable: false,
            ),
          ],
          schema: schema,
          rowCount: 4000,
        ),
    ];

    AggregateIterator aggregate(
      RecordBatchIterator input,
      MemoryManager? memory,
    ) => AggregateIterator(
      input: input,
      groupBy: [PhysicalColumnExpression('k', DataType.int64)],
      aggregates: [PhysicalCount()],
      aliases: ['c'],
      options: EngineOptions(
        spillThresholdBytes: 1 << 30, // only the shared budget can spill
        spillDir: dir.path,
        memory: memory,
      ),
    );

    Future<Map<Object?, Object?>> drain(RecordBatchIterator op) async {
      final counts = <Object?, Object?>{};
      RecordBatch? b;
      while ((b = await op.next()) != null) {
        final keys = b!.column('k')!.toList();
        final values = b.column('c')!.toList();
        for (var i = 0; i < keys.length; i++) {
          counts[keys[i]] = values[i];
        }
      }
      await op.close();
      return counts;
    }

    test('a budget below the threshold spills and stays within it', () async {
      final expected = await drain(
        aggregate(_ListIterator(schema, batches), null),
      );
      expect(expected, hasLength(20000));
      const budget = 256 * 1024;
      final manager = _PeakManager(budget);
      final counts = await drain(
        aggregate(_ListIterator(schema, batches), manager),
      );
      expect(counts, expected);
      expect(manager.peak, greaterThan(0), reason: 'groups are reserved');
      expect(manager.peak, lessThanOrEqualTo(budget));
      expect(
        manager.denied,
        greaterThan(0),
        reason: 'the budget forced a spill',
      );
      expect(manager.used, 0);
    });

    test('a stream closed after its first batch releases its groups', () async {
      final manager = _PeakManager(1 << 30);
      final op = aggregate(_ListIterator(schema, batches), manager);
      expect(await op.next(), isNotNull);
      expect(manager.used, greaterThan(0));
      await op.close();
      expect(manager.used, 0);
    });

    test('a group the pool refused stays out of the table', () async {
      // Batch 0 arrives with the pool full, so group 2 goes to a partition;
      // the pool frees up before batch 1. Group 2 must not then be created in
      // the table too, or it comes out twice.
      final manager = MemoryManager(1 << 20, hardCap: true);
      final input = _CallbackIterator(schema, [
        (
          batch: RecordBatch(
            columns: [
              NumericColumn.int64(
                name: 'k',
                values: [1, 2, 2],
                nullable: false,
              ),
            ],
            schema: schema,
            rowCount: 3,
          ),
          before: () => manager.reserve(manager.budget - 1),
        ),
        (
          batch: RecordBatch(
            columns: [
              NumericColumn.int64(name: 'k', values: [2, 2], nullable: false),
            ],
            schema: schema,
            rowCount: 2,
          ),
          before: () => manager.release(manager.budget - 1),
        ),
      ]);
      final counts = await drain(aggregate(input, manager));
      expect(counts, {1: 1, 2: 4});
    });

    test('a failing input releases the groups already reserved', () async {
      final manager = _PeakManager(1 << 30);
      final op = aggregate(_FailingIterator(schema, batches.first), manager);
      await expectLater(op.next(), throwsStateError);
      expect(manager.used, greaterThan(0));
      await op.close();
      expect(manager.used, 0);
    });
  });

  group('blocking operators release what they hold', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('release_'));
    tearDown(() => dir.deleteSync(recursive: true));

    final schema = Schema(
      fields: [Field(name: 'k', dataType: DataType.int64, nullable: false)],
    );

    /// [count] batches of [rows] consecutive values from [start].
    _ListIterator feed(int start, int count, int rows) =>
        _ListIterator(schema, [
          for (var i = 0; i < count; i++)
            RecordBatch(
              columns: [
                NumericColumn.int64(
                  name: 'k',
                  values: [for (var j = 0; j < rows; j++) start + i * rows + j],
                  nullable: false,
                ),
              ],
              schema: schema,
              rowCount: rows,
            ),
        ]);

    test('a set operation closed early returns its reservations', () async {
      final memory = MemoryManager(1 << 30);
      final op = SetOpIterator(
        feed(0, 10, 1000),
        feed(5, 10, 1000),
        schema,
        lp.SetOpType.union,
        false,
        EngineOptions(
          spillThresholdBytes: 20000,
          spillDir: dir.path,
          memory: memory,
        ),
        schema,
        schema,
      );
      expect(await op.next(), isNotNull);
      await op.close();
      expect(memory.used, 0);
    });

    test('a failed spill finish returns its budget and files', () async {
      final budget = SpillBudget(2000);
      final op = SetOpIterator(
        feed(0, 2, 20),
        feed(5, 2, 20),
        schema,
        lp.SetOpType.union,
        false,
        EngineOptions(
          spillThresholdBytes: 1,
          spillDir: dir.path,
          spillBudget: budget,
        ),
        schema,
        schema,
      );
      await expectLater(op.next(), throwsA(isA<Failure>()));
      await op.close();
      expect(budget.used, 0);
      expect(
        dir
            .listSync(recursive: true)
            .whereType<File>()
            .where((file) => file.path.contains('spill-')),
        isEmpty,
      );
    });
  });
}
