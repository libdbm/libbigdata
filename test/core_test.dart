import 'dart:async';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/core/backoff.dart';
import 'package:libbigdata/src/core/conversions.dart';
import 'package:libbigdata/src/core/encoding.dart';
import 'package:libbigdata/src/core/keys.dart';
import 'package:libbigdata/src/core/partitions.dart';
import 'package:libbigdata/src/core/paths.dart';
import 'package:libbigdata/src/core/row_filter.dart';
import 'package:libbigdata/src/core/stats.dart';
import 'package:libbigdata/src/formats/delta/delta_schema.dart';
import 'package:libbigdata/src/formats/delta/roaring_bitmap.dart';
import 'package:libbigdata/src/formats/iceberg/iceberg_schema.dart';
import 'package:libbigdata/src/sql/exec/kernels.dart';
import 'package:libbigdata/src/sql/execution/memory_manager.dart';
import 'package:libbigdata/src/sql/execution/exec_core.dart';
import 'package:path/path.dart' as p;
import 'package:test/test.dart';

void main() {
  group('column immutability', () {
    NumericColumn<int> intCol() =>
        NumericColumn.int64(name: 'v', values: [1, 2, null, 4], nullable: true);

    test('buffer is a read-only view but still casts to its concrete type', () {
      final col = intCol();
      final buf = col.buffer;
      expect(buf, isA<Int64List>()); // concrete type preserved
      expect((buf as Int64List)[0], 1);
      expect(() => (col.buffer as List<int>)[0] = 99, throwsUnsupportedError);
      // The column's own values are untouched.
      expect(col[0], 1);
    });

    test('nullBitmap is a read-only view', () {
      final col = intCol();
      final bitmap = col.nullBitmap;
      expect(bitmap, isNotNull);
      expect(() => bitmap![0] = 0xFF, throwsUnsupportedError);
    });

    test('Table.columns rejects structural mutation', () {
      final col = intCol();
      final table = Table(
        name: 't',
        columns: [col],
        schema: Schema(
          fields: [Field(name: 'v', dataType: DataType.int64, nullable: true)],
        ),
      );
      expect(() => table.columns.add(col), throwsUnsupportedError);
      // Mutating the source list after construction does not affect the table.
      final source = [col];
      final t2 = Table(
        name: 't',
        columns: source,
        schema: Schema(
          fields: [Field(name: 'v', dataType: DataType.int64, nullable: true)],
        ),
      );
      source.clear();
      expect(t2.columns.length, 1);
    });
  });

  // Parquet writes UTF8 min/max in unsigned BYTE order (so does pyarrow), so the
  // pruning probe has to compare the same way. Dart's String.compareTo is
  // UTF-16 code-unit order, which disagrees above the BMP: surrogates
  // (D800-DFFF) sort below E000-FFFF, but their code points sort above.
  group('string bounds pruning uses byte order', () {
    RowFilter probe(String value) => RowFilter.compare('s', FilterOp.eq, value);

    ColumnBounds bounds(String min, String max) => ColumnBounds(
      min: min,
      max: max,
      rowCount: 10,
      hasStats: true,
      nullCount: 0,
    );

    test('an astral bound keeps a BMP value inside its byte range', () {
      // "\uE000" < "\uF000" < "\u{10000}" by UTF-8 bytes. In UTF-16 order the
      // max reads as its lead surrogate D800, putting F000 above it — and the
      // row group gets pruned away.
      expect(
        probe('\uF000').keep({'s': bounds('\uE000', '\u{10000}')}),
        isTrue,
      );
    });

    test('a value genuinely outside the range is still pruned', () {
      expect(
        probe('\u0041').keep({'s': bounds('\uE000', '\u{10000}')}),
        isFalse,
      );
    });

    test('ordinary ASCII ranges are unaffected', () {
      expect(probe('m').keep({'s': bounds('a', 'z')}), isTrue);
      expect(probe('Z').keep({'s': bounds('a', 'z')}), isFalse);
    });
  });

  // hasNulls drives real decisions (Parquet's required-leaf choice, concat's
  // nullability, the reader's authoritative-data promotion) but had no direct
  // coverage of its own.
  group('hasNulls', () {
    ListColumn<String> strings(List<String?> data, {bool nullable = true}) =>
        ListColumn<String>(
          name: 'v',
          dataType: DataType.string,
          data: data,
          nullable: nullable,
        );

    test('a nullable boxed column reports the nulls it holds', () {
      expect(strings(['a', null, 'c']).hasNulls, isTrue);
      expect(strings([null, null]).hasNulls, isTrue);
    });

    test('a nullable boxed column with no nulls reports none', () {
      expect(strings(['a', 'b']).hasNulls, isFalse);
      expect(strings([]).hasNulls, isFalse);
    });

    test('a non-nullable boxed column reports none', () {
      expect(strings(['a', 'b'], nullable: false).hasNulls, isFalse);
    });

    test('a non-nullable boxed column rejects a null outright', () {
      expect(() => strings(['a', null], nullable: false), throwsArgumentError);
    });

    // KNOWN DIVERGENCE, pinned so it cannot drift further: NumericColumn
    // allocates its null bitmap whenever the column is nullable and answers
    // `_nullBitmap != null`, so it reports "is nullable" where ListColumn
    // reports "holds a null". Every consumer only ever WIDENS nullability on a
    // true (concat, the Parquet reader's promotion, the writer's required-leaf
    // choice), so this over-declares rather than corrupts.
    test('a nullable numeric column reports nullability, not actual nulls', () {
      expect(
        NumericColumn.int64(
          name: 'v',
          values: [1, null],
          nullable: true,
        ).hasNulls,
        isTrue,
      );
      expect(
        NumericColumn.int64(name: 'v', values: [1, 2], nullable: true).hasNulls,
        isTrue, // no null present; ListColumn would answer false here
      );
      expect(
        NumericColumn.int64(
          name: 'v',
          values: [1, 2],
          nullable: false,
        ).hasNulls,
        isFalse,
      );
    });

    test('a mostly-null column costs no more than its data', () {
      // The null POSITIONS used to be retained in a Set<int> that memoryUsage
      // never counted, so a mostly-null column under-reported several-fold and
      // could slip past a spill/maxBytes budget.
      const rows = 100000;
      final column = strings([for (var i = 0; i < rows; i++) null]);
      expect(column.hasNulls, isTrue);
      expect(column.memoryUsage, rows * 8);
    });
  });

  group('numeric edges', () {
    SqlEngine engine() {
      final e = SqlEngine();
      e.registerTable(
        't',
        Table(
          name: 't',
          columns: [
            NumericColumn.float64(
              name: 'x',
              values: [1e30, double.infinity, double.nan, 1.5],
              nullable: false,
            ),
            NumericColumn.int64(
              name: 'id',
              values: [1, 2, 3, 4],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'x', dataType: DataType.float64),
              Field(name: 'id', dataType: DataType.int64),
            ],
          ),
        ),
      );
      return e;
    }

    test(
      'CAST of an out-of-range / non-finite double to int is NULL (no crash)',
      () async {
        final t = await engine().execute(
          'SELECT CAST(x AS BIGINT) AS r FROM t',
        );
        final r = t.toRows().map((row) => row['r']).toList();
        expect(r[0], isNull); // 1e30 — out of int64 range
        expect(r[1], isNull); // Infinity
        expect(r[2], isNull); // NaN
        expect(r[3], 1); // 1.5 → 1 (in range)
      },
    );

    test(
      'ROUND with an extreme exponent does not return Infinity/NaN',
      () async {
        final t = await engine().execute(
          'SELECT ROUND(x, 400) AS r FROM t WHERE id = 4',
        );
        final v = t.toRows().single['r'] as double;
        expect(v.isFinite, isTrue);
        expect(v, closeTo(1.5, 1e-9));
      },
    );

    test(
      'negative LIMIT fails loud instead of silently returning empty',
      () async {
        expect(
          engine().execute('SELECT id FROM t LIMIT -1'),
          throwsA(anything),
        );
      },
    );
  });

  group('sum overflow', () {
    SqlEngine engineWith(List<int> values) {
      final e = SqlEngine();
      e.registerTable(
        't',
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'v', values: values, nullable: false),
          ],
          schema: Schema(
            fields: [Field(name: 'v', dataType: DataType.int64)],
          ),
        ),
      );
      return e;
    }

    test('SUM past 2^63 does not overflow (was a silent negative)', () async {
      // 3 × 8e18 = 2.4e19 > int64 max (~9.2e18) — would wrap to negative.
      final t = await engineWith([
        8000000000000000000,
        8000000000000000000,
        8000000000000000000,
      ]).execute('SELECT SUM(v) AS s FROM t');
      expect(
        t.toRows().single['s'],
        Decimal(BigInt.parse('24000000000000000000'), 0),
      );
    });

    test('SUM stays exact past 2^53 (no float64 precision loss)', () async {
      // (2^53 + 1) twice = 2^54 + 2; float64 would round to 2^54.
      const big = (1 << 53) + 1;
      final t = await engineWith([
        big,
        big,
      ]).execute('SELECT SUM(v) AS s FROM t');
      expect(
        t.toRows().single['s'],
        Decimal(BigInt.from(big) + BigInt.from(big), 0),
      );
    });

    test('the integer-SUM column is a BigInt-backed decimal', () async {
      final t = await engineWith([
        1,
        2,
        3,
      ]).execute('SELECT SUM(v) AS s FROM t');
      // The result column is a lossless decimal holding a BigInt. (The final
      // result *schema field* is still mislabeled by the SELECT projection's
      // placeholder column types — a separate, pre-existing planner-binding gap
      // that affects every aggregate output, not just SUM.)
      expect(t.column('s')!.dataType, DataType.decimal);
      expect(t.toRows().single['s'], Decimal(BigInt.from(6), 0));
    });

    test('SUM over a floating column stays float64', () async {
      final e = SqlEngine();
      e.registerTable(
        'f',
        Table(
          name: 'f',
          columns: [
            NumericColumn.float64(
              name: 'x',
              values: [1.5, 2.5],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [Field(name: 'x', dataType: DataType.float64)],
          ),
        ),
      );
      final t = await e.execute('SELECT SUM(x) AS s FROM f');
      expect(t.toRows().single['s'], 4.0);
    });
  });

  group('composite keys', () {
    Table keyed(List<String> a, List<String> b, List<String> v) => Table(
      name: 't',
      columns: [
        ListColumn<String>(name: 'a', dataType: DataType.string, data: a),
        ListColumn<String>(name: 'b', dataType: DataType.string, data: b),
        ListColumn<String>(name: 'v', dataType: DataType.string, data: v),
      ],
      schema: Schema(
        fields: [
          Field(name: 'a', dataType: DataType.string),
          Field(name: 'b', dataType: DataType.string),
          Field(name: 'v', dataType: DataType.string),
        ],
      ),
    );

    Table mergeTbl(List<int> id1, List<int> id2, List<String> name) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id1', values: id1, nullable: false),
        NumericColumn.int64(name: 'id2', values: id2, nullable: false),
        ListColumn<String>(name: 'name', dataType: DataType.string, data: name),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id1', dataType: DataType.int64, nullable: false),
          Field(name: 'id2', dataType: DataType.int64, nullable: false),
          Field(name: 'name', dataType: DataType.string, nullable: false),
        ],
      ),
    );

    test('splitByKeys keeps tuples a string join would collide separate', () {
      // (a="1", b="23") vs (a="12", b="3") → "123" under a naive join.
      final groups = keyed(['1', '12'], ['23', '3'], [
        'x',
        'y',
      ]).splitByKeys(['a', 'b']);
      expect(groups.length, 2);
      expect({for (final g in groups) g.values['a']}, {'1', '12'});
    });

    test('splitByKeys groups NaN keys together (audit 7)', () {
      // double.nan == double.nan is false, so NaN keys never compared equal:
      // one partition PER NaN ROW (then a spurious maxPartitions failure). SQL
      // grouping treats NaN as equal to itself.
      final t = Table(
        name: 't',
        columns: [
          NumericColumn.adopt(
            name: 'k',
            dataType: DataType.float64,
            data: Float64List.fromList([double.nan, double.nan, 1.5]),
          ),
          NumericColumn.int64(name: 'v', values: [1, 2, 3], nullable: false),
        ],
        schema: Schema(
          fields: [
            Field(name: 'k', dataType: DataType.float64, nullable: false),
            Field(name: 'v', dataType: DataType.int64, nullable: false),
          ],
        ),
      );
      final groups = t.splitByKeys(['k']);
      expect(groups.length, 2);
      final nan = groups.firstWhere((g) => (g.values['k'] as double).isNaN);
      expect(nan.table.rowCount, 2);
    });

    test('splitByKeys keeps a real null distinct from the string "null"', () {
      final groups = keyed(['null', 'z'], ['x', 'x'], ['1', '2'])
          .append(
            Table(
              name: 't',
              columns: [
                ListColumn<String>(
                  name: 'a',
                  dataType: DataType.string,
                  data: [null],
                  nullable: true,
                ),
                ListColumn<String>(
                  name: 'b',
                  dataType: DataType.string,
                  data: ['x'],
                ),
                ListColumn<String>(
                  name: 'v',
                  dataType: DataType.string,
                  data: ['3'],
                ),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'a', dataType: DataType.string, nullable: true),
                  Field(name: 'b', dataType: DataType.string),
                  Field(name: 'v', dataType: DataType.string),
                ],
              ),
            ),
          )
          .splitByKeys(['a', 'b']);
      // "null" (string), "z", and a real null are three distinct groups.
      expect(groups.length, 3);
    });

    test('Delta MERGE on a composite key does not collide tuples', () async {
      final dir = Directory.systemTemp.createTempSync('ck_merge_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final w = DeltaWriter('${dir.path}/t');
      // Two rows whose (id1,id2) tuples both stringify to "123".
      await w.write(mergeTbl([1, 12], [23, 3], ['a', 'b']));
      // Source updates ONLY (1,23).
      await w.merge(
        mergeTbl([1], [23], ['UPDATED']),
        on: ['id1', 'id2'],
        whenMatchedUpdate: {'name': (t, s) => s['name']},
        whenNotMatchedInsert: false,
      );
      final t = await (await DataFrameIO.readDelta('${dir.path}/t')).execute();
      final byPair = {
        for (final r in t.toRows()) '${r['id1']}_${r['id2']}': r['name'],
      };
      expect(byPair['1_23'], 'UPDATED');
      expect(
        byPair['12_3'],
        'b',
      ); // untouched — would be clobbered on collision
    });
  });

  group('nested schema compatibility', () {
    Field struct(String name, List<Field> members) =>
        Field(name: name, dataType: DataType.struct, children: members);

    final a = Field(name: 'a', dataType: DataType.int32);

    test('structs with different members are incompatible', () {
      final s1 = Schema(
        fields: [
          struct('s', [a, Field(name: 'b', dataType: DataType.string)]),
        ],
      );
      final s2 = Schema(
        fields: [
          struct('s', [a, Field(name: 'c', dataType: DataType.float64)]),
        ],
      );
      expect(s1.isCompatibleWith(s2), isFalse);
    });

    test('identical nested structs stay compatible', () {
      final members = [a, Field(name: 'b', dataType: DataType.string)];
      expect(
        Schema(
          fields: [struct('s', members)],
        ).isCompatibleWith(Schema(fields: [struct('s', members)])),
        isTrue,
      );
    });

    test('decimal scale mismatch is incompatible; precision alone is fine', () {
      Schema dec(int p, int s) => Schema(
        fields: [
          Field(name: 'd', dataType: DataType.decimal, precision: p, scale: s),
        ],
      );
      expect(dec(18, 2).isCompatibleWith(dec(18, 3)), isFalse); // scale differs
      expect(dec(18, 2).isCompatibleWith(dec(20, 2)), isTrue); // precision only
    });

    test('plain width-promotable scalars remain compatible', () {
      final i32 = Schema(
        fields: [Field(name: 'x', dataType: DataType.int32)],
      );
      final i64 = Schema(
        fields: [Field(name: 'x', dataType: DataType.int64)],
      );
      expect(i32.isCompatibleWith(i64), isTrue);
    });
  });

  group('type inference', () {
    group('inference over all rows (not a 100-row sample)', () {
      test('a wide int beyond row 100 is not truncated', () async {
        // First 129 rows fit int8; row 130 needs int32. Building from a 100-row
        // sample would pick int8 and truncate 1000000 into an Int8List.
        final rows = [
          for (var i = 0; i < 130; i++) {'v': i < 129 ? 1 : 1000000},
        ];
        final t = await DataFrame.fromRows(rows).execute();
        expect(t.column('v')!.toList()[129], 1000000);
      });

      test('a field appearing only after row 100 is kept', () async {
        final rows = [
          for (var i = 0; i < 150; i++)
            i < 120 ? {'a': i} : {'a': i, 'late': 'x'},
        ];
        final t = await DataFrame.fromRows(rows).execute();
        expect(t.schema.fieldNames, contains('late'));
      });

      test('a type that changes after the sample widens to string', () async {
        final rows = [
          for (var i = 0; i < 130; i++) {'v': i < 125 ? i : 'oops'},
        ];
        // Must not throw a CastError during column build.
        final t = await DataFrame.fromRows(rows).execute();
        expect(t.column('v')!.dataType, DataType.string);
      });
    });

    group('float columns accept integer source values', () {
      test('an explicit float64 schema over int data builds and coerces', () {
        final schema = Schema(
          fields: [
            Field(name: 'price', dataType: DataType.float64, nullable: true),
          ],
        );
        final t = Table.fromRows('t', [
          {'price': 10},
          {'price': 10.5},
          {'price': null},
        ], schema: schema);
        final vals = t.column('price')!.toList();
        expect(vals[0], 10.0);
        expect(vals[1], 10.5);
        expect(vals[2], isNull);
      });

      test('a float32 field yields a float32 column (no dtype divergence)', () {
        final schema = Schema(
          fields: [
            Field(name: 'x', dataType: DataType.float32, nullable: false),
          ],
        );
        final t = Table.fromRows('t', [
          {'x': 1},
          {'x': 2},
        ], schema: schema);
        expect(t.column('x')!.dataType, DataType.float32);
      });
    });

    // An explicit schema is a CONTRACT. Column validation only checks a
    // column's declared dtype, not its values, so a boxed column would happily
    // hold a value of the wrong runtime type — the table then claims a type its
    // data contradicts, and the mismatch only surfaces far downstream (a writer,
    // a comparison, an isolate transfer).
    group('an explicit schema rejects values of the wrong type', () {
      Table build(DataType type, List<Object?> values) => Table.fromRows(
        't',
        [
          for (final v in values) {'a': v},
        ],
        schema: Schema(
          fields: [Field(name: 'a', dataType: type, nullable: true)],
        ),
      );

      test('an int into a string column throws', () {
        expect(
          () => build(DataType.string, [9]),
          throwsA(isA<ArgumentError>()),
        );
      });

      test('a bool into a string column throws', () {
        expect(
          () => build(DataType.string, [true]),
          throwsA(isA<ArgumentError>()),
        );
      });

      test('a string into an int column throws', () {
        expect(
          () => build(DataType.int64, ['oops']),
          throwsA(isA<ArgumentError>()),
        );
      });

      test('a string into a boolean column throws', () {
        expect(
          () => build(DataType.boolean, ['true']),
          throwsA(isA<ArgumentError>()),
        );
      });

      test('the error names the column and both types', () {
        expect(
          () => build(DataType.string, [9]),
          throwsA(
            isA<ArgumentError>().having(
              (e) => e.message.toString(),
              'message',
              allOf(contains('a'), contains('string')),
            ),
          ),
        );
      });

      test('nulls are always accepted', () {
        expect(build(DataType.string, [null, 'x']).column('a')!.toList(), [
          null,
          'x',
        ]);
      });

      test('int -> float widening still works (not a mismatch)', () {
        expect(build(DataType.float64, [10, 10.5]).column('a')!.toList(), [
          10.0,
          10.5,
        ]);
      });

      test('an inferred schema widens and coerces rather than throwing', () {
        // Types come from the values there, so mixed input widens to the common
        // type (string) rather than throwing. The value is COERCED to that type
        // ('9', not raw int 9) so the column's data matches its declared dtype —
        // an inferred string column holding an int would be a dtype lie the
        // Parquet/Arrow writers and the compare kernel later trip on.
        final t = Table.fromRows('t', [
          {'a': 9},
          {'a': 'x'},
        ]);
        expect(t.column('a')!.toList(), ['9', 'x']);
        expect(t.column('a')!.toList().every((v) => v is String), isTrue);
        expect(t.schema.field('a')!.dataType, DataType.string);
      });
    });

    // A decimal is stored unscaled with its scale on the Field, so a column
    // value alone is a meaningless integer (1.50 reads as 150). Table.row is the
    // boundary that owns the schema, so it is where a decimal becomes readable.
    group('Decimal', () {
      Table decimals(List<int> unscaled, int scale) => Table(
        name: 't',
        columns: [
          ListColumn<BigInt>(
            name: 'v',
            dataType: DataType.decimal,
            data: [for (final u in unscaled) BigInt.from(u)],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(
              name: 'v',
              dataType: DataType.decimal,
              precision: 10,
              scale: scale,
              nullable: false,
            ),
          ],
        ),
      );

      test('renders its value, not the unscaled integer', () {
        expect(Decimal(BigInt.from(150), 2).toString(), '1.50');
        expect(Decimal(BigInt.from(-99), 2).toString(), '-0.99');
        expect(Decimal(BigInt.from(7), 0).toString(), '7');
        expect(Decimal(BigInt.from(5), 4).toString(), '0.0005');
      });

      test('compares and equals BY VALUE across scales', () {
        // 1.5 and 1.50 are the same number; Java's BigDecimal.equals famously
        // disagrees, which is a long-standing source of bugs.
        expect(Decimal(BigInt.from(15), 1), Decimal(BigInt.from(150), 2));
        expect(
          Decimal(BigInt.from(15), 1).hashCode,
          Decimal(BigInt.from(150), 2).hashCode,
        );
        expect(
          Decimal(BigInt.from(150), 2).compareTo(Decimal(BigInt.from(151), 2)),
          lessThan(0),
        );
        expect(
          Decimal(BigInt.from(2), 0).compareTo(Decimal(BigInt.from(150), 2)),
          greaterThan(0),
        );
        expect(Decimal(BigInt.zero, 4), Decimal(BigInt.zero, 0));
      });

      test('from(num) takes the shortest round-trip scale', () {
        expect(Decimal.from(1.50), Decimal(BigInt.from(15), 1));
        expect(Decimal.from(1.005), Decimal(BigInt.from(1005), 3));
        expect(Decimal.from(7), Decimal(BigInt.from(7), 0));
      });

      test('toDouble is exact for representable values', () {
        expect(Decimal(BigInt.from(150), 2).toDouble(), 1.5);
        expect(Decimal(BigInt.from(6), 0).toDouble(), 6.0);
      });

      test('stays exact past 2^53, where a double would round', () {
        final big = Decimal(BigInt.parse('9007199254740993'), 0);
        expect(big.toString(), '9007199254740993');
        expect(big.unscaled, BigInt.parse('9007199254740993'));
      });

      test('Table.row renders a decimal column', () {
        expect(decimals([150, 250], 2).toRows().map((r) => r['v'].toString()), [
          '1.50',
          '2.50',
        ]);
      });

      test('Column.toList stays raw — a Column carries no scale', () {
        // The scale lives on the Field, so only Table can render one. This is
        // the internal representation the kernels operate on.
        expect(decimals([150], 2).column('v')!.toList(), [BigInt.from(150)]);
      });

      test('rows round-trip back through Table.fromRows', () {
        final t = decimals([150, 250], 2);
        final back = Table.fromRows('t', t.toRows(), schema: t.schema);
        expect(back.column('v')!.toList(), [
          BigInt.from(150),
          BigInt.from(250),
        ]);
      });

      test('a Decimal of a different scale re-scales into the column', () {
        final schema = Schema(
          fields: [
            Field(
              name: 'v',
              dataType: DataType.decimal,
              precision: 10,
              scale: 2,
              nullable: true,
            ),
          ],
        );
        // 1.5 (scale 1) into a scale-2 column is exact padding -> 150.
        final t = Table.fromRows('t', [
          {'v': Decimal(BigInt.from(15), 1)},
          {'v': Decimal(BigInt.from(150), 2)},
        ], schema: schema);
        expect(t.column('v')!.toList(), [BigInt.from(150), BigInt.from(150)]);
      });

      test('a Decimal needing more scale than the column fails loud', () {
        final schema = Schema(
          fields: [
            Field(
              name: 'v',
              dataType: DataType.decimal,
              precision: 10,
              scale: 2,
              nullable: true,
            ),
          ],
        );
        // 1.005 cannot be stored at scale 2 without dropping a digit.
        expect(
          () => Table.fromRows('t', [
            {'v': Decimal(BigInt.from(1005), 3)},
          ], schema: schema),
          throwsA(isA<ArgumentError>()),
        );
      });

      test('trailing zeros re-scale down without loss', () {
        final schema = Schema(
          fields: [
            Field(
              name: 'v',
              dataType: DataType.decimal,
              precision: 10,
              scale: 0,
              nullable: true,
            ),
          ],
        );
        // 7.00 IS 7 — normalizing first means this is exact, not a rounding.
        final t = Table.fromRows('t', [
          {'v': Decimal(BigInt.from(700), 2)},
        ], schema: schema);
        expect(t.column('v')!.toList(), [BigInt.from(7)]);
      });
    });

    group('Column.concat width promotion', () {
      test('int32 + int64 promotes to int64 without truncating', () {
        final a = NumericColumn.int32(
          name: 'v',
          values: [1, 2],
          nullable: false,
        );
        final b = NumericColumn.int64(
          name: 'v',
          values: [1 << 40],
          nullable: false,
        );
        final c = Column.concat([a, b]);
        expect(c.dataType, DataType.int64);
        expect(c.toList(), [1, 2, 1 << 40]);
      });
    });

    group('Table.describe / Table.concat (review findings)', () {
      Table ints(String name, List<int> v) => Table(
        name: name,
        columns: [NumericColumn.int64(name: 'n', values: v, nullable: false)],
        schema: Schema(
          fields: [Field(name: 'n', dataType: DataType.int64, nullable: false)],
        ),
      );

      test('describe honors previewRows (finding 6)', () {
        final t = ints('t', List.generate(10, (i) => i));
        final three = t.describe(3);
        expect(three, contains('Preview (first 3 rows)'));
        expect(three, contains('... 7 more rows'));
        // toString stays at the default 5.
        expect(t.toString(), contains('Preview (first 5 rows)'));
      });

      test('concat equals repeated append but N-way (finding 10)', () {
        final parts = [
          ints('t', [1, 2]),
          ints('t', [3]),
          ints('t', [4, 5, 6]),
        ];
        final viaConcat = Table.concat(parts);
        var viaAppend = parts.first;
        for (final p in parts.skip(1)) {
          viaAppend = viaAppend.append(p);
        }
        expect(viaConcat.column('n')!.toList(), [1, 2, 3, 4, 5, 6]);
        expect(
          viaConcat.column('n')!.toList(),
          viaAppend.column('n')!.toList(),
        );
      });

      test('concat of one table is a no-op; empty is empty', () {
        expect(
          Table.concat([
            ints('t', [9]),
          ]).column('n')!.toList(),
          [9],
        );
        expect(Table.concat(const []).rowCount, 0);
      });
    });

    group('decimal scale safety', () {
      Table dec(int scale) => Table(
        name: 't',
        columns: [
          ListColumn<BigInt>(
            name: 'amt',
            dataType: DataType.decimal,
            data: [BigInt.from(100)],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(
              name: 'amt',
              dataType: DataType.decimal,
              nullable: false,
              precision: 10,
              scale: scale,
            ),
          ],
        ),
      );

      test('appending decimals of different scale is rejected', () {
        expect(() => dec(2).append(dec(4)), throwsArgumentError);
      });

      test('appending decimals of the same scale is allowed', () {
        expect(dec(2).append(dec(2)).rowCount, 2);
      });
    });

    group('bigIntToBytes overflow guard', () {
      test('a value too large for the byte width throws', () {
        expect(
          () => bigIntToBytes(BigInt.from(300), 1, littleEndian: false),
          throwsRangeError,
        );
      });

      test('a value that fits round-trips', () {
        final bytes = bigIntToBytes(BigInt.from(-5), 4, littleEndian: false);
        expect(bytesToBigInt(bytes, littleEndian: false), BigInt.from(-5));
      });
    });

    group('memoryUsage accounts dynamic string columns', () {
      test('a string column reports more than 8 bytes/row', () {
        final col = ListColumn(
          name: 's',
          dataType: DataType.string,
          data: ['hello world', 'another value'],
          nullable: false,
        );
        expect(col.memoryUsage, greaterThan(2 * 8));
      });
    });
  });

  group('decimal schema mapping', () {
    final schema = Schema(
      fields: [
        Field(name: 'id', dataType: DataType.int64, nullable: false),
        Field(
          name: 'amt',
          dataType: DataType.decimal,
          nullable: true,
          precision: 12,
          scale: 4,
        ),
      ],
    );

    test('Delta schemaString emits + parses decimal(P,S)', () {
      final json = DeltaSchema.toJson(schema);
      expect(json, contains('"decimal(12,4)"'));
      final back = DeltaSchema.fromJson(json);
      final f = back.fields[1];
      expect(f.dataType, DataType.decimal);
      expect(f.precision, 12);
      expect(f.scale, 4);
    });

    test('Iceberg schema emits + parses decimal(P,S)', () {
      final json = IcebergSchema.toJson(schema);
      expect(json['fields'][1]['type'], 'decimal(12,4)');
      final back = IcebergSchema.fromJson(json);
      final f = back.fields[1];
      expect(f.dataType, DataType.decimal);
      expect(f.precision, 12);
      expect(f.scale, 4);
    });
  });

  group('secure identifiers', () {
    Table t() => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: [1], nullable: false),
      ],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    String part(Directory dir) => dir
        .listSync()
        .whereType<File>()
        .map((f) => f.uri.pathSegments.last)
        .firstWhere((n) => n.endsWith('.parquet'));

    test('two version-0 Delta writes get distinct part-file UUIDs', () async {
      final root = Directory.systemTemp.createTempSync('secid_');
      try {
        final a = Directory('${root.path}/a')..createSync();
        final b = Directory('${root.path}/b')..createSync();
        // Back-to-back writes; both are version 0, so the only differing part of
        // the filename is the random UUID segment.
        await DeltaWriter(a.path).write(t());
        await DeltaWriter(b.path).write(t());
        expect(part(a), isNot(equals(part(b))));
      } finally {
        root.deleteSync(recursive: true);
      }
    });
  });

  group('atomic replace', () {
    test('replaceAtomic overwrites an existing destination', () async {
      final dir = Directory.systemTemp.createTempSync('atomic_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final dest = '${dir.path}/data';
      await File(dest).writeAsString('old');
      final tmp = File('${dir.path}/data.tmp');
      await tmp.writeAsString('new');

      await replaceAtomic(tmp, dest);

      expect(await File(dest).readAsString(), 'new');
      expect(await tmp.exists(), isFalse); // temp consumed by the rename
    });

    test('replaceAtomic works when the destination does not exist', () async {
      final dir = Directory.systemTemp.createTempSync('atomic2_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final dest = '${dir.path}/fresh';
      final tmp = File('${dir.path}/fresh.tmp')..writeAsStringSync('x');
      await replaceAtomic(tmp, dest);
      expect(await File(dest).readAsString(), 'x');
    });

    test('recoverBackup restores a backup when dest is missing', () async {
      // Simulate a crash mid Windows backup-swap: dest is gone, the only intact
      // copy is at <dest>.bak-<token>.
      final dir = Directory.systemTemp.createTempSync('recover_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final dest = '${dir.path}/registry.json';
      await File('$dest.bak-abc123').writeAsString('{"t":{"version":3}}');

      await recoverBackup(dest);

      expect(await File(dest).readAsString(), '{"t":{"version":3}}');
      expect(await File('$dest.bak-abc123').exists(), isFalse);
    });

    test('recoverBackup deletes an orphan backup when dest exists', () async {
      // A crash AFTER the install but before the cleanup leaves a stale backup
      // beside a good dest.
      final dir = Directory.systemTemp.createTempSync('recover2_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final dest = '${dir.path}/registry.json';
      await File(dest).writeAsString('current');
      await File('$dest.bak-stale').writeAsString('old');

      await recoverBackup(dest);

      expect(await File(dest).readAsString(), 'current'); // untouched
      expect(await File('$dest.bak-stale').exists(), isFalse); // orphan removed
    });

    test('recoverBackup is a no-op when there is no backup', () async {
      final dir = Directory.systemTemp.createTempSync('recover3_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final dest = '${dir.path}/registry.json';
      await File(dest).writeAsString('data');
      await recoverBackup(dest);
      expect(await File(dest).readAsString(), 'data');
    });
  });

  group('iceberg single-value bound encoding', () {
    test('encodes primitives little-endian per the Iceberg spec', () {
      // int/long little-endian
      expect(icebergBound(258, DataType.int32)!.toList(), [2, 1, 0, 0]);
      expect(icebergBound(1, DataType.int64)!.toList(), [
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
      ]);
      // boolean is a single byte
      expect(icebergBound(true, DataType.boolean)!.toList(), [1]);
      expect(icebergBound(false, DataType.boolean)!.toList(), [0]);
      // string is UTF-8
      expect(icebergBound('AB', DataType.string)!.toList(), [65, 66]);
      // double 1.0 = 0x3FF0000000000000, little-endian
      expect(icebergBound(1.0, DataType.float64)!.toList(), [
        0,
        0,
        0,
        0,
        0,
        0,
        240,
        63,
      ]);
    });

    test('declines to encode types it cannot bound safely', () {
      // A wrong bound makes external engines skip valid data, so decimal /
      // temporal / binary bounds are omitted (null) rather than guessed.
      expect(icebergBound(BigInt.from(5), DataType.decimal), isNull);
      expect(icebergBound(0, DataType.date), isNull);
      expect(icebergBound(0, DataType.timestamp), isNull);
    });
  });

  group('data-file path resolution', () {
    // One resolver for the Delta reader AND writer: a URI-scheme path must be
    // treated as absolute on BOTH sides (the writer previously joined it under
    // the root, reading/deleting the wrong file).
    test('a URI-scheme path is absolute; a relative path joins the root', () {
      expect(
        resolveDataFile(
          '/root',
          's3://bucket/data.parquet',
          allowExternal: true,
        ),
        's3://bucket/data.parquet',
      );
      expect(
        resolveDataFile(
          '/root',
          'file:///abs/data.parquet',
          allowExternal: true,
        ),
        'file:///abs/data.parquet',
      );
      expect(
        resolveDataFile('/root', '/abs/data.parquet', allowExternal: true),
        '/abs/data.parquet',
      );
      expect(
        resolveDataFile('/root', 'part-0.parquet', allowExternal: true),
        '/root/part-0.parquet',
      );
    });

    test(
      'a URI-scheme path outside the root is rejected without allowExternal',
      () {
        expect(
          () => resolveDataFile('/root', 's3://bucket/data.parquet'),
          throwsA(isA<ArgumentError>()),
        );
      },
    );
  });

  group('path handling', () {
    group('sandbox traversal guard (#2)', () {
      test('platform-aware split detects ".." in a Windows path', () {
        final win = p.windows.normalize('..\\..\\secret.parquet');
        // The fix: p.split sees the backslash segments.
        expect(p.windows.split(win).contains('..'), isTrue);
        // The old bug: splitting on a literal '/' missed it entirely.
        expect(win.split('/').contains('..'), isFalse);
      });

      test('still detects ".." in a POSIX path (no regression)', () {
        expect(p.posix.split('../secret.parquet').contains('..'), isTrue);
      });
    });

    group('table-name derivation (#3)', () {
      test('basenameWithoutExtension yields the name on Windows', () {
        expect(
          p.windows.basenameWithoutExtension('C:\\data\\iris.parquet'),
          'iris',
        );
        // The old bug: split('/').last returned the whole path on Windows.
        expect('C:\\data\\iris.parquet'.split('/').last, isNot('iris'));
      });

      test('still yields the name on POSIX (no regression)', () {
        expect(p.posix.basenameWithoutExtension('data/iris.parquet'), 'iris');
        // Multi-dot: only the final extension is stripped (matches prior behavior).
        expect(p.posix.basenameWithoutExtension('a/foo.bar.csv'), 'foo.bar');
      });
    });
  });

  group('writer type fidelity', () {
    Table t() => Table(
      name: 't',
      columns: [
        ListColumn<DateTime>(
          name: 'ts',
          dataType: DataType.timestamp,
          data: [DateTime.utc(2021, 3, 15, 10, 30, 15)],
          nullable: false,
        ),
        ListColumn<BigInt>(
          name: 'dec',
          dataType: DataType.decimal,
          data: [BigInt.from(12345)],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'ts', dataType: DataType.timestamp, nullable: false),
          Field(
            name: 'dec',
            dataType: DataType.decimal,
            nullable: false,
            precision: 10,
            scale: 2,
          ),
        ],
      ),
    );

    test('formatDecimal scales unscaled BigInt', () {
      expect(formatDecimal(BigInt.from(12345), 2), '123.45');
      expect(formatDecimal(BigInt.from(-99), 2), '-0.99');
      expect(formatDecimal(BigInt.zero, 2), '0.00');
      expect(formatDecimal(BigInt.from(7), 0), '7');
      expect(formatDecimal(BigInt.from(5), 4), '0.0005');
    });

    test('JSON writer emits ISO date + scaled decimal (no DateTime crash)', () {
      final json = t().toJsonLines();
      expect(json, contains('"dec":"123.45"'));
      expect(json, contains('2021-03-15T10:30:15.000Z'));
    });

    test('CSV writer emits scaled decimal', () async {
      final dir = Directory.systemTemp.createTempSync('csvfid_');
      try {
        final path = '${dir.path}/o.csv';
        await CsvWriter().writeTable(t(), path);
        final csv = File(path).readAsStringSync();
        expect(csv, contains('123.45'));
        expect(csv, isNot(contains('12345')));
      } finally {
        dir.deleteSync(recursive: true);
      }
    });
  });

  // ---- Relocated: audit_phasea (D-T1 nanosecond timestamp conversion) ----
  group('D-T1 nanosecond timestamp conversion', () {
    test('timestampFromEpochNanos truncates to microseconds', () {
      // 1_500_000_000_123_456_789 ns -> 1_500_000_000_123_456 us.
      final dt = timestampFromEpochNanos(1500000000123456789);
      expect(dt.microsecondsSinceEpoch, 1500000000123456);
      expect(dt.isUtc, isTrue);
    });
  });

  // ---- Relocated: audit_phasece (B3-2 RecordBatch.select fails loud) ----
  group('B3-2 RecordBatch.select fails loud on an unknown column', () {
    RecordBatch batch() => RecordBatch(
      columns: [
        NumericColumn.int64(name: 'a', values: [1, 2], nullable: false),
        NumericColumn.int64(name: 'b', values: [3, 4], nullable: false),
      ],
      schema: Schema(
        fields: [
          Field(name: 'a', dataType: DataType.int64, nullable: false),
          Field(name: 'b', dataType: DataType.int64, nullable: false),
        ],
      ),
      rowCount: 2,
    );

    test('selecting an existing column works', () {
      final r = batch().select(['b']);
      expect(r.schema.fields.map((f) => f.name), ['b']);
    });

    test('selecting a missing column throws (was silently skipped)', () {
      expect(() => batch().select(['a', 'missing']), throwsArgumentError);
    });
  });

  // ---- Relocated: audit_round47 (#2 sandbox fails closed) ----
  group('#2 sandbox fails closed', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('r47paths_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('canonicalize throws on an unresolvable (broken) symlink', () {
      final link = '${dir.path}/broken';
      Link(link).createSync('${dir.path}/does_not_exist');
      // Pre-fix this returned the lexical path (fail-open); it must now deny.
      expect(() => canonicalize(link), throwsA(isA<FileSystemException>()));
    });

    test('confine denies a symlink escaping the root', () {
      final outside = Directory.systemTemp.createTempSync('r47out_');
      File('${outside.path}/f').writeAsStringSync('x');
      Link('${dir.path}/link').createSync('${outside.path}/f');
      expect(
        () => confine(dir.path, '${dir.path}/link'),
        throwsA(isA<ArgumentError>()),
      );
      outside.deleteSync(recursive: true);
    });
  });

  // ---- Relocated: audit_round47 (#6 core type invariants) ----
  group('#6 core type invariants', () {
    test('a concrete column under a null_ field is rejected', () {
      expect(
        () => Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'x', values: [1, 2], nullable: false),
          ],
          schema: Schema(
            fields: [Field(name: 'x', dataType: DataType.null_)],
          ),
        ),
        throwsArgumentError,
      );
    });

    test('Column.concat rejects mismatched names', () {
      final a = NumericColumn.int64(name: 'a', values: [1], nullable: false);
      final b = NumericColumn.int64(name: 'b', values: [2], nullable: false);
      expect(() => Column.concat([a, b]), throwsArgumentError);
    });

    test('Column.concat rejects incompatible types', () {
      final ints = NumericColumn.int64(name: 'x', values: [1], nullable: false);
      final strs = ListColumn<String>(
        name: 'x',
        dataType: DataType.string,
        data: ['a'],
        nullable: false,
      );
      expect(() => Column.concat([ints, strs]), throwsArgumentError);
    });

    test('Column.concat of same-type columns still works', () {
      final a = ListColumn<String>(
        name: 'x',
        dataType: DataType.string,
        data: ['a', 'b'],
        nullable: false,
      );
      final b = ListColumn<String>(
        name: 'x',
        dataType: DataType.string,
        data: ['c'],
        nullable: false,
      );
      expect(Column.concat([a, b]).toList(), ['a', 'b', 'c']);
    });
  });

  // ---- Relocated: audit_round47 (#10 unknown encoding fails loud) ----
  group('#10 unknown encoding fails loud', () {
    test('resolveEncoding throws on an unknown name', () {
      expect(() => resolveEncoding('not-a-real-encoding'), throwsArgumentError);
    });
    test('resolveEncoding maps null / known names', () {
      expect(resolveEncoding(null).name, 'utf-8');
      expect(resolveEncoding('utf-8').name, 'utf-8');
      expect(resolveEncoding('latin1').name, 'iso-8859-1');
    });
  });

  // ---- Relocated: audit_round48 (S1/C9 content keys) ----
  group('S1/C9 content keys (binary + map)', () {
    test('canonicalKey routes content-equal binary keys identically', () {
      final a = canonicalKey(Uint8List.fromList([1, 2, 3]));
      final b = canonicalKey(Uint8List.fromList([1, 2, 3]));
      // Distinct objects, but must hash/compare by CONTENT so the grace-join
      // router sends them to the same partition (was identity → dropped match).
      expect(a, equals(b));
      expect(a.hashCode, equals(b.hashCode));
    });

    test('RowKey compares maps by value (struct/map keys)', () {
      final k1 = RowKey([
        {'a': 1, 'b': 2},
      ]);
      final k2 = RowKey([
        {'b': 2, 'a': 1}, // same content, different insertion order
      ]);
      expect(k1, equals(k2));
      expect(k1.hashCode, equals(k2.hashCode));
      final set = {k1};
      expect(set.contains(k2), isTrue);
    });

    test('canonicalKey wraps maps for value semantics', () {
      final a = canonicalKey({'x': 1});
      final b = canonicalKey({'x': 1});
      expect(a, equals(b));
      expect(a.hashCode, equals(b.hashCode));
    });

    test('unequal maps stay distinct', () {
      expect(
        RowKey([
          {'a': 1},
        ]),
        isNot(
          equals(
            RowKey([
              {'a': 2},
            ]),
          ),
        ),
      );
    });
  });

  // ---- Relocated: audit_round48 (C1/C2/C3 DataChunk parity with Table) ----
  group('C1/C2/C3 DataChunk parity with Table', () {
    test('C1 fromRows coerces integer literals for a float field', () {
      final schema = Schema(
        fields: [Field(name: 'v', dataType: DataType.float64, nullable: true)],
      );
      // Integer literal 10 then double 10.5 — cast<double?> would throw.
      final chunk = DataChunk.fromRows([
        {'v': 10},
        {'v': 10.5},
      ], schema);
      expect(chunk.column('v')!.toList(), [10.0, 10.5]);
    });

    Schema schemaOf(DataType t) => Schema(
      fields: [Field(name: 'n', dataType: t, nullable: false)],
    );
    DataChunk chunkOf(DataType t, List<int> v) =>
        DataChunk(columns: [_num(t, v)], schema: schemaOf(t));

    test('C2 append promotes width and reconciles the schema', () {
      final a = chunkOf(DataType.int32, [1, 2]);
      final b = chunkOf(DataType.int64, [3, 4]);
      final joined = a.append(b); // was: throws int64 != int32
      expect(joined.column('n')!.toList(), [1, 2, 3, 4]);
      expect(joined.schema.fields.first.dataType, DataType.int64);
    });

    test('Table.fromChunks concatenates all chunks once', () {
      final table = Table.fromChunks('t', [
        chunkOf(DataType.int64, [1, 2]),
        chunkOf(DataType.int64, [3]),
        chunkOf(DataType.int64, [4, 5, 6]),
      ]);
      expect(table.rowCount, 6);
      expect(table.column('n')!.toList(), [1, 2, 3, 4, 5, 6]);
    });

    test('Table.fromChunks with a single chunk round-trips', () {
      final table = Table.fromChunks('t', [
        chunkOf(DataType.int64, [7, 8]),
      ]);
      expect(table.column('n')!.toList(), [7, 8]);
    });

    test('C3 validate rejects a concrete column under a null_ field', () {
      expect(
        () => DataChunk(
          columns: [
            _num(DataType.int64, [1]),
          ],
          schema: Schema(
            fields: [Field(name: 'n', dataType: DataType.null_)],
          ),
        ),
        throwsArgumentError,
      );
    });
  });

  // ---- Relocated: audit_round48 (C7 RecordBatch.select ambiguity) ----
  group('C7 RecordBatch.select ambiguity', () {
    test('selecting an ambiguous bare name throws', () {
      final batch = RecordBatch(
        columns: [
          NumericColumn.int64(name: 'id', values: [1], nullable: false),
          NumericColumn.int64(name: 'id', values: [2], nullable: false),
        ],
        schema: Schema(
          fields: [
            Field(name: 'id', dataType: DataType.int64, qualifier: 'a'),
            Field(name: 'id', dataType: DataType.int64, qualifier: 'b'),
          ],
        ),
        rowCount: 1,
      );
      expect(() => batch.select(['id']), throwsA(isA<StateError>()));
    });
  });

  // ---- Relocated: audit_round48 (C4/C5 ListColumn memory + stats parity) ----
  group('C4/C5 ListColumn memory + stats parity', () {
    test('C4 timestamp memoryUsage counts DateTime payload, not fixedSize', () {
      final col = ListColumn<DateTime>(
        name: 't',
        dataType: DataType.timestamp,
        data: [for (var i = 0; i < 100; i++) DateTime.utc(2020, 1, 1 + i % 27)],
        nullable: false,
      );
      // Old code: 100 * fixedSize(8) = 800. Now ≥ one ref (8) + heap payload
      // per row, so materially larger — a boxed column can't slip the budget.
      expect(col.memoryUsage, greaterThan(100 * 8));
    });

    test('C5 ListColumn.stats caps distinctCount', () {
      final col = ListColumn<int>(
        name: 'v',
        dataType: DataType.int64, // numeric, but boxed as a ListColumn
        data: [for (var i = 0; i < 70000; i++) i],
        nullable: false,
      );
      final s = col.stats!;
      expect(s.distinctCount, lessThanOrEqualTo(1 << 16)); // was unbounded
    });

    test('C5 ListColumn.stats variance is stable for large magnitudes', () {
      final col = ListColumn<double>(
        name: 'v',
        dataType: DataType.float64,
        data: [1000000000.0, 1000000001.0, 1000000002.0],
        nullable: false,
      );
      // population stdDev of {0,1,2} = sqrt(2/3); sumSquares would cancel here.
      expect(col.stats!.stdDev, closeTo(0.8164965809, 1e-6));
    });
  });

  // ---- Relocated: audit_round49 (#2 explicit-schema strictness, non-Delta) --
  group('#2 explicit-schema strictness', () {
    Schema idName() => Schema(
      fields: [
        Field(name: 'id', dataType: DataType.int64, nullable: false),
        Field(name: 'name', dataType: DataType.string, nullable: true),
      ],
    );

    test('Table.fromRows with an explicit schema rejects an unknown key', () {
      expect(
        () => Table.fromRows('t', [
          {'id': 1, 'nmae': 'typo'},
        ], schema: idName()),
        throwsArgumentError,
      );
    });

    test('inferred Table.fromRows is unaffected (all keys become fields)', () {
      final t = Table.fromRows('t', [
        {'id': 1, 'extra': 9},
      ]);
      expect(t.schema.fields.map((f) => f.name).toSet(), {'id', 'extra'});
    });
  });

  // ---- Relocated: audit_round50 (#1 fixed-width integer factories) ----
  group('#1 fixed-width integer factories range-check', () {
    test('int32 rejects an out-of-range value (was a silent wrap)', () {
      expect(
        () =>
            NumericColumn.int32(name: 'n', values: [1 << 40], nullable: false),
        throwsRangeError,
      );
    });

    test('int8 rejects 300 (was stored as 44)', () {
      expect(
        () => NumericColumn.int8(name: 'n', values: [300], nullable: false),
        throwsRangeError,
      );
    });

    test('int16 rejects 40000', () {
      expect(
        () => NumericColumn.int16(name: 'n', values: [40000], nullable: false),
        throwsRangeError,
      );
    });

    test('in-range values still build', () {
      final c = NumericColumn.int32(
        name: 'n',
        values: [-2147483648, 0, 2147483647],
        nullable: false,
      );
      expect(c.toList(), [-2147483648, 0, 2147483647]);
    });
  });

  // ---- Relocated: audit_round50 (#4 mixed int/float inference -> decimal) ----
  group('#4 mixed int/float inference is lossless (decimal, not float64)', () {
    test('a large int mixed with a float infers exact decimal', () {
      const big = 9007199254740993; // 2^53 + 1, not exactly a float64
      final schema = Schema.infer([
        {'v': big},
        {'v': 0.5},
      ], sampleSize: 2);
      final field = schema.fields.single;
      expect(field.dataType, DataType.decimal);
      expect(field.scale, 1);

      final t = Table.fromRows('t', [
        {'v': big},
        {'v': 0.5},
      ], schema: schema);
      // Unscaled BigInt at scale 1: big*10 and 5 — the integer is preserved
      // exactly (float64 would have rounded 2^53+1 to 2^53).
      expect(t.column('v')!.toList(), [
        BigInt.from(big) * BigInt.from(10),
        BigInt.from(5),
      ]);
    });

    test('pure-int and pure-float columns are unaffected', () {
      expect(
        Schema.infer([
          {'a': 1},
          {'a': 2},
        ]).fields.single.dataType,
        DataType.int8,
      );
      expect(
        Schema.infer([
          {'b': 1.5},
          {'b': 2.5},
        ]).fields.single.dataType,
        DataType.float64,
      );
    });
  });

  // ---- Relocated: audit_round51 (#R3-R7 decimal inference correctness) ----
  group('#R3-R7 decimal inference correctness', () {
    test('values below 1 infer a valid decimal(precision>=scale)', () {
      final f = Schema.infer([
        {'x': 0},
        {'x': 0.05},
      ]).fields.single;
      expect(f.dataType, DataType.decimal);
      expect(f.scale, 2);
      expect(f.precision! >= f.scale!, isTrue); // was decimal(1,2) — invalid
    });

    test('an int mixed with a non-finite double stays float64 (no throw)', () {
      final f = Schema.infer([
        {'x': 1},
        {'x': double.infinity},
      ]).fields.single;
      expect(f.dataType, DataType.float64);
    });

    test('a decimal column mixing BigInt with raw num builds (no crash)', () {
      final schema = Schema(
        fields: [
          Field(name: 'x', dataType: DataType.decimal, precision: 10, scale: 1),
        ],
      );
      final t = Table.fromRows('t', [
        {'x': BigInt.from(15)}, // already unscaled at scale 1 (=1.5)
        {'x': 2}, // raw int → 20 unscaled
      ], schema: schema);
      expect(t.column('x')!.toList(), [BigInt.from(15), BigInt.from(20)]);
    });

    test('a large int mixed with a float still infers exact decimal', () {
      const big = 9007199254740993;
      final t = Table.fromRows(
        't',
        [
          {'v': big},
          {'v': 0.5},
        ],
        schema: Schema.infer([
          {'v': big},
          {'v': 0.5},
        ]),
      );
      expect(t.column('v')!.toList(), [
        BigInt.from(big) * BigInt.from(10),
        BigInt.from(5),
      ]);
    });
  });

  // ---- Relocated: review_defects (#2 null in a non-null column) ----
  group('#2 null in a non-null column', () {
    test('numeric factory rejects a null when non-nullable', () {
      expect(
        () => NumericColumn.int64(
          name: 'v',
          values: [1, null, 3],
          nullable: false,
        ),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('ListColumn rejects a null when non-nullable', () {
      expect(
        () => ListColumn<String>(
          name: 'v',
          dataType: DataType.string,
          data: ['a', null],
          nullable: false,
        ),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('Table rejects a null column under a non-nullable field', () {
      expect(
        () => Table(
          name: 't',
          schema: Schema(
            fields: [
              Field(name: 'v', dataType: DataType.int64, nullable: false),
            ],
          ),
          columns: [
            NumericColumn.int64(name: 'v', values: [1, null], nullable: true),
          ],
        ),
        throwsA(isA<ArgumentError>()),
      );
    });
  });

  // ---- Relocated: review_defects (#3 int16 is rebuilt as int16) ----
  group('#3 int16 is rebuilt as int16 (not int32)', () {
    DataType typeOf(Table r, String col) =>
        r.schema.fields.firstWhere((f) => f.name == col).dataType;

    test('Kernels.fromValues keeps int16', () {
      final c = Kernels.fromValues('v', DataType.int16, [1, 2, 3]);
      expect(c.dataType, DataType.int16);
    });

    test('GROUP BY on an int16 column returns an int16 key', () async {
      final engine = SqlEngine();
      engine.registerTable(
        't',
        Table(
          name: 't',
          schema: Schema(
            fields: [
              Field(name: 'v', dataType: DataType.int16, nullable: false),
            ],
          ),
          columns: [
            NumericColumn.int16(
              name: 'v',
              values: [1, 2, 1, 2],
              nullable: false,
            ),
          ],
        ),
      );
      final r = await engine.execute(
        'SELECT v, COUNT(*) AS c FROM t GROUP BY v',
      );
      expect(typeOf(r, 'v'), DataType.int16);
      expect(r.rowCount, 2);
    });
  });

  // ---- Relocated: review_defects (#5 NumericColumn.adopt validates buffer) --
  group('#5 NumericColumn.adopt validates its buffer', () {
    test('a buffer whose type mismatches the dataType throws', () {
      expect(
        () => NumericColumn.adopt(
          name: 'v',
          dataType: DataType.int8,
          data: Int64List(1),
        ),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('a wrong-length null bitmap throws', () {
      expect(
        () => NumericColumn.adopt(
          name: 'v',
          dataType: DataType.int64,
          data: Int64List(2),
          nullBitmap: Uint8List(5),
        ),
        throwsA(isA<ArgumentError>()),
      );
    });
  });

  // ---- Relocated: review_defects (#6 nested memory accounting) ----
  test('#6 nested memory accounting includes the deep payload', () {
    const n = 4 * 1024 * 1024;
    final column = ListColumn<List<dynamic>>(
      name: 'x',
      dataType: DataType.list,
      data: [List<int>.filled(n, 0)],
    );
    expect(column.memoryUsage, greaterThanOrEqualTo(n));
  });

  // ---- Relocated: review_findings (finding 1: payload-aware memoryUsage) ----
  group('finding 1: payload-aware memoryUsage', () {
    test('a single fat binary value is not counted as 8 bytes', () {
      final payload = List<int>.filled(4 * 1024 * 1024, 0); // 4 MiB
      final column = ListColumn<List<int>>(
        name: 'blob',
        dataType: DataType.binary,
        data: [payload],
      );
      // Before the fix this reported ~8 bytes for the one row.
      expect(column.memoryUsage, greaterThanOrEqualTo(payload.length));
    });

    test('a nested list value contributes more than 8 bytes', () {
      final column = ListColumn<List<dynamic>>(
        name: 'items',
        dataType: DataType.list,
        data: [List<int>.filled(1000, 1)],
      );
      expect(column.memoryUsage, greaterThan(1000));
    });
  });

  // ---- Relocated: review_findings (finding 5: Table.toChunks(0) throws) ----
  group('finding 5: invalid sizes throw instead of hanging', () {
    Table sample() => Table(
      name: 't',
      schema: Schema(
        fields: [Field(name: 'v', dataType: DataType.int64)],
      ),
      columns: [
        ListColumn<int>(name: 'v', dataType: DataType.int64, data: [1, 2, 3]),
      ],
    );

    test('Table.toChunks(0) throws', () {
      expect(() => sample().toChunks(0), throwsA(isA<ArgumentError>()));
    });
  });

  // ---- Relocated: review_round44 (#9 core schema/field immutability) ----
  group('#9 core schema/field immutability', () {
    test('Schema.fields is unmodifiable', () {
      final s = Schema(
        fields: [Field(name: 'a', dataType: DataType.int64)],
      );
      expect(
        () => s.fields.add(Field(name: 'b', dataType: DataType.int64)),
        throwsUnsupportedError,
      );
    });

    test('a passed field list is copied (later mutation does not leak in)', () {
      final fields = [Field(name: 'a', dataType: DataType.int64)];
      final s = Schema(fields: fields);
      fields.add(Field(name: 'b', dataType: DataType.int64));
      expect(s.fields.length, 1); // the schema kept its own copy
    });

    test('Field.children is unmodifiable', () {
      final f = Field(
        name: 'l',
        dataType: DataType.list,
        children: [Field(name: 'item', dataType: DataType.int64)],
      );
      expect(
        () => f.children.add(Field(name: 'x', dataType: DataType.int64)),
        throwsUnsupportedError,
      );
    });
  });

  // ---- Relocated: review_round45 (#1 binary/composite value equality) ----
  group('#1 binary/composite value equality', () {
    test('RowKey compares binary parts by content, not identity', () {
      final a = RowKey([
        Uint8List.fromList([1, 2, 3]),
      ]);
      final b = RowKey([
        Uint8List.fromList([1, 2, 3]),
      ]); // distinct object
      expect(a, equals(b));
      expect(a.hashCode, equals(b.hashCode));
      final set = {a};
      expect(set.contains(b), isTrue);
    });

    test('canonicalKey lets equal binary values collide in a Map', () {
      final m = <Object?, int>{};
      m[canonicalKey(Uint8List.fromList([9, 9]))] = 1;
      expect(m[canonicalKey(Uint8List.fromList([9, 9]))], 1); // distinct object
    });
  });

  // ---- Relocated: review_round46 (#9 DataChunk immutability) ----
  group('#9 DataChunk immutability', () {
    test('columns list is unmodifiable', () {
      final chunk = DataChunk(
        columns: [
          NumericColumn.int64(name: 'a', values: [1, 2], nullable: false),
        ],
        schema: Schema(
          fields: [Field(name: 'a', dataType: DataType.int64, nullable: false)],
        ),
      );
      expect(
        () => chunk.columns.add(
          NumericColumn.int64(name: 'b', values: [3, 4], nullable: false),
        ),
        throwsUnsupportedError,
      );
    });
  });

  group('inference + key-splitting (relocated)', () {
    test('Schema.infer keeps fields appearing only beyond the sample', () {
      final rows = [
        for (int i = 0; i < 150; i++)
          {'a': i, if (i >= 120) 'late': i, if (i >= 120) 'lateNull': null},
      ];
      final schema = Schema.infer(rows, nullable: false, sampleSize: 100);
      final names = schema.fields.map((f) => f.name).toSet();
      expect(names, containsAll(['a', 'late', 'lateNull']));
      expect(schema.fields.firstWhere((f) => f.name == 'a').nullable, isFalse);
      expect(
        schema.fields.firstWhere((f) => f.name == 'late').nullable,
        isTrue,
      );
    });

    test(
      'Table.splitByKeys distinguishes real null from the string "null"',
      () {
        final t = Table(
          name: 't',
          columns: [
            ListColumn<String>(
              name: 'p',
              dataType: DataType.string,
              data: ['null', null, 'null', null],
              nullable: true,
            ),
            NumericColumn.int64(
              name: 'id',
              values: [1, 2, 3, 4],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'p', dataType: DataType.string, nullable: true),
              Field(name: 'id', dataType: DataType.int64),
            ],
          ),
        );
        final parts = t.splitByKeys(['p']);
        expect(parts.length, 2);
        final byNull = {
          for (final part in parts)
            (part.values['p'] == null): {
              for (final r in part.table.toRows()) r['id'] as int,
            },
        };
        expect(byNull[false], {1, 3});
        expect(byNull[true], {2, 4});
      },
    );
  });

  group('fullJitterBackoff', () {
    test('attempt 0 yields a 1ms delay (window of 1)', () {
      final r = Random(1);
      for (var i = 0; i < 20; i++) {
        expect(fullJitterBackoff(0, r).inMilliseconds, 1);
      }
    });

    test('every delay is within (0, cap]', () {
      final r = Random(2);
      for (var attempt = 0; attempt < 40; attempt++) {
        final ms = fullJitterBackoff(attempt, r, capMs: 256).inMilliseconds;
        expect(ms, greaterThan(0));
        expect(ms, lessThanOrEqualTo(256));
      }
    });

    test('a huge attempt does not overflow or throw (shift is capped)', () {
      final r = Random(3);
      // 1 << 1000000 would overflow; the shift cap keeps the window at cap.
      final ms = fullJitterBackoff(1000000, r, capMs: 256).inMilliseconds;
      expect(ms, inInclusiveRange(1, 256));
    });

    test('the window grows with the attempt up to the cap', () {
      final r = Random(4);
      // attempt 2 -> window min(256, 4) = 4, so delay in [1,4].
      for (var i = 0; i < 50; i++) {
        expect(
          fullJitterBackoff(2, r, capMs: 256).inMilliseconds,
          inInclusiveRange(1, 4),
        );
      }
    });
  });

  group('addColumn preserves decimal metadata', () {
    Table base() => Table.fromRows(
      't',
      [
        {'a': 1},
      ],
      schema: Schema(
        fields: [Field(name: 'a', dataType: DataType.int64, nullable: false)],
      ),
    );
    final decimal = ListColumn<BigInt>(
      name: 'p',
      dataType: DataType.decimal,
      data: [BigInt.from(150)], // 1.50 at scale 2
      nullable: false,
    );

    test('rejects a bare decimal column (scale would be dropped)', () {
      expect(() => base().addColumn(decimal), throwsArgumentError);
    });

    test('keeps the scale when a Field is supplied', () {
      final t = base().addColumn(
        decimal,
        field: Field(
          name: 'p',
          dataType: DataType.decimal,
          nullable: false,
          precision: 10,
          scale: 2,
        ),
      );
      expect(t.schema.field('p')!.scale, 2);
      expect(t.column('p')!.toList(), [BigInt.from(150)]);
    });

    test('a plain scalar column still needs no Field', () {
      final t = base().addColumn(
        NumericColumn.int64(name: 'b', values: [7], nullable: false),
      );
      expect(t.schema.field('b')!.dataType, DataType.int64);
    });
  });

  group('withFileLock', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('lock_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('serializes overlapping async on one isolate', () async {
      // Without the in-isolate mutex both bodies read counter=0, await, then
      // write 1 → a lost update. The mutex forces them to run one at a time → 2.
      final lock = '${dir.path}/a.lock';
      var counter = 0;
      Future<void> increment() => withFileLock(lock, () async {
        final seen = counter;
        await Future<void>.delayed(Duration.zero); // yield mid-critical-section
        counter = seen + 1;
      });
      await Future.wait([increment(), increment()]);
      expect(counter, 2);
    });

    test('a timeout fails loud while a holder runs', () async {
      final lock = '${dir.path}/b.lock';
      final holderEntered = Completer<void>();
      final releaseHolder = Completer<void>();
      // Hold the lock (via the in-isolate mutex) until we let it go.
      final holder = withFileLock(lock, () async {
        holderEntered.complete();
        await releaseHolder.future;
      });
      await holderEntered.future;
      await expectLater(
        withFileLock(
          lock,
          () async => 'ran',
          timeout: const Duration(milliseconds: 50),
        ),
        throwsA(isA<TimeoutException>()),
      );
      releaseHolder.complete();
      await holder;
    });

    test('a null timeout waits and completes (default)', () async {
      final lock = '${dir.path}/c.lock';
      final order = <String>[];
      final first = withFileLock(lock, () async {
        await Future<void>.delayed(const Duration(milliseconds: 20));
        order.add('first');
      });
      final second = withFileLock(lock, () async => order.add('second'));
      await Future.wait([first, second]);
      expect(order, ['first', 'second']);
    });

    test('the lock body still runs and returns its value', () async {
      final lock = '${dir.path}/d.lock';
      expect(await withFileLock(lock, () async => 42), 42);
    });

    test('a timed-out waiter does not release the next waiter early', () async {
      // Regression: a waiter that times out must NOT complete its queue barrier
      // while the real holder still runs — doing so lets the next waiter enter
      // the body concurrently with the holder, breaking mutual exclusion.
      final lock = '${dir.path}/e.lock';
      var active = 0;
      var peak = 0;
      void enter() {
        active++;
        if (active > peak) peak = active;
      }

      final holderIn = Completer<void>();
      final release = Completer<void>();
      final holder = withFileLock(lock, () async {
        enter();
        holderIn.complete();
        await release.future;
        active--;
      });
      await holderIn.future;

      // A waiter that times out while the holder is still inside.
      final timedOut = withFileLock(lock, () async {
        enter();
        active--;
      }, timeout: const Duration(milliseconds: 30));

      // A waiter queued behind it. On the buggy code the timeout above completes
      // its barrier and lets THIS one run while the holder is still active.
      final trailing = withFileLock(lock, () async {
        enter();
        active--;
      });

      await expectLater(timedOut, throwsA(isA<TimeoutException>()));
      // Give the trailing waiter a chance to (wrongly) run alongside the holder.
      await Future<void>.delayed(const Duration(milliseconds: 30));
      expect(active, 1, reason: 'only the holder may be inside the lock');
      expect(peak, 1, reason: 'no waiter ran concurrently with the holder');

      release.complete();
      await holder;
      await trailing; // proceeds only once the holder has released
      expect(peak, 1);
    });
  });

  group('thin-coverage areas (audit 7)', () {
    test('RoaringBitmap promotes array -> bitmap across the 4096 boundary', () {
      // The container flips representation past 4096 entries; an off-by-one
      // there silently drops or invents deleted-row positions.
      final bitmap = RoaringBitmap();
      for (int i = 0; i < 5000; i++) {
        bitmap.add(i * 2); // sparse, one container
      }
      expect(bitmap.length, 5000);
      for (final probe in [0, 4095 * 2, 4096 * 2, 4999 * 2]) {
        expect(bitmap.contains(probe), isTrue, reason: '$probe');
      }
      expect(bitmap.contains(1), isFalse);
      expect(bitmap.contains(4096 * 2 + 1), isFalse);
      // Round-trip through the serialized treemap form at the promoted size.
      final back = Roaring.deserializeToBitmap(
        Roaring.serializeTreemap(bitmap.values),
      );
      expect(back.length, 5000);
      expect(back.contains(4096 * 2), isTrue);
      expect(back.contains(1), isFalse);
    });

    test('MemoryManager hardCap denies an oversized-when-empty reserve', () {
      final soft = MemoryManager(100);
      expect(soft.reserve(500), isTrue); // progress grant
      soft.release(500);
      final hard = MemoryManager(100, hardCap: true);
      expect(hard.reserve(500), isFalse); // strict ceiling
      expect(hard.reserve(60), isTrue);
      expect(hard.reserve(60), isFalse); // pool exhausted -> spill signal
      hard.release(60);
      expect(hard.reserve(60), isTrue);
    });

    test(
      'SqlInvariantEvaluator counts only definitive violations (3VL)',
      () async {
        // NOT(NULL) is NULL and WHERE drops it: an unknown result does not
        // violate a CHECK constraint.
        final t = Table(
          name: 't',
          columns: [
            ListColumn<int>(
              name: 'v',
              dataType: DataType.int64,
              data: [5, -1, null],
              nullable: true,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'v', dataType: DataType.int64, nullable: true),
            ],
          ),
        );
        const invariants = SqlInvariantEvaluator();
        expect(await invariants.violations(t, 'v >= 0'), 1); // only -1
        expect(await invariants.violations(t, 'v >= -10'), 0);
      },
    );
  });

  group('Hive path escaping (audit 7)', () {
    test('special characters round-trip through escape/unescape', () {
      for (final v in ['50%', 'a b', 'a/b', 'a=b', 'ünïcode', 'x%2Fy', '"q"']) {
        final escaped = escapePathName(v);
        expect(unescapePathName(escaped), v, reason: v);
      }
    });

    test('escaping covers path metacharacters, not plain text', () {
      expect(escapePathName('50%'), '50%25');
      expect(escapePathName('a/b'), 'a%2Fb');
      expect(escapePathName('a=b'), 'a%3Db');
      expect(escapePathName('plain'), 'plain');
      expect(escapePathName('a b'), 'a b'); // space is legal in a path
      expect(escapePathName('ünïcode'), 'ünïcode'); // non-ASCII stays raw
    });

    test('unescape tolerates malformed sequences (old unescaped tables)', () {
      expect(unescapePathName('50%'), '50%');
      expect(unescapePathName('a%zz'), 'a%zz');
      expect(unescapePathName('trailing%2'), 'trailing%2');
    });

    test('encodePartition escapes; null keeps the Hive marker', () {
      expect(encodePartition('50%'), '50%25');
      expect(encodePartition(null), hiveDefaultPartition);
    });
  });

  group('decodePartitionValue (external review)', () {
    test('decodes each declared type, null via the empty string', () {
      expect(decodePartitionValue('5', DataType.int64), 5);
      expect(decodePartitionValue('1.5', DataType.float64), 1.5);
      expect(decodePartitionValue('true', DataType.boolean), true);
      expect(decodePartitionValue('false', DataType.boolean), false);
      expect(
        decodePartitionValue('2024-01-15', DataType.date),
        DateTime.utc(2024, 1, 15),
      );
      expect(
        decodePartitionValue('123.45', DataType.decimal, scale: 2),
        BigInt.from(12345),
      );
      expect(decodePartitionValue('', DataType.int64), isNull);
      expect(decodePartitionValue(null, DataType.int64), isNull);
    });

    test('malformed values fail loud, naming column/type/value', () {
      // tryParse-to-null / non-'true'→false used to FABRICATE data from
      // corrupt or foreign metadata.
      for (final (raw, type) in [
        ('12x', DataType.int64),
        ('1.2.3', DataType.float64),
        ('tru', DataType.boolean),
        ('TRUE', DataType.boolean),
        ('not-a-date', DataType.date),
        ('later', DataType.timestamp),
      ]) {
        expect(
          () => decodePartitionValue(raw, type, column: 'c'),
          throwsA(
            isA<FormatException>().having(
              (e) => e.message,
              'message',
              allOf(contains(raw), contains('c'), contains(type.name)),
            ),
          ),
          reason: '$raw as ${type.name}',
        );
      }
    });
  });
}

NumericColumn _num(DataType t, List<int> v) {
  if (t == DataType.int32) {
    return NumericColumn.int32(name: 'n', values: v, nullable: false);
  }
  return NumericColumn.int64(name: 'n', values: v, nullable: false);
}
