import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/api/dataframe.dart' show planOf;
import 'package:libbigdata/src/sql/operators/physical_operators.dart';
import 'package:test/test.dart';

import 'support/results.dart';

void main() {
  group('decimal columns keep their scale', () {
    late DataFrame df;
    setUp(() async {
      // 1.50, 2.75, 10.00 at decimal(4,2), built through SQL.
      final engine = SqlEngine.open().ok;
      addTearDown(engine.dispose);
      engine.registerTable(
        'r',
        Table.fromRows('r', [
          {'k': 'a', 'x': 1.5},
          {'k': 'a', 'x': 2.75},
          {'k': 'b', 'x': 10.0},
        ]),
      );
      df = DataFrame.fromTable(
        await engine
            .execute('SELECT k, CAST(x AS DECIMAL(4,2)) AS d FROM r')
            .ok,
      );
    });

    List<String> text(Table table, String column) => [
      for (final row in table.toRows()) '${row[column]}',
    ];

    test('select, rename, withColumn and drop', () async {
      expect(text(await df.select(['d']).execute().ok, 'd'), [
        '1.50',
        '2.75',
        '10.00',
      ]);
      expect(text(await df.rename({'d': 'e'}).execute().ok, 'e'), [
        '1.50',
        '2.75',
        '10.00',
      ]);
      expect(text(await df.withColumn('z', 'k').execute().ok, 'd'), [
        '1.50',
        '2.75',
        '10.00',
      ]);
      expect(text(await df.drop(['k']).execute().ok, 'd'), [
        '1.50',
        '2.75',
        '10.00',
      ]);
    });

    test('groupBy sums at the column scale', () async {
      final sums = await df.groupBy(['k']).agg({'d': 'sum'}).execute().ok;
      expect(
        {for (final row in sums.toRows()) row['k']: '${row['d']}'},
        {'a': '4.25', 'b': '10.00'},
      );
    });

    test('filter compares decimal values', () async {
      expect((await df.filter('d > 2').execute().ok).rowCount, 2);
      expect((await df.filter('d == 2.75').execute().ok).rowCount, 1);
    });

    test('rows round-trip through Table.fromRows', () async {
      final table = await df.execute().ok;
      final back = Table.fromRows('back', table.toRows());
      expect(back.schema.field('d')!.dataType, DataType.decimal);
      expect(text(back, 'd'), ['1.50', '2.75', '10.00']);
    });

    test('a chunk row is a Decimal', () async {
      final table = await df.execute().ok;
      expect('${table.toChunks().first.row(0)['d']}', '1.50');
    });
  });

  group('lazy file source (finding 6)', () {
    late Directory dir;
    late String path;
    setUp(() async {
      dir = Directory.systemTemp.createTempSync('df_lazy_');
      path = '${dir.path}/data.parquet';
      await ParquetWriter(path)
          .write(
            Table(
              name: 't',
              columns: [
                NumericColumn.int64(
                  name: 'id',
                  values: List.generate(100, (i) => i),
                  nullable: false,
                ),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'id', dataType: DataType.int64, nullable: false),
                ],
              ),
            ),
          )
          .ok;
    });
    tearDown(() => dir.deleteSync(recursive: true));

    test('readParquet(...).filter(...).head(n) streams correct rows', () async {
      final df = await DataFrameIO.readParquet(path).ok;
      final out = await df.filter('id >= 90').head(3).ok;
      expect(out.column('id')!.toList(), [90, 91, 92]);
    });

    test('count() streams the lazy source (no full materialization)', () async {
      final df = await DataFrameIO.readParquet(path).ok;
      expect(await df.count().ok, 100);
      expect(await df.filter('id >= 90').count().ok, 10);
    });

    test('a lazy readParquet still reads fully on execute', () async {
      final df = await DataFrameIO.readParquet(path).ok;
      expect((await df.execute().ok).rowCount, 100);
    });
  });

  group('ops', () {
    DataFrame people() => DataFrame.fromRows([
      {'dept': 'eng', 'name': 'A', 'salary': 100, 'age': 30},
      {'dept': 'eng', 'name': 'B', 'salary': 120, 'age': 40},
      {'dept': 'sales', 'name': 'C', 'salary': 80, 'age': 50},
    ]);

    test('filter rejects a string compared with a numeric column', () async {
      for (final predicate in ["salary == '100'", "salary > '90'"]) {
        await expectLater(
          people().filter(predicate).execute(),
          failsWith(Kind.invalid, contains('number')),
          reason: predicate,
        );
      }
      // An unquoted word is a column name, as in SQL.
      await expectLater(
        people().filter('age < thirty').execute(),
        failsWith(Kind.invalid, contains('Unknown column: thirty')),
      );
      final t = await people().filter('salary >= 100').execute().ok;
      expect(t.column('name')!.toList(), ['A', 'B']);
    });

    test('filter compares a temporal column with a string', () async {
      final events = DataFrame.fromTable(
        Table(
          name: 'e',
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64),
              Field(name: 't', dataType: DataType.timestamp),
            ],
          ),
          columns: [
            NumericColumn.int64(name: 'id', values: [1, 2], nullable: false),
            ListColumn<DateTime>(
              name: 't',
              dataType: DataType.timestamp,
              data: [DateTime.utc(2024, 5, 1), DateTime.utc(2024, 7, 1)],
            ),
          ],
        ),
      );
      final t = await events.filter("t > '2024-06-01'").execute().ok;
      expect(t.column('id')!.toList(), [2]);
      await expectLater(
        events.filter("t > 'soon'").execute(),
        failsWith(Kind.invalid),
      );
    });

    group('orderBy null placement', () {
      DataFrame nums() => DataFrame.fromRows([
        {'x': 5},
        {'x': null},
        {'x': 3},
      ]);

      test('nulls sort last in both directions, as SQL', () async {
        final desc = await nums().orderBy('x', ascending: false).execute().ok;
        expect(desc.column('x')!.toList(), [5, 3, null]);
        final asc = await nums().orderBy('x', ascending: true).execute().ok;
        expect(asc.column('x')!.toList(), [3, 5, null]);
      });

      test('a boolean column sorts false before true', () async {
        final flags = DataFrame.fromRows([
          {'id': 1, 'flag': true},
          {'id': 2, 'flag': false},
          {'id': 3, 'flag': true},
          {'id': 4, 'flag': false},
        ]);
        final asc = await flags.orderBy('flag').execute().ok;
        expect(asc.column('flag')!.toList(), [false, false, true, true]);
        final desc = await flags.orderBy('flag', ascending: false).execute().ok;
        expect(desc.column('flag')!.toList(), [true, true, false, false]);
      });

      test('a null boolean sorts last in both directions', () async {
        final flags = DataFrame.fromRows([
          for (final flag in [true, false, null, true, false]) {'flag': flag},
        ]);
        for (final (ascending, expected) in [
          (true, [false, false, true, true, null]),
          (false, [true, true, false, false, null]),
        ]) {
          final sorted = await flags
              .orderBy('flag', ascending: ascending)
              .execute()
              .ok;
          expect(sorted.column('flag')!.toList(), expected);
        }
      });

      test('orderBy then limit runs as a top-N sort', () async {
        final plan = await planOf(nums().orderBy('x').limit(2));
        expect(plan, isA<PhysicalLimit>());
        expect((plan as PhysicalLimit).input, isA<PhysicalTopN>());
        final top = await nums().orderBy('x').limit(2).execute().ok;
        expect(top.column('x')!.toList(), [3, 5]);
      });
    });

    test('a frame whose schema cannot be derived reports none', () async {
      final bad = people().select(['nope']);
      expect(bad.schema, isNull);
      expect(bad.columns, isNull);
      await expectLater(bad.execute(), failsWith(Kind.invalid));
    });

    group('groupBy + agg', () {
      test('a groupBy not followed by agg is invalid', () async {
        final grouped = people().groupBy(['dept']);
        final cases = <String, Future<Result<Object?, ErrorDetails>>>{
          'execute': grouped.execute(),
          'executeStream': grouped.executeStream().first,
          'count': grouped.count(),
          'toList': grouped.toList(),
          'filter then agg': grouped.filter('age > 0').agg({
            'salary': 'sum',
          }).execute(),
        };
        for (final MapEntry(:key, :value) in cases.entries) {
          await expectLater(
            value,
            failsWith(Kind.invalid, contains('groupBy')),
            reason: key,
          );
        }
      });

      test('aggregates within groups', () async {
        final t = await people()
            .groupBy(['dept'])
            .agg({'salary': 'sum', 'age': 'avg'})
            .execute()
            .ok;
        final rows = {for (final r in t.toRows()) r['dept']: r};
        expect(
          rows['eng']!['salary'],
          Decimal(BigInt.from(220), 0),
        ); // integer SUM → decimal
        expect(rows['eng']!['age'], 35.0);
        expect(rows['sales']!['salary'], Decimal(BigInt.from(80), 0));
        expect(rows['sales']!['age'], 50.0);
      });

      test('global aggregate (no groupBy) yields one row', () async {
        final t = await people()
            .agg({'salary': 'sum', 'name': 'count'})
            .execute()
            .ok;
        expect(t.rowCount, 1);
        expect(t.toRows().first['salary'], Decimal(BigInt.from(300), 0));
        expect(t.toRows().first['name'], 3);
      });

      test('min/max', () async {
        final t = await people()
            .groupBy(['dept'])
            .agg({'salary': 'min', 'age': 'max'})
            .execute()
            .ok;
        final eng = t.toRows().firstWhere((r) => r['dept'] == 'eng');
        expect(eng['salary'], 100);
        expect(eng['age'], 40);
      });
    });

    group('join', () {
      DataFrame depts() => DataFrame.fromRows([
        {'dept': 'eng', 'building': 'B1'},
        {'dept': 'ops', 'building': 'B2'},
      ]);

      test('inner join keeps only matches', () async {
        final t = await people().join(depts(), 'dept').execute().ok;
        expect(t.rowCount, 2); // both eng rows; sales has no match
        expect(t.toRows().every((r) => r['building'] == 'B1'), isTrue);
      });

      test('left join keeps unmatched left rows with nulls', () async {
        final t = await people()
            .join(depts(), 'dept', type: JoinType.left)
            .execute()
            .ok;
        expect(t.rowCount, 3);
        final sales = t.toRows().firstWhere((r) => r['dept'] == 'sales');
        expect(sales['building'], isNull);
      });

      test('a column whose name has a dot is taken as written', () async {
        final dotted = DataFrame.fromTable(
          Table.fromRows('c', [
            {'a.b': 2, 'k': 'x'},
            {'a.b': 1, 'k': 'y'},
          ]),
        );
        final sorted = dotted.orderBy('a.b').select(['a.b', 'k']);
        expect(sorted.columns, ['a.b', 'k']);
        expect((await sorted.execute().ok).toRows(), [
          {'a.b': 1, 'k': 'y'},
          {'a.b': 2, 'k': 'x'},
        ]);
        expect((await dotted.drop(['a.b']).execute().ok).schema.fieldNames, [
          'k',
        ]);
        final renamed = dotted.rename({'a.b': 'n'});
        expect(renamed.columns, ['n', 'k']);
        expect((await renamed.execute().ok).schema.fieldNames, ['n', 'k']);
        final grouped = dotted.groupBy(['k']).agg({'a.b': 'sum'});
        expect(grouped.columns, ['k', 'a.b']);
        expect((await grouped.execute().ok).schema.fieldNames, ['k', 'a.b']);
      });

      test('a join takes a qualified key the frame reports', () async {
        final p = DataFrame.fromTable(
          Table.fromRows('p', [
            {'id': 1, 'dept': 'eng', 'name': 'Ann'},
            {'id': 2, 'dept': 'ops', 'name': 'Bo'},
          ]),
        );
        final d = DataFrame.fromTable(
          Table.fromRows('d', [
            {'dept': 'eng', 'name': 'Engineering'},
          ]),
        );
        final budgets = DataFrame.fromTable(
          Table.fromRows('b', [
            {'d.name': 'Engineering', 'budget': 10},
          ]),
        );
        final joined = p.join(d, 'dept').join(budgets, 'd.name');
        final result = await joined.execute().ok;
        expect(result.schema.fieldNames, [
          'id',
          'dept',
          'p.name',
          'd.name',
          'budget',
        ]);
        expect(joined.columns, result.schema.fieldNames);
        expect(result.toRows(), [
          {
            'id': 1,
            'dept': 'eng',
            'p.name': 'Ann',
            'd.name': 'Engineering',
            'budget': 10,
          },
        ]);
      });
    });

    group('withColumn', () {
      test('arithmetic on a decimal column keeps it decimal', () async {
        final prices = DataFrame.fromTable(
          Table(
            name: 'p',
            schema: Schema(
              fields: [
                Field(
                  name: 'd',
                  dataType: DataType.decimal,
                  precision: 10,
                  scale: 2,
                ),
              ],
            ),
            columns: [
              ListColumn<BigInt>(
                name: 'd',
                dataType: DataType.decimal,
                data: [BigInt.from(150), BigInt.from(-25)],
              ),
            ],
          ),
        );
        final t = await prices.withColumn('z', 'd * 2').execute().ok;
        expect(t.schema.field('z')!.dataType, DataType.decimal);
        expect(t.toRows().map((r) => r['z'].toString()), ['3.00', '-0.50']);
      });

      test('computes an arithmetic column', () async {
        final t = await people().withColumn('bonus', 'salary * 2').execute().ok;
        final rows = t.toRows();
        expect(rows.first['bonus'], 200);
        expect(rows[2]['bonus'], 160);
      });

      test('adds a constant column', () async {
        final t = await people().withColumn('flag', '1').execute().ok;
        expect(t.toRows().every((r) => r['flag'] == 1), isTrue);
      });

      test('a negative literal operand parses (audit 7)', () async {
        // `salary * -1` used to split at the unary '-': the left became the
        // unknown column "salary *" (null) and the result silently nulled.
        final t = await people().withColumn('neg', 'salary * -1').execute().ok;
        expect(t.toRows().first['neg'], -100);
        final s = await people().withColumn('less', 'salary - -5').execute().ok;
        expect(s.toRows().first['less'], 105);
      });
    });

    // The columnar implementations of join/agg/withColumn run on the vectorized
    // engine; these pin behaviors that differ from (or extend) the old row path.
    group('columnar parity (#6)', () {
      DataFrame depts() => DataFrame.fromRows([
        {'dept': 'eng', 'building': 'B1'},
        {'dept': 'ops', 'building': 'B2'},
      ]);

      test('multi-aggregate with mean alias equals avg', () async {
        final t = await people()
            .groupBy(['dept'])
            .agg({'salary': 'sum', 'age': 'mean'})
            .execute()
            .ok;
        final eng = t.toRows().firstWhere((r) => r['dept'] == 'eng');
        expect(eng['salary'], Decimal(BigInt.from(220), 0));
        expect(eng['age'], 35.0);
      });

      test('min/max work on string columns', () async {
        final t = await people()
            .groupBy(['dept'])
            .agg({'name': 'min'})
            .execute()
            .ok;
        final eng = t.toRows().firstWhere((r) => r['dept'] == 'eng');
        expect(eng['name'], 'A');
      });

      test('count skips nulls', () async {
        final df = DataFrame.fromRows([
          {'g': 'a', 'v': 1},
          {'g': 'a', 'v': null},
          {'g': 'b', 'v': 2},
        ]);
        final t = await df.groupBy(['g']).agg({'v': 'count'}).execute().ok;
        final a = t.toRows().firstWhere((r) => r['g'] == 'a');
        expect(a['v'], 1); // the null is not counted
      });

      test('empty-input global aggregate yields one NULL row', () async {
        final t = await people()
            .filter('salary > 1000')
            .agg({'salary': 'sum'})
            .execute()
            .ok;
        // A global aggregate always returns exactly one row; SUM over no rows is
        // NULL (SQL semantics).
        expect(t.rowCount, 1);
        expect(t.toRows().single['salary'], isNull);
      });

      test('right join keeps unmatched right rows with their key', () async {
        // The join is on a shared column (USING), so its one key column holds
        // the right side's key for rows only the right side has.
        final t = await people()
            .join(depts(), 'dept', type: JoinType.right)
            .execute()
            .ok;
        final ops = t.toRows().firstWhere((r) => r['building'] == 'B2');
        expect(ops['dept'], 'ops');
      });

      test('full join keeps both unmatched sides', () async {
        final t = await people()
            .join(depts(), 'dept', type: JoinType.full)
            .execute()
            .ok;
        expect(
          t.toRows().any((r) => r['building'] == 'B2' && r['dept'] == 'ops'),
          isTrue,
        );
        expect(
          t.toRows().any((r) => r['dept'] == 'sales' && r['building'] == null),
          isTrue,
        );
      });

      test('cross join is the cartesian product', () async {
        final t = await people()
            .join(depts(), 'dept', type: JoinType.cross)
            .execute()
            .ok;
        expect(t.rowCount, 6); // 3 x 2
      });

      test('one-to-many join fans out', () async {
        final left = DataFrame.fromRows([
          {'k': 1, 'lv': 'x'},
        ]);
        final right = DataFrame.fromRows([
          {'k': 1, 'rv': 'a'},
          {'k': 1, 'rv': 'b'},
        ]);
        final t = await left.join(right, 'k').execute().ok;
        expect(t.rowCount, 2);
      });

      test('withColumn replaces an existing column in place', () async {
        final t = await people()
            .withColumn('salary', 'salary + 1')
            .execute()
            .ok;
        final names = t.schema.fieldNames;
        expect(names.where((n) => n == 'salary').length, 1); // not duplicated
        expect(t.toRows().first['salary'], 101);
      });

      test('withColumn float result and divide-by-zero is NULL', () async {
        final t = await people().withColumn('half', 'salary / 2').execute().ok;
        expect(t.toRows().first['half'], 50.0);
        // Division by zero is NULL (SQL semantics), not NaN.
        final z = await people().withColumn('z', 'salary / 0').execute().ok;
        expect(z.toRows().first['z'], isNull);
      });

      test(
        'withColumn numeric + on strings throws (no concatenation)',
        () async {
          final df = DataFrame.fromRows([
            {'a': 'x', 'b': 'y'},
          ]);
          await expectLater(
            df.withColumn('c', 'a + b').execute(),
            failsWith(Kind.invalid),
          );
        },
      );
    });

    group('plan fusion (chained ops run as one streamed plan)', () {
      test(
        'filter -> select -> limit fuses and limit short-circuits',
        () async {
          final df = DataFrame.fromRows([
            for (var i = 0; i < 1000; i++)
              {'id': i, 'v': i * 2, 'extra': 'x$i'},
          ]);
          final out = await df
              .filter('id >= 100')
              .select(['id', 'v'])
              .limit(3)
              .toList()
              .ok;
          expect(out, [
            {'id': 100, 'v': 200},
            {'id': 101, 'v': 202},
            {'id': 102, 'v': 204},
          ]);
        },
      );

      test('a chain ending in an unfusible op (sample) still works', () async {
        final df = DataFrame.fromRows([
          for (var i = 0; i < 100; i++) {'id': i},
        ]);
        final out = await df
            .filter('id >= 50')
            .sample(0.2, seed: 7)
            .execute()
            .ok;
        expect(out.rowCount, 10); // 50 rows after filter, 20% sampled
        for (final r in out.toRows()) {
          expect(r['id'] as int, greaterThanOrEqualTo(50));
        }
      });
    });

    group('drop', () {
      test('removes named columns, preserves the rest', () async {
        final t = await people().drop(['salary', 'age']).execute().ok;
        expect(t.schema.fieldNames, ['dept', 'name']);
        final rows = t.toRows();
        expect(rows.length, 3);
        expect(rows.first.keys.toSet(), {'dept', 'name'});
        expect(rows.first['name'], 'A');
      });
    });

    group('rename', () {
      test('renames a column, keeping its data', () async {
        final t = await people().rename({'name': 'employee'}).execute().ok;
        expect(t.schema.fieldNames, contains('employee'));
        expect(t.schema.fieldNames, isNot(contains('name')));
        expect(t.column('employee')![0], 'A');
        expect(t.column('employee')![2], 'C');
      });
    });

    group('union', () {
      test('concatenates rows of two frames with the same schema', () async {
        final t = await people().union(people()).execute().ok;
        expect(t.rowCount, 6);
        expect(t.schema.fieldNames, ['dept', 'name', 'salary', 'age']);
        final names = t.column('name')!.toList();
        expect(names, ['A', 'B', 'C', 'A', 'B', 'C']);
      });
    });

    group('head / tail', () {
      test('head returns the first n rows in order', () async {
        final t = await people().head(2).ok;
        expect(t.rowCount, 2);
        expect(t.column('name')!.toList(), ['A', 'B']);
      });

      test('tail returns the last n rows in order', () async {
        final t = await people().tail(2).ok;
        expect(t.rowCount, 2);
        expect(t.column('name')!.toList(), ['B', 'C']);
      });

      test('head with n beyond rowCount returns all rows', () async {
        final t = await people().head(100).ok;
        expect(t.rowCount, 3);
      });
    });

    group('transform', () {
      test('applies a synchronous Table transformer', () async {
        final t = await people()
            .transform((table) => table.head(1))
            .execute()
            .ok;
        expect(t.rowCount, 1);
        expect(t.column('name')![0], 'A');
      });

      test('throws when operations are pending', () {
        expect(
          () => people().filter('salary > 90').transform((table) => table),
          throwsStateError,
        );
      });
    });

    group('clone', () {
      test('clone executes to the same result as the original', () async {
        final original = people().filter('salary >= 100');
        final cloned = original.clone();
        final a = await original.execute().ok;
        final b = await cloned.execute().ok;
        expect(b.rowCount, a.rowCount);
        expect(b.column('name')!.toList(), a.column('name')!.toList());
      });

      test('operations added after clone do not affect the original', () async {
        final base = people();
        final narrowed = base.clone().filter('dept = \'sales\'');
        expect((await narrowed.execute().ok).rowCount, 1);
        expect((await base.execute().ok).rowCount, 3);
      });
    });
  });

  group('engine execution', () {
    DataFrame frame() => DataFrame.fromTable(
      Table(
        name: 't',
        columns: [
          NumericColumn.int64(
            name: 'id',
            values: [1, 2, 3, 4, 5],
            nullable: false,
          ),
          ListColumn<String>(
            name: 'Dept Name',
            dataType: DataType.string,
            data: ['x', 'x', 'y', 'y', 'z'],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'id', dataType: DataType.int64, nullable: false),
            Field(
              name: 'Dept Name',
              dataType: DataType.string,
              nullable: false,
            ),
          ],
        ),
      ),
    );

    group('DataFrame filter on the vectorized engine', () {
      test('single comparison still works (backward compatible)', () async {
        final t = await frame().filter('id >= 3').execute().ok;
        expect(t.column('id')!.toList(), [3, 4, 5]);
      });

      test('quoted column names with spaces and == work', () async {
        final t = await frame().filter('"Dept Name" == \'y\'').execute().ok;
        expect(t.rowCount, 2);
        expect(t.column('id')!.toList(), [3, 4]);
      });

      test('AND (new capability)', () async {
        final t = await frame().filter('id >= 2 AND id <= 4').execute().ok;
        expect(t.column('id')!.toList(), [2, 3, 4]);
      });

      test('OR (new capability)', () async {
        final t = await frame().filter('id == 1 OR id == 5').execute().ok;
        expect(t.column('id')!.toList(), [1, 5]);
      });

      test('mixed AND/OR with a quoted spaced name', () async {
        final t = await frame()
            .filter('"Dept Name" = \'x\' OR id >= 4 AND id <= 5')
            .execute()
            .ok;
        // OR is lower precedence: (Dept = 'x') OR (id>=4 AND id<=5) -> ids 1,2,4,5
        expect(t.column('id')!.toList(), [1, 2, 4, 5]);
      });

      test('filter preserves typed (NumericColumn) output', () async {
        final t = await frame().filter('id > 2').execute().ok;
        expect(t.column('id'), isA<NumericColumn>());
      });

      test(
        'a non-boolean predicate casts to BOOLEAN; an empty one fails',
        () async {
          // As DuckDB: a nonzero number keeps the row.
          final kept = await frame().filter('id - 1').execute().ok;
          expect(kept.rowCount, (await frame().execute().ok).rowCount - 1);
          await expectLater(
            frame().filter('').execute(),
            failsWith(Kind.invalid),
          );
        },
      );
    });

    group('DataFrame distinct on the engine', () {
      test('dedupes rows preserving first-seen order, typed key', () async {
        final df = DataFrame.fromTable(
          Table(
            name: 't',
            columns: [
              NumericColumn.int64(
                name: 'a',
                values: [1, 1, 2, 2],
                nullable: false,
              ),
              ListColumn<String>(
                name: 'b',
                dataType: DataType.string,
                data: ['p', 'p', 'q', 'q'],
                nullable: false,
              ),
            ],
            schema: Schema(
              fields: [
                Field(name: 'a', dataType: DataType.int64, nullable: false),
                Field(name: 'b', dataType: DataType.string, nullable: false),
              ],
            ),
          ),
        );
        final t = await df.distinct().execute().ok;
        expect(t.rowCount, 2);
        expect(t.column('a')!.toList(), [1, 2]);
        expect(t.column('a'), isA<NumericColumn>());
      });
    });

    group('stub fixes', () {
      test('toTable throws when operations are pending', () {
        expect(() => frame().filter('id > 1').toTable(), throwsStateError);
      });

      test('toTable returns the source when no operations are pending', () {
        final t = frame().toTable();
        expect(t.rowCount, 5);
      });

      test('sample with a seed is deterministic', () async {
        final a = await frame().sample(0.6, seed: 42).execute().ok;
        final b = await frame().sample(0.6, seed: 42).execute().ok;
        expect(a.column('id')!.toList(), b.column('id')!.toList());
      });
    });
  });

  group('predicates', () {
    group('DataFrame Predicate Tests', () {
      test('handles column names with spaces', () async {
        final data = [
          {
            'First Name': 'Alice',
            'Last Name': 'Smith',
            'Age': 30,
            'Status': 'Active',
          },
          {
            'First Name': 'Bob',
            'Last Name': 'Jones',
            'Age': 25,
            'Status': 'Inactive',
          },
          {
            'First Name': 'Charlie',
            'Last Name': 'Brown',
            'Age': 35,
            'Status': 'Active',
          },
          {
            'First Name': 'David',
            'Last Name': 'Wilson',
            'Age': 28,
            'Status': 'Active',
          },
        ];

        final df = DataFrame.fromRows(data);

        // Test filtering with column names containing spaces
        final filtered1 = df.filter('"First Name" == \'Alice\'');
        final table1 = await filtered1.execute().ok;
        expect(table1.rowCount, equals(1));
        expect(table1.toRows().first['First Name'], equals('Alice'));

        // Test with different operators
        final filtered2 = df.filter('Age >= 30');
        final table2 = await filtered2.execute().ok;
        expect(table2.rowCount, equals(2));

        // Test with multiple words in column name
        final filtered3 = df.filter('"Last Name" != \'Smith\'');
        final table3 = await filtered3.execute().ok;
        expect(table3.rowCount, equals(3));
      });

      test('handles column names with special characters', () async {
        final data = [
          {'Special-Column': 'A', 'Value': 100},
          {'Special-Column': 'B', 'Value': 200},
          {'Special-Column': 'A', 'Value': 150},
        ];

        final df = DataFrame.fromRows(data);

        // Test with special characters
        final filtered = df.filter('"Special-Column" == \'A\'');
        final table = await filtered.execute().ok;
        expect(table.rowCount, equals(2));
      });

      test('handles various predicate formats', () async {
        final data = [
          {'Product Name': 'Widget A', 'Price': 10.99, 'In Stock': true},
          {'Product Name': 'Widget B', 'Price': 25.50, 'In Stock': false},
          {'Product Name': 'Widget C', 'Price': 15.00, 'In Stock': true},
        ];

        final df = DataFrame.fromRows(data);

        // Test with decimal values
        final filtered1 = df.filter('Price > 15.0');
        final table1 = await filtered1.execute().ok;
        expect(table1.rowCount, equals(1));
        expect(table1.toRows().first['Product Name'], equals('Widget B'));

        // Test with boolean values
        final filtered2 = df.filter('"In Stock" == true');
        final table2 = await filtered2.execute().ok;
        expect(table2.rowCount, equals(2));

        // Test with string containing spaces
        final filtered3 = df.filter('"Product Name" == \'Widget A\'');
        final table3 = await filtered3.execute().ok;
        expect(table3.rowCount, equals(1));
      });

      test('handles edge cases', () async {
        final data = [
          {'Column With Many Spaces': 'Value 1', 'Normal': 100},
          {'Column With Many Spaces': 'Value 2', 'Normal': 200},
        ];

        final df = DataFrame.fromRows(data);

        // Test column with multiple spaces
        final filtered = df.filter('"Column With Many Spaces" == \'Value 1\'');
        final table = await filtered.execute().ok;
        expect(table.rowCount, equals(1));
        expect(table.toRows().first['Normal'], equals(100));
      });

      test('throws error for invalid predicates', () async {
        final data = [
          {'Name': 'Alice', 'Age': 30},
        ];

        final df = DataFrame.fromRows(data);

        // A comparison missing its right side
        await expectLater(
          df.filter('Name >').execute(),
          failsWith(Kind.invalid),
        );

        // Test completely invalid predicate
        await expectLater(df.filter('').execute(), failsWith(Kind.invalid));
      });
    });
  });

  group(
    'DataFrame arithmetic is left-associative within a precedence level',
    () {
      test('a - b - c computes (a - b) - c', () async {
        final df = DataFrame.fromRows([
          {'a': 10, 'b': 3, 'c': 2},
        ]).withColumn('r', 'a - b - c');
        final t = await df.execute().ok;
        // (10 - 3) - 2 = 5, not 10 - (3 - 2) = 9.
        expect(t.column('r')!.toList().first, 5);
      });

      test('a / b / c computes (a / b) / c', () async {
        final df = DataFrame.fromRows([
          {'a': 100.0, 'b': 5.0, 'c': 2.0},
        ]).withColumn('r', 'a / b / c');
        final t = await df.execute().ok;
        // (100 / 5) / 2 = 10, not 100 / (5 / 2) = 40.
        expect(t.column('r')!.toList().first, 10.0);
      });
    },
  );

  group('#7 DataFrame eager schema derivation', () {
    test('withColumn then select sees the new column', () {
      final df = DataFrame.fromRows([
        {'a': 1},
        {'a': 2},
      ]);
      final x = df.withColumn('x', 'a + 1');
      expect(x.schema!.fieldNames, containsAll(['a', 'x']));
      final sel = x.select(['x']); // must not throw on the derived schema
      expect(sel.schema!.fieldNames, ['x']);
    });

    test('join exposes both sides in columns', () {
      final left = DataFrame.fromRows([
        {'id': 1, 'l': 'a'},
      ]);
      final right = DataFrame.fromRows([
        {'id': 1, 'r': 'b'},
      ]);
      final joined = left.join(right, 'id');
      expect(joined.schema!.fieldNames, containsAll(['id', 'l', 'r']));
    });
  });

  group('#7 DataFrame bounded collector', () {
    test('execute() caps like SQL; maxRows: null opts out', () async {
      final df = DataFrame.fromRows([
        for (var i = 0; i < 5; i++) {'a': i},
      ]).select(['a']);
      await expectLater(df.execute(maxRows: 2), failsWith(Kind.limit));
      final t = await df.execute(maxRows: null).ok;
      expect(t.rowCount, 5);
    });
  });

  group('#8 DataFrame intermediate byte cap', () {
    test(
      'a tiny maxBytes bounds an unfused intermediate; null opts out',
      () async {
        final df = DataFrame.fromRows([
          for (var i = 0; i < 100; i++) {'a': i},
        ]).sample(1.0); // sample is unfusible → materializes an intermediate
        await expectLater(df.execute(maxBytes: 1), failsWith(Kind.limit));
        expect((await df.execute(maxBytes: null).ok).rowCount, 100);
      },
    );
  });

  group('#9 no-op DataFrame.execute honors caps', () {
    test('execute(maxRows: 1) throws on a 2-row no-op plan', () async {
      final df = DataFrame.fromRows([
        {'a': 1},
        {'a': 2},
      ]);
      await expectLater(df.execute(maxRows: 1), failsWith(Kind.limit));
    });

    test('under the cap a no-op execute returns the table', () async {
      final df = DataFrame.fromRows([
        {'a': 1},
      ]);
      expect((await df.execute(maxRows: 10).ok).rowCount, 1);
    });
  });

  // A caller's limits reach the other frame of a join or union: its plan is
  // lowered with the same ExecutionContext, so a tighter cap applies where it
  // materializes (a sample on that side), and executeStream, which asks for
  // no cap, has none imposed there.
  group('execution policy reaches nested plans', () {
    DataFrame frame(int rows, String column) => DataFrame.fromRows([
      for (var i = 0; i < rows; i++) {'id': i, column: 'x$i'},
    ]);
    DataFrame left(int rows) => frame(rows, 'l');
    DataFrame right(int rows) => frame(rows, 'r');

    test("a caller's byte cap bounds the join's RIGHT side", () async {
      // The join yields 1 row, so the FINAL cap can't be what fires, only the
      // right side's own materialization (forced by sample) can.
      await expectLater(
        left(1).join(right(5000).sample(1.0), 'id').execute(maxBytes: 1024),
        failsWith(Kind.limit),
      );
    });

    test("a caller's byte cap bounds a union's right side", () async {
      await expectLater(
        left(1).union(left(5000).sample(1.0)).execute(maxBytes: 1024),
        failsWith(Kind.limit),
      );
    });

    test('a generous cap lets the same join through', () async {
      final t = await left(1).join(right(5000).sample(1.0), 'id').execute().ok;
      expect(t.rowCount, 1);
    });

    test('executeStream imposes no cap of its own', () async {
      // The right side materializes ~5000 rows; executeStream previously
      // inherited a 1 GiB cap it never asked for. Assert it simply works.
      final batches = await left(
        1,
      ).join(right(5000).sample(1.0), 'id').executeStream().ok.toList();
      expect(batches.fold<int>(0, (a, b) => a + b.rowCount), 1);
    });

    test('an EngineOptions reaches the blocking operators', () async {
      // maxOutputRows is enforced by the operators themselves, so it can only
      // take effect if ctx.options was threaded into them.
      await expectLater(
        left(100)
            .join(right(100), 'id')
            .execute(options: const EngineOptions(maxOutputRows: 10)),
        failsWith(Kind.limit),
      );
    });
  });

  group('terminal operations report failures as Err', () {
    final bad = DataFrame.fromRows([
      {'id': 1},
    ]).filter('nope > 1');

    final terminals =
        <String, Future<Result<Object?, ErrorDetails>> Function()>{
          'execute': () => bad.execute(),
          'toList': () => bad.toList(),
          'toJsonString': () => bad.toJsonString(),
          'head': () => bad.head(),
          'tail': () => bad.tail(),
          'count': () => bad.count(),
          'show': () => bad.show(),
          'cache': () => bad.cache(),
          'toCsv': () => bad.toCsv(),
          'toJson': () => bad.toJson(),
          'toJsonLines': () => bad.toJsonLines(),
          'executeStream': () => bad.executeStream().last,
        };
    for (final MapEntry(:key, :value) in terminals.entries) {
      test(key, () async {
        await expectLater(value(), failsWith(Kind.invalid));
      });
    }
  });

  group('builders never throw', () {
    final frame = DataFrame.fromTable(
      Table.fromRows('t', [
        {'a': 1},
      ]),
    );

    test('an unknown aggregate function is an Err from execute', () async {
      final built = frame.agg({'a': 'median_of_nothing'});
      await expectLater(
        built.execute(),
        failsWith(Kind.invalid, contains('median_of_nothing')),
      );
    });

    test('an unknown column in select is an Err from execute', () async {
      final built = frame.select(['nope']);
      await expectLater(
        built.execute(),
        failsWith(Kind.invalid, contains('nope')),
      );
    });

    test('a bad withColumn expression is an Err from execute', () async {
      final built = frame.withColumn('b', 'nope + 1');
      await expectLater(built.execute(), failsWith(Kind.invalid));
    });
  });

  group('execution resources', () {
    test(
      'a crash mid-spill leaves runs only inside a session directory',
      () async {
        // A child process with its own TMPDIR spills a sort, then exits mid-stream
        // without cleanup, as a crash would. The orphan sweep only reclaims
        // session directories, so a run left anywhere else would never go.
        final temp = Directory.systemTemp.createTempSync('df_spill_');
        addTearDown(() => temp.deleteSync(recursive: true));
        final probe = File('tool/_spill_probe.dart')
          ..writeAsStringSync('''
import 'dart:io';
import 'package:libbigdata/libbigdata.dart';

Future<void> main() async {
  final frame = DataFrame.fromRows([
    for (var i = 0; i < 20000; i++) {'id': 20000 - i},
  ]).orderBy('id');
  await for (final _ in frame.executeStream(
    options: const EngineOptions(spillThresholdBytes: 1),
  )) {
    exit(3);
  }
}
''');
        addTearDown(probe.deleteSync);
        final result = await Process.run(
          Platform.resolvedExecutable,
          ['run', probe.path],
          environment: {'TMPDIR': temp.path},
        );
        expect(result.exitCode, 3, reason: '${result.stderr}');
        final runs = [
          for (final entity in temp.listSync(recursive: true))
            if (entity is File &&
                entity.path.split('/').last.startsWith('spill-'))
              entity.path,
        ];
        expect(runs, isNotEmpty, reason: 'the sort did not spill');
        for (final run in runs) {
          expect(run, contains('/libbigdata-session-'), reason: run);
        }
      },
    );

    test('the input to sample is held under the byte cap', () async {
      final frame = DataFrame.fromRows([
        for (var i = 0; i < 5000; i++) {'id': i},
      ]).sample(0.5, seed: 1);
      final stream = frame.executeStream(
        options: const EngineOptions(maxMaterializedBytes: 1024),
      );
      await expectLater(stream.first, failsWith(Kind.limit));
    });
  });
}
