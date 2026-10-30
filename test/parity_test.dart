// Parity tests for Phase 19 (SPEC-dataframe-planner.md): each DataFrame
// operation, and chained pipelines, give the same answer as the equivalent
// SQL query on the same data (column names, types, and rows: in order where
// the query defines one, as a sorted multiset otherwise). They encode the
// owner's decisions, so many fail until the task that implements each one:
//
//   parity expressions          T161 (SQL expression grammar, `==`, precedence)
//   parity select/filter/limit/orderBy/drop/rename/withColumn/distinct
//                               T162 (first operations, NULLS LAST)
//   parity agg/join/union       T163 (C7 names, USING joins, C10 arity kind)
//   parity pipelines            T163 (with T161 and T162)
//   parity sample               T164 (logical Sample, exact seeded rows)
//   parity file-backed          T165 (lazy readers, pruning as SQL prunes)
//   parity readers              T165 (every file reader lazy, its options kept)
//
// Run one group with `dart test test/parity_test.dart -N "parity sample"`.
//
// Join name collisions follow SQL `USING` (C8): a colliding non-key column is
// qualified by its side, and the side of an in-memory frame is its table's
// name, so the frames here wrap tables named as the SQL registers them.

import 'dart:convert';
import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/api/dataframe.dart' show planOf;
import 'package:libbigdata/src/formats/arrow/arrow_reader.dart';
import 'package:libbigdata/src/sql/planner/file_scan.dart';
import 'package:test/test.dart';

import 'support/results.dart';

/// `p`: people, with a spaced column name and NULLs in `dept`, `salary` and
/// `age`; rows 2 and 6 share `name` and `dept`.
final Table people = Table.fromRows(
  'p',
  [
    {
      'id': 1,
      'name': 'Alice',
      'First Name': 'Alice',
      'dept': 'eng',
      'salary': 100.0,
      'age': 30,
    },
    {
      'id': 2,
      'name': 'Bob',
      'First Name': 'Bob',
      'dept': 'eng',
      'salary': 80.0,
      'age': null,
    },
    {
      'id': 3,
      'name': 'Carol',
      'First Name': 'Carol',
      'dept': 'ops',
      'salary': null,
      'age': 41,
    },
    {
      'id': 4,
      'name': 'Dan',
      'First Name': 'Dan',
      'dept': null,
      'salary': 60.0,
      'age': 25,
    },
    {
      'id': 5,
      'name': 'Eve',
      'First Name': 'Eve',
      'dept': 'ops',
      'salary': 90.0,
      'age': 35,
    },
    {
      'id': 6,
      'name': 'Bob',
      'First Name': 'Bob',
      'dept': 'eng',
      'salary': 85.0,
      'age': 28,
    },
  ],
  schema: Schema(
    fields: [
      Field(name: 'id', dataType: DataType.int64, nullable: false),
      Field(name: 'name', dataType: DataType.string),
      Field(name: 'First Name', dataType: DataType.string),
      Field(name: 'dept', dataType: DataType.string),
      Field(name: 'salary', dataType: DataType.float64),
      Field(name: 'age', dataType: DataType.int64),
    ],
  ),
);

/// `d`: departments. `dept` is the join key; `name` collides with `p.name`;
/// `hr` has no people and `p` has a person with no department.
final Table depts = Table.fromRows(
  'd',
  [
    {'dept': 'eng', 'floor': 3, 'name': 'Engineering'},
    {'dept': 'ops', 'floor': 1, 'name': 'Operations'},
    {'dept': 'hr', 'floor': 2, 'name': 'People'},
  ],
  schema: Schema(
    fields: [
      Field(name: 'dept', dataType: DataType.string),
      Field(name: 'floor', dataType: DataType.int64),
      Field(name: 'name', dataType: DataType.string),
    ],
  ),
);

/// `e`: events, for literal typing against a timestamp column (C15).
final Table events = Table.fromRows(
  'e',
  [
    {'id': 1, 'seen': DateTime.utc(2024, 5, 1)},
    {'id': 2, 'seen': DateTime.utc(2024, 7, 1)},
    {'id': 3, 'seen': null},
  ],
  schema: Schema(
    fields: [
      Field(name: 'id', dataType: DataType.int64, nullable: false),
      Field(name: 'seen', dataType: DataType.timestamp),
    ],
  ),
);

late SqlEngine engine;

/// The rows of [table] as text, one line per row, cells joined by ` | `
/// (read by position, so duplicate column names cannot collapse a row).
List<String> lines(Table table) {
  final values = [for (final column in table.columns) column.toList()];
  return [
    for (var row = 0; row < table.rowCount; row++)
      [for (final column in values) '${column[row]}'].join(' | '),
  ];
}

/// Expect [actual] and [expected] to hold the same column names, types and
/// rows, in order when [ordered], as a sorted multiset otherwise.
void match(Table actual, Table expected, {bool ordered = false}) {
  expect(
    actual.schema.fieldNames,
    expected.schema.fieldNames,
    reason: 'column names',
  );
  expect(
    [for (final field in actual.schema.fields) field.dataType],
    [for (final field in expected.schema.fields) field.dataType],
    reason: 'column types',
  );
  final have = lines(actual);
  final want = lines(expected);
  if (!ordered) {
    have.sort();
    want.sort();
  }
  expect(have, want, reason: ordered ? 'rows in order' : 'rows');
}

/// Run [frame] and [sql] and expect the same result (see [match]).
Future<void> same(DataFrame frame, String sql, {bool ordered = false}) async {
  final actual = await frame.execute().ok;
  final expected = await engine.execute(sql).ok;
  match(actual, expected, ordered: ordered);
}

void main() {
  setUpAll(() {
    engine = SqlEngine.open(allowExternalPaths: true).ok;
    engine.registerTable('p', people);
    engine.registerTable('d', depts);
    engine.registerTable('e', events);
  });
  tearDownAll(() => engine.dispose());

  final p = DataFrame.fromTable(people);
  final d = DataFrame.fromTable(depts);

  group('parity select/filter/limit/orderBy/drop/rename/withColumn/distinct', () {
    test('select picks columns in the order given', () async {
      await same(p.select(['salary', 'id']), 'SELECT salary, id FROM p');
    });

    test('select takes a spaced name as written', () async {
      await same(
        p.select(['First Name', 'id']),
        'SELECT "First Name", id FROM p',
      );
    });

    test('filter on a numeric comparison', () async {
      await same(p.filter('age > 28'), 'SELECT * FROM p WHERE age > 28');
    });

    test('filter on a single-quoted string literal', () async {
      await same(
        p.filter("dept = 'eng'"),
        "SELECT * FROM p WHERE dept = 'eng'",
      );
    });

    test('filter drops rows where the predicate is NULL', () async {
      await same(
        p.filter('salary >= 80'),
        'SELECT * FROM p WHERE salary >= 80',
      );
    });

    test('limit keeps the first rows', () async {
      await same(p.limit(3), 'SELECT * FROM p LIMIT 3', ordered: true);
    });

    test('limit with an offset after an order', () async {
      await same(
        p.orderBy('id').limit(2, offset: 1),
        'SELECT * FROM p ORDER BY id LIMIT 2 OFFSET 1',
        ordered: true,
      );
    });

    test('limit past the end returns what remains', () async {
      await same(
        p.orderBy('id').limit(10, offset: 4),
        'SELECT * FROM p ORDER BY id LIMIT 10 OFFSET 4',
        ordered: true,
      );
    });

    test('orderBy ascending puts NULLs last', () async {
      await same(
        p.orderBy('age'),
        'SELECT * FROM p ORDER BY age',
        ordered: true,
      );
    });

    test('orderBy descending puts NULLs last', () async {
      await same(
        p.orderBy('age', ascending: false),
        'SELECT * FROM p ORDER BY age DESC',
        ordered: true,
      );
    });

    test('orderBy ascending on a double column puts NULLs last', () async {
      await same(
        p.orderBy('salary'),
        'SELECT * FROM p ORDER BY salary NULLS LAST',
        ordered: true,
      );
    });

    test('orderBy on a spaced name', () async {
      await same(
        p.orderBy('First Name', ascending: false).select(['First Name']),
        'SELECT "First Name" FROM p ORDER BY "First Name" DESC',
        ordered: true,
      );
    });

    test('drop removes the named columns', () async {
      await same(
        p.drop(['salary', 'First Name']),
        'SELECT id, name, dept, age FROM p',
      );
    });

    test('drop ignores an unknown name', () async {
      await same(
        p.drop(['missing', 'age']),
        'SELECT id, name, "First Name", '
        'dept, salary FROM p',
      );
    });

    test('rename renames in place', () async {
      await same(
        p.rename({'age': 'years', 'First Name': 'forename'}),
        'SELECT id, name, "First Name" AS forename, dept, salary, age AS years '
        'FROM p',
      );
    });

    test('rename ignores an unknown name', () async {
      await same(p.rename({'missing': 'x'}), 'SELECT * FROM p');
    });

    test('withColumn appends a new column', () async {
      await same(
        p.withColumn('bonus', 'salary * 2'),
        'SELECT id, name, "First Name", dept, salary, age, salary * 2 AS bonus '
        'FROM p',
      );
    });

    test('withColumn replaces a column in place', () async {
      await same(
        p.withColumn('age', 'age + 1'),
        'SELECT id, name, "First Name", dept, salary, age + 1 AS age FROM p',
      );
    });

    test('withColumn then select sees the new column', () async {
      await same(
        p.withColumn('twice', 'id * 2').select(['twice']),
        'SELECT id * 2 AS twice FROM p',
      );
    });

    test('distinct removes duplicate rows', () async {
      await same(
        p.select(['name', 'dept']).distinct(),
        'SELECT DISTINCT name, dept FROM p',
      );
    });

    test('distinct keeps one NULL', () async {
      await same(p.select(['dept']).distinct(), 'SELECT DISTINCT dept FROM p');
    });
  });

  group('parity expressions', () {
    test('a non-boolean filter casts to BOOLEAN, as DuckDB', () async {
      // A nonzero number keeps the row; NULL and 0 drop it.
      await same(p.filter('age'), 'SELECT * FROM p WHERE age');
      match(
        await engine.execute('SELECT id FROM p WHERE age').ok,
        await engine.execute('SELECT id FROM p WHERE age <> 0').ok,
      );
      expect(
        (await engine.execute('SELECT id FROM p WHERE 1').ok).rowCount,
        (await engine.execute('SELECT id FROM p').ok).rowCount,
      );
    });

    test('filter on a quoted spaced name', () async {
      await same(
        p.filter('"First Name" = \'Alice\''),
        'SELECT * FROM p WHERE "First Name" = \'Alice\'',
      );
    });

    test('filter accepts == as =', () async {
      await same(
        p.filter('"First Name" == \'Bob\''),
        'SELECT * FROM p WHERE "First Name" = \'Bob\'',
      );
    });

    test('SQL accepts == as =', () async {
      match(
        await engine.execute("SELECT id FROM p WHERE dept == 'ops'").ok,
        await engine.execute("SELECT id FROM p WHERE dept = 'ops'").ok,
      );
    });

    test('filter reads a double-quoted name as an identifier', () async {
      await same(
        p.filter('"dept" = \'eng\''),
        'SELECT * FROM p WHERE dept = \'eng\'',
      );
    });

    test('filter with parentheses, IS NULL and IN', () async {
      const predicate = "(age > 30 OR age IS NULL) AND dept IN ('eng', 'ops')";
      await same(p.filter(predicate), 'SELECT * FROM p WHERE $predicate');
    });

    test('filter with LIKE and NOT', () async {
      const predicate = "name LIKE '%o%' AND NOT salary > 82";
      await same(p.filter(predicate), 'SELECT * FROM p WHERE $predicate');
    });

    test('filter with IS NOT NULL', () async {
      await same(
        p.filter('salary IS NOT NULL'),
        'SELECT * FROM p WHERE salary IS NOT NULL',
      );
    });

    test('filter compares two columns', () async {
      await same(
        p.filter('salary > age * 2'),
        'SELECT * FROM p WHERE salary > age * 2',
      );
    });

    test('filter with the literal on the left', () async {
      await same(p.filter('30 < age'), 'SELECT * FROM p WHERE 30 < age');
    });

    test('filter calls a function', () async {
      await same(
        p.filter("UPPER(name) = 'BOB'"),
        "SELECT * FROM p WHERE UPPER(name) = 'BOB'",
      );
    });

    test('filter reads a string literal against a timestamp', () async {
      await same(
        DataFrame.fromTable(events).filter("seen > '2024-06-01'"),
        "SELECT * FROM e WHERE seen > '2024-06-01'",
      );
    });

    test('withColumn uses standard arithmetic precedence', () async {
      await same(
        p.withColumn('x', 'age + 10 * 2').select(['id', 'x']),
        'SELECT id, age + 10 * 2 AS x FROM p',
      );
    });

    test('withColumn with parentheses', () async {
      await same(
        p.withColumn('x', '(age + 10) * 2').select(['id', 'x']),
        'SELECT id, (age + 10) * 2 AS x FROM p',
      );
    });

    test('withColumn on a quoted spaced name', () async {
      await same(
        p.withColumn('forename', 'LOWER("First Name")').select([
          'id',
          'forename',
        ]),
        'SELECT id, LOWER("First Name") AS forename FROM p',
      );
    });

    test('withColumn with CASE', () async {
      const expression = "CASE WHEN age > 30 THEN 'senior' ELSE 'junior' END";
      await same(
        p.withColumn('band', expression).select(['id', 'band']),
        'SELECT id, $expression AS band FROM p',
      );
    });

    test('withColumn with CAST', () async {
      await same(
        p.withColumn('years', 'CAST(age AS DOUBLE)').select(['id', 'years']),
        'SELECT id, CAST(age AS DOUBLE) AS years FROM p',
      );
    });

    test('withColumn adds two columns', () async {
      await same(
        p.withColumn('x', 'salary + age').select(['id', 'x']),
        'SELECT id, salary + age AS x FROM p',
      );
    });

    test('a malformed predicate fails with Kind.invalid', () async {
      expect(p.filter('age >').execute(), failsWith(Kind.invalid));
    });

    test('a malformed expression fails with Kind.invalid', () async {
      expect(p.withColumn('x', 'age +').execute(), failsWith(Kind.invalid));
    });
  });

  group('parity agg/join/union', () {
    test('agg without groupBy: count and sum', () async {
      await same(
        p.agg({'salary': 'count', 'age': 'sum'}),
        'SELECT COUNT(salary) AS salary, SUM(age) AS age FROM p',
      );
    });

    test('agg without groupBy: avg, min and max', () async {
      await same(
        p.agg({'salary': 'avg', 'age': 'min', 'name': 'max'}),
        'SELECT AVG(salary) AS salary, MIN(age) AS age, MAX(name) AS name '
        'FROM p',
      );
    });

    test('agg mean is avg', () async {
      await same(p.agg({'age': 'mean'}), 'SELECT AVG(age) AS age FROM p');
    });

    test('groupBy and agg: count and sum per group', () async {
      await same(
        p.groupBy(['dept']).agg({'id': 'count', 'salary': 'sum'}),
        'SELECT dept, COUNT(id) AS id, SUM(salary) AS salary FROM p '
        'GROUP BY dept',
      );
    });

    test('groupBy and agg: avg, min and max per group', () async {
      await same(
        p.groupBy(['dept']).agg({'salary': 'avg', 'age': 'min', 'id': 'max'}),
        'SELECT dept, AVG(salary) AS salary, MIN(age) AS age, MAX(id) AS id '
        'FROM p GROUP BY dept',
      );
    });

    test('groupBy on two keys', () async {
      await same(
        p.groupBy(['dept', 'name']).agg({'id': 'count'}),
        'SELECT dept, name, COUNT(id) AS id FROM p GROUP BY dept, name',
      );
    });

    test('groupBy on a spaced name', () async {
      await same(
        p.groupBy(['First Name']).agg({'age': 'max'}),
        'SELECT "First Name", MAX(age) AS age FROM p GROUP BY "First Name"',
      );
    });

    test('groupBy followed by something other than agg is invalid', () async {
      expect(
        p.groupBy(['dept']).select(['dept']).execute(),
        failsWith(Kind.invalid),
      );
    });

    test('inner join qualifies colliding names by side', () async {
      await same(p.join(d, 'dept'), 'SELECT * FROM p JOIN d USING (dept)');
    });

    test('left join qualifies colliding names by side', () async {
      await same(
        p.join(d, 'dept', type: JoinType.left),
        'SELECT * FROM p LEFT JOIN d USING (dept)',
      );
    });

    test('right join qualifies colliding names by side', () async {
      await same(
        p.join(d, 'dept', type: JoinType.right),
        'SELECT * FROM p RIGHT JOIN d USING (dept)',
      );
    });

    test('full join qualifies colliding names by side', () async {
      await same(
        p.join(d, 'dept', type: JoinType.full),
        'SELECT * FROM p FULL JOIN d USING (dept)',
      );
    });

    test('sides of one table name are qualified left and right', () async {
      final a = DataFrame.fromRows([
        {'k': 1, 'v': 'a'},
      ]);
      final b = DataFrame.fromRows([
        {'k': 1, 'v': 'b'},
      ]);
      final joined = a.join(b, 'k');
      final result = await joined.execute().ok;
      expect(result.schema.fieldNames, ['k', 'left.v', 'right.v']);
      expect(joined.columns, result.schema.fieldNames);
      expect(lines(result), ['1 | a | b']);
      expect((await p.join(p, 'id').execute().ok).schema.fieldNames.take(3), [
        'id',
        'left.name',
        'left.First Name',
      ]);
    });

    test('cross join ignores on', () async {
      await same(
        p.join(d, 'dept', type: JoinType.cross),
        'SELECT * FROM p CROSS JOIN d',
      );
    });

    test('inner join without a name collision coalesces the key', () async {
      await same(
        p.join(d.drop(['name']), 'dept'),
        'SELECT * FROM p JOIN (SELECT dept, floor FROM d) AS d USING (dept)',
      );
    });

    test('right join without a name collision keeps the right key', () async {
      await same(
        p.join(d.drop(['name']), 'dept', type: JoinType.right),
        'SELECT * FROM p RIGHT JOIN (SELECT dept, floor FROM d) AS d '
        'USING (dept)',
      );
    });

    test('full join without a name collision coalesces the key', () async {
      await same(
        p.join(d.drop(['name']), 'dept', type: JoinType.full),
        'SELECT * FROM p FULL JOIN (SELECT dept, floor FROM d) AS d '
        'USING (dept)',
      );
    });

    test('union keeps duplicates (UNION ALL)', () async {
      await same(
        p.select(['dept']).union(d.select(['dept'])),
        'SELECT dept FROM p UNION ALL SELECT dept FROM d',
      );
    });

    test('union takes the left names and the common type', () async {
      await same(
        p.select(['salary']).union(d.select(['floor'])),
        'SELECT salary FROM p UNION ALL SELECT floor FROM d',
      );
    });

    test('union with a different column count fails with Kind.schema', () {
      expect(
        p.select(['id', 'name']).union(d.select(['floor'])).execute(),
        failsWith(Kind.schema),
      );
    });

    test('the schema of agg, join and union is the result\'s', () async {
      String shape(Schema schema) => [
        for (final f in schema.fields) '${f.name}: ${f.dataType}',
      ].join(', ');
      for (final frame in [
        p.groupBy(['dept']).agg({'id': 'sum', 'salary': 'avg', 'age': 'count'}),
        p.agg({'age': 'sum'}),
        p.join(d, 'dept', type: JoinType.right),
        p.join(d, 'dept', type: JoinType.cross),
        d.select(['floor']).union(p.select(['salary'])),
      ]) {
        final result = await frame.execute().ok;
        expect(shape(frame.schema!), shape(result.schema));
      }
    });

    test('SQL UNION with a different column count fails with Kind.schema', () {
      expect(
        engine.execute('SELECT id, name FROM p UNION ALL SELECT floor FROM d'),
        failsWith(Kind.schema),
      );
    });
  });

  // A joined frame reports a colliding column as `p.name` and `d.name`, and
  // every name-taking operation takes those names and keeps them, where SQL
  // names a selected `p.name` just `name`: these compare values, and check
  // the frame's names against its own `columns`.
  group('parity qualified names', () {
    /// `f`: floors, a third side that also has `name`.
    final floors = Table.fromRows(
      'f',
      [
        {'dept': 'eng', 'name': 'Third'},
        {'dept': 'ops', 'name': 'First'},
      ],
      schema: Schema(
        fields: [
          Field(name: 'dept', dataType: DataType.string),
          Field(name: 'name', dataType: DataType.string),
        ],
      ),
    );
    setUpAll(() => engine.registerTable('f', floors));

    final joined = p.join(d, 'dept');

    /// Run [frame] and [sql], expect the same rows (see [match]) and
    /// [names] for the frame's result, its `columns` included.
    Future<void> values(
      DataFrame frame,
      String sql,
      List<String> names, {
      bool ordered = false,
    }) async {
      final actual = await frame.execute().ok;
      final expected = await engine.execute(sql).ok;
      expect(actual.schema.fieldNames, names, reason: 'column names');
      expect(frame.columns, names, reason: 'reported columns');
      final have = lines(actual);
      final want = lines(expected);
      if (!ordered) {
        have.sort();
        want.sort();
      }
      expect(have, want, reason: 'rows');
    }

    test('select takes qualified names and keeps them', () async {
      await values(
        joined.select(['d.name', 'p.name']),
        'SELECT d.name, p.name FROM p JOIN d USING (dept)',
        ['d.name', 'p.name'],
      );
    });

    test('select takes every name the frame reports', () async {
      final all = joined.select(joined.columns!);
      await same(all, 'SELECT * FROM p JOIN d USING (dept)');
      expect(all.columns, joined.columns);
      expect(joined[['p.name']].columns, ['p.name']);
    });

    test('orderBy takes a qualified name', () async {
      await values(
        joined.orderBy('p.name', ascending: false).select(['p.name']),
        'SELECT p.name FROM p JOIN d USING (dept) ORDER BY p.name DESC',
        ['p.name'],
        ordered: true,
      );
    });

    test('drop takes a qualified name and keeps the others', () async {
      await values(
        joined.drop(['d.name', 'First Name']),
        'SELECT id, p.name, dept, salary, age, floor '
        'FROM p JOIN d USING (dept)',
        ['id', 'p.name', 'dept', 'salary', 'age', 'floor'],
      );
    });

    test('rename takes a qualified name', () async {
      await values(
        joined.rename({'p.name': 'person'}).select(['person', 'd.name']),
        'SELECT p.name, d.name FROM p JOIN d USING (dept)',
        ['person', 'd.name'],
      );
    });

    test('withColumn keeps the qualified names', () async {
      await values(
        joined.withColumn('up', 'floor + 1').select(['p.name', 'up']),
        'SELECT p.name, floor + 1 FROM p JOIN d USING (dept)',
        ['p.name', 'up'],
      );
    });

    test('groupBy and agg take qualified names', () async {
      await values(
        joined.groupBy(['d.name']).agg({'p.name': 'count'}),
        'SELECT d.name, COUNT(p.name) FROM p JOIN d USING (dept) '
        'GROUP BY d.name',
        ['d.name', 'p.name'],
      );
    });

    test('a join after a join takes its key', () async {
      await values(
        joined.join(DataFrame.fromTable(events), 'id').select(['id', 'd.name']),
        'SELECT id, d.name FROM p JOIN d USING (dept) JOIN e USING (id)',
        ['id', 'd.name'],
      );
    });

    test('a bare name both sides have is ambiguous', () {
      final choices = contains('Ambiguous column "name": use p.name or d.name');
      expect(
        joined.select(['name']).execute(),
        failsWith(Kind.invalid, choices),
      );
      expect(
        joined.orderBy('name').execute(),
        failsWith(Kind.invalid, choices),
      );
      expect(joined.drop(['name']).execute(), failsWith(Kind.invalid, choices));
      expect(
        joined.groupBy(['name']).agg({'id': 'count'}).execute(),
        failsWith(Kind.invalid, choices),
      );
    });

    test('a three-way join qualifies every side', () async {
      final three = joined.join(DataFrame.fromTable(floors), 'dept');
      await same(
        three,
        'SELECT * FROM p JOIN d USING (dept) JOIN f USING (dept)',
      );
      expect(three.columns, (await three.execute().ok).schema.fieldNames);
      expect(three.columns!.where((c) => c.endsWith('name')), [
        'p.name',
        'd.name',
        'f.name',
      ]);
      await values(
        three.select(['f.name', 'p.name']),
        'SELECT f.name, p.name FROM p JOIN d USING (dept) JOIN f USING (dept)',
        ['f.name', 'p.name'],
      );
    });

    test('the schema of these frames is the result\'s', () async {
      String shape(Schema schema) => [
        for (final f in schema.fields) '${f.name}: ${f.dataType}',
      ].join(', ');
      for (final frame in [
        joined.select(['p.name', 'floor']),
        joined.drop(['p.name']),
        joined.rename({'d.name': 'department'}),
        joined.withColumn('up', 'floor + 1'),
        joined.groupBy(['d.name']).agg({'salary': 'sum'}),
        joined.join(DataFrame.fromTable(floors), 'dept'),
      ]) {
        final result = await frame.execute().ok;
        expect(shape(frame.schema!), shape(result.schema));
      }
    });
  });

  group('parity pipelines', () {
    test('filter, select, groupBy and agg, orderBy, limit', () async {
      await same(
        p
            .filter('age > 20')
            .select(['dept', 'salary'])
            .groupBy(['dept'])
            .agg({'salary': 'sum'})
            .orderBy('salary', ascending: false)
            .limit(2),
        'SELECT * FROM (SELECT dept, SUM(salary) AS salary FROM p '
        'WHERE age > 20 GROUP BY dept) AS s ORDER BY salary DESC LIMIT 2',
        ordered: true,
      );
    });

    test('join, filter, select, orderBy', () async {
      await same(
        p
            .join(d.drop(['name']), 'dept')
            .filter('floor > 1')
            .select(['id', 'floor'])
            .orderBy('id'),
        'SELECT id, floor FROM p JOIN (SELECT dept, floor FROM d) AS d '
        'USING (dept) WHERE floor > 1 ORDER BY id',
        ordered: true,
      );
    });

    test('withColumn, filter on it, orderBy descending', () async {
      await same(
        p
            .withColumn('total', 'salary + age')
            .filter('total > 110')
            .select(['id', 'total'])
            .orderBy('total', ascending: false),
        'SELECT id, salary + age AS total FROM p WHERE salary + age > 110 '
        'ORDER BY total DESC',
        ordered: true,
      );
    });

    test('rename, filter on the new name, select', () async {
      await same(
        p.rename({'age': 'years'}).filter('years < 35').select(['id', 'years']),
        'SELECT id, age AS years FROM p WHERE age < 35',
      );
    });

    test('distinct, orderBy with NULLs, limit', () async {
      await same(
        p.select(['dept']).distinct().orderBy('dept').limit(3),
        'SELECT DISTINCT dept FROM p ORDER BY dept LIMIT 3',
        ordered: true,
      );
    });

    test('union, groupBy and agg', () async {
      await same(
        p
            .select(['dept', 'id'])
            .union(d.select(['dept', 'floor']))
            .groupBy(['dept'])
            .agg({'id': 'count'}),
        'SELECT dept, COUNT(id) AS id FROM (SELECT dept, id FROM p '
        'UNION ALL SELECT dept, floor FROM d) AS u GROUP BY dept',
      );
    });

    test('a quoted filter, withColumn with precedence, orderBy', () async {
      await same(
        p
            .filter('"First Name" <> \'Dan\'')
            .withColumn('x', 'age + 10 * 2')
            .select(['id', 'x'])
            .orderBy('x'),
        'SELECT id, age + 10 * 2 AS x FROM p WHERE "First Name" <> \'Dan\' '
        'ORDER BY x',
        ordered: true,
      );
    });
  });

  group('parity sample', () {
    // SQL has no sample, so these pin today's exact seeded rows.
    List<Object?> ids(Table table) => table.column('id')!.toList();

    test('a seeded sample keeps round(n * fraction) pinned rows', () async {
      expect(ids(await p.sample(0.5, seed: 7).execute().ok), [5, 4, 1]);
    });

    test('the same seed gives the same rows', () async {
      final first = await p.sample(0.5, seed: 11).execute().ok;
      final second = await p.sample(0.5, seed: 11).execute().ok;
      expect(lines(first), lines(second));
    });

    test('a sample keeps the input columns and types', () async {
      match(
        await p.sample(1.0, seed: 3).execute().ok,
        await engine.execute('SELECT * FROM p').ok,
      );
    });

    test('a sample of zero is empty with the input schema', () async {
      match(
        await p.sample(0.0, seed: 3).execute().ok,
        await engine.execute('SELECT * FROM p WHERE id < 0').ok,
      );
    });

    test('a sample after a filter samples the filtered rows', () async {
      final sample = await p
          .filter('salary > 50')
          .sample(0.4, seed: 3)
          .execute()
          .ok;
      expect(ids(sample), [2, 6]);
      final filtered = lines(
        await engine.execute('SELECT * FROM p WHERE salary > 50').ok,
      );
      expect(filtered, containsAll(lines(sample)));
    });

    test('a select after a sample', () async {
      expect(
        lines(await p.sample(0.5, seed: 7).select(['name']).execute().ok),
        ['Eve', 'Dan', 'Alice'],
      );
    });

    test('a fraction outside [0, 1] fails with Kind.invalid', () {
      expect(p.sample(1.5).execute(), failsWith(Kind.invalid));
    });
  });

  group('parity file-backed', () {
    late Directory directory;
    late String left;
    late String right;
    late String wide;
    setUpAll(() async {
      directory = Directory.systemTemp.createTempSync('parity_');
      left = '${directory.path}/p.parquet';
      right = '${directory.path}/d.parquet';
      wide = '${directory.path}/wide.parquet';
      await ParquetWriter(left).write(people).ok;
      await ParquetWriter(right).write(depts).ok;
      // 1000 rows in 10 row groups; `id` ascends, so `id >= 900` prunes 9.
      await ParquetWriter(
            wide,
            options: const ParquetWriterOptions(rowGroupSize: 100),
          )
          .write(
            Table.fromRows(
              'wide',
              [
                for (var i = 0; i < 1000; i++)
                  {'id': i, 'group': 'g${i % 7}', 'value': i * 0.5},
              ],
              schema: Schema(
                fields: [
                  Field(name: 'id', dataType: DataType.int64, nullable: false),
                  Field(name: 'group', dataType: DataType.string),
                  Field(name: 'value', dataType: DataType.float64),
                ],
              ),
            ),
          )
          .ok;
    });
    tearDownAll(() => directory.deleteSync(recursive: true));

    // Both lazy roots: the Parquet reader and a scan over the file's schema.
    final roots = <String, Future<DataFrame> Function(String)>{
      'readParquet': (path) => DataFrameIO.readParquet(path).ok,
      'scan': (path) async => DataFrame.scan(
        path,
        (await DataFrameIO.readParquet(path).ok).schema!,
      ),
    };

    for (final MapEntry(key: root, value: open) in roots.entries) {
      test('$root: select', () async {
        await same(
          (await open(left)).select(['name', 'age']),
          "SELECT name, age FROM '$left'",
        );
      });

      test('$root: filter', () async {
        await same(
          (await open(left)).filter('age >= 30'),
          "SELECT * FROM '$left' WHERE age >= 30",
        );
      });

      test('$root: filter on a quoted spaced name', () async {
        await same(
          (await open(left)).filter('"First Name" = \'Eve\''),
          "SELECT * FROM '$left' WHERE \"First Name\" = 'Eve'",
        );
      });

      test('$root: orderBy puts NULLs last', () async {
        await same(
          (await open(left)).orderBy('salary'),
          "SELECT * FROM '$left' ORDER BY salary",
          ordered: true,
        );
      });

      test('$root: orderBy descending and limit', () async {
        await same(
          (await open(wide)).orderBy('value', ascending: false).limit(5),
          "SELECT * FROM '$wide' ORDER BY value DESC LIMIT 5",
          ordered: true,
        );
      });

      test('$root: groupBy and agg', () async {
        await same(
          (await open(
            wide,
          )).groupBy(['group']).agg({'value': 'sum', 'id': 'count'}),
          'SELECT "group", SUM(value) AS value, COUNT(id) AS id '
          "FROM '$wide' GROUP BY \"group\"",
        );
      });

      test('$root: join two files', () async {
        await same(
          (await open(left)).join((await open(right)).drop(['name']), 'dept'),
          "SELECT * FROM '$left' AS p JOIN (SELECT dept, floor FROM '$right') "
          'AS d USING (dept)',
        );
      });

      test('$root: withColumn with precedence', () async {
        await same(
          (await open(
            left,
          )).withColumn('x', 'age + 10 * 2').select(['id', 'x']),
          "SELECT id, age + 10 * 2 AS x FROM '$left'",
        );
      });

      test('$root: filter, select, groupBy and agg, orderBy, limit', () async {
        await same(
          (await open(wide))
              .filter('id >= 300')
              .select(['group', 'value'])
              .groupBy(['group'])
              .agg({'value': 'max'})
              .orderBy('value')
              .limit(3),
          'SELECT * FROM (SELECT "group", MAX(value) AS value '
          "FROM '$wide' WHERE id >= 300 GROUP BY \"group\") AS s "
          'ORDER BY value LIMIT 3',
          ordered: true,
        );
      });

      test('$root: filter and select prune as SQL does', () async {
        ParquetReader.rowGroupsPruned = 0;
        ParquetReader.columnsRead = 0;
        final expected = await engine
            .execute("SELECT id FROM '$wide' WHERE id >= 900")
            .ok;
        final pruned = ParquetReader.rowGroupsPruned;
        final read = ParquetReader.columnsRead;
        expect(pruned, 9, reason: 'SQL prunes 9 of 10 row groups');

        ParquetReader.rowGroupsPruned = 0;
        ParquetReader.columnsRead = 0;
        final actual = await (await open(
          wide,
        )).filter('id >= 900').select(['id']).execute().ok;
        match(actual, expected);
        expect(ParquetReader.rowGroupsPruned, pruned, reason: 'row groups');
        expect(ParquetReader.columnsRead, read, reason: 'columns read');
      });

      test(
        '$root: a filter after a limit sees only the limited rows',
        () async {
          final frame = (await open(wide)).limit(100).filter('id >= 900');
          expect(await frame.count().ok, 0);
        },
      );

      test('$root: a filter after a sample sees every sampled row', () async {
        // Pruning `id >= 900` below the sample would sample only the last
        // row group, so the sample would hold different rows.
        final table = await (await open(wide)).execute().ok;
        Future<List<Object?>> ids(DataFrame frame) async =>
            (await frame.sample(0.1, seed: 5).filter('id >= 900').execute().ok)
                .column('id')!
                .toList();
        expect(
          await ids(await open(wide)),
          await ids(DataFrame.fromTable(table)),
        );
      });

      test('$root: a filter on a computed column does not prune', () async {
        final frame = (await open(
          wide,
        )).withColumn('id', 'id + 900').filter('id >= 900');
        expect(await frame.count().ok, 1000);
      });

      test('$root: a select after distinct keeps every column', () async {
        final frame = (await open(wide)).distinct().select(['group']);
        expect(await frame.count().ok, 1000);
      });

      test(
        '$root: a filter on an aggregated column does not prune the raw one',
        () async {
          // Each group's least id is below 7; pruning `id >= 900` on the
          // file's id would leave only ids from 900.
          final frame = (await open(
            wide,
          )).groupBy(['group']).agg({'id': 'min'}).filter('id >= 900');
          expect(await frame.count().ok, 0);
        },
      );

      test('$root: a filter on a group key prunes as SQL does', () async {
        ParquetReader.rowGroupsPruned = 0;
        final expected = await engine
            .execute(
              "SELECT id, SUM(value) AS value FROM '$wide' GROUP BY id "
              'HAVING id >= 900',
            )
            .ok;
        final pruned = ParquetReader.rowGroupsPruned;
        expect(pruned, 9, reason: 'SQL prunes 9 of 10 row groups');

        ParquetReader.rowGroupsPruned = 0;
        final actual = await (await open(wide))
            .groupBy(['id'])
            .agg({'value': 'sum'})
            .filter('id >= 900')
            .execute()
            .ok;
        match(actual, expected);
        expect(ParquetReader.rowGroupsPruned, pruned, reason: 'row groups');
      });

      test('$root: a filter over a union does not prune a branch', () async {
        // The right branch's `value` position holds its id, so pruning it on
        // its own `value` would drop ids from 450 to 899.
        final union = (await open(wide))
            .withColumn('a', 'id + 0')
            .select(['a', 'value'])
            .union((await open(wide)).select(['value', 'id']));
        expect(await union.filter('value >= 450').count().ok, 650);
      });

      test(
        '$root: a filter does not prune a join side on a column it drops',
        () async {
          // `value` is the left side's id * 2; the right side's own `value`
          // (at most 499.5) is dropped, so it must not prune that side.
          final joined = (await open(wide))
              .withColumn('value', 'id * 2')
              .join((await open(wide)).drop(['value']), 'id');
          expect(await joined.filter('value >= 1800').count().ok, 100);
        },
      );
    }
  });

  group('parity readers', () {
    late Directory directory;
    String at(String name) => '${directory.path}/$name';

    /// 1000 rows from [from], `id` ascending, written in 100-row groups.
    Table wide(int from, int count) => Table.fromRows(
      'wide',
      [
        for (var i = from; i < from + count; i++)
          {'id': i, 'group': 'g${i % 7}', 'value': i * 0.5},
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'group', dataType: DataType.string),
          Field(name: 'value', dataType: DataType.float64),
        ],
      ),
    );

    /// The id of each snapshot of the Iceberg table [path], oldest first.
    List<int> snapshots(String path) {
      final latest = Directory('$path/metadata')
          .listSync()
          .whereType<File>()
          .map((file) => file.path)
          .where((name) => name.endsWith('.metadata.json'))
          .reduce((a, b) => a.compareTo(b) > 0 ? a : b);
      final metadata = jsonDecode(File(latest).readAsStringSync()) as Map;
      return [
        for (final snapshot in metadata['snapshots'] as List)
          snapshot['snapshot-id'] as int,
      ];
    }

    setUpAll(() async {
      directory = Directory.systemTemp.createTempSync('parity_readers_');
      final frame = DataFrame.fromTable(people);
      await frame.writeCsv(at('p.csv')).ok;
      File(
        at('semi.csv'),
      ).writeAsStringSync('1;Alice;30\n2;Bob;\n3;"Carol;C";41\n');
      await frame.writeJson(at('p.json')).ok;
      await frame.writeJsonLines(at('p.jsonl')).ok;
      await frame.writeArrow(at('p.arrow')).ok;
      await ParquetWriter(at('p.parquet')).write(people).ok;
      const groups = ParquetWriterOptions(rowGroupSize: 100);
      await ParquetWriter(
        at('wide.parquet'),
        options: groups,
      ).write(wide(0, 1000)).ok;
      // Ten commits of 100 rows each: a filter on `id` skips files by stats.
      for (var i = 0; i < 10; i++) {
        await DeltaWriter(at('delta')).write(wide(i * 100, 100)).ok;
        await IcebergWriter(at('iceberg')).write(wide(i * 100, 100)).ok;
      }
      for (final (dept, from) in [('a', 0), ('b', 500)]) {
        Directory(at('set/dept=$dept')).createSync(recursive: true);
        await ParquetWriter(
          at('set/dept=$dept/part.parquet'),
          options: groups,
        ).write(wide(from, 500)).ok;
      }
    });
    tearDownAll(() => directory.deleteSync(recursive: true));

    /// Expect the lazy [read] to root its plan at a file scan and to give
    /// the [eager] reader's result, and SQL's for [sql] when given.
    Future<DataFrame> agree(
      Future<Result<DataFrame, ErrorDetails>> read,
      Future<Table> eager, {
      String? sql,
    }) async {
      final frame = await read.ok;
      expect(await planOf(frame), isA<PhysicalFileScan>(), reason: 'lazy');
      final actual = await frame.execute().ok;
      match(actual, await eager, ordered: true);
      if (sql != null) {
        match(actual, await engine.execute(sql).ok, ordered: true);
      }
      return frame;
    }

    test('readCsv', () async {
      await agree(
        DataFrameIO.readCsv(at('p.csv')),
        CsvReader().readFile(at('p.csv')).ok,
        sql: "SELECT * FROM '${at('p.csv')}'",
      );
    });

    test('readCsv with `;` and no header', () async {
      const options = CsvOptions(delimiter: ';', hasHeader: false);
      final frame = await agree(
        DataFrameIO.readCsv(at('semi.csv'), options: options),
        CsvReader(options: options).readFile(at('semi.csv')).ok,
      );
      expect((await frame.execute().ok).rowCount, 3);
    });

    test('readCsv with filter, columns, limit and batchSize', () async {
      final filter = RowFilter.compare('age', FilterOp.ge, 30);
      await agree(
        DataFrameIO.readCsv(
          at('p.csv'),
          filter: filter,
          columns: ['age', 'name'],
          limit: 2,
          batchSize: 1,
        ),
        CsvReader()
            .readFile(
              at('p.csv'),
              filter: filter,
              columns: ['age', 'name'],
              limit: 2,
            )
            .ok,
      );
    });

    test('readCsv then filter and select', () async {
      final frame = await DataFrameIO.readCsv(at('p.csv')).ok;
      match(
        await frame.filter('age >= 30').select(['name']).execute().ok,
        await engine
            .execute("SELECT name FROM '${at('p.csv')}' WHERE age >= 30")
            .ok,
        ordered: true,
      );
    });

    test('readJson', () async {
      await agree(
        DataFrameIO.readJson(at('p.json')),
        JsonReader().readFile(at('p.json')).ok,
        sql: "SELECT * FROM '${at('p.json')}'",
      );
    });

    test('readJsonLines', () async {
      await agree(
        DataFrameIO.readJsonLines(at('p.jsonl')),
        JsonReader(JsonOptions.jsonLines()).readFile(at('p.jsonl')).ok,
        sql: "SELECT * FROM '${at('p.jsonl')}'",
      );
    });

    test('readJson with options', () async {
      const options = JsonOptions(jsonLines: true, inferTypes: false);
      await agree(
        DataFrameIO.readJson(at('p.jsonl'), options: options),
        JsonReader(options).readFile(at('p.jsonl')).ok,
      );
    });

    test('readArrow', () async {
      await agree(
        DataFrameIO.readArrow(at('p.arrow')),
        ArrowReader(at('p.arrow')).read(),
        sql: "SELECT * FROM '${at('p.arrow')}'",
      );
    });

    test('readArrowStream over an IPC file', () async {
      await agree(
        DataFrameIO.readArrowStream(at('p.arrow')),
        ArrowReader(at('p.arrow')).readStream(),
      );
    });

    test('readParquet with filter and columns', () async {
      final filter = RowFilter.compare('id', FilterOp.ge, 900);
      ParquetReader.rowGroupsPruned = 0;
      ParquetReader.columnsRead = 0;
      final frame = await agree(
        DataFrameIO.readParquet(
          at('wide.parquet'),
          filter: filter,
          columns: ['id', 'value'],
        ),
        ParquetReader(
          at('wide.parquet'),
          options: ParquetReaderOptions(
            filter: filter,
            columns: ['id', 'value'],
          ),
        ).read().ok,
      );
      // The filter prunes as a hint: the kept row group comes back whole.
      expect(ParquetReader.rowGroupsPruned, 18, reason: 'both reads prune 9');
      expect(ParquetReader.columnsRead, 4, reason: 'both read 2 columns');
      ParquetReader.columnsRead = 0;
      final rows = await frame.filter('id >= 950').select(['id']).count().ok;
      expect(rows, 50);
      expect(ParquetReader.columnsRead, 1, reason: 'a later select narrows');
    });

    test('readDelta, and a filter skips files as SQL does', () async {
      await agree(
        DataFrameIO.readDelta(at('delta')),
        DeltaTable(at('delta')).read().ok,
        sql: "SELECT * FROM '${at('delta')}'",
      );
      ParquetReader.columnsRead = 0;
      final expected = await engine
          .execute("SELECT id FROM '${at('delta')}' WHERE id >= 900")
          .ok;
      final read = ParquetReader.columnsRead;
      expect(read, 1, reason: 'SQL reads one column of one file');
      ParquetReader.columnsRead = 0;
      final frame = await DataFrameIO.readDelta(at('delta')).ok;
      match(
        await frame.filter('id >= 900').select(['id']).execute().ok,
        expected,
        ordered: true,
      );
      expect(ParquetReader.columnsRead, read);
    });

    test('readDelta at a version', () async {
      final frame = await agree(
        DataFrameIO.readDelta(at('delta'), version: 1),
        DeltaTable(at('delta')).read(version: 1).ok,
      );
      expect((await frame.execute().ok).rowCount, 200);
    });

    test('readIceberg, and a filter skips files as SQL does', () async {
      await agree(
        DataFrameIO.readIceberg(at('iceberg')),
        IcebergTable(at('iceberg')).read().ok,
        sql: "SELECT * FROM '${at('iceberg')}'",
      );
      ParquetReader.columnsRead = 0;
      IcebergTable.manifestsRead = 0;
      final expected = await engine
          .execute("SELECT id FROM '${at('iceberg')}' WHERE id >= 900")
          .ok;
      final read = ParquetReader.columnsRead;
      final manifests = IcebergTable.manifestsRead;
      expect(read, 1, reason: 'SQL reads one column of one file');
      ParquetReader.columnsRead = 0;
      IcebergTable.manifestsRead = 0;
      final frame = await DataFrameIO.readIceberg(at('iceberg')).ok;
      match(
        await frame.filter('id >= 900').select(['id']).execute().ok,
        expected,
        ordered: true,
      );
      expect(ParquetReader.columnsRead, read);
      expect(IcebergTable.manifestsRead, manifests);
    });

    test('readIceberg at a snapshot', () async {
      final first = snapshots(at('iceberg')).first;
      final frame = await agree(
        DataFrameIO.readIceberg(at('iceberg'), snapshotId: first),
        IcebergTable(at('iceberg')).read(snapshotId: first).ok,
      );
      expect((await frame.execute().ok).rowCount, 100);
    });

    test('readDataset over partitions', () async {
      await agree(
        DataFrameIO.readDataset(at('set')),
        Dataset(at('set')).read().ok,
      );
      final filter = RowFilter.compare('dept', FilterOp.eq, 'b');
      await agree(
        DataFrameIO.readDataset(
          at('set'),
          filter: filter,
          columns: ['id', 'dept'],
        ),
        Dataset(at('set'), filter: filter, columns: ['id', 'dept']).read().ok,
      );
    });

    test('readDataset: a filter prunes partitions and row groups', () async {
      final frame = await DataFrameIO.readDataset(at('set')).ok;
      ParquetReader.columnsRead = 0;
      expect(await frame.select(['id']).count().ok, 1000);
      expect(ParquetReader.columnsRead, 10, reason: 'two files, 5 groups');
      ParquetReader.columnsRead = 0;
      expect(await frame.filter("dept = 'b'").select(['id']).count().ok, 500);
      expect(ParquetReader.columnsRead, 5, reason: 'one partition read');
      ParquetReader.rowGroupsPruned = 0;
      expect(await frame.filter('id >= 900').count().ok, 100);
      expect(ParquetReader.rowGroupsPruned, 9);
    });

    test('errors found by the schema read come from the reader', () async {
      final missing = at('missing');
      for (final (read, kind) in [
        (DataFrameIO.readCsv('$missing.csv'), Kind.missing),
        (DataFrameIO.readJson('$missing.json'), Kind.missing),
        (DataFrameIO.readJsonLines('$missing.jsonl'), Kind.missing),
        (DataFrameIO.readArrow('$missing.arrow'), Kind.missing),
        (DataFrameIO.readParquet('$missing.parquet'), Kind.missing),
        (DataFrameIO.readDelta(missing), Kind.missing),
        (DataFrameIO.readIceberg(missing), Kind.missing),
        (DataFrameIO.readDataset(missing), Kind.missing),
        (DataFrameIO.readDelta(at('set')), Kind.missing),
        (DataFrameIO.readIceberg(at('set')), Kind.missing),
        (DataFrameIO.readDelta(at('delta'), version: 99), Kind.missing),
        (DataFrameIO.readIceberg(at('iceberg'), snapshotId: 1), Kind.missing),
        (DataFrameIO.readCsv(at('p.csv'), columns: ['nope']), Kind.invalid),
        (
          DataFrameIO.readParquet(at('p.parquet'), columns: ['x']),
          Kind.invalid,
        ),
        (DataFrameIO.readDataset(at('set'), columns: ['x']), Kind.invalid),
        (DataFrameIO.readArrow(at('p.csv')), Kind.corrupt),
        (DataFrameIO.readParquet(at('p.csv')), Kind.corrupt),
      ]) {
        await expectLater(read, failsWith(kind));
      }
    });

    test('a reader cap bounds the result of the lazy frame', () async {
      final frame = await DataFrameIO.readCsv(at('p.csv'), maxRows: 3).ok;
      await expectLater(frame.execute(), failsWith(Kind.limit));
      await expectLater(frame.toList(), failsWith(Kind.limit));
      expect((await frame.limit(3).execute().ok).rowCount, 3);
      expect((await frame.filter('id <= 2').execute().ok).rowCount, 2);
      // A terminal that streams sets no result cap, so the reader's applies
      // only where a result is materialized.
      expect(await frame.count().ok, 6);
      // The tighter of the two caps applies.
      await expectLater(
        (await DataFrameIO.readCsv(at('p.csv')).ok).execute(maxRows: 3),
        failsWith(Kind.limit),
      );
      final bytes = await DataFrameIO.readParquet(
        at('wide.parquet'),
        maxBytes: 1000,
      ).ok;
      await expectLater(bytes.execute(), failsWith(Kind.limit));
      // A joined frame's reader cap bounds the join's result too.
      final joined = (await DataFrameIO.readCsv(
        at('p.csv'),
      ).ok).join(frame.select(['id']), 'id');
      await expectLater(joined.execute(), failsWith(Kind.limit));
    });

    test('readDelta reads the version current at the call', () async {
      final path = at('pinned_delta');
      await DeltaWriter(path).write(wide(0, 100)).ok;
      final frame = await DataFrameIO.readDelta(path).ok;
      await DeltaWriter(path).write(wide(100, 50)).ok;
      expect((await frame.execute().ok).rowCount, 100);
      expect(await frame.count().ok, 100);
      final fresh = await DataFrameIO.readDelta(path).ok;
      expect((await fresh.execute().ok).rowCount, 150);
      // An explicit version is read as given.
      final first = await DataFrameIO.readDelta(path, version: 0).ok;
      expect((await first.execute().ok).rowCount, 100);
    });

    test('readIceberg reads the snapshot current at the call', () async {
      final path = at('pinned_iceberg');
      await IcebergWriter(path).write(wide(0, 100)).ok;
      final frame = await DataFrameIO.readIceberg(path).ok;
      await IcebergWriter(path).write(wide(100, 50)).ok;
      expect((await frame.execute().ok).rowCount, 100);
      expect(await frame.count().ok, 100);
      final fresh = await DataFrameIO.readIceberg(path).ok;
      expect((await fresh.execute().ok).rowCount, 150);
      final first = snapshots(path).first;
      final old = await DataFrameIO.readIceberg(path, snapshotId: first).ok;
      expect((await old.execute().ok).rowCount, 100);
    });

    test('a glob dataset is named dataset in join qualifiers', () async {
      final frame = await DataFrameIO.readDataset(at('set/*/*.parquet')).ok;
      final joined = frame.join(DataFrame.fromTable(wide(0, 3)), 'id');
      expect(joined.columns, contains('dataset.value'));
      final table = await joined.execute().ok;
      expect(table.schema.fieldNames, contains('dataset.value'));
    });

    test('reads the scan cannot stream are read when called', () async {
      File(at('crlf.csv')).writeAsStringSync('a;b|1;2|3;4');
      const options = CsvOptions(delimiter: ';', lineTerminator: '|');
      final frame = await DataFrameIO.readCsv(
        at('crlf.csv'),
        options: options,
      ).ok;
      expect(await planOf(frame), isNot(isA<PhysicalFileScan>()));
      match(
        await frame.execute().ok,
        await CsvReader(options: options).readFile(at('crlf.csv')).ok,
        ordered: true,
      );
      final json = await DataFrameIO.readJson(
        at('p.json'),
        maxRows: null,
        maxBytes: null,
      ).ok;
      expect(await planOf(json), isNot(isA<PhysicalFileScan>()));
      final delta = await DataFrameIO.readDelta(at('delta'), concurrency: 2).ok;
      expect(await planOf(delta), isNot(isA<PhysicalFileScan>()));
      match(
        await delta.execute().ok,
        await DeltaTable(at('delta')).read().ok,
        ordered: true,
      );
    });
  });
}
