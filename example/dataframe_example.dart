// The DataFrame API end to end: build, transform, aggregate, join, and write.
//
// Run with: dart run example/dataframe_example.dart
//
// See doc/DATAFRAME.md for the full reference, including the exact grammar
// accepted by filter() predicates and withColumn() expressions.
import 'dart:io';

import 'package:libbigdata/libbigdata.dart';

Future<void> main() async {
  final employees = DataFrame.fromRows([
    {'id': 1, 'name': 'Alice', 'dept': 'eng', 'salary': 100, 'age': 30},
    {'id': 2, 'name': 'Bob', 'dept': 'sales', 'salary': 70, 'age': 25},
    {'id': 3, 'name': 'Carol', 'dept': 'eng', 'salary': 120, 'age': 41},
    {'id': 4, 'name': 'Dan', 'dept': 'sales', 'salary': 60, 'age': 38},
    {'id': 5, 'name': 'Erin', 'dept': 'eng', 'salary': 90, 'age': 29},
  ]);

  print('=== 1. Schema is known without executing ===');
  print('columns: ${employees.columns}');
  print('dtypes:  ${employees.dtypes}\n');

  print('=== 2. filter, select, orderBy, limit ===');
  final senior = await employees
      .filter('age >= 30')
      .select(['name', 'dept', 'salary'])
      .orderBy('salary', ascending: false)
      .limit(3)
      .execute();
  _show(senior);

  print('=== 3. AND / OR in a predicate ===');
  // A predicate is a SQL expression: AND binds tighter than OR, parentheses
  // group, strings are single-quoted and `==` is `=`.
  final picked = await employees
      .filter("(dept = 'eng' OR dept = 'sales') AND salary >= 100")
      .execute();
  _show(picked);

  print('=== 4. withColumn: standard operator precedence ===');
  // As in SQL, '*' binds tighter than '+': 'salary + 10 * 2' is salary + 20.
  final bonused = await employees
      .select(['name', 'salary'])
      .withColumn('adjusted', 'salary + 10 * 2')
      .execute();
  _show(bonused);

  print('=== 5. groupBy + agg ===');
  // Note: the output column keeps the INPUT column's name, so 'salary' below
  // holds SUM(salary). Use rename() to give it a different name.
  final byDept = await employees
      .groupBy(['dept'])
      .agg({'salary': 'sum', 'age': 'avg'})
      .rename({'salary': 'total_salary', 'age': 'avg_age'})
      .execute();
  _show(byDept);

  print('=== 6. join ===');
  final departments = DataFrame.fromRows([
    {'dept': 'eng', 'floor': 3},
    {'dept': 'sales', 'floor': 1},
  ]);
  final located = await employees
      .select(['name', 'dept'])
      .join(departments, 'dept', type: JoinType.left)
      .execute();
  _show(located);

  print('=== 7. Terminal operations ===');
  // Every terminal returns a Result: Ok with the value, or Err with why not.
  print('count():  ${await employees.count()}');
  if (await employees.head(2) case Ok(:final value)) {
    print('head(2):  ${value.toRows()}');
  }
  print('toList(): ${await employees.select(['name']).toList()}\n');

  print('=== 8. Bounded execution ===');
  switch (await employees.execute(maxRows: 2)) {
    case Ok():
      print('execute(maxRows: 2) unexpectedly succeeded');
    case Err(:final error):
      print('execute(maxRows: 2) rejected as expected: $error');
  }
  // EngineOptions tunes the blocking operators; here the sort never spills.
  final sorted = await employees
      .orderBy('salary')
      .execute(options: const EngineOptions(spillThresholdBytes: 0));
  _show(sorted);

  print('=== 9. Round-trip through a file ===');
  final directory = await Directory.systemTemp.createTemp('libbigdata_example');
  try {
    final path = '${directory.path}/employees.csv';
    if (await employees.writeCsv(path) case Err(:final error)) {
      print('write failed: $error');
      return;
    }

    // readCsv is lazy: it reads the header and an inference sample here, and
    // the file is scanned when a terminal runs, under its maxRows/maxBytes
    // caps. Reading returns a Result too.
    switch (await DataFrameIO.readCsv(path)) {
      case Ok(value: final reloaded):
        final top = await reloaded
            .filter('salary >= 90')
            .orderBy('salary', ascending: false)
            .head(2);
        _show(top);
        print(
          'rows matching filter: '
          '${await reloaded.filter("dept = 'eng'").count()}',
        );
      case Err(:final error):
        print('read failed: $error');
    }
  } finally {
    await directory.delete(recursive: true);
  }

  print('\n=== 10. Handing off to SQL for what DataFrame does not cover ===');
  final SqlEngine engine;
  switch (SqlEngine.open()) {
    case Ok(:final value):
      engine = value;
    case Err(:final error):
      print('Could not open the engine: $error');
      return;
  }
  try {
    if (await engine.registerDataFrame('employees', employees) case Err(
      :final error,
    )) {
      print('Could not register the frame: $error');
      return;
    }
    // Two aggregates over one column, aliased output, and HAVING: all of these
    // need SQL.
    final result = await engine.sql(
      'SELECT dept, MIN(salary) AS lo, MAX(salary) AS hi '
      'FROM employees GROUP BY dept HAVING MAX(salary) > 80',
    );
    switch (result) {
      case Ok(:final value):
        _show(await value.execute());
      case Err(:final error):
        print('query failed: $error');
    }
  } finally {
    await engine.dispose();
  }
}

/// Print a terminal operation's table, or why it failed.
void _show(Result<Table, ErrorDetails> result) {
  switch (result) {
    case Ok(:final value):
      print(value.describe());
    case Err(:final error):
      print('failed: $error');
  }
}
