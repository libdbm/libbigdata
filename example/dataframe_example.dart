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
  print(senior.describe());

  print('=== 3. AND / OR in a predicate ===');
  // AND binds tighter than OR. Parentheses are NOT supported; chain filter()
  // calls instead, which compose as AND.
  final picked = await employees
      .filter('dept == "eng" AND salary >= 100')
      .execute();
  print(picked.describe());

  print('=== 4. withColumn: flat operator precedence ===');
  // Operators apply left to right, so 'salary + 10 * 2' is (salary + 10) * 2.
  final bonused = await employees
      .select(['name', 'salary'])
      .withColumn('adjusted', 'salary + 10 * 2')
      .execute();
  print(bonused.describe());

  print('=== 5. groupBy + agg ===');
  // Note: the output column keeps the INPUT column's name, so 'salary' below
  // holds SUM(salary). Use rename() to give it a different name.
  final byDept = await employees
      .groupBy(['dept'])
      .agg({'salary': 'sum', 'age': 'avg'})
      .rename({'salary': 'total_salary', 'age': 'avg_age'})
      .execute();
  print(byDept.describe());

  print('=== 6. join ===');
  final departments = DataFrame.fromRows([
    {'dept': 'eng', 'floor': 3},
    {'dept': 'sales', 'floor': 1},
  ]);
  final located = await employees
      .select(['name', 'dept'])
      .join(departments, 'dept', type: JoinType.left)
      .execute();
  print(located.describe());

  print('=== 7. Terminal operations ===');
  print('count():  ${await employees.count()}');
  print('head(2):  ${(await employees.head(2)).toRows()}');
  print('toList(): ${(await employees.select(['name']).toList())}\n');

  print('=== 8. Bounded execution ===');
  try {
    await employees.execute(maxRows: 2);
  } on StateError catch (e) {
    print('execute(maxRows: 2) rejected as expected: ${e.message}\n');
  }

  print('=== 9. Round-trip through a file, read back lazily ===');
  final directory = await Directory.systemTemp.createTemp('libbigdata_example');
  try {
    final path = '${directory.path}/employees.csv';
    await employees.writeCsv(path);

    // A file-backed DataFrame is lazy: the filter and limit reach the scan, so
    // this does not decode the whole file.
    final reloaded = await DataFrameIO.readCsv(path);
    final top = await reloaded
        .filter('salary >= 90')
        .orderBy('salary', ascending: false)
        .head(2);
    print(top.describe());
    print(
      'rows matching filter: ${await reloaded.filter('dept == "eng"').count()}',
    );
  } finally {
    await directory.delete(recursive: true);
  }

  print('\n=== 10. Handing off to SQL for what DataFrame does not cover ===');
  final engine = SqlEngine();
  try {
    await engine.registerDataFrame('employees', employees);
    // Two aggregates over one column, aliased output, and HAVING: all of these
    // need SQL.
    final result = await engine.sql(
      'SELECT dept, MIN(salary) AS lo, MAX(salary) AS hi '
      'FROM employees GROUP BY dept HAVING MAX(salary) > 80',
    );
    print((await result.execute()).describe());
  } finally {
    await engine.dispose();
  }
}
