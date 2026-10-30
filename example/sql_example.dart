#!/usr/bin/env dart

import 'package:libbigdata/libbigdata.dart';

void main() async {
  print('=== BigData SQL Engine Example ===\n');

  // Create SQL engine
  final SqlEngine engine;
  switch (SqlEngine.open()) {
    case Ok(:final value):
      engine = value;
    case Err(:final error):
      print('Could not open the engine: $error');
      return;
  }

  // Create sample data
  final schema = Schema(
    fields: [
      Field(name: 'id', dataType: DataType.int32),
      Field(name: 'name', dataType: DataType.string),
      Field(name: 'age', dataType: DataType.int32),
      Field(name: 'salary', dataType: DataType.float64),
      Field(name: 'department', dataType: DataType.string),
    ],
  );

  final table = Table.fromRows('employees', [
    {
      'id': 1,
      'name': 'Alice',
      'age': 30,
      'salary': 70000.0,
      'department': 'Engineering',
    },
    {
      'id': 2,
      'name': 'Bob',
      'age': 35,
      'salary': 80000.0,
      'department': 'Engineering',
    },
    {
      'id': 3,
      'name': 'Charlie',
      'age': 28,
      'salary': 60000.0,
      'department': 'Sales',
    },
    {
      'id': 4,
      'name': 'Diana',
      'age': 32,
      'salary': 75000.0,
      'department': 'Sales',
    },
    {'id': 5, 'name': 'Eve', 'age': 29, 'salary': 65000.0, 'department': 'HR'},
    {
      'id': 6,
      'name': 'Frank',
      'age': 41,
      'salary': 90000.0,
      'department': 'Engineering',
    },
    {
      'id': 7,
      'name': 'Grace',
      'age': 26,
      'salary': 55000.0,
      'department': 'HR',
    },
    {
      'id': 8,
      'name': 'Henry',
      'age': 38,
      'salary': 85000.0,
      'department': 'Sales',
    },
  ], schema: schema);

  // Register table
  engine.registerTable('employees', table);

  // Example 1: Basic SELECT
  print('1. Basic SELECT:');
  print('   SELECT * FROM employees LIMIT 3\n');
  var result = await engine.execute('SELECT * FROM employees LIMIT 3');
  _show(result);

  // Example 2: WHERE clause
  print('\n2. WHERE clause:');
  print(
    "   SELECT name, age FROM employees WHERE department = 'Engineering'\n",
  );
  result = await engine.execute(
    "SELECT name, age FROM employees WHERE department = 'Engineering'",
  );
  _show(result);

  // Example 3: Aggregation
  print('\n3. Aggregation:');
  print(
    '   SELECT COUNT(*) as total, AVG(salary) as avg_salary FROM employees\n',
  );
  result = await engine.execute(
    'SELECT COUNT(*) as total, AVG(salary) as avg_salary FROM employees',
  );
  _show(result);

  // Example 4: GROUP BY
  print('\n4. GROUP BY:');
  print('''   SELECT department, 
          COUNT(*) as count, 
          AVG(salary) as avg_salary 
   FROM employees 
   GROUP BY department\n''');
  result = await engine.execute('''
    SELECT department, COUNT(*) as count, AVG(salary) as avg_salary 
    FROM employees 
    GROUP BY department
  ''');
  _show(result);

  // Example 5: ORDER BY and LIMIT
  print('\n5. ORDER BY and LIMIT:');
  print('   SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3\n');
  result = await engine.execute(
    'SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3',
  );
  _show(result);

  // Example 6: WITH clause (CTE)
  print('\n6. WITH clause (Common Table Expression):');
  print('''   WITH high_earners AS (
     SELECT * FROM employees WHERE salary > 70000
   )
   SELECT department, COUNT(*) as count 
   FROM high_earners 
   GROUP BY department\n''');
  result = await engine.execute('''
    WITH high_earners AS (
      SELECT * FROM employees WHERE salary > 70000
    )
    SELECT department, COUNT(*) as count 
    FROM high_earners 
    GROUP BY department
  ''');
  _show(result);

  // Example 7: File as table (like DuckDB)
  print('\n7. Query files directly (if files exist):');
  print("   SELECT * FROM 'data.csv' LIMIT 5");
  print("   SELECT COUNT(*) FROM 'sales.parquet'");
  print("   Note: These would work if the files existed\n");

  // Example 8: DataFrame integration
  print('8. DataFrame integration:');
  final df = DataFrame.fromTable(table);
  final filtered = df
      .filter("age > 30")
      .select(['name', 'age', 'department'])
      .orderBy('age', ascending: false)
      .limit(3);

  final dfResult = await filtered.execute();
  print(
    '   DataFrame pipeline: filter(age > 30) -> select -> orderBy -> limit(3)\n',
  );
  _show(dfResult);
}

/// Print a query's table, or why it failed.
void _show(Result<Table, ErrorDetails> result) {
  switch (result) {
    case Ok(:final value):
      _printTable(value);
    case Err(:final error):
      print('   failed: $error');
  }
}

void _printTable(Table table) {
  if (table.rowCount == 0) {
    print('   (empty result)');
    return;
  }

  // Calculate column widths
  final widths = <int>[];
  for (final column in table.columns) {
    int maxWidth = column.name.length;
    for (int i = 0; i < table.rowCount && i < 10; i++) {
      final value = column[i];
      final str = _formatValue(value);
      if (str.length > maxWidth) maxWidth = str.length;
    }
    widths.add(maxWidth > 20 ? 20 : maxWidth);
  }

  // Print header
  final buffer = StringBuffer('   ');
  for (int i = 0; i < table.columns.length; i++) {
    buffer.write(table.columns[i].name.padRight(widths[i] + 2));
  }
  print(buffer);

  buffer.clear();
  buffer.write('   ');
  for (final width in widths) {
    buffer.write('-' * width + '  ');
  }
  print(buffer);

  // Print rows
  for (int row = 0; row < table.rowCount && row < 10; row++) {
    buffer.clear();
    buffer.write('   ');
    for (int col = 0; col < table.columns.length; col++) {
      final value = table.columns[col][row];
      final str = _formatValue(value);
      buffer.write(str.padRight(widths[col] + 2));
    }
    print(buffer);
  }

  if (table.rowCount > 10) {
    print('   ... (${table.rowCount - 10} more rows)');
  }
}

String _formatValue(dynamic value) {
  if (value == null) return 'NULL';
  if (value is double) {
    return value.toStringAsFixed(2);
  }
  return value.toString();
}
