# libbigdata examples

Run any example with `dart run example/<file>`.

## [`dataframe_example.dart`](dataframe_example.dart)

The DataFrame API end to end: building from rows, `filter` / `select` / `orderBy` / `limit`,
`withColumn`, `groupBy` + `agg`, `join`, terminal operations, bounded execution, a lazy
round-trip through a CSV file, and handing off to SQL for what the DataFrame API does not
cover.

```bash
dart run example/dataframe_example.dart
```

## [`sql_example.dart`](sql_example.dart)

The SQL engine end to end: build a `Table` from rows, register it with a `SqlEngine`, then
run `SELECT`, `WHERE`, `GROUP BY` / aggregate, `ORDER BY` and `JOIN` queries, printing each
result as a formatted table.

```bash
dart run example/sql_example.dart
```

## [`logging_example.dart`](logging_example.dart)

Configuring `LoggingConfig`: log levels, per-subsystem loggers (`Loggers.parquet` and
friends), coloured console output, and logging to a file.

```bash
dart run example/logging_example.dart
```

## Minimal usage

```dart
import 'package:libbigdata/libbigdata.dart';

Future<void> main() async {
  // Query a file directly with SQL.
  final result = await sql.execute(
    "SELECT city, count(*) FROM 'people.csv' GROUP BY city",
  );
  print(result.toRows());

  // Or use the DataFrame API.
  final df = await DataFrameIO.readCsv('people.csv');
  final adults = await df
      .filter('age >= 18')
      .select(['name', 'age'])
      .orderBy('age', ascending: false)
      .limit(10)
      .execute();
  print(adults.toRows());
}
```

See the [main README](../README.md), [doc/DATAFRAME.md](../doc/DATAFRAME.md) and
[doc/SQL.md](../doc/SQL.md) for more.
