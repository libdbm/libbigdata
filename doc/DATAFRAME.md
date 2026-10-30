# DataFrame API

The DataFrame API is a chainable, lazily-evaluated interface for tabular data.
Operations build a plan; nothing is read or computed until a terminal operation
runs. The whole chain is then fused into a single physical plan and streamed
once, over the same columnar engine the [SQL engine](SQL.md) uses.

```dart
import 'package:libbigdata/libbigdata.dart';

// Reading and every terminal operation return a Result: Ok with the value, or
// Err with an ErrorDetails (a Kind plus a message).
if (await DataFrameIO.readCsv('people.csv') case Ok(value: final df)) {
  final adults = await df
      .filter('age >= 18')
      .select(['name', 'age', 'city'])
      .orderBy('age', ascending: false)
      .limit(10)
      .execute();
  if (adults case Ok(:final value)) print(value.toRows());
}
```

## Table of contents

- [Creating a DataFrame](#creating-a-dataframe)
- [Laziness and execution](#laziness-and-execution)
- [Transformations](#transformations)
- [Expression syntax](#expression-syntax)
- [Terminal operations](#terminal-operations)
- [Resource limits](#resource-limits)
- [Writing output](#writing-output)
- [When to use SQL instead](#when-to-use-sql-instead)

## Creating a DataFrame

### From memory

```dart
final df = DataFrame.fromRows([
  {'name': 'Alice', 'age': 30},
  {'name': 'Bob', 'age': 25},
]);

final typed = DataFrame.fromRows(rows, schema: schema); // explicit schema
final fromTable = DataFrame.fromTable(table);
final blank = DataFrame.empty(schema);
```

`fromRows` infers the schema over *all* rows when none is given. Passing an
explicit `schema` is held to stricter rules: values that do not match the
declared types are rejected rather than coerced.

Text columns are stored as UTF-8, so a Dart string with an unpaired surrogate
(a code unit from U+D800 to U+DFFF without its partner) reads back with U+FFFD
in its place. Strings read from files cannot contain one.

### From files

`DataFrameIO` is an extension on `DataFrame` providing the readers and writers.

Each reader returns a `Result` (`R<T>` below is `Result<T, ErrorDetails>`); a
`*Batches` reader yields one per batch, and a failure is its last item.

| Reader | Returns | Notes |
|---|---|---|
| `readCsv(path, {options, filter, columns, limit, batchSize, maxRows, maxBytes})` | `Future<R<DataFrame>>` | lazy; schema inference, `CsvOptions` for delimiters and headers; `filter`, `columns` and `limit` apply as it scans, `batchSize` rows at a time |
| `readCsvBatches(path, {options, batchSize, filter, columns, limit})` | `Stream<R<DataFrame>>` | out-of-core |
| `readJson(path, {options, maxRows, maxBytes})` | `Future<R<DataFrame>>` | lazy |
| `readJsonBatches(path, {options, batchSize})` | `Stream<R<DataFrame>>` | JSON Lines |
| `readJsonLines(path, {maxRows, maxBytes})` | `Future<R<DataFrame>>` | lazy |
| `readParquet(path, {filter, columns, maxRows, maxBytes})` | `Future<R<DataFrame>>` | lazy; predicate and column pushdown |
| `readArrow(path, {maxRows, maxBytes})` | `Future<R<DataFrame>>` | lazy; Arrow IPC file |
| `readArrowStream(path, {maxRows, maxBytes})` | `Future<R<DataFrame>>` | Arrow IPC stream (`pa.ipc.new_stream`), read when called; a file reads lazily |
| `readArrowBatches(path, {batchSize})` | `Stream<R<DataFrame>>` | Arrow IPC file, at most `batchSize` rows per batch |
| `readDataset(path, {filter, columns, concurrency, maxRows, maxBytes})` | `Future<R<DataFrame>>` | lazy; multi-file dataset |
| `readDatasetBatches(path, {filter, columns, batchSize})` | `Stream<R<DataFrame>>` | multi-file dataset, out-of-core |
| `readDelta(path, {version, asOf, concurrency, maxRows, maxBytes, allowExternalPaths})` | `Future<R<DataFrame>>` | lazy; Delta Lake, time travel |
| `readIceberg(path, {snapshotId, asOf, maxRows, maxBytes, allowExternalPaths})` | `Future<R<DataFrame>>` | lazy; Iceberg, snapshots |

All are static members of `DataFrameIO`, so call them as `DataFrameIO.readCsv(...)`.

The readers and writers share one set of option names:

- `options:` takes the format's options (`CsvOptions`, `JsonOptions`), always named.
- `maxRows:` / `maxBytes:` cap every reader or writer that materializes its result
  (null opts out).
- `batchSize:` is the rows per batch of a `*Batches` reader or a writer.
- `columns:` projects, and `filter:` takes a `RowFilter`. For Parquet and datasets it
  only prunes, by statistics and partition values, so a kept row group or file comes
  back whole (chain a `filter` call to filter exactly); a CSV reader has no statistics,
  so it keeps exactly the rows the filter matches.
- `asOf:` is time travel in milliseconds since the epoch, for Delta and Iceberg alike.

There are also `fromCsvString`, `fromJsonString` and `fromJsonLinesString` for
in-memory text.

The file readers (`readCsv`, `readJson`, `readJsonLines`, `readParquet`,
`readArrow`, `readArrowStream` over an IPC file, `readDelta`, `readIceberg` and
`readDataset`) return **lazy** frames. The reader reads the schema (a Parquet or
Arrow footer, a CSV header and inference sample, a JSON inference sample, a
Delta log or Iceberg metadata, every file of a dataset) and the data is read
when a terminal operation runs, through a streaming file scan that carries the
reader's options, so this reads only as much of the file as it needs:

```dart
if (await DataFrameIO.readParquet('huge.parquet') case Ok(value: final df)) {
  final sample = await df.filter("status = 'error'").head(10); // stops early
  final total = await df.count();                               // never materializes
}
```

A later `filter` and `select` reach the scan as SQL's do: Parquet skips row
groups and columns, Delta and Iceberg skip files (Iceberg manifests too) and
read only the selected columns, a dataset skips partitions, and CSV, JSON and
Arrow decode only the selected columns. The reader's own `filter`, `columns`,
`limit`, version or snapshot apply as they did when it read eagerly.

Errors the schema read finds are an `Err` from the reader: a missing file, an
unreadable header or footer, a directory that is not a Delta or Iceberg table,
an unknown version or snapshot, an unknown name in `columns`, and dataset files
whose column types conflict. Anything found further into the data (a value that
does not parse, a record with a field the inference sample did not have, a
missing or unreadable data file, a data or delete file outside the table) is
reported by the terminal. A Delta or Iceberg frame without a version or
snapshot is fixed to the version or snapshot current when the reader was
called: every terminal reads that, so a commit made later does not change the
frame's result (call the reader again to see it).

The reader's `maxRows`/`maxBytes` bound every materialized result of the frame
and of frames built on it (`execute`, `toList`, `head`, `tail`, `cache`, ...),
together with the terminal's own caps: the tighter applies. `count` and
`executeStream` set no result cap, so they read a file of any size in bounded
memory.

These are read when called, and return a frame over the resulting table: CSV
with a custom `lineTerminator` or with `maxFileBytes` (the scan streams on the
default terminators only), JSON with `expandArrays` or with `maxRows: null,
maxBytes: null` (whole-file schema inference), an Arrow IPC stream (it has no
footer to read the schema from), Delta with `concurrency` above 1 and a dataset
with `concurrency` above 1 and both caps null (parallel reads), and an empty
file with no schema to read. Use a `*Batches` reader or
`SqlEngine.executeStream` to process one of these in bounded memory.

## Laziness and execution

Every transformation returns a **new** DataFrame. None mutate the receiver and
none do work. Calling a terminal operation folds the accumulated operations into
one physical plan and executes it in a single streaming pass, so there are no
per-operation intermediate tables, and a trailing `limit` short-circuits the
stream rather than materializing everything first.

`sample` keeps exactly `round(n * fraction)` of its n input rows, so it holds
its whole input before emitting any: under the memory budget and the byte cap on
a materialized intermediate (`EngineOptions.maxMaterializedBytes`, or `maxBytes`
when tighter, else the library default), failing with `Kind.limit` past either.
A filter after a `sample` is not pushed below it.

A frame from a file reader is lazy too: the reader reads only the schema, and
the plan is rooted at a scan of the file, so a filter and a projection reach it
(see [From files](#from-files)).

Schemas are derived eagerly as you chain, so `columns` and `dtypes` are
available without executing:

```dart
final projected = df.withColumn('total', 'price * quantity');
print(projected.columns); // includes 'total' before any execution
```

## Transformations

All of these are lazy and return a new `DataFrame`.

| Method | Description |
|---|---|
| `select(List<String> columns)` | Keep the named columns. `df[['a','b']]` is an alias. |
| `filter(String predicate)` | Keep matching rows. `where` is an alias. |
| `limit(int n, {int offset = 0})` | Take `n` rows starting at `offset`. |
| `orderBy(String column, {bool ascending = true})` | Sort by one column. |
| `groupBy(List<String> columns)` | Set group keys; must be followed immediately by `agg`. |
| `agg(Map<String, String> aggregations)` | Aggregate, keyed `{column: function}`. |
| `join(DataFrame other, String on, {JoinType type})` | Equi-join on a shared column name. |
| `withColumn(String name, String expression)` | Add or replace a computed column. |
| `drop(List<String> columns)` | Remove columns. |
| `rename(Map<String, String> renames)` | Rename columns, `{from: to}`. |
| `distinct()` | Remove duplicate rows. |
| `union(DataFrame other)` | Concatenate another DataFrame. |
| `sample(double fraction, {int? seed})` | Random subset; pass `seed` for reproducibility. |

`orderBy` sorts nulls last in both directions, as SQL `ORDER BY` does, and
orders booleans `false` before `true`.

`JoinType` is one of `inner`, `left`, `right`, `full`, `cross`. The `on`
argument is a single column name present in both sides, not a general join
condition. For anything richer, such as multi-key joins, inequality conditions
or semi/anti joins, use the [SQL engine](SQL.md).

A join runs as SQL `JOIN ... USING (on)`, so the key is one column: a `right`
join keeps the right side's key, and a `full` join the key of whichever side
has the row. A `cross` join ignores `on`. A non-key column that both sides have
is qualified by its side's name, as SQL names it: if `people` wraps a table
named `p` and `depts` one named `d`, `people.join(depts, 'dept')` has columns
`p.name` and `d.name`. A side's name is its table's name, or the file's base
name for a file-backed frame (`readParquet('d.parquet')` is `d`). When both
sides have the same name (two `fromRows` frames are both `dataframe`, or a
frame joined to itself) they are `left` and `right`. `schema` and `columns`
report the same names.

Every operation that takes column names (`select`, `[]`, `orderBy`, `drop`,
`rename`, `groupBy`, `agg`, `withColumn` and a later `join`'s `on`) takes them
as `columns` reports them, and the result keeps the name given:
`joined.select(['p.name'])` has one column named `p.name`,
`joined.rename({'d.name': 'department'})` renames the department's `name`, and
`joined.select(joined.columns!)` selects everything. A bare `name` that both
sides have is ambiguous and fails with `Kind.invalid`, naming the choices
(`Ambiguous column "name": use p.name or d.name`). A name only one side has
stays bare (`dept`, `floor`). A column whose own name holds a dot, such as a
CSV header `a.b`, is taken as written. In a `filter` or `withColumn`
expression on the joined frame, `p.name` is SQL's qualified reference to the
same column; after a `select`, `drop`, `rename` or `withColumn` the column is
one named `p.name`, written `"p.name"` in an expression. SQL
names a selected `p.name` just `name`, where `select(['p.name'])` keeps
`p.name`. Each further join qualifies its own side the same way, so joining a
third frame `f` that also has `name` gives `p.name`, `d.name` and `f.name`.

`union` is SQL `UNION ALL`: rows are matched by position, duplicates are kept,
and the result has this frame's column names with, per position, the common
type of both sides. A different column count fails with `Kind.schema`.

```dart
final joined = await orders
    .join(customers, 'customer_id', type: JoinType.left)
    .withColumn('total', 'price * quantity')
    .execute();
```

### Grouping and aggregation

```dart
final byDept = await df
    .groupBy(['dept'])
    .agg({'salary': 'sum', 'age': 'avg'})
    .execute();
```

Supported functions, case-insensitive: `count`, `sum`, `avg`, `mean` (an alias
of `avg`), `min`, `max`. Anything else fails at execution with `Kind.invalid`. `min` and `max`
also work on strings.

Three behaviours to be aware of:

- **The output column keeps the input column's name.** `agg({'salary': 'sum'})`
  produces a column called `salary`, not `sum(salary)`. Chain `.rename({...})`
  if you want a different name.
- **One aggregate per column.** The argument is a `Map` keyed by column name, so
  two aggregates over the same column are not expressible. Use SQL for that.
- **`agg` reads group keys from the immediately preceding operation.** Anything
  else after `groupBy` fails at execution with `Kind.invalid`. Always chain
  them directly.

With no `groupBy`, `agg` is a global aggregate returning exactly one row. Over
an empty input it returns one row of nulls.

Integer `sum` returns a `Decimal`, which is exact and cannot overflow, and
`avg`/`mean` return `double`.

## Expression syntax

`filter` and `withColumn` take a SQL scalar expression, the same grammar as a
`WHERE` clause or a `SELECT` item, bound to the columns of the frame they are
called on. Everything the SQL engine accepts in a row expression is available:
comparisons, `AND`/`OR`/`NOT`, parentheses, arithmetic, `IS [NOT] NULL`,
`IN (...)` lists, `LIKE`, `BETWEEN`, `CASE`, `CAST`, function calls, and
columns or literals on either side of an operator. See [SQL.md](SQL.md) for the
full grammar.

- **Names.** A column name that is not a plain identifier (it contains a space,
  a `-`, or another symbol) is written as a double-quoted identifier:
  `"First Name"`. Double quotes always mean an identifier.
- **Strings.** String literals are single-quoted: `dept = 'sales'`.
- **`==`.** `==` is accepted as `=`, in DataFrame expressions and in SQL alike.
- **Precedence.** Standard SQL precedence: `*`, `/` and `%` bind tighter than
  `+` and `-`, comparisons tighter than `NOT`, `NOT` tighter than `AND`, and
  `AND` tighter than `OR`. `a + 10 * 2` is `a + 20`.
- **Typing.** As in SQL, a string never compares with or combines with a
  number (`age = '30'` is `Kind.invalid`; write `age = 30` or `CAST`). A string
  literal compared with a date or timestamp column is read as one
  (`t > '2024-06-01'`; text without a zone is read as UTC), and text that does
  not parse is `Kind.invalid`.

### Filter predicates

```dart
df.filter('age >= 18')
df.filter("dept = 'sales'")
df.filter('id >= 2 AND id <= 4')
df.filter('id == 1 OR id == 5')
df.filter('"Dept Name" = \'y\'')              // a quoted name with a space
df.filter("(age > 30 OR age IS NULL) AND dept IN ('eng', 'ops')")
df.filter("name LIKE 'A%' AND NOT salary > 100")
df.filter('salary > age * 2')                  // column against column
df.filter("UPPER(name) = 'BOB'")
```

A row is kept where the predicate is `TRUE`; `NULL` and `FALSE` drop it, as in
`WHERE`. A predicate that is not boolean is cast to `BOOLEAN`, as DuckDB does:
`filter('age')` keeps the rows where `age` is nonzero, and text follows CAST's
rules. A predicate that contains an aggregate function is `Kind.invalid`, and a
subquery is `Kind.unsupported`.

### withColumn expressions

```dart
df.withColumn('bonus', 'salary * 2')
df.withColumn('flag', '1')
df.withColumn('neg', 'salary * -1')
df.withColumn('salary', 'salary + 1')  // replaces the column in place
df.withColumn('x', '(age + 10) * 2')
df.withColumn('band', "CASE WHEN age > 30 THEN 'senior' ELSE 'junior' END")
df.withColumn('years', 'CAST(age AS DOUBLE)')
df.withColumn('forename', 'LOWER("First Name")')
```

Division always produces `float64`, as does any operator with a double
operand. Otherwise a decimal operand produces a `decimal` (`price * 2` on a
`DECIMAL(10,2)` column stays exact at scale 2), and two integers produce
`int64`, which fails with `Kind.invalid` on 64-bit overflow rather than wrapping. Division and
modulo by zero yield `NULL`, following SQL semantics.

An unknown column, an aggregate function or a malformed expression is
`Kind.invalid`, reported by the terminal operation; the builder itself never
throws.

## Terminal operations

These execute the plan. Each returns a `Result` (`R<T>` below is
`Result<T, ErrorDetails>`): `Ok` with the value, or `Err` reported as the SQL
engine reports it, since predicates and expressions are caller-supplied
strings. An invalid predicate is `Kind.invalid`, a cap `Kind.limit`, a deadline
`Kind.timeout`.

| Method | Returns | Description |
|---|---|---|
| `execute({maxRows, maxBytes, options})` | `Future<R<Table>>` | Run the plan and materialize the result. |
| `executeStream({options})` | `Stream<R<Table>>` | Run the plan, yielding batches; a failure is the last item. |
| `toList()` | `Future<R<List<Map<String, dynamic>>>>` | Execute and convert to rows. |
| `toJsonString()` | `Future<R<String>>` | Execute and JSON-encode the rows. |
| `head([int n = 5])` | `Future<R<Table>>` | First `n` rows; limits before execution. |
| `tail([int n = 5])` | `Future<R<Table>>` | Last `n` rows; materializes the result first. |
| `count()` | `Future<R<int>>` | Row count, streamed without retaining columns. |
| `show([int rows = 20])` | `Future<R<void>>` | Print a formatted preview. |
| `cache()` | `Future<R<DataFrame>>` | Execute once and wrap the result for reuse. |

Non-executing accessors: `schema`, `columns`, `dtypes` (all derived eagerly,
`null` if unknown) and `clone()`.

`toTable()` and `transform(fn)` operate on the source table synchronously and
throw a `StateError` if operations are pending, so call `execute()` first. They
do not silently discard the pending chain.

### Streaming

`executeStream` yields batches for results larger than memory:

```dart
await for (final result in df.filter('amount > 100').executeStream()) {
  switch (result) {
    case Ok(:final value):
      process(value);
    case Err(:final error):
      print('stopped: $error'); // the last item of the stream
  }
}
```

Peak memory stays at roughly one batch for a fusible plan. Blocking operators
such as sorts, aggregates and joins buffer their input by nature, and
`executeStream` applies no row or byte caps, since a stream is asked for
precisely when the caller does not want a ceiling. Use `execute` when you want
the limits enforced.

### Reusing a DataFrame

A DataFrame is a plan, not a result, so re-executing re-runs the work. When you
branch off the same intermediate more than once, `cache()` it:

```dart
if (await df.filter('year == 2024').cache() case Ok(value: final base)) {
  final byCity = await base.groupBy(['city']).agg({'amount': 'sum'}).execute();
  final top = await base.orderBy('amount', ascending: false).head(10);
}
```

## Resource limits

`execute()` bounds materialization by default. It returns an `Err` of kind
`Kind.limit` once the result exceeds `maxRows` (10,000,000) or `maxBytes` (1 GiB). Pass `null` to opt
out of either.

```dart
final strict = await df.execute(maxRows: 1000);  // Err(Kind.limit) past the cap
final open = await df.execute(maxBytes: null);   // no byte ceiling
```

`options:` takes an `EngineOptions` for the blocking operators: the spill
threshold and directory, the output-row cap, and shared memory or spill-disk
budgets (`MemoryManager`, `SpillBudget`). All three are exported from
`package:libbigdata/libbigdata.dart`.

```dart
final sorted = await df.orderBy('amount').execute(
  options: const EngineOptions(spillThresholdBytes: 64 << 20),
);
```

The limits propagate through every materialization boundary in the plan, not
just the final result, so a small cap also bounds a wide intermediate join or
union. They apply even to a no-op plan. A lazy file reader's own `maxRows` and
`maxBytes` bound the result as well, the tighter cap applying.

## Writing output

The writers are instance methods on a DataFrame, also from `DataFrameIO`.

```dart
// Each write returns a Result; an Err leaves no partial file behind.
if (await df.writeCsv('out.csv') case Err(:final error)) print(error);
final json = await df.writeJson('out.json');
final lines = await df.writeJsonLines('out.jsonl');
final arrow = await df.writeArrow('out.arrow');
final delta = await df.writeDelta('table/', mode: DeltaWriteMode.append); // version
final iceberg = await df.writeIceberg('warehouse/table/');               // version

final csv = await df.toCsv();        // Result<String, ErrorDetails>
final text = await df.toJson();
```

`writeDelta` and `writeIceberg` return the committed table version. `writeCsv`
and `writeJson` stream batch by batch, while `toCsv` and `toJson` materialize
the whole result and are themselves bounded by the same row and byte caps.

For Parquet, `DataFrameIO.writeParquetStream` accrues row groups so it can write
files larger than memory.

## When to use SQL instead

The DataFrame API covers single-column sorts, single-key equi-joins, `UNION ALL`
and SQL row expressions in `filter` and `withColumn`. Reach for
[`SqlEngine`](SQL.md) when you need multi-key or non-equi joins, semi/anti joins
and subqueries, multi-column `ORDER BY`, several aggregates over one column,
aliased aggregate output, `HAVING`, CTEs, or set operations other than
`UNION ALL`.

The two interoperate. Register a DataFrame and query it:

```dart
if (SqlEngine.open() case Ok(value: final engine)) {
  if (await engine.registerDataFrame('people', df) case Ok()) {
    final result =
        await engine.sql('SELECT city, COUNT(*) AS c FROM people GROUP BY city');
  }
  await engine.dispose();
}
```

The frame is registered as a view and is not run at registration: each query
that reads `people` plans the frame's operations with its own, through the same
optimizer, so a query's `WHERE` on a file-backed frame prunes row groups and
only the columns the query uses are read. Each query reads the frame's sources
again. `df.sql(query)` does the same in an engine of its own (the frame is
`df`, or `tableName`), disposed when the query returns. The columns are named
as `columns` names them. `registerDataFrame`'s `maxRows`/`maxBytes`, with the
caps of the frame's readers, bound each query's result (see [SQL.md](SQL.md)).

`SqlEngine.sql` returns a `DataFrame`, so you can chain DataFrame operations
onto a SQL result.

## More

- [SQL.md](SQL.md) for the SQL engine.
- [DESIGN.md](DESIGN.md) for the architecture and execution model.
- [CLI.md](CLI.md) for the native command-line interface.
- [parquet_support.md](parquet_support.md) for Parquet format coverage.
</content>
