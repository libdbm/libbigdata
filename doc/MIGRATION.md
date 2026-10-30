# Migrating to 2.0

In 2.0, public entry points return `Result<T, ErrorDetails>` for expected failures instead of throwing. `ErrorDetails` carries a `kind` (a `Kind`: conflict, indeterminate, limit, schema, corrupt, denied, invalid, missing, unsupported, io, timeout), a `message` and a `cause`. Programming errors are still thrown as `Error`s.

No failure escapes a method that returns a `Result`: an unexpected error, including one raised by code you pass in (a predicate, a custom coordinator, a source stream), comes back as an `Err` with the error as its `cause`. Only argument preconditions (a batch size of 0, `retain < 1`, opening a writer twice, using a disposed engine) still throw.

`Result` is sealed, so a `switch` over `Ok` and `Err` must handle both:

```dart
switch (await DataFrameIO.readCsv('people.csv')) {
  case Ok(:final value):
    use(value);
  case Err(:final error):
    print('${error.kind.name}: ${error.message}');
}
```

The sections below cover each area with before and after examples. The [inventory](#inventory) at the end lists every member that changed, what it threw in 1.x and the kind it reports now.

Other breaking changes in 2.0 that are not about `Result` (a read-only sandboxed engine, `"x"` as a SQL identifier, no string-to-number coercion, `DataType.decimal.dartType`) are listed in the CHANGELOG.

## Delta writes

`DeltaWriter.write`, `delete`, `update` and `merge`, `DataFrameIO.writeDelta`, `DeltaCommitCoordinator.commit` and `InvariantEvaluator.violations` return `Result<int, ErrorDetails>`. The `Ok` value is what 1.x returned: the committed version, or -1 for a mutation that matched nothing.

Before:

```dart
try {
  final version = await DeltaWriter(path).write(table);
  print('committed $version');
} on CommitConflict {
  // another writer won; retry later
} on StateError catch (error) {
  print('rejected: $error');
}
```

After:

```dart
switch (await DeltaWriter(path).write(table)) {
  case Ok(:final value):
    print('committed $value');
  case Err(:final error) when error.kind == Kind.conflict:
    // another writer won; retry later
  case Err(:final error):
    print('rejected: $error');
}
```

The kinds a Delta write returns:

| Outcome | 1.x | 2.0 `Kind` |
| --- | --- | --- |
| Lost the commit race after `maxRetries` | `CommitConflict` | conflict |
| A mutation's basis changed under it (a concurrent commit, `delta.appendOnly` turned on) | `StateError` | conflict |
| Publish outcome unknown; staged files are kept | `DeltaCommitIndeterminate` | indeterminate |
| Append schema or partition columns differ from the table | `StateError` | schema |
| Merge source over `maxSourceRows` or `maxSourceBytes`, too many partitions | `StateError` | limit |
| Mutation on a `delta.appendOnly` table | `UnsupportedError` | denied |
| Invariant violated, unknown column, merge cardinality, `partitionBy` naming every column | `StateError`, `ArgumentError` | invalid |
| Unsupported protocol feature, invariants with no evaluator, partitioned update or merge | `UnsupportedError` | unsupported |
| No table, no commits at the path | `ArgumentError`, `StateError` | missing |
| Gapped, truncated or 0-byte log | `StateError` | corrupt |
| File lock not acquired within `timeout` | `TimeoutException` | timeout |
| File system failure | `FileSystemException` | io |

A custom `DeltaCommitCoordinator` returns `Future<Result<int, ErrorDetails>>` from `commit`: `Err(ErrorDetails(Kind.conflict, ...))` for a lost race, `Kind.indeterminate` when the publish may have landed. SQL `UPDATE`, `INSERT` and `MERGE` report the writer's kinds through [SQL](#sql).

## Iceberg writes

`IcebergWriter.write`, `IcebergWriter.expireSnapshots`, `DataFrameIO.writeIceberg` and `IcebergBackend.commit` return `Result<int, ErrorDetails>`. The `Ok` value is what 1.x returned: the committed version, or the number of snapshots expired.

Before:

```dart
try {
  await IcebergWriter(path).write(table);
} on IcebergCommitConflict {
  // retry later
}
```

After:

```dart
switch (await IcebergWriter(path).write(table)) {
  case Ok():
    break;
  case Err(:final error) when error.kind == Kind.conflict:
    // retry later
  case Err(:final error):
    print('rejected: $error');
}
```

| Outcome | 1.x | 2.0 `Kind` |
| --- | --- | --- |
| Append schema differs from the table's | `StateError` | schema |
| Lost the commit race, or the schema or partition spec changed concurrently | `IcebergCommitConflict`, `StateError` | conflict |
| Publish outcome unknown; staged files are kept | `IcebergCommitIndeterminate` | indeterminate |
| Metadata over `maxMetadataBytes`, or more partitions than `maxPartitions` | `StateError` | limit |
| `expireSnapshots` on a path with no table | `StateError` | missing |
| A snapshot names a missing manifest list or manifest | `FormatException` | corrupt |
| Staged files could not be removed after a failure; `cause` is that failure | `StateError` | io |
| Expired files could not be deleted (the expiry has committed) | `FileSystemException` | io |

`expireSnapshots(retain: 0)` is a programming error and still throws `ArgumentError`. A custom `IcebergBackend` returns `Future<Result<int, ErrorDetails>>` from `commit` and `Future<Result<Map<String, dynamic>?, ErrorDetails>>` from `current`; return `Err(ErrorDetails(Kind.conflict, ...))` for a lost race so the writer cleans up, and `Kind.indeterminate` when the publish may have landed so it keeps its files.

## SQL

`SqlEngine.execute` returns `Result<Table, ErrorDetails>`, `SqlEngine.sql` and `DataFrame.sql` return `Result<DataFrame, ErrorDetails>`, and `SqlEngine.executeStream` yields `Result<Table, ErrorDetails>` items: each batch as an `Ok`, and at most one `Err`, which ends the stream. The stream itself never errors.

SQL text is treated as untrusted input, so nothing a query does is thrown, including errors the engine raises internally. Only calling a disposed engine throws (`StateError`).

Before:

```dart
try {
  final table = await engine.execute(query);
  render(table);
} on FormatException catch (e) {
  reply(400, e.message);
} on ArgumentError catch (e) {
  reply(403, '$e');
}

await for (final batch in engine.executeStream(query)) {
  render(batch);
}
```

After:

```dart
switch (await engine.execute(query)) {
  case Ok(:final value):
    render(value);
  case Err(:final error) when error.kind == Kind.denied:
    reply(403, error.message);
  case Err(:final error):
    reply(400, error.message);
}

await for (final result in engine.executeStream(query)) {
  switch (result) {
    case Ok(:final value):
      render(value);
    case Err(:final error):
      reply(400, error.message);
  }
}
```

| Outcome | 1.x | 2.0 `Kind` |
| --- | --- | --- |
| SQL that does not parse, an unknown column, a type mismatch, integer overflow | `FormatException`, `StateError` | invalid |
| SQL the engine does not implement (WITH RECURSIVE, window functions) | `UnsupportedError` | unsupported |
| Unknown table, view or file | `StateError`, `ArgumentError`, `FileSystemException` | missing |
| A path outside the sandbox or working directory; a write on a read-only engine | `ArgumentError` | denied |
| An unreadable data file | `FormatException`, `StateError` | corrupt |
| A result over `maxRows`/`maxBytes`, an operator over its memory or spill budget | `StateError` | limit |
| Past the deadline, or `lockTimeout` waiting for an earlier query | `TimeoutException`, `StateError` | timeout |
| An INSERT row that does not fit the table | `ArgumentError` | schema |
| Set-operation branches with different column counts | `ArgumentError` | schema |
| UPDATE, INSERT, MERGE commit failures | as `DeltaWriter` | as [Delta writes](#delta-writes) |
| Any other error raised while running the query | the error | invalid, with the error as `cause` |

## DataFrame terminals

Building a DataFrame (`filter`, `select`, `join`, ...) is unchanged and lazy. The operations that run it return a `Result`, reported the same way as SQL since predicates and expressions are caller-supplied strings:

| Operation | 2.0 return type |
| --- | --- |
| `execute`, `head`, `tail` | `Future<Result<Table, ErrorDetails>>` |
| `executeStream` | `Stream<Result<Table, ErrorDetails>>`, ending after an `Err` |
| `toList` | `Future<Result<List<Map<String, dynamic>>, ErrorDetails>>` |
| `toJsonString`, `toCsv`, `toJson`, `toJsonLines` | `Future<Result<String, ErrorDetails>>` |
| `count` | `Future<Result<int, ErrorDetails>>` |
| `show` | `Future<Result<void, ErrorDetails>>` |
| `cache` | `Future<Result<DataFrame, ErrorDetails>>` |

Before:

```dart
final table = await df.filter('age > 30').execute(maxRows: 1000);
```

After:

```dart
switch (await df.filter('age > 30').execute(maxRows: 1000)) {
  case Ok(:final value):
    use(value);
  case Err(:final error) when error.kind == Kind.limit:
    print('too many rows: ${error.message}');
  case Err(:final error):
    print('failed: $error');
}
```

An invalid predicate or expression is `Kind.invalid`, a `maxRows`/`maxBytes` or operator cap is `Kind.limit`, `EngineOptions.timeout` is `Kind.timeout`, and a lazily read file that turns out unreadable is `Kind.corrupt`.

## DataFrame expressions

`filter`/`where` predicates and `withColumn` expressions are SQL scalar expressions, parsed by the SQL engine's grammar and bound to the frame's columns as a `WHERE` clause or a `SELECT` item is. The 1.x languages were two small regex grammars. Most simple predicates (`age >= 18`, `dept = 'sales'`, `id >= 2 AND id <= 4`) mean the same; the changes are below, the first of them silent.

**Standard precedence in `withColumn`.** This changes results without an error. In 1.x operators applied left to right; `*`, `/` and `%` now bind tighter than `+` and `-`.

Before (1.x evaluated `(salary + 10) * 2`):

```dart
df.withColumn('adjusted', 'salary + 10 * 2');
```

After (2.0 evaluates `salary + 20`; write the parentheses to keep the 1.x result):

```dart
df.withColumn('adjusted', '(salary + 10) * 2');
```

**Names with spaces or `-` are quoted identifiers.**

Before:

```dart
df.filter('First Name == "Alice"');
df.filter('Special-Column == "A"');
```

After:

```dart
df.filter('"First Name" = \'Alice\'');
df.filter('"Special-Column" = \'A\'');
```

**Double quotes are identifiers; strings are single-quoted.** A double-quoted value is read as a column name, so it fails with `Kind.invalid` (`Unknown column: ...`) where 1.x compared with a string.

Before:

```dart
df.filter('dept == "eng"');
df.filter('t > "2024-06-01"');
```

After:

```dart
df.filter("dept = 'eng'");
df.filter("t > '2024-06-01'");
```

**`==` is `=`.** It is still accepted, in DataFrame expressions and now in SQL as well (`WHERE dept == 'eng'`).

**Brackets are gone.** `[name]` was an optional column quote in 1.x predicates; it is now a parse error (`Kind.invalid`).

Before:

```dart
df.filter('[age]>=18');
```

After:

```dart
df.filter('age >= 18');
```

**An unquoted word is a column.** In 1.x the right-hand side of a comparison was always a literal, so `a > b` compared `a` with the string `'b'` and `age < thirty` failed as a non-number. Now `b` and `thirty` are columns: `a > b` compares two columns, and an unknown name is `Kind.invalid` (`Unknown column: thirty`).

**Grouped predicates no longer need chained `filter` calls.** Parentheses, `NOT`, `IS [NOT] NULL`, `IN (...)`, `BETWEEN`, `LIKE`, `CASE`, `CAST`, function calls, arithmetic on either side, and literals on the left are available.

Before:

```dart
df.filter('a == 1 OR a == 4').filter('b == 3');
```

After:

```dart
df.filter('(a = 1 OR a = 4) AND b = 3');
```

**Errors as SQL reports them.** A malformed expression, an unknown column, a string compared or combined with a number, and an aggregate function (`filter('SUM(x) > 1')`, `withColumn('t', 'SUM(x)')`) are `Kind.invalid`; a subquery is `Kind.unsupported`. A predicate that is not boolean is cast to `BOOLEAN`, as SQL `WHERE` does: `filter('age')` keeps the rows where `age` is nonzero. As before, the builders never throw and the terminal operation returns the `Err`.

## DataFrame results

DataFrame operations are planned and run by the SQL engine's planner, so a frame and the equivalent query give the same rows in the same order.

**`orderBy` puts NULLs last in both directions**, as SQL `ORDER BY` does (DuckDB's default). In 1.x an ascending sort put them first.

Before (1.x):

```dart
df.orderBy('x'); // x: null, 3, 5
```

After:

```dart
df.orderBy('x'); // x: 3, 5, null
```

To keep NULLs first, write the order in SQL: `df.sql('SELECT * FROM df ORDER BY x NULLS FIRST')`. A descending sort is unchanged.

**`join` qualifies a colliding column by its side**, as SQL `JOIN ... USING` does. The key is still one column; a non-key column both sides have is named after each side's table (a file-backed frame uses the file's base name). In 1.x both copies kept the bare name.

Before (1.x):

```dart
// people (table 'p') and depts (table 'd') both have `name`
people.join(depts, 'dept').columns; // [id, name, dept, name]
```

After:

```dart
people.join(depts, 'dept').columns; // [id, p.name, dept, d.name]
```

When both sides have the same table name (two `DataFrame.fromRows` frames are both `dataframe`, or a frame joined to itself), the qualifiers are `left` and `right`: `left.name`, `right.name`. Rename a column before the join to choose its name yourself.

The operations that take column names (`select`, `orderBy`, `drop`, `rename`, `groupBy`, `agg`, `withColumn`, `join`) take the qualified names and keep them: `joined.select(['p.name'])` has a column named `p.name`. A bare `name` both sides have fails with `Kind.invalid` naming the qualified choices.

**`union` with a different column count fails with `Kind.schema`**, and SQL `UNION`, `INTERSECT` and `EXCEPT` fail with the same kind and message. In 1.x a DataFrame union threw or dropped the extra columns. `union`'s `schema` now reports the promoted type of each column (an `int64` column unioned with a `float64` one is `float64`), as the result has.

**The file readers return lazy frames.** `readCsv`, `readJson`, `readJsonLines`, `readParquet` (with or without `filter`/`columns`), `readArrow`, `readArrowStream` over an IPC file, `readDelta`, `readIceberg` and `readDataset` read the schema when called and the data when a terminal runs, with the reader's options. In 1.x all but `readParquet` without options decoded the file when called.

What moves to the terminal: an error found past the schema (a value that does not parse, a JSON record with a field the inference sample lacked, an unreadable or missing data file, a data or delete file outside the table) and the reader's `maxRows`/`maxBytes`. What stays at the reader: a missing file, an unreadable header or footer, a directory that is not a table, an unknown version or snapshot, an unknown name in `columns`.

Before (1.x):

```dart
final read = await DataFrameIO.readCsv(path, maxRows: 1000); // Err(Kind.limit) here
```

After:

```dart
if (await DataFrameIO.readCsv(path, maxRows: 1000) case Ok(value: final df)) {
  final rows = await df.execute(); // Err(Kind.limit) past 1000 result rows
  final all = await df.count();    // Ok: count and executeStream set no result cap
}
```

The reader's caps bound every materialized result of the frame (and of a frame that joins or unions it) together with the terminal's caps, the tighter applying; they bound the result, not the rows scanned, so `df.filter(...)` under the cap succeeds. A Delta or Iceberg frame without a version or snapshot is fixed to the version or snapshot current at the reader call, so a commit made after it does not change the frame's result; call the reader again to see the commit. A file-backed frame is named after its file in join qualifiers and in its result (`p.csv` is `p`; a glob dataset is `dataset`), where it was `csv`, `json_data`, `arrow_table`, `delta`, `iceberg` or `dataset`.

These are still read when called: CSV with a custom `lineTerminator` or `maxFileBytes`, JSON with `expandArrays` or with `maxRows: null, maxBytes: null`, an Arrow IPC stream, Delta with `concurrency` above 1, a dataset with `concurrency` above 1 and both caps null, and an empty file.

**`registerDataFrame` and `DataFrame.sql` no longer run the frame.** The frame is registered as a view: each query that reads it plans the frame's operations with the query's, so a `WHERE` on a file-backed frame prunes its row groups and only the columns the query uses are read. Each query reads the frame's sources again, so a change to a source file shows in the next query (in 1.x the frame was read once, at registration). An invalid expression or unknown column still fails `registerDataFrame`; a source file removed after registration fails the query.

`maxRows`/`maxBytes` move from the registration to the queries: they bound the result of each `execute` that reads the frame, the tighter of them, the caps of the readers the frame is built on and the query's own applying. `executeStream` sets no result cap.

Before (1.x):

```dart
await engine.registerDataFrame('r', df, maxRows: 1000); // Err(Kind.limit) for a 5000-row frame
```

After:

```dart
await engine.registerDataFrame('r', df, maxRows: 1000);      // Ok
await engine.execute('SELECT COUNT(*) AS n FROM r');          // Ok: one row
await engine.execute('SELECT * FROM r');                      // Err(Kind.limit) past 1000 rows
```

## Reads

Every read returns a `Result`: `DataFrameIO.read*` and `from*` give `Result<DataFrame, ErrorDetails>`, the reader classes (`ParquetReader`, `CsvReader`, `JsonReader`, `DeltaTable`, `IcebergTable`, `Dataset`) give `Result<Table, ErrorDetails>` (or `Result<Schema, ErrorDetails>` for `ParquetReader.schema`), and the `*Batches` streams yield `Result` items, ending after an `Err`.

Before:

```dart
try {
  final df = await DataFrameIO.readParquet(path);
  use(df);
} on FormatException catch (e) {
  print('corrupt: ${e.message}');
} on FileSystemException {
  print('missing');
}
```

After:

```dart
switch (await DataFrameIO.readParquet(path)) {
  case Ok(:final value):
    use(value);
  case Err(:final error) when error.kind == Kind.missing:
    print('missing');
  case Err(:final error):
    print('${error.kind.name}: ${error.message}');
}
```

| Outcome | 1.x | 2.0 `Kind` |
| --- | --- | --- |
| No such file, Delta log, Iceberg metadata, version or snapshot | `FileSystemException`, `ArgumentError`, `StateError` | missing |
| A malformed file, page, log or metadata | `FormatException`, `StateError` | corrupt |
| Past `maxRows`, `maxBytes`, a plan, block or delete-index cap | `StateError` | limit |
| An unknown projected or filtered column | `ArgumentError` | invalid |
| Column types that differ across `Dataset` files | `ArgumentError` | schema |
| A Delta or Iceberg data path outside the table root | `ArgumentError` | denied |
| A Delta reader feature, Iceberg format version or file format this library does not read | `UnsupportedError` | unsupported |
| Any other file-system failure | `FileSystemException` | io |

An unexpected error inside a read (a `RangeError` or `TypeError` from the decoder) is also returned, as `Kind.corrupt` with the error as its `cause`, and logged. Calling a `*Batches` method with `batchSize <= 0` is an argument precondition and still throws `ArgumentError`.

## Writes

The file writers return `Result<void, ErrorDetails>`: `ParquetWriter.write`, `open`, `writeBatch`, `writeBatches` and `close`; `CsvWriter.writeFile`, `writeStream`, `writeTable` and `writeSink`; `JsonWriter.writeFile`, `writeJsonLinesToStream` and `writeTable`; `Table.writeJson` and `writeJsonLines`; and `DataFrame.writeCsv`, `writeJson`, `writeJsonLines`, `writeArrow`, `writeArrowStream` and `DataFrameIO.writeParquetStream`. Every write goes to a temporary file that replaces the destination only on success, so an `Err` leaves no partial file; an `Err` from `ParquetWriter.writeBatch` also abandons the write.

Before:

```dart
try {
  await ParquetWriter(path).write(table);
} on FileSystemException catch (e) {
  print('could not write: $e');
}
```

After:

```dart
if (await ParquetWriter(path).write(table) case Err(:final error)) {
  print('could not write: $error');
}
```

| Outcome | 1.x | 2.0 `Kind` |
| --- | --- | --- |
| The destination cannot be created, written or replaced | `FileSystemException` | io |
| A value its Parquet column type would truncate (an int32 column holding a wider value) | `ArgumentError` | invalid |
| A streamed batch whose schema differs from the first | `ArgumentError`, `StateError` | schema |
| A compression codec or Parquet version this writer cannot produce; a column type Arrow IPC cannot carry | `ArgumentError`, `UnsupportedError` | unsupported |
| The DataFrame being written fails | as its terminal | as [DataFrame terminals](#dataframe-terminals) |

`ParquetWriterOptions(rowGroupSize: 0)` and calling `ParquetWriter.open` twice are programming errors and still throw.

## Engines, catalogs and the CLI

`SqlEngine.open(...)` replaces the `SqlEngine(...)` constructor, with the same named parameters. It returns `Result<SqlEngine, ErrorDetails>` synchronously, `Kind.io` when the spill session directory cannot be created.

Before:

```dart
final engine = SqlEngine(sandboxRoot: root);
await engine.registerFile('people', 'people.csv');
final table = await engine.execute('SELECT * FROM people');
```

After:

```dart
final SqlEngine engine;
switch (SqlEngine.open(sandboxRoot: root)) {
  case Ok(:final value):
    engine = value;
  case Err(:final error):
    return report(error);
}
if (await engine.registerFile('people', 'people.csv') case Err(:final error)) {
  return report(error);
}
final result = await engine.execute('SELECT * FROM people');
```

| Member | 2.0 |
| --- | --- |
| `SqlEngine(...)` | `SqlEngine.open(...)`, returning `Result<SqlEngine, ErrorDetails>` |
| `SqlEngine.registerFile` | `Future<Result<void, ErrorDetails>>`: `denied`, `missing`, `invalid` (unknown extension), `corrupt`, `timeout` |
| `SqlEngine.registerDataFrame` | `Future<Result<void, ErrorDetails>>`: the DataFrame's failure found by planning it (`invalid`), or `timeout`; `maxRows`/`maxBytes` bound the results of queries that read it (`limit` there) |
| `SqlEngine.dispose` | `Future<Result<void, ErrorDetails>>`: `timeout` when operations do not drain, `io` deleting the spill directory |
| top-level `sql` | A function, `sql(query, {timeout})`, returning `Result<Table, ErrorDetails>` from a shared engine opened on first use. In 1.x `sql` was the shared `SqlEngine` (`sql.execute(...)`); to register tables, open an engine. |
| `DataFrame.sql` (extension) and `SqlDataFrame.createEngine` | `Result<DataFrame, ErrorDetails>` and `Result<SqlEngine, ErrorDetails>` |
| `LocalCatalog.lookup` | `Future<Result<Map<String, dynamic>?, ErrorDetails>>`: `corrupt`, `io` |
| `LocalCatalog.guarded` | `Future<Result<T, ErrorDetails>>`: `corrupt`, `io`, or the body's failure |
| `IcebergCatalog.current` (all implementations) | `Future<Result<Map<String, dynamic>?, ErrorDetails>>`: `limit`, `corrupt`, `io` |
| `CommitConflict`, `DeltaCommitIndeterminate`, `IcebergCommitConflict`, `IcebergCommitIndeterminate` | No longer exported; match `Kind.conflict` and `Kind.indeterminate` instead |

The `sql` command-line tool prints `Error: <kind>: <message>` and exits 1 when a `--csv` or `--parquet` file cannot be loaded, and `Query failed: <kind>: <message>` for a failed query.

## ORDER BY and GROUP BY positions

SQL `ORDER BY` and `GROUP BY` take a column name, a select alias or an
expression, not a position. 1.x read `ORDER BY 2` and `GROUP BY 1` as
select-list columns; 2.0 reports `Kind.invalid`.

```sql
-- 1.x
SELECT department, AVG(salary) FROM employees GROUP BY 1 ORDER BY 2 DESC;
-- 2.0
SELECT department, AVG(salary) AS average FROM employees
GROUP BY department ORDER BY average DESC;
```

## Column internals

The column types build through copying factories and read through safe members;
the members that handed out or adopted the backing buffers are no longer public.

| 1.x | 2.0 |
| --- | --- |
| `NumericColumn.adopt(data: buffer, ...)` | `NumericColumn.int64(name: ..., values: ...)` (or the `int8`/`int16`/`int32`/`float32`/`float64` factory), which copies |
| `numbers.rawBuffer` | `numbers.buffer`, an unmodifiable view of the same data |
| `numbers.rawNullBitmap` | `numbers.nullBitmap`, an unmodifiable view; or `isNullAt(i)` |
| `StringColumn.adopt(offsets: ..., bytes: ...)` | `StringColumn(name: ..., values: ...)` |
| `strings.encode()` | none: the dictionary codes are an engine detail; use `interned()` for a column that repeats values |
| `BoolColumn.adopt(bits: ...)` | `BoolColumn(name: ..., values: ...)` |
| `flags.rawBits`, `flags.rawNullBitmap` | `flags[i]`, `flags.isNullAt(i)`, `flags.toList()` |

The public column types are `abstract final`: they cannot be extended or
implemented outside the library, and `Column` stays `sealed`.

## Renamed members

| 1.x | 2.0 |
| --- | --- |
| `DataFrameIO.readCsvOptimized(path, ...)` | `DataFrameIO.readCsv(path, ...)`: passing `filter`, `columns`, `limit` or `batchSize` selects the batch scan |
| `SqlEngine.getTables()` | `SqlEngine.tables` |
| `SqlEngine.getTableSchema(name)` | `SqlEngine.schema(name)` |
| `CsvWriter.writeTableToStream(table, sink)` | `CsvWriter.writeSink(table, sink)` |

## Inventory

Each row names a member exported from `package:libbigdata/libbigdata.dart`, what it threw in 1.x for an expected outcome, and the `Kind` it reports in 2.0. A member that can fail in several ways has one row per outcome. Rows marked "(check)" were reviewed during the conversion; where they stayed thrown `Error`s, the sections above say so. The headings name the conversion tasks.

### T41: Delta commit chain

| Member | Throws in 1.x | Kind |
| --- | --- | --- |
| `DeltaCommitCoordinator.commit` | `CommitConflict` when the version is lost to another writer (contract for all implementations) | conflict |
| `DeltaCommitCoordinator.commit` | `DeltaCommitIndeterminate` when the publish outcome is unknown (contract for all implementations) | indeterminate |
| `FilesystemDeltaCommitCoordinator.commit` | `CommitConflict` after `maxRetries` contended or lost commit attempts | conflict |
| `FilesystemDeltaCommitCoordinator.commit` | `DeltaCommitIndeterminate` from `DeltaLog.commit` when the rename may have landed | indeterminate |
| `FilesystemDeltaCommitCoordinator.commit` | `FileSystemException` from claim, write or rename of the commit file | io |
| `FileLockDeltaCommitCoordinator.commit` | `TimeoutException` when `timeout` is set and the OS commit lock is not acquired in time | timeout |
| `FileLockDeltaCommitCoordinator.commit` | Everything the inner coordinator throws (`CommitConflict`, `DeltaCommitIndeterminate`, `FileSystemException`) | conflict, indeterminate, io |
| `LocalCatalogDeltaCommitCoordinator.commit` | `DeltaCommitIndeterminate` when the commit published but the registry pointer write failed | indeterminate |
| `LocalCatalogDeltaCommitCoordinator.commit` | `FormatException` for a corrupt or oversized catalog registry file | corrupt |
| `LocalCatalogDeltaCommitCoordinator.commit` | `CommitConflict` and `FileSystemException` passed through from the inner coordinator | conflict, io |
| `DeltaWriter.write` | `ArgumentError` when `partitionBy` names every column | invalid |
| `DeltaWriter.write` | `UnsupportedError` for an unsupported reader/writer protocol feature, or `delta.invariants` with no evaluator | unsupported |
| `DeltaWriter.write` | `StateError` when an incoming row violates a declared `delta.invariants` predicate | invalid |
| `DeltaWriter.write` | `StateError` for an append whose schema or partition columns differ from the committed table | schema |
| `DeltaWriter.write` | `StateError` from `Table.splitByKeys` when distinct partitions exceed `maxPartitions` | limit |
| `DeltaWriter.write` | `CommitConflict` / `DeltaCommitIndeterminate` / `TimeoutException` from the coordinator | conflict, indeterminate, timeout |
| `DeltaWriter.write` | `FileSystemException` writing data files or the log; `StateError` for log replay over the metadata byte cap | io, limit |
| `DeltaWriter.delete` | `ArgumentError` / `StateError` from `DeltaLog.snapshot` (not a Delta table, no commits) | missing |
| `DeltaWriter.delete` | `StateError` for a non-contiguous, truncated or 0-byte log in the replay range | corrupt |
| `DeltaWriter.delete` | `UnsupportedError` for an unsupported protocol or `delta.appendOnly=true` | unsupported, denied |
| `DeltaWriter.delete` | `StateError` when a concurrent commit changed the table since it was read; `CommitConflict` from the coordinator | conflict |
| `DeltaWriter.delete` | `DeltaCommitIndeterminate`, `TimeoutException`, `FileSystemException` from the coordinator and I/O | indeterminate, timeout, io |
| `DeltaWriter.update` | `ArgumentError` when an assignment names a column the table lacks | invalid |
| `DeltaWriter.update` | `UnsupportedError` for a partitioned table, `delta.appendOnly`, invariants or an unsupported protocol | unsupported, denied |
| `DeltaWriter.update` | `StateError` when a concurrent commit changed the table; `CommitConflict` from the coordinator | conflict |
| `DeltaWriter.update` | Snapshot failures as for `delete`, plus `DeltaCommitIndeterminate`, `TimeoutException`, `FileSystemException` | invalid, corrupt, indeterminate, timeout, io |
| `DeltaWriter.merge` | `ArgumentError` for an empty `on` list or a join key missing from source or target | invalid |
| `DeltaWriter.merge` | `StateError` when the source exceeds `maxSourceRows` or `maxSourceBytes` | limit |
| `DeltaWriter.merge` | `StateError` for a cardinality violation (several source rows match one key) | invalid |
| `DeltaWriter.merge` | `UnsupportedError` for a partitioned table, `delta.appendOnly`, invariants or an unsupported protocol | unsupported, denied |
| `DeltaWriter.merge` | `StateError` when a concurrent commit changed the table; `CommitConflict` from the coordinator | conflict |
| `DeltaWriter.merge` | Snapshot failures as for `delete`, plus `DeltaCommitIndeterminate`, `TimeoutException`, `FileSystemException` | invalid, corrupt, indeterminate, timeout, io |
| `SqlInvariantEvaluator.violations` | Everything `SqlEngine.execute` throws for an invalid invariant predicate (it runs the predicate through the engine) | invalid |

### T42: Delta writes through DataFrameIO and SQL DML

| Member | Throws in 1.x | Kind |
| --- | --- | --- |
| `DataFrameIO.writeDelta` | Everything `DeltaWriter.write` throws (invariant violation, schema mismatch, partition cap, conflict, indeterminate, I/O) | invalid, schema, limit, conflict, indeterminate, io |
| `DataFrameIO.writeDelta` | Everything `DataFrame.execute` throws while materializing the frame (row/byte cap, bad expression) | limit, invalid |
| `SqlEngine.execute` (UPDATE) | `ArgumentError` "Access denied" for a target path outside the sandbox, or any write on a `readOnly` engine | denied |
| `SqlEngine.execute` (UPDATE) | `FormatException` for a non-boolean WHERE, an invalid SET expression, or a non-exact value into a decimal column | invalid |
| `SqlEngine.execute` (UPDATE) | Everything `DeltaWriter.update` throws | invalid, corrupt, conflict, indeterminate, timeout, io |
| `SqlEngine.execute` (INSERT) | `StateError` when the target is not an existing Delta table | invalid |
| `SqlEngine.execute` (INSERT) | `ArgumentError` for an unknown or repeated column, a wrong value count, or a value that cannot be stored in the column type | schema |
| `SqlEngine.execute` (INSERT) | Everything `DeltaWriter.write` throws (append mode) | invalid, schema, conflict, indeterminate, io |
| `SqlEngine.execute` (MERGE) | `StateError` when the MERGE source query exceeds the materialization cap | limit |
| `SqlEngine.execute` (MERGE) | Source read failures (`FileSystemException`, `FormatException`, `StateError` wrapper) for a file source | io, corrupt |
| `SqlEngine.execute` (MERGE) | Everything `DeltaWriter.merge` throws | invalid, limit, conflict, indeterminate, io |
| `SqlEngine.executeStream` (UPDATE/INSERT/MERGE) | Same as `SqlEngine.execute` for DML; the error surfaces on the stream | as above |

### T43: Iceberg commit chain

| Member | Throws in 1.x | Kind |
| --- | --- | --- |
| `IcebergBackend.commit` | `IcebergCommitConflict` / `IcebergCommitIndeterminate` (contract for all implementations) | conflict, indeterminate |
| `FilesystemIcebergBackend.commit` | `IcebergCommitConflict` after the attempt or deadline bound, a stale claim, or a reclaimed marker | conflict |
| `FilesystemIcebergBackend.commit` | `IcebergCommitIndeterminate` when the metadata rename may have published | indeterminate |
| `FilesystemIcebergBackend.commit` | `StateError` when current metadata exceeds the byte cap; `FormatException` for undecodable metadata JSON | limit, corrupt |
| `FilesystemIcebergBackend.commit` | `FileSystemException` staging or publishing metadata | io |
| `FileLockIcebergBackend.commit` | Same as `FilesystemIcebergBackend.commit`, under an OS file lock (no timeout is exposed) | conflict, indeterminate, limit, corrupt, io |
| `LocalCatalogIcebergBackend.commit` | `IcebergCommitIndeterminate` when metadata published but the registry pointer write failed | indeterminate |
| `LocalCatalogIcebergBackend.commit` | `FormatException` for a corrupt or oversized catalog registry file | corrupt |
| `LocalCatalogIcebergBackend.commit` | Inner coordinator failures (`IcebergCommitConflict`, oversized metadata, `FileSystemException`) | conflict, limit, io |
| `IcebergWriter.write` | `StateError` for an append whose schema differs from the table schema | schema |
| `IcebergWriter.write` | `StateError` when the schema or partition spec changed concurrently; `IcebergCommitConflict` | conflict |
| `IcebergWriter.write` | `IcebergCommitIndeterminate` from the backend (staged files are retained) | indeterminate |
| `IcebergWriter.write` | `StateError` when new metadata would exceed `maxMetadataBytes`, or partitions exceed `maxPartitions` | limit |
| `IcebergWriter.write` | `FormatException` for metadata naming a missing snapshot, schema, manifest list or partition field | corrupt |
| `IcebergWriter.write` | `StateError` when cleanup of staged files fails after a failed commit; `FileSystemException` | io |
| `IcebergWriter.expireSnapshots` | `ArgumentError` when `retain < 1` (check) | invalid |
| `IcebergWriter.expireSnapshots` | `StateError` when there is no Iceberg table at the path | invalid |
| `IcebergWriter.expireSnapshots` | `StateError` when the table disappeared mid-commit; `IcebergCommitConflict` | conflict |
| `IcebergWriter.expireSnapshots` | `FormatException` when a retained snapshot references a missing manifest list or manifest | corrupt |
| `IcebergWriter.expireSnapshots` | `FileSystemException` when expired files could not be deleted (now orphaned) | io |
| `IcebergWriter.expireSnapshots` | `StateError` for metadata over the byte cap unless `allowOversizedMetadata`; `IcebergCommitIndeterminate` | limit, indeterminate |

### T44: Iceberg writes through DataFrameIO

| Member | Throws in 1.x | Kind |
| --- | --- | --- |
| `DataFrameIO.writeIceberg` | Everything `IcebergWriter.write` throws (schema mismatch, conflict, indeterminate, metadata cap, corrupt metadata, I/O) | schema, conflict, indeterminate, limit, corrupt, io |
| `DataFrameIO.writeIceberg` | Everything `DataFrame.execute` throws while materializing the frame | limit, invalid |

### T45: SQL execution

| Member | Throws in 1.x | Kind |
| --- | --- | --- |
| `SqlEngine.execute` | `FormatException` for a SQL parse error, nesting too deep, an invalid literal or an unknown column | invalid |
| `SqlEngine.execute` | `StateError` "Table not found" / "File not found or could not be loaded"; `StateError` for a recursive view | missing, invalid |
| `SqlEngine.execute` | `UnsupportedError` for unsupported SQL (WITH RECURSIVE, window functions, unknown function, complex join) | unsupported |
| `SqlEngine.execute` | `ArgumentError` for CREATE VIEW of an existing view or DROP VIEW of a missing one | invalid, missing |
| `SqlEngine.execute` | `ArgumentError.value` for a file path with an unsupported extension | invalid |
| `SqlEngine.execute` | `ArgumentError` "Access denied" for an absolute, `..` or symlink-escaping path, or a write on a `readOnly` engine | denied |
| `SqlEngine.execute` | `StateError` when the result exceeds `maxRows`/`maxBytes`, or spill exceeds `maxSpillBytes` | limit |
| `SqlEngine.execute` | `TimeoutException` past the deadline; `StateError` when `lockTimeout` expires waiting for a prior query | timeout |
| `SqlEngine.execute` | `FileSystemException` reading a referenced file; `StateError` when a finalized spill run is missing | io |
| `SqlEngine.execute` | `FormatException` or wrapped `StateError` "Failed to read ... file" for a malformed data file | corrupt |
| `SqlEngine.sql` | Same as `SqlEngine.execute` (it wraps it) | invalid, denied, limit, timeout, io, corrupt |
| `SqlEngine.executeStream` | Same planning failures as `SqlEngine.execute`, delivered as stream errors | invalid, denied, io, corrupt |
| `SqlEngine.executeStream` | `TimeoutException` past the deadline (including consumer time); `StateError` for spill or output limits | timeout, limit |

### T46: DataFrame terminal operations

| Member | Throws in 1.x | Kind |
| --- | --- | --- |
| `DataFrame.execute` | `StateError` when the result or an unfused intermediate exceeds `maxRows`/`maxBytes` | limit |
| `DataFrame.execute` | `ArgumentError` for an invalid filter predicate, unknown operator, unknown column or unsupported aggregation | invalid |
| `DataFrame.execute` | `TimeoutException` when `EngineOptions.timeout` expires; `StateError` for spill budget overrun | timeout, limit |
| `DataFrame.execute` | File errors from a lazy `readParquet` scan (`FileSystemException`, `FormatException`) | io, corrupt |
| `DataFrame.executeStream` | Same as `execute` except the row/byte caps, delivered as stream errors | invalid, timeout, limit, io, corrupt |
| `DataFrame.toList` | Same as `execute` (default caps) | limit, invalid, timeout, io, corrupt |
| `DataFrame.toJsonString` | Same as `execute` (default caps) | limit, invalid, timeout, io, corrupt |
| `DataFrame.head` | Same as `execute` | limit, invalid, timeout, io, corrupt |
| `DataFrame.tail` | Same as `execute` (materializes the whole result first) | limit, invalid, timeout, io, corrupt |
| `DataFrame.count` | Same as `executeStream` (no caps) | invalid, timeout, limit, io, corrupt |
| `DataFrame.show` | Same as `execute` | limit, invalid, timeout, io, corrupt |
| `DataFrame.cache` | Same as `execute` | limit, invalid, timeout, io, corrupt |

### T47: Read paths

| Member | Throws in 1.x | Kind |
| --- | --- | --- |
| `DataFrameIO.readCsv` / `fromCsvString` | Everything `CsvReader.readFile` / `readString` throws | io, corrupt, invalid, limit |
| `DataFrameIO.readCsvOptimized` | `StateError` when the result exceeds `maxRows`/`maxBytes`; plus `CsvReader.readBatches` failures | limit, io, corrupt, invalid |
| `DataFrameIO.readCsvBatches` | Everything `CsvReader.readBatches` throws, as stream errors | io, corrupt, invalid |
| `DataFrameIO.readJson` / `readJsonLines` / `fromJsonString` / `fromJsonLinesString` | Everything `JsonReader.readFile` / `readString` throws | io, corrupt, invalid, limit |
| `DataFrameIO.readJsonBatches` | Everything `JsonReader.readBatches` throws, as stream errors | io, corrupt, invalid |
| `DataFrameIO.readParquet` | `FileSystemException`; `FormatException` for bad magic, footer or codec; `ArgumentError` for unknown `columns` | io, corrupt, invalid |
| `DataFrameIO.readDataset` / `readDatasetBatches` | Everything `Dataset.read` / `readBatches` throws | io, schema, invalid, limit, corrupt |
| `DataFrameIO.readDelta` | Everything `DeltaTable.read` throws | invalid, corrupt, denied, limit, io |
| `DataFrameIO.readIceberg` | Everything `IcebergTable.read` throws | invalid, corrupt, denied, limit, io |
| `DataFrameIO.readArrow` / `readArrowStream` / `readArrowBatches` | `FormatException` for a missing magic, bad footer or unsupported compression; `StateError` over `maxBlockBytes`; `FileSystemException` | corrupt, limit, io |
| `ParquetReader.schema` | `FileSystemException`; `FormatException` for bad magic, footer length, version or empty schema | io, corrupt |
| `ParquetReader.read` | `StateError` over `maxRows`/`maxBytes`; `ArgumentError` for unknown projected columns | limit, invalid |
| `ParquetReader.read` | `FormatException` for an unsupported codec or corrupt page data; `FileSystemException` | corrupt, io |
| `ParquetReader.readBatches` | `StateError` for a nested row group over the streaming ceiling; corrupt data and I/O as for `read` | limit, corrupt, io |
| `CsvReader.readFile` | `FileSystemException` "File not found"; `StateError` over `maxBytes`/`maxRows` | missing, limit |
| `CsvReader.readFile` | `FormatException` for malformed CSV or a value that does not parse as the column type | corrupt |
| `CsvReader.readFile` | `ArgumentError` when `columnNames` length differs from the CSV, or a selected column is unknown | invalid |
| `CsvReader.readString` | `StateError` over `maxBytes`; `FormatException` and `ArgumentError` as for `readFile` | limit, corrupt, invalid |
| `CsvReader.readBatches` | `FileSystemException`; `UnsupportedError` for an unsupported option; `ArgumentError` for an invalid `filter` or selection | io, unsupported, invalid |
| `CsvReader` (constructor via `CsvOptions`) | `ArgumentError` when a delimiter, quote or escape is not one character (check) | invalid |
| `JsonReader.readFile` / `readStream` | `FileSystemException`; `FormatException` for invalid JSON; `StateError` over `maxRows`/`maxBytes` | io, corrupt, limit |
| `JsonReader.readFile` | `ArgumentError` for unknown projected columns | invalid |
| `JsonReader.readString` | `FormatException` for invalid JSON or a non-object root; `StateError` over the caps | corrupt, limit |
| `JsonReader.readBatches` | `FileSystemException`; `UnsupportedError` for `expandArrays`; `FormatException` for an invalid line | io, unsupported, corrupt |
| `DeltaTable.read` | `ArgumentError` "Not a Delta table", unknown version or no version before timestamp; `StateError` no commits | missing |
| `DeltaTable.read` | `StateError` for a non-contiguous, truncated or 0-byte log, or missing metaData | corrupt |
| `DeltaTable.read` | `UnsupportedError` for an unsupported reader feature | unsupported |
| `DeltaTable.read` | `ArgumentError` "Access denied" for a data or deletion-vector path outside the table root | denied |
| `DeltaTable.read` | `StateError` over `maxRows`/`maxBytes`, the log byte cap or the deletion-vector cap | limit |
| `DeltaTable.read` | `FileSystemException` and Parquet decode failures for data files | io, corrupt |
| `DeltaTable.readBatches` | Same as `read` except the result caps, as stream errors | invalid, corrupt, denied, io |
| `IcebergTable.read` | `ArgumentError` "Not an Iceberg table"; `StateError` for an unknown or expired snapshot | invalid |
| `IcebergTable.read` | `UnsupportedError` for format-version other than 1 or 2, or a non-Parquet data/delete file | unsupported |
| `IcebergTable.read` | `FormatException` for metadata naming a missing schema or snapshot, or an equality delete with no ids | corrupt |
| `IcebergTable.read` | `StateError` over `maxPlanEntries`, the delete-index budget, the metadata cap or `maxRows`/`maxBytes` | limit |
| `IcebergTable.read` | `ArgumentError` "Access denied" for a path outside the table root; `FileSystemException` | denied, io |
| `IcebergTable.readBatches` | Same as `read` except the result caps, as stream errors | invalid, corrupt, limit, denied, io |
| `Dataset.read` | `ArgumentError` "Dataset path not found" | missing |
| `Dataset.read` | `ArgumentError` for incompatible column types or nested shapes across files | schema |
| `Dataset.read` | `ArgumentError` for unknown projected columns; `StateError` over `maxRows`/`maxBytes` | invalid, limit |
| `Dataset.read` | Per-file reader failures (`FileSystemException`, `FormatException`) | io, corrupt |
| `Dataset.readBatches` | Same as `read` except the result caps, as stream errors | io, schema, invalid, corrupt |

### T47a: Writers

| Member | Throws in 1.x | Kind |
| --- | --- | --- |
| `ParquetWriter.write` / `open` | `ArgumentError` for an unsupported compression codec, `rowGroupSize <= 0` or unsupported format version | invalid |
| `ParquetWriter.write` / `open` / `close` | `FileSystemException` creating, writing or renaming the file | io |
| `ParquetWriter.open` | `StateError` when a write is already open (check) | invalid |
| `ParquetWriter.writeBatch` / `writeBatches` | `ArgumentError` when a batch schema does not match the writer schema | schema |
| `ParquetWriter.writeBatch` / `writeBatches` | `FileSystemException` | io |
| `CsvWriter.writeFile` / `writeStream` | `StateError` when a later batch schema is not compatible with the first | schema |
| `CsvWriter.writeFile` / `writeTable` / `writeTableToStream` | `FileSystemException` | io |
| `JsonWriter.writeFile` / `writeJsonLinesToStream` / `writeTable` | `FileSystemException` | io |
| `TableJsonWriterExtension.writeJson` / `writeJsonLines` | `FileSystemException` | io |
| `DataFrameIO.writeParquetStream` | Everything `ParquetWriter.writeBatches` throws, plus `DataFrame.execute` failures per batch | schema, invalid, io |
| `DataFrameIO.writeArrow` | `ArgumentError` for `batchSize <= 0` (check); `UnsupportedError` for an unsupported column type (check); `FileSystemException` | invalid, unsupported, io |
| `DataFrameIO.writeArrow` | `DataFrame.execute` failures (default caps) | limit, invalid |
| `DataFrameIO.writeArrowStream` | `ArgumentError` when a batch schema does not match the writer schema; `FileSystemException` | schema, io |
| `DataFrameIO.writeCsv` | `CsvWriter.writeFile` failures plus `DataFrame.executeStream` failures | schema, io, invalid |
| `DataFrameIO.writeJson` / `writeJsonLines` | `JsonWriter.writeFile` failures plus `DataFrame.executeStream` failures | io, invalid |
| `DataFrameIO.toCsv` / `toJson` / `toJsonLines` | `StateError` over `maxRows`/`maxBytes` plus `DataFrame.execute` failures (these execute the plan; could move to T46) | limit, invalid |

### T47b: Catalogs, registration, CLI and exception types

| Member | Throws in 1.x | Kind |
| --- | --- | --- |
| `LocalCatalog.lookup` | `FormatException` for a corrupt, non-object or oversized registry file | corrupt |
| `LocalCatalog.lookup` | `FileSystemException` reading the registry | io |
| `LocalCatalog.guarded` | `FormatException` for a corrupt registry; `FileSystemException` on read, write or atomic replace | corrupt, io |
| `IcebergCatalog.current` | Contract: oversized or corrupt metadata, I/O (all implementations below) | limit, corrupt, io |
| `FilesystemIcebergCatalog.current` | `StateError` when metadata exceeds the byte cap; `FormatException` / `TypeError` for malformed metadata JSON | limit, corrupt |
| `FilesystemIcebergCatalog.current` | `FileSystemException` | io |
| `LocalCatalogIcebergCatalog.current` | `FormatException` for a corrupt registry; plus everything `FilesystemIcebergCatalog.current` throws | corrupt, limit, io |
| `SqlEngine.registerFile` | `ArgumentError` "Access denied" for a path outside the sandbox or working directory | denied |
| `SqlEngine.registerFile` | `ArgumentError` "File not found" | missing |
| `SqlEngine.registerFile` | `ArgumentError.value` for an unsupported extension; `FormatException` reading the schema | invalid, corrupt |
| `SqlEngine.registerFile` | `StateError` when `lockTimeout` expires | timeout |
| `SqlEngine.registerDataFrame` | `StateError` over `maxRows`/`maxBytes`, plus everything `DataFrame.execute` throws | limit, invalid, io, corrupt |
| `SqlEngine.registerDataFrame` | `StateError` when `lockTimeout` expires | timeout |
| `SqlEngine()` | `StateError` when the spill session directory cannot be created; replaced by `SqlEngine.open`, which returns a `Result` (the constructor becomes private) | io |
| `SqlEngine.dispose` | `StateError` when in-flight operations do not drain within `timeout` | timeout |
| `SqlEngine.dispose` | `FileSystemException` deleting the spill session directory | io |
| `sql` (top-level `SqlEngine`) | `StateError` on first use when the spill session directory cannot be created; then as `SqlEngine` | io |
| `bin/sql.dart` | Catches query errors and prints them; `ArgumentError` for a malformed `--csv`/`--parquet` spec and registration errors exit 1 | invalid, io, denied |
| `CommitConflict` | Exported exception type; replaced by `Kind.conflict` and no longer exported | conflict |
| `DeltaCommitIndeterminate` | Exported exception type; replaced by `Kind.indeterminate` and no longer exported | indeterminate |
| `IcebergCommitConflict` | Exported exception type (extends `StateError`); replaced by `Kind.conflict` and no longer exported | conflict |
| `IcebergCommitIndeterminate` | Exported exception type (extends `StateError`); replaced by `Kind.indeterminate` and no longer exported | indeterminate |

### Internal, mapped at a boundary

| Member | Throws in 1.x | Where it is mapped |
| --- | --- | --- |
| `SpillBudget.charge` | `StateError` when a charge would exceed `cap` | Inside the operators; `SqlEngine` (T45) and `DataFrame` (T46) report it as `limit` |

### Staying thrown

These fail only when the calling code is wrong, so they stay thrown `Error`s rather than becoming `Result`s:

| Member | Throws | Why |
| --- | --- | --- |
| `Schema.fromJson` / `Field.fromJson` | `TypeError` for malformed JSON | Fed the library's own schema JSON; untrusted input reaches them only through readers, whose boundary (T47) reports `corrupt` |
| `Table.select` / `removeColumn` / `addColumn` / `append` / `concat` / `fromChunks` | `ArgumentError` for an unknown column or incompatible schema | Programming errors in the calling code |
| `DataFrame.transform` / `toTable` with pending operations | `StateError` | Programming error; call `execute` first |
| Any method on a disposed `SqlEngine` | `StateError` | Programming error |
| `IcebergWriter.expireSnapshots` with `retain < 1` | `ArgumentError` | A constant in the calling code; the current snapshot can never be expired |
