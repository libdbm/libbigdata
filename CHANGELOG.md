# Changelog

## 2.0.0

Every fallible public entry point now returns a `Result<T, ErrorDetails>`
instead of throwing, and nothing escapes one: an unexpected error is returned
with the error as its `cause`, and only argument preconditions throw; `doc/MIGRATION.md` has before and after examples for each
area.

### Breaking

- Columns no longer expose or adopt their backing buffers. `NumericColumn`, `StringColumn`,
  `BoolColumn` and `ListColumn` are `abstract final` public types built through their copying
  factories; their implementations are not exported. `NumericColumn.adopt`, `rawBuffer` and
  `rawNullBitmap`, `StringColumn.adopt` and `encode`, and `BoolColumn.adopt`, `rawBits` and
  `rawNullBitmap` are gone from the public API. `buffer` and `nullBitmap` (unmodifiable views)
  and every read member stay.
- SQL `ORDER BY` and `GROUP BY` no longer take a position: `ORDER BY 2` and `GROUP BY 1`
  are `Kind.invalid`; use a column name, a select alias or an expression.
- Public multi-word names have shorter forms: `DataFrameIO.readCsvOptimized` folds into
  `readCsv`, which takes `filter`, `columns`, `limit` and `batchSize` and scans in batches
  when any is given; `SqlEngine.getTables()` and `getTableSchema(name)` are `tables` and
  `schema(name)`; `CsvWriter.writeTableToStream` is `writeSink`.
- Logging: the library logs under `Loggers.root` (`libbigdata`) and its children
  `Loggers.dataframe`, `delta`, `json` and `sql`, instead of flat class names; the unused
  `Loggers` constants are removed. A second `LoggingConfig.initialize` replaces the previous
  configuration instead of being ignored. `Level`, `Logger` and `LogRecord` are exported.
- One option vocabulary for the readers and writers: format `options:` and the caps are
  always named; every batch API takes `batchSize` (it was `size` on most readers, and
  `bufferSize` on `readCsvOptimized`); CSV projection is `columns` (was `selectColumns`);
  CSV readers take `filter: RowFilter`, applied exactly per row (a string predicate before);
  Delta time travel is `asOf` (was `timestamp`), as for Iceberg; `writeJson` and
  `writeJsonLines` take `path`. Every reader that materializes its result takes
  `maxRows`/`maxBytes`. A `batchSize` below 1 throws at the call rather than inside the
  stream. `RowFilter.matches` evaluates a filter exactly against one row.
- Exported classes carry Dart 3 class modifiers: `Column` and `RowFilter` are `sealed`; the
  filesystem and local-catalog Iceberg catalogs, `FilesystemCommitCoordinator` and
  `MemoryManager` are `base` (extend them, do not implement them); the coordinator, catalog,
  backend and invariant interfaces stay implementable; every other exported class is `final`.
- Public API hygiene for 2.0:
  - `ParquetWriterOptions.compression` takes the new `ParquetCompression` enum (it took an
    unexported Thrift type, so no codec could be chosen without a `src/` import).
  - The deprecated `DataFrameIO.fromCsv`, `fromJson`, `fromJsonLines`, `fromArrow`,
    `fromArrowStream`, `fromArrowBatches`, `toArrow` and `toArrowStream` aliases are removed;
    use the `read*` / `write*` members they forwarded to.
  - `DataFrameIO.readDatasetBatches` yields DataFrames, as the other batch readers do.
  - Internals are no longer exported: `unifyTables`, `decimalStats`, `distinctStatsCap`,
    `defaultChunkSize`, `parseTemporal`, and the unused `DelimiterDetectionResult` (removed).
    The `ParquetReader` / `IcebergTable` diagnostic counters are `@internal`.
- SQL follows DuckDB's defaults: a number with a decimal point (`1.50`, `0.1`) is an exact
  `DECIMAL` (it was a `DOUBLE`; write an exponent or `CAST(... AS DOUBLE)` for a double),
  and `ORDER BY ... DESC` puts NULLs last, as `ASC` does (it put them first).
- `NumericColumn.hasNulls` reports whether a null is present, like every other column; it
  reported whether the column was nullable. The Parquet writer keeps a nullable field
  OPTIONAL even with no null in it (string and boolean columns without nulls were written
  REQUIRED). `Field.isCompatibleWith` accepts only types that concatenate without a cast.
- `SqlEngine.execute`, `SqlEngine.sql` and `DataFrame.sql` return `Result`, and
  `SqlEngine.executeStream` yields `Result<Table, ErrorDetails>` items, ending after an `Err`.
  SQL is treated as untrusted input, so no failure is thrown: SQL that does not parse or
  type-check is `Kind.invalid`, unimplemented SQL `Kind.unsupported`, an unknown table or file
  `Kind.missing`, a path outside the sandbox or a write on a read-only engine `Kind.denied`,
  an unreadable data file `Kind.corrupt`, a result or spill cap `Kind.limit`, and a deadline
  `Kind.timeout`. UPDATE, INSERT and MERGE return the Delta writer's kinds; an INSERT that does
  not fit the table is `Kind.schema`. Any other error is `Kind.invalid` with the error as its
  `cause`. Only using a disposed engine still throws.
- DataFrame terminal operations return `Result`: `execute`, `toList`, `toJsonString`,
  `head`, `tail`, `count`, `show`, `cache`, `toCsv`, `toJson` and `toJsonLines`, and
  `executeStream` yields `Result<Table, ErrorDetails>` items. Predicates and expressions are
  caller-supplied strings, so failures are reported as `SqlEngine.execute` reports them: an
  invalid predicate is `Kind.invalid`, a cap is `Kind.limit`, a deadline `Kind.timeout`.
- Reads return `Result`: `ParquetReader.schema`, `read` and `readBatches`, `CsvReader` and
  `JsonReader` reads, `DeltaTable.read`/`readBatches`, `IcebergTable.read`/`readBatches`,
  `Dataset.read`/`readBatches`, and every `DataFrameIO.read*` and `from*`. Streams yield
  `Result` items. A missing file or table is `Kind.missing`, a malformed file
  `Kind.corrupt`, a cap `Kind.limit`, an unknown projected column `Kind.invalid`, a path
  outside a lake table `Kind.denied`, and an unsupported format feature `Kind.unsupported`.
- Writes return `Result<void, ErrorDetails>`: `ParquetWriter` (`write`, `open`, `writeBatch`,
  `writeBatches`, `close`), `CsvWriter` and `JsonWriter` file and stream writes,
  `Table.writeJson`/`writeJsonLines`, and the `DataFrameIO` writes (`writeCsv`, `writeJson`,
  `writeJsonLines`, `writeArrow`, `writeArrowStream`, `writeParquetStream`). An unwritable
  destination is `Kind.io`, a value its Parquet type would truncate `Kind.invalid`, a batch
  that does not match the first batch's schema `Kind.schema`, and an unwritable codec or
  type `Kind.unsupported`. No partial file is left after an `Err`.
- `SqlEngine.open(...)` replaces the `SqlEngine(...)` constructor and returns
  `Result<SqlEngine, ErrorDetails>` (`Kind.io` when the spill directory cannot be created).
  `registerFile`, `registerDataFrame` and `dispose` return `Result`.
- The top-level `sql` is a function, `sql(query)`, returning `Result<Table, ErrorDetails>` from
  a shared engine opened on first use. It was the shared `SqlEngine` itself; to register
  tables, open an engine.
- `LocalCatalog.lookup` and `guarded`, and `IcebergCatalog.current` (all implementations),
  return `Result`.
- `CommitConflict`, `DeltaCommitIndeterminate`, `IcebergCommitConflict` and
  `IcebergCommitIndeterminate` are no longer exported: a commit reports `Kind.conflict` or
  `Kind.indeterminate`. A custom `DeltaCommitCoordinator` or `IcebergBackend` returns
  `Err(ErrorDetails(Kind.conflict, ...))` for a lost race.
- The CLI prints `Error: <kind>: <message>` and exits 1 when a `--csv`/`--parquet` file
  cannot be loaded.
- `SqlEngine` is read-only by default when `sandboxRoot` is set: UPDATE, INSERT, MERGE and
  CREATE/DROP VIEW fail with `Kind.denied`. Pass `readOnly: false` to allow writes.
- SQL never converts a string literal to a number. `n = '1'`, `n < '3'`, `n IN ('1', '2')`
  and `n + '1'` on a numeric column fail with `Kind.invalid` at planning. `=` and `IN`
  previously matched nothing, `<` failed only at run time, and `+` raised a `TypeError`.
  Write the number or `CAST('1' AS BIGINT)`.
- The CSV, JSON and Parquet readers report a missing file as `Kind.missing`, with the
  `PathNotFoundException` as its `cause`.
- Delta writes return `Result<int, ErrorDetails>` instead of throwing:
  `DeltaWriter.write`, `delete`, `update` and `merge`, `DeltaCommitCoordinator.commit` (all
  implementations), `DataFrameIO.writeDelta` and `InvariantEvaluator.violations`. A lost
  commit race is `Kind.conflict` (after `maxRetries`, with the attempt count in its message),
  an unconfirmed publish is `Kind.indeterminate` (staged files are kept), a schema or
  partitioning mismatch is `Kind.schema`, a merge source or partition count over its cap is
  `Kind.limit`, and `delta.appendOnly` refusing a mutation is `Kind.denied`. `CommitConflict`
  and `DeltaCommitIndeterminate` are no longer thrown out of these calls. See
  `doc/MIGRATION.md`.
- Iceberg writes return `Result<int, ErrorDetails>` instead of throwing:
  `IcebergWriter.write` and `expireSnapshots`, `IcebergBackend.commit` (all implementations)
  and `DataFrameIO.writeIceberg`. An append schema mismatch is `Kind.schema`; a lost commit
  race, or a schema or partition spec changed concurrently, is `Kind.conflict`; an
  unconfirmed publish is `Kind.indeterminate`; the metadata or partition cap is
  `Kind.limit`; a missing table is `Kind.missing`. When staged files cannot be removed after
  a failure the result is `Kind.io` with the original failure as its `cause`.
  `expireSnapshots(retain: 0)` still throws `ArgumentError`.
- `DataType.decimal.dartType` is `BigInt`, the type decimal columns store (unscaled), instead
  of `double`.
- In SQL, `"..."` is a quoted identifier (standard SQL), no longer a string literal. Write
  strings in single quotes: `WHERE name = 'x'`, not `WHERE name = "x"`. An unknown column
  error now reminds you of this. A quoted path after `FROM` is still read as a file.
- In SQL, `LENGTH` (`CHAR_LENGTH`) and `SUBSTR` count Unicode code points, as `LIKE` does,
  instead of UTF-16 code units: `LENGTH('a𝄞b')` is 3 (was 4), and `SUBSTR` no longer
  splits a character outside the Basic Multilingual Plane.
- DataFrame `filter` rejects a quoted value, or anything that is not a number, compared
  with a numeric column (`Kind.invalid`), as SQL does: `filter("age == '30'")` was read
  as `age == 30`. Write the number unquoted.
- DataFrame `filter`/`where` and `withColumn` take SQL scalar expressions, parsed and bound
  as SQL parses and binds them. `withColumn` arithmetic follows standard precedence, which
  changes results without an error: `'a + 10 * 2'` was `(a + 10) * 2` and is now `a + 20`.
  A name with a space or `-` is a double-quoted identifier (`'"First Name" = ...'`), a
  double-quoted value is an identifier (strings take single quotes), `[name]` brackets are
  a parse error, and an unquoted word on either side is a column. `==` is accepted as `=`,
  in SQL too. Parentheses, `NOT`, `IS NULL`, `IN`, `LIKE`, `CASE`, `CAST` and functions are
  available. `doc/MIGRATION.md` has before and after examples.
- DataFrame `orderBy` sorts NULLs last in both directions, as SQL `ORDER BY` does; an
  ascending sort put them first. DataFrame `select`, `filter`, `limit`, `orderBy`, `drop`,
  `rename`, `withColumn` and `distinct` are planned by the SQL planner, so a filter and a
  projection on a file-backed frame (`DataFrame.scan`, `readParquet`) prune row groups and
  columns, and `orderBy(...).limit(n)` keeps only the best `n` rows.
- DataFrame `groupBy().agg()`, `join` and `union` are planned by the SQL planner too, so a
  filter or projection reaches the scans on both sides of a join or union. `join` runs as
  SQL `JOIN ... USING (on)`: a non-key column both sides have is qualified by its side's
  name (`p.name` and `d.name`; `left.name` and `right.name` when both sides have the same
  name, as two `fromRows` frames do), where it came out twice under its bare name, and
  `schema`/`columns` report those names. `union`'s `schema` reports each column's promoted
  type, as the result has. A set operation whose branches have different column counts is
  `Kind.schema` in SQL, as it is for DataFrame `union`.
- The DataFrame file readers return lazy frames: `readCsv`, `readJson`, `readJsonLines`,
  `readParquet` (with or without `filter`/`columns`), `readArrow`, `readArrowStream` over an
  IPC file, `readDelta`, `readIceberg` and `readDataset` read the schema when called and the
  data when a terminal runs, with the reader's options, so a later `filter`/`select` prunes
  files, row groups and columns. A missing file, an unreadable schema, a directory that is
  not a table, an unknown version or snapshot, and an unknown column in `columns` are still
  an `Err` from the reader; a bad value further into the file, an unreadable data file, a
  data or delete file outside the table, and the reader's `maxRows`/`maxBytes` are reported
  by the terminal. The reader's caps bound a materialized result (`execute`, `toList`,
  `head`, ...) together with the terminal's own; `count` and `executeStream` set no result
  cap, so they no longer fail on a file larger than the reader's cap. `readDelta` without
  `version`/`asOf` and `readIceberg` without `snapshotId`/`asOf` fix the version or snapshot
  current at the reader call, so a later commit does not change the frame's result. A frame
  from a file reader is named after the file (`p.csv` is `p`; a glob dataset is `dataset`)
  in join qualifiers and its result, where
  it was `csv`, `json_data`, `arrow_table`, `delta`, `iceberg` or `dataset`. Read when
  called, as before: a CSV with a custom `lineTerminator` or `maxFileBytes`, JSON with
  `expandArrays` or with both caps null (whole-file inference), an Arrow IPC stream, Delta
  with `concurrency` above 1, a dataset with `concurrency` above 1 and both caps null, and
  an empty file.
- `SqlEngine.registerDataFrame` and `DataFrame.sql` register the frame's plan as a view
  instead of running the frame: each query that reads it plans the frame's operations with
  its own, so a query's `WHERE` prunes a file-backed frame's row groups and its columns
  narrow the scans, and each query reads the frame's sources again. The table's columns are
  named as `DataFrame.columns` names them. `registerDataFrame`'s `maxRows`/`maxBytes` (and
  the caps of the readers the frame is built on) now bound the result of each `execute` that
  reads the frame, where they bounded the frame's own result at registration: registering a
  frame larger than the cap succeeds, `SELECT COUNT(*)` over it succeeds, and a result past
  the cap fails with `Kind.limit`. An invalid expression or unknown column still fails the
  registration; a source file removed after it fails the query.
- In SQL, a `JOIN` or `INNER JOIN` without `ON` or `USING` is a parse error, as in
  PostgreSQL and DuckDB. It previously ran as a cross join; write `CROSS JOIN` for that.
- In SQL, a string value compared or combined with a number is an error (`Kind.invalid`):
  `WHERE s = 1`, `s IN (1, 2)` and `a IN (SELECT s ...)` on a text `s` matched nothing.
  An aggregate with the wrong number of arguments (`COUNT(a, b)`, `SUM(a, b)`) is an error;
  the extra arguments were ignored.
- CSV dates and timestamps without a zone read as UTC, as elsewhere in the library; they
  were read in the host's time zone, so the same file gave different instants on different
  machines.
- SQL casts follow DuckDB: a fractional value cast to an integer rounds half away from zero
  (`CAST(2.5 AS INTEGER)` was 2, now 3); `INTEGER`, `SMALLINT` and `TINYINT` are 32, 16 and
  8 bits wide (all were 64) and an out-of-range value is an error; `CAST(2 AS BOOLEAN)` and
  the text `t`, `yes`, `y` are true (they were false) and other unparsable text is NULL.
- String and boolean columns are `StringColumn` and `BoolColumn` instead of
  `ListColumn<String>` and `ListColumn<bool>`, wherever the library builds them: file
  readers, `Table.fromRows`, `DataFrame.fromRows` and SQL results. Values, `[]` and
  `toList()` are unchanged; code that tests for or casts to `ListColumn` should use
  `Column`. A Dart string with an unpaired surrogate is stored with U+FFFD in its place.

### Added

- The Arrow IPC stream format: `writeArrowStream` writes it (with `compression`, and
  `batchSize` rows per batch) and `readArrowStream` reads it; both were the file format
  before. pyarrow's `ipc.open_stream` reads what is written, and `ipc.new_stream` output
  reads, compressed or not.
- `Ok` and `Err` compare by value, and `Result.map` transforms an `Ok` value.
- Exported the types public signatures already took: `DeltaLog` (custom Delta commit
  coordinators), `IcebergCommitCoordinator` and `FilesystemCommitCoordinator` (Iceberg backends),
  `CatalogTxn` (`LocalCatalog.guarded`), `ColumnBounds` (`RowFilter.keep`), and the
  `SqlDataFrame` extension (`DataFrame.sql`).
- Avro: the `zstandard` codec reads.
- Delta tables with struct, array and map columns read and write. Their schema was read and
  written as `string`, so such tables were unreadable and a nested table written here had a
  wrong schema.
- `StringColumn` holds strings as one UTF-8 buffer with an offset per row: an 8-character
  ASCII value costs about 12 bytes, against 24 in a `ListColumn`. `BoolColumn` holds booleans
  as bits: 1 bit a value, 2 with nulls, against 64.
- `ParquetReader.read(concurrency: n)` decodes row groups in up to `n` isolates.
- `DeltaTable.readBatches` and `IcebergTable.readBatches` take `filter:` and `columns:`. A
  file is skipped when its partition values or column statistics show it cannot match
  (Iceberg also skips manifests by their partition summaries), row groups are pruned in the
  files read, and only the listed columns are read.
- `DeltaTable.schema()` and `IcebergTable.schema()` read a table's schema from its metadata
  without opening data files.
- In SQL, `FROM '<dir>'` and `registerFile` accept a Delta or Iceberg table directory and
  stream it with the query's filter and columns pushed down.
- The CLI's `--root DIR` option confines SQL and loaded file paths to DIR, which may be
  given absolutely, instead of to relative paths under the current directory.
- `Result<T, E>` (`Ok`, `Err`), `ErrorDetails` and `Kind`, the types 2.0's public entry points
  return for expected failures. `doc/MIGRATION.md` lists every entry point that changes.
- `make interop` checks the interop prerequisites (Python modules, `duckdb`, a JDK) with
  install hints for anything missing, then runs the interop-tagged tests. See
  `tool/interop/README.md`.
- `EngineOptions`, `MemoryManager` and `SpillBudget` are exported from
  `package:libbigdata/libbigdata.dart`, so `DataFrame.execute(options: ...)` no longer needs a
  `src/` import.
- SQL `DATE '...'` and `TIMESTAMP '...'` literals.
- Query deadlines: `SqlEngine.open(timeout:)` sets a default and `execute`, `executeStream` and
  `sql` take a per-call `timeout:`, counted from the call. A query past its deadline fails
  with `Kind.timeout`, whether it is running, queued behind another query on the engine, or
  waiting on a slow read. Work Dart cannot interrupt, such as a read in progress, finishes
  in the background: it holds the engine until it ends, `dispose` waits for it, and its spill
  files are removed before the next query runs.
- A long query gives the event loop a turn about every 10 ms, so timers and I/O elsewhere in
  the isolate, such as a server's requests, are not held up until it finishes.

### Security

- Parquet footers: a Thrift list, set or map that declares more elements than bytes remain
  is `Kind.corrupt` at its header.
- SQL limits: an expression tree deeper than 4,096 levels, or an `EXCEPT`/`INTERSECT` chain
  of more than 2,048 branches, is `Kind.invalid` naming the limit, checked right after
  parsing. Such queries already failed safely, but with the message "Stack Overflow".
- A long chain of `UNION`/`UNION ALL` no longer crashes the process: 1,000 branches (26 KB of
  SQL) ended it with an uncaught stack overflow during execution. A chain of unions is
  planned as one n-ary union, with one `DISTINCT` up to the last distinct `UNION`, and runs
  without nesting at any length.
- Decimal precision and scale read from Delta, Iceberg, Parquet, Arrow and Avro schemas are
  checked (precision 1-38 for Delta and Iceberg, 1-76 otherwise; scale 0 to precision). A
  schema declaring `decimal(10,2000000000)` made later formatting allocate gigabytes.
- Crafted Parquet pages fail as corrupt instead of allocating or misreading: a
  DELTA_BINARY_PACKED header could request a terabyte allocation (it must now follow the
  spec's block and miniblock sizes, with bit widths no wider than the type), a V1 page too
  short for its level length, a V2 page with nulls but no definition levels (read as zeros),
  a V2 page whose levels exceed it, and a nested leaf whose pages disagree with its metadata.
  An unknown value encoding is unsupported; it was decoded as PLAIN.
- Delta invariants are parsed as one expression over the row's columns and evaluated on a
  read-only engine. An invariant read from table metadata could close the parenthesis it was
  placed in and add a subquery, which let anyone able to write a table's metadata probe the
  contents of files the writing process could read.
- `LIKE` no longer translates patterns to regular expressions. A crafted pattern such as
  `'%a%a%a%a%b'` previously took tens of seconds per row; matching is now O(n·m) worst case.
- New `SqlEngine.open(readOnly: true)` rejects every statement that writes.
- Parquet dictionary indices and boolean RLE streams stop decoding at the page's value count.
  A 7-byte run claiming 2^27 values no longer expands to 134M entries.
- DELTA_BINARY_PACKED decoding stops materializing at the requested count; a 12-byte stream
  claiming 2^27 values at bit width 0 no longer expands.
- DELTA_BYTE_ARRAY validates prefix and suffix lengths before allocating and caps decoded
  output at 256 MiB per page. Corrupt lengths are `Kind.corrupt` instead of a `RangeError`.
- Parquet V2 data pages reject negative or out-of-range null counts and level lengths before
  allocating, and take the present-value count from the definition levels.
- Parquet footers are checked for schema depth (100), element count (1M) and consistent
  child counts before any reader walks the schema. A deeply nested footer previously
  overflowed the stack.
- Avro's decoded value budget (10M) now spans the whole file instead of resetting per record,
  so many small records cannot add up to an unbounded allocation. Schema parsing is capped
  at depth 100; a deeply nested schema previously overflowed the stack.
- Arrow flatbuffer reads are bounds-checked, and a record batch with fewer nodes or buffers
  than its schema needs is rejected before decoding. Corrupt files are `Kind.corrupt`
  instead of a `RangeError`.
- An Arrow decimal buffer shorter than its declared width times the row count is
  `Kind.corrupt`. It previously read the neighbouring buffer's bytes as values.
- The SQL nesting limit (256) counts `CASE ... END` blocks as well as parentheses. 2,000
  nested CASE expressions previously overflowed the stack. A `StackOverflowError` during
  parsing is reported as `Kind.invalid`.
- A mutation test (`dart test -t fuzz --run-skipped`) feeds 10,000 corrupted Parquet, Avro
  and Arrow files each to the readers. The cases it found now fail as `Kind.corrupt`:
  unknown Parquet enum values, pages or column chunks missing required headers, Avro schema
  JSON of the wrong shape, and invalid Arrow type codes, int widths and float precisions.
- Without `sandboxRoot`, SQL file paths are resolved through symlinks and must stay under
  the working directory. A symlink under it that pointed outside was previously followed.
- Nested derived tables (`FROM (SELECT * FROM (...) x) x`) parse in linear time. Each level
  previously parsed its subquery twice, so a 500-byte query could take hours to parse.

### Fixed

- SQL `UPDATE` casts a `SET` value to the type of an integer column, as `CAST` does:
  `SET salary = salary * 1.05` stores the rounded integer (half away from zero) instead of
  failing with "is int16 but the value 1050000 is a _BigIntImpl". A value out of the column's
  range is still `Kind.invalid`, and nothing is committed.
- SQL `WHERE` and DataFrame `filter` cast a predicate that is not boolean to `BOOLEAN`, as
  DuckDB does: `WHERE age` keeps the rows where `age` is nonzero and `WHERE 1` keeps every
  row. `WHERE` used to return no rows for such a predicate, without an error.
- SQL registers every file and view a query names, wherever it appears (select list,
  GROUP BY, ORDER BY, join conditions, CASE, CAST, function arguments, subqueries). A
  query the planner rejects (for example a scalar subquery) that also names a missing or
  denied file now reports that file's error instead of `Kind.unsupported`.
- DataFrame executions that spill and set no `spillDir` keep their runs in a session
  directory under `<temp>/libbigdata-spill/` (removed when the execution ends, and reclaimed
  by the orphan sweep after a crash); they were written loose into the system temp, where
  a crash left them for good. The intermediate that `sample()` materializes is held under
  the tighter of `maxBytes` and `EngineOptions.maxMaterializedBytes` (the library default
  when neither is set), including from `count()` and `executeStream()`, which set no cap.
- `DataFrame.groupBy` not followed directly by `agg` (for example `groupBy(...).execute()`
  or `groupBy(...).filter(...).agg(...)`) is `Kind.invalid`; the grouping used to be dropped
  without a word.
- Two paths to one lock file through a symlink (on macOS, `/var/...` and `/private/var/...`)
  now exclude each other within an isolate: the in-isolate lock was keyed on the lexical
  path. Cross-isolate locking is unchanged.
- A date or time with a field out of range (`DATE '2024-13-45'`, `'2024-02-30'`, hour 24)
  no longer rolls over into another date (2025-02-14): a SQL literal or comparison with it
  is `Kind.invalid`, a CAST of it is `NULL`, and CSV inference reads it as text.
- DataFrame builders never throw: `agg` with an unknown function and `select` of an unknown
  column report `Kind.invalid` from `execute` instead of throwing `ArgumentError` at the call.
- Iceberg data files without field ids (tables built with `add_files` or `migrate`) resolve
  their columns through `schema.name-mapping.default`, so a renamed column reads its values
  instead of NULL. A read of the current table binds the current schema rather than the
  snapshot's, so a rename or added column shows without a new snapshot; time travel keeps
  the snapshot's schema.
- SQL planner: a misspelled qualifier in a join condition is an unknown column instead of
  binding to the other side; `MERGE ... USING table_name` parses and `ON t.id = s.key`
  merges on both names; a correlated subquery or comma join over file tables uses their
  basename qualifiers; `HAVING` without `GROUP BY` aggregates the whole input; `GROUP BY`
  matches function names in any case, `t.a` with `a`, and select aliases;
  `COUNT(DISTINCT k)` under `GROUP BY k` works; an unknown column in `IN` or a comparison,
  an ungrouped column, and an aggregate in `WHERE` each get an error that names the
  actual problem.
- SQL: two files whose paths differ only in case are two tables (they shared one catalog
  entry). `executeStream` starts its deadline at the call, throws on a disposed engine as
  `execute` does, and reports an engine disposed before the stream is listened to as an
  `Err` instead of an error on the stream.
- CLI: `-f` scripts split on semicolons outside strings and comments; a load path may
  contain a colon, and must have the loader's extension; `.tables` shows column counts
  instead of a wrong `0 rows`; `-o csv` quotes headers, carriage returns and every value
  type; loading files with no SQL opens the REPL instead of exiting; the usage text names
  the installed `libbigdata` command.
- A table accepts a numeric column built with the factories' default `nullable: true` under
  a non-nullable field when it holds no null. `DataFrame.union` of frames with different
  column counts is `Kind.schema`; `sample` outside 0 to 1 is `Kind.invalid`. An unwritable
  log file no longer crashes the process. `writeDelta`, `writeIceberg` and `writeArrow`
  take `maxRows` and `maxBytes`. `Table.fromChunks` widens like `append`. String columns
  refuse more than 2 GiB of text before their offsets overflow.
- Avro named types resolve by full name, so two types with the same short name in
  different namespaces no longer replace each other, and a reference by full name
  (`c.Color`) to an enum or fixed type resolves. Avro read caps report `Kind.limit`
  (they were `StateError`s reported as corrupt) and an unknown codec `Kind.unsupported`.
- Parquet: a nested read over `maxBytes`, and a nested row group over the streaming ceiling,
  report `Kind.limit` instead of `Kind.corrupt`; a repeated column whose first value
  continues a record is a format error instead of a crash.
- Delta and Iceberg scans: an eager read through `openScan` applies its projection and
  filter, so the byte cap counts only the columns read; Iceberg batches honour the requested
  size; a projected column the table lacks is `Kind.invalid` instead of being dropped; a
  relocated Iceberg table under a directory named `data` re-roots correctly; a short Delta
  deletion-vector path is `Kind.corrupt` instead of a `RangeError`.
- Delta and Iceberg paths in `file:` URI form (`file:/x`, `file:///x`, `file://localhost/x`)
  resolve to the file in every reader, writer and vacuum; Delta read a `file:/x` add path as
  a file under the table root. A Delta path with a malformed percent escape is
  `Kind.corrupt` instead of an `ArgumentError`, and an absolute Delta path is
  percent-decoded as the protocol requires.
- `Dataset` discovery: files and directories starting with `_` or `.` (`_SUCCESS`,
  `_temporary`, `_delta_log`, `.crc`) are skipped; a Delta or Iceberg table directory reads
  as the table, and one nested below the dataset root is `Kind.unsupported`; `key=value`
  directories above the root are no longer partitions; a partition key has one type across
  files (`k=1` and `k=1.5` are both doubles, and `NaN` is text).
- A Delta overwrite keeps the metadata (`delta.invariants`, comments, generation
  expressions) of columns it does not change. An overwrite with a schema built in Dart
  dropped it, so an invariant stopped being enforced.
- Delta reads find checkpoints by listing `_delta_log` instead of trusting `_last_checkpoint`.
  Time travel to a version below the newest checkpoint, or any read after log cleanup lost
  `_last_checkpoint`, failed when the commits before it had been cleaned up.
- Iceberg `asOf` time travel follows `snapshot-log`, so it gives the right snapshot after a
  rollback; an `asOf` before the first snapshot is `Kind.missing` instead of an empty table.
  Hadoop catalog tables (`v<n>.metadata.json`) and gzip-compressed metadata files read,
  through `IcebergTable` and `openScan` alike.
- Iceberg `expireSnapshots` keeps every snapshot a tag or branch names (and a branch's
  ancestors up to its `min-snapshots-to-keep`). It expired them and deleted their files,
  leaving the tag or branch pointing at a snapshot that no longer existed. The
  `metadata-log` is now capped at `write.metadata.previous-versions-max` (default 100)
  instead of growing with every commit.
- Iceberg data files that carry field ids are matched to the table schema by id only.
  A column dropped and then re-added under the same name read the dropped column's old
  values from older files; it now reads NULL, as pyiceberg does. Equality-delete keys
  follow the same rule. Files without field ids match through the table's name mapping,
  or by name when it has none.
- Delta date partition values and date min/max statistics are the value's calendar day.
  A local `DateTime` was converted to UTC first, so on a host east of UTC the partition
  read one day earlier than the data and engines using the statistics could skip files that
  matched. Binary partition values read back as bytes. Partition directory names hold a
  date as `2024-01-01` and a decimal at its scale (`1.50`), not `2024-01-01 00:00:00.000Z`
  and the unscaled `150`.
- JSON: integers beyond int64 (such as uint64 ids) read exactly as decimals; they were
  rounded through a double. NaN and infinities write as null; they made the write fail. A
  `.jsonl` or `.ndjson` destination is written as JSON Lines by `writeTable` and
  `Table.writeJson` too. A file holding one top-level object reads through `readFile`, and
  a stray `}` in an array is rejected. `JsonOptions.encoding` applies to writes.
- CSV: with a thousands separator set (`CsvOptions.european()`), only correctly grouped
  digits are numbers; `1.2.3` read as 123. With an escape character other than the quote,
  the writer escapes the escape character itself, so `a\b,c` reads back unchanged. A string
  equal to a null token (`NA`, `null`, ...) is quoted on write and reads back as text.
- CSV reads agree across `readString`, `readFile` and `readBatches`, which now share one
  pipeline. A comment or blank line before the header made `readString` fail and `readFile`
  read the header as data; files with carriage-return line endings read as empty through
  `readFile`; a header-only file read as no columns through `readFile`; `maxRows` counted
  the header; and an empty header cell was named `""` by one path and `column_1` by the
  other. A line break inside a quoted field keeps its `\r\n`. A read that returns no rows
  still carries the columns.
- Arrow dates and timestamps exchange correctly with pyarrow. The writer omitted the date
  unit, which the format reads as milliseconds, so pyarrow read 2020-01-01 as 1970-01-01;
  the reader assumed days and microseconds where the format's defaults are milliseconds and
  seconds, so pyarrow `date64` and `timestamp('s')` columns read as wrong dates. Files from
  earlier versions still read as days.
- Crafted Arrow files fail as corrupt instead of reading neighbouring bytes or crashing:
  validity, int16, boolean, date and timestamp buffers are length-checked, string offsets
  are checked against their own buffer, list and map children are checked, and a seconds
  timestamp that overflows is refused. Dictionary-encoded and big-endian files are reported
  as unsupported (dictionary files were reported as corrupt).
- A decimal key matches a double key in a join or `IN (SELECT ...)`; they never matched.
  A NULL literal takes the type of the other values in `CASE`, `COALESCE` and set
  operations: `CASE WHEN ... THEN NULL ELSE a END` was a string column,
  `COALESCE(NULL, 1) + 1` failed, and `SELECT a ... UNION SELECT NULL` failed.
- `ROUND(x, n)` no longer clamps values past 2^63: `ROUND(1e300, 2)` returned
  92233720368547760.0. The `TINYINT`, `REAL`, `BOOL` and `DATETIME` cast types parse.
- A decimal in a `VALUES` table keeps its scale: `VALUES (CAST(1.5 AS DECIMAL(5,2)))` read
  back as 150. Rows of different scales share the widest, as in DuckDB.
- Column alias lists on tables and subqueries rename the columns:
  `FROM (SELECT a, b FROM t) v(b, a)` and `FROM t x(p)` were parsed and then ignored. A list
  longer than the table's columns is an error.
- SQL results now match DuckDB in three cases:
  - `ORDER BY u.a` sorts by `u.a` when the output also has a column named `a` from another
    table; it sorted by that column.
  - RIGHT and FULL `JOIN ... USING` (and DataFrame `join` with `type: right` or `full`) fill
    the key from the right side for rows only the right side has; it was NULL.
  - A date or timestamp column `IN ('2024-01-05', ...)` compares dates, as `=` does; it
    matched nothing.
- A query that stops reading early (a `LIMIT` above a spilling UNION, INTERSECT, semi join
  or hash join, or a cancelled stream) closes its spill files and returns its memory
  reservations; each such query kept a deleted spill file open. A spill that fails while
  finishing returns its spill budget and files. A large sort merges its runs in passes of at
  most 64, so it no longer runs out of file descriptors.
- A join whose output has two columns of the same name (`t.id`, `u.id`) can now be sorted,
  grouped or deduplicated past the spill threshold. It failed with "Duplicate field name"
  once the data was large enough to spill, while the same query worked in memory.
- `MIN`, `MAX` and `ORDER BY` on binary columns compare bytes, as DuckDB does. MIN and MAX
  returned NULL and ORDER BY failed.
- DataFrame operations keep a decimal column's precision and scale. `select`, `rename`,
  `withColumn`, `drop` and `groupBy().agg()` read 1.50 back as 150, and `filter('d > 2')`
  compared the unscaled value. `DataChunk.row` returns `Decimal` like `Table.row`, and
  `Table.fromRows` types `Decimal` and `BigInt` values as decimal, so rows round-trip.
- Parquet row-group pruning no longer trusts the deprecated `min`/`max` statistics for
  strings, binary and unsigned integers. Older writers filled them in signed byte order, so
  a row group holding `'a'` and `'é'` could be skipped for `s = 'a'`. A column whose declared
  sort order is unknown is not pruned.
- Parquet converted types from 17 up were numbered wrongly (INT_32 and INT_64 were
  missing), so files with INTERVAL or BSON columns, such as DuckDB's INTERVAL, failed to
  open, and INT_32/INT_64 columns were tagged JSON/BSON internally. An unknown converted
  type is now ignored rather than failing the file. JSON and ENUM columns read as text.
- The Parquet writer no longer writes files no reader can open: a top-level column named
  like a nested field (`id` beside `s.id`, `value` beside a map) took the nested field's
  repetition; a null in a required column was written without its definition level (it is
  now `Kind.invalid`); and pages were split by value count only, so large strings made pages
  over the reader's 256 MiB limit (pages now end at about 1 MiB). Map keys are written
  REQUIRED, which pyarrow requires.
- A query whose `WITH` clause repeats a name is rejected (`Kind.invalid`). It used to leave
  the first CTE's rows in the catalog in place of the table it shadowed, even on a read-only
  engine. A CTE or view named like a registered file now hides the file for that query; the
  file was read instead.
- SQL `IN (...)` parses in linear time. Each nesting level used to be parsed twice, so a
  168-byte query of nested `IN` lists blocked the isolate for 17 seconds, and a query
  deadline could not stop it. `x IN y` without parentheses is now a parse error; it used to
  match nothing (or everything with `NOT IN`).
- `CREATE VIEW v AS SELECT ...` parses; the body had to be in parentheses, though the SQL
  guide showed it without them.
- Zstd-compressed Parquet written by DuckDB reads: libcompress 1.3.0 rejected some pages
  whose Huffman weight tables are long ("FSE weight decode: tail loop exceeded 100
  iterations"). libbigdata now requires libcompress 2.0.0.
- A `<>` filter on a floating-point column no longer skips a Parquet row group whose
  statistics show one value equal to the literal. Statistics leave NaN out of min and max,
  and `NaN <> 5` is true, so such a group could hold matching NaN rows that were silently
  dropped. Parquet and Delta statistics carry no NaN count, so they no longer prune `<>` on
  floating-point columns; Iceberg prunes when its NaN counts show none.
- `CAST(... AS DATE)` and `CAST(... AS TIMESTAMP)` parse ISO-8601 into date and timestamp
  values. They previously returned the input string under a date or timestamp type.
  Unparseable text gives NULL. `CAST(... AS TIME)` is `Kind.unsupported` instead of
  returning the string.
- Comparing a date or timestamp with a string literal compares instants, in SQL
  (`ts > '2024-06-01'`) and in DataFrame `filter("t > '2024-06-01'")`. Both previously
  failed on the mismatched types. A string that is not a date is `Kind.invalid`.
- `Table.statistics` on a decimal column returns its min, max and mean at the field's scale.
  It previously threw a `TypeError`. DataFrame `withColumn` arithmetic on a decimal column
  produces a decimal at the right scale; it previously failed column validation.
- `GROUP BY t.id, u.id` returns both keys. The two `id` group columns previously collided
  with "Duplicate field name: id".
- `SUBSTR` follows Postgres: a NULL start or length gives NULL, a start before the string
  counts toward the length (`SUBSTR('abc', 0, 2)` is `'a'`), and a negative length is
  `Kind.invalid`. It previously raised `TypeError` or `RangeError`, ignored a NULL length, and
  returned `'ab'` for start 0.
- `ABS` of the minimum int64 is the integer overflow error (`Kind.invalid`) instead of returning the
  negative input.
- Arithmetic with a string operand (`s + 1`) is `Kind.invalid`, naming the operator,
  instead of a raw `TypeError`.
- CLI: decimals print at their scale (`1.50`) in table, CSV and JSON output, and JSON
  encodes dates and decimals instead of failing. A failed query exits with status 1. End of
  input leaves the REPL instead of looping, and `.quit` removes the spill directory.
  `.output` with no argument shows the current format, `--version` matches the package
  version, and the help example no longer suggests `-tv` (which meant `--version`).
- Delta and Iceberg string statistics order by Unicode code point, as external engines do.
  A value outside the Basic Multilingual Plane (an emoji) sorted below U+FF01, recording a
  wrong max that let engines skip files that matched. Infinities are now recorded as Iceberg
  bounds instead of being counted as NaN; Delta, whose JSON stats cannot hold infinity, omits
  that bound.
- Parquet files carry the modern LogicalType annotation alongside the legacy converted type
  for strings, decimals, dates and timestamps (UTC microseconds). The LogicalType Thrift
  structs previously wrote no fields at all, so none was ever emitted.
- `doc/parquet_support.md` no longer claims V2 data pages are written; the writer emits V1.
- Delta reads start from the standard checkpoint other engines write (`_last_checkpoint` and
  a single-file `NN.checkpoint.parquet`) instead of replaying every JSON commit from version
  0. Vacuum also counts that checkpoint's files as referenced. Multi-part checkpoints are
  read too, and a log whose earlier commits were cleaned up reads from its checkpoint.
- A Delta write under the default file-lock coordinator reclaims an empty claim left by a
  crashed writer at once, instead of waiting out its 5-minute TTL. A DELETE, UPDATE or MERGE
  aborts when another writer changed the table's protocol or metadata (for example turned on
  `delta.appendOnly`) after it read the table; it previously committed anyway.
- Iceberg commits record `metadata-log` (each earlier metadata file) and `parent-snapshot-id`
  on every new snapshot, so engines such as PyIceberg see the table's history and lineage.
- Iceberg reads reject a `format-version` other than 1 or 2, and a data or delete file that
  is not Parquet, as `Kind.unsupported`. A v3 table previously read as v2, and an ORC file
  failed as corrupt Parquet. An equality delete written under an unpartitioned spec now
  applies to the whole table after partition evolution, and other equality deletes match by
  partition spec as well as partition values.
- An Iceberg filesystem commit re-reads the current metadata after winning its version claim
  and retries if another writer published in between. A writer that read the table before
  that publish could previously write a second metadata file at the same version.
- An Iceberg overwrite that changes the schema keeps the field ids of unchanged columns and
  numbers new columns above `last-column-id`. It previously renumbered from 1, so a new
  column could take a dropped column's id and engines binding by id read old snapshots into
  the wrong column.
- Iceberg writes widen int8 and int16 columns to int32, Iceberg's smallest integer. Small
  integers (which `fromRows` infers as int8) previously wrote data files that failed to read
  back with "Column id has type int8 but schema field declares int32".
- Delta `vacuumOrphans` no longer deletes live files whose logged path is URL-encoded (a
  partition value such as `50%` or `a b`) or absolute. It compared the logged form with the
  path on disk, so those files looked unreferenced.
- A Parquet page that decompresses to zero bytes, as pyarrow writes for an all-null column
  chunk, reads as empty. It previously failed with "maxSize must be positive".
- Booleans order as false before true. `WHERE flag > false` returned no rows, `MIN` and
  `MAX` of a boolean column returned null, and `ORDER BY` a boolean column (or DataFrame
  `orderBy`) threw a `TypeError`.
- `LEFT`, `RIGHT` and `FULL JOIN` without `ON` or `USING` are a parse error (`Kind.invalid`). They
  previously ran as a cross join. Plain and `INNER JOIN` do too (see Breaking).
- `ORDER BY 2` and `GROUP BY 1` are `Kind.invalid` (see Breaking). An integer there was
  previously a constant, so `ORDER BY 2` left the rows unsorted.
- SQL `--` line comments and `/* */` block comments are whitespace. A `--` comment was
  previously parsed as two minus signs, so `SELECT id --1` returned `id + 1`.
- A WHERE clause on a file no longer filters other scans of the same file in the query.
  `SELECT id FROM 'f' WHERE a < 1000 UNION ALL SELECT id FROM 'f'` returned the filtered
  rows twice, and an `IN (SELECT ... FROM 'f')` subquery saw only the outer query's rows.
  Pushed filters now belong to a single scan, and each UNION ALL branch prunes row groups
  with its own WHERE.
- A file referenced only in a UNION, INTERSECT or EXCEPT branch (or in a subquery inside
  one) is read. It previously failed with "File not found or could not be loaded".
- `LIKE`'s `_` matches one Unicode code point, so `'𝄞' LIKE '_'` is true.
- Avro deflate OCFs with highly repetitive data (over 200x compression) read back. The ratio
  guard rejected the library's own output; the byte budget still bounds inflation.
- Arrow record batches with many columns read with `maxRows: null`. The 10M cap was summed
  across every column's nodes; it is now the larger of 10M and one value per bit of the
  decompressed body.
- A Parquet column with a UTF8 annotation on a numeric or boolean type is reported as
  corrupt. It was read as a string column holding booleans or bytes. A UTF8
  FIXED_LEN_BYTE_ARRAY column, which DuckDB reads as text, reads as text.
- Iceberg writes fill each manifest-list entry's partition summaries (null counts, NaN
  presence and bounds), which were left null, so readers can skip manifests.

### Performance

Measured on an Apple M5 Max with Dart 3.13.4, AOT-compiled (`benchmark/`). "Before" is
1.0.1 with this release's hardening and correctness changes but none of the performance
work:

| Operation | Before | 2.0.0 |
|---|--:|--:|
| Parquet whole-file read, 1M rows, 8 columns, Snappy | 649 ms | 266 ms |
| Parquet whole-file read, flights 1M rows | 537 ms | 150 ms |
| Parquet string column read (`name`, 1M rows) | 125 ms | 37 ms |
| Parquet boolean column read (`active`, 1M rows) | 52 ms | 6 ms |
| SQL GROUP BY, 1M rows | 76 ms | 31 ms |
| SQL ORDER BY ... LIMIT 100, 1M rows | 703 ms | 26 ms |
| SQL UNION, 1M rows | 44 ms | 7 ms |
| SQL GROUP BY over a view, 1M rows | 95 ms | 47 ms |

Parquet reads decode into typed buffers, including dictionary, DELTA_BINARY_PACKED and
BYTE_STREAM_SPLIT pages, and parse the schema once per file. ORDER BY with LIMIT keeps only
the best rows. GROUP BY folds each batch into its groups as it arrives instead of holding
the rows. An inner hash join builds on the smaller side. A Delta or Iceberg query reads only
the columns it uses: on a 1M-row, 6-column table, `SELECT id` takes 10-11 ms on Delta and
18 ms on Iceberg, against 81-90 ms and 84 ms when every column is read.

## 1.0.1

- Fix a small linting issue.

## 1.0.0

First public release.

### DataFrame API

- Chainable, lazily-evaluated operations (`select`, `filter`, `orderBy`, `groupBy`, `agg`,
  `join`, `withColumn`, `distinct`, `union`, `sample`), executed on a columnar engine with
  vectorized filter and projection kernels.
- Typed columnar storage (`Column`, `Table`, `Schema`, `Field`) with null bitmaps, zero-copy
  slicing, and automatic schema inference.

### SQL engine

- Query files directly: `SELECT * FROM 'data.parquet' WHERE x > 10`, or register DataFrames
  and tables with `SqlEngine`.
- Joins across multiple files, CTEs and views, set operations, grouping sets, window-free
  aggregation with exact `BigInt` sums, and full three-valued (NULL) logic.
- `VALUES` as a table source (`SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(n, s)`), and
  `SELECT` without a `FROM` clause.
- `INSERT INTO '<delta table>' [(cols)] VALUES (...)`, alongside `UPDATE` and `MERGE`.
- Predicate and projection pushdown into the Parquet reader; `executeStream` for incremental
  results.
- Operators spill to disk past a configurable memory budget (default 256 MB), so `ORDER BY`,
  `GROUP BY` and equi-joins work on inputs larger than memory.

### Formats

- **CSV**: auto-detected delimiters and types, streaming batch reads, streaming writes.
- **JSON / JSON Lines**: streaming batch reads and writes.
- **Parquet**: read and write, with Snappy, GZIP, ZSTD and LZ4 (`LZ4_RAW` read+write, Hadoop
  `LZ4` read) codecs, optional page CRC32 verification, and nested list/struct/map types.
- **Arrow IPC**: read and write, streaming batches.
- **Delta Lake**: append/overwrite, `delete`/`update`/`merge`, time travel, deletion vectors,
  and pluggable commit coordinators for multi-writer safety.
- **Apache Iceberg**: snapshots, time travel, schema evolution, partitioning, positional and
  equality deletes, and pluggable catalogs.
- **Datasets**: read partitioned directory trees as a single table, with optional parallel
  reads across isolates.

### Correctness

Numeric values are never silently rewritten. Integer literals are typed by range, the fixed-width
Parquet encoders reject a value they would truncate, and a mixed integer/float column (CSV, JSON, or
`Schema.infer`) promotes to an exact `decimal` rather than rounding under float64. Where an exact
result is impossible (unifying `int64` with `float64` in a set operation), the conversion fails
loudly instead of rounding. JSON field flattening rejects a name collision rather than silently
keeping whichever value came last.

### Types

`Decimal` is an exact decimal value (unscaled `BigInt` + scale) with value-based equality, returned
by `Table.toRows()` for decimal columns, including integer `SUM`, which is exact beyond 2^63.

Temporal, decimal, and nested types round-trip across Arrow and Parquet. Read paths are bounded by
opt-out resource caps and fail loudly rather than silently truncating or corrupting data. Data files
are flushed to stable media before being published. Interoperability with PySpark, `deltalake` and
PyIceberg is verified by an opt-in test suite.

### Command line

- `libbigdata`: run SQL against CSV and Parquet files, with table/JSON/CSV output and an
  interactive REPL. Install with `dart pub global activate libbigdata`.
