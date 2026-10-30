# Design

This document describes how libbigdata is built: the columnar data model, the
query pipeline, and the cross-cutting mechanisms (spilling, resource limits,
pushdown, commit coordination) that the rest of the library is organised
around. For usage see [DATAFRAME.md](DATAFRAME.md) and [SQL.md](SQL.md).

## Table of contents

- [Shape of the codebase](#shape-of-the-codebase)
- [Data model](#data-model)
- [Query pipeline](#query-pipeline)
- [Execution model](#execution-model)
- [The DataFrame API on the same engine](#the-dataframe-api-on-the-same-engine)
- [Memory, spilling and limits](#memory-spilling-and-limits)
- [Pushdown](#pushdown)
- [Formats layer](#formats-layer)
- [Table formats and commit coordination](#table-formats-and-commit-coordination)
- [Errors](#errors)
- [Design principles](#design-principles)

## Shape of the codebase

Roughly 52,000 lines of Dart under `lib/`, with a single public API in
`lib/libbigdata.dart`. Everything else is under `lib/src/` and private.

| Area | Share | Contents |
|---|---|---|
| `src/formats` | ~53% | parquet, delta, iceberg, csv, arrow, avro, json, dataset |
| `src/sql` | ~30% | parser, ast, planner, operators, execution, exec |
| `src/core` | ~12% | data model plus cross-cutting policy |
| `src/api` | ~4% | `dataframe.dart`, `operations.dart`, `io_extensions.dart` |
| `src/util` | <1% | parallelism, line splitting, logging config |

The weighting is the main thing to take from this table. libbigdata is more a
file-format library with a query engine attached than a query engine with
readers attached. Parquet alone is the deepest subtree in the project, split
into `thrift/`, `reader/`, `writer/`, `encoding/`, `schema/` and
`filter/`.

## Data model

Everything in the data model is immutable and defensively wrapped at
construction.

```
Table
 |- Schema -> List<Field> (name, DataType, nullable, metadata)
 `- List<Column>          (one per field, all the same length)
      |- NumericColumn -> TypedData buffer + Uint8List null bitmap
      |- StringColumn  -> UTF-8 bytes + Int32List offsets + null bitmap
      |- BoolColumn    -> one bit a value + null bitmap
      `- ListColumn    -> boxed List<T?> + a hasNulls flag
```

**`DataType`** (`core/data_type.dart`) is a value class rather than an enum, so
it can carry `dartType`, `fixedSize` and the `isNumeric` / `isTemporal` /
`isNested` predicates alongside the singletons (`DataType.int8`,
`DataType.decimal`, `DataType.struct` and so on).

**`Field`** and **`Schema`** (`core/schema.dart`) hold the name, type,
nullability and metadata. `Field` also carries information deliberately excluded
from equality and `hashCode`: the join `qualifier`, `children` for nested types,
`precision`/`scale` for decimals, and the Iceberg field `id`. `Schema.resolve`
fails loud on an ambiguous name rather than picking one.

**`Column<T>`** (`core/column.dart`) is the sealed columnar contract:
`length`, `hasNulls`, `operator []`, `slice`, `filter`, `toList`, `stats` and
`memoryUsage`. It has four public types, each `abstract final`, built through
copying factories and read through safe members:

- `NumericColumn<T extends num>` is the typed path, holding one `TypedData`
  buffer plus an optional `Uint8List` null bitmap at one bit per row. Its
  per-width factories range-check before storing, because narrow typed lists
  silently wrap on overflow rather than throwing. `buffer` and `nullBitmap`
  are unmodifiable views.
- `StringColumn` holds its strings as one UTF-8 buffer with an offset per row,
  and compares and hashes the bytes without building Dart strings.
- `BoolColumn` holds one bit a value.
- `ListColumn<T>` is the boxed fallback for nested values and anything without
  a typed layout. It stores only a `hasNulls` flag rather than the null
  positions, deliberately: a set of null indices would be invisible to
  `memoryUsage` and would therefore defeat spill budgeting.

Each type's implementation is a vector class (`NumericVector`,
`StringVector`, `BoolVector`, `ListVector`) in `core/column_impl.dart`, a part
of the same library, which the package export hides. Only the vectors can
adopt an existing buffer without copying or hand out their live buffers, so
the readers, the kernels and the writers use them, and code outside the
package cannot.

**`Decimal`** (`core/decimal.dart`) is a `BigInt` unscaled value plus a scale.
Columns store the unscaled value while the scale lives on the `Field`, which is
why a `Column` alone cannot render a decimal. This split is the root of a
recurring class of bug, so decimal handling is worth touching carefully.

**`Table`** (`core/table.dart`) is a name, a schema, and one column per field,
validated on construction. Slicing copies only the rows it keeps.

## Query pipeline

```
SQL text
   |
   v  SqlParser (petitparser)
AST                        src/sql/ast/
   |
   v  LogicalPlanBuilder (a StatementVisitor and TableVisitor)
Logical plan               src/sql/planner/logical_plan.dart
   |
   v  PushDownFilter, then PushDownProjection
Optimized logical plan
   |
   v  PhysicalPlanBuilder
Physical plan              src/sql/planner/physical_plan.dart
   |
   v  pull-based iteration
RecordBatch stream
```

`SqlEngine` (`sql/execution/sql_engine.dart`) is the entry point and owns a
`TableCatalog`, a `SqlParser`, a `QueryPlanner` and a per-engine spill
directory. `QueryPlanner.plan` builds the logical plan and hands it to
`QueryPlanner.lower`, which runs the two pushdown rules and the physical
builder. `lower` is the single entry point for every logical plan, whether a
statement or a DataFrame built it.

The abstract execution contract does not live in `physical_plan.dart` but in
`sql/execution/exec_core.dart`, which defines `PhysicalPlan`,
`RecordBatchIterator` and `RecordBatch`. That file exists purely as a
dependency-inversion layer to break an import cycle between the physical plan
and the spill code. It is a good example of how the module boundaries here were
arrived at empirically rather than designed up front.

## Execution model

Execution is a **pull-based Volcano iterator model over columnar batches**. Every
operator is an async `RecordBatchIterator` exposing `next()` and `close()`, so a
plan is a chain of `next()` calls rather than a materialized dataflow. Operators
call into a vectorized kernel layer (`sql/exec/kernels.dart`) for the primitives:
`gather`, `concat`, mask application.

Operators are split across two locations. Scan, filter and projection live in
`planner/physical_plan.dart` (the file scan in `planner/file_scan.dart`); hash
join, aggregate, sort, limit, sample and set operations live in `sql/operators/`, which is assembled as a single Dart library from
`part` files under `physical_operators.dart`. The filesystem boundary there is
finer-grained than the language boundary.

There are three distinct batch sizes, which is worth knowing before tuning
anything:

| Constant | Value | Meaning |
|---|---|---|
| `defaultBatchSize` | 1024 | execution batch, `planner/physical_plan.dart` |
| `defaultScanBatch` | 10000 | format reader batch, `formats/scan.dart` |
| `defaultChunkSize` | 2048 | `DataChunk`, `core/data_chunk.dart` |

`DataChunk` and `RecordBatch` are two separate batch abstractions. The query
engine uses `RecordBatch` exclusively. `DataChunk` remains part of the core
public API but is not on the execution path.

`execute` drains the iterator into a list of batches, checking materialization
caps on each batch so it stops and throws at the cap rather than buffering and
then failing. `executeStream` builds the plan under the engine's serialization
lock and then releases that lock before draining. That is safe because the
physical plan binds concrete sources at plan time, so no operator dereferences
the catalog during iteration. Work on one engine is otherwise serialized, with
in-flight operation counting so `dispose()` never removes the spill directory
from under a live query.

## The DataFrame API on the same engine

A `DataFrame` is a source table, a list of `Operation` nodes, and a schema. Each
builder method returns a new DataFrame with one more operation appended; nothing
executes.

`DataFrame._buildPlan` turns the operations into one logical plan over a scan
of the source, in a catalog of its own, and lowers it with `QueryPlanner.lower`,
the entry point SQL uses, so the same optimizer rules and physical builder run.
Most operations have `Operation.lower(input)`, which returns their logical node
(`Projection`, `Filter`, `Sort`, `Limit`, `Distinct`, `Aggregate`, `Sample`).
`GroupByOperation` has no node of its own: its columns are the keys of the
`AggregateOperation` that must follow it. A join or union lowers the other frame
into the same catalog and combines the two plans into a `USING` `Join` or a
`Union`, so a pushed filter or projection reaches both sides. `sample` lowers
to a `Sample` node, which `PhysicalSample` runs: it buffers its input under the
memory budget and the options' `maxMaterializedBytes` (which `_buildPlan` sets
to the tighter of it and the caller's `maxBytes`), then emits the seeded
partial Fisher-Yates choice of `round(n * fraction)` rows. `PushDownFilter`
does not cross a `Sample`, since pruning its input would change the rows it
keeps.

The lazy file path works through `DataFrame.scan`, which seeds the operation
list with a `ScanOperation`. That operation is always the plan root;
`_buildPlan` registers the file as a lazy source and scans it, which lowers to a
`PhysicalFileScan`. This is what lets `readParquet(p).filter(...).head(n)`
stream and stop early instead of decoding the whole file.

The relationship runs both ways: `SqlEngine.sql` returns a `DataFrame` (as a
`Result`), and a DataFrame can be registered with SQL as a view over its
logical plan.

## Memory, spilling and limits

`core/limits.dart` is the single home for resource policy, holding both the
materialization caps (10M rows, 1 GiB) and the security ceilings for untrusted
input: footer bytes, schema depth and field count, Delta log bytes, table
metadata bytes, deletion vector bytes, and partitions per write. The shared
`enforceMaterializationCaps` is used by every reader and both query entry
points; it was hand-copied to more than eleven sites before being centralized.

`EngineOptions` carries the runtime knobs: spill threshold (256 MiB by default,
`0` disables), spill directory, output row cap, and optional `MemoryManager` and
`SpillBudget`. `EngineOptions.unbounded()` is the explicit opt-out.
`MemoryManager` is advisory rather than blocking: a denied reservation means
"spill", never "wait".

Operators that exceed the budget spill to disk instead of failing. Sort does an
external merge sort, aggregate does a grace/hash-partitioned aggregate with
recursive re-partitioning, and hash join does a grace hash join. Spill files use
one of two formats:

- **Parquet**, the default, is lossless and can be re-read one row group at a
  time. It is required wherever files are co-read or streamed, such as the sort
  merge and the join probe side.
- **Arrow** is cheaper to write and zero-copy for numerics, but is read whole
  file, so it is used only for budget-sized files that are fully consumed. It
  falls back to Parquet when the schema contains a type Arrow cannot encode.

`SpillBudget` caps combined live on-disk spill bytes and refunds compression
savings on finalize. Each engine spills under its own session directory marked
with a periodically refreshed liveness file, so a concurrent engine's orphan
sweep will not reclaim a live session.

## Pushdown

Two layers, kept independent of each other.

Logical pushdown runs in `planner/pushdown.dart`. `PushDownFilter` walks the
conjuncts and attaches a `RowFilter` to a scan only when every column in that
conjunct unambiguously belongs to that one scan. The `Filter` node is left in
place afterwards, so pushdown can only ever over-keep rows, never drop them
incorrectly. `PushDownProjection` runs second.

The predicate representation, `RowFilter`, lives in `core/row_filter.dart` and
is format-neutral. The Parquet-side file of the same name is a thin adapter that
supplies statistics-to-bounds conversion, so core never depends on Parquet
thrift definitions.

Parquet uses the predicate: `ParquetReader` prunes row groups against
per-column statistics on both the eager and streaming paths, and applies
projection to decode fewer leaf columns. The Delta, Iceberg and dataset scans
also use it to skip whole files before reading them as Parquet. CSV, JSON and
Arrow apply projection only and rely on the plan's exact filter operator for
correctness.

## Formats layer

Format dispatch is centralized in `openScan(path)` (`formats/scan.dart`). A
directory holding a `_delta_log` is a Delta table and one holding
`metadata/*.metadata.json` an Iceberg table; anything else goes through an
extension-based switch over Parquet, CSV, JSON Lines, JSON and Arrow. It raises
an `ArgumentError` on anything else rather than falling through to a default
format. Every entry point routes through it.

| Format | Reader | Writer |
|---|---|---|
| Parquet | `ParquetReader` | `ParquetWriter` |
| CSV | `CsvReader` | `CsvWriter` |
| JSON | `JsonReader` | `JsonWriter` |
| Arrow IPC | `ArrowReader` | `ArrowWriter`, `ArrowStreamWriter` |
| Avro | `AvroOcfReader`, `AvroReader` | `AvroOcfWriter`, `AvroWriter` |
| Delta Lake | `DeltaTable` | `DeltaWriter` |
| Iceberg | `IcebergTable` | `IcebergWriter` |
| Dataset | `Dataset` | |

Parquet is implemented down to the wire format, including a hand-written Thrift
compact protocol, the Dremel record shredding and assembly for nested types, and
the encodings (RLE/bit-packed, delta binary packed, delta byte array,
byte-stream-split).

## Table formats and commit coordination

Delta Lake and Iceberg both need atomic commits, so both are built on an
injectable coordinator boundary rather than a hardcoded filesystem protocol.

The shared primitive is `LocalCatalog` (`core/local_catalog.dart`), a
lock-guarded JSON registry mapping a table key to its current version and
metadata pointer, with per-key locks so a slow commit on one table does not
block another. It lives in core so it carries no format dependency.

`DeltaCommitCoordinator` has a single `commit` method that reclaims stale
claims, claims the next version, builds the actions, and publishes, retrying on
conflict but never on a hard error from the build step. Implementations are
filesystem, file-lock (the default, wrapping an inner coordinator in a real OS
lock) and local-catalog. Iceberg has the parallel structure, split into a
catalog and a backend.

Both document the same honest scope, which is worth repeating here. On a bare
filesystem the compare-and-swap window is narrowed but not closed. The
coordinator interface exists as the injection boundary for a real catalog such as
REST, Glue or Hive.

The commit outcome taxonomy is deliberately three-valued, and callers are
expected to distinguish the cases:

- **Success**: `Ok` with the committed version.
- **Conflict** (`Err` of `Kind.conflict`): the commit definitively did not
  land. The writer has cleaned up its staged files; retry.
- **Indeterminate** (`Err` of `Kind.indeterminate`): the outcome is unknown,
  typically a lost catalog response. The writer keeps its staged files. Inspect
  the pointer; do not blindly retry.

## Errors

Every public entry point that can fail in an expected way returns a
`Result<T, ErrorDetails>` (`core/result.dart`). Internal code keeps throwing:
an expected failure is raised as the internal `Failure` exception (carrying the
`ErrorDetails`) where it is detected, and converted once at the public method.
Nothing a `Result` method raises escapes it. Two conversions exist
(`core/failure.dart`):

- `capture`, for readers, writers and commits: a `Failure` keeps its kind, a
  `FormatException` is `corrupt`, a file-system exception is `missing` or `io`,
  a `TimeoutException` is `timeout`. Anything else, an `Error` included (a bug,
  or a failure in caller-supplied code such as a predicate or a custom
  coordinator), is reported as a fallback kind (`corrupt` for readers,
  `invalid` otherwise; `unsupported` for an `UnsupportedError`) with the error
  as the cause, and logged with its stack trace.
- `report`, for the SQL engine and DataFrame terminals, whose input
  (SQL text, predicate strings) is untrusted: the same, except that a
  `FormatException` is the expression's, so `invalid`.

Argument preconditions are the exception: they are checked before the
converted body runs, so they still throw (`size <= 0`, `retain < 1`,
`rowGroupSize <= 0`, a second `ParquetWriter.open`, `batchSize <= 0`, a
disposed engine).

## Design principles

The recurring commitments in the codebase, as actually implemented:

1. **Immutable by default.** Columns, tables, schemas and plans are all
   immutable and validated on construction.
2. **Lazy and fused.** Both APIs build plans and fold them into a single
   streaming execution rather than materializing per step.
3. **Fail loud over silently wrong.** Ambiguous name resolution, out-of-range
   narrowing, corrupt files and unknown formats all raise rather than guess.
4. **Bounded by default, opt out explicitly.** Caps are non-null by default and
   stop at the limit rather than buffering and then throwing. Passing `null`
   disables them.
5. **Spill rather than fail.** Blocking operators degrade to disk past the
   memory budget.
6. **Honest scope.** Where a guarantee cannot be made on a bare filesystem, it
   is documented as such and an injection boundary is provided instead of an
   implied guarantee.

## More

- [DATAFRAME.md](DATAFRAME.md) for the DataFrame API.
- [SQL.md](SQL.md) for the SQL engine.
- [parquet_support.md](parquet_support.md) for Parquet format coverage.
- [CLI.md](CLI.md) for the command-line interface.
</content>
