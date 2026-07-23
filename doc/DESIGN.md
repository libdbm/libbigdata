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
- [Design principles](#design-principles)

## Shape of the codebase

Roughly 45,500 lines of Dart under `lib/`, with a single public API in
`lib/libbigdata.dart`. Everything else is under `lib/src/` and private.

| Area | Share | Contents |
|---|---|---|
| `src/formats` | ~55% | parquet, delta, iceberg, csv, arrow, avro, json, dataset |
| `src/sql` | ~29% | parser, ast, planner, operators, execution, exec |
| `src/core` | ~10% | data model plus cross-cutting policy |
| `src/api` | ~4% | `dataframe.dart`, `io_extensions.dart` |
| `src/util` | <1% | parallelism, line splitting, logging config |

The weighting is the main thing to take from this table. libbigdata is more a
file-format library with a query engine attached than a query engine with
readers attached. Parquet alone is the deepest subtree in the project, split
into `core/`, `thrift/`, `reader/`, `writer/`, `encoding/`, `schema/` and
`filter/`.

## Data model

Everything in the data model is immutable and defensively wrapped at
construction.

```
Table
 |- Schema -> List<Field> (name, DataType, nullable, metadata)
 `- List<Column>          (one per field, all the same length)
      |- NumericColumn -> TypedData buffer + Uint8List null bitmap
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

**`Column<T>`** (`core/column.dart`) is the abstract columnar contract:
`length`, `hasNulls`, `operator []`, `slice`, `filter`, `toList`, `stats` and
`memoryUsage`. There are two implementations, and the split matters:

- `NumericColumn<T extends num>` is the typed path, holding one `TypedData`
  buffer plus an optional `Uint8List` null bitmap at one bit per row. Its
  per-width factories range-check before storing, because narrow typed lists
  silently wrap on overflow rather than throwing.
- `ListColumn<T>` is the boxed fallback for strings, nested values and anything
  without a typed buffer. It stores only a `hasNulls` flag rather than the null
  positions, deliberately: a set of null indices would be invisible to
  `memoryUsage` and would therefore defeat spill budgeting.

**`Decimal`** (`core/decimal.dart`) is a `BigInt` unscaled value plus a scale.
Columns store the unscaled value while the scale lives on the `Field`, which is
why a `Column` alone cannot render a decimal. This split is the root of a
recurring class of bug, so decimal handling is worth touching carefully.

**`Table`** (`core/table.dart`) is a name, a schema, and one column per field,
validated on construction. Slicing is zero-copy.

## Query pipeline

```
SQL text
   |
   v  SqlParser (petitparser)
AST                        src/sql/ast/
   |
   v  LogicalPlanBuilder (an ASTVisitor)
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
directory. `QueryPlanner.plan` runs the three stages above explicitly.

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

Operators are split across two locations. Scan, filter, projection and hash join
live in `planner/physical_plan.dart`; aggregate, sort, limit and set operations
live in `sql/operators/`, which is assembled as a single Dart library from
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

`Operation` has two methods:

- `toPhysical(input, groupKeys, ctx)` folds the operation into the growing
  physical plan, returning `null` if it cannot be a plan node.
- `apply(table, ctx)` is the imperative fallback.

`DataFrame._buildPlan` folds every operation through `toPhysical` into one
physical plan. `GroupByOperation` produces no node of its own and instead
stashes group keys for the following aggregate. When `toPhysical` returns
`null`, currently only for `sample`, the plan so far is materialized, `apply`
runs imperatively, and fusion restarts from a fresh table scan. So the accurate
statement is one physical plan per unfusible boundary, not strictly one per
DataFrame.

The lazy file path works through `DataFrame.scan`, which seeds the operation
list with a `ScanOperation`. That operation is always the plan root and throws
if it is ever applied or folded; `_buildPlan` special-cases it into a
`PhysicalFileScan`. This is what lets `readParquet(p).filter(...).head(n)`
stream and stop early instead of decoding the whole file.

The relationship runs both ways: `SqlEngine.sql` returns a `DataFrame`, and a
DataFrame can be registered as a SQL table.

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

Only Parquet uses the predicate: `ParquetReader` prunes row groups against
per-column statistics on both the eager and streaming paths, and applies
projection to decode fewer leaf columns. CSV and JSON apply projection only and
rely on the plan's exact filter operator for correctness.

## Formats layer

Format dispatch is centralized in `openScan(path)` (`formats/scan.dart`), an
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
injectable coordinator seam rather than a hardcoded filesystem protocol.

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
coordinator interface exists as the injection seam for a real catalog such as
REST, Glue or Hive.

The commit outcome taxonomy is deliberately three-valued, and callers are
expected to distinguish the cases:

- **Success.**
- **Conflict** (`CommitConflict`, `IcebergCommitConflict`): the commit
  definitively did not land. Retry and clean up staged files.
- **Indeterminate** (`DeltaCommitIndeterminate`, `IcebergCommitIndeterminate`):
  the outcome is unknown, typically a lost catalog response. Inspect the
  pointer. Do not blindly retry, and do not delete staged files.

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
   is documented as such and an injection seam is provided instead of an
   implied guarantee.

## More

- [DATAFRAME.md](DATAFRAME.md) for the DataFrame API.
- [SQL.md](SQL.md) for the SQL engine.
- [parquet_support.md](parquet_support.md) for Parquet format coverage.
- [CLI.md](CLI.md) for the command-line interface.
</content>
