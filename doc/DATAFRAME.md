# DataFrame API

The DataFrame API is a chainable, lazily-evaluated interface for tabular data.
Operations build a plan; nothing is read or computed until a terminal operation
runs. The whole chain is then fused into a single physical plan and streamed
once, over the same columnar engine the [SQL engine](SQL.md) uses.

```dart
import 'package:libbigdata/libbigdata.dart';

final df = await DataFrameIO.readCsv('people.csv');

final adults = await df
    .filter('age >= 18')
    .select(['name', 'age', 'city'])
    .orderBy('age', ascending: false)
    .limit(10)
    .execute();

print(adults.toRows());
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

### From files

`DataFrameIO` is an extension on `DataFrame` providing the readers and writers.

| Reader | Returns | Notes |
|---|---|---|
| `DataFrameIO.readCsv(path, [options])` | `Future<DataFrame>` | schema inference, `CsvOptions` for delimiters and headers |
| `DataFrameIO.readCsvOptimized(path, ...)` | `Future<DataFrame>` | bounded fast path |
| `DataFrameIO.readCsvBatches(path, ...)` | `Stream<DataFrame>` | out-of-core |
| `DataFrameIO.readJson(path, [options])` | `Future<DataFrame>` | |
| `DataFrameIO.readJsonBatches(path, ...)` | `Stream<DataFrame>` | JSON Lines |
| `DataFrameIO.readJsonLines(path)` | `Future<DataFrame>` | |
| `DataFrameIO.readParquet(path, ...)` | `Future<DataFrame>` | predicate and column pushdown |
| `DataFrameIO.readArrow(path)` | `Future<DataFrame>` | Arrow IPC file |
| `DataFrameIO.readArrowBatches(path, ...)` | `Stream<DataFrame>` | |
| `DataFrameIO.readDataset(path, ...)` | `Future<DataFrame>` | multi-file dataset |
| `DataFrameIO.readDelta(path, ...)` | `Future<DataFrame>` | Delta Lake, time travel |
| `DataFrameIO.readIceberg(path, ...)` | `Future<DataFrame>` | Iceberg, snapshots |

There are also `fromCsvString`, `fromJsonString` and `fromJsonLinesString` for
in-memory text, plus `fromCsv` / `fromJson` / `fromArrow` aliases of the
corresponding `read*` methods.

File-backed DataFrames are **lazy**. The source is not decoded until a terminal
operation runs, and the plan is rooted at a streaming file scan, so this reads
only as much of the file as it needs:

```dart
final df = await DataFrameIO.readParquet('huge.parquet');
final sample = await df.filter('status == "error"').head(10); // stops early
final total = await df.count();                               // never materializes
```

## Laziness and execution

Every transformation returns a **new** DataFrame. None mutate the receiver and
none do work. Calling a terminal operation folds the accumulated operations into
one physical plan and executes it in a single streaming pass, so there are no
per-operation intermediate tables, and a trailing `limit` short-circuits the
stream rather than materializing everything first.

One exception: `sample` cannot be expressed as a plan node. Where it appears,
the plan up to that point is materialized, `sample` is applied imperatively, and
fusion resumes from a fresh scan. Every other operation fuses.

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

`orderBy` sorts nulls first when ascending and last when descending.

`JoinType` is one of `inner`, `left`, `right`, `full`, `cross`. The `on`
argument is a single column name present in both sides, not a general join
condition. For anything richer, such as multi-key joins, inequality conditions
or semi/anti joins, use the [SQL engine](SQL.md).

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
of `avg`), `min`, `max`. Anything else throws `ArgumentError`. `min` and `max`
also work on strings.

Three behaviours to be aware of:

- **The output column keeps the input column's name.** `agg({'salary': 'sum'})`
  produces a column called `salary`, not `sum(salary)`. Chain `.rename({...})`
  if you want a different name.
- **One aggregate per column.** The argument is a `Map` keyed by column name, so
  two aggregates over the same column are not expressible. Use SQL for that.
- **`agg` reads group keys from the immediately preceding operation.** Anything
  between `groupBy` and `agg` silently drops the keys and degrades to a global
  aggregate. Always chain them directly.

With no `groupBy`, `agg` is a global aggregate returning exactly one row. Over
an empty input it returns one row of nulls.

Integer `sum` returns a `Decimal`, which is exact and cannot overflow, and
`avg`/`mean` return `double`.

## Expression syntax

`filter` and `withColumn` take strings, but they are two different, very small
languages. Both are deliberately narrow and neither is SQL. If you need more
than what follows, use the SQL engine.

### Filter predicates

```
predicate  := comparison ( (' AND ' | ' OR ') comparison )*
comparison := ( column | '[' column ']' ) operator literal
operator   := '==' | '=' | '!=' | '<>' | '<' | '<=' | '>' | '>='
```

```dart
df.filter('age >= 18')
df.filter('dept = \'sales\'')
df.filter('id >= 2 AND id <= 4')
df.filter('id == 1 OR id == 5')
df.filter('Dept Name == "y"')   // unquoted names may contain spaces
df.filter('[age]>=18')          // brackets make surrounding whitespace optional
```

`AND` and `OR` are case-insensitive, left-associative, and `AND` binds tighter
than `OR`, so `a == "x" OR id >= 4 AND id <= 5` means
`a == "x" OR (id >= 4 AND id <= 5)`. Literals are coerced using the *column's*
declared type: a numeric column parses the literal as a number, and a boolean
column yields `true` only for the text `true`.

**Not supported.** These are the sharp edges worth knowing:

| Construct | Result |
|---|---|
| Parentheses, `(a = 1 OR b = 2) AND c = 3` | throws `StateError: Unknown column: (a` |
| Arithmetic, `price * qty > 10` | throws `StateError: Unknown column: price * qty` |
| `IS NULL`, `IN`, `BETWEEN`, `LIKE`, `NOT` | throws `ArgumentError`, no operator found |
| No whitespace around the operator, `age>=18` | throws `ArgumentError`; bracket the name instead |
| Column against column, `a > b` | **compares against the literal string `"b"`** |

The last row is the only silent one. The right-hand side is always a literal,
never a column reference. Everything else fails loudly.

Because `AND` and `OR` are split on before column names are parsed, an
unbracketed column name containing a space-delimited `and` or `or`, for example
`Research And Development`, is split mid-name. Bracketing does not help here, so
restructure the query or use SQL.

Since parentheses are unavailable, express a grouped predicate by chaining
`filter` calls, which compose as `AND`:

```dart
df.filter('a == 1 OR a == 4').filter('b == 3'); // (a==1 OR a==4) AND b==3
```

### withColumn expressions

```
expression := operand ( ('+' | '-' | '*' | '/' | '%') operand )*
operand    := column | number | 'string' | "string"
```

```dart
df.withColumn('bonus', 'salary * 2')
df.withColumn('flag', '1')
df.withColumn('neg', 'salary * -1')
df.withColumn('salary', 'salary + 1')  // replaces the column in place
```

> **Operator precedence is flat.** Multiplication does not bind tighter than
> addition. Operators apply left to right, so `a + b * 2` evaluates as
> `(a + b) * 2`. With `a = 2, b = 3` the result is `10`, not `8`. Parentheses
> are also unsupported, so build a multi-step expression with successive
> `withColumn` calls.

Division always produces `float64`. Other operators produce `float64` if either
operand is a double and `int64` otherwise. Division and modulo by zero yield
`NULL`, following SQL semantics.

Not supported: parentheses, function calls such as `abs` or `coalesce`,
`CASE WHEN`, comparison and boolean operators, string concatenation (`+` is
numeric and throws on strings), and bracketed `[column]` names. An unrecognised
token is treated as a column name and throws `StateError: Unknown column: ...`
at execution time.

## Terminal operations

These execute the plan.

| Method | Returns | Description |
|---|---|---|
| `execute({maxRows, maxBytes, options})` | `Future<Table>` | Run the plan and materialize the result. |
| `executeStream({options})` | `Stream<Table>` | Run the plan, yielding batches. |
| `toList()` | `Future<List<Map<String, dynamic>>>` | Execute and convert to rows. |
| `toJsonString()` | `Future<String>` | Execute and JSON-encode the rows. |
| `head([int n = 5])` | `Future<Table>` | First `n` rows; limits before execution. |
| `tail([int n = 5])` | `Future<Table>` | Last `n` rows; materializes the result first. |
| `count()` | `Future<int>` | Row count, streamed without retaining columns. |
| `show([int rows = 20])` | `Future<void>` | Print a formatted preview. |
| `cache()` | `Future<DataFrame>` | Execute once and wrap the result for reuse. |

Non-executing accessors: `schema`, `columns`, `dtypes` (all derived eagerly,
`null` if unknown) and `clone()`.

`toTable()` and `transform(fn)` operate on the source table synchronously and
throw a `StateError` if operations are pending, so call `execute()` first. They
do not silently discard the pending chain.

### Streaming

`executeStream` yields batches for results larger than memory:

```dart
await for (final batch in df.filter('amount > 100').executeStream()) {
  process(batch);
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
final base = await df.filter('year == 2024').cache();
final byCity = await base.groupBy(['city']).agg({'amount': 'sum'}).execute();
final top = await base.orderBy('amount', ascending: false).head(10);
```

## Resource limits

`execute()` bounds materialization by default. It throws a `StateError` once the
result exceeds `maxRows` (10,000,000) or `maxBytes` (1 GiB). Pass `null` to opt
out of either.

```dart
await df.execute(maxRows: 1000);        // stricter
await df.execute(maxBytes: null);       // no byte ceiling
```

The limits propagate through every materialization boundary in the plan, not
just the final result, so a small cap also bounds a wide intermediate join or
union. They apply even to a no-op plan.

## Writing output

The writers are instance methods on a DataFrame, also from `DataFrameIO`.

```dart
await df.writeCsv('out.csv');
await df.writeJson('out.json');
await df.writeJsonLines('out.jsonl');
await df.writeArrow('out.arrow');
await df.writeDelta('table/', mode: DeltaWriteMode.append);
await df.writeIceberg('warehouse/table/');

final csv = await df.toCsv();        // to a string
final json = await df.toJson();
```

`writeDelta` and `writeIceberg` return the number of rows written. `writeCsv`
and `writeJson` stream batch by batch, while `toCsv` and `toJson` materialize
the whole result and are themselves bounded by the same row and byte caps.

For Parquet, `DataFrameIO.writeParquetStream` accrues row groups so it can write
files larger than memory.

## When to use SQL instead

The DataFrame API covers single-column sorts, single-key equi-joins and simple
arithmetic. Reach for [`SqlEngine`](SQL.md) when you need multi-key or non-equi
joins, semi/anti joins and subqueries, `IN` / `BETWEEN` / `LIKE` / `IS NULL`,
`CASE` and `CAST`, functions, multi-column `ORDER BY`, several aggregates over
one column, aliased aggregate output, `HAVING`, CTEs, or set operations.

The two interoperate. Register a DataFrame and query it:

```dart
final engine = SqlEngine();
await engine.registerDataFrame('people', df);
final result = await engine.sql('SELECT city, COUNT(*) c FROM people GROUP BY city');
```

`SqlEngine.sql` returns a `DataFrame`, so you can chain DataFrame operations
onto a SQL result.

## More

- [SQL.md](SQL.md) for the SQL engine.
- [DESIGN.md](DESIGN.md) for the architecture and execution model.
- [CLI.md](CLI.md) for the native command-line interface.
- [parquet_support.md](parquet_support.md) for Parquet format coverage.
</content>
