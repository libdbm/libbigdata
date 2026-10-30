# SQL Engine

libbigdata includes a SQL query engine written in pure Dart for analytical
workloads. It parses SQL to a logical plan, builds a physical plan, and executes
it over a columnar, batch-oriented engine. It can query registered in-memory
tables and DataFrames, or read CSV, JSON, Parquet and Arrow IPC files, and Delta
Lake and Iceberg table directories, directly by path.

- A recursive-descent parser built on the `petitparser` package.
- A Volcano-style iterator execution model over columnar chunks.
- Streaming execution for results larger than memory; operators that exceed the
  memory budget (`ORDER BY`, `GROUP BY`, joins, set operations) spill to disk
  rather than failing.
- A programmatic Dart API and a native command-line interface ([CLI.md](CLI.md)).

## Table of contents

- [Capabilities](#capabilities)
- [Programmatic API](#programmatic-api)
- [Working with results](#working-with-results)
- [Syntax reference](#syntax-reference)
- [Data types](#data-types)
- [Architecture](#architecture)
- [More](#more)

## Capabilities

### Supported

Queries:

- `SELECT` with `DISTINCT`, column aliases (`AS`), and expression projections.
- `WHERE` with `=` (also written `==`), `!=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `LIKE`
  (`%` and `_` wildcards), `IS [NOT] NULL`, `[NOT] BETWEEN`, and `[NOT] IN` with
  an explicit value list.
- `JOIN`: `INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`, with `ON` (single or
  `AND`-ed column equalities) and single- or multi-column `USING`. Every join
  but `CROSS JOIN` needs `ON` or `USING`.
- Subquery predicates in a top-level `WHERE` condition: `col IN (SELECT …)`,
  `col NOT IN (SELECT …)`, and `[NOT] EXISTS (SELECT …)`. These plan as
  semi/anti joins. Correlated `IN` and `EXISTS` are supported (the correlation
  must be `inner = outer` equalities in the subquery's `WHERE`); see the
  restrictions below.
- `GROUP BY` and `HAVING`. A `GROUP BY` item may be a `SELECT` alias when no input
  column has that name, and `HAVING` without `GROUP BY` treats the whole input as
  one group. Every selected column must be grouped or aggregated.
- `ORDER BY` (multiple keys, `ASC`/`DESC`, `NULLS FIRST`/`NULLS LAST`), `LIMIT`,
  and `OFFSET`. NULLs sort last in both directions unless `NULLS FIRST` is
  written, as in DuckDB. An aggregate
  may be written out in `ORDER BY` (`ORDER BY SUM(x) DESC`) as well as aliased;
  it is computed once whether or not it also appears in the `SELECT` list.
- Common table expressions: `WITH`, multiple CTEs, and CTE column aliases.
- Subqueries in the `FROM` clause (derived tables), which require an alias.
- `VALUES` tables in the `FROM` clause, written without enclosing parentheses:
  `FROM VALUES (1, 'a'), (2, 'b')`. Columns are named `column1`, `column2`, and
  so on.
- `SELECT` without a `FROM` clause, for constant expressions (`SELECT 1 + 1`).
- Set operations: `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`.
- `CASE` (simple and searched) and `CAST(expr AS type)`.

Aggregate functions: `COUNT(*)`, `COUNT(column)`, `COUNT(DISTINCT column)`,
`SUM`, `AVG`, `MIN`, `MAX`. `SUM` over integer columns returns an exact
`DECIMAL` (a `Decimal` in Dart), so it cannot overflow.

Scalar functions:

- String: `UPPER`, `LOWER`, `LENGTH` (`CHAR_LENGTH`), `TRIM`, `CONCAT`,
  `SUBSTR`/`SUBSTRING`. `SUBSTR` follows Postgres: positions start at 1, a start
  before the string counts toward the length, and a negative length is an error.
  `LENGTH` and `SUBSTR` count characters (Unicode code points), so a character
  outside the Basic Multilingual Plane, such as `'𝄞'`, counts once.
- Numeric: `ABS`, `ROUND`, `FLOOR`, `CEIL`/`CEILING`, `SQRT`, `POWER`/`POW`,
  `MOD`.
- Null handling: `COALESCE`.

Views and Delta DML:

- `CREATE [OR REPLACE] VIEW` (with `IF NOT EXISTS`) and `DROP VIEW [IF EXISTS]`. The
  body after `AS` may be written bare or in parentheses.
- `UPDATE`, `INSERT` and `MERGE` against a Delta Lake table addressed by path. These route
  through the Delta writer; they are not general catalog-table statements. A
  `MERGE` source is a file path, a parenthesized `SELECT`, or a table name, and
  its `ON` keys may have different names on the two sides (`ON t.id = s.key`).
  `INSERT INTO 'path' [(columns)] VALUES (...), ...` appends constant rows to an
  existing table; it does not create one.

Files as tables: query files by quoting the path, and join across files:

```sql
SELECT city, COUNT(*) FROM 'people.csv' GROUP BY city;

SELECT o.id, c.name
FROM 'orders.parquet' o
JOIN 'customers.parquet' c ON o.customer_id = c.id;
```

For Parquet, column projection and simple `WHERE` predicates are pushed down into
the reader.

A Delta Lake or Apache Iceberg table is queried by quoting its directory: the
one holding `_delta_log`, or `metadata/*.metadata.json`. Simple `WHERE`
predicates skip data files whose partition values or column bounds cannot
match (for Iceberg, whole manifests too, by the manifest list's partition
summaries), then prune row groups in the rest. Only the columns the query
uses are read from each data file:

```sql
SELECT id, amount FROM 'warehouse/orders' WHERE region = 'eu';
```

### Not supported

Using any of these raises a parse or execution error rather than returning
incorrect results:

- Window functions (`ROW_NUMBER`, `RANK`, `OVER`, `PARTITION BY`).
- `IN (SELECT …)` / `EXISTS` outside a top-level `WHERE` condition (a `SELECT`
  item, `HAVING`, or inside `OR` / `CASE`).
- **Correlated `NOT IN (SELECT …)`** (its null semantics are per-row and can't be
  decorrelated to one anti join) and **uncorrelated `EXISTS`**.
- A correlated `IN`/`EXISTS` subquery that uses `GROUP BY`, `DISTINCT`, an
  aggregate, a set operation, `LIMIT`, or a non-equi / non-top-level correlation.
- Scalar subqueries (`SELECT (SELECT …)`) and subqueries in a `SELECT` list.
- Aggregates in `ORDER BY` **when combined with `DISTINCT` or a set operation**;
  alias the aggregate in the `SELECT` list and order by the alias instead.
- Statistical aggregates (`STDDEV`, `VARIANCE`).
- Date/time functions (`DATE_TRUNC`, `YEAR`, `NOW`, `DATEDIFF`, and similar).
- Recursive CTEs (`WITH RECURSIVE`).
- `CREATE TABLE`, `DROP TABLE`, `INSERT ... SELECT`, `INSERT` into a registered
  table, and `DELETE`.

## Programmatic API

### SqlEngine

The entry point for SQL execution:

```dart
import 'package:libbigdata/libbigdata.dart';

final opened = SqlEngine.open(); // Result<SqlEngine, ErrorDetails>
if (opened case Ok(value: final engine)) {
  // ... use the engine, then:
  await engine.dispose();
}
```

`SqlEngine.open` fails with `Kind.io` only when its spill directory cannot be
created. A shared engine sits behind the top-level `sql(query)` function, ready
to use without opening one. Its queries are serialized and share one catalog;
for a service, prefer a per-request `SqlEngine.open()`.

Every query method returns a `Result`: `Ok` with the value, or `Err` with an
`ErrorDetails`. See [Error handling](#error-handling).

#### execute

```dart
Future<Result<Table, ErrorDetails>> execute(
  String query, {
  int? maxRows,
  int? maxBytes,
  Duration? timeout,
})
```

Executes a query and returns a `Table`.

```dart
if (await engine.execute('SELECT COUNT(*) FROM employees') case Ok(
  :final value,
)) {
  print('Rows: ${value.rowCount}');
}
```

#### sql

```dart
Future<Result<DataFrame, ErrorDetails>> sql(String query, {Duration? timeout})
```

Executes a query and returns a `DataFrame` for further chaining.

```dart
final adults = await engine.sql('SELECT * FROM employees WHERE age > 30');
if (adults case Ok(value: final df)) {
  final written = await df.writeCsv('output.csv'); // Result<void, ErrorDetails>
}
```

#### executeStream

```dart
Stream<Result<Table, ErrorDetails>> executeStream(String query, {Duration? timeout})
```

Executes a query and yields result batches as they are produced, for results
larger than memory. Each batch is an `Ok`; a failure arrives as one `Err` and
ends the stream.

```dart
await for (final result in engine.executeStream('SELECT * FROM large_table')) {
  switch (result) {
    case Ok(:final value):
      print('Batch: ${value.rowCount} rows');
    case Err(:final error):
      print('Stopped: $error');
  }
}
```

#### Registering data

```dart
void registerTable(String name, Table table);
Future<Result<void, ErrorDetails>> registerFile(String tableName, String filePath); // CSV, JSON, Parquet, Arrow, a Delta or Iceberg table directory
Future<Result<void, ErrorDetails>> registerDataFrame(String name, DataFrame df);
```

```dart
final table = Table.fromRows('employees', [
  {'id': 1, 'name': 'Alice', 'age': 30},
  {'id': 2, 'name': 'Bob', 'age': 35},
]);
engine.registerTable('employees', table);

for (final (name, path) in [
  ('sales', 'data/sales.csv'),
  ('products', 'data/products.parquet'),
]) {
  if (await engine.registerFile(name, path) case Err(:final error)) {
    print('could not register $path: $error');
  }
}
```

`registerDataFrame` registers the frame as a view; it does not run it. Each
query that reads the name plans the frame's operations together with its own,
through the same optimizer, so a `WHERE` on a file-backed frame prunes its row
groups and the query reads only the columns it uses:

```dart
if (await DataFrameIO.readParquet('data/events.parquet') case Ok(value: final events)) {
  await engine.registerDataFrame('events', events);
  // Reads only `id` and `kind`, and only the row groups that can hold id >= 900.
  final recent = await engine.execute('SELECT kind FROM events WHERE id >= 900');
}
```

The table's columns are the frame's, named as `DataFrame.columns` names them
(`p.name` and `d.name` after a join with a name collision, written
`"p.name"` in SQL), and the query's alias qualifies them. Each query reads the
frame's sources again. The frame's own tables and files are held apart from the
engine's names, so they neither take nor hide a registered name. Registering a
table or file under the same name replaces the frame, and a CTE of that name
hides it for its query.

```dart
Future<Result<void, ErrorDetails>> registerDataFrame(
  String name,
  DataFrame df, {
  int? maxRows = defaultMaxMaterializedRows,
  int? maxBytes = defaultMaxMaterializedBytes,
});
```

`maxRows`/`maxBytes` bound the result of each `execute` that reads the frame,
as a reader's caps bound a frame built on it: the tighter of them, the caps of
the readers the frame is built on and the query's own applies, and a result past
it fails with `Kind.limit`. They bound the result, not the rows read, so
`SELECT COUNT(*)` over a larger frame succeeds; `executeStream` sets no result
cap. An invalid expression or unknown column in the frame fails
`registerDataFrame`, which plans the frame once; what only reading can show (a
source file removed since) fails the query.

Note the argument order of `Table.fromRows`: the table name comes first, then the
rows, with an optional named `schema`. Files can also be queried inline by
quoting the path, without registering them:

```dart
final rows = await engine.execute("SELECT * FROM 'data/sales.csv' LIMIT 10");
// Result<Table, ErrorDetails>
```

#### Utility methods

```dart
List<String> listTables();           // registered table names
Map<String, Table> get tables;       // all registered tables
Schema? schema(String name);         // schema of one table
```

### Error handling

SQL text is treated as untrusted input, so a query never throws; every failure
comes back as an `Err` whose `kind` says what went wrong:

```dart
switch (await engine.execute('SELECT invalid_column FROM employees')) {
  case Ok(:final value):
    print(value.toRows());
  case Err(:final error) when error.kind == Kind.invalid:
    print('Bad query: ${error.message}'); // parse error, unknown column, types
  case Err(:final error) when error.kind == Kind.missing:
    print('No such table or file: ${error.message}');
  case Err(:final error):
    print('Query failed: $error'); // denied, limit, timeout, corrupt, ...
}
```

| Kind | Meaning |
| --- | --- |
| `invalid` | SQL that does not parse or type-check (including syntax the grammar lacks, such as window functions), or an expression that fails at run time (overflow) |
| `unsupported` | SQL that parses but this engine does not implement (`WITH RECURSIVE`, scalar subqueries, unknown functions) |
| `missing` | An unknown table, view or file |
| `denied` | A path outside the sandbox, or a write on a read-only engine |
| `corrupt` | A data file that cannot be read |
| `limit` | A result, memory or spill cap |
| `timeout` | The query's deadline, or `lockTimeout` |
| `schema`, `conflict`, `indeterminate`, `io` | From `UPDATE`, `INSERT` and `MERGE`, as the Delta writer reports them |

Only calling a disposed engine throws (`StateError`).

## Working with results

`execute` returns a `Table` when it succeeds:

```dart
final Table result;
switch (await engine.execute('SELECT name, age FROM employees')) {
  case Ok(:final value):
    result = value;
  case Err(:final error):
    return print(error);
}

print('Rows: ${result.rowCount}');
print('Columns: ${result.schema.fieldNames}');

// Access columns by name.
final names = result.column('name')!;
final ages = result.column('age')!;
for (var i = 0; i < result.rowCount; i++) {
  print('${names[i]}, ${ages[i]}');
}

// Convert to a list of row maps.
final asRows = result.toRows();

// Convert to a DataFrame for further operations.
final df = DataFrame.fromTable(result);
final written = await df.writeCsv('output.csv');
```

Iterate large results in chunks with `toChunks`:

```dart
for (final chunk in result.toChunks(1000)) {
  print('Processing chunk: ${chunk.rowCount} rows');
}
```

## Syntax reference

### Statement structure

```sql
[WITH cte_name [(columns)] AS (SELECT ...)]
SELECT [DISTINCT] select_list
FROM table_reference [alias]
[JOIN table_reference ON condition]
[WHERE condition]
[GROUP BY expression [, ...]]
[HAVING condition]
[ORDER BY expression [ASC | DESC] [, ...]]
[LIMIT count [OFFSET start]]
[UNION | UNION ALL | INTERSECT | EXCEPT ...]
```

### Quoting and comments

- `'text'` is a string. A doubled quote inside it is a literal quote: `'it''s'`.
- A number with a decimal point is an exact `DECIMAL`, as in DuckDB: `1.50` is
  `DECIMAL(3,2)`, and `0.1 + 0.2` is exactly `0.3`. A number with an exponent
  (`1e3`, `2.5E-1`) is a `DOUBLE`, as is one with more than 38 digits. Write
  `CAST(2.5 AS DOUBLE)` for a double without an exponent.
- `"name"` and `` `name` `` are identifiers. Use them for a column whose name is a
  keyword or contains spaces: `SELECT "order", "two words" FROM t`. A `"..."`
  that names no column raises `Unknown column` with a reminder that strings take
  single quotes.
- After `FROM`, a quoted path in either quote style is a file:
  `FROM 'data.parquet'` and `FROM "data.parquet"` read the same file.
- `-- ...` runs to the end of the line and `/* ... */` may span lines. Both are
  whitespace, and neither counts toward the nesting limit.
- Limits on untrusted SQL, each a `Kind.invalid` when exceeded: 1,048,576 characters of text;
  256 levels of parentheses or `CASE`; an expression tree 4,096 levels deep (a
  chain of `+` or `AND` adds a level per operator, and nested chains add up);
  2,048 branches in one chain of `EXCEPT`/`INTERSECT`. A chain of `UNION` and
  `UNION ALL` has no branch limit.

### SELECT and FROM

```sql
SELECT id, name, age FROM employees;
SELECT * FROM employees;
SELECT name AS employee_name, age * 12 AS age_months FROM employees;
SELECT DISTINCT department FROM employees;

-- A subquery (derived table) requires an alias.
SELECT dept, avg_salary
FROM (
  SELECT department AS dept, AVG(salary) AS avg_salary
  FROM employees
  GROUP BY department
) summary
WHERE avg_salary > 70000;
```

### JOIN

```sql
SELECT e.name, d.name AS department
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id;

SELECT e.name, d.name AS dept, p.title AS project
FROM employees e
JOIN departments d ON e.dept_id = d.id
JOIN projects p ON e.project_id = p.id;
```

### WHERE

```sql
SELECT * FROM employees WHERE age > 30;
SELECT * FROM employees WHERE department = 'Engineering' AND salary > 70000;
SELECT * FROM employees WHERE name LIKE 'A%';
SELECT * FROM employees WHERE manager IS NOT NULL;
SELECT * FROM employees WHERE age BETWEEN 30 AND 40;
SELECT * FROM employees WHERE department IN ('Engineering', 'Sales');

-- Subquery predicates (top-level WHERE only). These plan as semi/anti joins.
SELECT * FROM employees e
WHERE e.dept_id IN (SELECT id FROM departments WHERE active);

SELECT * FROM employees e
WHERE EXISTS (SELECT 1 FROM sales s WHERE s.employee_id = e.id);
```

`IN` accepts either an explicit value list or a single-column subquery. A
subquery `IN`/`EXISTS` is only planned in a top-level `WHERE` condition, and a
correlation must be `inner = outer` equalities in the subquery's `WHERE`; see
[Capabilities](#capabilities) for the exact restrictions.

### Aggregation

```sql
SELECT department,
       COUNT(*) AS employee_count,
       AVG(salary) AS avg_salary,
       SUM(salary) AS total_payroll
FROM employees
GROUP BY department
HAVING COUNT(*) >= 5
ORDER BY avg_salary DESC;
```

### ORDER BY and LIMIT

```sql
SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 10;

-- Multiple keys, and paging with OFFSET.
SELECT * FROM employees ORDER BY department ASC, salary DESC;
SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 10 OFFSET 10;

-- ORDER BY and GROUP BY take a column name, a select alias or an expression.
SELECT department, AVG(salary) AS average FROM employees
GROUP BY department ORDER BY average DESC;
```

Neither clause takes a position: `ORDER BY 2` and `GROUP BY 1` are errors
(`Kind.invalid`), unlike DuckDB, so a query never depends on the select list's
order.

### Common table expressions

```sql
WITH high_performers AS (
  SELECT * FROM employees WHERE performance_rating >= 4
),
department_stats AS (
  SELECT department, COUNT(*) AS count, AVG(salary) AS avg_salary
  FROM high_performers
  GROUP BY department
)
SELECT department, count, avg_salary
FROM department_stats
ORDER BY count DESC;
```

### Set operations

```sql
SELECT name FROM current_employees
UNION
SELECT name FROM former_employees;
```

### CASE and CAST

```sql
SELECT name,
       CASE WHEN salary > 100000 THEN 'Executive'
            WHEN salary > 75000  THEN 'Senior'
            WHEN salary > 50000  THEN 'Mid-Level'
            ELSE 'Entry-Level'
       END AS salary_band,
       CAST(age AS VARCHAR) AS age_text
FROM employees;
```

`CAST` supports `TINYINT`, `SMALLINT`, `INTEGER`/`INT`, `BIGINT`,
`DOUBLE`/`FLOAT`/`REAL`, `DECIMAL`/`NUMERIC`, `VARCHAR`/`TEXT`, `BOOLEAN`/`BOOL`, `DATE`
and `TIMESTAMP`/`DATETIME`. The rules follow DuckDB:

- An integer type has its width (`TINYINT` 8 bits, `SMALLINT` 16, `INTEGER` 32,
  `BIGINT` 64), and a value that does not fit is an error.
- A fractional value cast to an integer rounds half away from zero:
  `CAST(2.5 AS INTEGER)` is 3 and `CAST(-2.5 AS INTEGER)` is -3.
- A number cast to `BOOLEAN` is true when nonzero. Text is true for `true`, `t`,
  `yes`, `y` or `1` and false for `false`, `f`, `no`, `n` or `0`, in any case.
- Text that does not parse as the target type casts to `NULL` (DuckDB raises
  an error instead).
- `DATE` and `TIMESTAMP` parse ISO-8601 (`'2024-01-01'`, `'2024-01-01 10:30:00'`,
  `'2024-01-01T10:30:00+02:00'`). A string without a zone is read as UTC. `DATE`
  keeps the calendar day of a timestamp. A field out of range (month 13,
  February 30, hour 24) does not parse, so `CAST('2024-02-30' AS DATE)` is
  `NULL` and `DATE '2024-02-30'` is a parse error.
- `TIME` raises an unsupported-operation error; there is no time-of-day type.

`DATE '2024-01-01'` and `TIMESTAMP '2024-01-01 10:30:00'` are literals; malformed
text is a parse error. Comparing a date or timestamp with a string literal reads
the string the same way, so `ts > '2024-06-01'` and `ts > TIMESTAMP '2024-06-01'`
match the same rows. A string that is not a date is an error (`Kind.invalid`).

Numbers and strings are never converted implicitly. `WHERE n = '1'`, `n < '3'`,
`n IN ('1', '2')` and `n + '1'` on a numeric `n` are errors (`Kind.invalid`)
before the query runs; write the number, or `CAST('1' AS BIGINT)`.

### Operators

```sql
SELECT salary + bonus, salary - tax, salary * 1.1, salary / 12, age % 2
FROM employees;

WHERE (dept = 'Sales' OR dept = 'Marketing')
  AND salary > 60000
  AND NOT (age < 21 OR age > 65)
```

### Pattern matching and null tests

```sql
WHERE name LIKE 'John%'          -- starts with 'John'
  AND email LIKE '%@gmail.com'   -- ends with '@gmail.com'
  AND phone LIKE '555-____'      -- 555- followed by 4 characters
  AND manager IS NULL
  AND bonus IS NOT NULL
```

### Views and Delta DML

```sql
CREATE VIEW active AS SELECT * FROM employees WHERE status = 'active';
DROP VIEW IF EXISTS active;

UPDATE 'warehouse/employees' SET salary = salary * 1.05 WHERE department = 'Sales';

MERGE INTO 'warehouse/employees' USING updates
  ON employees.id = updates.id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;
```

## Data types

| SQL type  | Dart type | Description            | Example               |
|-----------|-----------|------------------------|-----------------------|
| `INTEGER` | `int`     | 32-bit signed integer  | `42`                  |
| `BIGINT`  | `int`     | 64-bit signed integer  | `9223372036854775807` |
| `FLOAT`   | `double`  | Single precision when read from a file; `CAST(x AS FLOAT)` is `DOUBLE` | `3.14e0` |
| `DOUBLE`  | `double`  | Double precision       | `2.718281828e0`       |
| `DECIMAL(p,s)` | `Decimal` | Exact decimal     | `1.50`                |
| `VARCHAR` | `String`  | Variable-length string | `'Hello World'`       |
| `BOOLEAN` | `bool`    | Boolean                | `TRUE`, `FALSE`       |

Null semantics: `COUNT(*)` counts all rows while `COUNT(column)` excludes nulls;
use `IS NULL` / `IS NOT NULL` to test for nulls, and `COALESCE(a, b, ...)` to
return the first non-null argument.

## Architecture

```
SQL text
   |
   v
Parser (petitparser)   ->  AST (statements, expressions)
   |
   v
Query planner          ->  Logical plan
   |
   v
Physical planner       ->  Physical plan (iterators, operators)
   |
   v
Execution engine       ->  Columnar data chunks (batched, streaming)
```

1. Parser: a `petitparser` grammar producing an AST.
2. Query planner: lowers the AST to a logical plan, then a physical plan.
3. Execution engine: a Volcano-style iterator model over columnar chunks.
4. Storage integration: columnar readers/writers for CSV, JSON, and Parquet.

For the data model, execution model, spilling, pushdown and commit coordination
in full, see [DESIGN.md](DESIGN.md).

## More

- [DATAFRAME.md](DATAFRAME.md): the DataFrame API.
- [DESIGN.md](DESIGN.md): architecture and execution model.
- [CLI.md](CLI.md): the native command-line interface.
- [parquet_support.md](parquet_support.md): Parquet format coverage.
