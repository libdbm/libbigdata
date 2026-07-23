# SQL Engine

libbigdata includes a SQL query engine written in pure Dart for analytical
workloads. It parses SQL to a logical plan, builds a physical plan, and executes
it over a columnar, batch-oriented engine. It can query registered in-memory
tables and DataFrames, or read CSV, JSON, and Parquet files directly by path.

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
- `WHERE` with `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `LIKE`
  (`%` and `_` wildcards), `IS [NOT] NULL`, `[NOT] BETWEEN`, and `[NOT] IN` with
  an explicit value list.
- `JOIN`: `INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`, with `ON` (single or
  `AND`-ed column equalities) and single- or multi-column `USING`.
- Subquery predicates in a top-level `WHERE` condition: `col IN (SELECT …)`,
  `col NOT IN (SELECT …)`, and `[NOT] EXISTS (SELECT …)`. These plan as
  semi/anti joins. Correlated `IN` and `EXISTS` are supported (the correlation
  must be `inner = outer` equalities in the subquery's `WHERE`); see the
  restrictions below.
- `GROUP BY` and `HAVING`.
- `ORDER BY` (multiple keys, `ASC`/`DESC`), `LIMIT`, and `OFFSET`. An aggregate
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
`SUM`, `AVG`, `MIN`, `MAX`. `SUM` over integer columns returns an
arbitrary-precision `BigInt` to avoid overflow.

Scalar functions:

- String: `UPPER`, `LOWER`, `LENGTH` (`CHAR_LENGTH`), `TRIM`, `CONCAT`,
  `SUBSTR`/`SUBSTRING`.
- Numeric: `ABS`, `ROUND`, `FLOOR`, `CEIL`/`CEILING`, `SQRT`, `POWER`/`POW`,
  `MOD`.
- Null handling: `COALESCE`.

Views and Delta DML:

- `CREATE [OR REPLACE] VIEW` (with `IF NOT EXISTS`) and `DROP VIEW [IF EXISTS]`.
- `UPDATE` and `MERGE` against a Delta Lake table addressed by path. These route
  through the Delta writer; they are not general catalog-table statements.

Files as tables — query files by quoting the path, and join across files:

```sql
SELECT city, COUNT(*) FROM 'people.csv' GROUP BY city;

SELECT o.id, c.name
FROM 'orders.parquet' o
JOIN 'customers.parquet' c ON o.customer_id = c.id;
```

For Parquet, column projection and simple `WHERE` predicates are pushed down into
the reader.

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
- `CREATE TABLE`, `DROP TABLE`, standalone `INSERT`, standalone `DELETE`.
- Multi-column `USING`, and `NULLS FIRST`/`NULLS LAST` in `ORDER BY`.

## Programmatic API

### SqlEngine

The entry point for SQL execution:

```dart
import 'package:libbigdata/libbigdata.dart';

final engine = SqlEngine();
```

A shared engine is also exported as the top-level `sql` value, ready to use
without construction. Its queries are serialized and share one catalog; for a
service, prefer a per-request `SqlEngine()`.

#### execute

```dart
Future<Table> execute(String query, {int? maxRows, int? maxBytes})
```

Executes a query and returns a `Table`.

```dart
final result = await engine.execute('SELECT COUNT(*) FROM employees');
print('Rows: ${result.rowCount}');
```

#### sql

```dart
Future<DataFrame> sql(String query)
```

Executes a query and returns a `DataFrame` for further chaining.

```dart
final df = await engine.sql('SELECT * FROM employees WHERE age > 30');
await df.writeCsv('output.csv');
```

#### executeStream

```dart
Stream<Table> executeStream(String query)
```

Executes a query and yields result batches as they are produced, for results
larger than memory.

```dart
await for (final batch in engine.executeStream('SELECT * FROM large_table')) {
  print('Batch: ${batch.rowCount} rows');
}
```

#### Registering data

```dart
void registerTable(String name, Table table);
Future<void> registerFile(String tableName, String filePath); // CSV, JSON, Parquet
Future<void> registerDataFrame(String name, DataFrame df);
```

```dart
final table = Table.fromRows('employees', [
  {'id': 1, 'name': 'Alice', 'age': 30},
  {'id': 2, 'name': 'Bob', 'age': 35},
]);
engine.registerTable('employees', table);

await engine.registerFile('sales', 'data/sales.csv');
await engine.registerFile('products', 'data/products.parquet');
```

Note the argument order of `Table.fromRows`: the table name comes first, then the
rows, with an optional named `schema`. Files can also be queried inline by
quoting the path, without registering them:

```dart
final rows = await engine.execute("SELECT * FROM 'data/sales.csv' LIMIT 10");
```

#### Utility methods

```dart
List<String> listTables();           // registered table names
Map<String, Table> getTables();      // all registered tables
Schema? getTableSchema(String name); // schema of one table
```

### Error handling

```dart
try {
  final result = await engine.execute('SELECT invalid_column FROM table');
} on FormatException catch (e) {
  // SQL parse error.
  print('Syntax error: $e');
} on StateError catch (e) {
  // Execution error (table not found, unknown column, etc.).
  print('Execution error: $e');
}
```

## Working with results

`execute` returns a `Table`:

```dart
final result = await engine.execute('SELECT name, age FROM employees');

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
await df.writeCsv('output.csv');
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
```

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

`CAST` supports `INTEGER`, `BIGINT`, `SMALLINT`, `FLOAT`, `DOUBLE`, `DECIMAL`,
`NUMERIC`, `VARCHAR`, `TEXT`, `BOOLEAN`, `DATE`, `TIME`, and `TIMESTAMP`.

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
| `FLOAT`   | `double`  | Single precision       | `3.14`                |
| `DOUBLE`  | `double`  | Double precision       | `2.718281828`         |
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

- [DATAFRAME.md](DATAFRAME.md) — the DataFrame API.
- [DESIGN.md](DESIGN.md) — architecture and execution model.
- [CLI.md](CLI.md) — the native command-line interface.
- [parquet_support.md](parquet_support.md) — Parquet format coverage.
