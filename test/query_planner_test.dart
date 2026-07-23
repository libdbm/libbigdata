import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/sql/catalog/table_catalog.dart';
import 'package:libbigdata/src/sql/parser/sql_parser.dart';
import 'package:libbigdata/src/sql/planner/query_planner.dart';
import 'package:libbigdata/src/sql/planner/physical_plan.dart';
import 'package:libbigdata/src/sql/operators/physical_operators.dart';
import 'package:test/test.dart';

/// Direct QueryPlanner structural tests (TC2). The planner (1900 LOC) had no
/// dedicated suite; these pin the physical-plan shape for the decisions that
/// matter (scan/filter/projection/join/aggregate/sort) so refactors and the
/// deferred nested-aggregate hoist have a guardrail.
void main() {
  late TableCatalog catalog;
  late SqlParser parser;
  late QueryPlanner planner;

  Table nums(String name) => Table(
    name: name,
    columns: [
      NumericColumn.int64(name: 'id', values: [1, 2, 3], nullable: false),
      NumericColumn.int64(name: 'v', values: [10, 20, 30], nullable: false),
      ListColumn<String>(
        name: 'k',
        dataType: DataType.string,
        data: ['a', 'b', 'a'],
        nullable: false,
      ),
    ],
    schema: Schema(
      fields: [
        Field(name: 'id', dataType: DataType.int64, nullable: false),
        Field(name: 'v', dataType: DataType.int64, nullable: false),
        Field(name: 'k', dataType: DataType.string, nullable: false),
      ],
    ),
  );

  setUp(() {
    catalog = TableCatalog();
    catalog.registerTable('t', nums('t'));
    catalog.registerTable('u', nums('u'));
    parser = SqlParser();
    planner = QueryPlanner(catalog);
  });

  PhysicalPlan planFor(String sql) => planner.plan(parser.parse(sql));

  // The physical nodes expose their children through per-type fields, not a
  // common accessor; enumerate them here for the tree walk.
  List<PhysicalPlan> childrenOf(PhysicalPlan p) => switch (p) {
    PhysicalAlias(:final input) => [input],
    PhysicalFilter(:final input) => [input],
    PhysicalProjection(:final input) => [input],
    PhysicalHashJoin(:final left, :final right) => [left, right],
    PhysicalAggregate(:final input) => [input],
    PhysicalSort(:final input) => [input],
    PhysicalLimit(:final input) => [input],
    PhysicalSetOperation(:final left, :final right) => [left, right],
    _ => const [],
  };

  // Walk the plan tree looking for a node of a given type.
  bool has<T>(PhysicalPlan p) => p is T || childrenOf(p).any(has<T>);

  test('a bare scan plans to a table scan', () {
    expect(has<PhysicalTableScan>(planFor('SELECT * FROM t')), isTrue);
  });

  test('a projection plans a PhysicalProjection', () {
    expect(has<PhysicalProjection>(planFor('SELECT id, v FROM t')), isTrue);
  });

  test('a WHERE plans a filter (or pushes it into the scan)', () {
    final p = planFor('SELECT * FROM t WHERE id > 1');
    expect(has<PhysicalFilter>(p) || has<PhysicalTableScan>(p), isTrue);
  });

  test('a join plans a PhysicalHashJoin', () {
    expect(
      has<PhysicalHashJoin>(planFor('SELECT * FROM t JOIN u ON t.id = u.id')),
      isTrue,
    );
  });

  test('a GROUP BY plans a PhysicalAggregate', () {
    expect(
      has<PhysicalAggregate>(planFor('SELECT k, SUM(v) FROM t GROUP BY k')),
      isTrue,
    );
  });

  test('a global aggregate plans a PhysicalAggregate', () {
    expect(has<PhysicalAggregate>(planFor('SELECT SUM(v) FROM t')), isTrue);
  });

  test('an ORDER BY plans a PhysicalSort', () {
    expect(has<PhysicalSort>(planFor('SELECT * FROM t ORDER BY v')), isTrue);
  });

  test('a LIMIT plans a PhysicalLimit', () {
    expect(has<PhysicalLimit>(planFor('SELECT * FROM t LIMIT 2')), isTrue);
  });

  // The outermost PhysicalAggregate — the one whose `aggregates` are what the
  // SELECT/HAVING above it read. (A DISTINCT rewrite stacks a second one below.)
  PhysicalAggregate? search(PhysicalPlan p) {
    if (p is PhysicalAggregate) return p;
    for (final child in childrenOf(p)) {
      final found = search(child);
      if (found != null) return found;
    }
    return null;
  }

  PhysicalAggregate aggregateOf(PhysicalPlan p) =>
      search(p) ?? (throw StateError('no PhysicalAggregate in plan'));

  test('a nested aggregate is hoisted, not rejected (CB3)', () {
    final p = planFor('SELECT SUM(v) + 1 FROM t');
    expect(has<PhysicalAggregate>(p), isTrue);
    // The aggregate step computes the bare SUM; the `+ 1` runs in a projection
    // above it, over the aggregate's output.
    expect(has<PhysicalProjection>(p), isTrue);
    expect(aggregateOf(p).aggregates.single.function, 'SUM');
  });

  test('an aggregate written twice is computed once', () {
    expect(
      aggregateOf(planFor('SELECT SUM(v) - SUM(v) FROM t')).aggregates,
      hasLength(1),
    );
  });

  test('distinct aggregates over the same column stay separate', () {
    // COUNT(v) and COUNT(DISTINCT v) are different aggregates; dedup must key on
    // the DISTINCT flag, not just the function name and argument.
    expect(
      aggregateOf(
        planFor('SELECT COUNT(v) + COUNT(DISTINCT v) FROM t'),
      ).aggregates,
      hasLength(2),
    );
  });

  test('an aggregate only in HAVING is still computed', () {
    expect(
      aggregateOf(
        planFor('SELECT k FROM t GROUP BY k HAVING SUM(v) > 5'),
      ).aggregates.single.function,
      'SUM',
    );
  });

  test('an aggregate in ORDER BY is hoisted, not rejected', () {
    final p = planFor(
      'SELECT k, SUM(v) AS s FROM t GROUP BY k ORDER BY SUM(v)',
    );
    expect(has<PhysicalSort>(p), isTrue);
    expect(aggregateOf(p).aggregates.single.function, 'SUM');
  });

  test('an aggregate written in both SELECT and ORDER BY is computed once', () {
    // The ORDER BY hoist must bind to the SELECT list's aggregate, not add a
    // second one — _bind dedups structurally.
    expect(
      aggregateOf(
        planFor('SELECT k, SUM(v) AS s FROM t GROUP BY k ORDER BY SUM(v)'),
      ).aggregates,
      hasLength(1),
    );
  });

  test('an aggregate only in ORDER BY is still computed', () {
    expect(
      aggregateOf(
        planFor('SELECT k FROM t GROUP BY k ORDER BY SUM(v)'),
      ).aggregates.single.function,
      'SUM',
    );
  });

  test('an aggregate in ORDER BY under DISTINCT is rejected at plan time', () {
    // The carry is unavailable under DISTINCT (an extra column changes which
    // rows are distinct), so this must stay loud rather than leak `__agg$0`.
    expect(
      () => planFor('SELECT DISTINCT k FROM t GROUP BY k ORDER BY SUM(v)'),
      throwsA(isA<UnsupportedError>()),
    );
  });
}
