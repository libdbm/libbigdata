import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/sql/ast/ast.dart'
    show
        ColumnExpression,
        ExistsExpression,
        FunctionExpression,
        QueryStatement,
        SelectStatement;
import 'package:libbigdata/src/sql/catalog/table_catalog.dart';
import 'package:libbigdata/src/sql/execution/references.dart';
import 'package:libbigdata/src/sql/parser/sql_parser.dart';
import 'package:libbigdata/src/sql/planner/expressions.dart';
import 'package:libbigdata/src/sql/planner/logical_plan.dart' as logical;
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
    PhysicalTopN(:final input) => [input],
    PhysicalLimit(:final input) => [input],
    PhysicalSetOperation(:final left, :final right) => [left, right],
    PhysicalUnion(:final inputs) => inputs,
    _ => const [],
  };

  // Walk the plan tree looking for a node of a given type.
  bool has<T>(PhysicalPlan p) => p is T || childrenOf(p).any(has<T>);

  test('a logical plan lowers through the rules and the builder', () {
    // ORDER BY + LIMIT built by hand, as DataFrame will: lowering runs the
    // optimizer rules and the physical builder, which picks top-N.
    final scan = logical.TableScan(
      tableName: 't',
      tableSchema: catalog.table('t')!.schema,
    );
    final plan = logical.Limit(
      input: logical.Sort(
        input: scan,
        sortExpressions: [
          logical.SortExpression(
            expression: logical.ColumnExpression(
              name: 'v',
              columnType: DataType.int64,
            ),
          ),
        ],
      ),
      limit: 2,
    );
    expect(has<PhysicalTopN>(planner.lower(plan)), isTrue);
  });

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

  // The outermost PhysicalAggregate: the one whose `aggregates` are what the
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
    // second one: _bind dedups structurally.
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

  group('top-n', () {
    test('ORDER BY with a small LIMIT plans a top-n in place of the sort', () {
      final plan = planFor('SELECT * FROM t ORDER BY v LIMIT 2 OFFSET 1');
      expect(has<PhysicalTopN>(plan), isTrue);
      expect(has<PhysicalSort>(plan), isFalse);
      expect(has<PhysicalLimit>(plan), isTrue);
    });

    test('a top-n sits under the projection that trims sort columns', () {
      final plan = planFor('SELECT id FROM t ORDER BY v DESC LIMIT 2');
      expect(has<PhysicalTopN>(plan), isTrue);
      expect(has<PhysicalSort>(plan), isFalse);
    });

    test('LIMIT 0, a large LIMIT or no ORDER BY keep their plans', () {
      expect(
        has<PhysicalTopN>(planFor('SELECT * FROM t ORDER BY v LIMIT 0')),
        isFalse,
      );
      expect(
        has<PhysicalTopN>(planFor('SELECT * FROM t ORDER BY v LIMIT 1000000')),
        isFalse,
      );
      expect(has<PhysicalTopN>(planFor('SELECT * FROM t LIMIT 2')), isFalse);
    });
  });

  group('hash join build side', () {
    Table sized(String name, int rows) => Table(
      name: name,
      columns: [
        NumericColumn.int64(
          name: 'k',
          values: [for (var i = 0; i < rows; i++) i],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [Field(name: 'k', dataType: DataType.int64, nullable: false)],
      ),
    );

    PhysicalHashJoin joinOf(PhysicalPlan p) =>
        p is PhysicalHashJoin ? p : childrenOf(p).map(joinOf).first;

    setUp(() {
      catalog.registerTable('big', sized('big', 1000000));
      catalog.registerTable('small', sized('small', 100));
    });

    test('an inner join builds on the smaller side', () {
      final bigFirst = joinOf(
        planFor('SELECT * FROM big b JOIN small s ON b.k = s.k'),
      );
      expect(bigFirst.buildLeft, isFalse);
      final smallFirst = joinOf(
        planFor('SELECT * FROM small s JOIN big b ON s.k = b.k'),
      );
      expect(smallFirst.buildLeft, isTrue);
    });

    test('an outer join keeps building on the right', () {
      final join = joinOf(
        planFor('SELECT * FROM small s LEFT JOIN big b ON s.k = b.k'),
      );
      expect(join.buildLeft, isFalse);
    });
  });

  group('references', () {
    References walk(String sql) {
      final references = References();
      switch (parser.parse(sql)) {
        case final QueryStatement statement:
          statement.accept(references);
        case final SelectStatement statement:
          statement.accept(references);
      }
      return references;
    }

    void found(String sql, Set<String> tables, [Set<String> files = const {}]) {
      final references = walk(sql);
      expect(references.tables, tables, reason: sql);
      expect(references.files, files, reason: sql);
    }

    test('FROM tables and file paths', () {
      found("SELECT * FROM a, 'f.csv'", {'a'}, {'f.csv'});
    });

    test('the select list', () {
      found('SELECT (SELECT x FROM a) FROM b', {'a', 'b'});
    });

    test('WHERE', () {
      found(
        "SELECT * FROM a WHERE x IN (SELECT y FROM 'f.csv')",
        {'a'},
        {'f.csv'},
      );
    });

    test('GROUP BY and HAVING', () {
      found(
        'SELECT k FROM a GROUP BY (SELECT y FROM b) '
        'HAVING COUNT(*) > (SELECT COUNT(*) FROM c)',
        {'a', 'b', 'c'},
      );
    });

    test('ORDER BY', () {
      found('SELECT * FROM a ORDER BY (SELECT y FROM b)', {'a', 'b'});
    });

    test('join ON conditions', () {
      found(
        "SELECT * FROM a JOIN 'f.csv' ON a.x IN (SELECT y FROM b) "
        'LEFT JOIN c ON a.x = c.x',
        {'a', 'b', 'c'},
        {'f.csv'},
      );
    });

    test('set-operation branches', () {
      found('SELECT x FROM a UNION SELECT x FROM b EXCEPT SELECT x FROM c', {
        'a',
        'b',
        'c',
      });
    });

    test('CTE bodies', () {
      found(
        "WITH w AS (SELECT * FROM 'f.csv') SELECT * FROM w",
        {'w'},
        {'f.csv'},
      );
    });

    test('CASE operand, WHEN, THEN and ELSE', () {
      found(
        'SELECT CASE (SELECT x FROM a) '
        'WHEN (SELECT x FROM b) THEN (SELECT x FROM c) '
        'ELSE (SELECT x FROM d) END FROM e',
        {'a', 'b', 'c', 'd', 'e'},
      );
      found('SELECT CASE WHEN x IN (SELECT y FROM a) THEN 1 END FROM b', {
        'a',
        'b',
      });
    });

    test('CAST', () {
      found('SELECT CAST((SELECT x FROM a) AS INT) FROM b', {'a', 'b'});
    });

    test('function arguments and FILTER', () {
      found('SELECT COALESCE((SELECT x FROM a), 1) FROM b', {'a', 'b'});
      // The parser does not produce FILTER; build the node directly.
      final subquery = parser.parse('SELECT y FROM c') as SelectStatement;
      final references = References();
      FunctionExpression(
        name: 'sum',
        arguments: [ColumnExpression(column: 'x')],
        filter: ExistsExpression(subquery: subquery),
      ).accept(references);
      expect(references.tables, {'c'});
    });

    test('IN operand, values and subquery', () {
      found(
        'SELECT * FROM a WHERE (SELECT x FROM b) '
        'IN ((SELECT x FROM c), 1)',
        {'a', 'b', 'c'},
      );
      found(
        "SELECT * FROM a WHERE x NOT IN (SELECT y FROM 'f.csv')",
        {'a'},
        {'f.csv'},
      );
    });

    test('EXISTS and scalar subqueries', () {
      found(
        'SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b) '
        'AND x = (SELECT MAX(y) FROM c)',
        {'a', 'b', 'c'},
      );
    });

    test('unary and binary operands', () {
      found(
        'SELECT -(SELECT x FROM a) + (SELECT x FROM b) FROM c '
        'WHERE NOT x IN (SELECT y FROM d)',
        {'a', 'b', 'c', 'd'},
      );
    });

    test('VALUES rows and FROM subqueries', () {
      found(
        'SELECT * FROM VALUES ((SELECT x FROM a), 1) v(p, q), '
        '(SELECT * FROM b) s',
        {'a', 'b'},
      );
    });
  });

  group('expression conversion', () {
    // Conversion reads no scope: columns stay unresolved (typed as string)
    // and are bound against the input schema when the plan holding the
    // expression is lowered, which is where an unknown or ambiguous column is
    // reported.
    final schema = Schema(
      fields: [
        Field(name: 'a', dataType: DataType.int64, qualifier: 't'),
        Field(name: 'b', dataType: DataType.float64, qualifier: 't'),
        Field(name: 'name', dataType: DataType.string, qualifier: 't'),
      ],
    );

    String render(logical.LogicalExpression expression) => switch (expression) {
      logical.ColumnExpression(:final table, :final name) =>
        table == null ? name : '$table.$name',
      logical.LiteralExpression(:final value, :final dataType) =>
        value is String ? "'$value':$dataType" : '$value:$dataType',
      logical.BinaryExpression(:final left, :final operator, :final right) =>
        '(${render(left)} ${operator.name} ${render(right)})',
      logical.InExpression(:final value, :final list, :final negated) =>
        '(${render(value)} ${negated ? 'notIn' : 'in'} '
            '[${list.map(render).join(', ')}])',
      logical.FunctionExpression(:final name, :final arguments) =>
        '$name(${arguments.map(render).join(', ')})',
      logical.CaseExpression(:final whenClauses, :final elseExpression) =>
        'case(${[for (final w in whenClauses) '${render(w.condition)} -> ${render(w.result)}'].join('; ')}'
            '${elseExpression == null ? '' : '; else ${render(elseExpression)}'})',
      logical.CastExpression(
        :final expression,
        :final targetType,
        :final precision,
        :final scale,
      ) =>
        'cast(${render(expression)} as $targetType'
            '${precision == null ? '' : '($precision,$scale)'})',
      _ => throw StateError('unexpected ${expression.runtimeType}'),
    };

    logical.LogicalExpression convert(String text) =>
        const Converter().convert(parser.parseExpression(text));

    String converted(String text) => render(convert(text));

    // Lowers a projection of [text] over an empty input with [input]'s
    // schema, the path a SELECT item takes.
    PhysicalPlan bind(String text, Schema input) => logical.Projection(
      input: logical.Values(rows: const [], valueSchema: input),
      expressions: [convert(text)],
      aliases: const ['x'],
    ).accept(PhysicalPlanBuilder(catalog));

    test('columns convert unresolved, with their qualifier', () {
      expect(converted('a'), 'a');
      expect(converted('t.a'), 't.a');
      final column = convert('t.a') as logical.ColumnExpression;
      expect(column.columnType, DataType.string);
    });

    test('literals take the narrowest type that holds them', () {
      expect(converted('42'), '42:int8');
      expect(converted('3000000000'), '3000000000:int64');
      expect(converted('1.50'), "cast('1.50':string as decimal(3,2))");
      expect(converted('1e3'), '1000.0:float64');
      expect(converted("'x'"), "'x':string");
      expect(converted('TRUE'), 'true:boolean');
      expect(converted('NULL'), 'null:${DataType.null_}');
      expect(converted('-5'), '-5:int8');
      expect(
        converted("DATE '2024-01-31'"),
        "cast('2024-01-31':string as date)",
      );
      expect(
        converted("TIMESTAMP '2024-01-31 10:00:00'"),
        "cast('2024-01-31 10:00:00':string as timestamp)",
      );
    });

    test('operators convert with SQL precedence', () {
      expect(converted('a + b * 2'), '(a add (b multiply 2:int8))');
      expect(converted('(a + b) * 2'), '((a add b) multiply 2:int8)');
      expect(
        converted('a = 1 OR b = 2 AND name <> \'x\''),
        "((a eq 1:int8) or ((b eq 2:int8) and (name neq 'x':string)))",
      );
      expect(converted('NOT a = 1'), '((a eq 1:int8) eq false:boolean)');
      expect(converted('-a'), '(0:int64 subtract a)');
      expect(converted('+a'), 'a');
    });

    test('CASE converts its operand and every branch', () {
      expect(
        converted("CASE WHEN a > 1 THEN 'big' ELSE 'small' END"),
        "case((a gt 1:int8) -> 'big':string; else 'small':string)",
      );
      expect(
        converted("CASE a WHEN 1 THEN 'one' END"),
        "case((a eq 1:int8) -> 'one':string)",
      );
    });

    test('CAST converts its target type', () {
      expect(converted('CAST(a AS BIGINT)'), 'cast(a as int64)');
      expect(converted('CAST(b AS DECIMAL(10,2))'), 'cast(b as decimal(10,2))');
      expect(converted('CAST(b AS NUMERIC(8,3))'), 'cast(b as decimal(8,3))');
      expect(converted('CAST(b AS DECIMAL)'), 'cast(b as decimal)');
      expect(
        () => convert('CAST(name AS TIME)'),
        throwsA(
          isA<UnsupportedError>().having(
            (e) => e.message,
            'message',
            'CAST to TIME is not supported',
          ),
        ),
      );
    });

    test('function calls convert their arguments', () {
      expect(converted('UPPER(name)'), 'UPPER(name)');
      expect(converted('COALESCE(a, 0)'), 'COALESCE(a, 0:int8)');
    });

    test('IN converts its value list', () {
      expect(converted('a IN (1, 2)'), '(a in [1:int8, 2:int8])');
      expect(converted('a NOT IN (1)'), '(a notIn [1:int8])');
      expect(
        () => convert('a IN (SELECT a FROM t)'),
        throwsA(
          isA<UnsupportedError>().having(
            (e) => e.message,
            'message',
            startsWith('IN (SELECT …) is only supported as a top-level WHERE'),
          ),
        ),
      );
    });

    test('IS rewrites to null-safe comparisons', () {
      expect(converted('a IS NULL'), '(a isNull null:${DataType.null_})');
      expect(
        converted('a IS NOT NULL'),
        '(a isNotNull null:${DataType.null_})',
      );
      expect(
        converted('a IS TRUE'),
        '((a isNotNull null:boolean) and (a eq true:boolean))',
      );
      expect(
        converted('a IS NOT FALSE'),
        '((a isNull null:boolean) or (a eq true:boolean))',
      );
      expect(
        () => convert('a IS 5'),
        throwsA(
          isA<UnsupportedError>().having(
            (e) => e.message,
            'message',
            'the operand of IS must be NULL, TRUE, or FALSE',
          ),
        ),
      );
    });

    test('LIKE converts to its operator', () {
      expect(converted("name LIKE 'a%'"), "(name like 'a%':string)");
      expect(converted("name NOT LIKE 'a%'"), "(name notLike 'a%':string)");
    });

    test('subqueries are refused with the existing messages', () {
      expect(
        () => convert('EXISTS (SELECT a FROM t)'),
        throwsA(
          isA<UnsupportedError>().having(
            (e) => e.message,
            'message',
            'Unsupported SQL expression: ExistsExpression',
          ),
        ),
      );
    });

    test('an unknown column is reported when the plan binds', () {
      // Conversion accepts it; binding against the schema reports it.
      expect(converted('zz + 1'), '(zz add 1:int8)');
      expect(
        () => bind('zz + 1', schema),
        throwsA(
          isA<FormatException>().having(
            (e) => e.message,
            'message',
            "Unknown column: zz (for the string 'zz', use single quotes)",
          ),
        ),
      );
      expect(
        () => bind('u.a', schema),
        throwsA(
          isA<FormatException>().having(
            (e) => e.message,
            'message',
            'Unknown column: u.a',
          ),
        ),
      );
    });

    test('an ambiguous bare column is reported when the plan binds', () {
      final joined = Schema(
        fields: [
          Field(name: 'a', dataType: DataType.int64, qualifier: 't'),
          Field(name: 'a', dataType: DataType.int64, qualifier: 'u'),
        ],
      );
      expect(
        () => bind('a', joined),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            'Ambiguous column "a" (qualify it, e.g. alias.a)',
          ),
        ),
      );
      expect(bind('u.a', joined), isA<PhysicalProjection>());
      expect(bind('t.a + b', schema), isA<PhysicalProjection>());
    });
  });
}
