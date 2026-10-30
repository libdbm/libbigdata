// The logical planner. LogicalPlanBuilder keeps the statement-level visits
// here; its helpers live in parts by concern: joins.dart (FROM folding),
// subqueries.dart (WHERE and IN/EXISTS decorrelation), aggregates.dart
// (grouping and aggregates), sets.dart (set operations).
import 'dart:math' as math;

import '../../core/conversions.dart' show dateOf, parseUtc;
import '../../core/paths.dart';
import '../../core/schema.dart';
import '../../core/data_type.dart';
import '../ast/ast.dart' as ast;
import 'logical_plan.dart' as logical;
import 'expressions.dart';
import 'physical_plan.dart';
import 'file_scan.dart';
import '../exec/kernels.dart';
import '../operators/physical_operators.dart';
import '../catalog/table_catalog.dart';
import '../exec/where_pushdown.dart';
import '../execution/engine_options.dart';
import '../../core/row_filter.dart';
import '../../core/limits.dart';
import '../../core/failure.dart';
import '../../core/result.dart';

part 'physical_plan_builder.dart';
part 'pushdown.dart';
part 'sets.dart';
part 'joins.dart';
part 'subqueries.dart';
part 'aggregates.dart';

/// The dtype a binary expression's output column actually has: boolean for
/// comparison/logical/membership/null tests (they emit a bool-mask column),
/// otherwise the arithmetic type the kernel emits ([Kernels.arithResultType]).
/// Keeps output-schema typing (projection + DISTINCT slots) in lock-step with
/// what `evalColumn` produces. Top-level so both plan builders share it.
DataType _binaryResultType(
  logical.BinaryOperator operator,
  DataType a,
  DataType b,
) {
  switch (operator) {
    case logical.BinaryOperator.and:
    case logical.BinaryOperator.or:
    case logical.BinaryOperator.eq:
    case logical.BinaryOperator.neq:
    case logical.BinaryOperator.lt:
    case logical.BinaryOperator.lte:
    case logical.BinaryOperator.gt:
    case logical.BinaryOperator.gte:
    case logical.BinaryOperator.like:
    case logical.BinaryOperator.notLike:
    case logical.BinaryOperator.isNull:
    case logical.BinaryOperator.isNotNull:
      return DataType.boolean;
    case logical.BinaryOperator.add:
    case logical.BinaryOperator.subtract:
    case logical.BinaryOperator.multiply:
    case logical.BinaryOperator.divide:
    case logical.BinaryOperator.modulo:
      return Kernels.arithResultType(a, b, operator);
  }
}

/// Query planner converts AST to logical plan, then to physical plan
class QueryPlanner {
  final TableCatalog catalog;
  final EngineOptions options;

  QueryPlanner(this.catalog, [this.options = const EngineOptions()]);

  /// Plan a query from AST
  PhysicalPlan plan(ast.Statement statement) =>
      lower(_buildLogicalPlan(statement));

  /// [plan] as a physical plan: the optimizer rules, in order, then the
  /// physical builder. Every logical plan the engine runs comes through here,
  /// whether a statement built it or a caller did (DataFrame).
  ///
  /// The rules rewrite [plan] in place: [PushDownFilter] attaches each
  /// pushable WHERE conjunct to the scan it reads, then [PushDownProjection]
  /// narrows each scan to the columns the plan uses.
  PhysicalPlan lower(logical.LogicalPlan plan) {
    var optimized = PushDownFilter().optimize(plan);
    optimized = PushDownProjection().optimize(optimized);
    return optimized.accept(PhysicalPlanBuilder(catalog, options));
  }

  /// Compile a scalar AST expression to a physical expression bound to
  /// [inputSchema], reusing the SELECT AST→logical→physical pipeline so DML
  /// SET/WHERE share the engine's Kleene three-valued logic, checked integer
  /// arithmetic and divide-by-zero→NULL rather than a separate row evaluator.
  PhysicalExpression compileScalar(
    ast.Expression expression, {
    required Schema inputSchema,
  }) {
    final converted = const Converter().convert(expression);
    return PhysicalPlanBuilder(
      catalog,
      options,
    )._convertToPhysicalExpression(converted, inputSchema: inputSchema);
  }

  /// [expression], written outside a statement (a DataFrame predicate or
  /// computed column), compiled as [compileScalar] compiles it and bound to
  /// [inputSchema] as a SELECT item is: an unknown column is a
  /// [FormatException], an aggregate is a [FormatException] (there is no
  /// grouping to compute it in), and a subquery is an [UnsupportedError].
  PhysicalExpression bind(
    ast.Expression expression, {
    required Schema inputSchema,
  }) {
    final builder = PhysicalPlanBuilder(catalog, options);
    final compiled = builder._convertToPhysicalExpression(
      convert(expression),
      inputSchema: inputSchema,
    );
    builder._validate(compiled, inputSchema);
    return compiled;
  }

  /// [expression], written outside a statement, as a logical expression for
  /// a plan a caller builds (a DataFrame filter or computed column): an
  /// aggregate is a [FormatException] and a subquery an [UnsupportedError].
  /// Columns are bound when the plan is lowered.
  logical.LogicalExpression convert(ast.Expression expression) {
    if (LogicalPlanBuilder(catalog)._containsAggregate(expression)) {
      throw const FormatException(
        'An aggregate function is not allowed in a row expression',
      );
    }
    return const Converter().convert(expression);
  }

  /// Build logical plan from AST
  logical.LogicalPlan _buildLogicalPlan(ast.Statement statement) {
    final builder = LogicalPlanBuilder(catalog);
    return switch (statement) {
      ast.QueryStatement() => statement.accept(builder),
      ast.SelectStatement() => statement.accept(builder),
      // DML and DDL run directly in SqlEngine, never through the planner.
      _ => throw UnsupportedError(
        '${statement.runtimeType} is executed directly, not planned',
      ),
    };
  }
}

/// Builds logical plan from AST
class LogicalPlanBuilder
    implements
        ast.StatementVisitor<logical.LogicalPlan>,
        ast.TableVisitor<logical.LogicalPlan> {
  final TableCatalog catalog;
  // Distinguishes the synthetic columns of nested `IN (SELECT …)` semi-joins so
  // two of them never collide.
  int _inSubquery = 0;

  LogicalPlanBuilder(this.catalog);

  @override
  logical.LogicalPlan visitQueryStatement(ast.QueryStatement node) {
    // The engine NEVER plans a QueryStatement: SqlEngine materializes each CTE
    // into a catalog table first and plans the inner SELECT (_materializeCtes).
    // A parallel CTE-planning path here would silently diverge from that: the
    // ~110-line one this replaced was dead code and exactly such a trap.
    throw UnsupportedError(
      'plan the inner SelectStatement; CTEs are materialized by SqlEngine',
    );
  }

  @override
  logical.LogicalPlan visitSelectStatement(ast.SelectStatement node) {
    // Start with FROM clause or dummy single-row table. Multiple FROM entries
    // (comma list) fold left-to-right; a WHERE equality conjunct joining two
    // sides becomes that fold step's HASH JOIN condition (extracted below):
    // left as a filter over a cross join it executed as a full O(n·m)
    // cartesian product.
    logical.LogicalPlan plan;
    var residualWhere = node.where;
    if (node.from.isNotEmpty) {
      final entries = [for (final f in node.from) f.accept(this)];
      if (entries.length == 1 || residualWhere == null) {
        plan = entries.first;
        for (int i = 1; i < entries.length; i++) {
          plan = logical.Join(
            left: plan,
            right: entries[i],
            joinType: logical.JoinType.cross,
          );
        }
      } else {
        final result = _foldCommaJoin(node, entries, residualWhere);
        plan = result.plan;
        residualWhere = result.residual;
      }
    } else {
      // SELECT without FROM (e.g., SELECT 1)
      plan = logical.Values(
        rows: [
          [logical.LiteralExpression(1, DataType.int32)],
        ],
        valueSchema: Schema(
          fields: [Field(name: 'column1', dataType: DataType.int32)],
        ),
      );
    }

    // Apply WHERE (minus any conjuncts already folded into comma-join
    // conditions above). A top-level `col IN (SELECT …)` / `NOT IN` conjunct
    // becomes a SEMI / anti join (the subquery is the build side); the rest
    // stays a Filter.
    if (residualWhere != null) {
      plan = _applyWhere(plan, residualWhere);
    }

    // Aggregate discovery is STATEMENT-WIDE: an aggregate in the SELECT list,
    // HAVING, or ORDER BY (not only the SELECT list) makes this an aggregate
    // query. `SELECT 1 FROM t ORDER BY SUM(n)` is a global aggregate; scanning
    // only the SELECT list mis-planned SUM as a scalar function.
    final hasAggregates =
        node.selectList.any((i) => _containsAggregate(i.expression)) ||
        (node.having != null && _containsAggregate(node.having!)) ||
        node.orderBy.any((o) => _containsAggregate(o.expression));
    // A GROUP BY, a HAVING, OR an aggregate anywhere, means we're
    // aggregating: a HAVING without GROUP BY groups the whole input into one
    // group, as in DuckDB (it was applied as a WHERE).
    final aggregating =
        hasAggregates || node.groupBy.isNotEmpty || node.having != null;

    // Group-by keys as logical expressions (empty for a global aggregate),
    // available to the SELECT/ORDER-BY rewrites below.
    final groups = [
      for (final g in node.groupBy)
        _convertExpression(_groupTarget(g, node.selectList, plan)),
    ];

    // Hoist the aggregates out of the SELECT list and HAVING BEFORE building the
    // aggregate step: it has to compute every aggregate the statement mentions,
    // wherever it was written, and a HAVING aggregate need not appear in the
    // SELECT list at all. Each clause keeps its rewritten expression, in which
    // the aggregates are now references to the columns that step emits.
    final aggregates = _Aggregates();
    final selections = <ast.Expression>[];
    // ORDER BY with its aggregates hoisted too, index-parallel to node.orderBy.
    // Identical to it when not aggregating: _hoist is inert on an
    // aggregate-free expression.
    final orderings = [for (final item in node.orderBy) item.expression];
    var having = node.having;
    if (aggregating) {
      // Under DISTINCT / a set operation the carry below is unavailable (an
      // extra column changes which rows are distinct, and breaks set-op arity),
      // so ORDER BY must reference an output column, as SQL requires anyway.
      if (node.distinct || node.operations.isNotEmpty) {
        _rejectAggregateOrderBy(node.orderBy);
      }
      for (final item in node.selectList) {
        selections.add(_hoist(item.expression, aggregates));
      }
      if (having != null) having = _hoist(having, aggregates);
      // Hoisted LAST so `__agg$i` numbering follows SELECT-list order, and an
      // aggregate already written there binds to the SAME column (see _bind).
      for (int i = 0; i < orderings.length; i++) {
        orderings[i] = _hoist(orderings[i], aggregates);
      }
      plan = _buildAggregate(
        plan,
        groups,
        aggregates.expressions,
        aggregates.aliases,
      );
    }

    // Apply HAVING. When aggregating this filters the aggregate's output, where
    // the hoisted aggregates are plain column references.
    if (having != null) {
      plan = logical.Filter(input: plan, predicate: _convertExpression(having));
    }

    // Output aliases to project back to after sorting, when ORDER BY forced
    // extra columns into the projection (see below). Null = no trim needed.
    List<String>? sortTrim;
    // Qualified columns the projection carried for ORDER BY, by full name.
    var carried = const <String>{};

    // For aggregate queries, the aggregate operator emits columns as
    // [group keys…, aggregates…] named by group-key name / `__agg$i`. A
    // projection computes the SELECT list over those, in SELECT-list order and
    // under the SELECT-list aliases; a group key ordered-by but NOT selected is
    // carried through, then trimmed after the sort.
    if (!_isSelectStar(node.selectList) && aggregating) {
      final expressions = <logical.LogicalExpression>[];
      final aliases = <String>[];
      for (int i = 0; i < node.selectList.length; i++) {
        final item = node.selectList[i];
        // The aggregates are already `__agg$i` references (hoisted above); what
        // is left of the expression is group keys, and a group key: plain
        // column or expression (`k % 2`): must reference its own emitted
        // column rather than re-evaluate over now-absent base columns.
        final expression = _rewriteOverAggregate(
          _convertExpression(selections[i]),
          groups,
        );
        _requireGrouped(expression, groups, aggregates.aliases);
        expressions.add(expression);
        aliases.add(item.alias ?? _expressionName(item.expression));
      }
      // Two unaliased same-named items (`COUNT(*), COUNT(x)`) must not emit a
      // duplicate-name schema: same rule as the non-aggregating projection.
      _uniquifyAuto(aliases, node.selectList);

      final extras = <logical.LogicalExpression>[];
      final extraAliases = <String>[];
      if (node.orderBy.isNotEmpty &&
          !node.distinct &&
          node.operations.isEmpty) {
        final have = aliases.toSet();
        for (final expression in orderings) {
          _carry(
            expression,
            groups,
            aggregates.aliases,
            have,
            extras,
            extraAliases,
          );
        }
      }

      plan = logical.Projection(
        input: plan,
        expressions: [...expressions, ...extras],
        aliases: [...aliases, ...extraAliases],
      );
      if (extras.isNotEmpty) sortTrim = List.of(aliases);
    }

    // `SELECT *` with a GLOBAL aggregate (no grouping columns) has no columns to
    // project and is not valid SQL: fail loud rather than emit a zero-column
    // result. (With a GROUP BY the star means the group keys, handled below.)
    if (_isSelectStar(node.selectList) && aggregating && groups.isEmpty) {
      throw UnsupportedError(
        'SELECT * with a global aggregate has no grouping columns; list the '
        'grouped columns explicitly or add a GROUP BY',
      );
    }

    // `SELECT *` under aggregation means the group keys; there is no SELECT
    // list to project, so nothing above trims the aggregate's output. An
    // aggregate reaches [aggregates] here only from HAVING (a star list holds
    // none), and HAVING has now been applied, so drop the `__agg$i` columns it
    // needed rather than leak them to the caller.
    if (_isSelectStar(node.selectList) &&
        aggregating &&
        aggregates.expressions.isNotEmpty) {
      // A star list holds no aggregate, so reaching here with one means there is
      // a GROUP BY: there is always at least one key.
      final keys = [
        for (int i = 0; i < groups.length; i++) _groupKeyName(groups[i], i),
      ];
      // An ORDER BY aggregate needs its column past this projection too, or the
      // sort would reference one just dropped.
      final extras = <logical.LogicalExpression>[];
      final extraAliases = <String>[];
      if (node.orderBy.isNotEmpty &&
          !node.distinct &&
          node.operations.isEmpty) {
        final have = keys.toSet();
        for (final expression in orderings) {
          _carry(
            expression,
            groups,
            aggregates.aliases,
            have,
            extras,
            extraAliases,
          );
        }
      }
      plan = logical.Projection(
        input: plan,
        expressions: [
          for (int i = 0; i < groups.length; i++)
            _rewriteOverAggregate(groups[i], groups),
          ...extras,
        ],
        aliases: [...keys, ...extraAliases],
      );
      if (extras.isNotEmpty) sortTrim = keys;
    }

    // Apply SELECT (projection) - only if not aggregating
    if (!_isSelectStar(node.selectList) && !aggregating) {
      final expressions = <logical.LogicalExpression>[];
      final aliases = <String>[];

      for (final item in node.selectList) {
        expressions.add(_convertExpression(item.expression));
        aliases.add(item.alias ?? _expressionName(item.expression));
      }

      // Disambiguate duplicate auto-generated aliases (e.g. SELECT a.name,
      // b.name) by qualifying the ones from qualified columns. Explicit AS
      // aliases are left as written.
      final counts = <String, int>{};
      for (final a in aliases) {
        counts[a] = (counts[a] ?? 0) + 1;
      }
      for (int i = 0; i < node.selectList.length; i++) {
        final item = node.selectList[i];
        final expression = item.expression;
        if (item.alias == null &&
            counts[aliases[i]]! > 1 &&
            expression is ast.ColumnExpression &&
            expression.table != null) {
          aliases[i] = '${expression.table}.${expression.column}';
        }
      }
      // Two unaliased COMPUTED columns both default to 'expr' (`SELECT a+b, c*d`),
      // so only the first was addressable. Suffix the later duplicates positionally
      // so every output column has a distinct name.
      _uniquifyAuto(aliases, node.selectList);

      // ORDER BY runs above this projection, so a base column it references that
      // is not in the SELECT list (e.g. `SELECT id ... ORDER BY amount`) would
      // not be in the sort's input. Carry such columns through the projection so
      // the sort can see them, then trim them back out after the sort. (Skip
      // under DISTINCT / set operations, where ORDER BY must use output columns.)
      final extras = <logical.LogicalExpression>[];
      final extraAliases = <String>[];
      if (node.orderBy.isNotEmpty &&
          !node.distinct &&
          node.operations.isEmpty) {
        // A qualified reference counts as present only when the SELECT list
        // outputs that same qualified column; its bare name may belong to
        // another table's column (`SELECT t.a ... ORDER BY u.a`).
        final have = {
          ...aliases,
          for (final item in node.selectList)
            if (item.expression case ast.ColumnExpression(
              :final table?,
              :final column,
            ))
              '$table.$column',
        };
        final seen = <String>{};
        for (final item in node.orderBy) {
          _collectSortColumns(
            item.expression,
            have,
            seen,
            extras,
            extraAliases,
          );
        }
      }

      plan = logical.Projection(
        input: plan,
        expressions: [...expressions, ...extras],
        aliases: [...aliases, ...extraAliases],
      );
      if (extras.isNotEmpty) sortTrim = List.of(aliases);
      carried = {
        for (final alias in extraAliases)
          if (alias.contains('.')) alias,
      };
    }

    // Apply DISTINCT (per-branch: it binds to this SELECT core, left of any tail)
    if (node.distinct) {
      plan = logical.Distinct(plan);
    }

    // Fold the set-operation tails onto this core BEFORE ORDER BY / LIMIT, so
    // those clauses apply to the WHOLE combined result (a trailing `ORDER BY` on
    // `A UNION B` sorts the union, not just B). Left-associative, INTERSECT tighter
    // than UNION/EXCEPT.
    if (node.operations.isNotEmpty) {
      plan = _foldOperations(plan, node.operations);
    }

    // Apply ORDER BY. In an aggregate query the sort runs over the aggregate's
    // output, so a group-key reference (`ORDER BY k % 2`) must resolve to the
    // emitted `group$i` column rather than re-evaluate over absent base columns.
    if (node.orderBy.isNotEmpty) {
      // ORDER BY takes a name or an expression; a bare integer is refused
      // rather than read as a select-list position.
      for (final item in node.orderBy) {
        if (_ordinal(item.expression) case final int position) {
          throw FormatException(
            'ORDER BY position $position is not supported: '
            'sort by a column name or expression',
          );
        }
      }
      final sorts = [
        for (int i = 0; i < node.orderBy.length; i++)
          logical.SortExpression(
            expression: switch (aggregating) {
              true => _rewriteOverAggregate(
                _convertExpression(orderings[i]),
                groups,
              ),
              false => _sorted(_convertExpression(orderings[i]), carried),
            },
            ascending: node.orderBy[i].ascending,
            nullsFirst: node.orderBy[i].nullsFirst,
          ),
      ];

      plan = logical.Sort(input: plan, sortExpressions: sorts);
    }

    // Trim any ORDER-BY-only columns added above so the output matches SELECT.
    if (sortTrim != null) {
      plan = logical.Projection(
        input: plan,
        expressions: [
          for (final a in sortTrim)
            logical.ColumnExpression(name: a, columnType: DataType.string),
        ],
        aliases: sortTrim,
      );
    }

    // Apply LIMIT/OFFSET
    if (node.limit != null || node.offset != null) {
      plan = logical.Limit(input: plan, limit: node.limit, offset: node.offset);
    }

    return plan;
  }

  @override
  logical.LogicalPlan visitSimpleTable(ast.SimpleTable node) {
    // Check if it's a file path
    if (node.isFilePath) {
      // Files are registered under their exact path ([fileKey], so
      // same-basename files coexist and case matters); the basename only
      // supplies the default column-qualifier alias.
      final key = fileKey(node.table);
      final table = catalog.table(key);
      if (table == null) {
        throw Failure.of(
          Kind.missing,
          'File not found or could not be loaded: ${node.table}',
        );
      }

      final alias = _qualifier(node);
      return _renamed(
        logical.TableScan(
          tableName: key,
          alias: alias,
          tableSchema: table.schema,
        ),
        node.columns,
        alias,
      );
    }

    // Regular table (includes materialized CTEs, registered under their name).
    // Default the alias to the table name so a bare `FROM t` still qualifies its
    // columns as `t` (needed to route qualified ON columns in joins/self-joins).
    final table = catalog.table(node.table);
    final alias = node.alias ?? node.table;
    if (table == null) {
      // A registered DataFrame: its plan, built for this reference alone
      // (the rules rewrite a plan in place), over a catalog of its own.
      final frame = catalog.frame(node.table);
      if (frame != null) {
        final roots = TableCatalog();
        return _renamed(
          logical.Frame(alias: alias, input: frame.plan(roots), catalog: roots),
          node.columns,
          alias,
        );
      }
      throw Failure.of(Kind.missing, 'Table not found: ${node.table}');
    }

    return _renamed(
      logical.TableScan(
        tableName: node.table,
        alias: alias,
        tableSchema: table.schema,
      ),
      node.columns,
      alias,
    );
  }

  /// [plan] with its leading columns renamed to [names] and qualified by
  /// [alias], for a column alias list (`FROM t x(p, q)`); [plan] itself when
  /// there is no list. A list longer than the columns is an error.
  logical.LogicalPlan _renamed(
    logical.LogicalPlan plan,
    List<String>? names,
    String alias,
  ) {
    if (names == null) return plan;
    final fields = plan.schema.fields;
    if (names.length > fields.length) {
      throw Failure.of(
        Kind.invalid,
        'Table $alias has ${fields.length} columns but ${names.length} '
        'column aliases were given',
      );
    }
    return logical.AliasScope(
      alias: alias,
      input: logical.Projection(
        input: plan,
        expressions: [
          for (final field in fields)
            logical.ColumnExpression(
              name: field.name,
              table: field.qualifier,
              columnType: field.dataType,
              isNullable: field.nullable,
            ),
        ],
        aliases: [
          for (final (index, field) in fields.indexed)
            index < names.length ? names[index] : field.name,
        ],
      ),
    );
  }

  @override
  logical.LogicalPlan visitJoinedTable(ast.JoinedTable node) {
    // Start with the left table
    logical.LogicalPlan result = node.left.accept(this);

    // Apply each join in sequence
    for (final join in node.joins) {
      final right = join.right.accept(this);

      // Convert join type
      logical.JoinType logicalJoinType;
      switch (join.type) {
        case ast.JoinType.inner:
          logicalJoinType = logical.JoinType.inner;
          break;
        case ast.JoinType.left:
          logicalJoinType = logical.JoinType.left;
          break;
        case ast.JoinType.right:
          logicalJoinType = logical.JoinType.right;
          break;
        case ast.JoinType.full:
          logicalJoinType = logical.JoinType.full;
          break;
        case ast.JoinType.cross:
          logicalJoinType = logical.JoinType.cross;
          break;
        // NOTE: NATURAL JOIN is treated as INNER; it does NOT derive its
        // condition from the common columns. Pre-existing behaviour, made
        // explicit here rather than left to a `default`. Semi/anti never arrive
        // through this path: SQL has no surface syntax for them, they are only
        // ever derived from IN/EXISTS.
        case ast.JoinType.natural:
          logicalJoinType = logical.JoinType.inner;
          break;
      }

      // Handle join condition
      logical.LogicalExpression? condition;
      List<String>? usingColumns;

      if (join.condition != null) {
        if (join.condition is ast.OnCondition) {
          final clause = join.condition as ast.OnCondition;
          condition = _convertExpression(clause.expression);
        } else if (join.condition is ast.UsingCondition) {
          final clause = join.condition as ast.UsingCondition;
          usingColumns = clause.columns;
        }
      }

      result = logical.Join(
        left: result,
        right: right,
        joinType: logicalJoinType,
        condition: condition,
        usingColumns: usingColumns,
      );
    }

    return result;
  }

  /// Collect base columns referenced by an ORDER BY expression that are not
  /// already a SELECT output ([have]), so they can be carried through the
  /// projection for the sort and trimmed afterwards. [seen] dedups.
  void _collectSortColumns(
    ast.Expression expression,
    Set<String> have,
    Set<String> seen,
    List<logical.LogicalExpression> extras,
    List<String> extraAliases,
  ) {
    void recurse(ast.Expression e) =>
        _collectSortColumns(e, have, seen, extras, extraAliases);
    if (expression is ast.ColumnExpression) {
      // A qualified column is carried under its full name, which the sort
      // then references (see _sorted).
      final name = expression.table == null
          ? expression.column
          : '${expression.table}.${expression.column}';
      if (!have.contains(name) && seen.add(name)) {
        extras.add(_convertExpression(expression));
        extraAliases.add(name);
      }
    } else if (expression is ast.BinaryExpression) {
      recurse(expression.left);
      recurse(expression.right);
    } else if (expression is ast.UnaryExpression) {
      recurse(expression.operand);
    } else if (expression is ast.FunctionExpression) {
      expression.arguments.forEach(recurse);
    } else if (expression is ast.CastExpression) {
      recurse(expression.expression);
    } else if (expression is ast.CaseExpression) {
      if (expression.expression != null) recurse(expression.expression!);
      for (final w in expression.whenClauses) {
        recurse(w.condition);
        recurse(w.result);
      }
      if (expression.elseExpression != null) {
        recurse(expression.elseExpression!);
      }
    } else if (expression is ast.InExpression) {
      recurse(expression.expression);
      expression.values.forEach(recurse);
    }
    // Literals, stars, and nested subqueries reference no base columns here.
  }

  /// The qualifier [table]'s columns carry: its alias, else a file's
  /// basename (`'data/a.csv'` is `a`) or the table's name. Correlation and
  /// comma-join folding need this form: a raw path never matches a reference.
  static String _qualifier(ast.SimpleTable table) =>
      table.alias ??
      (table.isFilePath ? tableNameFromPath(table.table) : table.table);

  logical.LogicalExpression _convertExpression(ast.Expression expression) =>
      const Converter().convert(expression);

  /// The integer an ORDER BY or GROUP BY item is when it is a bare integer
  /// literal (a position, which both clauses refuse), or null otherwise.
  int? _ordinal(ast.Expression expression) =>
      expression is ast.LiteralExpression && expression.value is int
      ? expression.value as int
      : null;

  /// What a GROUP BY item groups on: a select alias that no input column
  /// shadows names its expression; anything else stands as written. A bare
  /// integer (a position) is refused.
  ast.Expression _groupTarget(
    ast.Expression expression,
    List<ast.SelectItem> items,
    logical.LogicalPlan input,
  ) {
    if (_ordinal(expression) case final int position) {
      throw FormatException(
        'GROUP BY position $position is not supported: '
        'group by a column name or expression',
      );
    }
    // A SELECT alias, when no input column has the name (an input column
    // wins, as in DuckDB): `SELECT a AS x ... GROUP BY x`.
    if (expression case ast.ColumnExpression(
      table: null,
      :final column,
    ) when input.schema.fields.every((f) => f.name != column)) {
      for (final item in items) {
        if (item.alias == column && !_containsAggregate(item.expression)) {
          return item.expression;
        }
      }
    }
    return expression;
  }

  bool _isSelectStar(List<ast.SelectItem> items) {
    return items.length == 1 && items[0].expression is ast.StarExpression;
  }

  String _expressionName(ast.Expression expression) {
    if (expression is ast.ColumnExpression) {
      return expression.column;
    } else if (expression is ast.FunctionExpression) {
      return expression.name;
    }
    return 'expr';
  }

  /// Give each AUTO-aliased (no explicit AS) column a distinct name: a later
  /// duplicate is suffixed with its 1-based position (`expr` → `expr_2`). Explicit
  /// aliases and already-unique names are left untouched.
  void _uniquifyAuto(List<String> aliases, List<ast.SelectItem> items) {
    final used = <String>{};
    for (int i = 0; i < aliases.length; i++) {
      if (items[i].alias != null) {
        used.add(aliases[i]); // an explicit alias is kept as written
        continue;
      }
      if (used.add(aliases[i])) continue; // first use of this name: keep it
      var candidate = '${aliases[i]}_${i + 1}';
      while (!used.add(candidate)) {
        candidate = '${candidate}_';
      }
      aliases[i] = candidate;
    }
  }

  /// [expression] with each qualified column the projection [carried] for the
  /// sort (under its full name, see _collectSortColumns) referenced by that
  /// name.
  logical.LogicalExpression _sorted(
    logical.LogicalExpression expression,
    Set<String> carried,
  ) {
    if (carried.isEmpty) return expression;
    return _replace(expression, (e) {
      if (e is! logical.ColumnExpression || e.table == null) return null;
      final name = '${e.table}.${e.name}';
      if (!carried.contains(name)) return null;
      return logical.ColumnExpression(
        name: name,
        columnType: e.columnType,
        isNullable: e.isNullable,
      );
    });
  }

  // Implement required visitor methods (throw for unsupported)
  @override
  logical.LogicalPlan visitSubqueryTable(ast.SubqueryTable node) {
    // Qualify the derived columns with the subquery's alias so `alias.col` works.
    final plan = logical.AliasScope(
      alias: node.alias,
      input: node.subquery.accept(this),
    );
    return _renamed(plan, node.columns, node.alias);
  }

  @override
  logical.LogicalPlan visitValuesTable(ast.ValuesTable node) {
    if (node.values.isEmpty) {
      throw ArgumentError('VALUES requires at least one row');
    }
    final logicalRows = [
      for (final row in node.values) row.map(_convertExpression).toList(),
    ];

    // Every row must have the same arity, else the columns can't be formed.
    final arity = logicalRows.first.length;
    for (final row in logicalRows) {
      if (row.length != arity) {
        throw ArgumentError(
          'VALUES rows have different column counts '
          '(${row.length} vs $arity)',
        );
      }
    }
    final aliases = node.columnAliases;
    if (aliases != null && aliases.length != arity) {
      throw ArgumentError(
        'VALUES has $arity column(s) but ${aliases.length} column alias(es) '
        'were given',
      );
    }

    // Unify each column's type over ALL rows, not just the first: `VALUES
    // (1), (2.5)` is a float column, and typing it from row 0 alone would feed
    // a double into an int buffer. `commonType` (not `inferredCommon`) keeps
    // VALUES consistent with how set-operation branches unify.
    final fields = <Field>[];
    for (int i = 0; i < arity; i++) {
      DataType? type;
      var nullable = false;
      for (final row in logicalRows) {
        final cell = row[i];
        // A NULL literal carries no usable type; it only forces nullability,
        // and must not take part in unification (its inferred type would
        // otherwise conflict with the real one).
        if (cell is logical.LiteralExpression && cell.value == null) {
          nullable = true;
          continue;
        }
        nullable = nullable || cell.nullable;
        final unified = type == null
            ? cell.dataType
            : DataType.commonType(type, cell.dataType);
        if (unified == null) {
          throw ArgumentError(
            'VALUES column ${i + 1} has incompatible types '
            '($type vs ${cell.dataType})',
          );
        }
        type = unified;
      }
      // A decimal column takes the widest scale of its cells (each cell's
      // value is restated at it when the rows are built), with room for the
      // widest integer part.
      final decimal = type == DataType.decimal
          ? _widest([for (final row in logicalRows) row[i]])
          : null;
      fields.add(
        Field(
          // An all-NULL column has no type to infer; the first cell's inferred
          // type stands in and every value is null anyway.
          name: aliases?[i] ?? 'column${i + 1}',
          precision: decimal?.precision,
          scale: decimal?.scale,
          dataType: type ?? logicalRows.first[i].dataType,
          nullable: nullable,
        ),
      );
    }

    final values = logical.Values(
      rows: logicalRows,
      valueSchema: Schema(fields: fields),
    );
    // Qualify the columns with the alias so `t.n` resolves: same treatment a
    // subquery table gets.
    final alias = node.alias;
    return alias == null
        ? values
        : logical.AliasScope(alias: alias, input: values);
  }

  /// The precision and scale a decimal column needs to hold [cells]: the
  /// widest scale, and the widest integer part beside it. A cell's own scale
  /// comes from its CAST; an integer has scale 0.
  ({int precision, int scale}) _widest(List<logical.LogicalExpression> cells) {
    var scale = 0;
    var whole = 1;
    for (final cell in cells) {
      final (precision, places) = switch (cell) {
        logical.CastExpression(:final precision, :final scale)
            when cell.targetType == DataType.decimal =>
          (precision ?? 38, scale ?? 0),
        _ when cell.dataType.isInteger => (19, 0),
        _ => (38, 0),
      };
      if (places > scale) scale = places;
      if (precision - places > whole) whole = precision - places;
    }
    return (precision: math.min(38, whole + scale), scale: scale);
  }
}
