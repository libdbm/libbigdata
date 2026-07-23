// NOTE (audit 7): at ~2300 lines this is the second-largest file after
// physical_plan.dart; splitting both into parts is on the same deferred
// roadmap entry (A2) — internally coherent, split only with a regression net.
import '../../core/conversions.dart' show decimalTypeParams;
import '../../core/paths.dart';
import '../../core/schema.dart';
import '../../core/data_type.dart';
import '../ast/ast.dart' as ast;
import 'logical_plan.dart' as logical;
import 'physical_plan.dart';
import 'file_scan.dart';
import '../exec/kernels.dart';
import '../operators/physical_operators.dart';
import '../catalog/table_catalog.dart';
import '../exec/where_pushdown.dart';
import '../execution/engine_options.dart';
import '../../core/row_filter.dart';

part 'physical_plan_builder.dart';
part 'pushdown.dart';

/// The dtype a binary expression's output column actually has: boolean for
/// comparison/logical/membership/null tests (they emit a bool-mask column),
/// otherwise the arithmetic type the kernel emits ([Kernels.arithResultType]).
/// Keeps output-schema typing (projection + DISTINCT slots) in lock-step with
/// what `evalColumn` produces. Top-level so both plan builders share it.
DataType _binaryResultType(logical.BinaryOperator op, DataType a, DataType b) {
  switch (op) {
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
      return Kernels.arithResultType(a, b, op);
  }
}

/// The aggregates one SELECT statement computes, in the order first written.
/// The aggregate step emits each as the internal column [aliases]`[i]`, which
/// the SELECT projection and HAVING above it read. Held per statement (never on
/// the builder, which a subquery re-enters) — see [LogicalPlanBuilder._hoist].
class _Aggregates {
  final expressions = <logical.AggregateExpression>[];
  final aliases = <String>[];
}

/// Query planner converts AST to logical plan, then to physical plan
class QueryPlanner {
  final TableCatalog catalog;
  final EngineOptions options;

  QueryPlanner(this.catalog, [this.options = const EngineOptions()]);

  /// Plan a query from AST
  PhysicalPlan plan(ast.Statement statement) {
    // Convert AST to logical plan
    final logicalPlan = _buildLogicalPlan(statement);

    // Optimize logical plan (rule-based optimization)
    final optimized = _optimizeLogicalPlan(logicalPlan);

    // Convert to physical plan
    final physicalPlan = _buildPhysicalPlan(optimized);

    return physicalPlan;
  }

  /// Compile a scalar AST expression to a physical expression bound to
  /// [inputSchema], reusing the SELECT AST→logical→physical pipeline so DML
  /// SET/WHERE share the engine's Kleene three-valued logic, checked integer
  /// arithmetic and divide-by-zero→NULL rather than a separate row evaluator.
  PhysicalExpression compileScalar(
    ast.Expression expr, {
    required Schema inputSchema,
  }) {
    final logicalExpr = LogicalPlanBuilder(catalog)._convertExpression(expr);
    return PhysicalPlanBuilder(
      catalog,
      options,
    )._convertToPhysicalExpression(logicalExpr, inputSchema: inputSchema);
  }

  /// Build logical plan from AST
  logical.LogicalPlan _buildLogicalPlan(ast.Statement statement) {
    final builder = LogicalPlanBuilder(catalog);
    return statement.accept(builder);
  }

  /// Optimize logical plan
  logical.LogicalPlan _optimizeLogicalPlan(logical.LogicalPlan plan) {
    var optimized = plan;

    // Apply optimization rules
    optimized = PushDownFilter().optimize(optimized);
    optimized = PushDownProjection().optimize(optimized);

    return optimized;
  }

  /// Build physical plan from logical plan
  PhysicalPlan _buildPhysicalPlan(logical.LogicalPlan logical) {
    final builder = PhysicalPlanBuilder(catalog, options);
    return logical.accept(builder);
  }
}

/// Builds logical plan from AST
class LogicalPlanBuilder extends ast.ASTVisitor<logical.LogicalPlan> {
  final TableCatalog catalog;
  // Distinguishes the synthetic columns of nested `IN (SELECT …)` semi-joins so
  // two of them never collide.
  int _inSubquery = 0;

  LogicalPlanBuilder(this.catalog);

  @override
  logical.LogicalPlan visitQueryStatement(ast.QueryStatement node) {
    // The engine NEVER plans a QueryStatement: SqlEngine materializes each CTE
    // into a catalog table first and plans the inner SELECT (_materializeCtes).
    // A parallel CTE-planning path here would silently diverge from that — the
    // ~110-line one this replaced was dead code and exactly such a trap.
    throw UnsupportedError(
      'plan the inner SelectStatement; CTEs are materialized by SqlEngine',
    );
  }

  @override
  logical.LogicalPlan visitSelectStatement(ast.SelectStatement node) {
    // Start with FROM clause or dummy single-row table. Multiple FROM entries
    // (comma list) fold left-to-right; a WHERE equality conjunct joining two
    // sides becomes that fold step's HASH JOIN condition (extracted below) —
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
    // A GROUP BY, OR an aggregate anywhere, means we're aggregating.
    final aggregating = hasAggregates || node.groupBy.isNotEmpty;

    // Group-by keys as logical expressions (empty for a global aggregate),
    // available to the SELECT/ORDER-BY rewrites below.
    final groupExprs = node.groupBy.map(_convertExpression).toList();

    // Hoist the aggregates out of the SELECT list and HAVING BEFORE building the
    // aggregate step: it has to compute every aggregate the statement mentions,
    // wherever it was written, and a HAVING aggregate need not appear in the
    // SELECT list at all. Each clause keeps its rewritten expression, in which
    // the aggregates are now references to the columns that step emits.
    final aggregates = _Aggregates();
    final selectExprs = <ast.Expression>[];
    // ORDER BY with its aggregates hoisted too, index-parallel to node.orderBy.
    // Identical to it when not aggregating — _hoist is inert on an
    // aggregate-free expression.
    final orderExprs = [for (final item in node.orderBy) item.expression];
    var having = node.having;
    if (aggregating) {
      // Under DISTINCT / a set operation the carry below is unavailable (an
      // extra column changes which rows are distinct, and breaks set-op arity),
      // so ORDER BY must reference an output column — as SQL requires anyway.
      if (node.distinct || node.setOps.isNotEmpty) {
        _rejectAggregateOrderBy(node.orderBy);
      }
      for (final item in node.selectList) {
        selectExprs.add(_hoist(item.expression, aggregates));
      }
      if (having != null) having = _hoist(having, aggregates);
      // Hoisted LAST so `__agg$i` numbering follows SELECT-list order, and an
      // aggregate already written there binds to the SAME column (see _bind).
      for (int i = 0; i < orderExprs.length; i++) {
        orderExprs[i] = _hoist(orderExprs[i], aggregates);
      }
      plan = _buildAggregate(
        plan,
        groupExprs,
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
        // is left of the expression is group keys, and a group key — plain
        // column or expression (`k % 2`) — must reference its own emitted
        // column rather than re-evaluate over now-absent base columns.
        expressions.add(
          _rewriteOverAggregate(_convertExpression(selectExprs[i]), groupExprs),
        );
        aliases.add(item.alias ?? _expressionName(item.expression));
      }
      // Two unaliased same-named items (`COUNT(*), COUNT(x)`) must not emit a
      // duplicate-name schema — same rule as the non-aggregating projection.
      _uniquifyAuto(aliases, node.selectList);

      final extraExprs = <logical.LogicalExpression>[];
      final extraAliases = <String>[];
      if (node.orderBy.isNotEmpty && !node.distinct && node.setOps.isEmpty) {
        final have = aliases.toSet();
        for (final expr in orderExprs) {
          _carry(
            expr,
            groupExprs,
            aggregates.aliases,
            have,
            extraExprs,
            extraAliases,
          );
        }
      }

      plan = logical.Projection(
        input: plan,
        expressions: [...expressions, ...extraExprs],
        aliases: [...aliases, ...extraAliases],
      );
      if (extraExprs.isNotEmpty) sortTrim = List.of(aliases);
    }

    // `SELECT *` with a GLOBAL aggregate (no grouping columns) has no columns to
    // project and is not valid SQL — fail loud rather than emit a zero-column
    // result. (With a GROUP BY the star means the group keys, handled below.)
    if (_isSelectStar(node.selectList) && aggregating && groupExprs.isEmpty) {
      throw UnsupportedError(
        'SELECT * with a global aggregate has no grouping columns; list the '
        'grouped columns explicitly or add a GROUP BY',
      );
    }

    // `SELECT *` under aggregation means the group keys — there is no SELECT
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
        for (int i = 0; i < groupExprs.length; i++)
          _groupKeyName(groupExprs[i], i),
      ];
      // An ORDER BY aggregate needs its column past this projection too, or the
      // sort would reference one just dropped.
      final extraExprs = <logical.LogicalExpression>[];
      final extraAliases = <String>[];
      if (node.orderBy.isNotEmpty && !node.distinct && node.setOps.isEmpty) {
        final have = keys.toSet();
        for (final expr in orderExprs) {
          _carry(
            expr,
            groupExprs,
            aggregates.aliases,
            have,
            extraExprs,
            extraAliases,
          );
        }
      }
      plan = logical.Projection(
        input: plan,
        expressions: [
          for (int i = 0; i < groupExprs.length; i++)
            _rewriteOverAggregate(groupExprs[i], groupExprs),
          ...extraExprs,
        ],
        aliases: [...keys, ...extraAliases],
      );
      if (extraExprs.isNotEmpty) sortTrim = keys;
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
        final expr = item.expression;
        if (item.alias == null &&
            counts[aliases[i]]! > 1 &&
            expr is ast.ColumnExpression &&
            expr.table != null) {
          aliases[i] = '${expr.table}.${expr.column}';
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
      final extraExprs = <logical.LogicalExpression>[];
      final extraAliases = <String>[];
      if (node.orderBy.isNotEmpty && !node.distinct && node.setOps.isEmpty) {
        final have = aliases.toSet();
        final seen = <String>{};
        for (final item in node.orderBy) {
          _collectSortColumns(
            item.expression,
            have,
            seen,
            extraExprs,
            extraAliases,
          );
        }
      }

      plan = logical.Projection(
        input: plan,
        expressions: [...expressions, ...extraExprs],
        aliases: [...aliases, ...extraAliases],
      );
      if (extraExprs.isNotEmpty) sortTrim = List.of(aliases);
    }

    // Apply DISTINCT (per-branch: it binds to this SELECT core, left of any tail)
    if (node.distinct) {
      plan = logical.Distinct(plan);
    }

    // Fold the set-operation tails onto this core BEFORE ORDER BY / LIMIT, so
    // those clauses apply to the WHOLE combined result (a trailing `ORDER BY` on
    // `A UNION B` sorts the union, not just B). Left-associative, INTERSECT tighter
    // than UNION/EXCEPT.
    if (node.setOps.isNotEmpty) {
      plan = _foldSetOps(plan, node.setOps);
    }

    // Apply ORDER BY. In an aggregate query the sort runs over the aggregate's
    // output, so a group-key reference (`ORDER BY k % 2`) must resolve to the
    // emitted `group$i` column rather than re-evaluate over absent base columns.
    if (node.orderBy.isNotEmpty) {
      final sortExprs = [
        for (int i = 0; i < node.orderBy.length; i++)
          logical.SortExpression(
            expression: aggregating
                ? _rewriteOverAggregate(
                    _convertExpression(orderExprs[i]),
                    groupExprs,
                  )
                : _convertExpression(orderExprs[i]),
            ascending: node.orderBy[i].ascending,
            nullsFirst: node.orderBy[i].nullsFirst,
          ),
      ];

      plan = logical.Sort(input: plan, sortExpressions: sortExprs);
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

  (logical.SetOpType, bool) _setOpType(ast.SetOperationType type) =>
      switch (type) {
        ast.SetOperationType.union => (logical.SetOpType.union, false),
        ast.SetOperationType.unionAll => (logical.SetOpType.union, true),
        ast.SetOperationType.intersect => (logical.SetOpType.intersect, false),
        ast.SetOperationType.except => (logical.SetOpType.except, false),
      };

  /// Fold a flat left-to-right list of set-operation [tails] onto [base] honoring
  /// SQL precedence: INTERSECT binds tighter than UNION/EXCEPT, and both are
  /// left-associative. Collapse each INTERSECT into the current term as it
  /// appears, start a new term on UNION/EXCEPT, then left-fold the terms — so
  /// `A UNION B INTERSECT C` is `A UNION (B INTERSECT C)` and `A EXCEPT B EXCEPT C`
  /// is `(A EXCEPT B) EXCEPT C`.
  logical.LogicalPlan _foldSetOps(
    logical.LogicalPlan base,
    List<ast.SetOperation> tails,
  ) {
    var term = base;
    final terms = <logical.LogicalPlan>[];
    final ops = <(logical.SetOpType, bool)>[];
    for (final tail in tails) {
      final (type, all) = _setOpType(tail.type);
      final right = tail.right.accept(this);
      if (type == logical.SetOpType.intersect) {
        term = _bindSetOperation(term, right, type, all);
      } else {
        terms.add(term);
        ops.add((type, all));
        term = right;
      }
    }
    terms.add(term);
    var acc = terms.first;
    for (int i = 0; i < ops.length; i++) {
      acc = _bindSetOperation(acc, terms[i + 1], ops[i].$1, ops[i].$2);
    }
    return acc;
  }

  /// Bind a set operation to SQL semantics: equal arity, a common type resolved
  /// per position (with a CAST inserted on the differing branch), and the RIGHT
  /// branch stamped with the LEFT output NAMES (SQL derives set-op output names
  /// from the left). Without this the operator forwards raw batches — a wider
  /// right side silently loses columns, and a valid positional union with
  /// different names fails downstream on a name mismatch.
  logical.LogicalPlan _bindSetOperation(
    logical.LogicalPlan left,
    logical.LogicalPlan right,
    logical.SetOpType type,
    bool all,
  ) {
    final lf = left.schema.fields;
    final rf = right.schema.fields;
    if (lf.length != rf.length) {
      throw ArgumentError(
        'Set operation branches have different column counts '
        '(${lf.length} vs ${rf.length})',
      );
    }
    // Types are NOT reconciled here: a logical column ref carries the `string`
    // PLACEHOLDER type, so commonType() against it lies — pairing a literal/
    // CAST/comparison branch with a bare-column branch injected a stringifying
    // CAST into a valid all-numeric set operation (`SELECT 1 UNION SELECT n`
    // failed at the int64→string cast). visitSetOperation reconciles on the
    // REAL physical types (including decimal scale); only the arity check and
    // the left-name stamping belong at this layer.
    final leftNames = [for (final f in lf) f.name];
    return logical.SetOperation(
      left: left,
      right: _renameBranch(right, rf, leftNames),
      type: type,
      all: all,
    );
  }

  /// Project [input] so its columns carry [names] (the LEFT branch's names).
  /// Returns [input] unchanged when the names already match.
  logical.LogicalPlan _renameBranch(
    logical.LogicalPlan input,
    List<Field> fields,
    List<String> names,
  ) {
    var needs = false;
    for (int i = 0; i < fields.length; i++) {
      if (names[i] != fields[i].name) needs = true;
    }
    if (!needs) return input;
    return logical.Projection(
      input: input,
      expressions: [
        for (final f in fields)
          logical.ColumnExpression(name: f.name, columnType: f.dataType),
      ],
      aliases: names,
    );
  }

  @override
  logical.LogicalPlan visitTableReference(ast.TableReference node) {
    throw UnimplementedError();
  }

  @override
  logical.LogicalPlan visitSimpleTable(ast.SimpleTable node) {
    // Check if it's a file path
    if (node.isFilePath) {
      // Files are registered under their raw path string (so same-basename files
      // coexist); the basename only supplies the default column-qualifier alias.
      final table = catalog.table(node.table);
      if (table == null) {
        throw StateError(
          'File not found or could not be loaded: ${node.table}',
        );
      }

      return logical.TableScan(
        tableName: node.table,
        alias: node.alias ?? tableNameFromPath(node.table),
        tableSchema: table.schema,
      );
    }

    // Regular table (includes materialized CTEs, registered under their name).
    // Default the alias to the table name so a bare `FROM t` still qualifies its
    // columns as `t` (needed to route qualified ON columns in joins/self-joins).
    final table = catalog.table(node.table);
    if (table == null) {
      throw StateError('Table not found: ${node.table}');
    }

    return logical.TableScan(
      tableName: node.table,
      alias: node.alias ?? node.table,
      tableSchema: table.schema,
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
        // NOTE: NATURAL JOIN is treated as INNER — it does NOT derive its
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
          final onCond = join.condition as ast.OnCondition;
          condition = _convertExpression(onCond.expression);
        } else if (join.condition is ast.UsingCondition) {
          final usingCond = join.condition as ast.UsingCondition;
          usingColumns = usingCond.columns;
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
    ast.Expression expr,
    Set<String> have,
    Set<String> seen,
    List<logical.LogicalExpression> extraExprs,
    List<String> extraAliases,
  ) {
    void recurse(ast.Expression e) =>
        _collectSortColumns(e, have, seen, extraExprs, extraAliases);
    if (expr is ast.ColumnExpression) {
      final name = expr.column;
      if (!have.contains(name) && seen.add(name)) {
        extraExprs.add(_convertExpression(expr));
        extraAliases.add(name);
      }
    } else if (expr is ast.BinaryExpression) {
      recurse(expr.left);
      recurse(expr.right);
    } else if (expr is ast.UnaryExpression) {
      recurse(expr.operand);
    } else if (expr is ast.FunctionExpression) {
      expr.arguments.forEach(recurse);
    } else if (expr is ast.CastExpression) {
      recurse(expr.expression);
    } else if (expr is ast.CaseExpression) {
      if (expr.expression != null) recurse(expr.expression!);
      for (final w in expr.whenClauses) {
        recurse(w.condition);
        recurse(w.result);
      }
      if (expr.elseExpression != null) recurse(expr.elseExpression!);
    } else if (expr is ast.InExpression) {
      recurse(expr.expression);
      expr.values.forEach(recurse);
    } else if (expr is ast.BetweenExpression) {
      recurse(expr.expression);
      recurse(expr.lower);
      recurse(expr.upper);
    }
    // Literals, stars, and nested subqueries reference no base columns here.
  }

  /// Apply WHERE, pulling any top-level `col IN/NOT IN (SELECT …)` conjunct out
  /// as a SEMI / null-aware anti join (the subquery is the build side); the rest
  /// stays one Filter. A semi/anti join emits the left schema, so the plan's
  /// columns are unchanged for everything above.
  /// Fold a comma-list FROM into joins, pulling each WHERE conjunct of the
  /// form `l = r` — where one column resolves only in the already-folded left
  /// side and the other only in the entry being joined — into that join's
  /// condition (the physical layer then hash-joins on it). Conjuncts that
  /// don't match stay in the returned residual WHERE, so anything uncertain
  /// (ambiguous names, expressions, non-equalities) keeps its exact previous
  /// filter-over-cross-join behavior.
  ({logical.LogicalPlan plan, ast.Expression? residual}) _foldCommaJoin(
    ast.SelectStatement node,
    List<logical.LogicalPlan> entries,
    ast.Expression where,
  ) {
    String? qualifier(ast.TableReference f) =>
        f is ast.SimpleTable ? (f.alias ?? f.table) : null;
    final quals = [for (final f in node.from) qualifier(f)];
    final names = [
      for (final e in entries) {for (final f in e.schema.fields) f.name},
    ];

    final conjuncts = _andConjuncts(where);
    final used = <ast.Expression>{};
    var plan = entries.first;
    final leftNames = {...names.first};
    final leftQuals = {if (quals.first != null) quals.first!};
    for (int i = 1; i < entries.length; i++) {
      logical.LogicalExpression? condition;
      for (final c in conjuncts) {
        if (used.contains(c)) continue;
        if (!_joinsSides(c, leftNames, leftQuals, names[i], quals[i])) {
          continue;
        }
        used.add(c);
        final eq = _convertExpression(c);
        condition = condition == null
            ? eq
            : logical.BinaryExpression(
                condition,
                logical.BinaryOperator.and,
                eq,
              );
      }
      plan = logical.Join(
        left: plan,
        right: entries[i],
        joinType: condition == null
            ? logical.JoinType.cross
            : logical.JoinType.inner,
        condition: condition,
      );
      leftNames.addAll(names[i]);
      final q = quals[i];
      if (q != null) leftQuals.add(q);
    }
    final rest = [
      for (final c in conjuncts)
        if (!used.contains(c)) c,
    ];
    return (
      plan: plan,
      residual: rest.isEmpty
          ? null
          : rest.reduce(
              (a, b) =>
                  ast.BinaryExpression(left: a, operator: 'AND', right: b),
            ),
    );
  }

  /// Whether [c] is a `column = column` equality whose sides land on OPPOSITE
  /// sides of the current fold step. A qualified column resolves by its
  /// qualifier; an unqualified one must appear in exactly one side's schema.
  bool _joinsSides(
    ast.Expression c,
    Set<String> leftNames,
    Set<String> leftQuals,
    Set<String> rightNames,
    String? rightQual,
  ) {
    if (c is! ast.BinaryExpression || c.operator != '=') return false;
    final l = c.left;
    final r = c.right;
    if (l is! ast.ColumnExpression || r is! ast.ColumnExpression) return false;
    int? side(ast.ColumnExpression e) {
      final t = e.table;
      if (t != null) {
        if (t == rightQual) return 1;
        if (leftQuals.contains(t)) return 0;
        return null; // unknown qualifier — leave it to normal binding
      }
      final inLeft = leftNames.contains(e.column);
      final inRight = rightNames.contains(e.column);
      if (inLeft == inRight) return null; // absent, or ambiguous across sides
      return inLeft ? 0 : 1;
    }

    final sl = side(l);
    final sr = side(r);
    return sl != null && sr != null && sl != sr;
  }

  logical.LogicalPlan _applyWhere(
    logical.LogicalPlan plan,
    ast.Expression where,
  ) {
    final residual = <ast.Expression>[];
    var result = plan;
    for (final conjunct in _andConjuncts(where)) {
      final joined =
          _inSubqueryJoin(conjunct, result) ?? _existsJoin(conjunct, result);
      if (joined != null) {
        result = joined;
      } else {
        residual.add(conjunct);
      }
    }
    if (residual.isNotEmpty) {
      result = logical.Filter(
        input: result,
        predicate: _convertExpression(residual.reduce(_and)),
      );
    }
    return result;
  }

  /// Flatten a predicate into its ANDed conjuncts.
  List<ast.Expression> _andConjuncts(ast.Expression e) {
    if (e is ast.BinaryExpression && e.operator.toUpperCase() == 'AND') {
      return [..._andConjuncts(e.left), ..._andConjuncts(e.right)];
    }
    return [e];
  }

  ast.Expression _and(ast.Expression a, ast.Expression b) =>
      ast.BinaryExpression(left: a, operator: 'AND', right: b);

  /// Turn a top-level `col IN/NOT IN (SELECT …)` conjunct into a SEMI (IN) /
  /// null-aware anti (NOT IN) join with the subquery as the build side. An
  /// uncorrelated subquery joins on `col = <subquery column>`; a CORRELATED one
  /// is decorrelated — each `inner = outer` equality in the subquery's WHERE
  /// becomes an extra join key, and its inner column is projected out for that
  /// key (see [_correlationEquality]). Returns null when [conjunct] is not such a
  /// predicate (it stays a Filter) or its LHS is not a plain column (the join
  /// needs a column key — that case reaches [_convertExpression], which rejects
  /// a subquery IN).
  logical.LogicalPlan? _inSubqueryJoin(
    ast.Expression conjunct,
    logical.LogicalPlan left,
  ) {
    if (conjunct is! ast.InExpression || conjunct.subquery == null) return null;
    if (conjunct.expression is! ast.ColumnExpression) return null;

    final sub = conjunct.subquery!;
    final innerScope = <String>{};
    for (final ref in sub.from) {
      _fromScope(ref, innerScope);
    }

    // Split the subquery's WHERE into correlation equalities (inner = outer) and
    // everything else. A non-equi or non-top-level outer reference stays in the
    // local part and is caught by _requireNoOuterRefs below (fail loud).
    final correlations =
        <({ast.ColumnExpression outer, ast.ColumnExpression inner})>[];
    final local = <ast.Expression>[];
    if (sub.where != null) {
      for (final c in _andConjuncts(sub.where!)) {
        final correlation = _correlationEquality(c, innerScope);
        if (correlation != null) {
          correlations.add(correlation);
        } else {
          local.add(c);
        }
      }
    }

    // Correlated NOT IN would need PER-OUTER-ROW null awareness (`x NOT IN S_o`
    // is UNKNOWN only when *this* row's group S_o holds a null), but a single
    // decorrelated anti join decides "build has a null" GLOBALLY — so a null in
    // one group would wrongly blank out every row. Correlated IN (a semi join)
    // has no such issue: IN selects only on TRUE, and a semi join emits exactly
    // the true matches.
    if (correlations.isNotEmpty && conjunct.negated) {
      throw UnsupportedError(
        'correlated NOT IN (SELECT …) is not supported (its null semantics are '
        'per-row and cannot be decorrelated to a single anti join)',
      );
    }

    // Decorrelation is only sound for a plain SELECT: aggregation / DISTINCT /
    // set operations / paging over a correlated body change what "a matching row
    // exists" means per outer row.
    if (correlations.isNotEmpty) {
      if (sub.groupBy.isNotEmpty ||
          sub.having != null ||
          sub.distinct ||
          sub.setOps.isNotEmpty ||
          sub.limit != null ||
          sub.offset != null ||
          sub.selectList.any((i) => _containsAggregate(i.expression))) {
        throw UnsupportedError(
          'correlated IN (SELECT …) is only supported over a plain SELECT '
          '(no GROUP BY / DISTINCT / aggregate / set operation / LIMIT)',
        );
      }
    }

    // Every remaining reference must be to the subquery's own scope. An outer
    // ref left after extracting the equi-correlations is a correlation shape we
    // don't decorrelate — a non-equi (`u.z > t.z`) leaves `correlations` empty
    // and the ref in `local`, so this must run for BOTH branches (and gives a
    // clearer message than strict binding's bare "Unknown column").
    _requireNoOuterRefs(sub, local, innerScope);

    final logical.LogicalPlan subPlan;
    if (correlations.isEmpty) {
      // Uncorrelated: use the ORIGINAL subquery plan UNCHANGED, so GROUP BY /
      // HAVING / LIMIT / set operations are preserved. (Rebuilding a partial AST
      // silently dropped them — a plausible wrong answer.)
      subPlan = sub.accept(this);
      if (subPlan.schema.fields.length != 1) {
        throw ArgumentError(
          'IN (SELECT …) must return exactly one column, got '
          '${subPlan.schema.fields.length}',
        );
      }
    } else {
      // Correlated (guarded to a plain SELECT above): rebuild with the
      // correlation equalities removed from WHERE and each correlation inner
      // column projected.
      final subquery = ast.SelectStatement(
        distinct: sub.distinct,
        selectList: [
          ...sub.selectList,
          // Alias each correlation column uniquely so it can't collide with the
          // IN column when they are the same (`SELECT c.x … WHERE c.x = o.x`).
          for (int i = 0; i < correlations.length; i++)
            ast.SelectItem(
              expression: correlations[i].inner,
              alias: '__corr$i',
            ),
        ],
        from: sub.from,
        where: local.isEmpty ? null : local.reduce(_and),
      );
      subPlan = subquery.accept(this);
      if (subPlan.schema.fields.length != 1 + correlations.length) {
        throw ArgumentError(
          'IN (SELECT …) must return exactly one column, got '
          '${subPlan.schema.fields.length - correlations.length}',
        );
      }
    }
    final fields = subPlan.schema.fields;

    final n = _inSubquery++;
    final alias = '__in_sub$n';
    // Rename each output to a synthetic name so a join key can never collide with
    // an outer column of the same name, and qualify it with a synthetic alias so
    // the physical builder routes it to the right (build) side.
    final keyNames = [
      for (int i = 0; i < fields.length; i++) '__in_key${n}_$i',
    ];
    final right = logical.AliasScope(
      alias: alias,
      input: logical.Projection(
        input: subPlan,
        expressions: [
          for (int i = 0; i < fields.length; i++)
            logical.ColumnExpression(
              name: fields[i].name,
              columnType: fields[i].dataType,
            ),
        ],
        aliases: keyNames,
      ),
    );

    // key 0 is the IN column; keys 1..N are the correlation columns.
    final outerKeys = <ast.Expression>[
      conjunct.expression,
      for (final c in correlations) c.outer,
    ];
    logical.LogicalExpression subRef(int i) => logical.ColumnExpression(
      name: keyNames[i],
      table: alias,
      columnType: fields[i].dataType,
    );
    logical.LogicalExpression condition = logical.BinaryExpression(
      _convertExpression(outerKeys[0]),
      logical.BinaryOperator.eq,
      subRef(0),
    );
    for (int i = 1; i < outerKeys.length; i++) {
      condition = logical.BinaryExpression(
        condition,
        logical.BinaryOperator.and,
        logical.BinaryExpression(
          _convertExpression(outerKeys[i]),
          logical.BinaryOperator.eq,
          subRef(i),
        ),
      );
    }
    return logical.Join(
      left: left,
      right: right,
      joinType: conjunct.negated
          ? logical.JoinType.antiNullAware
          : logical.JoinType.semi,
      condition: condition,
    );
  }

  /// Turn a top-level `[NOT] EXISTS (SELECT … WHERE inner = outer …)` conjunct
  /// into a SEMI (EXISTS) / plain anti (NOT EXISTS) join, keyed on the
  /// correlation equalities alone (EXISTS has no probe column — existence is the
  /// whole test). Returns null when [conjunct] is not an EXISTS.
  ///
  /// NOT EXISTS is a PLAIN anti, not null-aware: it is TRUE exactly when the
  /// subquery yields no row, indifferent to nulls (unlike NOT IN). A null
  /// correlation key on the outer side simply doesn't match, so that row is kept
  /// — which a plain anti does.
  logical.LogicalPlan? _existsJoin(
    ast.Expression conjunct,
    logical.LogicalPlan left,
  ) {
    // `notExpression` binds a leading NOT before EXISTS is reached, so
    // `NOT EXISTS (…)` arrives as a NOT over an ExistsExpression.
    var node = conjunct;
    var negated = false;
    if (node is ast.UnaryExpression &&
        node.operator.toUpperCase() == 'NOT' &&
        node.operand is ast.ExistsExpression) {
      negated = true;
      node = node.operand;
    }
    if (node is! ast.ExistsExpression) return null;
    negated = negated != node.negated; // combine with a rule-parsed NOT
    final sub = node.subquery;
    final innerScope = <String>{};
    for (final ref in sub.from) {
      _fromScope(ref, innerScope);
    }

    final correlations =
        <({ast.ColumnExpression outer, ast.ColumnExpression inner})>[];
    final local = <ast.Expression>[];
    if (sub.where != null) {
      for (final c in _andConjuncts(sub.where!)) {
        final correlation = _correlationEquality(c, innerScope);
        if (correlation != null) {
          correlations.add(correlation);
        } else {
          local.add(c);
        }
      }
    }

    // An uncorrelated EXISTS is a whole-table "any row?" test, not a key join —
    // it has no operator here. (Correlated EXISTS is the common, useful shape.)
    if (correlations.isEmpty) {
      throw UnsupportedError(
        'uncorrelated EXISTS (SELECT …) is not supported; correlate it '
        '(EXISTS (SELECT … WHERE inner = outer)) or use a JOIN',
      );
    }
    if (sub.groupBy.isNotEmpty ||
        sub.having != null ||
        sub.distinct ||
        sub.setOps.isNotEmpty ||
        sub.limit != null ||
        sub.offset != null ||
        sub.selectList.any((i) => _containsAggregate(i.expression))) {
      throw UnsupportedError(
        'correlated EXISTS (SELECT …) is only supported over a plain SELECT '
        '(no GROUP BY / DISTINCT / aggregate / set operation / LIMIT)',
      );
    }
    _requireNoOuterRefs(sub, local, innerScope);

    // EXISTS ignores the subquery's SELECT list; the build side is just its
    // correlation columns, filtered by the local WHERE. Each is uniquely aliased
    // so two correlations on the same inner column can't collide.
    final subquery = ast.SelectStatement(
      selectList: [
        for (int i = 0; i < correlations.length; i++)
          ast.SelectItem(expression: correlations[i].inner, alias: '__corr$i'),
      ],
      from: sub.from,
      where: local.isEmpty ? null : local.reduce(_and),
    );
    final subPlan = subquery.accept(this);
    final fields = subPlan.schema.fields;

    final n = _inSubquery++;
    final alias = '__ex_sub$n';
    final keyNames = [
      for (int i = 0; i < fields.length; i++) '__ex_key${n}_$i',
    ];
    final right = logical.AliasScope(
      alias: alias,
      input: logical.Projection(
        input: subPlan,
        expressions: [
          for (int i = 0; i < fields.length; i++)
            logical.ColumnExpression(
              name: fields[i].name,
              columnType: fields[i].dataType,
            ),
        ],
        aliases: keyNames,
      ),
    );
    logical.LogicalExpression subRef(int i) => logical.ColumnExpression(
      name: keyNames[i],
      table: alias,
      columnType: fields[i].dataType,
    );
    logical.LogicalExpression condition = logical.BinaryExpression(
      _convertExpression(correlations[0].outer),
      logical.BinaryOperator.eq,
      subRef(0),
    );
    for (int i = 1; i < correlations.length; i++) {
      condition = logical.BinaryExpression(
        condition,
        logical.BinaryOperator.and,
        logical.BinaryExpression(
          _convertExpression(correlations[i].outer),
          logical.BinaryOperator.eq,
          subRef(i),
        ),
      );
    }
    return logical.Join(
      left: left,
      right: right,
      joinType: negated ? logical.JoinType.anti : logical.JoinType.semi,
      condition: condition,
    );
  }

  /// If [c] is `inner = outer` (or `outer = inner`) — a plain column equality
  /// relating one of the subquery's own columns to an OUTER column (a qualifier
  /// not in [innerScope]) — return the pair. An unqualified column counts as
  /// inner (Postgres's innermost-scope rule). Null for anything else.
  ({ast.ColumnExpression outer, ast.ColumnExpression inner})?
  _correlationEquality(ast.Expression c, Set<String> innerScope) {
    if (c is! ast.BinaryExpression || c.operator != '=') return null;
    final a = c.left, b = c.right;
    if (a is! ast.ColumnExpression || b is! ast.ColumnExpression) return null;
    final aOuter = a.table != null && !innerScope.contains(a.table);
    final bOuter = b.table != null && !innerScope.contains(b.table);
    if (aOuter && !bOuter) return (outer: a, inner: b);
    if (bOuter && !aOuter) return (outer: b, inner: a);
    return null; // both inner (local), both outer, or not a correlation
  }

  /// Fail loud if any reference in the subquery's [local] WHERE, SELECT, GROUP
  /// BY, HAVING, or ORDER BY qualifies with a table outside its own scope. The
  /// inner plan is built in isolation, so such a reference would fall through
  /// Schema.resolve's unknown-qualifier fallback to a BARE match on an inner
  /// column and silently answer wrong. (Unqualified outer refs are safe: they
  /// bind to an inner column, or fail loud as unknown.)
  void _requireNoOuterRefs(
    ast.SelectStatement sub,
    List<ast.Expression> local,
    Set<String> innerScope,
  ) {
    final used = <String>{};
    for (final c in local) {
      _usedQualifiers(c, used);
    }
    if (sub.having != null) _usedQualifiers(sub.having!, used);
    for (final item in sub.selectList) {
      _usedQualifiers(item.expression, used);
    }
    for (final g in sub.groupBy) {
      _usedQualifiers(g, used);
    }
    for (final o in sub.orderBy) {
      _usedQualifiers(o.expression, used);
    }
    final outer = used.difference(innerScope);
    if (outer.isNotEmpty) {
      throw UnsupportedError(
        'correlated IN (SELECT …) supports only top-level `inner = outer` '
        'equalities in the subquery WHERE; ${outer.join(", ")} is referenced '
        'elsewhere',
      );
    }
  }

  /// The table qualifiers a FROM reference makes visible to its own scope: a
  /// table's alias (or its name when unaliased), a derived table's alias.
  void _fromScope(ast.TableReference ref, Set<String> out) {
    if (ref is ast.SimpleTable) {
      out.add(ref.alias ?? ref.table);
    } else if (ref is ast.SubqueryTable) {
      out.add(ref.alias);
    } else if (ref is ast.ValuesTable) {
      if (ref.alias != null) out.add(ref.alias!);
    } else if (ref is ast.JoinedTable) {
      _fromScope(ref.left, out);
      for (final j in ref.joins) {
        _fromScope(j.right, out);
      }
    }
  }

  /// Collect the table qualifiers used by column references in [expr], WITHOUT
  /// descending into a nested subquery (a separate scope).
  void _usedQualifiers(ast.Expression expr, Set<String> out) {
    if (expr is ast.ColumnExpression) {
      if (expr.table != null) out.add(expr.table!);
    } else if (expr is ast.BinaryExpression) {
      _usedQualifiers(expr.left, out);
      _usedQualifiers(expr.right, out);
    } else if (expr is ast.UnaryExpression) {
      _usedQualifiers(expr.operand, out);
    } else if (expr is ast.FunctionExpression) {
      for (final a in expr.arguments) {
        _usedQualifiers(a, out);
      }
    } else if (expr is ast.CastExpression) {
      _usedQualifiers(expr.expression, out);
    } else if (expr is ast.CaseExpression) {
      if (expr.expression != null) _usedQualifiers(expr.expression!, out);
      for (final w in expr.whenClauses) {
        _usedQualifiers(w.condition, out);
        _usedQualifiers(w.result, out);
      }
      if (expr.elseExpression != null) {
        _usedQualifiers(expr.elseExpression!, out);
      }
    } else if (expr is ast.InExpression) {
      _usedQualifiers(expr.expression, out);
      for (final v in expr.values) {
        _usedQualifiers(v, out);
      }
      // NOT its subquery — a separate scope.
    } else if (expr is ast.BetweenExpression) {
      _usedQualifiers(expr.expression, out);
      _usedQualifiers(expr.lower, out);
      _usedQualifiers(expr.upper, out);
    }
    // Literals, stars, nested subqueries: no outer refs contributed here.
  }

  // Expression conversion helpers
  logical.LogicalExpression _convertExpression(ast.Expression expr) {
    if (expr is ast.ColumnExpression) {
      return logical.ColumnExpression(
        name: expr.column,
        table: expr.table,
        columnType: DataType.string, // Will be resolved during binding
      );
    } else if (expr is ast.LiteralExpression) {
      return logical.LiteralExpression(
        expr.value,
        _inferLiteralType(expr.value),
      );
    } else if (expr is ast.InExpression) {
      if (expr.subquery != null) {
        // A subquery IN is planned as a semi/anti join by _applyWhere, but only
        // as a top-level WHERE conjunct. Reaching here means another position (a
        // SELECT item, HAVING, inside OR / CASE), which the join rewrite can't
        // express.
        throw UnsupportedError(
          'IN (SELECT …) is only supported as a top-level WHERE condition; '
          'elsewhere, use a JOIN or list the values explicitly',
        );
      }
      return logical.InExpression(
        value: _convertExpression(expr.expression),
        list: expr.values.map(_convertExpression).toList(),
        negated: expr.negated,
      );
    } else if (expr is ast.BinaryExpression) {
      // `x IS [NOT] TRUE/FALSE` is a 2-valued predicate distinct from `IS NULL`
      // and from `= true` (NULL IS TRUE is FALSE, not NULL). The parser produces
      // `IS`/`IS NOT` with a boolean-literal RHS, which the generic mapping would
      // collapse to isNull. Desugar it to the correct 3VL form here. (A NULL RHS
      // falls through to the isNull/isNotNull mapping below.)
      final boolIs = _convertBooleanIs(expr);
      if (boolIs != null) return boolIs;
      return logical.BinaryExpression(
        _convertExpression(expr.left),
        _convertBinaryOperator(expr.operator),
        _convertExpression(expr.right),
      );
    } else if (expr is ast.FunctionExpression) {
      return logical.FunctionExpression(
        name: expr.name,
        arguments: expr.arguments.map(_convertExpression).toList(),
        returnType: DataType.string, // Will be resolved during binding
      );
    } else if (expr is ast.CaseExpression) {
      final clauses = <logical.WhenClause>[
        for (final w in expr.whenClauses)
          logical.WhenClause(
            expr.expression == null
                ? _convertExpression(w.condition)
                : logical.BinaryExpression(
                    _convertExpression(expr.expression!),
                    logical.BinaryOperator.eq,
                    _convertExpression(w.condition),
                  ),
            _convertExpression(w.result),
          ),
      ];
      return logical.CaseExpression(
        whenClauses: clauses,
        elseExpression: expr.elseExpression == null
            ? null
            : _convertExpression(expr.elseExpression!),
      );
    } else if (expr is ast.CastExpression) {
      final target = _parseDataType(expr.targetType);
      // `DECIMAL(p,s)` carries an explicit precision/scale; bare DECIMAL does
      // not (a decimal source then keeps its own scale, an int casts at 0).
      // decimalTypeParams matches the `decimal(p,s)` spelling only, so map the
      // NUMERIC synonym onto it.
      final params = target == DataType.decimal && expr.targetType.contains('(')
          ? decimalTypeParams(
              expr.targetType.toLowerCase().replaceFirst('numeric', 'decimal'),
            )
          : null;
      return logical.CastExpression(
        _convertExpression(expr.expression),
        target,
        precision: params?.precision,
        scale: params?.scale,
      );
    } else if (expr is ast.UnaryExpression) {
      final operator = expr.operator.toUpperCase();
      if (operator == '+') return _convertExpression(expr.operand);
      if (operator == '-') {
        final operand = expr.operand;
        // Fold a numeric literal (so `-5` is a real value, not `0 - 5`).
        if (operand is ast.LiteralExpression && operand.value is num) {
          final v = -(operand.value as num);
          return logical.LiteralExpression(v, _inferLiteralType(v));
        }
        return logical.BinaryExpression(
          logical.LiteralExpression(0, DataType.int64),
          logical.BinaryOperator.subtract,
          _convertExpression(operand),
        );
      }
      if (operator == 'NOT') {
        // NOT x ≡ (x = false) under the engine's boolean evaluation.
        return logical.BinaryExpression(
          _convertExpression(expr.operand),
          logical.BinaryOperator.eq,
          logical.LiteralExpression(false, DataType.boolean),
        );
      }
      throw UnsupportedError('Unsupported unary operator: ${expr.operator}');
    }

    // Fail closed: an unsupported expression node must not silently become a
    // NULL column.
    throw UnsupportedError('Unsupported SQL expression: ${expr.runtimeType}');
  }

  /// Map a SQL type name (optionally parametrized, `DECIMAL(10,2)`) to a
  /// [DataType].
  DataType _parseDataType(String type) {
    final base = type.toUpperCase().replaceAll(RegExp(r'\s*\(.*\)$'), '');
    switch (base) {
      case 'INT':
      case 'INTEGER':
      case 'BIGINT':
      case 'SMALLINT':
      case 'TINYINT':
        return DataType.int64;
      case 'DOUBLE':
      case 'FLOAT':
      case 'REAL':
        return DataType.float64;
      // The engine HAS an exact decimal type — mapping these to float64
      // silently destroyed precision on any value past 2^53.
      case 'DECIMAL':
      case 'NUMERIC':
        return DataType.decimal;
      case 'BOOLEAN':
      case 'BOOL':
        return DataType.boolean;
      case 'DATE':
        return DataType.date;
      case 'TIMESTAMP':
      case 'DATETIME':
        return DataType.timestamp;
      default:
        return DataType.string;
    }
  }

  logical.BinaryOperator _convertBinaryOperator(String operator) {
    switch (operator.toUpperCase()) {
      case '+':
        return logical.BinaryOperator.add;
      case '-':
        return logical.BinaryOperator.subtract;
      case '*':
        return logical.BinaryOperator.multiply;
      case '/':
        return logical.BinaryOperator.divide;
      case '%':
        return logical.BinaryOperator.modulo;
      case '=':
        return logical.BinaryOperator.eq;
      case '<>':
      case '!=':
        return logical.BinaryOperator.neq;
      case '<':
        return logical.BinaryOperator.lt;
      case '<=':
        return logical.BinaryOperator.lte;
      case '>':
        return logical.BinaryOperator.gt;
      case '>=':
        return logical.BinaryOperator.gte;
      case 'AND':
        return logical.BinaryOperator.and;
      case 'OR':
        return logical.BinaryOperator.or;
      case 'LIKE':
        return logical.BinaryOperator.like;
      case 'NOT LIKE':
        return logical.BinaryOperator.notLike;
      case 'IS':
        return logical.BinaryOperator.isNull;
      case 'IS NOT':
        return logical.BinaryOperator.isNotNull;
      default:
        throw ArgumentError('Unknown operator: $operator');
    }
  }

  /// Desugar `x IS [NOT] TRUE/FALSE` to its 2-valued semantics, or null when
  /// [expr] is not that form (a plain comparison, or `IS [NOT] NULL`). Unlike
  /// `= true`, these never yield NULL: `x IS TRUE` is FALSE (not NULL) when x is
  /// NULL. `x IS b` ≡ `(x IS NOT NULL) AND (x = b)`; `x IS NOT b` is its negation
  /// `(x IS NULL) OR (x = !b)`.
  logical.LogicalExpression? _convertBooleanIs(ast.BinaryExpression expr) {
    final isOp = expr.operator == 'IS';
    final isNotOp = expr.operator == 'IS NOT';
    if (!isOp && !isNotOp) return null;
    final right = expr.right;
    // IS [NOT] NULL: the generic isNull/isNotNull mapping below is correct.
    if (right is ast.LiteralExpression && right.value == null) return null;
    // Only NULL and TRUE/FALSE are valid IS operands. Anything else (`x IS 5`,
    // `x IS y`) used to fall to the generic mapping, which IGNORES the right
    // operand and silently evaluated `x IS NULL` — reject instead.
    if (right is! ast.LiteralExpression || right.value is! bool) {
      throw UnsupportedError(
        "the operand of IS${isNotOp ? ' NOT' : ''} must be NULL, TRUE, or "
        'FALSE',
      );
    }
    final wanted = right.value as bool;
    final value = _convertExpression(expr.left);
    logical.LogicalExpression cmp(bool b) => logical.BinaryExpression(
      value,
      logical.BinaryOperator.eq,
      logical.LiteralExpression(b, DataType.boolean),
    );
    logical.LogicalExpression nullCheck(logical.BinaryOperator op) =>
        logical.BinaryExpression(
          value,
          op,
          logical.LiteralExpression(null, DataType.boolean),
        );
    return isOp
        ? logical.BinaryExpression(
            nullCheck(logical.BinaryOperator.isNotNull),
            logical.BinaryOperator.and,
            cmp(wanted),
          )
        : logical.BinaryExpression(
            nullCheck(logical.BinaryOperator.isNull),
            logical.BinaryOperator.or,
            cmp(!wanted),
          );
  }

  DataType _inferLiteralType(dynamic value) {
    if (value == null) return DataType.string;
    // Type an integer by its RANGE. A flat int32 makes the declared dtype a lie
    // for anything past 2^31, and a boxed literal column carries that lie to a
    // fixed-width encoder that keeps only the low 32 bits.
    if (value is int) return DataType.intType(value);
    if (value is double) return DataType.float64;
    if (value is bool) return DataType.boolean;
    if (value is String) return DataType.string;
    return DataType.string;
  }

  bool _isSelectStar(List<ast.SelectItem> items) {
    return items.length == 1 && items[0].expression is ast.StarExpression;
  }

  String _expressionName(ast.Expression expr) {
    if (expr is ast.ColumnExpression) {
      return expr.column;
    } else if (expr is ast.FunctionExpression) {
      return expr.name;
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

  /// The OUTPUT column name the aggregate gives group key [g] at position [i]:
  /// a plain column keeps its name; any other expression is `group$i`. MUST
  /// match the physical aggregate's `_groupName`, so a later reference (SELECT
  /// projection, ORDER BY) resolves to the column the aggregate actually emits.
  String _groupKeyName(logical.LogicalExpression g, int i) =>
      g is logical.ColumnExpression ? g.name : 'group$i';

  /// Rewrite [expr] so any sub-expression that IS a GROUP BY key becomes a
  /// reference to that key's output column. After aggregation the base columns
  /// are gone (only the group keys + aggregates remain), so `SELECT k % 2` /
  /// `ORDER BY k % 2` over `GROUP BY k % 2` must reference the `group0` column
  /// rather than re-evaluate `k % 2` against columns that no longer exist.
  logical.LogicalExpression _rewriteOverAggregate(
    logical.LogicalExpression expr,
    List<logical.LogicalExpression> groupBy,
  ) {
    for (int i = 0; i < groupBy.length; i++) {
      if (_sameExpr(expr, groupBy[i])) {
        return logical.ColumnExpression(
          name: _groupKeyName(groupBy[i], i),
          columnType: groupBy[i].dataType,
        );
      }
    }
    if (expr is logical.BinaryExpression) {
      return logical.BinaryExpression(
        _rewriteOverAggregate(expr.left, groupBy),
        expr.operator,
        _rewriteOverAggregate(expr.right, groupBy),
      );
    }
    if (expr is logical.CastExpression) {
      return logical.CastExpression(
        _rewriteOverAggregate(expr.expression, groupBy),
        expr.targetType,
        precision: expr.precision,
        scale: expr.scale,
      );
    }
    if (expr is logical.FunctionExpression) {
      return logical.FunctionExpression(
        name: expr.name,
        arguments: [
          for (final a in expr.arguments) _rewriteOverAggregate(a, groupBy),
        ],
        returnType: expr.returnType,
      );
    }
    if (expr is logical.CaseExpression) {
      return logical.CaseExpression(
        whenClauses: [
          for (final w in expr.whenClauses)
            logical.WhenClause(
              _rewriteOverAggregate(w.condition, groupBy),
              _rewriteOverAggregate(w.result, groupBy),
            ),
        ],
        elseExpression: expr.elseExpression == null
            ? null
            : _rewriteOverAggregate(expr.elseExpression!, groupBy),
      );
    }
    return expr; // leaf column/literal (a group-key column matched above)
  }

  /// If an ORDER BY expression references a column the aggregate step emits — a
  /// GROUP BY key, or a hoisted `__agg$i` — that isn't in the SELECT output
  /// ([have]), carry that column through the projection so the sort (which runs
  /// above it) can see it; it is trimmed back out after the sort.
  ///
  /// Only ever carries columns that step actually emits, so it cannot invent a
  /// reference. [orderExpr] must already be hoisted (see [_hoist]).
  void _carry(
    ast.Expression orderExpr,
    List<logical.LogicalExpression> groupBy,
    List<String> aggregates,
    Set<String> have,
    List<logical.LogicalExpression> extraExprs,
    List<String> extraAliases,
  ) {
    final rewritten = _rewriteOverAggregate(
      _convertExpression(orderExpr),
      groupBy,
    );
    final refs = <String>{};
    _collectColumnNames(rewritten, refs);
    final emitted = {
      for (int i = 0; i < groupBy.length; i++) _groupKeyName(groupBy[i], i),
      ...aggregates,
    };
    for (final name in refs) {
      if (emitted.contains(name) && have.add(name)) {
        extraExprs.add(
          logical.ColumnExpression(name: name, columnType: DataType.string),
        );
        extraAliases.add(name);
      }
    }
  }

  void _collectColumnNames(logical.LogicalExpression expr, Set<String> out) {
    if (expr is logical.ColumnExpression) {
      out.add(expr.name);
    } else if (expr is logical.BinaryExpression) {
      _collectColumnNames(expr.left, out);
      _collectColumnNames(expr.right, out);
    } else if (expr is logical.CastExpression) {
      _collectColumnNames(expr.expression, out);
    } else if (expr is logical.FunctionExpression) {
      for (final a in expr.arguments) {
        _collectColumnNames(a, out);
      }
    } else if (expr is logical.CaseExpression) {
      for (final w in expr.whenClauses) {
        _collectColumnNames(w.condition, out);
        _collectColumnNames(w.result, out);
      }
      if (expr.elseExpression != null) {
        _collectColumnNames(expr.elseExpression!, out);
      }
    }
  }

  bool _isAggregateFunction(String name) {
    final upper = name.toUpperCase();
    return [
      'COUNT',
      'SUM',
      'AVG',
      'MIN',
      'MAX',
      'STDDEV',
      'VARIANCE',
    ].contains(upper);
  }

  /// Whether [expr] contains an aggregate ANYWHERE (recursively). Decides
  /// whether the statement aggregates at all, and gates the [_hoist] walk — the
  /// two MUST cover the same node shapes.
  bool _containsAggregate(ast.Expression expr) {
    if (expr is ast.FunctionExpression) {
      if (_isAggregateFunction(expr.name)) return true;
      return expr.arguments.any(_containsAggregate);
    }
    if (expr is ast.BinaryExpression) {
      return _containsAggregate(expr.left) || _containsAggregate(expr.right);
    }
    if (expr is ast.UnaryExpression) return _containsAggregate(expr.operand);
    if (expr is ast.CastExpression) return _containsAggregate(expr.expression);
    if (expr is ast.InExpression) {
      return _containsAggregate(expr.expression) ||
          expr.values.any(_containsAggregate);
    }
    if (expr is ast.CaseExpression) {
      if (expr.expression != null && _containsAggregate(expr.expression!)) {
        return true;
      }
      if (expr.elseExpression != null &&
          _containsAggregate(expr.elseExpression!)) {
        return true;
      }
      return expr.whenClauses.any(
        (w) => _containsAggregate(w.condition) || _containsAggregate(w.result),
      );
    }
    return false;
  }

  /// Register [agg] and return the name of the column the aggregate step emits
  /// for it, reusing a structurally-equal one already registered so an aggregate
  /// written more than once (`SUM(x) - SUM(x)`, or a HAVING repeating a SELECT
  /// item) is computed once.
  ///
  /// The match is structural rather than over a rendered key: the key this
  /// replaced named every non-column argument `expr` and ignored DISTINCT, so
  /// `SUM(x+1)` collided with `SUM(x*100)` and `COUNT(x)` with
  /// `COUNT(DISTINCT x)` — a HAVING then filtered on the wrong aggregate.
  String _bind(logical.AggregateExpression agg, _Aggregates into) {
    for (int i = 0; i < into.expressions.length; i++) {
      final other = into.expressions[i];
      if (other.function.name == agg.function.name &&
          other.distinct == agg.distinct &&
          _sameArgument(other.argument, agg.argument)) {
        return into.aliases[i];
      }
    }
    into.expressions.add(agg);
    into.aliases.add('__agg\$${into.aliases.length}');
    return into.aliases.last;
  }

  /// Structural equality over two aggregate arguments; null = no argument, as in
  /// `COUNT(*)`.
  bool _sameArgument(
    logical.LogicalExpression? a,
    logical.LogicalExpression? b,
  ) => (a == null || b == null) ? a == b : _sameExpr(a, b);

  /// Replace every aggregate call in [expr] with a reference to the column the
  /// aggregate step emits for it, registering each in [into] on first sight, so
  /// an aggregate may be written anywhere in an expression (`SUM(x)+1`,
  /// `ROUND(AVG(x),2)`, `CASE WHEN SUM(x)>5 …`) rather than only as a whole
  /// SELECT item.
  ///
  /// Returns an AST rather than a logical expression so everything AROUND the
  /// aggregate still goes through [_convertExpression] — operator mapping, unary
  /// folding and NOT desugaring live there and must not be reimplemented here. A
  /// subtree holding no aggregate is returned as-is, so an aggregate-free
  /// expression plans exactly as it did before.
  ///
  /// The shapes walked here MUST match [_containsAggregate]: one checked there
  /// but missed here plans the aggregate as a scalar function.
  ast.Expression _hoist(ast.Expression expr, _Aggregates into) {
    if (!_containsAggregate(expr)) return expr;
    if (expr is ast.FunctionExpression) {
      if (_isAggregateFunction(expr.name)) {
        return ast.ColumnExpression(
          column: _bind(_convertAggregateExpression(expr), into),
        );
      }
      return ast.FunctionExpression(
        name: expr.name,
        arguments: [for (final a in expr.arguments) _hoist(a, into)],
        distinct: expr.distinct,
        filter: expr.filter,
        window: expr.window,
      );
    }
    if (expr is ast.BinaryExpression) {
      return ast.BinaryExpression(
        left: _hoist(expr.left, into),
        operator: expr.operator,
        right: _hoist(expr.right, into),
      );
    }
    if (expr is ast.UnaryExpression) {
      return ast.UnaryExpression(
        operator: expr.operator,
        operand: _hoist(expr.operand, into),
      );
    }
    if (expr is ast.CastExpression) {
      return ast.CastExpression(
        expression: _hoist(expr.expression, into),
        targetType: expr.targetType,
      );
    }
    if (expr is ast.InExpression) {
      return ast.InExpression(
        expression: _hoist(expr.expression, into),
        values: [for (final v in expr.values) _hoist(v, into)],
        subquery: expr.subquery,
        negated: expr.negated,
      );
    }
    if (expr is ast.CaseExpression) {
      return ast.CaseExpression(
        expression: expr.expression == null
            ? null
            : _hoist(expr.expression!, into),
        whenClauses: [
          for (final w in expr.whenClauses)
            ast.WhenClause(
              condition: _hoist(w.condition, into),
              result: _hoist(w.result, into),
            ),
        ],
        elseExpression: expr.elseExpression == null
            ? null
            : _hoist(expr.elseExpression!, into),
      );
    }
    return expr;
  }

  /// An aggregate in ORDER BY is normally hoisted and carried through the SELECT
  /// projection (see [_carry]) — but under DISTINCT / a set operation that carry
  /// is unavailable, so reject there. Fail loud rather than let the sort
  /// reference an `__agg$i` column no projection emitted.
  void _rejectAggregateOrderBy(List<ast.OrderByItem> orderBy) {
    for (final item in orderBy) {
      if (_containsAggregate(item.expression)) {
        throw UnsupportedError(
          'Aggregate in ORDER BY is not supported with DISTINCT or a set '
          'operation — alias it in the SELECT list and order by the alias '
          '(SELECT SUM(x) AS s … ORDER BY s)',
        );
      }
    }
  }

  /// Build an aggregate node, rewriting a single-distinct-column aggregate into
  /// two stacked aggregates so it spills/scales (see [_tryDistinctRewrite]);
  /// otherwise a plain [logical.Aggregate] whose per-group distinct accumulators
  /// hold in-memory Sets — BOUNDED at execution time by a shared distinct budget
  /// (`EngineOptions.maxOutputRows`) that fails loud past the cap rather than
  /// growing without limit.
  logical.LogicalPlan _buildAggregate(
    logical.LogicalPlan input,
    List<logical.LogicalExpression> groupBy,
    List<logical.AggregateExpression> aggregates,
    List<String> aliases,
  ) {
    return _tryDistinctRewrite(input, groupBy, aggregates, aliases) ??
        _tryMultiDistinctRewrite(input, groupBy, aggregates, aliases) ??
        logical.Aggregate(
          input: input,
          groupBy: groupBy,
          aggregates: aggregates,
          aliases: aliases,
        );
  }

  /// Structural equality over logical expressions (there is no `==`). Used to
  /// decide whether every DISTINCT aggregate is over the SAME expression so the
  /// two-level rewrite can dedup it once. Unrecognized shapes compare unequal, so
  /// a query with them safely falls back to the plain aggregate.
  bool _sameExpr(logical.LogicalExpression a, logical.LogicalExpression b) {
    if (identical(a, b)) return true;
    if (a is logical.ColumnExpression && b is logical.ColumnExpression) {
      return a.name == b.name && a.table == b.table;
    }
    if (a is logical.LiteralExpression && b is logical.LiteralExpression) {
      return a.value == b.value && a.literalType == b.literalType;
    }
    if (a is logical.BinaryExpression && b is logical.BinaryExpression) {
      return a.operator == b.operator &&
          _sameExpr(a.left, b.left) &&
          _sameExpr(a.right, b.right);
    }
    if (a is logical.CastExpression && b is logical.CastExpression) {
      return a.targetType == b.targetType &&
          _sameExpr(a.expression, b.expression);
    }
    if (a is logical.FunctionExpression && b is logical.FunctionExpression) {
      if (a.name != b.name || a.arguments.length != b.arguments.length) {
        return false;
      }
      for (var i = 0; i < a.arguments.length; i++) {
        if (!_sameExpr(a.arguments[i], b.arguments[i])) return false;
      }
      return true;
    }
    if (a is logical.CaseExpression && b is logical.CaseExpression) {
      if (a.whenClauses.length != b.whenClauses.length) return false;
      for (var i = 0; i < a.whenClauses.length; i++) {
        if (!_sameExpr(
              a.whenClauses[i].condition,
              b.whenClauses[i].condition,
            ) ||
            !_sameExpr(a.whenClauses[i].result, b.whenClauses[i].result)) {
          return false;
        }
      }
      final ae = a.elseExpression, be = b.elseExpression;
      if ((ae == null) != (be == null)) return false;
      return ae == null || _sameExpr(ae, be!);
    }
    return false;
  }

  /// `agg(DISTINCT e) … GROUP BY k` → inner `GROUP BY k, e` (distinct the (k,e)
  /// pairs, which spills via the partitioned hash aggregate) then outer
  /// `GROUP BY k` with the now non-distinct `agg(e)`. Removes the per-group
  /// `seen` sets that would otherwise grow unbounded. Applies when every
  /// aggregate is DISTINCT over the SAME single expression [e] (a plain column or
  /// any structurally-equal expression such as `x+1` / `LOWER(name)`), with no
  /// non-distinct aggregates; returns null otherwise (caller falls back or tries
  /// the multi/mixed expansion).
  logical.Aggregate? _tryDistinctRewrite(
    logical.LogicalPlan input,
    List<logical.LogicalExpression> groupBy,
    List<logical.AggregateExpression> aggregates,
    List<String> aliases,
  ) {
    if (aggregates.isEmpty || !aggregates.every((a) => a.distinct)) return null;
    final first = aggregates.first.argument;
    if (first == null) return null; // e.g. COUNT(DISTINCT *) — no argument
    final same = aggregates.every(
      (a) => a.argument != null && _sameExpr(a.argument!, first),
    );
    if (!same) return null;

    final inner = logical.Aggregate(
      input: input,
      groupBy: [...groupBy, first],
      aggregates: const [],
      aliases: const [],
    );
    // Every inner group-by expression is emitted as its own column: a plain
    // column keeps its name; any other expression becomes the positional
    // `group$i` (i = its index in the inner group-by), matching `_groupName`.
    logical.LogicalExpression outerRef(logical.LogicalExpression e, int i) =>
        e is logical.ColumnExpression
        ? e
        : logical.ColumnExpression(name: 'group$i', columnType: e.dataType);

    final outerArg = outerRef(first, groupBy.length);
    final outerAggs = [
      for (final a in aggregates)
        logical.AggregateExpression(
          function: a.function,
          argument: outerArg,
          // distinct: false — the inner already de-duplicated (k, e).
        ),
    ];
    return logical.Aggregate(
      input: inner,
      // The outer reads the INNER'S OUTPUT, so the group keys must be remapped
      // too — not just the distinct argument. Reusing the original expressions
      // here referenced base columns the inner no longer emits, so
      // `GROUP BY LOWER(g)` with an all-DISTINCT aggregate threw
      // "Unknown column: g" (a mixed aggregate bailed out of this rewrite and
      // silently worked, which is why it went unnoticed).
      groupBy: [
        for (int i = 0; i < groupBy.length; i++) outerRef(groupBy[i], i),
      ],
      aggregates: outerAggs,
      aliases: aliases,
    );
  }

  /// Resolve an expression's REAL data type against [schema] (logical column
  /// types are placeholders until binding). Handles the shapes a DISTINCT/agg
  /// argument can take; falls back to the expression's own declared type.
  DataType _resolvedType(logical.LogicalExpression e, Schema schema) {
    if (e is logical.ColumnExpression) {
      final i = schema.resolve(e.name, qualifier: e.table);
      return i >= 0 ? schema.fields[i].dataType : e.dataType;
    }
    if (e is logical.CastExpression) return e.targetType;
    if (e is logical.LiteralExpression) return e.literalType;
    if (e is logical.BinaryExpression) {
      // Type over resolved operands with the SAME rule the executor uses, so a
      // DISTINCT slot's type matches the column it will hold (int arithmetic is
      // int64, division/float is float64) rather than a narrower SQL-promotion.
      return _binaryResultType(
        e.operator,
        _resolvedType(e.left, schema),
        _resolvedType(e.right, schema),
      );
    }
    return e.dataType;
  }

  /// Bound the remaining DISTINCT shapes that [_tryDistinctRewrite] can't (mixed
  /// distinct + non-distinct, or DISTINCT over more than one expression) via
  /// grouping-set expansion: one "lane" per distinct aggregate emits the distinct
  /// `(k, dᵢ)` pairs (deduped by the spilling grouped aggregate); one raw lane
  /// carries all non-distinct aggregates. Each lane fills only its own value
  /// slot(s) and NULLs the rest; the lanes are `UNION ALL`ed (streaming) and a
  /// final `GROUP BY k` reads each aggregate from its own slot. COUNT/SUM/AVG/
  /// MIN/MAX ignore NULL, so slots never cross-contaminate. Unlike a self-join,
  /// the final GROUP BY handles multi-column and NULL group keys natively. All
  /// stages ride the existing bounded operators — no unbounded per-group Set.
  ///
  /// Returns null (→ plain aggregate) unless there is at least one DISTINCT
  /// aggregate and every group key is a plain column.
  logical.LogicalPlan? _tryMultiDistinctRewrite(
    logical.LogicalPlan input,
    List<logical.LogicalExpression> groupBy,
    List<logical.AggregateExpression> aggregates,
    List<String> aliases,
  ) {
    if (!aggregates.any((a) => a.distinct)) return null;
    if (!groupBy.every((g) => g is logical.ColumnExpression)) return null;
    if (aggregates.any((a) => a.distinct && a.argument == null)) return null;

    final keyNames = [
      for (final g in groupBy) (g as logical.ColumnExpression).name,
    ];
    final slotNames = [for (var i = 0; i < aggregates.length; i++) '__d\$$i'];
    // The slot column type must be the REAL type (logical column types are
    // placeholders resolved during binding), so the typed-NULL slots in one lane
    // match the value column in another — otherwise UNION ALL / spill mismatches.
    final schema = input.schema;
    DataType slotType(int i) {
      final arg = aggregates[i].argument;
      return arg == null ? DataType.int64 : _resolvedType(arg, schema);
    }

    // Project [src] to the common wide schema: the group keys, then one slot per
    // aggregate ([populate] gives this lane's value for a slot it owns, else a
    // typed NULL).
    logical.Projection lane(
      logical.LogicalPlan src,
      logical.LogicalExpression? Function(int slot) populate,
    ) {
      return logical.Projection(
        input: src,
        expressions: [
          ...groupBy,
          for (var i = 0; i < aggregates.length; i++)
            populate(i) ?? logical.LiteralExpression(null, slotType(i)),
        ],
        aliases: [...keyNames, ...slotNames],
      );
    }

    final lanes = <logical.LogicalPlan>[];
    // One lane per DISTINCT aggregate: distinct (k, dᵢ) pairs, slot i = dᵢ.
    for (var i = 0; i < aggregates.length; i++) {
      if (!aggregates[i].distinct) continue;
      final d = aggregates[i].argument!;
      final inner = logical.Aggregate(
        input: input,
        groupBy: [...groupBy, d],
        aggregates: const [],
        aliases: const [],
      );
      final dName = d is logical.ColumnExpression
          ? d.name
          : 'group${groupBy.length}';
      final dRef = logical.ColumnExpression(
        name: dName,
        columnType: d.dataType,
      );
      lanes.add(lane(inner, (slot) => slot == i ? dRef : null));
    }
    // One raw lane for ALL non-distinct aggregates (+ COUNT(*) as a literal 1).
    if (aggregates.any((a) => !a.distinct)) {
      lanes.add(
        lane(input, (slot) {
          final a = aggregates[slot];
          if (a.distinct) return null;
          return a.argument ?? logical.LiteralExpression(1, DataType.int64);
        }),
      );
    }

    var union = lanes.first;
    for (var i = 1; i < lanes.length; i++) {
      union = logical.SetOperation(
        left: union,
        right: lanes[i],
        type: logical.SetOpType.union,
        all: true,
      );
    }

    return logical.Aggregate(
      input: union,
      groupBy: groupBy,
      aggregates: [
        for (var i = 0; i < aggregates.length; i++)
          logical.AggregateExpression(
            function: aggregates[i].function,
            argument: logical.ColumnExpression(
              name: slotNames[i],
              columnType: slotType(i),
            ),
          ),
      ],
      aliases: aliases,
    );
  }

  logical.AggregateExpression _convertAggregateExpression(
    ast.FunctionExpression func,
  ) {
    final function = logical.AggregateFunction(
      func.name.toUpperCase(),
      DataType.float64, // Will be refined based on function
    );

    final argument = func.arguments.isNotEmpty
        ? _convertExpression(func.arguments[0])
        : null;

    return logical.AggregateExpression(
      function: function,
      argument: argument,
      distinct: func.distinct,
    );
  }

  // Implement required visitor methods (throw for unsupported)
  @override
  logical.LogicalPlan visitSubqueryTable(ast.SubqueryTable node) {
    // Qualify the derived columns with the subquery's alias so `alias.col` works.
    return logical.AliasScope(
      alias: node.alias,
      input: node.subquery.accept(this),
    );
  }

  @override
  logical.LogicalPlan visitTableFunction(ast.TableFunction node) {
    throw UnsupportedError('Table functions not yet implemented');
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

    // Unify each column's type over ALL rows, not just the first — `VALUES
    // (1), (2.5)` is a float column, and typing it from row 0 alone would feed
    // a double into an int buffer. `commonType` (not `inferredCommon`) keeps
    // VALUES consistent with how set-operation branches unify.
    final fields = <Field>[];
    for (int i = 0; i < arity; i++) {
      DataType? type;
      var nullable = false;
      for (final row in logicalRows) {
        final cell = row[i];
        // A NULL literal carries no usable type — it only forces nullability,
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
      fields.add(
        Field(
          // An all-NULL column has no type to infer; the first cell's inferred
          // type stands in and every value is null anyway.
          name: aliases?[i] ?? 'column${i + 1}',
          dataType: type ?? logicalRows.first[i].dataType,
          nullable: nullable,
        ),
      );
    }

    final values = logical.Values(
      rows: logicalRows,
      valueSchema: Schema(fields: fields),
    );
    // Qualify the columns with the alias so `t.n` resolves — same treatment a
    // subquery table gets.
    final alias = node.alias;
    return alias == null
        ? values
        : logical.AliasScope(alias: alias, input: values);
  }

  @override
  logical.LogicalPlan visitInsertStatement(ast.InsertStatement node) {
    throw UnsupportedError('INSERT not yet implemented');
  }

  @override
  logical.LogicalPlan visitCreateTableStatement(ast.CreateTableStatement node) {
    throw UnsupportedError('CREATE TABLE not yet implemented');
  }

  // Expression visitors (not used in logical plan building)
  @override
  logical.LogicalPlan visitExpression(ast.Expression node) {
    throw UnimplementedError();
  }

  @override
  logical.LogicalPlan visitColumnExpression(ast.ColumnExpression node) {
    throw UnimplementedError();
  }

  @override
  logical.LogicalPlan visitLiteralExpression(ast.LiteralExpression node) {
    throw UnimplementedError();
  }

  @override
  logical.LogicalPlan visitBinaryExpression(ast.BinaryExpression node) {
    throw UnimplementedError();
  }

  @override
  logical.LogicalPlan visitUnaryExpression(ast.UnaryExpression node) {
    throw UnimplementedError();
  }

  @override
  logical.LogicalPlan visitFunctionExpression(ast.FunctionExpression node) {
    throw UnimplementedError();
  }

  @override
  logical.LogicalPlan visitCaseExpression(ast.CaseExpression node) {
    throw UnimplementedError();
  }

  @override
  logical.LogicalPlan visitInExpression(ast.InExpression node) {
    throw UnimplementedError();
  }

  @override
  logical.LogicalPlan visitBetweenExpression(ast.BetweenExpression node) {
    throw UnimplementedError();
  }

  @override
  logical.LogicalPlan visitExistsExpression(ast.ExistsExpression node) {
    throw UnimplementedError();
  }

  @override
  logical.LogicalPlan visitSubqueryExpression(ast.SubqueryExpression node) {
    throw UnimplementedError();
  }

  logical.LogicalPlan visitStarExpression(ast.StarExpression node) {
    throw UnimplementedError();
  }

  logical.LogicalPlan visitQualifiedStarExpression(
    ast.QualifiedStarExpression node,
  ) {
    throw UnimplementedError();
  }

  @override
  logical.LogicalPlan visitCastExpression(ast.CastExpression node) {
    throw UnimplementedError();
  }

  @override
  logical.LogicalPlan visitWindowExpression(ast.WindowExpression node) {
    throw UnimplementedError();
  }

  /// Get a table name from a file path (platform-agnostic: the file's basename
  /// without its extension, so Windows backslash paths derive correctly too).
}
