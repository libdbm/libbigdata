part of 'query_planner.dart';

/// WHERE and IN/EXISTS subquery decorrelation into semi and anti joins.
extension _Subqueries on LogicalPlanBuilder {
  /// Apply WHERE, pulling any top-level `col IN/NOT IN (SELECT …)` conjunct out
  /// as a SEMI / null-aware anti join (the subquery is the build side); the rest
  /// stays one Filter. A semi/anti join emits the left schema, so the plan's
  /// columns are unchanged for everything above.
  logical.LogicalPlan _applyWhere(
    logical.LogicalPlan plan,
    ast.Expression where,
  ) {
    // WHERE filters rows before any grouping, so an aggregate cannot be
    // computed there (it was reported as an unknown function).
    if (_containsAggregate(where)) {
      throw const FormatException(
        'An aggregate function is not allowed in WHERE; use HAVING',
      );
    }
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
  /// is decorrelated, each `inner = outer` equality in the subquery's WHERE
  /// becomes an extra join key, and its inner column is projected out for that
  /// key (see [_correlationEquality]). Returns null when [conjunct] is not such a
  /// predicate (it stays a Filter) or its LHS is not a plain column (the join
  /// needs a column key; that case reaches [_convertExpression], which rejects
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
    // decorrelated anti join decides "build has a null" GLOBALLY, so a null in
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
          sub.operations.isNotEmpty ||
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
    // don't decorrelate: a non-equi (`u.z > t.z`) leaves `correlations` empty
    // and the ref in `local`, so this must run for BOTH branches (and gives a
    // clearer message than strict binding's bare "Unknown column").
    _requireNoOuterRefs(sub, local, innerScope);

    final logical.LogicalPlan subPlan;
    if (correlations.isEmpty) {
      // Uncorrelated: use the ORIGINAL subquery plan UNCHANGED, so GROUP BY /
      // HAVING / LIMIT / set operations are preserved. (Rebuilding a partial AST
      // silently dropped them: a plausible wrong answer.)
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
    // Key 0 is the IN column; keys 1..N are the correlation columns.
    return _subqueryJoin(
      left,
      subPlan,
      [conjunct.expression, for (final c in correlations) c.outer],
      conjunct.negated ? logical.JoinType.antiNullAware : logical.JoinType.semi,
      'in',
    );
  }

  /// Turn a top-level `[NOT] EXISTS (SELECT … WHERE inner = outer …)` conjunct
  /// into a SEMI (EXISTS) / plain anti (NOT EXISTS) join, keyed on the
  /// correlation equalities alone (EXISTS has no probe column: existence is the
  /// whole test). Returns null when [conjunct] is not an EXISTS.
  ///
  /// NOT EXISTS is a PLAIN anti, not null-aware: it is TRUE exactly when the
  /// subquery yields no row, indifferent to nulls (unlike NOT IN). A null
  /// correlation key on the outer side simply doesn't match, so that row is kept,
  /// which a plain anti does.
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

    // An uncorrelated EXISTS is a whole-table "any row?" test, not a key join;
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
        sub.operations.isNotEmpty ||
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
    return _subqueryJoin(
      left,
      subPlan,
      [for (final c in correlations) c.outer],
      negated ? logical.JoinType.anti : logical.JoinType.semi,
      'ex',
    );
  }

  /// The [type] (semi or anti) join of [left] with [subPlan], matching each
  /// [outer] key to the subquery output at the same position. The outputs are
  /// renamed to synthetic keys under a synthetic alias ([prefix] tells IN from
  /// EXISTS), so a key never collides with an outer column of the same name
  /// and the physical builder routes it to this (build) side.
  logical.Join _subqueryJoin(
    logical.LogicalPlan left,
    logical.LogicalPlan subPlan,
    List<ast.Expression> outer,
    logical.JoinType type,
    String prefix,
  ) {
    final fields = subPlan.schema.fields;
    final n = _inSubquery++;
    final alias = '__${prefix}_sub$n';
    final keyNames = [
      for (int i = 0; i < fields.length; i++) '__${prefix}_key${n}_$i',
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
    logical.LogicalExpression equal(int i) => logical.BinaryExpression(
      _convertExpression(outer[i]),
      logical.BinaryOperator.eq,
      logical.ColumnExpression(
        name: keyNames[i],
        table: alias,
        columnType: fields[i].dataType,
      ),
    );
    var condition = equal(0);
    for (int i = 1; i < outer.length; i++) {
      condition = logical.BinaryExpression(
        condition,
        logical.BinaryOperator.and,
        equal(i),
      );
    }
    return logical.Join(
      left: left,
      right: right,
      joinType: type,
      condition: condition,
    );
  }

  /// If [c] is `inner = outer` (or `outer = inner`): a plain column equality
  /// relating one of the subquery's own columns to an OUTER column (a qualifier
  /// not in [innerScope]): return the pair. An unqualified column counts as
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
      out.add(LogicalPlanBuilder._qualifier(ref));
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

  /// Collect the table qualifiers used by column references in [expression],
  /// WITHOUT descending into a nested subquery (a separate scope).
  void _usedQualifiers(ast.Expression expression, Set<String> out) {
    if (expression is ast.ColumnExpression) {
      if (expression.table != null) out.add(expression.table!);
    } else if (expression is ast.BinaryExpression) {
      _usedQualifiers(expression.left, out);
      _usedQualifiers(expression.right, out);
    } else if (expression is ast.UnaryExpression) {
      _usedQualifiers(expression.operand, out);
    } else if (expression is ast.FunctionExpression) {
      for (final a in expression.arguments) {
        _usedQualifiers(a, out);
      }
    } else if (expression is ast.CastExpression) {
      _usedQualifiers(expression.expression, out);
    } else if (expression is ast.CaseExpression) {
      if (expression.expression != null) {
        _usedQualifiers(expression.expression!, out);
      }
      for (final w in expression.whenClauses) {
        _usedQualifiers(w.condition, out);
        _usedQualifiers(w.result, out);
      }
      if (expression.elseExpression != null) {
        _usedQualifiers(expression.elseExpression!, out);
      }
    } else if (expression is ast.InExpression) {
      _usedQualifiers(expression.expression, out);
      for (final v in expression.values) {
        _usedQualifiers(v, out);
      }
      // NOT its subquery: a separate scope.
    }
    // Literals, stars, nested subqueries: no outer refs contributed here.
  }
}
