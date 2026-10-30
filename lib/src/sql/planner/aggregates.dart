part of 'query_planner.dart';

/// The aggregates one SELECT statement computes, in the order first written.
/// The aggregate step emits each as the internal column [aliases]`[i]`, which
/// the SELECT projection and HAVING above it read. Held per statement (never on
/// the builder, which a subquery re-enters): see `_hoist`.
class _Aggregates {
  final expressions = <logical.AggregateExpression>[];
  final aliases = <String>[];
}

/// Aggregation and grouping: group keys, aggregate hoisting, distinct rewrites.
extension _Aggregation on LogicalPlanBuilder {
  /// The OUTPUT column name the aggregate gives group key [g] at position [i]:
  /// a plain column keeps its name; any other expression is `group$i`. MUST
  /// match the physical aggregate's `_groupName`, so a later reference (SELECT
  /// projection, ORDER BY) resolves to the column the aggregate actually emits.
  String _groupKeyName(logical.LogicalExpression g, int i) =>
      g is logical.ColumnExpression ? g.name : 'group$i';

  /// Rewrite [expression] so any sub-expression that IS a GROUP BY key becomes
  /// a reference to that key's output column. After aggregation the base
  /// columns are gone (only the group keys + aggregates remain), so
  /// `SELECT k % 2` / `ORDER BY k % 2` over `GROUP BY k % 2` must reference the
  /// `group0` column rather than re-evaluate `k % 2` against columns that no
  /// longer exist.
  logical.LogicalExpression _rewriteOverAggregate(
    logical.LogicalExpression expression,
    List<logical.LogicalExpression> groupBy,
  ) => _replace(expression, (e) {
    logical.ColumnExpression reference(int i) {
      final key = groupBy[i];
      return logical.ColumnExpression(
        name: _groupKeyName(key, i),
        // The aggregate keeps a key column's qualifier (see _groupName), so
        // `t.id` and `u.id` stay distinct references.
        table: key is logical.ColumnExpression ? key.table : null,
        columnType: key.dataType,
      );
    }

    for (int i = 0; i < groupBy.length; i++) {
      if (_same(e, groupBy[i])) return reference(i);
    }
    // `SELECT t.a ... GROUP BY a` (or the reverse): a column matches the one
    // key column of its name when either side is unqualified.
    if (e is logical.ColumnExpression) {
      final named = [
        for (int i = 0; i < groupBy.length; i++)
          if (groupBy[i] case logical.ColumnExpression(
            :final name,
            :final table,
          ) when name == e.name && (table == null || e.table == null))
            i,
      ];
      if (named.length == 1) return reference(named.single);
    }
    return null;
  });

  /// Reject a column in an aggregate query's output [expression] that is
  /// neither a group key nor an aggregate ([aggregates] are their column
  /// names): it has no single value per group. It was reported as an unknown
  /// column.
  void _requireGrouped(
    logical.LogicalExpression expression,
    List<logical.LogicalExpression> groupBy,
    List<String> aggregates,
  ) {
    final keys = {
      for (int i = 0; i < groupBy.length; i++) _groupKeyName(groupBy[i], i),
    };
    _replace(expression, (e) {
      if (e is logical.ColumnExpression &&
          !keys.contains(e.name) &&
          !aggregates.contains(e.name)) {
        final name = e.table == null ? e.name : '${e.table}.${e.name}';
        throw FormatException(
          'Column "$name" must appear in the GROUP BY clause or be used in an '
          'aggregate function',
        );
      }
      return null;
    });
  }

  /// [expression] with every subexpression [swap] returns a replacement for
  /// replaced, outermost first.
  logical.LogicalExpression _replace(
    logical.LogicalExpression expression,
    logical.LogicalExpression? Function(logical.LogicalExpression) swap,
  ) {
    final swapped = swap(expression);
    if (swapped != null) return swapped;
    logical.LogicalExpression walk(logical.LogicalExpression e) =>
        _replace(e, swap);
    return switch (expression) {
      logical.BinaryExpression() => logical.BinaryExpression(
        walk(expression.left),
        expression.operator,
        walk(expression.right),
      ),
      logical.CastExpression() => logical.CastExpression(
        walk(expression.expression),
        expression.targetType,
        precision: expression.precision,
        scale: expression.scale,
      ),
      logical.FunctionExpression() => logical.FunctionExpression(
        name: expression.name,
        arguments: [for (final a in expression.arguments) walk(a)],
        returnType: expression.returnType,
      ),
      logical.CaseExpression() => logical.CaseExpression(
        whenClauses: [
          for (final w in expression.whenClauses)
            logical.WhenClause(walk(w.condition), walk(w.result)),
        ],
        elseExpression: expression.elseExpression == null
            ? null
            : walk(expression.elseExpression!),
      ),
      _ => expression, // leaf column/literal
    };
  }

  /// If an ORDER BY expression references a column the aggregate step emits: a
  /// GROUP BY key, or a hoisted `__agg$i`; that isn't in the SELECT output
  /// ([have]), carry that column through the projection so the sort (which runs
  /// above it) can see it; it is trimmed back out after the sort.
  ///
  /// Only ever carries columns that step actually emits, so it cannot invent a
  /// reference. [ordering] must already be hoisted (see [_hoist]).
  void _carry(
    ast.Expression ordering,
    List<logical.LogicalExpression> groupBy,
    List<String> aggregates,
    Set<String> have,
    List<logical.LogicalExpression> extras,
    List<String> extraAliases,
  ) {
    final rewritten = _rewriteOverAggregate(
      _convertExpression(ordering),
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
        extras.add(
          logical.ColumnExpression(name: name, columnType: DataType.string),
        );
        extraAliases.add(name);
      }
    }
  }

  void _collectColumnNames(
    logical.LogicalExpression expression,
    Set<String> out,
  ) {
    if (expression is logical.ColumnExpression) {
      out.add(expression.name);
    } else if (expression is logical.BinaryExpression) {
      _collectColumnNames(expression.left, out);
      _collectColumnNames(expression.right, out);
    } else if (expression is logical.CastExpression) {
      _collectColumnNames(expression.expression, out);
    } else if (expression is logical.FunctionExpression) {
      for (final a in expression.arguments) {
        _collectColumnNames(a, out);
      }
    } else if (expression is logical.CaseExpression) {
      for (final w in expression.whenClauses) {
        _collectColumnNames(w.condition, out);
        _collectColumnNames(w.result, out);
      }
      if (expression.elseExpression != null) {
        _collectColumnNames(expression.elseExpression!, out);
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

  /// Whether [expression] contains an aggregate ANYWHERE (recursively). Decides
  /// whether the statement aggregates at all, and controls the [_hoist] walk:
  /// the two MUST cover the same node shapes.
  bool _containsAggregate(ast.Expression expression) {
    if (expression is ast.FunctionExpression) {
      if (_isAggregateFunction(expression.name)) return true;
      return expression.arguments.any(_containsAggregate);
    }
    if (expression is ast.BinaryExpression) {
      return _containsAggregate(expression.left) ||
          _containsAggregate(expression.right);
    }
    if (expression is ast.UnaryExpression) {
      return _containsAggregate(expression.operand);
    }
    if (expression is ast.CastExpression) {
      return _containsAggregate(expression.expression);
    }
    if (expression is ast.InExpression) {
      return _containsAggregate(expression.expression) ||
          expression.values.any(_containsAggregate);
    }
    if (expression is ast.CaseExpression) {
      if (expression.expression != null &&
          _containsAggregate(expression.expression!)) {
        return true;
      }
      if (expression.elseExpression != null &&
          _containsAggregate(expression.elseExpression!)) {
        return true;
      }
      return expression.whenClauses.any(
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
  /// `COUNT(DISTINCT x)`: a HAVING then filtered on the wrong aggregate.
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
  ) => (a == null || b == null) ? a == b : _same(a, b);

  /// Replace every aggregate call in [expression] with a reference to the
  /// column the aggregate step emits for it, registering each in [into] on
  /// first sight, so an aggregate may be written anywhere in an expression
  /// (`SUM(x)+1`, `ROUND(AVG(x),2)`, `CASE WHEN SUM(x)>5 …`) rather than only
  /// as a whole SELECT item.
  ///
  /// Returns an AST rather than a logical expression so everything AROUND the
  /// aggregate still goes through [_convertExpression]: operator mapping, unary
  /// folding and NOT desugaring live there and must not be reimplemented here. A
  /// subtree holding no aggregate is returned as-is, so an aggregate-free
  /// expression plans exactly as it did before.
  ///
  /// The shapes walked here MUST match [_containsAggregate]: one checked there
  /// but missed here plans the aggregate as a scalar function.
  ast.Expression _hoist(ast.Expression expression, _Aggregates into) {
    if (!_containsAggregate(expression)) return expression;
    if (expression is ast.FunctionExpression) {
      if (_isAggregateFunction(expression.name)) {
        return ast.ColumnExpression(
          column: _bind(_convertAggregateExpression(expression), into),
        );
      }
      return ast.FunctionExpression(
        name: expression.name,
        arguments: [for (final a in expression.arguments) _hoist(a, into)],
        distinct: expression.distinct,
        filter: expression.filter,
      );
    }
    if (expression is ast.BinaryExpression) {
      return ast.BinaryExpression(
        left: _hoist(expression.left, into),
        operator: expression.operator,
        right: _hoist(expression.right, into),
      );
    }
    if (expression is ast.UnaryExpression) {
      return ast.UnaryExpression(
        operator: expression.operator,
        operand: _hoist(expression.operand, into),
      );
    }
    if (expression is ast.CastExpression) {
      return ast.CastExpression(
        expression: _hoist(expression.expression, into),
        targetType: expression.targetType,
      );
    }
    if (expression is ast.InExpression) {
      return ast.InExpression(
        expression: _hoist(expression.expression, into),
        values: [for (final v in expression.values) _hoist(v, into)],
        subquery: expression.subquery,
        negated: expression.negated,
      );
    }
    if (expression is ast.CaseExpression) {
      return ast.CaseExpression(
        expression: expression.expression == null
            ? null
            : _hoist(expression.expression!, into),
        whenClauses: [
          for (final w in expression.whenClauses)
            ast.WhenClause(
              condition: _hoist(w.condition, into),
              result: _hoist(w.result, into),
            ),
        ],
        elseExpression: expression.elseExpression == null
            ? null
            : _hoist(expression.elseExpression!, into),
      );
    }
    return expression;
  }

  /// An aggregate in ORDER BY is normally hoisted and carried through the SELECT
  /// projection (see [_carry]), but under DISTINCT / a set operation that carry
  /// is unavailable, so reject there. Fail loud rather than let the sort
  /// reference an `__agg$i` column no projection emitted.
  void _rejectAggregateOrderBy(List<ast.OrderByItem> orderBy) {
    for (final item in orderBy) {
      if (_containsAggregate(item.expression)) {
        throw UnsupportedError(
          'Aggregate in ORDER BY is not supported with DISTINCT or a set '
          'operation: alias it in the SELECT list and order by the alias '
          '(SELECT SUM(x) AS s … ORDER BY s)',
        );
      }
    }
  }

  /// Build an aggregate node, rewriting a single-distinct-column aggregate into
  /// two stacked aggregates so it spills/scales (see [_tryDistinctRewrite]);
  /// otherwise a plain [logical.Aggregate] whose per-group distinct accumulators
  /// hold in-memory Sets: BOUNDED at execution time by a shared distinct budget
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
  bool _same(logical.LogicalExpression a, logical.LogicalExpression b) {
    if (identical(a, b)) return true;
    if (a is logical.ColumnExpression && b is logical.ColumnExpression) {
      return a.name == b.name && a.table == b.table;
    }
    if (a is logical.LiteralExpression && b is logical.LiteralExpression) {
      return a.value == b.value && a.literalType == b.literalType;
    }
    if (a is logical.BinaryExpression && b is logical.BinaryExpression) {
      return a.operator == b.operator &&
          _same(a.left, b.left) &&
          _same(a.right, b.right);
    }
    if (a is logical.CastExpression && b is logical.CastExpression) {
      return a.targetType == b.targetType && _same(a.expression, b.expression);
    }
    if (a is logical.FunctionExpression && b is logical.FunctionExpression) {
      // SQL function names are case-insensitive: `UPPER(x)` is `upper(x)`.
      if (a.name.toUpperCase() != b.name.toUpperCase() ||
          a.arguments.length != b.arguments.length) {
        return false;
      }
      for (var i = 0; i < a.arguments.length; i++) {
        if (!_same(a.arguments[i], b.arguments[i])) return false;
      }
      return true;
    }
    if (a is logical.CaseExpression && b is logical.CaseExpression) {
      if (a.whenClauses.length != b.whenClauses.length) return false;
      for (var i = 0; i < a.whenClauses.length; i++) {
        if (!_same(a.whenClauses[i].condition, b.whenClauses[i].condition) ||
            !_same(a.whenClauses[i].result, b.whenClauses[i].result)) {
          return false;
        }
      }
      final left = a.elseExpression, right = b.elseExpression;
      if ((left == null) != (right == null)) return false;
      return left == null || _same(left, right!);
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
    if (first == null) return null; // e.g. COUNT(DISTINCT *): no argument
    final same = aggregates.every(
      (a) => a.argument != null && _same(a.argument!, first),
    );
    if (!same) return null;
    // DISTINCT over a group key has one value per group, so the plain
    // aggregate is already bounded; grouping by the key twice duplicated it.
    if (groupBy.any((g) => _same(g, first))) return null;

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
          // distinct: false, since the inner already de-duplicated (k, e).
        ),
    ];
    return logical.Aggregate(
      input: inner,
      // The outer reads the INNER'S OUTPUT, so the group keys must be remapped
      // too, not just the distinct argument. Reusing the original expressions
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
  /// stages ride the existing bounded operators: no unbounded per-group Set.
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
    // As in _tryDistinctRewrite: a DISTINCT over a group key stays plain.
    if (aggregates.any(
      (a) => a.distinct && groupBy.any((g) => _same(g, a.argument!)),
    )) {
      return null;
    }

    final keyNames = [
      for (final g in groupBy) (g as logical.ColumnExpression).name,
    ];
    final slotNames = [for (var i = 0; i < aggregates.length; i++) '__d\$$i'];
    // The slot column type must be the REAL type (logical column types are
    // placeholders resolved during binding), so the typed-NULL slots in one lane
    // match the value column in another; otherwise UNION ALL / spill mismatches.
    final schema = input.schema;
    DataType slotType(int i) {
      final arg = aggregates[i].argument;
      return arg == null ? DataType.int64 : _resolvedType(arg, schema);
    }

    // Project [source] to the common wide schema: the group keys, then one slot
    // per aggregate ([populate] gives this lane's value for a slot it owns,
    // else a typed NULL).
    logical.Projection lane(
      logical.LogicalPlan source,
      logical.LogicalExpression? Function(int slot) populate,
    ) {
      return logical.Projection(
        input: source,
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
      final column = d is logical.ColumnExpression
          ? d.name
          : 'group${groupBy.length}';
      final reference = logical.ColumnExpression(
        name: column,
        columnType: d.dataType,
      );
      lanes.add(lane(inner, (slot) => slot == i ? reference : null));
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
    ast.FunctionExpression call,
  ) {
    final name = call.name.toUpperCase();
    final function = logical.AggregateFunction(
      name,
      DataType.float64, // Will be refined based on function
    );
    // COUNT takes `*` (no argument) or one; every other aggregate takes one.
    final count = call.arguments.length;
    if (count > 1 || (count == 0 && name != 'COUNT')) {
      throw Failure.of(Kind.invalid, '$name takes one argument, not $count');
    }

    final argument = call.arguments.isNotEmpty
        ? _convertExpression(call.arguments[0])
        : null;

    return logical.AggregateExpression(
      function: function,
      argument: argument,
      distinct: call.distinct,
    );
  }
}
