part of 'query_planner.dart';

/// Builds physical plan from logical plan
class PhysicalPlanBuilder extends logical.LogicalPlanVisitor<PhysicalPlan> {
  final TableCatalog catalog;
  final EngineOptions options;

  PhysicalPlanBuilder(this.catalog, [this.options = const EngineOptions()]);

  @override
  PhysicalPlan visitTableScan(logical.TableScan node) {
    // A lazy file source streams from disk instead of a materialized Table,
    // forwarding any pushed-down row-group pruning filter to the reader.
    final source = catalog.source(node.tableName);
    if (source != null) {
      return PhysicalFileScan(
        path: source.path,
        schema: source.schema,
        filter: node.pushedFilter ?? source.filter,
        projection: node.projection,
        alias: node.alias,
      );
    }

    final table = catalog.table(node.tableName);
    if (table == null) {
      throw StateError('Table not found: ${node.tableName}');
    }

    return PhysicalTableScan(
      tableName: node.tableName,
      table: table,
      projection: node.projection,
      alias: node.alias,
    );
  }

  @override
  PhysicalPlan visitAliasScope(logical.AliasScope node) {
    return PhysicalAlias(input: node.input.accept(this), alias: node.alias);
  }

  @override
  PhysicalPlan visitProjection(logical.Projection node) {
    final input = node.input.accept(this);
    final expressions = node.expressions
        .map((e) => _convertToPhysicalExpression(e, inputSchema: input.schema))
        .toList();
    for (final e in expressions) {
      _validateRefs(e, input.schema);
    }

    return PhysicalProjection(
      input: input,
      expressions: expressions,
      aliases: node.aliases,
    );
  }

  @override
  PhysicalPlan visitFilter(logical.Filter node) {
    final input = node.input.accept(this);
    final predicate = _convertToPhysicalExpression(
      node.predicate,
      inputSchema: input.schema,
    );
    _validateRefs(predicate, input.schema);

    return PhysicalFilter(input: input, predicate: predicate);
  }

  /// Bind step: every column an expression references must exist in [input], so
  /// a mistyped column fails loud at plan time instead of silently becoming null
  /// (a wrong-result path). Existence-only — types are already resolved against
  /// [input] during conversion; [Schema.resolve] throws on an ambiguous bare name.
  void _validateRefs(PhysicalExpression expr, Schema input) {
    // A lazy/file scan may not have resolved its schema at plan time (it is read
    // from the footer at execution); skip binding then and let the runtime
    // backstop catch a genuine miss.
    if (input.fields.isEmpty) return;
    if (expr is PhysicalColumnExpression) {
      // Strict: a qualifier the plan-time schema doesn't carry is a typo'd alias,
      // not a fall-back to a bare column of the same name.
      if (input.resolve(
            expr.columnName,
            qualifier: expr.qualifier,
            strict: true,
          ) <
          0) {
        final q = expr.qualifier == null ? '' : '${expr.qualifier}.';
        throw FormatException('Unknown column: $q${expr.columnName}');
      }
    } else if (expr is PhysicalBinaryExpression) {
      _validateRefs(expr.left, input);
      _validateRefs(expr.right, input);
    } else if (expr is PhysicalFunctionExpression) {
      for (final a in expr.args) {
        _validateRefs(a, input);
      }
    } else if (expr is PhysicalCaseExpression) {
      for (final w in expr.whens) {
        _validateRefs(w.condition, input);
        _validateRefs(w.result, input);
      }
      if (expr.elseExpr != null) _validateRefs(expr.elseExpr!, input);
    } else if (expr is PhysicalCastExpression) {
      _validateRefs(expr.expr, input);
    }
    // Literal and any other leaf: nothing to validate.
  }

  @override
  PhysicalPlan visitJoin(logical.Join node) {
    final left = node.left.accept(this);
    final right = node.right.accept(this);

    // USING: the same column name on both sides. Null qualifiers coalesce each
    // shared key to one output column (see PhysicalHashJoin.dropIndices).
    if (node.usingColumns != null) {
      final columns = node.usingColumns!;
      return PhysicalHashJoin(
        left: left,
        right: right,
        leftKeys: columns,
        rightKeys: columns,
        joinType: node.joinType,
        options: options,
      );
    }

    // A semi/anti join FILTERS the left side, so it must never fall into the
    // cross-product branch below — that rewrites the join type to `cross` and
    // would MULTIPLY rows instead. An unconditional one (uncorrelated EXISTS) is
    // a row-count test, not a key join, and has no operator yet.
    if (logical.semiLike(node.joinType) && node.condition == null) {
      throw UnsupportedError(
        'An unconditional ${node.joinType.name} join is not supported',
      );
    }

    // Cross product (explicit CROSS or a join with no condition).
    if (node.joinType == logical.JoinType.cross || node.condition == null) {
      return PhysicalHashJoin(
        left: left,
        right: right,
        leftKeys: const [],
        rightKeys: const [],
        joinType: logical.JoinType.cross,
        options: options,
      );
    }

    // Equi-join: split the ON condition into ANDed `col = col` equalities, and
    // assign each to the side that owns it (don't trust the written order), so
    // `a.id = b.id` and `b.id = a.id` both work. Anything but a conjunction of
    // column equalities relating the two inputs fails loud — a hash join has no
    // residual-predicate path.
    final la = _aliases(node.left);
    // Bare column names available on each side (walk scans directly; the logical
    // Join.schema would collide on shared names since it is unqualified).
    final ln = _names(node.left);
    final rn = _names(node.right);
    bool onLeft(logical.ColumnExpression c) => c.table != null
        ? la.contains(c.table)
        : ln.contains(c.name) && !rn.contains(c.name);
    bool onRight(logical.ColumnExpression c) => c.table != null
        ? !la.contains(c.table)
        : rn.contains(c.name) && !ln.contains(c.name);

    final leftKeys = <String>[];
    final rightKeys = <String>[];
    final leftQualifiers = <String?>[];
    final rightQualifiers = <String?>[];
    for (final conjunct in _joinConjuncts(node.condition!)) {
      if (conjunct is! logical.BinaryExpression ||
          conjunct.operator != logical.BinaryOperator.eq ||
          conjunct.left is! logical.ColumnExpression ||
          conjunct.right is! logical.ColumnExpression) {
        throw UnsupportedError('Complex join conditions not yet supported');
      }
      final c1 = conjunct.left as logical.ColumnExpression;
      final c2 = conjunct.right as logical.ColumnExpression;
      final logical.ColumnExpression lc;
      final logical.ColumnExpression rc;
      if (onLeft(c1) && onRight(c2)) {
        lc = c1;
        rc = c2;
      } else if (onLeft(c2) && onRight(c1)) {
        lc = c2;
        rc = c1;
      } else {
        throw StateError(
          'Join condition does not relate the two inputs (or is ambiguous)',
        );
      }
      leftKeys.add(lc.name);
      leftQualifiers.add(lc.table);
      rightKeys.add(rc.name);
      rightQualifiers.add(rc.table);
    }
    return PhysicalHashJoin(
      left: left,
      right: right,
      leftKeys: leftKeys,
      rightKeys: rightKeys,
      leftQualifiers: leftQualifiers,
      rightQualifiers: rightQualifiers,
      joinType: node.joinType,
      options: options,
    );
  }

  /// Flatten a join ON condition into its ANDed conjuncts, so `a = b AND c = d`
  /// becomes two equi-keys. (Mirrors PushDownFilter's splitter; a join must fail
  /// loud on a non-equality conjunct, where pushdown would merely skip it.)
  List<logical.LogicalExpression> _joinConjuncts(logical.LogicalExpression e) {
    if (e is logical.BinaryExpression &&
        e.operator == logical.BinaryOperator.and) {
      return [..._joinConjuncts(e.left), ..._joinConjuncts(e.right)];
    }
    return [e];
  }

  /// Table aliases exposed by a logical subtree (used to route ON-condition
  /// columns to the correct join input).
  Set<String> _aliases(logical.LogicalPlan node) {
    final out = <String>{};
    void walk(logical.LogicalPlan p) {
      // A derived relation is an opaque scope — contribute its own alias, don't
      // descend into the inner scans' aliases.
      if (p is logical.AliasScope) {
        out.add(p.alias);
        return;
      }
      if (p is logical.TableScan && p.alias != null) out.add(p.alias!);
      for (final c in p.children) {
        walk(c);
      }
    }

    walk(node);
    return out;
  }

  /// Bare column names exposed by a logical subtree (its scans / derived relations).
  Set<String> _names(logical.LogicalPlan node) {
    final out = <String>{};
    void walk(logical.LogicalPlan p) {
      if (p is logical.AliasScope) {
        for (final f in p.schema.fields) {
          out.add(f.name);
        }
        return;
      }
      if (p is logical.TableScan) {
        for (final f in p.tableSchema.fields) {
          out.add(f.name);
        }
      }
      for (final c in p.children) {
        walk(c);
      }
    }

    walk(node);
    return out;
  }

  @override
  PhysicalPlan visitAggregate(logical.Aggregate node) {
    final input = node.input.accept(this);

    // Convert group by expressions
    final groupBy = node.groupBy
        .map((e) => _convertToPhysicalExpression(e, inputSchema: input.schema))
        .toList();

    // Convert aggregate expressions
    final aggregates = <PhysicalAggregateExpression>[];
    for (final agg in node.aggregates) {
      final physicalAgg = _convertAggregateExpression(agg, input.schema);
      aggregates.add(physicalAgg);
    }

    return PhysicalAggregate(
      input: input,
      groupBy: groupBy,
      aggregates: aggregates,
      aliases: node.aliases,
      options: options,
    );
  }

  @override
  PhysicalPlan visitSort(logical.Sort node) {
    final input = node.input.accept(this);

    final sortExpressions = node.sortExpressions
        .map(
          (sortExpr) => SortExpression(
            expression: _convertToPhysicalExpression(
              sortExpr.expression,
              inputSchema: input.schema,
            ),
            ascending: sortExpr.ascending,
            nullsFirst: sortExpr.nullsFirst,
          ),
        )
        .toList();

    return PhysicalSort(
      input: input,
      sortExpressions: sortExpressions,
      options: options,
    );
  }

  @override
  PhysicalPlan visitLimit(logical.Limit node) {
    final input = node.input.accept(this);

    return PhysicalLimit(input: input, limit: node.limit, offset: node.offset);
  }

  @override
  PhysicalPlan visitDistinct(logical.Distinct node) {
    // Implement as GROUP BY all columns. Use the physical input schema (real
    // types) rather than the logical node schema (which can mislabel numerics as
    // string), so the group keys — and the spill they may drive — are typed
    // correctly.
    final input = node.input.accept(this);
    final schema = input.schema;

    final groupBy = <PhysicalExpression>[];
    for (final field in schema.fields) {
      groupBy.add(
        PhysicalColumnExpression(
          field.name,
          field.dataType,
          qualifier: field.qualifier,
          precision: field.precision,
          scale: field.scale,
        ),
      );
    }

    return PhysicalAggregate(
      input: input,
      groupBy: groupBy,
      aggregates: [],
      aliases: [],
      options: options,
    );
  }

  // Helper methods
  /// Convert a logical expression to physical, resolving column/expression types
  /// against [inputSchema] (the child plan's real-typed schema) so the output
  /// schema matches the columns `evalColumn` produces — instead of the
  /// `DataType.string` placeholder logical column refs carry. A null/empty
  /// [inputSchema] (e.g. a not-yet-read lazy scan) keeps the placeholder.
  PhysicalExpression _convertToPhysicalExpression(
    logical.LogicalExpression expr, {
    Schema? inputSchema,
  }) {
    if (expr is logical.ColumnExpression) {
      var type = expr.dataType;
      int? precision;
      int? scale;
      var children = const <Field>[];
      if (inputSchema != null && inputSchema.fields.isNotEmpty) {
        final i = inputSchema.resolve(expr.name, qualifier: expr.table);
        if (i >= 0) {
          final f = inputSchema.fields[i];
          type = f.dataType;
          precision = f.precision;
          scale = f.scale; // thread decimal scale for scaled arithmetic
          children = f.children; // and nested shape for spillability
        }
      }
      return PhysicalColumnExpression(
        expr.name,
        type,
        qualifier: expr.table,
        precision: precision,
        scale: scale,
        children: children,
      );
    } else if (expr is logical.LiteralExpression) {
      return PhysicalLiteralExpression(expr.value, expr.dataType);
    } else if (expr is logical.InExpression) {
      return PhysicalInExpression(
        value: _convertToPhysicalExpression(
          expr.value,
          inputSchema: inputSchema,
        ),
        list: [
          for (final element in expr.list)
            _convertToPhysicalExpression(element, inputSchema: inputSchema),
        ],
        negated: expr.negated,
      );
    } else if (expr is logical.BinaryExpression) {
      final left = _convertToPhysicalExpression(
        expr.left,
        inputSchema: inputSchema,
      );
      final right = _convertToPhysicalExpression(
        expr.right,
        inputSchema: inputSchema,
      );
      return PhysicalBinaryExpression(
        left: left,
        operator: expr.operator,
        right: right,
        resultType: _binaryResultType(
          expr.operator,
          left.dataType,
          right.dataType,
        ),
      );
    } else if (expr is logical.FunctionExpression) {
      return PhysicalFunctionExpression(
        expr.name,
        expr.arguments
            .map(
              (a) => _convertToPhysicalExpression(a, inputSchema: inputSchema),
            )
            .toList(),
      );
    } else if (expr is logical.CaseExpression) {
      final whens = [
        for (final w in expr.whenClauses)
          PhysicalWhen(
            _convertToPhysicalExpression(w.condition, inputSchema: inputSchema),
            _convertToPhysicalExpression(w.result, inputSchema: inputSchema),
          ),
      ];
      final elseExpr = expr.elseExpression == null
          ? null
          : _convertToPhysicalExpression(
              expr.elseExpression!,
              inputSchema: inputSchema,
            );
      // The base evalColumn stamps this type onto the produced column, so type
      // it as the common type across the resolved branch results (not the
      // placeholder the first branch's logical column carried).
      var caseType = whens.first.result.dataType;
      for (final w in whens.skip(1)) {
        caseType = DataType.commonType(caseType, w.result.dataType) ?? caseType;
      }
      if (elseExpr != null) {
        caseType = DataType.commonType(caseType, elseExpr.dataType) ?? caseType;
      }
      return PhysicalCaseExpression(whens, elseExpr, caseType);
    } else if (expr is logical.CastExpression) {
      return PhysicalCastExpression(
        _convertToPhysicalExpression(expr.expression, inputSchema: inputSchema),
        expr.targetType,
        targetPrecision: expr.precision,
        targetScale: expr.scale,
      );
    }

    // Fail closed rather than degrading an unsupported expression to NULL.
    throw UnsupportedError(
      'Unsupported expression in physical plan: ${expr.runtimeType}',
    );
  }

  PhysicalAggregateExpression _convertAggregateExpression(
    logical.AggregateExpression agg, [
    Schema? inputSchema,
  ]) {
    // Schema-aware conversion resolves the argument's real type against the
    // input schema (e.g. SUM/MIN/MAX over an int column gets int64, not the
    // placeholder — SUM then accumulates exactly as a decimal).
    final argument = agg.argument != null
        ? _convertToPhysicalExpression(agg.argument!, inputSchema: inputSchema)
        : null;

    switch (agg.function.name.toUpperCase()) {
      case 'COUNT':
        return PhysicalCount(argument: argument, distinct: agg.distinct);
      case 'SUM':
        return PhysicalSum(argument: argument!, distinct: agg.distinct);
      case 'AVG':
        return PhysicalAvg(argument: argument!, distinct: agg.distinct);
      case 'MIN':
        return PhysicalMin(argument: argument!);
      case 'MAX':
        return PhysicalMax(argument: argument!);
      default:
        throw UnsupportedError(
          'Aggregate function ${agg.function.name} not supported',
        );
    }
  }

  @override
  PhysicalPlan visitSetOperation(logical.SetOperation node) {
    final left = node.left.accept(this);
    final right = node.right.accept(this);
    // Reconcile the branches on their REAL physical types (logical column refs
    // are `string` placeholders, so type reconciliation can't happen at logical
    // binding). Per position take commonType; the branches are coerced to this
    // shared schema at the iterator boundary so int-width / int-float unions
    // promote correctly instead of crashing at concat/validate.
    final lf = left.schema.fields;
    final rf = right.schema.fields;
    if (lf.length != rf.length) {
      throw ArgumentError(
        'Set operation branches have different column counts '
        '(${lf.length} vs ${rf.length})',
      );
    }
    final fields = <Field>[];
    for (int i = 0; i < lf.length; i++) {
      final ct = DataType.commonType(lf[i].dataType, rf[i].dataType);
      if (ct == null) {
        throw ArgumentError(
          'Set operation column ${i + 1} has incompatible types '
          '(${lf[i].dataType} vs ${rf[i].dataType})',
        );
      }
      // A decimal's scale lives on the FIELD, not the DataType, so the branches
      // can share `decimal` yet store the SAME value as different unscaled
      // BigInts (1.5 is 15@scale1 but 150@scale2). Reconcile to the WIDEST scale
      // so the iterator restates each branch to one representation; the value
      // then concats, dedups, and compares correctly. Widest so neither narrows.
      final scale = ct == DataType.decimal ? _widestScale(lf[i], rf[i]) : null;
      fields.add(
        Field(
          name: lf[i].name,
          dataType: ct,
          nullable: lf[i].nullable || rf[i].nullable,
          precision: ct == DataType.decimal
              ? _reconciledPrecision(lf[i], rf[i], scale)
              : null,
          scale: scale,
          // A nested column's shape must survive reconciliation — the set-op
          // spill writer rejects a children-less list/struct/map field.
          children: lf[i].children.isNotEmpty ? lf[i].children : rf[i].children,
        ),
      );
    }
    return PhysicalSetOperation(
      left: left,
      right: right,
      type: node.type,
      all: node.all,
      options: options,
      schema: Schema(fields: fields),
      leftSchema: left.schema,
      rightSchema: right.schema,
    );
  }

  /// The scale a decimal set-op column must be restated at: the wider of the two
  /// branches, so neither narrows and loses digits. An INTEGER branch (e.g.
  /// `int_col UNION decimal_col`) contributes scale 0 — its natural scale, into
  /// which it restates losslessly — so the result keeps the decimal side's scale
  /// instead of collapsing to null (which made `5` and `0.05` both key as
  /// `BigInt(5)` and silently dedup). Null only when a branch is neither decimal
  /// nor integer (e.g. float — its decimal cast fails loud elsewhere).
  int? _widestScale(Field left, Field right) {
    final ls = _branchScale(left);
    final rs = _branchScale(right);
    if (ls == null || rs == null) return null;
    return ls > rs ? ls : rs;
  }

  int? _branchScale(Field f) => f.dataType == DataType.decimal
      ? f.scale
      : (f.dataType.isInteger ? 0 : null);

  /// The precision a decimal set-op column needs once both branches are restated
  /// at [resultScale]: the widest INTEGER-digit count plus the result scale —
  /// `decimal(3,1)` ∪ `decimal(3,2)` is `decimal(4,2)`, not `decimal(3,2)`, or the
  /// declared metadata can't represent the values (an invalid Parquet/Arrow
  /// contract). An integer branch contributes its type's max decimal width.
  int? _reconciledPrecision(Field left, Field right, int? resultScale) {
    if (resultScale == null) return null;
    final li = _branchIntDigits(left);
    final ri = _branchIntDigits(right);
    if (li == null || ri == null) return null;
    return (li > ri ? li : ri) + resultScale;
  }

  int? _branchIntDigits(Field f) {
    if (f.dataType == DataType.decimal) {
      if (f.precision == null || f.scale == null) return null;
      return f.precision! - f.scale!;
    }
    return switch (f.dataType) {
      DataType.int8 => 3,
      DataType.int16 => 5,
      DataType.int32 => 10,
      DataType.int64 => 19,
      _ => null,
    };
  }

  // Unimplemented visitors

  @override
  PhysicalPlan visitWindow(logical.Window node) {
    throw UnsupportedError('Window functions not yet implemented');
  }

  @override
  PhysicalPlan visitValues(logical.Values node) => PhysicalValues(
    rows: [
      for (final row in node.rows)
        [
          for (final cell in row)
            _convertToPhysicalExpression(cell, inputSchema: node.valueSchema),
        ],
    ],
    valueSchema: node.valueSchema,
  );
}
