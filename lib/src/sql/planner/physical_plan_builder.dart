part of 'query_planner.dart';

/// Builds physical plan from logical plan
class PhysicalPlanBuilder extends logical.LogicalPlanVisitor<PhysicalPlan> {
  final TableCatalog catalog;
  final EngineOptions options;

  PhysicalPlanBuilder(this.catalog, [this.options = const EngineOptions()]);

  /// Largest LIMIT plus OFFSET a sort is replaced by a top-N for.
  static const topLimit = 10000;

  /// The sort a LIMIT above it reads only the first [_topCount] rows of, so
  /// [visitSort] plans it as a top-N.
  logical.Sort? _topSort;
  int _topCount = 0;

  @override
  PhysicalPlan visitTableScan(logical.TableScan node) {
    // A lazy file source streams from disk instead of a materialized Table,
    // forwarding any pushed-down row-group pruning filter to the reader.
    final source = catalog.source(node.tableName);
    if (source != null) {
      return PhysicalFileScan(
        path: source.path,
        schema: source.schema,
        filter: node.pushedFilter,
        projection: node.projection,
        alias: node.alias,
        open: source.open,
      );
    }

    final table = catalog.table(node.tableName);
    if (table == null) {
      throw Failure.of(Kind.missing, 'Table not found: ${node.tableName}');
    }

    return PhysicalTableScan(
      tableName: node.tableName,
      table: table,
      projection: node.projection,
      alias: node.alias,
    );
  }

  /// [plan] with the columns that carry no qualifier qualified by [name], or
  /// [plan] itself when [name] is null (see [logical.Join.sides]).
  PhysicalPlan _side(PhysicalPlan plan, String? name) =>
      name == null ? plan : PhysicalAlias(input: plan, alias: name, keep: true);

  @override
  PhysicalPlan visitAliasScope(logical.AliasScope node) {
    // A registered frame's scans name entries in the frame's own catalog.
    final builder = switch (node) {
      logical.Frame(:final catalog) => PhysicalPlanBuilder(catalog, options),
      _ => this,
    };
    return PhysicalAlias(input: node.input.accept(builder), alias: node.alias);
  }

  @override
  PhysicalPlan visitProjection(logical.Projection node) {
    final input = node.input.accept(this);
    final expressions = node.expressions
        .map((e) => _convertToPhysicalExpression(e, inputSchema: input.schema))
        .toList();
    for (final e in expressions) {
      _validate(e, input.schema);
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
    _validate(predicate, input.schema);

    return PhysicalFilter(input: input, predicate: condition(predicate));
  }

  /// Bind step: every column an expression references must exist in [input], so
  /// a mistyped column fails loud at plan time instead of silently becoming null
  /// (a wrong-result path). Existence-only: types are already resolved against
  /// [input] during conversion; [Schema.resolve] throws on an ambiguous bare name.
  void _validate(PhysicalExpression expression, Schema input) {
    // A lazy/file scan may not have resolved its schema at plan time (it is read
    // from the footer at execution); skip binding then and let the runtime
    // backstop catch a genuine miss.
    if (input.fields.isEmpty) return;
    if (expression is PhysicalColumnExpression) {
      // Strict: a qualifier the plan-time schema doesn't carry is a typo'd alias,
      // not a fall-back to a bare column of the same name.
      if (input.resolve(
            expression.columnName,
            qualifier: expression.qualifier,
            strict: true,
          ) <
          0) {
        final name = expression.columnName;
        if (expression.qualifier != null) {
          throw FormatException(
            'Unknown column: ${expression.qualifier}.$name',
          );
        }
        // "..." quotes an identifier, so a string written in double quotes
        // lands here.
        throw FormatException(
          'Unknown column: $name (for the string \'$name\', use single quotes)',
        );
      }
    } else if (expression is PhysicalBinaryExpression) {
      _validate(expression.left, input);
      _validate(expression.right, input);
    } else if (expression is PhysicalFunctionExpression) {
      for (final a in expression.arguments) {
        _validate(a, input);
      }
    } else if (expression is PhysicalCaseExpression) {
      for (final w in expression.whens) {
        _validate(w.condition, input);
        _validate(w.result, input);
      }
      if (expression.elseExpression != null) {
        _validate(expression.elseExpression!, input);
      }
    } else if (expression is PhysicalCastExpression) {
      _validate(expression.expression, input);
    } else if (expression is PhysicalInExpression) {
      _validate(expression.value, input);
      for (final element in expression.list) {
        _validate(element, input);
      }
    }
    // Literal and any other leaf: nothing to validate.
  }

  @override
  PhysicalPlan visitJoin(logical.Join node) {
    final sides = node.sides;
    final left = _side(node.left.accept(this), sides?.$1);
    final right = _side(node.right.accept(this), sides?.$2);

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
    // cross-product branch below; that rewrites the join type to `cross` and
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
    // column equalities relating the two inputs fails loud: a hash join has no
    // residual-predicate path.
    final leftAliases = _aliases(node.left);
    final rightAliases = _aliases(node.right);
    // Bare column names available on each side (walk scans directly; the logical
    // Join.schema would collide on shared names since it is unqualified).
    final leftNames = _names(node.left);
    final rightNames = _names(node.right);
    bool onLeft(logical.ColumnExpression c) => c.table != null
        ? leftAliases.contains(c.table)
        : leftNames.contains(c.name) && !rightNames.contains(c.name);
    // The right side by its own aliases: counting any qualifier the left lacked
    // as the right's would bind an unknown `x.a` to the right's `a`.
    bool onRight(logical.ColumnExpression c) => c.table != null
        ? rightAliases.contains(c.table)
        : rightNames.contains(c.name) && !leftNames.contains(c.name);

    final leftKeys = <String>[];
    final rightKeys = <String>[];
    final leftQualifiers = <String?>[];
    final rightQualifiers = <String?>[];
    for (final conjunct in logical.conjuncts(node.condition!)) {
      if (conjunct is! logical.BinaryExpression ||
          conjunct.operator != logical.BinaryOperator.eq ||
          conjunct.left is! logical.ColumnExpression ||
          conjunct.right is! logical.ColumnExpression) {
        throw UnsupportedError('Complex join conditions not yet supported');
      }
      final first = conjunct.left as logical.ColumnExpression;
      final second = conjunct.right as logical.ColumnExpression;
      final logical.ColumnExpression leftColumn;
      final logical.ColumnExpression rightColumn;
      if (onLeft(first) && onRight(second)) {
        leftColumn = first;
        rightColumn = second;
      } else if (onLeft(second) && onRight(first)) {
        leftColumn = second;
        rightColumn = first;
      } else if (first.table case final qualifier?
          when !leftAliases.contains(qualifier) &&
              !rightAliases.contains(qualifier)) {
        throw FormatException('Unknown column: $qualifier.${first.name}');
      } else if (second.table case final qualifier?
          when !leftAliases.contains(qualifier) &&
              !rightAliases.contains(qualifier)) {
        throw FormatException('Unknown column: $qualifier.${second.name}');
      } else {
        throw StateError(
          'Join condition does not relate the two inputs (or is ambiguous)',
        );
      }
      leftKeys.add(leftColumn.name);
      leftQualifiers.add(leftColumn.table);
      rightKeys.add(rightColumn.name);
      rightQualifiers.add(rightColumn.table);
      _rejectStringKey(left.schema, leftColumn, right.schema, rightColumn);
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

  /// A join key pairing text with a number never matches; say so rather than
  /// return no rows (an `IN (SELECT ...)` subquery joins on its column too).
  static void _rejectStringKey(
    Schema leftSchema,
    logical.ColumnExpression leftKey,
    Schema rightSchema,
    logical.ColumnExpression rightKey,
  ) {
    DataType? typeOf(Schema schema, logical.ColumnExpression key) {
      final index = schema.resolve(key.name, qualifier: key.table);
      return index < 0 ? null : schema.fields[index].dataType;
    }

    final a = typeOf(leftSchema, leftKey);
    final b = typeOf(rightSchema, rightKey);
    if (a == null || b == null) return;
    if ((a == DataType.string && b.isNumeric) ||
        (b == DataType.string && a.isNumeric)) {
      throw FormatException(
        'Join key ${leftKey.name} (${a.name}) cannot match '
        '${rightKey.name} (${b.name}); CAST one side to the other\'s type',
      );
    }
  }

  /// Table aliases exposed by a logical subtree (used to route ON-condition
  /// columns to the correct join input).
  Set<String> _aliases(logical.LogicalPlan node) {
    final out = <String>{};
    void walk(logical.LogicalPlan p) {
      // A derived relation is an opaque scope: contribute its own alias, don't
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
    for (final aggregate in node.aggregates) {
      final converted = _convertAggregateExpression(aggregate, input.schema);
      aggregates.add(converted);
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
          (sort) => SortExpression(
            expression: _convertToPhysicalExpression(
              sort.expression,
              inputSchema: input.schema,
            ),
            ascending: sort.ascending,
            nullsFirst: sort.nullsFirst,
          ),
        )
        .toList();

    if (identical(node, _topSort)) {
      return PhysicalTopN(
        input: input,
        sortExpressions: sortExpressions,
        count: _topCount,
      );
    }
    return PhysicalSort(
      input: input,
      sortExpressions: sortExpressions,
      options: options,
    );
  }

  @override
  PhysicalPlan visitLimit(logical.Limit node) {
    // LIMIT over ORDER BY needs only the first limit + offset rows of the
    // sort, so a small one plans a top-N. A projection between them (trimming
    // ORDER BY-only columns) keeps every row in order, so it may sit between.
    final limit = node.limit;
    final below = node.input;
    final sort = switch (below) {
      logical.Sort() => below,
      logical.Projection(input: final logical.Sort inner) => inner,
      _ => null,
    };
    final count = limit == null ? 0 : limit + (node.offset ?? 0);
    if (sort != null && count > 0 && count <= topLimit) {
      _topSort = sort;
      _topCount = count;
    }
    final input = node.input.accept(this);

    return PhysicalLimit(input: input, limit: node.limit, offset: node.offset);
  }

  /// A sample's input is buffered whole, under the options' byte cap on a
  /// materialized result ([EngineOptions.maxMaterializedBytes], else the
  /// library default).
  @override
  PhysicalPlan visitSample(logical.Sample node) => PhysicalSample(
    input: node.input.accept(this),
    fraction: node.fraction,
    seed: node.seed,
    bytes: options.maxMaterializedBytes ?? defaultMaxMaterializedBytes,
    options: options,
  );

  @override
  PhysicalPlan visitDistinct(logical.Distinct node) {
    if (node.input case final logical.Union union) return _distinctUnion(union);
    // Implement as GROUP BY all columns. Use the physical input schema (real
    // types) rather than the logical node schema (which can mislabel numerics as
    // string), so the group keys (and the spill they may drive) are typed
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
          children: field.children,
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
  /// schema matches the columns `evalColumn` produces, instead of the
  /// `DataType.string` placeholder logical column refs carry. A null/empty
  /// [inputSchema] (e.g. a not-yet-read lazy scan) keeps the placeholder.
  static const _comparisons = {
    logical.BinaryOperator.eq,
    logical.BinaryOperator.neq,
    logical.BinaryOperator.lt,
    logical.BinaryOperator.lte,
    logical.BinaryOperator.gt,
    logical.BinaryOperator.gte,
  };

  static const _arithmetic = {
    logical.BinaryOperator.add,
    logical.BinaryOperator.subtract,
    logical.BinaryOperator.multiply,
    logical.BinaryOperator.divide,
    logical.BinaryOperator.modulo,
  };

  static const _symbols = {
    logical.BinaryOperator.add: '+',
    logical.BinaryOperator.subtract: '-',
    logical.BinaryOperator.multiply: '*',
    logical.BinaryOperator.divide: '/',
    logical.BinaryOperator.modulo: '%',
  };

  /// Throws when either operand of arithmetic is a string, naming the
  /// operator: the kernel would otherwise fail with a raw TypeError.
  void _rejectStringArithmetic(
    logical.BinaryOperator operator,
    PhysicalExpression left,
    PhysicalExpression right,
  ) {
    if (left.dataType != DataType.string && right.dataType != DataType.string) {
      return;
    }
    throw FormatException(
      'Cannot apply ${_symbols[operator]} to ${left.dataType.name} and '
      '${right.dataType.name}; CAST the string to a number',
    );
  }

  /// Throws when [side] is a string literal and [other] is numeric. There is
  /// no implicit coercion: `n = '1'` silently matched nothing, `n < '3'`
  /// failed only at run time, and `n + '1'` raised a raw TypeError.
  void _rejectStringNumber(PhysicalExpression side, PhysicalExpression other) {
    if (_null(side) || _null(other) || side.dataType != DataType.string) return;
    final type = other.dataType;
    if (!type.isNumeric) return;
    final target = type.isInteger
        ? 'BIGINT'
        : type == DataType.decimal
        ? 'DECIMAL'
        : 'DOUBLE';
    if (side is PhysicalLiteralExpression) {
      throw FormatException(
        "A string literal '${side.value}' cannot be compared or combined with "
        "a ${type.name} value; write a number, or CAST('${side.value}' AS "
        '$target)',
      );
    }
    // A string column never equals a number, so the comparison would match
    // nothing without a word.
    throw FormatException(
      'A string value cannot be compared or combined with a ${type.name} '
      'value; CAST one side to the other\'s type',
    );
  }

  /// Whether [expression] is the NULL literal, which compares with anything.
  static bool _null(PhysicalExpression expression) =>
      expression is PhysicalLiteralExpression && expression.value == null;

  /// [side] of a comparison, with a string literal read as a date or
  /// timestamp when the other side's [type] is one: `ts > '2024-06-01'`
  /// compares instants, as `ts > TIMESTAMP '2024-06-01'` does. Text that is
  /// not a date is a [FormatException], never a comparison that silently
  /// matches nothing.
  PhysicalExpression _temporal(
    logical.BinaryOperator operator,
    PhysicalExpression side,
    DataType type,
  ) {
    if (!_comparisons.contains(operator) ||
        side is! PhysicalLiteralExpression ||
        side.value is! String) {
      return side;
    }
    if (type != DataType.date && type != DataType.timestamp) return side;
    final at =
        parseUtc(side.value as String) ??
        (throw FormatException(
          'Cannot compare a ${type.name} with \'${side.value}\'',
        ));
    return PhysicalLiteralExpression(
      type == DataType.date ? dateOf(at) : at,
      type,
    );
  }

  PhysicalExpression _convertToPhysicalExpression(
    logical.LogicalExpression expression, {
    Schema? inputSchema,
  }) {
    if (expression is logical.ColumnExpression) {
      var type = expression.dataType;
      int? precision;
      int? scale;
      var children = const <Field>[];
      if (inputSchema != null && inputSchema.fields.isNotEmpty) {
        final i = inputSchema.resolve(
          expression.name,
          qualifier: expression.table,
        );
        if (i >= 0) {
          final f = inputSchema.fields[i];
          type = f.dataType;
          precision = f.precision;
          scale = f.scale; // thread decimal scale for scaled arithmetic
          children = f.children; // and nested shape for spillability
        }
      }
      return PhysicalColumnExpression(
        expression.name,
        type,
        qualifier: expression.table,
        precision: precision,
        scale: scale,
        children: children,
      );
    } else if (expression is logical.LiteralExpression) {
      return PhysicalLiteralExpression(expression.value, expression.dataType);
    } else if (expression is logical.InExpression) {
      final value = _convertToPhysicalExpression(
        expression.value,
        inputSchema: inputSchema,
      );
      // Each element is a comparison with the value, so a date or timestamp
      // string converts as it does for `=`.
      final list = [
        for (final element in expression.list)
          _temporal(
            logical.BinaryOperator.eq,
            _convertToPhysicalExpression(element, inputSchema: inputSchema),
            value.dataType,
          ),
      ];
      // An unknown column first: its placeholder type would otherwise be
      // reported as a string/number mismatch.
      if (inputSchema != null) {
        for (final side in [value, ...list]) {
          _validate(side, inputSchema);
        }
      }
      for (final element in list) {
        _rejectStringNumber(element, value);
        _rejectStringNumber(value, element);
      }
      return PhysicalInExpression(
        value: value,
        list: list,
        negated: expression.negated,
      );
    } else if (expression is logical.BinaryExpression) {
      final first = _convertToPhysicalExpression(
        expression.left,
        inputSchema: inputSchema,
      );
      final second = _convertToPhysicalExpression(
        expression.right,
        inputSchema: inputSchema,
      );
      // As for IN: an unknown column is reported as unknown, not as a type.
      if (inputSchema != null) {
        _validate(first, inputSchema);
        _validate(second, inputSchema);
      }
      if (_arithmetic.contains(expression.operator)) {
        _rejectStringArithmetic(expression.operator, first, second);
      }
      if (_arithmetic.contains(expression.operator) ||
          _comparisons.contains(expression.operator)) {
        _rejectStringNumber(first, second);
        _rejectStringNumber(second, first);
      }
      final left = _temporal(expression.operator, first, second.dataType);
      final right = _temporal(expression.operator, second, first.dataType);
      return PhysicalBinaryExpression(
        left: left,
        operator: expression.operator,
        right: right,
        resultType: _binaryResultType(
          expression.operator,
          left.dataType,
          right.dataType,
        ),
      );
    } else if (expression is logical.FunctionExpression) {
      return PhysicalFunctionExpression(
        expression.name,
        expression.arguments
            .map(
              (a) => _convertToPhysicalExpression(a, inputSchema: inputSchema),
            )
            .toList(),
      );
    } else if (expression is logical.CaseExpression) {
      final whens = [
        for (final w in expression.whenClauses)
          PhysicalWhen(
            _convertToPhysicalExpression(w.condition, inputSchema: inputSchema),
            _convertToPhysicalExpression(w.result, inputSchema: inputSchema),
          ),
      ];
      final elseExpression = expression.elseExpression == null
          ? null
          : _convertToPhysicalExpression(
              expression.elseExpression!,
              inputSchema: inputSchema,
            );
      // The base evalColumn stamps this type onto the produced column, so type
      // it as the common type across the resolved branch results (not the
      // placeholder the first branch's logical column carried).
      var caseType = whens.first.result.dataType;
      for (final w in whens.skip(1)) {
        caseType = DataType.commonType(caseType, w.result.dataType) ?? caseType;
      }
      if (elseExpression != null) {
        caseType =
            DataType.commonType(caseType, elseExpression.dataType) ?? caseType;
      }
      return PhysicalCaseExpression(whens, elseExpression, caseType);
    } else if (expression is logical.CastExpression) {
      return PhysicalCastExpression(
        _convertToPhysicalExpression(
          expression.expression,
          inputSchema: inputSchema,
        ),
        expression.targetType,
        targetPrecision: expression.precision,
        targetScale: expression.scale,
      );
    }

    // Fail closed rather than degrading an unsupported expression to NULL.
    throw UnsupportedError(
      'Unsupported expression in physical plan: ${expression.runtimeType}',
    );
  }

  PhysicalAggregateExpression _convertAggregateExpression(
    logical.AggregateExpression aggregate, [
    Schema? inputSchema,
  ]) {
    // Schema-aware conversion resolves the argument's real type against the
    // input schema (e.g. SUM/MIN/MAX over an int column gets int64, not the
    // placeholder: SUM then accumulates exactly as a decimal).
    final argument = aggregate.argument != null
        ? _convertToPhysicalExpression(
            aggregate.argument!,
            inputSchema: inputSchema,
          )
        : null;

    switch (aggregate.function.name.toUpperCase()) {
      case 'COUNT':
        return PhysicalCount(argument: argument, distinct: aggregate.distinct);
      case 'SUM':
        return PhysicalSum(argument: argument!, distinct: aggregate.distinct);
      case 'AVG':
        return PhysicalAvg(argument: argument!, distinct: aggregate.distinct);
      case 'MIN':
        return PhysicalMin(argument: argument!);
      case 'MAX':
        return PhysicalMax(argument: argument!);
      default:
        throw UnsupportedError(
          'Aggregate function ${aggregate.function.name} not supported',
        );
    }
  }

  @override
  PhysicalPlan visitSetOperation(logical.SetOperation node) {
    final left = node.left.accept(this);
    final right = node.right.accept(this);
    return PhysicalSetOperation(
      left: left,
      right: right,
      type: node.type,
      all: node.all,
      options: options,
      schema: Schema(
        fields: _reconcile(left.schema.fields, right.schema.fields),
      ),
      leftSchema: left.schema,
      rightSchema: right.schema,
    );
  }

  @override
  PhysicalPlan visitUnion(logical.Union node) =>
      _union([for (final input in node.inputs) input.accept(this)]);

  PhysicalPlan _union(List<PhysicalPlan> inputs) {
    // Folding the pairwise rule left to right gives the schema a chain of
    // binary unions had, so the n-ary form changes no result type.
    var fields = inputs.first.schema.fields;
    for (final input in inputs.skip(1)) {
      fields = _reconcile(fields, input.schema.fields);
    }
    return PhysicalUnion(
      inputs: inputs,
      schema: Schema(fields: fields),
      options: options,
    );
  }

  /// A distinct UNION over [node]'s branches, through the set-operation
  /// dedup: every branch but the last as one [PhysicalUnion], then the last.
  /// The dedup keys on column codes, which a group-by over every column does
  /// not, and the plan stays two levels deep however many branches there are.
  PhysicalPlan _distinctUnion(logical.Union node) {
    final inputs = [for (final input in node.inputs) input.accept(this)];
    final head = inputs.length == 2
        ? inputs.first
        : _union(inputs.sublist(0, inputs.length - 1));
    final last = inputs.last;
    return PhysicalSetOperation(
      left: head,
      right: last,
      type: logical.SetOpType.union,
      all: false,
      options: options,
      schema: Schema(
        fields: _reconcile(head.schema.fields, last.schema.fields),
      ),
      leftSchema: head.schema,
      rightSchema: last.schema,
    );
  }

  /// The output fields of a set operation over branches with [leftFields] and
  /// [rightFields], reconciled on their REAL physical types (logical column
  /// refs are `string` placeholders, so type reconciliation can't happen at
  /// logical binding). Per position take commonType; the branches are coerced
  /// to this shared schema at the iterator boundary so int-width / int-float
  /// unions promote correctly instead of crashing at concat/validate.
  List<Field> _reconcile(List<Field> leftFields, List<Field> rightFields) {
    if (leftFields.length != rightFields.length) {
      throw Failure.of(
        Kind.schema,
        'Set operation branches have different column counts '
        '(${leftFields.length} vs ${rightFields.length})',
      );
    }
    final fields = <Field>[];
    for (int i = 0; i < leftFields.length; i++) {
      final common = DataType.commonType(
        leftFields[i].dataType,
        rightFields[i].dataType,
      );
      if (common == null) {
        throw ArgumentError(
          'Set operation column ${i + 1} has incompatible types '
          '(${leftFields[i].dataType} vs ${rightFields[i].dataType})',
        );
      }
      // A decimal's scale lives on the FIELD, not the DataType, so the branches
      // can share `decimal` yet store the SAME value as different unscaled
      // BigInts (1.5 is 15@scale1 but 150@scale2). Reconcile to the WIDEST scale
      // so the iterator restates each branch to one representation; the value
      // then concats, dedups, and compares correctly. Widest so neither narrows.
      final scale = common == DataType.decimal
          ? _widestScale(leftFields[i], rightFields[i])
          : null;
      fields.add(
        Field(
          name: leftFields[i].name,
          dataType: common,
          nullable: leftFields[i].nullable || rightFields[i].nullable,
          precision: common == DataType.decimal
              ? _reconciledPrecision(leftFields[i], rightFields[i], scale)
              : null,
          scale: scale,
          // A nested column's shape must survive reconciliation: the set-op
          // spill writer rejects a children-less list/struct/map field.
          children: leftFields[i].children.isNotEmpty
              ? leftFields[i].children
              : rightFields[i].children,
        ),
      );
    }
    return fields;
  }

  /// The scale a decimal set-op column must be restated at: the wider of the two
  /// branches, so neither narrows and loses digits. An INTEGER branch (e.g.
  /// `int_col UNION decimal_col`) contributes scale 0; its natural scale, into
  /// which it restates losslessly, so the result keeps the decimal side's scale
  /// instead of collapsing to null (which made `5` and `0.05` both key as
  /// `BigInt(5)` and silently dedup). Null only when a branch is neither decimal
  /// nor integer (e.g. float; its decimal cast fails loud elsewhere).
  int? _widestScale(Field left, Field right) {
    final leftScale = _branchScale(left);
    final rightScale = _branchScale(right);
    if (leftScale == null || rightScale == null) return null;
    return leftScale > rightScale ? leftScale : rightScale;
  }

  int? _branchScale(Field f) => f.dataType == DataType.decimal
      ? f.scale
      : (f.dataType.isInteger ? 0 : null);

  /// The precision a decimal set-op column needs once both branches are restated
  /// at [resultScale]: the widest INTEGER-digit count plus the result scale;
  /// `decimal(3,1)` ∪ `decimal(3,2)` is `decimal(4,2)`, not `decimal(3,2)`, or the
  /// declared metadata can't represent the values (an invalid Parquet/Arrow
  /// contract). An integer branch contributes its type's max decimal width.
  int? _reconciledPrecision(Field left, Field right, int? resultScale) {
    if (resultScale == null) return null;
    final leftDigits = _branchIntDigits(left);
    final rightDigits = _branchIntDigits(right);
    if (leftDigits == null || rightDigits == null) return null;
    return (leftDigits > rightDigits ? leftDigits : rightDigits) + resultScale;
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
