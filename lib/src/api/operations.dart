part of 'dataframe.dart';

/// Base class for DataFrame operations
@immutable
abstract class Operation {
  const Operation();

  /// Imperative application (fallback / unfusible ops). Most ops also implement
  /// [toPhysical] so they fuse into a single streamed plan. [ctx] bounds any
  /// intermediate this op materializes (by bytes) — see [ExecutionContext].
  Future<Table> apply(Table table, ExecutionContext ctx);

  /// Contribute this op to a fused physical plan over [input], or return null if
  /// it can't be expressed as a plan node (then the caller materializes + uses
  /// [apply]). [groupKeys] carries a preceding GROUP BY's keys to an aggregate.
  ///
  /// [ctx] is the CALLER's resource policy and must be threaded into any nested
  /// plan an op builds (join/union build one for their right side). Without it
  /// those nested builds fell back to a hard-coded default: a caller's tighter
  /// cap was silently discarded, and `executeStream` — which asks for no cap —
  /// had one imposed on it.
  Future<PhysicalPlan?> toPhysical(
    PhysicalPlan input,
    List<String> groupKeys,
    ExecutionContext ctx,
  ) async => null;

  /// The schema this op produces given [input] (`null` = unknown). Reshaping ops
  /// (withColumn/join/aggregate) OVERRIDE this so the DataFrame's cached schema
  /// reflects the transform instead of silently retaining the source schema.
  /// The default passes [input] through unchanged. [groupKeys] carries a
  /// preceding GROUP BY's keys to an aggregate.
  Schema? outputSchema(Schema? input, List<String> groupKeys) => input;
}

/// A lazy file source at the root of a DataFrame built by an I/O factory
/// (readParquet/…). Rooted at a streaming [PhysicalFileScan] in [_buildPlan] so a
/// chained `.filter().head(n)` streams and stops early rather than decoding the
/// whole file. It is ALWAYS the plan root, so its [apply]/[toPhysical] are never
/// invoked (the root is built directly from it).
class ScanOperation extends Operation {
  final String path;
  final Schema tableSchema;
  const ScanOperation(this.path, this.tableSchema);

  @override
  Future<Table> apply(Table table, ExecutionContext ctx) =>
      throw StateError('ScanOperation is a plan root, never applied');

  @override
  Future<PhysicalPlan?> toPhysical(
    PhysicalPlan input,
    List<String> groupKeys,
    ExecutionContext ctx,
  ) async => throw StateError('ScanOperation is a plan root, never folded');
}

/// dtype of [name] in [schema] (placeholder string if absent — resolved at eval).
DataType _dtypeOf(Schema schema, String name) =>
    schema.field(name)?.dataType ?? DataType.string;

/// A schema-only physical scan, so an op's `.schema` getter can be reused to
/// derive its output schema without touching data.
PhysicalTableScan _schemaScan(Schema schema) =>
    PhysicalTableScan(tableName: 'df', table: Table.empty('df', schema));

/// Select columns operation
class SelectOperation extends Operation {
  final List<String> columns;

  const SelectOperation(this.columns);

  @override
  Future<Table> apply(Table table, ExecutionContext ctx) async {
    return table.select(columns);
  }

  @override
  Future<PhysicalPlan?> toPhysical(
    PhysicalPlan input,
    List<String> groupKeys,
    ExecutionContext ctx,
  ) async => PhysicalProjection(
    input: input,
    expressions: [
      for (final c in columns)
        PhysicalColumnExpression(c, _dtypeOf(input.schema, c)),
    ],
    aliases: columns,
  );
}

/// Filter operation. The predicate keeps the lenient DataFrame syntax (column
/// names may contain spaces; `==`/`=`/`!=`/`<>`/`<`/`<=`/`>`/`>=`, optional
/// `[brackets]`) and now also supports `AND`/`OR`. It compiles to the shared
/// vectorized engine ([PhysicalFilter]) rather than per-row evaluation.
class FilterOperation extends Operation {
  final String predicate;

  const FilterOperation(this.predicate);

  @override
  Future<Table> apply(Table table, ExecutionContext ctx) async {
    final compiled = _PredicateCompiler.compile(predicate, table.schema);
    final scan = PhysicalTableScan(tableName: table.name, table: table);
    return _runPlan(
      PhysicalFilter(input: scan, predicate: compiled),
      table.name,
      maxBytes: ctx.maxBytes,
    );
  }

  @override
  Future<PhysicalPlan?> toPhysical(
    PhysicalPlan input,
    List<String> groupKeys,
    ExecutionContext ctx,
  ) async => PhysicalFilter(
    input: input,
    predicate: _PredicateCompiler.compile(predicate, input.schema),
  );
}

/// Limit operation
class LimitOperation extends Operation {
  final int n;
  final int offset;

  const LimitOperation(this.n, this.offset);

  @override
  Future<Table> apply(Table table, ExecutionContext ctx) async {
    final start = offset;
    final end = (offset + n).clamp(0, table.rowCount);
    return table.slice(start, end);
  }

  @override
  Future<PhysicalPlan?> toPhysical(
    PhysicalPlan input,
    List<String> groupKeys,
    ExecutionContext ctx,
  ) async => PhysicalLimit(input: input, limit: n, offset: offset);
}

/// Order by operation
class OrderByOperation extends Operation {
  final String column;
  final bool ascending;

  const OrderByOperation(this.column, this.ascending);

  @override
  Future<Table> apply(Table table, ExecutionContext ctx) async {
    final column = table.column(this.column);
    if (column == null) {
      throw ArgumentError('Column not found: ${this.column}');
    }

    final indices = List.generate(table.rowCount, (i) => i);
    indices.sort((a, b) {
      final aVal = column[a];
      final bVal = column[b];

      if (aVal == null && bVal == null) return 0;
      if (aVal == null) return ascending ? -1 : 1;
      if (bVal == null) return ascending ? 1 : -1;

      if (aVal is Comparable && bVal is Comparable) {
        final result = aVal.compareTo(bVal);
        return ascending ? result : -result;
      }

      return 0;
    });

    return table.filter(indices);
  }

  @override
  Future<PhysicalPlan?> toPhysical(
    PhysicalPlan input,
    List<String> groupKeys,
    ExecutionContext ctx,
  ) async => PhysicalSort(
    input: input,
    sortExpressions: [
      SortExpression(
        expression: PhysicalColumnExpression(
          column,
          _dtypeOf(input.schema, column),
        ),
        ascending: ascending,
        // Match apply()'s ordering: nulls first ascending, last descending.
        nullsFirst: ascending,
      ),
    ],
    options: ctx.options,
  );
}

/// Group by operation
class GroupByOperation extends Operation {
  final List<String> columns;

  const GroupByOperation(this.columns);

  /// Metadata key used to carry grouping keys to a following [AggregateOperation].
  static const groupKeysMeta = '__group_keys';

  @override
  Future<Table> apply(Table table, ExecutionContext ctx) async {
    for (final c in columns) {
      if (table.column(c) == null) {
        throw ArgumentError('Group-by column not found: $c');
      }
    }
    // groupBy() on its own does not reshape data; it records the keys so a
    // following agg() can aggregate within each group.
    return Table(
      name: table.name,
      columns: table.columns,
      schema: table.schema,
      metadata: {...table.metadata, groupKeysMeta: columns},
    );
  }
}

/// Join operation
class JoinOperation extends Operation {
  final DataFrame other;
  final String on;
  final JoinType type;

  const JoinOperation(this.other, this.on, this.type);

  @override
  Future<Table> apply(Table table, ExecutionContext ctx) async {
    // Materialize the build side WITHOUT the result caps — it's an intermediate
    // feeding the join, not the user-facing result (which _runPlan bounds).
    final otherTable = await other.execute(
      maxRows: null,
      maxBytes: ctx.maxBytes,
    );
    final left = PhysicalTableScan(tableName: table.name, table: table);
    final right = PhysicalTableScan(
      tableName: otherTable.name,
      table: otherTable,
    );
    final join = PhysicalHashJoin(
      left: left,
      right: right,
      leftKeys: [on],
      rightKeys: [on],
      joinType: _planJoin(type),
      options: ctx.options,
    );
    return _runPlan(join, table.name, maxBytes: ctx.maxBytes);
  }

  @override
  Future<PhysicalPlan?> toPhysical(
    PhysicalPlan input,
    List<String> groupKeys,
    ExecutionContext ctx,
  ) async => PhysicalHashJoin(
    left: input,
    // The caller's policy, not a hard-coded default: a tighter cap must apply
    // to the right side too, and an unbounded stream must not acquire one.
    right: await other._buildPlan(ctx),
    leftKeys: [on],
    rightKeys: [on],
    joinType: _planJoin(type),
    options: ctx.options,
  );

  @override
  Schema? outputSchema(Schema? input, List<String> groupKeys) {
    final otherSchema = other.schema;
    if (input == null || otherSchema == null) return null;
    // Reuse the physical join's exact output-schema logic (left fields + right
    // fields, de-duping the collided key) rather than re-deriving it here.
    return PhysicalHashJoin(
      left: _schemaScan(input),
      right: _schemaScan(otherSchema),
      leftKeys: [on],
      rightKeys: [on],
      joinType: _planJoin(type),
    ).schema;
  }

  static plan.JoinType _planJoin(JoinType t) {
    switch (t) {
      case JoinType.inner:
        return plan.JoinType.inner;
      case JoinType.left:
        return plan.JoinType.left;
      case JoinType.right:
        return plan.JoinType.right;
      case JoinType.full:
        return plan.JoinType.full;
      case JoinType.cross:
        return plan.JoinType.cross;
    }
  }
}

/// Aggregate operation
class AggregateOperation extends Operation {
  final Map<String, String> aggregations;

  const AggregateOperation(this.aggregations);

  @override
  Future<Table> apply(Table table, ExecutionContext ctx) async {
    final keys =
        (table.metadata[GroupByOperation.groupKeysMeta] as List?)
            ?.cast<String>() ??
        const <String>[];
    DataType typeOf(String name) {
      for (final f in table.schema.fields) {
        if (f.name == name) return f.dataType;
      }
      return DataType.string;
    }

    final groupBy = [
      for (final k in keys) PhysicalColumnExpression(k, typeOf(k)),
    ];
    final aggregates = <PhysicalAggregateExpression>[];
    final aliases = <String>[];
    aggregations.forEach((column, func) {
      aggregates.add(
        _aggExpr(func, PhysicalColumnExpression(column, typeOf(column))),
      );
      aliases.add(column); // output column name matches the input (legacy)
    });

    final scan = PhysicalTableScan(tableName: table.name, table: table);
    final agg = PhysicalAggregate(
      input: scan,
      groupBy: groupBy,
      aggregates: aggregates,
      aliases: aliases,
      options: ctx.options,
    );
    return _runPlan(agg, table.name, maxBytes: ctx.maxBytes);
  }

  @override
  Future<PhysicalPlan?> toPhysical(
    PhysicalPlan input,
    List<String> groupKeys,
    ExecutionContext ctx,
  ) async {
    final schema = input.schema;
    final groupBy = [
      for (final k in groupKeys)
        PhysicalColumnExpression(k, _dtypeOf(schema, k)),
    ];
    final aggregates = <PhysicalAggregateExpression>[];
    final aliases = <String>[];
    aggregations.forEach((column, func) {
      aggregates.add(
        _aggExpr(
          func,
          PhysicalColumnExpression(column, _dtypeOf(schema, column)),
        ),
      );
      aliases.add(column);
    });
    return PhysicalAggregate(
      input: input,
      groupBy: groupBy,
      aggregates: aggregates,
      aliases: aliases,
      options: ctx.options,
    );
  }

  @override
  Schema? outputSchema(Schema? input, List<String> groupKeys) {
    if (input == null) return null;
    final groupBy = [
      for (final k in groupKeys)
        PhysicalColumnExpression(k, _dtypeOf(input, k)),
    ];
    final aggregates = <PhysicalAggregateExpression>[];
    final aliases = <String>[];
    aggregations.forEach((column, func) {
      aggregates.add(
        _aggExpr(
          func,
          PhysicalColumnExpression(column, _dtypeOf(input, column)),
        ),
      );
      aliases.add(column);
    });
    return PhysicalAggregate(
      input: _schemaScan(input),
      groupBy: groupBy,
      aggregates: aggregates,
      aliases: aliases,
    ).schema;
  }

  static PhysicalAggregateExpression _aggExpr(
    String func,
    PhysicalColumnExpression arg,
  ) {
    switch (func.toLowerCase()) {
      case 'count':
        return PhysicalCount(argument: arg);
      case 'sum':
        return PhysicalSum(argument: arg);
      case 'avg':
      case 'mean':
        return PhysicalAvg(argument: arg);
      case 'min':
        return PhysicalMin(argument: arg);
      case 'max':
        return PhysicalMax(argument: arg);
      default:
        throw ArgumentError('Unsupported aggregation function: $func');
    }
  }
}

/// With column operation
class WithColumnOperation extends Operation {
  final String name;
  final String expression;

  const WithColumnOperation(this.name, this.expression);

  @override
  Future<Table> apply(Table table, ExecutionContext ctx) async {
    final fields = table.schema.fields;
    final expressions = <PhysicalExpression>[];
    final aliases = <String>[];
    for (final f in fields) {
      expressions.add(PhysicalColumnExpression(f.name, f.dataType));
      aliases.add(f.name);
    }
    final computed = _ExprCompiler.compile(expression.trim(), table.schema);
    final at = aliases.indexOf(name);
    if (at >= 0) {
      expressions[at] = computed; // replace an existing column in place
    } else {
      expressions.add(computed);
      aliases.add(name);
    }
    final scan = PhysicalTableScan(tableName: table.name, table: table);
    return _runPlan(
      PhysicalProjection(
        input: scan,
        expressions: expressions,
        aliases: aliases,
      ),
      table.name,
      maxBytes: ctx.maxBytes,
    );
  }

  @override
  Future<PhysicalPlan?> toPhysical(
    PhysicalPlan input,
    List<String> groupKeys,
    ExecutionContext ctx,
  ) async {
    final expressions = <PhysicalExpression>[];
    final aliases = <String>[];
    for (final f in input.schema.fields) {
      expressions.add(PhysicalColumnExpression(f.name, f.dataType));
      aliases.add(f.name);
    }
    final computed = _ExprCompiler.compile(expression.trim(), input.schema);
    final at = aliases.indexOf(name);
    if (at >= 0) {
      expressions[at] = computed;
    } else {
      expressions.add(computed);
      aliases.add(name);
    }
    return PhysicalProjection(
      input: input,
      expressions: expressions,
      aliases: aliases,
    );
  }

  @override
  Schema? outputSchema(Schema? input, List<String> groupKeys) {
    if (input == null) return null;
    final expressions = <PhysicalExpression>[];
    final aliases = <String>[];
    for (final f in input.fields) {
      expressions.add(PhysicalColumnExpression(f.name, f.dataType));
      aliases.add(f.name);
    }
    final computed = _ExprCompiler.compile(expression.trim(), input);
    final at = aliases.indexOf(name);
    if (at >= 0) {
      expressions[at] = computed;
    } else {
      expressions.add(computed);
      aliases.add(name);
    }
    return PhysicalProjection(
      input: _schemaScan(input),
      expressions: expressions,
      aliases: aliases,
    ).schema;
  }
}

/// Drop columns operation
class DropOperation extends Operation {
  final List<String> columns;

  const DropOperation(this.columns);

  @override
  Future<Table> apply(Table table, ExecutionContext ctx) async {
    Table result = table;
    for (final column in columns) {
      result = result.removeColumn(column);
    }
    return result;
  }

  @override
  Future<PhysicalPlan?> toPhysical(
    PhysicalPlan input,
    List<String> groupKeys,
    ExecutionContext ctx,
  ) async {
    final keep = [
      for (final f in input.schema.fields)
        if (!columns.contains(f.name)) f.name,
    ];
    return PhysicalProjection(
      input: input,
      expressions: [
        for (final c in keep)
          PhysicalColumnExpression(c, _dtypeOf(input.schema, c)),
      ],
      aliases: keep,
    );
  }
}

/// Rename operation
class RenameOperation extends Operation {
  final Map<String, String> renames;

  const RenameOperation(this.renames);

  @override
  Future<Table> apply(Table table, ExecutionContext ctx) async {
    return table.renameColumns(renames);
  }

  @override
  Future<PhysicalPlan?> toPhysical(
    PhysicalPlan input,
    List<String> groupKeys,
    ExecutionContext ctx,
  ) async => PhysicalProjection(
    input: input,
    expressions: [
      for (final f in input.schema.fields)
        PhysicalColumnExpression(f.name, f.dataType),
    ],
    aliases: [for (final f in input.schema.fields) renames[f.name] ?? f.name],
  );
}

/// Distinct operation: deduplicate rows over all columns. Implemented as a
/// group-by over every column on the shared engine, which uses typed
/// value-equality keys (no lossy stringification) and preserves first-seen order.
class DistinctOperation extends Operation {
  const DistinctOperation();

  @override
  Future<Table> apply(Table table, ExecutionContext ctx) async {
    // precision/scale, like the SQL planner's column expressions: a group key
    // built without them binds a decimal at scale 0, so DISTINCT renders 1.5 as
    // "15" and compares it as 15.
    final groupBy = [
      for (final f in table.schema.fields)
        PhysicalColumnExpression(
          f.name,
          f.dataType,
          precision: f.precision,
          scale: f.scale,
        ),
    ];
    final scan = PhysicalTableScan(tableName: table.name, table: table);
    final agg = PhysicalAggregate(
      input: scan,
      groupBy: groupBy,
      aggregates: const [],
      aliases: const [],
      options: ctx.options,
    );
    return _runPlan(agg, table.name, maxBytes: ctx.maxBytes);
  }

  @override
  Future<PhysicalPlan?> toPhysical(
    PhysicalPlan input,
    List<String> groupKeys,
    ExecutionContext ctx,
  ) async => PhysicalAggregate(
    input: input,
    groupBy: [
      for (final f in input.schema.fields)
        PhysicalColumnExpression(
          f.name,
          f.dataType,
          precision: f.precision,
          scale: f.scale,
        ),
    ],
    aggregates: const [],
    aliases: const [],
    options: ctx.options,
  );
}

/// Union operation
class UnionOperation extends Operation {
  final DataFrame other;

  const UnionOperation(this.other);

  @override
  Future<Table> apply(Table table, ExecutionContext ctx) async {
    // Intermediate (union input), not the user-facing result — don't cap it.
    final otherTable = await other.execute(
      maxRows: null,
      maxBytes: ctx.maxBytes,
    );
    return table.append(otherTable);
  }

  @override
  Future<PhysicalPlan?> toPhysical(
    PhysicalPlan input,
    List<String> groupKeys,
    ExecutionContext ctx,
  ) async {
    // The caller's policy, not a hard-coded default (see JoinOperation).
    final right = await other._buildPlan(ctx);
    // Reconcile the two branches on their real types (per-position commonType,
    // left names) so int-width / int-float unions promote instead of crashing.
    final lf = input.schema.fields;
    final rf = right.schema.fields;
    final fields = [
      for (int i = 0; i < lf.length; i++)
        () {
          final ct =
              DataType.commonType(lf[i].dataType, rf[i].dataType) ??
              (throw ArgumentError(
                'union column ${i + 1}: incompatible types '
                '(${lf[i].dataType} vs ${rf[i].dataType})',
              ));
          // A decimal's scale is on the Field, so two branches can share
          // `decimal` yet store a value as different unscaled BigInts. Reconcile
          // to the wider scale (the iterator restates each branch to it), or the
          // concatenated column mixes representations.
          final ls = lf[i].dataType == DataType.decimal ? lf[i].scale : null;
          final rs = rf[i].dataType == DataType.decimal ? rf[i].scale : null;
          final scale = ct == DataType.decimal && ls != null && rs != null
              ? (ls > rs ? ls : rs)
              : null;
          final lp = lf[i].precision, rp = rf[i].precision;
          // Result precision = widest integer-digit count (precision − scale) +
          // result scale; rescaling the narrower branch UP adds unscaled digits,
          // so plain max(precision) understates it (see physical builder).
          final precision =
              ct == DataType.decimal &&
                  lp != null &&
                  rp != null &&
                  ls != null &&
                  rs != null &&
                  scale != null
              ? ((lp - ls) > (rp - rs) ? (lp - ls) : (rp - rs)) + scale
              : null;
          return Field(
            name: lf[i].name,
            dataType: ct,
            nullable: lf[i].nullable || rf[i].nullable,
            precision: precision,
            scale: scale,
          );
        }(),
    ];
    return PhysicalSetOperation(
      left: input,
      right: right,
      type: plan.SetOpType.union,
      all: true, // UNION ALL — matches table.append (no dedup)
      schema: Schema(fields: fields),
      leftSchema: input.schema,
      rightSchema: right.schema,
      options: ctx.options,
    );
  }
}

/// Sample operation
/// Takes EXACTLY `round(rowCount * fraction)` rows, uniformly and (with a
/// [seed]) reproducibly.
///
/// The exact count is why this is the one operator with no [toPhysical]: it
/// cannot know how many rows to keep until it knows how many there are, so the
/// upstream plan is materialized first. A streaming sampler would have to be
/// Bernoulli (keep each row with probability [fraction] independently), which
/// yields only an APPROXIMATE count — a different contract. The materialization
/// is bounded by the caller's [ExecutionContext].
class SampleOperation extends Operation {
  final double fraction;
  final int? seed;

  const SampleOperation(this.fraction, this.seed);

  @override
  Future<Table> apply(Table table, ExecutionContext ctx) async {
    final n = (table.rowCount * fraction).round();
    // Typed, so the index scratch is 4 bytes/row instead of a boxed word.
    final indices = Int32List(table.rowCount);
    for (var i = 0; i < indices.length; i++) {
      indices[i] = i;
    }
    // Partial Fisher-Yates: only the n indices actually kept need shuffling.
    final random = seed != null ? math.Random(seed) : math.Random();
    for (var i = 0; i < n; i++) {
      final j = i + random.nextInt(indices.length - i);
      final t = indices[i];
      indices[i] = indices[j];
      indices[j] = t;
    }
    return table.filter(indices.take(n).toList());
  }
}
