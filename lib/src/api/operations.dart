part of 'dataframe.dart';

/// Base class for DataFrame operations
@immutable
abstract class Operation {
  const Operation();

  /// This op as a logical node over [input], lowered with the rest of the
  /// plan by [QueryPlanner.lower], or null for an op that has none: join and
  /// union, which [DataFrame] combines with the other frame's plan
  /// ([JoinOperation.combine], [UnionOperation.combine]).
  logical.LogicalPlan? lower(logical.LogicalPlan input) => null;

  /// The schema this op produces given [input] (`null` = unknown), its fields
  /// still qualified by join side ([DataFrame.schema] reports them as a result
  /// names them). Reshaping ops (select/drop/rename/withColumn/join/aggregate/
  /// union) OVERRIDE this so the DataFrame's cached schema reflects the
  /// transform instead of silently retaining the source schema. The default
  /// passes [input] through unchanged.
  Schema? outputSchema(Schema? input) => input;
}

/// A lazy file source at the root of a DataFrame built by an I/O factory
/// (readParquet/…). [_buildPlan] roots the plan at a logical scan of it, which
/// lowers to a streaming [PhysicalFileScan] carrying the pushed filter and
/// projection, so a chained `.filter().head(n)` streams and stops early rather
/// than decoding the whole file. It is ALWAYS the plan root, so its [lower]
/// is never invoked.
class ScanOperation extends Operation {
  final String path;
  final Schema tableSchema;

  /// Opens the scan with the options its reader was called with; null opens
  /// `openScan(path)` ([LazySource.open]).
  final Scan Function()? open;

  /// The caps the reader was called with (null: none). A materializing
  /// terminal bounds the frame's result by the tighter of these and its own.
  final int? maxRows;
  final int? maxBytes;

  const ScanOperation(
    this.path,
    this.tableSchema, {
    this.open,
    this.maxRows,
    this.maxBytes,
  });
}

/// A logical reference to [field], under its qualifier when it has one.
logical.ColumnExpression _logical(Field field) => logical.ColumnExpression(
  name: field.name,
  table: field.qualifier,
  columnType: field.dataType,
  isNullable: field.nullable,
);

/// The names [schema]'s fields are reported under, in a result and in
/// [DataFrame.columns]: a name more than one side of a join has is qualified
/// by its side, `p.name` ([Kernels.disambiguate]).
List<String> _names(Schema schema) =>
    Kernels.disambiguate(Table.empty('names', schema)).schema.fieldNames;

/// The index of the field of [schema] that [name] refers to, or -1 when
/// there is none. [name] is a name as [_names] reports it, so `p.name` is
/// the `name` of side `p` and a field named `a.b` is `a.b`. A bare name that
/// more than one side has is [Kind.invalid], naming the qualified choices.
int _find(Schema schema, String name) {
  final names = _names(schema);
  final at = names.indexOf(name);
  if (at >= 0) return at;
  final choices = [
    for (var i = 0; i < names.length; i++)
      if (schema.fields[i].name == name) names[i],
  ];
  if (choices.isEmpty) return -1;
  final rest = choices.sublist(0, choices.length - 1).join(', ');
  throw Failure.of(
    Kind.invalid,
    'Ambiguous column "$name": use $rest or ${choices.last}',
  );
}

/// A logical reference to [name] in [schema], resolved by [_find]
/// (unresolved if absent, which fails when the plan is lowered or
/// evaluated).
logical.ColumnExpression _named(Schema schema, String name) =>
    switch (_find(schema, name)) {
      -1 => logical.ColumnExpression(name: name, columnType: DataType.string),
      final at => _logical(schema.fields[at]),
    };

/// [text], a SQL scalar expression, as a logical expression. A malformed
/// expression or an aggregate is a [FormatException], a subquery an
/// [UnsupportedError]; columns are bound when the plan is lowered.
logical.LogicalExpression _parse(String text) =>
    QueryPlanner(TableCatalog()).convert(SqlParser().parseExpression(text));

/// The schema [build] lowers to over scans of empty tables with [schemas],
/// its fields still qualified by side: how an op derives the schema of its
/// frame, through the physical builder that runs it, without touching data.
/// The optimizer rules, which leave the schema as it is, are not run: a scan
/// of a joined schema holds `name` twice, which a projection by name cannot
/// narrow.
Schema _shape(
  List<Schema> schemas,
  logical.LogicalPlan Function(List<logical.LogicalPlan> scans) build,
) {
  final catalog = TableCatalog();
  final scans = <logical.LogicalPlan>[];
  for (int i = 0; i < schemas.length; i++) {
    catalog.registerTable('input$i', Table.empty('input$i', schemas[i]));
    scans.add(logical.TableScan(tableName: 'input$i', tableSchema: schemas[i]));
  }
  return build(scans).accept(PhysicalPlanBuilder(catalog)).schema;
}

/// The schema [operation] lowers to over [input] (see [_shape]).
Schema? _over(Schema? input, Operation operation) => input == null
    ? null
    : _shape([input], (scans) => operation.lower(scans.single)!);

/// Select columns operation
class SelectOperation extends Operation {
  final List<String> columns;

  const SelectOperation(this.columns);

  @override
  logical.LogicalPlan lower(logical.LogicalPlan input) => logical.Projection(
    input: input,
    expressions: [for (final c in columns) _named(input.schema, c)],
    aliases: columns,
  );

  @override
  Schema? outputSchema(Schema? input) => _over(input, this);
}

/// Filter operation. The predicate is a SQL expression, as in a WHERE
/// clause; a non-boolean one is cast to BOOLEAN when the plan is lowered.
class FilterOperation extends Operation {
  final String predicate;

  const FilterOperation(this.predicate);

  @override
  logical.LogicalPlan lower(logical.LogicalPlan input) =>
      logical.Filter(input: input, predicate: _parse(predicate));
}

/// Limit operation
class LimitOperation extends Operation {
  final int n;
  final int offset;

  const LimitOperation(this.n, this.offset);

  @override
  logical.LogicalPlan lower(logical.LogicalPlan input) =>
      logical.Limit(input: input, limit: n, offset: offset);
}

/// Order by operation. NULLs sort last in both directions, as SQL's default.
class OrderByOperation extends Operation {
  final String column;
  final bool ascending;

  const OrderByOperation(this.column, this.ascending);

  @override
  logical.LogicalPlan lower(logical.LogicalPlan input) => logical.Sort(
    input: input,
    sortExpressions: [
      logical.SortExpression(
        expression: _named(input.schema, column),
        ascending: ascending,
        nullsFirst: false,
      ),
    ],
  );
}

/// Group by operation. It is never part of the plan: its [columns] are the
/// group keys of the [AggregateOperation] that must follow it.
class GroupByOperation extends Operation {
  final List<String> columns;

  const GroupByOperation(this.columns);
}

/// Join operation: a SQL `USING (on)` join (a cross join ignores [on]), so the
/// key is one coalesced column. Each side's columns that carry no qualifier
/// are qualified by the side's name, [name] for this frame and
/// [DataFrame._name] for [other], or `left` and `right` when the two are the
/// same; a colliding non-key column then comes out as `p.name` and `d.name`,
/// as SQL names it. [on] is a name the frames report: a side that reports
/// it qualified (`d.name`, from an earlier join) is joined under the names it
/// reports.
class JoinOperation extends Operation {
  final DataFrame other;
  final String on;
  final JoinType type;

  /// The name of the frame this joins [other] to.
  final String name;

  const JoinOperation(this.other, this.on, this.type, this.name);

  /// The join of [input] with [branch], the logical plan of [other].
  logical.LogicalPlan combine(
    logical.LogicalPlan input,
    logical.LogicalPlan branch,
  ) {
    final right = other._name;
    final same = name.toLowerCase() == right.toLowerCase();
    final cross = type == JoinType.cross;
    return logical.Join(
      left: cross ? input : _flat(input),
      right: cross ? branch : _flat(branch),
      joinType: _planJoin(type),
      usingColumns: cross ? null : [on],
      sides: same ? ('left', 'right') : (name, right),
    );
  }

  /// [plan], or a projection of it under the names it reports when it
  /// reports [on] under a qualified name, so the key is one column of that
  /// name on each side.
  logical.LogicalPlan _flat(logical.LogicalPlan plan) {
    final schema = plan.schema;
    final at = _find(schema, on);
    if (at < 0 || schema.fields[at].name == on) return plan;
    return logical.Projection(
      input: plan,
      expressions: [for (final f in schema.fields) _logical(f)],
      aliases: _names(schema),
    );
  }

  @override
  Schema? outputSchema(Schema? input) {
    final otherSchema = other._schema;
    if (input == null || otherSchema == null) return null;
    return _shape([input, otherSchema], (scans) => combine(scans[0], scans[1]));
  }

  static logical.JoinType _planJoin(JoinType t) {
    switch (t) {
      case JoinType.inner:
        return logical.JoinType.inner;
      case JoinType.left:
        return logical.JoinType.left;
      case JoinType.right:
        return logical.JoinType.right;
      case JoinType.full:
        return logical.JoinType.full;
      case JoinType.cross:
        return logical.JoinType.cross;
    }
  }
}

/// Aggregate operation: one SQL aggregate, grouped by [keys], whose outputs
/// are the keys and then each aggregated column under its input column's
/// name (`{'salary': 'sum'}` gives `salary`), each under the name given
/// (`groupBy(['d.name'])` gives `d.name`).
class AggregateOperation extends Operation {
  final Map<String, String> aggregations;

  /// The columns of the [GroupByOperation] before this one (none for a
  /// global aggregate).
  final List<String> keys;

  const AggregateOperation(this.aggregations, this.keys);

  @override
  logical.LogicalPlan lower(logical.LogicalPlan input) {
    final schema = input.schema;
    final references = [for (final k in keys) _named(schema, k)];
    final aggregate = logical.Aggregate(
      input: input,
      groupBy: references,
      aggregates: [
        for (final MapEntry(key: column, value: function)
            in aggregations.entries)
          logical.AggregateExpression(
            function: _function(function),
            argument: _named(schema, column),
          ),
      ],
      aliases: [...aggregations.keys],
    );
    // A key named by its side (`d.name`) comes out of the aggregate as its
    // bare name: the projection keeps the name given.
    if (keys.indexed.every((key) => references[key.$1].name == key.$2)) {
      return aggregate;
    }
    return logical.Projection(
      input: aggregate,
      expressions: [for (final f in aggregate.schema.fields) _logical(f)],
      aliases: [...keys, ...aggregations.keys],
    );
  }

  @override
  Schema? outputSchema(Schema? input) => _over(input, this);

  /// The SQL aggregate [function] names (`mean` is `AVG`). The type is the
  /// placeholder SQL's planner gives one; the physical builder types it from
  /// its argument.
  static logical.AggregateFunction _function(String function) =>
      logical.AggregateFunction(switch (function.toLowerCase()) {
        'count' => 'COUNT',
        'sum' => 'SUM',
        'avg' || 'mean' => 'AVG',
        'min' => 'MIN',
        'max' => 'MAX',
        _ => throw ArgumentError('Unsupported aggregation function: $function'),
      }, DataType.float64);
}

/// With column operation
class WithColumnOperation extends Operation {
  final String name;
  final String expression;

  const WithColumnOperation(this.name, this.expression);

  /// A projection of every input column under the name it reports, with the
  /// column [name] replaced in place by [expression], or appended when there
  /// is none.
  @override
  logical.LogicalPlan lower(logical.LogicalPlan input) {
    final schema = input.schema;
    final expressions = <logical.LogicalExpression>[
      for (final f in schema.fields) _logical(f),
    ];
    final aliases = _names(schema);
    final computed = _parse(expression);
    final at = _find(schema, name);
    if (at >= 0) {
      expressions[at] = computed;
    } else {
      expressions.add(computed);
      aliases.add(name);
    }
    return logical.Projection(
      input: input,
      expressions: expressions,
      aliases: aliases,
    );
  }

  @override
  Schema? outputSchema(Schema? input) => _over(input, this);
}

/// Drop columns operation: the others keep the names they report.
class DropOperation extends Operation {
  final List<String> columns;

  const DropOperation(this.columns);

  @override
  logical.LogicalPlan lower(logical.LogicalPlan input) {
    final schema = input.schema;
    final names = _names(schema);
    final dropped = {for (final c in columns) _find(schema, c)};
    final keep = [
      for (var i = 0; i < names.length; i++)
        if (!dropped.contains(i)) i,
    ];
    return logical.Projection(
      input: input,
      expressions: [for (final i in keep) _logical(schema.fields[i])],
      aliases: [for (final i in keep) names[i]],
    );
  }

  @override
  Schema? outputSchema(Schema? input) => _over(input, this);
}

/// Rename operation: the columns not renamed keep the names they report.
class RenameOperation extends Operation {
  final Map<String, String> renames;

  const RenameOperation(this.renames);

  @override
  logical.LogicalPlan lower(logical.LogicalPlan input) {
    final schema = input.schema;
    final names = _names(schema);
    final targets = {
      for (final MapEntry(:key, :value) in renames.entries)
        _find(schema, key): value,
    };
    return logical.Projection(
      input: input,
      expressions: [for (final f in schema.fields) _logical(f)],
      aliases: [for (var i = 0; i < names.length; i++) targets[i] ?? names[i]],
    );
  }

  @override
  Schema? outputSchema(Schema? input) => _over(input, this);
}

/// Distinct operation: deduplicate rows over all columns, as SQL's DISTINCT
/// (a group-by over every column, with typed value-equality keys, keeping
/// first-seen order).
class DistinctOperation extends Operation {
  const DistinctOperation();

  @override
  logical.LogicalPlan lower(logical.LogicalPlan input) =>
      logical.Distinct(input);
}

/// Union operation: SQL `UNION ALL`, matched by position. The result has
/// this frame's names and, per position, the common type of the two sides; a
/// different column count is a [Kind.schema] failure.
class UnionOperation extends Operation {
  final DataFrame other;

  const UnionOperation(this.other);

  /// The union of [input] with [branch], the logical plan of [other].
  logical.LogicalPlan combine(
    logical.LogicalPlan input,
    logical.LogicalPlan branch,
  ) => logical.Union([input, branch]);

  @override
  Schema? outputSchema(Schema? input) {
    final otherSchema = other._schema;
    if (input == null || otherSchema == null) return null;
    return _shape([input, otherSchema], (scans) => combine(scans[0], scans[1]));
  }
}

/// Sample operation
/// Takes EXACTLY `round(rowCount * fraction)` rows, uniformly and (with a
/// [seed]) reproducibly.
///
/// The exact count needs the row count, so the sample operator buffers its
/// whole input, under the memory budget and the byte cap on a materialized
/// intermediate. A streaming sampler would have to be Bernoulli (keep each
/// row with probability [fraction] independently), which yields only an
/// APPROXIMATE count, a different contract. A fraction outside [0, 1] is
/// [Kind.invalid].
class SampleOperation extends Operation {
  final double fraction;
  final int? seed;

  const SampleOperation(this.fraction, this.seed);

  @override
  logical.LogicalPlan lower(logical.LogicalPlan input) =>
      logical.Sample(input: input, fraction: fraction, seed: seed);
}
