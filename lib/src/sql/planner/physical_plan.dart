import 'dart:async';
import 'dart:io';
import 'dart:math' as math;
import 'dart:typed_data';
import '../../core/table.dart';
import '../../core/column.dart';
import '../../core/columnar.dart' show columnsFromRows;
import '../../core/conversions.dart'
    show
        decimalParts,
        formatDecimal,
        rescaleUnscaled,
        toUnscaled,
        unscaledFromString;
import '../../core/schema.dart';
import '../../core/data_type.dart';
import '../../core/keys.dart'
    show canonicalKey, partitionOf, RowKey, keyValueBytes;
import '../exec/kernels.dart';
import '../execution/engine_options.dart';
import '../execution/exec_core.dart';
import '../operators/spill.dart';
import 'logical_plan.dart';

// The execution-core contracts (PhysicalPlan / RecordBatchIterator / RecordBatch)
// now live in execution/exec_core.dart so `spill.dart` can depend on RecordBatch
// without importing this file (which imports spill.dart) — breaking the cycle.
// Re-exported so existing importers of this file are unaffected.
export '../execution/exec_core.dart'
    show
        PhysicalPlan,
        RecordBatchIterator,
        RecordBatch,
        ColumnListSchema,
        closeAll;

part 'physical_expression.dart';

/// Batch size for iterator processing
const int defaultBatchSize = 1024;

/// Coerce [value] — produced at [valueScale] by one branch of a multi-branch
/// expression (CASE / COALESCE) — to that expression's declared [type] and
/// [scale].
///
/// The planner widens the branches' TYPES to a common one, but a branch still
/// yields its own value: an int branch under a float64 result yields an `int`,
/// and a scale-2 branch under a scale-4 result yields the wrong magnitude. A
/// column whose values contradict its dtype is a lie the rest of the engine
/// trusts — a Parquet write rejects it, and `BigInt.from(0) != 0` splits one
/// decimal value across two DISTINCT groups.
Object? coerceToResult(
  Object? value,
  int valueScale,
  DataType type,
  int scale,
) {
  if (value == null) return null;
  if (type == DataType.float32 || type == DataType.float64) {
    return (value as num).toDouble();
  }
  if (type == DataType.decimal) {
    // A decimal branch is already unscaled — lift it to the common scale. A
    // raw int/double branch (e.g. `ELSE 0`) is scaled from its natural scale.
    if (value is BigInt) {
      return valueScale == scale
          ? value
          : value * BigInt.from(10).pow(scale - valueScale);
    }
    return toUnscaled(value as num, scale);
  }
  // A string common type (`CASE`/`COALESCE` mixing a string with a non-string,
  // e.g. `COALESCE(int_col, 'none')`) must render the non-string branch to its
  // string form — storing the raw int under a string-typed column is the same
  // dtype lie a Parquet write rejects and the compare kernel trips on.
  if (type == DataType.string && value is! String) {
    return value.toString();
  }
  return value;
}

/// Table scan physical operator
class PhysicalTableScan extends PhysicalPlan {
  final String tableName;
  final Table table;
  final List<String>? projection;
  final String? alias;
  final int batchSize;

  PhysicalTableScan({
    required this.tableName,
    required this.table,
    this.projection,
    this.alias,
    this.batchSize = defaultBatchSize,
  });

  @override
  Schema get schema {
    final base = projection != null
        ? table.schema.select(projection!)
        : table.schema;
    return alias != null ? base.qualify(alias!) : base;
  }

  @override
  int? get estimatedRows => table.rowCount;

  @override
  RecordBatchIterator execute() {
    return TableScanIterator(
      table: table,
      projection: projection,
      alias: alias,
      batchSize: batchSize,
    );
  }
}

/// Scans literal rows — `VALUES (1), (2)`, and the one-row source a `SELECT`
/// with no FROM sits on.
///
/// Cells are folded to constants at execution time. Nothing here depends on a
/// row, so each cell is evaluated against an empty batch: literals ignore it,
/// and a cell that does reference a column fails loud rather than quietly
/// yielding null. A VALUES list is written out by hand in the query, so it is
/// always small enough for a single batch.
class PhysicalValues extends PhysicalPlan {
  final List<List<PhysicalExpression>> rows;
  final Schema valueSchema;

  PhysicalValues({required this.rows, required this.valueSchema});

  @override
  Schema get schema => valueSchema;

  @override
  int? get estimatedRows => rows.length;

  @override
  RecordBatchIterator execute() =>
      ValuesIterator(rows: rows, valueSchema: valueSchema);
}

class ValuesIterator extends RecordBatchIterator {
  final List<List<PhysicalExpression>> rows;
  final Schema valueSchema;
  bool _done = false;

  ValuesIterator({required this.rows, required this.valueSchema});

  @override
  Schema get schema => valueSchema;

  @override
  Future<RecordBatch?> next() async {
    if (_done) return null;
    _done = true;

    // One synthetic row, no columns: constant cells evaluate through the
    // vectorized kernels on a 1-row slice (a 0-row batch yields 0-length
    // columns with no element to read), and a column REFERENCE still fails
    // loud because there is no column to resolve.
    final empty = RecordBatch(
      columns: const [],
      schema: valueSchema,
      rowCount: 1,
    );
    final names = [for (final field in valueSchema.fields) field.name];
    final materialized = <Map<String, dynamic>>[];
    for (final row in rows) {
      final values = <String, dynamic>{};
      for (int i = 0; i < row.length; i++) {
        values[names[i]] = row[i].evaluate(empty, 0);
      }
      materialized.add(values);
    }

    final columns = columnsFromRows(materialized, valueSchema, strict: true);
    return RecordBatch(
      columns: columns,
      schema: valueSchema,
      rowCount: materialized.length,
    );
  }

  @override
  Future<void> close() async {}
}

class TableScanIterator extends RecordBatchIterator {
  final Table table;
  final List<String>? projection;
  final String? alias;
  final int batchSize;
  int _currentOffset = 0;

  TableScanIterator({
    required this.table,
    this.projection,
    this.alias,
    required this.batchSize,
  });

  @override
  Schema get schema {
    final base = projection != null
        ? table.schema.select(projection!)
        : table.schema;
    return alias != null ? base.qualify(alias!) : base;
  }

  @override
  Future<RecordBatch?> next() async {
    if (_currentOffset >= table.rowCount) {
      return null;
    }

    final endOffset = (_currentOffset + batchSize).clamp(0, table.rowCount);
    final batchRowCount = endOffset - _currentOffset;

    // Get columns for this batch
    final batchColumns = <Column>[];
    final columnsToRead = projection ?? table.schema.fieldNames;

    // Special case: If no columns are needed (e.g., COUNT(*)), return empty columns
    if (columnsToRead.isEmpty && projection != null) {
      _currentOffset = endOffset;
      return RecordBatch(columns: [], schema: schema, rowCount: batchRowCount);
    }

    for (final columnName in columnsToRead) {
      final column = table.column(columnName);
      if (column != null) {
        // Use the efficient slice method
        final slicedColumn = column.slice(_currentOffset, endOffset);
        batchColumns.add(slicedColumn);
      }
    }

    _currentOffset = endOffset;

    return RecordBatch(
      columns: batchColumns,
      schema: schema,
      rowCount: batchRowCount,
    );
  }

  @override
  Future<void> close() async {
    // No resources to release for in-memory table scan
  }
}

/// Re-tags a derived relation's output columns with a single table [alias] as
/// their qualifier (like a scan's alias), giving it a clean scope so the outer
/// query can address `alias.col`.
class PhysicalAlias extends PhysicalPlan {
  final PhysicalPlan input;
  final String alias;

  PhysicalAlias({required this.input, required this.alias});

  @override
  Schema get schema => input.schema.qualify(alias);

  @override
  int? get estimatedRows => input.estimatedRows;

  @override
  RecordBatchIterator execute() => _AliasIterator(input.execute(), schema);
}

class _AliasIterator extends RecordBatchIterator {
  final RecordBatchIterator input;
  final Schema _schema;

  _AliasIterator(this.input, this._schema);

  @override
  Schema get schema => _schema;

  @override
  Future<RecordBatch?> next() async {
    final batch = await input.next();
    if (batch == null) return null;
    return RecordBatch(
      columns: batch.columns,
      schema: _schema,
      rowCount: batch.rowCount,
    );
  }

  @override
  Future<void> close() => input.close();
}

/// Filter physical operator
class PhysicalFilter extends PhysicalPlan {
  final PhysicalPlan input;
  final PhysicalExpression predicate;

  PhysicalFilter({required this.input, required this.predicate});

  @override
  Schema get schema => input.schema;

  @override
  int? get estimatedRows => input.estimatedRows != null
      ? (input.estimatedRows! * 0.3)
            .round() // Assume 30% selectivity
      : null;

  @override
  RecordBatchIterator execute() {
    return FilterIterator(input: input.execute(), predicate: predicate);
  }
}

class FilterIterator extends RecordBatchIterator {
  final RecordBatchIterator input;
  final PhysicalExpression predicate;

  FilterIterator({required this.input, required this.predicate});

  @override
  Schema get schema => input.schema;

  @override
  Future<RecordBatch?> next() async {
    while (true) {
      final batch = await input.next();
      if (batch == null) return null;

      // Vectorized predicate evaluation -> selection vector -> typed gather.
      final mask = predicate.evalMask(batch);
      final selected = mask.selected();
      if (selected.length == batch.rowCount) {
        return batch; // all rows pass, no copy
      }
      if (selected.isNotEmpty) {
        return batch.gather(selected);
      }
      // If no rows passed the filter, try next batch
    }
  }

  @override
  Future<void> close() async {
    await input.close();
  }
}

/// Projection physical operator
class PhysicalProjection extends PhysicalPlan {
  final PhysicalPlan input;
  final List<PhysicalExpression> expressions;
  final List<String> aliases;

  PhysicalProjection({
    required this.input,
    required this.expressions,
    required this.aliases,
  });

  @override
  Schema get schema {
    final fields = <Field>[];
    for (int i = 0; i < expressions.length; i++) {
      fields.add(
        Field(
          name: aliases[i],
          dataType: expressions[i].dataType,
          nullable: true,
          // Carry decimal precision/scale so a computed decimal column keeps its
          // scale (else the unscaled BigInt is misread as scale 0), and nested
          // children so a passed-through list/struct/map column keeps its shape
          // (the spill writer needs it).
          precision: expressions[i].precision,
          scale: expressions[i].scale,
          children: expressions[i].children,
        ),
      );
    }
    return Schema(fields: fields);
  }

  @override
  int? get estimatedRows => input.estimatedRows;

  @override
  RecordBatchIterator execute() {
    return ProjectionIterator(
      input: input.execute(),
      expressions: expressions,
      aliases: aliases,
    );
  }
}

class ProjectionIterator extends RecordBatchIterator {
  final RecordBatchIterator input;
  final List<PhysicalExpression> expressions;
  final List<String> aliases;
  late final Schema _schema;

  ProjectionIterator({
    required this.input,
    required this.expressions,
    required this.aliases,
  }) {
    final fields = <Field>[];
    for (int i = 0; i < expressions.length; i++) {
      fields.add(
        Field(
          name: aliases[i],
          dataType: expressions[i].dataType,
          nullable: true,
          // Carry decimal precision/scale so a computed decimal column keeps its
          // scale through the emitted batches (else it's misread as scale 0).
          precision: expressions[i].precision,
          scale: expressions[i].scale,
        ),
      );
    }
    _schema = Schema(fields: fields);
  }

  @override
  Schema get schema => _schema;

  @override
  Future<RecordBatch?> next() async {
    final batch = await input.next();
    if (batch == null) return null;

    final projectedColumns = <Column>[];

    for (int i = 0; i < expressions.length; i++) {
      final column = expressions[i].evalColumn(batch);
      projectedColumns.add(Kernels.rename(column, aliases[i]));
    }

    return RecordBatch(
      columns: projectedColumns,
      schema: schema,
      rowCount: batch.rowCount,
    );
  }

  @override
  Future<void> close() async {
    await input.close();
  }
}

/// Hash join physical operator
class PhysicalHashJoin extends PhysicalPlan {
  final PhysicalPlan left;
  final PhysicalPlan right;
  // Equi-join keys, one entry per ANDed `left = right` equality (a single-column
  // join is the common one-entry case; multi-column USING and ANDed ON produce
  // several). The four lists are PARALLEL. Empty means no key — a cross join.
  final List<String> leftKeys;
  final List<String> rightKeys;
  final List<String?> leftQualifiers;
  final List<String?> rightQualifiers;
  final JoinType joinType;
  final EngineOptions options;

  PhysicalHashJoin({
    required this.left,
    required this.right,
    required this.leftKeys,
    required this.rightKeys,
    List<String?>? leftQualifiers,
    List<String?>? rightQualifiers,
    required this.joinType,
    this.options = const EngineOptions(),
  }) : leftQualifiers = leftQualifiers ?? List.filled(leftKeys.length, null),
       rightQualifiers = rightQualifiers ?? List.filled(rightKeys.length, null);

  /// Right-side key fields to drop (kept once from the left). Only de-dups a key
  /// column that would actually collide — same name AND same qualifier (e.g. an
  /// unqualified in-memory join, or USING). A qualified cross-file `a.id = b.id`
  /// keeps both `a.id` and `b.id`, so chained joins can still reference either.
  static Set<int> dropIndices(
    Schema right,
    List<String> leftKeys,
    List<String> rightKeys,
    List<String?> leftQualifiers,
    List<String?> rightQualifiers,
  ) {
    final drop = <int>{};
    for (int j = 0; j < leftKeys.length; j++) {
      if (leftKeys[j] == rightKeys[j] &&
          leftQualifiers[j] == rightQualifiers[j]) {
        final index = right.resolve(
          rightKeys[j],
          qualifier: rightQualifiers[j],
        );
        if (index >= 0) drop.add(index);
      }
    }
    return drop;
  }

  /// Each key column's decimal scale, or null when it isn't decimal — see
  /// [scaledKey]. A key that doesn't resolve is caught later by the batch lookup.
  static List<int?> keyScales(
    Schema schema,
    List<String> keys,
    List<String?> qualifiers,
  ) => [
    for (int j = 0; j < keys.length; j++)
      () {
        final index = schema.resolve(keys[j], qualifier: qualifiers[j]);
        if (index < 0) return null;
        final field = schema.fields[index];
        return field.dataType == DataType.decimal ? field.scale : null;
      }(),
  ];

  /// The joined schema, widening the NULL-PRODUCING side's fields to nullable.
  ///
  /// An outer join manufactures nulls for unmatched rows (`Kernels.gather`
  /// null-pads), so a field the source declared non-nullable is no longer. A
  /// Parquet column that merely happened to hold no nulls comes back `required`,
  /// and copying that through made `Table._validate` reject the join's own
  /// output — "contains nulls but is declared non-nullable" — instead of
  /// returning rows.
  static Schema joinSchema(
    Schema left,
    Schema right,
    Set<int> drop,
    JoinType joinType,
  ) {
    // A semi/anti join FILTERS the left side by whether the right matched; it
    // emits no right column, so there is nothing to pad or drop.
    if (semiLike(joinType)) return left;
    // LEFT keeps every left row and nulls the right; RIGHT is the mirror; FULL
    // does both. INNER/CROSS emit only matched pairs, so nothing is padded.
    final nullLeft = joinType == JoinType.right || joinType == JoinType.full;
    final nullRight = joinType == JoinType.left || joinType == JoinType.full;
    final fields = <Field>[
      for (final field in left.fields)
        nullLeft ? field.copyWith(nullable: true) : field,
    ];
    for (int j = 0; j < right.fields.length; j++) {
      if (drop.contains(j)) continue;
      final field = right.fields[j];
      fields.add(nullRight ? field.copyWith(nullable: true) : field);
    }
    return Schema(fields: fields);
  }

  @override
  Schema get schema => joinSchema(
    left.schema,
    right.schema,
    dropIndices(
      right.schema,
      leftKeys,
      rightKeys,
      leftQualifiers,
      rightQualifiers,
    ),
    joinType,
  );

  @override
  int? get estimatedRows {
    final leftRows = left.estimatedRows;
    final rightRows = right.estimatedRows;
    if (leftRows == null || rightRows == null) return null;

    // Estimate based on join type. Exhaustive on purpose: a `default` here once
    // let the semi/anti types estimate null instead of failing to compile.
    switch (joinType) {
      case JoinType.inner:
        return (leftRows * rightRows * 0.1).round(); // Assume 10% match rate
      case JoinType.left:
        return leftRows;
      case JoinType.right:
        return rightRows;
      case JoinType.full:
        return leftRows + rightRows;
      case JoinType.cross:
        return leftRows * rightRows;
      // A semi/anti join filters the left side, so it is bounded by it — never
      // the product. Same 10% match rate as inner, and its complement.
      case JoinType.semi:
        return (leftRows * 0.1).round();
      case JoinType.anti:
      case JoinType.antiNullAware:
        return (leftRows * 0.9).round();
    }
  }

  @override
  RecordBatchIterator execute() {
    return HashJoinIterator(
      left: left.execute(),
      right: right.execute(),
      leftKeys: leftKeys,
      rightKeys: rightKeys,
      leftQualifiers: leftQualifiers,
      rightQualifiers: rightQualifiers,
      joinType: joinType,
      options: options,
    );
  }
}

class HashJoinIterator extends RecordBatchIterator {
  final RecordBatchIterator left;
  final RecordBatchIterator right;
  final List<String> leftKeys;
  final List<String> rightKeys;
  final List<String?> leftQualifiers;
  final List<String?> rightQualifiers;
  final JoinType joinType;
  final EngineOptions options;

  late final Schema _schema;
  late final Set<int> _drop;
  // The key columns' decimal scales, or null when they aren't decimal. Taken
  // from the PLAN's schemas, once: a spilled batch's schema has lost them
  // (schemaOf rebuilds fields from dataTypes alone). Both sides are needed
  // SEPARATELY — the whole defect is that the two scales can differ.
  late final List<int?> _leftScales;
  late final List<int?> _rightScales;
  StreamIterator<RecordBatch>? _output;
  final List<SpillRun> _runs = [];
  // Grace-partition writers open but not yet finalized into [_runs]; tracked so
  // close() can abort+delete them if the join is cancelled or throws during the
  // build/probe-partition phase (otherwise their fds + temp files leak).
  final List<SpillWriter> _writers = [];
  // Shared-budget reservation for the in-memory build (right) buffer.
  int _reserved = 0;
  // Running emitted-row count for the CROSS JOIN maxOutputRows guard.
  int _crossEmitted = 0;
  // Running emitted-row count for the equi/hash-join maxOutputRows guard.
  int _probeEmitted = 0;
  // Empty, real-typed template columns for null-side gathers (so unmatched-row
  // batches have the same column types as matched batches across partitions).
  List<Column>? _leftEmpty;
  List<Column>? _rightEmpty;

  HashJoinIterator({
    required this.left,
    required this.right,
    required this.leftKeys,
    required this.rightKeys,
    required this.leftQualifiers,
    required this.rightQualifiers,
    required this.joinType,
    this.options = const EngineOptions(),
  }) {
    _drop = PhysicalHashJoin.dropIndices(
      right.schema,
      leftKeys,
      rightKeys,
      leftQualifiers,
      rightQualifiers,
    );
    _schema = PhysicalHashJoin.joinSchema(
      left.schema,
      right.schema,
      _drop,
      joinType,
    );
    _leftScales = PhysicalHashJoin.keyScales(
      left.schema,
      leftKeys,
      leftQualifiers,
    );
    _rightScales = PhysicalHashJoin.keyScales(
      right.schema,
      rightKeys,
      rightQualifiers,
    );
    _reconcileScales();
  }

  /// Reconcile a key position that is DECIMAL on one side and an INTEGER on the
  /// other: give the integer side scale 0 so [_keyAt] wraps its value as a
  /// [Decimal] too. A raw `int` and a `Decimal` never hash-equal, so without this
  /// every `int = decimal` match silently drops; once both are Decimals,
  /// `Decimal`'s cross-scale `==` matches `5` with `5.00`. Decimal/decimal (both
  /// non-null) and integer/integer (both null) positions are left untouched.
  void _reconcileScales() {
    for (int j = 0; j < _leftScales.length; j++) {
      final leftDecimal = _leftScales[j] != null;
      final rightDecimal = _rightScales[j] != null;
      if (leftDecimal == rightDecimal) continue; // both or neither: no lift
      if (leftDecimal &&
          _isInteger(right.schema, rightKeys[j], rightQualifiers[j])) {
        _rightScales[j] = 0;
      } else if (rightDecimal &&
          _isInteger(left.schema, leftKeys[j], leftQualifiers[j])) {
        _leftScales[j] = 0;
      }
    }
  }

  static bool _isInteger(Schema schema, String key, String? qualifier) {
    final index = schema.resolve(key, qualifier: qualifier);
    return index >= 0 && schema.fields[index].dataType.isInteger;
  }

  @override
  Schema get schema => _schema;

  String get _spillDir => options.spillDir ?? Directory.systemTemp.path;

  /// The key columns of [b] for one side, in key order.
  List<Column> _keyCols(
    RecordBatch b,
    List<String> keys,
    List<String?> qualifiers,
  ) => [
    for (int j = 0; j < keys.length; j++)
      b.column(keys[j], qualifier: qualifiers[j])!,
  ];

  /// The join key for one row: a single canonicalized value for a one-column key
  /// (the common, fast case) or a [RowKey] over the columns for a composite key.
  /// Null if ANY key column is null — a null never equi-matches, so such a row is
  /// treated exactly as the old single-null-key case (no match; emitted only as
  /// an unmatched-outer row). [canonicalKey] makes a binary/list key compare by
  /// CONTENT and a decimal by VALUE (its scale differs per side).
  Object? _keyAt(List<Column> cols, List<int?> scales, int row) {
    if (cols.length == 1) {
      final v = cols[0][row];
      return v == null ? null : canonicalKey(v, scale: scales[0]);
    }
    final parts = <Object?>[];
    for (int j = 0; j < cols.length; j++) {
      final v = cols[j][row];
      if (v == null) return null;
      parts.add(canonicalKey(v, scale: scales[j]));
    }
    return RowKey(parts);
  }

  @override
  Future<RecordBatch?> next() async {
    final output = _output ??= StreamIterator(_generate());
    return (await output.moveNext()) ? output.current : null;
  }

  Stream<RecordBatch> _generate() async* {
    if (joinType == JoinType.cross) {
      yield* _crossJoin();
      return;
    }

    // Drain the build (right) side, buffering until the budget is exceeded; past
    // that, grace-partition both sides by key hash so each partition fits.
    final rightBuffer = <RecordBatch>[];
    var bytes = 0;
    List<SpillWriter>? rightParts;
    List<SpillWriter>? leftParts;
    var partitions = 0;
    RecordBatch? batch;
    // Shared budget applies only when spilling is enabled (forces grace
    // partitioning earlier when the global pool is exhausted).
    final mem = options.spills ? options.memory : null;

    // `x NOT IN (S)` is UNKNOWN for EVERY row when S holds a null — a GLOBAL
    // property that can't be decided per grace partition, so track it while the
    // whole build side is consumed and short-circuit below.
    var buildHasNull = false;

    while ((batch = await right.next()) != null) {
      _rightEmpty ??= [for (final c in batch!.columns) c.slice(0, 0)];
      if (joinType == JoinType.antiNullAware && !buildHasNull) {
        final cols = _keyCols(batch!, rightKeys, rightQualifiers);
        for (int i = 0; i < batch.rowCount; i++) {
          if (_keyAt(cols, _rightScales, i) == null) {
            buildHasNull = true;
            break;
          }
        }
      }
      final mu = batch!.memoryUsage;
      if (rightParts == null) {
        final localOver =
            options.spills && bytes + mu > options.spillThresholdBytes;
        final globalOver = !localOver && mem != null && !mem.reserve(mu);
        if (!localOver && !globalOver && mem != null) _reserved += mu;
        if (localOver || globalOver) {
          partitions = math.max(
            16,
            (bytes ~/ options.spillThresholdBytes + 1) * 2,
          );
          // Create the spill dir once, off the hot path.
          await Directory(_spillDir).create(recursive: true);
          // The build (right) partition is read fully into the hash table → Arrow
          // when types allow; the left/probe partition is streamed during
          // probing, so it stays Parquet (lazy read).
          rightParts = [
            for (int i = 0; i < partitions; i++)
              SpillWriter(
                _spillDir,
                _rightEmpty!.schemaOf(template: right.schema),
                format: SpillFormat.arrow,
                budget: options.spillBudget,
              ),
          ];
          leftParts = [
            for (int i = 0; i < partitions; i++)
              SpillWriter(_spillDir, left.schema, budget: options.spillBudget),
          ];
          _writers
            ..addAll(rightParts)
            ..addAll(leftParts); // track until finished into _runs
          for (final b in rightBuffer) {
            await _route(
              b,
              rightParts,
              partitions,
              rightKeys,
              rightQualifiers,
              _rightScales,
            );
          }
          rightBuffer.clear();
          bytes = 0;
          mem?.release(_reserved); // buffered build rows are on disk now
          _reserved = 0;
          await _route(
            batch,
            rightParts,
            partitions,
            rightKeys,
            rightQualifiers,
            _rightScales,
          );
          continue;
        }
        rightBuffer.add(batch);
        bytes += mu;
      } else {
        await _route(
          batch,
          rightParts,
          partitions,
          rightKeys,
          rightQualifiers,
          _rightScales,
        );
      }
    }

    // NOT IN is null-aware over the WHOLE build side (see [buildHasNull]): an
    // empty build makes it TRUE for every left row (emit all, even null keys); a
    // build containing a null makes it UNKNOWN for every row (emit none). Neither
    // is decidable per partition, so resolve both here, once the build is fully
    // consumed. Otherwise it reduces to a plain anti join, which IS per-partition.
    if (joinType == JoinType.antiNullAware) {
      if (rightParts == null && rightBuffer.isEmpty) {
        yield* _emitAllLeft(_streamOf(left));
        return;
      }
      if (buildHasNull) return;
    }

    if (rightParts == null) {
      // Build fits the budget: hash it once, stream-probe the left side.
      final rightBuild = rightBuffer.isEmpty
          ? null
          : RecordBatch.concatAll(rightBuffer, rightBuffer.first.schema);
      yield* _buildAndProbe(rightBuild, _streamOf(left));
      return;
    }

    // Grace: partition the left (probe) side too, then join partition pairs.
    while ((batch = await left.next()) != null) {
      _leftEmpty ??= [for (final c in batch!.columns) c.slice(0, 0)];
      await _route(
        batch!,
        leftParts!,
        partitions,
        leftKeys,
        leftQualifiers,
        _leftScales,
      );
    }
    final rightRuns = [for (final w in rightParts) await w.finish()];
    final leftRuns = [for (final w in leftParts!) await w.finish()];
    _runs
      ..addAll(rightRuns)
      ..addAll(leftRuns);
    _writers.clear(); // all finished → now owned by _runs

    for (int p = 0; p < partitions; p++) {
      yield* _joinPartition(rightRuns[p], leftRuns[p], 0);
    }
  }

  static const _maxJoinRepartitionDepth = 6;

  /// A build partition under this absolute size is always built in memory, even
  /// when the (possibly tiny) spill budget is smaller — recursion is a skew
  /// mitigation for genuinely large partitions, not a hard fit-the-budget rule.
  static const _minJoinBuildBytes = 1 << 20; // 1 MB

  /// Join one build/probe partition pair. If the build side fits the budget it's
  /// hashed and probed directly; if a skewed partition still exceeds it, BOTH
  /// sides are re-partitioned with the same salted hash (so matching keys
  /// co-locate) and joined recursively. At the recursion cap — where salted
  /// repartitioning can no longer shrink an irreducible single-key partition —
  /// it falls back to a memory-bounded block-nested-loop rather than
  /// materializing the whole partition (which would OOM on a hot key).
  Stream<RecordBatch> _joinPartition(
    SpillRun rightRun,
    SpillRun leftRun,
    int depth,
  ) async* {
    final buildBudget = options.spillThresholdBytes > _minJoinBuildBytes
        ? options.spillThresholdBytes
        : _minJoinBuildBytes;
    final canRepartition = depth < _maxJoinRepartitionDepth;

    // A semi/anti build is a DISTINCT-KEY set. It is usually far smaller than the
    // rows (a hot key costs one entry), but a high-cardinality partition can
    // still exceed the budget — so meter it byte-by-byte and repartition (not the
    // block-nested-loop, whose per-block membership is wrong for semi/anti); fail
    // loud at the recursion cap rather than grow unbounded.
    if (semiLike(joinType)) {
      final keys = <Object?>{};
      var keyBytes = 0;
      var reserved = 0;
      final mem = options.spills ? options.memory : null;
      var over = false;
      await for (final b in rightRun.read()) {
        final cols = _keyCols(b, rightKeys, rightQualifiers);
        for (int j = 0; j < b.rowCount; j++) {
          final k = _keyAt(cols, _rightScales, j);
          if (k == null || !keys.add(k)) continue;
          final kb = keyValueBytes(k);
          keyBytes += kb;
          final granted = mem == null || mem.reserve(kb);
          if (granted && mem != null) reserved += kb;
          if (keyBytes > buildBudget || !granted) {
            over = true;
            break;
          }
        }
        if (over) break;
      }
      if (!over) {
        try {
          yield* _probeSet(keys, leftRun.read());
        } finally {
          mem?.release(reserved);
        }
        return;
      }
      // Over budget: drop the partial set, then repartition below (or fail loud
      // at the cap — salted repartitioning can no longer shrink it).
      mem?.release(reserved);
      if (!canRepartition) {
        throw StateError(
          'semi/anti join distinct-key set exceeds the memory budget at the '
          'repartition cap',
        );
      }
      // fall through to the shared repartition tail.
    } else {
      final partBatches = <RecordBatch>[];
      var bytes = 0;
      var overBudget = false;
      await for (final b in rightRun.read()) {
        partBatches.add(b);
        bytes += b.memoryUsage;
        // Detect over-budget even at the cap so we stop short of reading (and
        // materializing) the whole partition and take the streaming fallback.
        if (bytes > buildBudget) {
          overBudget = true;
          break;
        }
      }
      if (!overBudget) {
        final rightBuild = partBatches.isEmpty
            ? null
            : RecordBatch.concatAll(partBatches, partBatches.first.schema);
        yield* _buildAndProbe(rightBuild, leftRun.read());
        return;
      }
      partBatches.clear(); // release the partial build before re-partitioning

      if (!canRepartition) {
        // Irreducible skew at the recursion cap: stream the build side in bounded
        // blocks instead of concatenating it all into memory.
        yield* _blockNestedLoopJoin(rightRun, leftRun, buildBudget);
        return;
      }
    }
    const fanout = 16;
    final salt = depth + 1;
    final rSchema = (_rightEmpty ?? _emptyCols(right.schema)).schemaOf(
      template: right.schema,
    );
    final lSchema = (_leftEmpty ?? _emptyCols(left.schema)).schemaOf(
      template: left.schema,
    );

    await Directory(_spillDir).create(recursive: true);
    final rsub = _newParts(fanout, rSchema, SpillFormat.arrow);
    await for (final b in rightRun.read()) {
      await _route(
        b,
        rsub,
        fanout,
        rightKeys,
        rightQualifiers,
        _rightScales,
        salt: salt,
      );
    }
    final lsub = _newParts(fanout, lSchema, SpillFormat.parquet);
    await for (final b in leftRun.read()) {
      await _route(
        b,
        lsub,
        fanout,
        leftKeys,
        leftQualifiers,
        _leftScales,
        salt: salt,
      );
    }

    final rRuns = <SpillRun>[];
    for (final w in rsub) {
      final r = await w.finish();
      _writers.remove(w);
      _runs.add(r);
      rRuns.add(r);
    }
    final lRuns = <SpillRun>[];
    for (final w in lsub) {
      final r = await w.finish();
      _writers.remove(w);
      _runs.add(r);
      lRuns.add(r);
    }
    for (int p = 0; p < fanout; p++) {
      yield* _joinPartition(rRuns[p], lRuns[p], depth + 1);
      await rRuns[p].delete();
      _runs.remove(rRuns[p]);
      await lRuns[p].delete();
      _runs.remove(lRuns[p]);
    }
  }

  /// Allocate [n] tracked spill partition writers of [schema].
  List<SpillWriter> _newParts(int n, Schema schema, SpillFormat format) {
    final parts = [
      for (int i = 0; i < n; i++)
        SpillWriter(
          _spillDir,
          schema,
          format: format,
          budget: options.spillBudget,
        ),
    ];
    _writers.addAll(parts);
    return parts;
  }

  /// Hash [rightBuild] by key, then probe a stream of left batches, emitting one
  /// output batch per left batch plus (for outer joins) the unmatched rows.
  ///
  /// [forceUnmatchedLeft] overrides whether unmatched-left rows are emitted here
  /// (the block-nested-loop fallback suppresses them per block and emits them
  /// once in a separate pass); [collectKeys], when given, receives this build's
  /// distinct keys so that pass knows which left rows never matched.
  Stream<RecordBatch> _buildAndProbe(
    RecordBatch? rightBuild,
    Stream<RecordBatch> leftStream, {
    bool? forceUnmatchedLeft,
    Set<Object?>? collectKeys,
  }) async* {
    // A semi/anti join emits LEFT rows only, by membership in the build's
    // distinct key set — not paired rows. (antiNullAware's global empty/null
    // cases are resolved in _generate before reaching here.)
    if (semiLike(joinType)) {
      final keys = <Object?>{};
      if (rightBuild != null) {
        final cols = _keyCols(rightBuild, rightKeys, rightQualifiers);
        for (int j = 0; j < rightBuild.rowCount; j++) {
          final k = _keyAt(cols, _rightScales, j);
          if (k != null) keys.add(k); // a null key matches nothing
        }
      }
      yield* _probeSet(keys, leftStream);
      return;
    }

    final rn = rightBuild?.rowCount ?? 0;
    final rightKeyCols = rightBuild == null
        ? null
        : _keyCols(rightBuild, rightKeys, rightQualifiers);
    final keyToRows = <Object?, List<int>>{};
    for (int j = 0; j < rn; j++) {
      // A null (any null key part) never equi-matches, so it is not indexed.
      final k = _keyAt(rightKeyCols!, _rightScales, j);
      if (k != null) {
        keyToRows.putIfAbsent(k, () => []).add(j);
      }
    }
    if (collectKeys != null) collectKeys.addAll(keyToRows.keys);
    final emitUnmatchedRight =
        joinType == JoinType.right || joinType == JoinType.full;
    final matchedRight = emitUnmatchedRight
        ? List<bool>.filled(rn, false)
        : null;
    final emitUnmatchedLeft =
        forceUnmatchedLeft ??
        (joinType == JoinType.left || joinType == JoinType.full);
    final rightCols =
        rightBuild?.columns ?? _rightEmpty ?? _emptyCols(right.schema);

    final max = options.maxOutputRows;
    await for (final leftBatch in leftStream) {
      _leftEmpty ??= [for (final c in leftBatch.columns) c.slice(0, 0)];
      final leftKeyCols = _keyCols(leftBatch, leftKeys, leftQualifiers);
      final leftIdx = <int>[];
      final rightIdx = <int>[];
      for (int i = 0; i < leftBatch.rowCount; i++) {
        final k = _keyAt(leftKeyCols, _leftScales, i);
        final matches = k == null ? null : keyToRows[k];
        if (matches == null) {
          if (emitUnmatchedLeft) {
            leftIdx.add(i);
            rightIdx.add(-1);
          }
        } else {
          for (final j in matches) {
            leftIdx.add(i);
            rightIdx.add(j);
            if (matchedRight != null) matchedRight[j] = true;
            // Flush INSIDE the match loop: a single hot key can produce far more
            // than one batch of pairs, so buffering the whole left batch's
            // matches would allocate the full (skewed) product before yielding.
            if (leftIdx.length >= defaultBatchSize) {
              if (max != null && _probeEmitted + leftIdx.length > max) {
                _probeOverflow(max);
              }
              _probeEmitted += leftIdx.length;
              yield _assemble(leftBatch.columns, rightCols, leftIdx, rightIdx);
              leftIdx.clear();
              rightIdx.clear();
            }
          }
        }
      }
      if (leftIdx.isNotEmpty) {
        if (max != null && _probeEmitted + leftIdx.length > max) {
          _probeOverflow(max);
        }
        _probeEmitted += leftIdx.length;
        yield _assemble(leftBatch.columns, rightCols, leftIdx, rightIdx);
      }
    }

    if (matchedRight != null) {
      final leftCols = _leftEmpty ?? _emptyCols(left.schema);
      final leftIdx = <int>[];
      final rightIdx = <int>[];
      for (int j = 0; j < rn; j++) {
        if (!matchedRight[j]) {
          leftIdx.add(-1);
          rightIdx.add(j);
          if (leftIdx.length >= defaultBatchSize) {
            if (max != null && _probeEmitted + leftIdx.length > max) {
              _probeOverflow(max);
            }
            _probeEmitted += leftIdx.length;
            yield _assemble(leftCols, rightCols, leftIdx, rightIdx);
            leftIdx.clear();
            rightIdx.clear();
          }
        }
      }
      if (leftIdx.isNotEmpty) {
        if (max != null && _probeEmitted + leftIdx.length > max) {
          _probeOverflow(max);
        }
        _probeEmitted += leftIdx.length;
        yield _assemble(leftCols, rightCols, leftIdx, rightIdx);
      }
    }
  }

  Never _probeOverflow(int max) =>
      throw StateError('JOIN exceeds maxOutputRows=$max');

  /// Whether a semi/anti join emits a left row, given whether its key is null
  /// and whether it is a member of the build's distinct key set.
  ///
  /// SEMI (IN / EXISTS): a match exists → emit; a null key matches nothing.
  /// ANTI (NOT EXISTS): NO match → emit, INCLUDING a null key (which matches
  /// nothing, so no matching row exists). antiNullAware (NOT IN): a null key is
  /// UNKNOWN, so NOT emitted; its global empty/null cases are handled in
  /// _generate, leaving the plain non-null-not-in-build case here.
  bool _emitLeft(bool isNull, bool inBuild) => switch (joinType) {
    JoinType.semi => !isNull && inBuild,
    JoinType.anti => isNull || !inBuild,
    JoinType.antiNullAware => !isNull && !inBuild,
    _ => throw StateError('not a semi/anti join: $joinType'),
  };

  /// Probe [leftStream] against a build's distinct key set, emitting LEFT rows
  /// only (a semi/anti join's schema is the left schema), per [_emitLeft].
  Stream<RecordBatch> _probeSet(
    Set<Object?> buildKeys,
    Stream<RecordBatch> leftStream,
  ) async* {
    final max = options.maxOutputRows;
    await for (final leftBatch in leftStream) {
      _leftEmpty ??= [for (final c in leftBatch.columns) c.slice(0, 0)];
      final leftKeyCols = _keyCols(leftBatch, leftKeys, leftQualifiers);
      final idx = <int>[];
      for (int i = 0; i < leftBatch.rowCount; i++) {
        final k = _keyAt(leftKeyCols, _leftScales, i);
        if (_emitLeft(k == null, k != null && buildKeys.contains(k))) {
          idx.add(i);
        }
      }
      if (idx.isEmpty) continue;
      if (max != null && _probeEmitted + idx.length > max) {
        _probeOverflow(max);
      }
      _probeEmitted += idx.length;
      yield _assembleLeft(leftBatch.columns, idx);
    }
  }

  /// Emit every left row (a semi/anti join's schema is the left schema). Used for
  /// `x NOT IN (empty)`, which is TRUE for every row.
  Stream<RecordBatch> _emitAllLeft(Stream<RecordBatch> leftStream) async* {
    final max = options.maxOutputRows;
    await for (final leftBatch in leftStream) {
      _leftEmpty ??= [for (final c in leftBatch.columns) c.slice(0, 0)];
      if (leftBatch.rowCount == 0) continue;
      if (max != null && _probeEmitted + leftBatch.rowCount > max) {
        _probeOverflow(max);
      }
      _probeEmitted += leftBatch.rowCount;
      yield RecordBatch(
        columns: leftBatch.columns,
        schema: _schema,
        rowCount: leftBatch.rowCount,
      );
    }
  }

  /// Gather the left columns at [idx] into an output batch (the semi/anti output
  /// is the left schema; no right column is emitted).
  RecordBatch _assembleLeft(List<Column> leftCols, List<int> idx) {
    final li = Int32List.fromList(idx);
    return RecordBatch(
      columns: [for (final c in leftCols) Kernels.gather(c, li)],
      schema: _schema,
      rowCount: idx.length,
    );
  }

  /// Memory-bounded join for a partition that still exceeds [buildBudget] at the
  /// recursion cap (irreducible single-key skew — salting can't split it). Reads
  /// the re-readable build run in blocks of ≤ [buildBudget] and probes the whole
  /// (re-readable) probe run against each block via [_buildAndProbe]; matched
  /// pairs and per-block unmatched-RIGHT rows fall out of that call directly.
  ///
  /// Unmatched-LEFT rows (LEFT/FULL) can't be decided per block — a left row is
  /// globally unmatched only if NO block held its key. Rather than track a
  /// per-left-row flag (∝ probe size), we accumulate the set of distinct build
  /// keys and emit unmatched-left in one extra probe pass. That set is ~1 under
  /// genuine single-key skew and always smaller than materializing the build
  /// rows. Peak memory ≈ one block + one probe batch + the distinct-key set.
  Stream<RecordBatch> _blockNestedLoopJoin(
    SpillRun rightRun,
    SpillRun leftRun,
    int buildBudget,
  ) async* {
    final emitUnmatchedLeft =
        joinType == JoinType.left || joinType == JoinType.full;
    final seenKeys = emitUnmatchedLeft ? <Object?>{} : null;

    final block = <RecordBatch>[];
    var bytes = 0;
    await for (final b in rightRun.read()) {
      if (block.isNotEmpty && bytes + b.memoryUsage > buildBudget) {
        yield* _buildAndProbe(
          RecordBatch.concatAll(block, block.first.schema),
          leftRun.read(),
          forceUnmatchedLeft: false,
          collectKeys: seenKeys,
        );
        block.clear();
        bytes = 0;
      }
      block.add(b);
      bytes += b.memoryUsage;
    }
    if (block.isNotEmpty) {
      yield* _buildAndProbe(
        RecordBatch.concatAll(block, block.first.schema),
        leftRun.read(),
        forceUnmatchedLeft: false,
        collectKeys: seenKeys,
      );
    }

    if (seenKeys == null) return;
    // Extra pass: emit each probe row whose key is null or was never a build key.
    final rightCols = _rightEmpty ?? _emptyCols(right.schema);
    await for (final leftBatch in leftRun.read()) {
      _leftEmpty ??= [for (final c in leftBatch.columns) c.slice(0, 0)];
      final leftKeyCols = _keyCols(leftBatch, leftKeys, leftQualifiers);
      final leftIdx = <int>[];
      final rightIdx = <int>[];
      for (int i = 0; i < leftBatch.rowCount; i++) {
        final k = _keyAt(leftKeyCols, _leftScales, i);
        if (k == null || !seenKeys.contains(k)) {
          leftIdx.add(i);
          rightIdx.add(-1);
        }
      }
      if (leftIdx.isNotEmpty) {
        yield _assemble(leftBatch.columns, rightCols, leftIdx, rightIdx);
      }
    }
  }

  /// CROSS join. The left side is streamed batch-by-batch; the right (build) side
  /// is buffered under the memory budget and, if it doesn't fit, spilled to one
  /// re-readable Parquet run and re-scanned from disk for each left batch — so a
  /// large right input never has to be held in memory. The optional maxOutputRows
  /// guard is enforced as a running emitted-row count.
  Stream<RecordBatch> _crossJoin() async* {
    final mem = options.spills ? options.memory : null;
    final rightBuffer = <RecordBatch>[];
    var bytes = 0;
    SpillWriter? writer; // non-null once the right side overflows to disk
    RecordBatch? batch;

    while ((batch = await right.next()) != null) {
      _rightEmpty ??= [for (final c in batch!.columns) c.slice(0, 0)];
      final mu = batch!.memoryUsage;
      if (writer == null) {
        final localOver =
            options.spills && bytes + mu > options.spillThresholdBytes;
        final globalOver = !localOver && mem != null && !mem.reserve(mu);
        if (!localOver && !globalOver && mem != null) _reserved += mu;
        if (localOver || globalOver) {
          await Directory(_spillDir).create(recursive: true);
          writer = SpillWriter(
            _spillDir,
            _rightEmpty!.schemaOf(template: right.schema),
            budget: options.spillBudget,
          );
          _writers.add(writer);
          for (final b in rightBuffer) {
            await writer.add(b);
          }
          rightBuffer.clear();
          bytes = 0;
          mem?.release(_reserved); // buffered right rows are on disk now
          _reserved = 0;
          await writer.add(batch);
          continue;
        }
        rightBuffer.add(batch);
        bytes += mu;
      } else {
        await writer.add(batch);
      }
    }

    if (writer == null) {
      // In-memory fast path (unchanged): empty right → empty product.
      if (rightBuffer.isEmpty) {
        mem?.release(_reserved);
        _reserved = 0;
        return;
      }
      final rightBatch = RecordBatch.concatAll(rightBuffer, right.schema);
      mem?.release(_reserved);
      _reserved = 0;
      await for (final leftBatch in _streamOf(left)) {
        yield* _crossPair(leftBatch, rightBatch);
      }
      return;
    }

    // Spilled: re-scan the right run from disk for each left batch.
    final run = await writer.finish();
    _writers.remove(writer);
    _runs.add(run);
    await for (final leftBatch in _streamOf(left)) {
      await for (final rightSub in run.read()) {
        yield* _crossPair(leftBatch, rightSub);
      }
    }
  }

  /// Emit the product of one left batch and one right batch in batch-sized
  /// chunks, enforcing the running maxOutputRows guard.
  Stream<RecordBatch> _crossPair(
    RecordBatch leftBatch,
    RecordBatch right,
  ) async* {
    final max = options.maxOutputRows;
    final leftIdx = <int>[];
    final rightIdx = <int>[];

    Never overflow() =>
        throw StateError('CROSS JOIN exceeds maxOutputRows=$max');

    for (int i = 0; i < leftBatch.rowCount; i++) {
      for (int j = 0; j < right.rowCount; j++) {
        leftIdx.add(i);
        rightIdx.add(j);
        if (leftIdx.length >= defaultBatchSize) {
          if (max != null && _crossEmitted + leftIdx.length > max) overflow();
          _crossEmitted += leftIdx.length;
          yield _assemble(leftBatch.columns, right.columns, leftIdx, rightIdx);
          leftIdx.clear();
          rightIdx.clear();
        }
      }
    }
    if (leftIdx.isNotEmpty) {
      if (max != null && _crossEmitted + leftIdx.length > max) overflow();
      _crossEmitted += leftIdx.length;
      yield _assemble(leftBatch.columns, right.columns, leftIdx, rightIdx);
    }
  }

  /// Gather left+right columns at the paired indices into one output batch
  /// (negative index -> null), dropping the de-duplicated right key column.
  RecordBatch _assemble(
    List<Column> leftCols,
    List<Column> rightCols,
    List<int> leftIdx,
    List<int> rightIdx,
  ) {
    final li = Int32List.fromList(leftIdx);
    final ri = Int32List.fromList(rightIdx);
    final out = <Column>[
      for (final c in leftCols) Kernels.gather(c, li),
      for (int j = 0; j < rightCols.length; j++)
        if (!_drop.contains(j)) Kernels.gather(rightCols[j], ri),
    ];
    return RecordBatch(columns: out, schema: _schema, rowCount: leftIdx.length);
  }

  /// Route a batch's rows to partition spill files by join-key hash (null keys
  /// hash consistently, so null-left and null-right co-locate and are handled by
  /// [_buildAndProbe] exactly like the in-memory join). Spill files derive their
  /// schema from the actual column dataTypes, keeping each batch self-consistent.
  /// [scales] MUST be the scales of the side [keys] belong to (see [scaledKey]).
  /// Routing and matching have to key identically, or two equal composite/decimal
  /// keys land in different partitions and are never compared. [_keyAt] has
  /// already canonicalized, so [partitionOf] is passed the finished key.
  Future<void> _route(
    RecordBatch batch,
    List<SpillWriter> parts,
    int partitions,
    List<String> keys,
    List<String?> qualifiers,
    List<int?> scales, {
    int salt = 0,
  }) async {
    final keyCols = _keyCols(batch, keys, qualifiers);
    final realSchema = batch.columns.schemaOf(template: batch.schema);
    final buckets = [for (int i = 0; i < partitions; i++) <int>[]];
    for (int i = 0; i < batch.rowCount; i++) {
      // Shared content-hash router; a null key (any null part) hashes
      // consistently so null-left and null-right co-locate. salt redistributes
      // on recursive re-partition.
      buckets[partitionOf(_keyAt(keyCols, scales, i), partitions, salt: salt)]
          .add(i);
    }
    for (int p = 0; p < partitions; p++) {
      if (buckets[p].isEmpty) continue;
      final index = Int32List.fromList(buckets[p]);
      await parts[p].add(
        RecordBatch(
          columns: [for (final c in batch.columns) Kernels.gather(c, index)],
          schema: realSchema,
          rowCount: index.length,
        ),
      );
    }
  }

  List<Column> _emptyCols(Schema s) => [
    for (final f in s.fields) Kernels.fromValues(f.name, f.dataType, const []),
  ];

  Stream<RecordBatch> _streamOf(RecordBatchIterator iter) async* {
    RecordBatch? b;
    while ((b = await iter.next()) != null) {
      _leftEmpty ??= [for (final c in b!.columns) c.slice(0, 0)];
      yield b!;
    }
  }

  @override
  Future<void> close() async {
    try {
      await closeAll([
        () => left.close(),
        () => right.close(),
        () async {
          options.memory?.release(_reserved); // return any held reservation
          _reserved = 0;
        },
        // Abort writers still open (cancel/throw during partitioning) + delete.
        for (final writer in _writers) () => writer.abort(),
        for (final run in _runs) () => run.delete(),
      ]);
    } finally {
      _writers.clear();
    }
  }
}
