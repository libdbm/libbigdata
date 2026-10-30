import 'dart:async';
import 'dart:math' as math;
import '../../core/table.dart';
import '../../core/column.dart';
import '../../core/columnar.dart' show columnFromValues, columnsFromRows;
import '../../core/conversions.dart'
    show
        dateOf,
        decimalParts,
        formatDecimal,
        parseUtc,
        rescaleUnscaled,
        toUnscaled,
        unscaledFromString;
import '../../core/schema.dart';
import '../../core/data_type.dart';
import '../../core/decimal.dart';
import '../exec/kernels.dart';
import '../execution/exec_core.dart';
import 'logical_plan.dart';

// The execution-core contracts (PhysicalPlan / RecordBatchIterator / RecordBatch)
// now live in execution/exec_core.dart so `spill.dart` can depend on RecordBatch
// without importing this file (which imports spill.dart): breaking the cycle.
// Re-exported so existing importers of this file are unaffected.
export '../execution/exec_core.dart'
    show
        PhysicalPlan,
        RecordBatchIterator,
        RecordBatch,
        ColumnListSchema,
        checkDeadline,
        pause,
        closeAll,
        Deadline;

part 'physical_expression.dart';

/// Batch size for iterator processing
const int defaultBatchSize = 1024;

/// Coerce [value]: produced at [valueScale] by one branch of a multi-branch
/// expression (CASE / COALESCE), to that expression's declared [type] and
/// [scale].
///
/// The planner widens the branches' TYPES to a common one, but a branch still
/// yields its own value: an int branch under a float64 result yields an `int`,
/// and a scale-2 branch under a scale-4 result yields the wrong magnitude. A
/// column whose values contradict its dtype is a lie the rest of the engine
/// trusts: a Parquet write rejects it, and `BigInt.from(0) != 0` splits one
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
    // A decimal branch is already unscaled: lift it to the common scale. A
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
  // string form: storing the raw int under a string-typed column is the same
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

/// Scans literal rows: `VALUES (1), (2)`, and the one-row source a `SELECT`
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
    await pause();
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
        final value = row[i].evaluate(empty, 0);
        // A decimal cell evaluates to an unscaled integer at its own scale;
        // as a Decimal it is restated at the column's.
        values[names[i]] =
            value is BigInt &&
                valueSchema.fields[i].dataType == DataType.decimal
            ? Decimal(value, row[i].scale ?? 0)
            : value;
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
  int _offset = 0;

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
    await pause();
    if (_offset >= table.rowCount) {
      return null;
    }

    final endOffset = (_offset + batchSize).clamp(0, table.rowCount);
    final batchRowCount = endOffset - _offset;

    // Get columns for this batch
    final batchColumns = <Column>[];
    final columnsToRead = projection ?? table.schema.fieldNames;

    // Special case: If no columns are needed (e.g., COUNT(*)), return empty columns
    if (columnsToRead.isEmpty && projection != null) {
      _offset = endOffset;
      return RecordBatch(columns: [], schema: schema, rowCount: batchRowCount);
    }

    for (final columnName in columnsToRead) {
      final column = table.column(columnName);
      if (column != null) {
        // Use the efficient slice method
        final slicedColumn = column.slice(_offset, endOffset);
        batchColumns.add(slicedColumn);
      }
    }

    _offset = endOffset;

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
/// query can address `alias.col`. With [keep], a column that already has a
/// qualifier keeps it (a DataFrame join side that holds an earlier join).
class PhysicalAlias extends PhysicalPlan {
  final PhysicalPlan input;
  final String alias;
  final bool keep;

  PhysicalAlias({required this.input, required this.alias, this.keep = false});

  @override
  Schema get schema {
    final schema = input.schema;
    if (!keep) return schema.qualify(alias);
    return Schema(
      fields: [
        for (final f in schema.fields)
          f.qualifier == null ? f.copyWith(qualifier: alias) : f,
      ],
      metadata: schema.metadata,
    );
  }

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
    await pause();
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
    await pause();
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
    await pause();
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
