import 'dart:async';

import '../../core/schema.dart';
import '../../core/table.dart';
import '../../core/row_filter.dart';
import '../../formats/scan.dart';
import 'physical_plan.dart';

/// Lazy, streaming scan of a file source: pulls one [Table] batch at a time from
/// the format's streaming reader (no whole-file materialization). Parquet also
/// receives the pushed-down [filter] (row-group pruning) and [projection] (fewer
/// columns decoded); CSV/JSON apply projection by selecting columns per batch
/// and rely on the plan's exact `PhysicalFilter` for predicates.
class PhysicalFileScan extends PhysicalPlan {
  final String path;
  final Schema _schema;
  final RowFilter? filter;
  final List<String>? projection;
  final String? alias;
  final int batchSize;

  PhysicalFileScan({
    required this.path,
    required Schema schema,
    this.filter,
    this.projection,
    this.alias,
    this.batchSize = defaultBatchSize,
  }) : _schema = schema;

  @override
  Schema get schema {
    final base = projection != null ? _schema.select(projection!) : _schema;
    return alias != null ? base.qualify(alias!) : base;
  }

  @override
  int? get estimatedRows => null;

  @override
  RecordBatchIterator execute() => _FileScanIterator(this);
}

class _FileScanIterator extends RecordBatchIterator {
  final PhysicalFileScan _plan;
  StreamIterator<Table>? _batches;

  _FileScanIterator(this._plan);

  @override
  Schema get schema => _plan.schema;

  @override
  Future<RecordBatch?> next() async {
    _batches ??= StreamIterator(
      openScan(_plan.path).batches(
        size: _plan.batchSize,
        projection: _plan.projection,
        filter: _plan.filter,
      ),
    );
    if (!await _batches!.moveNext()) return null;
    final table = _batches!.current;
    // Assemble columns in the scan schema's field order (by base name), so
    // every format lines up with `schema` regardless of the reader's own
    // column ordering or whether it honored the projection.
    final columns = [
      for (final field in _plan.schema.fields) table.column(field.name)!,
    ];
    return RecordBatch(
      columns: columns,
      schema: _plan.schema,
      rowCount: table.rowCount,
    );
  }

  @override
  Future<void> close() async {
    await _batches?.cancel();
  }
}
