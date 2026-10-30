import 'dart:async';

import '../../core/failure.dart';
import '../../core/result.dart';
import '../../core/schema.dart';
import '../../core/table.dart';
import '../../core/row_filter.dart';
import '../../core/scan.dart';
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

  /// Opens the scan of [path] with its reader's options; null opens
  /// `openScan(path)`.
  final Scan Function()? open;

  PhysicalFileScan({
    required this.path,
    required Schema schema,
    this.filter,
    this.projection,
    this.alias,
    this.batchSize = defaultBatchSize,
    this.open,
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
    await pause();
    _batches ??= StreamIterator(
      (_plan.open?.call() ?? openScan(_plan.path)).batches(
        size: _plan.batchSize,
        projection: _plan.projection,
        filter: _plan.filter,
      ),
    );
    try {
      if (!await _batches!.moveNext()) return null;
    } on FormatException catch (e) {
      throw Failure.of(
        Kind.corrupt,
        'Failed to read ${_plan.path}: ${e.message}',
        e,
      );
    }
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
