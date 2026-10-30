import '../core/table.dart';
import '../core/row_filter.dart';
import 'scan.dart';

/// A fully-serializable description of one file read: safe to send to an
/// isolate ([path] is a String, [filter] a closure-free [RowFilter] tree).
class ReadSpec {
  final String path;
  final RowFilter? filter;

  /// Column projection already intersected with this file's schema (so the
  /// reader never hard-errors on an absent column); null means read all.
  final List<String>? columns;

  /// Per-file materialization caps, forwarded to the reader so `null` means
  /// truly unbounded (matching the serial path) rather than the reader's
  /// default 10M-row / 1-GiB fallback.
  final int? maxRows;
  final int? maxBytes;
  const ReadSpec(
    this.path, {
    this.filter,
    this.columns,
    this.maxRows,
    this.maxBytes,
  });
}

/// Read one file into a [Table]. Declared top-level so it can be the body of
/// `Isolate.run(() => readTableTask(spec))`. The returned Table is pure data
/// and crosses the isolate boundary by copy. Format selection goes through the
/// single registry ([openScan]) so the parallel path stays identical to the
/// serial/streaming paths.
Future<Table> readTableTask(ReadSpec spec) async {
  return openScan(spec.path).read(
    maxRows: spec.maxRows,
    maxBytes: spec.maxBytes,
    projection: spec.columns,
    filter: spec.filter,
  );
}
