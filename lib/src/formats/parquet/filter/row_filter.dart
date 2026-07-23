import 'dart:typed_data';

import '../../../core/data_type.dart';
import '../../../core/row_filter.dart';
import '../schema/schema_converter.dart' show SchemaConverter;
import '../thrift/parquet_thrift.dart' as thrift;

// The filter types are format-neutral and live in core; re-exported here so
// existing Parquet-layer imports keep working.
export '../../../core/row_filter.dart';

/// Decode a Parquet column chunk's [Statistics] into typed [ColumnBounds].
/// Tolerates both this library's encoding (int/double as little-endian 8 bytes)
/// and the spec's native widths, and falls back to the deprecated min/max
/// fields. This is the Parquet-specific adapter onto the neutral [ColumnBounds]
/// (so `core` never depends on the Parquet thrift types).
ColumnBounds statsToBounds(
  thrift.Statistics? stats,
  DataType type,
  int rowCount, {
  int? unsignedBits,
}) {
  if (stats == null) {
    return ColumnBounds(rowCount: rowCount, hasStats: false);
  }
  final minBytes = stats.minValue ?? stats.min;
  final maxBytes = stats.maxValue ?? stats.max;
  return ColumnBounds(
    min: minBytes == null ? null : _decode(minBytes, type, unsignedBits),
    max: maxBytes == null ? null : _decode(maxBytes, type, unsignedBits),
    nullCount: stats.nullCount,
    rowCount: rowCount,
    hasStats: true,
  );
}

/// [ColumnBounds.decode], then reinterpret an unsigned column's stat. The spec
/// stores a UINT_* min/max in UNSIGNED order but as raw physical bytes, so a
/// uint32 value ≥ 2^31 decoded signed read back NEGATIVE — a filter on that
/// range then "proved" no row could match and silently pruned the row group.
/// (The value decode path already reinterprets; the pruning path did not.)
Object? _decode(Uint8List bytes, DataType type, int? unsignedBits) {
  final v = ColumnBounds.decode(bytes, type);
  if (unsignedBits == null || v is! int) return v;
  return SchemaConverter.reinterpretUnsigned(v, unsignedBits);
}
