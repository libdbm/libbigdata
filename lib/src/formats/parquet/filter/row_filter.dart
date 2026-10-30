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
/// and the spec's native widths. The deprecated min/max fields are used only
/// for types whose order is signed: older writers (PARQUET-686) filled them in
/// signed byte order, which is wrong for strings, binary and unsigned integers
/// and would prune row groups that match. This is the Parquet-specific adapter
/// onto the neutral [ColumnBounds] (so `core` never depends on the Parquet
/// thrift types).
ColumnBounds statsToBounds(
  thrift.Statistics? stats,
  DataType type,
  int rowCount, {
  int? unsignedBits,
}) {
  if (stats == null) {
    return ColumnBounds(rowCount: rowCount, hasStats: false);
  }
  final signed =
      unsignedBits == null &&
      (type.isInteger ||
          type == DataType.float32 ||
          type == DataType.float64 ||
          type == DataType.boolean ||
          type == DataType.date ||
          type == DataType.timestamp);
  final low = stats.minValue ?? (signed ? stats.min : null);
  final high = stats.maxValue ?? (signed ? stats.max : null);
  return ColumnBounds(
    min: low == null ? null : _decode(low, type, unsignedBits),
    max: high == null ? null : _decode(high, type, unsignedBits),
    nullCount: stats.nullCount,
    rowCount: rowCount,
    hasStats: true,
  );
}

/// [ColumnBounds.decode], then reinterpret an unsigned column's stat. The spec
/// stores a UINT_* min/max in UNSIGNED order but as raw physical bytes, so a
/// uint32 value ≥ 2^31 decoded signed read back NEGATIVE: a filter on that
/// range then "proved" no row could match and silently pruned the row group.
/// (The value decode path already reinterprets; the pruning path did not.)
Object? _decode(Uint8List bytes, DataType type, int? unsignedBits) {
  final value = ColumnBounds.decode(bytes, type);
  if (unsignedBits == null || value is! int) return value;
  return SchemaConverter.reinterpretUnsigned(value, unsignedBits);
}
