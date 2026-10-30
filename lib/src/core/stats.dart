import 'dart:convert';
import 'dart:typed_data';

import 'column.dart';
import 'data_type.dart';

/// Compare [a] and [b] by Unicode code point, which is UTF-8 byte order and
/// the order Parquet, Delta and Iceberg string statistics use. `String.compareTo`
/// orders by UTF-16 code unit, which puts U+1F600 (a surrogate pair) below
/// U+FF01, so an external engine would prune a file that does match.
int compareCodePoints(String a, String b) {
  final x = a.runes.iterator;
  final y = b.runes.iterator;
  while (true) {
    final more = x.moveNext();
    final other = y.moveNext();
    if (!more || !other) return more ? 1 : (other ? -1 : 0);
    final difference = x.current - y.current;
    if (difference != 0) return difference;
  }
}

/// Order two non-null stat values of the same kind.
int _compare(Object a, Object b) => a is String && b is String
    ? compareCodePoints(a, b)
    : (a as Comparable).compareTo(b);

/// Mergeable per-column statistics: total [count] (including nulls and NaN),
/// [nulls], [nans], and [min]/[max]. [addColumn] folds one column's values, so
/// a streaming writer accumulates across batches. min/max exclude only NaN,
/// which has no place in an order; ±Inf are valid bounds (a renderer that
/// cannot represent them, such as Delta's JSON, omits them). Shared by the
/// Delta and Iceberg writers so the accumulation is not duplicated (each
/// format renders these differently).
class StatsAccumulator {
  int count = 0;
  int nulls = 0;
  int nans = 0;
  Object? min;
  Object? max;

  void addColumn(Column column) {
    final n = column.length;
    count += n;
    Object? low = min;
    Object? high = max;
    if (column is NumericVector) {
      // Typed fast path: read the flat buffer directly, with no whole-column List
      // allocation and no per-value boxing.
      final raw = column.raw as List;
      for (int i = 0; i < n; i++) {
        if (column.isNullAt(i)) {
          nulls++;
          continue;
        }
        final num v = raw[i];
        if (v is double && v.isNaN) {
          nans++;
          continue;
        }
        if (low == null || v.compareTo(low as num) < 0) low = v;
        if (high == null || v.compareTo(high as num) > 0) high = v;
      }
    } else {
      for (int i = 0; i < n; i++) {
        final v = column[i];
        if (v == null) {
          nulls++;
        } else if (v is num || v is String || v is BigInt || v is DateTime) {
          if (v is double && v.isNaN) {
            nans++;
            continue;
          }
          if (low == null || _compare(v, low) < 0) low = v;
          if (high == null || _compare(v, high) > 0) high = v;
        }
      }
    }
    min = low;
    max = high;
  }
}

/// Encode a stat bound [value] to Iceberg's single-value binary form for the
/// primitive [type], or null when [type] has no LOW-RISK deterministic encoding
/// here. int/long/float/double are little-endian; string is UTF-8. A boolean
/// has no bound ([StatsAccumulator] orders no booleans). Temporal (date/timestamp), decimal and binary bounds are omitted
/// deliberately: a WRONG bound makes an external engine skip a file that DOES
/// match (silent data loss), and those encodings cannot be validated against a
/// real Iceberg reader here, so omitting them (spec-legal) is the safe choice.
Uint8List? icebergBound(Object value, DataType type) {
  if (type == DataType.int8 ||
      type == DataType.int16 ||
      type == DataType.int32) {
    return (ByteData(
      4,
    )..setInt32(0, (value as num).toInt(), Endian.little)).buffer.asUint8List();
  }
  if (type == DataType.int64) {
    return (ByteData(
      8,
    )..setInt64(0, (value as num).toInt(), Endian.little)).buffer.asUint8List();
  }
  if (type == DataType.float32) {
    return (ByteData(4)
          ..setFloat32(0, (value as num).toDouble(), Endian.little))
        .buffer
        .asUint8List();
  }
  if (type == DataType.float64) {
    return (ByteData(8)
          ..setFloat64(0, (value as num).toDouble(), Endian.little))
        .buffer
        .asUint8List();
  }
  if (type == DataType.string) {
    return Uint8List.fromList(utf8.encode(value as String));
  }
  return null; // date/timestamp/decimal/binary/nested: omitted (safe)
}
