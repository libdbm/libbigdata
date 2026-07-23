import 'dart:convert';
import 'dart:typed_data';

import 'column.dart';
import 'data_type.dart';

/// Mergeable per-column statistics: total [count] (including nulls and NaN),
/// [nulls], [nans] (non-finite floats), and finite [min]/[max]. [addColumn]
/// folds one column's values, so a streaming writer accumulates across batches.
/// min/max EXCLUDE NaN/±Inf — they would poison ordering and break JSON stat
/// encoding. Shared by the Delta and Iceberg writers so the accumulation is not
/// duplicated (each format renders these differently).
class StatsAccumulator {
  int count = 0;
  int nulls = 0;
  int nans = 0;
  Object? min;
  Object? max;

  void addColumn(Column column) {
    final n = column.length;
    count += n;
    Object? mn = min;
    Object? mx = max;
    if (column is NumericColumn) {
      // Typed fast path: read the flat buffer directly — no whole-column List
      // allocation and no per-value boxing.
      final raw = column.rawBuffer as List;
      for (int i = 0; i < n; i++) {
        if (column.isNullAt(i)) {
          nulls++;
          continue;
        }
        final num v = raw[i];
        if (v is double && !v.isFinite) {
          nans++;
          continue;
        }
        if (mn == null || v.compareTo(mn as num) < 0) mn = v;
        if (mx == null || v.compareTo(mx as num) > 0) mx = v;
      }
    } else {
      for (int i = 0; i < n; i++) {
        final v = column[i];
        if (v == null) {
          nulls++;
        } else if (v is num || v is String || v is BigInt || v is DateTime) {
          if (v is double && !v.isFinite) {
            nans++;
            continue;
          }
          if (mn == null || (v as Comparable).compareTo(mn) < 0) mn = v;
          if (mx == null || (v as Comparable).compareTo(mx) > 0) mx = v;
        }
      }
    }
    min = mn;
    max = mx;
  }
}

/// Encode a stat bound [value] to Iceberg's single-value binary form for the
/// primitive [type], or null when [type] has no LOW-RISK deterministic encoding
/// here. int/long/float/double are little-endian; string is UTF-8; boolean is
/// one byte. Temporal (date/timestamp), decimal and binary bounds are omitted
/// deliberately — a WRONG bound makes an external engine skip a file that DOES
/// match (silent data loss), and those encodings cannot be validated against a
/// real Iceberg reader here, so omitting them (spec-legal) is the safe choice.
Uint8List? icebergBound(Object value, DataType type) {
  if (type == DataType.boolean) {
    return Uint8List.fromList([(value as bool) ? 1 : 0]);
  }
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
  return null; // date/timestamp/decimal/binary/nested — omitted (safe)
}
