import 'dart:convert';
import 'dart:typed_data';

import 'data_type.dart';

/// Comparison operators supported for statistics-based pruning.
enum FilterOp { eq, ne, lt, le, gt, ge }

/// A format-neutral predicate used to prune scan units (Parquet row groups,
/// partition files, …) using column statistics. It is a *pruning hint*: [keep]
/// returns true when a unit might contain matching rows, so a unit is skipped
/// only when the stats prove it cannot match. Unknown columns or absent stats
/// always keep.
///
/// This type lives in `core` so the SQL layer and every format depend on it
/// (rather than SQL depending on a Parquet-specific type). Decoding raw
/// statistics from a particular format's metadata is the format's job (e.g. the
/// Parquet `statsToBounds` adapter), built on [ColumnBounds.decode].
abstract class RowFilter {
  const RowFilter();

  /// A single `column OP value` comparison. [value] must be an int, double,
  /// String or bool matching the column's type.
  factory RowFilter.compare(String column, FilterOp operator, Object value) =
      _LeafFilter;

  /// True if every part keeps (a part proving no-match skips the unit).
  factory RowFilter.and(List<RowFilter> parts) = _AndFilter;

  /// True if any part keeps.
  factory RowFilter.or(List<RowFilter> parts) = _OrFilter;

  /// Whether a scan unit described by [bounds] might contain matching rows.
  /// False means the unit can be skipped entirely.
  bool keep(Map<String, ColumnBounds> bounds);

  /// Column names referenced anywhere in this filter.
  Set<String> get columns;
}

/// Decoded per-column statistics for one scan unit.
class ColumnBounds {
  /// Smallest non-null value, or null when unknown / no non-null values.
  final Object? min;

  /// Largest non-null value, or null when unknown / no non-null values.
  final Object? max;

  /// Number of nulls, or null when unknown.
  final int? nullCount;

  /// Total rows in the unit.
  final int rowCount;

  /// Whether statistics were present at all.
  final bool hasStats;

  const ColumnBounds({
    this.min,
    this.max,
    this.nullCount,
    required this.rowCount,
    this.hasStats = false,
  });

  /// Decode a min/max statistic [bytes] for [type]. Tolerates both this
  /// library's encoding (int/double as little-endian 8 bytes) and the spec's
  /// native widths. Returns null for types whose stats can't be safely decoded
  /// (no bounds → no pruning).
  static Object? decode(Uint8List bytes, DataType type) {
    if (bytes.isEmpty) return null;
    final data = ByteData.sublistView(bytes);
    switch (type) {
      case DataType.int8:
      case DataType.int16:
      case DataType.int32:
      case DataType.int64:
      case DataType.date:
      case DataType.timestamp:
        return _int(data, bytes.length);
      case DataType.float32:
      case DataType.float64:
        return bytes.length == 4
            ? data.getFloat32(0, Endian.little)
            : data.getFloat64(0, Endian.little);
      // decimal omitted: stats are unscaled two's-complement, not float —
      // falls through to no-bounds (no pruning) rather than mis-decoding.
      case DataType.string:
        return utf8.decode(bytes, allowMalformed: true);
      case DataType.boolean:
        return bytes[0] != 0;
      default:
        return null; // unsupported type -> no bounds -> no pruning
    }
  }

  static int _int(ByteData data, int length) {
    switch (length) {
      case 1:
        return data.getInt8(0);
      case 2:
        return data.getInt16(0, Endian.little);
      case 4:
        return data.getInt32(0, Endian.little);
      default:
        return data.getInt64(0, Endian.little);
    }
  }
}

class _LeafFilter extends RowFilter {
  final String column;
  final FilterOp operation;
  final Object value;

  const _LeafFilter(this.column, this.operation, this.value);

  @override
  Set<String> get columns => {column};

  @override
  bool keep(Map<String, ColumnBounds> bounds) {
    final b = bounds[column];
    if (b == null || !b.hasStats) return true; // cannot prune

    // Whole unit is null: no comparison can be TRUE.
    if (b.nullCount != null && b.nullCount == b.rowCount) return false;

    final lo = b.min, hi = b.max;
    if (lo == null || hi == null) return true; // incomplete stats
    if (!_comparable(value, lo)) return true; // type mismatch -> keep

    switch (operation) {
      case FilterOp.eq:
        return _cmp(value, lo) >= 0 && _cmp(value, hi) <= 0;
      case FilterOp.ne:
        // Skip only when every value equals the literal and there are no nulls.
        final uniform = _cmp(lo, hi) == 0 && _cmp(lo, value) == 0;
        return !(uniform && b.nullCount == 0);
      case FilterOp.lt:
        return _cmp(lo, value) < 0;
      case FilterOp.le:
        return _cmp(lo, value) <= 0;
      case FilterOp.gt:
        return _cmp(hi, value) > 0;
      case FilterOp.ge:
        return _cmp(hi, value) >= 0;
    }
  }

  static bool _comparable(Object a, Object b) =>
      (a is num && b is num) ||
      (a is String && b is String) ||
      (a is bool && b is bool);

  static int _cmp(Object a, Object b) {
    if (a is num && b is num) return a.compareTo(b);
    if (a is String && b is String) return _compareUtf8(a, b);
    if (a is bool && b is bool) return (a ? 1 : 0) - (b ? 1 : 0);
    throw ArgumentError('incomparable values: $a, $b');
  }

  /// Compare by UTF-8 BYTES — the order Parquet's UTF8 statistics are written
  /// in, by this library's page writer and by pyarrow alike.
  ///
  /// `String.compareTo` orders by UTF-16 code UNIT, and the two disagree above
  /// the BMP: UTF-8 orders by code point, while UTF-16 puts surrogates
  /// (D800-DFFF) BELOW E000-FFFF. Comparing a probe in UTF-16 order against
  /// byte-ordered bounds can therefore place it outside a range that really
  /// contains it, and prune a row group holding matching rows. Only reachable
  /// for a column mixing astral and E000-FFFF characters, but the bounds are
  /// bytes, so the comparison must be too.
  static int _compareUtf8(String a, String b) {
    if (a == b) return 0;
    final x = utf8.encode(a);
    final y = utf8.encode(b);
    final shared = x.length < y.length ? x.length : y.length;
    for (var i = 0; i < shared; i++) {
      if (x[i] != y[i]) return x[i] - y[i];
    }
    return x.length - y.length;
  }
}

class _AndFilter extends RowFilter {
  final List<RowFilter> parts;
  const _AndFilter(this.parts);

  @override
  Set<String> get columns => {for (final p in parts) ...p.columns};

  @override
  bool keep(Map<String, ColumnBounds> bounds) =>
      parts.every((p) => p.keep(bounds));
}

class _OrFilter extends RowFilter {
  final List<RowFilter> parts;
  const _OrFilter(this.parts);

  @override
  Set<String> get columns => {for (final p in parts) ...p.columns};

  @override
  bool keep(Map<String, ColumnBounds> bounds) =>
      parts.any((p) => p.keep(bounds));
}
