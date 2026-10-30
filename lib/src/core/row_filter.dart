import 'dart:convert';
import 'dart:typed_data';

import 'data_type.dart';
import 'stats.dart' show compareCodePoints;

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
sealed class RowFilter {
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

  /// Whether one [row] (column name to value) matches, exactly: a reader
  /// without statistics, such as CSV, filters each row with this. A null
  /// value never matches a comparison, and a text value is read in the
  /// literal's type (`'10'` against `5` compares as the number 10); text that
  /// does not read as that type does not match.
  bool matches(Map<String, Object?> row);

  /// Column names referenced anywhere in this filter.
  Set<String> get columns;
}

/// Decoded per-column statistics for one scan unit.
final class ColumnBounds {
  /// Smallest non-null value, or null when unknown / no non-null values.
  final Object? min;

  /// Largest non-null value, or null when unknown / no non-null values.
  final Object? max;

  /// Number of nulls, or null when unknown.
  final int? nullCount;

  /// Number of NaN values, or null when unknown. Statistics leave NaN out of
  /// [min] and [max], so a floating-point unit needs this to prove it is
  /// uniform.
  final int? nanCount;

  /// Total rows in the unit.
  final int rowCount;

  /// Whether statistics were present at all.
  final bool hasStats;

  const ColumnBounds({
    this.min,
    this.max,
    this.nullCount,
    this.nanCount,
    required this.rowCount,
    this.hasStats = false,
  });

  /// The bounds of a unit whose every row holds [value] (a partition value):
  /// [rows] rows, all null when [value] is.
  factory ColumnBounds.exact(Object? value, {int? rows}) => ColumnBounds(
    min: value,
    max: value,
    nullCount: value == null ? rows ?? 1 : 0,
    nanCount: 0,
    rowCount: rows ?? 1,
    hasStats: true,
  );

  /// Whether bounds of [type] are judged by the lake formats: numeric (not
  /// decimal), string and boolean, whose single-value forms are unambiguous.
  /// Delta writes dates, timestamps and decimals as text whose form need not
  /// match a filter's value.
  static bool judged(DataType type) =>
      type.isInteger ||
      type == DataType.float32 ||
      type == DataType.float64 ||
      type == DataType.string ||
      type == DataType.boolean;

  /// [value] (a JSON statistic or a partition value) as a value of [type], or
  /// null when it is not one.
  static Object? typed(Object? value, DataType type) => switch (type) {
    DataType.float32 || DataType.float64 when value is num => value.toDouble(),
    DataType.string when value is String => value,
    DataType.boolean when value is bool => value,
    _ when type.isInteger && value is int => value,
    _ => null,
  };

  /// Decode a min/max statistic [bytes] for [type]. Tolerates both this
  /// library's encoding (int/double as little-endian 8 bytes) and the spec's
  /// native widths. Returns null, meaning no bounds and no pruning, for types
  /// whose stats can't be safely decoded, for widths the type cannot have,
  /// and for text that is not valid UTF-8 (a truncated bound read with
  /// replacement characters could rank above real values).
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
        return switch (bytes.length) {
          4 => data.getFloat32(0, Endian.little),
          8 => data.getFloat64(0, Endian.little),
          _ => null,
        };
      // decimal omitted: stats are unscaled two's-complement, not float, so it
      // falls through to no-bounds (no pruning) rather than mis-decoding.
      case DataType.string:
        try {
          return utf8.decode(bytes);
        } on FormatException {
          return null;
        }
      case DataType.boolean:
        return bytes[0] != 0;
      default:
        return null; // unsupported type -> no bounds -> no pruning
    }
  }

  static int? _int(ByteData data, int length) => switch (length) {
    1 => data.getInt8(0),
    2 => data.getInt16(0, Endian.little),
    4 => data.getInt32(0, Endian.little),
    8 => data.getInt64(0, Endian.little),
    _ => null,
  };
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
    final range = bounds[column];
    if (range == null || !range.hasStats) return true; // cannot prune

    // Whole unit is null: no comparison can be TRUE.
    if (range.nullCount != null && range.nullCount == range.rowCount) {
      return false;
    }

    final low = range.min, high = range.max;
    if (low == null || high == null) return true; // incomplete stats
    if (!_comparable(value, low)) return true; // type mismatch -> keep

    switch (operation) {
      case FilterOp.eq:
        return _compare(value, low) >= 0 && _compare(value, high) <= 0;
      case FilterOp.ne:
        // Skip only when every value equals the literal and there are no nulls.
        // NaN is outside the bounds and never equals the literal, so a
        // floating-point unit must also be known to hold no NaN.
        if (low is double && range.nanCount != 0) return true;
        final uniform = _compare(low, high) == 0 && _compare(low, value) == 0;
        return !(uniform && range.nullCount == 0);
      case FilterOp.lt:
        return _compare(low, value) < 0;
      case FilterOp.le:
        return _compare(low, value) <= 0;
      case FilterOp.gt:
        return _compare(high, value) > 0;
      case FilterOp.ge:
        return _compare(high, value) >= 0;
    }
  }

  @override
  bool matches(Map<String, Object?> row) {
    final cell = _typed(row[column]);
    if (cell == null || !_comparable(cell, value)) return false;
    final order = _compare(cell, value);
    return switch (operation) {
      FilterOp.eq => order == 0,
      FilterOp.ne => order != 0,
      FilterOp.lt => order < 0,
      FilterOp.le => order <= 0,
      FilterOp.gt => order > 0,
      FilterOp.ge => order >= 0,
    };
  }

  /// [cell] in [value]'s type when it is text: an integer first (a double
  /// would round a 64-bit one), then a double, or `true`/`false`.
  Object? _typed(Object? cell) {
    if (cell is! String) return cell;
    if (value is num) return int.tryParse(cell) ?? double.tryParse(cell);
    if (value is bool) {
      final lower = cell.toLowerCase();
      return lower == 'true' ? true : (lower == 'false' ? false : null);
    }
    return cell;
  }

  static bool _comparable(Object a, Object b) =>
      (a is num && b is num) ||
      (a is String && b is String) ||
      (a is bool && b is bool);

  static int _compare(Object a, Object b) {
    if (a is num && b is num) return a.compareTo(b);
    if (a is String && b is String) return _compareUtf8(a, b);
    if (a is bool && b is bool) return (a ? 1 : 0) - (b ? 1 : 0);
    throw ArgumentError('incomparable values: $a, $b');
  }

  /// Compare by code point, which is UTF-8 BYTE order: the order Parquet's
  /// UTF8 statistics are written in, by this library's page writer and by
  /// pyarrow alike. `String.compareTo` orders by UTF-16 code UNIT, which puts
  /// a character past U+FFFF (surrogates) below U+E000 to U+FFFF, so it could
  /// place a probe outside a range that really contains it and prune a row
  /// group holding matching rows.
  static int _compareUtf8(String a, String b) => compareCodePoints(a, b);
}

class _AndFilter extends RowFilter {
  final List<RowFilter> parts;
  const _AndFilter(this.parts);

  @override
  Set<String> get columns => {for (final p in parts) ...p.columns};

  @override
  bool keep(Map<String, ColumnBounds> bounds) =>
      parts.every((p) => p.keep(bounds));

  @override
  bool matches(Map<String, Object?> row) => parts.every((p) => p.matches(row));
}

class _OrFilter extends RowFilter {
  final List<RowFilter> parts;
  const _OrFilter(this.parts);

  @override
  Set<String> get columns => {for (final p in parts) ...p.columns};

  @override
  bool keep(Map<String, ColumnBounds> bounds) =>
      parts.any((p) => p.keep(bounds));

  @override
  bool matches(Map<String, Object?> row) => parts.any((p) => p.matches(row));
}
