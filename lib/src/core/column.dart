import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';
import 'package:meta/meta.dart';
import 'conversions.dart' show formatDecimal;
import 'data_type.dart';

part 'column_impl.dart';

/// Bound on a column's distinct-value probe (for `distinctCount` stats): past
/// this many distinct values the exact count saturates, so a high-cardinality
/// column can't allocate a Set as large as itself. Shared by NumericColumn and
/// ListColumn so both stats paths are bounded identically.
const int distinctStatsCap = 1 << 16;

/// Base class for columnar data storage
@immutable
sealed class Column<T> {
  final String name;
  final DataType dataType;
  final bool nullable;

  const Column({
    required this.name,
    required this.dataType,
    this.nullable = true,
  });

  /// Number of elements in the column
  int get length;

  /// Check if column has any null values
  bool get hasNulls;

  /// Get value at index
  T? operator [](int index);

  /// Get a slice of the column
  Column<T> slice(int start, [int? end]);

  /// Filter column by indices
  Column<T> filter(List<int> indices);

  /// Convert to list
  List<T?> toList();

  /// Get memory usage in bytes
  int get memoryUsage;

  /// Create a copy of the column
  Column<T> copy();

  /// The same column under a new [name], with no type demotion. The typed
  /// columns share their underlying (immutable) buffers; a boxed [ListColumn]
  /// makes a shallow copy of its value list.
  Column<T> withName(String name);

  /// Apply a function to each element
  Column<R> map<R>(R Function(T?) transform, DataType resultType);

  /// Get statistics for numeric columns
  ColumnStats? get stats;

  /// Widest integer each float type represents exactly; every |v| at or below it
  /// round-trips, so the exactness check below only runs past it. float32 holds
  /// integers exactly only to 2^24: using the float64 limit (2^53) for a float32
  /// target let ints in (2^24, 2^53] pass the guard and then truncate silently
  /// when stored into the Float32List.
  static const _exactIntLimit32 = 1 << 24;
  static const _exactIntLimit64 = 1 << 53;

  /// [value] as a double for a [target] float column, rejecting an INTEGER
  /// conversion that would not round-trip at the target's precision. (A
  /// float→float narrowing is conventionally lossy and is not checked here.)
  double _exactDouble(num value, DataType target) {
    final result = value.toDouble();
    final float32 = target == DataType.float32;
    final limit = float32 ? _exactIntLimit32 : _exactIntLimit64;
    if (value is int && value.abs() > limit) {
      // Round-trip at the TARGET precision: a float32 value must survive a
      // 32-bit slot (a float64 comparison would pass values float32 truncates).
      // Compare in BigInt space: toInt() CLAMPS at the int64 boundary, masking
      // the very loss being checked.
      final narrowed = float32 ? (Float32List(1)..[0] = result)[0] : result;
      if (BigInt.from(narrowed) != BigInt.from(value)) {
        throw ArgumentError(
          'Column "$name": $value cannot be represented exactly as '
          '${target.name} (it would become ${narrowed.toStringAsFixed(0)}). '
          'CAST the operands explicitly to choose how to handle the loss.',
        );
      }
    }
    return result;
  }

  /// Return this column re-typed to [target], or itself when already [target].
  ///
  /// Supports the numeric promotions `DataType.commonType` produces: int→wider
  /// int, int→float, float32→float64, int→decimal: used to align set-operation
  /// branches to a shared type before union/concat. Anything else (e.g.
  /// →string) throws.
  ///
  /// int→float is NOT lossless in general: float64 holds every integer only up
  /// to 2^53 (float32, up to 2^24), so a 64-bit id could round to a DIFFERENT
  /// number. `int64 UNION float64` is float64 under standard SQL, and the
  /// lossless alternative (decimal) needs a scale that only the data can supply,
  /// which a set-op's plan-time schema cannot know. So a value that would
  /// round is REJECTED here rather than silently rewritten.
  Column castTo(DataType target) {
    if (dataType == target) return this;
    // A column of NULL literals (`SELECT NULL` in a UNION branch) takes any
    // type: every value is null.
    if (dataType == DataType.null_) return _empty(target);
    if (dataType.isNumeric && target.isNumeric) {
      final values = toList();
      if (target == DataType.float32 || target == DataType.float64) {
        final doubles = [
          for (final v in values)
            v == null ? null : _exactDouble(v as num, target),
        ];
        return target == DataType.float32
            ? NumericColumn.float32(
                name: name,
                values: doubles,
                nullable: nullable,
              )
            : NumericColumn.float64(
                name: name,
                values: doubles,
                nullable: nullable,
              );
      }
      final ints = [
        for (final v in values) v == null ? null : (v as num).toInt(),
      ];
      switch (target) {
        case DataType.int8:
          return NumericColumn.int8(
            name: name,
            values: ints,
            nullable: nullable,
          );
        case DataType.int16:
          return NumericColumn.int16(
            name: name,
            values: ints,
            nullable: nullable,
          );
        case DataType.int32:
          return NumericColumn.int32(
            name: name,
            values: ints,
            nullable: nullable,
          );
        case DataType.int64:
          return NumericColumn.int64(
            name: name,
            values: ints,
            nullable: nullable,
          );
        case DataType.decimal:
          // Integer → scale-0 unscaled BigInt (e.g. `int_col UNION SUM(x)`); a
          // float→decimal set-op needs a scale we don't carry here, so fall
          // through to the loud error rather than store a double in a decimal.
          if (dataType != DataType.float32 && dataType != DataType.float64) {
            return ListColumn<BigInt>(
              name: name,
              dataType: DataType.decimal,
              data: [
                for (final v in values)
                  if (v == null) null else BigInt.from((v as num).toInt()),
              ],
              nullable: nullable,
            );
          }
          break;
        default:
          break;
      }
    }
    throw ArgumentError('Cannot cast column "$name" from $dataType to $target');
  }

  /// [length] nulls named [name], typed [target].
  Column _empty(DataType target) {
    final values = List<Never?>.filled(length, null);
    return switch (target) {
      DataType.int8 => NumericColumn.int8(name: name, values: values),
      DataType.int16 => NumericColumn.int16(name: name, values: values),
      DataType.int32 => NumericColumn.int32(name: name, values: values),
      DataType.int64 => NumericColumn.int64(name: name, values: values),
      DataType.float32 => NumericColumn.float32(name: name, values: values),
      DataType.float64 => NumericColumn.float64(name: name, values: values),
      DataType.string => StringColumn(name: name, values: values),
      DataType.boolean => BoolColumn(name: name, values: values),
      _ => ListColumn<Object?>(name: name, dataType: target, data: values),
    };
  }

  /// Restate this decimal column's UNSCALED values from scale [from] to scale
  /// [to], so two decimals written at different scales become the same
  /// representation and can be concatenated, compared, and deduplicated as one
  /// column. `1.5` is `BigInt(15)` at scale 1 and `BigInt(150)` at scale 2.
  /// WIDENING ONLY: narrowing would drop digits, and every caller aligns to the
  /// wider of the two scales.
  Column rescaleDecimal(int from, int to) {
    if (dataType != DataType.decimal) {
      throw ArgumentError('rescaleDecimal on a non-decimal column "$name"');
    }
    if (to < from) {
      throw ArgumentError(
        'rescaleDecimal cannot narrow scale $from to $to on "$name"',
      );
    }
    if (to == from) return this;
    final factor = BigInt.from(10).pow(to - from);
    return ListColumn<BigInt>(
      name: name,
      dataType: DataType.decimal,
      data: [
        for (final v in toList()) v == null ? null : (v as BigInt) * factor,
      ],
      nullable: nullable,
    );
  }

  /// Concatenate [pieces] (same name/logical type) into one column. Typed
  /// numeric pieces of one family (all integer or all float) are copied into a
  /// typed buffer of the widest piece's type, with packed-null-bitmap
  /// realignment; string and boolean pieces stay typed; anything else falls
  /// back to a boxed [ListColumn]. Lives in core so format readers don't depend on the SQL
  /// kernel layer to concatenate columns.
  static Column concat(List<Column> pieces) {
    final name = pieces.first.name;
    // All pieces must name the same column: a mismatch is a caller bug that
    // would otherwise silently relabel data.
    for (final c in pieces) {
      if (c.name != name) {
        throw ArgumentError(
          'concat: column name mismatch ("${c.name}" vs "$name")',
        );
      }
    }
    // The result type is the first CONCRETE (non-null_) piece; an all-null
    // (null_) piece widens to it. Mixed concrete types are a conflict, caught in
    // the boxed fallback below rather than mislabeled under the first type.
    var dataType = DataType.null_;
    for (final c in pieces) {
      if (c.dataType != DataType.null_) {
        dataType = c.dataType;
        break;
      }
    }
    final nullable = pieces.any((c) => c.hasNulls);

    // Promote to the WIDEST piece, not just the first's family: a trailing
    // int64 piece copied into an int32 buffer would truncate silently. Mixed
    // integer/float families fall through to the boxed path.
    bool integer(DataType t) =>
        t == DataType.int8 ||
        t == DataType.int16 ||
        t == DataType.int32 ||
        t == DataType.int64;
    bool float(DataType t) => t == DataType.float32 || t == DataType.float64;
    // Promote to the WIDEST piece's ACTUAL width (not a blanket int32) so an
    // all-int8 (or all-int16) concat stays int8 (int16) and its dtype keeps
    // matching the schema, while a mixed set widens only as far as needed (a
    // trailing int64 into an int32 buffer would truncate).
    int rank(DataType t) => t == DataType.int8
        ? 0
        : t == DataType.int16
        ? 1
        : t == DataType.int32
        ? 2
        : 3;
    DataType? widest;
    if (pieces.every((c) => integer(c.dataType))) {
      widest = pieces
          .map((c) => c.dataType)
          .reduce((a, b) => rank(a) >= rank(b) ? a : b);
    } else if (pieces.every((c) => float(c.dataType))) {
      widest = pieces.any((c) => c.dataType == DataType.float64)
          ? DataType.float64
          : DataType.float32;
    }

    if (widest != null && pieces.every((c) => c is NumericVector)) {
      final typed = pieces.cast<NumericVector>();
      var total = 0;
      for (final c in typed) {
        total += c.length;
      }
      final out = NumericVector._allocate(widest, total);
      final bitmap = nullable ? Uint8List((total + 7) >> 3) : null;
      var offset = 0;
      for (final c in typed) {
        final length = c.length;
        NumericVector._copyRange(out, offset, c.raw, 0, length);
        final source = c.nulls;
        if (bitmap != null && source != null) {
          // Shift this piece's bits to the running offset.
          _copyBits(source, bitmap, offset, length);
        }
        offset += length;
      }
      return NumericVector.adopt(
        name: name,
        dataType: widest,
        data: out,
        nullBitmap: bitmap,
      );
    }

    if (pieces.every((c) => c is StringVector)) {
      return StringVector._concat(name, pieces.cast<StringVector>());
    }
    if (pieces.every((c) => c is BoolVector)) {
      return BoolVector._concat(name, pieces.cast<BoolVector>());
    }

    // One numeric family with a boxed piece among them: box the values under
    // the widest type.
    if (widest != null) {
      return ListColumn(
        name: name,
        dataType: widest,
        data: [for (final c in pieces) ...c.toList()],
        nullable: nullable,
      );
    }

    // Boxed fallback: pieces that aren't a uniform numeric family must all be
    // the SAME concrete type (or null_): concatenating incompatible types under
    // one label is exactly the "lying column" this guards against.
    for (final c in pieces) {
      if (c.dataType != DataType.null_ && c.dataType != dataType) {
        throw ArgumentError(
          'concat: incompatible column types (${c.dataType} vs $dataType) '
          'for "$name"',
        );
      }
    }
    final all = <dynamic>[];
    for (final c in pieces) {
      all.addAll(c.toList());
    }
    return ListColumn(
      name: name,
      dataType: dataType,
      data: all,
      nullable: nullable,
    );
  }
}

/// OR the first [count] bits of [source] into [target] from bit [at]. Whole
/// bytes are copied when [at] falls on a byte boundary, as it does for pieces
/// of a multiple of 8 rows; [target] must be clear over the range.
void _copyBits(Uint8List source, Uint8List target, int at, int count) {
  var i = 0;
  if (at & 7 == 0) {
    final bytes = count >> 3;
    target.setRange(at >> 3, (at >> 3) + bytes, source);
    i = bytes << 3;
  }
  for (; i < count; i++) {
    if ((source[i >> 3] & (1 << (i & 7))) != 0) {
      final j = at + i;
      target[j >> 3] |= 1 << (j & 7);
    }
  }
}

/// Statistics for numeric columns
final class ColumnStats {
  /// The smallest non-null value.
  final num min;

  /// The largest non-null value.
  final num max;

  /// The mean of the non-null values.
  final double mean;

  /// The population standard deviation of the non-null values.
  final double stdDev;

  /// The number of nulls.
  final int nullCount;

  /// The number of distinct non-null values, counted up to a fixed cap: a
  /// column with more reports the cap.
  final int distinctCount;

  /// Statistics with the given values.
  const ColumnStats({
    required this.min,
    required this.max,
    required this.mean,
    required this.stdDev,
    required this.nullCount,
    required this.distinctCount,
  });
}

/// Stats for a decimal [column] whose unscaled values sit at [scale].
ColumnStats? decimalStats(Column column, int scale) => _summarize([
  for (int i = 0; i < column.length; i++)
    if (column[i] case final BigInt unscaled)
      double.parse(formatDecimal(unscaled, scale))
    else
      null,
], column.length);

/// Welford's online mean/variance (stable for large magnitudes) plus a bounded
/// distinct set, mirroring NumericColumn.stats, over [values] of a column of
/// [length] rows; null when every value is null.
ColumnStats? _summarize(Iterable<num?> values, int length) {
  num min = double.infinity;
  num max = double.negativeInfinity;
  double mean = 0, squares = 0;
  int count = 0;
  final seen = <num>{};
  var capped = false;

  for (final v in values) {
    if (v == null) continue;
    if (v < min) min = v;
    if (v > max) max = v;
    count++;
    final delta = v - mean;
    mean += delta / count;
    squares += delta * (v - mean);
    if (!capped) {
      seen.add(v);
      if (seen.length >= distinctStatsCap) capped = true;
    }
  }

  if (count == 0) return null;

  final variance = squares / count;
  final deviation = variance > 0 ? sqrt(variance) : 0.0;

  return ColumnStats(
    min: min,
    max: max,
    mean: mean,
    stdDev: deviation,
    nullCount: length - count,
    distinctCount: seen.length,
  );
}

/// Generic column of boxed values, held in a List
abstract final class ListColumn<T> extends Column<T> {
  ListColumn._({required super.name, required super.dataType, super.nullable});

  /// Copy [data]; a null in a non-nullable column is an [ArgumentError].
  factory ListColumn({
    required String name,
    required DataType dataType,
    required List<T?> data,
    bool nullable,
  }) = ListVector<T>._;
}

/// Optimized column for numeric types using typed arrays
abstract final class NumericColumn<T extends num> extends Column<T> {
  NumericColumn._({
    required super.name,
    required super.dataType,
    required super.nullable,
  });

  /// Throw if [value] is outside the signed range [[min], [max]] of a fixed-width
  /// integer column. A narrow typed list silently WRAPS an out-of-range int
  /// (Int8List stores 300 as 44), so the explicit-width factories range-check
  /// first: the caller asked for this width, so an oversized value is an error,
  /// not something to silently truncate or auto-widen.
  static void _range(int value, int min, int max, String name, int index) {
    if (value < min || value > max) {
      throw RangeError(
        'Column "$name": value $value at index $index is outside the '
        '[$min, $max] range of its integer type',
      );
    }
  }

  factory NumericColumn.int8({
    required String name,
    required List<int?> values,
    bool nullable = true,
  }) {
    final data = Int8List(values.length);
    final nulls = nullable ? Uint8List((values.length + 7) ~/ 8) : null;

    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        _range(values[i]!, -128, 127, name, i);
        data[i] = values[i]!;
      } else if (nulls != null) {
        final byte = i ~/ 8;
        final bit = i % 8;
        nulls[byte] |= (1 << bit);
      } else {
        throw ArgumentError(
          'Column "$name" is non-nullable but the value at index $i is null',
        );
      }
    }

    return NumericVector._(
      name: name,
      dataType: DataType.int8,
      data: data,
      nullBitmap: nulls,
    );
  }

  factory NumericColumn.int16({
    required String name,
    required List<int?> values,
    bool nullable = true,
  }) {
    final data = Int16List(values.length);
    final nulls = nullable ? Uint8List((values.length + 7) ~/ 8) : null;

    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        _range(values[i]!, -32768, 32767, name, i);
        data[i] = values[i]!;
      } else if (nulls != null) {
        final byte = i ~/ 8;
        final bit = i % 8;
        nulls[byte] |= (1 << bit);
      } else {
        throw ArgumentError(
          'Column "$name" is non-nullable but the value at index $i is null',
        );
      }
    }

    return NumericVector._(
      name: name,
      dataType: DataType.int16,
      data: data,
      nullBitmap: nulls,
    );
  }

  factory NumericColumn.int32({
    required String name,
    required List<int?> values,
    bool nullable = true,
  }) {
    final data = Int32List(values.length);
    final nulls = nullable ? Uint8List((values.length + 7) ~/ 8) : null;

    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        _range(values[i]!, -2147483648, 2147483647, name, i);
        data[i] = values[i]!;
      } else if (nulls != null) {
        final byte = i ~/ 8;
        final bit = i % 8;
        nulls[byte] |= (1 << bit);
      } else {
        throw ArgumentError(
          'Column "$name" is non-nullable but the value at index $i is null',
        );
      }
    }

    return NumericVector._(
      name: name,
      dataType: DataType.int32,
      data: data,
      nullBitmap: nulls,
    );
  }

  factory NumericColumn.float64({
    required String name,
    required List<double?> values,
    bool nullable = true,
  }) {
    final data = Float64List(values.length);
    final nulls = nullable ? Uint8List((values.length + 7) ~/ 8) : null;

    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        data[i] = values[i]!;
      } else if (nulls != null) {
        final byte = i ~/ 8;
        final bit = i % 8;
        nulls[byte] |= (1 << bit);
      } else {
        throw ArgumentError(
          'Column "$name" is non-nullable but the value at index $i is null',
        );
      }
    }

    return NumericVector._(
      name: name,
      dataType: DataType.float64,
      data: data,
      nullBitmap: nulls,
    );
  }

  factory NumericColumn.int64({
    required String name,
    required List<int?> values,
    bool nullable = true,
  }) {
    final data = Int64List(values.length);
    final nulls = nullable ? Uint8List((values.length + 7) ~/ 8) : null;
    // No range check: a Dart VM int is already exactly 64-bit, so it always fits
    // an Int64List (values beyond 64 bits arrive as BigInt on the decimal path).
    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        data[i] = values[i]!;
      } else if (nulls != null) {
        nulls[i ~/ 8] |= (1 << (i % 8));
      } else {
        throw ArgumentError(
          'Column "$name" is non-nullable but the value at index $i is null',
        );
      }
    }
    return NumericVector._(
      name: name,
      dataType: DataType.int64,
      data: data,
      nullBitmap: nulls,
    );
  }

  factory NumericColumn.float32({
    required String name,
    required List<double?> values,
    bool nullable = true,
  }) {
    final data = Float32List(values.length);
    final nulls = nullable ? Uint8List((values.length + 7) ~/ 8) : null;
    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        data[i] = values[i]!;
      } else if (nulls != null) {
        nulls[i ~/ 8] |= (1 << (i % 8));
      } else {
        throw ArgumentError(
          'Column "$name" is non-nullable but the value at index $i is null',
        );
      }
    }
    return NumericVector._(
      name: name,
      dataType: DataType.float32,
      data: data,
      nullBitmap: nulls,
    );
  }

  /// The typed buffer (Int8/Int32/Int64/Float32/Float64List) as a **read-only
  /// view**, for callers that want to iterate without boxing. The view rejects
  /// writes, so external code can't mutate the column's storage behind its
  /// schema; the concrete type is preserved (`buffer as Int64List` still works).
  /// Use with [nullBitmap]/[isNullAt].
  TypedData get buffer;

  /// Packed null bitmap (bit set = null) as a **read-only view**, or null when
  /// the column is non-null.
  Uint8List? get nullBitmap;

  /// Whether the value at [index] is null.
  bool isNullAt(int index);
}

/// Strings stored as one UTF-8 buffer with an offset per row: row `i` is
/// `bytes[offsets[i], offsets[i + 1])`. A value costs its encoded bytes plus a
/// four-byte offset, where a [ListColumn] holds a reference and a heap
/// `String` of two bytes per code unit. Nulls are a packed bitmap (bit set
/// means null) present only when the column is nullable; a null row has an
/// empty range.
///
/// UTF-8 cannot hold an unpaired UTF-16 surrogate (U+D800 to U+DFFF), so one
/// in a Dart string is stored, and read back, as U+FFFD. Strings decoded from
/// a file never contain one; only strings built in Dart can.
abstract final class StringColumn extends Column<String> {
  StringColumn._({required super.name, required super.nullable})
    : super(dataType: DataType.string);

  /// Encode [values]; a null in a non-nullable column is an [ArgumentError].
  factory StringColumn({
    required String name,
    required List<String?> values,
    bool nullable = true,
  }) {
    final count = values.length;
    final offsets = Int32List(count + 1);
    final nulls = nullable ? Uint8List((count + 7) >> 3) : null;
    final builder = BytesBuilder(copy: false);
    for (var i = 0; i < count; i++) {
      final value = values[i];
      if (value != null) {
        builder.add(utf8.encode(value));
      } else if (nulls != null) {
        nulls[i >> 3] |= 1 << (i & 7);
      } else {
        throw ArgumentError(
          'Column "$name" is non-nullable but the value at index $i is null',
        );
      }
      if (builder.length > _maxBytes) {
        throw ArgumentError(
          'Column "$name" holds more than $_maxBytes bytes of strings',
        );
      }
      offsets[i + 1] = builder.length;
    }
    return StringVector._(
      name: name,
      offsets: offsets,
      bytes: builder.takeBytes(),
      nulls: nulls,
      nullable: nullable,
    );
  }

  /// Offsets are Int32, so the buffer is capped at the largest Int32 value.
  static const _maxBytes = 0x7fffffff;

  /// Whether row [index] is null.
  bool isNullAt(int index);

  /// Order of the non-null row [index] against the non-null row [at] of
  /// [other], read from the bytes in the order [String.compareTo] gives.
  int compareAt(int index, StringColumn other, int at);

  /// Order of the non-null row [index] against the UTF-8 [bytes] of a value.
  int compareBytes(int index, Uint8List bytes);

  /// The values, decoding each distinct value once and sharing that [String]
  /// across its rows.
  List<String?> interned();

  /// Rows [indices] in order; a negative index gives a null row.
  StringColumn gather(Int32List indices);

  @override
  StringColumn slice(int start, [int? end]);

  @override
  StringColumn filter(List<int> indices);

  @override
  StringColumn copy();

  @override
  StringColumn withName(String name);
}

/// Booleans packed one bit a value (bit set means true), with a packed null
/// bitmap (bit set means null) present only when the column is nullable: two
/// bits a value at most, where a [ListColumn] holds an 8-byte reference.
abstract final class BoolColumn extends Column<bool> {
  BoolColumn._({required super.name, required super.nullable})
    : super(dataType: DataType.boolean);

  /// Pack [values]; a null in a non-nullable column is an [ArgumentError].
  factory BoolColumn({
    required String name,
    required List<bool?> values,
    bool nullable = true,
  }) {
    final count = values.length;
    final bits = Uint8List((count + 7) >> 3);
    final nulls = nullable ? Uint8List((count + 7) >> 3) : null;
    for (var i = 0; i < count; i++) {
      final value = values[i];
      if (value == true) {
        bits[i >> 3] |= 1 << (i & 7);
      } else if (value == null) {
        if (nulls == null) {
          throw ArgumentError(
            'Column "$name" is non-nullable but the value at index $i is null',
          );
        }
        nulls[i >> 3] |= 1 << (i & 7);
      }
    }
    return BoolVector._(
      name: name,
      length: count,
      bits: bits,
      nulls: nulls,
      nullable: nullable,
    );
  }

  /// Whether row [index] is null.
  bool isNullAt(int index);

  /// Rows [indices] in order; a negative index gives a null row.
  BoolColumn gather(Int32List indices);

  @override
  BoolColumn slice(int start, [int? end]);

  @override
  BoolColumn filter(List<int> indices);

  @override
  BoolColumn copy();

  @override
  BoolColumn withName(String name);
}
