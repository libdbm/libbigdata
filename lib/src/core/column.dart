import 'dart:math';
import 'dart:typed_data';
import 'package:meta/meta.dart';
import 'data_type.dart';

/// Bound on a column's distinct-value probe (for `distinctCount` stats): past
/// this many distinct values the exact count saturates, so a high-cardinality
/// column can't allocate a Set as large as itself. Shared by NumericColumn and
/// ListColumn so both stats paths are bounded identically.
const int distinctStatsCap = 1 << 16;

/// Base class for columnar data storage
@immutable
abstract class Column<T> {
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

  /// The same column under a new [name], sharing the underlying (immutable)
  /// data — no materialization or type demotion.
  Column<T> withName(String name);

  /// Apply a function to each element
  Column<R> map<R>(R Function(T?) fn, DataType resultType);

  /// Get statistics for numeric columns
  ColumnStats? get stats;

  /// Widest integer each float type represents exactly; every |v| at or below it
  /// round-trips, so the exactness check below only runs past it. float32 holds
  /// integers exactly only to 2^24 — using the float64 limit (2^53) for a float32
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
      // Compare in BigInt space — toInt() CLAMPS at the int64 boundary, masking
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
  /// Supports the numeric promotions `DataType.commonType` produces — int→wider
  /// int, int→float, float32→float64, int→decimal — used to align set-operation
  /// branches to a shared type before union/concat. Anything else (e.g.
  /// →string) throws.
  ///
  /// int→float is NOT lossless in general: float64 holds every integer only up
  /// to 2^53 (float32, up to 2^24), so a 64-bit id could round to a DIFFERENT
  /// number. `int64 UNION float64` is float64 under standard SQL, and the
  /// lossless alternative (decimal) needs a scale that only the data can supply
  /// — which a set-op's plan-time schema cannot know. So a value that would
  /// round is REJECTED here rather than silently rewritten.
  Column castTo(DataType target) {
    if (dataType == target) return this;
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

  /// Restate this decimal column's UNSCALED values from scale [from] to scale
  /// [to], so two decimals written at different scales become the same
  /// representation and can be concatenated, compared, and deduplicated as one
  /// column. `1.5` is `BigInt(15)` at scale 1 and `BigInt(150)` at scale 2.
  /// WIDENING ONLY — narrowing would drop digits, and every caller aligns to the
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

  /// Concatenate [pieces] (same name/logical type) into one column. Same-width
  /// numeric pieces (int8/int16 widen to int32) are copied into a typed buffer
  /// with packed-null-bitmap realignment; anything else falls back to a boxed
  /// [ListColumn]. Lives in core so format readers don't depend on the SQL
  /// kernel layer to concatenate columns.
  static Column concat(List<Column> pieces) {
    final name = pieces.first.name;
    // All pieces must name the same column — a mismatch is a caller bug that
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
    bool isInt(DataType t) =>
        t == DataType.int8 ||
        t == DataType.int16 ||
        t == DataType.int32 ||
        t == DataType.int64;
    bool isFloat(DataType t) => t == DataType.float32 || t == DataType.float64;
    // Promote to the WIDEST piece's ACTUAL width — not a blanket int32 — so an
    // all-int8 (or all-int16) concat stays int8 (int16) and its dtype keeps
    // matching the schema, while a mixed set widens only as far as needed (a
    // trailing int64 into an int32 buffer would truncate).
    int intRank(DataType t) => t == DataType.int8
        ? 0
        : t == DataType.int16
        ? 1
        : t == DataType.int32
        ? 2
        : 3;
    DataType? outType;
    if (pieces.every((c) => isInt(c.dataType))) {
      outType = pieces
          .map((c) => c.dataType)
          .reduce((a, b) => intRank(a) >= intRank(b) ? a : b);
    } else if (pieces.every((c) => isFloat(c.dataType))) {
      outType = pieces.any((c) => c.dataType == DataType.float64)
          ? DataType.float64
          : DataType.float32;
    }

    if (outType != null && pieces.every((c) => c is NumericColumn)) {
      final typed = pieces.cast<NumericColumn>();
      var total = 0;
      for (final c in typed) {
        total += c.length;
      }
      final out = NumericColumn._allocLike(outType, total);
      final bitmap = nullable ? Uint8List((total + 7) >> 3) : null;
      var offset = 0;
      for (final c in typed) {
        final length = c.length;
        NumericColumn._copyRange(out, offset, c.rawBuffer, 0, length);
        final src = c.rawNullBitmap;
        if (bitmap != null && src != null) {
          for (int i = 0; i < length; i++) {
            if ((src[i >> 3] & (1 << (i & 7))) != 0) {
              final j =
                  offset + i; // shift this piece's bits to the running offset
              bitmap[j >> 3] |= (1 << (j & 7));
            }
          }
        }
        offset += length;
      }
      return NumericColumn.adopt(
        name: name,
        dataType: outType,
        data: out,
        nullBitmap: bitmap,
      );
    }

    // Boxed fallback: pieces that aren't a uniform numeric family must all be
    // the SAME concrete type (or null_) — concatenating incompatible types under
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

/// Statistics for numeric columns
class ColumnStats {
  final num min;
  final num max;
  final double mean;
  final double stdDev;
  final int nullCount;
  final int distinctCount;

  const ColumnStats({
    required this.min,
    required this.max,
    required this.mean,
    required this.stdDev,
    required this.nullCount,
    required this.distinctCount,
  });
}

/// Generic column implementation using List
class ListColumn<T> extends Column<T> {
  final List<T?> _data;

  /// Whether any slot is null. Only the ANSWER is kept: retaining the null
  /// POSITIONS costs a heap `Set<int>` entry per null — several times the
  /// column's own payload for a mostly-null column, and invisible to
  /// [memoryUsage], which would under-count the column and let it slip past a
  /// spill/maxBytes budget.
  final bool _hasNulls;

  ListColumn({
    required super.name,
    required super.dataType,
    required List<T?> data,
    super.nullable,
  }) : _data = List.unmodifiable(data),
       _hasNulls = nullable && data.any((value) => value == null) {
    // A non-nullable column must not silently retain a null and lie via
    // hasNulls (which drives e.g. Parquet's required-leaf decision).
    if (!nullable && _data.any((v) => v == null)) {
      throw ArgumentError(
        'Column "$name" is non-nullable but contains a null value',
      );
    }
  }

  @override
  int get length => _data.length;

  @override
  bool get hasNulls => _hasNulls;

  @override
  T? operator [](int index) {
    if (index < 0 || index >= length) {
      throw RangeError('Index $index out of range [0, $length)');
    }
    return _data[index];
  }

  @override
  Column<T> slice(int start, [int? end]) {
    final stop = end ?? length;
    if (start < 0 || start > length || stop < start || stop > length) {
      throw RangeError(
        'Invalid slice range [$start, $stop) for column of length $length',
      );
    }
    return ListColumn(
      name: name,
      dataType: dataType,
      data: _data.sublist(start, stop),
      nullable: nullable,
    );
  }

  @override
  Column<T> filter(List<int> indices) {
    final filtered = <T?>[];
    for (final index in indices) {
      if (index < 0 || index >= length) {
        throw RangeError('Index $index out of range [0, $length)');
      }
      filtered.add(_data[index]);
    }
    return ListColumn(
      name: name,
      dataType: dataType,
      data: filtered,
      nullable: nullable,
    );
  }

  @override
  List<T?> toList() => List.from(_data);

  @override
  int get memoryUsage {
    // A ListColumn stores BOXED objects, not a typed buffer, so `fixedSize`
    // (bytes in a packed buffer) under-counts badly here: every row costs at
    // least one reference slot, and DateTime/BigInt payloads live on the heap
    // beyond it. Base = one reference per row (Smi ints and canonical bools live
    // inline in that slot); add heap payloads below. Under-counting here would
    // let a boxed column slip past the maxBytes/spill budget.
    int size = length * 8;
    // fromRows builds string columns as ListColumn<dynamic>, so a `T == String`
    // check misses them and under-reports — driving spill/budget decisions to
    // under-spill. Key off the runtime dtype instead.
    if (dataType == DataType.string) {
      for (final value in _data) {
        if (value is String) {
          size += value.length * 2;
        }
      }
    } else if (dataType == DataType.timestamp || dataType == DataType.date) {
      // Each present value is a heap DateTime beyond its reference slot.
      for (final value in _data) {
        if (value is DateTime) size += 16;
      }
    } else if (dataType == DataType.decimal) {
      for (final value in _data) {
        if (value is BigInt) size += 24;
      }
    } else if (dataType == DataType.binary) {
      // A single fat binary payload must not masquerade as 8 bytes and slip
      // past a maxBytes/spill budget.
      for (final value in _data) {
        if (value is List<int>) {
          size += value.length;
        }
      }
    } else if (dataType.isNested) {
      // Recursively size list/map/struct payloads so a fat binary/string nested
      // inside can't masquerade as a couple of references and slip past a
      // maxBytes/spill budget. The per-row reference (8) is already in `size`, so
      // add only the deep payload beyond it. A shared `seen` set counts shared
      // substructures once and, with the depth cap, bounds cyclic data.
      final seen = <Object>{};
      for (final value in _data) {
        size += _deepSize(value, 0, seen) - 8;
      }
    }
    return size;
  }

  static const _maxNestedDepth = 100;

  /// Approximate deep byte size of a nested value: String x2, container
  /// reference + recursively its children. Guarded by [_maxNestedDepth] and an
  /// identity [seen] set (a revisited/too-deep node counts as a bare reference).
  static int _deepSize(Object? value, int depth, Set<Object> seen) {
    if (value == null) return 8;
    if (value is String) return 8 + value.length * 2;
    if (value is List || value is Map) {
      if (depth >= _maxNestedDepth || !seen.add(value)) return 8;
      var size = 8;
      if (value is List) {
        for (final element in value) {
          size += _deepSize(element, depth + 1, seen);
        }
      } else if (value is Map) {
        value.forEach((key, val) {
          size += _deepSize(key, depth + 1, seen);
          size += _deepSize(val, depth + 1, seen);
        });
      }
      return size;
    }
    return 8; // scalar (num/bool/DateTime/...)
  }

  @override
  Column<T> copy() => ListColumn(
    name: name,
    dataType: dataType,
    data: List.from(_data),
    nullable: nullable,
  );

  @override
  Column<T> withName(String name) => ListColumn<T>(
    name: name,
    dataType: dataType,
    data: _data, // shared (immutable); the ctor re-wraps unmodifiable
    nullable: nullable,
  );

  @override
  Column<R> map<R>(R Function(T?) fn, DataType resultType) {
    final mapped = _data.map(fn).toList();
    return ListColumn<R>(
      name: name,
      dataType: resultType,
      data: mapped,
      nullable: nullable,
    );
  }

  @override
  ColumnStats? get stats {
    if (!dataType.isNumeric || _data.isEmpty) return null;

    num min = double.infinity;
    num max = double.negativeInfinity;
    // Welford's online mean/variance (stable for large magnitudes) + a bounded
    // distinct set — mirrors NumericColumn.stats so a numeric column that lands
    // in a ListColumn (int16/decimal, or a Kernels.fromValues fallback) gets the
    // same guards instead of an unbounded Set and a cancellation-prone variance.
    double mean = 0, m2 = 0;
    int count = 0;
    final seen = <T>{};
    var distinctCapped = false;

    for (final value in _data) {
      if (value != null) {
        final num v = value as num;
        if (v < min) min = v;
        if (v > max) max = v;
        count++;
        final delta = v - mean;
        mean += delta / count;
        m2 += delta * (v - mean);
        if (!distinctCapped) {
          seen.add(value);
          if (seen.length >= distinctStatsCap) distinctCapped = true;
        }
      }
    }

    if (count == 0) return null;

    final variance = m2 / count;
    final stdDev = variance > 0 ? sqrt(variance) : 0.0;

    return ColumnStats(
      min: min,
      max: max,
      mean: mean,
      stdDev: stdDev,
      nullCount: length - count,
      distinctCount: seen.length,
    );
  }
}

/// Optimized column for numeric types using typed arrays
class NumericColumn<T extends num> extends Column<T> {
  final TypedData _data;
  final Uint8List? _nullBitmap;
  final DataType _type;

  NumericColumn._({
    required super.name,
    required super.dataType,
    required TypedData data,
    Uint8List? nullBitmap,
  }) : _data = data,
       _nullBitmap = nullBitmap,
       _type = dataType,
       super(nullable: nullBitmap != null);

  /// Throw if [value] is outside the signed range [[min], [max]] of a fixed-width
  /// integer column. A narrow typed list silently WRAPS an out-of-range int
  /// (Int8List stores 300 as 44), so the explicit-width factories range-check
  /// first — the caller asked for this width, so an oversized value is an error,
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
    final nullBitmap = nullable ? Uint8List((values.length + 7) ~/ 8) : null;

    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        _range(values[i]!, -128, 127, name, i);
        data[i] = values[i]!;
      } else if (nullBitmap != null) {
        final byteIndex = i ~/ 8;
        final bitIndex = i % 8;
        nullBitmap[byteIndex] |= (1 << bitIndex);
      } else {
        throw ArgumentError(
          'Column "$name" is non-nullable but the value at index $i is null',
        );
      }
    }

    return NumericColumn._(
      name: name,
      dataType: DataType.int8,
      data: data,
      nullBitmap: nullBitmap,
    );
  }

  factory NumericColumn.int16({
    required String name,
    required List<int?> values,
    bool nullable = true,
  }) {
    final data = Int16List(values.length);
    final nullBitmap = nullable ? Uint8List((values.length + 7) ~/ 8) : null;

    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        _range(values[i]!, -32768, 32767, name, i);
        data[i] = values[i]!;
      } else if (nullBitmap != null) {
        final byteIndex = i ~/ 8;
        final bitIndex = i % 8;
        nullBitmap[byteIndex] |= (1 << bitIndex);
      } else {
        throw ArgumentError(
          'Column "$name" is non-nullable but the value at index $i is null',
        );
      }
    }

    return NumericColumn._(
      name: name,
      dataType: DataType.int16,
      data: data,
      nullBitmap: nullBitmap,
    );
  }

  factory NumericColumn.int32({
    required String name,
    required List<int?> values,
    bool nullable = true,
  }) {
    final data = Int32List(values.length);
    final nullBitmap = nullable ? Uint8List((values.length + 7) ~/ 8) : null;

    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        _range(values[i]!, -2147483648, 2147483647, name, i);
        data[i] = values[i]!;
      } else if (nullBitmap != null) {
        final byteIndex = i ~/ 8;
        final bitIndex = i % 8;
        nullBitmap[byteIndex] |= (1 << bitIndex);
      } else {
        throw ArgumentError(
          'Column "$name" is non-nullable but the value at index $i is null',
        );
      }
    }

    return NumericColumn._(
      name: name,
      dataType: DataType.int32,
      data: data,
      nullBitmap: nullBitmap,
    );
  }

  factory NumericColumn.float64({
    required String name,
    required List<double?> values,
    bool nullable = true,
  }) {
    final data = Float64List(values.length);
    final nullBitmap = nullable ? Uint8List((values.length + 7) ~/ 8) : null;

    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        data[i] = values[i]!;
      } else if (nullBitmap != null) {
        final byteIndex = i ~/ 8;
        final bitIndex = i % 8;
        nullBitmap[byteIndex] |= (1 << bitIndex);
      } else {
        throw ArgumentError(
          'Column "$name" is non-nullable but the value at index $i is null',
        );
      }
    }

    return NumericColumn._(
      name: name,
      dataType: DataType.float64,
      data: data,
      nullBitmap: nullBitmap,
    );
  }

  /// Adopts a prebuilt typed [data] buffer WITHOUT copying — the caller must not
  /// retain a mutable reference to [data] afterwards (the column is immutable).
  /// [dataType] must match the buffer width: int8->Int8List, int16->Int16List,
  /// int32->Int32List, int64->Int64List, float32->Float32List,
  /// float64->Float64List. Library-internal: external callers should build a
  /// column via the typed factories (which copy), not adopt a raw buffer.
  @internal
  factory NumericColumn.adopt({
    required String name,
    required DataType dataType,
    required TypedData data,
    Uint8List? nullBitmap,
  }) {
    final bufferOk = switch (dataType) {
      DataType.int8 => data is Int8List,
      DataType.int16 => data is Int16List,
      DataType.int32 => data is Int32List,
      DataType.int64 => data is Int64List,
      DataType.float32 => data is Float32List,
      DataType.float64 => data is Float64List,
      _ => false,
    };
    if (!bufferOk) {
      throw ArgumentError(
        'NumericColumn.adopt: buffer ${data.runtimeType} does not match '
        'dataType $dataType',
      );
    }
    if (nullBitmap != null) {
      final length = data.lengthInBytes ~/ (dataType.fixedSize ?? 8);
      final expected = (length + 7) >> 3;
      if (nullBitmap.length != expected) {
        throw ArgumentError(
          'NumericColumn.adopt: nullBitmap length ${nullBitmap.length} != '
          'expected $expected for $length rows',
        );
      }
    }
    return NumericColumn._(
      name: name,
      dataType: dataType,
      data: data,
      nullBitmap: nullBitmap,
    );
  }

  factory NumericColumn.int64({
    required String name,
    required List<int?> values,
    bool nullable = true,
  }) {
    final data = Int64List(values.length);
    final nullBitmap = nullable ? Uint8List((values.length + 7) ~/ 8) : null;
    // No range check: a Dart VM int is already exactly 64-bit, so it always fits
    // an Int64List (values beyond 64 bits arrive as BigInt on the decimal path).
    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        data[i] = values[i]!;
      } else if (nullBitmap != null) {
        nullBitmap[i ~/ 8] |= (1 << (i % 8));
      } else {
        throw ArgumentError(
          'Column "$name" is non-nullable but the value at index $i is null',
        );
      }
    }
    return NumericColumn._(
      name: name,
      dataType: DataType.int64,
      data: data,
      nullBitmap: nullBitmap,
    );
  }

  factory NumericColumn.float32({
    required String name,
    required List<double?> values,
    bool nullable = true,
  }) {
    final data = Float32List(values.length);
    final nullBitmap = nullable ? Uint8List((values.length + 7) ~/ 8) : null;
    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        data[i] = values[i]!;
      } else if (nullBitmap != null) {
        nullBitmap[i ~/ 8] |= (1 << (i % 8));
      } else {
        throw ArgumentError(
          'Column "$name" is non-nullable but the value at index $i is null',
        );
      }
    }
    return NumericColumn._(
      name: name,
      dataType: DataType.float32,
      data: data,
      nullBitmap: nullBitmap,
    );
  }

  /// The typed buffer (Int8/Int32/Int64/Float32/Float64List) as a **read-only
  /// view** — for callers that want to iterate without boxing. The view rejects
  /// writes, so external code can't mutate the column's storage behind its
  /// schema; the concrete type is preserved (`buffer as Int64List` still works).
  /// Trusted internal hot paths use [rawBuffer]. Use with [nullBitmap]/[isNullAt].
  TypedData get buffer => _asUnmodifiable(_data);

  /// Packed null bitmap (bit set = null) as a **read-only view**, or null when
  /// the column is non-null.
  Uint8List? get nullBitmap => _nullBitmap?.asUnmodifiableView();

  /// Internal, boxing-free access to the backing buffer for trusted hot paths
  /// (concat/stats/kernels). External callers must use [buffer], the unmodifiable
  /// view. Not part of the public API.
  @internal
  TypedData get rawBuffer => _data;

  /// Internal, boxing-free access to the backing null bitmap for trusted hot
  /// paths. External callers must use [nullBitmap]. Not part of the public API.
  @internal
  Uint8List? get rawNullBitmap => _nullBitmap;

  /// Whether the value at [index] is null.
  bool isNullAt(int index) => _isNull(index);

  bool _isNull(int index) {
    if (_nullBitmap == null) return false;
    final byteIndex = index ~/ 8;
    final bitIndex = index % 8;
    return (_nullBitmap[byteIndex] & (1 << bitIndex)) != 0;
  }

  @override
  int get length => _data.lengthInBytes ~/ (_type.fixedSize ?? 8);

  @override
  bool get hasNulls => _nullBitmap != null;

  @override
  T? operator [](int index) {
    if (index < 0 || index >= length) {
      throw RangeError('Index $index out of range [0, $length)');
    }
    if (_isNull(index)) return null;

    if (_data is Int8List) return (_data)[index] as T;
    if (_data is Int16List) return (_data)[index] as T;
    if (_data is Int32List) return (_data)[index] as T;
    if (_data is Int64List) return (_data)[index] as T;
    if (_data is Float32List) return (_data)[index] as T;
    if (_data is Float64List) return (_data)[index] as T;

    throw UnsupportedError('Unsupported numeric type');
  }

  @override
  Column<T> slice(int start, [int? end]) {
    final stop = end ?? length;
    if (start < 0 || start > length || stop < start || stop > length) {
      throw RangeError(
        'Invalid slice range [$start, $stop) for length $length',
      );
    }
    final count = stop - start;
    final out = _allocLike(_type, count);
    _copyRange(out, 0, _data, start, count); // typed bulk copy, no boxing
    final src = _nullBitmap;
    Uint8List? bitmap;
    if (src != null) {
      bitmap = Uint8List(
        (count + 7) >> 3,
      ); // realign packed null bits to offset 0
      for (int i = 0; i < count; i++) {
        final s = start + i;
        if ((src[s >> 3] & (1 << (s & 7))) != 0) {
          bitmap[i >> 3] |= (1 << (i & 7));
        }
      }
    }
    return NumericColumn.adopt(
          name: name,
          dataType: _type,
          data: out,
          nullBitmap: bitmap,
        )
        as Column<T>;
  }

  @override
  Column<T> filter(List<int> indices) {
    final n = indices.length;
    final out = _allocLike(_type, n);
    final dst = out as List<num>; // typed list, indexed assignment is unboxed
    final src = _data as List<num>;
    final srcBitmap = _nullBitmap;
    final bitmap = srcBitmap == null ? null : Uint8List((n + 7) >> 3);
    for (int i = 0; i < n; i++) {
      final s = indices[i];
      if (s < 0 || s >= length) {
        throw RangeError('Index $s out of range [0, $length)');
      }
      if (srcBitmap != null && (srcBitmap[s >> 3] & (1 << (s & 7))) != 0) {
        bitmap![i >> 3] |= (1 << (i & 7));
      } else {
        dst[i] = src[s];
      }
    }
    return NumericColumn.adopt(
          name: name,
          dataType: _type,
          data: out,
          nullBitmap: bitmap,
        )
        as Column<T>;
  }

  /// Allocate a typed buffer of [n] elements matching numeric [type].
  /// Wrap [d] in a same-concrete-type unmodifiable view. `asUnmodifiableView`
  /// (Dart 3.3+, non-deprecated) returns e.g. an [Int64List] that throws on
  /// write, so `buffer as Int64List` casts still succeed. Covers exactly the
  /// buffer types [_allocLike]/`adopt` produce.
  static TypedData _asUnmodifiable(TypedData d) {
    if (d is Int8List) return d.asUnmodifiableView();
    if (d is Int16List) return d.asUnmodifiableView();
    if (d is Int32List) return d.asUnmodifiableView();
    if (d is Int64List) return d.asUnmodifiableView();
    if (d is Float32List) return d.asUnmodifiableView();
    if (d is Float64List) return d.asUnmodifiableView();
    throw UnsupportedError('not a numeric buffer type: ${d.runtimeType}');
  }

  static TypedData _allocLike(DataType type, int n) {
    switch (type) {
      case DataType.int8:
        return Int8List(n);
      case DataType.int16:
        return Int16List(n);
      case DataType.int32:
        return Int32List(n);
      case DataType.int64:
        return Int64List(n);
      case DataType.float32:
        return Float32List(n);
      case DataType.float64:
        return Float64List(n);
      default:
        throw UnsupportedError('not a numeric buffer type: $type');
    }
  }

  /// Typed `setRange` copy of [length] elements from [src]@[from] into
  /// [out]@[offset]. Widens when [out] is wider than [src] (both are List of the
  /// same num kind).
  static void _copyRange(
    TypedData out,
    int offset,
    TypedData src,
    int from,
    int length,
  ) {
    if (out is Int8List) {
      out.setRange(offset, offset + length, src as List<int>, from);
    } else if (out is Int16List) {
      out.setRange(offset, offset + length, src as List<int>, from);
    } else if (out is Int32List) {
      out.setRange(offset, offset + length, src as List<int>, from);
    } else if (out is Int64List) {
      out.setRange(offset, offset + length, src as List<int>, from);
    } else if (out is Float32List) {
      out.setRange(offset, offset + length, src as List<double>, from);
    } else if (out is Float64List) {
      out.setRange(offset, offset + length, src as List<double>, from);
    }
  }

  @override
  List<T?> toList() {
    final n = length;
    final buffer = _data;
    final bitmap = _nullBitmap;
    // Box once into a List<T?> via a tight per-type loop (avoids operator[]'s
    // bounds/null/type-cascade overhead). Callers wanting zero boxing should
    // iterate [buffer] + [nullBitmap] directly.
    final list = List<T?>.filled(n, null);
    if (buffer is Int8List) {
      for (int i = 0; i < n; i++) {
        if (bitmap == null || (bitmap[i >> 3] & (1 << (i & 7))) == 0) {
          list[i] = buffer[i] as T;
        }
      }
    } else if (buffer is Int16List) {
      for (int i = 0; i < n; i++) {
        if (bitmap == null || (bitmap[i >> 3] & (1 << (i & 7))) == 0) {
          list[i] = buffer[i] as T;
        }
      }
    } else if (buffer is Int32List) {
      for (int i = 0; i < n; i++) {
        if (bitmap == null || (bitmap[i >> 3] & (1 << (i & 7))) == 0) {
          list[i] = buffer[i] as T;
        }
      }
    } else if (buffer is Int64List) {
      for (int i = 0; i < n; i++) {
        if (bitmap == null || (bitmap[i >> 3] & (1 << (i & 7))) == 0) {
          list[i] = buffer[i] as T;
        }
      }
    } else if (buffer is Float32List) {
      for (int i = 0; i < n; i++) {
        if (bitmap == null || (bitmap[i >> 3] & (1 << (i & 7))) == 0) {
          list[i] = buffer[i] as T;
        }
      }
    } else if (buffer is Float64List) {
      for (int i = 0; i < n; i++) {
        if (bitmap == null || (bitmap[i >> 3] & (1 << (i & 7))) == 0) {
          list[i] = buffer[i] as T;
        }
      }
    }
    return list;
  }

  @override
  int get memoryUsage =>
      _data.lengthInBytes + (_nullBitmap?.lengthInBytes ?? 0);

  @override
  Column<T> copy() {
    // Zero-box: memcpy the typed buffer + null bitmap, not via toList().
    final out = _allocLike(dataType, length);
    _copyRange(out, 0, _data, 0, length);
    return NumericColumn<T>.adopt(
      name: name,
      dataType: dataType,
      data: out,
      nullBitmap: _nullBitmap == null ? null : Uint8List.fromList(_nullBitmap),
    );
  }

  @override
  Column<T> withName(String name) => NumericColumn<T>.adopt(
    name: name,
    dataType: dataType,
    data: _data,
    nullBitmap: _nullBitmap,
  );

  @override
  Column<R> map<R>(R Function(T?) fn, DataType resultType) {
    final mapped = toList().map(fn).toList();
    return ListColumn<R>(
      name: name,
      dataType: resultType,
      data: mapped,
      nullable: nullable,
    );
  }

  @override
  ColumnStats? get stats {
    if (_data.lengthInBytes == 0) return null;

    num min = double.infinity;
    num max = double.negativeInfinity;
    // Welford's online algorithm: numerically stable mean/variance in one pass,
    // avoiding the catastrophic cancellation of `sumSquared/count - mean²` for
    // large-magnitude values.
    double mean = 0, m2 = 0;
    int count = 0;
    // Bound the distinct-value set: an N-row column of distinct values would
    // otherwise hold ~N boxed entries just to count distincts. Past the cap the
    // exact count is dropped (distinctCount saturates at the cap), like the
    // Parquet writer's bounded distinct probe.
    final seen = <num>{};
    var distinctCapped = false;

    // Read the flat typed buffer + null bitmap directly — no operator[] dispatch
    // or boxing per element.
    final buffer = _data as List;
    for (int i = 0; i < length; i++) {
      if (!isNullAt(i)) {
        final num value = buffer[i];
        if (value < min) min = value;
        if (value > max) max = value;
        count++;
        final delta = value - mean;
        mean += delta / count;
        m2 += delta * (value - mean);
        if (!distinctCapped) {
          seen.add(value);
          if (seen.length >= distinctStatsCap) distinctCapped = true;
        }
      }
    }

    if (count == 0) return null;

    final variance = m2 / count;
    final stdDev = variance > 0 ? sqrt(variance) : 0.0;

    return ColumnStats(
      min: min,
      max: max,
      mean: mean,
      stdDev: stdDev,
      nullCount: length - count,
      distinctCount: seen.length,
    );
  }
}
