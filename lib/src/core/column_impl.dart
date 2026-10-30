part of 'column.dart';

/// The typed-array implementation of [NumericColumn]: the buffer and null
/// bitmap it holds, adoption of prebuilt buffers without copying, and the
/// live buffers for trusted hot paths.
final class NumericVector<T extends num> extends NumericColumn<T> {
  final TypedData _data;
  final Uint8List? _nullBitmap;
  final DataType _type;
  final bool _hasNulls;

  NumericVector._({
    required super.name,
    required super.dataType,
    required TypedData data,
    Uint8List? nullBitmap,
  }) : _data = data,
       _nullBitmap = nullBitmap,
       _type = dataType,
       // A nullable column (the factories' default) carries a bitmap even
       // with no null in it, and a table then refused it under a
       // non-nullable field.
       _hasNulls = nullBitmap != null && nullBitmap.any((byte) => byte != 0),
       super._(nullable: nullBitmap != null);

  /// Adopts a prebuilt typed [data] buffer WITHOUT copying: the caller must not
  /// retain a mutable reference to [data] afterwards (the column is immutable).
  /// [dataType] must match the buffer width: int8->Int8List, int16->Int16List,
  /// int32->Int32List, int64->Int64List, float32->Float32List,
  /// float64->Float64List. Library-internal: external callers should build a
  /// column via the typed factories (which copy), not adopt a raw buffer.
  factory NumericVector.adopt({
    required String name,
    required DataType dataType,
    required TypedData data,
    Uint8List? nullBitmap,
  }) {
    final matches = switch (dataType) {
      DataType.int8 => data is Int8List,
      DataType.int16 => data is Int16List,
      DataType.int32 => data is Int32List,
      DataType.int64 => data is Int64List,
      DataType.float32 => data is Float32List,
      DataType.float64 => data is Float64List,
      _ => false,
    };
    if (!matches) {
      throw ArgumentError(
        'NumericVector.adopt: buffer ${data.runtimeType} does not match '
        'dataType $dataType',
      );
    }
    if (nullBitmap != null) {
      final length = data.lengthInBytes ~/ (dataType.fixedSize ?? 8);
      final expected = (length + 7) >> 3;
      if (nullBitmap.length != expected) {
        throw ArgumentError(
          'NumericVector.adopt: nullBitmap length ${nullBitmap.length} != '
          'expected $expected for $length rows',
        );
      }
    }
    return NumericVector._(
      name: name,
      dataType: dataType,
      data: data,
      nullBitmap: nullBitmap,
    );
  }

  @override
  TypedData get buffer => _asUnmodifiable(_data);

  @override
  Uint8List? get nullBitmap => _nullBitmap?.asUnmodifiableView();

  /// Internal, boxing-free access to the backing buffer for trusted hot paths
  /// (concat/stats/kernels). External callers must use [buffer], the unmodifiable
  /// view. Not part of the public API.
  TypedData get raw => _data;

  /// Internal, boxing-free access to the backing null bitmap for trusted hot
  /// paths. External callers must use [nullBitmap]. Not part of the public API.
  Uint8List? get nulls => _nullBitmap;

  @override
  bool isNullAt(int index) => _isNull(index);

  bool _isNull(int index) {
    if (_nullBitmap == null) return false;
    final byte = index ~/ 8;
    final bit = index % 8;
    return (_nullBitmap[byte] & (1 << bit)) != 0;
  }

  @override
  int get length => _data.lengthInBytes ~/ (_type.fixedSize ?? 8);

  @override
  bool get hasNulls => _hasNulls;

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
    final out = _allocate(_type, count);
    _copyRange(out, 0, _data, start, count); // typed bulk copy, no boxing
    final source = _nullBitmap;
    Uint8List? bitmap;
    if (source != null) {
      bitmap = Uint8List(
        (count + 7) >> 3,
      ); // realign packed null bits to offset 0
      for (int i = 0; i < count; i++) {
        final row = start + i;
        if ((source[row >> 3] & (1 << (row & 7))) != 0) {
          bitmap[i >> 3] |= (1 << (i & 7));
        }
      }
    }
    return NumericVector.adopt(
          name: name,
          dataType: _type,
          data: out,
          nullBitmap: bitmap,
        )
        as Column<T>;
  }

  @override
  Column<T> filter(List<int> indices) {
    final count = indices.length;
    final out = _allocate(_type, count);
    // Typed list, indexed assignment is unboxed.
    final target = out as List<num>;
    final source = _data as List<num>;
    final nulls = _nullBitmap;
    final bitmap = nulls == null ? null : Uint8List((count + 7) >> 3);
    for (int i = 0; i < count; i++) {
      final row = indices[i];
      if (row < 0 || row >= length) {
        throw RangeError('Index $row out of range [0, $length)');
      }
      if (nulls != null && (nulls[row >> 3] & (1 << (row & 7))) != 0) {
        bitmap![i >> 3] |= (1 << (i & 7));
      } else {
        target[i] = source[row];
      }
    }
    return NumericVector.adopt(
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
  /// buffer types [_allocate]/`adopt` produce.
  static TypedData _asUnmodifiable(TypedData data) {
    if (data is Int8List) return data.asUnmodifiableView();
    if (data is Int16List) return data.asUnmodifiableView();
    if (data is Int32List) return data.asUnmodifiableView();
    if (data is Int64List) return data.asUnmodifiableView();
    if (data is Float32List) return data.asUnmodifiableView();
    if (data is Float64List) return data.asUnmodifiableView();
    throw UnsupportedError('not a numeric buffer type: ${data.runtimeType}');
  }

  static TypedData _allocate(DataType type, int count) {
    switch (type) {
      case DataType.int8:
        return Int8List(count);
      case DataType.int16:
        return Int16List(count);
      case DataType.int32:
        return Int32List(count);
      case DataType.int64:
        return Int64List(count);
      case DataType.float32:
        return Float32List(count);
      case DataType.float64:
        return Float64List(count);
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
    TypedData source,
    int from,
    int length,
  ) {
    if (out is Int8List) {
      out.setRange(offset, offset + length, source as List<int>, from);
    } else if (out is Int16List) {
      out.setRange(offset, offset + length, source as List<int>, from);
    } else if (out is Int32List) {
      out.setRange(offset, offset + length, source as List<int>, from);
    } else if (out is Int64List) {
      out.setRange(offset, offset + length, source as List<int>, from);
    } else if (out is Float32List) {
      out.setRange(offset, offset + length, source as List<double>, from);
    } else if (out is Float64List) {
      out.setRange(offset, offset + length, source as List<double>, from);
    }
  }

  @override
  List<T?> toList() {
    final count = length;
    final buffer = _data;
    final bitmap = _nullBitmap;
    // Box once into a List<T?> via a tight per-type loop (avoids operator[]'s
    // bounds/null/type-cascade overhead). Callers wanting zero boxing should
    // iterate [buffer] + [nullBitmap] directly.
    final list = List<T?>.filled(count, null);
    if (buffer is Int8List) {
      for (int i = 0; i < count; i++) {
        if (bitmap == null || (bitmap[i >> 3] & (1 << (i & 7))) == 0) {
          list[i] = buffer[i] as T;
        }
      }
    } else if (buffer is Int16List) {
      for (int i = 0; i < count; i++) {
        if (bitmap == null || (bitmap[i >> 3] & (1 << (i & 7))) == 0) {
          list[i] = buffer[i] as T;
        }
      }
    } else if (buffer is Int32List) {
      for (int i = 0; i < count; i++) {
        if (bitmap == null || (bitmap[i >> 3] & (1 << (i & 7))) == 0) {
          list[i] = buffer[i] as T;
        }
      }
    } else if (buffer is Int64List) {
      for (int i = 0; i < count; i++) {
        if (bitmap == null || (bitmap[i >> 3] & (1 << (i & 7))) == 0) {
          list[i] = buffer[i] as T;
        }
      }
    } else if (buffer is Float32List) {
      for (int i = 0; i < count; i++) {
        if (bitmap == null || (bitmap[i >> 3] & (1 << (i & 7))) == 0) {
          list[i] = buffer[i] as T;
        }
      }
    } else if (buffer is Float64List) {
      for (int i = 0; i < count; i++) {
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
    final out = _allocate(dataType, length);
    _copyRange(out, 0, _data, 0, length);
    return NumericVector<T>.adopt(
      name: name,
      dataType: dataType,
      data: out,
      nullBitmap: _nullBitmap == null ? null : Uint8List.fromList(_nullBitmap),
    );
  }

  @override
  Column<T> withName(String name) => NumericVector<T>.adopt(
    name: name,
    dataType: dataType,
    data: _data,
    nullBitmap: _nullBitmap,
  );

  @override
  Column<R> map<R>(R Function(T?) transform, DataType resultType) {
    final mapped = toList().map(transform).toList();
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
    double mean = 0, squares = 0;
    int count = 0;
    // Bound the distinct-value set: an N-row column of distinct values would
    // otherwise hold ~N boxed entries just to count distincts. Past the cap the
    // exact count is dropped (distinctCount saturates at the cap), like the
    // Parquet writer's bounded distinct probe.
    final seen = <num>{};
    var capped = false;

    // Read the flat typed buffer + null bitmap directly: no operator[] dispatch
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
        squares += delta * (value - mean);
        if (!capped) {
          seen.add(value);
          if (seen.length >= distinctStatsCap) capped = true;
        }
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
}

/// The boxed implementation of [ListColumn]: an unmodifiable copy of the
/// values.
final class ListVector<T> extends ListColumn<T> {
  final List<T?> _data;

  /// Whether any slot is null. Only the ANSWER is kept: retaining the null
  /// POSITIONS costs a heap `Set<int>` entry per null: several times the
  /// column's own payload for a mostly-null column, and invisible to
  /// [memoryUsage], which would under-count the column and let it slip past a
  /// spill/maxBytes budget.
  final bool _hasNulls;

  ListVector._({
    required super.name,
    required super.dataType,
    required List<T?> data,
    super.nullable,
  }) : _data = List.unmodifiable(data),
       _hasNulls = nullable && data.any((value) => value == null),
       super._() {
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
    // check misses them and under-reports: driving spill/budget decisions to
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

  static const _maxDepth = 100;

  /// Approximate deep byte size of a nested value: String x2, container
  /// reference + recursively its children. Guarded by [_maxDepth] and an
  /// identity [seen] set (a revisited/too-deep node counts as a bare reference).
  static int _deepSize(Object? value, int depth, Set<Object> seen) {
    if (value == null) return 8;
    if (value is String) return 8 + value.length * 2;
    if (value is List || value is Map) {
      if (depth >= _maxDepth || !seen.add(value)) return 8;
      var size = 8;
      if (value is List) {
        for (final element in value) {
          size += _deepSize(element, depth + 1, seen);
        }
      } else if (value is Map) {
        value.forEach((key, item) {
          size += _deepSize(key, depth + 1, seen);
          size += _deepSize(item, depth + 1, seen);
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
    data: _data, // the ctor copies it into a new unmodifiable list
    nullable: nullable,
  );

  @override
  Column<R> map<R>(R Function(T?) transform, DataType resultType) {
    final mapped = _data.map(transform).toList();
    return ListColumn<R>(
      name: name,
      dataType: resultType,
      data: mapped,
      nullable: nullable,
    );
  }

  /// Null for a decimal: its values are unscaled and the scale lives on the
  /// field, so only [decimalStats] (which `Table.statistics` uses) can scale
  /// them.
  @override
  ColumnStats? get stats {
    if (!dataType.isNumeric || dataType == DataType.decimal || _data.isEmpty) {
      return null;
    }
    return _summarize(_data.cast<num?>(), length);
  }
}

/// The UTF-8 implementation of [StringColumn]: the offsets, bytes and null
/// bitmap it holds, adoption of prebuilt buffers without copying, and the
/// value codes the engine's kernels use.
final class StringVector extends StringColumn {
  final Int32List _offsets;
  final Uint8List _bytes;
  final Uint8List? _nulls;
  final bool _hasNulls;

  StringVector._({
    required super.name,
    required Int32List offsets,
    required Uint8List bytes,
    required Uint8List? nulls,
    required super.nullable,
  }) : _offsets = offsets,
       _bytes = bytes,
       _nulls = nulls,
       _hasNulls = nulls != null && nulls.any((byte) => byte != 0),
       super._();

  /// Wrap existing buffers without copying. [offsets] holds one more entry
  /// than there are rows, starts at 0, never decreases and ends within
  /// [bytes]; [nulls], when given, makes the column nullable. For trusted
  /// internal readers, like [NumericVector.adopt]; not public API.
  factory StringVector.adopt({
    required String name,
    required Int32List offsets,
    required Uint8List bytes,
    Uint8List? nulls,
  }) {
    if (offsets.isEmpty || offsets.first != 0) {
      throw ArgumentError('StringVector.adopt: offsets must start at 0');
    }
    for (var i = 1; i < offsets.length; i++) {
      if (offsets[i] < offsets[i - 1]) {
        throw ArgumentError('StringVector.adopt: offsets decrease at $i');
      }
    }
    if (offsets.last > bytes.length) {
      throw ArgumentError(
        'StringVector.adopt: offsets end at ${offsets.last} past '
        '${bytes.length} bytes',
      );
    }
    final count = offsets.length - 1;
    if (nulls != null && nulls.length != (count + 7) >> 3) {
      throw ArgumentError(
        'StringVector.adopt: null bitmap length ${nulls.length} != '
        'expected ${(count + 7) >> 3} for $count rows',
      );
    }
    return StringVector._(
      name: name,
      offsets: offsets,
      bytes: bytes,
      nulls: nulls,
      nullable: nulls != null,
    );
  }

  bool _isNull(int index) {
    final nulls = _nulls;
    return nulls != null && (nulls[index >> 3] & (1 << (index & 7))) != 0;
  }

  String _decode(int index) => utf8.decode(
    Uint8List.sublistView(_bytes, _offsets[index], _offsets[index + 1]),
  );

  @override
  bool isNullAt(int index) => _isNull(index);

  @override
  int compareAt(int index, StringColumn other, int at) {
    final vector = other as StringVector;
    return _order(
      _bytes,
      _offsets[index],
      _offsets[index + 1],
      vector._bytes,
      vector._offsets[at],
      vector._offsets[at + 1],
    );
  }

  @override
  int compareBytes(int index, Uint8List bytes) => _order(
    _bytes,
    _offsets[index],
    _offsets[index + 1],
    bytes,
    0,
    bytes.length,
  );

  /// UTF-8 byte order is code point order, where [String.compareTo] orders
  /// UTF-16 code units. The two differ only where a character of U+E000 to
  /// U+FFFF (lead byte EE or EF) meets one past U+FFFF (lead byte F0 to F4):
  /// UTF-16 writes the latter as surrogates, U+D800 to U+DFFF, so it sorts
  /// first. Where the bytes first differ, both sides start a character, so
  /// checking the two lead bytes there is enough.
  static int _order(
    Uint8List a,
    int aStart,
    int aEnd,
    Uint8List b,
    int bStart,
    int bEnd,
  ) {
    final length = min(aEnd - aStart, bEnd - bStart);
    for (var k = 0; k < length; k++) {
      final x = a[aStart + k];
      final y = b[bStart + k];
      if (x == y) continue;
      if ((x == 0xee || x == 0xef) && y >= 0xf0) return 1;
      if ((y == 0xee || y == 0xef) && x >= 0xf0) return -1;
      return x - y;
    }
    return (aEnd - aStart) - (bEnd - bStart);
  }

  /// A code per row, the same for rows holding equal values (all nulls share
  /// one), numbered by first appearance; [firsts] holds the first row of each
  /// code. Hashes and compares the bytes, so no [String] is made. For the
  /// engine's kernels; not public API.
  ({Int32List codes, Int32List firsts}) encode() {
    final count = length;
    final codes = Int32List(count);
    var firsts = Int32List(16);
    var hashes = Int32List(16);
    var slots = Int32List(64)..fillRange(0, 64, -1);
    var distinct = 0;
    var nullCode = -1;
    for (var i = 0; i < count; i++) {
      if (_isNull(i)) {
        if (nullCode < 0) {
          nullCode = distinct;
          if (distinct == firsts.length) {
            firsts = _grow(firsts);
            hashes = _grow(hashes);
          }
          firsts[distinct++] = i;
        }
        codes[i] = nullCode;
        continue;
      }
      final start = _offsets[i];
      final end = _offsets[i + 1];
      var hash = 0x811c9dc5 & 0x7fffffff;
      for (var k = start; k < end; k++) {
        hash = ((hash ^ _bytes[k]) * 0x01000193) & 0x7fffffff;
      }
      var slot = hash & (slots.length - 1);
      while (true) {
        final code = slots[slot];
        if (code < 0) {
          if (distinct == firsts.length) {
            firsts = _grow(firsts);
            hashes = _grow(hashes);
          }
          firsts[distinct] = i;
          hashes[distinct] = hash;
          slots[slot] = distinct;
          codes[i] = distinct++;
          if (distinct * 2 > slots.length) {
            slots = Int32List(slots.length * 2)
              ..fillRange(0, slots.length * 2, -1);
            for (var c = 0; c < distinct; c++) {
              if (c == nullCode) continue;
              var s = hashes[c] & (slots.length - 1);
              while (slots[s] >= 0) {
                s = (s + 1) & (slots.length - 1);
              }
              slots[s] = c;
            }
          }
          break;
        }
        if (code != nullCode &&
            hashes[code] == hash &&
            _order(
                  _bytes,
                  _offsets[firsts[code]],
                  _offsets[firsts[code] + 1],
                  _bytes,
                  start,
                  end,
                ) ==
                0) {
          codes[i] = code;
          break;
        }
        slot = (slot + 1) & (slots.length - 1);
      }
    }
    return (
      codes: codes,
      firsts: Int32List.fromList(firsts.sublist(0, distinct)),
    );
  }

  static Int32List _grow(Int32List list) =>
      Int32List(list.length * 2)..setRange(0, list.length, list);

  @override
  List<String?> interned() {
    final (:codes, :firsts) = encode();
    final distinct = [for (final first in firsts) this[first]];
    return [for (final code in codes) distinct[code]];
  }

  /// Throw if [size] bytes of strings would overflow the 32-bit offsets.
  void _fits(int size) {
    if (size > StringColumn._maxBytes) {
      throw ArgumentError(
        'Column "$name" holds more than ${StringColumn._maxBytes} bytes of '
        'strings',
      );
    }
  }

  @override
  StringColumn gather(Int32List indices) {
    final count = indices.length;
    var size = 0;
    var padded = false;
    final offsets = Int32List(count + 1);
    for (var i = 0; i < count; i++) {
      final row = indices[i];
      if (row >= length) {
        throw RangeError('Index $row out of range [0, $length)');
      }
      if (row < 0) {
        padded = true;
      } else {
        size += _offsets[row + 1] - _offsets[row];
        _fits(size);
      }
      offsets[i + 1] = size;
    }
    final bytes = Uint8List(size);
    final source = _nulls;
    final nullable = this.nullable || padded;
    final nulls = nullable ? Uint8List((count + 7) >> 3) : null;
    for (var i = 0; i < count; i++) {
      final row = indices[i];
      if (row < 0 ||
          (source != null && (source[row >> 3] & (1 << (row & 7))) != 0)) {
        nulls![i >> 3] |= 1 << (i & 7);
      } else {
        bytes.setRange(offsets[i], offsets[i + 1], _bytes, _offsets[row]);
      }
    }
    return StringVector._(
      name: name,
      offsets: offsets,
      bytes: bytes,
      nulls: nulls,
      nullable: nullable,
    );
  }

  @override
  int get length => _offsets.length - 1;

  @override
  bool get hasNulls => _hasNulls;

  @override
  String? operator [](int index) {
    if (index < 0 || index >= length) {
      throw RangeError('Index $index out of range [0, $length)');
    }
    return _isNull(index) ? null : _decode(index);
  }

  @override
  StringColumn slice(int start, [int? end]) {
    final stop = end ?? length;
    if (start < 0 || start > length || stop < start || stop > length) {
      throw RangeError(
        'Invalid slice range [$start, $stop) for column of length $length',
      );
    }
    final count = stop - start;
    final base = _offsets[start];
    final offsets = Int32List(count + 1);
    for (var i = 1; i <= count; i++) {
      offsets[i] = _offsets[start + i] - base;
    }
    final source = _nulls;
    final nulls = source == null ? null : Uint8List((count + 7) >> 3);
    if (source != null) {
      for (var i = 0; i < count; i++) {
        final row = start + i;
        if ((source[row >> 3] & (1 << (row & 7))) != 0) {
          nulls![i >> 3] |= 1 << (i & 7);
        }
      }
    }
    return StringVector._(
      name: name,
      offsets: offsets,
      bytes: Uint8List.fromList(
        Uint8List.sublistView(_bytes, base, _offsets[stop]),
      ),
      nulls: nulls,
      nullable: nullable,
    );
  }

  @override
  StringColumn filter(List<int> indices) {
    final count = indices.length;
    final offsets = Int32List(count + 1);
    var size = 0;
    for (var i = 0; i < count; i++) {
      final row = indices[i];
      if (row < 0 || row >= length) {
        throw RangeError('Index $row out of range [0, $length)');
      }
      size += _offsets[row + 1] - _offsets[row];
      _fits(size);
      offsets[i + 1] = size;
    }
    final bytes = Uint8List(size);
    final source = _nulls;
    final nulls = source == null ? null : Uint8List((count + 7) >> 3);
    for (var i = 0; i < count; i++) {
      final row = indices[i];
      bytes.setRange(offsets[i], offsets[i + 1], _bytes, _offsets[row]);
      if (source != null && (source[row >> 3] & (1 << (row & 7))) != 0) {
        nulls![i >> 3] |= 1 << (i & 7);
      }
    }
    return StringVector._(
      name: name,
      offsets: offsets,
      bytes: bytes,
      nulls: nulls,
      nullable: nullable,
    );
  }

  @override
  List<String?> toList() => [
    for (var i = 0; i < length; i++) _isNull(i) ? null : _decode(i),
  ];

  @override
  int get memoryUsage =>
      _bytes.lengthInBytes +
      _offsets.lengthInBytes +
      (_nulls?.lengthInBytes ?? 0);

  @override
  StringColumn copy() => StringVector._(
    name: name,
    offsets: Int32List.fromList(_offsets),
    bytes: Uint8List.fromList(_bytes),
    nulls: _nulls == null ? null : Uint8List.fromList(_nulls),
    nullable: nullable,
  );

  @override
  StringColumn withName(String name) => StringVector._(
    name: name,
    offsets: _offsets,
    bytes: _bytes,
    nulls: _nulls,
    nullable: nullable,
  );

  @override
  Column<R> map<R>(R Function(String?) transform, DataType resultType) =>
      ListColumn<R>(
        name: name,
        dataType: resultType,
        data: toList().map(transform).toList(),
        nullable: nullable,
      );

  /// Null, as for any non-numeric column.
  @override
  ColumnStats? get stats => null;

  /// [pieces] joined into one column, buffers copied end to end.
  static StringVector _concat(String name, List<StringVector> pieces) {
    var rows = 0;
    var size = 0;
    for (final piece in pieces) {
      rows += piece.length;
      size += piece._offsets.last;
    }
    if (size > StringColumn._maxBytes) {
      throw ArgumentError(
        'Column "$name" holds more than ${StringColumn._maxBytes} bytes of '
        'strings',
      );
    }
    final nullable = pieces.any((piece) => piece.nullable);
    final offsets = Int32List(rows + 1);
    final bytes = Uint8List(size);
    final nulls = nullable ? Uint8List((rows + 7) >> 3) : null;
    var row = 0;
    var at = 0;
    for (final piece in pieces) {
      final count = piece.length;
      final used = piece._offsets.last;
      bytes.setRange(at, at + used, piece._bytes);
      for (var i = 0; i < count; i++) {
        offsets[row + i + 1] = at + piece._offsets[i + 1];
      }
      final source = piece._nulls;
      if (nulls != null && source != null) _copyBits(source, nulls, row, count);
      row += count;
      at += used;
    }
    return StringVector._(
      name: name,
      offsets: offsets,
      bytes: bytes,
      nulls: nulls,
      nullable: nullable,
    );
  }
}

/// The bit-packed implementation of [BoolColumn]: the value and null bitmaps
/// it holds, adoption of prebuilt bitmaps without copying, and the live
/// bitmaps for trusted hot paths.
final class BoolVector extends BoolColumn {
  final int _length;
  final Uint8List _bits;
  final Uint8List? _nulls;
  final bool _hasNulls;

  BoolVector._({
    required super.name,
    required int length,
    required Uint8List bits,
    required Uint8List? nulls,
    required super.nullable,
  }) : _length = length,
       _bits = bits,
       _nulls = nulls,
       _hasNulls = nulls != null && nulls.any((byte) => byte != 0),
       super._();

  /// Wrap existing bitmaps of [length] values without copying; [nulls], when
  /// given, makes the column nullable.
  factory BoolVector.adopt({
    required String name,
    required int length,
    required Uint8List bits,
    Uint8List? nulls,
  }) {
    final size = (length + 7) >> 3;
    if (length < 0 ||
        bits.length != size ||
        (nulls != null && nulls.length != size)) {
      throw ArgumentError(
        'BoolVector.adopt: bitmaps of ${bits.length} and ${nulls?.length} '
        'bytes do not hold $length values',
      );
    }
    return BoolVector._(
      name: name,
      length: length,
      bits: bits,
      nulls: nulls,
      nullable: nulls != null,
    );
  }

  /// The value bits (bit set means true; a null row's bit is meaningless):
  /// the mutable backing storage, for trusted hot paths like
  /// [NumericVector.raw]; not public API.
  Uint8List get bits => _bits;

  /// The null bitmap (bit set means null), or null for a non-nullable column.
  Uint8List? get nulls => _nulls;

  static bool _bit(Uint8List bits, int index) =>
      (bits[index >> 3] & (1 << (index & 7))) != 0;

  @override
  bool isNullAt(int index) {
    final nulls = _nulls;
    return nulls != null && _bit(nulls, index);
  }

  @override
  int get length => _length;

  @override
  bool get hasNulls => _hasNulls;

  @override
  bool? operator [](int index) {
    if (index < 0 || index >= length) {
      throw RangeError('Index $index out of range [0, $length)');
    }
    return isNullAt(index) ? null : _bit(_bits, index);
  }

  @override
  BoolColumn slice(int start, [int? end]) {
    final stop = end ?? length;
    if (start < 0 || start > length || stop < start || stop > length) {
      throw RangeError(
        'Invalid slice range [$start, $stop) for column of length $length',
      );
    }
    return _pick(stop - start, (i) => start + i, nullable);
  }

  @override
  BoolColumn filter(List<int> indices) {
    for (final index in indices) {
      if (index < 0 || index >= length) {
        throw RangeError('Index $index out of range [0, $length)');
      }
    }
    return _pick(indices.length, (i) => indices[i], nullable);
  }

  @override
  BoolColumn gather(Int32List indices) {
    var padded = false;
    for (final index in indices) {
      if (index >= length) {
        throw RangeError('Index $index out of range [0, $length)');
      }
      if (index < 0) padded = true;
    }
    return _pick(indices.length, (i) => indices[i], nullable || padded);
  }

  /// [count] rows, row `i` taken from row `source(i)` (null when negative).
  BoolColumn _pick(int count, int Function(int) source, bool nullable) {
    final bits = Uint8List((count + 7) >> 3);
    final nulls = nullable ? Uint8List((count + 7) >> 3) : null;
    for (var i = 0; i < count; i++) {
      final row = source(i);
      if (row < 0 || isNullAt(row)) {
        nulls![i >> 3] |= 1 << (i & 7);
      } else if (_bit(_bits, row)) {
        bits[i >> 3] |= 1 << (i & 7);
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

  @override
  List<bool?> toList() => [
    for (var i = 0; i < length; i++) isNullAt(i) ? null : _bit(_bits, i),
  ];

  @override
  int get memoryUsage => _bits.lengthInBytes + (_nulls?.lengthInBytes ?? 0);

  @override
  BoolColumn copy() => BoolVector._(
    name: name,
    length: _length,
    bits: Uint8List.fromList(_bits),
    nulls: _nulls == null ? null : Uint8List.fromList(_nulls),
    nullable: nullable,
  );

  @override
  BoolColumn withName(String name) => BoolVector._(
    name: name,
    length: _length,
    bits: _bits,
    nulls: _nulls,
    nullable: nullable,
  );

  @override
  Column<R> map<R>(R Function(bool?) transform, DataType resultType) =>
      ListColumn<R>(
        name: name,
        dataType: resultType,
        data: toList().map(transform).toList(),
        nullable: nullable,
      );

  /// Null, as for any non-numeric column.
  @override
  ColumnStats? get stats => null;

  /// [pieces] joined into one column.
  static BoolVector _concat(String name, List<BoolVector> pieces) {
    var count = 0;
    for (final piece in pieces) {
      count += piece.length;
    }
    final nullable = pieces.any((piece) => piece.nullable);
    final bits = Uint8List((count + 7) >> 3);
    final nulls = nullable ? Uint8List((count + 7) >> 3) : null;
    var row = 0;
    for (final piece in pieces) {
      // A null row's value bit is meaningless, so the bits copy as they are.
      _copyBits(piece._bits, bits, row, piece.length);
      final source = piece._nulls;
      if (nulls != null && source != null) {
        _copyBits(source, nulls, row, piece.length);
      }
      row += piece.length;
    }
    return BoolVector._(
      name: name,
      length: count,
      bits: bits,
      nulls: nulls,
      nullable: nullable,
    );
  }
}
