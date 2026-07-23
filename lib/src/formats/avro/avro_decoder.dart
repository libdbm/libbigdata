import 'dart:convert';
import 'dart:typed_data';

import '../../core/conversions.dart';
import '../../core/limits.dart';

/// Avro schema model (subset sufficient for Iceberg manifests) parsed from the
/// writer's JSON schema, plus a binary decoder. Named types (records/enums/fixed)
/// are registered so later string references resolve.
sealed class AvroType {}

class AvroPrimitive extends AvroType {
  final String name; // null|boolean|int|long|float|double|bytes|string
  final String? logicalType; // date|timestamp-millis|decimal|uuid|...
  final int? precision; // decimal
  final int? scale; // decimal
  AvroPrimitive(this.name, {this.logicalType, this.precision, this.scale});
}

class AvroRecord extends AvroType {
  final String name;
  final List<AvroField> fields = [];
  AvroRecord(this.name);
}

class AvroField {
  final String name;
  final AvroType type;
  AvroField(this.name, this.type);
}

class AvroArray extends AvroType {
  final AvroType items;
  AvroArray(this.items);
}

class AvroMap extends AvroType {
  final AvroType values;
  AvroMap(this.values);
}

class AvroUnion extends AvroType {
  final List<AvroType> branches;
  AvroUnion(this.branches);
}

class AvroEnum extends AvroType {
  final List<String> symbols;
  AvroEnum(this.symbols);
}

class AvroFixed extends AvroType {
  final int size;
  final String? logicalType; // decimal (fixed-backed)
  final int? precision;
  final int? scale;
  AvroFixed(this.size, {this.logicalType, this.precision, this.scale});
}

class AvroRef extends AvroType {
  final String name;
  AvroRef(this.name);
}

/// A parsed Avro schema with a binary decoder.
class AvroSchema {
  final AvroType root;
  final Map<String, AvroType> named;

  AvroSchema._(this.root, this.named);

  /// Per-record logical-value budget: an array of `null` costs ~0 payload bytes
  /// per element, so a tiny block count can otherwise declare billions of items
  /// and pass the compressed/decompressed byte guards. Charged BEFORE expansion.
  static const _maxValuesPerRecord = defaultMaxMaterializedRows;

  /// Collection-nesting depth cap (array-of-array…), defence against a
  /// pathological schema driving unbounded recursion.
  static const _maxDepth = 100;

  int _valuesLeft = 0;
  int _depth = 0;

  factory AvroSchema.parse(String json) {
    final named = <String, AvroType>{};
    final root = _parse(jsonDecode(json), named, null);
    return AvroSchema._(root, named);
  }

  static AvroType _parse(
    dynamic json,
    Map<String, AvroType> named,
    String? namespace,
  ) {
    if (json is String) {
      const primitives = {
        'null',
        'boolean',
        'int',
        'long',
        'float',
        'double',
        'bytes',
        'string',
      };
      if (primitives.contains(json)) return AvroPrimitive(json);
      return AvroRef(json); // named-type reference
    }
    if (json is List) {
      return AvroUnion([for (final b in json) _parse(b, named, namespace)]);
    }
    final map = json as Map<String, dynamic>;
    final type = map['type'];
    final ns = (map['namespace'] as String?) ?? namespace;
    switch (type) {
      case 'record':
        final name = map['name'] as String;
        final record = AvroRecord(name);
        named[name] = record;
        if (ns != null) named['$ns.$name'] = record;
        for (final f in (map['fields'] as List).cast<Map<String, dynamic>>()) {
          record.fields.add(
            AvroField(f['name'] as String, _parse(f['type'], named, ns)),
          );
        }
        return record;
      case 'array':
        return AvroArray(_parse(map['items'], named, ns));
      case 'map':
        return AvroMap(_parse(map['values'], named, ns));
      case 'enum':
        final e = AvroEnum((map['symbols'] as List).cast<String>());
        named[map['name'] as String] = e;
        return e;
      case 'fixed':
        final fx = AvroFixed(
          map['size'] as int,
          logicalType: map['logicalType'] as String?,
          precision: map['precision'] as int?,
          scale: map['scale'] as int?,
        );
        named[map['name'] as String] = fx;
        return fx;
      default:
        // {"type":"long","logicalType":"timestamp-micros"} etc. — keep the
        // logical type so [_decode] can convert (date/timestamp/decimal/uuid).
        final logical = map['logicalType'] as String?;
        if (logical != null && type is String) {
          return AvroPrimitive(
            type,
            logicalType: logical,
            precision: map['precision'] as int?,
            scale: map['scale'] as int?,
          );
        }
        return _parse(type, named, ns);
    }
  }

  /// Apply a SCALAR Avro logical type to a freshly-decoded primitive value.
  /// date/local-timestamp/timestamp → [DateTime]; decimal (bytes, big-endian
  /// two's-complement) → unscaled [BigInt]; uuid/time/unknown → unchanged.
  /// Structural logical types (`map`/`array`) are NOT handled here (they live on
  /// array types and are left to the caller, e.g. Iceberg manifests).
  static Object? _logical(Object? raw, String? logicalType) {
    if (raw == null || logicalType == null) return raw;
    switch (logicalType) {
      case 'date':
        return dateFromEpochDays(raw as int);
      case 'timestamp-millis':
      case 'local-timestamp-millis':
        return timestampFromEpochMillis(raw as int);
      case 'timestamp-micros':
      case 'local-timestamp-micros':
        return timestampFromEpochMicros(raw as int);
      case 'decimal':
        // bytes-backed decimal → unscaled BigInt (big-endian two's-complement).
        return raw is Uint8List ? bytesToBigInt(raw, littleEndian: false) : raw;
      default:
        // time-millis/time-micros (kept as int), uuid (kept as String), etc.
        return raw;
    }
  }

  /// Decode one top-level value (typically a record → `Map<String,dynamic>`).
  /// Resets the per-record value/depth budgets so intra-record collection
  /// amplification is bounded (the outer `maxRecords` guard only fires AFTER a
  /// whole record decodes).
  Object? decode(AvroReader reader) {
    _valuesLeft = _maxValuesPerRecord;
    _depth = 0;
    return _decode(root, reader);
  }

  /// Guards EVERY recursive descent, not just collection blocks. Records,
  /// unions, and named-type refs recurse through here, so a self-referential
  /// schema (record → union[null, ref-to-self]) is bounded by [_maxDepth]
  /// instead of stack-overflowing the isolate.
  Object? _decode(AvroType type, AvroReader r) {
    if (++_depth > _maxDepth) {
      throw FormatException('Avro decoding nesting exceeds depth $_maxDepth');
    }
    try {
      return _decodeInner(type, r);
    } finally {
      _depth--;
    }
  }

  Object? _decodeInner(AvroType type, AvroReader r) {
    switch (type) {
      case final AvroPrimitive p:
        final raw = switch (p.name) {
          'null' => null,
          'boolean' => r.readBoolean(),
          'int' || 'long' => r.readLong(),
          'float' => r.readFloat(),
          'double' => r.readDouble(),
          'bytes' => r.readBytes(),
          _ => r.readString(),
        };
        return _logical(raw, p.logicalType);
      case final AvroRecord rec:
        return {for (final f in rec.fields) f.name: _decode(f.type, r)};
      case final AvroUnion u:
        final i = r.readLong();
        if (i < 0 || i >= u.branches.length) {
          throw FormatException('Avro union branch index $i out of range');
        }
        return _decode(u.branches[i], r);
      case final AvroArray a:
        return _decodeBlocks(r, () => _decode(a.items, r));
      case final AvroMap m:
        final out = <String, Object?>{};
        _decodeBlocks(r, () {
          final k = r.readString();
          out[k] = _decode(m.values, r);
          return null;
        });
        return out;
      case final AvroEnum e:
        final i = r.readLong();
        if (i < 0 || i >= e.symbols.length) {
          throw FormatException('Avro enum symbol index $i out of range');
        }
        return e.symbols[i];
      case final AvroFixed fx:
        final bytes = r.readFixed(fx.size);
        // decimal-as-fixed → unscaled BigInt (big-endian two's-complement).
        if (fx.logicalType == 'decimal') {
          return bytesToBigInt(bytes, littleEndian: false);
        }
        return bytes;
      case final AvroRef ref:
        final resolved = named[ref.name];
        if (resolved == null) {
          throw FormatException('unknown Avro type ${ref.name}');
        }
        return _decode(resolved, r);
    }
  }

  /// Decode array/map blocks: repeated [count][items...] until a 0 count. A
  /// negative count is followed by a byte-size (skipped) then |count| items.
  List<Object?> _decodeBlocks(AvroReader r, Object? Function() item) {
    // Depth is charged at _decode's entry (arrays/maps call _decode on each
    // item), so no separate collection-depth guard is needed here.
    final out = <Object?>[];
    while (true) {
      var count = r.readLong();
      if (count == 0) break;
      if (count < 0) {
        r.readLong(); // block byte size (unused)
        count = -count;
      }
      // Charge the declared count against the per-record budget BEFORE expanding:
      // a null-element array costs ~0 bytes/item, so `count` alone (not the byte
      // guards) is what must be bounded. `count < 0` catches the -(minInt) wrap.
      if (count < 0 || count > _valuesLeft) {
        throw FormatException(
          'Avro collection declares $count items, over the remaining per-record '
          'value budget ($_valuesLeft)',
        );
      }
      _valuesLeft -= count;
      for (int i = 0; i < count; i++) {
        out.add(item());
      }
    }
    return out;
  }
}

/// Binary cursor over Avro-encoded bytes.
class AvroReader {
  final Uint8List _b;
  final ByteData _view;
  int _pos = 0;

  AvroReader(this._b) : _view = ByteData.sublistView(_b);

  bool get atEnd => _pos >= _b.length;
  int get position => _pos;

  int _varint() {
    int result = 0, shift = 0;
    // A 64-bit varint is at most 10 bytes; cap the loop and bounds-check each
    // read so truncated or all-high-bit input fails loud instead of running off
    // the buffer (raw RangeError) or spinning forever (JS out-of-bounds → NaN).
    for (int i = 0; i < 10; i++) {
      if (_pos >= _b.length) {
        throw const FormatException('Truncated Avro varint');
      }
      final b = _b[_pos++];
      result |= (b & 0x7f) << shift;
      if ((b & 0x80) == 0) return result;
      shift += 7;
    }
    throw const FormatException('Avro varint exceeds 64 bits');
  }

  int readLong() {
    final n = _varint();
    return (n >>> 1) ^ -(n & 1); // zigzag decode
  }

  bool readBoolean() {
    _need(1);
    return _b[_pos++] != 0;
  }

  double readFloat() {
    _need(4);
    final v = _view.getFloat32(_pos, Endian.little);
    _pos += 4;
    return v;
  }

  double readDouble() {
    _need(8);
    final v = _view.getFloat64(_pos, Endian.little);
    _pos += 8;
    return v;
  }

  Uint8List readBytes() {
    final length = readLong();
    _need(length);
    final out = Uint8List.sublistView(_b, _pos, _pos + length);
    _pos += length;
    return out;
  }

  Uint8List readFixed(int size) {
    _need(size);
    final out = Uint8List.sublistView(_b, _pos, _pos + size);
    _pos += size;
    return out;
  }

  // Fail loud (FormatException) on a truncated/over-long read instead of letting
  // sublistView throw a low-signal RangeError.
  void _need(int n) {
    if (n < 0 || _pos + n > _b.length) {
      throw const FormatException('Truncated Avro data');
    }
  }

  String readString() => utf8.decode(readBytes());
}
