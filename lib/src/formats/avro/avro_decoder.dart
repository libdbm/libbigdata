import 'dart:convert';
import 'dart:typed_data';

import '../../core/conversions.dart';
import '../../core/limits.dart';
import '../../util/varint.dart';

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

  /// Logical-value budget for everything this schema decodes (one schema is
  /// parsed per file): an array of `null` costs ~0 payload bytes per element,
  /// so a tiny block count can otherwise declare billions of items and pass the
  /// compressed/decompressed byte guards. Charged BEFORE expansion, and never
  /// reset, so many small records cannot add up past it.
  static const _maxValues = defaultMaxMaterializedRows;

  /// Collection-nesting depth cap (array-of-array…), defence against a
  /// pathological schema driving unbounded recursion.
  static const _maxDepth = 100;

  int _remaining = _maxValues;
  int _depth = 0;

  factory AvroSchema.parse(String json) {
    final named = <String, AvroType>{};
    final root = _parse(jsonDecode(json), named, null, 0);
    return AvroSchema._(root, named);
  }

  static AvroType _parse(
    dynamic json,
    Map<String, AvroType> named,
    String? namespace,
    int depth,
  ) {
    if (depth > defaultMaxSchemaDepth) {
      throw const FormatException(
        'Avro schema nesting exceeds depth $defaultMaxSchemaDepth',
      );
    }
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
      // A named-type reference: a short name resolves in the enclosing
      // namespace first, then as written.
      final full = _full(json, namespace);
      return AvroRef(named.containsKey(full) ? full : json);
    }
    if (json is List) {
      return AvroUnion([
        for (final branch in json) _parse(branch, named, namespace, depth + 1),
      ]);
    }
    if (json is! Map<String, dynamic>) {
      throw FormatException('Avro schema: unexpected ${json.runtimeType}');
    }
    final map = json;
    final type = map['type'];
    final space = _field<String?>(map, 'namespace') ?? namespace;
    switch (type) {
      case 'record':
        // Named types are registered by FULL name: two records called `Item`
        // in different namespaces are different types, and the short name
        // let the second replace the first.
        final name = _full(_field<String>(map, 'name'), space);
        final record = AvroRecord(name);
        named[name] = record;
        // A record's fields live in the namespace of its full name.
        final inner = name.contains('.')
            ? name.substring(0, name.lastIndexOf('.'))
            : null;
        for (final field in _field<List>(map, 'fields')) {
          if (field is! Map<String, dynamic>) {
            throw FormatException('Avro schema: field ${field.runtimeType}');
          }
          record.fields.add(
            AvroField(
              _field<String>(field, 'name'),
              _parse(field['type'], named, inner, depth + 1),
            ),
          );
        }
        return record;
      case 'array':
        return AvroArray(_parse(map['items'], named, space, depth + 1));
      case 'map':
        return AvroMap(_parse(map['values'], named, space, depth + 1));
      case 'enum':
        final enumeration = AvroEnum([
          for (final symbol in _field<List>(map, 'symbols'))
            symbol is String
                ? symbol
                : throw FormatException('Avro schema: enum symbol $symbol'),
        ]);
        named[_full(_field<String>(map, 'name'), space)] = enumeration;
        return enumeration;
      case 'fixed':
        final fixed = AvroFixed(
          _field<int>(map, 'size'),
          logicalType: _field<String?>(map, 'logicalType'),
          precision: _precision(map),
          scale: _field<int?>(map, 'scale'),
        );
        named[_full(_field<String>(map, 'name'), space)] = fixed;
        return fixed;
      default:
        // {"type":"long","logicalType":"timestamp-micros"} etc.: keep the
        // logical type so [_decode] can convert (date/timestamp/decimal/uuid).
        final logical = _field<String?>(map, 'logicalType');
        if (logical != null && type is String) {
          return AvroPrimitive(
            type,
            logicalType: logical,
            precision: _precision(map),
            scale: _field<int?>(map, 'scale'),
          );
        }
        return _parse(type, named, space, depth + 1);
    }
  }

  /// The full name of a named type called [name] in [namespace]: a dotted
  /// name is already full.
  static String _full(String name, String? namespace) =>
      name.contains('.') || namespace == null || namespace.isEmpty
      ? name
      : '$namespace.$name';

  /// A decimal's precision, checked with its scale: both come from the file.
  /// Null for any other logical type.
  static int? _precision(Map<String, dynamic> map) {
    final precision = _field<int?>(map, 'precision');
    if (_field<String?>(map, 'logicalType') != 'decimal') return precision;
    return checkedDecimal(
      precision ?? 0,
      _field<int?>(map, 'scale') ?? 0,
    ).precision;
  }

  /// [map]'s [key] as a [T], or a [FormatException] naming the key: the
  /// schema JSON comes from the file, so its shape is not guaranteed.
  static T _field<T>(Map<String, dynamic> map, String key) {
    final value = map[key];
    if (value is! T) {
      throw FormatException(
        'Avro schema: "$key" is ${value.runtimeType}, expected $T',
      );
    }
    return value;
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
  /// Resets the depth only; the value budget carries across records, since the
  /// outer `maxRecords` guard only fires AFTER a whole record decodes.
  Object? decode(AvroReader reader) {
    _depth = 0;
    return _decode(root, reader);
  }

  /// Guards EVERY recursive descent, not just collection blocks. Records,
  /// unions, and named-type refs recurse through here, so a self-referential
  /// schema (record → union[null, ref-to-self]) is bounded by [_maxDepth]
  /// instead of stack-overflowing the isolate.
  Object? _decode(AvroType type, AvroReader reader) {
    if (++_depth > _maxDepth) {
      throw FormatException('Avro decoding nesting exceeds depth $_maxDepth');
    }
    try {
      return _decodeInner(type, reader);
    } finally {
      _depth--;
    }
  }

  Object? _decodeInner(AvroType type, AvroReader reader) {
    switch (type) {
      case final AvroPrimitive primitive:
        final raw = switch (primitive.name) {
          'null' => null,
          'boolean' => reader.readBoolean(),
          'int' || 'long' => reader.readLong(),
          'float' => reader.readFloat(),
          'double' => reader.readDouble(),
          'bytes' => reader.readBytes(),
          _ => reader.readString(),
        };
        return _logical(raw, primitive.logicalType);
      case final AvroRecord record:
        return {
          for (final field in record.fields)
            field.name: _decode(field.type, reader),
        };
      case final AvroUnion union:
        final index = reader.readLong();
        if (index < 0 || index >= union.branches.length) {
          throw FormatException('Avro union branch index $index out of range');
        }
        return _decode(union.branches[index], reader);
      case final AvroArray array:
        return _decodeBlocks(reader, () => _decode(array.items, reader));
      case final AvroMap map:
        final out = <String, Object?>{};
        _decodeBlocks(reader, () {
          final key = reader.readString();
          out[key] = _decode(map.values, reader);
          return null;
        });
        return out;
      case final AvroEnum enumeration:
        final index = reader.readLong();
        if (index < 0 || index >= enumeration.symbols.length) {
          throw FormatException('Avro enum symbol index $index out of range');
        }
        return enumeration.symbols[index];
      case final AvroFixed fixed:
        final bytes = reader.readFixed(fixed.size);
        // decimal-as-fixed → unscaled BigInt (big-endian two's-complement).
        if (fixed.logicalType == 'decimal') {
          return bytesToBigInt(bytes, littleEndian: false);
        }
        return bytes;
      case final AvroRef ref:
        final resolved = named[ref.name];
        if (resolved == null) {
          throw FormatException('unknown Avro type ${ref.name}');
        }
        return _decode(resolved, reader);
    }
  }

  /// Decode array/map blocks: repeated [count][items...] until a 0 count. A
  /// negative count is followed by a byte-size (skipped) then |count| items.
  List<Object?> _decodeBlocks(AvroReader reader, Object? Function() item) {
    // Depth is charged at _decode's entry (arrays/maps call _decode on each
    // item), so no separate collection-depth guard is needed here.
    final out = <Object?>[];
    while (true) {
      var count = reader.readLong();
      if (count == 0) break;
      if (count < 0) {
        reader.readLong(); // block byte size (unused)
        count = -count;
      }
      // Charge the declared count against the value budget BEFORE expanding:
      // a null-element array costs ~0 bytes/item, so `count` alone (not the byte
      // guards) is what must be bounded. `count < 0` catches the -(minInt) wrap.
      if (count < 0 || count > _remaining) {
        throw FormatException(
          'Avro collection declares $count items, over the remaining '
          'value budget ($_remaining)',
        );
      }
      _remaining -= count;
      for (int i = 0; i < count; i++) {
        out.add(item());
      }
    }
    return out;
  }
}

/// Binary cursor over Avro-encoded bytes.
class AvroReader {
  final Uint8List _bytes;
  final ByteData _view;
  int _position = 0;

  AvroReader(this._bytes) : _view = ByteData.sublistView(_bytes);

  bool get atEnd => _position >= _bytes.length;
  int get position => _position;

  int _varint() {
    final (:value, :next) = readVarint(_bytes, _position, label: 'Avro');
    _position = next;
    return value;
  }

  int readLong() {
    return unzigzag(_varint());
  }

  bool readBoolean() {
    _need(1);
    return _bytes[_position++] != 0;
  }

  double readFloat() {
    _need(4);
    final value = _view.getFloat32(_position, Endian.little);
    _position += 4;
    return value;
  }

  double readDouble() {
    _need(8);
    final value = _view.getFloat64(_position, Endian.little);
    _position += 8;
    return value;
  }

  Uint8List readBytes() {
    final length = readLong();
    _need(length);
    final out = Uint8List.sublistView(_bytes, _position, _position + length);
    _position += length;
    return out;
  }

  Uint8List readFixed(int size) {
    _need(size);
    final out = Uint8List.sublistView(_bytes, _position, _position + size);
    _position += size;
    return out;
  }

  // Fail loud (FormatException) on a truncated/over-long read instead of letting
  // sublistView throw a low-signal RangeError.
  void _need(int n) {
    if (n < 0 || _position + n > _bytes.length) {
      throw const FormatException('Truncated Avro data');
    }
  }

  String readString() => utf8.decode(readBytes());
}
