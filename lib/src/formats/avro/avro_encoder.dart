import 'dart:convert';
import 'dart:typed_data';

import '../../core/conversions.dart';
import '../../util/varint.dart';
import 'avro_decoder.dart';

/// Byte sink for Avro binary encoding (mirror of [AvroReader]).
class AvroWriter {
  final BytesBuilder _bytes = BytesBuilder();

  void writeLong(int value) => writeVarint(_bytes, zigzag(value));

  void writeBoolean(bool value) => _bytes.addByte(value ? 1 : 0);

  void writeFloat(double value) {
    final data = ByteData(4)..setFloat32(0, value, Endian.little);
    _bytes.add(data.buffer.asUint8List());
  }

  void writeDouble(double value) {
    final data = ByteData(8)..setFloat64(0, value, Endian.little);
    _bytes.add(data.buffer.asUint8List());
  }

  void writeBytes(List<int> value) {
    writeLong(value.length);
    _bytes.add(value);
  }

  void writeFixed(List<int> value) => _bytes.add(value);

  void writeString(String value) => writeBytes(utf8.encode(value));

  Uint8List toBytes() => _bytes.toBytes();
}

/// Encodes Dart values (records as `Map<String,dynamic>`) per an [AvroSchema].
extension AvroEncode on AvroSchema {
  void encode(AvroWriter writer, Object? value) => _encode(root, value, writer);

  void _encode(AvroType type, Object? value, AvroWriter writer) {
    switch (type) {
      case final AvroPrimitive primitive:
        final raw = _physical(
          value,
          primitive.logicalType,
          scale: primitive.scale,
        );
        switch (primitive.name) {
          case 'null':
            break;
          case 'boolean':
            writer.writeBoolean(raw as bool);
          case 'int':
          case 'long':
            writer.writeLong(raw as int);
          case 'float':
            writer.writeFloat((raw as num).toDouble());
          case 'double':
            writer.writeDouble((raw as num).toDouble());
          case 'bytes':
            writer.writeBytes(raw as List<int>);
          default:
            writer.writeString(raw as String);
        }
      case final AvroRecord record:
        final map = value as Map;
        for (final field in record.fields) {
          _encode(field.type, map[field.name], writer);
        }
      case final AvroUnion union:
        // Choose the branch whose type actually accepts the runtime value, not
        // merely the first non-null branch; otherwise a ["null","int","string"]
        // union with a String value would pick "int" and crash.
        final index = union.branches.indexWhere(
          (branch) => _accepts(branch, value),
        );
        if (index < 0) {
          // Falling back to branch 0 wrote a nullable union's value as NULL:
          // silently discarding data the caller believed it had stored, which is
          // the worst available answer. A union with no matching branch is a
          // caller error; say so.
          throw ArgumentError(
            'No branch of union ${union.branches.map(_describe).toList()} accepts '
            '${value.runtimeType} ($value)',
          );
        }
        writer.writeLong(index);
        _encode(union.branches[index], value, writer);
      case final AvroArray array:
        final list = (value as List?) ?? const [];
        if (list.isNotEmpty) {
          writer.writeLong(list.length);
          for (final item in list) {
            _encode(array.items, item, writer);
          }
        }
        writer.writeLong(0);
      case final AvroMap mapping:
        final map = (value as Map?) ?? const {};
        if (map.isNotEmpty) {
          writer.writeLong(map.length);
          map.forEach((key, item) {
            writer.writeString(key as String);
            _encode(mapping.values, item, writer);
          });
        }
        writer.writeLong(0);
      case final AvroEnum enumeration:
        final symbol = enumeration.symbols.indexOf(value as String);
        if (symbol < 0) {
          throw ArgumentError(
            'Value "$value" is not one of enum ${enumeration.symbols}',
          );
        }
        writer.writeLong(symbol);
      case final AvroFixed fixed:
        writer.writeFixed(
          _physical(
                value,
                fixed.logicalType,
                scale: fixed.scale,
                width: fixed.size,
              )
              as List<int>,
        );
      case final AvroRef ref:
        _encode(named[ref.name]!, value, writer);
    }
  }

  /// Whether [type] can encode the runtime [value]: used to pick a union branch.
  ///
  /// A LOGICAL type is matched on the Dart form the decoder produces (`DateTime`
  /// for date/timestamp, `BigInt` for decimal), not on the physical form it is
  /// stored as. Testing the physical type meant a decoded `DateTime` matched no
  /// branch of the very union it came from.
  bool _accepts(AvroType type, Object? value) {
    switch (type) {
      case final AvroPrimitive primitive:
        if (primitive.name != 'null' && primitive.logicalType != null) {
          return _acceptsLogical(primitive.logicalType!, value);
        }
        switch (primitive.name) {
          case 'null':
            return value == null;
          case 'boolean':
            return value is bool;
          case 'int':
          case 'long':
            return value is int;
          case 'float':
          case 'double':
            return value is num;
          case 'bytes':
            return value is List<int>;
          default: // string
            return value is String;
        }
      case AvroEnum _:
        return value is String;
      case final AvroFixed fixed:
        return fixed.logicalType != null
            ? _acceptsLogical(fixed.logicalType!, value)
            : value is List<int>;
      case AvroArray _:
        return value is List;
      case AvroMap _:
      case AvroRecord _:
        return value is Map;
      case final AvroRef ref:
        return _accepts(named[ref.name]!, value);
      case AvroUnion _:
        return false; // Avro forbids directly nested unions
    }
  }

  /// Whether [value] is the Dart form [logicalType] decodes to.
  bool _acceptsLogical(String logicalType, Object? value) =>
      switch (logicalType) {
        'date' ||
        'timestamp-millis' ||
        'timestamp-micros' ||
        'local-timestamp-millis' ||
        'local-timestamp-micros' => value is DateTime,
        'decimal' => value is BigInt || value is List<int>,
        'uuid' => value is String,
        // time-millis/time-micros stay ints; an unknown logical type keeps its
        // physical form, so accept anything the branch would otherwise take.
        _ => value is int || value is String,
      };

  /// The physical value [logicalType] stores, from the Dart form the decoder
  /// produces. The exact inverse of `AvroSchema._logical`, so a decode → encode
  /// round trip is lossless. A value already in physical form passes through
  /// (nothing forces a caller to build its records the decoder's way).
  Object? _physical(
    Object? value,
    String? logicalType, {
    int? scale,
    int? width,
  }) {
    if (value == null || logicalType == null) return value;
    switch (logicalType) {
      case 'date':
        return value is DateTime ? dateToEpochDays(value) : value;
      case 'timestamp-millis':
      case 'local-timestamp-millis':
        return value is DateTime ? value.toUtc().millisecondsSinceEpoch : value;
      case 'timestamp-micros':
      case 'local-timestamp-micros':
        return value is DateTime ? timestampToEpochMicros(value) : value;
      case 'decimal':
        // Big-endian two's-complement of the UNSCALED value, mirroring
        // bytesToBigInt on the way in. `bytes` is variable-width, so use the
        // minimum that holds the value; `fixed` must fill its declared size.
        if (value is! BigInt) return value;
        return bigIntToBytes(
          value,
          width ?? _minimumWidth(value),
          littleEndian: false,
        );
      default:
        return value; // time-millis/time-micros/uuid keep their physical form
    }
  }

  /// Fewest bytes that hold [value] as two's-complement (at least one).
  static int _minimumWidth(BigInt value) {
    final bits = (value.isNegative ? ~value : value).bitLength + 1; // + sign
    return (bits + 7) ~/ 8;
  }

  /// A union branch's name, for the no-match error.
  static String _describe(AvroType type) => switch (type) {
    final AvroPrimitive primitive =>
      primitive.logicalType == null
          ? primitive.name
          : '${primitive.name}/${primitive.logicalType}',
    final AvroFixed fixed => 'fixed(${fixed.size})',
    final AvroRecord record => 'record ${record.name}',
    AvroEnum _ => 'enum',
    AvroArray _ => 'array',
    AvroMap _ => 'map',
    final AvroRef reference => reference.name,
    AvroUnion _ => 'union',
  };
}
