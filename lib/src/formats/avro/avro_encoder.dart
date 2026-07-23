import 'dart:convert';
import 'dart:typed_data';

import '../../core/conversions.dart';
import 'avro_decoder.dart';

/// Byte sink for Avro binary encoding (mirror of [AvroReader]).
class AvroWriter {
  final BytesBuilder _b = BytesBuilder();

  void writeLong(int value) {
    int n = (value << 1) ^ (value >> 63); // zigzag
    while ((n & ~0x7f) != 0) {
      _b.addByte((n & 0x7f) | 0x80);
      n = n >>> 7;
    }
    _b.addByte(n & 0x7f);
  }

  void writeBoolean(bool v) => _b.addByte(v ? 1 : 0);

  void writeFloat(double v) {
    final d = ByteData(4)..setFloat32(0, v, Endian.little);
    _b.add(d.buffer.asUint8List());
  }

  void writeDouble(double v) {
    final d = ByteData(8)..setFloat64(0, v, Endian.little);
    _b.add(d.buffer.asUint8List());
  }

  void writeBytes(List<int> v) {
    writeLong(v.length);
    _b.add(v);
  }

  void writeFixed(List<int> v) => _b.add(v);

  void writeString(String v) => writeBytes(utf8.encode(v));

  Uint8List toBytes() => _b.toBytes();
}

/// Encodes Dart values (records as `Map<String,dynamic>`) per an [AvroSchema].
extension AvroEncode on AvroSchema {
  void encode(AvroWriter w, Object? value) => _encode(root, value, w);

  void _encode(AvroType type, Object? value, AvroWriter w) {
    switch (type) {
      case final AvroPrimitive p:
        final raw = _physical(value, p.logicalType, scale: p.scale);
        switch (p.name) {
          case 'null':
            break;
          case 'boolean':
            w.writeBoolean(raw as bool);
          case 'int':
          case 'long':
            w.writeLong(raw as int);
          case 'float':
            w.writeFloat((raw as num).toDouble());
          case 'double':
            w.writeDouble((raw as num).toDouble());
          case 'bytes':
            w.writeBytes(raw as List<int>);
          default:
            w.writeString(raw as String);
        }
      case final AvroRecord rec:
        final map = value as Map;
        for (final f in rec.fields) {
          _encode(f.type, map[f.name], w);
        }
      case final AvroUnion u:
        // Choose the branch whose type actually accepts the runtime value, not
        // merely the first non-null branch — otherwise a ["null","int","string"]
        // union with a String value would pick "int" and crash.
        final index = u.branches.indexWhere((b) => _accepts(b, value));
        if (index < 0) {
          // Falling back to branch 0 wrote a nullable union's value as NULL —
          // silently discarding data the caller believed it had stored, which is
          // the worst available answer. A union with no matching branch is a
          // caller error; say so.
          throw ArgumentError(
            'No branch of union ${u.branches.map(_describe).toList()} accepts '
            '${value.runtimeType} ($value)',
          );
        }
        w.writeLong(index);
        _encode(u.branches[index], value, w);
      case final AvroArray a:
        final list = (value as List?) ?? const [];
        if (list.isNotEmpty) {
          w.writeLong(list.length);
          for (final item in list) {
            _encode(a.items, item, w);
          }
        }
        w.writeLong(0);
      case final AvroMap m:
        final map = (value as Map?) ?? const {};
        if (map.isNotEmpty) {
          w.writeLong(map.length);
          map.forEach((k, v) {
            w.writeString(k as String);
            _encode(m.values, v, w);
          });
        }
        w.writeLong(0);
      case final AvroEnum e:
        final symbol = e.symbols.indexOf(value as String);
        if (symbol < 0) {
          throw ArgumentError('Value "$value" is not one of enum ${e.symbols}');
        }
        w.writeLong(symbol);
      case final AvroFixed f:
        w.writeFixed(
          _physical(value, f.logicalType, scale: f.scale, width: f.size)
              as List<int>,
        );
      case final AvroRef ref:
        _encode(named[ref.name]!, value, w);
    }
  }

  /// Whether [type] can encode the runtime [value] — used to pick a union branch.
  ///
  /// A LOGICAL type is matched on the Dart form the decoder produces (`DateTime`
  /// for date/timestamp, `BigInt` for decimal), not on the physical form it is
  /// stored as. Testing the physical type meant a decoded `DateTime` matched no
  /// branch of the very union it came from.
  bool _accepts(AvroType type, Object? value) {
    switch (type) {
      case final AvroPrimitive p:
        if (p.name != 'null' && p.logicalType != null) {
          return _acceptsLogical(p.logicalType!, value);
        }
        switch (p.name) {
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
      case final AvroFixed f:
        return f.logicalType != null
            ? _acceptsLogical(f.logicalType!, value)
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
    final AvroPrimitive p =>
      p.logicalType == null ? p.name : '${p.name}/${p.logicalType}',
    final AvroFixed f => 'fixed(${f.size})',
    final AvroRecord r => 'record ${r.name}',
    AvroEnum _ => 'enum',
    AvroArray _ => 'array',
    AvroMap _ => 'map',
    final AvroRef r => r.name,
    AvroUnion _ => 'union',
  };
}
