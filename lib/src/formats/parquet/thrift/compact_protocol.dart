// TType wire constants intentionally mirror the Thrift spec's UPPER_CASE names.
// ignore_for_file: constant_identifier_names
import 'dart:convert';
import 'dart:typed_data';

import '../../../util/varint.dart';

/// A self-contained Thrift Compact Protocol reader/writer: the small subset the
/// Parquet metadata/page structs need. Replaces the `fbthrift` dependency.
///
/// Field/collection layout per the Thrift Compact Protocol spec: field headers
/// use 4-bit id-deltas, booleans pack their value into the field header, ints use
/// zigzag varints, and lengths use plain varints. Varint termination is correct
/// (stops once no bits remain above the low 7), so the previous fbthrift
/// `writeVarint64` bug does not apply here.

/// Abstract Thrift type ids (matching the canonical Thrift values).
class TType {
  static const int STOP = 0;
  static const int BOOL = 2;
  static const int BYTE = 3;
  static const int DOUBLE = 4;
  static const int I16 = 6;
  static const int I32 = 8;
  static const int I64 = 10;
  static const int STRING = 11;
  static const int STRUCT = 12;
  static const int MAP = 13;
  static const int SET = 14;
  static const int LIST = 15;
}

class TField {
  final String name;
  final int type;
  final int id;
  const TField(this.name, this.type, this.id);
}

class TList {
  final int elementType;
  final int length;
  const TList(this.elementType, this.length);
}

class TMap {
  final int keyType;
  final int valueType;
  final int length;
  const TMap(this.keyType, this.valueType, this.length);
}

class TStruct {
  final String name;
  const TStruct([this.name = '']);
}

/// The protocol surface the generated Parquet structs call.
abstract class TProtocol {
  // read
  void readStructBegin();
  void readStructEnd();
  TField readFieldBegin();
  void readFieldEnd();
  TList readListBegin();
  void readListEnd();
  TMap readMapBegin();
  void readMapEnd();
  TList readSetBegin();
  void readSetEnd();
  bool readBool();
  int readByte();
  int readI16();
  int readI32();
  int readI64();
  double readDouble();
  String readString();
  Int8List readBinary();
  // write
  void writeStructBegin(TStruct struct);
  void writeStructEnd();
  void writeFieldBegin(TField field);
  void writeFieldEnd();
  void writeFieldStop();
  void writeListBegin(TList list);
  void writeListEnd();
  void writeBool(bool value);
  void writeByte(int value);
  void writeI16(int value);
  void writeI32(int value);
  void writeI64(int value);
  void writeDouble(double value);
  void writeString(String value);
  void writeBinary(Int8List value);
}

// Compact wire type codes.
const int _cBoolTrue = 1;
const int _cBoolFalse = 2;
const int _cByte = 3;
const int _cI16 = 4;
const int _cI32 = 5;
const int _cI64 = 6;
const int _cDouble = 7;
const int _cBinary = 8;
const int _cList = 9;
const int _cSet = 10;
const int _cMap = 11;
const int _cStruct = 12;

int _compactType(int type) {
  switch (type) {
    case TType.BOOL:
      return _cBoolTrue;
    case TType.BYTE:
      return _cByte;
    case TType.I16:
      return _cI16;
    case TType.I32:
      return _cI32;
    case TType.I64:
      return _cI64;
    case TType.DOUBLE:
      return _cDouble;
    case TType.STRING:
      return _cBinary;
    case TType.LIST:
      return _cList;
    case TType.SET:
      return _cSet;
    case TType.MAP:
      return _cMap;
    case TType.STRUCT:
      return _cStruct;
    default:
      throw ArgumentError('No compact type for $type');
  }
}

int _ttype(int code) {
  switch (code & 0x0f) {
    case 0:
      return TType.STOP;
    case _cBoolTrue:
    case _cBoolFalse:
      return TType.BOOL;
    case _cByte:
      return TType.BYTE;
    case _cI16:
      return TType.I16;
    case _cI32:
      return TType.I32;
    case _cI64:
      return TType.I64;
    case _cDouble:
      return TType.DOUBLE;
    case _cBinary:
      return TType.STRING;
    case _cList:
      return TType.LIST;
    case _cSet:
      return TType.SET;
    case _cMap:
      return TType.MAP;
    case _cStruct:
      return TType.STRUCT;
    default:
      throw FormatException('Unknown compact type ${code & 0x0f}');
  }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

class CompactWriter implements TProtocol {
  final BytesBuilder _out = BytesBuilder();
  final List<int> _fieldStack = [];
  int _lastFieldId = 0;
  TField? _boolField;

  Uint8List takeBytes() => _out.toBytes();

  void _byte(int value) => _out.addByte(value & 0xff);

  void _varint(int value) => writeVarint(_out, value);

  // A 32-bit zigzag code keeps to 32 bits, as the compact protocol writes it.
  int _zigzag32(int value) => zigzag(value) & 0xffffffff;
  int _zigzag64(int value) => zigzag(value);

  @override
  void writeStructBegin(TStruct struct) {
    _fieldStack.add(_lastFieldId);
    _lastFieldId = 0;
  }

  @override
  void writeStructEnd() => _lastFieldId = _fieldStack.removeLast();

  @override
  void writeFieldBegin(TField field) {
    if (field.type == TType.BOOL) {
      _boolField = field;
    } else {
      _writeFieldBegin(field, _compactType(field.type));
    }
  }

  void _writeFieldBegin(TField field, int compactType) {
    final delta = field.id - _lastFieldId;
    if (field.id > _lastFieldId && delta <= 15) {
      _byte((delta << 4) | compactType);
    } else {
      _byte(compactType);
      writeI16(field.id);
    }
    _lastFieldId = field.id;
  }

  @override
  void writeFieldEnd() {}

  @override
  void writeFieldStop() => _byte(TType.STOP);

  @override
  void writeListBegin(TList list) {
    final code = _compactType(list.elementType);
    if (list.length <= 14) {
      _byte((list.length << 4) | code);
    } else {
      _byte(0xf0 | code);
      _varint(list.length & 0xffffffff);
    }
  }

  @override
  void writeListEnd() {}

  @override
  void writeBool(bool value) {
    if (_boolField != null) {
      _writeFieldBegin(_boolField!, value ? _cBoolTrue : _cBoolFalse);
      _boolField = null;
    } else {
      _byte(value ? _cBoolTrue : _cBoolFalse);
    }
  }

  @override
  void writeByte(int value) => _byte(value);

  @override
  void writeI16(int value) => _varint(_zigzag32(value));

  @override
  void writeI32(int value) => _varint(_zigzag32(value));

  @override
  void writeI64(int value) => _varint(_zigzag64(value));

  @override
  void writeDouble(double value) {
    final bytes = ByteData(8)..setFloat64(0, value, Endian.little);
    _out.add(bytes.buffer.asUint8List());
  }

  @override
  void writeString(String value) =>
      writeBinary(Int8List.fromList(utf8.encode(value)));

  @override
  void writeBinary(Int8List value) {
    _varint(value.length & 0xffffffff);
    _out.add(value.buffer.asUint8List(value.offsetInBytes, value.length));
  }

  // Read methods are unsupported on a writer.
  Never _unsupported() => throw UnsupportedError('CompactWriter cannot read');
  @override
  void readStructBegin() => _unsupported();
  @override
  void readStructEnd() => _unsupported();
  @override
  TField readFieldBegin() => _unsupported();
  @override
  void readFieldEnd() => _unsupported();
  @override
  TList readListBegin() => _unsupported();
  @override
  void readListEnd() => _unsupported();
  @override
  TMap readMapBegin() => _unsupported();
  @override
  void readMapEnd() => _unsupported();
  @override
  TList readSetBegin() => _unsupported();
  @override
  void readSetEnd() => _unsupported();
  @override
  bool readBool() => _unsupported();
  @override
  int readByte() => _unsupported();
  @override
  int readI16() => _unsupported();
  @override
  int readI32() => _unsupported();
  @override
  int readI64() => _unsupported();
  @override
  double readDouble() => _unsupported();
  @override
  String readString() => _unsupported();
  @override
  Int8List readBinary() => _unsupported();
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

class CompactReader implements TProtocol {
  final Uint8List _bytes;
  int _position = 0;
  final List<int> _fieldStack = [];
  int _lastFieldId = 0;
  bool? _boolValue;

  CompactReader(this._bytes);

  /// Bytes consumed so far (callers seek past non-length-prefixed page headers).
  int get consumed => _position;

  int _byte() {
    // Bounds-check: a truncated/corrupt buffer must fail as a domain
    // FormatException, not a raw RangeError.
    if (_position >= _bytes.length) {
      throw const FormatException('Truncated Thrift compact buffer');
    }
    return _bytes[_position++];
  }

  int _varint() {
    final (:value, :next) = readVarint(
      _bytes,
      _position,
      label: 'Thrift compact buffer',
    );
    _position = next;
    return value;
  }

  int _zigzagToInt(int value) => unzigzag(value);

  @override
  void readStructBegin() {
    _fieldStack.add(_lastFieldId);
    _lastFieldId = 0;
  }

  @override
  void readStructEnd() => _lastFieldId = _fieldStack.removeLast();

  @override
  TField readFieldBegin() {
    final type = _byte();
    if (type == TType.STOP) return const TField('', TType.STOP, 0);
    final modifier = (type & 0xf0) >> 4;
    final id = modifier == 0 ? readI16() : _lastFieldId + modifier;
    final low = type & 0x0f;
    if (low == _cBoolTrue || low == _cBoolFalse) {
      _boolValue = low == _cBoolTrue;
    }
    _lastFieldId = id;
    return TField('', _ttype(low), id);
  }

  @override
  void readFieldEnd() {}

  @override
  TList readListBegin() {
    final header = _byte();
    var length = (header >> 4) & 0x0f;
    if (length == 15) length = _varint();
    return TList(_ttype(header & 0x0f), _fits(length));
  }

  @override
  void readListEnd() {}

  @override
  TMap readMapBegin() {
    final length = _varint();
    final types = length == 0 ? 0 : _byte();
    return TMap(_ttype(types >> 4), _ttype(types & 0x0f), _fits(length));
  }

  /// [length], the declared element count of a container, when its elements
  /// can fit in the bytes left: every element takes at least one byte, so a
  /// larger count is corrupt (and would otherwise drive a loop, or an
  /// allocation, by a number the input chose).
  int _fits(int length) {
    final left = _bytes.length - _position;
    if (length > left) {
      throw FormatException(
        'Thrift container declares $length elements but $left bytes remain',
      );
    }
    return length;
  }

  @override
  void readMapEnd() {}

  @override
  TList readSetBegin() => readListBegin();

  @override
  void readSetEnd() {}

  @override
  bool readBool() {
    final value = _boolValue;
    if (value != null) {
      _boolValue = null;
      return value;
    }
    return _byte() == _cBoolTrue;
  }

  @override
  int readByte() => _byte();

  @override
  int readI16() => _zigzagToInt(_varint());

  @override
  int readI32() => _zigzagToInt(_varint());

  @override
  int readI64() => _zigzagToInt(_varint());

  /// Guard that [count] more bytes are within the logical buffer [_bytes]. Without this,
  /// readDouble's `ByteData.view(_bytes.buffer, …)` would read adjacent PARENT-buffer
  /// bytes past a slice (memory disclosure), and the string/binary slices would
  /// throw a raw RangeError instead of a domain FormatException.
  void _need(int count) {
    if (count < 0 || _position + count > _bytes.length) {
      throw const FormatException('Truncated Thrift compact buffer');
    }
  }

  @override
  double readDouble() {
    _need(8);
    final value = ByteData.view(
      _bytes.buffer,
      _bytes.offsetInBytes + _position,
      8,
    ).getFloat64(0, Endian.little);
    _position += 8;
    return value;
  }

  @override
  String readString() {
    final length = _varint();
    _need(length);
    final text = utf8.decode(
      Uint8List.sublistView(_bytes, _position, _position + length),
    );
    _position += length;
    return text;
  }

  @override
  Int8List readBinary() {
    final length = _varint();
    _need(length);
    final out = Int8List.sublistView(_bytes, _position, _position + length);
    _position += length;
    return out;
  }

  // Write methods are unsupported on a reader.
  Never _unsupported() => throw UnsupportedError('CompactReader cannot write');
  @override
  void writeStructBegin(TStruct struct) => _unsupported();
  @override
  void writeStructEnd() => _unsupported();
  @override
  void writeFieldBegin(TField field) => _unsupported();
  @override
  void writeFieldEnd() => _unsupported();
  @override
  void writeFieldStop() => _unsupported();
  @override
  void writeListBegin(TList list) => _unsupported();
  @override
  void writeListEnd() => _unsupported();
  @override
  void writeBool(bool value) => _unsupported();
  @override
  void writeByte(int value) => _unsupported();
  @override
  void writeI16(int value) => _unsupported();
  @override
  void writeI32(int value) => _unsupported();
  @override
  void writeI64(int value) => _unsupported();
  @override
  void writeDouble(double value) => _unsupported();
  @override
  void writeString(String value) => _unsupported();
  @override
  void writeBinary(Int8List value) => _unsupported();
}

/// Skips an unknown field value of [type].
class TProtocolUtil {
  /// Bounds recursion so a maliciously deeply-nested struct/list/set/map can't
  /// overflow the stack.
  static const _maxDepth = 64;

  static void skip(TProtocol protocol, int type, [int depth = 0]) {
    if (depth > _maxDepth) {
      throw FormatException('Thrift nesting exceeds max depth $_maxDepth');
    }
    switch (type) {
      case TType.BOOL:
        protocol.readBool();
        break;
      case TType.BYTE:
        protocol.readByte();
        break;
      case TType.I16:
        protocol.readI16();
        break;
      case TType.I32:
        protocol.readI32();
        break;
      case TType.I64:
        protocol.readI64();
        break;
      case TType.DOUBLE:
        protocol.readDouble();
        break;
      case TType.STRING:
        protocol.readBinary();
        break;
      case TType.STRUCT:
        protocol.readStructBegin();
        while (true) {
          final field = protocol.readFieldBegin();
          if (field.type == TType.STOP) break;
          skip(protocol, field.type, depth + 1);
          protocol.readFieldEnd();
        }
        protocol.readStructEnd();
        break;
      case TType.LIST:
        final list = protocol.readListBegin();
        for (int i = 0; i < list.length; i++) {
          skip(protocol, list.elementType, depth + 1);
        }
        protocol.readListEnd();
        break;
      case TType.SET:
        final members = protocol.readSetBegin();
        for (int i = 0; i < members.length; i++) {
          skip(protocol, members.elementType, depth + 1);
        }
        protocol.readSetEnd();
        break;
      case TType.MAP:
        final entries = protocol.readMapBegin();
        for (int i = 0; i < entries.length; i++) {
          skip(protocol, entries.keyType, depth + 1);
          skip(protocol, entries.valueType, depth + 1);
        }
        protocol.readMapEnd();
        break;
      default:
        throw FormatException('Cannot skip unknown type $type');
    }
  }
}
