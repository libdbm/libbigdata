import 'dart:typed_data';
import 'compact_protocol.dart';

export 'compact_protocol.dart'
    show TProtocol, TType, TField, TList, TStruct, TProtocolUtil;

/// Base for the generated Parquet Thrift structs: each implements compact-
/// protocol [read]/[write].
abstract class ParquetBase {
  void read(TProtocol protocol);
  void write(TProtocol protocol);
}

enum ParquetType {
  boolean(0),
  int32(1),
  int64(2),
  int96(3),
  float(4),
  double(5),
  byteArray(6),
  fixedLenByteArray(7);

  final int value;
  const ParquetType(this.value);
}

enum ConvertedType {
  utf8(0),
  map(1),
  mapKeyValue(2),
  list(3),
  enum_(4),
  decimal(5),
  date(6),
  timeMillis(7),
  timeMicros(8),
  timestampMillis(9),
  timestampMicros(10),
  uint8(11),
  uint16(12),
  uint32(13),
  uint64(14),
  int8(15),
  int16(16),
  int32(17),
  int64(18),
  json(19),
  bson(20),
  interval(21);

  final int value;
  const ConvertedType(this.value);
}

enum FieldRepetitionType {
  required(0),
  optional(1),
  repeated(2);

  final int value;
  const FieldRepetitionType(this.value);
}

enum Encoding {
  plain(0),
  plainDictionary(2),
  rle(3),
  bitPacked(4),
  deltaBinaryPacked(5),
  deltaLengthByteArray(6),
  deltaByteArray(7),
  rleDictionary(8),
  byteStreamSplit(9);

  final int value;
  const Encoding(this.value);
}

enum CompressionCodec {
  uncompressed(0),
  snappy(1),
  gzip(2),
  lzo(3),
  brotli(4),
  lz4(5),
  zstd(6),
  lz4Raw(7);

  final int value;
  const CompressionCodec(this.value);
}

enum PageType {
  dataPage(0),
  indexPage(1),
  dictionaryPage(2),
  dataPageV2(3);

  final int value;
  const PageType(this.value);
}

class Statistics extends ParquetBase {
  Uint8List? max;
  Uint8List? min;
  int? nullCount;
  int? distinctCount;
  Uint8List? maxValue;
  Uint8List? minValue;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          max = Uint8List.fromList(protocol.readBinary());
          break;
        case 2:
          min = Uint8List.fromList(protocol.readBinary());
          break;
        case 3:
          nullCount = protocol.readI64();
          break;
        case 4:
          distinctCount = protocol.readI64();
          break;
        case 5:
          maxValue = Uint8List.fromList(protocol.readBinary());
          break;
        case 6:
          minValue = Uint8List.fromList(protocol.readBinary());
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('Statistics'));
    if (max != null) {
      protocol.writeFieldBegin(TField('max', TType.STRING, 1));
      protocol.writeBinary(Int8List.fromList(max!));
      protocol.writeFieldEnd();
    }
    if (min != null) {
      protocol.writeFieldBegin(TField('min', TType.STRING, 2));
      protocol.writeBinary(Int8List.fromList(min!));
      protocol.writeFieldEnd();
    }
    if (nullCount != null) {
      protocol.writeFieldBegin(TField('nullCount', TType.I64, 3));
      protocol.writeI64(nullCount!);
      protocol.writeFieldEnd();
    }
    if (distinctCount != null) {
      protocol.writeFieldBegin(TField('distinctCount', TType.I64, 4));
      protocol.writeI64(distinctCount!);
      protocol.writeFieldEnd();
    }
    if (maxValue != null) {
      protocol.writeFieldBegin(TField('maxValue', TType.STRING, 5));
      protocol.writeBinary(Int8List.fromList(maxValue!));
      protocol.writeFieldEnd();
    }
    if (minValue != null) {
      protocol.writeFieldBegin(TField('minValue', TType.STRING, 6));
      protocol.writeBinary(Int8List.fromList(minValue!));
      protocol.writeFieldEnd();
    }
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class SchemaElement extends ParquetBase {
  ParquetType? type;
  int? typeLength;
  FieldRepetitionType? repetitionType;
  String? name;
  int? numChildren;
  ConvertedType? convertedType;
  int? scale;
  int? precision;
  int? fieldId;
  LogicalType? logicalType;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          final raw = protocol.readI32();
          type = ParquetType.values.firstWhere(
            (item) => item.value == raw,
            orElse: () =>
                throw FormatException('Invalid ParquetType value: $raw'),
          );
          break;
        case 2:
          typeLength = protocol.readI32();
          break;
        case 3:
          final raw = protocol.readI32();
          repetitionType = FieldRepetitionType.values.firstWhere(
            (item) => item.value == raw,
            orElse: () => throw FormatException(
              'Invalid FieldRepetitionType value: $raw',
            ),
          );
          break;
        case 4:
          name = protocol.readString();
          break;
        case 5:
          numChildren = protocol.readI32();
          break;
        case 6:
          // An unknown value (a newer writer) is treated as absent, as
          // parquet-mr does: the logical type or physical type still apply.
          final raw = protocol.readI32();
          convertedType = ConvertedType.values
              .where((item) => item.value == raw)
              .firstOrNull;
          break;
        case 7:
          scale = protocol.readI32();
          break;
        case 8:
          precision = protocol.readI32();
          break;
        case 9:
          fieldId = protocol.readI32();
          break;
        case 10:
          logicalType = LogicalType();
          logicalType!.read(protocol);
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('SchemaElement'));
    if (type != null) {
      protocol.writeFieldBegin(TField('type', TType.I32, 1));
      protocol.writeI32(type!.value);
      protocol.writeFieldEnd();
    }
    if (typeLength != null) {
      protocol.writeFieldBegin(TField('typeLength', TType.I32, 2));
      protocol.writeI32(typeLength!);
      protocol.writeFieldEnd();
    }
    if (repetitionType != null) {
      protocol.writeFieldBegin(TField('repetitionType', TType.I32, 3));
      protocol.writeI32(repetitionType!.value);
      protocol.writeFieldEnd();
    }
    if (name != null) {
      protocol.writeFieldBegin(TField('name', TType.STRING, 4));
      protocol.writeString(name!);
      protocol.writeFieldEnd();
    }
    if (numChildren != null) {
      protocol.writeFieldBegin(TField('numChildren', TType.I32, 5));
      protocol.writeI32(numChildren!);
      protocol.writeFieldEnd();
    }
    if (convertedType != null) {
      protocol.writeFieldBegin(TField('convertedType', TType.I32, 6));
      protocol.writeI32(convertedType!.value);
      protocol.writeFieldEnd();
    }
    if (scale != null) {
      protocol.writeFieldBegin(TField('scale', TType.I32, 7));
      protocol.writeI32(scale!);
      protocol.writeFieldEnd();
    }
    if (precision != null) {
      protocol.writeFieldBegin(TField('precision', TType.I32, 8));
      protocol.writeI32(precision!);
      protocol.writeFieldEnd();
    }
    if (fieldId != null) {
      protocol.writeFieldBegin(TField('fieldId', TType.I32, 9));
      protocol.writeI32(fieldId!);
      protocol.writeFieldEnd();
    }
    if (logicalType != null) {
      protocol.writeFieldBegin(TField('logicalType', TType.STRUCT, 10));
      logicalType!.write(protocol);
      protocol.writeFieldEnd();
    }
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

/// Write [value] as struct field [id], or nothing when it is null.
void _writeStructField(
  TProtocol protocol,
  String name,
  int id,
  ParquetBase? value,
) {
  if (value == null) return;
  protocol.writeFieldBegin(TField(name, TType.STRUCT, id));
  value.write(protocol);
  protocol.writeFieldEnd();
}

class LogicalType extends ParquetBase {
  StringType? string;
  MapType? map;
  ListType? list;
  EnumType? enum_;
  DecimalType? decimal;
  DateType? date;
  TimeType? time;
  TimestampType? timestamp;
  IntType? integer;
  UnknownType? unknown;
  JsonType? json;
  BsonType? bson;
  UuidType? uuid;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          string = StringType();
          string!.read(protocol);
          break;
        case 2:
          map = MapType();
          map!.read(protocol);
          break;
        case 3:
          list = ListType();
          list!.read(protocol);
          break;
        case 4:
          enum_ = EnumType();
          enum_!.read(protocol);
          break;
        case 5:
          decimal = DecimalType();
          decimal!.read(protocol);
          break;
        case 6:
          date = DateType();
          date!.read(protocol);
          break;
        case 7:
          time = TimeType();
          time!.read(protocol);
          break;
        case 8:
          timestamp = TimestampType();
          timestamp!.read(protocol);
          break;
        case 10:
          integer = IntType();
          integer!.read(protocol);
          break;
        case 11:
          unknown = UnknownType();
          unknown!.read(protocol);
          break;
        case 12:
          json = JsonType();
          json!.read(protocol);
          break;
        case 13:
          bson = BsonType();
          bson!.read(protocol);
          break;
        case 14:
          uuid = UuidType();
          uuid!.read(protocol);
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    // A union: exactly one member is set.
    protocol.writeStructBegin(TStruct('LogicalType'));
    _writeStructField(protocol, 'STRING', 1, string);
    _writeStructField(protocol, 'MAP', 2, map);
    _writeStructField(protocol, 'LIST', 3, list);
    _writeStructField(protocol, 'ENUM', 4, enum_);
    _writeStructField(protocol, 'DECIMAL', 5, decimal);
    _writeStructField(protocol, 'DATE', 6, date);
    _writeStructField(protocol, 'TIME', 7, time);
    _writeStructField(protocol, 'TIMESTAMP', 8, timestamp);
    _writeStructField(protocol, 'INTEGER', 10, integer);
    _writeStructField(protocol, 'UNKNOWN', 11, unknown);
    _writeStructField(protocol, 'JSON', 12, json);
    _writeStructField(protocol, 'BSON', 13, bson);
    _writeStructField(protocol, 'UUID', 14, uuid);
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class StringType extends ParquetBase {
  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;
      TProtocolUtil.skip(protocol, field.type);
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('StringType'));
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class MapType extends ParquetBase {
  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;
      TProtocolUtil.skip(protocol, field.type);
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('MapType'));
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class ListType extends ParquetBase {
  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;
      TProtocolUtil.skip(protocol, field.type);
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('ListType'));
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class EnumType extends ParquetBase {
  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;
      TProtocolUtil.skip(protocol, field.type);
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('EnumType'));
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class DecimalType extends ParquetBase {
  int? scale;
  int? precision;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          scale = protocol.readI32();
          break;
        case 2:
          precision = protocol.readI32();
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('DecimalType'));
    protocol.writeFieldBegin(TField('scale', TType.I32, 1));
    protocol.writeI32(scale ?? 0);
    protocol.writeFieldEnd();
    protocol.writeFieldBegin(TField('precision', TType.I32, 2));
    protocol.writeI32(precision!);
    protocol.writeFieldEnd();
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class DateType extends ParquetBase {
  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;
      TProtocolUtil.skip(protocol, field.type);
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('DateType'));
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class TimeType extends ParquetBase {
  bool? isAdjustedToUTC;
  TimeUnit? unit;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          isAdjustedToUTC = protocol.readBool();
          break;
        case 2:
          unit = TimeUnit();
          unit!.read(protocol);
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('TimeType'));
    protocol.writeFieldBegin(TField('isAdjustedToUTC', TType.BOOL, 1));
    protocol.writeBool(isAdjustedToUTC!);
    protocol.writeFieldEnd();
    _writeStructField(protocol, 'unit', 2, unit);
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class TimestampType extends ParquetBase {
  bool? isAdjustedToUTC;
  TimeUnit? unit;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          isAdjustedToUTC = protocol.readBool();
          break;
        case 2:
          unit = TimeUnit();
          unit!.read(protocol);
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('TimestampType'));
    protocol.writeFieldBegin(TField('isAdjustedToUTC', TType.BOOL, 1));
    protocol.writeBool(isAdjustedToUTC!);
    protocol.writeFieldEnd();
    _writeStructField(protocol, 'unit', 2, unit);
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class TimeUnit extends ParquetBase {
  MilliSeconds? millis;
  MicroSeconds? micros;
  NanoSeconds? nanos;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          millis = MilliSeconds();
          millis!.read(protocol);
          break;
        case 2:
          micros = MicroSeconds();
          micros!.read(protocol);
          break;
        case 3:
          nanos = NanoSeconds();
          nanos!.read(protocol);
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    // A union: exactly one member is set.
    protocol.writeStructBegin(TStruct('TimeUnit'));
    _writeStructField(protocol, 'MILLIS', 1, millis);
    _writeStructField(protocol, 'MICROS', 2, micros);
    _writeStructField(protocol, 'NANOS', 3, nanos);
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class MilliSeconds extends ParquetBase {
  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;
      TProtocolUtil.skip(protocol, field.type);
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('MilliSeconds'));
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class MicroSeconds extends ParquetBase {
  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;
      TProtocolUtil.skip(protocol, field.type);
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('MicroSeconds'));
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class NanoSeconds extends ParquetBase {
  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;
      TProtocolUtil.skip(protocol, field.type);
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('NanoSeconds'));
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class IntType extends ParquetBase {
  int? bitWidth;
  bool? isSigned;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          bitWidth = protocol.readByte();
          break;
        case 2:
          isSigned = protocol.readBool();
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('IntType'));
    protocol.writeFieldBegin(TField('bitWidth', TType.BYTE, 1));
    protocol.writeByte(bitWidth!);
    protocol.writeFieldEnd();
    protocol.writeFieldBegin(TField('isSigned', TType.BOOL, 2));
    protocol.writeBool(isSigned!);
    protocol.writeFieldEnd();
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class UnknownType extends ParquetBase {
  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;
      TProtocolUtil.skip(protocol, field.type);
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('UnknownType'));
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class JsonType extends ParquetBase {
  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;
      TProtocolUtil.skip(protocol, field.type);
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('JsonType'));
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class BsonType extends ParquetBase {
  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;
      TProtocolUtil.skip(protocol, field.type);
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('BsonType'));
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class UuidType extends ParquetBase {
  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;
      TProtocolUtil.skip(protocol, field.type);
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('UuidType'));
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class DataPageHeader extends ParquetBase {
  int numValues = 0;
  Encoding encoding = Encoding.plain;
  Encoding definitionLevelEncoding = Encoding.plain;
  Encoding repetitionLevelEncoding = Encoding.plain;
  Statistics? statistics;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          numValues = protocol.readI32();
          break;
        case 2:
          final raw = protocol.readI32();
          encoding = Encoding.values.firstWhere(
            (item) => item.value == raw,
            orElse: () => throw FormatException('Invalid Encoding value: $raw'),
          );
          break;
        case 3:
          final raw = protocol.readI32();
          definitionLevelEncoding = Encoding.values.firstWhere(
            (item) => item.value == raw,
            orElse: () => throw FormatException(
              'Invalid Encoding value for definition level: $raw',
            ),
          );
          break;
        case 4:
          final raw = protocol.readI32();
          repetitionLevelEncoding = Encoding.values.firstWhere(
            (item) => item.value == raw,
            orElse: () => throw FormatException(
              'Invalid Encoding value for repetition level: $raw',
            ),
          );
          break;
        case 5:
          statistics = Statistics();
          statistics!.read(protocol);
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('DataPageHeader'));

    protocol.writeFieldBegin(TField('num_values', TType.I32, 1));
    protocol.writeI32(numValues);
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('encoding', TType.I32, 2));
    protocol.writeI32(encoding.value);
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('definition_level_encoding', TType.I32, 3));
    protocol.writeI32(definitionLevelEncoding.value);
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('repetition_level_encoding', TType.I32, 4));
    protocol.writeI32(repetitionLevelEncoding.value);
    protocol.writeFieldEnd();

    if (statistics != null) {
      protocol.writeFieldBegin(TField('statistics', TType.STRUCT, 5));
      statistics!.write(protocol);
      protocol.writeFieldEnd();
    }

    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class DataPageHeaderV2 extends ParquetBase {
  int numValues = 0;
  int numNulls = 0;
  int numRows = 0;
  Encoding encoding = Encoding.plain;
  int definitionLevelsByteLength = 0;
  int repetitionLevelsByteLength = 0;
  bool? isCompressed;
  Statistics? statistics;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          numValues = protocol.readI32();
          break;
        case 2:
          numNulls = protocol.readI32();
          break;
        case 3:
          numRows = protocol.readI32();
          break;
        case 4:
          final raw = protocol.readI32();
          encoding = Encoding.values.firstWhere(
            (item) => item.value == raw,
            orElse: () => throw FormatException('Invalid Encoding value: $raw'),
          );
          break;
        case 5:
          definitionLevelsByteLength = protocol.readI32();
          break;
        case 6:
          repetitionLevelsByteLength = protocol.readI32();
          break;
        case 7:
          isCompressed = protocol.readBool();
          break;
        case 8:
          statistics = Statistics();
          statistics!.read(protocol);
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('DataPageHeaderV2'));
    for (final (name, id, value) in [
      ('num_values', 1, numValues),
      ('num_nulls', 2, numNulls),
      ('num_rows', 3, numRows),
      ('encoding', 4, encoding.value),
      ('definition_levels_byte_length', 5, definitionLevelsByteLength),
      ('repetition_levels_byte_length', 6, repetitionLevelsByteLength),
    ]) {
      protocol.writeFieldBegin(TField(name, TType.I32, id));
      protocol.writeI32(value);
      protocol.writeFieldEnd();
    }
    if (isCompressed != null) {
      protocol.writeFieldBegin(TField('is_compressed', TType.BOOL, 7));
      protocol.writeBool(isCompressed!);
      protocol.writeFieldEnd();
    }
    _writeStructField(protocol, 'statistics', 8, statistics);
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class DictionaryPageHeader extends ParquetBase {
  int numValues = 0;
  Encoding encoding = Encoding.plain;
  bool? isSorted;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          numValues = protocol.readI32();
          break;
        case 2:
          final raw = protocol.readI32();
          encoding = Encoding.values.firstWhere(
            (item) => item.value == raw,
            orElse: () => throw FormatException('Invalid Encoding value: $raw'),
          );
          break;
        case 3:
          isSorted = protocol.readBool();
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('DictionaryPageHeader'));
    protocol.writeFieldBegin(TField('num_values', TType.I32, 1));
    protocol.writeI32(numValues);
    protocol.writeFieldEnd();
    protocol.writeFieldBegin(TField('encoding', TType.I32, 2));
    protocol.writeI32(encoding.value);
    protocol.writeFieldEnd();
    if (isSorted != null) {
      protocol.writeFieldBegin(TField('is_sorted', TType.BOOL, 3));
      protocol.writeBool(isSorted!);
      protocol.writeFieldEnd();
    }
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class PageHeader extends ParquetBase {
  PageType type = PageType.dataPage;
  int uncompressedPageSize = 0;
  int compressedPageSize = 0;
  int? crc;
  DataPageHeader? dataPageHeader;
  DataPageHeaderV2? dataPageHeaderV2;
  DictionaryPageHeader? dictionaryPageHeader;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          final raw = protocol.readI32();
          type = PageType.values.firstWhere(
            (item) => item.value == raw,
            orElse: () => throw FormatException('Invalid PageType value: $raw'),
          );
          break;
        case 2:
          uncompressedPageSize = protocol.readI32();
          break;
        case 3:
          compressedPageSize = protocol.readI32();
          break;
        case 4:
          crc = protocol.readI32();
          break;
        case 5:
          dataPageHeader = DataPageHeader();
          dataPageHeader!.read(protocol);
          break;
        case 7:
          dictionaryPageHeader = DictionaryPageHeader();
          dictionaryPageHeader!.read(protocol);
          break;
        case 8:
          dataPageHeaderV2 = DataPageHeaderV2();
          dataPageHeaderV2!.read(protocol);
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('PageHeader'));

    protocol.writeFieldBegin(TField('type', TType.I32, 1));
    protocol.writeI32(type.value);
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('uncompressed_page_size', TType.I32, 2));
    protocol.writeI32(uncompressedPageSize);
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('compressed_page_size', TType.I32, 3));
    protocol.writeI32(compressedPageSize);
    protocol.writeFieldEnd();

    if (crc != null) {
      protocol.writeFieldBegin(TField('crc', TType.I32, 4));
      protocol.writeI32(crc!);
      protocol.writeFieldEnd();
    }

    if (dataPageHeader != null) {
      protocol.writeFieldBegin(TField('data_page_header', TType.STRUCT, 5));
      dataPageHeader!.write(protocol);
      protocol.writeFieldEnd();
    }

    if (dictionaryPageHeader != null) {
      protocol.writeFieldBegin(
        TField('dictionary_page_header', TType.STRUCT, 7),
      );
      dictionaryPageHeader!.write(protocol);
      protocol.writeFieldEnd();
    }

    if (dataPageHeaderV2 != null) {
      protocol.writeFieldBegin(TField('data_page_header_v2', TType.STRUCT, 8));
      dataPageHeaderV2!.write(protocol);
      protocol.writeFieldEnd();
    }

    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class KeyValue extends ParquetBase {
  String key = '';
  String? value;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          key = protocol.readString();
          break;
        case 2:
          value = protocol.readString();
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('KeyValue'));
    protocol.writeFieldBegin(TField('key', TType.STRING, 1));
    protocol.writeString(key);
    protocol.writeFieldEnd();
    if (value != null) {
      protocol.writeFieldBegin(TField('value', TType.STRING, 2));
      protocol.writeString(value!);
      protocol.writeFieldEnd();
    }
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class SortingColumn extends ParquetBase {
  int columnIdx = 0;
  bool descending = false;
  bool nullsFirst = false;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          columnIdx = protocol.readI32();
          break;
        case 2:
          descending = protocol.readBool();
          break;
        case 3:
          nullsFirst = protocol.readBool();
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('SortingColumn'));
    protocol.writeFieldBegin(TField('column_idx', TType.I32, 1));
    protocol.writeI32(columnIdx);
    protocol.writeFieldEnd();
    protocol.writeFieldBegin(TField('descending', TType.BOOL, 2));
    protocol.writeBool(descending);
    protocol.writeFieldEnd();
    protocol.writeFieldBegin(TField('nulls_first', TType.BOOL, 3));
    protocol.writeBool(nullsFirst);
    protocol.writeFieldEnd();
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class PageEncodingStats extends ParquetBase {
  PageType pageType = PageType.dataPage;
  Encoding encoding = Encoding.plain;
  int count = 0;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          final raw = protocol.readI32();
          pageType = PageType.values.firstWhere(
            (item) => item.value == raw,
            orElse: () => throw FormatException('Invalid PageType value: $raw'),
          );
          break;
        case 2:
          final raw = protocol.readI32();
          encoding = Encoding.values.firstWhere(
            (item) => item.value == raw,
            orElse: () => throw FormatException('Invalid Encoding value: $raw'),
          );
          break;
        case 3:
          count = protocol.readI32();
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('PageEncodingStats'));
    protocol.writeFieldBegin(TField('page_type', TType.I32, 1));
    protocol.writeI32(pageType.value);
    protocol.writeFieldEnd();
    protocol.writeFieldBegin(TField('encoding', TType.I32, 2));
    protocol.writeI32(encoding.value);
    protocol.writeFieldEnd();
    protocol.writeFieldBegin(TField('count', TType.I32, 3));
    protocol.writeI32(count);
    protocol.writeFieldEnd();
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class ColumnMetaData extends ParquetBase {
  ParquetType type = ParquetType.boolean;
  List<Encoding> encodings = [];
  List<String> pathInSchema = [];
  CompressionCodec codec = CompressionCodec.uncompressed;
  int numValues = 0;
  int totalUncompressedSize = 0;
  int totalCompressedSize = 0;
  List<KeyValue>? keyValueMetadata;
  int dataPageOffset = 0;
  int? indexPageOffset;
  int? dictionaryPageOffset;
  Statistics? statistics;
  List<PageEncodingStats>? encodingStats;
  int? bloomFilterOffset;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          final raw = protocol.readI32();
          type = ParquetType.values.firstWhere(
            (item) => item.value == raw,
            orElse: () =>
                throw FormatException('Invalid ParquetType value: $raw'),
          );
          break;
        case 2:
          encodings = [];
          final listHeader = protocol.readListBegin();
          for (int i = 0; i < listHeader.length; i++) {
            final raw = protocol.readI32();
            encodings.add(
              Encoding.values.firstWhere(
                (item) => item.value == raw,
                orElse: () => throw FormatException(
                  'Invalid Encoding value in list: $raw',
                ),
              ),
            );
          }
          protocol.readListEnd();
          break;
        case 3:
          pathInSchema = [];
          final pathHeader = protocol.readListBegin();
          for (int i = 0; i < pathHeader.length; i++) {
            pathInSchema.add(protocol.readString());
          }
          protocol.readListEnd();
          break;
        case 4:
          final raw = protocol.readI32();
          codec = CompressionCodec.values.firstWhere(
            (item) => item.value == raw,
            orElse: () =>
                throw FormatException('Invalid CompressionCodec value: $raw'),
          );
          break;
        case 5:
          numValues = protocol.readI64();
          break;
        case 6:
          totalUncompressedSize = protocol.readI64();
          break;
        case 7:
          totalCompressedSize = protocol.readI64();
          break;
        case 8:
          keyValueMetadata = [];
          final entryHeader = protocol.readListBegin();
          for (int i = 0; i < entryHeader.length; i++) {
            final entry = KeyValue();
            entry.read(protocol);
            keyValueMetadata!.add(entry);
          }
          protocol.readListEnd();
          break;
        case 9:
          dataPageOffset = protocol.readI64();
          break;
        case 10:
          indexPageOffset = protocol.readI64();
          break;
        case 11:
          dictionaryPageOffset = protocol.readI64();
          break;
        case 12:
          statistics = Statistics();
          statistics!.read(protocol);
          break;
        case 13:
          encodingStats = [];
          final statsHeader = protocol.readListBegin();
          for (int i = 0; i < statsHeader.length; i++) {
            final stats = PageEncodingStats();
            stats.read(protocol);
            encodingStats!.add(stats);
          }
          protocol.readListEnd();
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('ColumnMetaData'));

    protocol.writeFieldBegin(TField('type', TType.I32, 1));
    protocol.writeI32(type.value);
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('encodings', TType.LIST, 2));
    protocol.writeListBegin(TList(TType.I32, encodings.length));
    for (final encoding in encodings) {
      protocol.writeI32(encoding.value);
    }
    protocol.writeListEnd();
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('path_in_schema', TType.LIST, 3));
    protocol.writeListBegin(TList(TType.STRING, pathInSchema.length));
    for (final path in pathInSchema) {
      protocol.writeString(path);
    }
    protocol.writeListEnd();
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('codec', TType.I32, 4));
    protocol.writeI32(codec.value);
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('num_values', TType.I64, 5));
    protocol.writeI64(numValues);
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('total_uncompressed_size', TType.I64, 6));
    protocol.writeI64(totalUncompressedSize);
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('total_compressed_size', TType.I64, 7));
    protocol.writeI64(totalCompressedSize);
    protocol.writeFieldEnd();

    if (keyValueMetadata != null) {
      protocol.writeFieldBegin(TField('key_value_metadata', TType.LIST, 8));
      protocol.writeListBegin(TList(TType.STRUCT, keyValueMetadata!.length));
      for (final entry in keyValueMetadata!) {
        entry.write(protocol);
      }
      protocol.writeListEnd();
      protocol.writeFieldEnd();
    }

    protocol.writeFieldBegin(TField('data_page_offset', TType.I64, 9));
    protocol.writeI64(dataPageOffset);
    protocol.writeFieldEnd();

    if (indexPageOffset != null) {
      protocol.writeFieldBegin(TField('index_page_offset', TType.I64, 10));
      protocol.writeI64(indexPageOffset!);
      protocol.writeFieldEnd();
    }

    if (dictionaryPageOffset != null) {
      protocol.writeFieldBegin(TField('dictionary_page_offset', TType.I64, 11));
      protocol.writeI64(dictionaryPageOffset!);
      protocol.writeFieldEnd();
    }

    if (statistics != null) {
      protocol.writeFieldBegin(TField('statistics', TType.STRUCT, 12));
      statistics!.write(protocol);
      protocol.writeFieldEnd();
    }

    if (encodingStats != null) {
      protocol.writeFieldBegin(TField('encoding_stats', TType.LIST, 13));
      protocol.writeListBegin(TList(TType.STRUCT, encodingStats!.length));
      for (final stat in encodingStats!) {
        stat.write(protocol);
      }
      protocol.writeListEnd();
      protocol.writeFieldEnd();
    }

    if (bloomFilterOffset != null) {
      protocol.writeFieldBegin(TField('bloom_filter_offset', TType.I64, 14));
      protocol.writeI64(bloomFilterOffset!);
      protocol.writeFieldEnd();
    }

    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class ColumnChunk extends ParquetBase {
  String? filePath;
  int fileOffset = 0;
  ColumnMetaData? metaData;
  int? offsetIndexOffset;
  int? offsetIndexLength;
  int? columnIndexOffset;
  int? columnIndexLength;
  Uint8List? encryptedColumnMetadata;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          filePath = protocol.readString();
          break;
        case 2:
          fileOffset = protocol.readI64();
          break;
        case 3:
          metaData = ColumnMetaData();
          metaData!.read(protocol);
          break;
        case 4:
          offsetIndexOffset = protocol.readI64();
          break;
        case 5:
          offsetIndexLength = protocol.readI32();
          break;
        case 6:
          columnIndexOffset = protocol.readI64();
          break;
        case 7:
          columnIndexLength = protocol.readI32();
          break;
        case 8:
          // ColumnCryptoMetaData, a struct; encrypted columns are unsupported.
          TProtocolUtil.skip(protocol, field.type);
          break;
        case 9:
          encryptedColumnMetadata = Uint8List.fromList(protocol.readBinary());
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('ColumnChunk'));

    if (filePath != null) {
      protocol.writeFieldBegin(TField('file_path', TType.STRING, 1));
      protocol.writeString(filePath!);
      protocol.writeFieldEnd();
    }

    protocol.writeFieldBegin(TField('file_offset', TType.I64, 2));
    protocol.writeI64(fileOffset);
    protocol.writeFieldEnd();

    if (metaData != null) {
      protocol.writeFieldBegin(TField('meta_data', TType.STRUCT, 3));
      metaData!.write(protocol);
      protocol.writeFieldEnd();
    }

    if (offsetIndexOffset != null) {
      protocol.writeFieldBegin(TField('offset_index_offset', TType.I64, 4));
      protocol.writeI64(offsetIndexOffset!);
      protocol.writeFieldEnd();
    }

    if (offsetIndexLength != null) {
      protocol.writeFieldBegin(TField('offset_index_length', TType.I32, 5));
      protocol.writeI32(offsetIndexLength!);
      protocol.writeFieldEnd();
    }

    if (columnIndexOffset != null) {
      protocol.writeFieldBegin(TField('column_index_offset', TType.I64, 6));
      protocol.writeI64(columnIndexOffset!);
      protocol.writeFieldEnd();
    }

    if (columnIndexLength != null) {
      protocol.writeFieldBegin(TField('column_index_length', TType.I32, 7));
      protocol.writeI32(columnIndexLength!);
      protocol.writeFieldEnd();
    }

    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class RowGroup extends ParquetBase {
  List<ColumnChunk> columns = [];
  int totalByteSize = 0;
  int numRows = 0;
  List<SortingColumn>? sortingColumns;
  int? fileOffset;
  int? totalCompressedSize;
  int? ordinal;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          columns = [];
          final columnHeader = protocol.readListBegin();
          for (int i = 0; i < columnHeader.length; i++) {
            final chunk = ColumnChunk();
            chunk.read(protocol);
            columns.add(chunk);
          }
          protocol.readListEnd();
          break;
        case 2:
          totalByteSize = protocol.readI64();
          break;
        case 3:
          numRows = protocol.readI64();
          break;
        case 4:
          sortingColumns = [];
          final sortHeader = protocol.readListBegin();
          for (int i = 0; i < sortHeader.length; i++) {
            final column = SortingColumn();
            column.read(protocol);
            sortingColumns!.add(column);
          }
          protocol.readListEnd();
          break;
        case 5:
          fileOffset = protocol.readI64();
          break;
        case 6:
          totalCompressedSize = protocol.readI64();
          break;
        case 7:
          ordinal = protocol.readI16();
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('RowGroup'));

    protocol.writeFieldBegin(TField('columns', TType.LIST, 1));
    protocol.writeListBegin(TList(TType.STRUCT, columns.length));
    for (final column in columns) {
      column.write(protocol);
    }
    protocol.writeListEnd();
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('total_byte_size', TType.I64, 2));
    protocol.writeI64(totalByteSize);
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('num_rows', TType.I64, 3));
    protocol.writeI64(numRows);
    protocol.writeFieldEnd();

    if (sortingColumns != null) {
      protocol.writeFieldBegin(TField('sorting_columns', TType.LIST, 4));
      protocol.writeListBegin(TList(TType.STRUCT, sortingColumns!.length));
      for (final sortingColumn in sortingColumns!) {
        sortingColumn.write(protocol);
      }
      protocol.writeListEnd();
      protocol.writeFieldEnd();
    }

    if (fileOffset != null) {
      protocol.writeFieldBegin(TField('file_offset', TType.I64, 5));
      protocol.writeI64(fileOffset!);
      protocol.writeFieldEnd();
    }

    if (totalCompressedSize != null) {
      protocol.writeFieldBegin(TField('total_compressed_size', TType.I64, 6));
      protocol.writeI64(totalCompressedSize!);
      protocol.writeFieldEnd();
    }

    if (ordinal != null) {
      protocol.writeFieldBegin(TField('ordinal', TType.I16, 7));
      protocol.writeI16(ordinal!);
      protocol.writeFieldEnd();
    }

    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

/// The order a column's `min_value` and `max_value` statistics follow: a
/// union whose only member, [typeOrder], is the order the format defines for
/// the column's type.
class ColumnOrder extends ParquetBase {
  TypeDefinedOrder? typeOrder;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;
      if (field.id == 1 && field.type == TType.STRUCT) {
        typeOrder = TypeDefinedOrder()..read(protocol);
      } else {
        TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('ColumnOrder'));
    _writeStructField(protocol, 'TYPE_ORDER', 1, typeOrder);
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class TypeDefinedOrder extends ParquetBase {
  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;
      TProtocolUtil.skip(protocol, field.type);
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('TypeDefinedOrder'));
    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}

class FileMetaData extends ParquetBase {
  int version = 0;
  List<SchemaElement> schema = [];
  int numRows = 0;
  List<RowGroup> rowGroups = [];
  List<KeyValue>? keyValueMetadata;
  String? createdBy;
  List<ColumnOrder>? columnOrders;

  @override
  void read(TProtocol protocol) {
    protocol.readStructBegin();
    while (true) {
      final field = protocol.readFieldBegin();
      if (field.type == TType.STOP) break;

      switch (field.id) {
        case 1:
          version = protocol.readI32();
          break;
        case 2:
          schema = [];
          final schemaHeader = protocol.readListBegin();
          for (int i = 0; i < schemaHeader.length; i++) {
            final element = SchemaElement();
            element.read(protocol);
            schema.add(element);
          }
          protocol.readListEnd();
          break;
        case 3:
          numRows = protocol.readI64();
          break;
        case 4:
          rowGroups = [];
          final groupHeader = protocol.readListBegin();
          for (int i = 0; i < groupHeader.length; i++) {
            final rg = RowGroup();
            rg.read(protocol);
            rowGroups.add(rg);
          }
          protocol.readListEnd();
          break;
        case 5:
          keyValueMetadata = [];
          final entryHeader = protocol.readListBegin();
          for (int i = 0; i < entryHeader.length; i++) {
            final entry = KeyValue();
            entry.read(protocol);
            keyValueMetadata!.add(entry);
          }
          protocol.readListEnd();
          break;
        case 6:
          createdBy = protocol.readString();
          break;
        case 7:
          columnOrders = [];
          final orderHeader = protocol.readListBegin();
          for (int i = 0; i < orderHeader.length; i++) {
            final co = ColumnOrder();
            co.read(protocol);
            columnOrders!.add(co);
          }
          protocol.readListEnd();
          break;
        default:
          TProtocolUtil.skip(protocol, field.type);
      }
      protocol.readFieldEnd();
    }
    protocol.readStructEnd();
  }

  @override
  void write(TProtocol protocol) {
    protocol.writeStructBegin(TStruct('FileMetaData'));

    protocol.writeFieldBegin(TField('version', TType.I32, 1));
    protocol.writeI32(version);
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('schema', TType.LIST, 2));
    protocol.writeListBegin(TList(TType.STRUCT, schema.length));
    for (final element in schema) {
      element.write(protocol);
    }
    protocol.writeListEnd();
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('num_rows', TType.I64, 3));
    protocol.writeI64(numRows);
    protocol.writeFieldEnd();

    protocol.writeFieldBegin(TField('row_groups', TType.LIST, 4));
    protocol.writeListBegin(TList(TType.STRUCT, rowGroups.length));
    for (final rowGroup in rowGroups) {
      rowGroup.write(protocol);
    }
    protocol.writeListEnd();
    protocol.writeFieldEnd();

    if (keyValueMetadata != null) {
      protocol.writeFieldBegin(TField('key_value_metadata', TType.LIST, 5));
      protocol.writeListBegin(TList(TType.STRUCT, keyValueMetadata!.length));
      for (final entry in keyValueMetadata!) {
        entry.write(protocol);
      }
      protocol.writeListEnd();
      protocol.writeFieldEnd();
    }

    if (createdBy != null) {
      protocol.writeFieldBegin(TField('created_by', TType.STRING, 6));
      protocol.writeString(createdBy!);
      protocol.writeFieldEnd();
    }

    if (columnOrders != null) {
      protocol.writeFieldBegin(TField('column_orders', TType.LIST, 7));
      protocol.writeListBegin(TList(TType.STRUCT, columnOrders!.length));
      for (final order in columnOrders!) {
        order.write(protocol);
      }
      protocol.writeListEnd();
      protocol.writeFieldEnd();
    }

    protocol.writeFieldStop();
    protocol.writeStructEnd();
  }
}
