import 'dart:typed_data';

import '../../core/conversions.dart' show checkedDecimal;
import '../../core/data_type.dart';
import '../../core/limits.dart';
import '../../core/schema.dart';
import 'flatbuffers.dart';
import '../../core/failure.dart';
import '../../core/result.dart';

/// Builds and parses the Apache Arrow IPC metadata messages (FlatBuffer encoded)
/// per the Arrow `Message.fbs`/`Schema.fbs`/`File.fbs` schemas. Core types only.

// MetadataVersion.V5
const int metadataV5 = 4;

// MessageHeader union codes (0 = NONE).
const int headerSchema = 1;
const int headerDictionaryBatch = 2;
const int headerRecordBatch = 3;

// Type union codes (0 = NONE).
const int _typeNull = 1;
const int _typeInt = 2;
const int _typeFloat = 3;
const int _typeBinary = 4;
const int _typeUtf8 = 5;
const int _typeBool = 6;
const int _typeDecimal = 7;
const int _typeDate = 8;
const int _typeTimestamp = 10;
const int _typeList = 12;
const int _typeStruct = 13;
const int _typeMap = 17;

// Precision enum.
const int _precisionSingle = 1;
const int _precisionDouble = 2;

// DateUnit: DAY=0, MILLISECOND=1. The schema's default is MILLISECOND.
const int _dateDay = 0;
const int _dateMilli = 1;
// TimeUnit: SECOND=0, MILLISECOND=1, MICROSECOND=2, NANOSECOND=3. The schema's
// default for a timestamp is SECOND.
const int _timeSecond = 0;
const int _timeMicro = 2;

/// Metadata key carrying a timestamp column's Arrow TimeUnit so decode can scale
/// it back to a DateTime (our writes always use MICROSECOND).
const String timeUnitMeta = 'arrow.tsUnit';

/// Metadata key carrying a date column's Arrow DateUnit (DAY=0, MILLISECOND=1)
/// so decode uses the DECLARED width instead of guessing from buffer length.
const String dateUnitMeta = 'arrow.dateUnit';

/// Metadata key carrying a decimal column's Arrow bitWidth (default 128) so
/// decode uses the declared element width instead of `length ~/ n`.
const String decimalBitsMeta = 'arrow.decimalBits';

// BodyCompression CompressionType enum.
const int compressionLz4Frame = 0;
const int compressionZstd = 1;

/// A FieldNode: per-field (length, nullCount).
typedef NodeInfo = ({int length, int nullCount});

/// A Buffer: (offset, length) into the record batch body.
typedef BufferInfo = ({int offset, int length});

/// A Footer Block: (offset, metaDataLength, bodyLength).
typedef BlockInfo = ({int offset, int metaLength, int bodyLength});

// ---------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------

/// Build a Schema message (FlatBuffer) for [fields].
Uint8List buildSchemaMessage(List<Field> fields) {
  final builder = FbBuilder();
  final schema = _buildSchema(builder, fields);
  return builder.finish(_message(builder, headerSchema, schema, 0));
}

/// Build a RecordBatch message for [numRows] with the given field [nodes] and
/// [buffers]; [bodyLength] is the total (padded) size of the buffer body.
Uint8List buildRecordBatchMessage(
  int numRows,
  List<NodeInfo> nodes,
  List<BufferInfo> buffers,
  int bodyLength, {
  int? codec,
}) {
  final builder = FbBuilder();
  final nodeVector = builder.writeStructVector([
    for (final node in nodes) _structFieldNode(node),
  ], 8);
  final bufferVector = builder.writeStructVector([
    for (final buffer in buffers) _structBuffer(buffer),
  ], 8);
  int? compression;
  if (codec != null) {
    builder.startTable(2);
    builder.addUint8(0, codec); // CompressionType (LZ4_FRAME=0 default, ZSTD=1)
    // field 1 = method (BUFFER = 0, default, omitted)
    compression = builder.endTable();
  }
  builder.startTable(4);
  builder.addInt64(0, numRows);
  builder.addOffset(1, nodeVector);
  builder.addOffset(2, bufferVector);
  if (compression != null) builder.addOffset(3, compression);
  final batch = builder.endTable();
  return builder.finish(
    _message(builder, headerRecordBatch, batch, bodyLength),
  );
}

/// Build the IPC file Footer for [fields] and the record-batch [blocks].
Uint8List buildFooter(List<Field> fields, List<BlockInfo> blocks) {
  final builder = FbBuilder();
  final schema = _buildSchema(builder, fields);
  final vector = builder.writeStructVector([
    for (final block in blocks) _structBlock(block),
  ], 8);
  builder.startTable(5);
  builder.addInt16(0, metadataV5);
  builder.addOffset(1, schema);
  // field 2 = dictionaries (omitted)
  builder.addOffset(3, vector);
  return builder.finish(builder.endTable());
}

int _message(FbBuilder builder, int type, int header, int bodyLength) {
  builder.startTable(5);
  builder.addInt16(0, metadataV5);
  builder.addUint8(1, type);
  builder.addOffset(2, header);
  builder.addInt64(3, bodyLength);
  return builder.endTable();
}

int _buildSchema(FbBuilder builder, List<Field> fields) {
  final offsets = [for (final field in fields) _buildField(builder, field)];
  final vector = builder.writeOffsetVector(offsets);
  builder.startTable(4);
  // field 0 = endianness (Little = 0, default, omitted)
  builder.addOffset(1, vector);
  return builder.endTable();
}

int _buildField(FbBuilder builder, Field field) {
  final name = builder.writeString(field.name);
  // Children (built first, back-to-front) for nested types.
  int? children;
  if (field.children.isNotEmpty) {
    final offsets = [
      for (final child in field.children) _buildField(builder, child),
    ];
    children = builder.writeOffsetVector(offsets);
  }
  final (code, type) = _buildType(builder, field);
  builder.startTable(7);
  builder.addOffset(0, name);
  builder.addBool(1, field.nullable);
  builder.addUint8(2, code);
  builder.addOffset(3, type);
  // 4 dictionary
  if (children != null) builder.addOffset(5, children);
  // 6 custom_metadata (omitted)
  return builder.endTable();
}

/// Returns (Type union code, offset to the Type table). Decimal reads
/// precision/scale off [field]; date/timestamp are normalized (date=DAY/int32,
/// timestamp=MICROSECOND/int64).
(int, int) _buildType(FbBuilder builder, Field field) {
  switch (field.dataType) {
    case DataType.int8:
      return (_typeInt, _int(builder, 8));
    case DataType.int16:
      return (_typeInt, _int(builder, 16));
    case DataType.int32:
      return (_typeInt, _int(builder, 32));
    case DataType.int64:
      return (_typeInt, _int(builder, 64));
    case DataType.float32:
      return (_typeFloat, _float(builder, _precisionSingle));
    case DataType.float64:
      return (_typeFloat, _float(builder, _precisionDouble));
    case DataType.boolean:
      builder.startTable(0);
      return (_typeBool, builder.endTable());
    case DataType.string:
      builder.startTable(0);
      return (_typeUtf8, builder.endTable());
    case DataType.binary:
      builder.startTable(0);
      return (_typeBinary, builder.endTable());
    case DataType.null_:
      builder.startTable(0);
      return (_typeNull, builder.endTable());
    case DataType.date:
      return (_typeDate, _date(builder, _dateDay));
    case DataType.timestamp:
      return (_typeTimestamp, _timestamp(builder, _timeMicro));
    case DataType.decimal:
      return (
        _typeDecimal,
        _decimal(builder, field.precision ?? 38, field.scale ?? 0),
      );
    case DataType.list:
      builder.startTable(0);
      return (_typeList, builder.endTable());
    case DataType.struct:
      builder.startTable(0);
      return (_typeStruct, builder.endTable());
    case DataType.map:
      builder.startTable(1);
      builder.addBool(0, false); // keysSorted
      return (_typeMap, builder.endTable());
    default:
      throw Failure.of(
        Kind.unsupported,
        'Arrow IPC does not yet support type "${field.dataType.name}"',
      );
  }
}

int _int(FbBuilder builder, int bitWidth) {
  builder.startTable(2);
  builder.addInt32(0, bitWidth);
  builder.addBool(1, true); // signed
  return builder.endTable();
}

int _float(FbBuilder builder, int precision) {
  builder.startTable(1);
  builder.addInt16(0, precision);
  return builder.endTable();
}

int _date(FbBuilder builder, int unit) {
  builder.startTable(1);
  // A missing unit means MILLISECOND, so DAY must always be written.
  builder.addInt16(0, unit, _dateMilli);
  return builder.endTable();
}

int _timestamp(FbBuilder builder, int unit) {
  builder.startTable(2);
  builder.addInt16(0, unit);
  // field 1 = timezone (offset/string, omitted -> naive UTC)
  return builder.endTable();
}

int _decimal(FbBuilder builder, int precision, int scale) {
  builder.startTable(3);
  builder.addInt32(0, precision);
  builder.addInt32(1, scale);
  // field 2 = bitWidth (default 128, omitted)
  return builder.endTable();
}

Uint8List _structFieldNode(NodeInfo node) {
  final data = ByteData(16);
  data.setInt64(0, node.length, Endian.little);
  data.setInt64(8, node.nullCount, Endian.little);
  return data.buffer.asUint8List();
}

Uint8List _structBuffer(BufferInfo buffer) {
  final data = ByteData(16);
  data.setInt64(0, buffer.offset, Endian.little);
  data.setInt64(8, buffer.length, Endian.little);
  return data.buffer.asUint8List();
}

Uint8List _structBlock(BlockInfo block) {
  final data = ByteData(24);
  data.setInt64(0, block.offset, Endian.little);
  data.setInt32(8, block.metaLength, Endian.little);
  data.setInt64(16, block.bodyLength, Endian.little);
  return data.buffer.asUint8List();
}

// ---------------------------------------------------------------------------
// Parse
// ---------------------------------------------------------------------------

/// A parsed Message header: which kind + the table position of the header.
typedef ParsedMessage = ({int headerType, int headerTable, int bodyLength});

ParsedMessage parseMessage(FbReader reader) {
  final table = reader.root;
  return (
    headerType: reader.u8Field(table, 1, 0),
    headerTable: reader.offsetField(table, 2),
    bodyLength: reader.i64Field(table, 3, 0),
  );
}

/// Mutable budget shared across a whole schema parse: caps nesting depth (guards
/// the recursive parser against a StackOverflow) and the total field count
/// (guards against a wide schema allocating millions of `Field`s), BEFORE
/// allocation. The byte-level footer cap alone can't bound either within a legal
/// footer.
class _SchemaBudget {
  int fields = 0;
}

/// Parse a Schema table at [table] into libbigdata [Field]s.
List<Field> parseSchema(FbReader reader, int table) {
  // Field 0: endianness, Little = 0. Big-endian buffers would decode as
  // little-endian garbage.
  if (reader.i16Field(table, 0, 0) != 0) {
    throw Failure.of(
      Kind.unsupported,
      'Big-endian Arrow data is not supported',
    );
  }
  final n = reader.vectorLength(table, 1);
  final budget = _SchemaBudget();
  return [
    for (int i = 0; i < n; i++)
      _parseField(
        reader,
        reader.offsetVectorElement(table, 1, i),
        'col$i',
        budget,
        1,
      ),
  ];
}

/// Parse one Field table, recursing into children for nested types.
Field _parseField(
  FbReader reader,
  int table,
  String fallback,
  _SchemaBudget budget,
  int depth,
) {
  if (depth > defaultMaxSchemaDepth) {
    throw FormatException(
      'Arrow schema nesting exceeds depth $defaultMaxSchemaDepth',
    );
  }
  if (++budget.fields > defaultMaxSchemaFields) {
    throw FormatException(
      'Arrow schema declares more than $defaultMaxSchemaFields fields',
    );
  }
  final name = reader.stringField(table, 0) ?? fallback;
  final nullable = reader.boolField(table, 1, false);
  final code = reader.u8Field(table, 2, 0);
  final typeTable = reader.offsetField(table, 3);
  // Field 4: DictionaryEncoding. Its values arrive in dictionary batches,
  // which this reader does not decode.
  if (reader.offsetField(table, 4) != 0) {
    throw Failure.of(
      Kind.unsupported,
      'Arrow column $name is dictionary-encoded, which is not supported',
    );
  }
  final parsed = _parseType(reader, code, typeTable);
  final count = reader.vectorLength(table, 5);
  final children = [
    for (int i = 0; i < count; i++)
      _parseField(
        reader,
        reader.offsetVectorElement(table, 5, i),
        'child$i',
        budget,
        depth + 1,
      ),
  ];
  // Decoding indexes these children directly, so their shape is checked here.
  final shaped = switch (parsed.type) {
    DataType.list => children.length == 1,
    DataType.map =>
      children.length == 1 && children.single.children.length == 2,
    _ => true,
  };
  if (!shaped) {
    throw FormatException(
      'Arrow ${parsed.type.name} field $name has ${children.length} children',
    );
  }
  return Field(
    name: name,
    dataType: parsed.type,
    nullable: nullable,
    precision: parsed.precision,
    scale: parsed.scale,
    metadata: {
      if (parsed.timeUnit != null) timeUnitMeta: parsed.timeUnit,
      if (parsed.dateUnit != null) dateUnitMeta: parsed.dateUnit,
      if (parsed.decimalBits != null) decimalBitsMeta: parsed.decimalBits,
    },
    children: children,
  );
}

typedef _ParsedType = ({
  DataType type,
  int? precision,
  int? scale,
  int? timeUnit,
  int? dateUnit,
  int? decimalBits,
});

_ParsedType _scalar(DataType type) => (
  type: type,
  precision: null,
  scale: null,
  timeUnit: null,
  dateUnit: null,
  decimalBits: null,
);

_ParsedType _parseType(FbReader reader, int code, int table) {
  switch (code) {
    case _typeNull:
      return _scalar(DataType.null_);
    case _typeInt:
      final bits = table == 0 ? 32 : reader.i32Field(table, 0, 32);
      final signed = table == 0 ? true : reader.boolField(table, 1, false);
      if (!signed) {
        throw Failure.of(
          Kind.unsupported,
          'Arrow IPC v1 does not support unsigned ints',
        );
      }
      switch (bits) {
        case 8:
          return _scalar(DataType.int8);
        case 16:
          return _scalar(DataType.int16);
        case 32:
          return _scalar(DataType.int32);
        case 64:
          return _scalar(DataType.int64);
        default:
          throw FormatException('Arrow int bitWidth $bits');
      }
    case _typeFloat:
      final precision = table == 0
          ? _precisionDouble
          : reader.i16Field(table, 0, 0);
      if (precision == _precisionSingle) return _scalar(DataType.float32);
      if (precision == _precisionDouble) return _scalar(DataType.float64);
      if (precision != 0) {
        throw FormatException('Arrow float precision $precision');
      }
      throw Failure.of(
        Kind.unsupported,
        'Arrow float precision $precision (half unsupported)',
      );
    case _typeUtf8:
      return _scalar(DataType.string);
    case _typeBinary:
      return _scalar(DataType.binary);
    case _typeBool:
      return _scalar(DataType.boolean);
    case _typeDate:
      // unit (field 0) carried so decode picks 4- vs 8-byte elements from the
      // DECLARATION. A missing unit is null here, not the spec's MILLISECOND:
      // earlier versions of this library wrote DAY by omitting it, and decode
      // then tells the two apart by value width.
      final unit = table == 0 ? -1 : reader.i16Field(table, 0, -1);
      return (
        type: DataType.date,
        precision: null,
        scale: null,
        timeUnit: null,
        dateUnit: unit < 0 ? null : unit,
        decimalBits: null,
      );
    case _typeTimestamp:
      // unit (field 0) carried so decode can scale; missing means SECOND.
      final unit = table == 0
          ? _timeSecond
          : reader.i16Field(table, 0, _timeSecond);
      return (
        type: DataType.timestamp,
        precision: null,
        scale: null,
        timeUnit: unit,
        dateUnit: null,
        decimalBits: null,
      );
    case _typeDecimal:
      final (:precision, :scale) = checkedDecimal(
        table == 0 ? 38 : reader.i32Field(table, 0, 38),
        table == 0 ? 0 : reader.i32Field(table, 1, 0),
      );
      return (
        type: DataType.decimal,
        precision: precision,
        scale: scale,
        timeUnit: null,
        dateUnit: null,
        // bitWidth (field 2), default 128 per the spec.
        decimalBits: table == 0 ? 128 : reader.i32Field(table, 2, 128),
      );
    case _typeList:
      return _scalar(DataType.list);
    case _typeStruct:
      return _scalar(DataType.struct);
    case _typeMap:
      return _scalar(DataType.map);
    default:
      // 1 to 26 name real Arrow types this reader does not support; anything
      // else (0 is NONE) is not a type at all.
      if (code < 1 || code > 26) {
        throw FormatException('Invalid Arrow type code $code');
      }
      throw Failure.of(Kind.unsupported, 'Arrow type code $code unsupported');
  }
}

/// Parse a RecordBatch table at [table].
({int length, List<NodeInfo> nodes, List<BufferInfo> buffers, int? codec})
parseRecordBatch(FbReader reader, int table) {
  final length = reader.i64Field(table, 0, 0);
  final nodes = <NodeInfo>[];
  final nodeTotal = reader.vectorLength(table, 1);
  for (int i = 0; i < nodeTotal; i++) {
    final position = reader.structVectorElement(table, 1, i, 16);
    nodes.add((
      length: reader.i64(position),
      nullCount: reader.i64(position + 8),
    ));
  }
  final buffers = <BufferInfo>[];
  final bufferTotal = reader.vectorLength(table, 2);
  for (int i = 0; i < bufferTotal; i++) {
    final position = reader.structVectorElement(table, 2, i, 16);
    buffers.add((
      offset: reader.i64(position),
      length: reader.i64(position + 8),
    ));
  }
  final compression = reader.offsetField(table, 3);
  final codec = compression == 0
      ? null
      : reader.u8Field(compression, 0, 0); // default 0 = LZ4_FRAME
  return (length: length, nodes: nodes, buffers: buffers, codec: codec);
}

/// Parse the IPC Footer.
({List<Field> fields, List<BlockInfo> batches}) parseFooter(FbReader reader) {
  final footer = reader.root;
  final schemaTable = reader.offsetField(footer, 1);
  final fields = parseSchema(reader, schemaTable);
  final batches = <BlockInfo>[];
  final count = reader.vectorLength(footer, 3);
  for (int i = 0; i < count; i++) {
    final position = reader.structVectorElement(footer, 3, i, 24);
    batches.add((
      offset: reader.i64(position),
      metaLength: reader.i32(position + 8),
      bodyLength: reader.i64(position + 16),
    ));
  }
  return (fields: fields, batches: batches);
}
