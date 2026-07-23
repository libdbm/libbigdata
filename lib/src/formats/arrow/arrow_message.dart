import 'dart:typed_data';

import '../../core/data_type.dart';
import '../../core/limits.dart';
import '../../core/schema.dart';
import 'flatbuffers.dart';

/// Builds and parses the Apache Arrow IPC metadata messages (FlatBuffer encoded)
/// per the Arrow `Message.fbs`/`Schema.fbs`/`File.fbs` schemas. Core types only.

// MetadataVersion.V5
const int metadataV5 = 4;

// MessageHeader union codes (0 = NONE).
const int headerSchema = 1;
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
const int _precSingle = 1;
const int _precDouble = 2;

// DateUnit: DAY=0, MILLISECOND=1.
const int _dateDay = 0;
// TimeUnit: SECOND=0, MILLISECOND=1, MICROSECOND=2, NANOSECOND=3.
const int _timeMicro = 2;

/// Metadata key carrying a timestamp column's Arrow TimeUnit so decode can scale
/// it back to a DateTime (our writes always use MICROSECOND).
const String tsUnitMeta = 'arrow.tsUnit';

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
  final b = FbBuilder();
  final schema = _buildSchema(b, fields);
  return b.finish(_message(b, headerSchema, schema, 0));
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
  final b = FbBuilder();
  final nodesVec = b.writeStructVector([
    for (final n in nodes) _structFieldNode(n),
  ], 8);
  final buffersVec = b.writeStructVector([
    for (final bf in buffers) _structBuffer(bf),
  ], 8);
  int? compression;
  if (codec != null) {
    b.startTable(2);
    b.addUint8(0, codec); // CompressionType (LZ4_FRAME=0 default, ZSTD=1)
    // field 1 = method (BUFFER = 0, default, omitted)
    compression = b.endTable();
  }
  b.startTable(4);
  b.addInt64(0, numRows);
  b.addOffset(1, nodesVec);
  b.addOffset(2, buffersVec);
  if (compression != null) b.addOffset(3, compression);
  final rb = b.endTable();
  return b.finish(_message(b, headerRecordBatch, rb, bodyLength));
}

/// Build the IPC file Footer for [fields] and the record-batch [blocks].
Uint8List buildFooter(List<Field> fields, List<BlockInfo> blocks) {
  final b = FbBuilder();
  final schema = _buildSchema(b, fields);
  final batchesVec = b.writeStructVector([
    for (final bk in blocks) _structBlock(bk),
  ], 8);
  b.startTable(5);
  b.addInt16(0, metadataV5);
  b.addOffset(1, schema);
  // field 2 = dictionaries (omitted)
  b.addOffset(3, batchesVec);
  return b.finish(b.endTable());
}

int _message(FbBuilder b, int headerType, int headerOffset, int bodyLength) {
  b.startTable(5);
  b.addInt16(0, metadataV5);
  b.addUint8(1, headerType);
  b.addOffset(2, headerOffset);
  b.addInt64(3, bodyLength);
  return b.endTable();
}

int _buildSchema(FbBuilder b, List<Field> fields) {
  final fieldOffsets = [for (final f in fields) _buildField(b, f)];
  final vec = b.writeOffsetVector(fieldOffsets);
  b.startTable(4);
  // field 0 = endianness (Little = 0, default, omitted)
  b.addOffset(1, vec);
  return b.endTable();
}

int _buildField(FbBuilder b, Field f) {
  final nameOffset = b.writeString(f.name);
  // Children (built first, back-to-front) for nested types.
  int? childrenVec;
  if (f.children.isNotEmpty) {
    final childOffsets = [for (final c in f.children) _buildField(b, c)];
    childrenVec = b.writeOffsetVector(childOffsets);
  }
  final (typeCode, typeOffset) = _buildType(b, f);
  b.startTable(7);
  b.addOffset(0, nameOffset);
  b.addBool(1, f.nullable);
  b.addUint8(2, typeCode);
  b.addOffset(3, typeOffset);
  // 4 dictionary
  if (childrenVec != null) b.addOffset(5, childrenVec);
  // 6 custom_metadata (omitted)
  return b.endTable();
}

/// Returns (Type union code, offset to the Type table). Decimal reads
/// precision/scale off [f]; date/timestamp are normalized (date=DAY/int32,
/// timestamp=MICROSECOND/int64).
(int, int) _buildType(FbBuilder b, Field f) {
  switch (f.dataType) {
    case DataType.int8:
      return (_typeInt, _int(b, 8));
    case DataType.int16:
      return (_typeInt, _int(b, 16));
    case DataType.int32:
      return (_typeInt, _int(b, 32));
    case DataType.int64:
      return (_typeInt, _int(b, 64));
    case DataType.float32:
      return (_typeFloat, _float(b, _precSingle));
    case DataType.float64:
      return (_typeFloat, _float(b, _precDouble));
    case DataType.boolean:
      b.startTable(0);
      return (_typeBool, b.endTable());
    case DataType.string:
      b.startTable(0);
      return (_typeUtf8, b.endTable());
    case DataType.binary:
      b.startTable(0);
      return (_typeBinary, b.endTable());
    case DataType.null_:
      b.startTable(0);
      return (_typeNull, b.endTable());
    case DataType.date:
      return (_typeDate, _date(b, _dateDay));
    case DataType.timestamp:
      return (_typeTimestamp, _timestamp(b, _timeMicro));
    case DataType.decimal:
      return (_typeDecimal, _decimal(b, f.precision ?? 38, f.scale ?? 0));
    case DataType.list:
      b.startTable(0);
      return (_typeList, b.endTable());
    case DataType.struct:
      b.startTable(0);
      return (_typeStruct, b.endTable());
    case DataType.map:
      b.startTable(1);
      b.addBool(0, false); // keysSorted
      return (_typeMap, b.endTable());
    default:
      throw UnsupportedError(
        'Arrow IPC does not yet support type "${f.dataType.name}"',
      );
  }
}

int _int(FbBuilder b, int bitWidth) {
  b.startTable(2);
  b.addInt32(0, bitWidth);
  b.addBool(1, true); // signed
  return b.endTable();
}

int _float(FbBuilder b, int precision) {
  b.startTable(1);
  b.addInt16(0, precision);
  return b.endTable();
}

int _date(FbBuilder b, int unit) {
  b.startTable(1);
  b.addInt16(0, unit);
  return b.endTable();
}

int _timestamp(FbBuilder b, int unit) {
  b.startTable(2);
  b.addInt16(0, unit);
  // field 1 = timezone (offset/string, omitted -> naive UTC)
  return b.endTable();
}

int _decimal(FbBuilder b, int precision, int scale) {
  b.startTable(3);
  b.addInt32(0, precision);
  b.addInt32(1, scale);
  // field 2 = bitWidth (default 128, omitted)
  return b.endTable();
}

Uint8List _structFieldNode(NodeInfo n) {
  final bd = ByteData(16);
  bd.setInt64(0, n.length, Endian.little);
  bd.setInt64(8, n.nullCount, Endian.little);
  return bd.buffer.asUint8List();
}

Uint8List _structBuffer(BufferInfo bf) {
  final bd = ByteData(16);
  bd.setInt64(0, bf.offset, Endian.little);
  bd.setInt64(8, bf.length, Endian.little);
  return bd.buffer.asUint8List();
}

Uint8List _structBlock(BlockInfo bk) {
  final bd = ByteData(24);
  bd.setInt64(0, bk.offset, Endian.little);
  bd.setInt32(8, bk.metaLength, Endian.little);
  bd.setInt64(16, bk.bodyLength, Endian.little);
  return bd.buffer.asUint8List();
}

// ---------------------------------------------------------------------------
// Parse
// ---------------------------------------------------------------------------

/// A parsed Message header: which kind + the table position of the header.
typedef ParsedMessage = ({int headerType, int headerTable, int bodyLength});

ParsedMessage parseMessage(FbReader r) {
  final m = r.root;
  return (
    headerType: r.u8Field(m, 1, 0),
    headerTable: r.offsetField(m, 2),
    bodyLength: r.i64Field(m, 3, 0),
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

/// Parse a Schema table at [schemaTable] into libbigdata [Field]s.
List<Field> parseSchema(FbReader r, int schemaTable) {
  final n = r.vectorLength(schemaTable, 1);
  final budget = _SchemaBudget();
  return [
    for (int i = 0; i < n; i++)
      _parseField(
        r,
        r.offsetVectorElement(schemaTable, 1, i),
        'col$i',
        budget,
        1,
      ),
  ];
}

/// Parse one Field table, recursing into children for nested types.
Field _parseField(
  FbReader r,
  int ft,
  String fallbackName,
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
  final name = r.stringField(ft, 0) ?? fallbackName;
  final nullable = r.boolField(ft, 1, false);
  final typeCode = r.u8Field(ft, 2, 0);
  final typeTable = r.offsetField(ft, 3);
  final parsed = _parseType(r, typeCode, typeTable);
  final childCount = r.vectorLength(ft, 5);
  final children = [
    for (int i = 0; i < childCount; i++)
      _parseField(
        r,
        r.offsetVectorElement(ft, 5, i),
        'child$i',
        budget,
        depth + 1,
      ),
  ];
  return Field(
    name: name,
    dataType: parsed.type,
    nullable: nullable,
    precision: parsed.precision,
    scale: parsed.scale,
    metadata: {
      if (parsed.tsUnit != null) tsUnitMeta: parsed.tsUnit,
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
  int? tsUnit,
  int? dateUnit,
  int? decimalBits,
});

_ParsedType _scalar(DataType t) => (
  type: t,
  precision: null,
  scale: null,
  tsUnit: null,
  dateUnit: null,
  decimalBits: null,
);

_ParsedType _parseType(FbReader r, int code, int typeTable) {
  switch (code) {
    case _typeNull:
      return _scalar(DataType.null_);
    case _typeInt:
      final bits = typeTable == 0 ? 32 : r.i32Field(typeTable, 0, 32);
      final signed = typeTable == 0 ? true : r.boolField(typeTable, 1, false);
      if (!signed) {
        throw UnsupportedError('Arrow IPC v1 does not support unsigned ints');
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
          throw UnsupportedError('Arrow int bitWidth $bits');
      }
    case _typeFloat:
      final prec = typeTable == 0 ? _precDouble : r.i16Field(typeTable, 0, 0);
      if (prec == _precSingle) return _scalar(DataType.float32);
      if (prec == _precDouble) return _scalar(DataType.float64);
      throw UnsupportedError('Arrow float precision $prec (half unsupported)');
    case _typeUtf8:
      return _scalar(DataType.string);
    case _typeBinary:
      return _scalar(DataType.binary);
    case _typeBool:
      return _scalar(DataType.boolean);
    case _typeDate:
      // unit (field 0) carried so decode picks 4- vs 8-byte elements from the
      // DECLARATION, not a byte-length division. Default DAY.
      return (
        type: DataType.date,
        precision: null,
        scale: null,
        tsUnit: null,
        dateUnit: typeTable == 0
            ? _dateDay
            : r.i16Field(typeTable, 0, _dateDay),
        decimalBits: null,
      );
    case _typeTimestamp:
      // unit (field 0) carried so decode can scale; default MICROSECOND.
      final unit = typeTable == 0
          ? _timeMicro
          : r.i16Field(typeTable, 0, _timeMicro);
      return (
        type: DataType.timestamp,
        precision: null,
        scale: null,
        tsUnit: unit,
        dateUnit: null,
        decimalBits: null,
      );
    case _typeDecimal:
      final precision = typeTable == 0 ? 38 : r.i32Field(typeTable, 0, 38);
      final scale = typeTable == 0 ? 0 : r.i32Field(typeTable, 1, 0);
      return (
        type: DataType.decimal,
        precision: precision,
        scale: scale,
        tsUnit: null,
        dateUnit: null,
        // bitWidth (field 2), default 128 per the spec.
        decimalBits: typeTable == 0 ? 128 : r.i32Field(typeTable, 2, 128),
      );
    case _typeList:
      return _scalar(DataType.list);
    case _typeStruct:
      return _scalar(DataType.struct);
    case _typeMap:
      return _scalar(DataType.map);
    default:
      throw UnsupportedError('Arrow type code $code unsupported');
  }
}

/// Parse a RecordBatch table at [rbTable].
({int length, List<NodeInfo> nodes, List<BufferInfo> buffers, int? codec})
parseRecordBatch(FbReader r, int rbTable) {
  final length = r.i64Field(rbTable, 0, 0);
  final nodes = <NodeInfo>[];
  final nn = r.vectorLength(rbTable, 1);
  for (int i = 0; i < nn; i++) {
    final p = r.structVectorElement(rbTable, 1, i, 16);
    nodes.add((length: r.i64(p), nullCount: r.i64(p + 8)));
  }
  final buffers = <BufferInfo>[];
  final bn = r.vectorLength(rbTable, 2);
  for (int i = 0; i < bn; i++) {
    final p = r.structVectorElement(rbTable, 2, i, 16);
    buffers.add((offset: r.i64(p), length: r.i64(p + 8)));
  }
  final comp = r.offsetField(rbTable, 3);
  final codec = comp == 0
      ? null
      : r.u8Field(comp, 0, 0); // default 0 = LZ4_FRAME
  return (length: length, nodes: nodes, buffers: buffers, codec: codec);
}

/// Parse the IPC Footer.
({List<Field> fields, List<BlockInfo> batches}) parseFooter(FbReader r) {
  final f = r.root;
  final schemaTable = r.offsetField(f, 1);
  final fields = parseSchema(r, schemaTable);
  final batches = <BlockInfo>[];
  final n = r.vectorLength(f, 3);
  for (int i = 0; i < n; i++) {
    final p = r.structVectorElement(f, 3, i, 24);
    batches.add((
      offset: r.i64(p),
      metaLength: r.i32(p + 8),
      bodyLength: r.i64(p + 16),
    ));
  }
  return (fields: fields, batches: batches);
}
