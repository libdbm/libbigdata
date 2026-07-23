import 'dart:convert';
import 'dart:typed_data';

import '../../core/column.dart';
import '../../core/conversions.dart';
import '../../core/data_type.dart';
import '../../core/schema.dart';
import 'arrow_message.dart';

/// Maps libbigdata [Column]s to/from Apache Arrow record-batch buffers.
///
/// Arrow buffer order per field: primitive/bool = [validity, data]; utf8/binary
/// = [validity, offsets(int32, n+1), data]. Validity bit SET = valid (the
/// inverse of libbigdata's `NumericColumn.nullBitmap`, where SET = null), and is
/// emitted as a length-0 buffer when the field has no nulls. All body buffers
/// are padded to 8 bytes.

/// Number of Arrow buffers a single node of [type] contributes (children are
/// counted separately by the recursive walk): null=0, struct=1 (validity only),
/// list/map=2 (validity + offsets), string/binary=3, other scalars=2.
int bufferCount(DataType type) {
  if (type == DataType.null_) return 0;
  if (type == DataType.struct) return 1;
  if (type == DataType.string || type == DataType.binary) return 3;
  return 2;
}

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

/// Encode [column] into its Arrow node(s) + buffers in depth-first order. Scalars
/// produce one node; nested types (list/struct/map) produce this node followed
/// by their children's nodes/buffers. The caller assembles the buffers into the
/// body (optionally compressing each).
({List<NodeInfo> nodes, List<Uint8List> buffers}) encodeColumn(
  Column column,
  Field field,
) {
  final type = field.dataType;
  final n = column.length;
  // Null type: no buffers at all (not even validity).
  if (type == DataType.null_) {
    return (nodes: [(length: n, nullCount: n)], buffers: const []);
  }
  final nullCount = _nullCount(column, n);
  final node = (length: n, nullCount: nullCount);
  final validity = nullCount == 0 ? Uint8List(0) : _validity(column, n);

  // ---- nested ----
  if (type == DataType.list || type == DataType.map) {
    if (field.children.isEmpty) {
      throw ArgumentError(
        'Nested ${type.name} field "${field.name}" '
        'requires a child field describing its element',
      );
    }
    final childField = field.children.first;
    final offsets = Int32List(n + 1);
    final childValues = <dynamic>[];
    var offset = 0;
    for (int i = 0; i < n; i++) {
      final v = column[i];
      if (v != null) {
        final elems = _listElements(field, v);
        childValues.addAll(elems);
        offset += elems.length;
      }
      offsets[i + 1] = offset;
    }
    final childEnc = encodeColumn(
      ListColumn(
        name: childField.name,
        dataType: childField.dataType,
        data: childValues,
        nullable: childField.nullable,
      ),
      childField,
    );
    return (
      nodes: [node, ...childEnc.nodes],
      buffers: [
        validity,
        offsets.buffer.asUint8List(0, (n + 1) * 4),
        ...childEnc.buffers,
      ],
    );
  }
  if (type == DataType.struct) {
    final nodes = <NodeInfo>[node];
    final buffers = <Uint8List>[validity];
    for (final cf in field.children) {
      final memberValues = [
        for (int i = 0; i < n; i++)
          column[i] == null ? null : (column[i] as Map)[cf.name],
      ];
      final e = encodeColumn(
        ListColumn(
          name: cf.name,
          dataType: cf.dataType,
          data: memberValues,
          nullable: cf.nullable,
        ),
        cf,
      );
      nodes.addAll(e.nodes);
      buffers.addAll(e.buffers);
    }
    return (nodes: nodes, buffers: buffers);
  }

  // ---- scalar leaf ----
  final buffers = <Uint8List>[validity];
  if (type == DataType.string || type == DataType.binary) {
    final offsets = Int32List(n + 1);
    final data = BytesBuilder();
    var offset = 0;
    for (int i = 0; i < n; i++) {
      final v = column[i];
      if (v != null) {
        final bytes = type == DataType.string
            ? utf8.encode(v as String)
            : Uint8List.fromList((v as List).cast<int>());
        data.add(bytes);
        offset += bytes.length;
      }
      offsets[i + 1] = offset;
    }
    buffers.add(offsets.buffer.asUint8List(0, (n + 1) * 4));
    buffers.add(data.toBytes());
  } else if (type == DataType.boolean) {
    buffers.add(_boolData(column, n));
  } else if (type == DataType.date) {
    final out = Int32List(n);
    for (int i = 0; i < n; i++) {
      final v = column[i];
      if (v != null) out[i] = dateToEpochDays(v as DateTime);
    }
    buffers.add(out.buffer.asUint8List(0, n * 4));
  } else if (type == DataType.timestamp) {
    final out = Int64List(n);
    for (int i = 0; i < n; i++) {
      final v = column[i];
      if (v != null) out[i] = timestampToEpochMicros(v as DateTime);
    }
    buffers.add(out.buffer.asUint8List(0, n * 8));
  } else if (type == DataType.decimal) {
    final out = Uint8List(n * 16);
    for (int i = 0; i < n; i++) {
      final v = column[i];
      if (v != null) {
        out.setRange(
          i * 16,
          i * 16 + 16,
          bigIntToBytes(v as BigInt, 16, littleEndian: true),
        );
      }
    }
    buffers.add(out);
  } else {
    buffers.add(_numericData(column, type, n));
  }
  return (nodes: [node], buffers: buffers);
}

/// The Arrow child elements for one nested value: a list's items, or a map's
/// entries materialized as {keyName: k, valueName: v} structs.
List<dynamic> _listElements(Field field, Object value) {
  if (field.dataType == DataType.list) return value as List;
  // map: child is the entries struct (children [key, value]).
  final entries = field.children.first;
  final keyName = entries.children[0].name;
  final valueName = entries.children[1].name;
  return [
    for (final e in (value as Map).entries)
      {keyName: e.key, valueName: e.value},
  ];
}

int _nullCount(Column column, int n) {
  if (!column.hasNulls) return 0;
  var c = 0;
  for (int i = 0; i < n; i++) {
    if (column[i] == null) c++;
  }
  return c;
}

/// Arrow validity bitmap (bit SET = valid).
Uint8List _validity(Column column, int n) {
  final bytes = Uint8List((n + 7) >> 3);
  for (int i = 0; i < n; i++) {
    if (column[i] != null) bytes[i >> 3] |= 1 << (i & 7);
  }
  return bytes;
}

Uint8List _boolData(Column column, int n) {
  final bytes = Uint8List((n + 7) >> 3);
  for (int i = 0; i < n; i++) {
    if (column[i] == true) bytes[i >> 3] |= 1 << (i & 7);
  }
  return bytes;
}

Uint8List _numericData(Column column, DataType type, int n) {
  // Fast path: a NumericColumn whose typed buffer already matches the width.
  if (column is NumericColumn) {
    final b = column.rawBuffer;
    final ok =
        (type == DataType.int8 && b is Int8List) ||
        (type == DataType.int32 && b is Int32List) ||
        (type == DataType.int64 && b is Int64List) ||
        (type == DataType.float32 && b is Float32List) ||
        (type == DataType.float64 && b is Float64List);
    if (ok) return b.buffer.asUint8List(b.offsetInBytes, b.lengthInBytes);
  }
  switch (type) {
    case DataType.int8:
      final out = Int8List(n);
      for (int i = 0; i < n; i++) {
        final v = column[i];
        if (v != null) out[i] = v as int;
      }
      return out.buffer.asUint8List(0, n);
    case DataType.int16:
      final out = Int16List(n);
      for (int i = 0; i < n; i++) {
        final v = column[i];
        if (v != null) out[i] = v as int;
      }
      return out.buffer.asUint8List(0, n * 2);
    case DataType.int32:
      final out = Int32List(n);
      for (int i = 0; i < n; i++) {
        final v = column[i];
        if (v != null) out[i] = v as int;
      }
      return out.buffer.asUint8List(0, n * 4);
    case DataType.int64:
      final out = Int64List(n);
      for (int i = 0; i < n; i++) {
        final v = column[i];
        if (v != null) out[i] = v as int;
      }
      return out.buffer.asUint8List(0, n * 8);
    case DataType.float32:
      final out = Float32List(n);
      for (int i = 0; i < n; i++) {
        final v = column[i];
        if (v != null) out[i] = (v as num).toDouble();
      }
      return out.buffer.asUint8List(0, n * 4);
    case DataType.float64:
      final out = Float64List(n);
      for (int i = 0; i < n; i++) {
        final v = column[i];
        if (v != null) out[i] = (v as num).toDouble();
      }
      return out.buffer.asUint8List(0, n * 8);
    default:
      throw UnsupportedError('Arrow numeric encode for ${type.name}');
  }
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

/// Build a [Column] from its Arrow buffers. [fieldBuffers] are this field's
/// buffer slices (validity first), [body] is the whole record-batch body.
Column decodeColumn(
  Field field,
  NodeInfo node,
  List<BufferInfo> fieldBuffers,
  Uint8List body,
) {
  final type = field.dataType;
  final n = node.length;
  // Null type carries no buffers.
  if (type == DataType.null_) {
    return ListColumn(
      name: field.name,
      dataType: type,
      data: List<dynamic>.filled(n, null),
      nullable: true,
    );
  }
  final validity = fieldBuffers[0];
  final isValid = _validityReader(body, validity, node.nullCount, n);
  final bitmap = _nullBitmap(isValid, node.nullCount, n);

  switch (type) {
    case DataType.int8:
      return NumericColumn.adopt(
        name: field.name,
        dataType: type,
        data: _viewInt8(body, fieldBuffers[1], n),
        nullBitmap: bitmap,
      );
    case DataType.int32:
      return NumericColumn.adopt(
        name: field.name,
        dataType: type,
        data: _viewInt32(body, fieldBuffers[1], n),
        nullBitmap: bitmap,
      );
    case DataType.int64:
      return NumericColumn.adopt(
        name: field.name,
        dataType: type,
        data: _viewInt64(body, fieldBuffers[1], n),
        nullBitmap: bitmap,
      );
    case DataType.float32:
      return NumericColumn.adopt(
        name: field.name,
        dataType: type,
        data: _viewFloat32(body, fieldBuffers[1], n),
        nullBitmap: bitmap,
      );
    case DataType.float64:
      return NumericColumn.adopt(
        name: field.name,
        dataType: type,
        data: _viewFloat64(body, fieldBuffers[1], n),
        nullBitmap: bitmap,
      );
    case DataType.int16:
      final bd = _byteData(body, fieldBuffers[1]);
      final data = List<int?>.filled(n, null);
      for (int i = 0; i < n; i++) {
        if (isValid(i)) data[i] = bd.getInt16(i * 2, Endian.little);
      }
      return ListColumn<int>(
        name: field.name,
        dataType: type,
        data: data,
        nullable: node.nullCount > 0,
      );
    case DataType.boolean:
      final bd = _byteData(body, fieldBuffers[1]);
      final data = List<bool?>.filled(n, null);
      for (int i = 0; i < n; i++) {
        if (isValid(i)) {
          data[i] = (bd.getUint8(i >> 3) & (1 << (i & 7))) != 0;
        }
      }
      return ListColumn<bool>(
        name: field.name,
        dataType: type,
        data: data,
        nullable: node.nullCount > 0,
      );
    case DataType.string:
    case DataType.binary:
      final offsets = _byteData(body, fieldBuffers[1]);
      final dataBuf = fieldBuffers[2];
      if (offsets.lengthInBytes < (n + 1) * 4) {
        throw FormatException('Arrow ${field.name}: offsets buffer too short');
      }
      final data = List<dynamic>.filled(n, null);
      for (int i = 0; i < n; i++) {
        if (!isValid(i)) continue;
        final start = offsets.getInt32(i * 4, Endian.little);
        final end = offsets.getInt32((i + 1) * 4, Endian.little);
        if (start < 0 || end < start || dataBuf.offset + end > body.length) {
          throw FormatException(
            'Arrow ${field.name}: invalid byte offsets [$start, $end)',
          );
        }
        final bytes = body.sublist(
          dataBuf.offset + start,
          dataBuf.offset + end,
        );
        data[i] = type == DataType.string ? utf8.decode(bytes) : bytes;
      }
      return ListColumn(
        name: field.name,
        dataType: type,
        data: data,
        nullable: node.nullCount > 0,
      );
    case DataType.date:
      final buffer = fieldBuffers[1];
      // date32 (days) vs date64 (ms) from the DECLARED DateUnit — inferring it
      // from `length ~/ n` misclassified a padded buffer. Old readers of our
      // own files never hit the fallback (we write DAY and the unit is now
      // always parsed); the heuristic remains only for a unit-less schema.
      final unit = field.metadata[dateUnitMeta] as int?;
      final wide = unit != null ? unit == 1 : n > 0 && buffer.length ~/ n == 8;
      final bd = _byteData(body, buffer);
      final data = List<DateTime?>.filled(n, null);
      for (int i = 0; i < n; i++) {
        if (!isValid(i)) continue;
        data[i] = wide
            ? timestampFromEpochMillis(bd.getInt64(i * 8, Endian.little))
            : dateFromEpochDays(bd.getInt32(i * 4, Endian.little));
      }
      return ListColumn<DateTime>(
        name: field.name,
        dataType: type,
        data: data,
        nullable: node.nullCount > 0,
      );
    case DataType.timestamp:
      final unit = field.metadata[tsUnitMeta] as int? ?? 2; // default MICRO
      final bd = _byteData(body, fieldBuffers[1]);
      final data = List<DateTime?>.filled(n, null);
      for (int i = 0; i < n; i++) {
        if (!isValid(i)) continue;
        final raw = bd.getInt64(i * 8, Endian.little);
        data[i] = switch (unit) {
          0 => timestampFromEpochMicros(raw * 1000000), // SECOND
          1 => timestampFromEpochMillis(raw), // MILLISECOND
          3 => timestampFromEpochMicros(raw ~/ 1000), // NANOSECOND
          _ => timestampFromEpochMicros(raw), // MICROSECOND
        };
      }
      return ListColumn<DateTime>(
        name: field.name,
        dataType: type,
        data: data,
        nullable: node.nullCount > 0,
      );
    case DataType.decimal:
      final buffer = fieldBuffers[1];
      // Element width from the DECLARED bitWidth (default 128), not a byte-
      // length division that a padded buffer would skew.
      final bits = field.metadata[decimalBitsMeta] as int?;
      final width = bits != null
          ? bits ~/ 8
          : (n > 0 ? buffer.length ~/ n : 16);
      final data = List<BigInt?>.filled(n, null);
      for (int i = 0; i < n; i++) {
        if (!isValid(i)) continue;
        final bytes = body.sublist(
          buffer.offset + i * width,
          buffer.offset + i * width + width,
        );
        data[i] = bytesToBigInt(bytes, littleEndian: true);
      }
      return ListColumn<BigInt>(
        name: field.name,
        dataType: type,
        data: data,
        nullable: node.nullCount > 0,
      );
    default:
      throw UnsupportedError('Arrow decode for ${type.name}');
  }
}

/// A depth-first position into a record batch's node + buffer vectors.
class ArrowCursor {
  int node = 0;
  int buffer = 0;
}

/// Advance [cur] past one field subtree WITHOUT decoding it — for projection
/// pushdown, so an unselected column's buffers are never materialized. Must
/// consume exactly the nodes/buffers [decodeTree] would (incl. the `null_`
/// early-return and the nested-child recursion) or every later column desyncs.
void skipTree(Field field, ArrowCursor cur) {
  final type = field.dataType;
  cur.node++;
  if (type == DataType.null_) return; // 1 node, 0 buffers
  cur.buffer += bufferCount(type);
  if (type == DataType.list || type == DataType.map) {
    skipTree(field.children.first, cur);
  } else if (type == DataType.struct) {
    for (final cf in field.children) {
      skipTree(cf, cur);
    }
  }
}

/// Decode one field subtree (recursing into nested children), advancing [cur]
/// over the shared node/buffer vectors. Scalars delegate to [decodeColumn].
Column decodeTree(
  Field field,
  List<NodeInfo> nodes,
  List<BufferInfo> buffers,
  Uint8List body,
  ArrowCursor cur,
) {
  final type = field.dataType;
  final node = nodes[cur.node++];
  final n = node.length;
  if (type == DataType.null_) {
    return ListColumn(
      name: field.name,
      dataType: type,
      data: List<dynamic>.filled(n, null),
      nullable: true,
    );
  }
  final selfCount = bufferCount(type);
  final selfBufs = buffers.sublist(cur.buffer, cur.buffer + selfCount);
  cur.buffer += selfCount;

  if (type == DataType.list || type == DataType.map) {
    final isValid = _validityReader(body, selfBufs[0], node.nullCount, n);
    final offsets = _byteData(body, selfBufs[1]);
    final childList = decodeTree(
      field.children.first,
      nodes,
      buffers,
      body,
      cur,
    ).toList();
    if (offsets.lengthInBytes < (n + 1) * 4) {
      throw FormatException('Arrow ${field.name}: offsets buffer too short');
    }
    final data = List<dynamic>.filled(n, null);
    for (int i = 0; i < n; i++) {
      if (!isValid(i)) continue;
      final s = offsets.getInt32(i * 4, Endian.little);
      final e = offsets.getInt32((i + 1) * 4, Endian.little);
      if (s < 0 || e < s || e > childList.length) {
        throw FormatException(
          'Arrow ${field.name}: invalid list offsets [$s, $e)',
        );
      }
      final slice = childList.sublist(s, e);
      data[i] = type == DataType.list ? slice : _toMap(field, slice);
    }
    return ListColumn(
      name: field.name,
      dataType: type,
      data: data,
      nullable: node.nullCount > 0,
    );
  }

  if (type == DataType.struct) {
    final isValid = _validityReader(body, selfBufs[0], node.nullCount, n);
    final childLists = [
      for (final cf in field.children)
        decodeTree(cf, nodes, buffers, body, cur).toList(),
    ];
    final names = [for (final cf in field.children) cf.name];
    final data = List<dynamic>.filled(n, null);
    for (int i = 0; i < n; i++) {
      if (!isValid(i)) continue;
      data[i] = {
        for (int j = 0; j < names.length; j++) names[j]: childLists[j][i],
      };
    }
    return ListColumn(
      name: field.name,
      dataType: type,
      data: data,
      nullable: node.nullCount > 0,
    );
  }

  return decodeColumn(field, node, selfBufs, body);
}

Map<dynamic, dynamic> _toMap(Field mapField, List<dynamic> entries) {
  final e = mapField.children.first;
  final keyName = e.children[0].name;
  final valueName = e.children[1].name;
  return {for (final m in entries) (m as Map)[keyName]: m[valueName]};
}

bool Function(int) _validityReader(
  Uint8List body,
  BufferInfo validity,
  int nullCount,
  int n,
) {
  if (nullCount == 0 || validity.length == 0) return (_) => true;
  final bd = _byteData(body, validity);
  return (i) => (bd.getUint8(i >> 3) & (1 << (i & 7))) != 0;
}

/// libbigdata null bitmap (bit SET = null) from Arrow validity, or null.
Uint8List? _nullBitmap(bool Function(int) isValid, int nullCount, int n) {
  if (nullCount == 0) return null;
  final out = Uint8List((n + 7) >> 3);
  for (int i = 0; i < n; i++) {
    if (!isValid(i)) out[i >> 3] |= 1 << (i & 7);
  }
  return out;
}

ByteData _byteData(Uint8List body, BufferInfo b) =>
    ByteData.view(body.buffer, body.offsetInBytes + b.offset, b.length);

/// Reject a data buffer whose DECLARED length is shorter than the node's
/// element count needs: the typed views below read `n * width` bytes from the
/// buffer's offset regardless, so a corrupt batch that under-declares a
/// buffer would silently fabricate values from the NEIGHBORING buffer's bytes.
void _needBytes(BufferInfo b, int n, int width) {
  if (b.length < n * width) {
    throw FormatException(
      'Arrow record batch buffer holds ${b.length} bytes but the field node '
      'declares $n ${width * 8}-bit values (${n * width} bytes) — corrupt '
      'batch',
    );
  }
}

// Typed numeric views (zero-copy when aligned, copy otherwise).
Int8List _viewInt8(Uint8List body, BufferInfo b, int n) {
  _needBytes(b, n, 1);
  return Int8List.sublistView(body, b.offset, b.offset + n);
}

Int32List _viewInt32(Uint8List body, BufferInfo b, int n) {
  _needBytes(b, n, 4);
  final base = body.offsetInBytes + b.offset;
  if (base % 4 == 0) return body.buffer.asInt32List(base, n);
  final bd = _byteData(body, b);
  final out = Int32List(n);
  for (int i = 0; i < n; i++) {
    out[i] = bd.getInt32(i * 4, Endian.little);
  }
  return out;
}

Int64List _viewInt64(Uint8List body, BufferInfo b, int n) {
  _needBytes(b, n, 8);
  final base = body.offsetInBytes + b.offset;
  if (base % 8 == 0) return body.buffer.asInt64List(base, n);
  final bd = _byteData(body, b);
  final out = Int64List(n);
  for (int i = 0; i < n; i++) {
    out[i] = bd.getInt64(i * 8, Endian.little);
  }
  return out;
}

Float32List _viewFloat32(Uint8List body, BufferInfo b, int n) {
  _needBytes(b, n, 4);
  final base = body.offsetInBytes + b.offset;
  if (base % 4 == 0) return body.buffer.asFloat32List(base, n);
  final bd = _byteData(body, b);
  final out = Float32List(n);
  for (int i = 0; i < n; i++) {
    out[i] = bd.getFloat32(i * 4, Endian.little);
  }
  return out;
}

Float64List _viewFloat64(Uint8List body, BufferInfo b, int n) {
  _needBytes(b, n, 8);
  final base = body.offsetInBytes + b.offset;
  if (base % 8 == 0) return body.buffer.asFloat64List(base, n);
  final bd = _byteData(body, b);
  final out = Float64List(n);
  for (int i = 0; i < n; i++) {
    out[i] = bd.getFloat64(i * 8, Endian.little);
  }
  return out;
}
