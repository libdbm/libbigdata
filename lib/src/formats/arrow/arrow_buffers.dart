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
    final child = field.children.first;
    final offsets = Int32List(n + 1);
    final values = <dynamic>[];
    var offset = 0;
    for (int i = 0; i < n; i++) {
      final value = column[i];
      if (value != null) {
        final elements = _listElements(field, value);
        values.addAll(elements);
        offset += elements.length;
      }
      offsets[i + 1] = offset;
    }
    final encoded = encodeColumn(
      ListColumn(
        name: child.name,
        dataType: child.dataType,
        data: values,
        nullable: child.nullable,
      ),
      child,
    );
    return (
      nodes: [node, ...encoded.nodes],
      buffers: [
        validity,
        offsets.buffer.asUint8List(0, (n + 1) * 4),
        ...encoded.buffers,
      ],
    );
  }
  if (type == DataType.struct) {
    final nodes = <NodeInfo>[node];
    final buffers = <Uint8List>[validity];
    for (final member in field.children) {
      final values = [
        for (int i = 0; i < n; i++)
          column[i] == null ? null : (column[i] as Map)[member.name],
      ];
      final encoded = encodeColumn(
        ListColumn(
          name: member.name,
          dataType: member.dataType,
          data: values,
          nullable: member.nullable,
        ),
        member,
      );
      nodes.addAll(encoded.nodes);
      buffers.addAll(encoded.buffers);
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
      final value = column[i];
      if (value != null) {
        final bytes = type == DataType.string
            ? utf8.encode(value as String)
            : Uint8List.fromList((value as List).cast<int>());
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
      final value = column[i];
      if (value != null) out[i] = dateToEpochDays(value as DateTime);
    }
    buffers.add(out.buffer.asUint8List(0, n * 4));
  } else if (type == DataType.timestamp) {
    final out = Int64List(n);
    for (int i = 0; i < n; i++) {
      final value = column[i];
      if (value != null) out[i] = timestampToEpochMicros(value as DateTime);
    }
    buffers.add(out.buffer.asUint8List(0, n * 8));
  } else if (type == DataType.decimal) {
    final out = Uint8List(n * 16);
    for (int i = 0; i < n; i++) {
      final value = column[i];
      if (value != null) {
        out.setRange(
          i * 16,
          i * 16 + 16,
          bigIntToBytes(value as BigInt, 16, littleEndian: true),
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
    for (final entry in (value as Map).entries)
      {keyName: entry.key, valueName: entry.value},
  ];
}

int _nullCount(Column column, int n) {
  if (!column.hasNulls) return 0;
  var count = 0;
  for (int i = 0; i < n; i++) {
    if (column[i] == null) count++;
  }
  return count;
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
  if (column is NumericVector) {
    final buffer = column.raw;
    final ok =
        (type == DataType.int8 && buffer is Int8List) ||
        (type == DataType.int32 && buffer is Int32List) ||
        (type == DataType.int64 && buffer is Int64List) ||
        (type == DataType.float32 && buffer is Float32List) ||
        (type == DataType.float64 && buffer is Float64List);
    if (ok) {
      return buffer.buffer.asUint8List(
        buffer.offsetInBytes,
        buffer.lengthInBytes,
      );
    }
  }
  switch (type) {
    case DataType.int8:
      final out = Int8List(n);
      for (int i = 0; i < n; i++) {
        final value = column[i];
        if (value != null) out[i] = value as int;
      }
      return out.buffer.asUint8List(0, n);
    case DataType.int16:
      final out = Int16List(n);
      for (int i = 0; i < n; i++) {
        final value = column[i];
        if (value != null) out[i] = value as int;
      }
      return out.buffer.asUint8List(0, n * 2);
    case DataType.int32:
      final out = Int32List(n);
      for (int i = 0; i < n; i++) {
        final value = column[i];
        if (value != null) out[i] = value as int;
      }
      return out.buffer.asUint8List(0, n * 4);
    case DataType.int64:
      final out = Int64List(n);
      for (int i = 0; i < n; i++) {
        final value = column[i];
        if (value != null) out[i] = value as int;
      }
      return out.buffer.asUint8List(0, n * 8);
    case DataType.float32:
      final out = Float32List(n);
      for (int i = 0; i < n; i++) {
        final value = column[i];
        if (value != null) out[i] = (value as num).toDouble();
      }
      return out.buffer.asUint8List(0, n * 4);
    case DataType.float64:
      final out = Float64List(n);
      for (int i = 0; i < n; i++) {
        final value = column[i];
        if (value != null) out[i] = (value as num).toDouble();
      }
      return out.buffer.asUint8List(0, n * 8);
    default:
      throw UnsupportedError('Arrow numeric encode for ${type.name}');
  }
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

/// Build a [Column] from its Arrow buffers. [buffers] are this field's
/// buffer slices (validity first), [body] is the whole record-batch body.
Column decodeColumn(
  Field field,
  NodeInfo node,
  List<BufferInfo> buffers,
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
  final validity = buffers[0];
  final valid = _validityReader(body, validity, node.nullCount, n);
  final bitmap = _nullBitmap(valid, node.nullCount, n);

  switch (type) {
    case DataType.int8:
      return NumericVector.adopt(
        name: field.name,
        dataType: type,
        data: _viewInt8(body, buffers[1], n),
        nullBitmap: bitmap,
      );
    case DataType.int32:
      return NumericVector.adopt(
        name: field.name,
        dataType: type,
        data: _viewInt32(body, buffers[1], n),
        nullBitmap: bitmap,
      );
    case DataType.int64:
      return NumericVector.adopt(
        name: field.name,
        dataType: type,
        data: _viewInt64(body, buffers[1], n),
        nullBitmap: bitmap,
      );
    case DataType.float32:
      return NumericVector.adopt(
        name: field.name,
        dataType: type,
        data: _viewFloat32(body, buffers[1], n),
        nullBitmap: bitmap,
      );
    case DataType.float64:
      return NumericVector.adopt(
        name: field.name,
        dataType: type,
        data: _viewFloat64(body, buffers[1], n),
        nullBitmap: bitmap,
      );
    case DataType.int16:
      _needBytes(buffers[1], n, 2);
      final view = _byteData(body, buffers[1]);
      final data = List<int?>.filled(n, null);
      for (int i = 0; i < n; i++) {
        if (valid(i)) data[i] = view.getInt16(i * 2, Endian.little);
      }
      return ListColumn<int>(
        name: field.name,
        dataType: type,
        data: data,
        nullable: node.nullCount > 0,
      );
    case DataType.boolean:
      _needBytes(buffers[1], (n + 7) >> 3, 1);
      final view = _byteData(body, buffers[1]);
      final data = List<bool?>.filled(n, null);
      for (int i = 0; i < n; i++) {
        if (valid(i)) {
          data[i] = (view.getUint8(i >> 3) & (1 << (i & 7))) != 0;
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
      final offsets = _byteData(body, buffers[1]);
      final values = buffers[2];
      if (offsets.lengthInBytes < (n + 1) * 4) {
        throw FormatException('Arrow ${field.name}: offsets buffer too short');
      }
      final data = List<dynamic>.filled(n, null);
      for (int i = 0; i < n; i++) {
        if (!valid(i)) continue;
        final start = offsets.getInt32(i * 4, Endian.little);
        final end = offsets.getInt32((i + 1) * 4, Endian.little);
        if (start < 0 || end < start || end > values.length) {
          throw FormatException(
            'Arrow ${field.name}: invalid byte offsets [$start, $end)',
          );
        }
        final bytes = body.sublist(values.offset + start, values.offset + end);
        data[i] = type == DataType.string ? utf8.decode(bytes) : bytes;
      }
      return ListColumn(
        name: field.name,
        dataType: type,
        data: data,
        nullable: node.nullCount > 0,
      );
    case DataType.date:
      final buffer = buffers[1];
      // date32 (days) vs date64 (ms) from the DECLARED DateUnit: inferring it
      // from `length ~/ n` misclassified a padded buffer. Old readers of our
      // own files never hit the fallback (we write DAY and the unit is now
      // always parsed); the heuristic remains only for a unit-less schema.
      final unit = field.metadata[dateUnitMeta] as int?;
      final wide = unit != null ? unit == 1 : n > 0 && buffer.length ~/ n == 8;
      _needBytes(buffer, n, wide ? 8 : 4);
      final view = _byteData(body, buffer);
      final data = List<DateTime?>.filled(n, null);
      for (int i = 0; i < n; i++) {
        if (!valid(i)) continue;
        data[i] = wide
            ? timestampFromEpochMillis(view.getInt64(i * 8, Endian.little))
            : dateFromEpochDays(view.getInt32(i * 4, Endian.little));
      }
      return ListColumn<DateTime>(
        name: field.name,
        dataType: type,
        data: data,
        nullable: node.nullCount > 0,
      );
    case DataType.timestamp:
      final unit = field.metadata[timeUnitMeta] as int? ?? 2; // default MICRO
      _needBytes(buffers[1], n, 8);
      final view = _byteData(body, buffers[1]);
      final data = List<DateTime?>.filled(n, null);
      for (int i = 0; i < n; i++) {
        if (!valid(i)) continue;
        final raw = view.getInt64(i * 8, Endian.little);
        data[i] = switch (unit) {
          0 => timestampFromEpochMicros(_seconds(raw)), // SECOND
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
      final buffer = buffers[1];
      // Element width from the DECLARED bitWidth (default 128), not a byte-
      // length division that a padded buffer would skew.
      final bits = field.metadata[decimalBitsMeta] as int?;
      final width = bits != null
          ? bits ~/ 8
          : (n > 0 ? buffer.length ~/ n : 16);
      if (width <= 0) {
        throw FormatException('Arrow ${field.name}: decimal width $width');
      }
      _needBytes(buffer, n, width);
      final data = List<BigInt?>.filled(n, null);
      for (int i = 0; i < n; i++) {
        if (!valid(i)) continue;
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

/// Advance [cursor] past one field subtree WITHOUT decoding it, for projection
/// pushdown, so an unselected column's buffers are never materialized. Must
/// consume exactly the nodes/buffers [decodeTree] would (incl. the `null_`
/// early-return and the nested-child recursion) or every later column desyncs.
void skipTree(Field field, ArrowCursor cursor) {
  final type = field.dataType;
  cursor.node++;
  if (type == DataType.null_) return; // 1 node, 0 buffers
  cursor.buffer += bufferCount(type);
  if (type == DataType.list || type == DataType.map) {
    skipTree(field.children.first, cursor);
  } else if (type == DataType.struct) {
    for (final child in field.children) {
      skipTree(child, cursor);
    }
  }
}

/// Decode one field subtree (recursing into nested children), advancing [cursor]
/// over the shared node/buffer vectors. Scalars delegate to [decodeColumn].
Column decodeTree(
  Field field,
  List<NodeInfo> nodes,
  List<BufferInfo> buffers,
  Uint8List body,
  ArrowCursor cursor,
) {
  final type = field.dataType;
  final node = nodes[cursor.node++];
  final n = node.length;
  if (type == DataType.null_) {
    return ListColumn(
      name: field.name,
      dataType: type,
      data: List<dynamic>.filled(n, null),
      nullable: true,
    );
  }
  final count = bufferCount(type);
  final slices = buffers.sublist(cursor.buffer, cursor.buffer + count);
  cursor.buffer += count;

  if (type == DataType.list || type == DataType.map) {
    final valid = _validityReader(body, slices[0], node.nullCount, n);
    final offsets = _byteData(body, slices[1]);
    final items = decodeTree(
      field.children.first,
      nodes,
      buffers,
      body,
      cursor,
    ).toList();
    if (offsets.lengthInBytes < (n + 1) * 4) {
      throw FormatException('Arrow ${field.name}: offsets buffer too short');
    }
    final data = List<dynamic>.filled(n, null);
    for (int i = 0; i < n; i++) {
      if (!valid(i)) continue;
      final start = offsets.getInt32(i * 4, Endian.little);
      final end = offsets.getInt32((i + 1) * 4, Endian.little);
      if (start < 0 || end < start || end > items.length) {
        throw FormatException(
          'Arrow ${field.name}: invalid list offsets [$start, $end)',
        );
      }
      final slice = items.sublist(start, end);
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
    final valid = _validityReader(body, slices[0], node.nullCount, n);
    final members = [
      for (final child in field.children)
        decodeTree(child, nodes, buffers, body, cursor).toList(),
    ];
    final names = [for (final child in field.children) child.name];
    final data = List<dynamic>.filled(n, null);
    for (int i = 0; i < n; i++) {
      if (!valid(i)) continue;
      data[i] = {
        for (int j = 0; j < names.length; j++) names[j]: members[j][i],
      };
    }
    return ListColumn(
      name: field.name,
      dataType: type,
      data: data,
      nullable: node.nullCount > 0,
    );
  }

  return decodeColumn(field, node, slices, body);
}

Map<dynamic, dynamic> _toMap(Field field, List<dynamic> entries) {
  final pair = field.children.first;
  final keyName = pair.children[0].name;
  final valueName = pair.children[1].name;
  return {for (final item in entries) (item as Map)[keyName]: item[valueName]};
}

bool Function(int) _validityReader(
  Uint8List body,
  BufferInfo validity,
  int nullCount,
  int n,
) {
  if (nullCount == 0) return (_) => true;
  // A field with nulls must say which rows they are, one bit per row.
  _needBytes(validity, (n + 7) >> 3, 1);
  final view = _byteData(body, validity);
  return (i) => (view.getUint8(i >> 3) & (1 << (i & 7))) != 0;
}

/// libbigdata null bitmap (bit SET = null) from Arrow validity, or null.
Uint8List? _nullBitmap(bool Function(int) valid, int nullCount, int n) {
  if (nullCount == 0) return null;
  final out = Uint8List((n + 7) >> 3);
  for (int i = 0; i < n; i++) {
    if (!valid(i)) out[i >> 3] |= 1 << (i & 7);
  }
  return out;
}

ByteData _byteData(Uint8List body, BufferInfo buffer) => ByteData.view(
  body.buffer,
  body.offsetInBytes + buffer.offset,
  buffer.length,
);

/// [seconds] as microseconds, which must fit in 64 bits.
int _seconds(int seconds) {
  const limit = 9223372036854; // 2^63 / 10^6
  if (seconds > limit || seconds < -limit) {
    throw FormatException(
      'Arrow timestamp of $seconds seconds is out of range',
    );
  }
  return seconds * 1000000;
}

/// Reject a data buffer whose DECLARED length is shorter than the node's
/// element count needs: the typed views below read `n * width` bytes from the
/// buffer's offset regardless, so a corrupt batch that under-declares a
/// buffer would silently fabricate values from the NEIGHBORING buffer's bytes.
void _needBytes(BufferInfo buffer, int n, int width) {
  if (buffer.length < n * width) {
    throw FormatException(
      'Arrow record batch buffer holds ${buffer.length} bytes but the field node '
      'declares $n ${width * 8}-bit values (${n * width} bytes): corrupt '
      'batch',
    );
  }
}

// Typed numeric views (zero-copy when aligned, copy otherwise).
Int8List _viewInt8(Uint8List body, BufferInfo buffer, int n) {
  _needBytes(buffer, n, 1);
  return Int8List.sublistView(body, buffer.offset, buffer.offset + n);
}

Int32List _viewInt32(Uint8List body, BufferInfo buffer, int n) {
  _needBytes(buffer, n, 4);
  final base = body.offsetInBytes + buffer.offset;
  if (base % 4 == 0) return body.buffer.asInt32List(base, n);
  final view = _byteData(body, buffer);
  final out = Int32List(n);
  for (int i = 0; i < n; i++) {
    out[i] = view.getInt32(i * 4, Endian.little);
  }
  return out;
}

Int64List _viewInt64(Uint8List body, BufferInfo buffer, int n) {
  _needBytes(buffer, n, 8);
  final base = body.offsetInBytes + buffer.offset;
  if (base % 8 == 0) return body.buffer.asInt64List(base, n);
  final view = _byteData(body, buffer);
  final out = Int64List(n);
  for (int i = 0; i < n; i++) {
    out[i] = view.getInt64(i * 8, Endian.little);
  }
  return out;
}

Float32List _viewFloat32(Uint8List body, BufferInfo buffer, int n) {
  _needBytes(buffer, n, 4);
  final base = body.offsetInBytes + buffer.offset;
  if (base % 4 == 0) return body.buffer.asFloat32List(base, n);
  final view = _byteData(body, buffer);
  final out = Float32List(n);
  for (int i = 0; i < n; i++) {
    out[i] = view.getFloat32(i * 4, Endian.little);
  }
  return out;
}

Float64List _viewFloat64(Uint8List body, BufferInfo buffer, int n) {
  _needBytes(buffer, n, 8);
  final base = body.offsetInBytes + buffer.offset;
  if (base % 8 == 0) return body.buffer.asFloat64List(base, n);
  final view = _byteData(body, buffer);
  final out = Float64List(n);
  for (int i = 0; i < n; i++) {
    out[i] = view.getFloat64(i * 8, Endian.little);
  }
  return out;
}
