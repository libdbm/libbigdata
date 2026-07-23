import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

/// A minimal, self-contained FlatBuffers writer/reader — just enough to encode
/// and decode the Apache Arrow IPC metadata messages (Message, Schema, Field,
/// RecordBatch, Footer and their struct vectors).
///
/// FlatBuffers are built back-to-front: child objects are written first and
/// referenced by forward unsigned offsets (uoffset = current_tail - target_tail).
/// Tables carry a signed offset to a shared vtable that maps field index ->
/// byte offset within the table (0 = field absent). All scalars are little
/// endian. This implements only the subset Arrow needs: tables, vectors of
/// offsets, vectors of inline structs, strings and unions (a u8 type field plus
/// an offset field). Layout follows the FlatBuffers binary spec so the output is
/// readable by any conformant Arrow/FlatBuffers implementation.

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

class FbBuilder {
  ByteData _buf;
  int _tail = 0; // bytes written, measured from the end of _buf
  int _maxAlign = 1;
  bool _finished = false;

  // Current table state.
  Uint32List? _fieldTails; // per-field tail location (pre-finish)
  int _fieldCount = 0;
  int _tableStart = 0;
  final List<int> _vtables = []; // tails of previously written vtables (dedup)

  FbBuilder([int initialSize = 1024]) : _buf = ByteData(initialSize);

  int get _capacity => _buf.lengthInBytes;

  int _size() => _tail + ((-_tail) & (_maxAlign - 1));

  /// The finished buffer (call [finish] first).
  Uint8List finish(int root) {
    final before = _size();
    _prepare(max(4, _maxAlign), 1);
    final finished = _size();
    _setU32(finished, finished - root);
    for (int i = before + 1; i <= finished - 4; i++) {
      _setU8(i, 0);
    }
    _finished = true;
    return _buf.buffer.asUint8List(
      _buf.offsetInBytes + _capacity - finished,
      finished,
    );
  }

  // ---- tables -------------------------------------------------------------

  void startTable(int numFields) {
    _fieldTails = Uint32List(numFields);
    _fieldCount = numFields;
    _tableStart = _tail;
  }

  void addBool(int field, bool value, [bool def = false]) {
    if (value == def) return;
    _prepare(1, 1);
    _track(field);
    _buf.setUint8(_capacity - _tail, value ? 1 : 0);
  }

  void addUint8(int field, int value, [int def = 0]) {
    if (value == def) return;
    _prepare(1, 1);
    _track(field);
    _buf.setUint8(_capacity - _tail, value);
  }

  void addInt16(int field, int value, [int def = 0]) {
    if (value == def) return;
    _prepare(2, 1);
    _track(field);
    _setI16(_tail, value);
  }

  void addInt32(int field, int value, [int def = 0]) {
    if (value == def) return;
    _prepare(4, 1);
    _track(field);
    _setI32(_tail, value);
  }

  void addInt64(int field, int value, [int def = 0]) {
    if (value == def) return;
    _prepare(8, 1);
    _track(field);
    _setI64(_tail, value);
  }

  /// Add a reference field (table / vector / string) pointing at [offset].
  void addOffset(int field, int? offset) {
    if (offset == null) return;
    _prepare(4, 1);
    _track(field);
    _setU32(_tail, _tail - offset);
  }

  void _track(int field) => _fieldTails![field] = _tail;

  int endTable() {
    _prepare(4, 1); // space for the soffset to the vtable
    final tableTail = _tail;
    final tableSize = tableTail - _tableStart;

    // Field offsets relative to the table tail.
    final n = _fieldCount;
    final rel = Uint16List(n);
    for (int i = 0; i < n; i++) {
      final t = _fieldTails![i];
      rel[i] = t == 0 ? 0 : tableTail - t;
    }
    final vtUint16 = 2 + n; // [vtSize, tableSize, fields...]
    final vtBytes = vtUint16 * 2;

    // Try to reuse an identical vtable.
    int? vtableTail;
    for (final cand in _vtables) {
      final start = _capacity - cand;
      if (_buf.getUint16(start, Endian.little) != vtBytes) continue;
      var ok = true;
      for (int i = 0; i < n; i++) {
        if (_buf.getUint16(start + 4 + 2 * i, Endian.little) != rel[i]) {
          ok = false;
          break;
        }
      }
      if (ok) {
        vtableTail = cand;
        break;
      }
    }

    if (vtableTail == null) {
      _prepare(2, vtUint16);
      vtableTail = _tail;
      final at = _capacity - _tail;
      _buf.setUint16(at, vtBytes, Endian.little);
      _buf.setUint16(at + 2, tableSize, Endian.little);
      for (int i = 0; i < n; i++) {
        _buf.setUint16(at + 4 + 2 * i, rel[i], Endian.little);
      }
      _vtables.add(vtableTail);
    }

    // The table's soffset points back to its vtable.
    _setI32(tableTail, vtableTail - tableTail);
    _fieldTails = null;
    return tableTail;
  }

  // ---- strings / vectors --------------------------------------------------

  int writeString(String value) {
    final bytes = utf8.encode(value);
    _prepare(4, 1, extra: bytes.length + 1);
    _setU32(_tail, bytes.length);
    final at = _capacity - _tail + 4;
    for (int i = 0; i < bytes.length; i++) {
      _buf.setUint8(at + i, bytes[i]);
    }
    _buf.setUint8(at + bytes.length, 0); // null terminator
    return _tail;
  }

  /// Vector of forward offsets (e.g. a list of table offsets).
  int writeOffsetVector(List<int> offsets) {
    _prepare(4, 1 + offsets.length);
    final result = _tail;
    var t = _tail;
    _setU32(t, offsets.length);
    t -= 4;
    for (final offset in offsets) {
      _setU32(t, t - offset);
      t -= 4;
    }
    return result;
  }

  /// Vector of inline fixed-size structs. [structs] are in element order; each
  /// is exactly [align]-aligned and a multiple of [align] bytes.
  int writeStructVector(List<Uint8List> structs, int align) {
    for (int i = structs.length - 1; i >= 0; i--) {
      _putRaw(structs[i], align);
    }
    _prepare(4, 1);
    _setU32(_tail, structs.length);
    return _tail;
  }

  void _putRaw(Uint8List src, int align) {
    final n = src.length;
    _prepare(align, n ~/ align);
    final at = _capacity - _tail;
    for (int i = 0; i < n; i++) {
      _buf.setUint8(at + i, src[i]);
    }
  }

  // ---- low level ----------------------------------------------------------

  void _prepare(int size, int count, {int extra = 0}) {
    assert(!_finished);
    if (_maxAlign < size) _maxAlign = size;
    final dataSize = size * count + extra;
    final alignDelta = (-(_tail + dataSize)) & (size - 1);
    _ensure(alignDelta + dataSize);
    for (int i = _tail + 1; i <= _tail + alignDelta; i++) {
      _setU8(i, 0);
    }
    _tail += alignDelta + dataSize;
  }

  void _ensure(int extra) {
    if (_tail + extra <= _capacity) return;
    int cap = _capacity * 2;
    while (_tail + extra > cap) {
      cap *= 2;
    }
    final nb = ByteData(cap);
    final old = _buf.buffer.asUint8List(
      _buf.offsetInBytes + _capacity - _tail,
      _tail,
    );
    nb.buffer.asUint8List(cap - _tail, _tail).setAll(0, old);
    _buf = nb;
  }

  void _setU8(int tail, int v) => _buf.setUint8(_capacity - tail, v);
  void _setI16(int tail, int v) =>
      _buf.setInt16(_capacity - tail, v, Endian.little);
  void _setI32(int tail, int v) =>
      _buf.setInt32(_capacity - tail, v, Endian.little);
  void _setU32(int tail, int v) =>
      _buf.setUint32(_capacity - tail, v, Endian.little);
  void _setI64(int tail, int v) =>
      _buf.setInt64(_capacity - tail, v, Endian.little);
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// Reads a FlatBuffer. All positions are byte offsets from the start of [_data]
/// (which must be exactly the FlatBuffer message, i.e. position 0 holds the root
/// uoffset).
class FbReader {
  final ByteData _data;

  FbReader(Uint8List bytes)
    : _data = ByteData.view(
        bytes.buffer,
        bytes.offsetInBytes,
        bytes.lengthInBytes,
      );

  /// Position of the root table.
  int get root => deref(0);

  int u8(int o) => _data.getUint8(o);
  int i16(int o) => _data.getInt16(o, Endian.little);
  int u16(int o) => _data.getUint16(o, Endian.little);
  int i32(int o) => _data.getInt32(o, Endian.little);
  int u32(int o) => _data.getUint32(o, Endian.little);
  int i64(int o) => _data.getInt64(o, Endian.little);

  /// Follow a uoffset at [o] to the object it references.
  int deref(int o) => o + u32(o);

  /// Absolute location of table [field] (index), or 0 if absent.
  int fieldLoc(int table, int field) {
    final vtable = table - i32(table);
    final vtSize = u16(vtable);
    final byteOff = 4 + 2 * field;
    if (byteOff >= vtSize) return 0;
    final rel = u16(vtable + byteOff);
    return rel == 0 ? 0 : table + rel;
  }

  int i16Field(int table, int field, [int def = 0]) {
    final loc = fieldLoc(table, field);
    return loc == 0 ? def : i16(loc);
  }

  int i32Field(int table, int field, [int def = 0]) {
    final loc = fieldLoc(table, field);
    return loc == 0 ? def : i32(loc);
  }

  int i64Field(int table, int field, [int def = 0]) {
    final loc = fieldLoc(table, field);
    return loc == 0 ? def : i64(loc);
  }

  bool boolField(int table, int field, [bool def = false]) {
    final loc = fieldLoc(table, field);
    return loc == 0 ? def : u8(loc) != 0;
  }

  int u8Field(int table, int field, [int def = 0]) {
    final loc = fieldLoc(table, field);
    return loc == 0 ? def : u8(loc);
  }

  /// Referenced object position (table/vector/string) for an offset field, or 0.
  int offsetField(int table, int field) {
    final loc = fieldLoc(table, field);
    return loc == 0 ? 0 : deref(loc);
  }

  String? stringField(int table, int field) {
    final loc = fieldLoc(table, field);
    if (loc == 0) return null;
    final s = deref(loc);
    final length = u32(s);
    if (s + 4 + length > _data.lengthInBytes) {
      throw const FormatException('FlatBuffer string runs past the message');
    }
    return utf8.decode(
      _data.buffer.asUint8List(_data.offsetInBytes + s + 4, length),
    );
  }

  /// Number of elements in the vector referenced by [field], or 0.
  int vectorLength(int table, int field) {
    final v = offsetField(table, field);
    return v == 0 ? 0 : u32(v);
  }

  /// Position of element [i] in a vector of forward offsets (e.g. tables),
  /// dereferenced to the element object.
  int offsetVectorElement(int table, int field, int i) {
    final v = offsetField(table, field);
    final element = v + 4 + 4 * i;
    return deref(element);
  }

  /// Position of element [i] (inline struct of [structSize] bytes).
  int structVectorElement(int table, int field, int i, int structSize) {
    final v = offsetField(table, field);
    return v + 4 + structSize * i;
  }
}
