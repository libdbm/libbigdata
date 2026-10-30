import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

/// A minimal, self-contained FlatBuffers writer/reader, just enough to encode
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
  ByteData _buffer;
  int _tail = 0; // bytes written, measured from the end of _buffer
  int _alignment = 1;
  bool _finished = false;

  // Current table state.
  Uint32List? _tails; // per-field tail location (pre-finish)
  int _count = 0;
  int _start = 0;
  final List<int> _vtables = []; // tails of previously written vtables (dedup)

  FbBuilder([int size = 1024]) : _buffer = ByteData(size);

  int get _capacity => _buffer.lengthInBytes;

  int _size() => _tail + ((-_tail) & (_alignment - 1));

  /// The finished buffer (call [finish] first).
  Uint8List finish(int root) {
    final before = _size();
    _prepare(max(4, _alignment), 1);
    final finished = _size();
    _setU32(finished, finished - root);
    for (int i = before + 1; i <= finished - 4; i++) {
      _setU8(i, 0);
    }
    _finished = true;
    return _buffer.buffer.asUint8List(
      _buffer.offsetInBytes + _capacity - finished,
      finished,
    );
  }

  // ---- tables -------------------------------------------------------------

  void startTable(int count) {
    _tails = Uint32List(count);
    _count = count;
    _start = _tail;
  }

  void addBool(int field, bool value, [bool fallback = false]) {
    if (value == fallback) return;
    _prepare(1, 1);
    _track(field);
    _buffer.setUint8(_capacity - _tail, value ? 1 : 0);
  }

  void addUint8(int field, int value, [int fallback = 0]) {
    if (value == fallback) return;
    _prepare(1, 1);
    _track(field);
    _buffer.setUint8(_capacity - _tail, value);
  }

  void addInt16(int field, int value, [int fallback = 0]) {
    if (value == fallback) return;
    _prepare(2, 1);
    _track(field);
    _setI16(_tail, value);
  }

  void addInt32(int field, int value, [int fallback = 0]) {
    if (value == fallback) return;
    _prepare(4, 1);
    _track(field);
    _setI32(_tail, value);
  }

  void addInt64(int field, int value, [int fallback = 0]) {
    if (value == fallback) return;
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

  void _track(int field) => _tails![field] = _tail;

  int endTable() {
    _prepare(4, 1); // space for the soffset to the vtable
    final table = _tail;
    final size = table - _start;

    // Field offsets relative to the table tail.
    final n = _count;
    final offsets = Uint16List(n);
    for (int i = 0; i < n; i++) {
      final tail = _tails![i];
      offsets[i] = tail == 0 ? 0 : table - tail;
    }
    final slots = 2 + n; // [vtable size, table size, fields...]
    final bytes = slots * 2;

    // Try to reuse an identical vtable.
    int? vtable;
    for (final candidate in _vtables) {
      final start = _capacity - candidate;
      if (_buffer.getUint16(start, Endian.little) != bytes) continue;
      var ok = true;
      for (int i = 0; i < n; i++) {
        if (_buffer.getUint16(start + 4 + 2 * i, Endian.little) != offsets[i]) {
          ok = false;
          break;
        }
      }
      if (ok) {
        vtable = candidate;
        break;
      }
    }

    if (vtable == null) {
      _prepare(2, slots);
      vtable = _tail;
      final at = _capacity - _tail;
      _buffer.setUint16(at, bytes, Endian.little);
      _buffer.setUint16(at + 2, size, Endian.little);
      for (int i = 0; i < n; i++) {
        _buffer.setUint16(at + 4 + 2 * i, offsets[i], Endian.little);
      }
      _vtables.add(vtable);
    }

    // The table's soffset points back to its vtable.
    _setI32(table, vtable - table);
    _tails = null;
    return table;
  }

  // ---- strings / vectors --------------------------------------------------

  int writeString(String value) {
    final bytes = utf8.encode(value);
    _prepare(4, 1, extra: bytes.length + 1);
    _setU32(_tail, bytes.length);
    final at = _capacity - _tail + 4;
    for (int i = 0; i < bytes.length; i++) {
      _buffer.setUint8(at + i, bytes[i]);
    }
    _buffer.setUint8(at + bytes.length, 0); // null terminator
    return _tail;
  }

  /// Vector of forward offsets (e.g. a list of table offsets).
  int writeOffsetVector(List<int> offsets) {
    _prepare(4, 1 + offsets.length);
    final result = _tail;
    var tail = _tail;
    _setU32(tail, offsets.length);
    tail -= 4;
    for (final offset in offsets) {
      _setU32(tail, tail - offset);
      tail -= 4;
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

  void _putRaw(Uint8List bytes, int align) {
    final n = bytes.length;
    _prepare(align, n ~/ align);
    final at = _capacity - _tail;
    for (int i = 0; i < n; i++) {
      _buffer.setUint8(at + i, bytes[i]);
    }
  }

  // ---- low level ----------------------------------------------------------

  void _prepare(int size, int count, {int extra = 0}) {
    assert(!_finished);
    if (_alignment < size) _alignment = size;
    final bytes = size * count + extra;
    final padding = (-(_tail + bytes)) & (size - 1);
    _ensure(padding + bytes);
    for (int i = _tail + 1; i <= _tail + padding; i++) {
      _setU8(i, 0);
    }
    _tail += padding + bytes;
  }

  void _ensure(int extra) {
    if (_tail + extra <= _capacity) return;
    int size = _capacity * 2;
    while (_tail + extra > size) {
      size *= 2;
    }
    final grown = ByteData(size);
    final old = _buffer.buffer.asUint8List(
      _buffer.offsetInBytes + _capacity - _tail,
      _tail,
    );
    grown.buffer.asUint8List(size - _tail, _tail).setAll(0, old);
    _buffer = grown;
  }

  void _setU8(int tail, int value) => _buffer.setUint8(_capacity - tail, value);
  void _setI16(int tail, int value) =>
      _buffer.setInt16(_capacity - tail, value, Endian.little);
  void _setI32(int tail, int value) =>
      _buffer.setInt32(_capacity - tail, value, Endian.little);
  void _setU32(int tail, int value) =>
      _buffer.setUint32(_capacity - tail, value, Endian.little);
  void _setI64(int tail, int value) =>
      _buffer.setInt64(_capacity - tail, value, Endian.little);
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

  // Every offset comes from the untrusted message, so each read is checked and
  // a corrupt one raises FormatException instead of a RangeError.
  int u8(int position) => _data.getUint8(_check(position, 1));
  int i16(int position) => _data.getInt16(_check(position, 2), Endian.little);
  int u16(int position) => _data.getUint16(_check(position, 2), Endian.little);
  int i32(int position) => _data.getInt32(_check(position, 4), Endian.little);
  int u32(int position) => _data.getUint32(_check(position, 4), Endian.little);
  int i64(int position) => _data.getInt64(_check(position, 8), Endian.little);

  int _check(int position, int size) {
    if (position < 0 || position + size > _data.lengthInBytes) {
      throw FormatException(
        'FlatBuffer read of $size bytes at $position runs past the '
        '${_data.lengthInBytes}-byte message',
      );
    }
    return position;
  }

  /// Follow a uoffset at [position] to the object it references.
  int deref(int position) => position + u32(position);

  /// Absolute location of table [field] (index), or 0 if absent.
  int locate(int table, int field) {
    final vtable = table - i32(table);
    final size = u16(vtable);
    final slot = 4 + 2 * field;
    if (slot >= size) return 0;
    final offset = u16(vtable + slot);
    return offset == 0 ? 0 : table + offset;
  }

  int i16Field(int table, int field, [int fallback = 0]) {
    final location = locate(table, field);
    return location == 0 ? fallback : i16(location);
  }

  int i32Field(int table, int field, [int fallback = 0]) {
    final location = locate(table, field);
    return location == 0 ? fallback : i32(location);
  }

  int i64Field(int table, int field, [int fallback = 0]) {
    final location = locate(table, field);
    return location == 0 ? fallback : i64(location);
  }

  bool boolField(int table, int field, [bool fallback = false]) {
    final location = locate(table, field);
    return location == 0 ? fallback : u8(location) != 0;
  }

  int u8Field(int table, int field, [int fallback = 0]) {
    final location = locate(table, field);
    return location == 0 ? fallback : u8(location);
  }

  /// Referenced object position (table/vector/string) for an offset field, or 0.
  int offsetField(int table, int field) {
    final location = locate(table, field);
    return location == 0 ? 0 : deref(location);
  }

  String? stringField(int table, int field) {
    final location = locate(table, field);
    if (location == 0) return null;
    final start = deref(location);
    final length = u32(start);
    if (start + 4 + length > _data.lengthInBytes) {
      throw const FormatException('FlatBuffer string runs past the message');
    }
    return utf8.decode(
      _data.buffer.asUint8List(_data.offsetInBytes + start + 4, length),
    );
  }

  /// Number of elements in the vector referenced by [field], or 0.
  int vectorLength(int table, int field) {
    final vector = offsetField(table, field);
    return vector == 0 ? 0 : u32(vector);
  }

  /// Position of element [i] in a vector of forward offsets (e.g. tables),
  /// dereferenced to the element object.
  int offsetVectorElement(int table, int field, int i) {
    final vector = offsetField(table, field);
    final element = vector + 4 + 4 * i;
    return deref(element);
  }

  /// Position of element [i] (inline struct of [structSize] bytes).
  int structVectorElement(int table, int field, int i, int structSize) {
    final vector = offsetField(table, field);
    return vector + 4 + structSize * i;
  }
}
