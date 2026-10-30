import 'dart:typed_data';

/// Base-128 varints (unsigned LEB128) and zigzag, as Parquet's encodings, the
/// Thrift compact protocol, Avro and Snappy write them: one implementation for
/// every format, which each had its own.

/// Append [value] to [out] as a base-128 varint, reading it as unsigned 64
/// bits: a negative value takes all ten bytes.
void writeVarint(BytesBuilder out, int value) {
  var rest = value;
  while ((rest & ~0x7f) != 0) {
    out.addByte((rest & 0x7f) | 0x80);
    rest >>>= 7;
  }
  out.addByte(rest);
}

/// The varint in [bytes] at [position]: its value and the position after it.
/// One that the buffer ends inside, or that runs past [maxBytes] bytes (ten
/// for 64 bits), is a [FormatException] naming [label].
({int value, int next}) readVarint(
  Uint8List bytes,
  int position, {
  int maxBytes = 10,
  required String label,
}) {
  var value = 0;
  var shift = 0;
  for (var at = position; at < position + maxBytes; at++) {
    if (at >= bytes.length) {
      throw FormatException('Truncated $label: a varint runs past the end');
    }
    final byte = bytes[at];
    value |= (byte & 0x7f) << shift;
    if ((byte & 0x80) == 0) return (value: value, next: at + 1);
    shift += 7;
  }
  throw FormatException('$label varint is longer than $maxBytes bytes');
}

/// [value] zigzag-encoded, so small magnitudes of either sign are small codes.
int zigzag(int value) => (value << 1) ^ (value >> 63);

/// The value [code] zigzag-encodes. The shift is unsigned: a code with bit 63
/// set is a negative Dart int, and `>>` would carry the sign into the result.
int unzigzag(int code) => (code >>> 1) ^ -(code & 1);
