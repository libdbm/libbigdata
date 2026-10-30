import 'dart:convert';
import 'dart:typed_data';

/// Resolve a character encoding by [name], throwing on an unknown / misspelled
/// name instead of silently falling back to UTF-8: a wrong fallback produces
/// mojibake on read, or writes bytes in a different encoding than the caller
/// asked for, with no error. A null or empty name means UTF-8.
Encoding resolveEncoding(String? name) {
  if (name == null || name.isEmpty) return utf8;
  final encoding = Encoding.getByName(name);
  if (encoding == null) {
    throw ArgumentError.value(name, 'encoding', 'Unknown character encoding');
  }
  return encoding;
}

/// Whether `bytes[start, end)` is well-formed UTF-8: the bytes a strict
/// `utf8.decode` accepts, so no overlong forms, surrogates or code points past
/// U+10FFFF. Checks without allocating a [String].
bool validUtf8(Uint8List bytes, int start, int end) {
  var i = start;
  while (i < end) {
    final lead = bytes[i];
    if (lead < 0x80) {
      i++;
      continue;
    }
    final int size;
    var low = 0x80;
    var high = 0xbf;
    if (lead >= 0xc2 && lead <= 0xdf) {
      size = 2;
    } else if (lead >= 0xe0 && lead <= 0xef) {
      size = 3;
      if (lead == 0xe0) low = 0xa0; // overlong
      if (lead == 0xed) high = 0x9f; // surrogates
    } else if (lead >= 0xf0 && lead <= 0xf4) {
      size = 4;
      if (lead == 0xf0) low = 0x90; // overlong
      if (lead == 0xf4) high = 0x8f; // past U+10FFFF
    } else {
      return false;
    }
    if (i + size > end) return false;
    final second = bytes[i + 1];
    if (second < low || second > high) return false;
    for (var k = 2; k < size; k++) {
      final next = bytes[i + k];
      if (next < 0x80 || next > 0xbf) return false;
    }
    i += size;
  }
  return true;
}
