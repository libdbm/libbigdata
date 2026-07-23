import 'dart:convert';

/// Resolve a character encoding by [name], throwing on an unknown / misspelled
/// name instead of silently falling back to UTF-8 — a wrong fallback produces
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
