import 'dart:typed_data';

import 'decimal.dart';

/// Value conversions for temporal and decimal column types, shared by the Arrow
/// and Parquet codecs. Temporal columns hold Dart [DateTime]; decimal columns
/// hold unscaled [BigInt] (scale lives on the Field).

final DateTime _epoch = DateTime.utc(1970);

/// `date` → days since 1970-01-01 (UTC, calendar date only).
int dateToEpochDays(DateTime date) =>
    DateTime.utc(date.year, date.month, date.day).difference(_epoch).inDays;

DateTime dateFromEpochDays(int days) => _epoch.add(Duration(days: days));

/// The written date and time fields of ISO-8601 text, in the form
/// [DateTime.parse] accepts.
final _fields = RegExp(
  r'^\s*[+-]?(\d{4,6})-?(\d\d)-?(\d\d)(?:[ T](\d\d)(?::?(\d\d)(?::?(\d\d))?)?)?',
);

/// [text] parsed as ISO-8601 into a UTC [DateTime], or null when it does not
/// parse. Text without a zone is read as UTC, keeping its wall-clock fields;
/// text with an offset keeps its instant. Shared by SQL CAST, temporal
/// literals and comparisons with strings, and the DataFrame filter.
///
/// A field out of range (month 13, February 30, hour 24, minute 60) is null:
/// [DateTime.tryParse] would roll it over into a different, valid date.
DateTime? parseUtc(String text) {
  final parsed = DateTime.tryParse(text);
  if (parsed == null || !_inRange(text)) return null;
  if (parsed.isUtc) return parsed;
  return DateTime.utc(
    parsed.year,
    parsed.month,
    parsed.day,
    parsed.hour,
    parsed.minute,
    parsed.second,
    parsed.millisecond,
    parsed.microsecond,
  );
}

/// Whether the written fields of [text] name a real date and time, which is
/// so when building them does not roll any of them over.
bool _inRange(String text) {
  final match = _fields.firstMatch(text);
  if (match == null) return true;
  int field(int group, int fallback) =>
      int.parse(match.group(group) ?? '$fallback');
  final year = field(1, 0);
  final month = field(2, 1);
  final day = field(3, 1);
  final hour = field(4, 0);
  final minute = field(5, 0);
  final second = field(6, 0);
  final built = DateTime.utc(year, month, day, hour, minute, second);
  return built.year == year &&
      built.month == month &&
      built.day == day &&
      built.hour == hour &&
      built.minute == minute &&
      built.second == second;
}

/// The `date` value for [at]: its calendar day at UTC midnight.
DateTime dateOf(DateTime at) => DateTime.utc(at.year, at.month, at.day);

/// `timestamp` → microseconds since the Unix epoch (UTC).
int timestampToEpochMicros(DateTime time) =>
    time.toUtc().microsecondsSinceEpoch;

DateTime timestampFromEpochMicros(int micros) =>
    DateTime.fromMicrosecondsSinceEpoch(micros, isUtc: true);

DateTime timestampFromEpochMillis(int millis) =>
    DateTime.fromMillisecondsSinceEpoch(millis, isUtc: true);

/// `timestamp(NANOS)` → [DateTime]. Dart is microsecond-precision, so the
/// sub-microsecond digits are truncated (as in [int96ToTimestamp]).
DateTime timestampFromEpochNanos(int nanos) =>
    DateTime.fromMicrosecondsSinceEpoch(nanos ~/ 1000, isUtc: true);

/// Parquet INT96 (Impala/Spark): nanos-of-day (int64 LE) + Julian day (int32 LE).
DateTime int96ToTimestamp(List<int> bytes) {
  final view = ByteData.sublistView(Uint8List.fromList(bytes));
  final nanos = view.getInt64(0, Endian.little);
  final day = view.getInt32(8, Endian.little);
  const offset = 2440588;
  return _epoch.add(Duration(days: day - offset, microseconds: nanos ~/ 1000));
}

/// Format an unscaled decimal [unscaled] with [scale] as its human/Spark-readable
/// string (e.g. unscaled 12345, scale 2 → "123.45"; -99, 2 → "-0.99"; scale 0 →
/// the integer). The inverse representation used everywhere else is unscaled
/// [BigInt] + scale on the Field.
String formatDecimal(BigInt unscaled, int scale) {
  if (scale <= 0) return unscaled.toString();
  final negative = unscaled.isNegative;
  final digits = unscaled.abs().toString().padLeft(scale + 1, '0');
  final cut = digits.length - scale;
  final whole = digits.substring(0, cut);
  final fraction = digits.substring(cut);
  return '${negative ? '-' : ''}$whole.$fraction';
}

/// Decompose a numeric [value] into `(unscaled, scale)` via its SHORTEST
/// round-trip decimal string, so `0.5` → `(5, 1)` (not the binary tail
/// `0.5000000…`) and an integer is exact with scale 0. Used to promote a
/// mixed integer/float column to a lossless `decimal` (a large int mixed with a
/// float would otherwise round under float64). Throws on a non-finite double.
({BigInt unscaled, int scale}) decimalParts(num value) {
  if (value is int) return (unscaled: BigInt.from(value), scale: 0);
  final number = value.toDouble();
  if (!number.isFinite) {
    throw StateError('Cannot represent $number exactly as a decimal');
  }
  var text = number.toString(); // shortest round-trip representation
  final negative = text.startsWith('-');
  if (negative) text = text.substring(1);
  var exponent = 0;
  final index = text.indexOf('e');
  if (index >= 0) {
    exponent = int.parse(text.substring(index + 1));
    text = text.substring(0, index);
  }
  var scale = 0;
  final dot = text.indexOf('.');
  if (dot >= 0) {
    scale = text.length - dot - 1;
    text = text.substring(0, dot) + text.substring(dot + 1);
  }
  // A positive exponent shrinks the scale (adds trailing zeros).
  scale -= exponent;
  var unscaled = BigInt.parse(text);
  if (scale < 0) {
    unscaled *= BigInt.from(10).pow(-scale);
    scale = 0;
  }
  return (unscaled: negative ? -unscaled : unscaled, scale: scale);
}

/// [value] as an unscaled [BigInt] at [scale] (pads with trailing zeros for a
/// value of smaller natural scale). Lossless for integers and shortest-form
/// doubles. Throws if [scale] is SMALLER than the value's natural scale; that
/// would silently drop fractional digits, so the caller must pick a scale that
/// covers every value.
BigInt toUnscaled(num value, int scale) {
  final parts = decimalParts(value);
  final shift = scale - parts.scale;
  if (shift < 0) {
    throw ArgumentError(
      'toUnscaled: target scale $scale is smaller than the natural scale '
      '${parts.scale} of $value (would lose precision)',
    );
  }
  return shift == 0
      ? parts.unscaled
      : parts.unscaled * BigInt.from(10).pow(shift);
}

/// Re-scale an unscaled decimal [value] from scale [from] to [to]. Widening
/// pads (exact); narrowing is allowed only when no non-zero fractional digit
/// is dropped; otherwise it throws rather than silently rounding a type whose
/// whole point is exactness.
BigInt rescaleUnscaled(BigInt value, int from, int to) {
  if (to >= from) {
    return to == from ? value : value * BigInt.from(10).pow(to - from);
  }
  final divisor = BigInt.from(10).pow(from - to);
  if (value % divisor != BigInt.zero) {
    throw FormatException(
      'rescale from scale $from to $to would drop non-zero fractional digits '
      'of ${formatDecimal(value, from)}',
    );
  }
  return value ~/ divisor;
}

/// The largest decimal precision a supported format declares: Arrow and
/// Parquet 256-bit decimals. Delta and Iceberg allow 38.
const maxDecimalPrecision = 76;

/// [precision] and [scale] read from an untrusted schema, checked: precision
/// 1 to [max], scale 0 to precision. Decimal formatting and rescaling allocate
/// in proportion to the scale, so an unchecked one is a memory hazard.
({int precision, int scale}) checkedDecimal(
  int precision,
  int scale, {
  int max = maxDecimalPrecision,
}) {
  if (precision < 1 || precision > max || scale < 0 || scale > precision) {
    throw FormatException('Invalid decimal($precision,$scale)');
  }
  return (precision: precision, scale: scale);
}

/// Parse a `"decimal(p,s)"` type string (the Spark and Iceberg form) to
/// `(precision, scale)`, defaulting to `(38, 0)` when the string isn't that form.
/// One source for both format schema parsers. Both formats cap precision at
/// 38.
({int precision, int scale}) decimalTypeParams(String type) {
  final match = RegExp(r'decimal\((\d+)\s*,\s*(\d+)\)').firstMatch(type);
  if (match == null) return (precision: 38, scale: 0);
  final precision = int.tryParse(match.group(1)!);
  final scale = int.tryParse(match.group(2)!);
  if (precision == null || scale == null) {
    throw FormatException('Invalid decimal type: $type');
  }
  return checkedDecimal(precision, scale, max: 38);
}

/// Parse a decimal STRING (`"123.45"`, `"-0.99"`, `"12345678901234567890"`) to an
/// unscaled [BigInt] at [scale], EXACTLY, never via a double, which would round
/// a value past 2^53 (or throw on a clean-double round-trip through [toUnscaled]).
/// The string inverse of [formatDecimal]. Throws [FormatException] on a non-decimal
/// string or one carrying MORE fractional digits than [scale] can hold (the same
/// fail-loud contract as [toUnscaled], so a foreign writer's over-precise partition
/// value is rejected, not silently rounded).
BigInt unscaledFromString(String raw, int scale) {
  final text = raw.trim();
  final negative = text.startsWith('-');
  final body = negative || text.startsWith('+') ? text.substring(1) : text;
  final dot = body.indexOf('.');
  final whole = dot < 0 ? body : body.substring(0, dot);
  final fraction = dot < 0 ? '' : body.substring(dot + 1);
  // BigInt.parse would also take a sign, a 0x prefix and the like.
  if ((whole.isEmpty && fraction.isEmpty) ||
      !_digits.hasMatch(whole) ||
      !_digits.hasMatch(fraction)) {
    throw FormatException('Not a decimal value: "$raw"');
  }
  if (fraction.length > scale) {
    throw FormatException(
      'Decimal "$raw" has ${fraction.length} fractional digits, more than the '
      'declared scale $scale can represent without rounding',
    );
  }
  final digits = '$whole${fraction.padRight(scale, '0')}';
  final magnitude = BigInt.parse(digits.isEmpty ? '0' : digits);
  return negative ? -magnitude : magnitude;
}

final _digits = RegExp(r'^[0-9]*$');

/// Decimal `(scale, precision)` for a column of numeric [values], or `null` when
/// the values cannot be a lossless decimal (any non-finite double (NaN/∞) which
/// the caller should keep as a float instead of promoting). Integers and BigInt
/// contribute scale 0; nulls are ignored. `precision` is the max unscaled-digit
/// count and is never less than `scale` (a decimal type requires `precision ≥
/// scale`, so e.g. `0.05` yields `decimal(2,2)`, not the invalid `decimal(1,2)`).
({int scale, int precision})? inferDecimalMeta(Iterable<Object?> values) {
  var scale = 0;
  for (final v in values) {
    if (v == null) continue;
    if (v is double && !v.isFinite) return null; // NaN/∞ can't be a decimal
    final found = switch (v) {
      final Decimal d => d.scale,
      final num n => decimalParts(n).scale,
      _ => 0,
    };
    if (found > scale) scale = found;
  }
  var precision = 1;
  final factor = BigInt.from(10).pow(scale);
  for (final v in values) {
    if (v == null) continue;
    final BigInt unscaled;
    if (v is BigInt) {
      unscaled = v * factor; // already scale 0 → lift to the common scale
    } else if (v is Decimal) {
      unscaled = v.unscaled * BigInt.from(10).pow(scale - v.scale);
    } else if (v is num) {
      unscaled = toUnscaled(v, scale);
    } else {
      continue;
    }
    final digits = unscaled.abs().toString().length;
    if (digits > precision) precision = digits;
  }
  return (scale: scale, precision: precision < scale ? scale : precision);
}

/// Decode a two's-complement integer from [bytes].
BigInt bytesToBigInt(List<int> bytes, {required bool littleEndian}) {
  if (bytes.isEmpty) return BigInt.zero;
  final ordered = littleEndian
      ? bytes.reversed.toList()
      : bytes; // -> big-endian
  var result = BigInt.zero;
  for (final byte in ordered) {
    result = (result << 8) | BigInt.from(byte & 0xff);
  }
  final bits = bytes.length * 8;
  if ((result & (BigInt.one << (bits - 1))) != BigInt.zero) {
    result -= BigInt.one << bits; // sign-extend
  }
  return result;
}

/// Encode [value] as a fixed-width two's-complement integer.
Uint8List bigIntToBytes(BigInt value, int width, {required bool littleEndian}) {
  // Two's-complement of [width] bytes holds [-2^(n-1), 2^(n-1)-1]. Without
  // this check an out-of-range unscaled decimal would wrap silently and write a
  // corrupted value on disk; fail loud instead.
  final bits = width * 8;
  final limit = BigInt.one << (bits - 1);
  if (value >= limit || value < -limit) {
    throw RangeError(
      'Value $value does not fit in a $width-byte two\'s-complement integer',
    );
  }
  var v = value;
  if (v.isNegative) v += BigInt.one << (width * 8); // two's complement
  final out = Uint8List(width);
  final mask = BigInt.from(0xff);
  for (int i = width - 1; i >= 0; i--) {
    out[i] = (v & mask).toInt();
    v = v >> 8;
  }
  return littleEndian ? Uint8List.fromList(out.reversed.toList()) : out;
}
