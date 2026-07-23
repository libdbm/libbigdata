import 'dart:typed_data';

/// Value conversions for temporal and decimal column types, shared by the Arrow
/// and Parquet codecs. Temporal columns hold Dart [DateTime]; decimal columns
/// hold unscaled [BigInt] (scale lives on the Field).

final DateTime _epochUtc = DateTime.utc(1970);

/// `date` → days since 1970-01-01 (UTC, calendar date only).
int dateToEpochDays(DateTime d) =>
    DateTime.utc(d.year, d.month, d.day).difference(_epochUtc).inDays;

DateTime dateFromEpochDays(int days) => _epochUtc.add(Duration(days: days));

/// `timestamp` → microseconds since the Unix epoch (UTC).
int timestampToEpochMicros(DateTime t) => t.toUtc().microsecondsSinceEpoch;

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
  final bd = ByteData.sublistView(Uint8List.fromList(bytes));
  final nanos = bd.getInt64(0, Endian.little);
  final julianDay = bd.getInt32(8, Endian.little);
  const unixEpochJulianDay = 2440588;
  return _epochUtc.add(
    Duration(days: julianDay - unixEpochJulianDay, microseconds: nanos ~/ 1000),
  );
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
  final frac = digits.substring(cut);
  return '${negative ? '-' : ''}$whole.$frac';
}

/// Decompose a numeric [value] into `(unscaled, scale)` via its SHORTEST
/// round-trip decimal string, so `0.5` → `(5, 1)` (not the binary tail
/// `0.5000000…`) and an integer is exact with scale 0. Used to promote a
/// mixed integer/float column to a lossless `decimal` (a large int mixed with a
/// float would otherwise round under float64). Throws on a non-finite double.
({BigInt unscaled, int scale}) decimalParts(num value) {
  if (value is int) return (unscaled: BigInt.from(value), scale: 0);
  final d = value.toDouble();
  if (!d.isFinite) {
    throw StateError('Cannot represent $d exactly as a decimal');
  }
  var s = d.toString(); // shortest round-trip representation
  final negative = s.startsWith('-');
  if (negative) s = s.substring(1);
  var exp = 0;
  final e = s.indexOf('e');
  if (e >= 0) {
    exp = int.parse(s.substring(e + 1));
    s = s.substring(0, e);
  }
  var scale = 0;
  final dot = s.indexOf('.');
  if (dot >= 0) {
    scale = s.length - dot - 1;
    s = s.substring(0, dot) + s.substring(dot + 1);
  }
  scale -= exp; // a positive exponent shrinks the scale (adds trailing zeros)
  var unscaled = BigInt.parse(s);
  if (scale < 0) {
    unscaled *= BigInt.from(10).pow(-scale);
    scale = 0;
  }
  return (unscaled: negative ? -unscaled : unscaled, scale: scale);
}

/// The max fractional scale needed to represent every value in [values] exactly
/// (integers contribute 0). Nulls are ignored.
int decimalScaleOf(Iterable<num?> values) {
  var scale = 0;
  for (final v in values) {
    if (v == null) continue;
    final s = decimalParts(v).scale;
    if (s > scale) scale = s;
  }
  return scale;
}

/// [value] as an unscaled [BigInt] at [scale] (pads with trailing zeros for a
/// value of smaller natural scale). Lossless for integers and shortest-form
/// doubles. Throws if [scale] is SMALLER than the value's natural scale — that
/// would silently drop fractional digits, so the caller must pick a scale that
/// covers every value (see [decimalScaleOf]).
BigInt toUnscaled(num value, int scale) {
  final p = decimalParts(value);
  final diff = scale - p.scale;
  if (diff < 0) {
    throw ArgumentError(
      'toUnscaled: target scale $scale is smaller than the natural scale '
      '${p.scale} of $value (would lose precision)',
    );
  }
  return diff == 0 ? p.unscaled : p.unscaled * BigInt.from(10).pow(diff);
}

/// Re-scale an unscaled decimal [value] from scale [from] to [to]. Widening
/// pads (exact); narrowing is allowed only when no non-zero fractional digit
/// is dropped — otherwise it throws rather than silently rounding a type whose
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

/// Parse a `"decimal(p,s)"` type string (the Spark and Iceberg spelling) to
/// `(precision, scale)`, defaulting to `(38, 0)` when the string isn't that form.
/// One source for both format schema parsers, which spelled the identical regex.
({int precision, int scale}) decimalTypeParams(String type) {
  final m = RegExp(r'decimal\((\d+)\s*,\s*(\d+)\)').firstMatch(type);
  if (m == null) return (precision: 38, scale: 0);
  return (precision: int.parse(m.group(1)!), scale: int.parse(m.group(2)!));
}

/// Parse a decimal STRING (`"123.45"`, `"-0.99"`, `"12345678901234567890"`) to an
/// unscaled [BigInt] at [scale], EXACTLY — never via a double, which would round
/// a value past 2^53 (or throw on a clean-double round-trip through [toUnscaled]).
/// The string inverse of [formatDecimal]. Throws [FormatException] on a non-decimal
/// string or one carrying MORE fractional digits than [scale] can hold (the same
/// fail-loud contract as [toUnscaled], so a foreign writer's over-precise partition
/// value is rejected, not silently rounded).
BigInt unscaledFromString(String s, int scale) {
  final text = s.trim();
  final negative = text.startsWith('-');
  final body = negative || text.startsWith('+') ? text.substring(1) : text;
  final dot = body.indexOf('.');
  final intPart = dot < 0 ? body : body.substring(0, dot);
  final fracPart = dot < 0 ? '' : body.substring(dot + 1);
  if (intPart.isEmpty && fracPart.isEmpty) {
    throw FormatException('Not a decimal value: "$s"');
  }
  if (fracPart.length > scale) {
    throw FormatException(
      'Decimal "$s" has ${fracPart.length} fractional digits, more than the '
      'declared scale $scale can represent without rounding',
    );
  }
  final digits = '$intPart${fracPart.padRight(scale, '0')}';
  final magnitude = BigInt.parse(digits.isEmpty ? '0' : digits);
  return negative ? -magnitude : magnitude;
}

/// Decimal `(scale, precision)` for a column of numeric [values], or `null` when
/// the values cannot be a lossless decimal (any non-finite double — NaN/∞ — which
/// the caller should keep as a float instead of promoting). Integers and BigInt
/// contribute scale 0; nulls are ignored. `precision` is the max unscaled-digit
/// count and is never less than `scale` (a decimal type requires `precision ≥
/// scale`, so e.g. `0.05` yields `decimal(2,2)`, not the invalid `decimal(1,2)`).
({int scale, int precision})? inferDecimalMeta(Iterable<Object?> values) {
  var scale = 0;
  for (final v in values) {
    if (v == null) continue;
    if (v is double && !v.isFinite) return null; // NaN/∞ can't be a decimal
    if (v is num) {
      final s = decimalParts(v).scale;
      if (s > scale) scale = s;
    }
  }
  var precision = 1;
  final pow = BigInt.from(10).pow(scale);
  for (final v in values) {
    if (v == null) continue;
    final BigInt unscaled;
    if (v is BigInt) {
      unscaled = v * pow; // already scale 0 → lift to the common scale
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
  for (final b in ordered) {
    result = (result << 8) | BigInt.from(b & 0xff);
  }
  final bitLength = bytes.length * 8;
  if ((result & (BigInt.one << (bitLength - 1))) != BigInt.zero) {
    result -= BigInt.one << bitLength; // sign-extend
  }
  return result;
}

/// Encode [value] as a fixed-width two's-complement integer.
Uint8List bigIntToBytes(
  BigInt value,
  int byteWidth, {
  required bool littleEndian,
}) {
  // Two's-complement of [byteWidth] bytes holds [-2^(n-1), 2^(n-1)-1]. Without
  // this check an out-of-range unscaled decimal would wrap silently and write a
  // corrupted value on disk; fail loud instead.
  final bits = byteWidth * 8;
  final limit = BigInt.one << (bits - 1);
  if (value >= limit || value < -limit) {
    throw RangeError(
      'Value $value does not fit in a $byteWidth-byte two\'s-complement integer',
    );
  }
  var v = value;
  if (v.isNegative) v += BigInt.one << (byteWidth * 8); // two's complement
  final out = Uint8List(byteWidth);
  final mask = BigInt.from(0xff);
  for (int i = byteWidth - 1; i >= 0; i--) {
    out[i] = (v & mask).toInt();
    v = v >> 8;
  }
  return littleEndian ? Uint8List.fromList(out.reversed.toList()) : out;
}
