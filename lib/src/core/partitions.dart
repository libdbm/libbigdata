import 'dart:typed_data';

import 'conversions.dart' show formatDecimal, unscaledFromString;
import 'data_type.dart';

// Partition-value encoding. There are TWO conventions here and they are NOT
// interchangeable: conflating them under "one source of truth" would send a
// null partition value to Spark as the literal text
// "__HIVE_DEFAULT_PARTITION__":
//
//  * the Hive `col=value` PATH segment, shared by the Delta/Iceberg writers and
//    the Delta/Dataset readers ([encodePartition] / [isNullPartition]);
//  * the Delta protocol's `partitionValues` MAP ([encodePartitionValue] /
//    [isNullPartitionValue]), whose rules are fixed by the spec's
//    "Partition Value Serialization" section.

/// The marker Hive/Spark write in a `col=value` path segment for a NULL value.
const String hiveDefaultPartition = '__HIVE_DEFAULT_PARTITION__';

/// Encode a partition value for a `col=value` PATH segment: null → the Hive
/// default marker, else the value's text with Hive path escaping. Given the
/// column's [type] (and decimal [scale]), a date, timestamp or decimal takes
/// the same text as in [encodePartitionValue] (`2024-01-01`, `1.50`) rather
/// than Dart's `toString` (`2024-01-01 00:00:00.000Z`, the unscaled `150`).
///
/// This names a DIRECTORY. A Delta `add` action's `partitionValues` map is a
/// different serialization: use [encodePartitionValue] for that.
String encodePartition(Object? value, {DataType? type, int scale = 0}) =>
    value == null
    ? hiveDefaultPartition
    : escapePathName(
        type == null
            ? value.toString()
            : encodePartitionValue(value, type, scale: scale),
      );

/// Characters Hive/Spark escape in a partition path segment (escapePathName):
/// path metacharacters plus anything a filesystem or URI treats specially.
const String _pathSpecial = '"#%\'*/:=?\\{[]^';

/// Hive-style path escaping for a `col=value` segment VALUE: each special or
/// control character becomes `%XX` (uppercase hex). Without this a value like
/// `50%` was written RAW into the directory name and the add.path, which the
/// reader (and the Delta spec) treat as URL-encoded, so reading the table just
/// written threw FormatException, and `a/b` silently became a nested directory.
/// Matches Spark's on-disk layout; non-ASCII stays raw (legal in paths).
String escapePathName(String value) {
  final buffer = StringBuffer();
  for (final char in value.split('')) {
    final code = char.codeUnitAt(0);
    if (code < 0x20 || code == 0x7F || _pathSpecial.contains(char)) {
      buffer.write('%${code.toRadixString(16).toUpperCase().padLeft(2, '0')}');
    } else {
      buffer.write(char);
    }
  }
  return buffer.toString();
}

/// Inverse of [escapePathName] for READING a `col=value` segment value:
/// decodes `%XX` sequences; malformed sequences are kept literally (old tables
/// were written unescaped, and they must keep reading).
String unescapePathName(String value) {
  if (!value.contains('%')) return value;
  final buffer = StringBuffer();
  for (int i = 0; i < value.length; i++) {
    final char = value[i];
    if (char == '%' &&
        i + 3 <= value.length &&
        _isHex(value.codeUnitAt(i + 1)) &&
        _isHex(value.codeUnitAt(i + 2))) {
      buffer.writeCharCode(int.parse(value.substring(i + 1, i + 3), radix: 16));
      i += 2;
      continue;
    }
    buffer.write(char);
  }
  return buffer.toString();
}

/// Whether a raw `col=value` PATH segment [value] denotes a NULL partition
/// value: the Hive default marker, the legacy literal `'null'`, or an absent
/// segment.
bool isNullPartition(String? value) =>
    value == null || value == hiveDefaultPartition || value == 'null';

/// Encode a partition value for a Delta `add`/`remove` action's
/// `partitionValues` MAP, per the Delta protocol's Partition Value
/// Serialization.
///
/// Every value is a string and the format is per-type: null → `''` ("An empty
/// string for any type translates to a null partition value", so a genuinely
/// empty string is not representable, which is the spec's own quirk); date →
/// `yyyy-MM-dd`; timestamp → ISO8601 adjusted to UTC (the form the spec
/// recommends for modern writers); boolean → `true`/`false`; numeric → the
/// number's string form; binary → escaped bytes; decimal → the value written
/// out at [scale] (it is carried as an unscaled BigInt, so its raw `toString`
/// would emit 12345 for 123.45).
///
/// NOT the Hive marker: Spark casts this string straight to the column's type,
/// so `__HIVE_DEFAULT_PARTITION__` reads back as literal text on a string column
/// instead of null.
String encodePartitionValue(Object? value, DataType type, {int scale = 0}) {
  if (value == null) return '';
  switch (type) {
    case DataType.date:
      // The calendar day the value names, as the Parquet path stores it
      // (dateToEpochDays): converting a local DateTime to UTC first moved it
      // a day east or west of Greenwich, and stats then disagreed with data.
      if (value is DateTime) {
        return '${_pad(value.year, 4)}-${_pad(value.month, 2)}-'
            '${_pad(value.day, 2)}';
      }
      return value.toString();
    case DataType.timestamp:
      // toIso8601String() on a UTC DateTime yields `…THH:MM:SS.mmmZ`, which is
      // the "adjusted to UTC, ISO8601" form the spec recommends.
      return value is DateTime
          ? value.toUtc().toIso8601String()
          : value.toString();
    case DataType.decimal:
      return value is BigInt ? formatDecimal(value, scale) : value.toString();
    case DataType.binary:
      return value is List<int>
          ? [
              for (final byte in value)
                '\\u${byte.toRadixString(16).padLeft(4, '0')}',
            ].join()
          : value.toString();
    default:
      // string / numeric / boolean: the value's own string form already IS the
      // spec's format (Dart renders bools as `true`/`false`).
      return value.toString();
  }
}

/// A `partitionValues` MAP entry parsed into the column's declared logical
/// type: the ONE codec for every read AND mutation path. (The reader had a
/// private copy; the Delta `delete` path had none at all, so an int-partition
/// predicate compared `5` against the raw string `'5'` and silently deleted
/// nothing.)
///
/// Malformed input FAILS LOUD with a [FormatException] naming the column,
/// declared type, and offending text: the old tryParse-to-null arms fabricated
/// data from corrupt or foreign metadata (`'tru'` read back as boolean false,
/// `'12x'` as null) instead of rejecting the table.
///
/// Every arm produces the type the schema declares: the reconstructed
/// partition column is built with `columnFromValues(name, type, …)`, which
/// does NOT type-check its input.
Object? decodePartitionValue(
  String? value,
  DataType type, {
  int scale = 0,
  String? column,
}) {
  if (isNullPartitionValue(value)) return null;
  final raw = value!;
  Never bad() => throw FormatException(
    'Malformed partition value "$raw"'
    '${column == null ? '' : ' for column "$column"'} '
    '(declared type ${type.name})',
  );
  switch (type) {
    case DataType.int8:
    case DataType.int16:
    case DataType.int32:
    case DataType.int64:
      return int.tryParse(raw) ?? bad();
    case DataType.float32:
    case DataType.float64:
      return double.tryParse(raw) ?? bad();
    case DataType.boolean:
      if (raw == 'true') return true;
      if (raw == 'false') return false;
      bad();
    case DataType.date:
      // A date carries no time and no zone, so it is rebuilt as UTC midnight.
      // DateTime.parse defaults an unzoned string to LOCAL, and toUtc() would
      // then shift it across the day boundary west of Greenwich.
      final day = DateTime.tryParse(raw) ?? bad();
      return DateTime.utc(day.year, day.month, day.day);
    case DataType.timestamp:
      // The writer emits ISO8601-UTC. Spark may instead write an unzoned
      // `yyyy-MM-dd HH:mm:ss[.ffffff]`, which the spec says to read in the
      // writing system's zone, which is what parsing it as local then
      // converting does.
      return (DateTime.tryParse(raw) ?? bad()).toUtc();
    case DataType.decimal:
      // A decimal column holds an UNSCALED BigInt. Parse the partition STRING
      // exactly: going through num/double would round a high-precision value
      // past 2^53. Over-precise/malformed input already fails loud here.
      return unscaledFromString(raw, scale);
    case DataType.binary:
      // The escaped form [encodePartitionValue] writes: `\u0001\u0002`.
      if (raw.length % 6 != 0) bad();
      return Uint8List.fromList([
        for (var i = 0; i < raw.length; i += 6)
          raw.startsWith(r'\u', i)
              ? (int.tryParse(raw.substring(i + 2, i + 6), radix: 16) ?? bad())
              : bad(),
      ]);
    default:
      return raw;
  }
}

/// Whether a `partitionValues` MAP entry denotes NULL.
///
/// The spec's form is the empty string. The Hive marker and the literal `'null'`
/// are also accepted because THIS library wrote the marker into the map before
/// the two conventions were separated; those tables must keep reading.
bool isNullPartitionValue(String? value) =>
    value == null ||
    value.isEmpty ||
    value == hiveDefaultPartition ||
    value == 'null';

String _pad(int value, int width) => value.toString().padLeft(width, '0');

bool _isHex(int code) =>
    (code >= 0x30 && code <= 0x39) || // 0-9
    (code >= 0x41 && code <= 0x46) || // A-F
    (code >= 0x61 && code <= 0x66); // a-f
