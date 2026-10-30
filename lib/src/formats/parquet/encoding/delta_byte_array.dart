import 'dart:typed_data';
import 'byte_reader.dart';
import 'delta_binary_packed.dart';
import '../reader/page_reader.dart' show defaultMaxParquetPageBytes;

/// Delta Byte Array decoder
/// Used for byte arrays with common prefixes
class DeltaByteArrayDecoder {
  /// Decode delta byte array data.
  ///
  /// Prefix reuse lets a small page claim far more output than it holds, so
  /// the total decoded size is bounded by [limit] bytes.
  static List<Uint8List> decode(
    Uint8List data,
    int count, {
    int limit = defaultMaxParquetPageBytes,
  }) {
    if (data.isEmpty || count == 0) {
      return [];
    }

    final reader = ByteReader(data, label: 'DELTA_BYTE_ARRAY stream');
    final values = <Uint8List>[];

    // Two consecutive delta binary packed streams: prefix lengths then suffix
    // lengths. They are self-delimiting, so share the reader's position.
    final prefixes = DeltaBinaryPackedDecoder.decodeFrom(reader, count);
    final suffixes = DeltaBinaryPackedDecoder.decodeFrom(reader, count);
    // A stream that declared fewer values than `count` would otherwise index
    // past the length lists below with a raw RangeError.
    if (prefixes.length < count || suffixes.length < count) {
      throw FormatException(
        'Delta byte array: length stream shorter than $count values',
      );
    }

    // Read suffix data
    Uint8List previous = Uint8List(0);
    var total = 0;

    for (int i = 0; i < count; i++) {
      final prefix = prefixes[i];
      final suffix = suffixes[i];

      // Validate every length before allocating. A prefix longer than the
      // previous value is a structural violation, not something to zero-fill.
      if (prefix < 0 || suffix < 0) {
        throw FormatException(
          'Delta byte array: negative length (prefix $prefix, '
          'suffix $suffix)',
        );
      }
      if (previous.length < prefix) {
        throw FormatException(
          'Delta prefix length $prefix exceeds previous value length ${previous.length}',
        );
      }
      if (reader.position + suffix > reader.data.length) {
        throw FormatException(
          'Delta byte array: suffix of $suffix bytes runs past the page',
        );
      }
      total += prefix + suffix;
      if (total > limit) {
        throw FormatException(
          'Delta byte array: decoded size exceeds the $limit-byte limit',
        );
      }

      // Create new value
      final value = Uint8List(prefix + suffix);

      if (prefix > 0) {
        value.setRange(0, prefix, previous);
      }

      // Copy the suffix straight from the page buffer (transient: no temp copy).
      if (suffix > 0) {
        value.setRange(prefix, prefix + suffix, reader.data, reader.position);
        reader.position += suffix;
      }

      values.add(value);
      previous = value;
    }

    return values;
  }
}

/// Delta Length Byte Array decoder
/// Used for byte arrays with similar lengths
class DeltaLengthByteArrayDecoder {
  /// Decode delta length byte array data
  static List<Uint8List> decode(Uint8List data, int count) {
    if (data.isEmpty || count == 0) {
      return [];
    }

    final reader = ByteReader(data, label: 'DELTA_BYTE_ARRAY stream');
    final values = <Uint8List>[];

    // A delta binary packed stream of lengths, followed by the concatenated
    // bytes; share the reader's position so the bytes start right after.
    final lengths = DeltaBinaryPackedDecoder.decodeFrom(reader, count);
    if (lengths.length < count) {
      throw FormatException(
        'Delta length byte array: length stream shorter than $count values',
      );
    }

    // Read the actual byte arrays
    for (int i = 0; i < count; i++) {
      final length = lengths[i];

      if (length < 0) {
        throw FormatException('Invalid negative length: $length');
      }

      if (reader.position + length > reader.data.length) {
        throw FormatException(
          'Delta length byte array: value of $length bytes runs past the page',
        );
      }

      final value = reader.data.sublist(
        reader.position,
        reader.position + length,
      );
      values.add(value);
      reader.position += length;
    }

    return values;
  }
}
