import 'dart:typed_data';
import 'delta_binary_packed.dart';

/// Delta Byte Array decoder
/// Used for byte arrays with common prefixes
class DeltaByteArrayDecoder {
  /// Decode delta byte array data
  static List<Uint8List> decode(Uint8List data, int count) {
    if (data.isEmpty || count == 0) {
      return [];
    }

    final reader = ByteReader(data);
    final values = <Uint8List>[];

    // Two consecutive delta binary packed streams: prefix lengths then suffix
    // lengths. They are self-delimiting, so share the reader's position.
    final prefixLengths = DeltaBinaryPackedDecoder.decodeFrom(reader, count);
    final suffixLengths = DeltaBinaryPackedDecoder.decodeFrom(reader, count);
    // A stream that declared fewer values than `count` would otherwise index
    // past the length lists below with a raw RangeError.
    if (prefixLengths.length < count || suffixLengths.length < count) {
      throw FormatException(
        'Delta byte array: length stream shorter than $count values',
      );
    }

    // Read suffix data
    Uint8List previousValue = Uint8List(0);

    for (int i = 0; i < count; i++) {
      final prefixLen = prefixLengths[i];
      final suffixLen = suffixLengths[i];

      // Create new value
      final newValue = Uint8List(prefixLen + suffixLen);

      // Copy prefix from previous value. A prefix longer than the previous value
      // is a structural violation, not something to silently zero-fill.
      if (prefixLen > 0) {
        if (previousValue.length < prefixLen) {
          throw FormatException(
            'Delta prefix length $prefixLen exceeds previous value length ${previousValue.length}',
          );
        }
        newValue.setRange(0, prefixLen, previousValue);
      }

      // Copy the suffix straight from the page buffer (transient — no temp copy).
      if (suffixLen > 0) {
        if (reader.position + suffixLen > reader.data.length) {
          throw RangeError('Not enough bytes for suffix');
        }

        newValue.setRange(
          prefixLen,
          prefixLen + suffixLen,
          reader.data,
          reader.position,
        );
        reader.position += suffixLen;
      }

      values.add(newValue);
      previousValue = newValue;
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

    final reader = ByteReader(data);
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
        throw RangeError('Not enough bytes for value of length $length');
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
