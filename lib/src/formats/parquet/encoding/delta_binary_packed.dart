import 'dart:typed_data';
import 'dart:math' as math;

import 'rle_bitpacked.dart' show maxPageValues;

/// Delta Binary Packed decoder for integers
/// Based on Parquet specification for DELTA_BINARY_PACKED encoding
class DeltaBinaryPackedDecoder {
  /// Decode delta binary packed data
  /// [bits] is the column's physical width. Delta arithmetic wraps in the TYPE's
  /// width per the spec, so an INT32 column must reconstruct with 32-bit
  /// wrapping — the default 64 keeps INT64 (and the DELTA_*_BYTE_ARRAY length
  /// streams, whose values are small and positive) exactly as before.
  static List<int> decode(Uint8List data, int count, {int bits = 64}) {
    if (data.isEmpty || count == 0) {
      return [];
    }
    return decodeFrom(ByteReader(data), count, bits: bits);
  }

  /// Decode a delta binary packed stream from a shared [reader], advancing its
  /// position exactly to the end of the stream. This lets composite encodings
  /// (DELTA_BYTE_ARRAY, DELTA_LENGTH_BYTE_ARRAY) read consecutive streams.
  static List<int> decodeFrom(ByteReader reader, int count, {int bits = 64}) {
    // Header: block size, miniblocks per block, total value count, first value.
    // All four are untrusted ULEB128s; validate before deriving sizes from them.
    final blockSize = reader.readUleb128();
    final miniblocksPerBlock = reader.readUleb128();
    final totalValues = reader.readUleb128();
    if (miniblocksPerBlock <= 0) {
      throw const FormatException(
        'DELTA_BINARY_PACKED: miniblocks-per-block must be positive',
      );
    }
    if (totalValues < 0 || totalValues > maxPageValues) {
      throw FormatException(
        'DELTA_BINARY_PACKED: implausible value count $totalValues',
      );
    }
    final firstValue = reader.readZigzagVarlong();

    final values = <int>[firstValue];
    if (totalValues <= 1) {
      return values.length > count ? values.sublist(0, count) : values;
    }

    final valuesPerMiniblock = blockSize ~/ miniblocksPerBlock;
    if (valuesPerMiniblock <= 0 || valuesPerMiniblock > maxPageValues) {
      throw FormatException(
        'DELTA_BINARY_PACKED: implausible miniblock size $valuesPerMiniblock',
      );
    }
    int read = 1;

    // Each block: one min delta, then a bit width per miniblock, then the
    // miniblock bodies. Every present miniblock body packs exactly
    // valuesPerMiniblock values (zero-padded), so it is byte aligned.
    while (read < totalValues) {
      final minDelta = reader.readZigzagVarlong();
      final bitWidths = [
        for (int i = 0; i < miniblocksPerBlock; i++) reader.readByte(),
      ];

      for (int i = 0; i < miniblocksPerBlock && read < totalValues; i++) {
        final deltas = reader.readBitPacked(valuesPerMiniblock, bitWidths[i]);
        for (final delta in deltas) {
          if (read >= totalValues) break;
          final next = values.last + minDelta + delta;
          // Wrap in the column's width, mirroring the encoder: the spec makes
          // this arithmetic 2's-complement wrapping, and for INT32 the sum can
          // legitimately leave int32 range on the way back.
          values.add(bits == 64 ? next : next.toSigned(bits));
          read++;
        }
      }
    }

    return values.length > count ? values.sublist(0, count) : values;
  }
}

/// Helper class for reading bytes and various encodings
class ByteReader {
  final Uint8List data;
  int position = 0;

  ByteReader(this.data);

  bool hasMore() => position < data.length;

  int readByte() {
    if (position >= data.length) {
      throw const FormatException(
        'Truncated DELTA_BINARY_PACKED stream: not enough bytes to read',
      );
    }
    return data[position++];
  }

  /// Read ULEB128 encoded integer.
  ///
  /// A varint that runs off the end of the buffer is TRUNCATED input, not a
  /// smaller number: returning the bits gathered so far handed back a
  /// fabricated value and left the caller to misinterpret it downstream. Fail
  /// loud, like [readByte] and [readBitPacked] in this class already do.
  int readUleb128() {
    int result = 0;
    int shift = 0;
    var terminated = false;

    while (position < data.length) {
      final byte = data[position++];
      result |= (byte & 0x7F) << shift;

      if ((byte & 0x80) == 0) {
        terminated = true;
        break;
      }

      shift += 7;
      if (shift > 63) {
        // Allow up to 64-bit integers
        throw FormatException('ULEB128 too large');
      }
    }
    if (!terminated) {
      throw const FormatException(
        'Truncated DELTA_BINARY_PACKED stream: a ULEB128 ran past the end of '
        'the buffer with its continuation bit still set',
      );
    }

    return result;
  }

  /// Read zigzag encoded varlong
  int readZigzagVarlong() {
    final encoded = readUleb128();
    // Decode zigzag: (n >>> 1) ^ -(n & 1). The shift MUST be unsigned: zigzag
    // maps int64 onto uint64, so a value with bit 63 set reads back as a
    // negative Dart int, and an arithmetic `>>` would smear the sign bit across
    // the result. Reached whenever a value or delta is >= 2^62 (the encoder's
    // `n << 1` overflows into that range) — the other zigzag decoders in this
    // codebase (Avro, thrift compact) all use `>>>`.
    return (encoded >>> 1) ^ -(encoded & 1);
  }

  /// Read bit-packed integers
  List<int> readBitPacked(int count, int bitWidth) {
    if (bitWidth == 0) {
      return List.filled(count, 0);
    }

    final values = <int>[];
    int currentByte = 0;
    int bitsInByte = 0;
    int currentValue = 0;
    int bitsInValue = 0;

    for (int i = 0; i < count; i++) {
      while (bitsInValue < bitWidth) {
        if (bitsInByte == 0) {
          if (position >= data.length) {
            throw const FormatException(
              'Truncated DELTA_BINARY_PACKED stream: not enough bytes for '
              'bit-packed data',
            );
          }
          currentByte = data[position++];
          bitsInByte = 8;
        }

        final bitsToTake = math.min(bitWidth - bitsInValue, bitsInByte);
        final mask = (1 << bitsToTake) - 1;

        currentValue |= ((currentByte & mask) << bitsInValue);
        currentByte >>= bitsToTake;
        bitsInByte -= bitsToTake;
        bitsInValue += bitsToTake;
      }

      values.add(currentValue);
      currentValue = 0;
      bitsInValue = 0;
    }

    return values;
  }
}
