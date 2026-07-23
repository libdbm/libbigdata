import 'dart:typed_data';
import 'dart:math' as math;

/// Bit width holding rep/def levels 0..[maxLevel] — the ONE formula for the
/// level codec, shared by the shredder, the page decoder, and the schema
/// analyzer. Integer [int.bitLength] on purpose: `ceil(log2(maxLevel+1))`
/// agrees for every small level but a float rounding at a power-of-two
/// boundary could differ across platforms/JS, and a width mismatch between
/// writer and reader corrupts every nested/optional column.
int levelBitWidth(int maxLevel) => maxLevel <= 0 ? 0 : maxLevel.bitLength;

/// Absolute ceiling on the number of values any single RLE/bit-packed decode may
/// produce. A run length is an unbounded ULEB128, so a ~5-byte header can claim
/// billions of entries from untrusted input (a size bomb). Callers pass the
/// page's true value count via [RleBitpackedHybrid.decode]'s `maxValues`; this is
/// the backstop when none is given. ~134M is far above any realistic page.
const int maxPageValues = 1 << 27;

/// RLE/Bit-packed hybrid encoding/decoding
/// Used for dictionary indices, repetition/definition levels, and boolean values
class RleBitpackedHybrid {
  /// Decode RLE/Bit-packed hybrid encoded data.
  ///
  /// [maxValues] bounds the output: a run that would push the decoded count past
  /// it (plus a one-group slack, since bit-packed runs round up to multiples of
  /// 8) is rejected as a [FormatException] rather than allocated. Callers should
  /// pass the page's declared value count; when omitted [maxPageValues] applies.
  static List<int> decode(
    Uint8List data,
    int bitWidth, {
    int? length,
    bool hasLength = false,
    int? maxValues,
  }) {
    if (bitWidth < 0 || bitWidth > 32) {
      throw ArgumentError('Invalid bit width: $bitWidth');
    }

    final cap = (maxValues == null || maxValues < 0)
        ? maxPageValues
        : (maxValues > maxPageValues ? maxPageValues : maxValues) + 8;
    // Two run kinds have different DoS risk, so they get different bounds:
    //  - an RLE run (and a bit-width-0 run) claims its length from a tiny header
    //    and materializes that many copies WITHOUT reading more input — a genuine
    //    allocation bomb, held to the tight per-page [cap].
    //  - a bit-width>=1 BIT-PACKED run must physically read runLength*bitWidth/8
    //    bytes from the already-length-bounded buffer (readBitPacked throws when
    //    it runs past), so it is self-bounded by the input and only needs the
    //    absolute [maxPageValues] backstop. Writers (e.g. DuckDB) flush levels in
    //    fixed bit-packed blocks and pad the final block, legitimately overrunning
    //    the declared count by up to one block; the caller's clamp() trims it.
    void guard(int runLength, int have, int limit) {
      if (runLength < 0 || have + runLength > limit) {
        throw FormatException(
          'RLE run length $runLength exceeds page value bound $limit',
        );
      }
    }

    // Handle bit width 0 - all values are 0
    if (bitWidth == 0) {
      // With bit width 0, we can't encode any values other than 0
      // The data should only contain run length encodings
      final reader = ByteReader(data);

      // Some encodings have a 4-byte length prefix
      if (hasLength) {
        length = reader.readInt32();
      }

      final values = <int>[];
      final endPos = length != null ? reader.position + length : data.length;

      while (reader.position < endPos && reader.hasMore()) {
        // Read header using ULEB128
        final header = reader.readULEB128();

        // With bit width 0, only RLE runs make sense
        if ((header & 1) == 0) {
          // RLE run - all values are 0
          final runLength = header >> 1;
          guard(runLength, values.length, cap);
          for (int i = 0; i < runLength; i++) {
            values.add(0);
          }
        } else {
          // Bit-packed run with bit width 0 - all values are 0. Materializes
          // runLength zeros WITHOUT reading input, so it stays on the tight cap.
          final runLength = (header >> 1) * 8;
          guard(runLength, values.length, cap);
          for (int i = 0; i < runLength; i++) {
            values.add(0);
          }
        }
      }

      return values;
    }

    final reader = ByteReader(data);

    // Some encodings have a 4-byte length prefix
    if (hasLength) {
      length = reader.readInt32();
    }

    final values = <int>[];
    final endPos = length != null ? reader.position + length : data.length;

    while (reader.position < endPos && reader.hasMore()) {
      // Read header using ULEB128
      final header = reader.readULEB128();

      if ((header & 1) == 0) {
        // RLE run — one value copied runLength times with no further reads, so
        // it keeps the tight per-page cap (the real size-bomb vector).
        final runLength = header >> 1;
        guard(runLength, values.length, cap);
        final byteWidth = (bitWidth + 7) ~/ 8;
        final value = reader.readBits(byteWidth * 8);

        for (int i = 0; i < runLength; i++) {
          values.add(value);
        }
      } else {
        // Bit-packed run — readBitPacked reads runLength*bitWidth/8 bytes from
        // the length-bounded buffer (throws when short), so it is self-bounded;
        // only the absolute backstop applies, allowing a padded final block that
        // the caller's clamp() trims.
        final runLength = (header >> 1) * 8; // Stored as count/8
        guard(runLength, values.length, maxPageValues);
        final packedValues = readBitPacked(reader, runLength, bitWidth);
        values.addAll(packedValues);
      }
    }

    return values;
  }

  /// Read bit-packed values
  static List<int> readBitPacked(ByteReader reader, int count, int bitWidth) {
    final values = <int>[];

    if (bitWidth == 0) {
      // All zeros
      for (int i = 0; i < count; i++) {
        values.add(0);
      }
      return values;
    }

    // Calculate bytes needed
    final totalBits = count * bitWidth;
    final totalBytes = (totalBits + 7) ~/ 8;
    final bytes = reader.readBytes(totalBytes);

    // Unpack values
    int bitOffset = 0;
    for (int i = 0; i < count; i++) {
      final value = extractBits(bytes, bitOffset, bitWidth);
      values.add(value);
      bitOffset += bitWidth;
    }

    return values;
  }

  /// Extract bits from byte array
  static int extractBits(Uint8List bytes, int bitOffset, int bitWidth) {
    int value = 0;
    int bitsRead = 0;

    while (bitsRead < bitWidth) {
      final byteIndex = bitOffset ~/ 8;
      final bitInByte = bitOffset % 8;
      final bitsAvailable = 8 - bitInByte;
      final bitsToRead = math.min(bitsAvailable, bitWidth - bitsRead);

      if (byteIndex >= bytes.length) {
        throw FormatException(
          'Truncated bit-packed data: ran out of bytes at bit offset $bitOffset '
          '(width $bitWidth)',
        );
      }

      // Extract bits from current byte
      final mask = (1 << bitsToRead) - 1;
      final bits = (bytes[byteIndex] >> bitInByte) & mask;

      // Add to value
      value |= bits << bitsRead;

      bitsRead += bitsToRead;
      bitOffset += bitsToRead;
    }

    return value;
  }

  /// Encode values using RLE/Bit-packed hybrid
  static Uint8List encode(List<int> values, int bitWidth) {
    if (values.isEmpty) {
      return Uint8List(0);
    }

    final buffer = BytesBuilder();
    int i = 0;

    while (i < values.length) {
      // Try RLE first
      final runLength = countRun(values, i);
      if (runLength >= 8) {
        // Use RLE
        encodeRleRun(buffer, values[i], runLength, bitWidth);
        i += runLength;
      } else {
        // Use bit-packing
        final packLength = math.min(
          ((values.length - i) ~/ 8) * 8,
          128 * 8, // Max 128 groups of 8
        );
        if (packLength > 0) {
          encodeBitPackedRun(
            buffer,
            values.sublist(i, i + packLength),
            bitWidth,
          );
          i += packLength;
        } else {
          // Handle remaining values as RLE
          encodeRleRun(buffer, values[i], 1, bitWidth);
          i++;
        }
      }
    }

    return buffer.toBytes();
  }

  /// Count consecutive equal values
  static int countRun(List<int> values, int start) {
    if (start >= values.length) return 0;

    final value = values[start];
    int count = 1;

    for (int i = start + 1; i < values.length; i++) {
      if (values[i] != value) break;
      count++;
    }

    return count;
  }

  /// Encode RLE run
  static void encodeRleRun(
    BytesBuilder buffer,
    int value,
    int runLength,
    int bitWidth,
  ) {
    // Write header (run length << 1)
    writeULEB128(buffer, runLength << 1);

    // Write value
    final byteWidth = (bitWidth + 7) ~/ 8;
    final bytes = Uint8List(byteWidth);
    for (int i = 0; i < byteWidth; i++) {
      bytes[i] = (value >> (i * 8)) & 0xFF;
    }
    buffer.add(bytes);
  }

  /// Encode bit-packed run
  static void encodeBitPackedRun(
    BytesBuilder buffer,
    List<int> values,
    int bitWidth,
  ) {
    final count = (values.length ~/ 8) * 8;

    // Write header ((count / 8) << 1 | 1)
    writeULEB128(buffer, (count ~/ 8) << 1 | 1);

    // Pack values
    final packed = packBits(values.sublist(0, count), bitWidth);
    buffer.add(packed);
  }

  /// Pack values into bits
  static Uint8List packBits(List<int> values, int bitWidth) {
    if (bitWidth == 0) {
      return Uint8List(0);
    }

    final totalBits = values.length * bitWidth;
    final totalBytes = (totalBits + 7) ~/ 8;
    final bytes = Uint8List(totalBytes);

    int bitOffset = 0;
    for (final value in values) {
      writeBits(bytes, bitOffset, value, bitWidth);
      bitOffset += bitWidth;
    }

    return bytes;
  }

  /// Write bits to byte array
  static void writeBits(
    Uint8List bytes,
    int bitOffset,
    int value,
    int bitWidth,
  ) {
    int bitsWritten = 0;

    while (bitsWritten < bitWidth) {
      final byteIndex = bitOffset ~/ 8;
      final bitInByte = bitOffset % 8;
      final bitsAvailable = 8 - bitInByte;
      final bitsToWrite = math.min(bitsAvailable, bitWidth - bitsWritten);

      if (byteIndex >= bytes.length) {
        break;
      }

      // Extract bits to write. The shift MUST be unsigned: a bit-packed value is
      // a uint64 (a DELTA_BINARY_PACKED `delta - minDelta` reaching 2^63 reads
      // back as a negative Dart int), and an arithmetic `>>` would smear its
      // sign across every remaining byte.
      final mask = (1 << bitsToWrite) - 1;
      final bits = (value >>> bitsWritten) & mask;

      // Write to byte
      bytes[byteIndex] |= bits << bitInByte;

      bitsWritten += bitsToWrite;
      bitOffset += bitsToWrite;
    }
  }

  /// Write ULEB128 encoded integer
  static void writeULEB128(BytesBuilder buffer, int value) {
    while (value >= 0x80) {
      buffer.addByte((value & 0x7F) | 0x80);
      value >>= 7;
    }
    buffer.addByte(value & 0x7F);
  }
}

/// Helper class for reading bytes
class ByteReader {
  final Uint8List data;
  int position = 0;

  ByteReader(this.data);

  bool hasMore() => position < data.length;

  int readByte() {
    // Truncated input is a format violation (a catchable domain error), not a
    // raw RangeError (which reads as an internal VM/programmer bug).
    if (position >= data.length) {
      throw const FormatException(
        'Truncated RLE/bit-packed stream: no more bytes to read',
      );
    }
    return data[position++];
  }

  Uint8List readBytes(int count) {
    if (position + count > data.length) {
      throw FormatException(
        'Truncated RLE/bit-packed stream: need $count bytes at $position, '
        'only ${data.length - position} left',
      );
    }
    final bytes = data.sublist(position, position + count);
    position += count;
    return bytes;
  }

  int readInt32() {
    final bytes = readBytes(4);
    return ByteData.view(
      bytes.buffer,
      bytes.offsetInBytes,
    ).getInt32(0, Endian.little);
  }

  int readBits(int bitCount) {
    final byteCount = (bitCount + 7) ~/ 8;
    final bytes = readBytes(byteCount);
    int value = 0;
    for (int i = 0; i < byteCount; i++) {
      value |= bytes[i] << (i * 8);
    }
    return value;
  }

  int readULEB128() {
    int value = 0;
    int shift = 0;

    while (true) {
      final byte = readByte();
      value |= (byte & 0x7F) << shift;
      if ((byte & 0x80) == 0) {
        break;
      }
      shift += 7;
      if (shift > 35) {
        throw FormatException('ULEB128 value too large');
      }
    }

    return value;
  }
}
