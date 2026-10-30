import 'dart:math' as math;
import 'dart:typed_data';

import '../../../util/varint.dart' as varint;

/// Reads the bytes of one Parquet encoded stream (RLE/bit-packed,
/// DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY). Truncated input is a
/// [FormatException] naming [label], never a raw RangeError.
class ByteReader {
  final Uint8List data;
  final String label;
  int position = 0;

  ByteReader(this.data, {this.label = 'Parquet stream'});

  bool hasMore() => position < data.length;

  int readByte() {
    if (position >= data.length) {
      throw FormatException('Truncated $label: no more bytes to read');
    }
    return data[position++];
  }

  Uint8List readBytes(int count) {
    if (position + count > data.length) {
      throw FormatException(
        'Truncated $label: need $count bytes at $position, only '
        '${data.length - position} left',
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

  /// A little-endian unsigned value in the next `ceil(bitCount / 8)` bytes.
  int readBits(int bitCount) {
    final byteCount = (bitCount + 7) ~/ 8;
    final bytes = readBytes(byteCount);
    int value = 0;
    for (int i = 0; i < byteCount; i++) {
      value |= bytes[i] << (i * 8);
    }
    return value;
  }

  /// A base-128 varint of at most [maxBytes] bytes (5 for a 32-bit header).
  int readVarint({int maxBytes = 10}) {
    final (:value, :next) = varint.readVarint(
      data,
      position,
      maxBytes: maxBytes,
      label: label,
    );
    position = next;
    return value;
  }

  /// A zigzag-encoded varint.
  int readZigzag() => varint.unzigzag(readVarint());

  /// Advance past [count] bit-packed values of [bitWidth] without decoding
  /// them, consuming the same bytes [readBitPacked] would.
  void skipBitPacked(int count, int bitWidth) {
    final bytes = (count * bitWidth + 7) ~/ 8;
    if (position + bytes > data.length) {
      throw FormatException('Truncated $label: not enough bit-packed bytes');
    }
    position += bytes;
  }

  /// [count] bit-packed integers of [bitWidth] bits.
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
            throw FormatException(
              'Truncated $label: not enough bit-packed bytes',
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
