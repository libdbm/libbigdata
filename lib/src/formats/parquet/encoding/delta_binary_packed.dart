import 'dart:typed_data';
import 'dart:math' as math;

import 'byte_reader.dart';
import 'rle_bitpacked.dart' show maxPageValues;

/// Delta Binary Packed decoder for integers
/// Based on Parquet specification for DELTA_BINARY_PACKED encoding
class DeltaBinaryPackedDecoder {
  /// Decode delta binary packed data
  /// [bits] is the column's physical width. Delta arithmetic wraps in the TYPE's
  /// width per the spec, so an INT32 column must reconstruct with 32-bit
  /// wrapping: the default 64 keeps INT64 (and the DELTA_*_BYTE_ARRAY length
  /// streams, whose values are small and positive) exactly as before.
  static List<int> decode(Uint8List data, int count, {int bits = 64}) {
    if (data.isEmpty || count == 0) {
      return [];
    }
    return decodeFrom(
      ByteReader(data, label: 'DELTA_BINARY_PACKED stream'),
      count,
      bits: bits,
    );
  }

  /// Decode up to [count] values from [data] straight into [into] at [start],
  /// returning how many the stream held. The typed counterpart of [decode],
  /// with its checks: the header is validated, values past [count] are not
  /// materialized but their bytes must be present, and [bits] sets the
  /// wrapping width. The running value stays in a local and each miniblock is
  /// unpacked inline from the bytes.
  static int decodeInto(
    Uint8List data,
    int count,
    Int64List into,
    int start, {
    int bits = 64,
  }) {
    if (data.isEmpty || count == 0) return 0;
    final reader = ByteReader(data, label: 'DELTA_BINARY_PACKED stream');
    final (
      miniblocks: miniblocksPerBlock,
      span: valuesPerMiniblock,
      total: totalValues,
    ) = _header(
      reader,
    );
    var last = reader.readZigzag();
    into[start] = last;
    var written = 1;
    if (totalValues <= 1) return written;

    final widths = Uint8List(miniblocksPerBlock);
    final bytes = reader.data;
    var read = 1;
    while (read < totalValues) {
      final minDelta = reader.readZigzag();
      for (var i = 0; i < miniblocksPerBlock; i++) {
        widths[i] = _width(reader.readByte(), bits);
      }
      for (var i = 0; i < miniblocksPerBlock && read < totalValues; i++) {
        final width = widths[i];
        final span = math.min(valuesPerMiniblock, totalValues - read);
        final want = math.min(span, count - written);
        final size = (valuesPerMiniblock * width + 7) ~/ 8;
        if (reader.position + size > bytes.length) {
          throw const FormatException(
            'Truncated DELTA_BINARY_PACKED stream: not enough bytes for '
            'bit-packed data',
          );
        }
        if (width > 56) {
          // Rare: a delta wider than the inline buffer holds.
          final deltas = reader.readBitPacked(valuesPerMiniblock, width);
          for (var k = 0; k < want; k++) {
            final value = last + minDelta + deltas[k];
            last = bits == 64 ? value : value.toSigned(bits);
            into[start + written++] = last;
          }
        } else {
          final mask = (1 << width) - 1;
          var position = reader.position;
          var buffer = 0;
          var held = 0;
          for (var k = 0; k < want; k++) {
            while (held < width) {
              buffer |= bytes[position++] << held;
              held += 8;
            }
            final value = last + minDelta + (buffer & mask);
            buffer >>>= width;
            held -= width;
            last = bits == 64 ? value : value.toSigned(bits);
            into[start + written++] = last;
          }
          reader.position += size;
        }
        read += span;
      }
    }
    return written;
  }

  /// Decode a delta binary packed stream from a shared [reader], advancing its
  /// position exactly to the end of the stream. This lets composite encodings
  /// (DELTA_BYTE_ARRAY, DELTA_LENGTH_BYTE_ARRAY) read consecutive streams.
  static List<int> decodeFrom(ByteReader reader, int count, {int bits = 64}) {
    // Header: block size, miniblocks per block, total value count, first value.
    // All four are untrusted ULEB128s; validate before deriving sizes from them.
    final (
      miniblocks: miniblocksPerBlock,
      span: valuesPerMiniblock,
      total: totalValues,
    ) = _header(
      reader,
    );
    final first = reader.readZigzag();

    final values = <int>[first];
    if (totalValues <= 1) {
      return values.length > count ? values.sublist(0, count) : values;
    }

    int read = 1;

    // Each block: one min delta, then a bit width per miniblock, then the
    // miniblock bodies. Every present miniblock body packs exactly
    // valuesPerMiniblock values (zero-padded), so it is byte aligned.
    while (read < totalValues) {
      final minDelta = reader.readZigzag();
      final widths = [
        for (int i = 0; i < miniblocksPerBlock; i++)
          _width(reader.readByte(), bits),
      ];

      for (int i = 0; i < miniblocksPerBlock && read < totalValues; i++) {
        final width = widths[i];
        final span = math.min(valuesPerMiniblock, totalValues - read);
        // Only materialize up to [count]. A width-0 miniblock reads no input,
        // so expanding it would let a few header bytes claim 2^27 values; the
        // rest of the stream is still consumed so the reader ends at its end.
        final want = math.min(span, count - values.length);
        final deltas = (width == 0 || want <= 0)
            ? null
            : reader.readBitPacked(valuesPerMiniblock, width);
        if (deltas == null) reader.skipBitPacked(valuesPerMiniblock, width);
        for (int k = 0; k < want; k++) {
          final next = values.last + minDelta + (deltas?[k] ?? 0);
          // Wrap in the column's width, mirroring the encoder: the spec makes
          // this arithmetic 2's-complement wrapping, and for INT32 the sum can
          // legitimately leave int32 range on the way back.
          values.add(bits == 64 ? next : next.toSigned(bits));
        }
        read += span;
      }
    }

    return values.length > count ? values.sublist(0, count) : values;
  }

  /// Read and check the stream header: block size, miniblocks per block and
  /// total value count, all untrusted. The spec requires the block size to be
  /// a multiple of 128 and each miniblock a multiple of 32 values. A stream
  /// of more than one value has blocks, each with a width byte per miniblock,
  /// so there cannot be more miniblocks than bytes left (which bounds the
  /// width table this header sizes).
  static ({int miniblocks, int span, int total}) _header(ByteReader reader) {
    final block = reader.readVarint();
    final miniblocks = reader.readVarint();
    final total = reader.readVarint();
    if (total < 0 || total > maxPageValues) {
      throw FormatException(
        'DELTA_BINARY_PACKED: implausible value count $total',
      );
    }
    if (miniblocks <= 0 ||
        (total > 1 && miniblocks > reader.data.length - reader.position) ||
        block <= 0 ||
        block % 128 != 0 ||
        block > maxPageValues ||
        block % miniblocks != 0 ||
        (block ~/ miniblocks) % 32 != 0) {
      throw FormatException(
        'DELTA_BINARY_PACKED: invalid header (block size $block, '
        '$miniblocks miniblocks)',
      );
    }
    return (miniblocks: miniblocks, span: block ~/ miniblocks, total: total);
  }

  /// A miniblock bit width, which cannot exceed the column's width [bits].
  static int _width(int width, int bits) {
    if (width > bits) {
      throw FormatException(
        'DELTA_BINARY_PACKED: bit width $width exceeds the $bits-bit type',
      );
    }
    return width;
  }
}
