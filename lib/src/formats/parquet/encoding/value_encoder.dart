import 'dart:convert';
import 'dart:math' as math;
import 'dart:typed_data';
import '../thrift/parquet_thrift.dart' as thrift;
import 'rle_bitpacked.dart';

/// Throw if [value] does not fit a Parquet INT32.
///
/// `ByteData.setInt32` keeps only the low 32 bits, so an out-of-range value is
/// written as a silently DIFFERENT number. A column's declared dtype is not a
/// guarantee here: `validateColumns` checks the declared dtype and never
/// inspects values, and a boxed column can hold anything. This is the last
/// point before the bytes hit the file, so it fails loud rather than truncate.
void checkInt32(int value) {
  if (value < -2147483648 || value > 2147483647) {
    throw ArgumentError(
      'Parquet INT32 cannot hold $value; the column is declared int32 but '
      'carries a wider value (writing it would truncate to '
      '${value.toSigned(32)})',
    );
  }
}

/// Base class for value encoders
abstract class ValueEncoder {
  /// Encode values to bytes
  Uint8List encode(List<dynamic> values);

  /// Get encoding type
  thrift.Encoding get encoding;
}

/// Plain encoding for values
class PlainEncoder extends ValueEncoder {
  final thrift.ParquetType type;

  PlainEncoder(this.type);

  @override
  thrift.Encoding get encoding => thrift.Encoding.plain;

  @override
  Uint8List encode(List<dynamic> values) {
    final buffer = BytesBuilder();

    // PLAIN booleans are bit-packed (LSB first), 1 bit per non-null value.
    if (type == thrift.ParquetType.boolean) {
      final bools = [
        for (final v in values)
          if (v != null) v as bool,
      ];
      final bytes = Uint8List((bools.length + 7) ~/ 8);
      for (int i = 0; i < bools.length; i++) {
        if (bools[i]) bytes[i >> 3] |= 1 << (i & 7);
      }
      return bytes;
    }

    // Fixed-width numeric: write straight into ONE pre-sized buffer via a single
    // ByteData view — no per-value Uint8List/ByteData allocation and no
    // BytesBuilder copy (this is the PLAIN hot path for float/double columns,
    // which are never dictionary-encoded).
    final width = switch (type) {
      thrift.ParquetType.int32 || thrift.ParquetType.float => 4,
      thrift.ParquetType.int64 || thrift.ParquetType.double => 8,
      _ => 0,
    };
    if (width != 0) {
      var nonNull = 0;
      for (final v in values) {
        if (v != null) nonNull++;
      }
      final out = Uint8List(nonNull * width);
      final bd = ByteData.view(out.buffer);
      var off = 0;
      for (final v in values) {
        if (v == null) continue;
        switch (type) {
          case thrift.ParquetType.int32:
            checkInt32(v as int);
            bd.setInt32(off, v, Endian.little);
          case thrift.ParquetType.int64:
            bd.setInt64(off, v as int, Endian.little);
          case thrift.ParquetType.float:
            bd.setFloat32(off, (v as num).toDouble(), Endian.little);
          default: // double
            bd.setFloat64(off, (v as num).toDouble(), Endian.little);
        }
        off += width;
      }
      return out;
    }

    for (final value in values) {
      if (value == null) {
        // Null handling depends on definition levels
        continue;
      }

      switch (type) {
        case thrift.ParquetType.boolean:
          buffer.addByte(value ? 1 : 0);
          break;

        case thrift.ParquetType.int32:
          checkInt32(value);
          final bytes = Uint8List(4);
          ByteData.view(bytes.buffer).setInt32(0, value, Endian.little);
          buffer.add(bytes);
          break;

        case thrift.ParquetType.int64:
          final bytes = Uint8List(8);
          ByteData.view(bytes.buffer).setInt64(0, value, Endian.little);
          buffer.add(bytes);
          break;

        case thrift.ParquetType.float:
          final bytes = Uint8List(4);
          ByteData.view(bytes.buffer).setFloat32(0, value, Endian.little);
          buffer.add(bytes);
          break;

        case thrift.ParquetType.double:
          final bytes = Uint8List(8);
          ByteData.view(bytes.buffer).setFloat64(0, value, Endian.little);
          buffer.add(bytes);
          break;

        case thrift.ParquetType.byteArray:
          // Binary columns carry raw bytes; only String values are UTF-8
          // encoded. Stringifying a Uint8List would store "[1, 2, 3]".
          final bytes = value is Uint8List
              ? value
              : value is List<int>
              ? Uint8List.fromList(value)
              : Uint8List.fromList(utf8.encode(value.toString()));
          // Write length prefix
          final lenBytes = Uint8List(4);
          ByteData.view(
            lenBytes.buffer,
          ).setInt32(0, bytes.length, Endian.little);
          buffer.add(lenBytes);
          buffer.add(bytes);
          break;

        case thrift.ParquetType.fixedLenByteArray:
          // Assume value is already bytes of correct length
          buffer.add(value as Uint8List);
          break;

        case thrift.ParquetType.int96:
          // 12 bytes for INT96 (deprecated timestamp format)
          buffer.add(value as Uint8List);
          break;
      }
    }

    return buffer.toBytes();
  }
}

/// DELTA_BINARY_PACKED encoding for integers, matching DeltaBinaryPackedDecoder:
/// header (block size, miniblocks/block, total count, first value) followed by
/// blocks of [min delta][per-miniblock bit widths][packed (delta - min) values].
class DeltaBinaryPackedEncoder extends ValueEncoder {
  static const int _blockSize = 128;
  static const int _miniblocks = 4;
  static const int _valuesPerMini = _blockSize ~/ _miniblocks; // 32

  /// The column's physical type. Delta arithmetic wraps in the TYPE's width, not
  /// always 64 bits: the spec says overflow "should be allowed and handled as
  /// wrapping around in 2's complement notation", and parquet-mr/Arrow implement
  /// INT32 accordingly (Arrow's `min_delta_` is an `int32_t` read via a uint32
  /// VLQ capped at 5 bytes). A 64-bit delta on an int32 column therefore emits a
  /// varint those readers cannot consume — pyarrow fails the whole file with
  /// "Unexpected end of stream: InitBlock EOF".
  final thrift.ParquetType type;

  DeltaBinaryPackedEncoder([this.type = thrift.ParquetType.int64]);

  int get _bits => type == thrift.ParquetType.int32 ? 32 : 64;

  /// [value] wrapped to the column's width. A Dart int is already 64-bit
  /// two's-complement, so the 64-bit case is identity.
  int _wrap(int value) => _bits == 64 ? value : value.toSigned(_bits);

  @override
  thrift.Encoding get encoding => thrift.Encoding.deltaBinaryPacked;

  @override
  Uint8List encode(List<dynamic> values) {
    final ints = [
      for (final v in values)
        if (v != null) v as int,
    ];
    return encodeInts(ints);
  }

  /// Encode the already-null-filtered integers. Shared by the boxed [encode] and
  /// the typed writer, which fills an `Int64List` straight from a column's raw
  /// buffer (no per-value boxing).
  Uint8List encodeInts(List<int> ints) {
    final out = BytesBuilder();
    _uleb(out, _blockSize);
    _uleb(out, _miniblocks);
    _uleb(out, ints.length);
    if (ints.isEmpty) {
      _zigzag(out, 0);
      return out.toBytes();
    }
    _zigzag(out, ints[0]);
    if (ints.length == 1) return out.toBytes();

    final deltas = List<int>.generate(
      ints.length - 1,
      (i) => _wrap(ints[i + 1] - ints[i]),
    );

    int i = 0;
    while (i < deltas.length) {
      final blockLen = math.min(_blockSize, deltas.length - i);
      int minDelta = deltas[i];
      for (int j = 1; j < blockLen; j++) {
        if (deltas[i + j] < minDelta) minDelta = deltas[i + j];
      }
      _zigzag(out, minDelta);

      final usedMini = (blockLen + _valuesPerMini - 1) ~/ _valuesPerMini;
      final widths = List<int>.filled(_miniblocks, 0);
      final bodies = <Uint8List>[];
      for (int mb = 0; mb < usedMini; mb++) {
        final start = mb * _valuesPerMini;
        final end = math.min(start + _valuesPerMini, blockLen);
        final mini = List<int>.filled(_valuesPerMini, 0);
        // The spec makes `delta - minDelta` a UINT64, so its width must be
        // measured unsigned. A spread reaching 2^63 wraps to a negative Dart
        // int; comparing those signed let the max stay 0, the width stay 0, and
        // the block encode as zero bytes — every delta then decoded as minDelta.
        // Tracking the WIDTH rather than the value avoids an unsigned compare
        // entirely (a negative d is, as a uint64, exactly 64 bits wide).
        int bw = 0;
        for (int j = start; j < end; j++) {
          final d = _wrap(deltas[i + j] - minDelta);
          mini[j - start] = d;
          final width = d < 0 ? _bits : d.bitLength;
          if (width > bw) bw = width;
        }
        widths[mb] = bw;
        bodies.add(RleBitpackedHybrid.packBits(mini, bw));
      }
      for (final w in widths) {
        out.addByte(w);
      }
      for (final b in bodies) {
        out.add(b);
      }
      i += blockLen;
    }
    return out.toBytes();
  }

  void _uleb(BytesBuilder out, int value) {
    while ((value & ~0x7f) != 0) {
      out.addByte((value & 0x7f) | 0x80);
      value >>>= 7;
    }
    out.addByte(value & 0x7f);
  }

  void _zigzag(BytesBuilder out, int n) => _uleb(out, (n << 1) ^ (n >> 63));
}

/// Factory for creating encoders
class EncoderFactory {
  /// Create encoder for the given encoding and type
  static ValueEncoder create(
    thrift.Encoding encoding,
    thrift.ParquetType type,
  ) {
    switch (encoding) {
      case thrift.Encoding.plain:
        return PlainEncoder(type);
      // plainDictionary/rleDictionary never reach here: PageEncoder.encode
      // intercepts them into encodeDictionary. RLE is a LEVEL encoding
      // (RleBitpackedHybrid), never a value encoding.
      case thrift.Encoding.deltaBinaryPacked:
        return DeltaBinaryPackedEncoder(type);
      default:
        return PlainEncoder(type);
    }
  }
}
