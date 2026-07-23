import 'dart:typed_data';

import 'package:libcompress/libcompress.dart';

import 'thrift/parquet_thrift.dart' as thrift;

/// Parquet codecs we can decode on read (lzo and brotli cannot).
const _readable = {
  thrift.CompressionCodec.uncompressed,
  thrift.CompressionCodec.snappy,
  thrift.CompressionCodec.gzip,
  thrift.CompressionCodec.lz4,
  thrift.CompressionCodec.zstd,
  thrift.CompressionCodec.lz4Raw,
};

/// Codecs we can encode on write. The legacy Hadoop-framed `LZ4` stays
/// read-only; modern `LZ4_RAW` (a single raw block) is writable.
const _writable = {
  thrift.CompressionCodec.uncompressed,
  thrift.CompressionCodec.snappy,
  thrift.CompressionCodec.gzip,
  thrift.CompressionCodec.zstd,
  thrift.CompressionCodec.lz4Raw,
};

/// Whether [codec] can be decoded on read.
bool supported(final thrift.CompressionCodec codec) =>
    _readable.contains(codec);

/// Whether [codec] can be encoded on write.
bool writable(final thrift.CompressionCodec codec) => _writable.contains(codec);

/// Returns a block codec for the given Parquet [codec]. When [maxDecompressedSize]
/// is set, the underlying libcompress codec is constructed to cap decode output
/// at that many bytes and throw mid-decode BEFORE over-allocating — the only
/// pre-decode bound for zstd/LZ4_RAW (which carry no cheap length prefix). Null
/// keeps the codec's own default cap.
CompressionCodec codecFor(
  final thrift.CompressionCodec codec, {
  int? maxDecompressedSize,
}) {
  final max = maxDecompressedSize;
  switch (codec) {
    case thrift.CompressionCodec.uncompressed:
      return NoopCodec();
    case thrift.CompressionCodec.snappy:
      return max == null ? SnappyCodec() : SnappyCodec(maxSize: max);
    case thrift.CompressionCodec.gzip:
      return max == null ? GzipCodec() : GzipCodec(maxDecompressedSize: max);
    case thrift.CompressionCodec.lz4:
      return _ParquetLz4Codec(hadoop: true, maxDecompressedSize: max);
    case thrift.CompressionCodec.lz4Raw:
      return _ParquetLz4Codec(hadoop: false, maxDecompressedSize: max);
    case thrift.CompressionCodec.zstd:
      return max == null ? ZstdCodec() : ZstdCodec(maxDecompressedSize: max);
    default:
      throw FormatException('Unsupported Parquet codec: ${codec.name}');
  }
}

/// Codes Parquet LZ4 pages via libcompress's raw block API ([Lz4Codec.compressBlock]
/// /[Lz4Codec.decompressBlock]).
///
/// `LZ4_RAW` (`hadoop: false`) is a single raw block — read and written
/// directly. Legacy `LZ4` (`hadoop: true`) uses Hadoop block framing on read;
/// some writers emit a single raw block under the `LZ4` id too, so the Hadoop
/// path falls back to decoding the payload as one raw block. Hadoop `LZ4` write
/// is not supported (use `LZ4_RAW`).
class _ParquetLz4Codec implements CompressionCodec {
  _ParquetLz4Codec({required this.hadoop, this.maxDecompressedSize});

  final bool hadoop;
  final int? maxDecompressedSize;
  late final Lz4Codec _lz4 = maxDecompressedSize == null
      ? Lz4Codec()
      : Lz4Codec(maxDecompressedSize: maxDecompressedSize);

  @override
  Uint8List decompress(final Uint8List data) {
    if (hadoop) {
      try {
        return _decodeHadoop(data);
      } on _Lz4BudgetExceeded {
        rethrow; // a decompression bomb — never fall back and re-inflate it
      } on Object {
        // Structural: not Hadoop framing → fall back to a single raw block
        // (non-Hadoop writers emit one raw block under the LZ4 id). The raw
        // path is itself bounded by maxDecompressedSize.
      }
    }
    return _lz4.decompressBlock(data);
  }

  /// Hadoop framing: repeated [uncompressed size:4 BE] blocks, each made of one
  /// or more [compressed size:4 BE] raw LZ4 sub-blocks.
  Uint8List _decodeHadoop(final Uint8List data) {
    final out = BytesBuilder();
    final cap = maxDecompressedSize;
    int pos = 0;
    while (pos < data.length) {
      // `expected` is an UNTRUSTED framing length; a huge value here just means
      // "this may not be Hadoop framing" (e.g. a raw block misread) — let the
      // structural path fall back to a raw block. The real bomb guard is the
      // CUMULATIVE check on ACTUAL decoded output below: the per-sub-block cap
      // only bounds ONE block, so many legal sub-blocks could otherwise sum to
      // gigabytes and OOM.
      final expected = _be32(data, pos);
      pos += 4;
      int produced = 0;
      while (produced < expected) {
        final compLen = _be32(data, pos);
        pos += 4;
        final block = Uint8List.sublistView(data, pos, pos + compLen);
        pos += compLen;
        final decoded = _lz4.decompressBlock(block);
        out.add(decoded);
        produced += decoded.length;
        if (cap != null && out.length > cap) {
          // A decompression BOMB, not a framing error: throw the typed
          // exception so the caller's `on _Lz4BudgetExceeded { rethrow }` fires.
          // A plain FormatException here is caught by the structural `on Object`
          // fallback, which would re-run the raw decoder and re-inflate the bomb.
          throw _Lz4BudgetExceeded(
            'Hadoop LZ4 output exceeds maxDecompressedSize=$cap '
            '(cumulative across sub-blocks)',
          );
        }
      }
      if (produced != expected) {
        throw const FormatException('Hadoop LZ4 block size mismatch');
      }
    }
    return out.toBytes();
  }

  // Unsigned big-endian read via ByteData: identical on the VM and web (raw
  // `<<24` shifts would force a >2 GB length negative under JS's 32-bit bitwise
  // semantics, silently corrupting the decode), and bounds-checked so a truncated
  // header fails loud rather than throwing a raw RangeError.
  int _be32(final Uint8List d, final int i) {
    if (i < 0 || i + 4 > d.length) {
      throw const FormatException('Truncated Hadoop LZ4 block length');
    }
    return ByteData.sublistView(d, i, i + 4).getUint32(0, Endian.big);
  }

  @override
  Uint8List compress(final Uint8List data) {
    if (hadoop) {
      throw UnsupportedError(
        'Writing Hadoop-framed Parquet LZ4 is not '
        'supported; use LZ4_RAW',
      );
    }
    return _lz4.compressBlock(data);
  }

  @override
  String get name => 'LZ4';

  @override
  bool supports(final CodecMode mode) => mode == CodecMode.block;
}

/// A Hadoop-LZ4 page whose cumulative decoded output exceeds maxDecompressedSize
/// — a decompression bomb. Distinct from a structural framing error so the
/// decoder rethrows it instead of re-inflating the same bytes as a raw block.
class _Lz4BudgetExceeded implements Exception {
  _Lz4BudgetExceeded(this.message);
  final String message;
  @override
  String toString() => 'Lz4BudgetExceeded: $message';
}
