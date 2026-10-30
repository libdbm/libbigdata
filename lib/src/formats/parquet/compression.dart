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

/// The codecs [ParquetWriter] can write, as `ParquetWriterOptions.compression`
/// takes them. The legacy Hadoop-framed `LZ4` is read but never written; its
/// modern replacement `LZ4_RAW` is [lz4Raw].
enum ParquetCompression { none, snappy, gzip, zstd, lz4Raw }

/// The Thrift codec a [ParquetCompression] writes, for the writer's internals.
extension ParquetCodec on ParquetCompression {
  thrift.CompressionCodec get codec => switch (this) {
    ParquetCompression.none => thrift.CompressionCodec.uncompressed,
    ParquetCompression.snappy => thrift.CompressionCodec.snappy,
    ParquetCompression.gzip => thrift.CompressionCodec.gzip,
    ParquetCompression.zstd => thrift.CompressionCodec.zstd,
    ParquetCompression.lz4Raw => thrift.CompressionCodec.lz4Raw,
  };
}

/// Whether [codec] can be decoded on read.
bool supported(final thrift.CompressionCodec codec) =>
    _readable.contains(codec);

/// Returns a block codec for the given Parquet [codec]. When [maxSize] is set,
/// the codec caps decode output at that many bytes and throws mid-decode
/// BEFORE over-allocating: the only pre-decode bound for zstd/LZ4_RAW (which
/// carry no cheap length prefix). Null keeps the codec's own default cap.
CompressionCodec codecFor(final thrift.CompressionCodec codec, {int? maxSize}) {
  // A page may legitimately decompress to zero bytes (pyarrow writes one for
  // an all-null column chunk), but every libcompress codec rejects a bound of
  // 0. A bound of 1 still stops a bomb; callers reject output larger than the
  // declared size.
  final max = maxSize == 0 ? 1 : maxSize;
  switch (codec) {
    case thrift.CompressionCodec.uncompressed:
      return NoopCodec();
    case thrift.CompressionCodec.snappy:
      return max == null ? SnappyCodec() : SnappyCodec(maxSize: max);
    case thrift.CompressionCodec.gzip:
      return max == null ? GzipCodec() : GzipCodec(maxSize: max);
    case thrift.CompressionCodec.lz4:
      return _HadoopLz4Codec(max);
    case thrift.CompressionCodec.lz4Raw:
      return _lz4(Lz4Format.raw, max);
    case thrift.CompressionCodec.zstd:
      return max == null ? ZstdCodec() : ZstdCodec(maxSize: max);
    default:
      throw FormatException('Unsupported Parquet codec: ${codec.name}');
  }
}

/// An LZ4 codec for [format], capped at [max] bytes of output when given.
Lz4Codec _lz4(final Lz4Format format, final int? max) => max == null
    ? Lz4Codec(format: format)
    : Lz4Codec(format: format, maxSize: max);

/// Parquet's legacy `LZ4` codec: Hadoop block framing, as parquet-mr writes
/// it, falling back to one raw block, which older parquet-cpp writes under the
/// same id. A raw block misread as Hadoop framing can fail on any check,
/// including the size cap, so every failure falls back; the raw attempt has the
/// same cap, so a bomb is still bounded. When both fail, the Hadoop error is
/// the one reported. Writing is not supported (use `LZ4_RAW`).
final class _HadoopLz4Codec extends CompressionCodec {
  _HadoopLz4Codec(this.max);

  final int? max;

  @override
  Uint8List decompress(final Uint8List data, {final int? size}) {
    try {
      return _lz4(Lz4Format.hadoop, max).decompress(data, size: size);
    } on CompressionFormatException catch (error, trace) {
      try {
        return _lz4(Lz4Format.raw, max).decompress(data, size: size);
      } on CompressionFormatException {
        Error.throwWithStackTrace(error, trace);
      }
    }
  }

  @override
  Uint8List compress(final Uint8List data) => throw UnsupportedError(
    'Writing Hadoop-framed Parquet LZ4 is not supported; use LZ4_RAW',
  );

  @override
  String get name => 'LZ4';
}
