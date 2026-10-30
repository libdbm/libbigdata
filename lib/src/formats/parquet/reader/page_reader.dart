import 'dart:typed_data';
import '../thrift/compact_protocol.dart';
import 'package:libcompress/libcompress.dart';
import '../thrift/parquet_thrift.dart' as thrift;
import '../compression.dart';
import 'file_reader.dart';
import '../../../util/varint.dart';

/// Default per-page ceiling (compressed AND decompressed). A crafted page can
/// otherwise transiently allocate ~2× this (compressed + decompressed coexist
/// during `decompress`), so the default is well under a small-service budget
/// while far above any legitimate page. Override via `ParquetReaderOptions`.
const int defaultMaxParquetPageBytes = 256 << 20; // 256 MiB

/// Reads pages from a column chunk
class PageReader {
  final FileReader reader;
  final int offset;
  final int size;
  final thrift.CompressionCodec codecType;
  final bool verify;

  /// Per-page ceiling applied to BOTH the compressed page read and the declared
  /// decompressed size: configurable so a memory-bounded service can lower it.
  final int maxPageBytes;

  int position = 0;

  PageReader(
    this.reader,
    this.offset,
    this.size,
    this.codecType, {
    this.verify = false,
    this.maxPageBytes = defaultMaxParquetPageBytes,
  });

  /// Read next page
  Future<Page?> readNext() async {
    if (position >= size) {
      return null;
    }

    // Read page header. All reads use the absolute-offset readBytes API (never
    // the shared cursor) so the FileReader stays usable concurrently.
    final start = offset + position;

    final result = await readHeaderWithSize();
    if (result == null) {
      return null;
    }

    final header = result.header;
    final consumed = result.bytesConsumed;

    // Update position for header
    position += consumed;

    // Check if we have enough space for the page data
    if (position + header.compressedPageSize > size) {
      // This shouldn't happen in a valid file
      throw FormatException(
        'Page data exceeds column chunk bounds: '
        'position=$position, pageSize=${header.compressedPageSize}, '
        'chunkSize=$size',
      );
    }
    // Bound the COMPRESSED page BEFORE reading it: a column chunk can be
    // multi-GB, so the chunk-bounds check above does not stop a single
    // multi-GB page from being read whole here: defeating readBatches's
    // bounded-memory contract. The decompressed-size guards run only later,
    // inside decompress(). Reject an implausible compressed page up front.
    if (header.compressedPageSize < 0 ||
        header.compressedPageSize > maxPageBytes) {
      throw FormatException(
        'Compressed Parquet page is ${header.compressedPageSize} bytes, over '
        'the $maxPageBytes-byte ceiling',
      );
    }

    // Read the page data at its absolute offset (just past the header).
    final compressed = await reader.readBytes(
      start + consumed,
      header.compressedPageSize,
    );
    // A short read means the file is physically truncated below what its
    // metadata claims: accepting it would silently decode fabricated bytes.
    if (compressed.length != header.compressedPageSize) {
      throw FormatException(
        'Truncated Parquet page at offset $start: expected '
        '${header.compressedPageSize} bytes, got ${compressed.length}',
      );
    }
    position += header.compressedPageSize;

    // Optional page integrity check: CRC32 (IEEE) over the compressed bytes.
    // Mask both sides to 32 bits: the thrift I32 round-trip is signed.
    if (verify && header.crc != null) {
      if (Crc32.hash(compressed) != (header.crc! & 0xFFFFFFFF)) {
        throw FormatException(
          'Parquet page CRC mismatch at offset ${offset + position}',
        );
      }
    }

    // Decompress if needed
    final data = decompress(header, compressed);

    return Page(header: header, data: data);
  }

  /// The largest declared-output : compressed-input ratio tolerated above
  /// [_ratioFloor]. A malformed header can declare a tiny page expands to
  /// gigabytes (a decompression bomb); combined with [maxPageBytes] this bounds
  /// the allocation. The ratio is only applied to large declared sizes so
  /// ordinary small pages (which legitimately reach very high ratios) are never
  /// rejected.
  static const _maxDecompressRatio = 1000;
  static const _ratioFloor = 64 << 20; // 64 MiB

  /// A page header, even with rich column statistics, is at most a few MiB. This
  /// caps how far [readHeaderWithSize] grows its parse window so a corrupt header
  /// in a huge column chunk can't force reading the whole chunk to reject it.
  static const _maxPageHeaderBytes = 8 << 20; // 8 MiB

  /// Reject an implausible declared uncompressed size before allocating.
  void _guardSize(int declared, int compressed) {
    if (declared < 0 ||
        declared > maxPageBytes ||
        (compressed > 0 &&
            declared > _ratioFloor &&
            declared > compressed * _maxDecompressRatio)) {
      throw FormatException(
        'Implausible uncompressed page size $declared for $compressed '
        'compressed bytes',
      );
    }
  }

  /// Best-effort pre-decode bound: several codecs carry their OWN uncompressed
  /// length, which the codec allocates to BEFORE the post-decode size check runs.
  /// Where that length is cheap to read (Snappy's leading varint, gzip's trailing
  /// ISIZE) reject a payload self-declaring more than the header's [declared] size
  /// so a decompression bomb is refused before allocation. Codecs without a cheap
  /// length prefix (LZ4_RAW, zstd) fall back to [_guardSize] + the post-check.
  void _guardCodecOutput(int declared, Uint8List input) {
    int? claimed;
    if (codecType == thrift.CompressionCodec.snappy) {
      claimed = _snappyUncompressedLength(input);
    } else if (codecType == thrift.CompressionCodec.gzip && input.length >= 4) {
      // gzip trailer: ISIZE = uncompressed size mod 2^32 (little-endian). The
      // 1 GiB declared guard keeps legitimate sizes well under 2^32, so a bomb
      // producing far more than `declared` shows a larger ISIZE here.
      claimed = ByteData.sublistView(
        input,
        input.length - 4,
      ).getUint32(0, Endian.little);
    }
    if (claimed != null && claimed > declared) {
      throw FormatException(
        'Codec self-declared output $claimed exceeds the declared page size '
        '$declared (decompression bomb)',
      );
    }
  }

  /// Decode Snappy's leading little-endian base-128 varint (the uncompressed
  /// block length), or null if it isn't cleanly present in the first 5 bytes.
  static int? _snappyUncompressedLength(Uint8List data) {
    try {
      return readVarint(data, 0, maxBytes: 5, label: 'Snappy length').value;
    } on FormatException {
      return null;
    }
  }

  /// Decompress a page payload.
  ///
  /// DataPageV2 stores repetition/definition levels uncompressed ahead of the
  /// compressed values, so only the value bytes pass through the codec.
  Uint8List decompress(thrift.PageHeader header, Uint8List payload) {
    final declared = header.uncompressedPageSize;
    _guardSize(declared, payload.length);
    final v2 = header.type == thrift.PageType.dataPageV2
        ? header.dataPageHeaderV2
        : null;
    if (v2 == null) {
      _guardCodecOutput(declared, payload);
      // Build the codec bounded to THIS page's declared output so a zstd/LZ4_RAW
      // page (no cheap length prefix for the pre-guard) throws mid-decode before
      // over-allocating, rather than materializing its full real output first.
      final out = codecFor(codecType, maxSize: declared).decompress(payload);
      // A codec producing more than the header declares is corruption (or a
      // bomb): fail loud. Producing fewer is truncation, handled downstream.
      if (out.length > declared) {
        throw FormatException(
          'Decompressed page ${out.length} bytes exceeds declared $declared',
        );
      }
      return out;
    }
    if (v2.repetitionLevelsByteLength < 0 ||
        v2.definitionLevelsByteLength < 0) {
      throw const FormatException('Corrupt V2 page: negative level length');
    }
    final levels =
        v2.repetitionLevelsByteLength + v2.definitionLevelsByteLength;
    if (levels > declared) {
      throw FormatException(
        'Corrupt V2 page: $levels bytes of levels in a $declared-byte page',
      );
    }
    if (v2.isCompressed == false || levels >= payload.length) {
      return payload;
    }
    final valueBytes = Uint8List.sublistView(payload, levels);
    // The V2 value bytes decompress to `declared - levels`; bound the codec's
    // self-declared output against that remaining budget before decompressing.
    _guardCodecOutput(declared - levels, valueBytes);
    final values = codecFor(
      codecType,
      maxSize: declared - levels,
    ).decompress(valueBytes);
    final data = Uint8List(levels + values.length);
    data.setRange(0, levels, payload);
    data.setRange(levels, data.length, values);
    if (data.length > declared) {
      throw FormatException(
        'Decompressed page ${data.length} bytes exceeds declared $declared',
      );
    }
    return data;
  }

  /// Read a page header and return the bytes it consumed.
  ///
  /// Page headers are not length-prefixed and the spec sets no size limit (a
  /// header with rich statistics can be large), so we read a growing window and
  /// retry until the thrift header parses or the whole column chunk is
  /// exhausted. After parsing we seek back to just past the header bytes.
  Future<HeaderResult?> readHeaderWithSize() async {
    final available = size - position;
    if (available <= 0) {
      return null;
    }

    final start = offset + position;
    // Cap how far the window may grow: a page header (even with rich statistics)
    // is at most a few MiB. Without this, a corrupt/unparseable header in a
    // multi-GB column chunk grows the window ×4 until it reads the WHOLE chunk
    // just to reject it. Beyond the cap the header is deemed corrupt.
    final cap = available < _maxPageHeaderBytes
        ? available
        : _maxPageHeaderBytes;
    int window = cap < 1024 ? cap : 1024;

    while (true) {
      // Absolute-offset read (no shared cursor); consumed is returned so the
      // caller can locate the page data without seeking.
      final bytes = await reader.readBytes(start, window);
      if (bytes.isEmpty) {
        return null;
      }

      final bounded = window < cap; // room to widen within the header cap
      try {
        final protocol = CompactReader(bytes);
        final header = thrift.PageHeader();
        header.read(protocol);

        final consumed = protocol.consumed;
        return HeaderResult(header: header, bytesConsumed: consumed);
      } on Object catch (error) {
        // Headers are not length-prefixed, so running out of bytes mid-parse
        // means the window is too small: widen and retry. Truncation now
        // surfaces as a FormatException (not RangeError), so both are treated as
        // "need more bytes" while the window can still grow; only once it spans
        // the rest of the chunk is an unparseable header truly corrupt.
        if ((error is RangeError || error is FormatException) && bounded) {
          window = (window * 4) < cap ? window * 4 : cap;
          continue;
        }
        if (error is RangeError || error is FormatException) {
          throw FormatException(
            'Parquet page header at offset $start is corrupt or truncated '
            '(read $window of $available bytes remaining, header cap '
            '$_maxPageHeaderBytes)',
          );
        }
        rethrow;
      }
    }
  }
}

/// Result of reading header
class HeaderResult {
  final thrift.PageHeader header;
  final int bytesConsumed;

  HeaderResult({required this.header, required this.bytesConsumed});
}

/// Represents a page with its header and data
class Page {
  final thrift.PageHeader header;
  final Uint8List data;

  Page({required this.header, required this.data});
}
