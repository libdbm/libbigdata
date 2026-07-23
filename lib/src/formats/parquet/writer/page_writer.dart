import 'dart:convert';
import 'dart:typed_data';
import '../../../core/column.dart';
import '../thrift/compact_protocol.dart';
import 'package:libcompress/libcompress.dart';
import '../thrift/parquet_thrift.dart' as thrift;
import '../compression.dart';
import '../encoding/page_encoder.dart';
import '../encoding/value_encoder.dart' show checkInt32;
import 'file_writer.dart';

/// Writes pages to a column chunk
class PageWriter {
  final FileWriter writer;
  final thrift.CompressionCodec codecType;
  final CompressionCodec codec;
  final bool checksums;

  /// Single source of truth for `writeStatistics`: when false, no column chunk
  /// (flat OR nested leaf) gets statistics. Gating here (rather than nulling the
  /// stats after the fact in each caller) keeps flat and nested paths consistent.
  final bool writeStats;
  final List<thrift.PageEncodingStats> encodingStats = [];

  int uncompressedSize = 0;
  int compressedSize = 0;

  PageWriter({
    required this.writer,
    required this.codecType,
    this.checksums = false,
    this.writeStats = true,
  }) : codec = codecFor(codecType);

  /// Write pages for a column
  Future<thrift.ColumnMetaData> writeColumn(
    String name,
    thrift.ParquetType type,
    List<dynamic> values,
    thrift.Encoding encoding, {
    List<int>? defLevels,
    int defBitWidth = 0,
    List<int>? repLevels,
    int repBitWidth = 0,
    List<String>? path,
  }) async {
    final startOffset = writer.position;

    // Create page encoder
    final encoder = PageEncoder(
      type: type,
      encoding: encoding,
      defBitWidth: defBitWidth,
      repBitWidth: repBitWidth,
    );

    // Encode values into pages, tracking the dictionary and first data page
    // offsets the reader needs.
    final pages = encoder.encode(
      values,
      defLevels: defLevels,
      repLevels: repLevels,
    );
    int? dictOffset;
    int? dataOffset;
    for (final page in pages) {
      final pos = writer.position;
      if (page.isDictionary) {
        dictOffset = pos;
      } else {
        dataOffset ??= pos;
      }
      await writePage(page);
    }

    // Create column metadata
    final metadata = thrift.ColumnMetaData()
      ..type = type
      ..encodings = dictOffset != null
          ? [thrift.Encoding.rleDictionary, thrift.Encoding.plain]
          : [encoding]
      ..pathInSchema = path ?? [name]
      ..codec = codecType
      ..numValues = values.length
      ..totalUncompressedSize = uncompressedSize
      ..totalCompressedSize = compressedSize
      ..dataPageOffset = dataOffset ?? startOffset;
    if (dictOffset != null) {
      metadata.dictionaryPageOffset = dictOffset;
    }

    // Add statistics if available (unless the writer disabled them).
    if (writeStats) {
      final stats = calculateStats(type, values);
      if (stats != null) {
        metadata.statistics = stats;
      }
    }

    // encoding_stats is optional and its thrift writer is a stub, so omit it.

    return metadata;
  }

  /// Write a flat numeric column straight from its typed buffer (no boxing).
  /// [encoding] must be PLAIN or DELTA_BINARY_PACKED; stats and def-levels are
  /// computed unboxed. Mirrors [writeColumn] for the numeric fast path.
  Future<thrift.ColumnMetaData> writeColumnTyped(
    String name,
    thrift.ParquetType type,
    NumericColumn column,
    thrift.Encoding encoding, {
    List<int>? defLevels,
    int defBitWidth = 0,
    List<String>? path,
  }) async {
    final startOffset = writer.position;

    final encoder = PageEncoder(
      type: type,
      encoding: encoding,
      defBitWidth: defBitWidth,
    );
    final pages = encoder.encodeTyped(column, defLevels: defLevels);
    int? dataOffset;
    for (final page in pages) {
      dataOffset ??= writer.position;
      await writePage(page);
    }

    final metadata = thrift.ColumnMetaData()
      ..type = type
      ..encodings = [encoding]
      ..pathInSchema = path ?? [name]
      ..codec = codecType
      ..numValues = column.length
      ..totalUncompressedSize = uncompressedSize
      ..totalCompressedSize = compressedSize
      ..dataPageOffset = dataOffset ?? startOffset;

    if (writeStats) {
      final stats = calculateStatsTyped(type, column);
      if (stats != null) {
        metadata.statistics = stats;
      }
    }
    return metadata;
  }

  /// Single typed pass over a numeric column's raw buffer: null count + min/max,
  /// no boxing. Mirrors [calculateStats] for INT32/INT64/FLOAT/DOUBLE.
  thrift.Statistics? calculateStatsTyped(
    thrift.ParquetType type,
    NumericColumn column,
  ) {
    final n = column.length;
    if (n == 0) return null;

    final bitmap = column.rawNullBitmap;
    final stats = thrift.Statistics();
    var nullCount = 0;
    final buffer = column.rawBuffer;

    if (type == thrift.ParquetType.int32 || type == thrift.ParquetType.int64) {
      final data = buffer as List<int>;
      int? mn, mx;
      for (int i = 0; i < n; i++) {
        if (bitmap != null && (bitmap[i >> 3] & (1 << (i & 7))) != 0) {
          nullCount++;
          continue;
        }
        final v = data[i];
        if (mn == null || v < mn) mn = v;
        if (mx == null || v > mx) mx = v;
      }
      if (mn != null) {
        final enc = type == thrift.ParquetType.int32
            ? encodeInt32
            : encodeInt64;
        stats.min = enc(mn);
        stats.max = enc(mx!);
      }
    } else {
      // FLOAT / DOUBLE: exclude NaN (poisons ordering), keep ±Infinity.
      final data = buffer as List<double>;
      double? mn, mx;
      for (int i = 0; i < n; i++) {
        if (bitmap != null && (bitmap[i >> 3] & (1 << (i & 7))) != 0) {
          nullCount++;
          continue;
        }
        final v = data[i];
        if (v.isNaN) continue;
        if (mn == null || v < mn) mn = v;
        if (mx == null || v > mx) mx = v;
      }
      if (mn != null) {
        final enc = type == thrift.ParquetType.float
            ? encodeFloat
            : encodeDouble;
        stats.min = enc(mn);
        stats.max = enc(mx!);
      }
    }
    stats.nullCount = nullCount;
    stats.minValue = stats.min;
    stats.maxValue = stats.max;
    return stats;
  }

  /// Write a single page
  Future<void> writePage(EncodedPage page) async {
    // Update uncompressed size
    uncompressedSize += page.data.length;

    // Compress data
    final compressed = codec.compress(page.data);

    // Update header with compressed size
    page.header.compressedPageSize = compressed.length;

    // Optional page integrity: CRC32 (IEEE) over the compressed page bytes.
    // Stored as a signed int32 (the thrift `crc` field) — readers mask back.
    if (checksums) {
      page.header.crc = Crc32.hash(compressed).toSigned(32);
    }

    // Serialize header
    final protocol = CompactWriter();
    page.header.write(protocol);
    final headerBytes = protocol.takeBytes();

    // Parquet page headers are NOT length-prefixed; the thrift header is
    // written directly, followed by the compressed page data.
    await writer.write(headerBytes);
    await writer.write(compressed);

    // Update compressed size (header + data)
    compressedSize = compressedSize + headerBytes.length + compressed.length;

    // Track encoding stats
    updateEncodingStats(page);
  }

  /// Update encoding statistics
  void updateEncodingStats(EncodedPage page) {
    final encoding = page.isDictionary
        ? thrift.Encoding.plain
        : page.header.dataPageHeader?.encoding ?? thrift.Encoding.plain;

    final pageType = page.isDictionary
        ? thrift.PageType.dictionaryPage
        : thrift.PageType.dataPage;

    // Find or create stats for this encoding
    final stats = encodingStats.firstWhere(
      (s) => s.pageType == pageType && s.encoding == encoding,
      orElse: () {
        final s = thrift.PageEncodingStats()
          ..pageType = pageType
          ..encoding = encoding
          ..count = 0;
        encodingStats.add(s);
        return s;
      },
    );

    stats.count++;
  }

  /// Calculate column statistics
  thrift.Statistics? calculateStats(
    thrift.ParquetType type,
    List<dynamic> values,
  ) {
    if (values.isEmpty) {
      return null;
    }

    // Single pass: count nulls AND track min/max together, without collecting a
    // whole-column `nonNull` list or running four separate `where`/`reduce`
    // passes over the boxed values.
    final stats = thrift.Statistics();
    var nullCount = 0;

    switch (type) {
      case thrift.ParquetType.boolean:
        var anyFalse = false, anyTrue = false;
        for (final v in values) {
          if (v == null) {
            nullCount++;
          } else if (v as bool) {
            anyTrue = true;
          } else {
            anyFalse = true;
          }
        }
        if (anyFalse || anyTrue) {
          stats.min = encodeBool(!anyFalse);
          stats.max = encodeBool(anyTrue);
        }

      case thrift.ParquetType.int32:
      case thrift.ParquetType.int64:
        int? mn, mx;
        for (final v in values) {
          if (v == null) {
            nullCount++;
            continue;
          }
          final n = v as int;
          if (mn == null || n < mn) mn = n;
          if (mx == null || n > mx) mx = n;
        }
        if (mn != null) {
          final enc = type == thrift.ParquetType.int32
              ? encodeInt32
              : encodeInt64;
          stats.min = enc(mn);
          stats.max = enc(mx!);
        }

      case thrift.ParquetType.float:
      case thrift.ParquetType.double:
        // Exclude ONLY NaN (it poisons ordering); ±Infinity are legitimate and
        // MUST participate so pushdown stats aren't understated.
        double? mn, mx;
        for (final v in values) {
          if (v == null) {
            nullCount++;
            continue;
          }
          final d = v as double;
          if (d.isNaN) continue;
          if (mn == null || d < mn) mn = d;
          if (mx == null || d > mx) mx = d;
        }
        if (mn != null) {
          final enc = type == thrift.ParquetType.float
              ? encodeFloat
              : encodeDouble;
          stats.min = enc(mn);
          stats.max = enc(mx!);
        }

      case thrift.ParquetType.byteArray:
        // Unsigned lexicographic on the physical bytes (no stringifying binary).
        Uint8List? mn, mx;
        for (final v in values) {
          if (v == null) {
            nullCount++;
            continue;
          }
          final e = _byteArray(v);
          if (mn == null || _compareBytes(e, mn) < 0) mn = e;
          if (mx == null || _compareBytes(e, mx) > 0) mx = e;
        }
        stats.min = mn;
        stats.max = mx;

      default:
        for (final v in values) {
          if (v == null) nullCount++;
        }
    }
    stats.nullCount = nullCount;

    // Mirror the deprecated min/max into the modern min_value/max_value fields
    // so external readers (Arrow/Spark/DuckDB) get usable stats and pushdown.
    stats.minValue = stats.min;
    stats.maxValue = stats.max;

    return stats;
  }

  /// Encode a byteArray statistic value to its physical bytes.
  Uint8List _byteArray(dynamic value) {
    if (value is Uint8List) return value;
    if (value is List<int>) return Uint8List.fromList(value);
    return Uint8List.fromList(utf8.encode(value.toString()));
  }

  /// Unsigned lexicographic byte comparison (Parquet BYTE_ARRAY sort order).
  int _compareBytes(Uint8List a, Uint8List b) {
    final n = a.length < b.length ? a.length : b.length;
    for (var i = 0; i < n; i++) {
      if (a[i] != b[i]) return a[i] - b[i];
    }
    return a.length - b.length;
  }

  // Encoding helpers for statistics
  Uint8List encodeBool(bool value) {
    return Uint8List.fromList([value ? 1 : 0]);
  }

  Uint8List encodeInt32(int value) {
    // Same truncation hazard as the data encoder: a wrong min/max here would
    // silently mis-prune reads rather than corrupt bytes, which is worse.
    checkInt32(value);
    final bytes = Uint8List(4);
    ByteData.view(bytes.buffer).setInt32(0, value, Endian.little);
    return bytes;
  }

  Uint8List encodeInt64(int value) {
    final bytes = Uint8List(8);
    ByteData.view(bytes.buffer).setInt64(0, value, Endian.little);
    return bytes;
  }

  Uint8List encodeFloat(double value) {
    final bytes = Uint8List(4);
    ByteData.view(bytes.buffer).setFloat32(0, value, Endian.little);
    return bytes;
  }

  Uint8List encodeDouble(double value) {
    final bytes = Uint8List(8);
    ByteData.view(bytes.buffer).setFloat64(0, value, Endian.little);
    return bytes;
  }

  Uint8List encodeString(String value) {
    return Uint8List.fromList(utf8.encode(value));
  }
}
