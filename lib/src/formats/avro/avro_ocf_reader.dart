import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:libcompress/libcompress.dart';

import '../../core/limits.dart';
import 'avro_decoder.dart';

/// Reads an Avro Object Container File (OCF): 4-byte magic `Obj\x01`, a
/// `map<string,bytes>` of file metadata (`avro.schema`, `avro.codec`), a 16-byte
/// sync marker, then data blocks `[count][byteSize][block bytes][sync]`.
/// Supports the `null`, `deflate` (raw) and `snappy` (Avro CRC-framed) codecs.
class AvroOcfReader {
  /// A single block decompressing to more than this multiple of its compressed
  /// size (past a small floor) is treated as a decompression bomb.
  static const _maxRatio = 200;

  /// Reads all records, STREAMING the file block-by-block from disk so a huge
  /// (or malicious) compressed OCF is never fully loaded before the caps apply.
  /// [maxCompressedBytes] bounds the total COMPRESSED input consumed (distinct
  /// from [maxBytes], which bounds decompressed block bytes); the header/metadata
  /// prefix is bounded by [defaultMaxFooterBytes]. Pass null to opt any out.
  static List<Map<String, dynamic>> readPath(
    String path, {
    int? maxRecords = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    int? maxCompressedBytes = defaultMaxMaterializedBytes,
  }) {
    final raf = File(path).openSync();
    try {
      return _streamSync(
        raf,
        maxRecords: maxRecords,
        maxBytes: maxBytes,
        maxCompressed: maxCompressedBytes,
      );
    } finally {
      raf.closeSync();
    }
  }

  /// Async variant of [readPath] — streams from a [RandomAccessFile] without
  /// blocking the event loop or buffering the whole file. Used by Iceberg
  /// manifest resolution, which is O(#data files) and otherwise unbounded.
  static Future<List<Map<String, dynamic>>> readPathAsync(
    String path, {
    int? maxRecords = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    int? maxCompressedBytes = defaultMaxMaterializedBytes,
  }) async {
    final raf = await File(path).open();
    try {
      return await _streamAsync(
        raf,
        maxRecords: maxRecords,
        maxBytes: maxBytes,
        maxCompressed: maxCompressedBytes,
      );
    } finally {
      await raf.close();
    }
  }

  /// Stream records one at a time from an OCF file, block-by-block, WITHOUT
  /// materializing the whole record list (unlike [readPathAsync]). Used by the
  /// Iceberg manifest planner so a snapshot with many manifests stays bounded:
  /// only one Avro record is resident at a time; the caller keeps just the light
  /// fields it needs and enforces its own global budget. Per-file
  /// [maxBytes]/[maxCompressedBytes] guards still apply (pass null to opt out);
  /// there is no record-count cap here — the caller bounds the total.
  static Stream<Map<String, dynamic>> streamPathAsync(
    String path, {
    int? maxBytes = defaultMaxMaterializedBytes,
    int? maxCompressedBytes = defaultMaxMaterializedBytes,
  }) async* {
    final raf = await File(path).open();
    try {
      final fileLen = await raf.length();
      final h = await _readHeaderAsync(raf, fileLen);
      await raf.setPosition(h.blockStart);
      var decompressed = 0, compressed = 0, pos = h.blockStart;
      while (pos < fileLen) {
        final count = await _readLongAsync(raf);
        final size = await _readLongAsync(raf);
        if (size < 0 || size > fileLen) {
          throw const FormatException('Invalid Avro block size');
        }
        compressed += size;
        if (maxCompressedBytes != null && compressed > maxCompressedBytes) {
          throw StateError(
            'Avro OCF exceeds maxCompressedBytes=$maxCompressedBytes '
            '(compressed input); pass maxCompressedBytes: null to allow it',
          );
        }
        final raw = await raf.read(size);
        if (raw.length != size) {
          throw const FormatException('Truncated Avro block');
        }
        final ceiling = maxBytes == null ? null : maxBytes - decompressed;
        final block = _decompress(h.codec, raw, ceiling);
        decompressed += block.length;
        if (maxBytes != null && decompressed > maxBytes) {
          throw StateError(
            'Avro OCF exceeds maxBytes=$maxBytes (decompressed); pass '
            'maxBytes: null to allow it',
          );
        }
        if (!_sameSync(await raf.read(16), h.sync)) {
          throw const FormatException(
            'Avro block sync mismatch (corrupt or spliced OCF)',
          );
        }
        if (count <= 0) {
          throw FormatException(
            'Avro OCF block has a non-positive count: $count',
          );
        }
        final br = AvroReader(block);
        for (int i = 0; i < count; i++) {
          yield h.schema.decode(br) as Map<String, dynamic>;
        }
        if (!br.atEnd) {
          throw const FormatException(
            'Avro OCF block not fully consumed — declared record count too small',
          );
        }
        pos = await raf.position();
      }
    } finally {
      await raf.close();
    }
  }

  /// Decode records from an in-memory OCF [bytes] (for callers that already hold
  /// the whole file). [maxRecords]/[maxBytes] stop-at-cap across blocks; each
  /// block's decompression ratio is bounded as a bomb guard. Prefer [readPath]/
  /// [readPathAsync] for files (they stream + bound compressed input).
  static List<Map<String, dynamic>> read(
    Uint8List bytes, {
    int? maxRecords = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) {
    final r = AvroReader(bytes);
    final h = _readHeaderFrom(r);
    final records = <Map<String, dynamic>>[];
    var decompressedBytes = 0;
    while (!r.atEnd) {
      final count = r.readLong();
      final size = r.readLong();
      final ceiling = maxBytes == null ? null : maxBytes - decompressedBytes;
      final block = _decompress(h.codec, r.readFixed(size), ceiling);
      decompressedBytes += block.length;
      if (maxBytes != null && decompressedBytes > maxBytes) {
        throw StateError(
          'Avro OCF exceeds maxBytes=$maxBytes (decompressed); pass '
          'maxBytes: null to allow it',
        );
      }
      final blockSync = r.readFixed(16);
      if (!_sameSync(blockSync, h.sync)) {
        throw const FormatException(
          'Avro block sync mismatch (corrupt or spliced OCF)',
        );
      }
      _appendRecords(block, count, h.schema, records, maxRecords);
    }
    return records;
  }

  /// Parse the OCF header (magic + metadata map + sync) from [r], returning the
  /// schema/codec/sync and leaving [r] positioned at the first data block.
  static ({AvroSchema schema, String codec, Uint8List sync, int blockStart})
  _readHeaderFrom(AvroReader r) {
    final magic = r.readFixed(4);
    if (magic[0] != 0x4F ||
        magic[1] != 0x62 ||
        magic[2] != 0x6A ||
        magic[3] != 1) {
      throw const FormatException('Not an Avro OCF (bad magic)');
    }
    // File metadata: an Avro map<string,bytes>.
    final meta = <String, Uint8List>{};
    while (true) {
      var count = r.readLong();
      if (count == 0) break;
      if (count < 0) {
        r.readLong();
        count = -count;
      }
      for (int i = 0; i < count; i++) {
        final key = r.readString();
        meta[key] = Uint8List.fromList(r.readBytes());
      }
    }
    final sync = r.readFixed(16);
    final schemaMeta = meta['avro.schema'];
    if (schemaMeta == null) {
      throw const FormatException('Avro OCF missing avro.schema');
    }
    final schema = AvroSchema.parse(utf8.decode(schemaMeta));
    final codec = meta['avro.codec'] == null
        ? 'null'
        : utf8.decode(meta['avro.codec']!);
    return (schema: schema, codec: codec, sync: sync, blockStart: r.position);
  }

  /// Decode [count] records from a decompressed [block], appending to [records]
  /// and stopping at [maxRecords].
  static void _appendRecords(
    Uint8List block,
    int count,
    AvroSchema schema,
    List<Map<String, dynamic>> records,
    int? maxRecords,
  ) {
    // A non-positive count would silently drop the whole block (the loop runs
    // zero times) — a corrupt/hostile file must fail loud, not lose records.
    if (count <= 0) {
      throw FormatException('Avro OCF block has a non-positive count: $count');
    }
    final br = AvroReader(block);
    for (int i = 0; i < count; i++) {
      records.add(schema.decode(br) as Map<String, dynamic>);
      if (maxRecords != null && records.length > maxRecords) {
        throw StateError(
          'Avro OCF exceeds maxRecords=$maxRecords; pass maxRecords: null '
          'to allow it',
        );
      }
    }
    // Decoding exactly [count] records must consume the whole decompressed block;
    // leftover bytes mean the declared count under-reads (silent record loss).
    if (!br.atEnd) {
      throw const FormatException(
        'Avro OCF block not fully consumed — declared record count is too small',
      );
    }
  }

  /// Header (magic + `map<string,bytes>` metadata + 16-byte sync) ceiling. The
  /// metadata is normally a few KB; growing a small window up to this cap bounds
  /// a maliciously huge metadata section WITHOUT the old 128 MiB up-front read on
  /// every large file.
  static const _maxHeaderBytes = 4 << 20; // 4 MiB
  static const _initialHeaderWindow = 64 << 10; // 64 KiB

  /// Read only the header, growing a small window on demand (the header is not
  /// length-prefixed) until it parses or the [_maxHeaderBytes] cap is reached.
  static Future<
    ({AvroSchema schema, String codec, Uint8List sync, int blockStart})
  >
  _readHeaderAsync(RandomAccessFile raf, int fileLen) async {
    final cap = fileLen < _maxHeaderBytes ? fileLen : _maxHeaderBytes;
    var window = cap < _initialHeaderWindow ? cap : _initialHeaderWindow;
    while (true) {
      await raf.setPosition(0);
      final prefix = await raf.read(window);
      try {
        return _readHeaderFrom(AvroReader(prefix));
      } on Object catch (e) {
        if ((e is RangeError || e is FormatException) && window < cap) {
          window = (window * 4) < cap ? window * 4 : cap;
          continue;
        }
        rethrow;
      }
    }
  }

  static ({AvroSchema schema, String codec, Uint8List sync, int blockStart})
  _readHeaderSync(RandomAccessFile raf, int fileLen) {
    final cap = fileLen < _maxHeaderBytes ? fileLen : _maxHeaderBytes;
    var window = cap < _initialHeaderWindow ? cap : _initialHeaderWindow;
    while (true) {
      raf.setPositionSync(0);
      final prefix = raf.readSync(window);
      try {
        return _readHeaderFrom(AvroReader(prefix));
      } on Object catch (e) {
        if ((e is RangeError || e is FormatException) && window < cap) {
          window = (window * 4) < cap ? window * 4 : cap;
          continue;
        }
        rethrow;
      }
    }
  }

  static Future<List<Map<String, dynamic>>> _streamAsync(
    RandomAccessFile raf, {
    int? maxRecords,
    int? maxBytes,
    int? maxCompressed,
  }) async {
    final fileLen = await raf.length();
    final h = await _readHeaderAsync(raf, fileLen);
    await raf.setPosition(h.blockStart);
    final records = <Map<String, dynamic>>[];
    var decompressed = 0, compressed = 0, pos = h.blockStart;
    while (pos < fileLen) {
      final count = await _readLongAsync(raf);
      final size = await _readLongAsync(raf);
      if (size < 0 || size > fileLen) {
        throw const FormatException('Invalid Avro block size');
      }
      compressed += size;
      if (maxCompressed != null && compressed > maxCompressed) {
        throw StateError(
          'Avro OCF exceeds maxCompressedBytes=$maxCompressed (compressed '
          'input); pass maxCompressedBytes: null to allow it',
        );
      }
      final raw = await raf.read(size);
      if (raw.length != size) {
        throw const FormatException('Truncated Avro block');
      }
      final ceiling = maxBytes == null ? null : maxBytes - decompressed;
      final block = _decompress(h.codec, raw, ceiling);
      decompressed += block.length;
      if (maxBytes != null && decompressed > maxBytes) {
        throw StateError(
          'Avro OCF exceeds maxBytes=$maxBytes (decompressed); pass '
          'maxBytes: null to allow it',
        );
      }
      if (!_sameSync(await raf.read(16), h.sync)) {
        throw const FormatException(
          'Avro block sync mismatch (corrupt or spliced OCF)',
        );
      }
      _appendRecords(block, count, h.schema, records, maxRecords);
      pos = await raf.position();
    }
    return records;
  }

  static List<Map<String, dynamic>> _streamSync(
    RandomAccessFile raf, {
    int? maxRecords,
    int? maxBytes,
    int? maxCompressed,
  }) {
    final fileLen = raf.lengthSync();
    final h = _readHeaderSync(raf, fileLen);
    raf.setPositionSync(h.blockStart);
    final records = <Map<String, dynamic>>[];
    var decompressed = 0, compressed = 0, pos = h.blockStart;
    while (pos < fileLen) {
      final count = _readLongSync(raf);
      final size = _readLongSync(raf);
      if (size < 0 || size > fileLen) {
        throw const FormatException('Invalid Avro block size');
      }
      compressed += size;
      if (maxCompressed != null && compressed > maxCompressed) {
        throw StateError(
          'Avro OCF exceeds maxCompressedBytes=$maxCompressed (compressed '
          'input); pass maxCompressedBytes: null to allow it',
        );
      }
      final raw = raf.readSync(size);
      if (raw.length != size) {
        throw const FormatException('Truncated Avro block');
      }
      final ceiling = maxBytes == null ? null : maxBytes - decompressed;
      final block = _decompress(h.codec, raw, ceiling);
      decompressed += block.length;
      if (maxBytes != null && decompressed > maxBytes) {
        throw StateError(
          'Avro OCF exceeds maxBytes=$maxBytes (decompressed); pass '
          'maxBytes: null to allow it',
        );
      }
      if (!_sameSync(raf.readSync(16), h.sync)) {
        throw const FormatException(
          'Avro block sync mismatch (corrupt or spliced OCF)',
        );
      }
      _appendRecords(block, count, h.schema, records, maxRecords);
      pos = raf.positionSync();
    }
    return records;
  }

  /// Read one zigzag-encoded long (Avro block count/size) byte-by-byte from a RAF.
  static Future<int> _readLongAsync(RandomAccessFile raf) async {
    int result = 0, shift = 0;
    for (int i = 0; i < 10; i++) {
      final b = await raf.readByte();
      if (b < 0) throw const FormatException('Truncated Avro varint');
      result |= (b & 0x7f) << shift;
      if ((b & 0x80) == 0) return (result >>> 1) ^ -(result & 1);
      shift += 7;
    }
    throw const FormatException('Avro varint exceeds 64 bits');
  }

  static int _readLongSync(RandomAccessFile raf) {
    int result = 0, shift = 0;
    for (int i = 0; i < 10; i++) {
      final b = raf.readByteSync();
      if (b < 0) throw const FormatException('Truncated Avro varint');
      result |= (b & 0x7f) << shift;
      if ((b & 0x80) == 0) return (result >>> 1) ^ -(result & 1);
      shift += 7;
    }
    throw const FormatException('Avro varint exceeds 64 bits');
  }

  /// Raw-inflate [raw], stopping (with a [FormatException]) the moment the
  /// output would exceed [ceiling] — the decoder emits in bounded chunks, so a
  /// bomb is rejected before it is fully materialized.
  static Uint8List _inflateBounded(Uint8List raw, int? ceiling) {
    final sink = _CappedSink(ceiling);
    final input = ZLibDecoder(raw: true).startChunkedConversion(sink);
    input.add(raw);
    input.close();
    return sink.bytes();
  }

  /// The uncompressed length prefixing a snappy block (a base-128 varint).
  static int _snappyLength(Uint8List body) {
    var result = 0;
    var shift = 0;
    for (var i = 0; i < body.length && i < 5; i++) {
      final b = body[i];
      result |= (b & 0x7F) << shift;
      if (b & 0x80 == 0) return result;
      shift += 7;
    }
    throw const FormatException('Invalid snappy length prefix');
  }

  /// Reject a block whose decompressed size dwarfs its compressed size (a
  /// decompression bomb). The floor keeps small, naturally-compressible blocks
  /// from tripping the ratio.
  static void _guardRatio(int compressed, int decoded) {
    if (compressed > 0 &&
        decoded > (1 << 20) &&
        decoded > compressed * _maxRatio) {
      throw FormatException(
        'Avro block decompression ratio ${decoded ~/ compressed}x exceeds '
        '${_maxRatio}x (possible decompression bomb)',
      );
    }
  }

  static bool _sameSync(Uint8List a, Uint8List b) {
    if (a.length != b.length) return false;
    for (int i = 0; i < a.length; i++) {
      if (a[i] != b[i]) return false;
    }
    return true;
  }

  static Uint8List _decompress(String codec, Uint8List raw, int? ceiling) {
    switch (codec) {
      case 'null':
        if (ceiling != null && raw.length > ceiling) {
          throw FormatException(
            'Avro block ($raw.length bytes) exceeds the remaining '
            '$ceiling-byte budget',
          );
        }
        return raw;
      case 'deflate':
        // Inflate in bounded chunks, aborting the moment output would exceed the
        // budget — so a bomb never fully materializes.
        final out = _inflateBounded(raw, ceiling);
        _guardRatio(raw.length, out.length);
        return out;
      case 'snappy':
        // Avro snappy = raw snappy block + trailing 4-byte big-endian CRC32 of
        // the UNcompressed data. Verify it so a corrupt block is rejected.
        if (raw.length < 4) {
          throw const FormatException('Truncated Avro snappy block');
        }
        final body = Uint8List.sublistView(raw, 0, raw.length - 4);
        // A snappy block starts with the uncompressed length as a varint —
        // reject an over-budget block BEFORE decompressing it.
        final declared = _snappyLength(body);
        if (ceiling != null && declared > ceiling) {
          throw FormatException(
            'Avro snappy block decompresses to $declared bytes, over the '
            'remaining $ceiling-byte budget (possible decompression bomb)',
          );
        }
        final out = SnappyCodec(framing: false).decompress(body);
        _guardRatio(body.length, out.length);
        final want =
            (raw[raw.length - 4] << 24) |
            (raw[raw.length - 3] << 16) |
            (raw[raw.length - 2] << 8) |
            raw[raw.length - 1];
        if (Crc32.hash(out) != (want & 0xFFFFFFFF)) {
          throw const FormatException('Avro snappy block CRC32 mismatch');
        }
        return out;
      default:
        throw FormatException('Unsupported Avro codec: $codec');
    }
  }
}

/// A decode-output sink that accumulates bytes and throws the moment the running
/// total exceeds [ceiling] — aborting a decompression bomb mid-stream instead of
/// after full inflation.
class _CappedSink implements Sink<List<int>> {
  final int? ceiling;
  final BytesBuilder _b = BytesBuilder(copy: false);

  _CappedSink(this.ceiling);

  @override
  void add(List<int> data) {
    _b.add(data);
    if (ceiling != null && _b.length > ceiling!) {
      throw FormatException(
        'Avro deflate block exceeds the remaining $ceiling-byte budget '
        '(possible decompression bomb)',
      );
    }
  }

  @override
  void close() {}

  Uint8List bytes() => _b.toBytes();
}
