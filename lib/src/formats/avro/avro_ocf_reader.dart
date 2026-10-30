import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:libcompress/libcompress.dart';

import '../../core/failure.dart';
import '../../core/limits.dart';
import '../../util/varint.dart' show unzigzag;
import '../../core/result.dart';
import 'avro_decoder.dart';

/// Reads an Avro Object Container File (OCF): 4-byte magic `Obj\x01`, a
/// `map<string,bytes>` of file metadata (`avro.schema`, `avro.codec`), a 16-byte
/// sync marker, then data blocks `[count][byteSize][block bytes][sync]`.
/// Supports the `null`, `deflate` (raw), `snappy` (Avro CRC-framed) and
/// `zstandard` codecs.
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
    final file = File(path).openSync();
    try {
      return _streamSync(
        file,
        maxRecords: maxRecords,
        maxBytes: maxBytes,
        maxCompressed: maxCompressedBytes,
      );
    } finally {
      file.closeSync();
    }
  }

  /// Async variant of [readPath]: streams from a [RandomAccessFile] without
  /// blocking the event loop or buffering the whole file. Used by Iceberg
  /// manifest resolution, which is O(#data files) and otherwise unbounded.
  static Future<List<Map<String, dynamic>>> readPathAsync(
    String path, {
    int? maxRecords = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    int? maxCompressedBytes = defaultMaxMaterializedBytes,
  }) async {
    final records = <Map<String, dynamic>>[];
    await for (final record in streamPathAsync(
      path,
      maxBytes: maxBytes,
      maxCompressedBytes: maxCompressedBytes,
    )) {
      records.add(record);
      _checkRecords(records.length, maxRecords);
    }
    return records;
  }

  /// Stream records one at a time from an OCF file, block-by-block, WITHOUT
  /// materializing the whole record list (unlike [readPathAsync]). Used by the
  /// Iceberg manifest planner so a snapshot with many manifests stays bounded:
  /// only one Avro record is resident at a time; the caller keeps just the light
  /// fields it needs and enforces its own global budget. Per-file
  /// [maxBytes]/[maxCompressedBytes] guards still apply (pass null to opt out);
  /// there is no record-count cap here: the caller bounds the total.
  static Stream<Map<String, dynamic>> streamPathAsync(
    String path, {
    int? maxBytes = defaultMaxMaterializedBytes,
    int? maxCompressedBytes = defaultMaxMaterializedBytes,
  }) async* {
    final file = await File(path).open();
    try {
      final length = await file.length();
      final header = await _readHeaderAsync(file, length);
      await file.setPosition(header.blockStart);
      final budget = _Budget(maxBytes, maxCompressedBytes);
      var position = header.blockStart;
      while (position < length) {
        final count = await _readLongAsync(file);
        final size = await _readLongAsync(file);
        budget.reserve(size, length);
        final raw = await file.read(size);
        if (raw.length != size) {
          throw const FormatException('Truncated Avro block');
        }
        final block = budget.inflate(header.codec, raw);
        _checkSync(await file.read(16), header.sync);
        if (count <= 0) {
          throw FormatException(
            'Avro OCF block has a non-positive count: $count',
          );
        }
        final reader = AvroReader(block);
        for (int i = 0; i < count; i++) {
          yield header.schema.decode(reader) as Map<String, dynamic>;
        }
        if (!reader.atEnd) {
          throw const FormatException(
            'Avro OCF block not fully consumed: declared record count too small',
          );
        }
        position = await file.position();
      }
    } finally {
      await file.close();
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
    final reader = AvroReader(bytes);
    final header = _readHeaderFrom(reader);
    final records = <Map<String, dynamic>>[];
    final budget = _Budget(maxBytes, null);
    while (!reader.atEnd) {
      final count = reader.readLong();
      final size = reader.readLong();
      final block = budget.inflate(header.codec, reader.readFixed(size));
      _checkSync(reader.readFixed(16), header.sync);
      _appendRecords(block, count, header.schema, records, maxRecords);
    }
    return records;
  }

  /// Parse the OCF header (magic + metadata map + sync) from [reader], returning the
  /// schema/codec/sync and leaving [reader] positioned at the first data block.
  static ({AvroSchema schema, String codec, Uint8List sync, int blockStart})
  _readHeaderFrom(AvroReader reader) {
    final magic = reader.readFixed(4);
    if (magic[0] != 0x4F ||
        magic[1] != 0x62 ||
        magic[2] != 0x6A ||
        magic[3] != 1) {
      throw const FormatException('Not an Avro OCF (bad magic)');
    }
    // File metadata: an Avro map<string,bytes>.
    final meta = <String, Uint8List>{};
    while (true) {
      var count = reader.readLong();
      if (count == 0) break;
      if (count < 0) {
        reader.readLong();
        count = -count;
      }
      for (int i = 0; i < count; i++) {
        final key = reader.readString();
        meta[key] = Uint8List.fromList(reader.readBytes());
      }
    }
    final sync = reader.readFixed(16);
    final schemaMeta = meta['avro.schema'];
    if (schemaMeta == null) {
      throw const FormatException('Avro OCF missing avro.schema');
    }
    final schema = AvroSchema.parse(utf8.decode(schemaMeta));
    final codec = meta['avro.codec'] == null
        ? 'null'
        : utf8.decode(meta['avro.codec']!);
    return (
      schema: schema,
      codec: codec,
      sync: sync,
      blockStart: reader.position,
    );
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
    // zero times): a corrupt/hostile file must fail loud, not lose records.
    if (count <= 0) {
      throw FormatException('Avro OCF block has a non-positive count: $count');
    }
    final reader = AvroReader(block);
    for (int i = 0; i < count; i++) {
      records.add(schema.decode(reader) as Map<String, dynamic>);
      _checkRecords(records.length, maxRecords);
    }
    // Decoding exactly [count] records must consume the whole decompressed block;
    // leftover bytes mean the declared count under-reads (silent record loss).
    if (!reader.atEnd) {
      throw const FormatException(
        'Avro OCF block not fully consumed: declared record count is too small',
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
  _readHeaderAsync(RandomAccessFile file, int length) async {
    final cap = length < _maxHeaderBytes ? length : _maxHeaderBytes;
    var window = cap < _initialHeaderWindow ? cap : _initialHeaderWindow;
    while (true) {
      await file.setPosition(0);
      final prefix = await file.read(window);
      try {
        return _readHeaderFrom(AvroReader(prefix));
      } on Object catch (error) {
        if ((error is RangeError || error is FormatException) && window < cap) {
          window = (window * 4) < cap ? window * 4 : cap;
          continue;
        }
        rethrow;
      }
    }
  }

  static ({AvroSchema schema, String codec, Uint8List sync, int blockStart})
  _readHeaderSync(RandomAccessFile file, int length) {
    final cap = length < _maxHeaderBytes ? length : _maxHeaderBytes;
    var window = cap < _initialHeaderWindow ? cap : _initialHeaderWindow;
    while (true) {
      file.setPositionSync(0);
      final prefix = file.readSync(window);
      try {
        return _readHeaderFrom(AvroReader(prefix));
      } on Object catch (error) {
        if ((error is RangeError || error is FormatException) && window < cap) {
          window = (window * 4) < cap ? window * 4 : cap;
          continue;
        }
        rethrow;
      }
    }
  }

  static List<Map<String, dynamic>> _streamSync(
    RandomAccessFile file, {
    int? maxRecords,
    int? maxBytes,
    int? maxCompressed,
  }) {
    final length = file.lengthSync();
    final header = _readHeaderSync(file, length);
    file.setPositionSync(header.blockStart);
    final records = <Map<String, dynamic>>[];
    final budget = _Budget(maxBytes, maxCompressed);
    var position = header.blockStart;
    while (position < length) {
      final count = _readLongSync(file);
      final size = _readLongSync(file);
      budget.reserve(size, length);
      final raw = file.readSync(size);
      if (raw.length != size) {
        throw const FormatException('Truncated Avro block');
      }
      final block = budget.inflate(header.codec, raw);
      _checkSync(file.readSync(16), header.sync);
      _appendRecords(block, count, header.schema, records, maxRecords);
      position = file.positionSync();
    }
    return records;
  }

  /// Read one zigzag-encoded long (Avro block count/size) byte-by-byte from a RAF.
  static Future<int> _readLongAsync(RandomAccessFile file) async {
    int result = 0, shift = 0;
    for (int i = 0; i < 10; i++) {
      final byte = await file.readByte();
      if (byte < 0) throw const FormatException('Truncated Avro varint');
      result |= (byte & 0x7f) << shift;
      if ((byte & 0x80) == 0) return unzigzag(result);
      shift += 7;
    }
    throw const FormatException('Avro varint exceeds 64 bits');
  }

  static int _readLongSync(RandomAccessFile file) {
    int result = 0, shift = 0;
    for (int i = 0; i < 10; i++) {
      final byte = file.readByteSync();
      if (byte < 0) throw const FormatException('Truncated Avro varint');
      result |= (byte & 0x7f) << shift;
      if ((byte & 0x80) == 0) return unzigzag(result);
      shift += 7;
    }
    throw const FormatException('Avro varint exceeds 64 bits');
  }

  /// Raw-inflate [raw], stopping (with a [FormatException]) the moment the
  /// output would exceed [ceiling]: the decoder emits in bounded chunks, so a
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
      final byte = body[i];
      result |= (byte & 0x7F) << shift;
      if (byte & 0x80 == 0) return result;
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

  /// Throws [Kind.limit] once [count] records pass [maxRecords].
  static void _checkRecords(int count, int? maxRecords) {
    if (maxRecords != null && count > maxRecords) {
      throw Failure.of(
        Kind.limit,
        'Avro OCF exceeds maxRecords=$maxRecords; pass maxRecords: null to '
        'allow it',
      );
    }
  }

  /// A block's trailing [marker] must be the file's [sync].
  static void _checkSync(Uint8List marker, Uint8List sync) {
    if (!_sameSync(marker, sync)) {
      throw const FormatException(
        'Avro block sync mismatch (corrupt or spliced OCF)',
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
          throw Failure.of(
            Kind.limit,
            'Avro block (${raw.length} bytes) exceeds the remaining '
            '$ceiling-byte budget',
          );
        }
        return raw;
      case 'deflate':
        // Inflate in bounded chunks, aborting the moment output would exceed the
        // budget, so a bomb never fully materializes. No ratio guard: deflate
        // legitimately reaches ~1000x on repetitive data (our own writer's
        // output included), and the budget already bounds the output.
        return _inflateBounded(raw, ceiling);
      case 'snappy':
        // Avro snappy = raw snappy block + trailing 4-byte big-endian CRC32 of
        // the UNcompressed data. Verify it so a corrupt block is rejected.
        if (raw.length < 4) {
          throw const FormatException('Truncated Avro snappy block');
        }
        final body = Uint8List.sublistView(raw, 0, raw.length - 4);
        // A snappy block starts with the uncompressed length as a varint:
        // reject an over-budget block BEFORE decompressing it.
        final declared = _snappyLength(body);
        if (ceiling != null && declared > ceiling) {
          throw Failure.of(
            Kind.limit,
            'Avro snappy block decompresses to $declared bytes, over the '
            'remaining $ceiling-byte budget (possible decompression bomb)',
          );
        }
        final out = SnappyCodec(format: SnappyFormat.raw).decompress(body);
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
      case 'zstandard':
        // One zstd frame per block. The decoder stops at the budget.
        try {
          final out = ZstdCodec(maxSize: ceiling).decompress(raw);
          _guardRatio(raw.length, out.length);
          return out;
        } on LimitException {
          throw Failure.of(
            Kind.limit,
            'Avro zstandard block exceeds the remaining $ceiling-byte budget '
            '(possible decompression bomb)',
          );
        }
      default:
        throw Failure.of(
          Kind.unsupported,
          'Unsupported Avro codec: $codec (null, deflate, snappy and zstandard '
          'are supported)',
        );
    }
  }
}

/// A decode-output sink that accumulates bytes and throws the moment the running
/// total exceeds [ceiling]: aborting a decompression bomb mid-stream instead of
/// after full inflation.
class _CappedSink implements Sink<List<int>> {
  final int? ceiling;
  final BytesBuilder _bytes = BytesBuilder(copy: false);

  _CappedSink(this.ceiling);

  @override
  void add(List<int> data) {
    _bytes.add(data);
    if (ceiling != null && _bytes.length > ceiling!) {
      throw Failure.of(
        Kind.limit,
        'Avro deflate block exceeds the remaining $ceiling-byte budget '
        '(possible decompression bomb)',
      );
    }
  }

  @override
  void close() {}

  Uint8List bytes() => _bytes.toBytes();
}

/// The running totals of one OCF read against its caps: [maxCompressed] bytes
/// of compressed input, [maxBytes] of decompressed blocks (null opts out).
class _Budget {
  final int? maxBytes;
  final int? maxCompressed;
  int _compressed = 0;
  int _decompressed = 0;

  _Budget(this.maxBytes, this.maxCompressed);

  /// Account for a block of [size] compressed bytes, in a file of
  /// [fileLength], before it is read.
  void reserve(int size, int fileLength) {
    if (size < 0 || size > fileLength) {
      throw const FormatException('Invalid Avro block size');
    }
    _compressed += size;
    final cap = maxCompressed;
    if (cap != null && _compressed > cap) {
      throw Failure.of(
        Kind.limit,
        'Avro OCF exceeds maxCompressedBytes=$cap (compressed input); pass '
        'maxCompressedBytes: null to allow it',
      );
    }
  }

  /// [raw] decompressed with [codec], bounded by what is left of [maxBytes].
  Uint8List inflate(String codec, Uint8List raw) {
    final cap = maxBytes;
    final block = AvroOcfReader._decompress(
      codec,
      raw,
      cap == null ? null : cap - _decompressed,
    );
    _decompressed += block.length;
    if (cap != null && _decompressed > cap) {
      throw Failure.of(
        Kind.limit,
        'Avro OCF exceeds maxBytes=$cap (decompressed); pass maxBytes: null '
        'to allow it',
      );
    }
    return block;
  }
}
