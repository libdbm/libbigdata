import 'dart:io';
import 'dart:typed_data';

import 'package:libcompress/libcompress.dart';

import '../../core/paths.dart';
import '../../core/schema.dart';
import '../../core/table.dart';
import 'arrow_buffers.dart';
import 'arrow_message.dart';
import '../../core/ids.dart' as ids;

// Secure entropy for the write temp-file token so concurrent writers to the same
// destination can't collide.

/// Optional Arrow IPC body compression (per-buffer).
enum ArrowCompression { none, lz4, zstd }

/// Writes a [Table] as a real Apache Arrow IPC **file** (random-access):
/// `ARROW1` magic + a Schema message + one or more RecordBatch messages +
/// a Footer. Output is readable by pyarrow/pandas/Polars/DuckDB. Core types
/// only (see [encodeColumn]); other types raise `UnsupportedError`. With
/// [compression], each body buffer is compressed (LZ4_FRAME or ZSTD) per the
/// Arrow IPC body-compression spec.
class ArrowWriter {
  static final Uint8List _magic = Uint8List.fromList(const [
    65,
    82,
    82,
    79,
    87,
    49,
  ]); // "ARROW1"

  final File _file;
  final int batchSize;
  final ArrowCompression compression;

  ArrowWriter(
    String path, {
    this.batchSize = 65536,
    this.compression = ArrowCompression.none,
  }) : _file = File(path) {
    if (batchSize <= 0) {
      throw ArgumentError.value(batchSize, 'batchSize', 'must be > 0');
    }
  }

  /// Write [table] atomically: stream it to a sibling temp file (one record
  /// batch per [batchSize] rows, so the whole encoded file isn't buffered in
  /// memory) then rename onto the destination. On any failure the temp is
  /// aborted/deleted and the rethrows — a pre-existing file at [_file] is never
  /// left partial/corrupt (mirrors [ParquetWriter]).
  Future<void> write(Table table) async {
    final temp = '${_file.path}.${ids.token()}.tmp';
    if (table.rowCount == 0) {
      // Schema-only file: ArrowStreamWriter writes nothing when no batch is
      // added, so encode the empty file directly.
      try {
        await File(
          temp,
        ).writeAsBytes(_encode(table.schema, const [], compression));
        await replaceAtomic(File(temp), _file.path);
      } catch (_) {
        final f = File(temp);
        if (await f.exists()) await f.delete();
        rethrow;
      }
      return;
    }
    final writer = ArrowStreamWriter(
      temp,
      table.schema,
      compression: compression,
    );
    try {
      for (int s = 0; s < table.rowCount; s += batchSize) {
        final end = (s + batchSize).clamp(0, table.rowCount);
        await writer.add(table.slice(s, end));
      }
      await writer.close();
      await replaceAtomic(File(temp), _file.path);
    } catch (_) {
      // abort() no-ops once close() nulled the handle, so a failed rename after
      // a clean close would otherwise leak the finalized temp — delete it here
      // explicitly (mirrors the writeBatches failure path).
      await writer.abort();
      final f = File(temp);
      if (await f.exists()) await f.delete();
      rethrow;
    }
  }

  Future<void> writeStream(Table table) => write(table);

  /// Stream tables to disk as consecutive record batches with bounded memory:
  /// each batch is encoded and flushed via [ArrowStreamWriter] as it arrives,
  /// rather than buffering every [Table] before encoding. Atomic like [write]:
  /// everything goes to a sibling temp file that is renamed onto [_file] only
  /// after a clean close, so a throw mid-stream (or a partial write) never
  /// corrupts or deletes a pre-existing file at the destination.
  Future<void> writeBatches(Stream<Table> tables, {Schema? schema}) async {
    final temp = '${_file.path}.${ids.token()}.tmp';
    ArrowStreamWriter? writer;
    Schema? s = schema;
    try {
      await for (final t in tables) {
        s ??= t.schema;
        writer ??= ArrowStreamWriter(temp, s, compression: compression);
        await writer.add(t);
      }
      if (writer != null) {
        await writer.close();
        await replaceAtomic(File(temp), _file.path);
        return;
      }
    } catch (_) {
      // A throw mid-stream (unsupported type, disk full) must not leak the open
      // file handle, leave a partial temp, or touch the destination.
      await writer?.abort();
      final f = File(temp);
      if (await f.exists()) await f.delete();
      rethrow;
    }
    // No batches arrived: write a schema-only (empty) file, or error if we have
    // no schema to write.
    if (s == null) {
      throw ArgumentError(
        'writeBatches needs a schema when no tables are given',
      );
    }
    try {
      await File(temp).writeAsBytes(_encode(s, const [], compression));
      await replaceAtomic(File(temp), _file.path);
    } catch (_) {
      final f = File(temp);
      if (await f.exists()) await f.delete();
      rethrow;
    }
  }

  /// Encode a single [table], splitting into record batches of [batchSize] rows.
  static Uint8List encodeTable(
    Table table, {
    int batchSize = 65536,
    ArrowCompression compression = ArrowCompression.none,
  }) {
    if (batchSize <= 0) {
      throw ArgumentError.value(batchSize, 'batchSize', 'must be > 0');
    }
    final batches = <Table>[];
    if (table.rowCount == 0) {
      batches.add(table);
    } else {
      for (int start = 0; start < table.rowCount; start += batchSize) {
        final end = (start + batchSize) < table.rowCount
            ? start + batchSize
            : table.rowCount;
        batches.add(table.slice(start, end));
      }
    }
    return _encode(table.schema, batches, compression);
  }

  static Uint8List _encode(
    Schema schema,
    List<Table> batches,
    ArrowCompression compression,
  ) {
    final fields = schema.fields;
    final codec = switch (compression) {
      ArrowCompression.none => null,
      ArrowCompression.lz4 => Lz4Codec(),
      ArrowCompression.zstd => ZstdCodec(),
    };
    final codecCode = switch (compression) {
      ArrowCompression.none => null,
      ArrowCompression.lz4 => compressionLz4Frame,
      ArrowCompression.zstd => compressionZstd,
    };

    final out = BytesBuilder();
    out.add(_magic);
    out.add(Uint8List(2)); // pad magic to 8 bytes

    _writeMessage(out, buildSchemaMessage(fields), Uint8List(0));

    final blocks = <BlockInfo>[];
    for (final batch in batches) {
      final offset = out.length;
      final body = BytesBuilder();
      final buffers = <BufferInfo>[];
      final nodes = <NodeInfo>[];
      for (int c = 0; c < fields.length; c++) {
        final encoded = encodeColumn(batch.columns[c], fields[c]);
        nodes.addAll(encoded.nodes);
        for (final raw in encoded.buffers) {
          _writeBodyBuffer(body, buffers, raw, codec);
        }
      }
      final bodyBytes = body.toBytes();
      final message = buildRecordBatchMessage(
        batch.rowCount,
        nodes,
        buffers,
        bodyBytes.length,
        codec: codecCode,
      );
      final metaLength = _writeMessage(out, message, bodyBytes);
      blocks.add((
        offset: offset,
        metaLength: metaLength,
        bodyLength: bodyBytes.length,
      ));
    }

    final footer = buildFooter(fields, blocks);
    out.add(footer);
    out.add(_int32(footer.length));
    out.add(_magic);
    return out.toBytes();
  }

  /// Append one body buffer (optionally compressed), recording its
  /// (offset,length) and padding the body to 8 bytes. With a [codec], each
  /// buffer carries an int64 prefix: the uncompressed length, or -1 if stored
  /// raw because compression did not help. Empty buffers carry no prefix.
  static void _writeBodyBuffer(
    BytesBuilder body,
    List<BufferInfo> buffers,
    Uint8List raw,
    CompressionCodec? codec,
  ) {
    final offset = body.length;
    if (codec == null || raw.isEmpty) {
      body.add(raw);
      buffers.add((offset: offset, length: raw.length));
    } else {
      final compressed = codec.compress(raw);
      final useCompressed = compressed.length < raw.length;
      final payload = useCompressed ? compressed : raw;
      final prefix = Uint8List(8);
      ByteData.view(
        prefix.buffer,
      ).setInt64(0, useCompressed ? raw.length : -1, Endian.little);
      body.add(prefix);
      body.add(payload);
      buffers.add((offset: offset, length: 8 + payload.length));
    }
    final pad = (-body.length) & 7;
    for (int i = 0; i < pad; i++) {
      body.addByte(0);
    }
  }

  /// Write an encapsulated message (continuation, padded metadata, body) and
  /// return its metadata length (prefix + padded flatbuffer).
  static int _writeMessage(
    BytesBuilder out,
    Uint8List metadata,
    Uint8List body,
  ) {
    out.add(_uint32(0xFFFFFFFF)); // continuation marker
    final padded = (metadata.length + 7) & ~7;
    out.add(_int32(padded));
    out.add(metadata);
    if (padded > metadata.length) out.add(Uint8List(padded - metadata.length));
    out.add(body); // each buffer is already padded to 8, so body is 8-aligned
    return 8 + padded;
  }

  static Uint8List _int32(int v) {
    final b = Uint8List(4);
    ByteData.view(b.buffer).setInt32(0, v, Endian.little);
    return b;
  }

  static Uint8List _uint32(int v) {
    final b = Uint8List(4);
    ByteData.view(b.buffer).setUint32(0, v, Endian.little);
    return b;
  }
}

/// Incremental Arrow IPC file writer: appends one RecordBatch message to disk per
/// [add], so only the current batch is resident (unlike [ArrowWriter], which
/// builds the whole file in memory). Output is byte-compatible with [ArrowReader]
/// (and pyarrow). The file is created lazily on the first [add]; [close] writes
/// the footer. If nothing is added, no file is written.
class ArrowStreamWriter {
  final String path;
  final List<Field> _fields;
  final CompressionCodec? _codec;
  final int? _codecCode;
  final List<BlockInfo> _blocks = [];
  RandomAccessFile? _raf;
  int _written = 0;

  ArrowStreamWriter(
    this.path,
    Schema schema, {
    ArrowCompression compression = ArrowCompression.none,
  }) : _fields = schema.fields,
       _codec = switch (compression) {
         ArrowCompression.none => null,
         ArrowCompression.lz4 => Lz4Codec(),
         ArrowCompression.zstd => ZstdCodec(),
       },
       _codecCode = switch (compression) {
         ArrowCompression.none => null,
         ArrowCompression.lz4 => compressionLz4Frame,
         ArrowCompression.zstd => compressionZstd,
       };

  Future<void> _open() async {
    if (_raf != null) return;
    _raf = await File(path).open(mode: FileMode.write);
    final header = BytesBuilder();
    header.add(ArrowWriter._magic);
    header.add(Uint8List(2)); // pad magic to 8 bytes
    ArrowWriter._writeMessage(
      header,
      buildSchemaMessage(_fields),
      Uint8List(0),
    );
    await _writeChunk(header.toBytes());
  }

  /// Append [batch] as one RecordBatch message (flushed to disk immediately).
  Future<void> add(Table batch) async {
    // Encoding is strictly positional against the frozen writer schema, so a
    // batch whose columns are reordered/renamed/retyped would silently store
    // data under the wrong field. Require an EXACT type match — `isCompatibleWith`
    // permits int32→int64 widening, which would encode int64 values under an
    // int32 buffer and SILENTLY TRUNCATE them (Field.encodesAs guards decimal
    // scale/precision + nested shape too).
    if (!Schema(fields: _fields).encodesAs(batch.schema)) {
      throw ArgumentError(
        'Batch schema does not match the Arrow writer schema; every batch must '
        'have the same columns in the same order and EXACT type (a wider batch '
        'is not narrowed — declare the wider type when opening the writer)',
      );
    }
    await _open();
    final offset = _written;
    final body = BytesBuilder();
    final buffers = <BufferInfo>[];
    final nodes = <NodeInfo>[];
    for (int c = 0; c < _fields.length; c++) {
      final encoded = encodeColumn(batch.columns[c], _fields[c]);
      nodes.addAll(encoded.nodes);
      for (final raw in encoded.buffers) {
        ArrowWriter._writeBodyBuffer(body, buffers, raw, _codec);
      }
    }
    final bodyBytes = body.toBytes();
    final message = buildRecordBatchMessage(
      batch.rowCount,
      nodes,
      buffers,
      bodyBytes.length,
      codec: _codecCode,
    );
    final framed = BytesBuilder();
    final metaLength = ArrowWriter._writeMessage(framed, message, bodyBytes);
    await _writeChunk(framed.toBytes());
    _blocks.add((
      offset: offset,
      metaLength: metaLength,
      bodyLength: bodyBytes.length,
    ));
  }

  /// Write the footer, flush to stable media, and close. No-op (no file) if
  /// nothing was added.
  ///
  /// The flush is an fsync: `close()` only reaches the OS, so a published file
  /// could otherwise outlive a crash with content that never hit the disk while
  /// the metadata pointing at it (which IS fsynced) survived.
  Future<void> close() async {
    if (_raf == null) return;
    final footer = buildFooter(_fields, _blocks);
    final tail = BytesBuilder();
    tail.add(footer);
    tail.add(ArrowWriter._int32(footer.length));
    tail.add(ArrowWriter._magic);
    await _writeChunk(tail.toBytes());
    await _raf!.flush();
    await _raf!.close();
    _raf = null;
  }

  /// Best-effort teardown for the failure path: close the open file handle (so
  /// the descriptor is not leaked) and delete the partial file. No footer is
  /// written. Idempotent.
  Future<void> abort() async {
    if (_raf == null) return;
    try {
      await _raf!.close();
    } catch (_) {
      /* already closing/closed */
    }
    _raf = null;
    final f = File(path);
    if (await f.exists()) await f.delete();
  }

  Future<void> _writeChunk(Uint8List bytes) async {
    await _raf!.writeFrom(bytes);
    _written += bytes.length;
  }
}
