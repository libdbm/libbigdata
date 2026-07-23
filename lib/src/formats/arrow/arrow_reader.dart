import 'dart:io';
import 'dart:typed_data';

import 'package:libcompress/libcompress.dart';

import '../../core/column.dart';
import '../../core/limits.dart';
import '../../core/schema.dart';
import '../../core/table.dart';
import '../parquet/reader/file_reader.dart';
import 'arrow_buffers.dart';
import 'arrow_message.dart';
import 'flatbuffers.dart';

/// Reads a real Apache Arrow IPC **file** into a [Table]. Handles files with one
/// or many record batches (e.g. pyarrow output), concatenating them. Core types
/// only; other Arrow types raise `UnsupportedError`.
class ArrowReader {
  static const List<int> _magic = [65, 82, 82, 79, 87, 49]; // "ARROW1"

  /// Absolute ceiling on a single decompressed Arrow body buffer (bomb guard).
  static const _maxArrowBufferBytes = 1 << 31; // 2 GiB

  final File _file;

  ArrowReader(String path) : _file = File(path);

  /// Read the file into one [Table]. To stop an accidental full load from
  /// OOMing, the read STOPS and throws once it would exceed [maxRows] (default
  /// [defaultMaxMaterializedRows]) — driving the lazy per-block [readBatches] so
  /// the whole file is never buffered. Pass `maxRows: null` to read the whole
  /// file at once (unbounded).
  Future<Table> read({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) async {
    if (maxRows == null && maxBytes == null) {
      final bytes = await _file.readAsBytes();
      return decode(bytes);
    }
    final parts = <List<Column>>[];
    Schema? schema;
    var rows = 0;
    var bytes = 0;
    // Enforce the byte ceiling per physical block too, so a single oversized
    // batch fails before it is allocated rather than after decode.
    await for (final t in readBatches(maxBlockBytes: maxBytes)) {
      schema ??= t.schema;
      rows += t.rowCount;
      bytes += t.memoryUsage;
      enforceMaterializationCaps(rows, bytes, maxRows, maxBytes, 'Arrow file');
      parts.add(t.columns);
    }
    if (schema == null) {
      // No record batches (a schema-only / 0-row file): readBatches yields
      // nothing. Recover the schema from the FOOTER region alone — a crafted file
      // with a large body but zero declared batches must not slip past the
      // bounded path via a whole-file readAsBytes().
      return Table.empty('arrow_table', await readSchema());
    }
    final fields = schema.fields;
    return Table(
      name: 'arrow_table',
      schema: schema,
      columns: [
        for (int c = 0; c < fields.length; c++)
          parts.length == 1
              ? parts.first[c]
              : _concat(fields[c], [for (final p in parts) p[c]]),
      ],
    );
  }

  Future<Table> readStream() => read();

  /// Read ONLY the footer region and return its schema — without loading any
  /// record-batch body. Used both for the zero-batch case and by the scan
  /// contract's `readSchema`, which must not decode a whole batch just to learn
  /// the columns (the footer already holds them). Mirrors the footer prologue of
  /// [readBatches].
  Future<Schema> readSchema() async {
    final reader = FileReader(_file.path);
    await reader.open();
    try {
      final n = reader.length;
      if (n < 12) {
        throw FormatException('Not an Arrow IPC file (too small): $n bytes');
      }
      final tail = await reader.readBytes(n - 10, 10);
      if (tail.length < 10 || !_isMagic(tail, 4)) {
        throw FormatException('Not an Arrow IPC file (missing ARROW1 magic)');
      }
      final footerLen = ByteData.view(
        tail.buffer,
        tail.offsetInBytes,
      ).getInt32(0, Endian.little);
      if (footerLen < 0 ||
          footerLen > n - 10 ||
          footerLen > defaultMaxFooterBytes) {
        throw FormatException('Invalid Arrow footer length: $footerLen');
      }
      final footerBytes = await reader.readBytes(n - 10 - footerLen, footerLen);
      final footer = _parseFooter(footerBytes);
      return Schema(fields: footer.fields);
    } finally {
      await reader.close();
    }
  }

  /// Yield one [Table] per record batch with bounded memory: the footer is read
  /// from the tail, then each record-batch block is seeked and read on demand
  /// (peak memory ≈ one block + its decoded columns), instead of buffering the
  /// whole file. [batchSize] is currently advisory — one Table is yielded per
  /// record-batch block (sub-block row chunking would be a separate change).
  /// NOTE: dictionary-encoded Arrow files are unsupported (the writer emits none
  /// and the footer's dictionary vector is not read), so this is safe for files
  /// this library wrote; third-party dictionary files are out of scope.
  Stream<Table> readBatches({
    int batchSize = 10000,
    List<String>? columns,
    int? maxBlockBytes = defaultMaxMaterializedBytes,
  }) async* {
    if (batchSize <= 0) {
      throw ArgumentError.value(batchSize, 'batchSize', 'must be > 0');
    }
    final reader = FileReader(_file.path);
    await reader.open();
    try {
      final n = reader.length;
      if (n < 12) {
        throw FormatException('Not an Arrow IPC file (too small): $n bytes');
      }
      // Tail layout: [...][footer][footerLen:i32][ARROW1]. footerLen is at n-10,
      // the end magic at n-6.
      final tail = await reader.readBytes(n - 10, 10);
      if (tail.length < 10 || !_isMagic(tail, 4)) {
        throw FormatException('Not an Arrow IPC file (missing ARROW1 magic)');
      }
      final footerLen = ByteData.view(
        tail.buffer,
        tail.offsetInBytes,
      ).getInt32(0, Endian.little);
      if (footerLen < 0 ||
          footerLen > n - 10 ||
          footerLen > defaultMaxFooterBytes) {
        throw FormatException('Invalid Arrow footer length: $footerLen');
      }
      final footerBytes = await reader.readBytes(n - 10 - footerLen, footerLen);
      final footer = _parseFooter(footerBytes);
      // Projection pushdown: decode only the requested columns (skip-decode the
      // rest), and emit a schema restricted to them (file order). A requested
      // column absent from the file fails loud — consistent with Parquet/CSV —
      // rather than silently narrowing to the intersection.
      if (columns != null) {
        final present = {for (final f in footer.fields) f.name};
        final missing = columns.where((c) => !present.contains(c)).toList();
        if (missing.isNotEmpty) {
          throw ArgumentError(
            'Unknown column(s) in projection: ${missing.join(', ')}',
          );
        }
      }
      final keep = columns?.toSet();
      final outFields = keep == null
          ? footer.fields
          : [
              for (final f in footer.fields)
                if (keep.contains(f.name)) f,
            ];
      final schema = Schema(fields: outFields);
      for (final block in footer.batches) {
        final blockLength = block.metaLength + block.bodyLength;
        // Bound the physical block BEFORE allocating it: one giant record batch
        // would otherwise be read whole and decoded before any post-decode
        // maxRows/maxBytes guard fires — OOM. Pass maxBlockBytes: null to opt out.
        if (maxBlockBytes != null && blockLength > maxBlockBytes) {
          throw StateError(
            'Arrow record batch is $blockLength bytes, exceeds '
            'maxBlockBytes=$maxBlockBytes; pass maxBlockBytes: null to allow it',
          );
        }
        _checkRange(block.offset, blockLength, reader.length, 'footer block');
        final blockBytes = await reader.readBytes(block.offset, blockLength);
        yield Table(
          name: 'arrow_batch',
          columns: _readBatch(
            blockBytes,
            0,
            block.bodyLength,
            footer.fields,
            keep,
            maxBlockBytes,
          ),
          schema: schema,
        );
      }
    } finally {
      await reader.close();
    }
  }

  /// Decode a whole Arrow IPC file held in [bytes].
  static Table decode(Uint8List bytes) {
    final footer = _footer(bytes);
    final fields = footer.fields;
    final schema = Schema(fields: fields);

    final parts = <List<Column>>[];
    for (final block in footer.batches) {
      _checkRange(
        block.offset,
        block.metaLength + block.bodyLength,
        bytes.length,
        'footer block',
      );
      parts.add(_readBatch(bytes, block.offset, block.bodyLength, fields));
    }

    final columns = <Column>[];
    for (int c = 0; c < fields.length; c++) {
      if (parts.isEmpty) {
        columns.add(_empty(fields[c]));
      } else if (parts.length == 1) {
        columns.add(parts.first[c]);
      } else {
        columns.add(_concat(fields[c], [for (final p in parts) p[c]]));
      }
    }
    return Table(name: 'arrow_table', columns: columns, schema: schema);
  }

  static ({List<Field> fields, List<BlockInfo> batches}) _footer(
    Uint8List bytes,
  ) {
    final n = bytes.length;
    if (n < 12 || !_isMagic(bytes, 0) || !_isMagic(bytes, n - 6)) {
      throw FormatException('Not an Arrow IPC file (missing ARROW1 magic)');
    }
    final footerLen = ByteData.view(
      bytes.buffer,
      bytes.offsetInBytes + n - 10,
      4,
    ).getInt32(0, Endian.little);
    if (footerLen < 0 ||
        footerLen > n - 10 ||
        footerLen > defaultMaxFooterBytes) {
      throw FormatException('Invalid Arrow footer length: $footerLen');
    }
    final footerStart = n - 10 - footerLen;
    final footerBytes = Uint8List.sublistView(
      bytes,
      footerStart,
      footerStart + footerLen,
    );
    return _parseFooter(footerBytes);
  }

  /// Parse the footer flatbuffer, converting a raw RangeError from a corrupt
  /// vtable/vector offset into the house FormatException.
  static ({List<Field> fields, List<BlockInfo> batches}) _parseFooter(
    Uint8List footerBytes,
  ) {
    try {
      return parseFooter(FbReader(footerBytes));
    } on RangeError catch (e) {
      throw FormatException('Corrupt Arrow footer: $e');
    }
  }

  /// Fail loud on a range that a corrupt footer block would drive out of bounds,
  /// rather than surfacing a raw RangeError from the ByteData/sublistView below.
  static void _checkRange(int start, int length, int limit, String what) {
    if (start < 0 || length < 0 || start + length > limit) {
      throw FormatException(
        'Corrupt Arrow $what: bytes [$start, ${start + length}) exceed the '
        '$limit-byte buffer',
      );
    }
  }

  /// Decode one record-batch block. [bytes] may be the whole file (with the
  /// block at absolute [blockOffset]) or a block-local slice (with
  /// [blockOffset] == 0) — both work since every offset below is relative to
  /// [blockOffset].
  static List<Column> _readBatch(
    Uint8List bytes,
    int blockOffset,
    int bodyLength,
    List<Field> fields, [
    Set<String>? keep,
    int? maxDecompressedBytes,
  ]) {
    final bd = ByteData.view(bytes.buffer, bytes.offsetInBytes);
    final p = blockOffset;
    _checkRange(p, 8, bytes.length, 'batch header'); // continuation + metaSize
    final continuation = bd.getUint32(p, Endian.little);
    if (continuation != 0xFFFFFFFF) {
      throw FormatException(
        'Invalid record batch (missing continuation marker)',
      );
    }
    final metaSize = bd.getInt32(p + 4, Endian.little);
    final metaStart = p + 8;
    _checkRange(metaStart, metaSize, bytes.length, 'batch metadata');
    final r = FbReader(
      Uint8List.sublistView(bytes, metaStart, metaStart + metaSize),
    );
    final message = parseMessage(r);
    if (message.headerType != headerRecordBatch) {
      throw FormatException(
        'Expected a RecordBatch message at block offset $p',
      );
    }
    final rb = parseRecordBatch(r, message.headerTable);
    final bodyStart = metaStart + metaSize;
    _checkRange(bodyStart, bodyLength, bytes.length, 'batch body');
    // Validate the untrusted metadata graph BEFORE any decode/allocation: node
    // row counts, null counts, buffer bounds, and the codec are all attacker-
    // controlled, and a decode allocates List.filled(node.length) (a `null_`
    // node has no buffers, so nothing else bounds it).
    _validateRecordBatch(rb, bodyLength, defaultMaxMaterializedRows);
    var body = Uint8List.sublistView(bytes, bodyStart, bodyStart + bodyLength);
    var buffers = rb.buffers;

    if (rb.codec != null) {
      (body, buffers) = _decompressBody(
        body,
        rb.buffers,
        rb.codec!,
        maxDecompressedBytes,
      );
    }

    final cursor = ArrowCursor();
    final out = <Column>[];
    for (final field in fields) {
      // Projection pushdown: decode only selected fields; for the rest just
      // advance the cursor past their nodes/buffers (no decode, no alloc).
      if (keep == null || keep.contains(field.name)) {
        out.add(decodeTree(field, rb.nodes, buffers, body, cursor));
      } else {
        skipTree(field, cursor);
      }
    }
    return out;
  }

  /// Validate an untrusted record-batch metadata graph before decode/alloc:
  /// bounded logical rows (across all nodes), sane null counts, in-body buffer
  /// ranges, and a recognized compression code.
  static void _validateRecordBatch(
    ({int length, List<NodeInfo> nodes, List<BufferInfo> buffers, int? codec})
    rb,
    int bodyLength,
    int maxBatchRows,
  ) {
    if (rb.codec != null &&
        rb.codec != compressionLz4Frame &&
        rb.codec != compressionZstd) {
      throw FormatException('Unsupported Arrow compression code: ${rb.codec}');
    }
    var totalRows = 0;
    for (final node in rb.nodes) {
      if (node.length < 0 ||
          node.nullCount < 0 ||
          node.nullCount > node.length) {
        throw FormatException(
          'Invalid Arrow field node (length=${node.length}, '
          'nullCount=${node.nullCount})',
        );
      }
      totalRows += node.length;
      if (totalRows > maxBatchRows) {
        throw FormatException(
          'Arrow record batch declares more than $maxBatchRows rows across '
          'its field nodes',
        );
      }
    }
    for (final b in rb.buffers) {
      if (b.offset < 0 || b.length < 0 || b.offset + b.length > bodyLength) {
        throw FormatException(
          'Arrow buffer out of body bounds (offset=${b.offset}, '
          'length=${b.length}, body=$bodyLength)',
        );
      }
    }
  }

  /// Decompress each body buffer per the Arrow body-compression spec, returning
  /// a fresh contiguous (8-byte aligned) body plus rebased buffer infos.
  /// A zstd/lz4 codec bounded to [maxSize] decompressed bytes (throws mid-decode
  /// before over-allocating); null keeps the codec's own default cap.
  static CompressionCodec _boundedCodec(int codecCode, int? maxSize) {
    if (codecCode == compressionZstd) {
      return maxSize == null
          ? ZstdCodec()
          : ZstdCodec(maxDecompressedSize: maxSize);
    }
    return maxSize == null
        ? Lz4Codec()
        : Lz4Codec(maxDecompressedSize: maxSize);
  }

  static (Uint8List, List<BufferInfo>) _decompressBody(
    Uint8List body,
    List<BufferInfo> buffers,
    int codecCode, [
    int? maxDecompressedBytes,
  ]) {
    final out = BytesBuilder();
    final rebased = <BufferInfo>[];
    for (final b in buffers) {
      final offset = out.length;
      Uint8List decompressed;
      if (b.length == 0) {
        decompressed = Uint8List(0);
      } else {
        // Bound the 8-byte length prefix read: b.length may be < 8, so the
        // in-body buffer check isn't sufficient — verify the view fits.
        _checkRange(b.offset, 8, body.length, 'compression prefix');
        final prefix = ByteData.view(
          body.buffer,
          body.offsetInBytes + b.offset,
          8,
        ).getInt64(0, Endian.little);
        final payload = Uint8List.sublistView(
          body,
          b.offset + 8,
          b.offset + b.length,
        );
        // `prefix` is the declared uncompressed length (-1 = stored raw). Bound
        // it before allocating and verify the codec produced exactly it, so a
        // malformed prefix can't drive an unbounded alloc or silent corruption.
        if (prefix < -1 || prefix > _maxArrowBufferBytes) {
          throw FormatException('Implausible Arrow buffer length: $prefix');
        }
        if (prefix == -1) {
          decompressed = payload;
        } else {
          // Cap the codec to the tightest of this buffer's declared output and
          // the REMAINING cumulative budget, so it throws mid-decode BEFORE
          // over-allocating (zstd/lz4 have no cheap length prefix to pre-check).
          final remaining = maxDecompressedBytes == null
              ? null
              : maxDecompressedBytes - out.length;
          final cap = remaining == null || remaining >= prefix
              ? prefix
              : remaining;
          decompressed = _boundedCodec(codecCode, cap).decompress(payload);
        }
        if (prefix >= 0 && decompressed.length != prefix) {
          throw FormatException(
            'Arrow buffer decompressed to ${decompressed.length}, '
            'declared $prefix',
          );
        }
      }
      // Per-buffer bounding (the 2 GiB prefix cap above) does not bound the SUM:
      // a compressed block that passes the physical maxBlockBytes check can hold
      // many buffers that together decompress to far more (a compression bomb).
      // Enforce a cumulative ceiling on the reconstructed body.
      if (maxDecompressedBytes != null &&
          out.length + decompressed.length > maxDecompressedBytes) {
        throw FormatException(
          'Arrow batch decompresses to more than $maxDecompressedBytes bytes '
          '(compression bomb); pass maxBlockBytes: null to allow it',
        );
      }
      out.add(decompressed);
      rebased.add((offset: offset, length: decompressed.length));
      final pad = (-out.length) & 7;
      for (int i = 0; i < pad; i++) {
        out.addByte(0);
      }
    }
    return (out.toBytes(), rebased);
  }

  static bool _isMagic(Uint8List bytes, int at) {
    for (int i = 0; i < 6; i++) {
      if (bytes[at + i] != _magic[i]) return false;
    }
    return true;
  }

  static Column _empty(Field f) => ListColumn(
    name: f.name,
    dataType: f.dataType,
    data: const [],
    nullable: f.nullable,
  );

  /// Concatenate a column's pieces across record batches.
  static Column _concat(Field f, List<Column> parts) {
    // Typed bulk concat (NumericColumn → setRange, no boxing; non-numeric keeps a
    // boxed copy) instead of materializing every part into a boxed list. The
    // parts are slices of one logical column, so they share [f]'s name + dtype.
    return Column.concat(parts);
  }
}
