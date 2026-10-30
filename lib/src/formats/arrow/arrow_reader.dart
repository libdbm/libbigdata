import 'dart:io';
import 'dart:math' as math;
import 'dart:typed_data';

import 'package:libcompress/libcompress.dart';

import '../../core/column.dart';
import '../../core/failure.dart';
import '../../core/limits.dart';
import '../../core/result.dart';
import '../../core/schema.dart';
import '../../core/table.dart';
import '../parquet/reader/file_reader.dart';
import 'arrow_buffers.dart';
import 'arrow_message.dart';
import 'flatbuffers.dart';

/// Reads a real Apache Arrow IPC **file** into a [Table], or with [readStream]
/// the IPC **stream** format. Handles one or many record batches (e.g.
/// pyarrow output), concatenating them. Core types only; other Arrow types
/// raise `UnsupportedError`.
class ArrowReader {
  static const List<int> _magic = [65, 82, 82, 79, 87, 49]; // "ARROW1"

  /// Absolute ceiling on a single decompressed Arrow body buffer (bomb guard).
  static const _maxArrowBufferBytes = 1 << 31; // 2 GiB

  final File _file;

  ArrowReader(String path) : _file = File(path);

  /// Read the file into one [Table]. To stop an accidental full load from
  /// OOMing, the read STOPS and throws once it would exceed [maxRows] (default
  /// [defaultMaxMaterializedRows]): driving the lazy per-block [readBatches] so
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
    await for (final table in readBatches(maxBlockBytes: maxBytes)) {
      schema ??= table.schema;
      rows += table.rowCount;
      bytes += table.memoryUsage;
      enforceMaterializationCaps(rows, bytes, maxRows, maxBytes, 'Arrow file');
      parts.add(table.columns);
    }
    if (schema == null) {
      // No record batches (a schema-only / 0-row file): readBatches yields
      // nothing. Recover the schema from the FOOTER region alone: a crafted file
      // with a large body but zero declared batches must not slip past the
      // bounded path via a whole-file readAsBytes().
      return Table.empty('arrow_table', await readSchema());
    }
    return _join(schema.fields, parts);
  }

  /// One table from the per-batch column [parts] of [fields] (none for an
  /// empty file or stream).
  static Table _join(List<Field> fields, List<List<Column>> parts) => Table(
    name: 'arrow_table',
    schema: Schema(fields: fields),
    columns: [
      for (int c = 0; c < fields.length; c++)
        parts.isEmpty
            ? _empty(fields[c])
            : parts.length == 1
            ? parts.first[c]
            : _concat(fields[c], [for (final part in parts) part[c]]),
    ],
  );

  /// Read an IPC **stream** (what `pa.ipc.new_stream` writes: a Schema
  /// message, then record batches, ending at the end-of-stream marker or the
  /// end of the file) into one [Table], stopping at [maxRows]/[maxBytes] as
  /// [read] does. A file in the IPC file format (its `ARROW1` magic) reads as
  /// [read] reads it. A dictionary batch is [Kind.unsupported].
  Future<Table> readStream({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) async {
    final reader = FileReader(_file.path);
    await reader.open();
    try {
      final n = reader.length;
      if (n >= 6 && _isMagic(await reader.readBytes(0, 6), 0)) {
        return await read(maxRows: maxRows, maxBytes: maxBytes);
      }
      List<Field>? fields;
      final parts = <List<Column>>[];
      var rows = 0;
      var bytes = 0;
      var position = 0;
      while (position + 8 <= n) {
        final head = await reader.readBytes(position, 8);
        final view = ByteData.view(head.buffer, head.offsetInBytes, 8);
        if (view.getUint32(0, Endian.little) != 0xFFFFFFFF) {
          throw FormatException(
            'Arrow IPC stream message at byte $position has no continuation '
            'marker',
          );
        }
        final size = view.getInt32(4, Endian.little);
        if (size == 0) break; // the end-of-stream marker
        if (size < 0 || size > defaultMaxFooterBytes) {
          throw FormatException('Invalid Arrow message length: $size');
        }
        _checkRange(position + 8, size, n, 'stream message');
        final metadata = await reader.readBytes(position + 8, size);
        final ParsedMessage message;
        try {
          message = parseMessage(FbReader(metadata));
        } on RangeError catch (e) {
          throw FormatException('Corrupt Arrow message: $e');
        }
        final body = message.bodyLength;
        if (body < 0) throw FormatException('Invalid Arrow body length: $body');
        if (fields == null) {
          if (message.headerType != headerSchema) {
            throw const FormatException(
              'An Arrow IPC stream must begin with a Schema message',
            );
          }
          fields = parseSchema(FbReader(metadata), message.headerTable);
          position += 8 + size + body;
          continue;
        }
        if (message.headerType == headerDictionaryBatch) {
          throw Failure.of(
            Kind.unsupported,
            'Arrow dictionary batches are not supported',
          );
        }
        if (message.headerType != headerRecordBatch) {
          throw FormatException(
            'Unexpected Arrow message type ${message.headerType} at byte '
            '$position',
          );
        }
        final length = 8 + size + body;
        // Bound the physical batch before allocating it, as [readBatches]
        // does for a file's blocks.
        if (maxBytes != null && length > maxBytes) {
          throw Failure.of(
            Kind.limit,
            'Arrow record batch is $length bytes, exceeds maxBytes=$maxBytes; '
            'pass maxBytes: null to allow it',
          );
        }
        _checkRange(position, length, n, 'stream record batch');
        final block = await reader.readBytes(position, length);
        final columns = _readBatch(block, 0, body, fields, null, maxBytes);
        rows += columns.isEmpty ? 0 : columns.first.length;
        bytes += columns.fold(0, (sum, column) => sum + column.memoryUsage);
        enforceMaterializationCaps(
          rows,
          bytes,
          maxRows,
          maxBytes,
          'Arrow stream',
        );
        parts.add(columns);
        position += length;
      }
      if (fields == null) {
        throw const FormatException('Arrow IPC stream has no Schema message');
      }
      return _join(fields, parts);
    } finally {
      await reader.close();
    }
  }

  /// Read ONLY the footer region and return its schema, without loading any
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
      final size = ByteData.view(
        tail.buffer,
        tail.offsetInBytes,
      ).getInt32(0, Endian.little);
      if (size < 0 || size > n - 10 || size > defaultMaxFooterBytes) {
        throw FormatException('Invalid Arrow footer length: $size');
      }
      final raw = await reader.readBytes(n - 10 - size, size);
      final footer = _parseFooter(raw);
      return Schema(fields: footer.fields);
    } finally {
      await reader.close();
    }
  }

  /// Yield one [Table] per record batch with bounded memory: the footer is read
  /// from the tail, then each record-batch block is seeked and read on demand
  /// (peak memory ≈ one block + its decoded columns), instead of buffering the
  /// whole file. A record batch larger than [batchSize] rows is sliced; a
  /// [batchSize] below 1 is a programming error and throws at the call.
  /// NOTE: dictionary-encoded Arrow files are unsupported (the writer emits none
  /// and the footer's dictionary vector is not read), so this is safe for files
  /// this library wrote; third-party dictionary files are out of scope.
  Stream<Table> readBatches({
    int batchSize = 10000,
    List<String>? columns,
    int? maxBlockBytes = defaultMaxMaterializedBytes,
  }) {
    if (batchSize <= 0) {
      throw ArgumentError.value(batchSize, 'batchSize', 'must be > 0');
    }
    return _blocks(columns, maxBlockBytes).expand(
      (table) => [
        for (var start = 0; start < table.rowCount; start += batchSize)
          start == 0 && batchSize >= table.rowCount
              ? table
              : table.slice(
                  start,
                  start + batchSize < table.rowCount
                      ? start + batchSize
                      : table.rowCount,
                ),
      ],
    );
  }

  /// One [Table] per record-batch block of the file.
  Stream<Table> _blocks(List<String>? columns, int? maxBlockBytes) async* {
    final reader = FileReader(_file.path);
    await reader.open();
    try {
      final n = reader.length;
      if (n < 12) {
        throw FormatException('Not an Arrow IPC file (too small): $n bytes');
      }
      // Tail layout: [...][footer][size:i32][ARROW1]. The size is at n-10,
      // the end magic at n-6.
      final tail = await reader.readBytes(n - 10, 10);
      if (tail.length < 10 || !_isMagic(tail, 4)) {
        throw FormatException('Not an Arrow IPC file (missing ARROW1 magic)');
      }
      final size = ByteData.view(
        tail.buffer,
        tail.offsetInBytes,
      ).getInt32(0, Endian.little);
      if (size < 0 || size > n - 10 || size > defaultMaxFooterBytes) {
        throw FormatException('Invalid Arrow footer length: $size');
      }
      final raw = await reader.readBytes(n - 10 - size, size);
      final footer = _parseFooter(raw);
      // Projection pushdown: decode only the requested columns (skip-decode the
      // rest), and emit a schema restricted to them (file order). A requested
      // column absent from the file fails loud: consistent with Parquet/CSV,
      // rather than silently narrowing to the intersection.
      if (columns != null) {
        final present = {for (final f in footer.fields) f.name};
        final missing = columns.where((c) => !present.contains(c)).toList();
        if (missing.isNotEmpty) {
          throw Failure.of(
            Kind.invalid,
            'Unknown column(s) in projection: ${missing.join(', ')}',
          );
        }
      }
      final keep = columns?.toSet();
      final fields = keep == null
          ? footer.fields
          : [
              for (final f in footer.fields)
                if (keep.contains(f.name)) f,
            ];
      final schema = Schema(fields: fields);
      for (final block in footer.batches) {
        final length = block.metaLength + block.bodyLength;
        // Bound the physical block BEFORE allocating it: one giant record batch
        // would otherwise be read whole and decoded before any post-decode
        // maxRows/maxBytes guard fires, risking OOM. Pass maxBlockBytes: null to opt out.
        if (maxBlockBytes != null && length > maxBlockBytes) {
          throw Failure.of(
            Kind.limit,
            'Arrow record batch is $length bytes, exceeds '
            'maxBlockBytes=$maxBlockBytes; pass maxBlockBytes: null to allow it',
          );
        }
        _checkRange(block.offset, length, reader.length, 'footer block');
        final data = await reader.readBytes(block.offset, length);
        yield Table(
          name: 'arrow_batch',
          columns: _readBatch(
            data,
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
        columns.add(_concat(fields[c], [for (final part in parts) part[c]]));
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
    final size = ByteData.view(
      bytes.buffer,
      bytes.offsetInBytes + n - 10,
      4,
    ).getInt32(0, Endian.little);
    if (size < 0 || size > n - 10 || size > defaultMaxFooterBytes) {
      throw FormatException('Invalid Arrow footer length: $size');
    }
    final start = n - 10 - size;
    final raw = Uint8List.sublistView(bytes, start, start + size);
    return _parseFooter(raw);
  }

  /// Parse the footer flatbuffer, converting a raw RangeError from a corrupt
  /// vtable/vector offset into the house FormatException.
  static ({List<Field> fields, List<BlockInfo> batches}) _parseFooter(
    Uint8List bytes,
  ) {
    try {
      return parseFooter(FbReader(bytes));
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
  /// [blockOffset] == 0), both work since every offset below is relative to
  /// [blockOffset].
  static List<Column> _readBatch(
    Uint8List bytes,
    int blockOffset,
    int bodyLength,
    List<Field> fields, [
    Set<String>? keep,
    int? budget,
  ]) {
    final view = ByteData.view(bytes.buffer, bytes.offsetInBytes);
    final position = blockOffset;
    _checkRange(
      position,
      8,
      bytes.length,
      'batch header',
    ); // continuation + metaSize
    final continuation = view.getUint32(position, Endian.little);
    if (continuation != 0xFFFFFFFF) {
      throw FormatException(
        'Invalid record batch (missing continuation marker)',
      );
    }
    final metaSize = view.getInt32(position + 4, Endian.little);
    final metaStart = position + 8;
    _checkRange(metaStart, metaSize, bytes.length, 'batch metadata');
    final reader = FbReader(
      Uint8List.sublistView(bytes, metaStart, metaStart + metaSize),
    );
    final message = parseMessage(reader);
    if (message.headerType != headerRecordBatch) {
      throw FormatException(
        'Expected a RecordBatch message at block offset $position',
      );
    }
    final batch = parseRecordBatch(reader, message.headerTable);
    final bodyStart = metaStart + metaSize;
    _checkRange(bodyStart, bodyLength, bytes.length, 'batch body');
    // Validate the untrusted metadata graph BEFORE any decode/allocation: node
    // row counts, null counts, buffer bounds, and the codec are all attacker-
    // controlled, and a decode allocates List.filled(node.length) (a `null_`
    // node has no buffers, so nothing else bounds it).
    _validate(batch, bodyLength);
    var body = Uint8List.sublistView(bytes, bodyStart, bodyStart + bodyLength);
    var buffers = batch.buffers;

    if (batch.codec != null) {
      (body, buffers) = _decompress(body, batch.buffers, batch.codec!, budget);
    }

    _checkRows(batch.nodes, body.length);

    // The schema dictates how many nodes and buffers the decode walks; a batch
    // that declares fewer would index past its vectors.
    final need = ArrowCursor();
    for (final field in fields) {
      skipTree(field, need);
    }
    if (need.node > batch.nodes.length || need.buffer > batch.buffers.length) {
      throw FormatException(
        'Arrow record batch has ${batch.nodes.length} nodes and '
        '${batch.buffers.length} buffers; the schema needs ${need.node} and '
        '${need.buffer}',
      );
    }

    final cursor = ArrowCursor();
    final out = <Column>[];
    for (final field in fields) {
      // Projection pushdown: decode only selected fields; for the rest just
      // advance the cursor past their nodes/buffers (no decode, no alloc).
      if (keep == null || keep.contains(field.name)) {
        out.add(decodeTree(field, batch.nodes, buffers, body, cursor));
      } else {
        skipTree(field, cursor);
      }
    }
    return out;
  }

  /// Validate an untrusted record-batch metadata graph before decode/alloc:
  /// sane node lengths and null counts, in-body buffer ranges, and a
  /// recognized compression code.
  static void _validate(
    ({int length, List<NodeInfo> nodes, List<BufferInfo> buffers, int? codec})
    batch,
    int bodyLength,
  ) {
    if (batch.codec != null &&
        batch.codec != compressionLz4Frame &&
        batch.codec != compressionZstd) {
      throw FormatException(
        'Unsupported Arrow compression code: ${batch.codec}',
      );
    }
    for (final node in batch.nodes) {
      if (node.length < 0 ||
          node.nullCount < 0 ||
          node.nullCount > node.length) {
        throw FormatException(
          'Invalid Arrow field node (length=${node.length}, '
          'nullCount=${node.nullCount})',
        );
      }
    }
    for (final buffer in batch.buffers) {
      if (buffer.offset < 0 ||
          buffer.length < 0 ||
          buffer.offset + buffer.length > bodyLength) {
        throw FormatException(
          'Arrow buffer out of body bounds (offset=${buffer.offset}, '
          'length=${buffer.length}, body=$bodyLength)',
        );
      }
    }
  }

  /// Bound the values the decode allocates (it fills `List.filled(length)` per
  /// node). A `null_` node, or one without a validity buffer, is backed by no
  /// bytes, so the total across nodes may not exceed the row cap or one value
  /// per bit of the (decompressed) body, whichever is larger. Legitimate wide
  /// batches stay under the latter: every row of a typed column costs at least
  /// one bit.
  static void _checkRows(List<NodeInfo> nodes, int bodyLength) {
    final limit = math.max(defaultMaxMaterializedRows, bodyLength * 8);
    var total = 0;
    for (final node in nodes) {
      total += node.length;
      if (total > limit) {
        throw FormatException(
          'Arrow record batch declares more than $limit values across its '
          'field nodes for a $bodyLength-byte body',
        );
      }
    }
  }

  /// A zstd/lz4 codec bounded to [limit] decompressed bytes (throws mid-decode
  /// before over-allocating); null keeps the codec's own default cap.
  static CompressionCodec _codec(int code, int? limit) {
    if (code == compressionZstd) {
      return limit == null ? ZstdCodec() : ZstdCodec(maxSize: limit);
    }
    return limit == null ? Lz4Codec() : Lz4Codec(maxSize: limit);
  }

  /// Decompress each body buffer per the Arrow body-compression spec, returning
  /// a fresh contiguous (8-byte aligned) body plus rebased buffer infos.
  static (Uint8List, List<BufferInfo>) _decompress(
    Uint8List body,
    List<BufferInfo> buffers,
    int code, [
    int? budget,
  ]) {
    final out = BytesBuilder();
    final rebased = <BufferInfo>[];
    for (final buffer in buffers) {
      final offset = out.length;
      Uint8List decompressed;
      if (buffer.length == 0) {
        decompressed = Uint8List(0);
      } else {
        // Bound the 8-byte length prefix read: buffer.length may be < 8, so the
        // in-body buffer check isn't sufficient: verify the view fits.
        _checkRange(buffer.offset, 8, body.length, 'compression prefix');
        final prefix = ByteData.view(
          body.buffer,
          body.offsetInBytes + buffer.offset,
          8,
        ).getInt64(0, Endian.little);
        final payload = Uint8List.sublistView(
          body,
          buffer.offset + 8,
          buffer.offset + buffer.length,
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
          final remaining = budget == null ? null : budget - out.length;
          final cap = remaining == null || remaining >= prefix
              ? prefix
              : remaining;
          decompressed = _codec(code, cap).decompress(payload);
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
      if (budget != null && out.length + decompressed.length > budget) {
        throw FormatException(
          'Arrow batch decompresses to more than $budget bytes '
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

  static Column _empty(Field field) => ListColumn(
    name: field.name,
    dataType: field.dataType,
    data: const [],
    nullable: field.nullable,
  );

  /// Concatenate a column's pieces across record batches.
  static Column _concat(Field field, List<Column> parts) {
    // Typed bulk concat (NumericColumn → setRange, no boxing; non-numeric keeps a
    // boxed copy) instead of materializing every part into a boxed list. The
    // parts are slices of one logical column, so they share [field]'s name + dtype.
    return Column.concat(parts);
  }
}
