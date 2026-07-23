import 'dart:io';

import '../../core/column.dart';
import '../../core/data_type.dart';
import '../../core/paths.dart';
import '../../core/schema.dart';
import '../../core/table.dart';
import 'thrift/parquet_thrift.dart' as thrift;
import 'compression.dart';
import 'writer/file_writer.dart';
import 'writer/metadata_writer.dart';
import 'writer/row_group_writer.dart';
import 'schema/schema_converter.dart';
import '../../core/ids.dart' as ids;

/// Parquet file writer.
class ParquetWriter {
  final String filePath;
  final ParquetWriterOptions options;

  late FileWriter _fileWriter;
  final List<thrift.RowGroup> _rowGroups = [];
  int _totalRows = 0;

  // Streaming state.
  List<thrift.SchemaElement>? _schema;
  Schema? _coreSchema;
  List<String> _colNames = const [];
  // Row-group accrual holds COLUMN REFS (zero-copy), not boxed values — typed
  // columns are only materialized one row group at a time, at flush.
  Map<String, List<Column>>? _buffer;
  int _bufferRows = 0;
  // Rows already consumed from the FIRST accrued piece of each column; lets a
  // flush emit a prefix without re-slicing (re-copying) the tail every time.
  int _head = 0;
  bool _open = false;
  // True once the underlying file handle is acquired, independent of the
  // higher-level _open flag — so abort() always closes the fd + temp file even
  // if open() throws (e.g. writeMagic) before _open is set.
  bool _fileOpen = false;

  ParquetWriter(this.filePath, {ParquetWriterOptions? options})
    : options = options ?? const ParquetWriterOptions();

  /// Sibling temp path written to during the build; renamed onto [filePath] only
  /// once the footer is flushed, so [filePath] is never a half-written file. A
  /// per-writer random token makes it unique, so two writers targeting the same
  /// destination don't share (and corrupt/delete) one temp. Reassigned each
  /// [open] (a writer instance may be reused for multiple files).
  late String _temp;

  /// Write a whole table. Peak memory is bounded to one row group: columns are
  /// sliced per row group (no whole-table `toList`). A mid-write failure aborts
  /// cleanly (handle closed, partial file removed) rather than leaving a corrupt
  /// file at [filePath].
  Future<void> write(Table table) async {
    try {
      await open(table.schema, columns: table.columns);
      final fast = _fastNames(table.schema);
      final n = table.rowCount;
      for (int s = 0; s < n; s += options.rowGroupSize) {
        final e = (s + options.rowGroupSize).clamp(0, n);
        final data = <String, List<dynamic>>{};
        final typed = <String, NumericColumn>{};
        for (final column in table.columns) {
          if (column is NumericColumn && fast.contains(column.name)) {
            typed[column.name] = column.slice(s, e) as NumericColumn;
          } else {
            data[column.name] = _sliceColumn(column, s, e);
          }
        }
        await _writeRowGroup(data, e - s, typed: typed);
      }
      await close();
    } catch (_) {
      await abort();
      rethrow;
    }
  }

  /// Begin a streaming write. [columns] (when writing a whole table) lets leaves
  /// be marked required from real null content; streaming opens mark every leaf
  /// optional so any later batch may carry nulls.
  Future<void> open(Schema schema, {List<Column>? columns}) async {
    // Reject re-entry: a second open() while a write is live would overwrite the
    // temp path + file handle, leaking the first fd and orphaning its .tmp.
    if (_open || _fileOpen) {
      throw StateError(
        'ParquetWriter.open called while a write is already open; close() or '
        'abort() the current write first',
      );
    }
    if (!writable(options.compression)) {
      throw ArgumentError(
        'Compression codec ${options.compression.name} is not supported for writing',
      );
    }
    // Enforce in production too (the const-constructor assert is dev-only): a
    // non-positive row-group size stalls the write/flush loops forever.
    if (options.rowGroupSize <= 0) {
      throw ArgumentError.value(
        options.rowGroupSize,
        'rowGroupSize',
        'must be positive',
      );
    }
    // Likewise reject a version this library can't read back (the reader accepts
    // only {1,2}) — fail at write time rather than emit a self-unreadable file.
    if (!ParquetWriterOptions.supportedVersions.contains(options.version)) {
      throw ArgumentError.value(
        options.version,
        'version',
        'unsupported Parquet version (must be 1 or 2)',
      );
    }
    _rowGroups.clear();
    _totalRows = 0;
    _bufferRows = 0;
    _head = 0;

    // Convert the schema BEFORE acquiring the file handle, so a conversion
    // failure (e.g. an unsupported type) throws without ever holding an open
    // writer + .tmp file.
    final parquetSchema = SchemaConverter().toParquetSchema(schema);
    // Refine leaf nullability from real null content — only for a flat schema,
    // where leaves map 1:1 to fields. With nested fields the leaf↔field mapping
    // no longer holds, so keep the repetition types the schema emitter assigned.
    final nested = schema.fields.any(
      (f) =>
          f.dataType == DataType.list ||
          f.dataType == DataType.struct ||
          f.dataType == DataType.map,
    );
    if (!nested) {
      final leaves = parquetSchema.where((e) => e.type != null).toList();
      for (int i = 0; i < leaves.length; i++) {
        final required = columns != null && i < columns.length
            ? !columns[i].hasNulls
            : !(i < schema.fields.length && schema.fields[i].nullable);
        leaves[i].repetitionType = required
            ? thrift.FieldRepetitionType.required
            : thrift.FieldRepetitionType.optional;
      }
    }

    _temp = '$filePath.${ids.token()}.tmp';
    _fileWriter = FileWriter(_temp);
    try {
      await _fileWriter.open();
      _fileOpen =
          true; // fd held — abort() must close it even if a later step throws
      await _fileWriter.writeMagic();

      _schema = parquetSchema;
      _coreSchema = schema;
      _colNames = schema.fields.map((f) => f.name).toList();
      _buffer = {for (final n in _colNames) n: <Column>[]};
      _open = true;
    } catch (_) {
      // A failure once the fd is acquired (writeMagic, etc.) must self-clean for
      // a caller using the streaming API who invoked open() directly —
      // write()/writeBatches() wrap their own bodies, but a manual open() has no
      // outer guard. abort() is idempotent and gated on _fileOpen.
      await abort();
      rethrow;
    }
  }

  /// Append a batch to a streaming write, flushing full row groups as they fill.
  Future<void> writeBatch(Table table) async {
    if (!_open) await open(table.schema);
    // Reject schema drift BEFORE mutating the buffer. Encoding is positional
    // against the FROZEN physical schema, so the type must match EXACTLY —
    // `isCompatibleWith` permits int32→int64 widening, which would encode int64
    // values under an int32 column and SILENTLY TRUNCATE them (1<<40 → 0).
    if (!_coreSchema!.encodesAs(table.schema)) {
      throw ArgumentError(
        'Batch schema does not match the Parquet writer schema; every batch '
        'must have the same columns in the same order and EXACT type (a wider '
        'batch is not narrowed — declare the wider type when opening the writer)',
      );
    }
    for (final column in table.columns) {
      // Accrue the column REF (zero-copy); values are materialized only at flush.
      _buffer![column.name]!.add(column);
    }
    _bufferRows += table.rowCount;
    while (_bufferRows >= options.rowGroupSize) {
      await _flushBuffer(options.rowGroupSize);
    }
  }

  /// Drain a stream of batches into one Parquet file and finalize it. A failure
  /// aborts cleanly rather than leaving a corrupt file at [filePath].
  Future<void> writeBatches(Stream<Table> batches) async {
    try {
      await for (final batch in batches) {
        await writeBatch(batch);
      }
      await close();
    } catch (_) {
      await abort();
      rethrow;
    }
  }

  /// Finish a streaming write: flush the remainder, write the footer, close, then
  /// atomically rename the temp file onto [filePath].
  Future<void> close() async {
    if (!_open) return;
    try {
      if (_bufferRows > 0) await _flushBuffer(_bufferRows);
      final metadata = _createMetadata(_schema!);
      await MetadataWriter(_fileWriter).writeMetadata(metadata);
      await _fileWriter.close();
      // The clears run only AFTER the rename succeeds: if replaceAtomic throws,
      // _fileOpen stays true so the catch below (abort) still deletes the temp.
      await replaceAtomic(File(_temp), filePath);
      _fileOpen = false;
      _open = false;
      _buffer = null;
    } catch (_) {
      // SELF-CLEAN on any finalize failure (footer / fd close / rename): a caller
      // that doesn't wrap close() in its own abort() still can't leak the temp or
      // fd. abort() is idempotent (gated on _fileOpen, still true here).
      await abort();
      rethrow;
    }
  }

  /// Abandon an in-progress write: close the handle (best effort) and delete the
  /// temp file so no partial output survives at [filePath]. Gated on the file
  /// handle (not [_open]) so a failure during open() — before _open is set — is
  /// still cleaned up.
  Future<void> abort() async {
    if (!_fileOpen) return;
    _open = false;
    _fileOpen = false;
    _buffer = null;
    try {
      await _fileWriter.close();
    } catch (_) {
      /* already closing/closed */
    }
    final temp = File(_temp);
    if (await temp.exists()) await temp.delete();
  }

  Future<void> _flushBuffer(int n) async {
    // Consume the first n accrued rows by slicing only the head pieces and
    // advancing a shared cursor — the untouched tail is never re-copied (avoids
    // the O(n^2) re-materialization of a big multi-row-group batch). All columns
    // share piece boundaries (batches are appended atomically), so one cursor
    // drives every name.
    final dt = {for (final f in _coreSchema!.fields) f.name: f.dataType};
    bool fast(String name) {
      final pieces = _buffer![name]!;
      return pieces.isNotEmpty &&
          pieces.first is NumericColumn &&
          _isPlainNumeric(dt[name]!);
    }

    // Fast (numeric) columns accrue their typed slices for one zero-boxing
    // concat; the rest are boxed exactly as before.
    final slices = <String, List<Column>>{
      for (final name in _colNames)
        if (fast(name)) name: <Column>[],
    };
    final data = <String, List<dynamic>>{
      for (final name in _colNames)
        if (!fast(name)) name: <dynamic>[],
    };
    var need = n;
    final lead = _colNames.first;
    while (need > 0) {
      final piece = _buffer![lead]!.first;
      final take = (piece.length - _head).clamp(0, need);
      final end = _head + take;
      if (take > 0) {
        for (final name in _colNames) {
          final slice = _buffer![name]!.first.slice(_head, end);
          if (slices.containsKey(name)) {
            slices[name]!.add(slice);
          } else {
            data[name]!.addAll(slice.toList());
          }
        }
        need -= take;
      }
      if (end >= piece.length) {
        // Head piece fully consumed: drop it from every column, reset the cursor.
        for (final name in _colNames) {
          _buffer![name]!.removeAt(0);
        }
        _head = 0;
      } else {
        _head = end;
      }
    }
    final typed = <String, NumericColumn>{
      for (final e in slices.entries)
        e.key: Column.concat(e.value) as NumericColumn,
    };
    await _writeRowGroup(data, n, typed: typed);
    _bufferRows -= n;
  }

  Future<void> _writeRowGroup(
    Map<String, List<dynamic>> data,
    int numRows, {
    Map<String, NumericColumn> typed = const {},
  }) async {
    final rowGroup = await RowGroupWriter(
      _fileWriter,
      _schema!,
      options,
      coreFields: _coreSchema?.fields,
    ).write(_physicalize(data), numRows, typed: typed);
    _rowGroups.add(rowGroup);
    _totalRows += numRows;
  }

  /// Names of flat numeric fields eligible for the typed (no-boxing) write path.
  Set<String> _fastNames(Schema schema) => {
    for (final f in schema.fields)
      if (_isPlainNumeric(f.dataType)) f.name,
  };

  /// Whether [type] maps directly to a Parquet physical int/float with no
  /// logical conversion (so the raw buffer can be encoded as-is). Date, timestamp
  /// and decimal need [_physicalize] and stay on the boxed path.
  static bool _isPlainNumeric(DataType type) =>
      type == DataType.int8 ||
      type == DataType.int16 ||
      type == DataType.int32 ||
      type == DataType.int64 ||
      type == DataType.float32 ||
      type == DataType.float64;

  /// Convert logical column values (DateTime, BigInt) to their Parquet physical
  /// representation (epoch ints, unscaled int/bytes) before encoding. Columns of
  /// other types are passed through untouched.
  Map<String, List<dynamic>> _physicalize(Map<String, List<dynamic>> data) {
    final schema = _coreSchema;
    if (schema == null) return data;
    final out = <String, List<dynamic>>{};
    for (final field in schema.fields) {
      final values = data[field.name];
      if (values == null) continue;
      final type = field.dataType;
      if (type == DataType.date ||
          type == DataType.timestamp ||
          type == DataType.decimal) {
        final precision = field.precision ?? 38;
        out[field.name] = [
          for (final v in values) toPhysicalValue(type, precision, v),
        ];
      } else {
        out[field.name] = values;
      }
    }
    return out;
  }

  /// Box one row group's worth of a column's values ([start, end)) — caps peak
  /// memory at a row group instead of the whole column.
  List<dynamic> _sliceColumn(Column column, int start, int end) => [
    for (int i = start; i < end; i++) column[i],
  ];

  /// Create file metadata
  thrift.FileMetaData _createMetadata(List<thrift.SchemaElement> schema) {
    final metadata = thrift.FileMetaData();
    metadata.version = options.version;
    metadata.schema = schema;
    metadata.numRows = _totalRows;
    metadata.rowGroups = _rowGroups;

    // Add key-value metadata
    metadata.keyValueMetadata = [
      thrift.KeyValue()
        ..key = 'writer'
        ..value = 'libbigdata',
      thrift.KeyValue()
        ..key = 'writer.version'
        ..value = '1.0.0',
    ];

    if (options.createdBy != null) {
      metadata.createdBy = options.createdBy;
    } else {
      metadata.createdBy = 'libbigdata-parquet-writer';
    }

    return metadata;
  }
}

// Secure entropy so concurrent writers (isolates/pods) can't collide on temp
// paths via identical clock seeds.

/// Options for writing Parquet files
class ParquetWriterOptions {
  /// Parquet file version (1 or 2)
  final int version;

  /// Row group size (number of rows per row group)
  final int rowGroupSize;

  /// Compression codec to use
  final thrift.CompressionCodec compression;

  /// Whether to write statistics
  final bool writeStatistics;

  /// Whether to write page checksums (CRC32 over each compressed page). On by
  /// default so emitted files are verifiable; set `false` to skip the small
  /// write-time cost.
  final bool writeChecksums;

  /// Created by string
  final String? createdBy;

  /// Parquet metadata versions this library can both write AND read back. The
  /// reader (metadata_reader.validate) rejects anything else, so the writer must
  /// not emit a version it can't read.
  static const supportedVersions = {1, 2};

  const ParquetWriterOptions({
    this.version = 1,
    this.rowGroupSize = 100000,
    this.compression = thrift.CompressionCodec.snappy,
    this.writeStatistics = true,
    this.writeChecksums = true,
    this.createdBy,
  }) : assert(rowGroupSize > 0, 'rowGroupSize must be positive'),
       assert(version == 1 || version == 2, 'Parquet version must be 1 or 2');
}
