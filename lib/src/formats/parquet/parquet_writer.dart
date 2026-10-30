import 'dart:io';

import 'package:meta/meta.dart';

import '../../core/column.dart';
import '../../core/failure.dart';
import '../../core/result.dart';
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
final class ParquetWriter {
  final String filePath;
  final ParquetWriterOptions options;

  late FileWriter _writer;
  final List<thrift.RowGroup> _rowGroups = [];
  int _rows = 0;

  // Streaming state.
  List<thrift.SchemaElement>? _schema;
  Schema? _coreSchema;
  List<String> _names = const [];
  // Row-group accrual holds COLUMN REFS (zero-copy), not boxed values: typed
  // columns are only materialized one row group at a time, at flush.
  Map<String, List<Column>>? _buffer;
  int _buffered = 0;
  // Rows already consumed from the FIRST accrued piece of each column; lets a
  // flush emit a prefix without re-slicing (re-copying) the tail every time.
  int _head = 0;
  bool _open = false;
  // True once the underlying file handle is acquired, independent of the
  // higher-level _open flag, so abort() always closes the fd + temp file even
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
  ///
  /// A failure is reported as an [Err], with no partial file left behind:
  /// [Kind.io] when the file cannot be written, [Kind.invalid] for a value its
  /// column's physical type would truncate, and [Kind.unsupported] for a
  /// format version this writer cannot produce.
  @useResult
  Future<Result<void, ErrorDetails>> write(Table table) {
    _require(opening: true);
    return capture(() => _write(table));
  }

  /// The caller's preconditions, checked before any work so they throw rather
  /// than being reported: a positive row-group size and, when [opening], no
  /// write already open.
  void _require({bool opening = false}) {
    // Reject re-entry: a second open() while a write is live would overwrite the
    // temp path + file handle, leaking the first fd and orphaning its .tmp.
    if (opening && (_open || _fileOpen)) {
      throw StateError(
        'ParquetWriter.open called while a write is already open; close() or '
        'abort() the current write first',
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
  }

  Future<void> _write(Table table) async {
    try {
      await _begin(table.schema, columns: table.columns);
      final fast = _fastNames(table.schema);
      final count = table.rowCount;
      for (int start = 0; start < count; start += options.rowGroupSize) {
        final end = (start + options.rowGroupSize).clamp(0, count);
        final data = <String, List<dynamic>>{};
        final typed = <String, NumericVector>{};
        for (final column in table.columns) {
          if (column is NumericVector && fast.contains(column.name)) {
            typed[column.name] = column.slice(start, end) as NumericVector;
          } else {
            data[column.name] = _sliceColumn(column, start, end);
          }
        }
        await _writeRowGroup(data, end - start, typed: typed);
      }
      await _close();
    } catch (_) {
      await abort();
      rethrow;
    }
  }

  /// Begin a streaming write. [columns] (when writing a whole table) lets leaves
  /// be marked required from real null content; streaming opens mark every leaf
  /// optional so any later batch may carry nulls. Failures are reported as
  /// [write] reports them.
  @useResult
  Future<Result<void, ErrorDetails>> open(
    Schema schema, {
    List<Column>? columns,
  }) {
    _require(opening: true);
    return capture(() => _begin(schema, columns: columns));
  }

  Future<void> _begin(Schema schema, {List<Column>? columns}) async {
    // Reject a version this library can't read back (the reader accepts
    // only {1,2}): fail at write time rather than emit a self-unreadable file.
    if (!ParquetWriterOptions.supportedVersions.contains(options.version)) {
      throw Failure.of(
        Kind.unsupported,
        'Unsupported Parquet version ${options.version} (must be 1 or 2)',
      );
    }
    _rowGroups.clear();
    _rows = 0;
    _buffered = 0;
    _head = 0;

    // Convert the schema BEFORE acquiring the file handle, so a conversion
    // failure (e.g. an unsupported type) throws without ever holding an open
    // writer + .tmp file.
    final parquetSchema = SchemaConverter().toParquetSchema(schema);
    // Leaf nullability from the field, widened by real null content, only
    // for a flat schema, where leaves map 1:1 to fields. With nested fields
    // the leaf↔field mapping no longer holds, so keep the repetition types the
    // schema emitter assigned. A nullable field stays OPTIONAL with no null
    // in it: narrowing it to REQUIRED changed the schema on a round trip.
    final nested = schema.fields.any(
      (field) =>
          field.dataType == DataType.list ||
          field.dataType == DataType.struct ||
          field.dataType == DataType.map,
    );
    if (!nested) {
      final leaves = parquetSchema
          .where((element) => element.type != null)
          .toList();
      for (int i = 0; i < leaves.length; i++) {
        final nullable =
            (i < schema.fields.length && schema.fields[i].nullable) ||
            (columns != null && i < columns.length && columns[i].hasNulls);
        leaves[i].repetitionType = nullable
            ? thrift.FieldRepetitionType.optional
            : thrift.FieldRepetitionType.required;
      }
    }

    _temp = '$filePath.${ids.token()}.tmp';
    _writer = FileWriter(_temp);
    try {
      await _writer.open();
      _fileOpen =
          true; // fd held: abort() must close it even if a later step throws
      await _writer.writeMagic();

      _schema = parquetSchema;
      _coreSchema = schema;
      _names = schema.fields.map((field) => field.name).toList();
      _buffer = {for (final name in _names) name: <Column>[]};
      _open = true;
    } catch (_) {
      // A failure once the fd is acquired (writeMagic, etc.) must self-clean for
      // a caller using the streaming API who invoked open() directly:
      // write()/writeBatches() wrap their own bodies, but a manual open() has no
      // outer guard. abort() is idempotent and conditional on _fileOpen.
      await abort();
      rethrow;
    }
  }

  /// Append a batch to a streaming write, flushing full row groups as they fill.
  /// A failure ([Kind.schema] for a batch whose schema differs from the
  /// writer's, otherwise as [write]) abandons the write, so no partial file is
  /// left.
  @useResult
  Future<Result<void, ErrorDetails>> writeBatch(Table table) {
    _require();
    return capture(() async {
      try {
        await _writeBatch(table);
      } catch (_) {
        await abort();
        rethrow;
      }
    });
  }

  Future<void> _writeBatch(Table table) async {
    if (!_open) await _begin(table.schema);
    // Reject schema drift BEFORE mutating the buffer. Encoding is positional
    // against the FROZEN physical schema, so the type must match EXACTLY:
    // `isCompatibleWith` permits int32→int64 widening, which would encode int64
    // values under an int32 column and SILENTLY TRUNCATE them (1<<40 → 0).
    if (!_coreSchema!.encodesAs(table.schema)) {
      throw Failure.of(
        Kind.schema,
        'Batch schema does not match the Parquet writer schema; every batch '
        'must have the same columns in the same order and EXACT type (a wider '
        'batch is not narrowed: declare the wider type when opening the writer)',
      );
    }
    for (final column in table.columns) {
      // Accrue the column REF (zero-copy); values are materialized only at flush.
      _buffer![column.name]!.add(column);
    }
    _buffered += table.rowCount;
    while (_buffered >= options.rowGroupSize) {
      await _flushBuffer(options.rowGroupSize);
    }
  }

  /// Drain a stream of batches into one Parquet file and finalize it. A failure
  /// aborts cleanly rather than leaving a corrupt file at [filePath], and is
  /// reported as [writeBatch] reports it.
  @useResult
  Future<Result<void, ErrorDetails>> writeBatches(Stream<Table> batches) {
    _require();
    return capture(() => _writeBatches(batches));
  }

  Future<void> _writeBatches(Stream<Table> batches) async {
    try {
      await for (final batch in batches) {
        await _writeBatch(batch);
      }
      await _close();
    } catch (_) {
      await abort();
      rethrow;
    }
  }

  /// Finish a streaming write: flush the remainder, write the footer, close, then
  /// atomically rename the temp file onto [filePath]. A failure leaves no
  /// partial file and is reported as [write] reports it.
  @useResult
  Future<Result<void, ErrorDetails>> close() => capture(_close);

  Future<void> _close() async {
    if (!_open) return;
    try {
      if (_buffered > 0) await _flushBuffer(_buffered);
      final metadata = _createMetadata(_schema!);
      await MetadataWriter(_writer).writeMetadata(metadata);
      await _writer.close();
      // The clears run only AFTER the rename succeeds: if replaceAtomic throws,
      // _fileOpen stays true so the catch below (abort) still deletes the temp.
      await replaceAtomic(File(_temp), filePath);
      _fileOpen = false;
      _open = false;
      _buffer = null;
    } catch (_) {
      // SELF-CLEAN on any finalize failure (footer / fd close / rename): a caller
      // that doesn't wrap close() in its own abort() still can't leak the temp or
      // fd. abort() is idempotent (conditional on _fileOpen, still true here).
      await abort();
      rethrow;
    }
  }

  /// Abandon an in-progress write: close the handle (best effort) and delete the
  /// temp file so no partial output survives at [filePath]. Conditional on the file
  /// handle (not [_open]) so a failure during open() (before _open is set) is
  /// still cleaned up.
  Future<void> abort() async {
    if (!_fileOpen) return;
    _open = false;
    _fileOpen = false;
    _buffer = null;
    try {
      await _writer.close();
    } catch (_) {
      /* already closing/closed */
    }
    final temp = File(_temp);
    if (await temp.exists()) await temp.delete();
  }

  Future<void> _flushBuffer(int count) async {
    // Consume the first [count] accrued rows by slicing only the head pieces and
    // advancing a shared cursor: the untouched tail is never re-copied (avoids
    // the O(n^2) re-materialization of a big multi-row-group batch). All columns
    // share piece boundaries (batches are appended atomically), so one cursor
    // drives every name.
    final types = {
      for (final field in _coreSchema!.fields) field.name: field.dataType,
    };
    bool fast(String name) {
      final pieces = _buffer![name]!;
      return pieces.isNotEmpty &&
          pieces.first is NumericVector &&
          _isPlainNumeric(types[name]!);
    }

    // Fast (numeric) columns accrue their typed slices for one zero-boxing
    // concat; the rest are boxed exactly as before.
    final slices = <String, List<Column>>{
      for (final name in _names)
        if (fast(name)) name: <Column>[],
    };
    final data = <String, List<dynamic>>{
      for (final name in _names)
        if (!fast(name)) name: <dynamic>[],
    };
    var need = count;
    final lead = _names.first;
    while (need > 0) {
      final piece = _buffer![lead]!.first;
      final take = (piece.length - _head).clamp(0, need);
      final end = _head + take;
      if (take > 0) {
        for (final name in _names) {
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
        for (final name in _names) {
          _buffer![name]!.removeAt(0);
        }
        _head = 0;
      } else {
        _head = end;
      }
    }
    final typed = <String, NumericVector>{
      for (final entry in slices.entries)
        entry.key: Column.concat(entry.value) as NumericVector,
    };
    await _writeRowGroup(data, count, typed: typed);
    _buffered -= count;
  }

  Future<void> _writeRowGroup(
    Map<String, List<dynamic>> data,
    int rows, {
    Map<String, NumericVector> typed = const {},
  }) async {
    final rowGroup = await RowGroupWriter(
      _writer,
      _schema!,
      options,
      coreFields: _coreSchema?.fields,
    ).write(_physicalize(data), rows, typed: typed);
    _rowGroups.add(rowGroup);
    _rows += rows;
  }

  /// Names of flat numeric fields eligible for the typed (no-boxing) write path.
  Set<String> _fastNames(Schema schema) => {
    for (final field in schema.fields)
      if (_isPlainNumeric(field.dataType)) field.name,
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
          for (final value in values) toPhysicalValue(type, precision, value),
        ];
      } else {
        out[field.name] = values;
      }
    }
    return out;
  }

  /// Box one row group's worth of a column's values ([start, end)): caps peak
  /// memory at a row group instead of the whole column.
  List<dynamic> _sliceColumn(Column column, int start, int end) => [
    for (int i = start; i < end; i++) column[i],
  ];

  /// Create file metadata
  thrift.FileMetaData _createMetadata(List<thrift.SchemaElement> schema) {
    final metadata = thrift.FileMetaData();
    metadata.version = options.version;
    metadata.schema = schema;
    metadata.numRows = _rows;
    metadata.rowGroups = _rowGroups;
    // Every leaf's min_value and max_value follow its type's defined order:
    // signed integers, floats without NaN, unsigned bytes for byte arrays.
    metadata.columnOrders = [
      for (final element in schema.skip(1))
        if ((element.numChildren ?? 0) == 0)
          thrift.ColumnOrder()..typeOrder = thrift.TypeDefinedOrder(),
    ];

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

/// Options for writing Parquet files
final class ParquetWriterOptions {
  /// Parquet file version (1 or 2)
  final int version;

  /// Row group size (number of rows per row group)
  final int rowGroupSize;

  /// Compression codec to use
  final ParquetCompression compression;

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
    this.compression = ParquetCompression.snappy,
    this.writeStatistics = true,
    this.writeChecksums = true,
    this.createdBy,
  }) : assert(rowGroupSize > 0, 'rowGroupSize must be positive'),
       assert(version == 1 || version == 2, 'Parquet version must be 1 or 2');
}
