import 'dart:async';

import '../core/limits.dart';
import '../core/row_filter.dart';
import '../core/scan.dart';
import '../core/schema.dart';
import '../core/table.dart';
import 'arrow/arrow_reader.dart';
import 'csv/csv_reader.dart';
import 'json/json_reader.dart';
import 'parquet/parquet_reader.dart';
import 'parquet/thrift/parquet_thrift.dart' as thrift;

/// Default scan batch size when a caller doesn't specify one.
const int defaultScanBatch = 10000;

/// Open a format-neutral [Scan] for [path], dispatching by extension. This is the
/// single place the library's file-format set is known: every entry point (SQL
/// eager + lazy scans, dataset streaming + eager reads) routes format selection
/// through here, so the supported extensions and the unknown-extension behaviour
/// are identical everywhere. An unknown extension is a hard [ArgumentError],
/// never a silent fall-through to Parquet.
Scan openScan(String path) {
  final ext = path.split('.').last.toLowerCase();
  switch (ext) {
    case 'parquet':
      return _ParquetScan(path);
    case 'csv':
      return _CsvScan(path);
    case 'jsonl':
    case 'ndjson':
      return _JsonScan(path, JsonOptions.jsonLines());
    case 'json':
      return _JsonScan(path, const JsonOptions());
    case 'arrow':
      return _ArrowScan(path);
    default:
      throw ArgumentError.value(
        path,
        'path',
        'Unsupported file extension: .$ext',
      );
  }
}

/// Schema of the first streamed batch, or null if the stream is empty.
Future<Schema?> _peekSchema(Stream<Table> stream) async {
  final it = StreamIterator(stream);
  try {
    if (await it.moveNext()) return it.current.schema;
    return null;
  } finally {
    await it.cancel();
  }
}

class _ParquetScan implements Scan {
  final String path;

  /// The footer parsed by an earlier call on THIS scan. A caller that asks for
  /// the schema and then reads (the dataset planner does exactly this) would
  /// otherwise pay the footer preads + thrift parse twice per file — the bulk of
  /// the per-file metadata cost on network storage. Scoped to one scan object,
  /// so it lives no longer than a single caller's use of one file, and Parquet
  /// files are immutable once published.
  thrift.FileMetaData? _footer;

  _ParquetScan(this.path);

  @override
  Future<Schema?> readSchema() async {
    final reader = ParquetReader(path, footer: _footer);
    final schema = await reader.schema();
    _footer ??= reader.metadata;
    return schema;
  }

  @override
  Stream<Table> batches({
    int size = defaultScanBatch,
    List<String>? projection,
    RowFilter? filter,
  }) => ParquetReader(
    path,
    options: ParquetReaderOptions(filter: filter, columns: projection),
    footer: _footer,
  ).readBatches(size: size);

  @override
  Future<Table> read({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    List<String>? projection,
    RowFilter? filter,
  }) => ParquetReader(
    path,
    options: ParquetReaderOptions(filter: filter, columns: projection),
    footer: _footer,
  ).read(maxRows: maxRows, maxBytes: maxBytes);
}

class _CsvScan implements Scan {
  final String path;
  _CsvScan(this.path);

  @override
  Future<Schema?> readSchema() =>
      _peekSchema(CsvReader().readBatches(path, size: 512));

  @override
  Stream<Table> batches({
    int size = defaultScanBatch,
    List<String>? projection,
    RowFilter? filter, // CSV can't prune by stats; the plan filters exactly.
  }) => CsvReader().readBatches(path, size: size, selectColumns: projection);

  @override
  Future<Table> read({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    List<String>? projection,
    RowFilter? filter,
  }) async => CsvReader().readFile(
    path,
    selectColumns: projection,
    maxRows: maxRows,
    maxBytes: maxBytes,
  );
}

class _JsonScan implements Scan {
  final String path;
  final JsonOptions options;
  _JsonScan(this.path, this.options);

  @override
  Future<Schema?> readSchema() =>
      _peekSchema(JsonReader(options).readBatches(path, size: 512));

  @override
  Stream<Table> batches({
    int size = defaultScanBatch,
    List<String>? projection,
    RowFilter? filter,
  }) => JsonReader(options).readBatches(path, size: size, columns: projection);

  @override
  Future<Table> read({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    List<String>? projection,
    RowFilter? filter,
  }) async {
    final t = await JsonReader(
      options,
    ).readFile(path, maxRows: maxRows, maxBytes: maxBytes);
    // Honor the projection contract — never silently drop it (a missing column
    // then errors loudly, matching Parquet/CSV). The streaming batches() path
    // already pushes it into decode.
    return projection == null ? t : t.select(projection);
  }
}

class _ArrowScan implements Scan {
  final String path;
  _ArrowScan(this.path);

  @override
  // Footer-only: the Arrow footer already holds the schema, so don't decode a
  // whole record-batch body just to learn the columns.
  Future<Schema?> readSchema() => ArrowReader(path).readSchema();

  @override
  Stream<Table> batches({
    int size = defaultScanBatch,
    List<String>? projection,
    RowFilter? filter,
  }) => ArrowReader(path).readBatches(batchSize: size, columns: projection);

  @override
  Future<Table> read({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    List<String>? projection,
    RowFilter? filter,
  }) async {
    final t = await ArrowReader(
      path,
    ).read(maxRows: maxRows, maxBytes: maxBytes);
    // Honor the projection contract (batches() already pushes columns into the
    // reader; the eager read() at least stops RETAINING unselected columns).
    return projection == null ? t : t.select(projection);
  }
}
