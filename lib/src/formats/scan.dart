import 'dart:async';
import 'dart:io';

import '../core/limits.dart';
import '../core/row_filter.dart';
import '../core/scan.dart';
import '../core/schema.dart';
import '../core/table.dart';
import 'arrow/arrow_reader.dart';
import 'csv/csv_options.dart';
import 'csv/csv_reader.dart';
import 'dataset/dataset.dart';
import 'delta/delta_table.dart';
import 'iceberg/iceberg_catalog.dart';
import 'iceberg/iceberg_metadata.dart' show metadataFile;
import 'iceberg/iceberg_table.dart';
import 'json/json_reader.dart';
import 'parquet/parquet_reader.dart';
import 'parquet/thrift/parquet_thrift.dart' as thrift;
import '../core/failure.dart';
import '../core/result.dart';

/// Default scan batch size when a caller doesn't specify one.
const int defaultScanBatch = 10000;

/// Open a format-neutral [Scan] for [path]: a Delta table for a directory with
/// a `_delta_log`, an Iceberg table for one with `metadata/*.metadata.json`,
/// otherwise dispatching by extension. This is the
/// single place the library's file-format set is known: every entry point (SQL
/// eager + lazy scans, dataset streaming + eager reads) routes format selection
/// through here, so the supported extensions and the unknown-extension behaviour
/// are identical everywhere. An unknown extension is a hard [ArgumentError],
/// never a silent fall-through to Parquet.
Scan openScan(String path) {
  // A Delta table is a directory holding a transaction log; an Iceberg table
  // one whose metadata directory holds a `.metadata.json`.
  if (_delta(path)) return _DeltaScan(path);
  if (_iceberg(path)) return _IcebergScan(path);
  final extension = path.split('.').last.toLowerCase();
  switch (extension) {
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
        'Unsupported file extension: .$extension',
      );
  }
}

/// A [Scan] of the Parquet file [path] that prunes row groups by [filter] as
/// well as by the filter a plan pushes, and reads only [columns]: its
/// [Scan.readSchema] is the schema of [columns] (the file's, when null), and
/// a pushed projection is a subset of it. [filter] is a pruning hint, as for
/// [ParquetReaderOptions.filter]: a kept row group comes back whole.
Scan parquetScan(String path, {RowFilter? filter, List<String>? columns}) =>
    _ParquetScan(path, filter: filter, columns: columns);

/// A [Scan] of the CSV file [path] read with [options] (the defaults when
/// null), keeping exactly the rows [filter] matches, only [columns] (in that
/// order), and at most [limit] rows, in batches of [size] rows when given
/// (else the size the caller asks for). [Scan.readSchema] reports the
/// schema of [columns].
Scan csvScan(
  String path, {
  CsvOptions? options,
  RowFilter? filter,
  List<String>? columns,
  int? limit,
  int? size,
}) => _CsvScan(
  path,
  options: options,
  filter: filter,
  columns: columns,
  limit: limit,
  size: size,
);

/// A [Scan] of the JSON or JSON Lines file [path] read with [options].
Scan jsonScan(String path, JsonOptions options) => _JsonScan(path, options);

/// A [Scan] of the Arrow IPC file [path] that refuses a record batch larger
/// than [block] bytes before reading it (null: no ceiling).
Scan arrowScan(String path, {int? block = defaultMaxMaterializedBytes}) =>
    _ArrowScan(path, block: block);

/// A [Scan] of the Delta table [path] at [version] or [asOf] (the latest
/// when both are null, resolved when each scan starts).
Scan deltaScan(
  String path, {
  int? version,
  int? asOf,
  bool allowExternalPaths = false,
}) => _DeltaScan(
  path,
  version: version,
  asOf: asOf,
  allowExternalPaths: allowExternalPaths,
);

/// A [Scan] of the Iceberg table [path] at [snapshotId] or [asOf], else at
/// the current snapshot of [metadata] when given (the table's metadata as
/// read earlier), else at the current snapshot when each scan starts.
Scan icebergScan(
  String path, {
  int? snapshotId,
  int? asOf,
  Map<String, dynamic>? metadata,
  bool allowExternalPaths = false,
}) => _IcebergScan(
  path,
  snapshotId: snapshotId,
  asOf: asOf,
  metadata: metadata,
  allowExternalPaths: allowExternalPaths,
);

/// A [Scan] of the dataset [source] (a directory, glob or file) that prunes
/// files and row groups by [filter] as well as by the filter a plan pushes,
/// and reads only [columns]; [concurrency] overlaps the per-file schema
/// reads, as [Dataset.readBatches] does. [filter] is a pruning hint, as for
/// [Dataset.filter]. [Scan.readSchema] is the unified schema of [columns],
/// read from the first batch, or null when no file yields one.
Scan datasetScan(
  String source, {
  RowFilter? filter,
  List<String>? columns,
  int concurrency = 1,
}) => _DatasetScan(
  source,
  filter: filter,
  columns: columns,
  concurrency: concurrency,
);

/// [fixed] and [pushed] as one pruning filter, or whichever is set.
RowFilter? _both(RowFilter? fixed, RowFilter? pushed) => fixed == null
    ? pushed
    : pushed == null
    ? fixed
    : RowFilter.and([fixed, pushed]);

/// Whether [path] is a Delta or Iceberg table directory, which [openScan]
/// reads as one table.
bool isTable(String path) => _delta(path) || _iceberg(path);

/// Whether [path] is a Delta table directory.
bool _delta(String path) => Directory('$path/_delta_log').existsSync();

/// Whether [path] is an Iceberg table directory.
bool _iceberg(String path) {
  final metadata = Directory('$path/metadata');
  return metadata.existsSync() &&
      metadata.listSync().any(
        (entry) => metadataFile.hasMatch(entry.uri.pathSegments.last),
      );
}

/// Schema of the first streamed batch, or null if the stream is empty.
Future<Schema?> _peek(Stream<Table> stream) async {
  final iterator = StreamIterator(stream);
  try {
    if (await iterator.moveNext()) return iterator.current.schema;
    return null;
  } finally {
    await iterator.cancel();
  }
}

class _ParquetScan implements Scan {
  final String path;

  /// The reader's own pruning filter and columns ([parquetScan]).
  final RowFilter? filter;
  final List<String>? columns;

  /// The footer parsed by an earlier call on THIS scan. A caller that asks for
  /// the schema and then reads (the dataset planner does exactly this) would
  /// otherwise pay the footer preads + thrift parse twice per file: the bulk of
  /// the per-file metadata cost on network storage. Scoped to one scan object,
  /// so it lives no longer than a single caller's use of one file, and Parquet
  /// files are immutable once published.
  thrift.FileMetaData? _footer;

  _ParquetScan(this.path, {this.filter, this.columns});

  @override
  Future<Schema?> readSchema() async {
    final reader = ParquetReader(
      path,
      options: ParquetReaderOptions(columns: columns),
      footer: _footer,
    );
    final schema = await reader.schema().unwrap();
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
    options: ParquetReaderOptions(
      filter: _both(this.filter, filter),
      columns: projection ?? columns,
    ),
    footer: _footer,
  ).readBatches(batchSize: size).unwrap();

  @override
  Future<Table> read({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    List<String>? projection,
    RowFilter? filter,
  }) => ParquetReader(
    path,
    options: ParquetReaderOptions(
      filter: _both(this.filter, filter),
      columns: projection ?? columns,
    ),
    footer: _footer,
  ).read(maxRows: maxRows, maxBytes: maxBytes).unwrap();
}

class _CsvScan implements Scan {
  final String path;

  /// The reader's own options, exact filter, columns, row limit and batch
  /// size ([csvScan]).
  final CsvOptions? options;
  final RowFilter? filter;
  final List<String>? columns;
  final int? limit;
  final int? size;

  _CsvScan(
    this.path, {
    this.options,
    this.filter,
    this.columns,
    this.limit,
    this.size,
  });

  @override
  Future<Schema?> readSchema() => _peek(
    CsvReader(options: options)
        .readBatches(
          path,
          batchSize: 512,
          filter: filter,
          columns: columns,
          limit: limit,
        )
        .unwrap(),
  );

  @override
  Stream<Table> batches({
    int size = defaultScanBatch,
    List<String>? projection,
    RowFilter? filter, // CSV can't prune by stats; the plan filters exactly.
  }) => CsvReader(options: options)
      .readBatches(
        path,
        batchSize: this.size ?? size,
        filter: this.filter,
        columns: projection ?? columns,
        limit: limit,
      )
      .unwrap();

  @override
  Future<Table> read({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    List<String>? projection,
    RowFilter? filter,
  }) async => CsvReader(options: options)
      .readFile(
        path,
        filter: this.filter,
        columns: projection ?? columns,
        limit: limit,
        maxRows: maxRows,
        maxBytes: maxBytes,
      )
      .unwrap();
}

class _JsonScan implements Scan {
  final String path;
  final JsonOptions options;
  _JsonScan(this.path, this.options);

  @override
  Future<Schema?> readSchema() =>
      _peek(JsonReader(options).readBatches(path, batchSize: 512).unwrap());

  @override
  Stream<Table> batches({
    int size = defaultScanBatch,
    List<String>? projection,
    RowFilter? filter,
  }) => JsonReader(
    options,
  ).readBatches(path, batchSize: size, columns: projection).unwrap();

  @override
  Future<Table> read({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    List<String>? projection,
    RowFilter? filter,
  }) async {
    final table = await JsonReader(
      options,
    ).readFile(path, maxRows: maxRows, maxBytes: maxBytes).unwrap();
    // Honor the projection contract, never silently drop it (a missing column
    // then errors loudly, matching Parquet/CSV). The streaming batches() path
    // already pushes it into decode.
    return projection == null ? table : table.select(projection);
  }
}

class _ArrowScan implements Scan {
  final String path;

  /// The largest record batch read, in bytes (null: no ceiling).
  final int? block;

  _ArrowScan(this.path, {this.block = defaultMaxMaterializedBytes});

  @override
  // Footer-only: the Arrow footer already holds the schema, so don't decode a
  // whole record-batch body just to learn the columns.
  Future<Schema?> readSchema() => ArrowReader(path).readSchema();

  @override
  Stream<Table> batches({
    int size = defaultScanBatch,
    List<String>? projection,
    RowFilter? filter,
  }) => ArrowReader(
    path,
  ).readBatches(batchSize: size, columns: projection, maxBlockBytes: block);

  @override
  Future<Table> read({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    List<String>? projection,
    RowFilter? filter,
  }) async {
    final table = await ArrowReader(
      path,
    ).read(maxRows: maxRows, maxBytes: maxBytes);
    // Honor the projection contract (batches() already pushes columns into the
    // reader; the eager read() at least stops RETAINING unselected columns).
    return projection == null ? table : table.select(projection);
  }
}

class _DeltaScan implements Scan {
  final String path;

  /// The version read, by number or timestamp, and the path policy
  /// ([deltaScan]).
  final int? version;
  final int? asOf;
  final bool allowExternalPaths;

  _DeltaScan(
    this.path, {
    this.version,
    this.asOf,
    this.allowExternalPaths = false,
  });

  DeltaTable get _table =>
      DeltaTable(path, allowExternalPaths: allowExternalPaths);

  @override
  Future<Schema?> readSchema() =>
      _table.schema(version: version, asOf: asOf).unwrap();

  @override
  Stream<Table> batches({
    int size = defaultScanBatch,
    List<String>? projection,
    RowFilter? filter,
  }) => _table
      .readBatches(
        version: version,
        asOf: asOf,
        batchSize: size,
        filter: filter,
        columns: projection,
      )
      .unwrap();

  @override
  Future<Table> read({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    List<String>? projection,
    RowFilter? filter,
  }) =>
      _drain(this, 'Delta table $path', maxRows, maxBytes, projection, filter);
}

class _IcebergScan implements Scan {
  final String path;

  /// The snapshot read, by id or timestamp, the metadata read when neither
  /// is set, and the path policy ([icebergScan]).
  final int? snapshotId;
  final int? asOf;
  final Map<String, dynamic>? metadata;
  final bool allowExternalPaths;

  _IcebergScan(
    this.path, {
    this.snapshotId,
    this.asOf,
    this.metadata,
    this.allowExternalPaths = false,
  });

  IcebergTable get _table => IcebergTable(
    path,
    allowExternalPaths: allowExternalPaths,
    catalog: switch (metadata) {
      final Map<String, dynamic> metadata => _Fixed(metadata),
      null => null,
    },
  );

  @override
  Future<Schema?> readSchema() =>
      _table.schema(snapshotId: snapshotId, asOf: asOf).unwrap();

  @override
  Stream<Table> batches({
    int size = defaultScanBatch,
    List<String>? projection,
    RowFilter? filter,
  }) => _table
      .readBatches(
        snapshotId: snapshotId,
        asOf: asOf,
        batchSize: size,
        filter: filter,
        columns: projection,
      )
      .unwrap();

  @override
  Future<Table> read({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    List<String>? projection,
    RowFilter? filter,
  }) => _drain(
    this,
    'Iceberg table $path',
    maxRows,
    maxBytes,
    projection,
    filter,
  );
}

/// A catalog whose current metadata is always [metadata].
final class _Fixed implements IcebergCatalog {
  final Map<String, dynamic> metadata;
  const _Fixed(this.metadata);

  @override
  Future<Result<Map<String, dynamic>?, ErrorDetails>> current(
    String root, {
    bool allowOversized = false,
  }) async => Ok(metadata);
}

class _DatasetScan implements Scan {
  final String source;

  /// The reader's own pruning filter, columns and schema-read concurrency
  /// ([datasetScan]).
  final RowFilter? filter;
  final List<String>? columns;
  final int concurrency;

  _DatasetScan(this.source, {this.filter, this.columns, this.concurrency = 1});

  Dataset _dataset(List<String>? projection, RowFilter? filter) => Dataset(
    source,
    filter: _both(this.filter, filter),
    columns: projection ?? columns,
    concurrency: concurrency,
  );

  @override
  Future<Schema?> readSchema() => _peek(batches(size: 1));

  @override
  Stream<Table> batches({
    int size = defaultScanBatch,
    List<String>? projection,
    RowFilter? filter,
  }) => _dataset(projection, filter).readBatches(batchSize: size).unwrap();

  @override
  Future<Table> read({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    List<String>? projection,
    RowFilter? filter,
  }) => _drain(this, 'dataset $source', maxRows, maxBytes, projection, filter);
}

/// Read a lake table [scan] eagerly through its batches, so [projection] and
/// [filter] apply and the caps count only the columns read: reading the whole
/// table and projecting after charged [maxBytes] for every column.
Future<Table> _drain(
  Scan scan,
  String label,
  int? maxRows,
  int? maxBytes,
  List<String>? projection,
  RowFilter? filter,
) async {
  final batches = <Table>[];
  var rows = 0;
  var bytes = 0;
  await for (final batch in scan.batches(
    projection: projection,
    filter: filter,
  )) {
    rows += batch.rowCount;
    bytes += batch.memoryUsage;
    enforceMaterializationCaps(rows, bytes, maxRows, maxBytes, label);
    batches.add(batch);
  }
  if (batches.isNotEmpty) return Table.concat(batches);
  final schema = await scan.readSchema() ?? Schema(fields: const []);
  return Table.empty(
    'table',
    Schema(
      fields: [
        for (final field in schema.fields)
          if (projection == null || projection.contains(field.name)) field,
      ],
    ),
  );
}
