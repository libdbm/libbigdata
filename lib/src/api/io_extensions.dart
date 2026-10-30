import 'dart:async';
import 'dart:io';
import 'package:logging/logging.dart';
import 'package:meta/meta.dart';

import 'dataframe.dart';
import '../core/failure.dart';
import '../core/result.dart';
import '../core/scan.dart';
import '../core/table.dart';
import '../core/schema.dart';
import '../core/limits.dart';
import '../formats/csv/csv_reader.dart';
import '../formats/csv/csv_writer.dart';
import '../formats/csv/csv_options.dart';
import '../formats/json/json_reader.dart';
import '../formats/json/json_writer.dart';
import '../formats/arrow/arrow_reader.dart';
import '../formats/arrow/arrow_writer.dart';
import '../formats/parquet/parquet_reader.dart';
import '../formats/parquet/parquet_writer.dart';
import '../formats/scan.dart';
import '../core/row_filter.dart';
import '../formats/dataset/dataset.dart';
import '../formats/delta/delta_table.dart';
import '../formats/delta/delta_writer.dart';
import '../formats/delta/delta_commit_coordinator.dart';
import '../sql/execution/sql_invariants.dart';
import '../formats/iceberg/iceberg_table.dart';
import '../formats/iceberg/iceberg_writer.dart';
import '../formats/iceberg/iceberg_backend.dart';
import '../util/logging_config.dart' show Loggers;

/// Extension methods for DataFrame I/O operations
extension DataFrameIO on DataFrame {
  static final Logger _log = Logger(Loggers.dataframe);

  /// Read DataFrame from CSV file. Like every read here, a failure is
  /// reported as an [Err] with the reader's kind ([Kind.missing],
  /// [Kind.corrupt], [Kind.limit], [Kind.invalid], ...).
  ///
  /// The frame is lazy: the reader reads the header and the type-inference
  /// sample, so a missing file, a malformed header or an unknown column is an
  /// [Err] here, and the file is scanned with [options] when a terminal runs,
  /// so a later `filter`/`select` is applied as it streams. [filter] keeps
  /// exactly the rows it matches ([RowFilter.matches]), [columns] the named
  /// columns in that order, and [limit] the first rows after [filter]; the
  /// scan reads [batchSize] rows at a time when given. A materializing
  /// terminal fails with [Kind.limit] past [maxRows]/[maxBytes] (pass null to
  /// opt out), as well as past its own caps. A file with no header and no
  /// rows is read when called, as is a read the scan cannot stream: a custom
  /// [CsvOptions.lineTerminator], or a [CsvOptions.maxFileBytes] ceiling on
  /// the whole-file read. Use [readCsvBatches] to stream the result as well.
  @useResult
  static Future<Result<DataFrame, ErrorDetails>> readCsv(
    String path, {
    CsvOptions? options,
    RowFilter? filter,
    List<String>? columns,
    int? limit,
    int? batchSize,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) {
    final whole =
        filter == null && columns == null && limit == null && batchSize == null;
    // An invalid options object throws here, as a precondition, for the
    // whole-file read, as CsvReader's constructor does.
    if (whole) options?.validate();
    Future<Result<DataFrame, ErrorDetails>> eager() => whole
        ? _frame(
            CsvReader(
              options: options,
            ).readFile(path, maxRows: maxRows, maxBytes: maxBytes),
          )
        : _scan(
            path,
            options: options,
            filter: filter,
            columns: columns,
            limit: limit,
            batchSize: batchSize ?? 65536,
            maxRows: maxRows,
            maxBytes: maxBytes,
          );
    final settings = options ?? const CsvOptions();
    if (settings.lineTerminator != '\n' || settings.maxFileBytes != null) {
      return eager();
    }
    return _lazy(
      path,
      () => csvScan(
        path,
        options: options,
        filter: filter,
        columns: columns,
        limit: limit,
        size: batchSize,
      ),
      eager,
      maxRows: maxRows,
      maxBytes: maxBytes,
    );
  }

  /// A lazy frame over [path] that [open]s its scan when a terminal runs,
  /// bounded by [maxRows]/[maxBytes] as [lazy] is. The schema is read here,
  /// so a missing file or an unreadable schema is an [Err] from the reader;
  /// a source with no schema to read (an empty file) is read by [eager].
  /// [pin], when given, reads the schema in place of [open]'s scan and
  /// returns the scan to open in place of [open]: one fixed to the table
  /// version the schema was read from.
  static Future<Result<DataFrame, ErrorDetails>> _lazy(
    String path,
    Scan Function() open,
    Future<Result<DataFrame, ErrorDetails>> Function() eager, {
    Future<(Schema?, Scan Function())> Function()? pin,
    required int? maxRows,
    required int? maxBytes,
  }) async {
    final probe = await capture(
      pin ?? () async => (await open().readSchema(), open),
      fallback: Kind.corrupt,
    );
    return switch (probe) {
      Ok(value: (final Schema schema, final scan)) => Ok(
        lazy(path, schema, scan, maxRows: maxRows, maxBytes: maxBytes),
      ),
      Ok() => await eager(),
      Err(:final error) => Err(error),
    };
  }

  /// [table] as a DataFrame, or its [Err].
  static Future<Result<DataFrame, ErrorDetails>> _frame(
    Future<Result<Table, ErrorDetails>> table,
  ) async => switch (await table) {
    Ok(:final value) => Ok(DataFrame.fromTable(value)),
    Err(:final error) => Err(error),
  };

  /// Each batch of [tables] as a DataFrame; an [Err] passes through.
  static Stream<Result<DataFrame, ErrorDetails>> _frames(
    Stream<Result<Table, ErrorDetails>> tables,
  ) => tables.map(
    (result) => switch (result) {
      Ok(:final value) => Ok(DataFrame.fromTable(value)),
      Err(:final error) => Err(error),
    },
  );

  /// [readCsv]'s batch scan: the batches are collected under
  /// [maxRows]/[maxBytes] and concatenated into one DataFrame.
  static Future<Result<DataFrame, ErrorDetails>> _scan(
    String path, {
    required CsvOptions? options,
    required RowFilter? filter,
    required List<String>? columns,
    required int? limit,
    required int batchSize,
    required int? maxRows,
    required int? maxBytes,
  }) => capture(() async {
    final batches = <Table>[];
    var rows = 0;
    var bytes = 0;
    final stream = CsvReader(options: options)
        .readBatches(
          path,
          batchSize: batchSize,
          filter: filter,
          columns: columns,
          limit: limit,
        )
        .unwrap();
    await for (final batch in stream) {
      rows += batch.rowCount;
      bytes += batch.memoryUsage;
      enforceMaterializationCaps(
        rows,
        bytes,
        maxRows,
        maxBytes,
        'readCsv result',
      );
      batches.add(batch);
    }
    if (batches.isEmpty) return DataFrame.empty(Schema(fields: const []));
    return DataFrame.fromTable(Table.concat(batches));
  }, fallback: Kind.corrupt);

  /// Stream a CSV file as DataFrame batches of ~[batchSize] rows, holding only
  /// one batch in memory (bounded-memory read for large files). The schema is
  /// fixed across all batches. [filter] keeps exactly the rows it matches.
  static Stream<Result<DataFrame, ErrorDetails>> readCsvBatches(
    String path, {
    CsvOptions? options,
    int batchSize = 10000,
    RowFilter? filter,
    List<String>? columns,
    int? limit,
  }) => _frames(
    CsvReader(options: options).readBatches(
      path,
      batchSize: batchSize,
      filter: filter,
      columns: columns,
      limit: limit,
    ),
  );

  /// Create DataFrame from CSV string
  @useResult
  static Future<Result<DataFrame, ErrorDetails>> fromCsvString(
    String content, {
    CsvOptions? options,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => _frame(
    CsvReader(
      options: options,
    ).readString(content, maxRows: maxRows, maxBytes: maxBytes),
  );

  /// Read DataFrame from JSON file (JSON Lines for a `.jsonl`/`.ndjson`
  /// path). The frame is lazy, as [readCsv]'s is: the schema is inferred from
  /// the leading sample here, and the file is scanned with [options] when a
  /// terminal runs, its result bounded by [maxRows]/[maxBytes]. Read when
  /// called: an empty file, [JsonOptions.expandArrays] (the scan cannot
  /// expand arrays), and `maxRows: null, maxBytes: null`, which infers the
  /// schema from the whole file.
  @useResult
  static Future<Result<DataFrame, ErrorDetails>> readJson(
    String path, {
    JsonOptions? options,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => _json(path, options ?? const JsonOptions(), maxRows, maxBytes);

  /// [readJson] and [readJsonLines] with [options].
  static Future<Result<DataFrame, ErrorDetails>> _json(
    String path,
    JsonOptions options,
    int? maxRows,
    int? maxBytes,
  ) {
    Future<Result<DataFrame, ErrorDetails>> eager() => _frame(
      JsonReader(options).readFile(path, maxRows: maxRows, maxBytes: maxBytes),
    );
    if (options.expandArrays || (maxRows == null && maxBytes == null)) {
      return eager();
    }
    final lines = path.endsWith('.jsonl') || path.endsWith('.ndjson');
    final effective = lines ? options.copyWith(jsonLines: true) : options;
    return _lazy(
      path,
      () => jsonScan(path, effective),
      eager,
      maxRows: maxRows,
      maxBytes: maxBytes,
    );
  }

  /// Stream a JSON Lines file as DataFrame batches of ~[batchSize] rows,
  /// holding only one batch in memory. Requires JSON Lines
  /// (`.jsonl`/`.ndjson` auto-detected).
  static Stream<Result<DataFrame, ErrorDetails>> readJsonBatches(
    String path, {
    JsonOptions? options,
    int batchSize = 10000,
  }) {
    final base = options ?? const JsonOptions();
    final lines = path.endsWith('.jsonl') || path.endsWith('.ndjson');
    final effective = lines ? base.copyWith(jsonLines: true) : base;
    return _frames(
      JsonReader(effective).readBatches(path, batchSize: batchSize),
    );
  }

  /// Create DataFrame from JSON string
  @useResult
  static Future<Result<DataFrame, ErrorDetails>> fromJsonString(
    String content, {
    JsonOptions? options,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => _frame(
    JsonReader(
      options,
    ).readString(content, maxRows: maxRows, maxBytes: maxBytes),
  );

  /// Read DataFrame from JSON Lines file, lazily as [readJson] does.
  @useResult
  static Future<Result<DataFrame, ErrorDetails>> readJsonLines(
    String path, {
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => _json(path, JsonOptions.jsonLines(), maxRows, maxBytes);

  /// Create DataFrame from JSON Lines string
  @useResult
  static Future<Result<DataFrame, ErrorDetails>> fromJsonLinesString(
    String content, {
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => _frame(
    JsonReader(
      JsonOptions.jsonLines(),
    ).readString(content, maxRows: maxRows, maxBytes: maxBytes),
  );

  /// Read DataFrame from a Parquet file. An optional [filter] prunes row groups
  /// via column statistics (a kept row group comes back whole), and [columns]
  /// restricts which columns are decoded.
  ///
  /// The frame is lazy: the footer is read here, so a missing or malformed
  /// file is an [Err] from the reader, and the file is scanned when a terminal
  /// runs, so `readParquet(path).filter(...).head(n)` streams and stops early,
  /// and a later `filter`/`select` prunes row groups and columns as [filter]
  /// and [columns] do. A materializing terminal fails with [Kind.limit] past
  /// [maxRows]/[maxBytes] (pass null to opt out), as well as past its own.
  @useResult
  static Future<Result<DataFrame, ErrorDetails>> readParquet(
    String path, {
    RowFilter? filter,
    List<String>? columns,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => _lazy(
    path,
    () => parquetScan(path, filter: filter, columns: columns),
    // No footer schema available: an eager read.
    () => capture(
      () async => DataFrame.fromTable(
        await ParquetReader(
          path,
          options: ParquetReaderOptions(filter: filter, columns: columns),
        ).read(maxRows: maxRows, maxBytes: maxBytes).unwrap(),
      ),
      fallback: Kind.corrupt,
    ),
    maxRows: maxRows,
    maxBytes: maxBytes,
  );

  /// Write a stream of DataFrame batches to one Parquet file without
  /// materializing the whole dataset (bounded to one row group in memory).
  /// Rows accrue across batches into full row groups.
  /// A failure (a batch's own, or the write's) is reported as an [Err] with no
  /// partial file left behind.
  @useResult
  static Future<Result<void, ErrorDetails>> writeParquetStream(
    String path,
    Stream<DataFrame> batches, {
    ParquetWriterOptions? options,
  }) => ParquetWriter(path, options: options).writeBatches(
    batches.asyncMap(
      (frame) async =>
          (await frame.execute(maxRows: null, maxBytes: null)).unwrap(),
    ),
  );

  /// Read a multi-file dataset (directory, glob, or single file) as one
  /// DataFrame. Hive-style `key=value` path segments become partition columns;
  /// an optional [filter] prunes whole files by partition value (and is pushed
  /// into Parquet row-group pruning); [columns] projects the unified result.
  /// A materializing terminal fails with [Kind.limit] past [maxRows]/
  /// [maxBytes] (pass null to opt out), or use [readDatasetBatches] to
  /// stream.
  ///
  /// The frame is lazy: the files are found and the unified schema read here
  /// (a missing path, conflicting column types or an unknown column is an
  /// [Err] from the reader), and the files are scanned when a terminal runs,
  /// so a later `filter` prunes files and row groups as [filter] does.
  /// [concurrency] overlaps the per-file schema reads. With
  /// `maxRows: null, maxBytes: null` and a [concurrency] above 1 the dataset
  /// is read when called, its files in parallel; so is a dataset with no
  /// rows.
  @useResult
  static Future<Result<DataFrame, ErrorDetails>> readDataset(
    String pathOrGlob, {
    RowFilter? filter,
    List<String>? columns,
    int concurrency = 1,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) {
    Future<Result<DataFrame, ErrorDetails>> eager() => _frame(
      Dataset(
        pathOrGlob,
        filter: filter,
        columns: columns,
        concurrency: concurrency,
        maxRows: maxRows,
        maxBytes: maxBytes,
      ).read(),
    );
    if (concurrency > 1 && maxRows == null && maxBytes == null) return eager();
    return _lazy(
      pathOrGlob,
      () => datasetScan(
        pathOrGlob,
        filter: filter,
        columns: columns,
        concurrency: concurrency,
      ),
      eager,
      maxRows: maxRows,
      maxBytes: maxBytes,
    );
  }

  /// Stream a multi-file dataset as bounded batches (same discovery, partition
  /// pruning, projection, and unified schema as [readDataset]) without holding
  /// every file in memory at once. Single-isolate; use [readDataset] with
  /// `concurrency` for parallel eager reads. Each batch is a DataFrame, as
  /// the other batch readers yield.
  static Stream<Result<DataFrame, ErrorDetails>> readDatasetBatches(
    String pathOrGlob, {
    RowFilter? filter,
    List<String>? columns,
    int batchSize = 10000,
  }) => _frames(
    Dataset(
      pathOrGlob,
      filter: filter,
      columns: columns,
    ).readBatches(batchSize: batchSize),
  );

  /// Read an Apache Iceberg table as a DataFrame, with optional time travel by
  /// [snapshotId] or [asOf] (ms since epoch).
  ///
  /// The frame is lazy: the table metadata and the snapshot's schema are read
  /// here (a path that is not an Iceberg table or an unknown snapshot is an
  /// [Err] from the reader), and the snapshot is scanned when a terminal runs,
  /// so a later `filter` skips manifests and files and a `select` reads only
  /// its columns. Without [snapshotId] or [asOf], every terminal reads the
  /// snapshot and schema current when the reader was called, so a later
  /// commit does not change the frame. A materializing terminal fails with
  /// [Kind.limit]
  /// past [maxRows]/[maxBytes] (pass null to opt out).
  @useResult
  static Future<Result<DataFrame, ErrorDetails>> readIceberg(
    String path, {
    int? snapshotId,
    int? asOf,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    bool allowExternalPaths = false,
  }) => _lazy(
    path,
    () => icebergScan(
      path,
      snapshotId: snapshotId,
      asOf: asOf,
      allowExternalPaths: allowExternalPaths,
    ),
    () => _frame(
      IcebergTable(path, allowExternalPaths: allowExternalPaths).read(
        snapshotId: snapshotId,
        asOf: asOf,
        maxRows: maxRows,
        maxBytes: maxBytes,
      ),
    ),
    pin: snapshotId == null && asOf == null
        ? () async {
            final (schema, metadata) = await IcebergTable(
              path,
              allowExternalPaths: allowExternalPaths,
            ).head().unwrap();
            return (
              schema,
              () => icebergScan(
                path,
                metadata: metadata,
                allowExternalPaths: allowExternalPaths,
              ),
            );
          }
        : null,
    maxRows: maxRows,
    maxBytes: maxBytes,
  );

  /// Write this DataFrame as an Iceberg commit (creates the table if needed).
  ///
  /// The default [backend] is FILESYSTEM-based: safe for a SINGLE writer (or
  /// single host), but a bare filesystem cannot provide true cross-engine
  /// compare-and-swap of the current-metadata pointer, for concurrent writers
  /// across engines, pass a catalog-backed [IcebergBackend] (REST / Glue / Hive)
  /// that owns both the current-pointer read and the commit CAS.
  ///
  /// The DataFrame is materialized first, under [maxRows]/[maxBytes] (pass
  /// null to opt out).
  ///
  /// Returns the committed version, or an [Err] as [IcebergWriter.write] does.
  @useResult
  Future<Result<int, ErrorDetails>> writeIceberg(
    String path, {
    IcebergWriteMode mode = IcebergWriteMode.append,
    IcebergBackend? backend,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => guard(
    () async => switch (await execute(maxRows: maxRows, maxBytes: maxBytes)) {
      Ok(:final value) => await IcebergWriter(
        path,
        backend: backend,
      ).write(value, mode: mode),
      Err(:final error) => Err(error),
    },
  );

  /// Read a Delta Lake table as a DataFrame, with optional time travel by
  /// [version] or [asOf] (ms since epoch), as [readIceberg] does. Deletion
  /// vectors are applied.
  ///
  /// The frame is lazy, as [readIceberg]'s is: the log and the version's
  /// schema are read here (a path that is not a Delta table or an unknown
  /// version is an [Err] from the reader), and the version is scanned when a
  /// terminal runs, so a later `filter` skips files by partition values and
  /// statistics and a `select` reads only its columns. Without [version] or
  /// [asOf], every terminal reads the version that was latest when the reader
  /// was called, so a later commit does not change the frame. A
  /// [concurrency] above 1 reads the table when called, its files in
  /// parallel.
  @useResult
  static Future<Result<DataFrame, ErrorDetails>> readDelta(
    String path, {
    int? version,
    int? asOf,
    int concurrency = 1,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    bool allowExternalPaths = false,
  }) {
    Future<Result<DataFrame, ErrorDetails>> eager() => _frame(
      DeltaTable(path, allowExternalPaths: allowExternalPaths).read(
        version: version,
        asOf: asOf,
        concurrency: concurrency,
        maxRows: maxRows,
        maxBytes: maxBytes,
      ),
    );
    if (concurrency > 1) return eager();
    return _lazy(
      path,
      () => deltaScan(
        path,
        version: version,
        asOf: asOf,
        allowExternalPaths: allowExternalPaths,
      ),
      eager,
      pin: version == null && asOf == null
          ? () async {
              final (schema, number) = await DeltaTable(
                path,
                allowExternalPaths: allowExternalPaths,
              ).head().unwrap();
              return (
                schema,
                () => deltaScan(
                  path,
                  version: number,
                  allowExternalPaths: allowExternalPaths,
                ),
              );
            }
          : null,
      maxRows: maxRows,
      maxBytes: maxBytes,
    );
  }

  /// Write this DataFrame as a Delta Lake commit (creates the table if needed).
  ///
  /// Any `delta.invariants` the target table declares are ENFORCED against the
  /// rows being written (a violation commits nothing and returns an [Err] of
  /// kind [Kind.invalid]). This layer can
  /// wire the evaluator because it already depends on the query engine;
  /// constructing a [DeltaWriter] directly leaves it unwired, which REJECTS an
  /// invariant-carrying table rather than writing it unchecked.
  /// The default [coordinator] is FILESYSTEM-based: safe for a SINGLE writer (or
  /// single host), but a bare filesystem cannot provide true cross-engine
  /// compare-and-swap of the version pointer, for concurrent writers across
  /// engines, pass a catalog-backed [DeltaCommitCoordinator].
  ///
  /// The DataFrame is materialized first, under [maxRows]/[maxBytes] (pass
  /// null to opt out).
  ///
  /// Returns the committed version, or an [Err] as [DeltaWriter.write] does.
  @useResult
  Future<Result<int, ErrorDetails>> writeDelta(
    String path, {
    DeltaWriteMode mode = DeltaWriteMode.append,
    DeltaCommitCoordinator? coordinator,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => guard(
    () async => switch (await execute(maxRows: maxRows, maxBytes: maxBytes)) {
      Ok(:final value) => await DeltaWriter(
        path,
        coordinator: coordinator,
        invariants: const SqlInvariantEvaluator(),
      ).write(value, mode: mode),
      Err(:final error) => Err(error),
    },
  );

  /// Read DataFrame from an Apache Arrow IPC file (what [writeArrow] and
  /// `pa.ipc.new_file` write).
  ///
  /// The frame is lazy: the footer is read here (a missing file or one that
  /// is not an IPC file is an [Err] from the reader), and the record batches
  /// when a terminal runs, a `select` decoding only its columns. A record
  /// batch larger than [maxBytes] is refused before it is read, and a
  /// materializing terminal fails with [Kind.limit] past [maxRows]/[maxBytes]
  /// (pass null to opt out).
  @useResult
  static Future<Result<DataFrame, ErrorDetails>> readArrow(
    String path, {
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => _lazy(
    path,
    () => arrowScan(path, block: maxBytes),
    () => capture(
      () async => DataFrame.fromTable(
        await ArrowReader(path).read(maxRows: maxRows, maxBytes: maxBytes),
      ),
      fallback: Kind.corrupt,
    ),
    maxRows: maxRows,
    maxBytes: maxBytes,
  );

  /// Read DataFrame from an Apache Arrow IPC stream (what [writeArrowStream]
  /// and `pa.ipc.new_stream` write), read when called; an IPC file reads
  /// lazily, as [readArrow] reads it.
  @useResult
  static Future<Result<DataFrame, ErrorDetails>> readArrowStream(
    String path, {
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) async {
    if (await _ipcFile(path)) {
      return readArrow(path, maxRows: maxRows, maxBytes: maxBytes);
    }
    return capture(
      () async => DataFrame.fromTable(
        await ArrowReader(
          path,
        ).readStream(maxRows: maxRows, maxBytes: maxBytes),
      ),
      fallback: Kind.corrupt,
    );
  }

  /// Whether [path] begins with the Arrow IPC file magic, `ARROW1`; false
  /// when it cannot be read.
  static Future<bool> _ipcFile(String path) async {
    try {
      final file = await File(path).open();
      try {
        return String.fromCharCodes(await file.read(6)) == 'ARROW1';
      } finally {
        await file.close();
      }
    } on FileSystemException {
      return false;
    }
  }

  /// Stream DataFrames from an Apache Arrow IPC file, [batchSize] rows at most
  /// per batch.
  static Stream<Result<DataFrame, ErrorDetails>> readArrowBatches(
    String path, {
    int batchSize = 10000,
  }) => captureStream(
    () => ArrowReader(
      path,
    ).readBatches(batchSize: batchSize).map(DataFrame.fromTable),
    fallback: Kind.corrupt,
  );

  /// Write DataFrame to a real Apache Arrow IPC file. Optionally compress the
  /// record-batch bodies with LZ4_FRAME or ZSTD ([ArrowCompression]).
  ///
  /// Like every write here, a failure is reported as an [Err] with no partial
  /// file left behind: [Kind.io] when the file cannot be written,
  /// [Kind.unsupported] for a column type Arrow IPC cannot carry, or the
  /// DataFrame's own failure. The DataFrame is materialized first, under
  /// [maxRows]/[maxBytes] (pass null to opt out).
  @useResult
  Future<Result<void, ErrorDetails>> writeArrow(
    String path, {
    int batchSize = 10000,
    ArrowCompression compression = ArrowCompression.none,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) {
    // Constructed first so a non-positive batchSize throws, as a precondition.
    final writer = ArrowWriter(
      path,
      batchSize: batchSize,
      compression: compression,
    );
    return capture(
      () async => writer.write(
        (await execute(maxRows: maxRows, maxBytes: maxBytes)).unwrap(),
      ),
    );
  }

  /// Write DataFrame in the Arrow IPC **stream** format (`pa.ipc.open_stream`
  /// reads it), STREAMING batch-by-batch (peak memory ~one batch) rather than
  /// materializing the whole result first. Each record batch holds at most
  /// [batchSize] rows; [compression] is as for [writeArrow].
  @useResult
  Future<Result<void, ErrorDetails>> writeArrowStream(
    String path, {
    int batchSize = 10000,
    ArrowCompression compression = ArrowCompression.none,
  }) {
    // Constructed first so a non-positive batchSize throws, as a precondition.
    final writer = ArrowWriter(
      path,
      batchSize: batchSize,
      compression: compression,
      stream: true,
    );
    return capture(() => writer.writeBatches(executeStream().unwrap()));
  }

  /// Write this DataFrame to a CSV file, streaming batch-by-batch.
  @useResult
  Future<Result<void, ErrorDetails>> writeCsv(
    String path, {
    CsvOptions? options,
  }) => CsvWriter(options).writeFile(executeStream().unwrap(), path);

  /// This DataFrame as a CSV string (materializes the whole result in memory).
  /// [maxRows]/[maxBytes] fail loud past the cap so a huge result doesn't OOM
  /// silently; pass null to opt out (use [writeCsv] to stream to a file instead).
  /// Failures are reported as [DataFrame.execute] reports them.
  @useResult
  Future<Result<String, ErrorDetails>> toCsv({
    CsvOptions? options,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => _text(
    (table) => CsvWriter(options).writeString(table),
    maxRows,
    maxBytes,
  );

  /// This DataFrame, executed under [maxRows]/[maxBytes], rendered by [render].
  Future<Result<String, ErrorDetails>> _text(
    FutureOr<String> Function(Table table) render,
    int? maxRows,
    int? maxBytes,
  ) => report(
    () async => Ok(
      await render(
        (await execute(maxRows: maxRows, maxBytes: maxBytes)).unwrap(),
      ),
    ),
    _log,
  );

  /// Write this DataFrame to a JSON file, streaming batch-by-batch (JSON Lines
  /// auto-detected from a `.jsonl`/`.ndjson` extension).
  @useResult
  Future<Result<void, ErrorDetails>> writeJson(
    String path, {
    JsonOptions? options,
  }) {
    final lines = path.endsWith('.jsonl') || path.endsWith('.ndjson');
    final effective =
        options ?? (lines ? JsonOptions.jsonLines() : const JsonOptions());
    return JsonWriter(effective).writeFile(executeStream().unwrap(), path);
  }

  /// Write this DataFrame to a JSON Lines file, streaming.
  @useResult
  Future<Result<void, ErrorDetails>> writeJsonLines(
    String path, {
    JsonOptions? options,
  }) => JsonWriter(
    options ?? JsonOptions.jsonLines(),
  ).writeFile(executeStream().unwrap(), path);

  /// This DataFrame as a JSON string (materializes the whole result in memory).
  /// [maxRows]/[maxBytes] fail loud past the cap; pass null to opt out (use
  /// [writeJson] to stream to a file instead).
  @useResult
  Future<Result<String, ErrorDetails>> toJson({
    JsonOptions? options,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => _text(
    (table) => JsonWriter(options).writeString(table),
    maxRows,
    maxBytes,
  );

  /// This DataFrame as a JSON Lines string (materializes the whole result in
  /// memory). [maxRows]/[maxBytes] fail loud past the cap; pass null to opt out
  /// (use [writeJsonLines] to stream to a file instead).
  @useResult
  Future<Result<String, ErrorDetails>> toJsonLines({
    JsonOptions? options,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => _text(
    (table) =>
        JsonWriter(options ?? JsonOptions.jsonLines()).writeString(table),
    maxRows,
    maxBytes,
  );
}
