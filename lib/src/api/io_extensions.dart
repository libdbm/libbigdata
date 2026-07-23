import 'dart:async';
import 'dataframe.dart';
import '../core/table.dart';
import '../core/schema.dart';
import '../core/limits.dart';
import '../sql/exec/kernels.dart';
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

/// Extension methods for DataFrame I/O operations
extension DataFrameIO on DataFrame {
  /// Read DataFrame from CSV file
  static Future<DataFrame> readCsv(String path, [CsvOptions? options]) async {
    final reader = CsvReader(options: options);
    return DataFrame.fromTable(await reader.readFile(path));
  }

  /// Read a CSV file with filter / column-projection / limit pushdown applied
  /// during a bounded-memory streaming scan, returning the materialized RESULT as
  /// one DataFrame. The whole input file is never held at once — with a selective
  /// [filter] or [limit], peak memory tracks the result size rather than the file
  /// size. The materialized RESULT is still bounded like every other eager reader:
  /// it throws once it exceeds [maxRows]/[maxBytes] (pass `null` to opt out), so
  /// an unfiltered scan of a huge file fails loud instead of OOMing. [bufferSize]
  /// is the scan batch size in rows; use [readCsvBatches] to truly stream.
  static Future<DataFrame> readCsvOptimized(
    String path, {
    CsvOptions? options,
    String? filter,
    List<String>? selectColumns,
    int? limit,
    int? bufferSize,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) async {
    final batches = <Table>[];
    var rows = 0;
    var bytes = 0;
    final stream = CsvReader(options: options).readBatches(
      path,
      size: bufferSize ?? 65536,
      filter: filter,
      selectColumns: selectColumns,
      limit: limit,
    );
    await for (final batch in stream) {
      rows += batch.rowCount;
      bytes += batch.memoryUsage;
      enforceMaterializationCaps(
        rows,
        bytes,
        maxRows,
        maxBytes,
        'readCsvOptimized result',
      );
      batches.add(batch);
    }
    if (batches.isEmpty) return DataFrame.empty(Schema(fields: const []));
    return DataFrame.fromTable(_merge(batches));
  }

  /// Stream a CSV file as DataFrame batches of ~[size] rows, holding only one
  /// batch in memory (bounded-memory read for large files). The schema is fixed
  /// across all batches.
  static Stream<DataFrame> readCsvBatches(
    String path, {
    CsvOptions? options,
    int size = 10000,
    String? filter,
    List<String>? selectColumns,
    int? limit,
  }) {
    return CsvReader(options: options)
        .readBatches(
          path,
          size: size,
          filter: filter,
          selectColumns: selectColumns,
          limit: limit,
        )
        .map(DataFrame.fromTable);
  }

  /// Create DataFrame from CSV string
  static Future<DataFrame> fromCsvString(
    String csvContent, [
    CsvOptions? options,
  ]) async {
    final reader = CsvReader(options: options);
    return DataFrame.fromTable(await reader.readString(csvContent));
  }

  /// Read DataFrame from JSON file
  static Future<DataFrame> readJson(String path, [JsonOptions? options]) async {
    final reader = JsonReader(options);
    return DataFrame.fromTable(await reader.readFile(path));
  }

  /// Stream a JSON Lines file as DataFrame batches of ~[size] rows, holding only
  /// one batch in memory. Requires JSON Lines (`.jsonl`/`.ndjson` auto-detected).
  static Stream<DataFrame> readJsonBatches(
    String path, {
    JsonOptions? options,
    int size = 10000,
  }) {
    final base = options ?? const JsonOptions();
    final useJsonLines = path.endsWith('.jsonl') || path.endsWith('.ndjson');
    final effective = useJsonLines ? base.copyWith(jsonLines: true) : base;
    return JsonReader(
      effective,
    ).readBatches(path, size: size).map(DataFrame.fromTable);
  }

  /// Create DataFrame from JSON string
  static Future<DataFrame> fromJsonString(
    String jsonContent, [
    JsonOptions? options,
  ]) async {
    final reader = JsonReader(options);
    return DataFrame.fromTable(await reader.readString(jsonContent));
  }

  /// Read DataFrame from JSON Lines file
  static Future<DataFrame> readJsonLines(String path) async {
    final reader = JsonReader(JsonOptions.jsonLines());
    return DataFrame.fromTable(await reader.readFile(path));
  }

  /// Create DataFrame from JSON Lines string
  static Future<DataFrame> fromJsonLinesString(String content) async {
    final reader = JsonReader(JsonOptions.jsonLines());
    return DataFrame.fromTable(await reader.readString(content));
  }

  /// Read DataFrame from a Parquet file. An optional [filter] prunes row groups
  /// via column statistics, and [columns] restricts which columns are decoded.
  static Future<DataFrame> readParquet(
    String path, {
    RowFilter? filter,
    List<String>? columns,
  }) async {
    // A read-time filter/projection already pushes into the reader, so keep the
    // eager path for those. With neither, return a LAZY DataFrame rooted at a
    // streaming scan: `readParquet(path).filter(...).head(n)` then streams and
    // stops early instead of decoding the whole file up front.
    if (filter != null || columns != null) {
      final reader = ParquetReader(
        path,
        options: ParquetReaderOptions(filter: filter, columns: columns),
      );
      return DataFrame.fromTable(await reader.read());
    }
    final schema = await openScan(path).readSchema();
    if (schema == null) {
      // No footer schema available — fall back to an eager read.
      return DataFrame.fromTable(await ParquetReader(path).read());
    }
    return DataFrame.scan(path, schema);
  }

  /// Write a stream of DataFrame batches to one Parquet file without
  /// materializing the whole dataset (bounded to one row group in memory).
  /// Rows accrue across batches into full row groups.
  static Future<void> writeParquetStream(
    String path,
    Stream<DataFrame> batches, {
    ParquetWriterOptions? options,
  }) async {
    final writer = ParquetWriter(path, options: options);
    await writer.writeBatches(
      batches.asyncMap((df) => df.execute(maxRows: null, maxBytes: null)),
    );
  }

  /// Read a multi-file dataset (directory, glob, or single file) as one
  /// DataFrame. Hive-style `key=value` path segments become partition columns;
  /// an optional [filter] prunes whole files by partition value (and is pushed
  /// into Parquet row-group pruning); [columns] projects the unified result.
  /// [maxRows] caps the materialized result (default
  /// [defaultMaxMaterializedRows]) so an oversized dataset fails loud instead of
  /// OOMing; pass `maxRows: null` for an unbounded read (which can use
  /// [concurrency]), or [readDatasetBatches] to stream.
  static Future<DataFrame> readDataset(
    String pathOrGlob, {
    RowFilter? filter,
    List<String>? columns,
    int concurrency = 1,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) async {
    final table = await Dataset(
      pathOrGlob,
      filter: filter,
      columns: columns,
      concurrency: concurrency,
      maxRows: maxRows,
      maxBytes: maxBytes,
    ).read();
    return DataFrame.fromTable(table);
  }

  /// Stream a multi-file dataset as bounded batches (same discovery, partition
  /// pruning, projection, and unified schema as [readDataset]) without holding
  /// every file in memory at once. Single-isolate; use [readDataset] with
  /// `concurrency` for parallel eager reads.
  static Stream<Table> readDatasetBatches(
    String pathOrGlob, {
    RowFilter? filter,
    List<String>? columns,
    int size = 10000,
  }) {
    return Dataset(
      pathOrGlob,
      filter: filter,
      columns: columns,
    ).readBatches(size: size);
  }

  /// Read an Apache Iceberg table as a DataFrame, with optional time travel by
  /// [snapshotId] or [asOf] (ms since epoch).
  static Future<DataFrame> readIceberg(
    String path, {
    int? snapshotId,
    int? asOf,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    bool allowExternalPaths = false,
  }) async {
    final table =
        await IcebergTable(path, allowExternalPaths: allowExternalPaths).read(
          snapshotId: snapshotId,
          asOf: asOf,
          maxRows: maxRows,
          maxBytes: maxBytes,
        );
    return DataFrame.fromTable(table);
  }

  /// Write this DataFrame as an Iceberg commit (creates the table if needed).
  ///
  /// The default [backend] is FILESYSTEM-based: safe for a SINGLE writer (or
  /// single host), but a bare filesystem cannot provide true cross-engine
  /// compare-and-swap of the current-metadata pointer — for concurrent writers
  /// across engines, pass a catalog-backed [IcebergBackend] (REST / Glue / Hive)
  /// that owns both the current-pointer read and the commit CAS.
  Future<int> writeIceberg(
    String path, {
    IcebergWriteMode mode = IcebergWriteMode.append,
    IcebergBackend? backend,
  }) async {
    return IcebergWriter(
      path,
      backend: backend,
    ).write(await execute(), mode: mode);
  }

  /// Read a Delta Lake table as a DataFrame, with optional time travel by
  /// [version] or [timestamp] (ms since epoch). Deletion vectors are applied.
  static Future<DataFrame> readDelta(
    String path, {
    int? version,
    int? timestamp,
    int concurrency = 1,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    bool allowExternalPaths = false,
  }) async {
    final table = await DeltaTable(path, allowExternalPaths: allowExternalPaths)
        .read(
          version: version,
          timestamp: timestamp,
          concurrency: concurrency,
          maxRows: maxRows,
          maxBytes: maxBytes,
        );
    return DataFrame.fromTable(table);
  }

  /// Write this DataFrame as a Delta Lake commit (creates the table if needed).
  ///
  /// Any `delta.invariants` the target table declares are ENFORCED against the
  /// rows being written (a violation throws and commits nothing). This layer can
  /// wire the evaluator because it already depends on the query engine;
  /// constructing a [DeltaWriter] directly leaves it unwired, which REJECTS an
  /// invariant-carrying table rather than writing it unchecked.
  /// The default [coordinator] is FILESYSTEM-based: safe for a SINGLE writer (or
  /// single host), but a bare filesystem cannot provide true cross-engine
  /// compare-and-swap of the version pointer — for concurrent writers across
  /// engines, pass a catalog-backed [DeltaCommitCoordinator].
  Future<int> writeDelta(
    String path, {
    DeltaWriteMode mode = DeltaWriteMode.append,
    DeltaCommitCoordinator? coordinator,
  }) async {
    return DeltaWriter(
      path,
      coordinator: coordinator,
      invariants: const SqlInvariantEvaluator(),
    ).write(await execute(), mode: mode);
  }

  /// Read DataFrame from Arrow JSON file
  /// Note: This is a custom JSON format inspired by Arrow, not actual Arrow IPC
  static Future<DataFrame> readArrow(String path) async {
    final reader = ArrowReader(path);
    final table = await reader.read();
    return DataFrame.fromTable(table);
  }

  /// Read DataFrame from Arrow JSON stream file
  static Future<DataFrame> readArrowStream(String path) async {
    final reader = ArrowReader(path);
    final table = await reader.readStream();
    return DataFrame.fromTable(table);
  }

  /// Stream DataFrames from Arrow JSON file in batches
  static Stream<DataFrame> readArrowBatches(
    String path, {
    int batchSize = 10000,
  }) {
    final reader = ArrowReader(path);
    return reader
        .readBatches(batchSize: batchSize)
        .map((table) => DataFrame.fromTable(table));
  }

  /// Write DataFrame to a real Apache Arrow IPC file. Optionally compress the
  /// record-batch bodies with LZ4_FRAME or ZSTD ([ArrowCompression]).
  Future<void> writeArrow(
    String path, {
    int batchSize = 10000,
    ArrowCompression compression = ArrowCompression.none,
  }) async {
    final writer = ArrowWriter(
      path,
      batchSize: batchSize,
      compression: compression,
    );
    final table = await execute();
    await writer.write(table);
  }

  /// Write DataFrame to an Arrow IPC stream file, STREAMING batch-by-batch (peak
  /// memory ~one batch) rather than materializing the whole result first.
  Future<void> writeArrowStream(String path, {int batchSize = 10000}) async {
    final writer = ArrowWriter(path, batchSize: batchSize);
    await writer.writeBatches(executeStream());
  }

  /// Write this DataFrame to a CSV file, streaming batch-by-batch.
  Future<void> writeCsv(String path, [CsvOptions? options]) =>
      CsvWriter(options).writeFile(executeStream(), path);

  /// This DataFrame as a CSV string (materializes the whole result in memory).
  /// [maxRows]/[maxBytes] fail loud past the cap so a huge result doesn't OOM
  /// silently; pass null to opt out (use [writeCsv] to stream to a file instead).
  Future<String> toCsv([
    CsvOptions? options,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  ]) async => CsvWriter(
    options,
  ).writeString(await execute(maxRows: maxRows, maxBytes: maxBytes));

  /// Write this DataFrame to a JSON file, streaming batch-by-batch (JSON Lines
  /// auto-detected from a `.jsonl`/`.ndjson` extension).
  Future<void> writeJson(String filePath, [JsonOptions? options]) async {
    final useJsonLines =
        filePath.endsWith('.jsonl') || filePath.endsWith('.ndjson');
    final effective =
        options ??
        (useJsonLines ? JsonOptions.jsonLines() : const JsonOptions());
    await JsonWriter(effective).writeFile(executeStream(), filePath);
  }

  /// Write this DataFrame to a JSON Lines file, streaming.
  Future<void> writeJsonLines(String filePath, [JsonOptions? options]) async {
    await JsonWriter(
      options ?? JsonOptions.jsonLines(),
    ).writeFile(executeStream(), filePath);
  }

  /// This DataFrame as a JSON string (materializes the whole result in memory).
  /// [maxRows]/[maxBytes] fail loud past the cap; pass null to opt out (use
  /// [writeJson] to stream to a file instead).
  Future<String> toJson([
    JsonOptions? options,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  ]) async => JsonWriter(
    options,
  ).writeString(await execute(maxRows: maxRows, maxBytes: maxBytes));

  /// This DataFrame as a JSON Lines string (materializes the whole result in
  /// memory). [maxRows]/[maxBytes] fail loud past the cap; pass null to opt out
  /// (use [writeJsonLines] to stream to a file instead).
  Future<String> toJsonLines([
    JsonOptions? options,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  ]) async => JsonWriter(
    options ?? JsonOptions.jsonLines(),
  ).writeString(await execute(maxRows: maxRows, maxBytes: maxBytes));

  // Deprecated aliases for backward compatibility
  @Deprecated('Use readCsv instead')
  static Future<DataFrame> fromCsv(String path, [CsvOptions? options]) =>
      readCsv(path, options);

  @Deprecated('Use readJson instead')
  static Future<DataFrame> fromJson(String path, [JsonOptions? options]) =>
      readJson(path, options);

  @Deprecated('Use readJsonLines instead')
  static Future<DataFrame> fromJsonLines(String path) => readJsonLines(path);

  @Deprecated('Use readArrow instead')
  static Future<DataFrame> fromArrow(String path) => readArrow(path);

  @Deprecated('Use readArrowStream instead')
  static Future<DataFrame> fromArrowStream(String path) =>
      readArrowStream(path);

  @Deprecated('Use readArrowBatches instead')
  static Stream<DataFrame> fromArrowBatches(
    String path, {
    int batchSize = 10000,
  }) => readArrowBatches(path, batchSize: batchSize);

  @Deprecated('Use writeArrow instead')
  Future<void> toArrow(
    String path, {
    int batchSize = 10000,
    ArrowCompression compression = ArrowCompression.none,
  }) => writeArrow(path, batchSize: batchSize, compression: compression);

  @Deprecated('Use writeArrowStream instead')
  Future<void> toArrowStream(String path, {int batchSize = 10000}) =>
      writeArrowStream(path, batchSize: batchSize);
}

/// Concatenate the same-schema [tables] of a streaming read into one table.
Table _merge(List<Table> tables) {
  if (tables.length == 1) return tables.first;
  final schema = tables.first.schema;
  final columns = [
    for (final field in schema.fields)
      Kernels.concat([for (final t in tables) t.column(field.name)!]),
  ];
  return Table(name: tables.first.name, columns: columns, schema: schema);
}
