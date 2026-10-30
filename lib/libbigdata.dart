/// A pure Dart library for big data manipulation with DataFrame API and SQL query engine.
///
/// Similar to DuckDB, this library allows you to:
/// - Query files directly with SQL: SELECT * FROM 'data.csv'
/// - Use DataFrame API for data manipulation
/// - Read/write CSV, JSON, Parquet, Arrow IPC, Delta Lake and Apache Iceberg
library;

// Core data structures
export 'src/core/data_type.dart';
// Column: the immutable columnar types. Each public type builds through
// copying factories and reads through safe members (`buffer`/`nullBitmap` are
// unmodifiable views); its implementation, which can adopt or expose backing
// buffers, is a vector class the export hides.
export 'src/core/column.dart'
    hide
        distinctStatsCap,
        decimalStats,
        NumericVector,
        StringVector,
        BoolVector,
        ListVector;
export 'src/core/schema.dart';
export 'src/core/data_chunk.dart' hide defaultChunkSize;
export 'src/core/decimal.dart';
export 'src/core/result.dart';
export 'src/core/table.dart' hide unifyTables;

// API. `dataframe.dart` also declares the `Operation` plan nodes the DataFrame
// builds internally; they are deliberately not exported.
export 'src/api/dataframe.dart' show DataFrame, JoinType;
export 'src/api/io_extensions.dart';
export 'src/formats/arrow/arrow_writer.dart' show ArrowCompression;

// Utilities. The logging package's Level and Logger come with it, so
// configuring the library's logging needs no second import.
export 'src/util/logging_config.dart';
export 'package:logging/logging.dart' show Level, Logger, LogRecord;

// Format support
export 'src/formats/csv/csv_reader.dart' hide parseTemporal;
export 'src/formats/csv/csv_writer.dart';
export 'src/formats/csv/csv_options.dart';
export 'src/formats/json/json_reader.dart';
export 'src/formats/json/json_writer.dart';
export 'src/formats/parquet/parquet_reader.dart'
    show ParquetReader, ParquetReaderOptions;
export 'src/formats/parquet/parquet_writer.dart';
export 'src/formats/parquet/compression.dart' show ParquetCompression;
export 'src/formats/parquet/filter/row_filter.dart'
    show RowFilter, FilterOp, ColumnBounds;
export 'src/formats/dataset/dataset.dart' show Dataset;
export 'src/formats/delta/delta_table.dart' show DeltaTable;
export 'src/formats/delta/delta_writer.dart' show DeltaWriter, DeltaWriteMode;
export 'src/formats/delta/invariants.dart' show InvariantEvaluator, Invariant;
export 'src/sql/execution/sql_invariants.dart' show SqlInvariantEvaluator;
export 'src/core/local_catalog.dart' show LocalCatalog, CatalogTxn;
export 'src/formats/delta/delta_log.dart' show DeltaLog;
export 'src/formats/delta/delta_commit_coordinator.dart'
    show
        DeltaCommitCoordinator,
        FilesystemDeltaCommitCoordinator,
        FileLockDeltaCommitCoordinator,
        LocalCatalogDeltaCommitCoordinator;
export 'src/formats/iceberg/iceberg_table.dart' show IcebergTable;
export 'src/formats/iceberg/iceberg_writer.dart'
    show IcebergWriter, IcebergWriteMode;
export 'src/formats/iceberg/iceberg_catalog.dart'
    show IcebergCatalog, FilesystemIcebergCatalog, LocalCatalogIcebergCatalog;
export 'src/formats/iceberg/iceberg_backend.dart'
    show
        IcebergBackend,
        FilesystemIcebergBackend,
        FileLockIcebergBackend,
        LocalCatalogIcebergBackend;
// A commit's outcome is its Result: Kind.conflict means it definitely did not
// land (retry), Kind.indeterminate that it may have (inspect the table before
// retrying or cleaning up).
export 'src/formats/iceberg/iceberg_commit_coordinator.dart'
    show
        IcebergMetadataPublish,
        IcebergCommitCoordinator,
        FilesystemCommitCoordinator;

// SQL
export 'src/sql/execution/sql_engine.dart' show SqlEngine, SqlDataFrame, sql;
export 'src/sql/execution/engine_options.dart' show EngineOptions;
export 'src/sql/execution/memory_manager.dart' show MemoryManager;
export 'src/sql/operators/spill.dart' show SpillBudget;
