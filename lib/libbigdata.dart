/// A pure Dart library for big data manipulation with DataFrame API and SQL query engine.
///
/// Similar to DuckDB, this library allows you to:
/// - Query files directly with SQL: SELECT * FROM 'data.csv'
/// - Use DataFrame API for data manipulation
/// - Read/write CSV, JSON, Parquet, Arrow IPC, Delta Lake and Apache Iceberg
library;

// Core data structures
export 'src/core/data_type.dart';
// Column: the immutable columnar types. A few members are `@internal`
// (`NumericColumn.adopt`, `rawBuffer`, `rawNullBitmap`) and expose the raw
// mutable backing storage for trusted internal hot paths — Dart cannot hide
// class members from a wholesale export, so these are the analyzer-enforced
// (advisory) boundary, NOT public API. Consumers use the safe unmodifiable
// `buffer`/`nullBitmap` getters and the copying `NumericColumn.intN`/`floatN`
// factories. Fully removing them from the surface would need a public-interface
// / internal-impl split (a deliberate follow-up).
export 'src/core/column.dart';
export 'src/core/schema.dart';
export 'src/core/data_chunk.dart';
export 'src/core/decimal.dart';
export 'src/core/table.dart';

// API. `dataframe.dart` also declares the `Operation` plan nodes the DataFrame
// builds internally; they are deliberately not exported.
export 'src/api/dataframe.dart' show DataFrame, JoinType;
export 'src/api/io_extensions.dart';
export 'src/formats/arrow/arrow_writer.dart' show ArrowCompression;

// Utilities
export 'src/util/logging_config.dart';

// Format support
export 'src/formats/csv/csv_reader.dart';
export 'src/formats/csv/csv_writer.dart';
export 'src/formats/csv/csv_options.dart';
export 'src/formats/json/json_reader.dart';
export 'src/formats/json/json_writer.dart';
export 'src/formats/parquet/parquet_reader.dart'
    show ParquetReader, ParquetReaderOptions;
export 'src/formats/parquet/parquet_writer.dart';
export 'src/formats/parquet/filter/row_filter.dart' show RowFilter, FilterOp;
export 'src/formats/dataset/dataset.dart' show Dataset;
export 'src/formats/delta/delta_table.dart' show DeltaTable;
export 'src/formats/delta/delta_writer.dart' show DeltaWriter, DeltaWriteMode;
// The commit-outcome taxonomy a writer rethrows so a caller can tell a
// definitely-failed commit (CommitConflict — retry + clean up) from one whose
// outcome is unknown (DeltaCommitIndeterminate — inspect the pointer, do NOT
// blindly retry or delete staged files). (Iceberg's pair is on its coordinator
// export below; note the base classes differ — Iceberg extends StateError,
// Delta implements Exception — but each is catchable by its concrete type.)
export 'src/formats/delta/delta_log.dart'
    show CommitConflict, DeltaCommitIndeterminate;
export 'src/formats/delta/invariants.dart' show InvariantEvaluator, Invariant;
export 'src/sql/execution/sql_invariants.dart' show SqlInvariantEvaluator;
export 'src/core/local_catalog.dart' show LocalCatalog;
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
export 'src/formats/iceberg/iceberg_commit_coordinator.dart'
    show
        IcebergCommitConflict,
        IcebergCommitIndeterminate,
        IcebergMetadataPublish;

// SQL
export 'src/sql/execution/sql_engine.dart' show SqlEngine, sql;
