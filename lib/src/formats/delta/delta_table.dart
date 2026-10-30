import 'dart:convert';

import '../../core/partitions.dart';
import '../../core/limits.dart';
import '../../core/paths.dart';
import '../../core/row_filter.dart';
import '../../core/schema.dart';
import '../../core/table.dart';
import '../../core/columnar.dart';
import '../../util/parallel.dart';
import '../parquet/parquet_reader.dart';
import 'actions.dart';
import 'roaring_bitmap.dart';
import 'deletion_vector.dart';
import 'delta_log.dart';
import 'delta_schema.dart';
import '../../core/failure.dart';
import '../../core/result.dart';
import 'package:meta/meta.dart';

/// Reads a Delta Lake table: reconstructs a snapshot, reads its active data
/// files, applies deletion vectors, attaches partition columns, and unifies to
/// the table schema. Supports time travel by version or by `asOf` time (ms
/// since epoch), as [IcebergTable] does.
final class DeltaTable {
  final String tablePath;

  /// When false (default), embedded data/deletion-vector paths are confined to
  /// the table root: a malicious log can't point a file at `/etc/passwd`. Set
  /// true only for tables that legitimately reference an external location.
  final bool allowExternalPaths;

  DeltaTable(this.tablePath, {this.allowExternalPaths = false});

  /// Fail closed on a protocol this library can't fully honor: reading a table
  /// that requires an unsupported reader feature (column mapping, an unknown
  /// table feature, a newer reader version) with ordinary name/Parquet semantics
  /// would return plausible-but-wrong data. A null protocol is treated as the
  /// pre-table-features baseline (readable).
  void _ensureReadable(Protocol? protocol) {
    final reason = protocol?.readerUnsupported();
    if (reason != null) {
      throw Failure.of(
        Kind.unsupported,
        'Cannot read Delta table $tablePath: $reason',
      );
    }
  }

  /// Read the whole table. [concurrency] > 1 reads the data files with that many
  /// reads in flight (order preserved), overlapping per-file I/O latency:
  /// worthwhile for partition-heavy tables over network/object storage. Since the
  /// eager read already materializes every file, the extra peak memory is bounded
  /// by [concurrency] in-flight decodes. The default (1) is sequential.
  ///
  /// Per-file Parquet caps don't bound a table made of many files, so the total
  /// materialized rows/bytes are capped here too (the sequential path stops at
  /// the cap; the concurrent path checks after each gather). Pass `maxRows: null`
  /// / `maxBytes: null` to opt out, or [readBatches] for bounded-memory streaming.
  ///
  /// A failure is reported as an [Err]: [Kind.missing] for a path with no
  /// Delta log or an unknown version, [Kind.corrupt] for a gapped, truncated or
  /// incomplete log or an unreadable data file, [Kind.unsupported] for a reader
  /// feature this library does not implement, [Kind.denied] for a data or
  /// deletion-vector path outside the table, and [Kind.limit] past a cap.
  @useResult
  Future<Result<Table, ErrorDetails>> read({
    int? version,
    int? asOf,
    int concurrency = 1,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => capture(
    () => _read(
      version: version,
      timestamp: asOf,
      concurrency: concurrency,
      maxRows: maxRows,
      maxBytes: maxBytes,
    ),
    fallback: Kind.corrupt,
  );

  Future<Table> _read({
    int? version,
    int? timestamp,
    required int concurrency,
    int? maxRows,
    int? maxBytes,
  }) async {
    final snapshot = await DeltaLog(
      tablePath,
    ).snapshot(version: version, timestamp: timestamp);
    _ensureReadable(snapshot.protocol);
    final schema = DeltaSchema.fromJson(snapshot.metadata.schemaString);
    if (snapshot.files.isEmpty) return Table.empty('delta', schema);

    final tables = <Table>[];
    var rows = 0;
    var bytes = 0;
    void charge(Table table) {
      rows += table.rowCount;
      bytes += table.memoryUsage;
      enforceMaterializationCaps(
        rows,
        bytes,
        maxRows,
        maxBytes,
        'Delta result',
      );
      tables.add(table);
    }

    if (concurrency > 1) {
      // Charge in the per-completion hook so an over-cap load aborts EARLY
      // (after ~concurrency in-flight reads) instead of materializing every file
      // first; the returned list is input-ordered, so row order is preserved.
      var rows = 0;
      var bytes = 0;
      void check(Table table) {
        rows += table.rowCount;
        bytes += table.memoryUsage;
        enforceMaterializationCaps(
          rows,
          bytes,
          maxRows,
          maxBytes,
          'Delta result',
        );
      }

      final ordered = await parallelMap(
        snapshot.files,
        (add) => _readFile(add, schema, maxRows: maxRows, maxBytes: maxBytes),
        concurrency: concurrency,
        onEach: check,
      );
      return _unify(ordered, schema);
    }
    for (final add in snapshot.files) {
      charge(
        await _readFile(add, schema, maxRows: maxRows, maxBytes: maxBytes),
      );
    }
    return _unify(tables, schema);
  }

  /// The table's schema, from its log alone (no data file is read).
  ///
  /// A failure is reported as an [Err], as [read] reports it.
  @useResult
  Future<Result<Schema, ErrorDetails>> schema({int? version, int? asOf}) =>
      capture(
        () async => (await _head(version, asOf)).$1,
        fallback: Kind.corrupt,
      );

  /// The schema of the table (or of a time-travel snapshot) and the number
  /// of the version it belongs to, from its log alone, reported as [schema]
  /// reports it: a lazy reader reads that version at every terminal.
  @internal
  @useResult
  Future<Result<(Schema, int), ErrorDetails>> head({int? version, int? asOf}) =>
      capture(() => _head(version, asOf), fallback: Kind.corrupt);

  Future<(Schema, int)> _head(int? version, int? asOf) async {
    final snapshot = await DeltaLog(
      tablePath,
    ).snapshot(version: version, timestamp: asOf);
    _ensureReadable(snapshot.protocol);
    return (
      DeltaSchema.fromJson(snapshot.metadata.schemaString),
      snapshot.version,
    );
  }

  /// Stream the table one row-group batch at a time (each conformed to the table
  /// schema), so peak memory is a single batch plus the file's deletion index,
  /// not a whole data file. The bounded-memory alternative to [read].
  ///
  /// A [filter] skips each data file whose partition values or `add.stats`
  /// bounds prove it holds no matching row, and prunes row groups within the
  /// rest. It is a pruning hint, as for Parquet: kept batches may still hold
  /// rows that do not match, so the caller filters exactly.
  ///
  /// [columns] limits each batch to those table columns, in table order, and
  /// reads only them from each data file; a partition column comes from the
  /// file's partition values.
  ///
  /// Each batch arrives as an [Ok]; a failure as one [Err], reported as [read]
  /// reports it, which ends the stream.
  /// A [batchSize] below 1 is a programming error and throws at the call.
  Stream<Result<Table, ErrorDetails>> readBatches({
    int? version,
    int? asOf,
    int batchSize = 10000,
    RowFilter? filter,
    List<String>? columns,
  }) {
    if (batchSize <= 0) {
      throw ArgumentError.value(batchSize, 'batchSize', 'must be > 0');
    }
    return captureStream(
      () => _batches(
        version: version,
        timestamp: asOf,
        size: batchSize,
        filter: filter,
        columns: columns,
      ),
      fallback: Kind.corrupt,
    );
  }

  Stream<Table> _batches({
    int? version,
    int? timestamp,
    required int size,
    RowFilter? filter,
    List<String>? columns,
  }) async* {
    final snapshot = await DeltaLog(
      tablePath,
    ).snapshot(version: version, timestamp: timestamp);
    _ensureReadable(snapshot.protocol);
    final table = DeltaSchema.fromJson(snapshot.metadata.schemaString);
    _known(table, columns);
    final schema = columns == null
        ? table
        : Schema(
            fields: [
              for (final field in table.fields)
                if (columns.contains(field.name)) field,
            ],
          );
    for (final add in snapshot.files) {
      if (filter != null && !_mayMatch(add, filter, table)) continue;
      // The deletion vector is positional over the WHOLE file, so track a running
      // row offset and drop deleted positions per batch (mirrors the Iceberg
      // scan). The index itself is bounded by the delete count, not the file.
      final deleted = add.deletionVector == null
          ? RoaringBitmap()
          : await DeletionVector.load(
              add.deletionVector!,
              tablePath,
              allowExternalPaths: allowExternalPaths,
            );
      var base = 0;
      final file = _resolve(add.path);
      final probe = columns == null ? null : ParquetReader(file);
      final names = probe == null
          ? null
          : await projected(
              probe,
              (fields) => [
                for (final field in fields)
                  if (columns!.contains(field.name)) field.name,
              ],
            );
      // A deletion vector counts rows across the whole file, so row groups are
      // pruned only in a file without one; skipping a group would shift the
      // running offset the deleted positions are read against.
      await for (final batch in ParquetReader(
        file,
        options: ParquetReaderOptions(
          filter: add.deletionVector == null ? filter : null,
          columns: names,
        ),
        footer: names == null ? null : probe!.metadata,
      ).readBatches(batchSize: size).unwrap()) {
        var shaped = batch;
        if (deleted.isNotEmpty) {
          final keep = <int>[
            for (int i = 0; i < batch.rowCount; i++)
              if (!deleted.contains(base + i)) i,
          ];
          shaped = keep.length == batch.rowCount ? batch : batch.filter(keep);
        }
        base += batch.rowCount;
        if (shaped.rowCount == 0) continue;
        yield _unify([
          _appendPartitions(shaped, add.partitionValues, schema),
        ], schema);
      }
    }
  }

  /// Reject a projection naming a column [schema] lacks: dropping it silently
  /// returned fewer columns than asked for.
  void _known(Schema schema, List<String>? columns) {
    final unknown = [
      for (final name in columns ?? const <String>[])
        if (schema.field(name) == null) name,
    ];
    if (unknown.isNotEmpty) {
      throw Failure.of(
        Kind.invalid,
        'Unknown column(s) ${unknown.join(', ')} in $tablePath',
      );
    }
  }

  Future<Table> _readFile(
    AddFile add,
    Schema schema, {
    int? maxRows,
    int? maxBytes,
  }) async {
    // Forward the caller's opt-out (null) / caps to the per-file read, so a
    // DeltaTable.read(maxRows: null) isn't defeated by the Parquet per-file
    // default throwing on a single large file.
    var table = await ParquetReader(
      _resolve(add.path),
    ).read(maxRows: maxRows, maxBytes: maxBytes).unwrap();
    table = await _applyDeletionVector(table, add);
    return _appendPartitions(table, add.partitionValues, schema);
  }

  /// Whether [add] might hold a row matching [filter], judged from its
  /// partition values and its `stats` (numRecords, minValues, maxValues,
  /// nullCount). Only numeric (not decimal), string and boolean columns are
  /// judged: the log writes dates, timestamps and decimals as text whose form
  /// need not match the filter's value. Missing or unreadable stats keep the
  /// file.
  bool _mayMatch(AddFile add, RowFilter filter, Schema schema) {
    Map<String, dynamic>? stats;
    try {
      final decoded = add.stats == null ? null : jsonDecode(add.stats!);
      stats = decoded is Map<String, dynamic> ? decoded : null;
    } on FormatException {
      stats = null;
    }
    final records = stats?['numRecords'];
    final rows = records is int ? records : null;
    final bounds = <String, ColumnBounds>{};
    for (final name in filter.columns) {
      final field = schema.field(name);
      if (field == null || !ColumnBounds.judged(field.dataType)) continue;
      if (add.partitionValues.containsKey(name)) {
        final value = decodePartitionValue(
          add.partitionValues[name],
          field.dataType,
          column: name,
        );
        bounds[name] = ColumnBounds.exact(value, rows: rows);
        continue;
      }
      if (stats == null || rows == null) continue;
      final nulls = (stats['nullCount'] as Map?)?[name];
      bounds[name] = ColumnBounds(
        min: ColumnBounds.typed(
          (stats['minValues'] as Map?)?[name],
          field.dataType,
        ),
        max: ColumnBounds.typed(
          (stats['maxValues'] as Map?)?[name],
          field.dataType,
        ),
        nullCount: nulls is int ? nulls : null,
        rowCount: rows,
        hasStats: true,
      );
    }
    return filter.keep(bounds);
  }

  String _resolve(String path) =>
      resolveDataFile(tablePath, path, allowExternal: allowExternalPaths);

  /// Drop rows marked deleted by [add]'s deletion vector (merge-on-read).
  Future<Table> _applyDeletionVector(Table table, AddFile add) async {
    final vector = add.deletionVector;
    if (vector == null) return table;
    final deleted = await DeletionVector.load(
      vector,
      tablePath,
      allowExternalPaths: allowExternalPaths,
    );
    if (deleted.isEmpty) return table;
    final keep = <int>[
      for (int i = 0; i < table.rowCount; i++)
        if (!deleted.contains(i)) i,
    ];
    return table.filter(keep);
  }

  Table _appendPartitions(
    Table table,
    Map<String, String> values,
    Schema schema,
  ) {
    if (values.isEmpty) return table;
    final present = table.schema.fields.map((field) => field.name).toSet();
    final columns = [...table.columns];
    final fields = [...table.schema.fields];
    for (final field in schema.fields) {
      if (!values.containsKey(field.name) || present.contains(field.name)) {
        continue;
      }
      final value = decodePartitionValue(
        values[field.name],
        field.dataType,
        scale: field.scale ?? 0,
        column: field.name,
      );
      columns.add(
        columnFromValues(
          field.name,
          field.dataType,
          List.filled(table.rowCount, value),
        ),
      );
      fields.add(field);
    }
    return Table(
      name: table.name,
      columns: columns,
      schema: Schema(fields: fields),
    );
  }

  // Partition-value decoding lives in core/partitions.dart
  // (decodePartitionValue); ONE codec for the read and mutation paths.

  Table _unify(List<Table> tables, Schema schema) =>
      unifyTables('delta', tables, schema);
}
