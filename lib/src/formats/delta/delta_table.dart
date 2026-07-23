import '../../core/partitions.dart';
import '../../core/limits.dart';
import '../../core/paths.dart';
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

/// Reads a Delta Lake table: reconstructs a snapshot, reads its active data
/// files, applies deletion vectors, attaches partition columns, and unifies to
/// the table schema. Supports time travel by version or timestamp.
class DeltaTable {
  final String tablePath;

  /// When false (default), embedded data/deletion-vector paths are confined to
  /// the table root — a malicious log can't point a file at `/etc/passwd`. Set
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
      throw UnsupportedError('Cannot read Delta table $tablePath: $reason');
    }
  }

  /// Read the whole table. [concurrency] > 1 reads the data files with that many
  /// reads in flight (order preserved), overlapping per-file I/O latency —
  /// worthwhile for partition-heavy tables over network/object storage. Since the
  /// eager read already materializes every file, the extra peak memory is bounded
  /// by [concurrency] in-flight decodes. The default (1) is sequential.
  ///
  /// Per-file Parquet caps don't bound a table made of many files, so the total
  /// materialized rows/bytes are capped here too (the sequential path stops at
  /// the cap; the concurrent path checks after each gather). Pass `maxRows: null`
  /// / `maxBytes: null` to opt out, or [readBatches] for bounded-memory streaming.
  Future<Table> read({
    int? version,
    int? timestamp,
    int concurrency = 1,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
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

  /// Stream the table one row-group batch at a time (each conformed to the table
  /// schema), so peak memory is a single batch plus the file's deletion index —
  /// not a whole data file. The bounded-memory alternative to [read].
  Stream<Table> readBatches({
    int? version,
    int? timestamp,
    int size = 10000,
  }) async* {
    final snapshot = await DeltaLog(
      tablePath,
    ).snapshot(version: version, timestamp: timestamp);
    _ensureReadable(snapshot.protocol);
    final schema = DeltaSchema.fromJson(snapshot.metadata.schemaString);
    for (final add in snapshot.files) {
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
      await for (final batch in ParquetReader(
        _resolve(add.path),
      ).readBatches(size: size)) {
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
    ).read(maxRows: maxRows, maxBytes: maxBytes);
    table = await _applyDeletionVector(table, add);
    return _appendPartitions(table, add.partitionValues, schema);
  }

  String _resolve(String path) =>
      resolveDataFile(tablePath, path, allowExternal: allowExternalPaths);

  /// Drop rows marked deleted by [add]'s deletion vector (merge-on-read).
  Future<Table> _applyDeletionVector(Table table, AddFile add) async {
    final dv = add.deletionVector;
    if (dv == null) return table;
    final deleted = await DeletionVector.load(
      dv,
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
    final present = table.schema.fields.map((f) => f.name).toSet();
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
  // (decodePartitionValue) — ONE codec for the read and mutation paths.

  Table _unify(List<Table> tables, Schema schema) =>
      unifyTables('delta', tables, schema);
}
