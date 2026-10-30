import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:meta/meta.dart';

import '../../core/failure.dart';
import '../../core/result.dart';
import '../../core/schema.dart';
import '../../core/limits.dart';
import '../../core/table.dart';
import '../../core/data_type.dart';
import '../../core/columnar.dart' show columnFromValues;
import '../../core/conversions.dart';
import '../../core/paths.dart';
import '../../core/staging.dart';
import '../../core/stats.dart';
import '../avro/avro_ocf_reader.dart';
import '../avro/avro_ocf_writer.dart';
import '../parquet/parquet_writer.dart';
import 'iceberg_schema.dart';
import 'iceberg_backend.dart';
import 'iceberg_catalog.dart';
import 'iceberg_commit_coordinator.dart';
import 'manifest_schemas.dart';
import '../../core/ids.dart' as ids;

// Secure entropy for snapshot ids / UUIDs so concurrent writers (isolates/pods)
// can't collide via identical clock-seeded Random() in the same millisecond.
final _secure = Random.secure();

/// One identity partition field: the column, the schema field-id it reads, the
/// partition field-id the manifest projects it by, and its type.
typedef PartitionField = ({
  String name,
  int sourceId,
  int fieldId,
  DataType type,
  int? precision,
  int? scale,
});

enum IcebergWriteMode { append, overwrite }

/// Writes Apache Iceberg tables (format-version 2), optionally partitioned by
/// identity transforms (`partitionBy`): a Parquet data file per partition, an
/// Avro manifest + manifest list, and a new `metadata.json`
/// snapshot. Uses Iceberg's canonical manifest Avro schemas so the output is
/// readable by Spark / pyiceberg / DuckDB.
final class IcebergWriter {
  final String tablePath;
  final IcebergBackend _backend;

  /// The ceiling this writer refuses to publish metadata above, because the
  /// READER rejects a `metadata.json` over [maxIcebergMetadataBytes], an
  /// unchecked publish could emit a current metadata the next read (and
  /// `expireSnapshots`) can't open, bricking the table. Defaults to the read cap;
  /// injectable (tests set it low). A commit that would exceed it fails loud,
  /// leaving the last-good (readable) metadata for `expireSnapshots` to prune.
  final int _maxMetadataBytes;

  /// When false (default), every path this writer resolves from table metadata
  /// (manifests, manifest lists, data files) is CONFINED to the table root, so
  /// `expireSnapshots` cannot be tricked into reading (or DELETING) a file
  /// outside the table by a crafted/corrupt manifest that names an external
  /// absolute path. Set true only for a trusted table whose metadata
  /// legitimately references external locations.
  final bool allowExternalPaths;

  /// Pass a [backend] to plug in a real catalog compare-and-swap (REST / Glue /
  /// Hive): one object owns BOTH the current-metadata read and the commit CAS,
  /// so reads and writes can never resolve different pointers (the split-brain a
  /// separately-injected coordinator + catalog allowed).
  ///
  /// The DEFAULT is now [FileLockIcebergBackend]; it holds an exclusive OS file
  /// lock over the whole read → build → publish, giving genuine cross-PROCESS
  /// compare-and-swap (no lost update / duplicate version) on a lock-honoring
  /// filesystem. It does NOT serialize separate Dart ISOLATES (POSIX locks are
  /// per-process: use one writer per process there), and a bare
  /// [FilesystemIcebergBackend] (best-effort, unfenced) is still available for
  /// single-writer use.
  IcebergWriter(
    this.tablePath, {
    IcebergBackend? backend,
    int maxMetadataBytes = maxIcebergMetadataBytes,
    this.allowExternalPaths = false,
  }) : _backend = backend ?? FileLockIcebergBackend(),
       _maxMetadataBytes = maxMetadataBytes;

  /// Per-column manifest statistics for one data file, keyed by field-id in
  /// Iceberg's array-of-{key,value} map form: total value counts, null counts,
  /// NaN counts (float columns), and lower/upper bounds. Only TOP-LEVEL PRIMITIVE
  /// columns are described; nested leaves and the temporal/decimal/binary bounds
  /// [icebergBound] declines are omitted (spec-legal) so no external engine ever
  /// prunes on a bound this writer can't encode safely.
  Map<String, dynamic> _dataFileStats(Table data, Schema stamped) {
    final values = <Map<String, Object?>>[];
    final nulls = <Map<String, Object?>>[];
    final nans = <Map<String, Object?>>[];
    final lower = <Map<String, Object?>>[];
    final upper = <Map<String, Object?>>[];
    for (final column in data.columns) {
      final field = stamped.field(column.name);
      final id = field?.id;
      if (id == null || field!.dataType.isNested || field.children.isNotEmpty) {
        continue;
      }
      final type = field.dataType;
      final stats = StatsAccumulator()..addColumn(column);
      values.add({'key': id, 'value': stats.count});
      nulls.add({'key': id, 'value': stats.nulls});
      if (type == DataType.float32 || type == DataType.float64) {
        nans.add({'key': id, 'value': stats.nans});
      }
      final low = stats.min == null ? null : icebergBound(stats.min!, type);
      if (low != null) lower.add({'key': id, 'value': low});
      final high = stats.max == null ? null : icebergBound(stats.max!, type);
      if (high != null) upper.add({'key': id, 'value': high});
    }
    List<Map<String, Object?>>? orNull(List<Map<String, Object?>> entries) =>
        entries.isEmpty ? null : entries;
    return {
      'value_counts': orNull(values),
      'null_value_counts': orNull(nulls),
      'nan_value_counts': orNull(nans),
      'lower_bounds': orNull(lower),
      'upper_bounds': orNull(upper),
    };
  }

  /// Build the metadata publish, refusing one over [_maxMetadataBytes] BEFORE it
  /// is written: an oversized `metadata.json` would be unreadable (see the
  /// field). Uses the UTF-8 byte length to match the reader's `File.length` check.
  IcebergMetadataPublish _publish(String name, String json) {
    final bytes = utf8.encode(json).length;
    if (bytes > _maxMetadataBytes) {
      throw Failure.of(
        Kind.limit,
        'Iceberg metadata for $tablePath would be $bytes bytes, over the '
        '$_maxMetadataBytes-byte cap; run expireSnapshots to drop old snapshots '
        '(the last committed metadata is unchanged and still readable). Every '
        'commit retains all prior snapshots + the snapshot log.',
      );
    }
    return IcebergMetadataPublish(name, json);
  }

  /// [maxPartitions] bounds how many distinct partitions this write may create
  /// (default [defaultMaxPartitionsPerWrite]); `null` opts out. See that
  /// constant for why an unbounded partitioned write is a trap.
  ///
  /// Returns the committed version, or an [Err]: [Kind.schema] for an append
  /// whose schema differs from the table's, [Kind.conflict] when another writer
  /// won or changed the schema or partition spec concurrently,
  /// [Kind.indeterminate] when the commit may have landed (staged files are
  /// kept), [Kind.limit] for the metadata or partition cap, and [Kind.io] when
  /// staged files could not be cleaned up after a failure (its cause is that
  /// failure).
  @useResult
  Future<Result<int, ErrorDetails>> write(
    Table table, {
    IcebergWriteMode mode = IcebergWriteMode.append,
    List<String>? partitionBy,
    int? maxPartitions = defaultMaxPartitionsPerWrite,
  }) => guard(
    () => _write(
      _widen(table),
      mode: mode,
      partitionBy: partitionBy,
      maxPartitions: maxPartitions,
    ),
  );

  /// [table] with int8 and int16 columns widened to int32. Iceberg's smallest
  /// integer is `int`, so the schema already declares them as such; data files
  /// left narrow then failed the read's column validation.
  static Table _widen(Table table) {
    final narrow = {DataType.int8, DataType.int16};
    if (!table.schema.fields.any((field) => narrow.contains(field.dataType))) {
      return table;
    }
    return Table(
      name: table.name,
      schema: Schema(
        fields: [
          for (final field in table.schema.fields)
            narrow.contains(field.dataType)
                ? field.copyWith(dataType: DataType.int32)
                : field,
        ],
      ),
      columns: [
        for (final column in table.columns)
          narrow.contains(column.dataType)
              ? column.castTo(DataType.int32)
              : column,
      ],
    );
  }

  Future<Result<int, ErrorDetails>> _write(
    Table table, {
    required IcebergWriteMode mode,
    required List<String>? partitionBy,
    required int? maxPartitions,
  }) async {
    final columns = partitionBy ?? const <String>[];
    final root = Directory(tablePath).absolute.path;
    await Directory('$root/data').create(recursive: true);
    await Directory('$root/metadata').create(recursive: true);

    // Reject an incompatible append BEFORE writing anything: appending a
    // different schema would silently rebind the surviving old data files to a
    // new schema (and drop schema history). Only identical-schema appends are
    // supported (schema evolution / safe widening is out of scope).
    // The current metadata (if the table exists): used to REUSE existing field
    // ids on append (Iceberg projects by id) and to validate schema compatibility.
    final existing = await _backend.current(root).unwrap();
    if (mode == IcebergWriteMode.append &&
        existing != null &&
        !_schemaMatches(existing, table.schema)) {
      throw Failure.of(
        Kind.schema,
        'Iceberg append schema mismatch on $tablePath: the incoming schema '
        'differs from the existing table schema (only identical-schema '
        'appends are supported; use overwrite to replace the schema)',
      );
    }
    // Reuse the table's existing field-ids so the new Parquet's field-ids match
    // the metadata (external engines bind columns by id). An overwrite keeps
    // them for unchanged columns and numbers new ones above last-column-id.
    // Computed once and shared with the metadata so the two always agree.
    final reuseIds = existing == null
        ? null
        : mode == IcebergWriteMode.append
        ? IcebergSchema.idMap(currentSchema(existing, tablePath))
        : IcebergSchema.reuseFor(
            currentSchema(existing, tablePath),
            table.schema,
          );
    final lastId = (existing?['last-column-id'] as int?) ?? 0;

    final commit = ids.uuid();
    // Integer-only composition for a high-entropy ~62-bit id: nextDouble()*2^62
    // would keep only the double's 53-bit mantissa (and overflows on JS). Secure
    // entropy so concurrent writers don't generate the same snapshot id.
    final snapshotId =
        (_secure.nextInt(1 << 31) << 31) | _secure.nextInt(1 << 31);
    final now = DateTime.now().millisecondsSinceEpoch;

    final dataName = '00000-0-$commit.parquet';
    final manifestName = '$commit-m0.avro';
    final manifestPath = '$root/metadata/$manifestName';
    // The manifest-list path becomes known inside the commit loop; tracked here
    // so a terminal failure can clean it up too.
    String? manifestListPath;
    // Every data file written this transaction (one per partition), so a
    // terminal failure cleans up ALL of them, not just partition 0.
    final written = <String>[];

    // The data file, manifest, and manifest-list are written before the commit
    // is published. If the commit fails terminally (contention) or anything
    // throws afterward, none of them are referenced by a committed metadata.json:
    // delete them so they don't orphan on disk.
    List<String> staged() => [
      ...written, // every partition data file, not just partition 0
      manifestPath,
      if (manifestListPath != null) manifestListPath!,
    ];
    final Result<int, ErrorDetails> outcome;
    try {
      // 1. data file(s): tag the schema with Iceberg field-ids (matching the
      // metadata schema, nested types included) so the Parquet carries
      // file-level field-ids, enabling true Spark-Iceberg schema evolution /
      // column reordering. Unlike Delta, Iceberg KEEPS the partition columns in
      // the data files; the partition tuple is also recorded in the manifest.
      final stamped = IcebergSchema.build(
        table.schema,
        reuseIds: reuseIds,
        lastId: lastId,
      ).idSchema;
      // Partition spec: identity transforms over the named columns. The
      // field-ids are ASSIGNED by _resolveSpec, not hardcoded to 1000+i; they
      // are baked into the data manifest's Avro schema written below, so the
      // spec has to be resolved before that. This uses the pre-read `existing`;
      // the commit loop re-validates it against the version it actually claims.
      final partitions = [
        for (final column in columns)
          (
            name: column,
            sourceId: stamped.fields
                .firstWhere((field) => field.name == column)
                .id!,
            type: table.schema.field(column)!.dataType,
            precision: table.schema.field(column)!.precision,
            scale: table.schema.field(column)!.scale,
          ),
      ];
      final resolved = _resolveSpec(existing, partitions);
      final fields = resolved.fields;

      // The partition cap is enforced INSIDE splitByKeys as each new key is
      // discovered, so a high-cardinality blow-up fails loud before the whole
      // O(rows) grouping map is built (not after, once every partition is
      // already grouped).
      final parts = columns.isEmpty
          ? [(values: const <String, Object?>{}, table: table)]
          : table.splitByKeys(columns, maxPartitions: maxPartitions);

      final entries = <Map<String, dynamic>>[];
      final values = <Map<String, Object?>>[];
      var rows = 0;
      // Enumerate lazily (splitByKeys yields one partition copy at a time): no
      // index/length access, so all partition tables are never held at once.
      var index = -1;
      for (final part in parts) {
        index++;
        final name = columns.isEmpty
            ? dataName
            : '00000-$index-$commit.parquet';
        final path = '$root/data/$name';
        await ParquetWriter(path)
            .write(
              Table(
                name: table.name,
                columns: part.table.columns,
                schema: stamped,
              ),
            )
            .unwrap();
        written.add(path); // track EVERY partition file for cleanup
        values.add(part.values);
        final size = await File(path).length();
        rows += part.table.rowCount;
        final stats = _dataFileStats(part.table, stamped);
        entries.add({
          'status': 1, // ADDED
          'snapshot_id': snapshotId,
          'sequence_number': null,
          'file_sequence_number': null,
          'data_file': {
            'content': 0,
            'file_path': 'file://$path',
            'file_format': 'PARQUET',
            'partition': {
              for (final field in fields)
                field.name: _partValue(part.values[field.name], field.type),
            },
            'record_count': part.table.rowCount,
            'file_size_in_bytes': size,
            'column_sizes': null, // per-column on-disk size not tracked
            'value_counts': stats['value_counts'],
            'null_value_counts': stats['null_value_counts'],
            'nan_value_counts': stats['nan_value_counts'],
            'lower_bounds': stats['lower_bounds'],
            'upper_bounds': stats['upper_bounds'],
            'key_metadata': null,
            'split_offsets': null,
            'equality_ids': null,
            'sort_order_id': null,
          },
        });
      }

      // The manifest list summarizes each partition field over this manifest's
      // files, so a reader can skip the manifest without opening it.
      final summaries = fields.isEmpty
          ? null
          : [
              for (final field in fields)
                _summary(field.type, [
                  for (final value in values) value[field.name],
                ]),
            ];

      // 2. manifest, when partitioned, inject the partition record fields into
      // the manifest entry's `data_file.partition` Avro schema.
      final schema = fields.isEmpty
          ? manifestEntrySchema
          : _partitionManifestSchema(fields);
      await AvroOcfWriter.writePathAsync(manifestPath, schema, entries);
      final length = await File(manifestPath).length();

      // 3+4. Publish the manifest list + metadata.json against the freshly-read
      // previous metadata, atomically installing the new current-metadata
      // pointer. The [IcebergCommitCoordinator] owns the version claim / CAS: the
      // default FilesystemIcebergBackend uses an O_EXCL `vNNNNN.commit` marker
      // with lease-reclaim (a catalog-backed backend can supply true
      // cross-engine CAS). On a bare filesystem this narrows but cannot fully
      // close the window: real cross-engine CAS requires a catalog service. The
      // backend reads the current pointer through its own `current`, so the CAS
      // can't diverge from the read.
      outcome = await _backend.commit(
        root: root,
        build: (previous, version) async {
          // Re-validate the append schema against the version we actually
          // claimed: the pre-check used a possibly-stale read, and a concurrent
          // schema change could have landed since. Without this, an append could
          // publish old-schema data files under new metadata (the read path
          // assembles by position, so a mismatch silently corrupts).
          if (mode == IcebergWriteMode.append &&
              previous != null &&
              !_schemaMatches(previous, table.schema)) {
            throw Failure.of(
              Kind.conflict,
              'Iceberg append schema mismatch on $tablePath: the table schema '
              'changed concurrently; retry the write against the new schema',
            );
          }
          // The manifest's partition record was written with the pre-read's
          // field-ids and spec-id. If a concurrent writer registered a spec
          // since, republishing under a different id would make readers project
          // that record by the WRONG field-ids. Fail loud; the write is retried.
          final claimed = _resolveSpec(previous, partitions);
          if (claimed.id != resolved.id ||
              !_sameFieldIds(claimed.fields, resolved.fields)) {
            throw Failure.of(
              Kind.conflict,
              'Iceberg partition spec changed concurrently on $tablePath '
              '(resolved spec ${resolved.id}, now ${claimed.id}); retry the '
              'write',
            );
          }
          final sequence =
              ((previous?['last-sequence-number'] as int?) ?? 0) + 1;
          final records = <Map<String, dynamic>>[
            _manifestListEntry(
              'file://$root/metadata/$manifestName',
              length,
              snapshotId,
              sequence,
              rows,
              entries.length,
              specId: claimed.id,
              partitions: summaries,
            ),
          ];
          if (mode == IcebergWriteMode.append && previous != null) {
            records.addAll(await _priorManifestEntries(root, previous));
          }
          final manifestListName = 'snap-$snapshotId-0-$commit.avro';
          final listPath = '$root/metadata/$manifestListName';
          manifestListPath =
              listPath; // record for the outer cleanup on failure
          await AvroOcfWriter.writePathAsync(
            listPath,
            manifestListSchema,
            records,
          );

          final name =
              '${version.toString().padLeft(5, '0')}-$commit.metadata.json';
          final metadata = _buildMetadata(
            root: root,
            previous: previous,
            table: table,
            snapshotId: snapshotId,
            sequence: sequence,
            now: now,
            manifestList: 'file://$listPath',
            mode: mode,
            spec: claimed,
            reuseIds: reuseIds,
            lastId: lastId,
          );
          return _publish(name, jsonEncode(metadata));
        },
      );
    } on IcebergCommitIndeterminate {
      // As below, from a backend that throws rather than returns it.
      rethrow;
    } catch (error) {
      // A pre-publish staging error: none of these are referenced by a
      // committed metadata.json, so remove them.
      await _cleanup(error, staged());
      rethrow;
    }
    // An indeterminate commit (e.g. a catalog CAS whose response was lost to a
    // timeout/reset) may already reference the staged data files, so deleting
    // them would destroy a committed snapshot: RETAIN everything and surface
    // the ambiguity: resolving the current pointer is the caller's job. Any
    // other failure is known not to have published.
    if (outcome case Err(:final error) when error.kind != Kind.indeterminate) {
      await _cleanup(error, staged());
    }
    return outcome;
  }

  /// Drop old snapshots from the table's history and delete the files only they
  /// reached, returning how many snapshots were expired. This is the operation
  /// [maxIcebergMetadataBytes]'s error message tells users to run: an appending
  /// table's `snapshots` array grows forever, and every commit rewrites the
  /// whole of it.
  ///
  /// A snapshot expires only if it is BOTH outside the newest [retain] AND, when
  /// [before] is given, older than it: [retain] is a floor that protects recent
  /// history regardless of age, matching Iceberg's `expireOlderThan` +
  /// `retainLast`. The current snapshot is always retained, so [retain] must be
  /// at least 1. Expiry drops HISTORY, not data: the current snapshot still
  /// reads every row it did before.
  ///
  /// This DELETES files, so it is deliberately conservative:
  /// - The pruned metadata is published FIRST and files are deleted only after
  ///   it lands. A crash in between leaves unreferenced files (recoverable):
  ///   the reverse order would leave live metadata pointing at deleted files.
  /// - A file reachable from ANY retained snapshot is never deleted, even if an
  ///   expired snapshot reaches it too (an append carries prior manifests
  ///   forward, so this overlap is the normal case, not an edge one).
  /// - A manifest that cannot be read is a reachability the traversal cannot
  ///   prove, so it aborts rather than delete on an incomplete picture.
  /// [allowOversizedMetadata] bypasses the [maxIcebergMetadataBytes] read cap so
  /// a table ALREADY bricked with oversized metadata (e.g. written by another
  /// tool before the publish cap existed) can be read here and pruned back under
  /// the cap: a recovery escape hatch, not for normal use.
  ///
  /// Returns [Kind.missing] when there is no table, [Kind.conflict] when the
  /// commit loses, [Kind.corrupt] when a retained snapshot's files cannot be
  /// traced, and [Kind.io] when expired files could not be deleted (the expiry
  /// itself has committed). A [retain] below 1 is a programming error and
  /// throws.
  @useResult
  Future<Result<int, ErrorDetails>> expireSnapshots({
    int retain = 1,
    DateTime? before,
    bool allowOversizedMetadata = false,
  }) {
    if (retain < 1) {
      throw ArgumentError.value(
        retain,
        'retain',
        'must be at least 1 (the current snapshot can never be expired)',
      );
    }
    return guard(() => _expire(retain, before, allowOversizedMetadata));
  }

  Future<Result<int, ErrorDetails>> _expire(
    int retain,
    DateTime? before,
    bool allowOversizedMetadata,
  ) async {
    final root = Directory(tablePath).absolute.path;
    final existing = await _backend
        .current(root, allowOversized: allowOversizedMetadata)
        .unwrap();
    if (existing == null) {
      throw Failure.of(
        Kind.missing,
        'Cannot expire snapshots: no Iceberg table at $tablePath',
      );
    }

    // Nothing to expire: return WITHOUT committing. `_survivors` is pure, so this
    // pre-check matches the commit's re-derivation: a genuine no-op must not
    // publish a new metadata version (unbounded churn on scheduled maintenance)
    // nor leak the internal `_version` into the published JSON.
    final present = ((existing['snapshots'] as List?) ?? const [])
        .cast<Map<String, dynamic>>();
    if (present.every(_survivors(existing, present, retain, before).contains)) {
      return const Ok(0);
    }

    // Decide what goes BEFORE touching anything, from the metadata we publish
    // against: the commit below re-derives it against the version it claims.
    var expired = <Map<String, dynamic>>[];
    var doomed = <String>{};

    final outcome = await _backend.commit(
      root: root,
      allowOversized: allowOversizedMetadata,
      build: (previous, version) async {
        if (previous == null) {
          throw Failure.of(
            Kind.conflict,
            'Cannot expire snapshots: the Iceberg table at $tablePath '
            'disappeared mid-commit',
          );
        }
        final all = ((previous['snapshots'] as List?) ?? const [])
            .cast<Map<String, dynamic>>();
        final keep = _survivors(previous, all, retain, before);
        expired = [
          for (final snapshot in all)
            if (!keep.contains(snapshot)) snapshot,
        ];
        if (expired.isEmpty) {
          // A concurrent writer expired these between the pre-check and this
          // claim: publish the current metadata unchanged, but never leak the
          // internal `_version` (the real-expiry branch strips it below too).
          doomed = {};
          return _publish(
            _metadataName(version),
            jsonEncode(_published(previous)),
          );
        }
        // Reachability, computed while both sets still exist on disk. Schemas
        // are NOT pruned alongside the snapshots: a retained snapshot binds by
        // its own schema-id, and the schema list grows only per overwrite.
        // One manifest-reachability cache across BOTH loops: a manifest carried
        // forward by many snapshots is read once, not once per snapshot.
        final cache = <String, Set<String>>{};
        final live = <String>{};
        for (final snapshot in keep) {
          live.addAll(await _reachable(root, snapshot, cache));
        }
        final dead = <String>{};
        for (final snapshot in expired) {
          dead.addAll(await _reachable(root, snapshot, cache));
        }
        doomed = dead.difference(live);

        final pruned = _published(previous);
        pruned['snapshots'] = keep;
        pruned['metadata-log'] = _metadataLog(previous);
        final retained = {for (final snapshot in keep) snapshot['snapshot-id']};
        if (previous['snapshot-log'] is List) {
          pruned['snapshot-log'] = [
            for (final entry in previous['snapshot-log'] as List)
              if (retained.contains((entry as Map)['snapshot-id'])) entry,
          ];
        }
        return _publish(_metadataName(version), jsonEncode(pruned));
      },
    );
    if (outcome case Err(:final error)) return Err(error);

    // The pruned metadata is live; nothing reaches these files any more. A
    // delete failure is collected and thrown (like the staging cleanup) rather
    // than silently orphaning: the expiry itself has already committed.
    if (doomed.isNotEmpty) {
      final failed = await deleteStaged(doomed);
      if (failed.isNotEmpty) {
        throw FileSystemException(
          'Expired ${expired.length} snapshot(s) on $tablePath, but ${failed.length} '
          'of their files could not be deleted and are now orphaned: '
          '${failed.take(5).join(', ')}',
          tablePath,
        );
      }
    }
    return Ok(expired.length);
  }

  String _metadataName(int version) =>
      '${version.toString().padLeft(5, '0')}-${ids.uuid()}.metadata.json';

  /// The snapshots [retain]/[before] protect: the newest [retain] by position
  /// (the array is append-ordered), anything not older than [before], and the
  /// current snapshot, which is retained even if it satisfies neither, since
  /// expiring it would leave the table pointing at nothing. Every snapshot a
  /// ref names is retained too: a tag's snapshot, and a branch's head plus
  /// ancestors up to its `min-snapshots-to-keep` (default 1), so expiry never
  /// leaves a tag or branch pointing at a deleted snapshot.
  List<Map<String, dynamic>> _survivors(
    Map<String, dynamic> metadata,
    List<Map<String, dynamic>> all,
    int retain,
    DateTime? before,
  ) {
    // "Newest N" ranks by timestamp-ms, NOT array position: the spec does not
    // guarantee the snapshots array is chronological (an externally-written or
    // branched table may append out of order), and expiring by position could
    // drop a genuinely recent snapshot. Identical timestamps keep array order
    // (explicit index tie-break: List.sort is not stable), and a missing
    // timestamp ranks oldest.
    int stamp(Map<String, dynamic> snapshot) =>
        (snapshot['timestamp-ms'] as int?) ?? 0;
    final indexed = [for (int i = 0; i < all.length; i++) (i, all[i])];
    indexed.sort((a, b) {
      final order = stamp(a.$2).compareTo(stamp(b.$2));
      return order != 0 ? order : a.$1.compareTo(b.$1);
    });
    final ranked = [for (final entry in indexed) entry.$2];
    final newest = ranked.length <= retain
        ? ranked
        : ranked.sublist(ranked.length - retain);
    final byId = {
      for (final snapshot in all) snapshot['snapshot-id']: snapshot,
    };
    final named = <Object?>{};
    if (metadata['refs'] case final Map refs) {
      for (final ref in refs.values.whereType<Map>()) {
        final depth = ref['type'] == 'branch'
            ? (ref['min-snapshots-to-keep'] as int?) ?? 1
            : 1;
        var id = ref['snapshot-id'];
        for (var n = 0; n < depth && byId.containsKey(id); n++) {
          named.add(id);
          id = byId[id]!['parent-snapshot-id'];
        }
      }
    }
    final cutoff = before?.millisecondsSinceEpoch;
    final current = metadata['current-snapshot-id'];
    return [
      for (final snapshot in all)
        if (newest.contains(snapshot) ||
            named.contains(snapshot['snapshot-id']) ||
            (cutoff != null &&
                ((snapshot['timestamp-ms'] as int?) ?? 0) >= cutoff) ||
            (current != null &&
                current != -1 &&
                snapshot['snapshot-id'] == current))
          snapshot,
    ];
  }

  /// Every file [snapshot] reaches: its manifest list, that list's manifests,
  /// and every data/delete file those manifests name, INCLUDING entries marked
  /// DELETED, which the read path skips but an older retained snapshot may still
  /// count as live. Over-collecting here only ever spares a file.
  Future<Set<String>> _reachable(
    String root,
    Map<String, dynamic> snapshot,
    Map<String, Set<String>> cache,
  ) async {
    final reached = <String>{};
    final list = snapshot['manifest-list'];
    if (list is! String) return reached;
    final listPath = await _resolve(root, list);
    if (!await File(listPath).exists()) {
      // Deleting on an unprovable reachability is how a vacuum destroys a live
      // table. A snapshot in the metadata whose manifest list is gone is already
      // corrupt (the read path says so too): surface it, don't work around it.
      throw FormatException(
        'Iceberg snapshot ${snapshot['snapshot-id']} references manifest list '
        '$list but that file is missing, so the files it reaches cannot be '
        'determined (corrupt metadata) in $root',
      );
    }
    reached.add(listPath);
    for (final entry in await AvroOcfReader.readPathAsync(listPath)) {
      final path = await _resolve(root, entry['manifest_path'] as String);
      reached.addAll(await _manifestReached(root, path, cache));
    }
    return reached;
  }

  /// The files a SINGLE manifest reaches (itself + every data/delete file it
  /// names), memoized by manifest path in [cache]. Appends carry prior manifests
  /// forward, so the same manifest is referenced by many snapshots: caching
  /// makes `expireSnapshots` read each manifest Avro once instead of once per
  /// snapshot (it was quadratic, and could OOM on the very oversized table the
  /// operation exists to repair).
  Future<Set<String>> _manifestReached(
    String root,
    String path,
    Map<String, Set<String>> cache,
  ) async {
    final cached = cache[path];
    if (cached != null) return cached;
    if (!await File(path).exists()) {
      // Fail CLOSED, exactly as the missing manifest-list case: a manifest we
      // can't read is a reachability we can't PROVE, so skipping it would
      // under-count `live` and let expiry delete a file the retained snapshots
      // still reference. A missing manifest is corrupt metadata.
      throw FormatException(
        'Iceberg manifest list references manifest $path but that '
        'file is missing, so the files it reaches cannot be determined '
        '(corrupt metadata) in $root',
      );
    }
    final files = <String>{path};
    for (final record in await AvroOcfReader.readPathAsync(path)) {
      final file = (record['data_file'] as Map)['file_path'];
      if (file is String) files.add(await _resolve(root, file));
    }
    cache[path] = files;
    return files;
  }

  /// Remove the files this transaction staged after a terminal failure [cause]
  /// (none are referenced by a committed metadata.json). Unlike Delta's
  /// best-effort cleanup, this COLLECTS delete failures and throws a [Kind.io]
  /// failure whose cause is [cause], so a half-cleaned commit is observable
  /// instead of silently orphaning files. Returns normally when every file is
  /// removed, so the caller reports [cause].
  Future<void> _cleanup(Object cause, Iterable<String> paths) async {
    final failed = await deleteStaged(paths);
    if (failed.isNotEmpty) {
      throw Failure.of(
        Kind.io,
        'Iceberg commit failed ($cause); additionally, cleanup could not remove '
        'staged files: ${failed.join('; ')}',
        cause,
      );
    }
  }

  Map<String, dynamic> _manifestListEntry(
    String path,
    int length,
    int snapshotId,
    int sequence,
    int rows,
    int files, {
    required int specId,
    List<Map<String, Object?>>? partitions,
  }) => {
    'manifest_path': path,
    'manifest_length': length,
    // The spec this manifest's partition records were actually written under.
    // Hardcoding 0 made readers resolve the WRONG spec the moment a table's
    // partitioning changed; they look the spec up by this id and project
    // `data_file.partition` by that spec's field-ids.
    'partition_spec_id': specId,
    'content': 0,
    'sequence_number': sequence,
    'min_sequence_number': sequence,
    'added_snapshot_id': snapshotId,
    'added_files_count': files,
    'existing_files_count': 0,
    'deleted_files_count': 0,
    'added_rows_count': rows,
    'existing_rows_count': 0,
    'deleted_rows_count': 0,
    'partitions': partitions,
    'key_metadata': null,
  };

  /// The manifest-list summary of one identity partition field of [type] over
  /// the partition [values] of a manifest's files: whether any is null or NaN,
  /// and the lower and upper bounds in Iceberg's single-value form (left out
  /// for the types [icebergBound] declines).
  static Map<String, Object?> _summary(DataType type, List<Object?> values) {
    final stats = StatsAccumulator()
      ..addColumn(columnFromValues('p', type, values));
    final float = type == DataType.float32 || type == DataType.float64;
    return {
      'contains_null': stats.nulls > 0,
      'contains_nan': float ? stats.nans > 0 : null,
      'lower_bound': stats.min == null ? null : icebergBound(stats.min!, type),
      'upper_bound': stats.max == null ? null : icebergBound(stats.max!, type),
    };
  }

  /// The identity partition value for a column of [type], converted to match the
  /// Avro type [_avroPartType] declares. Null partition values stay null.
  static Object? _partValue(Object? value, DataType type) {
    if (value == null) return null;
    return switch (type) {
      DataType.date when value is DateTime => dateToEpochDays(value),
      DataType.timestamp when value is DateTime => timestampToEpochMicros(
        value,
      ),
      // Kept as the UNSCALED BigInt: the Avro encoder writes a decimal logical
      // type as big-endian two's-complement. toString() emitted the digits as
      // TEXT under an Avro `string`, which a reader's string<->bytes promotion
      // then decodes as unscaled bytes: garbage.
      _ =>
        value, // numeric/boolean/string/decimal pass through; binary stays bytes
    };
  }

  /// Avro type for an identity partition column (nullable union, default null).
  static Object _avroPartType(DataType type, {int? precision, int? scale}) {
    final avro = switch (type) {
      DataType.decimal => {
        'type': 'bytes',
        'logicalType': 'decimal',
        // Iceberg requires data_file.partition's field types to be the spec's
        // RESULT types, so a decimal must carry its precision/scale.
        'precision': precision ?? 38,
        'scale': scale ?? 0,
      },
      DataType.int8 || DataType.int16 || DataType.int32 => 'int',
      DataType.int64 => 'long',
      DataType.float32 => 'float',
      DataType.float64 => 'double',
      DataType.boolean => 'boolean',
      DataType.date => {'type': 'int', 'logicalType': 'date'},
      DataType.timestamp => {'type': 'long', 'logicalType': 'timestamp-micros'},
      DataType.binary => 'bytes',
      _ => 'string',
    };
    return ['null', avro];
  }

  /// Build a manifest-entry Avro schema whose `data_file.partition` record holds
  /// the partition fields (Iceberg requires the partition tuple typed by spec).
  static String _partitionManifestSchema(List<PartitionField> spec) {
    final schema = jsonDecode(manifestEntrySchema) as Map<String, dynamic>;
    Map<String, dynamic> recordOf(dynamic type) => type is List
        ? type.firstWhere(
                (branch) => branch is Map && branch['type'] == 'record',
              )
              as Map<String, dynamic>
        : type as Map<String, dynamic>;
    final dataFile = recordOf(
      (schema['fields'] as List).firstWhere(
        (field) => field['name'] == 'data_file',
      )['type'],
    );
    final partition = recordOf(
      (dataFile['fields'] as List).firstWhere(
        (field) => field['name'] == 'partition',
      )['type'],
    );
    partition['fields'] = [
      for (final field in spec)
        {
          'name': field.name,
          'field-id': field.fieldId,
          'type': _avroPartType(
            field.type,
            precision: field.precision,
            scale: field.scale,
          ),
          'default': null,
        },
    ];
    return jsonEncode(schema);
  }

  Future<List<Map<String, dynamic>>> _priorManifestEntries(
    String root,
    Map<String, dynamic> previous,
  ) async {
    final snapshot = currentSnapshot(previous, root);
    final path = await _resolve(root, snapshot['manifest-list'] as String);
    // A present snapshot whose manifest list is missing is CORRUPTION, not
    // "no priors": returning [] here would build an append-only snapshot that
    // silently drops every prior data file. Fail closed so the append aborts and
    // the prior state is preserved.
    if (!await File(path).exists()) {
      throw FormatException(
        'Iceberg snapshot ${snapshot['snapshot-id']} references manifest list '
        '${snapshot['manifest-list']} but that file is missing (corrupt metadata) '
        'in $root',
      );
    }
    // Re-emit each prior manifest_list record unchanged (their files persist).
    return AvroOcfReader.readPathAsync(path);
  }

  /// The partition spec this write commits under, resolved against [previous]'s
  /// metadata: the spec-id its manifests must declare, the fields WITH their
  /// field-ids, the full `partition-specs` array to publish, and the new
  /// `last-partition-id` high-water mark.
  ///
  /// Iceberg matches a spec by (source-id, transform, name), NOT by field-id,
  /// so a write whose partitioning matches an existing spec reuses that spec
  /// wholesale, ids included. A NEW partitioning gets the next spec-id and
  /// FRESH field-ids allocated above `last-partition-id`.
  ///
  /// Field-ids must never be reused across specs. A manifest names its spec by
  /// id and readers project `data_file.partition` BY FIELD-ID, so two specs both
  /// starting at 1000 make one spec's partition value decode as the other's
  /// column: `region='DE'` read out of a `country` value, and the manifest
  /// evaluator then prunes away files that do match the predicate.
  ({
    int id,
    List<PartitionField> fields,
    List<Map<String, dynamic>> specs,
    int lastPartitionId,
  })
  _resolveSpec(
    Map<String, dynamic>? previous,
    List<
      ({String name, int sourceId, DataType type, int? precision, int? scale})
    >
    partitions,
  ) {
    final specs = (previous?['partition-specs'] is List)
        ? (previous!['partition-specs'] as List)
              .cast<Map<String, dynamic>>()
              .map((spec) => {...spec})
              .toList()
        : <Map<String, dynamic>>[];

    var lastPartitionId = 999;
    for (final spec in specs) {
      for (final field
          in (spec['fields'] as List).cast<Map<String, dynamic>>()) {
        final id = field['field-id'] as int?;
        if (id != null && id > lastPartitionId) lastPartitionId = id;
      }
    }

    // Match on the identity of the partitioning, ignoring field-ids.
    final probe = [
      for (final part in partitions)
        {
          'source-id': part.sourceId,
          'transform': 'identity',
          'name': part.name,
        },
    ];
    for (final spec in specs) {
      if (!_sameSpecFields(spec['fields'], probe)) continue;
      // Reuse the matched spec's OWN field-ids: the manifest must describe the
      // partition record exactly as that spec declares it.
      final existing = (spec['fields'] as List).cast<Map<String, dynamic>>();
      return (
        id: spec['spec-id'] as int,
        fields: [
          for (int i = 0; i < partitions.length; i++)
            (
              name: partitions[i].name,
              sourceId: partitions[i].sourceId,
              fieldId: existing[i]['field-id'] as int,
              type: partitions[i].type,
              precision: partitions[i].precision,
              scale: partitions[i].scale,
            ),
        ],
        specs: specs,
        lastPartitionId: lastPartitionId,
      );
    }

    final id = specs.isEmpty
        ? 0
        : specs
                  .map((spec) => spec['spec-id'] as int)
                  .reduce((a, b) => a > b ? a : b) +
              1;
    final fields = [
      for (int i = 0; i < partitions.length; i++)
        (
          name: partitions[i].name,
          sourceId: partitions[i].sourceId,
          fieldId: lastPartitionId + 1 + i,
          type: partitions[i].type,
          precision: partitions[i].precision,
          scale: partitions[i].scale,
        ),
    ];
    specs.add({
      'spec-id': id,
      'fields': [
        for (final field in fields)
          {
            'source-id': field.sourceId,
            'field-id': field.fieldId,
            'transform': 'identity',
            'name': field.name,
          },
      ],
    });
    return (
      id: id,
      fields: fields,
      specs: specs,
      lastPartitionId: lastPartitionId + partitions.length,
    );
  }

  Map<String, dynamic> _buildMetadata({
    required String root,
    required Map<String, dynamic>? previous,
    required Table table,
    required int snapshotId,
    required int sequence,
    required int now,
    required String manifestList,
    required IcebergWriteMode mode,
    required ({
      int id,
      List<PartitionField> fields,
      List<Map<String, dynamic>> specs,
      int lastPartitionId,
    })
    spec,
    required Map<String, int>? reuseIds,
    required int lastId,
  }) {
    // EVOLVE the previous metadata rather than reconstructing it: an existing
    // table's partition specs, properties, sort orders, refs (branches/tags), and
    // snapshot history are all carried forward. Both append and overwrite evolve;
    // only a fresh table starts from scratch. (Overwrite keeps prior snapshots so
    // time travel to pre-overwrite versions still works: expiry is a separate op.)
    final evolve = previous != null;
    final appending = evolve && mode == IcebergWriteMode.append;

    // Schema history. Append preserves the existing schema list + current-schema-id
    // (identical-schema appends only). Overwrite may CHANGE the schema, but prior
    // snapshots are retained for time travel, so their schema must survive too:
    // KEEP the prior schemas and ADD the new one under a FRESH schema-id, pointing
    // current-schema-id at it. A retained old snapshot keeps its own schema-id and
    // the reader binds each snapshot by ITS schema-id, so time travel decodes old
    // files under the schema they were written with (dropping the old schema here
    // silently rebound old snapshots to the new schema: corruption).
    final List schemas;
    final int currentSchemaId;
    final base = previous;
    if (appending && base != null && base['schemas'] != null) {
      schemas = base['schemas'] as List;
      currentSchemaId = (base['current-schema-id'] as int?) ?? 0;
    } else if (evolve && base != null && base['schemas'] != null) {
      final prior = (base['schemas'] as List).cast<Map<String, dynamic>>();
      currentSchemaId =
          prior
              .map((schema) => (schema['schema-id'] as int?) ?? 0)
              .reduce((a, b) => a > b ? a : b) +
          1;
      schemas = [
        ...prior,
        IcebergSchema.toJson(
          table.schema,
          schemaId: currentSchemaId,
          reuseIds: reuseIds,
          lastId: lastId,
        ),
      ];
    } else {
      currentSchemaId = 0;
      schemas = [IcebergSchema.toJson(table.schema, schemaId: 0)];
    }

    final snapshot = {
      'snapshot-id': snapshotId,
      'sequence-number': sequence,
      'timestamp-ms': now,
      'manifest-list': manifestList,
      'summary': {
        'operation': mode == IcebergWriteMode.overwrite
            ? 'overwrite'
            : 'append',
      },
      'schema-id': currentSchemaId, // was hardcoded 0
      // Lineage: the snapshot this one replaced as current, which engines walk
      // for history and incremental reads.
      if (previous?['current-snapshot-id'] != null)
        'parent-snapshot-id': previous!['current-snapshot-id'],
    };
    // Retain ALL prior snapshots (time travel), append the new one.
    final snapshots = <Map<String, dynamic>>[
      if (evolve)
        ...((previous['snapshots'] as List?) ?? const [])
            .cast<Map<String, dynamic>>(),
      snapshot,
    ];

    // Partition specs come from _resolveSpec: the ONE place that decides which
    // spec-id and partition field-ids this write commits under, so the manifest
    // list and this metadata cannot disagree about them.
    final specs = spec.specs;
    final defaultSpecId = spec.id;
    final lastPartitionId = spec.lastPartitionId;

    // The current schema JSON (just built / carried): the source for the
    // name-mapping and last-column-id below.
    final currentSchema = schemas.cast<Map<String, dynamic>>().firstWhere(
      (schema) => (schema['schema-id'] as int?) == currentSchemaId,
    );

    // Properties: keep any user-set properties, (re)set our name-mapping, keyed
    // by the ACTUAL field-ids (from the id-stamped schema), recursing into nested
    // struct/list/map children so an external engine can resolve every leaf id.
    final properties = <String, dynamic>{
      if (evolve && previous['properties'] is Map)
        ...(previous['properties'] as Map).cast<String, dynamic>(),
      'schema.name-mapping.default': jsonEncode(_nameMapping(currentSchema)),
    };

    // Sort orders + refs: carry forward; refs keeps every non-main branch/tag and
    // moves main to the new snapshot.
    final sortOrders = evolve && previous['sort-orders'] is List
        ? previous['sort-orders']
        : [
            {'order-id': 0, 'fields': <dynamic>[]},
          ];
    final defaultSortOrderId =
        evolve && previous['default-sort-order-id'] != null
        ? previous['default-sort-order-id']
        : 0;
    final refs = <String, dynamic>{
      if (evolve && previous['refs'] is Map)
        ...(previous['refs'] as Map).cast<String, dynamic>(),
      'main': {'snapshot-id': snapshotId, 'type': 'branch'},
    };

    // last-column-id = highest field-id ever assigned, across EVERY retained
    // schema (nested ids included) so an external writer's next id never collides
    // with an existing leaf. `table.schema.fields.length` counted only top-level.
    var lastColumnId = 0;
    for (final schema in schemas.cast<Map<String, dynamic>>()) {
      for (final id in IcebergSchema.idMap(schema).values) {
        if (id > lastColumnId) lastColumnId = id;
      }
    }
    if (evolve && previous['last-column-id'] is int) {
      final prior = previous['last-column-id'] as int;
      if (prior > lastColumnId) lastColumnId = prior;
    }

    // snapshot-log: chronological (timestamp, snapshot-id) history; external
    // engines read it for time-travel-by-timestamp. Append the new snapshot to the
    // prior log.
    final snapshotLog = <Map<String, dynamic>>[
      if (evolve && previous['snapshot-log'] is List)
        ...(previous['snapshot-log'] as List).cast<Map<String, dynamic>>(),
      {'timestamp-ms': now, 'snapshot-id': snapshotId},
    ];
    return {
      'format-version': 2,
      'table-uuid': previous?['table-uuid'] ?? ids.uuid(),
      'location': 'file://$root',
      'last-updated-ms': now,
      'last-column-id': lastColumnId,
      'schemas': schemas,
      'current-schema-id': currentSchemaId,
      'partition-specs': specs,
      'default-spec-id': defaultSpecId,
      'last-partition-id': lastPartitionId,
      'properties': properties,
      'current-snapshot-id': snapshotId,
      'snapshots': snapshots,
      'sort-orders': sortOrders,
      'default-sort-order-id': defaultSortOrderId,
      'refs': refs,
      'last-sequence-number': sequence,
      'snapshot-log': snapshotLog,
      if (evolve) 'metadata-log': _metadataLog(previous),
    };
  }

  /// [previous] without the catalog's internal `_version` / `_file` keys, as
  /// it may be republished.
  static Map<String, dynamic> _published(Map<String, dynamic> previous) =>
      {...previous}
        ..remove('_version')
        ..remove('_file');

  /// The metadata-log a commit on top of [previous] publishes: the prior log
  /// plus the file [previous] was read from, so engines can walk earlier table
  /// states. It keeps the newest `write.metadata.previous-versions-max`
  /// entries (default 100, as Iceberg does); without the cap every commit
  /// grew it, and the metadata file with it.
  static List<Map<String, dynamic>> _metadataLog(
    Map<String, dynamic> previous,
  ) {
    final log = [
      if (previous['metadata-log'] is List)
        ...(previous['metadata-log'] as List).cast<Map<String, dynamic>>(),
      if (previous['_file'] is String)
        {
          'timestamp-ms': previous['last-updated-ms'],
          'metadata-file':
              'file://${File(previous['_file'] as String).absolute.path}',
        },
    ];
    final properties = previous['properties'];
    final limit = properties is Map
        ? int.tryParse('${properties['write.metadata.previous-versions-max']}')
        : null;
    final keep = (limit ?? 100).clamp(0, log.length);
    return log.sublist(log.length - keep);
  }

  /// Iceberg `schema.name-mapping.default` for a schema JSON: a recursive
  /// (field-id, names, [fields]) tree so every nested leaf id is resolvable.
  static List<Map<String, dynamic>> _nameMapping(Map<String, dynamic> schema) =>
      _mapFields((schema['fields'] as List).cast<Map<String, dynamic>>());

  static List<Map<String, dynamic>> _mapFields(
    List<Map<String, dynamic>> fields,
  ) => [
    for (final field in fields)
      _mapEntry(field['id'] as int, field['name'] as String, field['type']),
  ];

  static Map<String, dynamic> _mapEntry(int id, String name, Object? type) {
    final nested = _mapNested(type);
    return {
      'field-id': id,
      'names': [name],
      if (nested.isNotEmpty) 'fields': nested,
    };
  }

  static List<Map<String, dynamic>> _mapNested(Object? type) {
    if (type is! Map) return const [];
    switch (type['type']) {
      case 'struct':
        return _mapFields(
          (type['fields'] as List).cast<Map<String, dynamic>>(),
        );
      case 'list':
        return [
          _mapEntry(type['element-id'] as int, 'element', type['element']),
        ];
      case 'map':
        return [
          _mapEntry(type['key-id'] as int, 'key', type['key']),
          _mapEntry(type['value-id'] as int, 'value', type['value']),
        ];
      default:
        return const [];
    }
  }

  /// Whether two resolved specs assign the SAME partition field-ids, in order:
  /// the thing the manifest's Avro schema was written against.
  static bool _sameFieldIds(List<PartitionField> a, List<PartitionField> b) {
    if (a.length != b.length) return false;
    for (int i = 0; i < a.length; i++) {
      if (a[i].fieldId != b[i].fieldId) return false;
    }
    return true;
  }

  /// Whether two partition-spec field lists describe the same partitioning
  /// (same source-ids, transforms, and names in order), so an unchanged spec
  /// reuses its spec-id instead of being registered again.
  static bool _sameSpecFields(Object? a, List<Map<String, dynamic>> b) {
    if (a is! List || a.length != b.length) return false;
    final fields = a.cast<Map<String, dynamic>>();
    for (int i = 0; i < b.length; i++) {
      if (fields[i]['source-id'] != b[i]['source-id'] ||
          fields[i]['transform'] != b[i]['transform'] ||
          fields[i]['name'] != b[i]['name']) {
        return false;
      }
    }
    return true;
  }

  /// Whether [schema] is identical (field names + types, in order) to the
  /// current schema in [previous]'s metadata. Strict: schema evolution / widening
  /// is intentionally rejected.
  bool _schemaMatches(Map<String, dynamic> previous, Schema schema) {
    final schemas = previous['schemas'];
    // Absent/empty/non-list schema metadata is corruption, not a match: failing
    // OPEN here would let an append proceed against a table whose real schema is
    // unknown. Fail closed.
    if (schemas is! List || schemas.isEmpty) {
      throw FormatException(
        'Iceberg metadata has no readable schemas (corrupt or incomplete '
        'metadata); refusing to treat it as a schema match',
      );
    }
    final schemaId = previous['current-schema-id'];
    final list = schemas.cast<Map<String, dynamic>>();
    final current = schemaId != null
        ? list.firstWhere(
            (entry) => entry['schema-id'] == schemaId,
            orElse: () => throw FormatException(
              'Iceberg metadata references current-schema-id $schemaId but no '
              'schema has that id (corrupt metadata)',
            ),
          )
        : list.last;
    final old = (current['fields'] as List).cast<Map<String, dynamic>>();
    final incoming = (IcebergSchema.toJson(schema)['fields'] as List)
        .cast<Map<String, dynamic>>();
    if (old.length != incoming.length) return false;
    for (int i = 0; i < old.length; i++) {
      if (old[i]['name'] != incoming[i]['name']) return false;
      if ('${old[i]['type']}' != '${incoming[i]['type']}') return false;
      // `required` is a SIBLING of `type` in the field JSON, so the stringified
      // type above does NOT cover top-level nullability. Compare it explicitly:
      // publishing a nullable-with-null file under a required-field schema would
      // commit but then fail every read (the read path trusts the declared type).
      if (old[i]['required'] != incoming[i]['required']) return false;
    }
    return true;
  }

  /// Resolve an embedded metadata path against [root], then CONFINE it to the
  /// root unless [allowExternalPaths], so a manifest naming an external absolute
  /// file can never be read or (in expiry) deleted. [confine] throws on escape.
  Future<String> _resolve(String root, String path) async {
    final resolved = await resolveIcebergPath(root, path);
    return allowExternalPaths ? resolved : confine(root, resolved, label: path);
  }
}
