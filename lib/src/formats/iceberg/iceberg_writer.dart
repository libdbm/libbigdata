import 'dart:convert';
import 'dart:io';
import 'dart:math';

import '../../core/schema.dart';
import '../../core/limits.dart';
import '../../core/table.dart';
import '../../core/data_type.dart';
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
  DataType dt,
  int? precision,
  int? scale,
});

enum IcebergWriteMode { append, overwrite }

/// Writes Apache Iceberg tables (format-version 2), optionally partitioned by
/// identity transforms (`partitionBy`): a Parquet data file per partition, an
/// Avro manifest + manifest list, and a new `metadata.json`
/// snapshot. Uses Iceberg's canonical manifest Avro schemas so the output is
/// readable by Spark / pyiceberg / DuckDB.
class IcebergWriter {
  final String tablePath;
  final IcebergBackend _backend;

  /// The ceiling this writer refuses to publish metadata above — because the
  /// READER rejects a `metadata.json` over [maxIcebergMetadataBytes], an
  /// unchecked publish could emit a current metadata the next read (and
  /// `expireSnapshots`) can't open, bricking the table. Defaults to the read cap;
  /// injectable (tests set it low). A commit that would exceed it fails loud,
  /// leaving the last-good (readable) metadata for `expireSnapshots` to prune.
  final int _maxMetadataBytes;

  /// When false (default), every path this writer resolves from table metadata
  /// (manifests, manifest lists, data files) is CONFINED to the table root, so
  /// `expireSnapshots` cannot be tricked into reading — or DELETING — a file
  /// outside the table by a crafted/corrupt manifest that names an external
  /// absolute path. Set true only for a trusted table whose metadata
  /// legitimately references external locations.
  final bool allowExternalPaths;

  /// Pass a [backend] to plug in a real catalog compare-and-swap (REST / Glue /
  /// Hive): one object owns BOTH the current-metadata read and the commit CAS,
  /// so reads and writes can never resolve different pointers (the split-brain a
  /// separately-injected coordinator + catalog allowed).
  ///
  /// The DEFAULT is now [FileLockIcebergBackend] — it holds an exclusive OS file
  /// lock over the whole read → build → publish, giving genuine cross-PROCESS
  /// compare-and-swap (no lost update / duplicate version) on a lock-honoring
  /// filesystem. It does NOT serialize separate Dart ISOLATES (POSIX locks are
  /// per-process — use one writer per process there), and a bare
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
  Map<String, dynamic> _dataFileStats(Table data, Schema idSchema) {
    final valueCounts = <Map<String, Object?>>[];
    final nullCounts = <Map<String, Object?>>[];
    final nanCounts = <Map<String, Object?>>[];
    final lower = <Map<String, Object?>>[];
    final upper = <Map<String, Object?>>[];
    for (final column in data.columns) {
      final field = idSchema.field(column.name);
      final id = field?.id;
      if (id == null || field!.dataType.isNested || field.children.isNotEmpty) {
        continue;
      }
      final type = field.dataType;
      final stats = StatsAccumulator()..addColumn(column);
      valueCounts.add({'key': id, 'value': stats.count});
      nullCounts.add({'key': id, 'value': stats.nulls});
      if (type == DataType.float32 || type == DataType.float64) {
        nanCounts.add({'key': id, 'value': stats.nans});
      }
      final lo = stats.min == null ? null : icebergBound(stats.min!, type);
      if (lo != null) lower.add({'key': id, 'value': lo});
      final hi = stats.max == null ? null : icebergBound(stats.max!, type);
      if (hi != null) upper.add({'key': id, 'value': hi});
    }
    List<Map<String, Object?>>? orNull(List<Map<String, Object?>> l) =>
        l.isEmpty ? null : l;
    return {
      'value_counts': orNull(valueCounts),
      'null_value_counts': orNull(nullCounts),
      'nan_value_counts': orNull(nanCounts),
      'lower_bounds': orNull(lower),
      'upper_bounds': orNull(upper),
    };
  }

  /// Build the metadata publish, refusing one over [_maxMetadataBytes] BEFORE it
  /// is written — an oversized `metadata.json` would be unreadable (see the
  /// field). Uses the UTF-8 byte length to match the reader's `File.length` check.
  IcebergMetadataPublish _publish(String name, String json) {
    final bytes = utf8.encode(json).length;
    if (bytes > _maxMetadataBytes) {
      throw StateError(
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
  Future<int> write(
    Table table, {
    IcebergWriteMode mode = IcebergWriteMode.append,
    List<String>? partitionBy,
    int? maxPartitions = defaultMaxPartitionsPerWrite,
  }) async {
    final partCols = partitionBy ?? const <String>[];
    final root = Directory(tablePath).absolute.path;
    await Directory('$root/data').create(recursive: true);
    await Directory('$root/metadata').create(recursive: true);

    // Reject an incompatible append BEFORE writing anything: appending a
    // different schema would silently rebind the surviving old data files to a
    // new schema (and drop schema history). Only identical-schema appends are
    // supported (schema evolution / safe widening is out of scope).
    // The current metadata (if the table exists) — used to REUSE existing field
    // ids on append (Iceberg projects by id) and to validate schema compatibility.
    final existing = await _backend.current(root);
    if (mode == IcebergWriteMode.append &&
        existing != null &&
        !_schemaMatches(existing, table.schema)) {
      throw StateError(
        'Iceberg append schema mismatch on $tablePath: the incoming schema '
        'differs from the existing table schema (only identical-schema '
        'appends are supported; use overwrite to replace the schema)',
      );
    }
    // Reuse the table's existing field-ids on append so the new Parquet's
    // field-ids match the metadata (external engines bind columns by id).
    final reuseIds = mode == IcebergWriteMode.append && existing != null
        ? IcebergSchema.idMap(_currentSchema(existing))
        : null;

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
    // terminal failure cleans up ALL of them — not just partition 0.
    final writtenData = <String>[];

    // The data file, manifest, and manifest-list are written before the commit
    // is published. If the commit fails terminally (contention) or anything
    // throws afterward, none of them are referenced by a committed metadata.json
    // — delete them so they don't orphan on disk.
    try {
      // 1. data file(s) — tag the schema with Iceberg field-ids (matching the
      // metadata schema, nested types included) so the Parquet carries
      // file-level field-ids, enabling true Spark-Iceberg schema evolution /
      // column reordering. Unlike Delta, Iceberg KEEPS the partition columns in
      // the data files; the partition tuple is also recorded in the manifest.
      final idSchema = IcebergSchema.build(
        table.schema,
        reuseIds: reuseIds,
      ).idSchema;
      // Partition spec: identity transforms over the named columns. The
      // field-ids are ASSIGNED by _resolveSpec, not hardcoded to 1000+i — they
      // are baked into the data manifest's Avro schema written below, so the
      // spec has to be resolved before that. This uses the pre-read `existing`;
      // the commit loop re-validates it against the version it actually claims.
      final partitions = [
        for (final column in partCols)
          (
            name: column,
            sourceId: idSchema.fields.firstWhere((f) => f.name == column).id!,
            dt: table.schema.field(column)!.dataType,
            precision: table.schema.field(column)!.precision,
            scale: table.schema.field(column)!.scale,
          ),
      ];
      final resolved = _resolveSpec(existing, partitions);
      final specFields = resolved.fields;

      // The partition cap is enforced INSIDE splitByKeys as each new key is
      // discovered, so a high-cardinality blow-up fails loud before the whole
      // O(rows) grouping map is built (not after, once every partition is
      // already grouped).
      final parts = partCols.isEmpty
          ? [(values: const <String, Object?>{}, table: table)]
          : table.splitByKeys(partCols, maxPartitions: maxPartitions);

      final entries = <Map<String, dynamic>>[];
      var totalRows = 0;
      // Enumerate lazily (splitByKeys yields one partition copy at a time) — no
      // index/length access, so all partition tables are never held at once.
      var pi = -1;
      for (final part in parts) {
        pi++;
        final fileName = partCols.isEmpty
            ? dataName
            : '00000-$pi-$commit.parquet';
        final filePath = '$root/data/$fileName';
        await ParquetWriter(filePath).write(
          Table(
            name: table.name,
            columns: part.table.columns,
            schema: idSchema,
          ),
        );
        writtenData.add(filePath); // track EVERY partition file for cleanup
        final size = await File(filePath).length();
        totalRows += part.table.rowCount;
        final stats = _dataFileStats(part.table, idSchema);
        entries.add({
          'status': 1, // ADDED
          'snapshot_id': snapshotId,
          'sequence_number': null,
          'file_sequence_number': null,
          'data_file': {
            'content': 0,
            'file_path': 'file://$filePath',
            'file_format': 'PARQUET',
            'partition': {
              for (final s in specFields)
                s.name: _partValue(part.values[s.name], s.dt),
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

      // 2. manifest — when partitioned, inject the partition record fields into
      // the manifest entry's `data_file.partition` Avro schema.
      final mschema = specFields.isEmpty
          ? manifestEntrySchema
          : _partitionManifestSchema(specFields);
      await AvroOcfWriter.writePathAsync(manifestPath, mschema, entries);
      final manifestSize = await File(manifestPath).length();

      // 3+4. Publish the manifest list + metadata.json against the freshly-read
      // previous metadata, atomically installing the new current-metadata
      // pointer. The [IcebergCommitCoordinator] owns the version claim / CAS: the
      // default FilesystemIcebergBackend uses an O_EXCL `vNNNNN.commit` marker
      // with lease-reclaim (a catalog-backed backend can supply true
      // cross-engine CAS). On a bare filesystem this narrows but cannot fully
      // close the window — real cross-engine CAS requires a catalog service. The
      // backend reads the current pointer through its own `current`, so the CAS
      // can't diverge from the read.
      return await _backend.commit(
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
            throw StateError(
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
            throw StateError(
              'Iceberg partition spec changed concurrently on $tablePath '
              '(resolved spec ${resolved.id}, now ${claimed.id}); retry the '
              'write',
            );
          }
          final sequence =
              ((previous?['last-sequence-number'] as int?) ?? 0) + 1;
          final manifestEntries = <Map<String, dynamic>>[
            _manifestListEntry(
              'file://$root/metadata/$manifestName',
              manifestSize,
              snapshotId,
              sequence,
              totalRows,
              entries.length,
              specId: claimed.id,
            ),
          ];
          if (mode == IcebergWriteMode.append && previous != null) {
            manifestEntries.addAll(await _priorManifestEntries(root, previous));
          }
          final manifestListName = 'snap-$snapshotId-0-$commit.avro';
          final listPath = '$root/metadata/$manifestListName';
          manifestListPath =
              listPath; // record for the outer cleanup on failure
          await AvroOcfWriter.writePathAsync(
            listPath,
            manifestListSchema,
            manifestEntries,
          );

          final metaName =
              '${version.toString().padLeft(5, '0')}-$commit.metadata.json';
          final meta = _buildMetadata(
            root: root,
            previous: previous,
            table: table,
            snapshotId: snapshotId,
            sequence: sequence,
            now: now,
            manifestList: 'file://$listPath',
            mode: mode,
            spec: claimed,
          );
          return _publish(metaName, jsonEncode(meta));
        },
      );
    } on IcebergCommitIndeterminate {
      // The coordinator could not determine whether the commit landed (e.g. a
      // catalog CAS whose response was lost to a timeout/reset). The published
      // metadata may already reference the staged data files, so deleting them
      // would destroy a committed snapshot. RETAIN everything and surface the
      // ambiguity — resolving the current pointer is the caller's job.
      rethrow;
    } catch (e) {
      // Known-failed (terminal contention) or a pre-publish staging error: none
      // of these are referenced by a committed metadata.json, so remove them.
      await _cleanup(e, [
        ...writtenData, // every partition data file, not just partition 0
        manifestPath,
        if (manifestListPath != null) manifestListPath!,
      ]);
      rethrow;
    }
  }

  /// Drop old snapshots from the table's history and delete the files only they
  /// reached, returning how many snapshots were expired. This is the operation
  /// [maxIcebergMetadataBytes]'s error message tells users to run: an appending
  /// table's `snapshots` array grows forever, and every commit rewrites the
  /// whole of it.
  ///
  /// A snapshot expires only if it is BOTH outside the newest [retain] AND, when
  /// [before] is given, older than it — [retain] is a floor that protects recent
  /// history regardless of age, matching Iceberg's `expireOlderThan` +
  /// `retainLast`. The current snapshot is always retained, so [retain] must be
  /// at least 1. Expiry drops HISTORY, not data: the current snapshot still
  /// reads every row it did before.
  ///
  /// This DELETES files, so it is deliberately conservative:
  /// - The pruned metadata is published FIRST and files are deleted only after
  ///   it lands. A crash in between leaves unreferenced files (recoverable) —
  ///   the reverse order would leave live metadata pointing at deleted files.
  /// - A file reachable from ANY retained snapshot is never deleted, even if an
  ///   expired snapshot reaches it too (an append carries prior manifests
  ///   forward, so this overlap is the normal case, not an edge one).
  /// - A manifest that cannot be read is a reachability the traversal cannot
  ///   prove, so it aborts rather than delete on an incomplete picture.
  /// [allowOversizedMetadata] bypasses the [maxIcebergMetadataBytes] read cap so
  /// a table ALREADY bricked with oversized metadata (e.g. written by another
  /// tool before the publish cap existed) can be read here and pruned back under
  /// the cap — a recovery escape hatch, not for normal use.
  Future<int> expireSnapshots({
    int retain = 1,
    DateTime? before,
    bool allowOversizedMetadata = false,
  }) async {
    if (retain < 1) {
      throw ArgumentError.value(
        retain,
        'retain',
        'must be at least 1 (the current snapshot can never be expired)',
      );
    }
    final root = Directory(tablePath).absolute.path;
    final existing = await _backend.current(
      root,
      allowOversized: allowOversizedMetadata,
    );
    if (existing == null) {
      throw StateError(
        'Cannot expire snapshots: no Iceberg table at $tablePath',
      );
    }

    // Nothing to expire: return WITHOUT committing. `_survivors` is pure, so this
    // pre-check matches the commit's re-derivation — a genuine no-op must not
    // publish a new metadata version (unbounded churn on scheduled maintenance)
    // nor leak the internal `_version` into the published JSON.
    final present = ((existing['snapshots'] as List?) ?? const [])
        .cast<Map<String, dynamic>>();
    if (present.every(_survivors(existing, present, retain, before).contains)) {
      return 0;
    }

    // Decide what goes BEFORE touching anything, from the metadata we publish
    // against — the commit below re-derives it against the version it claims.
    var expired = <Map<String, dynamic>>[];
    var doomed = <String>{};

    await _backend.commit(
      root: root,
      allowOversized: allowOversizedMetadata,
      build: (previous, version) async {
        if (previous == null) {
          throw StateError(
            'Cannot expire snapshots: the Iceberg table at $tablePath '
            'disappeared mid-commit',
          );
        }
        final all = ((previous['snapshots'] as List?) ?? const [])
            .cast<Map<String, dynamic>>();
        final keep = _survivors(previous, all, retain, before);
        expired = [
          for (final s in all)
            if (!keep.contains(s)) s,
        ];
        if (expired.isEmpty) {
          // A concurrent writer expired these between the pre-check and this
          // claim: publish the current metadata unchanged, but never leak the
          // internal `_version` (the real-expiry branch strips it below too).
          doomed = {};
          return _publish(
            _metadataName(version),
            jsonEncode({...previous}..remove('_version')),
          );
        }
        // Reachability, computed while both sets still exist on disk. Schemas
        // are NOT pruned alongside the snapshots: a retained snapshot binds by
        // its own schema-id, and the schema list grows only per overwrite.
        // One manifest-reachability cache across BOTH loops: a manifest carried
        // forward by many snapshots is read once, not once per snapshot.
        final manifestCache = <String, Set<String>>{};
        final live = <String>{};
        for (final snapshot in keep) {
          live.addAll(await _reachable(root, snapshot, manifestCache));
        }
        final dead = <String>{};
        for (final snapshot in expired) {
          dead.addAll(await _reachable(root, snapshot, manifestCache));
        }
        doomed = dead.difference(live);

        final pruned = {...previous}..remove('_version');
        pruned['snapshots'] = keep;
        final retained = {for (final s in keep) s['snapshot-id']};
        if (previous['snapshot-log'] is List) {
          pruned['snapshot-log'] = [
            for (final e in previous['snapshot-log'] as List)
              if (retained.contains((e as Map)['snapshot-id'])) e,
          ];
        }
        return _publish(_metadataName(version), jsonEncode(pruned));
      },
    );

    // The pruned metadata is live; nothing reaches these files any more. A
    // delete failure is collected and thrown (like the staging cleanup) rather
    // than silently orphaning — the expiry itself has already committed.
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
    return expired.length;
  }

  String _metadataName(int version) =>
      '${version.toString().padLeft(5, '0')}-${ids.uuid()}.metadata.json';

  /// The snapshots [retain]/[before] protect: the newest [retain] by position
  /// (the array is append-ordered), anything not older than [before], and the
  /// current snapshot — which is retained even if it satisfies neither, since
  /// expiring it would leave the table pointing at nothing.
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
    // (explicit index tie-break — List.sort is not stable), and a missing
    // timestamp ranks oldest.
    int stamp(Map<String, dynamic> s) => (s['timestamp-ms'] as int?) ?? 0;
    final indexed = [for (int i = 0; i < all.length; i++) (i, all[i])];
    indexed.sort((a, b) {
      final c = stamp(a.$2).compareTo(stamp(b.$2));
      return c != 0 ? c : a.$1.compareTo(b.$1);
    });
    final ranked = [for (final e in indexed) e.$2];
    final newest = ranked.length <= retain
        ? ranked
        : ranked.sublist(ranked.length - retain);
    final cutoff = before?.millisecondsSinceEpoch;
    final current = metadata['current-snapshot-id'];
    return [
      for (final s in all)
        if (newest.contains(s) ||
            (cutoff != null && ((s['timestamp-ms'] as int?) ?? 0) >= cutoff) ||
            (current != null && current != -1 && s['snapshot-id'] == current))
          s,
    ];
  }

  /// Every file [snapshot] reaches: its manifest list, that list's manifests,
  /// and every data/delete file those manifests name — INCLUDING entries marked
  /// DELETED, which the read path skips but an older retained snapshot may still
  /// count as live. Over-collecting here only ever spares a file.
  Future<Set<String>> _reachable(
    String root,
    Map<String, dynamic> snapshot,
    Map<String, Set<String>> manifestCache,
  ) async {
    final reached = <String>{};
    final list = snapshot['manifest-list'];
    if (list is! String) return reached;
    final listPath = await _resolve(root, list);
    if (!await File(listPath).exists()) {
      // Deleting on an unprovable reachability is how a vacuum destroys a live
      // table. A snapshot in the metadata whose manifest list is gone is already
      // corrupt (the read path says so too) — surface it, don't work around it.
      throw FormatException(
        'Iceberg snapshot ${snapshot['snapshot-id']} references manifest list '
        '$list but that file is missing, so the files it reaches cannot be '
        'determined (corrupt metadata) in $root',
      );
    }
    reached.add(listPath);
    for (final entry in await AvroOcfReader.readPathAsync(listPath)) {
      final manifestPath = await _resolve(
        root,
        entry['manifest_path'] as String,
      );
      reached.addAll(await _manifestReached(root, manifestPath, manifestCache));
    }
    return reached;
  }

  /// The files a SINGLE manifest reaches (itself + every data/delete file it
  /// names), memoized by manifest path in [cache]. Appends carry prior manifests
  /// forward, so the same manifest is referenced by many snapshots — caching
  /// makes `expireSnapshots` read each manifest Avro once instead of once per
  /// snapshot (it was quadratic, and could OOM on the very oversized table the
  /// operation exists to repair).
  Future<Set<String>> _manifestReached(
    String root,
    String manifestPath,
    Map<String, Set<String>> cache,
  ) async {
    final cached = cache[manifestPath];
    if (cached != null) return cached;
    if (!await File(manifestPath).exists()) {
      // Fail CLOSED, exactly as the missing manifest-list case: a manifest we
      // can't read is a reachability we can't PROVE, so skipping it would
      // under-count `live` and let expiry delete a file the retained snapshots
      // still reference. A missing manifest is corrupt metadata.
      throw FormatException(
        'Iceberg manifest list references manifest $manifestPath but that '
        'file is missing, so the files it reaches cannot be determined '
        '(corrupt metadata) in $root',
      );
    }
    final files = <String>{manifestPath};
    for (final record in await AvroOcfReader.readPathAsync(manifestPath)) {
      final file = (record['data_file'] as Map)['file_path'];
      if (file is String) files.add(await _resolve(root, file));
    }
    cache[manifestPath] = files;
    return files;
  }

  /// Remove the files this transaction staged after a terminal failure [cause]
  /// (none are referenced by a committed metadata.json). Unlike Delta's
  /// best-effort cleanup, this COLLECTS delete failures and throws — so a
  /// half-cleaned commit is observable instead of silently orphaning files.
  /// Returns normally when every file is removed, so the caller rethrows [cause].
  Future<void> _cleanup(Object cause, Iterable<String> paths) async {
    final failed = await deleteStaged(paths);
    if (failed.isNotEmpty) {
      throw StateError(
        'Iceberg commit failed ($cause); additionally, cleanup could not remove '
        'staged files: ${failed.join('; ')}',
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
  }) => {
    'manifest_path': path,
    'manifest_length': length,
    // The spec this manifest's partition records were actually written under.
    // Hardcoding 0 made readers resolve the WRONG spec the moment a table's
    // partitioning changed — they look the spec up by this id and project
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
    'partitions': null,
    'key_metadata': null,
  };

  /// The identity partition value for a column of [dt], converted to match the
  /// Avro type [_avroPartType] declares. Null partition values stay null.
  static Object? _partValue(Object? v, DataType dt) {
    if (v == null) return null;
    return switch (dt) {
      DataType.date when v is DateTime => dateToEpochDays(v),
      DataType.timestamp when v is DateTime => timestampToEpochMicros(v),
      // Kept as the UNSCALED BigInt: the Avro encoder writes a decimal logical
      // type as big-endian two's-complement. toString() emitted the digits as
      // TEXT under an Avro `string`, which a reader's string<->bytes promotion
      // then decodes as unscaled bytes — garbage.
      _ => v, // numeric/boolean/string/decimal pass through; binary stays bytes
    };
  }

  /// Avro type for an identity partition column (nullable union, default null).
  static Object _avroPartType(DataType dt, {int? precision, int? scale}) {
    final t = switch (dt) {
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
    return ['null', t];
  }

  /// Build a manifest-entry Avro schema whose `data_file.partition` record holds
  /// the partition fields (Iceberg requires the partition tuple typed by spec).
  static String _partitionManifestSchema(List<PartitionField> spec) {
    final schema = jsonDecode(manifestEntrySchema) as Map<String, dynamic>;
    Map<String, dynamic> recordOf(dynamic type) => type is List
        ? type.firstWhere((t) => t is Map && t['type'] == 'record')
              as Map<String, dynamic>
        : type as Map<String, dynamic>;
    final dataFile = recordOf(
      (schema['fields'] as List).firstWhere(
        (f) => f['name'] == 'data_file',
      )['type'],
    );
    final partition = recordOf(
      (dataFile['fields'] as List).firstWhere(
        (f) => f['name'] == 'partition',
      )['type'],
    );
    partition['fields'] = [
      for (final s in spec)
        {
          'name': s.name,
          'field-id': s.fieldId,
          'type': _avroPartType(s.dt, precision: s.precision, scale: s.scale),
          'default': null,
        },
    ];
    return jsonEncode(schema);
  }

  Future<List<Map<String, dynamic>>> _priorManifestEntries(
    String root,
    Map<String, dynamic> previous,
  ) async {
    final snapshots = (previous['snapshots'] as List)
        .cast<Map<String, dynamic>>();
    final current = previous['current-snapshot-id'];
    // A present-but-dangling current-snapshot-id is corrupt: carrying manifests
    // forward from the wrong snapshot would rewrite history. Fail closed; only
    // an absent / -1 pointer falls back to the latest snapshot.
    final snap = (current != null && current != -1)
        ? snapshots.firstWhere(
            (s) => s['snapshot-id'] == current,
            orElse: () => throw FormatException(
              'Iceberg metadata references current-snapshot-id $current but no '
              'snapshot has that id (corrupt metadata) in $root',
            ),
          )
        : snapshots.last;
    final mlPath = await _resolve(root, snap['manifest-list'] as String);
    // A present snapshot whose manifest list is missing is CORRUPTION, not
    // "no priors": returning [] here would build an append-only snapshot that
    // silently drops every prior data file. Fail closed so the append aborts and
    // the prior state is preserved.
    if (!await File(mlPath).exists()) {
      throw FormatException(
        'Iceberg snapshot ${snap['snapshot-id']} references manifest list '
        '${snap['manifest-list']} but that file is missing (corrupt metadata) '
        'in $root',
      );
    }
    // Re-emit each prior manifest_list record unchanged (their files persist).
    return AvroOcfReader.readPathAsync(mlPath);
  }

  /// The partition spec this write commits under, resolved against [previous]'s
  /// metadata: the spec-id its manifests must declare, the fields WITH their
  /// field-ids, the full `partition-specs` array to publish, and the new
  /// `last-partition-id` high-water mark.
  ///
  /// Iceberg matches a spec by (source-id, transform, name) — NOT by field-id —
  /// so a write whose partitioning matches an existing spec reuses that spec
  /// wholesale, ids included. A NEW partitioning gets the next spec-id and
  /// FRESH field-ids allocated above `last-partition-id`.
  ///
  /// Field-ids must never be reused across specs. A manifest names its spec by
  /// id and readers project `data_file.partition` BY FIELD-ID, so two specs both
  /// starting at 1000 make one spec's partition value decode as the other's
  /// column — `region='DE'` read out of a `country` value, and the manifest
  /// evaluator then prunes away files that do match the predicate.
  ({
    int id,
    List<PartitionField> fields,
    List<Map<String, dynamic>> specs,
    int lastPartitionId,
  })
  _resolveSpec(
    Map<String, dynamic>? previous,
    List<({String name, int sourceId, DataType dt, int? precision, int? scale})>
    partitions,
  ) {
    final specs = (previous?['partition-specs'] is List)
        ? (previous!['partition-specs'] as List)
              .cast<Map<String, dynamic>>()
              .map((s) => {...s})
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
              dt: partitions[i].dt,
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
                  .map((s) => s['spec-id'] as int)
                  .reduce((a, b) => a > b ? a : b) +
              1;
    final fields = [
      for (int i = 0; i < partitions.length; i++)
        (
          name: partitions[i].name,
          sourceId: partitions[i].sourceId,
          fieldId: lastPartitionId + 1 + i,
          dt: partitions[i].dt,
          precision: partitions[i].precision,
          scale: partitions[i].scale,
        ),
    ];
    specs.add({
      'spec-id': id,
      'fields': [
        for (final f in fields)
          {
            'source-id': f.sourceId,
            'field-id': f.fieldId,
            'transform': 'identity',
            'name': f.name,
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
  }) {
    // EVOLVE the previous metadata rather than reconstructing it: an existing
    // table's partition specs, properties, sort orders, refs (branches/tags), and
    // snapshot history are all carried forward. Both append and overwrite evolve;
    // only a fresh table starts from scratch. (Overwrite keeps prior snapshots so
    // time travel to pre-overwrite versions still works — expiry is a separate op.)
    final evolve = previous != null;
    final isAppend = evolve && mode == IcebergWriteMode.append;

    // Schema history. Append preserves the existing schema list + current-schema-id
    // (identical-schema appends only). Overwrite may CHANGE the schema — but prior
    // snapshots are retained for time travel, so their schema must survive too:
    // KEEP the prior schemas and ADD the new one under a FRESH schema-id, pointing
    // current-schema-id at it. A retained old snapshot keeps its own schema-id and
    // the reader binds each snapshot by ITS schema-id, so time travel decodes old
    // files under the schema they were written with (dropping the old schema here
    // silently rebound old snapshots to the new schema — corruption).
    final List schemas;
    final int currentSchemaId;
    final prev = previous;
    if (isAppend && prev != null && prev['schemas'] != null) {
      schemas = prev['schemas'] as List;
      currentSchemaId = (prev['current-schema-id'] as int?) ?? 0;
    } else if (evolve && prev != null && prev['schemas'] != null) {
      final prior = (prev['schemas'] as List).cast<Map<String, dynamic>>();
      currentSchemaId =
          prior
              .map((s) => (s['schema-id'] as int?) ?? 0)
              .reduce((a, b) => a > b ? a : b) +
          1;
      schemas = [
        ...prior,
        IcebergSchema.toJson(table.schema, schemaId: currentSchemaId),
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
    };
    // Retain ALL prior snapshots (time travel), append the new one.
    final snapshots = <Map<String, dynamic>>[
      if (evolve)
        ...((previous['snapshots'] as List?) ?? const [])
            .cast<Map<String, dynamic>>(),
      snapshot,
    ];

    // Partition specs come from _resolveSpec — the ONE place that decides which
    // spec-id and partition field-ids this write commits under, so the manifest
    // list and this metadata cannot disagree about them.
    final specs = spec.specs;
    final defaultSpecId = spec.id;
    final lastPartitionId = spec.lastPartitionId;

    // The current schema JSON (just built / carried) — the source for the
    // name-mapping and last-column-id below.
    final currentSchema = schemas.cast<Map<String, dynamic>>().firstWhere(
      (s) => (s['schema-id'] as int?) == currentSchemaId,
    );

    // Properties: keep any user-set properties, (re)set our name-mapping — keyed
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
    for (final s in schemas.cast<Map<String, dynamic>>()) {
      for (final id in IcebergSchema.idMap(s).values) {
        if (id > lastColumnId) lastColumnId = id;
      }
    }
    if (isAppend && previous['last-column-id'] is int) {
      final prior = previous['last-column-id'] as int;
      if (prior > lastColumnId) lastColumnId = prior;
    }

    // snapshot-log: chronological (timestamp, snapshot-id) history — external
    // engines read it for time-travel-by-timestamp. Append the new snapshot to the
    // prior log. (metadata-log carries forward untouched; appending the previous
    // metadata FILE needs its path, which the coordinator owns, not this builder.)
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
      if (evolve && previous['metadata-log'] is List)
        'metadata-log': previous['metadata-log'],
    };
  }

  /// Iceberg `schema.name-mapping.default` for a schema JSON — a recursive
  /// (field-id, names, [fields]) tree so every nested leaf id is resolvable.
  static List<Map<String, dynamic>> _nameMapping(Map<String, dynamic> schema) =>
      _mapFields((schema['fields'] as List).cast<Map<String, dynamic>>());

  static List<Map<String, dynamic>> _mapFields(
    List<Map<String, dynamic>> fields,
  ) => [
    for (final f in fields)
      _mapEntry(f['id'] as int, f['name'] as String, f['type']),
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

  /// The current schema JSON object from an Iceberg [metadata] (by
  /// `current-schema-id`, else the last schema).
  static Map<String, dynamic> _currentSchema(Map<String, dynamic> metadata) {
    final schemas = (metadata['schemas'] as List).cast<Map<String, dynamic>>();
    final id = metadata['current-schema-id'];
    return schemas.firstWhere(
      (s) => s['schema-id'] == id,
      orElse: () => schemas.last,
    );
  }

  /// Whether two partition-spec field lists describe the same partitioning
  /// (same source-ids, transforms, and names in order) — so an unchanged spec
  /// reuses its spec-id instead of being registered again.
  /// Whether two resolved specs assign the SAME partition field-ids, in order —
  /// the thing the manifest's Avro schema was written against.
  static bool _sameFieldIds(List<PartitionField> a, List<PartitionField> b) {
    if (a.length != b.length) return false;
    for (int i = 0; i < a.length; i++) {
      if (a[i].fieldId != b[i].fieldId) return false;
    }
    return true;
  }

  static bool _sameSpecFields(Object? a, List<Map<String, dynamic>> b) {
    if (a is! List || a.length != b.length) return false;
    final af = a.cast<Map<String, dynamic>>();
    for (int i = 0; i < b.length; i++) {
      if (af[i]['source-id'] != b[i]['source-id'] ||
          af[i]['transform'] != b[i]['transform'] ||
          af[i]['name'] != b[i]['name']) {
        return false;
      }
    }
    return true;
  }

  /// Whether [schema] is identical (field names + types, in order) to the
  /// current schema in [previous]'s metadata. Strict — schema evolution / widening
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
            (s) => s['schema-id'] == schemaId,
            orElse: () => throw FormatException(
              'Iceberg metadata references current-schema-id $schemaId but no '
              'schema has that id (corrupt metadata)',
            ),
          )
        : list.last;
    final prevFields = (current['fields'] as List).cast<Map<String, dynamic>>();
    final newFields = (IcebergSchema.toJson(schema)['fields'] as List)
        .cast<Map<String, dynamic>>();
    if (prevFields.length != newFields.length) return false;
    for (int i = 0; i < prevFields.length; i++) {
      if (prevFields[i]['name'] != newFields[i]['name']) return false;
      if ('${prevFields[i]['type']}' != '${newFields[i]['type']}') return false;
      // `required` is a SIBLING of `type` in the field JSON, so the stringified
      // type above does NOT cover top-level nullability. Compare it explicitly:
      // publishing a nullable-with-null file under a required-field schema would
      // commit but then fail every read (the read path trusts the declared type).
      if (prevFields[i]['required'] != newFields[i]['required']) return false;
    }
    return true;
  }

  /// Resolve an embedded metadata path against [root], then CONFINE it to the
  /// root unless [allowExternalPaths] — so a manifest naming an external absolute
  /// file can never be read or (in expiry) deleted. [confine] throws on escape.
  Future<String> _resolve(String root, String path) async {
    final resolved = await resolveIcebergPath(root, path);
    return allowExternalPaths ? resolved : confine(root, resolved, label: path);
  }
}
