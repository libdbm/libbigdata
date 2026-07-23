import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:logging/logging.dart';

import '../../core/conversions.dart'
    show dateFromEpochDays, timestampFromEpochMicros;
import '../../core/data_type.dart';
import '../../core/partitions.dart';
import '../../core/keys.dart';
import '../../core/limits.dart';
import '../../core/paths.dart';
import '../../core/staging.dart';
import 'roaring_bitmap.dart';
import '../../core/schema.dart';
import '../../core/stats.dart';
import '../../core/table.dart';
import '../parquet/parquet_reader.dart';
import '../parquet/parquet_writer.dart';
import 'actions.dart';
import 'delta_commit_coordinator.dart';
import 'deletion_vector.dart';
import 'delta_log.dart';
import 'delta_schema.dart';
import 'invariants.dart';
import '../../core/ids.dart' as ids;

// Secure entropy for UUIDs / file-name tokens: a clock-seeded Random() seeds
// identically across isolates/pods that boot in the same millisecond, which
// would collide part-file names and table UUIDs. (Backoff jitter below stays on
// the plain Random — it needs no uniqueness guarantee.)

enum DeltaWriteMode { append, overwrite }

/// Writes Delta Lake tables: creates a table (version 0 with protocol+metaData),
/// appends or overwrites data files, and performs merge-on-read deletes that
/// produce deletion vectors. Optionally Hive-partitioned (`partitionBy`), which
/// writes one data file per partition; an unpartitioned commit writes one.
class DeltaWriter {
  static final Logger _log = Logger('DeltaWriter');

  final String tablePath;

  /// When false (default), embedded data/DV paths read during a mutation are
  /// confined to the table root (see [DeltaTable.allowExternalPaths]).
  final bool allowExternalPaths;

  /// Max commit-conflict retries before giving up (bounds lock-spinning under
  /// heavy concurrent ingestion). Applies to the default coordinator.
  final int maxRetries;

  /// The commit seam. Defaults to [FileLockDeltaCommitCoordinator] — it holds an
  /// exclusive OS file lock over the whole claim → build → publish, giving
  /// genuine cross-PROCESS compare-and-swap (no lost update). It does NOT
  /// serialize separate Dart ISOLATES (POSIX locks are per-process); a bare
  /// [FilesystemDeltaCommitCoordinator] (best-effort, unfenced) remains available
  /// for single-writer use, and a catalog-backed coordinator for cross-engine CAS.
  final DeltaCommitCoordinator _coordinator;

  /// Evaluates `delta.invariants` for [write]. Without one, an
  /// invariant-carrying table is REJECTED rather than written unchecked — pass
  /// `SqlInvariantEvaluator()` (or use [DataFrameIO.writeDelta], which wires it)
  /// to enforce instead. It is injected because evaluating SQL needs the query
  /// engine, which sits above this layer.
  final InvariantEvaluator? _invariants;

  /// Reports staged files that a terminal-failure cleanup could NOT delete
  /// (orphans needing a vacuum). Delta deliberately doesn't fail the commit on a
  /// cleanup miss (that would mask the original error), and the warning it logs
  /// is dropped unless the app configured `logging`; this callback is the
  /// reliable signal an operator can wire without that. Called with the orphaned
  /// paths, in addition to the log.
  final void Function(List<String> orphans)? _onCleanupFailure;

  DeltaWriter(
    this.tablePath, {
    this.maxRetries = 16,
    this.allowExternalPaths = false,
    DeltaCommitCoordinator? coordinator,
    InvariantEvaluator? invariants,
    void Function(List<String> orphans)? onCleanupFailure,
  }) : _invariants = invariants,
       _onCleanupFailure = onCleanupFailure,
       _coordinator =
           coordinator ??
           FileLockDeltaCommitCoordinator(
             inner: FilesystemDeltaCommitCoordinator(maxRetries: maxRetries),
           );

  /// Fail closed before writing to a table whose protocol or declared properties
  /// this library can't fully honor. The protocol covers unsupported reader
  /// features (we'd read stale/wrong basis rows) and unsupported writer features
  /// (generated columns, column mapping); a null protocol is the
  /// pre-table-features baseline (writable).
  ///
  /// The protocol alone is not enough, because two contracts are carried by the
  /// metadata rather than by a feature flag, and both are legal at writer
  /// version 2 — the version this library itself creates:
  ///
  /// - `delta.appendOnly` forbids removing or replacing data, so every mutation
  ///   except an [append] is refused.
  /// - `delta.invariants` in a field's metadata is a per-row CHECK this library
  ///   does not evaluate yet, so ANY write (append included) is refused rather
  ///   than committing rows that might violate it.
  ///
  /// Tables this library creates set neither, so this only bites on a table
  /// another engine configured.
  void _ensureWritable(
    Snapshot snapshot, {
    required bool append,
    bool enforceable = false,
  }) {
    final protocol = snapshot.protocol;
    final reason =
        protocol?.readerUnsupported() ?? protocol?.writerUnsupported();
    if (reason != null) {
      throw UnsupportedError('Cannot mutate Delta table $tablePath: $reason');
    }
    final metadata = snapshot.metadata;
    if (!append && metadata.configuration['delta.appendOnly'] == 'true') {
      throw UnsupportedError(
        'Cannot mutate Delta table $tablePath: the table declares '
        'delta.appendOnly=true, which forbids removing or replacing data '
        '(append is still allowed)',
      );
    }
    if (enforceable) return; // the caller checks the data against them itself
    final constrained = DeltaSchema.fromJson(metadata.schemaString).fields
        .where((field) => field.metadata.containsKey('delta.invariants'))
        .map((field) => field.name);
    if (constrained.isNotEmpty) {
      throw UnsupportedError(
        'Cannot write to Delta table $tablePath: column(s) '
        '${constrained.join(', ')} declare delta.invariants, which this write '
        'path does not evaluate — writing could violate them. '
        '${append ? 'Pass invariants: SqlInvariantEvaluator() to enforce them.' : 'update/merge cannot enforce them yet; rewrite with write() instead.'}',
      );
    }
  }

  /// Reject a copy-on-write mutation ([operation]) on a PARTITIONED table.
  ///
  /// `update`/`merge` rewrite affected files by streaming their live rows and
  /// rebuilding against the full schema. On a partitioned table that silently
  /// corrupts: partition columns live in the directory path, not the data file,
  /// so the streamed rows carry none — the rebuilt file null-fills every
  /// partition column, lands in the table root (not `col=val/`), and records an
  /// empty `partitionValues`. The whole rewritten partition then reads back with
  /// null partition values. Fail loud until the per-partition rewrite exists;
  /// `delete` is unaffected (it only writes deletion vectors, never rewrites).
  void _rejectPartitioned(Snapshot snapshot, String operation) {
    final parts = snapshot.metadata.partitionColumns;
    if (parts.isNotEmpty) {
      throw UnsupportedError(
        'Delta $operation on $tablePath is not supported for partitioned '
        'tables (partitionColumns ${parts.join(', ')}): the copy-on-write '
        'rewrite cannot re-partition the files it rewrites. Overwrite the '
        'affected data with write(mode: overwrite) instead.',
      );
    }
  }

  /// Reject [table] if it violates any invariant the committed schema declares.
  ///
  /// Runs INSIDE the commit loop, against the metadata of the version actually
  /// claimed: an invariant added concurrently must still bind, and re-checking
  /// per attempt is what makes that true.
  Future<void> _checkInvariants(Snapshot snapshot, Table table) async {
    final evaluator = _invariants;
    if (evaluator == null) return;
    final invariants = parseInvariants(
      DeltaSchema.fromJson(snapshot.metadata.schemaString),
    );
    for (final invariant in invariants) {
      final violations = await evaluator.violations(table, invariant.predicate);
      if (violations > 0) {
        throw StateError(
          'Delta invariant violated on $tablePath: $violations row(s) fail '
          '"${invariant.predicate}" (declared by column '
          '"${invariant.column}") — no data was committed',
        );
      }
    }
  }

  /// Resolve an embedded [path] to a filesystem path, confined to the table root
  /// unless [allowExternalPaths]. Shared with the reader so a URI-scheme path
  /// resolves identically on both sides.
  String _resolve(String path) =>
      resolveDataFile(tablePath, path, allowExternal: allowExternalPaths);

  /// Write [table] as a new commit. On an empty location this creates the table
  /// (writer version 2). [mode] overwrite tombstones all current files first.
  /// [partitionBy] writes a Hive-partitioned table (`col=val/...` directories,
  /// one data file per distinct partition; partition columns are stored in the
  /// path, not the data files — Spark/delta-rs reconstruct them on read).
  /// [maxPartitions] caps the distinct-partition count (enforced as keys are
  /// discovered, so a mistaken high-cardinality `partitionBy` fails loud before
  /// millions of tiny files are written); pass null to allow it.
  Future<int> write(
    Table table, {
    DeltaWriteMode mode = DeltaWriteMode.append,
    List<String>? partitionBy,
    int? maxPartitions = defaultMaxPartitionsPerWrite,
  }) async {
    final log = DeltaLog(tablePath);
    final now = DateTime.now().millisecondsSinceEpoch;
    final partCols = partitionBy ?? const <String>[];

    // Reject partitioning by EVERY column: partition columns live in the path,
    // not the data file, so this would write zero-column (zero-row) data files
    // and silently commit a version containing NONE of the input rows. Spark and
    // delta-rs reject this too. Leave at least one non-partition data column.
    if (partCols.isNotEmpty &&
        partCols.toSet().containsAll(table.schema.fields.map((f) => f.name))) {
      throw ArgumentError(
        'Cannot partition by every column (${partCols.join(', ')}): partition '
        'columns are stored in the directory path, so this would write empty '
        'data files and lose every row. Leave at least one non-partition column.',
      );
    }

    await Directory(tablePath).create(recursive: true);
    final priorVersions = await _versions(log);
    final guess = priorVersions.isEmpty ? 0 : priorVersions.last + 1;

    final incoming = DeltaSchema.toJson(table.schema);

    // Each data file to add. `path` is the URI-encoded PROTOCOL path (logged in
    // the add action); `physical` is the real filesystem-relative path (used for
    // cleanup) — the two conventions must stay distinct. `stats` is precomputed
    // eagerly so the partition rows can be released right after the file is
    // written, instead of retaining a Table per partition until commit.
    final adds =
        <
          ({
            String path,
            String physical,
            Map<String, String> values,
            int size,
            String stats,
          })
        >[];
    try {
      // Write the data file(s) once, then claim the next version, re-deriving
      // the actions on each conflict (version-0 protocol/metadata; overwrite
      // removes from a fresh snapshot).
      if (partCols.isEmpty) {
        final fileName =
            'part-${guess.toString().padLeft(5, '0')}-${ids.uuid()}.parquet';
        await ParquetWriter('$tablePath/$fileName').write(table);
        adds.add((
          path: fileName,
          physical: fileName,
          values: const {},
          size: await File('$tablePath/$fileName').length(),
          stats: _stats(table),
        ));
      } else {
        for (final part in table.splitByKeys(
          partCols,
          maxPartitions: maxPartitions,
        )) {
          final dirSegs = [
            for (final k in partCols) '$k=${encodePartition(part.values[k])}',
          ];
          final dir = dirSegs.join('/');
          await Directory('$tablePath/$dir').create(recursive: true);
          final fileName =
              'part-${guess.toString().padLeft(5, '0')}-${ids.uuid()}.parquet';
          final rel = '$dir/$fileName';
          // Partition columns live in the path, not the data file.
          final data = _dropColumns(part.table, partCols);
          await ParquetWriter('$tablePath/$rel').write(data);
          // The LOGGED path is URL-encoded per the protocol — readers (ours at
          // _resolve, and Spark/delta-rs) Uri-decode it. The Hive-escaped `%XX`
          // in the on-disk name must therefore be re-encoded (`%` → `%25`) or
          // decoding either throws or lands on the wrong path.
          final logged = [
            for (final seg in dirSegs) Uri.encodeComponent(seg),
            fileName,
          ].join('/');
          adds.add((
            path: logged,
            physical: rel,
            // The MAP takes the protocol's per-type serialization; the PATH
            // segment above keeps the Hive marker. They are different
            // conventions — see core/partitions.dart.
            values: {
              for (final k in partCols)
                k: encodePartitionValue(
                  part.values[k],
                  table.schema.field(k)!.dataType,
                  scale: table.schema.field(k)!.scale ?? 0,
                ),
            },
            size: await File('$tablePath/$rel').length(),
            // Stats are deterministic and version-independent, so compute them
            // now and let `data` be released — no Table retained until commit.
            stats: _stats(data),
          ));
        }
      }
      // The coordinator owns the reclaim + version-claim + fenced publish loop;
      // build() re-derives the actions for the chosen version on every attempt
      // so a commit is always tied to the version (and committed state) it reads.
      final v = await _coordinator.commit(
        log: log,
        label: 'append',
        build: (version) async {
          final actions = <Map<String, dynamic>>[];
          if (version == 0) {
            actions.add(Protocol(1, 2).toJson());
            actions.add(
              Metadata(
                id: ids.uuid(),
                schemaString: incoming,
                partitionColumns: partCols,
                createdTime: now,
              ).toJson(),
            );
          } else {
            // Re-read the latest committed state on EVERY attempt so the commit
            // is tied to the version it actually reads. An append must match the
            // table's committed schema (the read path null-pads divergent files,
            // so a mismatch would silently produce a mixed-schema table).
            final snap = await log.snapshot();
            _ensureWritable(
              snap,
              append: mode == DeltaWriteMode.append,
              enforceable: _invariants != null,
            );
            await _checkInvariants(snap, table);
            if (mode == DeltaWriteMode.append) {
              final committed = snap.metadata.schemaString;
              if (!DeltaSchema.sameStructure(committed, incoming)) {
                throw StateError(
                  'Delta append schema mismatch for $tablePath:\n'
                  '  committed: $committed\n'
                  '  incoming:  $incoming',
                );
              }
              // The partitioning must ALSO match the committed metadata: an
              // append with a different partitionBy would write partition values
              // that contradict the table's declared partitionColumns (e.g.
              // appending without partitionBy to a partitioned table declares an
              // empty partition map while the column is physically in the file),
              // which Spark/delta-rs mis-prune. Fail loud (overwrite may change
              // the partitioning; append may not).
              if (!_sameList(snap.metadata.partitionColumns, partCols)) {
                throw StateError(
                  'Delta append partitioning mismatch for $tablePath: committed '
                  'partitionColumns ${snap.metadata.partitionColumns} vs incoming '
                  '$partCols (use overwrite to change partitioning)',
                );
              }
            } else if (mode == DeltaWriteMode.overwrite) {
              // Overwrite replaces all data. If the incoming schema (or the
              // partitioning) differs, REPLACE the table metaData too (Delta
              // overwriteSchema) — otherwise the committed metaData would keep
              // declaring the old schema over new-schema data files and every
              // read would null-pad / mis-bind (silent wrong data). Preserve the
              // table id / name / config / createdTime so time-travel and the
              // table identity stay intact.
              final prev = snap.metadata;
              if (prev.schemaString != incoming ||
                  !_sameList(prev.partitionColumns, partCols)) {
                actions.add(
                  Metadata(
                    id: prev.id,
                    name: prev.name,
                    schemaString: incoming,
                    partitionColumns: partCols,
                    configuration: prev.configuration,
                    createdTime: prev.createdTime,
                  ).toJson(),
                );
              }
              // _removeOf, not a hand-built RemoveFile: a tombstone is matched
              // against the live set by (path, deletionVector) — RemoveFile.key
              // — so one built without the add's DV matches NOTHING, and the
              // file it meant to remove stays live alongside the new data. The
              // other two tombstone sites already reuse this.
              for (final add in snap.files) {
                actions.add(_removeOf(add, now));
              }
            }
          }
          for (final add in adds) {
            actions.add(
              AddFile(
                path: add.path,
                partitionValues: add.values,
                size: add.size,
                modificationTime: now,
                dataChange: true,
                stats: add.stats,
              ).toJson(),
            );
          }
          return actions;
        },
      );
      await _maybeCheckpoint(log, v);
      return v;
    } catch (e) {
      // Retain staged files on a conflict (they're reused on retry) AND on an
      // indeterminate outcome (the commit may have landed referencing them).
      if (e is! CommitConflict && e is! DeltaCommitIndeterminate) {
        await _cleanup([for (final a in adds) a.physical]);
      }
      rethrow;
    }
  }

  static bool _sameList(List<String> a, List<String> b) {
    if (a.length != b.length) return false;
    for (int i = 0; i < a.length; i++) {
      if (a[i] != b[i]) return false;
    }
    return true;
  }

  /// A copy of [table] without the [drop] columns (partition columns are stored
  /// in the path, not the Delta data file).
  static Table _dropColumns(Table table, List<String> drop) {
    final keep = drop.toSet();
    final cols = [
      for (final c in table.columns)
        if (!keep.contains(c.name)) c,
    ];
    return Table(
      name: table.name,
      columns: cols,
      schema: Schema(
        fields: [
          for (final f in table.schema.fields)
            if (!keep.contains(f.name)) f,
        ],
      ),
    );
  }

  /// Delete rows matching [predicate] via deletion vectors (merge-on-read): no
  /// data files are rewritten. Returns the committed version, or -1 if nothing
  /// matched. Requires the `deletionVectors` writer feature, which this method
  /// upgrades the protocol to on its FIRST deletion — `write` creates a table at
  /// the plain writer version 2, so a table that is only ever appended to stays
  /// readable by writers that lack the feature.
  Future<int> delete(bool Function(Map<String, dynamic> row) predicate) async {
    final log = DeltaLog(tablePath);
    final snapshot = await log.snapshot();
    _ensureWritable(snapshot, append: false);
    final now = DateTime.now().millisecondsSinceEpoch;
    final actions = <Map<String, dynamic>>[];

    // The first deletion vector requires the table to advertise the
    // `deletionVectors` reader/writer feature (reader v3 / writer v7).
    final protocol = snapshot.protocol;
    final dvEnabled =
        protocol != null &&
        (protocol.readerFeatures?.contains('deletionVectors') ?? false);

    // Partition columns live only in the log's STRING map; the predicate must
    // see them at their LOGICAL types (an int-partition `row['p'] == 5`
    // compared 5 against '5' and silently deleted nothing). Decode via the
    // same codec the reader uses, keyed by the table schema.
    final tableSchema = DeltaSchema.fromJson(snapshot.metadata.schemaString);

    final written = <String>[];
    try {
      for (final add in snapshot.files) {
        final dataPath = _resolve(add.path);

        // Compressed delete index (bits/element). Mutated in place: index is strictly
        // increasing across the file, so a just-added position is never re-checked,
        // and the contains() skip still excludes only the ORIGINAL deletes.
        final deleted = add.deletionVector == null
            ? RoaringBitmap()
            : await DeletionVector.load(
                add.deletionVector!,
                tablePath,
                allowExternalPaths: allowExternalPaths,
              );
        final originalCount = deleted.length;
        // Stream the file a batch at a time; never materialise the whole file.
        // Batch order matches physical row order, so base+i is the row index the
        // deletion vector addresses.
        var base = 0;
        final parts = <String, Object?>{
          for (final e in add.partitionValues.entries)
            e.key: decodePartitionValue(
              e.value,
              tableSchema.field(e.key)?.dataType ?? DataType.string,
              scale: tableSchema.field(e.key)?.scale ?? 0,
              column: e.key,
            ),
        };
        await for (final batch in ParquetReader(dataPath).readBatches()) {
          final rows = batch.toRows();
          for (int i = 0; i < rows.length; i++) {
            final index = base + i;
            if (deleted.contains(index)) continue;
            // Avoid a second per-row Map allocation when there are no partition
            // columns to merge in (the common, non-partitioned case).
            final row = parts.isEmpty ? rows[i] : {...rows[i], ...parts};
            if (predicate(row)) deleted.add(index);
          }
          base += rows.length;
        }
        if (deleted.length == originalCount) {
          continue; // nothing new in this file
        }

        final dv = await DeletionVector.write(tablePath, deleted.values);
        written.add(dv.path);

        actions.add(
          RemoveFile(
            path: add.path,
            deletionTimestamp: now,
            dataChange: true,
            deletionVector: add.deletionVector,
          ).toJson(),
        );
        actions.add(add.withDeletionVector(dv.dv).toJson());
      }
    } catch (_) {
      await _cleanup(written);
      rethrow;
    }

    if (actions.isEmpty) return -1;
    if (!dvEnabled) {
      // Upgrading to writer table-features mode (v7) must explicitly list the
      // features implied by the table's previous writer version (v2 implies
      // appendOnly + invariants); Spark rejects the table otherwise.
      actions.insert(
        0,
        Protocol(
          3,
          7,
          readerFeatures: const ['deletionVectors'],
          writerFeatures: const ['appendOnly', 'invariants', 'deletionVectors'],
        ).toJson(),
      );
    }
    return _commitMutation(log, actions, written, snapshot);
  }

  /// Update rows matching [predicate], applying [assignments] (column ->
  /// new-value-from-row) via copy-on-write: each affected data file is rewritten
  /// with the changes and its old `add` tombstoned. Returns the committed
  /// version, or -1 if nothing matched.
  Future<int> update(
    FutureOr<bool> Function(Map<String, dynamic> row) predicate,
    Map<String, FutureOr<Object?> Function(Map<String, dynamic> row)>
    assignments,
  ) async {
    final log = DeltaLog(tablePath);
    final snapshot = await log.snapshot();
    _ensureWritable(snapshot, append: false);
    _rejectPartitioned(snapshot, 'update');
    final schema = DeltaSchema.fromJson(snapshot.metadata.schemaString);
    // Validate assignment destinations up front: an assignment to a column not
    // in the table schema (a typo) would otherwise be silently dropped when the
    // rewritten rows are rebuilt against the fixed schema — committing a new
    // version that changed nothing.
    final fields = {for (final f in schema.fields) f.name};
    for (final column in assignments.keys) {
      if (!fields.contains(column)) {
        throw ArgumentError(
          'Update assigns to "$column", which is not a column of $tablePath '
          '(${fields.join(', ')})',
        );
      }
    }
    final now = DateTime.now().millisecondsSinceEpoch;
    final version = (await log.versions()).last + 1;
    final actions = <Map<String, dynamic>>[];
    final written = <String>[];

    try {
      // Evaluate [predicate] exactly ONCE per live row, in a first streaming
      // pass, caching each match in a bitmap keyed by live-row ordinal. The
      // rewrite pass reuses those cached decisions instead of re-running the
      // predicate, so a non-deterministic callback (random/stateful/clock) can
      // no longer decide a row differently between the "is this file affected"
      // scan and the rewrite. A file with no match is skipped (no rewrite), so
      // a selective update still touches only affected files; only their small
      // decision bitmaps are retained, not their rows (memory stays bounded —
      // the rewrite still streams). Reducing the double READ needs declarative,
      // statistics-prunable predicates and is left as future work.
      for (final add in snapshot.files) {
        final matched = RoaringBitmap();
        var index = 0;
        var any = false;
        await for (final row in _liveStream(add)) {
          if (await predicate(row)) {
            matched.add(index);
            any = true;
          }
          index++;
        }
        if (!any) continue; // file untouched -> skip
        var cursor = 0;
        final newAdd = await _writeStream(
          _liveStream(add).asyncMap((row) async {
            if (!matched.contains(cursor++)) return row;
            final updated = {...row};
            for (final entry in assignments.entries) {
              updated[entry.key] = await entry.value(row);
            }
            return updated;
          }),
          schema,
          version,
          now,
        );
        actions.add(_removeOf(add, now));
        if (newAdd != null) {
          actions.add(newAdd.toJson());
          written.add(newAdd.path);
        }
      }
    } catch (_) {
      await _cleanup(written);
      rethrow;
    }

    if (actions.isEmpty) return -1;
    return _commitMutation(log, actions, written, snapshot);
  }

  /// Merge [source] into the table on equi-key columns [on] (upsert): matched
  /// target rows are updated ([whenMatchedUpdate]) or deleted
  /// ([whenMatchedDelete]); unmatched source rows are inserted
  /// ([whenNotMatchedInsert]). Copy-on-write, single atomic commit.
  Future<int> merge(
    Table source, {
    required List<String> on,
    Map<
      String,
      Object? Function(Map<String, dynamic> target, Map<String, dynamic> src)
    >?
    whenMatchedUpdate,
    bool whenMatchedDelete = false,
    bool whenNotMatchedInsert = true,
    int? maxSourceRows = defaultMaxMaterializedRows,
    int? maxSourceBytes = defaultMaxMaterializedBytes,
  }) async {
    final log = DeltaLog(tablePath);
    final snapshot = await log.snapshot();
    _ensureWritable(snapshot, append: false);
    _rejectPartitioned(snapshot, 'merge');
    final schema = DeltaSchema.fromJson(snapshot.metadata.schemaString);
    final now = DateTime.now().millisecondsSinceEpoch;
    final version = (await log.versions()).last + 1;

    // Require join keys present in BOTH schemas: a key missing from the source
    // would read as null via `row[k]`, mis-classifying every source row as
    // unmatched and inserting `{key: null, …}` into the target.
    if (on.isEmpty) {
      throw ArgumentError('Delta merge requires at least one join key (on)');
    }
    final targetFields = {for (final f in schema.fields) f.name};
    final sourceFields = source.schema.fields.map((f) => f.name).toSet();
    for (final k in on) {
      if (!targetFields.contains(k) || !sourceFields.contains(k)) {
        throw ArgumentError(
          'Delta merge join key "$k" must be a column of both the source and '
          'the target ($tablePath)',
        );
      }
    }

    // Structural key: joining values into a string collides ((1,23) and (12,3)
    // both -> "123"), which would match the wrong target rows - see [RowKey].
    RowKey keyOf(Map<String, dynamic> r) => RowKey([for (final k in on) r[k]]);
    // NOTE: the source is the build side and is materialized whole (keyed by
    // join key); the target is streamed batch-by-batch, so peak memory is
    // bounded by the SOURCE size. Cap it fail-loud (opt out with
    // maxSourceRows: null) — a spilling columnar hash-merge is a known follow-up.
    if (maxSourceRows != null && source.rowCount > maxSourceRows) {
      throw StateError(
        'Delta merge source has ${source.rowCount} rows, over the '
        '$maxSourceRows cap; pass maxSourceRows: null to allow it (the source '
        'is materialized in memory) or pre-aggregate/split the source',
      );
    }
    // Bound the source by BYTES too: a small row count of very large string /
    // binary cells can exceed memory well under the row cap. Merge doesn't just
    // hold the source — it also builds `srcRows` (a Map per row), the `srcByKey`
    // index, per-row `RowKey`s, the `matched` set, and an `inserts` table, so
    // true peak is a MULTIPLE of the raw source bytes. Gate through `overByteCap`
    // (the shared `eagerReadOverheadFactor`, the same honest-ceiling accounting
    // every other eager path uses) rather than comparing raw `memoryUsage`, which
    // under-counted exactly the operation that allocates the most. `matched` and
    // `inserts` are each bounded by the (now-gated) source, so this one gate
    // bounds them all. Null opts out.
    if (overByteCap(source.memoryUsage, maxSourceBytes)) {
      throw StateError(
        'Delta merge source is ${source.memoryUsage} bytes (×'
        '$eagerReadOverheadFactor overhead for the in-memory index/row-maps it '
        'builds), over the $maxSourceBytes cap; pass maxSourceBytes: null to '
        'allow it or pre-aggregate/split the source',
      );
    }
    final srcRows = source.toRows();
    final mutates = whenMatchedDelete || whenMatchedUpdate != null;
    // Build the source lookup with SQL semantics: (a) a source row whose join
    // key contains a NULL never matches a target (`NULL = NULL` is UNKNOWN), so
    // it is excluded from the match set and falls to NOT-MATCHED/insert; (b) when
    // a MATCHED action is present, two source rows sharing a join key is a
    // cardinality violation (which target does the update/delete use?) — fail
    // loud instead of silently keeping the last (data corruption).
    final srcByKey = <RowKey, Map<String, dynamic>>{};
    for (final s in srcRows) {
      final parts = [for (final k in on) s[k]];
      if (parts.any((p) => p == null)) continue; // null key never matches
      final key = RowKey(parts);
      if (srcByKey.containsKey(key) && mutates) {
        throw StateError(
          'Delta MERGE cardinality violation: multiple source rows match join '
          'key $parts; dedupe the source before merging',
        );
      }
      srcByKey[key] = s;
    }
    final matched = <RowKey>{};
    final actions = <Map<String, dynamic>>[];
    final written = <String>[];

    // Stage all rewritten files inside a try so a failure mid-staging (a decoder
    // error, a bad assignment closure, a filesystem error on a later file)
    // cleans up the files already written this transaction rather than orphaning
    // them. Commit-phase failures are handled inside [_commitMutation].
    try {
      // Pass 1: stream every file to record matched keys (for insert exclusion)
      // and which files an action actually changes.
      final changedFiles = <AddFile>[];
      for (final add in snapshot.files) {
        var changed = false;
        await for (final row in _liveStream(add)) {
          final key = keyOf(row);
          if (!srcByKey.containsKey(key)) continue;
          matched.add(key);
          if (mutates) changed = true;
        }
        if (changed) changedFiles.add(add);
      }

      // Pass 2: stream-rewrite only the changed files (one batch in memory).
      for (final add in changedFiles) {
        final newAdd = await _writeStream(
          _liveStream(add).expand((row) {
            final src = srcByKey[keyOf(row)];
            if (src == null) return [row];
            if (whenMatchedDelete) return const <Map<String, dynamic>>[];
            if (whenMatchedUpdate != null) {
              final updated = {...row};
              whenMatchedUpdate.forEach(
                (column, fn) => updated[column] = fn(row, src),
              );
              return [updated];
            }
            return [row];
          }),
          schema,
          version,
          now,
        );
        actions.add(_removeOf(add, now));
        if (newAdd != null) {
          actions.add(newAdd.toJson());
          written.add(newAdd.path);
        }
      }

      if (whenNotMatchedInsert) {
        final inserts = [
          for (final s in srcRows)
            if (!matched.contains(keyOf(s))) s,
        ];
        if (inserts.isNotEmpty) {
          final add = await _writeData(
            Table.fromRows('t', inserts, schema: schema),
            version,
            now,
          );
          actions.add(add.toJson());
          written.add(add.path);
        }
      }
    } catch (_) {
      await _cleanup(written);
      rethrow;
    }

    if (actions.isEmpty) return -1;
    return _commitMutation(log, actions, written, snapshot);
  }

  /// Stream the live rows of a data file (physical rows minus its deletion
  /// vector), holding only one parquet batch in memory at a time.
  Stream<Map<String, dynamic>> _liveStream(AddFile add) async* {
    final path = _resolve(add.path);
    final deleted = add.deletionVector == null
        ? RoaringBitmap()
        : await DeletionVector.load(
            add.deletionVector!,
            tablePath,
            allowExternalPaths: allowExternalPaths,
          );
    var base = 0;
    await for (final batch in ParquetReader(path).readBatches()) {
      final rows = batch.toRows();
      for (int i = 0; i < rows.length; i++) {
        if (!deleted.contains(base + i)) yield rows[i];
      }
      base += rows.length;
    }
  }

  /// Write [rows] to a new data file in batches (peak memory ~one batch),
  /// accumulating Delta `add.stats` as it goes. Returns the `add` action, or
  /// null if no row was produced (the caller then tombstones with no
  /// replacement file).
  Future<AddFile?> _writeStream(
    Stream<Map<String, dynamic>> rows,
    Schema schema,
    int version,
    int now,
  ) async {
    final fileName =
        'part-${version.toString().padLeft(5, '0')}-${ids.uuid()}.parquet';
    final writer = ParquetWriter('$tablePath/$fileName');
    final stats = _StatsAcc();
    final buffer = <Map<String, dynamic>>[];
    var total = 0;

    Future<void> flush() async {
      if (buffer.isEmpty) return;
      final table = Table.fromRows('t', buffer, schema: schema);
      await writer.writeBatch(table);
      stats.add(table);
      total += buffer.length;
      buffer.clear();
    }

    try {
      await for (final row in rows) {
        buffer.add(row);
        if (buffer.length >= 10000) await flush();
      }
      await flush();
      if (total == 0) {
        await writer.abort();
        return null;
      }
      // close() (footer + atomic rename) and the stat/AddFile build are INSIDE
      // the guard: a footer/disk-full/rename/stat failure here would otherwise
      // throw AFTER the caller couldn't record the file, orphaning it.
      await writer.close();
      return AddFile(
        path: fileName,
        size: await File('$tablePath/$fileName').length(),
        modificationTime: now,
        dataChange: true,
        stats: stats.json(),
      );
    } catch (_) {
      // Clean up on ANY failure (mid-stream, finalize, or stat): abort() closes
      // an unfinalized writer's fd + deletes its temp; if close() already landed
      // the file at its real path, delete that too — the orphan is gone before
      // the original error rethrows, so the caller's cleanup can't miss it.
      try {
        await writer.abort();
      } catch (_) {}
      try {
        final f = File('$tablePath/$fileName');
        if (await f.exists()) await f.delete();
      } catch (_) {}
      rethrow;
    }
  }

  /// Write [table] as a new data file and return its `add` action.
  Future<AddFile> _writeData(Table table, int version, int now) async {
    final fileName =
        'part-${version.toString().padLeft(5, '0')}-${ids.uuid()}.parquet';
    await ParquetWriter('$tablePath/$fileName').write(table);
    return AddFile(
      path: fileName,
      size: await File('$tablePath/$fileName').length(),
      modificationTime: now,
      dataChange: true,
      stats: _stats(table),
    );
  }

  Map<String, dynamic> _removeOf(AddFile add, int now) => RemoveFile(
    path: add.path,
    deletionTimestamp: now,
    dataChange: true,
    deletionVector: add.deletionVector,
  ).toJson();

  static const _checkpointInterval = 10;

  /// Commit a copy-on-write mutation with optimistic retry and orphan cleanup.
  /// The mutation's matched / not-matched decisions were derived from [base], so
  /// on a version conflict we re-read the log and abort unless the new snapshot
  /// is still a safe basis: every file we tombstone must still be live AND no
  /// file may have been added since [base] (a concurrent append could turn a
  /// MERGE insert into a duplicate, or hide rows an UPDATE/DELETE should touch).
  /// Otherwise the already-written data/DV files remain valid and we retry.
  Future<int> _commitMutation(
    DeltaLog log,
    List<Map<String, dynamic>> actions,
    List<String> written,
    Snapshot base,
  ) async {
    final baseKeys = {for (final f in base.files) f.key};
    try {
      final v = await _coordinator.commit(
        log: log,
        label: 'mutation',
        build: (version) async {
          // version == latest + 1. Whenever the log advanced past our base,
          // re-validate the basis: a removed file we depend on, or any file
          // added since base, means our matched/not-matched decisions may be
          // stale. Abort rather than commit a non-serializable result.
          if (version - 1 > base.version) {
            final fresh = await log.snapshot();
            if (!_disjoint(fresh, actions) ||
                fresh.files.any((f) => !baseKeys.contains(f.key))) {
              throw StateError(
                'Delta mutation aborted: a concurrent commit changed the table '
                'since this transaction read it (retry the mutation)',
              );
            }
          }
          return actions;
        },
      );
      await _maybeCheckpoint(log, v);
      return v;
    } catch (e) {
      // Same as append: keep staged files on a conflict (reused on retry) and on
      // an indeterminate commit (a landed commit may reference them).
      if (e is! CommitConflict && e is! DeltaCommitIndeterminate) {
        await _cleanup(written);
      }
      rethrow;
    }
  }

  /// Write a consolidated checkpoint every [_checkpointInterval] commits. Purely
  /// an optimization: a failure here never fails the (already durable) commit.
  ///
  /// Swallowing the FAILURE is deliberate; swallowing the SIGNAL was not. A
  /// checkpoint that keeps failing leaves readers replaying an ever-growing log
  /// with nothing to show for it, so it is logged.
  Future<void> _maybeCheckpoint(DeltaLog log, int version) async {
    if (version > 0 && version % _checkpointInterval == 0) {
      try {
        await log.checkpoint(version);
      } catch (e, s) {
        _log.warning(
          'Delta checkpoint at version $version failed for $tablePath; the '
          'commit stands, but readers will replay a longer log until a later '
          'checkpoint succeeds',
          e,
          s,
        );
      }
    }
  }

  /// Delete the data/DV files this transaction wrote (on a terminal failure).
  /// Best-effort: a delete that fails is left as an orphan rather than masking
  /// the original commit error (contrast Iceberg, which surfaces such failures)
  /// — but it is logged AND reported to [_onCleanupFailure] (if set), since an
  /// orphan that nothing reports is an orphan nobody reclaims.
  Future<void> _cleanup(Iterable<String> names) async {
    final failed = await deleteStaged([
      for (final n in names) n.startsWith('/') ? n : '$tablePath/$n',
    ]);
    if (failed.isNotEmpty) {
      _log.warning(
        'Delta cleanup could not remove ${failed.length} staged file(s) in '
        '$tablePath; they are orphaned and need a vacuum: ${failed.join('; ')}',
      );
      _onCleanupFailure?.call(List.unmodifiable(failed));
    }
  }

  /// True iff every file [actions] tombstones is still live in [fresh] — i.e. no
  /// concurrent commit already removed a file this transaction depends on.
  bool _disjoint(Snapshot fresh, List<Map<String, dynamic>> actions) {
    final live = {for (final f in fresh.files) f.key};
    for (final a in actions) {
      final rm = a['remove'];
      if (rm != null &&
          !live.contains(
            RemoveFile.fromJson((rm as Map).cast<String, dynamic>()).key,
          )) {
        return false;
      }
    }
    return true;
  }

  Future<List<int>> _versions(DeltaLog log) async {
    if (!await Directory(log.logDir).exists()) return const [];
    return log.versions();
  }

  String _stats(Table table) => (_StatsAcc()..add(table)).json();
}

/// Accumulates Delta `add.stats` (numRecords + per-column min/max/nullCount)
/// across batches, so a streamed rewrite needn't hold the whole file. The
/// min/max/null accumulation is the shared [StatsAccumulator] (also used by the
/// Iceberg writer); this class renders it in Delta's JSON stats form.
class _StatsAcc {
  final _stats = <String, StatsAccumulator>{};
  final _fields = <String, Field>{};
  int _rows = 0;

  void add(Table table) {
    _rows += table.rowCount;
    for (final column in table.columns) {
      final field = table.schema.field(column.name);
      if (field != null) _fields[column.name] ??= field;
      (_stats[column.name] ??= StatsAccumulator()).addColumn(column);
    }
  }

  /// Serialize a stat bound per the column's LOGICAL type — the spec's JSON
  /// stats carry date as `yyyy-MM-dd`, timestamp as ISO-8601, decimal as its
  /// scaled decimal string (all shared with the partitionValues serialization).
  /// Emitting the raw storage int (epoch days/micros) or unscaled BigInt made
  /// external engines parse garbage bounds for these columns.
  Object? _render(String name, Object? v) {
    if (v == null) return null;
    final field = _fields[name];
    switch (field?.dataType) {
      case DataType.date:
        final date = v is DateTime ? v : dateFromEpochDays(v as int);
        return encodePartitionValue(date, DataType.date);
      case DataType.timestamp:
        final time = v is DateTime ? v : timestampFromEpochMicros(v as int);
        return encodePartitionValue(time, DataType.timestamp);
      case DataType.decimal:
        return encodePartitionValue(
          v,
          DataType.decimal,
          scale: field!.scale ?? 0,
        );
      default:
        return v;
    }
  }

  String json() => jsonEncode({
    'numRecords': _rows,
    'minValues': {
      for (final e in _stats.entries)
        if (e.value.min != null) e.key: _render(e.key, e.value.min),
    },
    'maxValues': {
      for (final e in _stats.entries)
        if (e.value.max != null) e.key: _render(e.key, e.value.max),
    },
    'nullCount': {for (final e in _stats.entries) e.key: e.value.nulls},
  });
}
