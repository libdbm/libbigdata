import '../../core/column.dart';
import '../../core/keys.dart';
import '../../core/limits.dart';
import 'iceberg_catalog.dart';
import '../../core/paths.dart';
import '../../core/schema.dart';
import '../../core/table.dart';
import '../../core/columnar.dart';
import '../avro/avro_ocf_reader.dart';
import '../delta/roaring_bitmap.dart';
import '../parquet/parquet_reader.dart';
import 'iceberg_schema.dart';

/// One resolved equality-delete file: equality field ids + current names, the
/// value keys to match, its data sequence number, and its partition. Matches the
/// record [_equalityDeletes] returns (records are structural, so this is just a
/// readable alias for indexing/grouping deletes during a scan).
typedef _EqDelete = ({
  List<int> ids,
  List<String> cols,
  Set<RowKey> keys,
  int seq,
  RowKey part,
});

/// Reads an Apache Iceberg table: resolve the latest (or requested) snapshot
/// from `metadata/*.metadata.json`, walk the manifest list → manifests → data
/// files (Parquet), apply position/equality delete files, and unify to the table
/// schema. Time travel by snapshot id or timestamp.
/// Composite row key with value-equality (avoids string-join collisions/null
/// ambiguity) for equality-delete matching.
/// A resolved scan: the table schema, live data files (each with its data
/// sequence number), and the position/equality delete files to apply.
class _ScanPlan {
  final Schema schema;
  final List<({String path, int seq, RowKey part})> dataFiles;
  // Position deletes carry their data sequence number + partition (like data and
  // equality files), so v2 applicability can be enforced per data file instead
  // of applying a stale delete purely by referenced path.
  final List<({String path, int seq, RowKey part})> posDeletes;
  // Equality deletes carry both the field IDS (the spec's identity — files
  // written before a rename keep the old physical name, so per-file resolution
  // must go by id) and the current-schema names (fallback for files without
  // embedded field ids).
  final List<
    ({String path, List<int> ids, List<String> cols, int seq, RowKey part})
  >
  eqDeletes;
  _ScanPlan(this.schema, this.dataFiles, this.posDeletes, this.eqDeletes);
}

class IcebergTable {
  final String tablePath;

  /// When false (default), embedded data/delete paths are confined to the table
  /// root; set true only for tables that legitimately reference external
  /// locations.
  final bool allowExternalPaths;

  /// Ceiling on the combined resident bytes of all delete indexes (position +
  /// equality) held during a scan. The key-count cap alone doesn't bound memory:
  /// composite string/binary equality keys, or many sparse position bitmaps, can
  /// exhaust memory far below the key-count limit. Fails loud before OOM.
  final int deleteBudgetBytes;

  /// Resolves the current-metadata pointer. Defaults to the local-file scan;
  /// pass a catalog-backed [IcebergCatalog] so reads select the SAME current
  /// snapshot a catalog-backed writer commits to (rather than an uncommitted or
  /// losing local metadata file).
  final IcebergCatalog _catalog;

  /// GLOBAL cap on manifest/data-file/delete-file records retained while
  /// planning a scan. Per-Avro-file caps don't bound a snapshot made of many
  /// manifests, so the plan is streamed (one Avro record resident at a time) and
  /// the total light plan entries are capped here. Null opts out.
  final int? maxPlanEntries;

  IcebergTable(
    this.tablePath, {
    this.allowExternalPaths = false,
    this.deleteBudgetBytes = 256 << 20, // 256 MiB
    this.maxPlanEntries = defaultMaxMaterializedRows,
    IcebergCatalog? catalog,
  }) : _catalog = catalog ?? const FilesystemIcebergCatalog();

  /// Read the table (or a time-travel snapshot) into one materialized [Table].
  ///
  /// Per-file Parquet caps don't bound a table made of many files, so the total
  /// materialized rows/bytes are capped here too (stop-at-cap: reading halts the
  /// moment the running total would exceed the limit). Pass `maxRows: null` /
  /// `maxBytes: null` to opt out, or [readBatches] for bounded-memory streaming.
  Future<Table> read({
    int? snapshotId,
    int? asOf,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) async {
    final plan = await _plan(snapshotId, asOf);
    final batches = <Table>[];
    var rows = 0;
    var bytes = 0;
    await for (final b in _stream(plan)) {
      rows += b.rowCount;
      bytes += b.memoryUsage;
      enforceMaterializationCaps(
        rows,
        bytes,
        maxRows,
        maxBytes,
        'Iceberg result',
      );
      batches.add(b);
    }
    if (batches.isEmpty) return Table.empty('iceberg', plan.schema);
    return _unify(batches, plan.schema);
  }

  /// Stream the table one (delete-applied) batch at a time, reading each data
  /// file lazily so peak memory is a single batch plus the delete index — not
  /// the whole table.
  Stream<Table> readBatches({int? snapshotId, int? asOf}) async* {
    yield* _stream(await _plan(snapshotId, asOf));
  }

  /// Resolve the snapshot to its live data files (each with its data sequence
  /// number) and delete files. The sequence number governs which equality
  /// deletes apply to which data files (v2 rule).
  Future<_ScanPlan> _plan(int? snapshotId, int? asOf) async {
    final meta = await _latestMetadata();
    // Select the snapshot FIRST, then bind the schema by THAT snapshot's
    // schema-id — a time-traveled old snapshot (retained across a schema-changing
    // overwrite) must decode its files under the schema they were written with,
    // not the current one. Falls back to current-schema-id for pre-schema-id
    // metadata.
    final snapshot = _selectSnapshot(meta, snapshotId, asOf);
    final schemaJson = _schemaForSnapshot(meta, snapshot);
    final schema = IcebergSchema.fromJson(schemaJson);
    final idToName = {
      for (final f
          in (schemaJson['fields'] as List).cast<Map<String, dynamic>>())
        f['id'] as int: f['name'] as String,
    };

    if (snapshot == null) {
      return _ScanPlan(schema, const [], const [], const []);
    }

    // Stream the manifest list + each manifest one Avro record at a time (never
    // materializing a whole manifest), retaining only the light plan tuples and
    // failing loud once their total exceeds the GLOBAL budget — per-Avro-file
    // caps alone don't bound a snapshot made of many manifests.
    var entries = 0;
    void account() {
      if (maxPlanEntries != null && ++entries > maxPlanEntries!) {
        throw StateError(
          'Iceberg scan plan exceeds maxPlanEntries=$maxPlanEntries manifest/'
          'data/delete records across the snapshot; the table needs compaction '
          '(or pass maxPlanEntries: null to allow it) in $tablePath',
        );
      }
    }

    // Manifest list -> manifests, each carrying the sequence number a null
    // entry sequence inherits.
    final manifests = <({String path, int seq})>[];
    if (snapshot['manifest-list'] != null) {
      await for (final m in AvroOcfReader.streamPathAsync(
        await _resolve(snapshot['manifest-list'] as String),
      )) {
        account();
        manifests.add((
          path: await _resolve(m['manifest_path'] as String),
          seq: (m['sequence_number'] as int?) ?? 0,
        ));
      }
    } else {
      for (final m in (snapshot['manifests'] as List? ?? const [])) {
        account();
        manifests.add((path: await _resolve(m as String), seq: 0));
      }
    }

    // Manifests -> live data files (+ seq + partition) + delete files.
    final dataFiles = <({String path, int seq, RowKey part})>[];
    final posDeletes = <({String path, int seq, RowKey part})>[];
    final eqDeletes =
        <
          ({
            String path,
            List<int> ids,
            List<String> cols,
            int seq,
            RowKey part,
          })
        >[];
    for (final mf in manifests) {
      await for (final entry in AvroOcfReader.streamPathAsync(mf.path)) {
        if ((entry['status'] as int) == 2) continue; // DELETED
        account();
        final df = entry['data_file'] as Map;
        final content = (df['content'] as int?) ?? 0;
        final path = await _resolve(df['file_path'] as String);
        // Inherit the manifest's sequence number when the entry's is null.
        final seq =
            (entry['sequence_number'] as int?) ??
            (entry['file_sequence_number'] as int?) ??
            mf.seq;
        final part = _partKey(df['partition']);
        if (content == 0) {
          dataFiles.add((path: path, seq: seq, part: part));
        } else if (content == 1) {
          posDeletes.add((path: path, seq: seq, part: part));
        } else if (content == 2) {
          final ids = (df['equality_ids'] as List?)?.cast<int>() ?? const [];
          // An equality delete with NO field ids would build an empty RowKey for
          // every data row AND every delete record, matching EVERYTHING — every
          // row of every applicable file silently dropped. Reject the malformed
          // file instead. An unknown field id is likewise a format error (not a
          // null-key that degenerates the match).
          if (ids.isEmpty) {
            throw FormatException(
              'Iceberg equality-delete file $path declares no equality_ids; it '
              'must name at least one field (corrupt delete file)',
            );
          }
          eqDeletes.add((
            path: path,
            ids: ids,
            cols: [
              for (final i in ids)
                idToName[i] ??
                    (throw FormatException(
                      'Iceberg equality-delete file $path references field id '
                      '$i, which is not in the table schema (corrupt delete '
                      'file)',
                    )),
            ],
            seq: seq,
            part: part,
          ));
        }
      }
    }
    return _ScanPlan(schema, dataFiles, posDeletes, eqDeletes);
  }

  /// Stream each data file, applying position + (sequence-gated) equality
  /// deletes columnarly per batch.
  Stream<Table> _stream(_ScanPlan plan) async* {
    // Shared byte budget across BOTH delete indexes — they stay resident together
    // for the whole scan (position deletes are held while equality deletes load).
    final budget = _DeleteBudget(deleteBudgetBytes);
    final posByFile = await _positionDeletes(plan.posDeletes, budget);
    final eq = await _equalityDeletes(plan.eqDeletes, budget);
    // Index equality deletes by partition ONCE, so each data file consults only
    // its own partition's deletes instead of a linear scan of EVERY delete file
    // (the dataFiles × deleteFiles blowup). Unpartitioned tables share the one
    // empty partition key, so behaviour is unchanged.
    final eqByPart = <RowKey, List<_EqDelete>>{};
    for (final e in eq) {
      (eqByPart[e.part] ??= <_EqDelete>[]).add(e);
    }
    for (final df in plan.dataFiles) {
      // v2 rules: an equality delete only affects data files with a SMALLER data
      // sequence number (so a row re-inserted after a delete survives) AND in
      // the SAME partition (a delete for key K in partition A must not touch K
      // in partition B). Unpartitioned tables have an empty partition on both
      // sides, so they still match table-wide.
      final applicable = [
        for (final e in eqByPart[df.part] ?? const [])
          if (df.seq < e.seq) e,
      ];
      // Merge applicable deletes that share a field-id + column signature: they
      // resolve identical columns, so union their key sets and probe each row
      // against ONE set per distinct signature rather than once per delete file
      // (the rows × applicableDeleteFiles blowup).
      final bySig = <String, List<_EqDelete>>{};
      for (final e in applicable) {
        (bySig['${e.ids.join(',')} ${e.cols.join(',')}'] ??= <_EqDelete>[]).add(
          e,
        );
      }
      final groups = [
        for (final entry in bySig.values)
          (
            ids: entry.first.ids,
            cols: entry.first.cols,
            // Reuse a lone file's set (no copy); union only when 2+ files share
            // a signature.
            keys: entry.length == 1
                ? entry.first.keys
                : <RowKey>{for (final e in entry) ...e.keys},
          ),
      ];
      // v2 rule: a position delete applies to a data file only when the delete's
      // data sequence number is >= the data file's (a delete never removes rows
      // from a NEWER file version at the same path). Path-reference already
      // scopes each delete to its exact target file.
      final posBitmaps = [
        for (final pe in posByFile[df.path] ?? const [])
          if (pe.seq >= df.seq) pe.positions,
      ];
      var offset = 0; // running row index within this data file
      // readBatches now reassembles nested schemas per row group too, so a nested
      // file is bounded (one row group per batch) and the caller's caps apply per
      // yielded batch — no whole-file materialization.
      await for (final batch in ParquetReader(df.path).readBatches()) {
        // Resolve equality columns by FIELD ID against this file's own schema
        // (name fallback for id-less files): a data file written before a
        // rename keeps the old physical name, and resolving by current name
        // read a null column — RowKey([null]) then matched a null delete key,
        // silently deleting EVERY row.
        final eqCols = [
          for (final g in groups)
            [
              for (int j = 0; j < g.ids.length; j++)
                _fieldColumn(batch, g.ids[j], g.cols[j]),
            ],
        ];
        final keep = <int>[];
        for (int i = 0; i < batch.rowCount; i++) {
          final row = offset + i;
          if (posBitmaps.any((b) => b.contains(row))) continue;
          var deleted = false;
          for (int k = 0; k < groups.length; k++) {
            final key = RowKey([for (final column in eqCols[k]) column?[i]]);
            if (groups[k].keys.contains(key)) {
              deleted = true;
              break;
            }
          }
          if (!deleted) keep.add(i);
        }
        offset += batch.rowCount;
        if (keep.isEmpty) continue;
        final kept = keep.length == batch.rowCount ? batch : batch.filter(keep);
        yield _shape(kept, plan.schema);
      }
    }
  }

  // --- snapshot/metadata resolution ---------------------------------------

  Future<Map<String, dynamic>> _latestMetadata() async {
    final meta = await _catalog.current(tablePath);
    if (meta == null) {
      throw ArgumentError(
        'Not an Iceberg table (no current metadata): $tablePath',
      );
    }
    return meta;
  }

  Map<String, dynamic> _currentSchema(Map<String, dynamic> meta) {
    final schemas = (meta['schemas'] as List?)?.cast<Map<String, dynamic>>();
    if (schemas != null && schemas.isNotEmpty) {
      final id = meta['current-schema-id'] as int?;
      // A present-but-dangling current-schema-id is corrupt metadata: reading
      // the wrong schema silently mis-types every column. Fail closed. Only a
      // genuinely absent id falls back to the first schema.
      if (id != null) {
        return schemas.firstWhere(
          (s) => s['schema-id'] == id,
          orElse: () => throw FormatException(
            'Iceberg metadata references current-schema-id $id but no schema '
            'has that id (corrupt metadata) in $tablePath',
          ),
        );
      }
      return schemas.first;
    }
    return meta['schema'] as Map<String, dynamic>;
  }

  /// The schema JSON for [snapshot] — bound by the snapshot's own `schema-id` so a
  /// time-traveled snapshot decodes under the schema it was written with. Falls
  /// back to the current schema when the snapshot carries no `schema-id` (older
  /// metadata) or the table is empty.
  Map<String, dynamic> _schemaForSnapshot(
    Map<String, dynamic> meta,
    Map<String, dynamic>? snapshot,
  ) {
    final schemas = (meta['schemas'] as List?)?.cast<Map<String, dynamic>>();
    final sid = snapshot?['schema-id'];
    if (sid != null) {
      // A present-but-unresolvable schema-id is corrupt metadata: fail closed
      // rather than decode the snapshot under the WRONG (current) schema — the
      // silent-wrong-answer its hardened siblings (_currentSchema,
      // _selectSnapshot) already reject. Only a snapshot with NO schema-id
      // (legacy) legitimately falls back to the current schema.
      final match =
          schemas?.where((s) => s['schema-id'] == sid).toList() ?? const [];
      if (match.isNotEmpty) return match.first;
      throw FormatException(
        'Iceberg snapshot references schema-id $sid but no schema declares it',
      );
    }
    return _currentSchema(meta);
  }

  Map<String, dynamic>? _selectSnapshot(
    Map<String, dynamic> meta,
    int? snapshotId,
    int? asOf,
  ) {
    final snapshots =
        (meta['snapshots'] as List?)?.cast<Map<String, dynamic>>() ?? const [];
    if (snapshots.isEmpty) return null;
    if (snapshotId != null) {
      // Naming the id and the table matters more since expireSnapshots exists:
      // an expired snapshot is now the ordinary way to land here, and the bare
      // "Bad state: No element" firstWhere throws says neither what was asked
      // for nor why it is gone.
      return snapshots.firstWhere(
        (s) => s['snapshot-id'] == snapshotId,
        orElse: () => throw StateError(
          'Iceberg table $tablePath has no snapshot $snapshotId (it may have '
          'been expired); available: '
          '${snapshots.map((s) => s['snapshot-id']).join(', ')}',
        ),
      );
    }
    if (asOf != null) {
      Map<String, dynamic>? best;
      for (final s in snapshots) {
        if ((s['timestamp-ms'] as int) <= asOf) {
          if (best == null ||
              (s['timestamp-ms'] as int) > (best['timestamp-ms'] as int)) {
            best = s;
          }
        }
      }
      return best;
    }
    final current = meta['current-snapshot-id'];
    // A present, valid current-snapshot-id that matches no snapshot is a
    // dangling pointer (corrupt metadata) — returning some other snapshot would
    // silently serve unrelated data. Fail closed. Only an absent / -1 pointer
    // (no explicit current) falls back to the latest snapshot.
    if (current != null && current != -1) {
      return snapshots.firstWhere(
        (s) => s['snapshot-id'] == current,
        orElse: () => throw FormatException(
          'Iceberg metadata references current-snapshot-id $current but no '
          'snapshot has that id (corrupt metadata) in $tablePath',
        ),
      );
    }
    return snapshots.last;
  }

  /// Resolve an embedded (possibly `file://`, possibly stale-absolute) path,
  /// re-rooting under [tablePath] so copied tables still read. The result is
  /// confined to the table root unless [allowExternalPaths] — a malicious
  /// manifest can't point a data/delete file outside the table.
  Future<String> _resolve(String path) async {
    final resolved = await _resolveRaw(path);
    return allowExternalPaths
        ? resolved
        : confine(tablePath, resolved, label: path);
  }

  Future<String> _resolveRaw(String path) =>
      resolveIcebergPath(tablePath, path);

  // --- deletes -------------------------------------------------------------

  Future<Map<String, List<({RoaringBitmap positions, int seq})>>>
  _positionDeletes(
    List<({String path, int seq, RowKey part})> files,
    _DeleteBudget budget,
  ) async {
    // Per referenced data-file path, the position bitmaps from each delete file
    // TAGGED with that delete file's data sequence number, so applicability
    // (deleteSeq >= dataFileSeq) can be enforced per data file in _stream.
    final out = <String, List<({RoaringBitmap positions, int seq})>>{};
    // Cache resolution by raw path: a position-delete file holds one row PER
    // deleted row, all naming a handful of distinct data files — resolving each
    // row hit the filesystem (File.exists) repeatedly for the same path.
    final resolved = <String, String>{};
    // Running total of ALREADY-committed (finished-file) index bytes, kept as an
    // accumulator so a batch charges in O(this-file) rather than re-summing every
    // prior file's bitmaps (which was ~O(#delete-files²) over the whole scan).
    var committedBytes = 0;
    var prevBytes = 0; // last-charged total (committed + this file's pending)
    for (final f in files) {
      // This delete file's positions, grouped by the data-file path they target.
      final perFile = <String, RoaringBitmap>{};
      // Stream the delete file batch-by-batch into the position index rather
      // than materializing the whole Table and then a second copy via toRows():
      // peak memory is one batch + the index, not the entire delete file twice.
      // The index is a compressed RoaringBitmap, not a Set<int>, so hundreds of
      // millions of (often contiguous) positions cost bits, not ~50 bytes each.
      await for (final batch in ParquetReader(f.path).readBatches()) {
        for (final r in batch.toRows()) {
          final raw = r['file_path'] as String;
          final target = resolved[raw] ??= await _resolve(raw);
          (perFile[target] ??= RoaringBitmap()).add((r['pos'] as num).toInt());
        }
        // Charge the per-batch growth of the resident index. `pending` folds only
        // over THIS file's bitmaps (bounded by the data files it references).
        final pending = perFile.values.fold(0, (s, b) => s + b.sizeInBytes);
        budget.charge((committedBytes + pending) - prevBytes);
        prevBytes = committedBytes + pending;
      }
      perFile.forEach((target, bitmap) {
        (out[target] ??= []).add((positions: bitmap, seq: f.seq));
      });
      // This file's bytes are now committed; reset the pending baseline.
      committedBytes += perFile.values.fold(0, (s, b) => s + b.sizeInBytes);
      prevBytes = committedBytes;
    }
    return out;
  }

  Future<
    List<
      ({
        List<int> ids,
        List<String> cols,
        Set<RowKey> keys,
        int seq,
        RowKey part,
      })
    >
  >
  _equalityDeletes(
    List<
      ({String path, List<int> ids, List<String> cols, int seq, RowKey part})
    >
    files,
    _DeleteBudget budget,
  ) async {
    final out =
        <
          ({
            List<int> ids,
            List<String> cols,
            Set<RowKey> keys,
            int seq,
            RowKey part,
          })
        >[];
    // Bound the TOTAL resident key count across ALL delete files, not per file:
    // every file's set stays live for the whole scan, so a per-file cap let N
    // files hold N × cap keys in memory at once.
    var total = 0;
    for (final f in files) {
      // Stream into the key set per batch (no whole-Table double-materialize).
      // Equality-delete keys are value tuples (not ints), so they can't be
      // Roaring-compressed like position deletes — cap both the count AND the
      // resident bytes (composite string/binary keys blow memory well under the
      // count cap) so a pathologically over-deleted table fails loud, not OOM.
      final keys = <RowKey>{};
      await for (final batch in ParquetReader(f.path).readBatches()) {
        // Resolve the delete file's own columns by FIELD ID (name fallback):
        // a delete file written before a rename carries the old physical
        // name. A delete file that lacks a declared equality field entirely
        // is corrupt — a null key here would silently match null-keyed rows.
        final columns = [
          for (int j = 0; j < f.ids.length; j++)
            _fieldColumn(batch, f.ids[j], f.cols[j]) ??
                (throw FormatException(
                  'Iceberg equality-delete file ${f.path} does not contain '
                  'its declared equality field id ${f.ids[j]} '
                  '("${f.cols[j]}") — corrupt delete file',
                )),
        ];
        for (int i = 0; i < batch.rowCount; i++) {
          final parts = [for (final column in columns) column[i]];
          if (keys.add(RowKey(parts))) {
            total++;
            budget.charge(_keyBytes(parts));
          }
          // Honor the same instance-configurable cap as the plan-entry budget
          // (null opts out), rather than a hard-coded constant.
          if (maxPlanEntries != null && total > maxPlanEntries!) {
            throw StateError(
              'Iceberg equality-delete key set exceeds maxPlanEntries='
              '$maxPlanEntries keys across all delete files; pass '
              'maxPlanEntries: null to allow it or compact the table',
            );
          }
        }
      }
      out.add((ids: f.ids, cols: f.cols, keys: keys, seq: f.seq, part: f.part));
    }
    return out;
  }

  /// Conservative resident-byte estimate of one equality-delete key tuple. Uses
  /// the shared [rowKeyBytes]/[keyValueBytes] estimator (also used by SQL set-op
  /// memory budgeting) so key-cost accounting is consistent across the engine.
  static int _keyBytes(List<Object?> parts) {
    var bytes = 48; // RowKey + backing list overhead
    for (final v in parts) {
      bytes += keyValueBytes(v);
    }
    return bytes;
  }

  /// A canonical, order-independent key for a data/delete file's partition tuple
  /// (an Avro-decoded `map<name, value>`), so an equality delete is scoped to its
  /// own partition. Empty/absent → the same empty key, so unpartitioned tables
  /// match table-wide (unchanged behaviour).
  static RowKey _partKey(Object? partition) {
    if (partition is! Map) return RowKey(const []);
    final names = [for (final k in partition.keys) k.toString()]..sort();
    return RowKey([
      for (final n in names) ...[n, partition[n]],
    ]);
  }

  /// Project a data-file batch onto the table [schema], resolving each target
  /// column by FIELD-ID first (Iceberg's stable identity) and falling back to
  /// name, then null-filling absent columns. Id resolution is what makes a column
  /// RENAME correct: an old data file keeps the field's id but its old physical
  /// name, so a name-only match would read the renamed column as all-null (and a
  /// reused name could attach the wrong physical column).
  /// The column for equality field [id] in file table [t]: resolved by the
  /// file's own embedded field-id when carried (so a renamed column still
  /// matches — Iceberg identity is the id, and old files keep the old physical
  /// name), else by the current-schema [name]. Same rule [_shape] applies to
  /// output columns.
  Column? _fieldColumn(Table t, int id, String name) {
    for (final f in t.schema.fields) {
      if (f.id == id) return t.column(f.name);
    }
    return t.column(name);
  }

  Table _shape(Table t, Schema schema) {
    // Index the data file's columns by their field-id (when the file carries
    // ids — Iceberg/Spark writers, incl. this library, emit them).
    final byId = <int, Column>{};
    for (final f in t.schema.fields) {
      final id = f.id;
      if (id == null) continue;
      final col = t.column(f.name);
      if (col != null) byId[id] = col;
    }
    return Table(
      name: 'iceberg',
      schema: schema,
      columns: [
        for (final f in schema.fields)
          // A by-id (or old-name) match carries the file's PHYSICAL name; rename
          // it to the target field so the column matches the table schema.
          ((f.id != null ? byId[f.id] : null) ?? t.column(f.name))?.withName(
                f.name,
              ) ??
              columnFromValues(
                f.name,
                f.dataType,
                List.filled(t.rowCount, null),
              ),
      ],
    );
  }

  Table _unify(List<Table> tables, Schema schema) =>
      unifyTables('iceberg', tables, schema);
}

/// A running resident-byte budget shared across a scan's delete indexes, failing
/// loud once the estimated retained bytes exceed [cap].
class _DeleteBudget {
  _DeleteBudget(this.cap);
  final int cap;
  int _used = 0;

  void charge(int bytes) {
    if (bytes <= 0) return;
    _used += bytes;
    if (_used > cap) {
      throw StateError(
        'Iceberg delete indexes exceed the $cap-byte budget (position + '
        'equality deletes resident together); the table needs compaction or a '
        'larger deleteBudgetBytes',
      );
    }
  }
}
