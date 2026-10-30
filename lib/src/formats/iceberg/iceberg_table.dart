import 'dart:convert';
import 'dart:typed_data';

import '../../core/column.dart';
import '../../core/data_type.dart';
import '../../core/keys.dart';
import '../../core/limits.dart';
import 'iceberg_catalog.dart';
import '../../core/paths.dart';
import '../../core/row_filter.dart';
import '../../core/schema.dart';
import '../../core/table.dart';
import '../../core/columnar.dart';
import '../avro/avro_ocf_reader.dart';
import '../delta/roaring_bitmap.dart';
import '../parquet/parquet_reader.dart';
import 'iceberg_schema.dart';
import '../../core/failure.dart';
import '../../core/result.dart';
import 'package:meta/meta.dart';

/// One resolved equality-delete file: equality field ids + current names, the
/// value keys to match, its data sequence number, and its partition. Matches the
/// record [_equalityDeletes] returns (records are structural, so this is just a
/// readable alias for indexing/grouping deletes during a scan).
typedef _EqualityDelete = ({
  List<int> ids,
  List<String> columns,
  Set<RowKey> keys,
  int sequence,
  RowKey part,
});

/// A resolved scan: the table schema, live data files (each with its data
/// sequence number), and the position/equality delete files to apply.
class _ScanPlan {
  final Schema schema;
  final List<({String path, int sequence, RowKey part})> dataFiles;
  // Position deletes carry their data sequence number + partition (like data and
  // equality files), so v2 applicability can be enforced per data file instead
  // of applying a stale delete purely by referenced path.
  final List<({String path, int sequence, RowKey part})> positionDeletes;
  // Equality deletes carry both the field IDS (the spec's identity: files
  // written before a rename keep the old physical name, so per-file resolution
  // must go by id) and the current-schema names (fallback for files without
  // embedded field ids).
  final List<
    ({
      String path,
      List<int> ids,
      List<String> columns,
      int sequence,
      RowKey part,
    })
  >
  equalityDeletes;
  // Field ids `schema.name-mapping.default` gives top-level column names, for
  // files that carry no ids of their own (see IcebergTable._mapped).
  final Map<String, int> mapping;
  _ScanPlan(
    this.schema,
    this.dataFiles,
    this.positionDeletes,
    this.equalityDeletes, [
    this.mapping = const {},
  ]);
}

/// Reads an Apache Iceberg table: resolve the latest (or requested) snapshot
/// from `metadata/*.metadata.json`, walk the manifest list → manifests → data
/// files (Parquet), apply position/equality delete files, and unify to the table
/// schema. Time travel by snapshot id or timestamp.
final class IcebergTable {
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

  /// Diagnostic counter: manifests opened by scan planning, across all
  /// tables. A manifest the manifest list's partition summaries rule out is
  /// not opened.
  @internal
  static int manifestsRead = 0;

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
  ///
  /// A failure is reported as an [Err]: [Kind.missing] for a path with no
  /// Iceberg metadata or an unknown or expired snapshot, [Kind.corrupt] for
  /// metadata naming a missing schema or file, [Kind.unsupported] for a format
  /// version or file format this library does not read, [Kind.denied] for a
  /// path outside the table, and [Kind.limit] past a cap.
  @useResult
  Future<Result<Table, ErrorDetails>> read({
    int? snapshotId,
    int? asOf,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => capture(
    () => _read(
      snapshotId: snapshotId,
      asOf: asOf,
      maxRows: maxRows,
      maxBytes: maxBytes,
    ),
    fallback: Kind.corrupt,
  );

  Future<Table> _read({
    int? snapshotId,
    int? asOf,
    int? maxRows,
    int? maxBytes,
  }) async {
    final plan = await _plan(snapshotId, asOf);
    final batches = <Table>[];
    var rows = 0;
    var bytes = 0;
    await for (final batch in _stream(plan)) {
      rows += batch.rowCount;
      bytes += batch.memoryUsage;
      enforceMaterializationCaps(
        rows,
        bytes,
        maxRows,
        maxBytes,
        'Iceberg result',
      );
      batches.add(batch);
    }
    if (batches.isEmpty) return Table.empty('iceberg', plan.schema);
    return _unify(batches, plan.schema);
  }

  /// The schema of the table (or of a time-travel snapshot), from its
  /// metadata alone (no manifest or data file is read).
  ///
  /// A failure is reported as an [Err], as [read] reports it.
  @useResult
  Future<Result<Schema, ErrorDetails>> schema({int? snapshotId, int? asOf}) =>
      capture(() async {
        final metadata = await _latestMetadata();
        return _schema(metadata, snapshotId, asOf);
      }, fallback: Kind.corrupt);

  /// The current schema and the current metadata it was read from, reported
  /// as [schema] reports it: a lazy reader reads that metadata at every
  /// terminal, so its current snapshot and schema stay those of this call.
  @internal
  @useResult
  Future<Result<(Schema, Map<String, dynamic>), ErrorDetails>> head() =>
      capture(() async {
        final metadata = await _latestMetadata();
        return (_schema(metadata, null, null), metadata);
      }, fallback: Kind.corrupt);

  /// The schema a read of [metadata] at [snapshotId] or [asOf] binds.
  Schema _schema(Map<String, dynamic> metadata, int? snapshotId, int? asOf) {
    final snapshot = _selectSnapshot(metadata, snapshotId, asOf);
    return IcebergSchema.fromJson(
      _readSchema(metadata, snapshot, snapshotId, asOf),
    );
  }

  /// Stream the table one (delete-applied) batch at a time, reading each data
  /// file lazily so peak memory is a single batch plus the delete index, not
  /// the whole table.
  ///
  /// A [filter] skips each data manifest whose manifest-list partition
  /// summaries, and each data file whose partition values or column bounds,
  /// prove it holds no matching row. Delete manifests and files are always
  /// read. It is a pruning hint: kept batches may still hold rows that do not
  /// match, so the caller filters exactly.
  ///
  /// [columns] limits each batch to those table columns, in table order, and
  /// reads from each data file only them and the keys of the equality deletes
  /// that apply to it, matched by field id so a renamed column still reads.
  ///
  /// Each batch arrives as an [Ok]; a failure as one [Err], reported as [read]
  /// reports it, which ends the stream.
  ///
  /// A data file is read one row group at a time, so a larger row group is
  /// sliced into batches of [batchSize] rows. A [batchSize] below 1 is a
  /// programming error and throws at the call.
  Stream<Result<Table, ErrorDetails>> readBatches({
    int? snapshotId,
    int? asOf,
    int batchSize = 10000,
    RowFilter? filter,
    List<String>? columns,
  }) {
    if (batchSize <= 0) {
      throw ArgumentError.value(batchSize, 'batchSize', 'must be > 0');
    }
    return captureStream(() async* {
      await for (final batch in _stream(
        await _plan(snapshotId, asOf, filter),
        columns,
      )) {
        for (var start = 0; start < batch.rowCount; start += batchSize) {
          final end = start + batchSize;
          yield start == 0 && end >= batch.rowCount
              ? batch
              : batch.slice(start, end < batch.rowCount ? end : batch.rowCount);
        }
      }
    }, fallback: Kind.corrupt);
  }

  /// Resolve the snapshot to its live data files (each with its data sequence
  /// number) and delete files. The sequence number governs which equality
  /// deletes apply to which data files (v2 rule).
  Future<_ScanPlan> _plan(
    int? snapshotId,
    int? asOf, [
    RowFilter? filter,
  ]) async {
    final metadata = await _latestMetadata();
    // v3 adds deletion vectors, row lineage and new types this reader does not
    // decode; reading such a table as v2 would silently return wrong rows.
    final format = metadata['format-version'] ?? 1;
    if (format != 1 && format != 2) {
      throw Failure.of(
        Kind.unsupported,
        'Iceberg format-version $format is not supported (1 and 2 are) in '
        '$tablePath',
      );
    }
    // Select the snapshot FIRST, then bind the schema by THAT snapshot's
    // schema-id: a time-traveled old snapshot (retained across a schema-changing
    // overwrite) must decode its files under the schema they were written with,
    // not the current one. Falls back to current-schema-id for pre-schema-id
    // metadata. A current read uses the current schema, as Iceberg's engines
    // do: a rename or added column commits no snapshot, and the snapshot's own
    // schema kept the old names.
    final snapshot = _selectSnapshot(metadata, snapshotId, asOf);
    final definition = _readSchema(metadata, snapshot, snapshotId, asOf);
    final schema = IcebergSchema.fromJson(definition);
    final names = {
      for (final field
          in (definition['fields'] as List).cast<Map<String, dynamic>>())
        field['id'] as int: field['name'] as String,
    };

    if (snapshot == null) {
      return _ScanPlan(schema, const [], const [], const []);
    }
    final mapping = _mapping(metadata);

    // Stream the manifest list + each manifest one Avro record at a time (never
    // materializing a whole manifest), retaining only the light plan tuples and
    // failing loud once their total exceeds the GLOBAL budget, per-Avro-file
    // caps alone don't bound a snapshot made of many manifests.
    var entries = 0;
    void account() {
      if (maxPlanEntries != null && ++entries > maxPlanEntries!) {
        throw Failure.of(
          Kind.limit,
          'Iceberg scan plan exceeds maxPlanEntries=$maxPlanEntries manifest/'
          'data/delete records across the snapshot; the table needs compaction '
          '(or pass maxPlanEntries: null to allow it) in $tablePath',
        );
      }
    }

    // Manifest list -> manifests, each carrying the sequence number a null
    // entry sequence inherits.
    final specs = filter == null ? null : _specs(metadata, names);
    final manifests = <({String path, int sequence, int spec})>[];
    if (snapshot['manifest-list'] != null) {
      await for (final manifest in AvroOcfReader.streamPathAsync(
        await _resolve(snapshot['manifest-list'] as String),
      )) {
        account();
        final spec = (manifest['partition_spec_id'] as int?) ?? 0;
        // Only a DATA manifest is skipped: a delete manifest can hold deletes
        // that apply beyond its own partitions.
        if (filter != null &&
            ((manifest['content'] as int?) ?? 0) == 0 &&
            !_summariesMayMatch(
              manifest['partitions'],
              specs![spec],
              filter,
              schema,
            )) {
          continue;
        }
        manifests.add((
          path: await _resolve(manifest['manifest_path'] as String),
          sequence: (manifest['sequence_number'] as int?) ?? 0,
          spec: spec,
        ));
      }
    } else {
      for (final manifest in (snapshot['manifests'] as List? ?? const [])) {
        account();
        manifests.add((
          path: await _resolve(manifest as String),
          sequence: 0,
          spec: 0,
        ));
      }
    }

    // Manifests -> live data files (+ sequence + partition) + delete files.
    final dataFiles = <({String path, int sequence, RowKey part})>[];
    final positionDeletes = <({String path, int sequence, RowKey part})>[];
    final equalityDeletes =
        <
          ({
            String path,
            List<int> ids,
            List<String> columns,
            int sequence,
            RowKey part,
          })
        >[];
    for (final manifest in manifests) {
      manifestsRead++;
      await for (final entry in AvroOcfReader.streamPathAsync(manifest.path)) {
        if ((entry['status'] as int) == 2) continue; // DELETED
        account();
        final file = entry['data_file'] as Map;
        final content = (file['content'] as int?) ?? 0;
        final path = await _resolve(file['file_path'] as String);
        // Inherit the manifest's sequence number when the entry's is null.
        final sequence =
            (entry['sequence_number'] as int?) ??
            (entry['file_sequence_number'] as int?) ??
            manifest.sequence;
        // Only Parquet is decoded; an Avro or ORC file read as Parquet would
        // fail as corrupt, or worse, be skipped by a later fallback.
        final kind = (file['file_format'] as String?)?.toUpperCase();
        if (kind != null && kind != 'PARQUET') {
          throw Failure.of(
            Kind.unsupported,
            'Iceberg ${const ['data', 'position-delete', 'equality-delete'][content.clamp(0, 2)]} '
            'file $path is $kind; only Parquet is supported',
          );
        }
        final part = _partKey(manifest.spec, file['partition']);
        if (content == 0) {
          if (filter != null &&
              !_fileMayMatch(file, specs![manifest.spec], filter, schema)) {
            continue;
          }
          dataFiles.add((path: path, sequence: sequence, part: part));
        } else if (content == 1) {
          positionDeletes.add((path: path, sequence: sequence, part: part));
        } else if (content == 2) {
          final ids = (file['equality_ids'] as List?)?.cast<int>() ?? const [];
          // An equality delete with NO field ids would build an empty RowKey for
          // every data row AND every delete record, matching EVERYTHING: every
          // row of every applicable file silently dropped. Reject the malformed
          // file instead. An unknown field id is likewise a format error (not a
          // null-key that degenerates the match).
          if (ids.isEmpty) {
            throw FormatException(
              'Iceberg equality-delete file $path declares no equality_ids; it '
              'must name at least one field (corrupt delete file)',
            );
          }
          equalityDeletes.add((
            path: path,
            ids: ids,
            columns: [
              for (final i in ids)
                names[i] ??
                    (throw FormatException(
                      'Iceberg equality-delete file $path references field id '
                      '$i, which is not in the table schema (corrupt delete '
                      'file)',
                    )),
            ],
            sequence: sequence,
            part: part,
          ));
        }
      }
    }
    return _ScanPlan(
      schema,
      dataFiles,
      positionDeletes,
      equalityDeletes,
      mapping,
    );
  }

  /// The field ids `schema.name-mapping.default` gives top-level column
  /// names, or empty without one. Tables built from existing files (Spark's
  /// `migrate`, `add_files`) carry it, and pyiceberg keeps a renamed column's
  /// old name in it, so a file without ids still reads by identity.
  static Map<String, int> _mapping(Map<String, dynamic> metadata) {
    final text =
        (metadata['properties'] as Map?)?['schema.name-mapping.default'];
    if (text is! String) return const {};
    final entries = jsonDecode(text);
    if (entries is! List) {
      throw const FormatException(
        'Iceberg schema.name-mapping.default is not a JSON list',
      );
    }
    return {
      for (final entry in entries.whereType<Map>())
        if (entry['field-id'] case final int id)
          for (final name in (entry['names'] as List? ?? const []))
            if (name is String) name: id,
    };
  }

  /// [table] with each column stamped with the field id [mapping] gives its
  /// name, when the file carries no ids at all. Matching then goes by id, as
  /// for any file with ids: a column the mapping does not name is absent.
  static Table _mapped(Table table, Map<String, int> mapping) {
    if (mapping.isEmpty || table.schema.fields.any((f) => f.id != null)) {
      return table;
    }
    return Table(
      name: table.name,
      columns: table.columns,
      schema: Schema(
        fields: [
          for (final field in table.schema.fields)
            if (mapping[field.name] case final id?)
              field.copyWith(id: id)
            else
              field,
        ],
      ),
    );
  }

  /// Stream each data file, applying position + (sequence-bounded) equality
  /// deletes columnarly per batch.
  Stream<Table> _stream(_ScanPlan plan, [List<String>? columns]) async* {
    // A projection naming a column the table lacks is an error: dropping it
    // silently returned fewer columns than asked for.
    final unknown = [
      for (final name in columns ?? const <String>[])
        if (plan.schema.field(name) == null) name,
    ];
    if (unknown.isNotEmpty) {
      throw Failure.of(
        Kind.invalid,
        'Unknown column(s) ${unknown.join(', ')} in $tablePath',
      );
    }
    final schema = columns == null
        ? plan.schema
        : Schema(
            fields: [
              for (final field in plan.schema.fields)
                if (columns.contains(field.name)) field,
            ],
          );
    // Shared byte budget across BOTH delete indexes; they stay resident together
    // for the whole scan (position deletes are held while equality deletes load).
    final budget = _DeleteBudget(deleteBudgetBytes);
    final positions = await _positionDeletes(plan.positionDeletes, budget);
    final deletes = await _equalityDeletes(
      plan.equalityDeletes,
      plan.mapping,
      budget,
    );
    // Index equality deletes by partition ONCE, so each data file consults only
    // its own partition's deletes instead of a linear scan of EVERY delete file
    // (the dataFiles × deleteFiles blowup). Unpartitioned tables share the one
    // empty partition key, so behaviour is unchanged.
    final byPartition = <RowKey, List<_EqualityDelete>>{};
    for (final delete in deletes) {
      (byPartition[delete.part] ??= <_EqualityDelete>[]).add(delete);
    }
    for (final file in plan.dataFiles) {
      // v2 rules: an equality delete only affects data files with a SMALLER data
      // sequence number (so a row re-inserted after a delete survives) AND in
      // the SAME partition (a delete for key K in partition A must not touch K
      // in partition B). Unpartitioned tables have an empty partition on both
      // sides, so they still match table-wide.
      final applicable = [
        for (final candidate in [
          ...?byPartition[file.part],
          // An equality delete under an unpartitioned spec applies table-wide
          // (to every spec's data files), as the spec requires.
          if (file.part != _global) ...?byPartition[_global],
        ])
          if (file.sequence < candidate.sequence) candidate,
      ];
      // Merge applicable deletes that share a field-id + column signature: they
      // resolve identical columns, so union their key sets and probe each row
      // against ONE set per distinct signature rather than once per delete file
      // (the rows × applicableDeleteFiles blowup).
      final signatures = <String, List<_EqualityDelete>>{};
      for (final delete in applicable) {
        (signatures['${delete.ids.join(',')}\u0000${delete.columns.join(',')}'] ??=
                <_EqualityDelete>[])
            .add(delete);
      }
      final groups = [
        for (final entry in signatures.values)
          (
            ids: entry.first.ids,
            columns: entry.first.columns,
            // Reuse a lone file's set (no copy); union only when 2+ files share
            // a signature.
            keys: entry.length == 1
                ? entry.first.keys
                : <RowKey>{for (final delete in entry) ...delete.keys},
          ),
      ];
      // v2 rule: a position delete applies to a data file only when the delete's
      // data sequence number is >= the data file's (a delete never removes rows
      // from a NEWER file version at the same path). Path-reference already
      // scopes each delete to its exact target file.
      final bitmaps = [
        for (final entry in positions[file.path] ?? const [])
          if (entry.sequence >= file.sequence) entry.positions,
      ];
      var offset = 0; // running row index within this data file
      final probe = ParquetReader(file.path);
      final names = columns == null
          ? null
          : await _fileColumns(
              probe,
              plan.mapping,
              {
                for (final field in schema.fields)
                  if (field.id != null) field.id!,
                for (final group in groups) ...group.ids,
              },
              {
                for (final field in schema.fields) field.name,
                for (final group in groups) ...group.columns,
              },
            );
      // readBatches now reassembles nested schemas per row group too, so a nested
      // file is bounded (one row group per batch) and the caller's caps apply per
      // yielded batch: no whole-file materialization.
      await for (final raw in ParquetReader(
        file.path,
        options: ParquetReaderOptions(columns: names),
        footer: names == null ? null : probe.metadata,
      ).readBatches().unwrap()) {
        final batch = _mapped(raw, plan.mapping);
        // Resolve equality columns by FIELD ID against this file's own schema
        // (name fallback for id-less files): a data file written before a
        // rename keeps the old physical name, and resolving by current name
        // read a null column: RowKey([null]) then matched a null delete key,
        // silently deleting EVERY row.
        final resolved = [
          for (final group in groups)
            [
              for (int j = 0; j < group.ids.length; j++)
                _fieldColumn(batch, group.ids[j], group.columns[j]),
            ],
        ];
        final keep = <int>[];
        for (int i = 0; i < batch.rowCount; i++) {
          final row = offset + i;
          if (bitmaps.any((bitmap) => bitmap.contains(row))) continue;
          var deleted = false;
          for (int k = 0; k < groups.length; k++) {
            final key = RowKey([for (final column in resolved[k]) column?[i]]);
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
        yield _shape(kept, schema);
      }
    }
  }

  /// The columns of the data file [reader] opens whose field id is in [ids]
  /// (in a file without field ids, the id [mapping] gives its name, or with
  /// no mapping, whose name is in [names]: the same matches [_shape] and
  /// [_fieldColumn] make after [_mapped]), or null to read them all from a
  /// nested file, whose columns are not projected. A file holding none of
  /// them still reads one, for its row count.
  static Future<List<String>?> _fileColumns(
    ParquetReader reader,
    Map<String, int> mapping,
    Set<int> ids,
    Set<String> names,
  ) => projected(reader, (fields) {
    final carries = fields.any((field) => field.id != null);
    final keyed = carries || mapping.isNotEmpty;
    return [
      for (final field in fields)
        if (keyed
            ? ids.contains(carries ? field.id : mapping[field.name])
            : names.contains(field.name))
          field.name,
    ];
  });

  // --- snapshot/metadata resolution ---------------------------------------

  Future<Map<String, dynamic>> _latestMetadata() async {
    final metadata = await _catalog.current(tablePath).unwrap();
    if (metadata == null) {
      throw Failure.of(
        Kind.missing,
        'Not an Iceberg table (no current metadata): $tablePath',
      );
    }
    return metadata;
  }

  /// The schema JSON a read binds: the current schema for a current read,
  /// the snapshot's own for time travel ([snapshotId] or [asOf]). The
  /// snapshot's schema is resolved either way, so a dangling schema-id is
  /// corrupt metadata for every read.
  Map<String, dynamic> _readSchema(
    Map<String, dynamic> metadata,
    Map<String, dynamic>? snapshot,
    int? snapshotId,
    int? asOf,
  ) {
    final own = _schemaForSnapshot(metadata, snapshot);
    return snapshotId == null && asOf == null
        ? currentSchema(metadata, tablePath)
        : own;
  }

  /// The schema JSON for [snapshot]: bound by the snapshot's own `schema-id` so a
  /// time-traveled snapshot decodes under the schema it was written with. Falls
  /// back to the current schema when the snapshot carries no `schema-id` (older
  /// metadata) or the table is empty.
  Map<String, dynamic> _schemaForSnapshot(
    Map<String, dynamic> metadata,
    Map<String, dynamic>? snapshot,
  ) {
    final schemas = (metadata['schemas'] as List?)
        ?.cast<Map<String, dynamic>>();
    final id = snapshot?['schema-id'];
    if (id != null) {
      // A present-but-unresolvable schema-id is corrupt metadata: fail closed
      // rather than decode the snapshot under the WRONG (current) schema: the
      // silent-wrong-answer its hardened siblings (currentSchema,
      // _selectSnapshot) already reject. Only a snapshot with NO schema-id
      // (legacy) legitimately falls back to the current schema.
      final match =
          schemas?.where((schema) => schema['schema-id'] == id).toList() ??
          const [];
      if (match.isNotEmpty) return match.first;
      throw FormatException(
        'Iceberg snapshot references schema-id $id but no schema declares it',
      );
    }
    return currentSchema(metadata, tablePath);
  }

  Map<String, dynamic>? _selectSnapshot(
    Map<String, dynamic> metadata,
    int? snapshotId,
    int? asOf,
  ) {
    final snapshots =
        (metadata['snapshots'] as List?)?.cast<Map<String, dynamic>>() ??
        const [];
    if (snapshots.isEmpty) return null;
    if (snapshotId != null) {
      // Naming the id and the table matters more since expireSnapshots exists:
      // an expired snapshot is now the ordinary way to land here, and the bare
      // "Bad state: No element" firstWhere throws says neither what was asked
      // for nor why it is gone.
      return snapshots.firstWhere(
        (snapshot) => snapshot['snapshot-id'] == snapshotId,
        orElse: () => throw Failure.of(
          Kind.missing,
          'Iceberg table $tablePath has no snapshot $snapshotId (it may have '
          'been expired); available: '
          '${snapshots.map((snapshot) => snapshot['snapshot-id']).join(', ')}',
        ),
      );
    }
    if (asOf != null) {
      // Time travel walks snapshot-log, the record of which snapshot was
      // current when: after a rollback the current snapshot is an OLDER one,
      // which the snapshots' own timestamps cannot show. Without a log, the
      // snapshots' timestamps are the history.
      final log = (metadata['snapshot-log'] as List?)?.cast<Map>() ?? const [];
      final history = log.isNotEmpty ? log : snapshots;
      Object? id;
      var best = -1;
      for (final entry in history) {
        final stamp = entry['timestamp-ms'] as int;
        if (stamp <= asOf && stamp >= best) {
          best = stamp;
          id = entry['snapshot-id'];
        }
      }
      if (best < 0) {
        throw Failure.of(
          Kind.missing,
          'Iceberg table $tablePath has no snapshot older than $asOf ms',
        );
      }
      return snapshots.firstWhere(
        (snapshot) => snapshot['snapshot-id'] == id,
        orElse: () => throw Failure.of(
          Kind.missing,
          'Iceberg table $tablePath was at snapshot $id at $asOf ms, but that '
          'snapshot has been expired',
        ),
      );
    }
    return currentSnapshot(metadata, tablePath);
  }

  /// Resolve an embedded (possibly `file://`, possibly stale-absolute) path,
  /// re-rooting under [tablePath] so copied tables still read. The result is
  /// confined to the table root unless [allowExternalPaths]: a malicious
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

  Future<Map<String, List<({RoaringBitmap positions, int sequence})>>>
  _positionDeletes(
    List<({String path, int sequence, RowKey part})> files,
    _DeleteBudget budget,
  ) async {
    // Per referenced data-file path, the position bitmaps from each delete file
    // TAGGED with that delete file's data sequence number, so applicability
    // (deleteSeq >= dataFileSeq) can be enforced per data file in _stream.
    final out = <String, List<({RoaringBitmap positions, int sequence})>>{};
    // Cache resolution by raw path: a position-delete file holds one row PER
    // deleted row, all naming a handful of distinct data files: resolving each
    // row hit the filesystem (File.exists) repeatedly for the same path.
    final resolved = <String, String>{};
    // Running total of ALREADY-committed (finished-file) index bytes, kept as an
    // accumulator so a batch charges in O(this-file) rather than re-summing every
    // prior file's bitmaps (which was ~O(#delete-files²) over the whole scan).
    var committed = 0;
    var previous = 0; // last-charged total (committed + this file's pending)
    for (final file in files) {
      // This delete file's positions, grouped by the data-file path they target.
      final bitmaps = <String, RoaringBitmap>{};
      // Stream the delete file batch-by-batch into the position index rather
      // than materializing the whole Table and then a second copy via toRows():
      // peak memory is one batch + the index, not the entire delete file twice.
      // The index is a compressed RoaringBitmap, not a Set<int>, so hundreds of
      // millions of (often contiguous) positions cost bits, not ~50 bytes each.
      await for (final batch in ParquetReader(
        file.path,
      ).readBatches().unwrap()) {
        for (final row in batch.toRows()) {
          final raw = row['file_path'] as String;
          final target = resolved[raw] ??= await _resolve(raw);
          (bitmaps[target] ??= RoaringBitmap()).add(
            (row['pos'] as num).toInt(),
          );
        }
        // Charge the per-batch growth of the resident index. `pending` folds only
        // over THIS file's bitmaps (bounded by the data files it references).
        final pending = bitmaps.values.fold(
          0,
          (sum, bitmap) => sum + bitmap.sizeInBytes,
        );
        budget.charge((committed + pending) - previous);
        previous = committed + pending;
      }
      bitmaps.forEach((target, bitmap) {
        (out[target] ??= []).add((positions: bitmap, sequence: file.sequence));
      });
      // This file's bytes are now committed; reset the pending baseline.
      committed += bitmaps.values.fold(
        0,
        (sum, bitmap) => sum + bitmap.sizeInBytes,
      );
      previous = committed;
    }
    return out;
  }

  Future<
    List<
      ({
        List<int> ids,
        List<String> columns,
        Set<RowKey> keys,
        int sequence,
        RowKey part,
      })
    >
  >
  _equalityDeletes(
    List<
      ({
        String path,
        List<int> ids,
        List<String> columns,
        int sequence,
        RowKey part,
      })
    >
    files,
    Map<String, int> mapping,
    _DeleteBudget budget,
  ) async {
    final out =
        <
          ({
            List<int> ids,
            List<String> columns,
            Set<RowKey> keys,
            int sequence,
            RowKey part,
          })
        >[];
    // Bound the TOTAL resident key count across ALL delete files, not per file:
    // every file's set stays live for the whole scan, so a per-file cap let N
    // files hold N × cap keys in memory at once.
    var total = 0;
    for (final file in files) {
      // Stream into the key set per batch (no whole-Table double-materialize).
      // Equality-delete keys are value tuples (not ints), so they can't be
      // Roaring-compressed like position deletes: cap both the count AND the
      // resident bytes (composite string/binary keys blow memory well under the
      // count cap) so a pathologically over-deleted table fails loud, not OOM.
      final keys = <RowKey>{};
      await for (final raw in ParquetReader(file.path).readBatches().unwrap()) {
        final batch = _mapped(raw, mapping);
        // Resolve the delete file's own columns by FIELD ID (name fallback):
        // a delete file written before a rename carries the old physical
        // name. A delete file that lacks a declared equality field entirely
        // is corrupt: a null key here would silently match null-keyed rows.
        final columns = [
          for (int j = 0; j < file.ids.length; j++)
            _fieldColumn(batch, file.ids[j], file.columns[j]) ??
                (throw FormatException(
                  'Iceberg equality-delete file ${file.path} does not contain '
                  'its declared equality field id ${file.ids[j]} '
                  '("${file.columns[j]}"): corrupt delete file',
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
            throw Failure.of(
              Kind.limit,
              'Iceberg equality-delete key set exceeds maxPlanEntries='
              '$maxPlanEntries keys across all delete files; pass '
              'maxPlanEntries: null to allow it or compact the table',
            );
          }
        }
      }
      out.add((
        ids: file.ids,
        columns: file.columns,
        keys: keys,
        sequence: file.sequence,
        part: file.part,
      ));
    }
    return out;
  }

  /// Conservative resident-byte estimate of one equality-delete key tuple. Uses
  /// the shared [rowKeyBytes]/[keyValueBytes] estimator (also used by SQL set-op
  /// memory budgeting) so key-cost accounting is consistent across the engine.
  static int _keyBytes(List<Object?> parts) {
    var bytes = 48; // RowKey + backing list overhead
    for (final value in parts) {
      bytes += keyValueBytes(value);
    }
    return bytes;
  }

  /// The partition key of a file under an unpartitioned spec.
  static final _global = RowKey(const []);

  /// A canonical, order-independent key for a data/delete file's partition
  /// [spec] and tuple (an Avro-decoded `map<name, value>`), so an equality
  /// delete is scoped to the same spec and partition. Empty/absent → the
  /// [_global] key whatever the spec, so unpartitioned tables
  /// match table-wide (unchanged behaviour).
  static RowKey _partKey(int spec, Object? partition) {
    if (partition is! Map || partition.isEmpty) return _global;
    final names = [for (final key in partition.keys) key.toString()]..sort();
    return RowKey([
      spec,
      for (final name in names) ...[name, partition[name]],
    ]);
  }

  /// Each partition spec's fields by spec id, in order: the partition field
  /// name, and the source column's name when the transform is identity (the
  /// only transform whose values bound the column itself).
  static Map<int, List<({String name, String? column})>> _specs(
    Map<String, dynamic> metadata,
    Map<int, String> names,
  ) => {
    for (final spec
        in (metadata['partition-specs'] as List? ?? const [])
            .cast<Map<String, dynamic>>())
      (spec['spec-id'] as int? ?? 0): [
        for (final field
            in (spec['fields'] as List? ?? const [])
                .cast<Map<String, dynamic>>())
          (
            name: field['name'] as String,
            column: field['transform'] == 'identity'
                ? names[field['source-id']]
                : null,
          ),
      ],
  };

  /// Whether a data manifest might hold a matching row, judged from its
  /// manifest-list partition [summaries] (one per field of its spec, in
  /// order). Absent summaries keep the manifest.
  static bool _summariesMayMatch(
    Object? summaries,
    List<({String name, String? column})>? fields,
    RowFilter filter,
    Schema schema,
  ) {
    if (summaries is! List || fields == null) return true;
    final bounds = <String, ColumnBounds>{};
    for (var i = 0; i < fields.length && i < summaries.length; i++) {
      final column = fields[i].column;
      if (column == null || !filter.columns.contains(column)) continue;
      final type = schema.field(column)?.dataType;
      final summary = summaries[i];
      if (type == null || !ColumnBounds.judged(type) || summary is! Map) {
        continue;
      }
      bounds[column] = ColumnBounds(
        min: _decode(summary['lower_bound'], type),
        max: _decode(summary['upper_bound'], type),
        nullCount: summary['contains_null'] == false ? 0 : null,
        nanCount: summary['contains_nan'] == false ? 0 : null,
        rowCount: 1,
        hasStats: true,
      );
    }
    return filter.keep(bounds);
  }

  /// Whether data file [file] might hold a matching row, judged from its
  /// identity partition values and its `lower_bounds`, `upper_bounds` and
  /// `null_value_counts`. Missing stats keep the file.
  static bool _fileMayMatch(
    Map file,
    List<({String name, String? column})>? fields,
    RowFilter filter,
    Schema schema,
  ) {
    final records = file['record_count'];
    final rows = records is int ? records : null;
    final partition = file['partition'];
    final lower = _byId(file['lower_bounds']);
    final upper = _byId(file['upper_bounds']);
    final nulls = _byId(file['null_value_counts']);
    final nans = _byId(file['nan_value_counts']);
    final bounds = <String, ColumnBounds>{};
    for (final column in filter.columns) {
      final field = schema.field(column);
      if (field == null || !ColumnBounds.judged(field.dataType)) continue;
      final type = field.dataType;
      final identity = fields
          ?.where((entry) => entry.column == column)
          .firstOrNull;
      if (identity != null &&
          partition is Map &&
          partition.containsKey(identity.name)) {
        final raw = partition[identity.name];
        final value = ColumnBounds.typed(raw, type);
        if (raw != null && value == null) continue;
        bounds[column] = ColumnBounds.exact(value, rows: rows);
        continue;
      }
      final id = field.id;
      if (id == null || rows == null) continue;
      final count = nulls[id];
      final nan = nans[id];
      bounds[column] = ColumnBounds(
        min: _decode(lower[id], type),
        max: _decode(upper[id], type),
        nullCount: count is int ? count : null,
        nanCount: nan is int ? nan : null,
        rowCount: rows,
        hasStats: true,
      );
    }
    return filter.keep(bounds);
  }

  /// An Iceberg map with int keys, written as an array of `{key, value}`
  /// records, by key.
  static Map<int, Object?> _byId(Object? entries) => {
    if (entries is List)
      for (final entry in entries)
        if (entry is Map && entry['key'] is int)
          entry['key'] as int: entry['value'],
  };

  /// A bound in Iceberg's single-value binary form as a value of [type], or
  /// null when it cannot be read as one. It is the Parquet statistics form,
  /// so [ColumnBounds.decode] reads it (a long or double column may hold a
  /// 4-byte bound written before the column was widened).
  static Object? _decode(Object? bytes, DataType type) => bytes is List<int>
      ? ColumnBounds.decode(Uint8List.fromList(bytes), type)
      : null;

  /// The column for equality field [id] in file table [table]. A file that
  /// carries field ids is matched by id ONLY (Iceberg identity is the id: old
  /// files keep a renamed column's old physical name, and a dropped then
  /// re-added column keeps its old name under a different id); a file without
  /// ids is matched by the current-schema [name]. Same rule [_shape] applies
  /// to output columns.
  Column? _fieldColumn(Table table, int id, String name) {
    if (!table.schema.fields.any((field) => field.id != null)) {
      return table.column(name);
    }
    for (final field in table.schema.fields) {
      if (field.id == id) return table.column(field.name);
    }
    return null;
  }

  /// Project a data-file batch onto the table [schema], resolving each target
  /// column as [_fieldColumn] does (by field id in a file that carries ids,
  /// else by name), then null-filling absent columns.
  Table _shape(Table table, Schema schema) {
    // Index the data file's columns by their field-id (when the file carries
    // ids: Iceberg/Spark writers, incl. this library, emit them).
    final keyed = table.schema.fields.any((field) => field.id != null);
    final byId = <int, Column>{};
    for (final field in table.schema.fields) {
      final id = field.id;
      if (id == null) continue;
      final column = table.column(field.name);
      if (column != null) byId[id] = column;
    }
    return Table(
      name: 'iceberg',
      schema: schema,
      columns: [
        for (final field in schema.fields)
          // A by-id match carries the file's PHYSICAL name; rename it to the
          // target field so the column matches the table schema.
          (keyed ? byId[field.id] : table.column(field.name))?.withName(
                field.name,
              ) ??
              columnFromValues(
                field.name,
                field.dataType,
                List.filled(table.rowCount, null),
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
      throw Failure.of(
        Kind.limit,
        'Iceberg delete indexes exceed the $cap-byte budget (position + '
        'equality deletes resident together); the table needs compaction or a '
        'larger deleteBudgetBytes',
      );
    }
  }
}
