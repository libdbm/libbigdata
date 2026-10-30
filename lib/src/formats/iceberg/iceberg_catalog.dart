import '../../core/local_catalog.dart';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:meta/meta.dart';

import '../../core/failure.dart';
import '../../core/limits.dart';
import '../../core/paths.dart' show embeddedPath;
import '../../core/result.dart';
import 'iceberg_metadata.dart';

/// Resolves the CURRENT table-metadata pointer for an Iceberg table. Both the
/// reader ([IcebergTable]) and the writer resolve "which metadata.json is
/// current" through this one boundary, so a catalog-backed implementation (REST /
/// Glue / Hive) gives coherent end-to-end behavior: reads and writes agree on
/// the current snapshot instead of the reader independently scanning local
/// files while writes go through a catalog CAS.
abstract interface class IcebergCatalog {
  /// The current table metadata JSON for [root] (annotated with a synthetic
  /// `_version` = its numeric prefix), or null when the table has no metadata
  /// yet. [allowOversized] bypasses the [maxIcebergMetadataBytes] read cap, for
  /// RECOVERY only (reading a table already bricked with oversized metadata so
  /// `expireSnapshots` can prune it); leave it false for normal reads.
  ///
  /// A failure is an [Err]: [Kind.limit] for metadata over the cap,
  /// [Kind.corrupt] for metadata or a registry that does not decode, and
  /// [Kind.io] when it cannot be read.
  @useResult
  Future<Result<Map<String, dynamic>?, ErrorDetails>> current(
    String root, {
    bool allowOversized,
  });
}

/// Cap on a single Iceberg `metadata.json` read: `readAsString`+`jsonDecode`
/// would otherwise materialize a huge or malicious file before any planning
/// limit applies. Centralized in `core/limits.dart`; `expireSnapshots` is the
/// way to keep the file small.
const int maxIcebergMetadataBytes = defaultMaxTableMetadataBytes;

/// Resolve an embedded manifest/manifest-list/data path against a table [root].
///
/// Iceberg records ABSOLUTE URIs, so a table copied to a new location still
/// names its old one. A path that does not exist as written is re-rooted at
/// [root] using the table's own `/metadata/` or `/data/` segment, and only a
/// candidate that EXISTS is accepted, so a genuinely missing file is still
/// reported at the path it claimed rather than at an invented one.
///
/// ONE resolver for the reader and the writer. They had drifted: the writer's
/// copy handled neither bare `file:` nor `/data/` and never checked existence,
/// and the writer's is what `expireSnapshots` uses to decide which files are
/// unreachable, so on a relocated table it computed deletions over paths that
/// were never going to match, silently orphaning the real ones.
Future<String> resolveIcebergPath(String root, String path) async {
  final raw = embeddedPath(root, path, encoded: false) ?? path;
  if (await File(raw).exists()) return raw;
  for (final marker in const ['/metadata/', '/data/']) {
    // The LAST one: the table's own directory, even when a directory above
    // the table is also called `data` or `metadata`.
    final i = raw.lastIndexOf(marker);
    if (i >= 0) {
      final candidate = '$root${raw.substring(i)}';
      if (await File(candidate).exists()) return candidate;
    }
  }
  return raw;
}

/// The current schema JSON of the Iceberg [metadata] of [table], for the
/// reader and the writer alike. A present-but-dangling `current-schema-id` is
/// corrupt metadata: reading under the wrong schema silently mis-types every
/// column, and the writer fell back to the last schema. Fail closed. Only a
/// genuinely absent id falls back to the first schema (and format v1 to its
/// single `schema`).
Map<String, dynamic> currentSchema(
  Map<String, dynamic> metadata,
  String table,
) {
  final schemas = (metadata['schemas'] as List?)?.cast<Map<String, dynamic>>();
  if (schemas != null && schemas.isNotEmpty) {
    final id = metadata['current-schema-id'] as int?;
    if (id != null) {
      return schemas.firstWhere(
        (schema) => schema['schema-id'] == id,
        orElse: () => throw FormatException(
          'Iceberg metadata references current-schema-id $id but no schema '
          'has that id (corrupt metadata) in $table',
        ),
      );
    }
    return schemas.first;
  }
  return metadata['schema'] as Map<String, dynamic>;
}

/// The current snapshot of the Iceberg [metadata] of [table], for the reader
/// and the writer alike. A present-but-dangling `current-snapshot-id` is
/// corrupt metadata: the reader would serve unrelated data, and the writer
/// would carry manifests forward from the wrong snapshot, rewriting history.
/// Fail closed; only an absent or -1 pointer falls back to the latest
/// snapshot. [metadata] must hold at least one snapshot.
Map<String, dynamic> currentSnapshot(
  Map<String, dynamic> metadata,
  String table,
) {
  final snapshots = (metadata['snapshots'] as List)
      .cast<Map<String, dynamic>>();
  final current = metadata['current-snapshot-id'];
  if (current == null || current == -1) return snapshots.last;
  return snapshots.firstWhere(
    (snapshot) => snapshot['snapshot-id'] == current,
    orElse: () => throw FormatException(
      'Iceberg metadata references current-snapshot-id $current but no '
      'snapshot has that id (corrupt metadata) in $table',
    ),
  );
}

/// Read + decode an Iceberg metadata.json, rejecting a file over
/// [maxIcebergMetadataBytes] before allocation. A gzip-compressed file (by its
/// magic bytes) is inflated, with the same cap on the inflated size.
Future<Map<String, dynamic>> readMetadataJson(
  String path, {
  bool allowOversized = false,
}) async {
  final file = File(path);
  void check(int length) {
    if (!allowOversized && length > maxIcebergMetadataBytes) {
      throw Failure.of(
        Kind.limit,
        'Iceberg metadata $path is $length bytes, over the '
        '$maxIcebergMetadataBytes-byte cap (expire old snapshots)',
      );
    }
  }

  check(await file.length());
  var bytes = await file.readAsBytes();
  if (bytes.length > 1 && bytes[0] == 0x1f && bytes[1] == 0x8b) {
    final inflated = BytesBuilder(copy: false);
    await for (final chunk in gzip.decoder.bind(Stream.value(bytes))) {
      inflated.add(chunk);
      check(inflated.length);
    }
    bytes = inflated.takeBytes();
  }
  final decoded = jsonDecode(utf8.decode(bytes));
  if (decoded is! Map<String, dynamic>) {
    throw FormatException('Iceberg metadata $path is not a JSON object');
  }
  return decoded;
}

/// Default catalog: resolve the current pointer by scanning `<root>/metadata/`
/// for the highest-numbered `<n>-*.metadata.json` (the historical behavior),
/// with the shared split-brain check in [latestMetadataFile].
base class FilesystemIcebergCatalog implements IcebergCatalog {
  const FilesystemIcebergCatalog();

  @override
  Future<Result<Map<String, dynamic>?, ErrorDetails>> current(
    String root, {
    bool allowOversized = false,
  }) => capture(() => _current(root, allowOversized), fallback: Kind.corrupt);

  Future<Map<String, dynamic>?> _current(
    String root,
    bool allowOversized,
  ) async {
    final directory = Directory('$root/metadata');
    if (!await directory.exists()) return null;
    final latest = await latestMetadataFile(directory);
    if (latest == null) return null;
    final metadata = await readMetadataJson(
      latest.path,
      allowOversized: allowOversized,
    );
    metadata['_version'] = latest.version;
    metadata['_file'] = latest.path;
    return metadata;
  }
}

/// Catalog backed by a shared [LocalCatalog]: resolves the current metadata
/// pointer in O(1) from the catalog (no `<root>/metadata/` scan), pairing with
/// the commit coordinator that records the pointer on every commit (the two
/// together are [LocalCatalogIcebergBackend]). Falls back to null (fresh
/// table) when the catalog has no entry yet.
base class LocalCatalogIcebergCatalog implements IcebergCatalog {
  const LocalCatalogIcebergCatalog(this.catalog);

  final LocalCatalog catalog;

  @override
  Future<Result<Map<String, dynamic>?, ErrorDetails>> current(
    String root, {
    bool allowOversized = false,
  }) => capture(() async {
    final entry = await catalog.lookup(root).unwrap();
    final pointer = entry?['pointer'] as String?;
    if (pointer == null) {
      // No catalog entry, but the table may ALREADY exist on disk (catalog
      // enabled for an existing table, a lost/reset catalog, or changed path
      // canonicalization). Consult the filesystem head: through
      // latestMetadataFile's split-brain check, so we adopt the existing
      // lineage instead of letting the writer rebuild version 0 over it and
      // publish a SECOND 00000-*.metadata.json. A genuinely fresh table (no
      // metadata dir) still returns null.
      return const FilesystemIcebergCatalog()._current(root, allowOversized);
    }
    final metadata = await readMetadataJson(
      '$root/metadata/$pointer',
      allowOversized: allowOversized,
    );
    metadata['_version'] = entry!['version'];
    metadata['_file'] = '$root/metadata/$pointer';
    return metadata;
  }, fallback: Kind.corrupt);
}
