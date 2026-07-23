import '../../core/local_catalog.dart';
import 'dart:convert';
import 'dart:io';

import '../../core/limits.dart';
import 'iceberg_metadata.dart';

/// Resolves the CURRENT table-metadata pointer for an Iceberg table. Both the
/// reader ([IcebergTable]) and the writer resolve "which metadata.json is
/// current" through this one seam, so a catalog-backed implementation (REST /
/// Glue / Hive) gives coherent end-to-end behavior — reads and writes agree on
/// the current snapshot instead of the reader independently scanning local
/// files while writes go through a catalog CAS.
abstract interface class IcebergCatalog {
  /// The current table metadata JSON for [root] (annotated with a synthetic
  /// `_version` = its numeric prefix), or null when the table has no metadata
  /// yet. [allowOversized] bypasses the [maxIcebergMetadataBytes] read cap — for
  /// RECOVERY only (reading a table already bricked with oversized metadata so
  /// `expireSnapshots` can prune it); leave it false for normal reads.
  Future<Map<String, dynamic>?> current(String root, {bool allowOversized});
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
/// [root] using the table's own `/metadata/` or `/data/` segment — and only a
/// candidate that EXISTS is accepted, so a genuinely missing file is still
/// reported at the path it claimed rather than at an invented one.
///
/// ONE resolver for the reader and the writer. They had drifted: the writer's
/// copy handled neither bare `file:` nor `/data/` and never checked existence —
/// and the writer's is what `expireSnapshots` uses to decide which files are
/// unreachable, so on a relocated table it computed deletions over paths that
/// were never going to match, silently orphaning the real ones.
Future<String> resolveIcebergPath(String root, String path) async {
  final raw = path.startsWith('file://')
      ? path.substring(7)
      : (path.startsWith('file:') ? path.substring(5) : path);
  if (await File(raw).exists()) return raw;
  for (final marker in const ['/metadata/', '/data/']) {
    final i = raw.indexOf(marker);
    if (i >= 0) {
      final candidate = '$root${raw.substring(i)}';
      if (await File(candidate).exists()) return candidate;
    }
  }
  return raw;
}

/// Read + decode an Iceberg metadata.json, rejecting a file over
/// [maxIcebergMetadataBytes] before allocation.
Future<Map<String, dynamic>> readMetadataJson(
  String path, {
  bool allowOversized = false,
}) async {
  final f = File(path);
  final len = await f.length();
  if (!allowOversized && len > maxIcebergMetadataBytes) {
    throw StateError(
      'Iceberg metadata $path is $len bytes, over the '
      '$maxIcebergMetadataBytes-byte cap (expire old snapshots)',
    );
  }
  return jsonDecode(await f.readAsString()) as Map<String, dynamic>;
}

/// Default catalog: resolve the current pointer by scanning `<root>/metadata/`
/// for the highest-numbered `<n>-*.metadata.json` (the historical behavior),
/// with the shared split-brain check in [latestMetadataFile].
class FilesystemIcebergCatalog implements IcebergCatalog {
  const FilesystemIcebergCatalog();

  @override
  Future<Map<String, dynamic>?> current(
    String root, {
    bool allowOversized = false,
  }) async {
    final dir = Directory('$root/metadata');
    if (!await dir.exists()) return null;
    final latest = await latestMetadataFile(dir);
    if (latest == null) return null;
    final meta = await readMetadataJson(
      latest.path,
      allowOversized: allowOversized,
    );
    meta['_version'] = latest.version;
    return meta;
  }
}

/// Catalog backed by a shared [LocalCatalog]: resolves the current metadata
/// pointer in O(1) from the catalog (no `<root>/metadata/` scan), pairing with
/// [LocalCatalogIcebergCommitCoordinator] which records the pointer on every
/// commit. Falls back to null (fresh table) when the catalog has no entry yet.
class LocalCatalogIcebergCatalog implements IcebergCatalog {
  const LocalCatalogIcebergCatalog(this.catalog);

  final LocalCatalog catalog;

  @override
  Future<Map<String, dynamic>?> current(
    String root, {
    bool allowOversized = false,
  }) async {
    final entry = await catalog.lookup(root);
    final pointer = entry?['pointer'] as String?;
    if (pointer == null) {
      // No catalog entry, but the table may ALREADY exist on disk (catalog
      // enabled for an existing table, a lost/reset catalog, or changed path
      // canonicalization). Consult the filesystem head — through
      // latestMetadataFile's split-brain check — so we adopt the existing
      // lineage instead of letting the writer rebuild version 0 over it and
      // publish a SECOND 00000-*.metadata.json. A genuinely fresh table (no
      // metadata dir) still returns null.
      return const FilesystemIcebergCatalog().current(
        root,
        allowOversized: allowOversized,
      );
    }
    final meta = await readMetadataJson(
      '$root/metadata/$pointer',
      allowOversized: allowOversized,
    );
    meta['_version'] = entry!['version'];
    return meta;
  }
}
