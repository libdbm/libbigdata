import 'dart:io';

/// Scan an Iceberg `metadata/` directory for the highest-versioned
/// `<n>-*.metadata.json` file. Returns its path and numeric version, or null
/// when the directory holds no such file. Throws [StateError] on a split-brain
/// — two distinct files claiming the same version — so reader and writer fail
/// closed identically (picking by directory order would be nondeterministic).
Future<({String path, int version})?> latestMetadataFile(Directory dir) async {
  final re = RegExp(r'^(\d+)-.*\.metadata\.json$');
  String? best;
  var bestN = -1;
  var collision = false;
  await for (final e in dir.list()) {
    if (e is! File) continue;
    final m = re.firstMatch(e.uri.pathSegments.last);
    if (m == null) continue;
    final n = int.parse(m.group(1)!);
    if (n > bestN) {
      bestN = n;
      best = e.path;
      collision = false;
    } else if (n == bestN) {
      collision = true;
    }
  }
  if (collision) {
    throw StateError(
      'Iceberg split-brain: multiple metadata files share version $bestN under '
      '${dir.path} (concurrent writers published the same version). Resolve '
      'manually, or use a catalog-backed IcebergBackend for fenced '
      'compare-and-swap.',
    );
  }
  if (best == null) return null;
  return (path: best, version: bestN);
}
