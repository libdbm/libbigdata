import 'dart:io';

import '../../core/failure.dart';
import '../../core/result.dart';

/// An Iceberg metadata file name, capturing its version: `<n>-<uuid>` (the
/// catalog form this library writes) or `v<n>` (the Hadoop catalog form), with
/// an optional `.gz` before `.metadata.json` or after it. Version-hint files
/// are not needed: the listing finds the highest version.
final RegExp metadataFile = RegExp(
  r'^(?:v(\d+)|(\d+)-.*?)(?:\.gz)?\.metadata\.json(?:\.gz)?$',
);

/// Scan an Iceberg `metadata/` directory for the highest-versioned
/// [metadataFile]. Returns its path and numeric version, or null when the
/// directory holds no such file. Throws a [Kind.corrupt] failure on a
/// split-brain (two distinct files claiming the same version), so reader and
/// writer fail closed identically (picking by directory order would be nondeterministic).
Future<({String path, int version})?> latestMetadataFile(
  Directory directory,
) async {
  String? best;
  var highest = -1;
  var collision = false;
  await for (final entry in directory.list()) {
    if (entry is! File) continue;
    final match = metadataFile.firstMatch(entry.uri.pathSegments.last);
    if (match == null) continue;
    final version = int.parse(match.group(1) ?? match.group(2)!);
    if (version > highest) {
      highest = version;
      best = entry.path;
      collision = false;
    } else if (version == highest) {
      collision = true;
    }
  }
  if (collision) {
    throw Failure.of(
      Kind.corrupt,
      'Iceberg split-brain: multiple metadata files share version $highest under '
      '${directory.path} (concurrent writers published the same version). Resolve '
      'manually, or use a catalog-backed IcebergBackend for fenced '
      'compare-and-swap.',
    );
  }
  if (best == null) return null;
  return (path: best, version: highest);
}
