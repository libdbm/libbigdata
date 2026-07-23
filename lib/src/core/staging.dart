import 'dart:io';

/// Delete the files a FAILED transaction staged (data/manifest/DV files that no
/// committed metadata references). Returns the subset that could NOT be removed,
/// each annotated with its error — empty on full success. NEVER throws.
///
/// The single shared primitive under the Delta and Iceberg writers' cleanup: the
/// file-deletion loop is identical, but the POLICY on a partial failure differs,
/// so each caller decides. Delta ignores the result (best-effort); Iceberg throws
/// when it is non-empty, so a half-cleaned commit is observable rather than
/// silently orphaning files. [paths] must be resolved (absolute) file paths.
Future<List<String>> deleteStaged(Iterable<String> paths) async {
  final failed = <String>[];
  for (final path in paths) {
    try {
      final file = File(path);
      if (await file.exists()) await file.delete();
    } catch (e) {
      failed.add('$path ($e)');
    }
  }
  return failed;
}
