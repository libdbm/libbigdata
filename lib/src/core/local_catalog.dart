import 'dart:convert';
import 'dart:io';

import 'ids.dart' as ids;
import 'limits.dart';
import 'paths.dart';

/// A lock-guarded local registry mapping a table key to its current version (and
/// an optional metadata pointer). It is the shared fenced compare-and-swap
/// primitive that the Delta AND Iceberg commit coordinators build on — the CAS
/// need is identical across both formats, so one registry unifies it and also
/// gives Iceberg an O(1) current-pointer (no metadata-dir scan).
///
/// GUARANTEE: fenced CAS for separate writer PROCESSES sharing a lock-honoring
/// filesystem (the OS lock over the registry serializes the whole
/// read → build → publish → record cycle). [withFileLock] also serializes
/// overlapping async WITHIN one isolate; it does NOT serialize separate ISOLATES
/// (they share no memory, and POSIX `fcntl` is per-process), and it is NOT a
/// networked catalog — cross-ISOLATE or cross-ENGINE CAS still needs one writer
/// per process or REST/Glue/Hive.
///
/// Lives in `core` (no format dependency); the coordinator implementations that
/// bind it to `DeltaCommitCoordinator`/`IcebergCommitCoordinator` live in the
/// format layer.
class LocalCatalog {
  /// The registry JSON file; a sibling `.lock` guards every read-modify-write.
  final String registryPath;

  const LocalCatalog(this.registryPath);

  String get _lockPath => '$registryPath.lock';

  /// A stable, filesystem-safe lock path for one table [key] (FNV-1a hex of the
  /// key — deterministic across processes, unlike `String.hashCode`). A hash
  /// collision only makes two keys share a lock (still correct, just coarser).
  String _keyLock(String key) {
    var h = 0x811c9dc5;
    for (final c in key.codeUnits) {
      h = ((h ^ c) * 0x01000193) & 0xFFFFFFFF;
    }
    return '$registryPath.${h.toRadixString(16).padLeft(8, '0')}.lock';
  }

  /// Run [body] for table [key], holding only THAT key's lock across the (slow)
  /// body — so a slow commit for one table no longer blocks unrelated tables on
  /// one global lock. The shared registry file is still read (snapshot) and
  /// written (applying only [key]'s entry) under a brief GLOBAL lock, so a
  /// concurrent commit to a DIFFERENT key can't clobber this key and vice versa.
  /// The body must mutate only [key] (the coordinators put exactly that key).
  Future<T> guarded<T>(
    String key,
    Future<T> Function(CatalogTxn txn) body,
  ) => withFileLock(_keyLock(key), () async {
    // Snapshot the registry under the shared lock (healing a crashed-swap loss
    // first, so a missing file isn't read as a fresh empty registry).
    final txn = await withFileLock(_lockPath, () async {
      await recoverBackup(registryPath);
      return CatalogTxn._(await _read());
    });
    final result = await body(txn);
    if (txn._dirty) {
      // Apply ONLY this key's entry to the shared file under the global lock,
      // re-reading so a concurrent different-key commit isn't clobbered. Same-key
      // commits are fenced by the per-key lock, so this key's entry is stable.
      await withFileLock(_lockPath, () async {
        await recoverBackup(registryPath);
        final map = await _read();
        map[key] = txn._map[key];
        await _write(map);
      });
    }
    return result;
  });

  /// The recorded entry for [key] (`{version, pointer?}`) read WITHOUT the lock —
  /// an O(1), eventually-consistent current-pointer lookup for readers.
  Future<Map<String, dynamic>?> lookup(String key) async {
    final map = await _read();
    final e = map[key];
    return e is Map<String, dynamic> ? e : null;
  }

  /// A MISSING registry is a fresh one; a present-but-unreadable registry is
  /// not. Decoding it loosely (or falling back to empty) would silently hand
  /// back "unregistered" for every table it tracks — re-registering each at
  /// version 0 and publishing a stale current-pointer for BOTH formats that
  /// build on it. Fail closed, and say which file and what to do about it: the
  /// raw alternative is a bare "Unexpected end of input" on every lookup of
  /// every table, with nothing naming the registry.
  Future<Map<String, dynamic>> _read() async {
    final file = File(registryPath);
    if (!await file.exists()) return <String, dynamic>{};
    // Bound the read: the registry is tiny (one small entry per table), so an
    // oversized file is corruption/hostile, not a real registry — reject it
    // before slurping it whole into memory.
    final size = await file.length();
    if (size > defaultMaxTableMetadataBytes) {
      throw FormatException(
        '$_corrupt: registry is $size bytes, exceeding the '
        '$defaultMaxTableMetadataBytes-byte cap',
      );
    }
    Object? decoded;
    try {
      decoded = jsonDecode(await file.readAsString());
    } on FormatException catch (e) {
      throw FormatException('$_corrupt: ${e.message}');
    }
    if (decoded is! Map<String, dynamic>) {
      throw FormatException('$_corrupt: expected a JSON object');
    }
    return decoded;
  }

  String get _corrupt =>
      'Corrupt catalog registry $registryPath — it maps every Delta/Iceberg '
      'table registered here to its current version, so it cannot be ignored. '
      'Restore it, or delete it to re-register those tables from scratch';

  Future<void> _write(Map<String, dynamic> map) async {
    await File(registryPath).parent.create(recursive: true);
    final tmp = '$registryPath.${ids.token()}.tmp';
    try {
      // flush: replaceAtomic is atomic but NOT durable, so the contents must be
      // on disk before the rename publishes them — otherwise a crash can leave
      // the registry naming versions it never recorded. Every sibling publish
      // site already does this.
      await File(tmp).writeAsString(jsonEncode(map), flush: true);
      await replaceAtomic(File(tmp), registryPath);
    } catch (_) {
      // Don't leak the temp on a failed publish (disk full / permissions /
      // rename failure) — nothing else sweeps registry temps.
      try {
        final f = File(tmp);
        if (await f.exists()) await f.delete();
      } catch (_) {
        /* best effort */
      }
      rethrow;
    }
  }
}

/// A view of the registry inside a [LocalCatalog.guarded] transaction.
class CatalogTxn {
  final Map<String, dynamic> _map;
  bool _dirty = false;

  CatalogTxn._(this._map);

  /// The current `{version, pointer?}` for [key], or null if unregistered.
  Map<String, dynamic>? entry(String key) {
    final e = _map[key];
    return e is Map<String, dynamic> ? e : null;
  }

  /// Record [key]'s new current [version] (and optional metadata [pointer]).
  void put(String key, {required int version, String? pointer}) {
    _map[key] = {'version': version, if (pointer != null) 'pointer': pointer};
    _dirty = true;
  }
}
