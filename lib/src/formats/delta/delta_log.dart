import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:path/path.dart' as p;

import '../../core/limits.dart';
import '../../core/paths.dart';
import 'actions.dart';
import 'deletion_vector.dart';
import '../../core/ids.dart' as ids;

// Secure entropy for temp-file tokens so concurrent writers (isolates/pods)
// can't collide via identical clock seeds in the same millisecond.

/// Thrown when a commit version was already claimed by a concurrent writer.
class CommitConflict implements Exception {
  final int version;
  CommitConflict(this.version);
  @override
  String toString() =>
      'Delta commit $version already claimed by a concurrent writer';
}

/// Thrown when the commit's atomic rename failed but the target commit file is
/// NON-EMPTY afterward — i.e. the commit may have actually LANDED despite the
/// error (an ambiguous rename response on a network filesystem). Distinct from
/// [CommitConflict] (which means the commit definitively did NOT land): on this,
/// the writer must RETAIN its staged data files, because the published commit
/// may already reference them. Mirrors Iceberg's `IcebergCommitIndeterminate`.
class DeltaCommitIndeterminate implements Exception {
  final int version;
  DeltaCommitIndeterminate(this.version);
  @override
  String toString() =>
      'Delta commit $version outcome indeterminate: the commit may have landed '
      '(staged files retained; resolve the log before any cleanup)';
}

/// A reconstructed table state at a specific version.
class Snapshot {
  final int version;
  final Protocol? protocol;
  final Metadata metadata;
  final List<AddFile> files;

  Snapshot({
    required this.version,
    required this.protocol,
    required this.metadata,
    required this.files,
  });
}

/// Reads a Delta transaction log (`_delta_log/`) and reconstructs snapshots by
/// folding the per-commit JSON actions. A library-private checkpoint
/// (`NN.checkpoint.json` + a `_libbigdata_last_checkpoint` pointer) consolidates
/// state periodically so the fold starts after the checkpoint instead of
/// replaying from version 0. Spark interop is preserved: we do not write the
/// Delta-standard `_last_checkpoint`/`.checkpoint.parquet`, so other engines
/// simply replay the JSON commits as before.
class DeltaLog {
  final String tablePath;

  DeltaLog(this.tablePath);

  String get logDir => '$tablePath/_delta_log';

  /// Memoized directory listing for the CURRENT operation. `versions()` is
  /// called ~3× per write attempt (reclaim, discovery, snapshot replay) and each
  /// listing is O(total history), so an append on a big log paid repeated full
  /// scans. The cache is invalidated whenever the listing could change — at the
  /// top of each write attempt ([reclaimOrphans]), after an orphan delete, and
  /// after a commit — so a retry after a concurrent writer still re-lists fresh.
  List<int>? _cachedVersions;

  void _invalidateVersions() => _cachedVersions = null;

  /// Commit versions present in the log, ascending.
  Future<List<int>> versions() async {
    final cached = _cachedVersions;
    if (cached != null) return cached;
    final dir = Directory(logDir);
    if (!await dir.exists()) {
      throw ArgumentError('Not a Delta table (no _delta_log): $tablePath');
    }
    final re = RegExp(r'^(\d{20})\.json$');
    final out = <int>[];
    await for (final e in dir.list()) {
      if (e is File) {
        final m = re.firstMatch(e.uri.pathSegments.last);
        if (m != null) out.add(int.parse(m.group(1)!));
      }
    }
    out.sort();
    return _cachedVersions = out;
  }

  /// Whether [version]'s commit file is a 0-byte (claimed-but-unpublished)
  /// transaction left by a crash. Such a file never carried content (commits
  /// publish atomically via temp+rename), so it is treated as not-committed.
  Future<bool> _isIncomplete(int version) async {
    final f = File(commitPath(version));
    return (await f.exists()) && (await f.length()) == 0;
  }

  /// A 0-byte commit younger than this is treated as a live in-flight claim
  /// (contended, not advanced past); older, it is a crash orphan to reclaim. A
  /// real commit's 0-byte window is a single sub-millisecond rename, so five
  /// minutes is a wide safety margin against process pauses / slow filesystems
  /// (a too-eager reclaim is what enables the concurrent-overwrite race the
  /// commit-time ownership fence guards) while bounding self-heal latency. This
  /// matches the Iceberg coordinator's 5-minute lease.
  static const _orphanTtl = Duration(minutes: 5);

  /// Whether [version] is a FRESH incomplete commit: a 0-byte file within
  /// [_orphanTtl] — i.e. a live claim a concurrent writer is mid-committing. A
  /// new commit must CONTEND with this (back off), not advance past it, or it
  /// would gap the log and commit against stale state.
  Future<bool> isFreshIncomplete(int version) async {
    final f = File(commitPath(version));
    if (!await f.exists() || (await f.length()) != 0) return false;
    final age = DateTime.now().difference((await f.stat()).modified);
    return age <= _orphanTtl;
  }

  /// Best-effort removal of crash-orphaned 0-byte commit files: a writer that
  /// crashed between the O_EXCL claim and the temp+rename leaves an empty
  /// `NNN.json` that this library skips but external engines (Spark, delta-rs)
  /// can choke on. Only files older than [_orphanTtl] are removed, so a live
  /// commit's brief empty window is never disturbed. Never throws.
  Future<void> _sweepOrphans() async {
    final now = DateTime.now();
    var deleted = false;
    for (final v in await versions()) {
      try {
        final f = File(commitPath(v));
        if ((await f.length()) == 0 &&
            now.difference((await f.stat()).modified) > _orphanTtl) {
          await f.delete();
          deleted = true;
        }
      } catch (_) {
        /* best effort: a racing writer or vanished file is fine */
      }
    }
    // The cached listing now includes a version we just removed — re-list.
    if (deleted) _invalidateVersions();
  }

  /// Reclaim crash-orphaned 0-byte commit slots (older than [_orphanTtl]) so the
  /// next commit reuses that version number and the log stays contiguous —
  /// external engines (Spark/delta-rs) reject a version gap. A live commit's
  /// transient sub-TTL empty window is left alone, so its slot is correctly
  /// skipped rather than stolen. Best-effort; never throws.
  ///
  /// NOTE: this only closes the *durable* gap. The transient empty-`NNN.json`
  /// window a concurrent external reader can observe mid-commit is not closable
  /// on a BARE filesystem in pure Dart (see [commit]); closing it needs a
  /// coordinator with real mutual exclusion — [FileLockDeltaCommitCoordinator]
  /// (OS advisory lock, cross-process on a lock-honoring mount) or a catalog CAS
  /// ([LocalCatalogDeltaCommitCoordinator]) now provides exactly that.
  Future<void> reclaimOrphans() async {
    // Force a fresh listing at the top of each write attempt so a retry after a
    // concurrent writer sees the new version (and so the attempt's later
    // versions()/snapshot() reuse this one listing).
    _invalidateVersions();
    if (!await Directory(logDir).exists()) return; // nothing committed yet
    await _sweepOrphans();
  }

  static String commitFileName(int version) =>
      '${version.toString().padLeft(20, '0')}.json';

  String commitPath(int version) => '$logDir/${commitFileName(version)}';

  /// Cap on one metadata file's cumulative decoded size during replay, so a huge
  /// overwrite commit (many `remove` lines) can't materialize wholesale before
  /// any data file is scanned. Centralized in `core/limits.dart` (a Delta
  /// checkpoint is legitimately larger than an Iceberg metadata.json).
  static const _maxMetadataBytes = defaultMaxDeltaLogBytes;

  /// Cap on ONE line's raw bytes. A hostile commit file with no newline would
  /// otherwise buffer its whole content as a single line before the cumulative
  /// check could fire. A legitimate action line is far smaller than this.
  static const _maxLineBytes = 256 << 20; // 256 MiB per line

  /// STREAM [path] line-by-line, applying [apply] to each — a large metadata file
  /// is never loaded whole (unlike `readAsLines`). The byte caps are enforced on
  /// the RAW bytes as they arrive from [openRead], BEFORE utf8-decoding or line
  /// assembly, so a newline-free hostile line can't buffer unbounded and the cap
  /// is measured in source bytes (not UTF-16 code units). Splits on `\n` and
  /// trims a trailing `\r` (so `\r\n` and `\n` endings both work).
  Future<void> _forEachLine(String path, void Function(String) apply) async {
    var total = 0;
    var lineBytes = 0;
    final buffer = BytesBuilder(copy: false);
    void emit() {
      var bytes = buffer.takeBytes();
      if (bytes.isNotEmpty && bytes.last == 0x0D) {
        bytes = Uint8List.sublistView(bytes, 0, bytes.length - 1); // strip \r
      }
      apply(utf8.decode(bytes));
      lineBytes = 0;
    }

    await for (final chunk in File(path).openRead()) {
      final bytes = chunk is Uint8List ? chunk : Uint8List.fromList(chunk);
      var start = 0;
      for (var i = 0; i < bytes.length; i++) {
        total++;
        if (total > _maxMetadataBytes) {
          throw StateError(
            'Delta metadata file exceeds the $_maxMetadataBytes-byte replay '
            'cap: $path',
          );
        }
        if (bytes[i] == 0x0A) {
          buffer.add(Uint8List.sublistView(bytes, start, i));
          emit();
          start = i + 1;
        } else if (++lineBytes > _maxLineBytes) {
          throw StateError(
            'Delta metadata line exceeds the $_maxLineBytes-byte cap (no '
            'newline in $path)',
          );
        }
      }
      if (start < bytes.length) {
        buffer.add(Uint8List.sublistView(bytes, start, bytes.length));
      }
    }
    if (buffer.length > 0) emit();
  }

  /// Reconstruct the snapshot at [version] (latest if null), or at the latest
  /// version committed at or before [timestamp] (ms since epoch).
  Future<Snapshot> snapshot({int? version, int? timestamp}) async {
    final all = await versions();
    if (all.isEmpty) {
      throw StateError('Delta table has no commits: $tablePath');
    }

    int target;
    if (version != null) {
      if (!all.contains(version)) {
        throw ArgumentError('Version $version not found in $tablePath');
      }
      // A 0-byte commit is an incomplete (claimed-but-never-published)
      // transaction — content is published atomically via temp+rename, so a
      // 0-byte file never held data. Refusing it is honest rather than returning
      // a snapshot mislabeled as this version.
      if (await _isIncomplete(version)) {
        throw StateError(
          'Delta version $version is an incomplete (0-byte) commit in $tablePath',
        );
      }
      target = version;
    } else if (timestamp != null) {
      target = -1;
      for (final v in all) {
        if (await _isIncomplete(v)) continue; // skip aborted commits
        if (await _commitTimestamp(v) <= timestamp) target = v;
      }
      if (target < 0) {
        throw ArgumentError('No version at or before timestamp $timestamp');
      }
    } else {
      // Default to the latest COMPLETE commit, so a crashed writer's trailing
      // 0-byte claim is invisible (it self-heals — the next writer commits the
      // following version on top of the last complete state).
      target = -1;
      for (int i = all.length - 1; i >= 0; i--) {
        if (!await _isIncomplete(all[i])) {
          target = all[i];
          break;
        }
      }
      if (target < 0) {
        throw StateError('Delta table has no complete commits: $tablePath');
      }
    }

    Protocol? protocol;
    Metadata? metadata;
    final active = <String, AddFile>{};
    void apply(String line) =>
        _applyLine(line, active, (p) => protocol = p, (m) => metadata = m);

    // Start from the newest checkpoint at or before the target, replaying only
    // the commits after it; otherwise fold from version 0.
    int start = 0;
    final cp = await _lastCheckpoint();
    if (cp != null && cp <= target) {
      await _forEachLine(checkpointPath(cp), apply);
      start = cp + 1;
    }
    // The replay MUST cover a contiguous run [start, target]: a missing
    // intermediate commit (deleted, or a transiently incomplete object-store
    // listing) would silently drop its add/remove actions and yield a wrong
    // active-file set. Fail loud as corruption instead.
    var expected = start;
    for (final v in all) {
      if (v < start) continue;
      if (v > target) break;
      if (v != expected) {
        throw StateError(
          'Delta log at $tablePath is non-contiguous: version $expected is '
          'missing between the checkpoint and version $target (the log is '
          'corrupt or a commit was deleted)',
        );
      }
      // A zero-byte commit INSIDE the replay interval is an incomplete
      // (claimed-but-unpublished) transaction, not an empty one. Applying it as
      // "no actions" would silently drop whatever it should have contained while
      // still folding later versions — a wrong active-file set. Fail closed.
      if (await _isIncomplete(v)) {
        throw StateError(
          'Delta log at $tablePath has an incomplete (0-byte) commit at version '
          '$v within the replay range [.., $target]; the log is corrupt or a '
          'commit did not finish publishing',
        );
      }
      expected++;
      await _forEachLine(commitPath(v), apply);
    }
    if (expected != target + 1) {
      throw StateError(
        'Delta log at $tablePath is truncated: versions $expected through '
        '$target are missing',
      );
    }

    if (metadata == null) {
      throw StateError('Delta log has no metaData up to version $target');
    }
    return Snapshot(
      version: target,
      protocol: protocol,
      metadata:
          metadata!, // null-checked just above; closure capture blocks promotion
      files: active.values.toList(),
    );
  }

  /// Append a commit: write one JSON action per line at [version]. Returns the
  /// committed version, or throws [CommitConflict] if another writer already
  /// claimed it.
  Future<int> commit(int version, List<Map<String, dynamic>> actions) async {
    await Directory(logDir).create(recursive: true);
    final target = File(commitPath(version));

    // Write the full content to a unique temp BEFORE claiming the version, so the
    // transient empty `NNN.json` window (between the O_EXCL claim and the rename
    // that publishes content) collapses to a single rename syscall rather than
    // spanning the whole content write — minimizing what a concurrent external
    // reader could observe.
    //
    // STREAM one action per line (like [checkpoint]): the StringBuffer approach
    // held three proportional copies at peak (buffer chunks, toString, UTF-8
    // encoding), and an overwrite carries one remove PER LIVE FILE. A failure
    // mid-write deletes the partial temp — nothing sweeps `_delta_log/*.tmp`,
    // so a leak here was permanent.
    final temp = File('${commitPath(version)}.${ids.token()}.tmp');
    try {
      final sink = temp.openWrite();
      try {
        // Every commit carries a commitInfo with a real commit timestamp (used
        // for time travel instead of unreliable filesystem mtimes).
        sink.writeln(
          jsonEncode({
            'commitInfo': {'timestamp': DateTime.now().millisecondsSinceEpoch},
          }),
        );
        for (final a in actions) {
          sink.writeln(jsonEncode(a));
        }
        await sink.flush();
      } finally {
        await sink.close();
      }
      // IOSink.flush drains only the Dart buffer; the content must be durable
      // before the rename below can publish it (writeAsString(flush:true)
      // provided this before).
      await syncFile(temp.path);
    } catch (_) {
      try {
        await temp.delete();
      } catch (_) {}
      rethrow;
    }

    // Atomically claim the version: create(exclusive:true) is O_EXCL, so exactly
    // one concurrent writer (libbigdata OR Spark/delta-rs racing the same
    // versioned name) can win. A genuine I/O error (missing dir, permissions) is
    // rethrown, not treated as a conflict. NOTE: fully closing the empty window
    // would need a hardlink (atomic create-with-content) or a lock service —
    // neither available in pure Dart; the O_EXCL-on-NN.json claim is what gives
    // cross-engine exclusion, so it is kept.
    try {
      await target.create(exclusive: true);
    } on FileSystemException {
      try {
        await temp.delete();
      } catch (_) {}
      if (await target.exists()) throw CommitConflict(version);
      rethrow;
    }
    // Record our claim's identity (its mtime) so we can detect a concurrent
    // writer reclaiming this slot while we're between the claim and the publish.
    final claimStamp = (await target.stat()).modified;
    // A new version file now exists — the memoized listing is stale.
    _invalidateVersions();
    // OWNERSHIP FENCE: before overwriting the slot with our content, re-verify
    // it still holds OUR 0-byte claim. If we stalled past the reclaim lease
    // (GC pause, machine sleep, slow FS), another writer could have reclaimed
    // the claim and PUBLISHED its own commit at this version — publishing now
    // would atomically clobber that committed snapshot (silent data loss). A
    // non-empty slot, a changed mtime, or a vanished file all mean we no longer
    // own it: fail as a conflict (retryable) rather than overwrite. Checked
    // BEFORE the publish try/catch so it can't be misread as indeterminate.
    // (Best-effort: mtime granularity narrows, not closes, the final rename
    // window — a catalog-backed coordinator with fencing is the real fix.)
    final claim = target.statSync();
    if (claim.type == FileSystemEntityType.notFound ||
        claim.size != 0 ||
        claim.modified != claimStamp) {
      try {
        await temp.delete();
      } catch (_) {}
      throw CommitConflict(version);
    }
    try {
      await replaceAtomic(temp, commitPath(version));
      return version;
    } catch (_) {
      try {
        await temp.delete();
      } catch (_) {}
      // If the target is now NON-EMPTY (or we can't even stat it), the rename may
      // have LANDED despite the error — the commit could be published and
      // reference the staged data. Signal indeterminate so the writer RETAINS its
      // files rather than deleting a possibly-committed snapshot. Only a
      // confirmed-empty target proves the rename didn't land: release the claim
      // and surface the original error.
      int targetLength;
      try {
        targetLength = await target.length();
      } catch (_) {
        targetLength = -1; // can't tell → treat as indeterminate (fail safe)
      }
      if (targetLength != 0) {
        throw DeltaCommitIndeterminate(version);
      }
      try {
        await target.delete();
      } catch (_) {}
      rethrow;
    }
  }

  /// A data file (part-*/DV) written but never committed is presumed orphaned by
  /// a crashed writer once older than this. Matches Delta's standard 7-day vacuum
  /// retention: a fixed 1h window would delete the staged files of any legitimate
  /// write/commit that runs longer than an hour (there is no writer heartbeat to
  /// distinguish live from dead), so it is deliberately generous. Override via
  /// [vacuumOrphans]'s `olderThan` for deployments with a known upper bound.
  static const _dataOrphanTtl = Duration(days: 7);

  /// Best-effort removal of crash-orphaned DATA files: `part-*.parquet` /
  /// `deletion_vector_*.bin` written before the log commit by a writer that then
  /// died (SIGKILL/eviction), so they are referenced by NO version's `add` or
  /// `remove`. Only files older than [olderThan] (default [_dataOrphanTtl])
  /// qualify, so a live writer's in-flight file is never touched; time-travel
  /// files (still referenced by a `remove`) are preserved — see
  /// [_referencedFiles], which must scan every commit for that to hold. Never
  /// throws.
  Future<void> vacuumOrphans({Duration? olderThan}) async {
    final ttl = olderThan ?? _dataOrphanTtl;
    final root = Directory(tablePath);
    if (!await root.exists()) return;
    final Set<String> referenced;
    try {
      referenced = await _referencedFiles();
    } catch (_) {
      return; // can't safely determine the live set → delete nothing
    }
    final now = DateTime.now();
    final re = RegExp(r'(^|/)(part-.*\.parquet|deletion_vector_.*\.bin)$');
    await for (final e in root.list(recursive: true, followLinks: false)) {
      if (e is! File) continue;
      final rel = p.split(p.relative(e.path, from: tablePath)).join('/');
      if (rel.startsWith('_delta_log/') || !re.hasMatch(rel)) continue;
      if (referenced.contains(rel)) continue;
      try {
        if (now.difference((await e.stat()).modified) > ttl) await e.delete();
      } catch (_) {
        /* best effort: racing writer or vanished file is fine */
      }
    }
  }

  /// Every data-file path (forward-slash, table-relative) referenced by an `add`
  /// OR `remove` in ANY commit, plus the latest checkpoint's adds — the file's
  /// `path` and any deletion-vector `.bin`.
  ///
  /// A `remove` counts as a reference: the file it tombstones is still the data
  /// every version before that commit reads. Anything not in this set is
  /// referenced by no version at all, which is the only thing [vacuumOrphans]
  /// may delete.
  Future<Set<String>> _referencedFiles() async {
    final out = <String>{};
    void collect(String line) {
      if (line.trim().isEmpty) return;
      final action = jsonDecode(line) as Map<String, dynamic>;
      final fileAction =
          (action['add'] ?? action['remove']) as Map<String, dynamic>?;
      if (fileAction == null) return;
      final path = fileAction['path'];
      if (path is String) out.add(path);
      final dv = fileAction['deletionVector'];
      if (dv is Map) {
        final bin = DeletionVector.absolutePath(
          tablePath,
          DeletionVectorDescriptor.fromJson(dv.cast<String, dynamic>()),
        );
        if (bin != null) {
          out.add(p.split(p.relative(bin, from: tablePath)).join('/'));
        }
      }
    }

    final cp = await _lastCheckpoint();
    if (cp != null) {
      await _forEachLine(checkpointPath(cp), collect);
    }
    // EVERY commit, including those at or below the checkpoint. Skipping them
    // was silent DATA LOSS: a checkpoint records the live adds and NO
    // tombstones, so a `remove` written before it became invisible here — and
    // the file it tombstoned, still reachable by time travel to any earlier
    // version, looked like an orphan and was deleted. Checkpoints fire
    // automatically every 10 commits, so this was the normal case, not an edge
    // one. The checkpoint is still read above: its adds cover any commit an
    // external engine's log retention has pruned, so the union is what is safe.
    // Vacuum is rare and already walks the whole tree, so the extra reads cost
    // nothing that matters.
    for (final v in await versions()) {
      // A commit that versions() just listed but we cannot read (lock, NFS,
      // permission) would yield an INCOMPLETE referenced set — and a caller
      // like vacuumOrphans would then delete still-live files. Fail closed;
      // the caller treats a throw as "can't determine the live set".
      await _forEachLine(commitPath(v), collect);
    }
    return out;
  }

  String checkpointPath(int version) =>
      '$logDir/${version.toString().padLeft(20, '0')}.checkpoint.json';

  String get _pointer => '$logDir/_libbigdata_last_checkpoint';

  /// Write a consolidated checkpoint at [version] — protocol + metaData + one
  /// `add` per live file, in the same JSON-line format as commits — and update
  /// the library checkpoint pointer. Both writes are atomic (temp + rename).
  Future<void> checkpoint(int version) async {
    await _sweepOrphans(); // housekeeping: drop crash-orphaned 0-byte commits
    // NOTE: data-file vacuum is NOT auto-invoked here — deleting a concurrent
    // writer's staged (uncommitted) files is unsafe without ownership/fencing, so
    // reclaiming them is an explicit, retention-gated [vacuumOrphans] decision.
    final snap = await snapshot(version: version);
    final temp = File('${checkpointPath(version)}.${ids.token()}.tmp');
    final pt = File('$_pointer.${ids.token()}.tmp');
    try {
      // Stream one action per line straight to the file. Building the whole
      // checkpoint in a StringBuffer held THREE copies at peak — the buffer's
      // chunks, the `toString()` result, and its UTF-8 encoding — all
      // proportional to the table's total file count, which is exactly what a
      // checkpoint exists to bound.
      final sink = temp.openWrite();
      try {
        if (snap.protocol != null) {
          sink.writeln(jsonEncode(snap.protocol!.toJson()));
        }
        sink.writeln(jsonEncode(snap.metadata.toJson()));
        for (final f in snap.files) {
          sink.writeln(jsonEncode(f.toJson()));
        }
        await sink.flush();
      } finally {
        await sink.close();
      }
      // IOSink.flush drains only the Dart buffer; the checkpoint must be on
      // stable media before the pointer below starts advertising it.
      await syncFile(temp.path);
      await replaceAtomic(temp, checkpointPath(version));
      await pt.writeAsString(jsonEncode({'version': version}), flush: true);
      await replaceAtomic(pt, _pointer);
    } catch (_) {
      // Don't leak temps on a failed checkpoint (disk full, permissions, a
      // failed rename): a persistent failure would otherwise drop one .tmp per
      // checkpoint interval — which nothing sweeps — while commits keep
      // succeeding, eventually filling the volume.
      for (final f in [temp, pt]) {
        try {
          if (await f.exists()) await f.delete();
        } catch (_) {
          /* best effort */
        }
      }
      rethrow;
    }
  }

  /// The newest checkpoint version, or null if there is none (or its file is
  /// missing, i.e. a stale pointer).
  Future<int?> _lastCheckpoint() async {
    final f = File(_pointer);
    if (!await f.exists()) return null;
    final v = (jsonDecode(await f.readAsString()) as Map)['version'];
    if (v is! int || !await File(checkpointPath(v)).exists()) return null;
    return v;
  }

  /// Apply one JSON action line to the folding [active] map / protocol /
  /// metadata. Shared by the commit fold and the checkpoint loader.
  void _applyLine(
    String line,
    Map<String, AddFile> active,
    void Function(Protocol) onProtocol,
    void Function(Metadata) onMetadata,
  ) {
    if (line.trim().isEmpty) return;
    final action = jsonDecode(line) as Map<String, dynamic>;
    if (action.containsKey('protocol')) {
      onProtocol(
        Protocol.fromJson((action['protocol'] as Map).cast<String, dynamic>()),
      );
    } else if (action.containsKey('metaData')) {
      onMetadata(
        Metadata.fromJson((action['metaData'] as Map).cast<String, dynamic>()),
      );
    } else if (action.containsKey('add')) {
      final add = AddFile.fromJson(
        (action['add'] as Map).cast<String, dynamic>(),
      );
      active[add.key] = add;
    } else if (action.containsKey('remove')) {
      final remove = RemoveFile.fromJson(
        (action['remove'] as Map).cast<String, dynamic>(),
      );
      active.remove(remove.key);
    }
    // commitInfo / cdc / txn / etc. are ignored.
  }

  /// The commit timestamp for [version]: `commitInfo.timestamp` if present,
  /// else the file's modified time.
  Future<int> _commitTimestamp(int version) async {
    // Stream only the FIRST line (commitInfo, if any) — no need to read the whole
    // commit file just for its timestamp.
    await for (final line in File(
      commitPath(version),
    ).openRead().transform(utf8.decoder).transform(const LineSplitter())) {
      if (line.isEmpty) continue;
      final action = jsonDecode(line) as Map<String, dynamic>;
      final ts = (action['commitInfo'] as Map?)?['timestamp'];
      if (ts is int) return ts;
      break; // commitInfo, if any, is the first action
    }
    final stat = await File(commitPath(version)).stat();
    return stat.modified.millisecondsSinceEpoch;
  }
}
