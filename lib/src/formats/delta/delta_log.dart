import '../../core/failure.dart';
import '../../core/result.dart';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:path/path.dart' as p;

import '../../core/limits.dart';
import '../../core/paths.dart';
import '../parquet/parquet_reader.dart';
import 'actions.dart';
import 'deletion_vector.dart';
import '../../core/ids.dart' as ids;

/// Raised when a commit version was already claimed by a concurrent writer,
/// and by a coordinator that ran out of retries contending for one; [message]
/// says which. Reported as [Kind.conflict].
class CommitConflict extends Failure {
  final int version;
  CommitConflict(this.version, [String? message])
    : super(
        ErrorDetails(
          Kind.conflict,
          message ??
              'Delta commit $version already claimed by a concurrent writer',
        ),
      );
}

/// Thrown when the commit's atomic rename failed but the target commit file is
/// NON-EMPTY afterward, i.e. the commit may have actually LANDED despite the
/// error (an ambiguous rename response on a network filesystem). Distinct from
/// [CommitConflict] (which means the commit definitively did NOT land): on this,
/// the writer must RETAIN its staged data files, because the published commit
/// may already reference them. Mirrors Iceberg's `IcebergCommitIndeterminate`.
class DeltaCommitIndeterminate extends Failure {
  final int version;
  DeltaCommitIndeterminate(this.version, [Object? cause])
    : super(
        ErrorDetails(
          Kind.indeterminate,
          'Delta commit $version outcome indeterminate: the commit may have '
          'landed (staged files retained; resolve the log before any cleanup)',
          cause,
        ),
      );
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

/// A checkpoint's version and files: this library's JSON form ([json]) or
/// the Delta-standard Parquet form, one file per part.
typedef _Checkpoint = ({int version, List<String> files, bool json});

/// Reads a Delta transaction log (`_delta_log/`) and reconstructs snapshots by
/// folding the per-commit JSON actions. A library-private checkpoint
/// (`NN.checkpoint.json` + a `_libbigdata_last_checkpoint` pointer) consolidates
/// state periodically so the fold starts after the checkpoint instead of
/// replaying from version 0. The Delta-standard checkpoint other engines write
/// (single-file or multi-part `NN.checkpoint.parquet`) is read too. Both are
/// found by listing the log, and the fold starts from the newest one at or
/// below the target version; the pointers are hints this reader does not
/// need. This library does not write the standard form, so other engines
/// replay its JSON commits.
final class DeltaLog {
  final String tablePath;

  DeltaLog(this.tablePath);

  String get logDir => '$tablePath/_delta_log';

  /// Memoized directory listing for the CURRENT operation. `versions()` is
  /// called ~3× per write attempt (reclaim, discovery, snapshot replay) and each
  /// listing is O(total history), so an append on a big log paid repeated full
  /// scans. The cache is invalidated whenever the listing could change, at the
  /// top of each write attempt ([reclaimOrphans]), after an orphan delete, and
  /// after a commit, so a retry after a concurrent writer still re-lists fresh.
  List<int>? _cached;

  void _invalidate() => _cached = null;

  /// Commit versions present in the log, ascending.
  Future<List<int>> versions() async {
    final cached = _cached;
    if (cached != null) return cached;
    final directory = Directory(logDir);
    if (!await directory.exists()) {
      throw Failure.of(
        Kind.missing,
        'Not a Delta table (no _delta_log): $tablePath',
      );
    }
    final pattern = RegExp(r'^(\d{20})\.json$');
    final out = <int>[];
    await for (final entry in directory.list()) {
      if (entry is File) {
        final match = pattern.firstMatch(entry.uri.pathSegments.last);
        if (match != null) out.add(int.parse(match.group(1)!));
      }
    }
    out.sort();
    return _cached = out;
  }

  /// Whether [version]'s commit file is a 0-byte (claimed-but-unpublished)
  /// transaction left by a crash. Such a file never carried content (commits
  /// publish atomically via temp+rename), so it is treated as not-committed.
  Future<bool> _isIncomplete(int version) async {
    final file = File(commitPath(version));
    return (await file.exists()) && (await file.length()) == 0;
  }

  /// A 0-byte commit younger than this is treated as a live in-flight claim
  /// (contended, not advanced past); older, it is a crash orphan to reclaim. A
  /// real commit's 0-byte window is a single sub-millisecond rename, so five
  /// minutes is a wide safety margin against process pauses / slow filesystems
  /// (a too-eager reclaim is what enables the concurrent-overwrite race the
  /// commit-time ownership fence guards) while bounding self-heal latency. This
  /// matches the Iceberg coordinator's 5-minute lease.
  static const _orphanTTL = Duration(minutes: 5);

  /// Whether [version] is a FRESH incomplete commit: a 0-byte file within
  /// [_orphanTTL], i.e. a live claim a concurrent writer is mid-committing. A
  /// new commit must CONTEND with this (back off), not advance past it, or it
  /// would gap the log and commit against stale state.
  Future<bool> isFreshIncomplete(int version) async {
    final file = File(commitPath(version));
    if (!await file.exists() || (await file.length()) != 0) return false;
    final age = DateTime.now().difference((await file.stat()).modified);
    return age <= _orphanTTL;
  }

  /// Best-effort removal of crash-orphaned 0-byte commit files: a writer that
  /// crashed between the O_EXCL claim and the temp+rename leaves an empty
  /// `NNN.json` that this library skips but external engines (Spark, delta-rs)
  /// can choke on. Only files older than [_orphanTTL] are removed, so a live
  /// commit's brief empty window is never disturbed. Never throws.
  Future<void> _sweepOrphans([Duration retention = _orphanTTL]) async {
    final now = DateTime.now();
    var deleted = false;
    for (final v in await versions()) {
      try {
        final file = File(commitPath(v));
        if ((await file.length()) == 0 &&
            now.difference((await file.stat()).modified) >= retention) {
          await file.delete();
          deleted = true;
        }
      } catch (_) {
        /* best effort: a racing writer or vanished file is fine */
      }
    }
    // The cached listing now includes a version we just removed: re-list.
    if (deleted) _invalidate();
  }

  /// Reclaim crash-orphaned 0-byte commit slots (older than [_orphanTTL]) so the
  /// next commit reuses that version number and the log stays contiguous:
  /// external engines (Spark/delta-rs) reject a version gap. A live commit's
  /// transient sub-TTL empty window is left alone, so its slot is correctly
  /// skipped rather than stolen. Best-effort; never throws.
  ///
  /// NOTE: this only closes the *durable* gap. The transient empty-`NNN.json`
  /// window a concurrent external reader can observe mid-commit is not closable
  /// on a BARE filesystem in pure Dart (see [commit]); closing it needs a
  /// coordinator with real mutual exclusion: [FileLockDeltaCommitCoordinator]
  /// (OS advisory lock, cross-process on a lock-honoring mount) or a catalog CAS
  /// ([LocalCatalogDeltaCommitCoordinator]) now provides exactly that.
  ///
  /// [olderThan] overrides the TTL for a caller that can prove no claim is
  /// live: under [FileLockDeltaCommitCoordinator]'s lock every empty claim was
  /// left by a writer that died, so it passes [Duration.zero].
  Future<void> reclaimOrphans({Duration olderThan = _orphanTTL}) async {
    // Force a fresh listing at the top of each write attempt so a retry after a
    // concurrent writer sees the new version (and so the attempt's later
    // versions()/snapshot() reuse this one listing).
    _invalidate();
    if (!await Directory(logDir).exists()) return; // nothing committed yet
    await _sweepOrphans(olderThan);
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

  /// STREAM [path] line-by-line, applying [apply] to each: a large metadata file
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
          throw Failure.of(
            Kind.limit,
            'Delta metadata file exceeds the $_maxMetadataBytes-byte replay '
            'cap: $path',
          );
        }
        if (bytes[i] == 0x0A) {
          buffer.add(Uint8List.sublistView(bytes, start, i));
          emit();
          start = i + 1;
        } else if (++lineBytes > _maxLineBytes) {
          throw Failure.of(
            Kind.limit,
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
      throw Failure.of(Kind.missing, 'Delta table has no commits: $tablePath');
    }

    int target;
    if (version != null) {
      if (!all.contains(version)) {
        throw Failure.of(
          Kind.missing,
          'Version $version not found in $tablePath',
        );
      }
      // A 0-byte commit is an incomplete (claimed-but-never-published)
      // transaction: content is published atomically via temp+rename, so a
      // 0-byte file never held data. Refusing it is honest rather than returning
      // a snapshot mislabeled as this version.
      if (await _isIncomplete(version)) {
        throw Failure.of(
          Kind.corrupt,
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
        throw Failure.of(
          Kind.missing,
          'No version at or before timestamp $timestamp',
        );
      }
    } else {
      // Default to the latest COMPLETE commit, so a crashed writer's trailing
      // 0-byte claim is invisible (it self-heals: the next writer commits the
      // following version on top of the last complete state).
      target = -1;
      for (int i = all.length - 1; i >= 0; i--) {
        if (!await _isIncomplete(all[i])) {
          target = all[i];
          break;
        }
      }
      if (target < 0) {
        throw Failure.of(
          Kind.missing,
          'Delta table has no complete commits: $tablePath',
        );
      }
    }

    Protocol? protocol;
    Metadata? metadata;
    final active = <String, AddFile>{};
    void apply(String line) => _applyLine(
      line,
      active,
      (value) => protocol = value,
      (value) => metadata = value,
    );

    // Start from the newest checkpoint at or before the target, replaying only
    // the commits after it; otherwise fold from version 0.
    int start = 0;
    _Checkpoint? base;
    for (final checkpoint in await _checkpoints()) {
      if (checkpoint.version <= target) base = checkpoint;
    }
    if (base != null && base.json) {
      await _forEachLine(base.files.single, apply);
      start = base.version + 1;
    } else if (base != null) {
      await for (final action in _parquetCheckpointActions(base)) {
        // A checkpoint's removes are vacuum tombstones for files no version
        // from here on reads, and its rows are unordered against the adds.
        if (action.containsKey('remove')) continue;
        _applyAction(
          action,
          active,
          (value) => protocol = value,
          (value) => metadata = value,
        );
      }
      start = base.version + 1;
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
        throw Failure.of(
          Kind.corrupt,
          'Delta log at $tablePath is non-contiguous: version $expected is '
          'missing between the checkpoint and version $target (the log is '
          'corrupt or a commit was deleted)',
        );
      }
      // A zero-byte commit INSIDE the replay interval is an incomplete
      // (claimed-but-unpublished) transaction, not an empty one. Applying it as
      // "no actions" would silently drop whatever it should have contained while
      // still folding later versions: a wrong active-file set. Fail closed.
      if (await _isIncomplete(v)) {
        throw Failure.of(
          Kind.corrupt,
          'Delta log at $tablePath has an incomplete (0-byte) commit at version '
          '$v within the replay range [.., $target]; the log is corrupt or a '
          'commit did not finish publishing',
        );
      }
      expected++;
      await _forEachLine(commitPath(v), apply);
    }
    if (expected != target + 1) {
      throw Failure.of(
        Kind.corrupt,
        'Delta log at $tablePath is truncated: versions $expected through '
        '$target are missing',
      );
    }

    if (metadata == null) {
      throw Failure.of(
        Kind.corrupt,
        'Delta log has no metaData up to version $target',
      );
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
    // spanning the whole content write: minimizing what a concurrent external
    // reader could observe.
    //
    // STREAM one action per line (like [checkpoint]): the StringBuffer approach
    // held three proportional copies at peak (buffer chunks, toString, UTF-8
    // encoding), and an overwrite carries one remove PER LIVE FILE. A failure
    // mid-write deletes the partial temp; nothing sweeps `_delta_log/*.tmp`,
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
        for (final action in actions) {
          sink.writeln(jsonEncode(action));
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
    // would need a hardlink (atomic create-with-content) or a lock service:
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
    final stamp = (await target.stat()).modified;
    // A new version file now exists: the memoized listing is stale.
    _invalidate();
    // OWNERSHIP FENCE: before overwriting the slot with our content, re-verify
    // it still holds OUR 0-byte claim. If we stalled past the reclaim lease
    // (GC pause, machine sleep, slow FS), another writer could have reclaimed
    // the claim and PUBLISHED its own commit at this version: publishing now
    // would atomically clobber that committed snapshot (silent data loss). A
    // non-empty slot, a changed mtime, or a vanished file all mean we no longer
    // own it: fail as a conflict (retryable) rather than overwrite. Checked
    // BEFORE the publish try/catch so it can't be misread as indeterminate.
    // (Best-effort: mtime granularity narrows, not closes, the final rename
    // window: a catalog-backed coordinator with fencing is the real fix.)
    final claim = target.statSync();
    if (claim.type == FileSystemEntityType.notFound ||
        claim.size != 0 ||
        claim.modified != stamp) {
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
      // have LANDED despite the error: the commit could be published and
      // reference the staged data. Signal indeterminate so the writer RETAINS its
      // files rather than deleting a possibly-committed snapshot. Only a
      // confirmed-empty target proves the rename didn't land: release the claim
      // and surface the original error.
      int length;
      try {
        length = await target.length();
      } catch (_) {
        length = -1; // can't tell → treat as indeterminate (fail safe)
      }
      if (length != 0) {
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
  static const _dataOrphanTTL = Duration(days: 7);

  /// Best-effort removal of crash-orphaned DATA files: `part-*.parquet` /
  /// `deletion_vector_*.bin` written before the log commit by a writer that then
  /// died (SIGKILL/eviction), so they are referenced by NO version's `add` or
  /// `remove`. Only files older than [olderThan] (default [_dataOrphanTTL])
  /// qualify, so a live writer's in-flight file is never touched; time-travel
  /// files (still referenced by a `remove`) are preserved: see
  /// [_referencedFiles], which must scan every commit for that to hold. Never
  /// throws.
  Future<void> vacuumOrphans({Duration? olderThan}) async {
    final retention = olderThan ?? _dataOrphanTTL;
    final root = Directory(tablePath);
    if (!await root.exists()) return;
    final Set<String> referenced;
    try {
      referenced = await _referencedFiles();
    } catch (_) {
      return; // can't safely determine the live set → delete nothing
    }
    final now = DateTime.now();
    final pattern = RegExp(r'(^|/)(part-.*\.parquet|deletion_vector_.*\.bin)$');
    await for (final entry in root.list(recursive: true, followLinks: false)) {
      if (entry is! File) continue;
      final relative = p
          .split(p.relative(entry.path, from: tablePath))
          .join('/');
      if (relative.startsWith('_delta_log/') || !pattern.hasMatch(relative)) {
        continue;
      }
      if (referenced.contains(relative)) continue;
      try {
        if (now.difference((await entry.stat()).modified) > retention) {
          await entry.delete();
        }
      } catch (_) {
        /* best effort: racing writer or vanished file is fine */
      }
    }
  }

  /// The logged [path] of an add or remove as the forward-slash, table-relative
  /// path [vacuumOrphans] lists on disk, or null when it lies outside the
  /// table. Resolved as the reader resolves it ([embeddedPath]): a relative
  /// path is URL-decoded (`city=50%2525/...` is the file under `city=50%25`).
  /// Comparing the raw logged form made vacuum delete live files.
  String? _tableRelative(String path) {
    final file = embeddedPath(tablePath, path);
    if (file == null) return null; // remote: never under the table
    final root = p.absolute(tablePath);
    final absolute = p.absolute(file);
    if (!p.isWithin(root, absolute)) return null;
    return p.split(p.relative(absolute, from: root)).join('/');
  }

  /// Every data-file path (forward-slash, table-relative) referenced by an `add`
  /// OR `remove` in ANY commit, plus the latest checkpoint's adds: the file's
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
      final file = (action['add'] ?? action['remove']) as Map<String, dynamic>?;
      if (file == null) return;
      final path = file['path'];
      if (path is String) {
        final relative = _tableRelative(path);
        if (relative != null) out.add(relative);
      }
      final vector = file['deletionVector'];
      if (vector is Map) {
        final bin = DeletionVector.absolutePath(
          tablePath,
          DeletionVectorDescriptor.fromJson(vector.cast<String, dynamic>()),
        );
        if (bin != null) {
          out.add(p.split(p.relative(bin, from: tablePath)).join('/'));
        }
      }
    }

    // Every checkpoint's adds and removes count: once an engine has cleaned
    // up the commits before one, it is the only record of them.
    for (final checkpoint in await _checkpoints()) {
      if (checkpoint.json) {
        await _forEachLine(checkpoint.files.single, collect);
      } else {
        await for (final action in _parquetCheckpointActions(checkpoint)) {
          collect(jsonEncode(action));
        }
      }
    }
    // EVERY commit, including those at or below the checkpoint. Skipping them
    // was silent DATA LOSS: a checkpoint records the live adds and NO
    // tombstones, so a `remove` written before it became invisible here, and
    // the file it tombstoned, still reachable by time travel to any earlier
    // version, looked like an orphan and was deleted. Checkpoints fire
    // automatically every 10 commits, so this was the normal case, not an edge
    // one. The checkpoint is still read above: its adds cover any commit an
    // external engine's log retention has pruned, so the union is what is safe.
    // Vacuum is rare and already walks the whole tree, so the extra reads cost
    // nothing that matters.
    for (final v in await versions()) {
      // A commit that versions() just listed but we cannot read (lock, NFS,
      // permission) would yield an INCOMPLETE referenced set, and a caller
      // like vacuumOrphans would then delete still-live files. Fail closed;
      // the caller treats a throw as "can't determine the live set".
      await _forEachLine(commitPath(v), collect);
    }
    return out;
  }

  String checkpointPath(int version) =>
      '$logDir/${version.toString().padLeft(20, '0')}.checkpoint.json';

  String get _pointer => '$logDir/_libbigdata_last_checkpoint';

  /// Write a consolidated checkpoint at [version]: protocol + metaData + one
  /// `add` per live file, in the same JSON-line format as commits, and update
  /// the library checkpoint pointer. Both writes are atomic (temp + rename).
  Future<void> checkpoint(int version) async {
    await _sweepOrphans(); // housekeeping: drop crash-orphaned 0-byte commits
    // NOTE: data-file vacuum is NOT auto-invoked here, since deleting a concurrent
    // writer's staged (uncommitted) files is unsafe without ownership/fencing, so
    // reclaiming them is an explicit, retention-bounded [vacuumOrphans] decision.
    final state = await snapshot(version: version);
    // Stream one action per line straight to the file. Building the whole
    // checkpoint in a StringBuffer held THREE copies at peak: the buffer's
    // chunks, the `toString()` result, and its UTF-8 encoding; all
    // proportional to the table's total file count, which is exactly what a
    // checkpoint exists to bound. publish fsyncs it before the rename, so it
    // is on stable media before the pointer starts advertising it, and deletes
    // a failed write's temp: nothing sweeps them, and a persistent failure
    // would otherwise leave one per checkpoint interval.
    await publishSink(checkpointPath(version), (sink) async {
      if (state.protocol != null) {
        sink.writeln(jsonEncode(state.protocol!.toJson()));
      }
      sink.writeln(jsonEncode(state.metadata.toJson()));
      for (final file in state.files) {
        sink.writeln(jsonEncode(file.toJson()));
      }
    });
    await publish(
      _pointer,
      (temp) => File(temp).writeAsString(jsonEncode({'version': version})),
    );
  }

  /// The files of the Delta-standard checkpoint at [version]: one
  /// `NN.checkpoint.parquet`, or for a [parts]-part checkpoint
  /// `NN.checkpoint.PPPPPPPPPP.TTTTTTTTTT.parquet` for each part P of T.
  List<String> parquetCheckpointPaths(int version, [int? parts]) {
    final prefix = '$logDir/${version.toString().padLeft(20, '0')}.checkpoint';
    if (parts == null) return ['$prefix.parquet'];
    final total = parts.toString().padLeft(10, '0');
    return [
      for (var part = 1; part <= parts; part++)
        '$prefix.${part.toString().padLeft(10, '0')}.$total.parquet',
    ];
  }

  /// Every complete checkpoint in the log, ascending by version, found by
  /// listing: this library's `NN.checkpoint.json` ([_Checkpoint.json]) and
  /// the Delta-standard single-file or multi-part Parquet form, which is
  /// complete when every part of it is present. At one version this
  /// library's form sorts last, so it is the one a fold picks. The
  /// `_last_checkpoint` pointer names only the newest checkpoint and log
  /// cleanup may lose it, so time travel cannot rely on it.
  Future<List<_Checkpoint>> _checkpoints() async {
    final own = RegExp(r'^(\d{20})\.checkpoint\.json$');
    final standard = RegExp(
      r'^(\d{20})\.checkpoint(?:\.(\d{10})\.(\d{10}))?\.parquet$',
    );
    final found = <_Checkpoint>[];
    // Multi-part checkpoints by (version, total): the part numbers present.
    final parts = <(int, int), Set<int>>{};
    await for (final entry in Directory(logDir).list()) {
      if (entry is! File) continue;
      final name = entry.uri.pathSegments.last;
      if (own.firstMatch(name) case final match?) {
        found.add((
          version: int.parse(match.group(1)!),
          files: [entry.path],
          json: true,
        ));
      } else if (standard.firstMatch(name) case final match?) {
        final version = int.parse(match.group(1)!);
        if (match.group(2) == null) {
          found.add((version: version, files: [entry.path], json: false));
        } else {
          (parts[(version, int.parse(match.group(3)!))] ??= {}).add(
            int.parse(match.group(2)!),
          );
        }
      }
    }
    for (final MapEntry(key: (version, total), value: present)
        in parts.entries) {
      if (total < 1 ||
          !Iterable.generate(total, (i) => i + 1).every(present.contains)) {
        continue;
      }
      found.add((
        version: version,
        files: parquetCheckpointPaths(version, total),
        json: false,
      ));
    }
    return found..sort((a, b) {
      final order = a.version.compareTo(b.version);
      return order != 0 ? order : (a.json ? 1 : 0) - (b.json ? 1 : 0);
    });
  }

  /// The actions in the standard [checkpoint], one per row across all its
  /// parts, in the same `{'add': {...}}` form as a JSON commit line. Streamed
  /// by row group.
  Stream<Map<String, dynamic>> _parquetCheckpointActions(
    _Checkpoint checkpoint,
  ) async* {
    final version = checkpoint.version;
    for (final file in checkpoint.files) {
      yield* _parquetFileActions(file, version);
    }
  }

  Stream<Map<String, dynamic>> _parquetFileActions(
    String file,
    int version,
  ) async* {
    final reader = ParquetReader(file);
    await for (final batch in reader.readBatches().unwrap()) {
      for (final row in batch.toRows()) {
        // A V2 checkpoint keeps its file actions in sidecar files; reading
        // only this file would silently miss them.
        if (row['sidecar'] != null) {
          throw Failure.of(
            Kind.unsupported,
            'Delta V2 checkpoint with sidecar files at version $version in '
            '$tablePath is not supported',
          );
        }
        for (final key in const ['add', 'remove', 'metaData', 'protocol']) {
          final value = row[key];
          if (value is Map) {
            yield {key: value.cast<String, dynamic>()};
            break;
          }
        }
      }
    }
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
    _applyAction(
      jsonDecode(line) as Map<String, dynamic>,
      active,
      onProtocol,
      onMetadata,
    );
  }

  /// Apply one decoded action to the folding [active] map / protocol /
  /// metadata.
  void _applyAction(
    Map<String, dynamic> action,
    Map<String, AddFile> active,
    void Function(Protocol) onProtocol,
    void Function(Metadata) onMetadata,
  ) {
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
    // Stream only the FIRST line (commitInfo, if any): no need to read the whole
    // commit file just for its timestamp.
    await for (final line in File(
      commitPath(version),
    ).openRead().transform(utf8.decoder).transform(const LineSplitter())) {
      if (line.isEmpty) continue;
      final action = jsonDecode(line) as Map<String, dynamic>;
      final stamp = (action['commitInfo'] as Map?)?['timestamp'];
      if (stamp is int) return stamp;
      break; // commitInfo, if any, is the first action
    }
    final stat = await File(commitPath(version)).stat();
    return stat.modified.millisecondsSinceEpoch;
  }
}
