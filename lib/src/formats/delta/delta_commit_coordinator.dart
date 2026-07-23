import 'dart:io';
import 'dart:math';

import 'package:logging/logging.dart';

import '../../core/backoff.dart';
import '../../core/local_catalog.dart';
import '../../core/paths.dart';
import 'delta_log.dart';

/// The atomic "who owns the next Delta version" seam of a commit. The default
/// [FilesystemDeltaCommitCoordinator] reproduces libbigdata's O_EXCL 0-byte
/// claim + temp→rename publish + lease-reclaim on a bare filesystem, with the
/// commit-time OWNERSHIP FENCE in [DeltaLog.commit] (which re-verifies the slot
/// still holds our own claim before overwriting it, so a reclaimed writer cannot
/// clobber a concurrently-published commit).
///
/// HONEST SCOPE: on a bare filesystem the reclaim window is narrowed but not
/// fully closed; true cross-engine compare-and-swap of the version pointer
/// requires the caller's catalog service (REST / Glue / Hive). This interface is
/// the injection seam for one — a catalog implementation drives the same
/// [build] callback and installs the commit through its fenced CAS.
abstract interface class DeltaCommitCoordinator {
  /// Repeatedly: reclaim orphaned claims, pick the next version (contending with
  /// a live in-flight claim rather than gapping the log), call [build] to derive
  /// the commit actions for that version, then publish via [DeltaLog.commit].
  /// Retries on [CommitConflict]; a hard error thrown by [build] (schema
  /// mismatch, stale basis) propagates and is NOT retried. Returns the committed
  /// version. [label] names the operation in abort messages.
  Future<int> commit({
    required DeltaLog log,
    required Future<List<Map<String, dynamic>>> Function(int version) build,
    String label,
  });
}

/// Default coordinator: the reclaim + version-claim + fenced temp→rename publish
/// loop, moved here from the writer so the retry/backoff policy is one place and
/// a catalog-backed coordinator can replace it. Behavior is unchanged from the
/// former inline loops.
class FilesystemDeltaCommitCoordinator implements DeltaCommitCoordinator {
  static final Logger _log = Logger('DeltaCommitCoordinator');

  const FilesystemDeltaCommitCoordinator({this.maxRetries = 16});

  /// Max commit-conflict / contention retries before surfacing a [StateError]
  /// (bounds lock-spinning under heavy concurrent ingestion).
  final int maxRetries;

  static final _rand = Random();

  /// Exponential backoff with full jitter before retrying a conflicted commit,
  /// so concurrent writers don't tight-spin on listing + atomic rename.
  Future<void> _backoff(int attempt) =>
      Future<void>.delayed(fullJitterBackoff(attempt, _rand));

  @override
  Future<int> commit({
    required DeltaLog log,
    required Future<List<Map<String, dynamic>>> Function(int version) build,
    String label = 'commit',
  }) async {
    for (var attempt = 0; ; attempt++) {
      // Reclaim stale (past-TTL) 0-byte claims each attempt so the next version
      // stays contiguous with the last complete commit (a gap is rejected by
      // external engines).
      await log.reclaimOrphans();
      final existing = await _versions(log);
      // A FRESH (sub-TTL) 0-byte tail is a concurrent writer's live claim:
      // contend (back off + retry) rather than advancing to last+1, which would
      // gap the log and commit against stale (pre-claim) state.
      if (existing.isNotEmpty && await log.isFreshIncomplete(existing.last)) {
        if (attempt >= maxRetries) {
          throw StateError(
            'Delta $label aborted after $maxRetries attempts: version '
            '${existing.last} is contended by another writer on ${log.tablePath}',
          );
        }
        // Contention is otherwise invisible: a writer that backs off for
        // seconds looks like a hang, and this is exactly where a user debugging
        // a stalled ingest needs signal.
        _log.info(
          'Delta $label on ${log.tablePath}: version ${existing.last} is '
          'claimed by another writer, retrying (attempt ${attempt + 1} of '
          '$maxRetries)',
        );
        await _backoff(attempt);
        continue;
      }
      final version = existing.isEmpty ? 0 : existing.last + 1;
      // build() re-derives the actions for THIS version on every attempt, so a
      // commit is always tied to the version (and committed state) it reads. A
      // hard error it throws (schema mismatch / stale basis) is not retried.
      final actions = await build(version);
      try {
        return await log.commit(version, actions);
      } on CommitConflict {
        if (attempt >= maxRetries) {
          throw StateError(
            'Delta $label aborted after $maxRetries commit conflicts on '
            '${log.tablePath}',
          );
        }
        _log.info(
          'Delta $label on ${log.tablePath}: version $version lost a commit '
          'race, retrying (attempt ${attempt + 1} of $maxRetries)',
        );
        await _backoff(attempt);
      }
    }
  }

  Future<List<int>> _versions(DeltaLog log) async {
    if (!await Directory(log.logDir).exists()) return const [];
    return log.versions();
  }
}

/// A commit coordinator that holds an EXCLUSIVE OS file lock over the ENTIRE
/// read-latest-version → build → publish critical section, giving genuine
/// compare-and-swap between SEPARATE writer PROCESSES that share a lock-honoring
/// filesystem (workers / pods on one host, or a lock-honoring mount). Unlike the
/// filesystem default — whose reclaim window on a bare FS is narrowed but not
/// fully closed — concurrent commits from different processes under this
/// coordinator are truly serialized, so no update is ever lost.
///
/// It delegates the actual claim/publish to [inner] (the tested filesystem loop)
/// while holding the lock, so behavior is identical bar the added serialization.
///
/// HONEST SCOPE: CAS-correct only for writer PROCESSES that TAKE the lock (also
/// use this coordinator on a lock-honoring filesystem). POSIX `fcntl` locks are
/// per-process, so this does NOT serialize concurrent writers within one process
/// (isolates/async) — use one writer per process there. An external engine that
/// publishes without taking the lock is not coordinated — a REST/Glue/Hive
/// catalog coordinator is still the answer for true cross-engine CAS.
class FileLockDeltaCommitCoordinator implements DeltaCommitCoordinator {
  const FileLockDeltaCommitCoordinator({
    this.inner = const FilesystemDeltaCommitCoordinator(),
    this.timeout,
  });

  final DeltaCommitCoordinator inner;

  /// Bounds the wait to acquire the commit lock. Null (default) waits forever;
  /// set a [Duration] so a hung/orphaned lock holder (e.g. a crashed pod on NFS)
  /// fails loud with a [TimeoutException] instead of stranding the caller.
  final Duration? timeout;

  @override
  Future<int> commit({
    required DeltaLog log,
    required Future<List<Map<String, dynamic>>> Function(int version) build,
    String label = 'commit',
  }) => withFileLock(
    '${log.logDir}/_commit.lock',
    () => inner.commit(log: log, build: build, label: label),
    timeout: timeout,
  );
}

/// A Delta commit coordinator backed by a shared [LocalCatalog] registry — the
/// SAME fenced-CAS primitive the Iceberg registry coordinator uses, so one
/// registry can coordinate Delta and Iceberg tables together across writer
/// processes. Holds the registry lock over the whole claim/publish and records
/// the committed version. Same honest scope as [FileLockDeltaCommitCoordinator]
/// (cross-process, not cross-engine/isolate).
class LocalCatalogDeltaCommitCoordinator implements DeltaCommitCoordinator {
  const LocalCatalogDeltaCommitCoordinator(
    this.catalog, {
    this.inner = const FilesystemDeltaCommitCoordinator(),
  });

  final LocalCatalog catalog;
  final DeltaCommitCoordinator inner;

  @override
  Future<int> commit({
    required DeltaLog log,
    required Future<List<Map<String, dynamic>>> Function(int version) build,
    String label = 'commit',
  }) async {
    // The registry is persisted by `guarded` AFTER this body returns — so a
    // registry-write failure lands AFTER the real Delta commit has published.
    // Left raw, the writer would treat it as pre-publish and DELETE the staged
    // files the published commit references (corrupting the table). Once the
    // format commit is durable, any later failure is INDETERMINATE: the commit
    // landed, only the advisory pointer wasn't recorded — so the writer must
    // RETAIN its files (the log dir remains the source of truth).
    int? published;
    try {
      return await catalog.guarded(log.tablePath, (txn) async {
        final version = await inner.commit(
          log: log,
          build: build,
          label: label,
        );
        published = version;
        txn.put(log.tablePath, version: version);
        return version;
      });
    } catch (e) {
      if (published != null &&
          e is! CommitConflict &&
          e is! DeltaCommitIndeterminate) {
        throw DeltaCommitIndeterminate(published!);
      }
      rethrow;
    }
  }
}
