import '../../core/local_catalog.dart';
import 'dart:io';
import 'dart:math';

import '../../core/backoff.dart';
import '../../core/paths.dart';

/// Thrown when the current-metadata pointer could not be advanced because a
/// concurrent writer won the version. Extends [StateError] so it stays
/// `is StateError` with the historical message (default behavior unchanged).
class IcebergCommitConflict extends StateError {
  IcebergCommitConflict(this.tablePath, this.version)
    : super(
        'Iceberg commit conflict on $tablePath: version $version is contended '
        'by another writer (retry the write)',
      );

  final String tablePath;
  final int version;
}

/// Thrown by a coordinator that CANNOT determine whether its commit landed — e.g.
/// a catalog compare-and-swap whose response was lost to a timeout or connection
/// reset after the swap may have already succeeded. Distinct from
/// [IcebergCommitConflict] (which means the commit definitively did NOT land): on
/// this, the writer must RETAIN its staged data files, because the published
/// metadata may already reference them. Extends [StateError] for `is StateError`
/// compatibility. The default [FilesystemCommitCoordinator] never throws it (all
/// of its failure modes are pre-publish).
class IcebergCommitIndeterminate extends StateError {
  IcebergCommitIndeterminate(this.tablePath, [String? detail])
    : super(
        'Iceberg commit outcome indeterminate on $tablePath'
        '${detail == null ? '' : ': $detail'} — staged files retained (the '
        'commit may have landed; resolve the current-metadata pointer before '
        'any cleanup)',
      );

  final String tablePath;
}

/// What a coordinator publishes for a claimed version: the metadata file name
/// (`NNNNN-uuid.metadata.json`) and its already-encoded JSON body. The
/// coordinator installs this as the new current-metadata pointer.
class IcebergMetadataPublish {
  IcebergMetadataPublish(this.metadataFileName, this.metadataJson);

  final String metadataFileName;
  final String metadataJson;
}

/// The atomic "who owns the current table-metadata pointer" seam of an Iceberg
/// commit. The default [FilesystemCommitCoordinator] reproduces libbigdata's
/// O_EXCL `vNNNNN.commit` marker + lease-reclaim on a bare filesystem. Supply a
/// catalog-backed implementation (REST / Glue / Hive) for true cross-engine
/// compare-and-swap.
///
/// HONEST SCOPE: on a bare filesystem the CAS window is narrowed but not fully
/// closed; real cross-engine CAS of the current-metadata pointer requires the
/// caller's catalog service. This interface is the injection seam for one.
abstract interface class IcebergCommitCoordinator {
  /// Repeatedly: (1) read current metadata via [readCurrent] (the expected
  /// pointer), (2) derive the next [build] version, (3) call [build] to stage
  /// side files and produce the metadata JSON, (4) atomically install it as the
  /// new current pointer iff the pointer is still the one read in (1). On a lost
  /// CAS, retry from (1); if it cannot win, throw [IcebergCommitConflict].
  /// Returns the committed version.
  Future<int> commit({
    required String root,
    required Future<Map<String, dynamic>?> Function() readCurrent,
    required Future<IcebergMetadataPublish> Function(
      Map<String, dynamic>? previous,
      int version,
    )
    build,
  });
}

/// Default coordinator: the O_EXCL `vNNNNN.commit` marker + lease-reclaim commit
/// loop, moved here verbatim from the writer so behavior is byte-for-byte
/// unchanged. Only the terminal contention throw is [IcebergCommitConflict]
/// (which extends [StateError] with the same historical message).
class FilesystemCommitCoordinator implements IcebergCommitCoordinator {
  /// [maxAttempts] contended tries before surfacing a conflict; contention
  /// backs off exponentially from [baseDelay], capped at [maxDelay], with full
  /// jitter so concurrent writers don't lockstep-collide (thundering herd). The
  /// defaults give a materially longer window than the historical fixed
  /// 25 ms × 40 (~1 s) so a busy table doesn't spuriously fail a valid writer.
  const FilesystemCommitCoordinator({
    this.maxAttempts = 60,
    this.baseDelay = const Duration(milliseconds: 25),
    this.maxDelay = const Duration(seconds: 1),
    this.maxElapsed = const Duration(seconds: 10),
  });

  final int maxAttempts;
  final Duration baseDelay;
  final Duration maxDelay;

  /// Overall wall-clock ceiling on contention retries, so a caller can bound the
  /// commit within a request SLO instead of waiting out all [maxAttempts]. `null`
  /// disables the deadline (attempt count is then the only bound). Whichever of
  /// [maxAttempts] / [maxElapsed] is reached first surfaces an
  /// [IcebergCommitConflict].
  final Duration? maxElapsed;

  /// A version-claim marker older than this is presumed orphaned by a crashed
  /// writer (a real commit takes milliseconds) and is reclaimed.
  static const _commitLease = Duration(minutes: 5);

  @override
  Future<int> commit({
    required String root,
    required Future<Map<String, dynamic>?> Function() readCurrent,
    required Future<IcebergMetadataPublish> Function(
      Map<String, dynamic>? previous,
      int version,
    )
    build,
  }) async {
    // Secure entropy so writers in separate processes/isolates don't seed the
    // same jitter sequence and re-collide in lockstep.
    final rand = Random.secure();
    final start = DateTime.now();
    for (var attempt = 0; ; attempt++) {
      final previous = await readCurrent();
      final version = ((previous?['_version'] as int?) ?? -1) + 1;
      final marker = File(
        '$root/metadata/v${version.toString().padLeft(5, '0')}.commit',
      );
      try {
        await marker.create(exclusive: true);
      } on PathExistsException {
        // The version is claimed. If the marker is stale, the holder is presumed
        // dead — reclaim it. If it is fresh (or already released), a writer
        // is/was active: back off and retry (the next iteration re-reads the
        // latest version). Bounded so a wedged marker surfaces a conflict.
        if (await marker.exists()) {
          final age = DateTime.now().difference((await marker.stat()).modified);
          if (age > _commitLease) {
            try {
              await marker.delete();
            } catch (_) {}
            continue;
          }
        }
        // Surface a conflict once EITHER bound is hit — the attempt count or the
        // wall-clock deadline — so a caller with a request SLO isn't held for the
        // full attempt budget.
        final elapsed = DateTime.now().difference(start);
        if (attempt >= maxAttempts ||
            (maxElapsed != null && elapsed >= maxElapsed!)) {
          throw IcebergCommitConflict(root, version);
        }
        // Full-jitter exponential backoff capped at maxDelay (shared helper).
        await Future<void>.delayed(
          fullJitterBackoff(
            attempt,
            rand,
            baseMs: baseDelay.inMilliseconds,
            capMs: maxDelay.inMilliseconds,
          ),
        );
        continue;
      }

      // Record our claim's identity (its mtime) so we can detect a concurrent
      // writer reclaiming the marker while our build runs.
      final claimStamp = (await marker.stat()).modified;
      try {
        final pub = await build(previous, version);
        // Re-verify we still own the claim before publishing: a slow build that
        // outlived the lease could have had its marker reclaimed by another
        // writer computing the SAME version — publishing now would create
        // split-brain metadata. Fail loud (retryable) rather than publish.
        // Best-effort (filesystem mtime granularity narrows, not closes, the
        // window — a catalog coordinator with fencing is the real fix).
        if (!await marker.exists() ||
            (await marker.stat()).modified != claimStamp) {
          throw IcebergCommitConflict(root, version);
        }
        // metadata.json is the commit point (the highest-numbered one is
        // current), so it must appear atomically: stage to a temp file, then
        // rename. A crash mid-write leaves the temp, never a half-written
        // current metadata.
        final metaPath = '$root/metadata/${pub.metadataFileName}';
        final temp = File('$metaPath.tmp');
        await temp.writeAsString(pub.metadataJson, flush: true);
        // The rename IS the publish. On NFS / object-backed filesystems (or after
        // a lost response) it can land remotely yet report an error. If the
        // destination now exists — or we can't tell — the commit may have
        // PUBLISHED, so surface IcebergCommitIndeterminate (the writer RETAINS its
        // staged data on it) rather than letting a raw error propagate to the
        // writer's cleanup, which would delete files the published snapshot
        // references. Only a confirmed-absent destination is a clean pre-publish
        // failure to rethrow.
        try {
          await temp.rename(metaPath);
        } catch (e) {
          bool present;
          try {
            present = await File(metaPath).exists();
          } catch (_) {
            present =
                true; // can't stat → fail safe (assume it may have landed)
          }
          if (present) {
            throw IcebergCommitIndeterminate(
              root,
              'metadata rename reported an error but $metaPath exists ($e)',
            );
          }
          rethrow;
        }
        return version;
      } finally {
        // Release the transient version claim (published or failed) — but ONLY
        // if we still own it; if another writer reclaimed the stale marker and
        // created its own, deleting it would drop that writer's live claim.
        try {
          if (await marker.exists() &&
              (await marker.stat()).modified == claimStamp) {
            await marker.delete();
          }
        } catch (_) {}
      }
    }
  }
}

/// An Iceberg commit coordinator that holds an EXCLUSIVE OS file lock over the
/// entire read-current → build → publish critical section, giving genuine
/// compare-and-swap between SEPARATE writer PROCESSES that share a lock-honoring
/// filesystem. Because commits are truly serialized, two processes can never
/// claim the same version (closing the highest-filename split-brain race at its
/// source) and no update is lost. Delegates the claim/publish to [inner] (the
/// tested filesystem loop) under the lock.
///
/// HONEST SCOPE: CAS-correct only for writer PROCESSES that TAKE the lock (also
/// use this coordinator on a lock-honoring filesystem). POSIX `fcntl` locks are
/// per-process, so this does NOT serialize concurrent writers within one process
/// (isolates/async). An external engine publishing without the lock is not
/// coordinated — a REST/Glue/Hive catalog coordinator is still the answer for
/// true cross-engine CAS. Readers ([IcebergTable]) need no lock: a publish is an
/// atomic rename, so a reader sees the old or the new metadata, never a partial
/// state.
class FileLockIcebergCommitCoordinator implements IcebergCommitCoordinator {
  const FileLockIcebergCommitCoordinator({
    this.inner = const FilesystemCommitCoordinator(),
    this.timeout,
  });

  final IcebergCommitCoordinator inner;

  /// Bounds the wait to acquire the commit lock. Null (default) waits forever;
  /// set a [Duration] so a hung/orphaned lock holder (e.g. a crashed pod on NFS)
  /// fails loud with a [TimeoutException] instead of stranding the caller.
  final Duration? timeout;

  @override
  Future<int> commit({
    required String root,
    required Future<Map<String, dynamic>?> Function() readCurrent,
    required Future<IcebergMetadataPublish> Function(
      Map<String, dynamic>? previous,
      int version,
    )
    build,
  }) => withFileLock(
    '$root/metadata/_commit.lock',
    () => inner.commit(root: root, readCurrent: readCurrent, build: build),
    timeout: timeout,
  );
}

/// An Iceberg commit coordinator backed by a shared [LocalCatalog] registry —
/// the SAME fenced-CAS primitive the Delta registry coordinator uses (one
/// registry coordinates both formats). Holds the registry lock over the whole
/// read-current → build → publish, records the committed version AND the
/// published metadata filename as the current pointer, so a paired
/// [LocalCatalogIcebergCatalog] resolves `current` in O(1) (no metadata-dir
/// scan). Same honest scope as the file-lock coordinator (cross-process, not
/// cross-engine/isolate).
class LocalCatalogIcebergCommitCoordinator implements IcebergCommitCoordinator {
  const LocalCatalogIcebergCommitCoordinator(
    this.catalog, {
    this.inner = const FilesystemCommitCoordinator(),
  });

  final LocalCatalog catalog;
  final IcebergCommitCoordinator inner;

  @override
  Future<int> commit({
    required String root,
    required Future<Map<String, dynamic>?> Function() readCurrent,
    required Future<IcebergMetadataPublish> Function(
      Map<String, dynamic>? previous,
      int version,
    )
    build,
  }) async {
    // The registry is persisted by `guarded` AFTER this body returns — so a
    // registry-write failure lands AFTER the real metadata.json has published.
    // Left raw, the writer would treat it as pre-publish and DELETE the staged
    // data/manifest files the published metadata references (corrupting the
    // table). Once the metadata is durable, any later failure is INDETERMINATE:
    // the commit landed, only the advisory pointer wasn't recorded — so the
    // writer must RETAIN its files (the highest-numbered metadata.json is
    // current regardless).
    var published = false;
    try {
      return await catalog.guarded(root, (txn) async {
        String? metadataFile;
        final version = await inner.commit(
          root: root,
          readCurrent: readCurrent,
          build: (previous, v) async {
            final pub = await build(previous, v);
            metadataFile = pub.metadataFileName; // capture the current pointer
            return pub;
          },
        );
        published = true;
        txn.put(root, version: version, pointer: metadataFile);
        return version;
      });
    } catch (e) {
      if (published &&
          e is! IcebergCommitConflict &&
          e is! IcebergCommitIndeterminate) {
        throw IcebergCommitIndeterminate(
          root,
          'the metadata.json published but the catalog registry pointer could '
          'not be recorded: $e',
        );
      }
      rethrow;
    }
  }
}
