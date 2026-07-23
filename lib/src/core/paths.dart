import 'dart:async';
import 'dart:io';
import 'dart:math';

import 'package:path/path.dart' as p;

import 'backoff.dart';
import 'ids.dart' as ids;

/// Flush [path]'s contents to stable media (fsync).
///
/// For a file written through an [IOSink]: `IOSink.flush` only drains the
/// Dart-side buffer to the OS, which is easy to mistake for durability. Dart
/// exposes a real fsync only on [RandomAccessFile], so re-open the file (append
/// mode, which neither truncates nor moves the writer's data) purely to flush
/// it. Writers holding their own [RandomAccessFile] should call `flush()` on it
/// directly instead.
///
/// Covers the file's CONTENT only — see [replaceAtomic] on the directory entry.
Future<void> syncFile(String path) async {
  final file = await File(path).open(mode: FileMode.append);
  try {
    await file.flush();
  } finally {
    await file.close();
  }
}

/// Atomically replace [dest] with [temp] (a fully-written temp file).
///
/// ATOMIC, not durable: the rename either happens or it doesn't, so a reader
/// never sees a half-written [dest] — but POSIX does not promise the rename
/// itself survives a crash without an fsync of the PARENT DIRECTORY, which
/// `dart:io` cannot express (it would need FFI, and this package has no native
/// dependencies). Callers must still fsync the file's contents before
/// publishing it (see [syncFile]), so a surviving directory entry cannot point
/// at unwritten data.
///
/// On POSIX, `File.rename` atomically replaces an existing destination. On
/// Windows, `rename` throws
/// when the destination exists, so we fall back to a BACKUP-SWAP: rename the old
/// dest aside to a backup, rename temp into place, then delete the backup — and
/// on an in-process rename FAILURE restore the backup. The POSIX path is
/// unchanged and genuinely crash-atomic.
///
/// WINDOWS IS NOT CRASH-ATOMIC in the rename itself. Between renaming
/// dest→backup and installing temp→dest there is a window where dest does not
/// exist; the in-process error handler recovers it, and a PROCESS/OS CRASH in
/// that window leaves dest missing with the only intact copy at
/// `<dest>.bak-<token>`. [recoverBackup] heals that on the next open (call it
/// before reading dest — [LocalCatalog] does), so the backup is restored rather
/// than the missing file read as fresh/empty. What remains genuinely POSIX-only
/// is DURABILITY: this cannot fsync the parent directory (no FFI), so a rename
/// that returns success can still be lost to power loss on either platform. A
/// durable directory-entry tier would need an FFI/platform adapter — out of
/// scope for a pure-Dart package.
Future<void> replaceAtomic(File temp, String dest) async {
  try {
    await temp.rename(dest);
  } on FileSystemException {
    if (!Platform.isWindows) rethrow;
    final target = File(dest);
    if (!await target.exists()) {
      await temp.rename(dest); // nothing to preserve
      return;
    }
    final backup = '$dest.bak-${ids.token()}';
    await target.rename(backup);
    try {
      await temp.rename(dest);
    } catch (e) {
      // temp.rename failed; the prior valid file now lives at [backup]. Try to
      // restore it — but if THAT throws too, dest would be left missing with the
      // only intact copy at [backup]. Surface where it is so it's recoverable
      // instead of rethrowing the opaque original error over a missing dest.
      try {
        await File(backup).rename(dest);
      } catch (restore) {
        throw FileSystemException(
          'replaceAtomic could not install "${temp.path}" nor restore the prior '
          'file; it is preserved at "$backup" — rename it back to "$dest" to '
          'recover (install error: $e; restore error: $restore)',
          dest,
        );
      }
      rethrow;
    }
    try {
      await File(backup).delete();
    } catch (_) {
      /* the swap succeeded; a leftover backup is harmless */
    }
  }
}

/// Recover from a crash during a Windows backup-swap [replaceAtomic]. That swap
/// renames the old [dest] aside to `<dest>.bak-<token>`, installs the new file,
/// then deletes the backup. A PROCESS/OS crash between the two renames leaves
/// [dest] missing with the only intact copy at the backup; a crash after the
/// install but before the cleanup leaves an orphan backup beside a good [dest].
/// Call this before reading [dest] on open: restore the backup when [dest] is
/// missing, or delete the orphan when [dest] is present. A no-op when no backup
/// exists (the normal case, and always on POSIX, where the swap is never taken).
Future<void> recoverBackup(String dest) async {
  final name = p.basename(dest);
  final parent = Directory(p.dirname(dest));
  if (!await parent.exists()) return;
  final backups = <File>[
    await for (final entry in parent.list(followLinks: false))
      if (entry is File && p.basename(entry.path).startsWith('$name.bak-'))
        entry,
  ];
  if (backups.isEmpty) return;
  if (await File(dest).exists()) {
    // dest is intact: the backups are orphans from a completed swap — drop them.
    for (final backup in backups) {
      try {
        await backup.delete();
      } catch (_) {
        /* a leftover backup is harmless; best-effort cleanup */
      }
    }
    return;
  }
  // dest is missing: an interrupted swap left exactly one backup (the lock
  // serializes swaps). Reinstate it; drop any extras defensively.
  await backups.first.rename(dest);
  for (final backup in backups.skip(1)) {
    try {
      await backup.delete();
    } catch (_) {
      /* best-effort */
    }
  }
}

/// Canonicalize [path] by resolving the symlinks of its longest EXISTING prefix
/// and re-appending the not-yet-created tail — so a symlinked ancestor is
/// followed even when the leaf (e.g. a write target) does not exist yet.
///
/// Fails CLOSED: if the existing prefix cannot be resolved (a broken or
/// unreadable symlink, a permission error), this throws rather than falling back
/// to the lexical path — approving an unresolvable link via its lexical name is
/// exactly how a sandbox check is bypassed. Callers ([confine], the SQL sandbox)
/// treat the throw as access-denied.
String canonicalize(String path) {
  var probe = p.normalize(path);
  final tail = <String>[];
  while (true) {
    if (FileSystemEntity.typeSync(probe, followLinks: false) !=
        FileSystemEntityType.notFound) {
      final real = File(probe).resolveSymbolicLinksSync();
      return tail.isEmpty ? real : p.joinAll([real, ...tail.reversed]);
    }
    final parent = p.dirname(probe);
    if (parent == probe) {
      return p.normalize(path); // hit root; nothing existed
    }
    tail.add(p.basename(probe));
    probe = parent;
  }
}

/// Confine [resolved] to [root]: return it when its canonical path is [root] or
/// lies within it, else throw [ArgumentError]. Both sides are canonicalized so a
/// symlink that points outside the root is caught even when the lexical path
/// looks contained. [label] names the offending value in the error (defaults to
/// [resolved]).
///
/// Used to stop a lake table (Delta/Iceberg) whose metadata embeds an absolute
/// or `file://` data/delete/deletion-vector path from reading files outside the
/// table root. Trusted external-location tables opt out at the reader boundary.
String confine(String root, String resolved, {String? label}) {
  final canonicalRoot = canonicalize(root);
  final canonical = canonicalize(resolved);
  if (canonical != canonicalRoot && !p.isWithin(canonicalRoot, canonical)) {
    throw ArgumentError(
      'Access denied: "${label ?? resolved}" resolves outside the table '
      'root "$root" (pass allowExternalPaths to permit external locations)',
    );
  }
  return resolved;
}

/// Resolve an embedded Delta/lake data-file [path] to a filesystem path,
/// confined to [root] unless [allowExternal]. An absolute path (leading `/`) OR
/// a URI with a scheme (`s3://`, `file://`, …) is taken as-is; anything else is
/// joined under [root].
///
/// ONE resolver for the Delta reader AND writer: they had diverged — the reader
/// treated a URI-scheme path as absolute, the writer treated ONLY a leading `/`
/// as absolute — so with [allowExternal] a data file recorded as `s3://…` was
/// resolved correctly on read but joined under [root] (a wrong path) on the
/// writer's update/cleanup, silently reading/deleting the wrong file.
String resolveDataFile(String root, String path, {bool allowExternal = false}) {
  final resolved = (path.startsWith('/') || path.contains('://'))
      ? path
      : '$root/${Uri.decodeFull(path)}';
  return allowExternal ? resolved : confine(root, resolved, label: path);
}

/// The default table name for a file path: its basename without extension
/// (`/data/sales.csv` → `sales`). Shared by the planner and the engine so a
/// file referenced in FROM resolves to the same name in both.
String tableNameFromPath(String filePath) =>
    p.basenameWithoutExtension(filePath);

/// Per-isolate, per-path async mutex tails: each value is the tail of the
/// Future-chain serializing [withFileLock] calls on that lock path WITHIN this
/// isolate. A top-level (isolate-private) map — isolates share no memory, so
/// this cannot and does not bridge separate isolates.
final Map<String, Future<void>> _isolateLocks = {};

/// Run [body] while holding an EXCLUSIVE OS advisory lock on [lockPath] (created
/// if absent), releasing it (and closing the handle) afterward whether [body]
/// succeeds or throws. Provides genuine mutual exclusion — real compare-and-swap
/// — between SEPARATE OS PROCESSES that share a lock-honoring filesystem (workers
/// / pods on one host, or a mount that honors `fcntl`/`LockFileEx`). Used to
/// serialize Delta/Iceberg commits so concurrent writer PROCESSES can't lose an
/// update.
///
/// SAME-ISOLATE concurrency IS serialized: an in-isolate per-path async mutex
/// chains overlapping calls on one isolate, so two un-awaited `withFileLock`s on
/// the same path can't run [body] concurrently even though POSIX `fcntl` (owned
/// by the PROCESS) would grant both the OS lock immediately.
///
/// SEPARATE ISOLATES are NOT serialized. Dart isolates share no memory, so the
/// in-isolate mutex can't reach across them, and the process-owned `fcntl` lock
/// grants each the same time. Use one writer per process, or an external
/// coordinator, for cross-isolate writers to the same table.
///
/// [timeout] bounds the TOTAL wait to acquire (the in-isolate mutex AND the OS
/// lock): null (default) waits forever (`blockingExclusive`, unchanged); a
/// [Duration] retries the non-blocking lock with jittered backoff and throws
/// [TimeoutException] once the deadline passes — a hung or orphaned holder (a
/// crashed pod on NFS) then fails loud instead of stranding the caller forever.
Future<T> withFileLock<T>(
  String lockPath,
  Future<T> Function() body, {
  Duration? timeout,
}) async {
  final key = p.canonicalize(lockPath);
  final stopwatch = timeout == null ? null : (Stopwatch()..start());
  Duration remaining() {
    final left = timeout! - stopwatch!.elapsed;
    return left.isNegative ? Duration.zero : left;
  }

  // In-isolate mutex: wait for the current tail on this path, then install our
  // own link. Serializes overlapping async on ONE isolate.
  final previous = _isolateLocks[key] ?? Future<void>.value();
  final done = Completer<void>();
  _isolateLocks[key] = done.future;

  void release() {
    done.complete();
    // Drop the map entry only if no one chained after us (else the key belongs
    // to a later caller's tail — leave it).
    if (identical(_isolateLocks[key], done.future)) _isolateLocks.remove(key);
  }

  // Whether we got PAST the in-isolate mutex wait (became the in-isolate holder).
  var acquired = false;
  try {
    if (timeout == null) {
      await previous;
    } else {
      await previous.timeout(
        remaining(),
        onTimeout: () => throw TimeoutException(
          'withFileLock: timed out waiting for an in-isolate holder of '
          '"$lockPath"',
          timeout,
        ),
      );
    }
    acquired = true;

    await Directory(p.dirname(lockPath)).create(recursive: true);
    // Append (not write) so acquiring the lock never truncates the lock file;
    // locking is on the fd, independent of write position.
    final handle = await File(lockPath).open(mode: FileMode.append);
    try {
      await _acquireOsLock(handle, lockPath, timeout, remaining);
      try {
        return await body();
      } finally {
        await handle.unlock();
      }
    } finally {
      await handle.close();
    }
  } finally {
    if (acquired) {
      // We were the in-isolate holder (or failed to get the OS lock AFTER the
      // predecessor finished — either way no in-isolate holder remains): release
      // the next waiter.
      release();
    } else {
      // We TIMED OUT waiting for `previous`, which is still the real holder. Do
      // NOT release now — that would let the next waiter run concurrently with
      // it. Keep our barrier chained to the real predecessor's completion.
      previous.whenComplete(release);
    }
  }
}

/// Acquire the exclusive OS lock on [handle]. Without a [timeout], WAIT
/// (`blockingExclusive`). With one, retry the NON-blocking `exclusive` variant
/// (which throws when another PROCESS holds it) with jittered backoff until
/// [remaining] reaches zero, then throw [TimeoutException]. Same-isolate
/// contention never reaches here — the in-isolate mutex already serialized it —
/// so the retry only spins on genuine cross-process contention.
Future<void> _acquireOsLock(
  RandomAccessFile handle,
  String lockPath,
  Duration? timeout,
  Duration Function() remaining,
) async {
  if (timeout == null) {
    await handle.lock(FileLock.blockingExclusive);
    return;
  }
  final rand = Random();
  var attempt = 0;
  while (true) {
    try {
      await handle.lock(FileLock.exclusive);
      return;
    } on FileSystemException {
      if (remaining() == Duration.zero) {
        throw TimeoutException(
          'withFileLock: timed out acquiring the OS lock on "$lockPath"',
          timeout,
        );
      }
      final backoff = fullJitterBackoff(attempt++, rand);
      final left = remaining();
      await Future<void>.delayed(backoff < left ? backoff : left);
    }
  }
}
