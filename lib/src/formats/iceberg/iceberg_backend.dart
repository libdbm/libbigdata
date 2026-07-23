import '../../core/local_catalog.dart';
import 'iceberg_catalog.dart';
import 'iceberg_commit_coordinator.dart';

/// A single Iceberg backend that owns BOTH the current-metadata read and the CAS
/// publish for a table. Folding them into one object removes the split-brain
/// hazard of injecting a SEPARATE coordinator and catalog that could resolve
/// different pointers: [commit] reads through this backend's OWN [current], so a
/// read and its compare-and-swap can never diverge (there is no injectable
/// `readCurrent` seam to feed it a different pointer than the one it swaps on).
///
/// HONEST SCOPE unchanged: a bare-filesystem backend narrows but does not close
/// the cross-ENGINE CAS window — a REST / Glue / Hive catalog is still required
/// for true cross-engine compare-and-swap of the current-metadata pointer.
abstract interface class IcebergBackend implements IcebergCatalog {
  // current(root, {allowOversized}) is inherited — the SAME read the CAS uses.

  /// Claim the next version, [build] its metadata + side files, and atomically
  /// install it as the new current pointer iff the pointer this backend's
  /// [current] reads is unchanged. Retries on a lost CAS; throws
  /// [IcebergCommitConflict] when it cannot win and [IcebergCommitIndeterminate]
  /// when the outcome is unknown. Returns the committed version. [allowOversized]
  /// forwards the recovery read-cap bypass to the internal current-pointer read.
  Future<int> commit({
    required String root,
    required Future<IcebergMetadataPublish> Function(
      Map<String, dynamic>? previous,
      int version,
    )
    build,
    bool allowOversized,
  });
}

/// Default backend: filesystem current-pointer scan (via [FilesystemIcebergCatalog])
/// plus the O_EXCL `vNNNNN.commit` marker + lease-reclaim commit loop. The loop
/// reads through THIS object's [current], so read and CAS cannot diverge.
class FilesystemIcebergBackend extends FilesystemIcebergCatalog
    implements IcebergBackend {
  const FilesystemIcebergBackend({
    FilesystemCommitCoordinator loop = const FilesystemCommitCoordinator(),
  }) : _loop = loop;

  final FilesystemCommitCoordinator _loop;

  @override
  Future<int> commit({
    required String root,
    required Future<IcebergMetadataPublish> Function(
      Map<String, dynamic>? previous,
      int version,
    )
    build,
    bool allowOversized = false,
  }) => _loop.commit(
    root: root,
    readCurrent: () => current(root, allowOversized: allowOversized),
    build: build,
  );
}

/// Filesystem backend that serializes the whole read → build → publish under an
/// exclusive OS file lock, giving genuine cross-PROCESS compare-and-swap on a
/// lock-honoring filesystem. Same honest scope as [FilesystemIcebergBackend].
class FileLockIcebergBackend extends FilesystemIcebergCatalog
    implements IcebergBackend {
  FileLockIcebergBackend({
    FilesystemCommitCoordinator loop = const FilesystemCommitCoordinator(),
  }) : _coordinator = FileLockIcebergCommitCoordinator(inner: loop);

  // The lock-under-commit loop lives in the coordinator (shared, tested); the
  // backend just feeds it this object's OWN `current`, so read and CAS agree.
  final FileLockIcebergCommitCoordinator _coordinator;

  @override
  Future<int> commit({
    required String root,
    required Future<IcebergMetadataPublish> Function(
      Map<String, dynamic>? previous,
      int version,
    )
    build,
    bool allowOversized = false,
  }) => _coordinator.commit(
    root: root,
    readCurrent: () => current(root, allowOversized: allowOversized),
    build: build,
  );
}

/// Backend backed by a shared [LocalCatalog]: O(1) current-pointer reads AND
/// catalog-fenced CAS through the SAME catalog. One object owns both, so the
/// read and the swap cannot use different catalogs — the exact split-brain the
/// old separate coordinator + catalog pair allowed.
class LocalCatalogIcebergBackend extends LocalCatalogIcebergCatalog
    implements IcebergBackend {
  LocalCatalogIcebergBackend(
    super.catalog, {
    FilesystemCommitCoordinator inner = const FilesystemCommitCoordinator(),
  }) : _inner = inner;

  final FilesystemCommitCoordinator _inner;

  late final LocalCatalogIcebergCommitCoordinator _coordinator =
      LocalCatalogIcebergCommitCoordinator(catalog, inner: _inner);

  @override
  Future<int> commit({
    required String root,
    required Future<IcebergMetadataPublish> Function(
      Map<String, dynamic>? previous,
      int version,
    )
    build,
    bool allowOversized = false,
  }) => _coordinator.commit(
    root: root,
    readCurrent: () => current(root, allowOversized: allowOversized),
    build: build,
  );
}
