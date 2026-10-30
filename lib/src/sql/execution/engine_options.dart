import 'memory_manager.dart';
import '../operators/spill.dart';
import '../../core/limits.dart';

/// Execution settings threaded from [SqlEngine] down to the blocking operators.
///
/// Blocking operators (sort, grouped aggregate) spill to disk once their buffered
/// input exceeds [spillThresholdBytes], bounding peak memory. The default budget
/// is large enough that ordinary queries stay fully in memory; set
/// [spillThresholdBytes] to `0` to disable spilling entirely.
final class EngineOptions {
  /// Memory budget (bytes) a blocking operator may buffer before spilling.
  /// `0` disables spilling (always in-memory).
  final int spillThresholdBytes;

  /// Directory for spill files. When null, the system temp directory is used.
  final String? spillDir;

  /// Ceiling on rows an unbounded blocking operator (CROSS JOIN, set ops) may
  /// produce/buffer: exceeding it throws (a running guard) rather than OOMing.
  /// Fail-closed by DEFAULT ([defaultMaxMaterializedRows]); `null` = unbounded
  /// (see [EngineOptions.unbounded]).
  final int? maxOutputRows;

  /// Optional shared budget bounding the COMBINED in-memory footprint of all
  /// blocking operators that see this options instance (one per plan, shared
  /// across queries on one engine). `null` (default) keeps per-operator budgets
  /// only: no global coordination.
  final MemoryManager? memory;

  /// Optional ceiling on the COMBINED on-disk size of live spill files across
  /// this engine's blocking operators. `null` (default) = unbounded: spilling is
  /// the OOM relief valve, so a wrong default would convert working large queries
  /// into failures: set it explicitly to bound disk use (fails fast rather than
  /// filling the disk).
  final SpillBudget? spillBudget;

  /// Caps the rows / bytes a fully-materialized result may reach before failing
  /// loud (stop-at-cap, never buffer-then-throw). Used where a result must be
  /// buffered whole: notably the MERGE source, which the Delta writer builds
  /// into an in-memory hash. `null` falls back to the library default
  /// (`defaultMaxMaterializedRows`/`Bytes`); a server can lower these to bound a
  /// single statement's footprint.
  final int? maxMaterializedRows;
  final int? maxMaterializedBytes;

  /// Default deadline for each query; a `timeout:` passed to a call overrides
  /// it. `null` (default) lets queries run to completion.
  final Duration? timeout;

  const EngineOptions({
    this.spillThresholdBytes = 256 << 20,
    this.spillDir,
    this.maxOutputRows = defaultMaxMaterializedRows, // fail-closed by default
    this.memory,
    this.spillBudget,
    this.maxMaterializedRows,
    this.maxMaterializedBytes,
    this.timeout,
  });

  /// Explicit "unbounded / offline analytics" opt-out: no output-row cap and no
  /// shared memory / spill-disk budget. Spilling stays enabled (256 MiB local
  /// threshold) so RAM is still relieved to disk. Prefer the bounded default for
  /// service use; use this only when the caller accepts unbounded resource use.
  factory EngineOptions.unbounded({
    int spillThresholdBytes = 256 << 20,
    String? spillDir,
  }) => EngineOptions(
    spillThresholdBytes: spillThresholdBytes,
    spillDir: spillDir,
    maxOutputRows: null,
  );

  /// Whether spilling is enabled.
  bool get spills => spillThresholdBytes > 0;
}
