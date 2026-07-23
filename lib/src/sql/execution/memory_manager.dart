/// A shared, opt-in budget for the in-memory buffers of blocking operators.
///
/// Each blocking operator (grouped aggregate, sort, hash join) independently
/// caps its own buffer at `spillThresholdBytes`. Without coordination, N
/// operators in one plan (e.g. a join feeding a sort, or nested joins) can each
/// buffer up to that limit, so peak memory is `N × threshold`. A single
/// [MemoryManager] shared across a plan (and across queries on one engine) bounds
/// the *combined* in-memory footprint: an operator [reserve]s before growing its
/// buffer and spills to disk when the global pool is exhausted, then [release]s
/// when it spills/clears/closes.
///
/// Reservation is advisory accounting (a counter), not an allocator — it never
/// blocks; a denied [reserve] simply tells the caller to spill instead.
///
/// CONTRACT: this class is isolate-private and synchronous. [reserve]/[release]
/// do a plain read-modify-write of [_used] with no `await` in between, so on
/// Dart's single event loop they can't interleave — no lock is needed. Do NOT
/// share one instance across isolates, and do NOT introduce an `await` inside the
/// reserve/release critical section; either would require a real lock or serial
/// queue to keep the counter consistent.
class MemoryManager {
  /// Total bytes operators may hold in memory at once across everything sharing
  /// this manager.
  final int budget;

  /// When true, an oversized-when-empty request is DENIED (a strict ceiling)
  /// rather than granted for progress. Safe because a denied [reserve] makes the
  /// caller spill, not fail — but it does forfeit the deadlock-avoidance
  /// guarantee for an indivisible unit larger than the whole budget, so it is
  /// opt-in. Default (`false`) keeps the advisory, progress-guaranteeing behavior.
  final bool hardCap;

  int _used = 0;

  MemoryManager(this.budget, {this.hardCap = false});

  /// Bytes currently reserved.
  int get used => _used;

  /// Grant [bytes] iff it keeps the total within [budget]; returns whether it was
  /// granted (and only then counted). Unless [hardCap], a request larger than the
  /// whole budget is granted when the pool is otherwise empty, so a single
  /// oversized batch can still make progress rather than deadlock.
  bool reserve(int bytes) {
    if (bytes <= 0) return true;
    if (_used + bytes <= budget || (!hardCap && _used == 0)) {
      _used += bytes;
      return true;
    }
    return false;
  }

  /// Return [bytes] to the pool. Clamped at zero so double-release can't drive
  /// the counter negative (which would falsely starve later operators).
  void release(int bytes) {
    if (bytes <= 0) return;
    _used -= bytes;
    if (_used < 0) _used = 0;
  }
}
