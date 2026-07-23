import 'dart:math';

// Full-jitter exponential backoff, shared by the Delta and Iceberg commit
// coordinators so their retry policy has one implementation

/// A uniformly-random delay in `(0, min(capMs, baseMs · 2^attempt)]` ms — full
/// jitter, so concurrent writers retrying a conflicted commit don't lockstep and
/// re-collide (thundering herd). Pass a [Random.secure] where cross-process
/// uniqueness matters. The shift is bounded so a high [attempt] can't overflow.
Duration fullJitterBackoff(
  int attempt,
  Random rand, {
  int baseMs = 1,
  int capMs = 256,
}) {
  final shift = attempt < 30 ? attempt : 30;
  final window = min(capMs, baseMs << shift);
  return Duration(milliseconds: rand.nextInt(window < 1 ? 1 : window) + 1);
}
