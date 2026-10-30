import 'dart:math';

// One secure RNG shared across the library's id/token generation, so concurrent
// writers (isolates / pods) can't collide via identical clock-seeded `Random()`
// in the same millisecond. Secure entropy is the point: do not replace with a
// seeded Random.
final _secure = Random.secure();

/// A random lowercase-hex token of [chars] characters (default 12 = 48 bits),
/// for collision-free temp-file / part-file suffixes. The value is opaque, only
/// its uniqueness matters.
String token([int chars = 12]) => [
  for (int i = 0; i < chars; i++) _secure.nextInt(16).toRadixString(16),
].join();

/// A random hex identifier in 8-4-4-4-12 (UUID) shape, for opaque data-file /
/// table names. Not RFC-4122 versioned: callers that need a real v4 UUID with
/// version/variant bits (e.g. deletion vectors) generate their own.
String uuid() {
  String hex(int count) => [
    for (int i = 0; i < count; i++)
      _secure.nextInt(256).toRadixString(16).padLeft(2, '0'),
  ].join();
  return '${hex(4)}-${hex(2)}-${hex(2)}-${hex(2)}-${hex(6)}';
}
