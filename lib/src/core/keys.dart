import 'decimal.dart';

/// A composite key with structural (value) equality over its [parts].
///
/// Use this instead of string-joining key columns: a string join collides when
/// a value contains the separator (or when different tuples concatenate to the
/// same text, e.g. `(1, 23)` and `(12, 3)` → `"123"`), which silently merges
/// distinct groups or matches the wrong MERGE rows. Element-wise equality has no
/// such ambiguity, and a real `null` part stays distinct from the string
/// `"null"`.
///
/// Parts may be binary values (`Uint8List`/`List<int>`) or nested lists. Dart's
/// typed-data and `List` types use IDENTITY equality/hashCode, so two equal
/// binary values decoded into separate objects would otherwise never match —
/// breaking binary joins, GROUP BY, DISTINCT, MERGE, and equality deletes. The
/// deep [_eq]/[_hash] helpers compare/hash such parts by content.
class RowKey {
  final List<Object?> parts;

  RowKey(this.parts);

  @override
  bool operator ==(Object other) {
    if (other is! RowKey || other.parts.length != parts.length) return false;
    for (int i = 0; i < parts.length; i++) {
      if (!_eq(parts[i], other.parts[i])) return false;
    }
    return true;
  }

  @override
  int get hashCode {
    var h = 0;
    for (final p in parts) {
      h = 0x1fffffff & (h + _hash(p));
      h = 0x1fffffff & (h + ((0x0007ffff & h) << 10));
      h ^= h >> 6;
    }
    return h;
  }
}

/// Deep value-equality: recurse into `List` (incl. `Uint8List`) element-wise and
/// into `Map` (struct/map columns) by key, otherwise fall back to `==`. Scalars
/// (int/String/bool/null/double) take the fast `==` path.
///
/// NaN equals NaN here (the SQL grouping/DISTINCT convention): `double.nan ==
/// double.nan` is false, so NaN keys otherwise never matched — splitByKeys
/// made one partition PER NaN ROW, and hash-identical keys compared unequal.
bool _eq(Object? a, Object? b) {
  if (identical(a, b)) return true;
  if (a is double && b is double && a.isNaN && b.isNaN) return true;
  if (a is List && b is List) {
    if (a.length != b.length) return false;
    for (int i = 0; i < a.length; i++) {
      if (!_eq(a[i], b[i])) return false;
    }
    return true;
  }
  if (a is Map && b is Map) {
    if (a.length != b.length) return false;
    for (final entry in a.entries) {
      if (!b.containsKey(entry.key) || !_eq(entry.value, b[entry.key])) {
        return false;
      }
    }
    return true;
  }
  return a == b;
}

/// Content hash mirroring [_eq]: hash a `List` by its elements, a `Map` by its
/// (key, value) pairs ORDER-INDEPENDENTLY (a commutative sum, since two equal
/// maps may iterate in different orders), else `hashCode`. NaN hashes to one
/// canonical value, mirroring [_eq]'s NaN == NaN.
int _hash(Object? p) {
  if (p is double && p.isNaN) return double.nan.hashCode;
  if (p is List) {
    var h = 0;
    for (final e in p) {
      h = 0x1fffffff & (h + _hash(e));
      h = 0x1fffffff & (h + ((0x0007ffff & h) << 10));
      h ^= h >> 6;
    }
    return h;
  }
  if (p is Map) {
    var h = 0;
    for (final entry in p.entries) {
      h = 0x3fffffff & (h + (_hash(entry.key) ^ _hash(entry.value)));
    }
    return h;
  }
  return p.hashCode;
}

/// A hashable/equatable wrapper for a `List` (e.g. binary/`Uint8List`) value so
/// it can be used directly as a `Map`/`Set` key with VALUE semantics. Scalars
/// need no wrapping.
class ListKey {
  final List value;
  ListKey(this.value);

  @override
  bool operator ==(Object other) => other is ListKey && _eq(value, other.value);

  @override
  int get hashCode => _hash(value);
}

/// A hashable/equatable wrapper for a `Map` (struct/map column) value so it keys
/// a collection with VALUE semantics — Dart maps use identity equality/hashCode,
/// so two structurally-equal decoded maps would otherwise never match.
class MapKey {
  final Map value;
  MapKey(this.value);

  @override
  bool operator ==(Object other) => other is MapKey && _eq(value, other.value);

  @override
  int get hashCode => _hash(value);
}

/// Canonicalize a raw column value for use as a `Map`/`Set` key: scalars pass
/// through unchanged (zero overhead on the hot path), while `List`/`Uint8List`
/// values are wrapped so equal bytes compare equal. Use at any site that keys a
/// collection on a raw column value (e.g. the hash join) rather than a [RowKey].
///
/// [scale] carries the [scaledKey] contract.
Object? canonicalKey(Object? v, {int? scale}) => v is List
    ? ListKey(v)
    : v is Map
    ? MapKey(v)
    : scaledKey(v, scale);

/// Wrap a decimal column value so it keys a collection BY VALUE.
///
/// [scale] MUST be the scale of the decimal [Field] the value came from, and
/// null for every other type. A decimal column stores an UNSCALED BigInt with
/// its scale on the Field, so the value 1.5 is `BigInt(15)` at scale 1 but
/// `BigInt(150)` at scale 2. Keying on the raw BigInt therefore both MISSES
/// equal values and MATCHES unequal ones — 2.5@scale1 and 0.25@scale2 are both
/// `BigInt(25)`, a false match that a join silently emits. [Decimal] aligns
/// scales in `==` and hashes its NORMALIZED form, so wrapping fixes both.
///
/// Take the scale from the PLAN's schema, never a batch's: `schemaOf()` rebuilds
/// fields from column dataTypes alone and drops precision/scale, so a spilled
/// batch no longer knows it.
///
/// A raw `int` with a non-null [scale] is an INTEGER key reconciled against a
/// decimal key on the other side of a join/set-op: it becomes `Decimal(v, scale)`
/// (typically scale 0, its natural scale) so `Decimal`'s cross-scale `==` matches
/// it with the decimal side (`5` with `5.00`). An int key never carries a scale
/// otherwise, so the ordinary int path is untouched.
Object? scaledKey(Object? v, int? scale) => scale == null
    ? v
    : v is BigInt
    ? Decimal(v, scale)
    : v is int
    ? Decimal(BigInt.from(v), scale)
    : v;

/// Assign [key] to one of [parts] hash partitions, salted so a recursive
/// re-partition redistributes keys that collided in the parent. Hashes the
/// CANONICAL key so a binary/list/map value routes by CONTENT — a raw
/// Uint8List/List/Map hashes by identity, so two content-equal keys would
/// otherwise land in different partitions and never be compared (silently
/// dropping join matches / set-op rows). Shared by every grace-partitioned
/// operator (join, aggregate, set-op) so no router can re-introduce that bug.
///
/// [scale] carries the same contract as in [canonicalKey], and MUST match the
/// scale used when the key is compared: partitioning and matching have to agree,
/// or equal values land in different partitions and are never compared at all.
int partitionOf(Object? key, int parts, {int salt = 0, int? scale}) {
  final base = canonicalKey(key, scale: scale).hashCode;
  final h = salt == 0 ? base : Object.hash(base, salt);
  return (h % parts + parts) % parts;
}

/// A conservative resident-byte estimate for one key value — a string/binary
/// blob or a nested tuple costs far more than a fixed per-key slot, so honest
/// memory budgeting must weigh the payload, not the count.
int keyValueBytes(Object? v) {
  switch (v) {
    case null:
      return 8;
    case final String s:
      return s.length * 2 + 16; // UTF-16 units + object header
    case final List b:
      var bytes = 16;
      for (final e in b) {
        bytes += keyValueBytes(e);
      }
      return bytes;
    case final Map m:
      var bytes = 16;
      for (final entry in m.entries) {
        bytes += keyValueBytes(entry.key) + keyValueBytes(entry.value);
      }
      return bytes;
    case final BigInt _:
      return 24;
    default:
      return 16; // num / bool / other scalar
  }
}

/// Resident-byte estimate of a whole [RowKey] tuple (the key object + its list +
/// each part's payload).
int rowKeyBytes(RowKey key) {
  var bytes = 48; // RowKey + backing list + hash-set slot overhead
  for (final p in key.parts) {
    bytes += keyValueBytes(p);
  }
  return bytes;
}
