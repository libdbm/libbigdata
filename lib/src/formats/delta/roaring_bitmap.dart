import 'dart:collection';
import 'dart:typed_data';

/// Roaring bitmap (de)serialization in the portable format used by Delta Lake
/// deletion vectors: the 32-bit "portable" container format wrapped in the
/// 64-bit RoaringTreemap layout (u64 LE bucket count, then per bucket a u32 LE
/// high key followed by a full 32-bit bitmap). Membership is what DVs need, so
/// values are exchanged as plain int sets.
class Roaring {
  static const int serialCookie = 12347; // with run containers
  static const int serialCookieNoRun = 12346;

  /// DoS backstop on TOTAL decoded cardinality when the caller supplies no exact
  /// count. A run container amplifies 4 payload bytes into up to 65536 values, so
  /// the descriptor-count guards alone don't bound the transient `Set<int>`. A
  /// Delta deletion vector is bounded by its data file's row count; callers that
  /// know it (via the DV descriptor) pass the exact `cardinality` for a tight
  /// bound, and this caps the rest.
  static const int _maxDecodedCardinality = 1 << 31; // ~2.1 billion

  // --- 64-bit treemap ------------------------------------------------------

  static Uint8List serializeTreemap(Iterable<int> values) {
    final buckets = SplayTreeMap<int, List<int>>();
    for (final v in values) {
      buckets.putIfAbsent(v >> 32, () => []).add(v & 0xFFFFFFFF);
    }
    final out = BytesBuilder();
    final header = ByteData(8)..setUint64(0, buckets.length, Endian.little);
    out.add(header.buffer.asUint8List());
    for (final entry in buckets.entries) {
      final key = ByteData(4)..setUint32(0, entry.key, Endian.little);
      out.add(key.buffer.asUint8List());
      out.add(_serialize32(entry.value));
    }
    return out.toBytes();
  }

  /// Reject an implausible element [count] read from untrusted input before it is
  /// used to allocate or drive a decode loop: it can't exceed the remaining bytes
  /// divided by the minimum bytes each element consumes.
  static void _checkCount(
    int count,
    int minBytesPerElem,
    int pos,
    ByteData bd,
  ) {
    final remaining = bd.lengthInBytes - pos;
    if (count < 0 || remaining < 0 || count > remaining ~/ minBytesPerElem) {
      throw FormatException(
        'Roaring count $count exceeds the $remaining bytes remaining',
      );
    }
  }

  /// The portable-format values of [bytes] as a plain `Set<int>`.
  ///
  /// Delegates to [deserializeToBitmap]. This used to be a byte-for-byte second
  /// decoder, and every hardening — the strictly-increasing bucket-key check,
  /// the cardinality budget — had to be hand-copied into both; the next one
  /// would have landed in only one. Nothing in `lib/` needs a Set: prefer the
  /// bitmap unless you specifically want the values materialized.
  static Set<int> deserializeTreemap(
    Uint8List bytes, [
    int start = 0,
    int? cardinality,
  ]) => deserializeToBitmap(bytes, start, cardinality).values.toSet();

  /// Roaring treemap buckets are serialized in strictly-increasing high-key
  /// order. Reject a duplicate/out-of-order key: duplicates collapse in the
  /// bitmap yet still count toward a declared cardinality, silently dropping
  /// deletes.
  static void _checkBucketKey(int key, int prevKey) {
    if (key <= prevKey) {
      throw FormatException(
        'Roaring bucket keys not strictly increasing (got $key after $prevKey)',
      );
    }
  }

  /// A caller that declares an EXACT [cardinality] (e.g. a Delta DV descriptor)
  /// lets us reject a payload that decodes to a different count — a deliberately-
  /// valid bomb whose CRC checks out but whose contents disagree with the header.
  static void _checkCardinality(int decoded, int? cardinality) {
    if (cardinality != null && decoded != cardinality) {
      throw FormatException(
        'Roaring decoded $decoded values but the descriptor declared '
        '$cardinality',
      );
    }
  }

  /// Like [deserializeTreemap] but builds a compressed [RoaringBitmap] directly,
  /// so the persistent membership structure never materializes a `Set<int>`
  /// (only a transient per-bucket set is held). For large deletion vectors this
  /// is bits/element instead of ~40-50 bytes/element.
  static RoaringBitmap deserializeToBitmap(
    Uint8List bytes, [
    int start = 0,
    int? cardinality,
  ]) {
    final bd = ByteData.sublistView(bytes);
    var pos = start;
    final bucketCount = bd.getUint64(pos, Endian.little);
    pos += 8;
    // Each bucket consumes >= 8 bytes (4-byte key + >= 4-byte container cookie);
    // reject an implausible count before iterating/allocating.
    _checkCount(bucketCount, 8, pos, bd);
    final limit = cardinality ?? _maxDecodedCardinality;
    final result = RoaringBitmap();
    var total = 0;
    var prevKey = -1;
    for (int b = 0; b < bucketCount; b++) {
      final key = bd.getUint32(pos, Endian.little);
      pos += 4;
      _checkBucketKey(key, prevKey);
      prevKey = key;
      final (low, consumed) = _deserialize32(bd, pos, limit - total);
      pos += consumed;
      total += low.length;
      for (final v in low) {
        result.add((key << 32) | v);
      }
    }
    // Compare the FINAL distinct membership, not the running sum (see
    // deserializeTreemap) — duplicate values collapse in RoaringBitmap.add.
    _checkCardinality(result.length, cardinality);
    return result;
  }

  // --- 32-bit portable bitmap ---------------------------------------------

  /// Serialize 16-bit-low values grouped into containers. [lows] are the low
  /// 16 bits within a single 32-bit bucket. Always emits the NO_RUNCONTAINER
  /// cookie (array/bitmap containers only) with an offset header.
  static Uint8List _serialize32(List<int> lows) {
    final containers = SplayTreeMap<int, List<int>>();
    for (final v in lows) {
      containers.putIfAbsent(v >> 16, () => []).add(v & 0xFFFF);
    }
    final keys = containers.keys.toList();
    final count = keys.length;

    final bodies = <Uint8List>[];
    for (final key in keys) {
      final values = containers[key]!..sort();
      if (values.length <= 4096) {
        final body = ByteData(2 * values.length);
        for (int i = 0; i < values.length; i++) {
          body.setUint16(i * 2, values[i], Endian.little);
        }
        bodies.add(body.buffer.asUint8List());
      } else {
        final body = Uint8List(8192);
        final bd = ByteData.sublistView(body);
        for (final v in values) {
          final word = v >> 6;
          final bit = v & 63;
          bd.setUint64(
            word * 8,
            bd.getUint64(word * 8, Endian.little) | (1 << bit),
            Endian.little,
          );
        }
        bodies.add(body);
      }
    }

    final headerSize =
        8 + count * 4 + count * 4; // cookie+count, descriptive, offsets
    final out = BytesBuilder();
    final head = ByteData(8)
      ..setUint32(0, serialCookieNoRun, Endian.little)
      ..setUint32(4, count, Endian.little);
    out.add(head.buffer.asUint8List());

    final desc = ByteData(count * 4);
    for (int i = 0; i < count; i++) {
      final card = containers[keys[i]]!.length;
      desc.setUint16(i * 4, keys[i], Endian.little);
      desc.setUint16(i * 4 + 2, card - 1, Endian.little);
    }
    out.add(desc.buffer.asUint8List());

    final offsets = ByteData(count * 4);
    var running = headerSize;
    for (int i = 0; i < count; i++) {
      offsets.setUint32(i * 4, running, Endian.little);
      running += bodies[i].length;
    }
    out.add(offsets.buffer.asUint8List());

    for (final body in bodies) {
      out.add(body);
    }
    return out.toBytes();
  }

  /// Deserialize a 32-bit portable bitmap at [start]; returns the low 32-bit
  /// values and the number of bytes consumed. [budget] is the remaining decoded
  /// cardinality allowed across the whole payload — a container is rejected
  /// BEFORE expansion once it would exceed it, so a run-container bomb (4 bytes →
  /// 65536 values) can't inflate the transient `Set<int>`.
  static (Set<int>, int) _deserialize32(ByteData bd, int start, int budget) {
    var pos = start;
    final cookie = bd.getUint32(pos, Endian.little);
    pos += 4;

    bool hasRun;
    int count;
    Uint8List? runBitset;
    if ((cookie & 0xFFFF) == serialCookie) {
      hasRun = true;
      count = ((cookie >> 16) & 0xFFFF) + 1;
      final runBytes = (count + 7) >> 3;
      runBitset = Uint8List(runBytes);
      for (int i = 0; i < runBytes; i++) {
        runBitset[i] = bd.getUint8(pos + i);
      }
      pos += runBytes;
    } else if (cookie == serialCookieNoRun) {
      hasRun = false;
      count = bd.getUint32(pos, Endian.little);
      pos += 4;
    } else {
      throw FormatException('Unknown Roaring cookie: $cookie');
    }

    // Validate the attacker-controlled container count against the bytes that
    // actually remain (each container descriptor is 4 bytes) BEFORE allocating —
    // otherwise a tiny payload declaring count=0xFFFFFFFF forces a multi-GB
    // List.filled. CRC (checked later) does not protect a deliberately-valid one.
    _checkCount(count, 4, pos, bd);
    final keys = List<int>.filled(count, 0);
    final cards = List<int>.filled(count, 0);
    for (int i = 0; i < count; i++) {
      keys[i] = bd.getUint16(pos, Endian.little);
      cards[i] = bd.getUint16(pos + 2, Endian.little) + 1;
      pos += 4;
    }

    // Offset header present unless run-cookie with fewer than 4 containers.
    final hasOffsets = !hasRun || count >= 4;
    if (hasOffsets) pos += count * 4;

    final result = <int>{};
    var added = 0; // decoded values in THIS 32-bit bitmap, charged to [budget]
    for (int i = 0; i < count; i++) {
      final high = keys[i] << 16;
      final isRun = hasRun && (runBitset![i >> 3] & (1 << (i & 7))) != 0;
      if (isRun) {
        final runCount = bd.getUint16(pos, Endian.little);
        pos += 2;
        for (int r = 0; r < runCount; r++) {
          final value = bd.getUint16(pos, Endian.little);
          final length = bd.getUint16(pos + 2, Endian.little);
          pos += 4;
          // A run must stay within the 16-bit container; value+length spilling
          // past 0xFFFF would leak into the next bucket's bit range.
          if (value + length > 0xFFFF) {
            throw FormatException(
              'Roaring run [$value,+$length] overflows its 16-bit container',
            );
          }
          // Charge the full run BEFORE expanding it — this is the amplification
          // path (4 bytes → up to 65536 values).
          added += length + 1;
          if (added > budget) _cardinalityBomb();
          for (int v = value; v <= value + length; v++) {
            result.add(high | v);
          }
        }
      } else if (cards[i] <= 4096) {
        added += cards[i];
        if (added > budget) _cardinalityBomb();
        for (int v = 0; v < cards[i]; v++) {
          result.add(high | bd.getUint16(pos, Endian.little));
          pos += 2;
        }
      } else {
        added += cards[i];
        if (added > budget) _cardinalityBomb();
        for (int w = 0; w < 1024; w++) {
          final word = bd.getUint64(pos, Endian.little);
          pos += 8;
          if (word == 0) continue;
          for (int bit = 0; bit < 64; bit++) {
            if ((word & (1 << bit)) != 0) result.add(high | (w << 6) | bit);
          }
        }
      }
    }
    return (result, pos - start);
  }

  static Never _cardinalityBomb() => throw const FormatException(
    'Roaring bitmap decodes to more values than allowed (cardinality bomb); the '
    'declared descriptor count or the safe backstop was exceeded',
  );
}

/// A mutable, compressed set of non-negative ints — the queryable in-memory
/// counterpart to [Roaring]'s (de)serialization. Membership over a large, often
/// dense set (e.g. hundreds of millions of Iceberg position deletes) costs a few
/// bits per value in the dense case, versus the ~40-50 bytes/element a Dart
/// `Set<int>` would spend and OOM on.
///
/// Layout mirrors the Roaring container model used by [Roaring._serialize32]:
/// bucket by the high 32 bits, then a container keyed by the next 16 bits that is
/// a sorted array while small and promotes to a 65 536-bit word bitmap past 4096
/// entries.
class RoaringBitmap {
  final Map<int, Map<int, _Container>> _buckets = {};
  int _length = 0;

  int get length => _length;

  bool get isEmpty => _length == 0;

  bool get isNotEmpty => _length != 0;

  /// Every value the bitmap holds, without materializing a `Set<int>`.
  ///
  /// Ascending WITHIN a container, but the containers themselves come out in
  /// insertion order — `_buckets` and the per-bucket map are plain `Map`s. The
  /// only caller that needs global ordering, `serializeTreemap`, sorts them
  /// itself; do not assume it here.
  Iterable<int> get values sync* {
    for (final MapEntry(key: hi, value: containers) in _buckets.entries) {
      for (final MapEntry(key: key, value: container) in containers.entries) {
        final prefix = (hi << 32) | (key << 16);
        for (final low in container.values) {
          yield prefix | low;
        }
      }
    }
  }

  void add(int value) {
    final hi = value >> 32;
    final containers = _buckets.putIfAbsent(hi, () => <int, _Container>{});
    final key = (value >> 16) & 0xFFFF;
    final low = value & 0xFFFF;
    final existing = containers[key];
    if (existing == null) {
      containers[key] = _ArrayContainer()..add(low);
      _length++;
    } else if (existing.add(low)) {
      _length++;
      final promoted = existing.promote();
      if (promoted != null) containers[key] = promoted;
    }
  }

  bool contains(int value) {
    final containers = _buckets[value >> 32];
    if (containers == null) return false;
    final c = containers[(value >> 16) & 0xFFFF];
    return c != null && c.contains(value & 0xFFFF);
  }

  /// A conservative estimate of resident bytes, for memory budgeting. Sums each
  /// container's backing store plus per-bucket/-container map overhead.
  int get sizeInBytes {
    var bytes = 32; // top-level map baseline
    for (final containers in _buckets.values) {
      bytes += 32 + containers.length * 16; // per-bucket map + entry overhead
      for (final c in containers.values) {
        bytes += c.sizeInBytes;
      }
    }
    return bytes;
  }
}

/// A container of 16-bit values within one Roaring bucket.
abstract class _Container {
  /// Add [low] (0..65535); returns true if it was newly added.
  bool add(int low);
  bool contains(int low);

  /// The 16-bit values held, ascending.
  Iterable<int> get values;

  /// Returns a denser representation when the array container overflows, else
  /// null.
  _Container? promote();

  /// Conservative resident-byte estimate of this container's backing store.
  int get sizeInBytes;
}

class _ArrayContainer implements _Container {
  // Sorted ascending; binary-searched for membership.
  final List<int> _values = [];

  @override
  bool add(int low) {
    final i = _search(low);
    if (i >= 0) return false;
    _values.insert(-(i + 1), low);
    return true;
  }

  @override
  bool contains(int low) => _search(low) >= 0;

  @override
  Iterable<int> get values => _values; // already sorted ascending

  @override
  _Container? promote() =>
      _values.length > 4096 ? _BitmapContainer.fromArray(_values) : null;

  @override
  int get sizeInBytes => 32 + _values.length * 8; // boxed-int list slots

  /// Index of [low], or `-(insertionPoint + 1)` when absent.
  int _search(int low) {
    var lo = 0;
    var hi = _values.length - 1;
    while (lo <= hi) {
      final mid = (lo + hi) >> 1;
      final v = _values[mid];
      if (v == low) return mid;
      if (v < low) {
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return -(lo + 1);
  }
}

class _BitmapContainer implements _Container {
  final Uint64List _words = Uint64List(1024); // 65536 bits

  _BitmapContainer.fromArray(List<int> values) {
    for (final v in values) {
      _words[v >> 6] |= 1 << (v & 63);
    }
  }

  @override
  bool add(int low) {
    final w = low >> 6;
    final mask = 1 << (low & 63);
    if (_words[w] & mask != 0) return false;
    _words[w] |= mask;
    return true;
  }

  @override
  bool contains(int low) => _words[low >> 6] & (1 << (low & 63)) != 0;

  @override
  Iterable<int> get values sync* {
    for (var w = 0; w < _words.length; w++) {
      var word = _words[w];
      while (word != 0) {
        final bit = word & -word; // lowest set bit
        // `_words` is a Uint64List, so a word with bit 63 set reads back as a
        // NEGATIVE Dart int and `word & -word` isolates it as -2^63 — the only
        // negative single-bit value. Dart defines (-2^63).bitLength as 63 (it is
        // the bit length of the magnitude minus one), so `bitLength - 1` would
        // say 62 and silently report the WRONG ROW.
        yield (w << 6) | (bit < 0 ? 63 : bit.bitLength - 1);
        word ^= bit;
      }
    }
  }

  @override
  _Container? promote() => null;

  @override
  int get sizeInBytes => 32 + _words.lengthInBytes; // fixed 8 KiB bitset
}
