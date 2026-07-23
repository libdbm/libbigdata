import 'package:meta/meta.dart';

import 'conversions.dart';

/// An exact decimal number: an [unscaled] integer and a [scale] (the number of
/// digits to the right of the point), so `Decimal(BigInt.from(150), 2)` is 1.50.
///
/// Columns store decimals unscaled, with the scale on their [Field] — a [Column]
/// alone cannot render one. This is the value a [Table] hands back, where the
/// schema supplies the scale.
///
/// Exact by construction: unlike a double it never rounds, so a 64-bit id or a
/// money amount survives a mixed integer/float column intact.
@immutable
class Decimal implements Comparable<Decimal> {
  /// The value as an integer, before applying [scale].
  final BigInt unscaled;

  /// Digits to the right of the decimal point. Never negative.
  final int scale;

  const Decimal(this.unscaled, this.scale);

  /// [value] as an exact decimal, taking its scale from the shortest
  /// round-trip form (`1.50` → 1.5 → scale 1; an int → scale 0).
  factory Decimal.from(num value) {
    final parts = decimalParts(value);
    return Decimal(parts.unscaled, parts.scale);
  }

  /// The nearest double. LOSSY above 2^53 — the whole point of this type is to
  /// avoid that, so prefer [toString] or [unscaled] when exactness matters.
  double toDouble() =>
      scale == 0 ? unscaled.toDouble() : double.parse(toString());

  /// This value with trailing fractional zeros removed (1.50 → 1.5, 0.00 → 0).
  /// Value-identical, so it is what [==]/[hashCode] compare.
  Decimal get normalized {
    if (scale <= 0 || unscaled == BigInt.zero) {
      return unscaled == BigInt.zero ? Decimal(BigInt.zero, 0) : this;
    }
    final ten = BigInt.from(10);
    var value = unscaled;
    var digits = scale;
    while (digits > 0 && value % ten == BigInt.zero) {
      value = value ~/ ten;
      digits--;
    }
    return Decimal(value, digits);
  }

  /// Compares by VALUE, so 1.5 and 1.50 are equal despite differing scales.
  @override
  int compareTo(Decimal other) {
    if (scale == other.scale) return unscaled.compareTo(other.unscaled);
    final target = scale > other.scale ? scale : other.scale;
    final ten = BigInt.from(10);
    return (unscaled * ten.pow(target - scale)).compareTo(
      other.unscaled * ten.pow(target - other.scale),
    );
  }

  /// Value equality: 1.5 == 1.50. (Java's BigDecimal famously differs here,
  /// requiring equal scales — a long-standing source of surprise.)
  @override
  bool operator ==(Object other) => other is Decimal && compareTo(other) == 0;

  @override
  int get hashCode {
    final n = normalized;
    return Object.hash(n.unscaled, n.scale);
  }

  /// The human/Spark-readable form: `Decimal(BigInt.from(12345), 2)` → "123.45".
  @override
  String toString() => formatDecimal(unscaled, scale);
}
