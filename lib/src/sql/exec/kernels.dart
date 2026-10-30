import 'dart:convert';
import 'dart:math' as math;
import 'dart:typed_data';

import '../../core/column.dart';
import '../../core/columnar.dart';
import '../../core/conversions.dart' show decimalParts;
import '../../core/data_type.dart';
import '../../core/schema.dart';
import '../../core/table.dart';
import '../planner/logical_plan.dart' show BinaryOperator;

/// Vectorized execution kernels: column-wise comparison, arithmetic, boolean
/// combination, gather and concat over [Column]s. Numeric columns are processed
/// through their typed buffer (no boxing); boxed [ListColumn]s fall back to
/// element access. All comparisons follow SQL three-valued logic.

/// A boolean result with explicit null tracking (SQL three-valued logic):
/// [bits] set = TRUE, [nulls] set = UNKNOWN. A value is TRUE only when its bit
/// is set and it is not null.
class BoolMask {
  final int length;
  final Uint8List bits;
  final Uint8List nulls;

  BoolMask(this.length)
    : bits = Uint8List((length + 7) >> 3),
      nulls = Uint8List((length + 7) >> 3);

  static bool _get(Uint8List b, int i) => (b[i >> 3] & (1 << (i & 7))) != 0;
  static void _set(Uint8List b, int i) => b[i >> 3] |= (1 << (i & 7));

  bool isNull(int i) => _get(nulls, i);
  bool isTrue(int i) => !_get(nulls, i) && _get(bits, i);
  bool isFalse(int i) => !_get(nulls, i) && !_get(bits, i);

  void setTrue(int i) => _set(bits, i);
  void setNull(int i) => _set(nulls, i);

  /// Indices where the value is TRUE (for filter gather).
  Int32List selected() {
    final out = <int>[];
    for (int i = 0; i < length; i++) {
      if (isTrue(i)) out.add(i);
    }
    return Int32List.fromList(out);
  }
}

class Kernels {
  /// Order two non-null values of the same kind: booleans as false < true
  /// (Dart's `bool` is not [Comparable]), binary as unsigned bytes with a
  /// shorter prefix first (as DuckDB orders BLOBs), anything else by
  /// [Comparable.compareTo]. Shared by comparisons, MIN/MAX and sorting.
  static int compare(Object a, Object b) {
    if (a is bool && b is bool) return a == b ? 0 : (a ? 1 : -1);
    if (a is Uint8List && b is Uint8List) return _bytes(a, b);
    return (a as Comparable).compareTo(b);
  }

  static int _bytes(Uint8List a, Uint8List b) {
    final shared = math.min(a.length, b.length);
    for (var i = 0; i < shared; i++) {
      final c = a[i] - b[i];
      if (c != 0) return c;
    }
    return a.length - b.length;
  }

  /// `column [NOT] IN (values)` -> mask, in one pass over the column instead of
  /// one pass per value.
  ///
  /// SQL three-valued logic: a null column value is UNKNOWN; a hit is TRUE
  /// (FALSE when [negated]); a miss is UNKNOWN if the list holds a null (the
  /// row might have matched it), else FALSE (TRUE when [negated]).
  ///
  /// [values] must hold only values whose `==`/`hashCode` agree with the
  /// engine's comparison semantics: see `PhysicalInExpression`, which decides
  /// when this kernel is applicable and otherwise falls back to comparisons.
  static BoolMask isIn(
    Column column,
    Set<Object?> values, {
    bool negated = false,
  }) {
    final n = column.length;
    final mask = BoolMask(n);
    final hasNull = values.contains(null);
    if (column is StringVector) {
      // Test each distinct value once, then spread the answer by code.
      final (:codes, :firsts) = column.encode();
      final hits = [for (final first in firsts) values.contains(column[first])];
      for (int i = 0; i < n; i++) {
        if (column.isNullAt(i)) {
          mask.setNull(i);
        } else if (hits[codes[i]]) {
          if (!negated) mask.setTrue(i);
        } else if (hasNull) {
          mask.setNull(i);
        } else if (negated) {
          mask.setTrue(i);
        }
      }
      return mask;
    }
    for (int i = 0; i < n; i++) {
      final v = column[i];
      if (v == null) {
        mask.setNull(i);
      } else if (values.contains(v)) {
        if (negated) continue; // FALSE
        mask.setTrue(i);
      } else if (hasNull) {
        mask.setNull(i);
      } else if (negated) {
        mask.setTrue(i);
      }
    }
    return mask;
  }

  /// `column OP literal` -> mask. Null column value or null literal -> UNKNOWN.
  ///
  /// [scale] is [column]'s decimal scale (0 for every other type). A decimal
  /// column holds an unscaled BigInt, so a bare `1.00` literal would compare
  /// against `100` and never match: see [_compareDecimalScalar].
  static BoolMask compareScalar(
    Column column,
    BinaryOperator operator,
    Object? literal, {
    int scale = 0,
  }) {
    final n = column.length;
    final mask = BoolMask(n);
    if (literal == null) {
      for (int i = 0; i < n; i++) {
        mask.setNull(i);
      }
      return mask;
    }
    if (column.dataType == DataType.decimal &&
        literal is num &&
        (literal is int || literal.isFinite)) {
      return _compareDecimalScalar(column, operator, literal, scale);
    }
    if (column is StringColumn && literal is String) {
      final bytes = utf8.encode(literal);
      for (int i = 0; i < n; i++) {
        if (column.isNullAt(i)) {
          mask.setNull(i);
        } else if (_order(column.compareBytes(i, bytes), operator)) {
          mask.setTrue(i);
        }
      }
      return mask;
    }
    if (column is NumericVector && literal is num) {
      final raw = column.raw as List;
      if (raw is List<int>) {
        for (int i = 0; i < n; i++) {
          if (column.isNullAt(i)) {
            mask.setNull(i);
          } else if (_holdsNumeric(raw[i], operator, literal)) {
            mask.setTrue(i);
          }
        }
        return mask;
      }
      if (raw is List<double>) {
        for (int i = 0; i < n; i++) {
          if (column.isNullAt(i)) {
            mask.setNull(i);
          } else if (_holdsNumeric(raw[i], operator, literal)) {
            mask.setTrue(i);
          }
        }
        return mask;
      }
    }
    for (int i = 0; i < n; i++) {
      final v = column[i];
      if (v == null) {
        mask.setNull(i);
      } else if (_holds(v, operator, literal)) {
        mask.setTrue(i);
      }
    }
    return mask;
  }

  /// `column OP literal` for a decimal [column] of [scale], comparing EXACTLY.
  ///
  /// Both sides become unscaled BigInt at `max(scale, literalScale)`:
  /// `decimalParts` recovers a literal's own scale from its shortest round-trip
  /// form (`1.00` -> 1.0 -> scale 1, `1.005` -> scale 3), so a literal finer
  /// than the column simply never equals any of its values, and ordering still
  /// lands on the right side. No rounding, no float round-trip.
  static BoolMask _compareDecimalScalar(
    Column column,
    BinaryOperator operator,
    num literal,
    int scale,
  ) {
    final parts = decimalParts(literal);
    final target = scale > parts.scale ? scale : parts.scale;
    final value = parts.unscaled * _pow10(target - parts.scale);
    final widen = _pow10(target - scale);
    final one = BigInt.one;
    final n = column.length;
    final mask = BoolMask(n);
    for (int i = 0; i < n; i++) {
      final v = _bigAt(column, i);
      if (v == null) {
        mask.setNull(i);
      } else if (_holds(widen == one ? v : v * widen, operator, value)) {
        mask.setTrue(i);
      }
    }
    return mask;
  }

  /// `columnA OP columnB` -> mask, element-wise with three-valued logic.
  ///
  /// [scaleA]/[scaleB] are the operands' decimal scales (0 otherwise). Two
  /// decimal columns are aligned to the wider scale before comparing, so
  /// `decimal(10,2) = decimal(10,4)` compares by VALUE and not by raw unscaled
  /// integer. A decimal compared against a float column goes through double
  /// (that operand is already inexact).
  static BoolMask compareColumns(
    Column a,
    BinaryOperator operator,
    Column b, {
    int scaleA = 0,
    int scaleB = 0,
  }) {
    final aDecimal = a.dataType == DataType.decimal;
    final bDecimal = b.dataType == DataType.decimal;
    if (aDecimal || bDecimal) {
      return _compareDecimalColumns(
        a,
        operator,
        b,
        aDecimal ? scaleA : null,
        bDecimal ? scaleB : null,
      );
    }
    final n = a.length;
    final mask = BoolMask(n);
    if (a is StringColumn && b is StringColumn) {
      for (int i = 0; i < n; i++) {
        if (a.isNullAt(i) || b.isNullAt(i)) {
          mask.setNull(i);
        } else if (_order(a.compareAt(i, b, i), operator)) {
          mask.setTrue(i);
        }
      }
      return mask;
    }
    // Typed fast reads for numeric operands (skip operator[]'s per-element
    // length-division + type cascade); non-numeric operands fall back.
    final numericA = a is NumericVector ? a : null;
    final aRaw = numericA?.raw as List?;
    final numericB = b is NumericVector ? b : null;
    final bRaw = numericB?.raw as List?;
    for (int i = 0; i < n; i++) {
      final x = numericA != null
          ? (numericA.isNullAt(i) ? null : aRaw![i])
          : a[i];
      final y = numericB != null
          ? (numericB.isNullAt(i) ? null : bRaw![i])
          : b[i];
      if (x == null || y == null) {
        mask.setNull(i);
      } else if (_holds(x, operator, y)) {
        mask.setTrue(i);
      }
    }
    return mask;
  }

  /// `a OP b` where at least one side is a decimal column. A null [scaleA] or
  /// [scaleB] marks that side as NOT decimal.
  static BoolMask _compareDecimalColumns(
    Column a,
    BinaryOperator operator,
    Column b,
    int? scaleA,
    int? scaleB,
  ) {
    final n = a.length;
    final mask = BoolMask(n);
    if (scaleA != null && scaleB != null) {
      // Both decimal: align to the wider scale and compare exactly.
      final target = scaleA > scaleB ? scaleA : scaleB;
      final widenA = _pow10(target - scaleA);
      final widenB = _pow10(target - scaleB);
      final one = BigInt.one;
      for (int i = 0; i < n; i++) {
        final x = _bigAt(a, i);
        final y = _bigAt(b, i);
        if (x == null || y == null) {
          mask.setNull(i);
        } else if (_holds(
          widenA == one ? x : x * widenA,
          operator,
          widenB == one ? y : y * widenB,
        )) {
          mask.setTrue(i);
        }
      }
      return mask;
    }
    // One decimal, one not. If the other side is an INTEGER column the compare
    // is still EXACT: lift the integer to the decimal's scale as a BigInt and
    // compare via BigInt: the double path loses precision above 2^53 (a
    // decimal(20,0) and an int both holding 9007199254740993 would compare
    // unequal). Only a genuine FLOAT operand is inherently inexact.
    final aIsDecimal = scaleA != null;
    final decimal = aIsDecimal ? a : b;
    final scale = (aIsDecimal ? scaleA : scaleB)!;
    final other = aIsDecimal ? b : a;
    if (other.dataType.isInteger) {
      final widen = _pow10(scale);
      for (int i = 0; i < n; i++) {
        final unscaled = _bigAt(decimal, i);
        final integer = _bigAt(other, i); // int lifted exactly to BigInt
        if (unscaled == null || integer == null) {
          mask.setNull(i);
        } else {
          final lifted = integer * widen;
          final x = aIsDecimal ? unscaled : lifted;
          final y = aIsDecimal ? lifted : unscaled;
          if (_holds(x, operator, y)) mask.setTrue(i);
        }
      }
      return mask;
    }
    // A genuine float operand: compare as doubles (already inexact).
    for (int i = 0; i < n; i++) {
      final x = _decimalAsNumber(a, i, scaleA);
      final y = _decimalAsNumber(b, i, scaleB);
      if (x == null || y == null) {
        mask.setNull(i);
      } else if (_holdsNumeric(x, operator, y)) {
        mask.setTrue(i);
      }
    }
    return mask;
  }

  /// An unscaled decimal value as a plain (lossy above 2^53) number.
  static num decimalToNum(BigInt unscaled, int scale) =>
      unscaled.toDouble() / math.pow(10, scale);

  /// Element [i] of [c] as a plain number: an unscaled decimal is divided back
  /// down by its [scale]; a null [scale] means the column is already numeric.
  static num? _decimalAsNumber(Column c, int i, int? scale) {
    if (scale == null) return c[i] as num?;
    final v = _bigAt(c, i);
    return v == null ? null : decimalToNum(v, scale);
  }

  /// Whether [arithmetic] produces a floating result for operands typed [a]/[b]
  /// under [operator]: division always floats, otherwise any float operand does.
  static bool _floating(DataType a, DataType b, BinaryOperator operator) =>
      operator == BinaryOperator.divide ||
      a == DataType.float32 ||
      a == DataType.float64 ||
      b == DataType.float32 ||
      b == DataType.float64;

  /// The dtype [arithmetic] will produce for operands typed [a]/[b] under
  /// [operator]: float64 for division or any float operand, else int64. Single
  /// source of truth so the planner's output-schema typing can never drift from
  /// what the kernel actually emits.
  static DataType arithResultType(
    DataType a,
    DataType b,
    BinaryOperator operator,
  ) {
    if (_floating(a, b, operator)) return DataType.float64;
    // A decimal operand keeps the result decimal (lossless BigInt), so SUM(int)
    // and other decimal columns compose in per-row arithmetic. Pure-integer
    // arithmetic stays int64 (int64 is the default int type; promoting every
    // `a + 1` to BigInt would be a large perf/ergonomics regression) and FAILS
    // LOUD on 64-bit overflow instead of silently wrapping.
    if (a == DataType.decimal || b == DataType.decimal) return DataType.decimal;
    return DataType.int64;
  }

  /// `columnA OP (columnB | literal)` arithmetic -> a typed numeric column.
  /// Result is int64 unless either side is floating or the operator is divide.
  /// [scaleA]/[scaleB] are the operand decimal scales and [resultScale] the
  /// output scale (a decimal column holds unscaled BigInt at that scale): used
  /// only on the decimal path to align/combine scales; 0 for int/scale-0 domains.
  static Column arithmetic(
    Column a,
    BinaryOperator operator,
    Object? b,
    String name, {
    int scaleA = 0,
    int scaleB = 0,
    int? resultScale,
  }) {
    final n = a.length;
    final columnB = b is Column ? b : null;
    final literalB = b is Column ? null : (b as num?);

    final bType =
        columnB?.dataType ??
        (literalB is double ? DataType.float64 : DataType.int64);
    final float = _floating(a.dataType, bType, operator);

    // Division or modulo by zero yields NULL (SQL semantics), never NaN or an
    // `int.toInt()` crash.
    final zeroDivides =
        operator == BinaryOperator.divide || operator == BinaryOperator.modulo;

    // Typed fast reads: index the raw buffer directly (with the cheap isNullAt)
    // rather than NumericColumn.operator[], which recomputes length (a division)
    // and runs a 6-way type cascade PER element. Non-numeric operands fall back.
    final numericA = a is NumericVector ? a : null;
    final aRaw = numericA?.raw as List?;
    final numericB = columnB is NumericVector ? columnB : null;
    final bRaw = numericB?.raw as List?;

    if (float) {
      // A decimal operand can reach the float path (`decimal + 1.5` is
      // float-typed, since any float operand floats the result), and it holds
      // unscaled BigInt: scale it back down rather than casting it to num.
      final aScale = a.dataType == DataType.decimal ? scaleA : null;
      final bScale = columnB?.dataType == DataType.decimal ? scaleB : null;
      final out = Float64List(n);
      final nulls = Uint8List((n + 7) >> 3);
      var hasNull = false;
      for (int i = 0; i < n; i++) {
        final num? x = aScale != null
            ? _decimalAsNumber(a, i, aScale)
            : (numericA != null
                  ? (numericA.isNullAt(i) ? null : aRaw![i] as num)
                  : a[i] as num?);
        final num? y = columnB == null
            ? literalB
            : (bScale != null
                  ? _decimalAsNumber(columnB, i, bScale)
                  : (numericB != null
                        ? (numericB.isNullAt(i) ? null : bRaw![i] as num)
                        : columnB[i] as num?));
        if (x == null || y == null || (zeroDivides && y == 0)) {
          nulls[i >> 3] |= (1 << (i & 7));
          hasNull = true;
        } else {
          out[i] = _arithmetic(x, operator, y).toDouble();
        }
      }
      return NumericVector.adopt(
        name: name,
        dataType: DataType.float64,
        data: out,
        nullBitmap: hasNull ? nulls : null,
      );
    }

    // Decimal result: a decimal operand keeps the result a LOSSLESS BigInt
    // column. Values are unscaled BigInt at their operand scale; the result is
    // produced at [resultScale]: +/-/% align both operands to it, * adds scales
    // (so `resultScale` was computed as scaleA+scaleB). Division never lands here
    // (it is float). A scale-0/int operand contributes scale 0 (unchanged SUM
    // composition).
    if (arithResultType(a.dataType, bType, operator) == DataType.decimal) {
      final scale = resultScale ?? 0;
      final data = List<BigInt?>.filled(n, null);
      for (int i = 0; i < n; i++) {
        final x = _bigAt(a, i);
        final y = columnB == null ? _bigLiteral(literalB) : _bigAt(columnB, i);
        if (x == null || y == null || (zeroDivides && y == BigInt.zero)) {
          continue; // stays null
        }
        data[i] = _arithmeticScaled(x, scaleA, operator, y, scaleB, scale);
      }
      return ListColumn<BigInt>(
        name: name,
        dataType: DataType.decimal,
        data: data,
        nullable: data.any((v) => v == null),
      );
    }

    final out = Int64List(n);
    final nulls = Uint8List((n + 7) >> 3);
    var hasNull = false;
    for (int i = 0; i < n; i++) {
      final num? x = numericA != null
          ? (numericA.isNullAt(i) ? null : aRaw![i] as num)
          : a[i] as num?;
      final num? y = columnB == null
          ? literalB
          : (numericB != null
                ? (numericB.isNullAt(i) ? null : bRaw![i] as num)
                : columnB[i] as num?);
      if (x == null || y == null || (zeroDivides && y == 0)) {
        nulls[i >> 3] |= (1 << (i & 7));
        hasNull = true;
      } else if (x is int && y is int) {
        // Fail loud on 64-bit overflow instead of silently wrapping in the
        // Int64List (SUM is exact BigInt; per-row arithmetic must not corrupt).
        out[i] = _checked(x, operator, y, name);
      } else {
        out[i] = _arithmetic(x, operator, y).toInt();
      }
    }
    return NumericVector.adopt(
      name: name,
      dataType: DataType.int64,
      data: out,
      nullBitmap: hasNull ? nulls : null,
    );
  }

  /// Per-row twin of [arithmetic] for one operand pair: the SAME float /
  /// decimal / checked-int semantics, so the per-row `evaluate` path cannot
  /// drift from the vectorized one (a raw `BigInt + int` would throw, and
  /// adding unaligned unscaled decimals would silently produce the wrong
  /// magnitude). [x]/[y] must be non-null; the
  /// caller owes the SQL null short-circuit.
  static Object? arithScalar(
    Object x,
    DataType typeX,
    int scaleX,
    BinaryOperator operator,
    Object y,
    DataType typeY,
    int scaleY, {
    int? resultScale,
    String name = 'expr',
  }) {
    final zeroDivides =
        operator == BinaryOperator.divide || operator == BinaryOperator.modulo;
    if (_floating(typeX, typeY, operator)) {
      final num a = typeX == DataType.decimal
          ? decimalToNum(x as BigInt, scaleX)
          : x as num;
      final num b = typeY == DataType.decimal
          ? decimalToNum(y as BigInt, scaleY)
          : y as num;
      if (zeroDivides && b == 0) return null;
      return _arithmetic(a, operator, b).toDouble();
    }
    if (arithResultType(typeX, typeY, operator) == DataType.decimal) {
      final a = x is BigInt ? x : BigInt.from((x as num).toInt());
      final b = y is BigInt ? y : BigInt.from((y as num).toInt());
      if (zeroDivides && b == BigInt.zero) return null;
      return _arithmeticScaled(
        a,
        scaleX,
        operator,
        b,
        scaleY,
        resultScale ?? 0,
      );
    }
    final a = x as num;
    final b = y as num;
    if (zeroDivides && b == 0) return null;
    if (a is int && b is int) return _checked(a, operator, b, name);
    return _arithmetic(a, operator, b).toInt();
  }

  /// Read element [i] of [c] as an unscaled [BigInt] (a decimal column already
  /// holds BigInt; an int is lifted exactly), or null.
  static BigInt? _bigAt(Column c, int i) {
    final v = c[i];
    if (v == null) return null;
    if (v is BigInt) return v;
    if (v is int) return BigInt.from(v);
    return BigInt.from((v as num).toInt());
  }

  static BigInt? _bigLiteral(num? literal) =>
      literal == null ? null : BigInt.from(literal.toInt());

  static BigInt _pow10(int e) => BigInt.from(10).pow(e);

  /// `x OP y` on unscaled BigInt decimals of scale [scaleX]/[scaleY],
  /// producing an unscaled result at [scale]. +/-/% align both operands to
  /// [scale] (`scale = max(scaleX,scaleY)`); * needs no alignment
  /// (`scale = scaleX+scaleY`). Division is the float path, never here.
  /// Subsumes the scale-0/int case (scaleX=scaleY=0).
  static BigInt _arithmeticScaled(
    BigInt x,
    int scaleX,
    BinaryOperator operator,
    BigInt y,
    int scaleY,
    int scale,
  ) {
    switch (operator) {
      case BinaryOperator.multiply:
        return x * y;
      case BinaryOperator.add:
      case BinaryOperator.subtract:
      case BinaryOperator.modulo:
        final alignedX = scaleX == scale ? x : x * _pow10(scale - scaleX);
        final alignedY = scaleY == scale ? y : y * _pow10(scale - scaleY);
        return operator == BinaryOperator.add
            ? alignedX + alignedY
            : operator == BinaryOperator.subtract
            ? alignedX - alignedY
            : alignedX.remainder(
                alignedY,
              ); // SQL MOD: sign of the dividend, not the divisor
      default:
        throw StateError('unsupported decimal arithmetic operator $operator');
    }
  }

  static const int _int64Min = -9223372036854775807 - 1; // -2^63

  static Never _overflow(String symbol, int x, int y, String name) {
    throw StateError(
      'integer overflow in "$name": $x $symbol $y exceeds the signed 64-bit range '
      '(cast an operand to a wider/decimal type)',
    );
  }

  /// `x OP y` for two ints, throwing on 64-bit overflow (add/subtract/multiply);
  /// modulo can't overflow. Uses standard sign/inverse checks so the fast path
  /// stays branch-cheap (no per-element BigInt).
  static int _checked(int x, BinaryOperator operator, int y, String name) {
    switch (operator) {
      case BinaryOperator.add:
        final s = x + y;
        if (((x ^ s) & (y ^ s)) < 0) _overflow('+', x, y, name);
        return s;
      case BinaryOperator.subtract:
        final d = x - y;
        if (((x ^ y) & (x ^ d)) < 0) _overflow('-', x, y, name);
        return d;
      case BinaryOperator.multiply:
        final p = x * y;
        if (x != 0 &&
            (p ~/ x != y ||
                (x == -1 && y == _int64Min) ||
                (y == -1 && x == _int64Min))) {
          _overflow('*', x, y, name);
        }
        return p;
      default:
        return _arithmetic(
          x,
          operator,
          y,
        ).toInt(); // modulo: magnitude ≤ operands
    }
  }

  /// Three-valued AND.
  static BoolMask and(BoolMask a, BoolMask b) {
    final m = BoolMask(a.length);
    for (int i = 0; i < a.length; i++) {
      if (a.isFalse(i) || b.isFalse(i)) {
        // FALSE dominates -> stays false.
      } else if (a.isTrue(i) && b.isTrue(i)) {
        m.setTrue(i);
      } else {
        m.setNull(i);
      }
    }
    return m;
  }

  /// Three-valued OR.
  static BoolMask or(BoolMask a, BoolMask b) {
    final m = BoolMask(a.length);
    for (int i = 0; i < a.length; i++) {
      if (a.isTrue(i) || b.isTrue(i)) {
        m.setTrue(i);
      } else if (a.isFalse(i) && b.isFalse(i)) {
        // both FALSE -> false
      } else {
        m.setNull(i);
      }
    }
    return m;
  }

  /// Three-valued NOT (UNKNOWN stays UNKNOWN).
  static BoolMask not(BoolMask a) {
    final m = BoolMask(a.length);
    for (int i = 0; i < a.length; i++) {
      if (a.isNull(i)) {
        m.setNull(i);
      } else if (a.isFalse(i)) {
        m.setTrue(i);
      }
    }
    return m;
  }

  /// Gather rows [indices] from [column], preserving typed buffers for numerics.
  /// A negative index yields a null (used for outer-join null padding).
  static Column gather(Column column, Int32List indices) {
    final n = indices.length;
    var anyNeg = false;
    for (final i in indices) {
      if (i < 0) {
        anyNeg = true;
        break;
      }
    }
    if (column is NumericVector) {
      final source = column.raw as List;
      final hasNulls = column.hasNulls;
      final needNulls = hasNulls || anyNeg;
      final nulls = needNulls ? Uint8List((n + 7) >> 3) : null;
      final outData = _alloc(column.dataType, n);
      final out = outData as List;
      if (out is List<int> && source is List<int>) {
        for (int i = 0; i < n; i++) {
          final s = indices[i];
          if (s < 0 || (hasNulls && column.isNullAt(s))) {
            nulls![i >> 3] |= (1 << (i & 7));
          } else {
            out[i] = source[s];
          }
        }
      } else if (out is List<double> && source is List<double>) {
        for (int i = 0; i < n; i++) {
          final s = indices[i];
          if (s < 0 || (hasNulls && column.isNullAt(s))) {
            nulls![i >> 3] |= (1 << (i & 7));
          } else {
            out[i] = source[s];
          }
        }
      }
      return NumericVector.adopt(
        name: column.name,
        dataType: column.dataType,
        data: outData,
        nullBitmap: nulls,
      );
    }
    if (column is StringColumn) return column.gather(indices);
    if (column is BoolColumn) return column.gather(indices);
    final data = [for (final i in indices) i < 0 ? null : column[i]];
    return ListColumn(
      name: column.name,
      dataType: column.dataType,
      data: data,
      nullable: column.nullable || anyNeg,
    );
  }

  /// Concatenate same-named, same-typed column pieces, keeping the typed buffer
  /// when every piece is numeric of the same type. Lifted from
  /// `parquet_reader._concat`.
  /// Concatenate columns. Delegates to the core [Column.concat] (kept here so
  /// existing SQL-layer callers are unchanged).
  static Column concat(List<Column> pieces) => Column.concat(pieces);

  /// Build a column from boxed [values], using a typed [NumericColumn] when the
  /// values already match the numeric [type] (no coercion: falls back to a
  /// boxed [ListColumn] otherwise, preserving exact value types).
  static Column fromValues(String name, DataType type, List<Object?> values) =>
      columnFromValues(name, type, values);

  /// Disambiguate a terminal result's column names: a bare name shared by more
  /// than one field (from joined inputs) becomes `qualifier.name`. Applied only
  /// at the user-facing boundary (SQL and DataFrame both call this), never to
  /// intermediate schemas.
  static Table disambiguate(Table table) {
    final fields = table.schema.fields;
    final counts = <String, int>{};
    for (final f in fields) {
      counts[f.name] = (counts[f.name] ?? 0) + 1;
    }
    if (counts.values.every((c) => c == 1)) return table;
    final newFields = <Field>[];
    final columns = <Column>[];
    for (int i = 0; i < fields.length; i++) {
      final f = fields[i];
      final name = (counts[f.name]! > 1 && f.qualifier != null)
          ? '${f.qualifier}.${f.name}'
          : f.name;
      // copyWith, not a fresh Field: this is a RENAME, and rebuilding by hand
      // silently dropped precision/scale/children/metadata from EVERY field,
      // not just the colliding ones, at the terminal user-facing boundary, so a
      // join that happened to share a column name rendered an untouched decimal
      // 1.50 as "150" and cost a nested column the children its Parquet/Arrow
      // re-write needs.
      newFields.add(f.copyWith(name: name));
      columns.add(rename(table.columns[i], name));
    }
    return Table(
      name: table.name,
      schema: Schema(fields: newFields),
      columns: columns,
    );
  }

  /// Return [column] under a new [name], preserving the typed buffer for numerics.
  static Column rename(Column column, String name) {
    if (column.name == name) return column;
    if (column is StringColumn) return column.withName(name);
    if (column is BoolColumn) return column.withName(name);
    if (column is NumericVector) {
      return NumericVector.adopt(
        name: name,
        dataType: column.dataType,
        data: column.raw,
        nullBitmap: column.nulls,
      );
    }
    return ListColumn(
      name: name,
      dataType: column.dataType,
      data: column.toList(),
      nullable: column.nullable,
    );
  }

  /// A column of [length] copies of [value] (for literal projection outputs).
  static Column constColumn(
    Object? value,
    DataType type,
    String name,
    int length,
  ) {
    if (value is num && type.isNumeric) {
      // Coerce the literal to its declared storage type so a float-typed literal
      // (e.g. `SELECT 5` common-typed to float64 in a CASE/COALESCE branch)
      // holds a double, not an int: concat / Parquet-write paths trust the dtype.
      final coerced = (type == DataType.float32 || type == DataType.float64)
          ? value.toDouble()
          : value;
      return ListColumn(
        name: name,
        dataType: type,
        data: List.filled(length, coerced),
        nullable: false,
      );
    }
    return ListColumn(
      name: name,
      dataType: type,
      data: List.filled(length, value),
      nullable: value == null,
    );
  }

  /// Materialize a [BoolMask] as a boolean column (for projection outputs).
  static Column maskToColumn(BoolMask mask, String name) {
    final nullable = mask.nulls.any((byte) => byte != 0);
    return BoolVector.adopt(
      name: name,
      length: mask.length,
      bits: Uint8List.fromList(mask.bits),
      nulls: nullable ? Uint8List.fromList(mask.nulls) : null,
    );
  }

  /// Read a boolean column into a [BoolMask].
  static BoolMask columnToMask(Column column) {
    final mask = BoolMask(column.length);
    if (column is BoolVector) {
      mask.bits.setAll(0, column.bits);
      final nulls = column.nulls;
      if (nulls != null) mask.nulls.setAll(0, nulls);
      return mask;
    }
    for (int i = 0; i < column.length; i++) {
      final v = column[i];
      if (v == null) {
        mask.setNull(i);
      } else if (v == true) {
        mask.setTrue(i);
      }
    }
    return mask;
  }

  static TypedData _alloc(DataType type, int n) {
    switch (type) {
      case DataType.int8:
        return Int8List(n);
      case DataType.int16:
        return Int16List(n);
      case DataType.int32:
        return Int32List(n);
      case DataType.int64:
        return Int64List(n);
      case DataType.float32:
        return Float32List(n);
      case DataType.float64:
        return Float64List(n);
      default:
        throw UnsupportedError('not a numeric type: $type');
    }
  }

  static bool _holdsNumeric(num a, BinaryOperator operator, num b) {
    switch (operator) {
      case BinaryOperator.eq:
        return a == b;
      case BinaryOperator.neq:
        return a != b;
      case BinaryOperator.lt:
        return a < b;
      case BinaryOperator.lte:
        return a <= b;
      case BinaryOperator.gt:
        return a > b;
      case BinaryOperator.gte:
        return a >= b;
      default:
        throw ArgumentError('not a comparison: $operator');
    }
  }

  /// Whether a comparison that came out [sign] (negative, zero or positive)
  /// satisfies [operator]; false for any other operator, as in [_holds].
  static bool _order(int sign, BinaryOperator operator) => switch (operator) {
    BinaryOperator.eq => sign == 0,
    BinaryOperator.neq => sign != 0,
    BinaryOperator.lt => sign < 0,
    BinaryOperator.lte => sign <= 0,
    BinaryOperator.gt => sign > 0,
    BinaryOperator.gte => sign >= 0,
    _ => false,
  };

  static bool _holds(Object a, BinaryOperator operator, Object b) {
    if (a is num && b is num) return _holdsNumeric(a, operator, b);
    if (operator == BinaryOperator.eq) return a == b;
    if (operator == BinaryOperator.neq) return a != b;
    // Ordered comparison across different runtime types (e.g. a numeric column
    // vs a string literal) would throw a raw TypeError from compareTo. Fail
    // with a clear, structured error instead.
    if (a.runtimeType != b.runtimeType) {
      throw ArgumentError(
        'Cannot order-compare ${a.runtimeType} with ${b.runtimeType}',
      );
    }
    if ((a is Comparable && b is Comparable) || (a is bool && b is bool)) {
      final c = compare(a, b);
      switch (operator) {
        case BinaryOperator.lt:
          return c < 0;
        case BinaryOperator.lte:
          return c <= 0;
        case BinaryOperator.gt:
          return c > 0;
        case BinaryOperator.gte:
          return c >= 0;
        default:
          break;
      }
    }
    return false;
  }

  static num _arithmetic(num a, BinaryOperator operator, num b) {
    switch (operator) {
      case BinaryOperator.add:
        return a + b;
      case BinaryOperator.subtract:
        return a - b;
      case BinaryOperator.multiply:
        return a * b;
      case BinaryOperator.divide:
        return b == 0 ? double.nan : a / b;
      case BinaryOperator.modulo:
        // SQL MOD takes the sign of the DIVIDEND (`-7 % 3 = -1`), which is
        // `remainder`, not Dart's `%` (which takes the divisor's sign).
        return b == 0 ? double.nan : a.remainder(b);
      default:
        throw ArgumentError('not an arithmetic op: $operator');
    }
  }
}
