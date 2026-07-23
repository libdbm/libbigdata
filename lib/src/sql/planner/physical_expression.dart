part of 'physical_plan.dart';

/// Comparison operators that produce a boolean mask.
const _comparisonOps = {
  BinaryOperator.eq,
  BinaryOperator.neq,
  BinaryOperator.lt,
  BinaryOperator.lte,
  BinaryOperator.gt,
  BinaryOperator.gte,
};

/// Arithmetic operators that produce a numeric column.
const _arithmeticOps = {
  BinaryOperator.add,
  BinaryOperator.subtract,
  BinaryOperator.multiply,
  BinaryOperator.divide,
  BinaryOperator.modulo,
};

/// Physical expressions for evaluation
abstract class PhysicalExpression {
  DataType get dataType;

  /// Decimal precision/scale for a [DataType.decimal] result (e.g. an integer
  /// SUM surfaced as a lossless decimal). Null for non-decimal expressions.
  int? get precision => null;
  int? get scale => null;

  /// Nested shape (list/struct/map children) for a column reference that
  /// passes a nested column through. Like decimal scale, the shape lives on
  /// the FIELD — dropping it at a projection boundary left spilled nested
  /// columns with children-less fields, which the Parquet spill writer
  /// rejects (`children.first` on empty). Empty for computed expressions.
  List<Field> get children => const [];

  Object? evaluate(RecordBatch batch, int rowIndex);

  /// Vectorized evaluation to a whole data column (projection outputs).
  /// Default: per-row interpretation — overridden by the fast paths below.
  Column evalColumn(RecordBatch batch) {
    final data = <dynamic>[];
    for (int i = 0; i < batch.rowCount; i++) {
      data.add(evaluate(batch, i));
    }
    return ListColumn(
      name: 'expr',
      dataType: dataType,
      data: data,
      nullable: data.contains(null),
    );
  }

  /// Vectorized evaluation to a boolean mask (filter predicates).
  /// Default: per-row interpretation — overridden by the fast paths below.
  BoolMask evalMask(RecordBatch batch) {
    final mask = BoolMask(batch.rowCount);
    for (int i = 0; i < batch.rowCount; i++) {
      final v = evaluate(batch, i);
      if (v == null) {
        mask.setNull(i);
      } else if (v == true) {
        mask.setTrue(i);
      }
    }
    return mask;
  }
}

/// Column reference expression
class PhysicalColumnExpression extends PhysicalExpression {
  final String columnName;
  final DataType columnType;
  final String? qualifier;
  final int? _precision;
  final int? _scale;
  final List<Field> _children;

  PhysicalColumnExpression(
    this.columnName,
    this.columnType, {
    this.qualifier,
    int? precision,
    int? scale,
    List<Field> children = const [],
  }) : _precision = precision,
       _scale = scale,
       _children = children;

  @override
  List<Field> get children => _children;

  @override
  DataType get dataType => columnType;

  // Carry the source field's decimal precision/scale so arithmetic over a
  // decimal column threads the correct scale (a decimal column holds unscaled
  // BigInt; the scale lives on the field, not the value).
  @override
  int? get precision => _precision;
  @override
  int? get scale => _scale;

  // A missing column must fail loud, never silently become null. The planner's
  // bind step validates references up front; this is the runtime backstop so an
  // unvalidated path can't produce wrong answers (a mistyped SELECT column or a
  // WHERE that silently filters everything). Outer joins are unaffected: the
  // join emits batches whose schema already carries every (null-filled) column.
  String get _missing =>
      'Unknown column: ${qualifier == null ? '' : '$qualifier.'}$columnName';

  @override
  Object? evaluate(RecordBatch batch, int rowIndex) {
    final column = batch.column(columnName, qualifier: qualifier);
    if (column == null) throw StateError(_missing);
    if (rowIndex >= column.length) return null;
    return column[rowIndex];
  }

  @override
  Column evalColumn(RecordBatch batch) {
    final column = batch.column(columnName, qualifier: qualifier);
    if (column == null) throw StateError(_missing);
    return column;
  }

  @override
  BoolMask evalMask(RecordBatch batch) {
    final column = batch.column(columnName, qualifier: qualifier);
    if (column == null) throw StateError(_missing);
    return Kernels.columnToMask(column);
  }
}

/// Literal expression
class PhysicalLiteralExpression extends PhysicalExpression {
  final dynamic value;
  final DataType literalType;

  PhysicalLiteralExpression(this.value, this.literalType);

  @override
  DataType get dataType => literalType;

  @override
  Object? evaluate(RecordBatch batch, int rowIndex) {
    return value;
  }

  @override
  Column evalColumn(RecordBatch batch) =>
      Kernels.constColumn(value, literalType, 'lit', batch.rowCount);

  @override
  BoolMask evalMask(RecordBatch batch) {
    final mask = BoolMask(batch.rowCount);
    for (int i = 0; i < batch.rowCount; i++) {
      if (value == null) {
        mask.setNull(i);
      } else if (value == true) {
        mask.setTrue(i);
      }
    }
    return mask;
  }
}

/// Binary expression
/// `value [NOT] IN (list)`, evaluated in SQL three-valued logic.
///
/// When every list element is a constant AND the probe's type compares by
/// value, the list becomes a set and the batch is scanned once. Otherwise it
/// falls back to one comparison per element combined with OR/AND — identical to
/// what the equivalent expanded predicate would do, which keeps a
/// non-constant list (`x IN (y, 2)`) working.
class PhysicalInExpression extends PhysicalExpression {
  final PhysicalExpression value;
  final List<PhysicalExpression> list;
  final bool negated;

  PhysicalInExpression({
    required this.value,
    required this.list,
    required this.negated,
  });

  /// Types whose `==`/`hashCode` agree with the engine's comparison semantics,
  /// so a set probe and a comparison give the same answer.
  ///
  /// `decimal` is excluded: it stores an unscaled BigInt, and `BigInt.from(1)`
  /// does not equal the literal `1`, so a set probe would silently miss every
  /// row where a comparison also (currently) misses — but by a different route.
  /// `binary` is excluded because a Uint8List hashes by identity, which would
  /// drop every row. Both fall back to comparisons, keeping IN's answer exactly
  /// equal to the corresponding `=`/`<>`.
  static final _hashable = {
    DataType.int8,
    DataType.int16,
    DataType.int32,
    DataType.int64,
    DataType.float32,
    DataType.float64,
    DataType.string,
    DataType.boolean,
  };

  /// The list as a constant set, or null when the fast path does not apply.
  late final Set<Object?>? _constants = () {
    if (!_hashable.contains(value.dataType)) return null;
    final out = <Object?>{};
    for (final element in list) {
      if (element is! PhysicalLiteralExpression) return null;
      out.add(element.value);
    }
    return out;
  }();

  @override
  DataType get dataType => DataType.boolean;

  /// Vectorized, so the decimal scale alignment in the comparison kernels
  /// applies here too — the per-row base fallback compares raw values and would
  /// never match an unscaled decimal against a literal.
  @override
  Column evalColumn(RecordBatch batch) =>
      Kernels.maskToColumn(evalMask(batch), 'expr');

  @override
  Object? evaluate(RecordBatch batch, int rowIndex) {
    final probe = value.evaluate(batch, rowIndex);
    if (probe == null) return null;
    var sawNull = false;
    for (final element in list) {
      final other = element.evaluate(batch, rowIndex);
      if (other == null) {
        sawNull = true;
      } else if (_equal(probe, other)) {
        return !negated;
      }
    }
    // No match: UNKNOWN if the list held a NULL the probe might have equalled.
    return sawNull ? null : negated;
  }

  /// Row-wise equality that understands an unscaled decimal probe: `1.00`
  /// arrives as a plain number, the column value as a BigInt scaled by
  /// [value].scale.
  bool _equal(Object probe, Object other) {
    if (probe is BigInt && other is num && value.dataType == DataType.decimal) {
      if (other is double && !other.isFinite) return false;
      final scale = value.scale ?? 0;
      final parts = decimalParts(other);
      // A literal finer than the column can equal none of its values.
      if (parts.scale > scale) return false;
      return probe == parts.unscaled * BigInt.from(10).pow(scale - parts.scale);
    }
    return probe == other;
  }

  @override
  BoolMask evalMask(RecordBatch batch) {
    final column = value.evalColumn(batch);
    final constants = _constants;
    if (constants != null) {
      return Kernels.isIn(column, constants, negated: negated);
    }
    final operator = negated ? BinaryOperator.neq : BinaryOperator.eq;
    BoolMask? mask;
    for (final element in list) {
      final next = element is PhysicalLiteralExpression
          ? Kernels.compareScalar(
              column,
              operator,
              element.value,
              scale: value.scale ?? 0,
            )
          : Kernels.compareColumns(
              column,
              operator,
              element.evalColumn(batch),
              scaleA: value.scale ?? 0,
              scaleB: element.scale ?? 0,
            );
      mask = mask == null
          ? next
          : (negated ? Kernels.and(mask, next) : Kernels.or(mask, next));
    }
    // The grammar requires at least one element.
    return mask!;
  }
}

class PhysicalBinaryExpression extends PhysicalExpression {
  final PhysicalExpression left;
  final BinaryOperator operator;
  final PhysicalExpression right;
  final DataType resultType;

  PhysicalBinaryExpression({
    required this.left,
    required this.operator,
    required this.right,
    required this.resultType,
  });

  @override
  DataType get dataType => resultType;

  // Decimal arithmetic scale rules (only meaningful when [resultType] is
  // decimal): +/-/% take the max operand scale (operands are aligned to it); *
  // adds the operand scales. A non-decimal operand contributes scale 0.
  int get _leftScale => left.scale ?? 0;
  int get _rightScale => right.scale ?? 0;

  /// `literal OP column` rewritten as `column OP' literal`.
  static BinaryOperator _flip(BinaryOperator operator) => switch (operator) {
    BinaryOperator.lt => BinaryOperator.gt,
    BinaryOperator.lte => BinaryOperator.gte,
    BinaryOperator.gt => BinaryOperator.lt,
    BinaryOperator.gte => BinaryOperator.lte,
    _ => operator, // eq / neq are symmetric
  };

  @override
  int? get scale {
    if (resultType != DataType.decimal) return null;
    return operator == BinaryOperator.multiply
        ? _leftScale + _rightScale
        : (_leftScale > _rightScale ? _leftScale : _rightScale);
  }

  @override
  int? get precision => resultType == DataType.decimal ? 38 : null;

  @override
  Object? evaluate(RecordBatch batch, int rowIndex) {
    // LIKE's ONLY implementation is this per-row one — evalMask/evalColumn
    // fall back to it — so it must not delegate to them (recursion).
    if (operator == BinaryOperator.like || operator == BinaryOperator.notLike) {
      final leftValue = left.evaluate(batch, rowIndex);
      final rightValue = right.evaluate(batch, rowIndex);
      if (leftValue == null || rightValue == null) return null;
      final match = _matchesPattern(
        leftValue.toString(),
        rightValue.toString(),
      );
      return operator == BinaryOperator.like ? match : !match;
    }
    // Everything else runs the VECTORIZED kernels on a one-row slice, so both
    // paths share the single implementation of 3VL, decimal scale alignment,
    // divide-by-zero, and checked integer arithmetic. The per-row twin this
    // replaces re-implemented all four and drifted SIX times (unaligned
    // decimal adds, `BigInt + int` TypeErrors, silent int64 wrap, ...).
    final column = evalColumn(batch.slice(rowIndex, rowIndex + 1));
    return column[0];
  }

  bool _matchesPattern(String value, String pattern) {
    // LIKE: % = any run, _ = one char. Build the regex char-by-char, escaping
    // every other character — otherwise pattern metachars (`.`, `(`, `+`, …)
    // would match as regex (`a.b` matching `acb`) or enable catastrophic
    // backtracking (ReDoS). (RegExp.escape leaves `%`/`_` untouched since they
    // aren't metacharacters, so they must be handled explicitly here.)
    final buffer = StringBuffer('^');
    for (final ch in pattern.split('')) {
      if (ch == '%') {
        buffer.write('.*');
      } else if (ch == '_') {
        buffer.write('.');
      } else {
        buffer.write(RegExp.escape(ch));
      }
    }
    buffer.write(r'$');
    // dotAll: SQL's `_` is ANY character and `%` ANY sequence — a line
    // terminator is not special. Without it `.` excludes \n \r \u2028 \u2029
    // (ECMAScript semantics), so `'a\nb' LIKE '%'` was FALSE. `^`/`$` stay
    // input-anchored (no multiLine), which is what LIKE wants.
    return RegExp(buffer.toString(), dotAll: true).hasMatch(value);
  }

  @override
  BoolMask evalMask(RecordBatch batch) {
    if (operator == BinaryOperator.isNull ||
        operator == BinaryOperator.isNotNull) {
      final l = left.evalColumn(batch);
      final wantNull = operator == BinaryOperator.isNull;
      final mask = BoolMask(l.length);
      for (int i = 0; i < l.length; i++) {
        // IS [NOT] NULL is always TRUE/FALSE — never UNKNOWN.
        if ((l[i] == null) == wantNull) mask.setTrue(i);
      }
      return mask;
    }
    if (_comparisonOps.contains(operator)) {
      if (right is PhysicalLiteralExpression) {
        return Kernels.compareScalar(
          left.evalColumn(batch),
          operator,
          (right as PhysicalLiteralExpression).value,
          scale: _leftScale,
        );
      }
      // `1.00 = v` reads as `v = 1.00`: flipping keeps the literal a literal, so
      // it gets the same exact decimal alignment as the usual operand order
      // instead of degrading into a constant-column comparison.
      if (left is PhysicalLiteralExpression) {
        return Kernels.compareScalar(
          right.evalColumn(batch),
          _flip(operator),
          (left as PhysicalLiteralExpression).value,
          scale: _rightScale,
        );
      }
      return Kernels.compareColumns(
        left.evalColumn(batch),
        operator,
        right.evalColumn(batch),
        scaleA: _leftScale,
        scaleB: _rightScale,
      );
    }
    if (operator == BinaryOperator.and) {
      return Kernels.and(left.evalMask(batch), right.evalMask(batch));
    }
    if (operator == BinaryOperator.or) {
      return Kernels.or(left.evalMask(batch), right.evalMask(batch));
    }
    return super.evalMask(batch); // like / in / notLike / notIn -> per-row
  }

  @override
  Column evalColumn(RecordBatch batch) {
    if (_arithmeticOps.contains(operator)) {
      final l = left.evalColumn(batch);
      final r = right is PhysicalLiteralExpression
          ? (right as PhysicalLiteralExpression).value
          : right.evalColumn(batch);
      return Kernels.arithmetic(
        l,
        operator,
        r,
        'expr',
        scaleA: _leftScale,
        scaleB: _rightScale,
        resultScale: scale,
      );
    }
    if (_comparisonOps.contains(operator) ||
        operator == BinaryOperator.and ||
        operator == BinaryOperator.or ||
        operator == BinaryOperator.isNull ||
        operator == BinaryOperator.isNotNull) {
      return Kernels.maskToColumn(evalMask(batch), 'expr');
    }
    return super.evalColumn(batch); // like / in -> per-row
  }
}

/// Scalar function call (string / math / null helpers). Uses the per-row base
/// fallback for vectorization, which is correct and off the hot path.
class PhysicalFunctionExpression extends PhysicalExpression {
  final String name;
  final List<PhysicalExpression> args;

  PhysicalFunctionExpression(this.name, this.args);

  @override
  DataType get dataType {
    switch (name.toUpperCase()) {
      case 'LENGTH':
      case 'CHAR_LENGTH':
        return DataType.int64;
      case 'ROUND':
      case 'FLOOR':
      case 'CEIL':
      case 'CEILING':
      case 'SQRT':
      case 'POWER':
      case 'POW':
        return DataType.float64; // always toDouble()
      case 'ABS':
        // ABS preserves the argument's kind: num.abs() on an int stays an int.
        return _isIntType(_argType(0)) ? DataType.int64 : DataType.float64;
      case 'MOD':
        // `a % b` is int for two ints, else double — mirror the kernel rule.
        return Kernels.arithResultType(
          _argType(0),
          _argType(1),
          BinaryOperator.modulo,
        );
      case 'UPPER':
      case 'LOWER':
      case 'TRIM':
      case 'SUBSTR':
      case 'SUBSTRING':
      case 'CONCAT':
        return DataType.string;
      case 'COALESCE':
        // The result may come from any argument, so type it to the widest common
        // type across them (values are coerced to this in evalColumn).
        if (args.isEmpty) return DataType.string;
        var type = args.first.dataType;
        for (final a in args.skip(1)) {
          type = DataType.commonType(type, a.dataType) ?? type;
        }
        return type;
      default:
        return args.isNotEmpty ? args.first.dataType : DataType.string;
    }
  }

  /// COALESCE may return any argument, so a decimal result must settle on the
  /// widest scale and lift each argument to it — otherwise the output field
  /// carries no scale and a decimal reads back as a scale-0 integer.
  @override
  int? get scale {
    if (dataType != DataType.decimal) return null;
    var widest = 0;
    for (final a in args) {
      final s = a.scale ?? 0;
      if (s > widest) widest = s;
    }
    return widest;
  }

  @override
  int? get precision => dataType == DataType.decimal ? 38 : null;

  DataType _argType(int i) =>
      i < args.length ? args[i].dataType : DataType.float64;

  static bool _isIntType(DataType t) =>
      t == DataType.int8 ||
      t == DataType.int16 ||
      t == DataType.int32 ||
      t == DataType.int64;

  /// Argument [i]'s evaluated value as a plain number: a decimal argument
  /// holds an unscaled BigInt, which is NOT a num — casting it directly made
  /// every math function over a decimal (including ABS(SUM(x)), since SUM(int)
  /// surfaces as decimal) throw a TypeError. Divide it back down by its scale,
  /// mirroring the kernel's _decimalAsNum.
  num? _numArg(List<dynamic> a, int i) {
    final v = a[i];
    if (v == null) return null;
    if (v is BigInt) return Kernels.decimalToNum(v, args[i].scale ?? 0);
    return v as num;
  }

  /// Coerce each produced value to the declared [dataType] so the column's
  /// values match its type (e.g. COALESCE(int,double)->float64 must hold doubles;
  /// a Parquet write would otherwise reject an int under a double leaf).
  @override
  Column evalColumn(RecordBatch batch) {
    final type = dataType;
    final float = type == DataType.float32 || type == DataType.float64;
    final data = <dynamic>[];
    for (int i = 0; i < batch.rowCount; i++) {
      var value = evaluate(batch, i);
      if (float && value != null) value = (value as num).toDouble();
      data.add(value);
    }
    return ListColumn(
      name: 'expr',
      dataType: type,
      data: data,
      nullable: data.contains(null),
    );
  }

  @override
  Object? evaluate(RecordBatch batch, int rowIndex) {
    final a = <dynamic>[for (final e in args) e.evaluate(batch, rowIndex)];
    switch (name.toUpperCase()) {
      case 'UPPER':
        return a[0]?.toString().toUpperCase();
      case 'LOWER':
        return a[0]?.toString().toLowerCase();
      case 'LENGTH':
      case 'CHAR_LENGTH':
        return a[0]?.toString().length;
      case 'TRIM':
        return a[0]?.toString().trim();
      case 'CONCAT':
        return a.where((x) => x != null).map((x) => x.toString()).join();
      case 'SUBSTR':
      case 'SUBSTRING':
        {
          if (a[0] == null) return null;
          final s = a[0].toString();
          final from = ((a[1] as num).toInt() - 1).clamp(
            0,
            s.length,
          ); // 1-based
          final to = (a.length > 2 && a[2] != null)
              ? (from + (a[2] as num).toInt()).clamp(0, s.length)
              : s.length;
          return s.substring(from, to);
        }
      case 'COALESCE':
        // Coerce the WINNING argument to the declared result type/scale. Which
        // argument won matters: only it knows the scale its value is unscaled
        // at. (`firstWhere` alone returned a raw int under a decimal/float
        // result — the same dtype lie CASE had.)
        for (int i = 0; i < a.length; i++) {
          if (a[i] != null) {
            return coerceToResult(
              a[i],
              args[i].scale ?? 0,
              dataType,
              scale ?? 0,
            );
          }
        }
        return null;
      case 'ABS':
        return a[0] == null ? null : _numArg(a, 0)!.abs();
      case 'ROUND':
        {
          if (a[0] == null) return null;
          final v = _numArg(a, 0)!.toDouble();
          if (a.length > 1 && a[1] != null) {
            final digits = (a[1] as num).toInt();
            // Rounding to >= 15 fractional digits adds no float64 precision, so
            // it's a no-op — and computing 10^digits there would overflow to
            // Infinity (then NaN). Clamp the low end too (10^-308 underflows to
            // 0 → division by 0). If scaling overflows for a huge value, return
            // it unchanged.
            if (digits >= 15 || !v.isFinite) return v;
            final f = math.pow(10, digits < -307 ? -307 : digits);
            final scaled = v * f;
            if (!scaled.isFinite) return v;
            return scaled.round() / f;
          }
          return v.roundToDouble();
        }
      case 'FLOOR':
        return a[0] == null ? null : _numArg(a, 0)!.floorToDouble();
      case 'CEIL':
      case 'CEILING':
        return a[0] == null ? null : _numArg(a, 0)!.ceilToDouble();
      case 'SQRT':
        return a[0] == null ? null : math.sqrt(_numArg(a, 0)!.toDouble());
      case 'POWER':
      case 'POW':
        return (a[0] == null || a[1] == null)
            ? null
            : math.pow(_numArg(a, 0)!.toDouble(), _numArg(a, 1)!.toDouble());
      case 'MOD':
        // Via the kernel twin: MOD's dataType comes from arithResultType, so a
        // decimal operand makes the result decimal — the value must then be an
        // unscaled BigInt at [scale], not a raw num (and 0 divides to NULL).
        return (a[0] == null || a[1] == null)
            ? null
            : Kernels.arithScalar(
                a[0],
                _argType(0),
                args[0].scale ?? 0,
                BinaryOperator.modulo,
                a[1],
                _argType(1),
                args[1].scale ?? 0,
                resultScale: scale,
              );
      default:
        // Fail closed: an unknown function must not become a silent all-null
        // column.
        throw UnsupportedError('Unknown SQL function: $name');
    }
  }
}

class PhysicalWhen {
  final PhysicalExpression condition;
  final PhysicalExpression result;
  PhysicalWhen(this.condition, this.result);
}

/// CASE WHEN ... THEN ... [ELSE ...] END.
class PhysicalCaseExpression extends PhysicalExpression {
  final List<PhysicalWhen> whens;
  final PhysicalExpression? elseExpr;
  final DataType resultType;

  PhysicalCaseExpression(this.whens, this.elseExpr, this.resultType);

  @override
  DataType get dataType => resultType;

  /// Every branch is a candidate result, so a decimal CASE must settle on ONE
  /// scale — the widest — and lift each branch to it. Without this the output
  /// field carries no scale at all and a decimal result reads back as a scale-0
  /// integer (1.50 as 150), the same defect MIN/MAX were fixed for.
  @override
  int? get scale {
    if (resultType != DataType.decimal) return null;
    var widest = 0;
    for (final w in whens) {
      final s = w.result.scale ?? 0;
      if (s > widest) widest = s;
    }
    final e = elseExpr;
    final s = e?.scale ?? 0;
    return s > widest ? s : widest;
  }

  @override
  int? get precision => resultType == DataType.decimal ? 38 : null;

  /// Coercion lives HERE rather than in an `evalColumn` override, so the
  /// vectorized and per-row paths cannot disagree: `evalColumn` is defined in
  /// terms of `evaluate`, so fixing the source fixes both. (Branch types are
  /// widened to a common `resultType` by the planner, but the VALUES were left
  /// alone — a float64-declared column then held Dart ints, which a Parquet
  /// write rejects and which splits one decimal value across two DISTINCT
  /// groups, since `BigInt.from(0) != 0`.)
  @override
  Object? evaluate(RecordBatch batch, int rowIndex) {
    for (final w in whens) {
      if (w.condition.evaluate(batch, rowIndex) == true) {
        return coerceToResult(
          w.result.evaluate(batch, rowIndex),
          w.result.scale ?? 0,
          resultType,
          scale ?? 0,
        );
      }
    }
    final e = elseExpr;
    if (e == null) return null;
    return coerceToResult(
      e.evaluate(batch, rowIndex),
      e.scale ?? 0,
      resultType,
      scale ?? 0,
    );
  }
}

/// CAST(expr AS type).
class PhysicalCastExpression extends PhysicalExpression {
  final PhysicalExpression expr;
  final DataType targetType;

  /// Explicit `DECIMAL(p,s)` parameters; null for other targets and for bare
  /// `DECIMAL`.
  final int? targetPrecision;
  final int? targetScale;

  PhysicalCastExpression(
    this.expr,
    this.targetType, {
    this.targetPrecision,
    this.targetScale,
  });

  @override
  DataType get dataType => targetType;

  /// `CAST(<decimal> AS DECIMAL)` passes the unscaled value straight through, so
  /// it must keep the SOURCE's scale — reporting null here declared a scale-0
  /// decimal over scale-1 data and rendered 1.5 as "15". An explicit
  /// `DECIMAL(p,s)` overrides both.
  @override
  int? get precision => targetType == DataType.decimal
      ? (targetPrecision ?? expr.precision ?? 38)
      : null;

  @override
  int? get scale =>
      targetType == DataType.decimal ? (targetScale ?? expr.scale ?? 0) : null;

  @override
  Object? evaluate(RecordBatch batch, int rowIndex) {
    final v = expr.evaluate(batch, rowIndex);
    if (v == null) return null;
    // A decimal is an UNSCALED BigInt: 1.5 at scale 1 arrives as BigInt(15).
    // Every branch below tests `v is num`, which a BigInt is not, so each one
    // fell through to the string/passthrough path and cast the RAW unscaled
    // integer — CAST(1.5 AS DOUBLE) gave 15.0, off by exactly 10^scale.
    // Resolve the real value first; decimal→decimal keeps the representation.
    if (v is BigInt &&
        expr.dataType == DataType.decimal &&
        targetType != DataType.decimal) {
      return _fromDecimal(v, expr.scale ?? 0);
    }
    switch (targetType) {
      case DataType.int8:
      case DataType.int16:
      case DataType.int32:
      case DataType.int64:
        if (v is num) {
          // A non-finite or out-of-int64-range value can't be an integer:
          // double.toInt() throws on inf/NaN and silently clamps a huge finite
          // value. SQL casts that overflow yield NULL, not a crash or a clamp.
          if (v is double &&
              (!v.isFinite ||
                  v < -9223372036854775808.0 ||
                  v >= 9223372036854775808.0)) {
            return null;
          }
          return v.toInt();
        }
        return int.tryParse(v.toString());
      case DataType.float32:
      case DataType.float64:
        return v is num ? v.toDouble() : double.tryParse(v.toString());
      case DataType.decimal:
        // A decimal column holds UNSCALED BigInt; never a double (that would
        // be a mistyped column that breaks RowKey dedup and formatDecimal).
        final s = targetScale;
        if (s == null) {
          // Bare DECIMAL: an integer casts to scale-0 BigInt; a BigInt passes
          // through (keeping the source scale); a non-integer needs an
          // explicit scale — refuse.
          if (v is BigInt) return v;
          if (v is int) return BigInt.from(v);
          throw StateError(
            'CAST to DECIMAL from a non-integer value ($v) needs an explicit '
            'scale — use DECIMAL(p,s)',
          );
        }
        // Explicit DECIMAL(p,s): produce the unscaled value AT s. Anything
        // that would drop digits fails LOUD (same contract as toUnscaled /
        // unscaledFromString) — SQL engines round here, but a silent rounding
        // is a wrong stored value in a type whose whole point is exactness.
        if (v is BigInt && expr.dataType == DataType.decimal) {
          return rescaleUnscaled(v, expr.scale ?? 0, s);
        }
        if (v is int) return BigInt.from(v) * BigInt.from(10).pow(s);
        if (v is num) {
          if (!v.isFinite) return null; // like the int/float casts above
          return toUnscaled(v, s);
        }
        return unscaledFromString(v.toString(), s);
      case DataType.boolean:
        return v is bool
            ? v
            : (v.toString().toLowerCase() == 'true' || v.toString() == '1');
      case DataType.string:
        return v.toString();
      default:
        return v;
    }
  }

  /// [unscaled] / 10^[scale] cast to [targetType]. Formatting once and parsing
  /// the text keeps the written value exact rather than dividing two doubles.
  Object? _fromDecimal(BigInt unscaled, int scale) {
    switch (targetType) {
      case DataType.int8:
      case DataType.int16:
      case DataType.int32:
      case DataType.int64:
        // SQL truncates toward zero; BigInt's ~/ already does. A quotient past
        // int64 yields NULL, like the out-of-range double cast above.
        final whole = unscaled ~/ BigInt.from(10).pow(scale);
        return whole.isValidInt ? whole.toInt() : null;
      case DataType.float32:
      case DataType.float64:
        return double.tryParse(formatDecimal(unscaled, scale));
      case DataType.string:
        return formatDecimal(unscaled, scale);
      default:
        return unscaled;
    }
  }
}
