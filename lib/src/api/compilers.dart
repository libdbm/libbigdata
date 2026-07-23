part of 'dataframe.dart';

/// Compiles the DataFrame predicate DSL into a vectorized [PhysicalExpression].
/// Supports `AND`/`OR` (case-insensitive, lowest-to-higher precedence OR<AND),
/// optional `[bracketed]` column names, names with spaces, and the comparison
/// operators `= == != <> < <= > >=`. No parentheses (flat precedence).
class _PredicateCompiler {
  static PhysicalExpression compile(String predicate, Schema schema) {
    final trimmed = predicate.trim();
    if (trimmed.isEmpty) {
      throw ArgumentError('Invalid predicate - empty');
    }
    return _or(trimmed, schema);
  }

  static PhysicalExpression _or(String s, Schema schema) {
    final parts = _split(s, ' OR ');
    var expr = _and(parts.first, schema);
    for (int i = 1; i < parts.length; i++) {
      expr = PhysicalBinaryExpression(
        left: expr,
        operator: plan.BinaryOperator.or,
        right: _and(parts[i], schema),
        resultType: DataType.boolean,
      );
    }
    return expr;
  }

  static PhysicalExpression _and(String s, Schema schema) {
    final parts = _split(s, ' AND ');
    var expr = _comparison(parts.first, schema);
    for (int i = 1; i < parts.length; i++) {
      expr = PhysicalBinaryExpression(
        left: expr,
        operator: plan.BinaryOperator.and,
        right: _comparison(parts[i], schema),
        resultType: DataType.boolean,
      );
    }
    return expr;
  }

  static PhysicalExpression _comparison(String raw, Schema schema) {
    final s = raw.trim();
    String column;
    String operator;
    String valueStr;
    final bracket = RegExp(
      r'^\[([^\]]+)\]\s*(==|!=|<>|>=|<=|>|<|=)\s*(.+)$',
    ).firstMatch(s);
    if (bracket != null) {
      column = bracket.group(1)!;
      operator = bracket.group(2)!;
      valueStr = bracket.group(3)!.trim();
    } else {
      final m = RegExp(r'\s+(==|!=|<>|>=|<=|>|<|=)\s+').firstMatch(s);
      if (m == null) {
        throw ArgumentError('Invalid predicate - no operator found: $s');
      }
      column = s.substring(0, m.start).trim();
      operator = m.group(1)!;
      valueStr = s.substring(m.end).trim();
    }

    if (valueStr.length >= 2 &&
        ((valueStr.startsWith('"') && valueStr.endsWith('"')) ||
            (valueStr.startsWith("'") && valueStr.endsWith("'")))) {
      valueStr = valueStr.substring(1, valueStr.length - 1);
    }

    var type = DataType.string;
    for (final f in schema.fields) {
      if (f.name == column) {
        type = f.dataType;
        break;
      }
    }
    Object? value = valueStr;
    if (type.isNumeric) {
      value = num.tryParse(valueStr) ?? valueStr;
    } else if (type == DataType.boolean) {
      value = valueStr.toLowerCase() == 'true';
    }

    return PhysicalBinaryExpression(
      left: PhysicalColumnExpression(column, type),
      operator: _operator(operator),
      right: PhysicalLiteralExpression(value, type),
      resultType: DataType.boolean,
    );
  }

  static plan.BinaryOperator _operator(String operator) {
    switch (operator) {
      case '=':
      case '==':
        return plan.BinaryOperator.eq;
      case '!=':
      case '<>':
        return plan.BinaryOperator.neq;
      case '>':
        return plan.BinaryOperator.gt;
      case '>=':
        return plan.BinaryOperator.gte;
      case '<':
        return plan.BinaryOperator.lt;
      case '<=':
        return plan.BinaryOperator.lte;
      default:
        throw ArgumentError('Unknown operator: $operator');
    }
  }

  /// Split [s] on [delim] (case-insensitive) at the top level, ignoring
  /// occurrences inside single/double quotes.
  static List<String> _split(String s, String delim) {
    final out = <String>[];
    final lowerDelim = delim.toLowerCase();
    int start = 0;
    int i = 0;
    String? quote;
    while (i <= s.length - delim.length) {
      final ch = s[i];
      if (quote != null) {
        if (ch == quote) quote = null;
        i++;
        continue;
      }
      if (ch == '"' || ch == "'") {
        quote = ch;
        i++;
        continue;
      }
      if (s.substring(i, i + delim.length).toLowerCase() == lowerDelim) {
        out.add(s.substring(start, i));
        start = i + delim.length;
        i += delim.length;
        continue;
      }
      i++;
    }
    out.add(s.substring(start));
    return out;
  }
}

/// Compiles the DataFrame `withColumn` arithmetic DSL into a vectorized
/// [PhysicalExpression]: a single operand (column ref, numeric/quoted literal)
/// or a binary `+ - * / %` expression, LEFT-associative (`a - b - c` = `(a-b)-c`,
/// `a / b / c` = `(a/b)/c`), so non-commutative operators are correct. Flat
/// precedence (no `*`-before-`+`). Numeric only — `+` does not concatenate
/// strings (Kernels.arithmetic throws); divide/modulo by zero yields NULL.
class _ExprCompiler {
  static PhysicalExpression compile(String expr, Schema schema) {
    // Greedy left operand matches through to the LAST top-level operator, so the
    // right operand is a single token and the recursion folds LEFT (was a
    // non-greedy left that took the FIRST operator and recursed right — wrong
    // for `-`/`/`/`%`).
    final m = RegExp(r'^(.+)\s*([+\-*/%])\s*(.+?)$').firstMatch(expr.trim());
    if (m == null) return _operand(expr.trim(), schema);
    var leftText = m.group(1)!.trim();
    var opText = m.group(2)!;
    var rightText = m.group(3)!.trim();
    // A trailing operator on the left means the greedy match split inside
    // `OP -literal`: `a * -1` matched left 'a *' with '-' as the operator, so
    // the whole expression evaluated as `(unknown column "a *") - 1` -> null.
    // Rebind: the real operator is the left's trailing character and the sign
    // belongs to the right operand.
    if (leftText.isNotEmpty &&
        '+-*/%'.contains(leftText[leftText.length - 1])) {
      rightText = '$opText$rightText';
      opText = leftText[leftText.length - 1];
      leftText = leftText.substring(0, leftText.length - 1).trim();
    }
    final left = compile(leftText, schema);
    final right = _operand(rightText, schema);
    final operator = _operator(opText);
    return PhysicalBinaryExpression(
      left: left,
      operator: operator,
      right: right,
      resultType: _resultType(operator, left, right),
    );
  }

  static PhysicalExpression _operand(String token, Schema schema) {
    for (final f in schema.fields) {
      if (f.name == token) return PhysicalColumnExpression(token, f.dataType);
    }
    final n = num.tryParse(token);
    if (n != null) {
      return PhysicalLiteralExpression(
        n,
        n is int ? DataType.int64 : DataType.float64,
      );
    }
    if (token.length >= 2 &&
        ((token.startsWith('"') && token.endsWith('"')) ||
            (token.startsWith("'") && token.endsWith("'")))) {
      return PhysicalLiteralExpression(
        token.substring(1, token.length - 1),
        DataType.string,
      );
    }
    // Unknown reference: treated as a column, so it fails loud at execution
    // time with `StateError: Unknown column: <token>`.
    return PhysicalColumnExpression(token, DataType.string);
  }

  static plan.BinaryOperator _operator(String operator) {
    switch (operator) {
      case '+':
        return plan.BinaryOperator.add;
      case '-':
        return plan.BinaryOperator.subtract;
      case '*':
        return plan.BinaryOperator.multiply;
      case '/':
        return plan.BinaryOperator.divide;
      case '%':
        return plan.BinaryOperator.modulo;
      default:
        throw ArgumentError('Unknown operator: $operator');
    }
  }

  static DataType _resultType(
    plan.BinaryOperator operator,
    PhysicalExpression l,
    PhysicalExpression r,
  ) {
    if (operator == plan.BinaryOperator.divide) return DataType.float64;
    final float =
        l.dataType.dartType == double || r.dataType.dartType == double;
    return float ? DataType.float64 : DataType.int64;
  }
}
