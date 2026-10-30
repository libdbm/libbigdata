import '../../core/conversions.dart' show decimalTypeParams;
import '../../core/data_type.dart';
import '../ast/ast.dart' as ast;
import 'logical_plan.dart' as logical;

/// Converts AST expressions to logical expressions.
///
/// Conversion reads no planner state and no schema: a column becomes an
/// unresolved [logical.ColumnExpression] (typed as string) that the physical
/// builder binds against its input schema, which is where an unknown or
/// ambiguous column is reported. Aggregates are converted as scalar function
/// calls; the planner replaces them with column references before calling
/// [convert]. A subquery (`IN (SELECT ...)`, `EXISTS`, a scalar subquery) is
/// rejected; the planner rewrites the forms it supports into joins first.
class Converter {
  const Converter();

  logical.LogicalExpression convert(ast.Expression expression) {
    if (expression is ast.ColumnExpression) {
      return logical.ColumnExpression(
        name: expression.column,
        table: expression.table,
        columnType: DataType.string, // Will be resolved during binding
      );
    } else if (expression is ast.LiteralExpression) {
      return logical.LiteralExpression(
        expression.value,
        _inferLiteralType(expression.value),
      );
    } else if (expression is ast.InExpression) {
      if (expression.subquery != null) {
        // A subquery IN is planned as a semi/anti join by _applyWhere, but only
        // as a top-level WHERE conjunct. Reaching here means another position (a
        // SELECT item, HAVING, inside OR / CASE), which the join rewrite can't
        // express.
        throw UnsupportedError(
          'IN (SELECT …) is only supported as a top-level WHERE condition; '
          'elsewhere, use a JOIN or list the values explicitly',
        );
      }
      return logical.InExpression(
        value: convert(expression.expression),
        list: expression.values.map(convert).toList(),
        negated: expression.negated,
      );
    } else if (expression is ast.BinaryExpression) {
      // `x IS [NOT] TRUE/FALSE` is a 2-valued predicate distinct from `IS NULL`
      // and from `= true` (NULL IS TRUE is FALSE, not NULL). The parser produces
      // `IS`/`IS NOT` with a boolean-literal RHS, which the generic mapping would
      // collapse to isNull. Desugar it to the correct 3VL form here. (A NULL RHS
      // falls through to the isNull/isNotNull mapping below.)
      final boolIs = _convertBooleanIs(expression);
      if (boolIs != null) return boolIs;
      return logical.BinaryExpression(
        convert(expression.left),
        _convertBinaryOperator(expression.operator),
        convert(expression.right),
      );
    } else if (expression is ast.FunctionExpression) {
      return logical.FunctionExpression(
        name: expression.name,
        arguments: expression.arguments.map(convert).toList(),
        returnType: DataType.string, // Will be resolved during binding
      );
    } else if (expression is ast.CaseExpression) {
      final clauses = <logical.WhenClause>[
        for (final w in expression.whenClauses)
          logical.WhenClause(
            expression.expression == null
                ? convert(w.condition)
                : logical.BinaryExpression(
                    convert(expression.expression!),
                    logical.BinaryOperator.eq,
                    convert(w.condition),
                  ),
            convert(w.result),
          ),
      ];
      return logical.CaseExpression(
        whenClauses: clauses,
        elseExpression: expression.elseExpression == null
            ? null
            : convert(expression.elseExpression!),
      );
    } else if (expression is ast.CastExpression) {
      final target = _parseDataType(expression.targetType);
      // `DECIMAL(p,s)` carries an explicit precision/scale; bare DECIMAL does
      // not (a decimal source then keeps its own scale, an int casts at 0).
      // decimalTypeParams matches the `decimal(p,s)` form only, so map the
      // NUMERIC synonym onto it.
      final params =
          target == DataType.decimal && expression.targetType.contains('(')
          ? decimalTypeParams(
              expression.targetType.toLowerCase().replaceFirst(
                'numeric',
                'decimal',
              ),
            )
          : null;
      return logical.CastExpression(
        convert(expression.expression),
        target,
        precision: params?.precision,
        scale: params?.scale,
      );
    } else if (expression is ast.UnaryExpression) {
      final operator = expression.operator.toUpperCase();
      if (operator == '+') return convert(expression.operand);
      if (operator == '-') {
        final operand = expression.operand;
        // Fold a numeric literal (so `-5` is a real value, not `0 - 5`).
        if (operand is ast.LiteralExpression && operand.value is num) {
          final v = -(operand.value as num);
          return logical.LiteralExpression(v, _inferLiteralType(v));
        }
        return logical.BinaryExpression(
          logical.LiteralExpression(0, DataType.int64),
          logical.BinaryOperator.subtract,
          convert(operand),
        );
      }
      if (operator == 'NOT') {
        // NOT x ≡ (x = false) under the engine's boolean evaluation.
        return logical.BinaryExpression(
          convert(expression.operand),
          logical.BinaryOperator.eq,
          logical.LiteralExpression(false, DataType.boolean),
        );
      }
      throw UnsupportedError(
        'Unsupported unary operator: ${expression.operator}',
      );
    }

    // Fail closed: an unsupported expression node must not silently become a
    // NULL column.
    throw UnsupportedError(
      'Unsupported SQL expression: ${expression.runtimeType}',
    );
  }

  /// Map a SQL type name (optionally parametrized, `DECIMAL(10,2)`) to a
  /// [DataType].
  DataType _parseDataType(String type) {
    final base = type.toUpperCase().replaceAll(RegExp(r'\s*\(.*\)$'), '');
    switch (base) {
      // The widths DuckDB and PostgreSQL give these names; a CAST checks
      // the value fits.
      case 'TINYINT':
        return DataType.int8;
      case 'SMALLINT':
        return DataType.int16;
      case 'INT':
      case 'INTEGER':
        return DataType.int32;
      case 'BIGINT':
        return DataType.int64;
      case 'DOUBLE':
      case 'FLOAT':
      case 'REAL':
        return DataType.float64;
      // The engine HAS an exact decimal type: mapping these to float64
      // silently destroyed precision on any value past 2^53.
      case 'DECIMAL':
      case 'NUMERIC':
        return DataType.decimal;
      case 'BOOLEAN':
      case 'BOOL':
        return DataType.boolean;
      case 'DATE':
        return DataType.date;
      case 'TIMESTAMP':
      case 'DATETIME':
        return DataType.timestamp;
      // There is no time-of-day type; falling through to string returned the
      // input unchanged under a TIME label.
      case 'TIME':
        throw UnsupportedError('CAST to TIME is not supported');
      default:
        return DataType.string;
    }
  }

  logical.BinaryOperator _convertBinaryOperator(String operator) =>
      logical.BinaryOperator.parse(operator);

  /// Desugar `x IS [NOT] TRUE/FALSE` to its 2-valued semantics, or null when
  /// [expression] is not that form (a plain comparison, or `IS [NOT] NULL`).
  /// Unlike `= true`, these never yield NULL: `x IS TRUE` is FALSE (not NULL)
  /// when x is NULL. `x IS b` ≡ `(x IS NOT NULL) AND (x = b)`; `x IS NOT b` is
  /// its negation `(x IS NULL) OR (x = !b)`.
  logical.LogicalExpression? _convertBooleanIs(
    ast.BinaryExpression expression,
  ) {
    final positive = expression.operator == 'IS';
    final negative = expression.operator == 'IS NOT';
    if (!positive && !negative) return null;
    final right = expression.right;
    // IS [NOT] NULL: the generic isNull/isNotNull mapping below is correct.
    if (right is ast.LiteralExpression && right.value == null) return null;
    // Only NULL and TRUE/FALSE are valid IS operands. Anything else (`x IS 5`,
    // `x IS y`) would fall to the generic mapping, which IGNORES the right
    // operand and silently evaluates `x IS NULL`, so reject instead.
    if (right is! ast.LiteralExpression || right.value is! bool) {
      throw UnsupportedError(
        "the operand of IS${negative ? ' NOT' : ''} must be NULL, TRUE, or "
        'FALSE',
      );
    }
    final wanted = right.value as bool;
    final value = convert(expression.left);
    logical.LogicalExpression compare(bool b) => logical.BinaryExpression(
      value,
      logical.BinaryOperator.eq,
      logical.LiteralExpression(b, DataType.boolean),
    );
    logical.LogicalExpression nullCheck(logical.BinaryOperator operator) =>
        logical.BinaryExpression(
          value,
          operator,
          logical.LiteralExpression(null, DataType.boolean),
        );
    return positive
        ? logical.BinaryExpression(
            nullCheck(logical.BinaryOperator.isNotNull),
            logical.BinaryOperator.and,
            compare(wanted),
          )
        : logical.BinaryExpression(
            nullCheck(logical.BinaryOperator.isNull),
            logical.BinaryOperator.or,
            compare(!wanted),
          );
  }

  DataType _inferLiteralType(dynamic value) {
    // NULL has no type of its own: it takes the other side's in CASE,
    // COALESCE and set operations (commonType skips null_).
    if (value == null) return DataType.null_;
    // Type an integer by its RANGE. A flat int32 makes the declared dtype a lie
    // for anything past 2^31, and a boxed literal column carries that lie to a
    // fixed-width encoder that keeps only the low 32 bits.
    if (value is int) return DataType.intType(value);
    if (value is double) return DataType.float64;
    if (value is bool) return DataType.boolean;
    if (value is String) return DataType.string;
    return DataType.string;
  }
}
