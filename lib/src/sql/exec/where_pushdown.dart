import '../planner/logical_plan.dart' as lp;
import '../../core/row_filter.dart';

/// Translate the prunable subset of a logical predicate into a [RowFilter] for
/// row-group / partition pruning. Returns null when nothing prunable can be
/// extracted. The translation is *conservative*; it never produces a filter
/// that could discard a matching row group:
///   - `A AND B`: prune on whichever side(s) are prunable (a subset of the
///     constraints still only over-keeps).
///   - `A OR B`: prunable only if BOTH sides are (else any group might match the
///     unprunable side): returns null otherwise.
///   - `col OP literal` (or `literal OP col`): a leaf comparison.
///   - `col [NOT] IN (literals)`: the equivalent OR of `=` (AND of `<>`).
/// Anything else (functions, NOT, BETWEEN, sub-expressions) yields null.
RowFilter? logicalToRowFilter(lp.LogicalExpression expression) {
  if (expression is lp.InExpression) return _logicalInToRowFilter(expression);
  if (expression is! lp.BinaryExpression) return null;
  final operator = expression.operator;

  if (operator == lp.BinaryOperator.and) {
    final l = logicalToRowFilter(expression.left);
    final r = logicalToRowFilter(expression.right);
    if (l != null && r != null) return RowFilter.and([l, r]);
    return l ?? r;
  }
  if (operator == lp.BinaryOperator.or) {
    final l = logicalToRowFilter(expression.left);
    final r = logicalToRowFilter(expression.right);
    if (l != null && r != null) return RowFilter.or([l, r]);
    return null;
  }

  final comparison = _logicalOp(operator);
  if (comparison == null) return null;

  final left = expression.left;
  final right = expression.right;
  if (left is lp.ColumnExpression && right is lp.LiteralExpression) {
    final v = right.value;
    return v == null ? null : RowFilter.compare(left.name, comparison, v);
  }
  if (left is lp.LiteralExpression && right is lp.ColumnExpression) {
    final v = left.value;
    return v == null
        ? null
        : RowFilter.compare(right.name, _flip(comparison), v);
  }
  return null;
}

/// `col [NOT] IN (literals)` as the equivalent OR of `=` (AND of `<>`), or
/// null when the probe is not a column or any element is not a non-null
/// literal.
RowFilter? _logicalInToRowFilter(lp.InExpression expression) {
  final column = expression.value;
  if (column is! lp.ColumnExpression) return null;
  final leaves = <RowFilter>[];
  for (final value in expression.list) {
    if (value is! lp.LiteralExpression || value.value == null) return null;
    leaves.add(
      RowFilter.compare(
        column.name,
        expression.negated ? FilterOp.ne : FilterOp.eq,
        value.value,
      ),
    );
  }
  if (leaves.isEmpty) return null;
  return expression.negated ? RowFilter.and(leaves) : RowFilter.or(leaves);
}

FilterOp? _logicalOp(lp.BinaryOperator operator) {
  switch (operator) {
    case lp.BinaryOperator.eq:
      return FilterOp.eq;
    case lp.BinaryOperator.neq:
      return FilterOp.ne;
    case lp.BinaryOperator.lt:
      return FilterOp.lt;
    case lp.BinaryOperator.lte:
      return FilterOp.le;
    case lp.BinaryOperator.gt:
      return FilterOp.gt;
    case lp.BinaryOperator.gte:
      return FilterOp.ge;
    default:
      return null;
  }
}

/// Flip a comparison for `literal OP column` so it reads `column OP' literal`.
FilterOp _flip(FilterOp o) {
  switch (o) {
    case FilterOp.lt:
      return FilterOp.gt;
    case FilterOp.le:
      return FilterOp.ge;
    case FilterOp.gt:
      return FilterOp.lt;
    case FilterOp.ge:
      return FilterOp.le;
    case FilterOp.eq:
    case FilterOp.ne:
      return o;
  }
}
