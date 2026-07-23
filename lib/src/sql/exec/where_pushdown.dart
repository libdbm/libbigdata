import '../ast/ast.dart';
import '../planner/logical_plan.dart' as lp;
import '../../core/row_filter.dart';

/// Translate the prunable subset of a SQL WHERE [expr] into a [RowFilter] for
/// row-group / partition pruning. Returns null when nothing prunable can be
/// extracted. The translation is *conservative* — it never produces a filter
/// that could discard a matching row group:
///   - `A AND B`: prune on whichever side(s) are prunable (a subset of the
///     constraints still only over-keeps).
///   - `A OR B`: prunable only if BOTH sides are (else any group might match the
///     unprunable side) — returns null otherwise.
///   - `col OP literal` (or `literal OP col`): a leaf comparison.
///   - `col [NOT] IN (literals)`: the equivalent OR of `=` (AND of `<>`).
/// Anything else (functions, NOT, BETWEEN, sub-expressions) yields null.
RowFilter? whereToRowFilter(Expression expr) {
  if (expr is InExpression) return _inToRowFilter(expr);
  if (expr is! BinaryExpression) return null;
  final operator = expr.operator.toUpperCase();

  if (operator == 'AND') {
    final l = whereToRowFilter(expr.left);
    final r = whereToRowFilter(expr.right);
    if (l != null && r != null) return RowFilter.and([l, r]);
    return l ?? r;
  }
  if (operator == 'OR') {
    final l = whereToRowFilter(expr.left);
    final r = whereToRowFilter(expr.right);
    if (l != null && r != null) return RowFilter.or([l, r]);
    return null;
  }

  final fop = _operator(operator);
  if (fop == null) return null;

  final left = expr.left;
  final right = expr.right;
  if (left is ColumnExpression && right is LiteralExpression) {
    final v = right.value;
    return v == null ? null : RowFilter.compare(left.column, fop, v);
  }
  if (left is LiteralExpression && right is ColumnExpression) {
    final v = left.value;
    return v == null ? null : RowFilter.compare(right.column, _flip(fop), v);
  }
  return null;
}

/// `col IN (a, b)` prunes exactly like `col = a OR col = b`, and `col NOT IN
/// (a, b)` like `col <> a AND col <> b`. A NULL in the list makes the predicate
/// UNKNOWN rather than TRUE, and a null-valued leaf is not prunable, so the
/// whole filter is dropped — over-keeping, never over-discarding.
RowFilter? _inToRowFilter(InExpression expr) {
  final column = expr.expression;
  if (column is! ColumnExpression || expr.subquery != null) return null;
  final leaves = <RowFilter>[];
  for (final value in expr.values) {
    if (value is! LiteralExpression || value.value == null) return null;
    leaves.add(
      RowFilter.compare(
        column.column,
        expr.negated ? FilterOp.ne : FilterOp.eq,
        value.value,
      ),
    );
  }
  if (leaves.isEmpty) return null;
  return expr.negated ? RowFilter.and(leaves) : RowFilter.or(leaves);
}

/// Logical-plan twin of [whereToRowFilter]: translate the prunable subset of a
/// logical predicate into a [RowFilter]. Same conservative (over-keeping) rules.
RowFilter? logicalToRowFilter(lp.LogicalExpression expr) {
  if (expr is lp.InExpression) return _logicalInToRowFilter(expr);
  if (expr is! lp.BinaryExpression) return null;
  final operator = expr.operator;

  if (operator == lp.BinaryOperator.and) {
    final l = logicalToRowFilter(expr.left);
    final r = logicalToRowFilter(expr.right);
    if (l != null && r != null) return RowFilter.and([l, r]);
    return l ?? r;
  }
  if (operator == lp.BinaryOperator.or) {
    final l = logicalToRowFilter(expr.left);
    final r = logicalToRowFilter(expr.right);
    if (l != null && r != null) return RowFilter.or([l, r]);
    return null;
  }

  final fop = _logicalOp(operator);
  if (fop == null) return null;

  final left = expr.left;
  final right = expr.right;
  if (left is lp.ColumnExpression && right is lp.LiteralExpression) {
    final v = right.value;
    return v == null ? null : RowFilter.compare(left.name, fop, v);
  }
  if (left is lp.LiteralExpression && right is lp.ColumnExpression) {
    final v = left.value;
    return v == null ? null : RowFilter.compare(right.name, _flip(fop), v);
  }
  return null;
}

/// Logical twin of [_inToRowFilter].
RowFilter? _logicalInToRowFilter(lp.InExpression expr) {
  final column = expr.value;
  if (column is! lp.ColumnExpression) return null;
  final leaves = <RowFilter>[];
  for (final value in expr.list) {
    if (value is! lp.LiteralExpression || value.value == null) return null;
    leaves.add(
      RowFilter.compare(
        column.name,
        expr.negated ? FilterOp.ne : FilterOp.eq,
        value.value,
      ),
    );
  }
  if (leaves.isEmpty) return null;
  return expr.negated ? RowFilter.and(leaves) : RowFilter.or(leaves);
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

FilterOp? _operator(String s) {
  switch (s) {
    case '=':
    case '==':
      return FilterOp.eq;
    case '!=':
    case '<>':
      return FilterOp.ne;
    case '<':
      return FilterOp.lt;
    case '<=':
      return FilterOp.le;
    case '>':
      return FilterOp.gt;
    case '>=':
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
