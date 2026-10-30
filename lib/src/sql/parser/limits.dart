import '../ast/ast.dart';

/// The deepest expression tree a statement may hold. The planner and the
/// physical builder recurse over expressions, and a long chain of binary
/// operators (`1 + 1 + ...`, `a AND b AND ...`) parses into a tree as deep as
/// the chain is long; past about 7,500 levels that recursion overflows the
/// stack. The limit leaves headroom for terms that are themselves deep.
const maxExpressionDepth = 4096;

/// The most branches one chain of EXCEPT and INTERSECT may join. Each of these
/// operators nests the plan one level (a UNION run does not: it is planned as
/// one n-ary union), and the chain overflows the stack past about 5,000.
const maxSetBranches = 2048;

/// Throws a [FormatException] when [node] (a parsed statement or expression)
/// holds an expression deeper than [maxExpressionDepth], or a SELECT whose
/// EXCEPT/INTERSECT chain joins more than [maxSetBranches] branches. Walks the
/// tree with an explicit stack, so the check cannot overflow itself.
void checkLimits(Object node) {
  final stack = <(Object, int)>[(node, 0)];
  while (stack.isNotEmpty) {
    final (current, outer) = stack.removeLast();
    final depth = current is Expression ? outer + 1 : outer;
    if (depth > maxExpressionDepth) {
      throw FormatException(
        'SQL expression nests deeper than $maxExpressionDepth levels',
      );
    }
    if (current is SelectStatement) _checkBranches(current);
    for (final child in _children(current)) {
      stack.add((child, depth));
    }
  }
}

void _checkBranches(SelectStatement select) {
  var branches = 1;
  for (final operation in select.operations) {
    if (operation.type == SetOperationType.except ||
        operation.type == SetOperationType.intersect) {
      branches++;
    }
  }
  if (branches > maxSetBranches) {
    throw FormatException(
      'An EXCEPT/INTERSECT chain joins $branches branches '
      '(limit $maxSetBranches)',
    );
  }
}

/// The nodes directly under [node] that hold expressions or statements.
Iterable<Object> _children(Object node) sync* {
  switch (node) {
    case BinaryExpression(:final left, :final right):
      yield left;
      yield right;
    case UnaryExpression(:final operand):
      yield operand;
    case FunctionExpression(:final arguments, :final filter):
      yield* arguments;
      if (filter != null) yield filter;
    case CaseExpression(:final expression, :final whenClauses):
      if (expression != null) yield expression;
      for (final clause in whenClauses) {
        yield clause.condition;
        yield clause.result;
      }
      if (node.elseExpression case final fallback?) yield fallback;
    case InExpression(:final expression, :final values, :final subquery):
      yield expression;
      yield* values;
      if (subquery != null) yield subquery;
    case ExistsExpression(:final subquery):
      yield subquery;
    case SubqueryExpression(:final subquery):
      yield subquery;
    case CastExpression(:final expression):
      yield expression;
    case QueryStatement(:final ctes, :final select):
      for (final cte in ctes) {
        yield cte.query;
      }
      yield select;
    case SelectStatement():
      for (final item in node.selectList) {
        yield item.expression;
      }
      yield* node.from;
      if (node.where case final where?) yield where;
      yield* node.groupBy;
      if (node.having case final having?) yield having;
      for (final item in node.orderBy) {
        yield item.expression;
      }
      for (final operation in node.operations) {
        yield operation.right;
      }
    case JoinedTable(:final left, :final joins):
      yield left;
      for (final join in joins) {
        yield join.right;
        if (join.condition case OnCondition(:final expression)) {
          yield expression;
        }
      }
    case SubqueryTable(:final subquery):
      yield subquery;
    case ValuesTable(:final values):
      for (final row in values) {
        yield* row;
      }
    case InsertStatement(:final values):
      for (final row in values) {
        yield* row;
      }
    case UpdateStatement(:final assignments, :final where):
      for (final assignment in assignments) {
        yield assignment.value;
      }
      if (where != null) yield where;
    case MergeStatement(:final sourceQuery):
      if (sourceQuery != null) yield sourceQuery;
    case CreateViewStatement(:final query):
      yield query;
  }
}
