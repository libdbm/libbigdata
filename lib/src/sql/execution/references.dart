import '../ast/ast.dart';

/// Collects the file paths and the names of other tables (candidate views,
/// CTEs or catalog tables) that a query references, from every table and
/// expression position: FROM entries and join ON conditions, the select
/// list, WHERE, GROUP BY, HAVING, ORDER BY, set-operation branches, CTE
/// bodies, VALUES rows, and each subquery nested in an expression.
class References
    implements
        StatementVisitor<void>,
        ExpressionVisitor<void>,
        TableVisitor<void> {
  /// Quoted file paths, in the order first reached.
  final Set<String> files = {};

  /// Names of non-file table references, in the order first reached.
  final Set<String> tables = {};

  void _all(final Iterable<Expression> expressions) {
    for (final expression in expressions) {
      expression.accept(this);
    }
  }

  @override
  void visitQueryStatement(final QueryStatement statement) {
    for (final cte in statement.ctes) {
      cte.query.accept(this);
    }
    statement.select.accept(this);
  }

  @override
  void visitSelectStatement(final SelectStatement statement) {
    for (final table in statement.from) {
      table.accept(this);
    }
    statement.where?.accept(this);
    statement.having?.accept(this);
    for (final operation in statement.operations) {
      operation.right.accept(this);
    }
    _all([for (final item in statement.selectList) item.expression]);
    _all(statement.groupBy);
    _all([for (final item in statement.orderBy) item.expression]);
  }

  @override
  void visitSimpleTable(final SimpleTable table) {
    if (table.isFilePath) {
      files.add(table.table);
    } else {
      tables.add(table.table);
    }
  }

  @override
  void visitJoinedTable(final JoinedTable table) {
    table.left.accept(this);
    for (final join in table.joins) {
      join.right.accept(this);
      if (join.condition case OnCondition(:final expression)) {
        expression.accept(this);
      }
    }
  }

  @override
  void visitSubqueryTable(final SubqueryTable table) =>
      table.subquery.accept(this);

  @override
  void visitValuesTable(final ValuesTable table) {
    for (final row in table.values) {
      _all(row);
    }
  }

  @override
  void visitStarExpression(final StarExpression expression) {}

  @override
  void visitQualifiedStarExpression(final QualifiedStarExpression expression) {}

  @override
  void visitColumnExpression(final ColumnExpression expression) {}

  @override
  void visitLiteralExpression(final LiteralExpression expression) {}

  @override
  void visitBinaryExpression(final BinaryExpression expression) {
    expression.left.accept(this);
    expression.right.accept(this);
  }

  @override
  void visitUnaryExpression(final UnaryExpression expression) =>
      expression.operand.accept(this);

  @override
  void visitFunctionExpression(final FunctionExpression expression) {
    _all(expression.arguments);
    expression.filter?.accept(this);
  }

  @override
  void visitCaseExpression(final CaseExpression expression) {
    expression.expression?.accept(this);
    for (final clause in expression.whenClauses) {
      clause.condition.accept(this);
      clause.result.accept(this);
    }
    expression.elseExpression?.accept(this);
  }

  @override
  void visitInExpression(final InExpression expression) {
    expression.expression.accept(this);
    _all(expression.values);
    expression.subquery?.accept(this);
  }

  @override
  void visitExistsExpression(final ExistsExpression expression) =>
      expression.subquery.accept(this);

  @override
  void visitSubqueryExpression(final SubqueryExpression expression) =>
      expression.subquery.accept(this);

  @override
  void visitCastExpression(final CastExpression expression) =>
      expression.expression.accept(this);
}
