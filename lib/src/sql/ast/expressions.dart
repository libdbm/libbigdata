import 'base.dart';
import 'statements.dart';

/// Column reference expression
class ColumnExpression extends Expression {
  final String? table;
  final String column;

  ColumnExpression({this.table, required this.column});

  String get qualifiedName => table != null ? '$table.$column' : column;

  @override
  R accept<R>(ExpressionVisitor<R> visitor) =>
      visitor.visitColumnExpression(this);
}

/// Literal value expression
class LiteralExpression extends Expression {
  final dynamic value;

  LiteralExpression(this.value);

  @override
  R accept<R>(ExpressionVisitor<R> visitor) =>
      visitor.visitLiteralExpression(this);
}

/// Binary operator expression
class BinaryExpression extends Expression {
  final Expression left;
  final String operator;
  final Expression right;

  BinaryExpression({
    required this.left,
    required this.operator,
    required this.right,
  });

  @override
  R accept<R>(ExpressionVisitor<R> visitor) =>
      visitor.visitBinaryExpression(this);
}

/// Unary operator expression
class UnaryExpression extends Expression {
  final String operator;
  final Expression operand;

  UnaryExpression({required this.operator, required this.operand});

  @override
  R accept<R>(ExpressionVisitor<R> visitor) =>
      visitor.visitUnaryExpression(this);
}

/// Function call expression
class FunctionExpression extends Expression {
  final String name;
  final List<Expression> arguments;
  final bool distinct;
  final Expression? filter;

  FunctionExpression({
    required this.name,
    this.arguments = const [],
    this.distinct = false,
    this.filter,
  });

  @override
  R accept<R>(ExpressionVisitor<R> visitor) =>
      visitor.visitFunctionExpression(this);
}

/// CASE expression
class CaseExpression extends Expression {
  final Expression? expression;
  final List<WhenClause> whenClauses;
  final Expression? elseExpression;

  CaseExpression({
    this.expression,
    required this.whenClauses,
    this.elseExpression,
  });

  @override
  R accept<R>(ExpressionVisitor<R> visitor) =>
      visitor.visitCaseExpression(this);
}

/// WHEN clause for CASE expression
class WhenClause {
  final Expression condition;
  final Expression result;

  WhenClause({required this.condition, required this.result});
}

/// IN expression
class InExpression extends Expression {
  final Expression expression;
  final List<Expression> values;
  final SelectStatement? subquery;
  final bool negated;

  InExpression({
    required this.expression,
    this.values = const [],
    this.subquery,
    this.negated = false,
  });

  @override
  R accept<R>(ExpressionVisitor<R> visitor) => visitor.visitInExpression(this);
}

/// EXISTS expression
class ExistsExpression extends Expression {
  final SelectStatement subquery;
  final bool negated;

  ExistsExpression({required this.subquery, this.negated = false});

  @override
  R accept<R>(ExpressionVisitor<R> visitor) =>
      visitor.visitExistsExpression(this);
}

/// Subquery expression
class SubqueryExpression extends Expression {
  final SelectStatement subquery;

  SubqueryExpression({required this.subquery});

  @override
  R accept<R>(ExpressionVisitor<R> visitor) =>
      visitor.visitSubqueryExpression(this);
}

/// Star (*) expression
class StarExpression extends Expression {
  @override
  R accept<R>(ExpressionVisitor<R> visitor) =>
      visitor.visitStarExpression(this);
}

/// Qualified star (table.*) expression
class QualifiedStarExpression extends Expression {
  final String table;

  QualifiedStarExpression({required this.table});

  @override
  R accept<R>(ExpressionVisitor<R> visitor) =>
      visitor.visitQualifiedStarExpression(this);
}

/// CAST expression
class CastExpression extends Expression {
  final Expression expression;
  final String targetType;

  CastExpression({required this.expression, required this.targetType});

  @override
  R accept<R>(ExpressionVisitor<R> visitor) =>
      visitor.visitCastExpression(this);
}
