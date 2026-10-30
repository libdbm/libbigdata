import 'expressions.dart';
import 'statements.dart';
import 'tables.dart';

/// Base class for SQL statements. Queries ([QueryStatement],
/// [SelectStatement]) accept a [StatementVisitor]; DML and DDL statements are
/// executed directly by the engine and accept no visitor.
abstract class Statement {}

/// Base class for SQL expressions
abstract class Expression {
  R accept<R>(ExpressionVisitor<R> visitor);
}

/// Base class for table references
abstract class TableReference {
  R accept<R>(TableVisitor<R> visitor);
}

/// Visitor over query statements
abstract interface class StatementVisitor<R> {
  R visitQueryStatement(QueryStatement statement);
  R visitSelectStatement(SelectStatement statement);
}

/// Visitor over expressions
abstract interface class ExpressionVisitor<R> {
  R visitStarExpression(StarExpression expression);
  R visitQualifiedStarExpression(QualifiedStarExpression expression);
  R visitColumnExpression(ColumnExpression expression);
  R visitLiteralExpression(LiteralExpression expression);
  R visitBinaryExpression(BinaryExpression expression);
  R visitUnaryExpression(UnaryExpression expression);
  R visitFunctionExpression(FunctionExpression expression);
  R visitCaseExpression(CaseExpression expression);
  R visitInExpression(InExpression expression);
  R visitExistsExpression(ExistsExpression expression);
  R visitSubqueryExpression(SubqueryExpression expression);
  R visitCastExpression(CastExpression expression);
}

/// Visitor over table references
abstract interface class TableVisitor<R> {
  R visitSimpleTable(SimpleTable table);
  R visitJoinedTable(JoinedTable table);
  R visitSubqueryTable(SubqueryTable table);
  R visitValuesTable(ValuesTable table);
}
