/// Base AST node definitions
abstract class ASTNode {
  R accept<R>(ASTVisitor<R> visitor);
}

/// Base class for SQL statements
abstract class Statement extends ASTNode {}

/// Base class for SQL expressions
abstract class Expression extends ASTNode {}

/// Base class for table references
abstract class TableReference extends ASTNode {}

/// AST visitor interface
abstract class ASTVisitor<R> {
  R visitQueryStatement(covariant Statement statement);
  R visitSelectStatement(covariant Statement statement);
  R visitInsertStatement(covariant Statement statement);
  R visitCreateTableStatement(covariant Statement statement);
  R visitExpression(covariant Expression expression);
  R visitColumnExpression(covariant Expression expression);
  R visitLiteralExpression(covariant Expression expression);
  R visitBinaryExpression(covariant Expression expression);
  R visitUnaryExpression(covariant Expression expression);
  R visitFunctionExpression(covariant Expression expression);
  R visitCaseExpression(covariant Expression expression);
  R visitInExpression(covariant Expression expression);
  R visitBetweenExpression(covariant Expression expression);
  R visitExistsExpression(covariant Expression expression);
  R visitSubqueryExpression(covariant Expression expression);
  R visitCastExpression(covariant Expression expression);
  R visitWindowExpression(covariant Expression expression);
  R visitTableReference(covariant TableReference table);
  R visitSimpleTable(covariant TableReference table);
  R visitJoinedTable(covariant TableReference table);
  R visitSubqueryTable(covariant TableReference table);
  R visitTableFunction(covariant TableReference table);
  R visitValuesTable(covariant TableReference table);
}
