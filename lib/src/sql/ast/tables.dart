import 'base.dart';
import 'statements.dart';

/// Simple table reference
class SimpleTable extends TableReference {
  final String? schema;
  final String table;
  final String? alias;
  final bool isFilePath;

  SimpleTable({
    this.schema,
    required this.table,
    this.alias,
    this.isFilePath = false,
  });

  String get qualifiedName => schema != null ? '$schema.$table' : table;

  @override
  R accept<R>(ASTVisitor<R> visitor) => visitor.visitSimpleTable(this);
}

/// Joined tables
class JoinedTable extends TableReference {
  final TableReference left;
  final List<JoinClause> joins;

  JoinedTable({required this.left, required this.joins});

  @override
  R accept<R>(ASTVisitor<R> visitor) => visitor.visitJoinedTable(this);
}

/// Join clause
class JoinClause {
  final JoinType type;
  final TableReference right;
  final JoinCondition? condition;

  JoinClause({required this.type, required this.right, this.condition});
}

/// Join types
enum JoinType { inner, left, right, full, cross, natural }

/// Base class for join conditions
abstract class JoinCondition {}

/// ON join condition
class OnCondition extends JoinCondition {
  final Expression expression;

  OnCondition(this.expression);
}

/// USING join condition
class UsingCondition extends JoinCondition {
  final List<String> columns;

  UsingCondition(this.columns);
}

/// NATURAL join condition
class NaturalJoin extends JoinCondition {}

/// Subquery as table
class SubqueryTable extends TableReference {
  final SelectStatement subquery;
  final String alias;

  SubqueryTable({required this.subquery, required this.alias});

  @override
  R accept<R>(ASTVisitor<R> visitor) => visitor.visitSubqueryTable(this);
}

/// Table function
class TableFunction extends TableReference {
  final String name;
  final List<Expression> arguments;
  final String? alias;

  TableFunction({required this.name, required this.arguments, this.alias});

  @override
  R accept<R>(ASTVisitor<R> visitor) => visitor.visitTableFunction(this);
}

/// VALUES table
class ValuesTable extends TableReference {
  final List<List<Expression>> values;
  final String? alias;
  final List<String>? columnAliases;

  ValuesTable({required this.values, this.alias, this.columnAliases});

  @override
  R accept<R>(ASTVisitor<R> visitor) => visitor.visitValuesTable(this);
}
