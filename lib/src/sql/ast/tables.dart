import 'base.dart';
import 'statements.dart';

/// Simple table reference
class SimpleTable extends TableReference {
  final String? schema;
  final String table;
  final String? alias;
  final bool isFilePath;

  /// New names for the table's leading columns (`FROM t x(p, q)`).
  final List<String>? columns;

  SimpleTable({
    this.schema,
    required this.table,
    this.alias,
    this.isFilePath = false,
    this.columns,
  });

  String get qualifiedName => schema != null ? '$schema.$table' : table;

  @override
  R accept<R>(TableVisitor<R> visitor) => visitor.visitSimpleTable(this);
}

/// Joined tables
class JoinedTable extends TableReference {
  final TableReference left;
  final List<JoinClause> joins;

  JoinedTable({required this.left, required this.joins});

  @override
  R accept<R>(TableVisitor<R> visitor) => visitor.visitJoinedTable(this);
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

/// Subquery as table
class SubqueryTable extends TableReference {
  final SelectStatement subquery;
  final String alias;

  /// New names for the subquery's leading columns (`FROM (...) v(p, q)`).
  final List<String>? columns;

  SubqueryTable({required this.subquery, required this.alias, this.columns});

  @override
  R accept<R>(TableVisitor<R> visitor) => visitor.visitSubqueryTable(this);
}

/// VALUES table
class ValuesTable extends TableReference {
  final List<List<Expression>> values;
  final String? alias;
  final List<String>? columnAliases;

  ValuesTable({required this.values, this.alias, this.columnAliases});

  @override
  R accept<R>(TableVisitor<R> visitor) => visitor.visitValuesTable(this);
}
