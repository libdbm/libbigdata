import 'base.dart';
import 'expressions.dart';

/// Query statement with CTEs
class QueryStatement extends Statement {
  final List<CommonTableExpression> ctes;
  final SelectStatement select;

  QueryStatement({this.ctes = const [], required this.select});

  R accept<R>(StatementVisitor<R> visitor) => visitor.visitQueryStatement(this);
}

/// Common Table Expression (WITH clause)
class CommonTableExpression {
  final String name;
  final List<String> columns;
  final SelectStatement query;
  final bool recursive;

  CommonTableExpression({
    required this.name,
    this.columns = const [],
    required this.query,
    this.recursive = false,
  });
}

/// SELECT statement
class SelectStatement extends Statement {
  final bool distinct;
  final List<SelectItem> selectList;
  final List<TableReference> from;
  final Expression? where;
  final List<Expression> groupBy;
  final Expression? having;
  final List<OrderByItem> orderBy;
  final int? limit;
  final int? offset;

  /// Set-operation tails applied to this core, LEFT to RIGHT. Each tail's `right`
  /// is a select-core; the planner folds them left-associatively with INTERSECT
  /// binding tighter than UNION/EXCEPT, then applies [orderBy]/[limit] over the
  /// whole result. Empty for a plain SELECT.
  final List<SetOperation> operations;

  SelectStatement({
    this.distinct = false,
    required this.selectList,
    this.from = const [],
    this.where,
    this.groupBy = const [],
    this.having,
    this.orderBy = const [],
    this.limit,
    this.offset,
    this.operations = const [],
  });

  R accept<R>(StatementVisitor<R> visitor) =>
      visitor.visitSelectStatement(this);
}

/// INSERT statement appending literal rows to a Delta table path. Executed
/// directly by the engine (DML), so it is never lowered through the
/// planner/visitor.
///
/// [columns] is empty when no column list was given, meaning the rows are
/// positional over the target's full schema.
class InsertStatement extends Statement {
  final String targetPath; // quoted file path to a Delta table
  final List<String> columns;
  final List<List<Expression>> values;

  InsertStatement({
    required this.targetPath,
    this.columns = const [],
    required this.values,
  });
}

/// UPDATE statement targeting a Delta table path. Executed directly by the
/// engine (DML), so it is never lowered through the planner/visitor.
class UpdateStatement extends Statement {
  final String targetPath; // quoted file path to a Delta table
  final List<UpdateAssignment> assignments;
  final Expression? where;

  UpdateStatement({
    required this.targetPath,
    required this.assignments,
    this.where,
  });
}

class UpdateAssignment {
  final String column;
  final Expression value;
  UpdateAssignment(this.column, this.value);
}

/// MERGE INTO statement (upsert) targeting a Delta table path. Subset:
/// `WHEN MATCHED THEN UPDATE SET * | DELETE` and `WHEN NOT MATCHED THEN INSERT *`.
class MergeStatement extends Statement {
  final String targetPath;
  final String? sourcePath; // file-path source, or
  final SelectStatement? sourceQuery; // subquery source
  final List<String> onKeys; // equi-key column names, on the target
  final List<String> sourceKeys; // the same keys' names on the source
  final bool matchedUpdateAll;
  final bool matchedDelete;
  final bool notMatchedInsert;

  MergeStatement({
    required this.targetPath,
    this.sourcePath,
    this.sourceQuery,
    required this.onKeys,
    List<String>? sourceKeys,
    this.matchedUpdateAll = false,
    this.matchedDelete = false,
    this.notMatchedInsert = false,
  }) : sourceKeys = sourceKeys ?? onKeys;
}

/// CREATE VIEW statement. A view is a persisted named query, when referenced it
/// materializes like a CTE. Executed directly by the engine (DDL).
class CreateViewStatement extends Statement {
  final String name;
  final List<String> columns;
  final SelectStatement query;
  final bool orReplace;
  final bool ifNotExists;

  CreateViewStatement({
    required this.name,
    this.columns = const [],
    required this.query,
    this.orReplace = false,
    this.ifNotExists = false,
  });
}

/// DROP VIEW statement. Executed directly by the engine (DDL).
class DropViewStatement extends Statement {
  final String name;
  final bool ifExists;

  DropViewStatement({required this.name, this.ifExists = false});
}

/// SELECT list item
class SelectItem {
  final Expression expression;
  final String? alias;

  SelectItem({required this.expression, this.alias});
}

/// Star (*) select item
class StarSelectItem extends SelectItem {
  StarSelectItem() : super(expression: StarExpression());
}

/// ORDER BY item
class OrderByItem {
  final Expression expression;
  final bool ascending;
  final bool nullsFirst;

  OrderByItem({
    required this.expression,
    this.ascending = true,
    this.nullsFirst = false,
  });
}

/// Set operation types
enum SetOperationType { union, unionAll, intersect, except }

/// Set operation for combining queries
class SetOperation {
  final SetOperationType type;
  final SelectStatement right;

  SetOperation({required this.type, required this.right});
}
