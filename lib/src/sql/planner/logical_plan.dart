import '../../core/schema.dart';
import '../../core/data_type.dart';
import '../../core/row_filter.dart';

/// Logical plan nodes represent the logical operations in a query
abstract class LogicalPlan {
  /// Schema that this plan produces
  Schema get schema;

  /// Child plans
  List<LogicalPlan> get children;

  /// Accept visitor pattern
  R accept<R>(LogicalPlanVisitor<R> visitor);
}

/// Table scan - read from a table
class TableScan extends LogicalPlan {
  final String tableName;
  final String? alias;
  final Schema tableSchema;
  List<String>?
  projection; // Optional column projection (set by PushDownProjection)
  RowFilter? pushedFilter; // Row-group pruning filter (set by PushDownFilter)

  TableScan({
    required this.tableName,
    this.alias,
    required this.tableSchema,
    this.projection,
    this.pushedFilter,
  });

  @override
  Schema get schema =>
      projection != null ? tableSchema.select(projection!) : tableSchema;

  @override
  List<LogicalPlan> get children => [];

  @override
  R accept<R>(LogicalPlanVisitor<R> visitor) => visitor.visitTableScan(this);
}

/// Projection - SELECT columns
class Projection extends LogicalPlan {
  final LogicalPlan input;
  final List<LogicalExpression> expressions;
  final List<String> aliases;

  Projection({
    required this.input,
    required this.expressions,
    required this.aliases,
  });

  @override
  Schema get schema {
    final fields = <Field>[];
    for (int i = 0; i < expressions.length; i++) {
      fields.add(
        Field(
          name: aliases[i],
          dataType: expressions[i].dataType,
          nullable: expressions[i].nullable,
        ),
      );
    }
    return Schema(fields: fields);
  }

  @override
  List<LogicalPlan> get children => [input];

  @override
  R accept<R>(LogicalPlanVisitor<R> visitor) => visitor.visitProjection(this);
}

/// Filter - WHERE clause
class Filter extends LogicalPlan {
  final LogicalPlan input;
  final LogicalExpression predicate;

  Filter({required this.input, required this.predicate});

  @override
  Schema get schema => input.schema;

  @override
  List<LogicalPlan> get children => [input];

  @override
  R accept<R>(LogicalPlanVisitor<R> visitor) => visitor.visitFilter(this);
}

/// Join operations
class Join extends LogicalPlan {
  final LogicalPlan left;
  final LogicalPlan right;
  final JoinType joinType;
  final LogicalExpression? condition;
  final List<String>? usingColumns;

  Join({
    required this.left,
    required this.right,
    required this.joinType,
    this.condition,
    this.usingColumns,
  });

  @override
  Schema get schema {
    // A semi/anti join filters the left side; the right contributes no column.
    if (semiLike(joinType)) return left.schema;

    // Combine schemas from both sides
    final leftSchema = left.schema;
    final rightSchema = right.schema;

    final fields = <Field>[];
    fields.addAll(leftSchema.fields);

    // For USING clause, don't duplicate columns
    if (usingColumns != null) {
      for (final field in rightSchema.fields) {
        if (!usingColumns!.contains(field.name)) {
          fields.add(field);
        }
      }
    } else {
      fields.addAll(rightSchema.fields);
    }

    return Schema(fields: fields);
  }

  @override
  List<LogicalPlan> get children => [left, right];

  @override
  R accept<R>(LogicalPlanVisitor<R> visitor) => visitor.visitJoin(this);
}

/// How a join pairs its two inputs. The last three emit LEFT COLUMNS ONLY — they
/// filter the left side by whether the right has a match, rather than combining
/// the two.
///
/// [anti] and [antiNullAware] differ ONLY in their null handling, and the
/// difference is not cosmetic: `NOT EXISTS` counts rows and is indifferent to
/// nulls, while `x NOT IN (S)` is UNKNOWN for EVERY row when S holds a null.
/// They are separate values, rather than a flag, so every switch on this enum is
/// forced to answer for both.
enum JoinType {
  inner,
  left,
  right,
  full,
  cross,
  semi, // EXISTS, IN (SELECT …)
  anti, // NOT EXISTS
  antiNullAware, // NOT IN (SELECT …)
}

/// Whether [joinType] emits LEFT COLUMNS ONLY, filtering the left side by
/// whether the right matched rather than pairing the two. One definition, so no
/// site has to re-list the family and miss one.
bool semiLike(JoinType joinType) =>
    joinType == JoinType.semi ||
    joinType == JoinType.anti ||
    joinType == JoinType.antiNullAware;

/// Aggregation - GROUP BY
class Aggregate extends LogicalPlan {
  final LogicalPlan input;
  final List<LogicalExpression> groupBy;
  final List<AggregateExpression> aggregates;
  final List<String> aliases;

  Aggregate({
    required this.input,
    required this.groupBy,
    required this.aggregates,
    required this.aliases,
  });

  @override
  Schema get schema {
    final fields = <Field>[];

    // Add grouping columns. An EXPRESSION key (`GROUP BY k % 2`) is emitted by
    // the physical aggregate as `group$i` — dropping it here left this schema
    // short a column, so a set-op branch over `SELECT * … GROUP BY <expr>`
    // failed its arity check on a valid query. Names must match the planner's
    // _groupKeyName / the physical aggregate's _groupName.
    for (int i = 0; i < groupBy.length; i++) {
      final expr = groupBy[i];
      fields.add(
        Field(
          name: expr is ColumnExpression ? expr.name : 'group$i',
          dataType: expr.dataType,
          nullable: expr.nullable,
        ),
      );
    }

    // Add aggregate columns
    for (int i = 0; i < aggregates.length; i++) {
      fields.add(
        Field(
          name: aliases[i],
          dataType: aggregates[i].dataType,
          nullable: aggregates[i].nullable,
        ),
      );
    }

    return Schema(fields: fields);
  }

  @override
  List<LogicalPlan> get children => [input];

  @override
  R accept<R>(LogicalPlanVisitor<R> visitor) => visitor.visitAggregate(this);
}

/// Sort - ORDER BY
class Sort extends LogicalPlan {
  final LogicalPlan input;
  final List<SortExpression> sortExpressions;

  Sort({required this.input, required this.sortExpressions});

  @override
  Schema get schema => input.schema;

  @override
  List<LogicalPlan> get children => [input];

  @override
  R accept<R>(LogicalPlanVisitor<R> visitor) => visitor.visitSort(this);
}

class SortExpression {
  final LogicalExpression expression;
  final bool ascending;
  final bool nullsFirst;

  SortExpression({
    required this.expression,
    this.ascending = true,
    this.nullsFirst = false,
  });
}

/// Limit - LIMIT/OFFSET
class Limit extends LogicalPlan {
  final LogicalPlan input;
  final int? limit;
  final int? offset;

  Limit({required this.input, this.limit, this.offset});

  @override
  Schema get schema => input.schema;

  @override
  List<LogicalPlan> get children => [input];

  @override
  R accept<R>(LogicalPlanVisitor<R> visitor) => visitor.visitLimit(this);
}

/// Set operations - UNION, INTERSECT, EXCEPT
class SetOperation extends LogicalPlan {
  final LogicalPlan left;
  final LogicalPlan right;
  final SetOpType type;
  final bool all; // UNION ALL vs UNION

  SetOperation({
    required this.left,
    required this.right,
    required this.type,
    this.all = false,
  });

  @override
  Schema get schema => left.schema; // Schemas must match

  @override
  List<LogicalPlan> get children => [left, right];

  @override
  R accept<R>(LogicalPlanVisitor<R> visitor) => visitor.visitSetOperation(this);
}

enum SetOpType { union, intersect, except }

/// Window operations
class Window extends LogicalPlan {
  final LogicalPlan input;
  final List<WindowExpression> windowExpressions;
  final List<String> aliases;

  Window({
    required this.input,
    required this.windowExpressions,
    required this.aliases,
  });

  @override
  Schema get schema {
    final inputSchema = input.schema;
    final fields = List<Field>.from(inputSchema.fields);

    // Add window function columns
    for (int i = 0; i < windowExpressions.length; i++) {
      fields.add(
        Field(
          name: aliases[i],
          dataType: windowExpressions[i].dataType,
          nullable: true,
        ),
      );
    }

    return Schema(fields: fields);
  }

  @override
  List<LogicalPlan> get children => [input];

  @override
  R accept<R>(LogicalPlanVisitor<R> visitor) => visitor.visitWindow(this);
}

class WindowExpression {
  final AggregateFunction function;
  final List<LogicalExpression> partitionBy;
  final List<SortExpression> orderBy;
  final WindowFrame? frame;

  WindowExpression({
    required this.function,
    this.partitionBy = const [],
    this.orderBy = const [],
    this.frame,
  });

  DataType get dataType => function.dataType;
}

class WindowFrame {
  final WindowFrameType type;
  final WindowFrameBound start;
  final WindowFrameBound end;

  WindowFrame({required this.type, required this.start, required this.end});
}

enum WindowFrameType { rows, range, groups }

class WindowFrameBound {
  final WindowBoundType type;
  final int? offset;

  WindowFrameBound(this.type, [this.offset]);
}

enum WindowBoundType {
  unboundedPreceding,
  preceding,
  currentRow,
  following,
  unboundedFollowing,
}

/// Distinct - Remove duplicates
class Distinct extends LogicalPlan {
  final LogicalPlan input;

  Distinct(this.input);

  @override
  Schema get schema => input.schema;

  @override
  List<LogicalPlan> get children => [input];

  @override
  R accept<R>(LogicalPlanVisitor<R> visitor) => visitor.visitDistinct(this);
}

/// A derived relation (subquery in FROM, or a future VIEW) referenced under an
/// [alias]: its output columns are re-qualified with [alias] so the outer query
/// can address them as `alias.col`, exactly like a table/file scan.
class AliasScope extends LogicalPlan {
  final String alias;
  final LogicalPlan input;

  AliasScope({required this.alias, required this.input});

  @override
  Schema get schema => input.schema.qualify(alias);

  @override
  List<LogicalPlan> get children => [input];

  @override
  R accept<R>(LogicalPlanVisitor<R> visitor) => visitor.visitAliasScope(this);
}

/// Values - VALUES clause
class Values extends LogicalPlan {
  final List<List<LogicalExpression>> rows;
  final Schema valueSchema;

  Values({required this.rows, required this.valueSchema});

  @override
  Schema get schema => valueSchema;

  @override
  List<LogicalPlan> get children => [];

  @override
  R accept<R>(LogicalPlanVisitor<R> visitor) => visitor.visitValues(this);
}

/// Logical expressions
abstract class LogicalExpression {
  DataType get dataType;
  bool get nullable;
}

/// Column reference
class ColumnExpression extends LogicalExpression {
  final String name;
  final String? table;
  final DataType columnType;
  final bool isNullable;

  ColumnExpression({
    required this.name,
    this.table,
    required this.columnType,
    this.isNullable = true,
  });

  @override
  DataType get dataType => columnType;

  @override
  bool get nullable => isNullable;
}

/// Literal value
class LiteralExpression extends LogicalExpression {
  final dynamic value;
  final DataType literalType;

  LiteralExpression(this.value, this.literalType);

  @override
  DataType get dataType => literalType;

  @override
  bool get nullable => value == null;
}

/// `value [NOT] IN (list)`.
///
/// Kept as its own node rather than desugared to an OR-chain of equalities:
/// the chain re-scans the batch once per list element, and a constant list can
/// instead be probed as a set in a single pass.
class InExpression extends LogicalExpression {
  final LogicalExpression value;
  final List<LogicalExpression> list;
  final bool negated;

  InExpression({
    required this.value,
    required this.list,
    required this.negated,
  });

  @override
  DataType get dataType => DataType.boolean;

  /// A NULL anywhere — probe or list — makes the result UNKNOWN.
  @override
  bool get nullable => true;
}

/// Binary operation
class BinaryExpression extends LogicalExpression {
  final LogicalExpression left;
  final BinaryOperator operator;
  final LogicalExpression right;

  BinaryExpression(this.left, this.operator, this.right);

  @override
  DataType get dataType {
    switch (operator) {
      case BinaryOperator.and:
      case BinaryOperator.or:
      case BinaryOperator.eq:
      case BinaryOperator.neq:
      case BinaryOperator.lt:
      case BinaryOperator.lte:
      case BinaryOperator.gt:
      case BinaryOperator.gte:
      case BinaryOperator.like:
      case BinaryOperator.notLike:
      case BinaryOperator.isNull:
      case BinaryOperator.isNotNull:
        return DataType.boolean;
      default:
        // For arithmetic ops, return the wider type
        return _widerType(left.dataType, right.dataType);
    }
  }

  @override
  bool get nullable => left.nullable || right.nullable;

  DataType _widerType(DataType a, DataType b) {
    // Simple type promotion rules
    if (a == DataType.float64 || b == DataType.float64) return DataType.float64;
    if (a == DataType.float32 || b == DataType.float32) return DataType.float32;
    if (a == DataType.int64 || b == DataType.int64) return DataType.int64;
    if (a == DataType.int32 || b == DataType.int32) return DataType.int32;
    return a;
  }
}

enum BinaryOperator {
  // Arithmetic
  add,
  subtract,
  multiply,
  divide,
  modulo,
  // Comparison
  eq,
  neq,
  lt,
  lte,
  gt,
  gte,
  // Logical
  and,
  or,
  // String
  like,
  notLike,
  // Null test (right operand ignored)
  isNull,
  isNotNull,
}

/// Aggregate expressions
class AggregateExpression extends LogicalExpression {
  final AggregateFunction function;
  final LogicalExpression? argument;
  final bool distinct;

  AggregateExpression({
    required this.function,
    this.argument,
    this.distinct = false,
  });

  @override
  DataType get dataType => function.dataType;

  @override
  bool get nullable => false; // Aggregates typically return non-null
}

class AggregateFunction {
  final String name;
  final DataType dataType;

  AggregateFunction(this.name, this.dataType);

  static final count = AggregateFunction('COUNT', DataType.int64);
  static final sum = AggregateFunction('SUM', DataType.float64);
  static final avg = AggregateFunction('AVG', DataType.float64);
  static final min = AggregateFunction('MIN', DataType.float64);
  static final max = AggregateFunction('MAX', DataType.float64);
}

/// Function call
class FunctionExpression extends LogicalExpression {
  final String name;
  final List<LogicalExpression> arguments;
  final DataType returnType;

  FunctionExpression({
    required this.name,
    required this.arguments,
    required this.returnType,
  });

  @override
  DataType get dataType => returnType;

  @override
  bool get nullable => true;
}

/// CASE expression
class CaseExpression extends LogicalExpression {
  final List<WhenClause> whenClauses;
  final LogicalExpression? elseExpression;

  CaseExpression({required this.whenClauses, this.elseExpression});

  @override
  DataType get dataType => whenClauses.first.result.dataType;

  @override
  bool get nullable => elseExpression?.nullable ?? true;
}

class WhenClause {
  final LogicalExpression condition;
  final LogicalExpression result;

  WhenClause(this.condition, this.result);
}

/// Cast expression
class CastExpression extends LogicalExpression {
  final LogicalExpression expression;
  final DataType targetType;

  /// Explicit `DECIMAL(p,s)` parameters; null for every other target (and for
  /// bare `DECIMAL`, where a decimal source keeps its own scale).
  final int? precision;
  final int? scale;

  CastExpression(
    this.expression,
    this.targetType, {
    this.precision,
    this.scale,
  });

  @override
  DataType get dataType => targetType;

  @override
  bool get nullable => expression.nullable;
}

/// Logical plan visitor
abstract class LogicalPlanVisitor<R> {
  R visitTableScan(TableScan node);
  R visitProjection(Projection node);
  R visitFilter(Filter node);
  R visitJoin(Join node);
  R visitAggregate(Aggregate node);
  R visitSort(Sort node);
  R visitLimit(Limit node);
  R visitSetOperation(SetOperation node);
  R visitWindow(Window node);
  R visitDistinct(Distinct node);
  R visitValues(Values node);
  R visitAliasScope(AliasScope node);
}
