import '../../core/failure.dart';
import '../../core/result.dart';
import '../../core/table.dart';
import '../../formats/delta/invariants.dart';
import '../ast/ast.dart';
import '../parser/sql_parser.dart';
import 'sql_engine.dart';

/// [InvariantEvaluator] backed by the real query engine, so a Delta invariant is
/// evaluated with exactly the semantics a query would give it: the same parser,
/// the same expression evaluator, the same three-valued logic, rather than a
/// second, weaker implementation that could drift.
///
/// Lives here, not in `formats/`, because `sql/` may depend on `formats/` but
/// not the reverse; [DeltaWriter] takes this as an injected boundary.
final class SqlInvariantEvaluator implements InvariantEvaluator {
  const SqlInvariantEvaluator();

  /// The table name the predicate is evaluated against. Delta invariants
  /// reference bare column names, so the name only has to not collide with one.
  static const _relation = 'delta_invariant_target';

  @override
  Future<Result<int, ErrorDetails>> violations(Table table, String predicate) =>
      guard(() => _violations(table, predicate));

  Future<Result<int, ErrorDetails>> _violations(
    Table table,
    String predicate,
  ) async {
    // The predicate comes from table metadata, so it is untrusted. It must be
    // one complete expression (so it cannot close the parenthesis it is
    // placed in) over the row's columns alone: no subqueries, which are the
    // only way an expression reaches another table or a file.
    final Expression parsed;
    try {
      parsed = SqlParser().parseExpression(predicate);
    } on FormatException catch (error) {
      return Err(_invalid(predicate, error.message));
    }
    if (!_scalar(parsed)) {
      return Err(_invalid(predicate, 'only column predicates are allowed'));
    }
    final SqlEngine engine;
    switch (SqlEngine.open(readOnly: true)) {
      case Ok(:final value):
        engine = value;
      case Err(:final error):
        return Err(error);
    }
    try {
      engine.registerTable(_relation, table);
      // NOT(predicate) counts only rows where the predicate is definitively
      // FALSE: under 3VL, NOT(NULL) is NULL and WHERE drops it. That IS the SQL
      // CHECK rule (an unknown result does not violate a constraint) so the
      // engine's own null handling gives the right answer for free.
      final result = await engine.execute(
        'SELECT COUNT(*) AS n FROM $_relation WHERE NOT ($predicate)',
      );
      return switch (result) {
        Ok(:final value) => Ok(value.column('n')!.toList().first as int),
        // Whatever the engine reports, the invariant itself is what is wrong.
        Err(:final error) => Err(_invalid(predicate, error.message, error)),
      };
    } finally {
      await engine.dispose();
    }
  }

  static ErrorDetails _invalid(
    String predicate,
    String reason, [
    Object? cause,
  ]) => ErrorDetails(
    Kind.invalid,
    'Delta invariant "$predicate" cannot be evaluated: $reason',
    cause,
  );

  /// Whether [expression] reads only the current row: columns, literals,
  /// operators, functions, CASE, CAST and value lists. Anything else, and any
  /// node this does not know, is refused.
  static bool _scalar(Expression expression) => switch (expression) {
    ColumnExpression() || LiteralExpression() => true,
    BinaryExpression(:final left, :final right) =>
      _scalar(left) && _scalar(right),
    UnaryExpression(:final operand) => _scalar(operand),
    FunctionExpression(:final arguments, :final filter) =>
      (filter == null || _scalar(filter)) && arguments.every(_scalar),
    CaseExpression(
      :final expression,
      :final whenClauses,
      :final elseExpression,
    ) =>
      (expression == null || _scalar(expression)) &&
          whenClauses.every(
            (clause) => _scalar(clause.condition) && _scalar(clause.result),
          ) &&
          (elseExpression == null || _scalar(elseExpression)),
    InExpression(:final expression, :final values, :final subquery) =>
      subquery == null && _scalar(expression) && values.every(_scalar),
    CastExpression(:final expression) => _scalar(expression),
    _ => false,
  };
}
