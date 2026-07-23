import '../../core/table.dart';
import '../../formats/delta/invariants.dart';
import 'sql_engine.dart';

/// [InvariantEvaluator] backed by the real query engine, so a Delta invariant is
/// evaluated with exactly the semantics a query would give it — the same parser,
/// the same expression evaluator, the same three-valued logic — rather than a
/// second, weaker implementation that could drift.
///
/// Lives here, not in `formats/`, because `sql/` may depend on `formats/` but
/// not the reverse; [DeltaWriter] takes this as an injected seam.
class SqlInvariantEvaluator implements InvariantEvaluator {
  const SqlInvariantEvaluator();

  /// The table name the predicate is evaluated against. Delta invariants
  /// reference bare column names, so the name only has to not collide with one.
  static const _relation = 'delta_invariant_target';

  @override
  Future<int> violations(Table table, String predicate) async {
    final engine = SqlEngine();
    try {
      engine.registerTable(_relation, table);
      // NOT(predicate) counts only rows where the predicate is definitively
      // FALSE: under 3VL, NOT(NULL) is NULL and WHERE drops it. That IS the SQL
      // CHECK rule — an unknown result does not violate a constraint — so the
      // engine's own null handling gives the right answer for free.
      final result = await engine.execute(
        'SELECT COUNT(*) AS n FROM $_relation WHERE NOT ($predicate)',
      );
      return result.column('n')!.toList().first as int;
    } finally {
      await engine.dispose();
    }
  }
}
