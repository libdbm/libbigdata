part of 'query_planner.dart';

/// FROM joins: comma-list folding with equi-keys taken from WHERE.
extension _Joins on LogicalPlanBuilder {
  /// Fold a comma-list FROM into joins, pulling each WHERE conjunct of the
  /// form `l = r`, where one column resolves only in the already-folded left
  /// side and the other only in the entry being joined: into that join's
  /// condition (the physical layer then hash-joins on it). Conjuncts that
  /// don't match stay in the returned residual WHERE, so anything uncertain
  /// (ambiguous names, expressions, non-equalities) keeps its exact previous
  /// filter-over-cross-join behavior.
  ({logical.LogicalPlan plan, ast.Expression? residual}) _foldCommaJoin(
    ast.SelectStatement node,
    List<logical.LogicalPlan> entries,
    ast.Expression where,
  ) {
    String? qualifier(ast.TableReference f) =>
        f is ast.SimpleTable ? LogicalPlanBuilder._qualifier(f) : null;
    final quals = [for (final f in node.from) qualifier(f)];
    final names = [
      for (final e in entries) {for (final f in e.schema.fields) f.name},
    ];

    final conjuncts = _andConjuncts(where);
    final used = <ast.Expression>{};
    var plan = entries.first;
    final leftNames = {...names.first};
    final leftQualifiers = {if (quals.first != null) quals.first!};
    for (int i = 1; i < entries.length; i++) {
      logical.LogicalExpression? condition;
      for (final c in conjuncts) {
        if (used.contains(c)) continue;
        if (!_joinsSides(c, leftNames, leftQualifiers, names[i], quals[i])) {
          continue;
        }
        used.add(c);
        final eq = _convertExpression(c);
        condition = condition == null
            ? eq
            : logical.BinaryExpression(
                condition,
                logical.BinaryOperator.and,
                eq,
              );
      }
      plan = logical.Join(
        left: plan,
        right: entries[i],
        joinType: condition == null
            ? logical.JoinType.cross
            : logical.JoinType.inner,
        condition: condition,
      );
      leftNames.addAll(names[i]);
      final q = quals[i];
      if (q != null) leftQualifiers.add(q);
    }
    final rest = [
      for (final c in conjuncts)
        if (!used.contains(c)) c,
    ];
    return (
      plan: plan,
      residual: rest.isEmpty
          ? null
          : rest.reduce(
              (a, b) =>
                  ast.BinaryExpression(left: a, operator: 'AND', right: b),
            ),
    );
  }

  /// Whether [c] is a `column = column` equality whose sides land on OPPOSITE
  /// sides of the current fold step. A qualified column resolves by its
  /// qualifier; an unqualified one must appear in exactly one side's schema.
  bool _joinsSides(
    ast.Expression c,
    Set<String> leftNames,
    Set<String> leftQualifiers,
    Set<String> rightNames,
    String? rightQualifier,
  ) {
    if (c is! ast.BinaryExpression || c.operator != '=') return false;
    final l = c.left;
    final r = c.right;
    if (l is! ast.ColumnExpression || r is! ast.ColumnExpression) return false;
    int? side(ast.ColumnExpression e) {
      final t = e.table;
      if (t != null) {
        if (t == rightQualifier) return 1;
        if (leftQualifiers.contains(t)) return 0;
        return null; // unknown qualifier: leave it to normal binding
      }
      final inLeft = leftNames.contains(e.column);
      final inRight = rightNames.contains(e.column);
      if (inLeft == inRight) return null; // absent, or ambiguous across sides
      return inLeft ? 0 : 1;
    }

    final leftSide = side(l);
    final rightSide = side(r);
    return leftSide != null && rightSide != null && leftSide != rightSide;
  }
}
