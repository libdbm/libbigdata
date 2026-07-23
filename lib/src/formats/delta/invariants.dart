import 'dart:convert';

import '../../core/schema.dart';
import '../../core/table.dart';

/// A column-level Delta `delta.invariants` constraint: [predicate] is a SQL
/// boolean expression that every row of [column]'s table must satisfy.
class Invariant {
  const Invariant(this.column, this.predicate);

  /// The field whose metadata declared it (for error messages — the predicate
  /// itself may reference any column).
  final String column;

  /// The SQL boolean expression, e.g. `id > 0`.
  final String predicate;
}

/// Evaluates a Delta invariant against data on its way into a table.
///
/// This is an injected seam rather than a direct call because evaluating SQL
/// needs the query engine, which sits ABOVE this layer: `sql/` already imports
/// `formats/` (spill writes Parquet/Arrow), so importing it back from here
/// would make the two modules mutually dependent. `SqlInvariantEvaluator` is
/// the real implementation; pass it to [DeltaWriter].
///
/// A [DeltaWriter] without one REJECTS an invariant-carrying table outright, so
/// the unwired default is safe rather than silently unenforced.
abstract interface class InvariantEvaluator {
  /// How many rows of [table] VIOLATE [predicate].
  ///
  /// SQL `CHECK` semantics under three-valued logic: a row violates only when
  /// the predicate is FALSE. A NULL result is not a violation (it is unknown,
  /// not false), which is why this cannot be `table.rowCount - matches`.
  Future<int> violations(Table table, String predicate);
}

/// The invariants [schema]'s fields declare, in field order.
///
/// Delta stores each one in the field's metadata under `delta.invariants`, as a
/// JSON *string* wrapping the expression:
/// `{"expression":{"expression":"id > 0"}}`. Some writers nest it one level
/// less (`{"expression":"id > 0"}`), so both are accepted.
List<Invariant> parseInvariants(Schema schema) {
  final found = <Invariant>[];
  for (final field in schema.fields) {
    final raw = field.metadata['delta.invariants'];
    if (raw == null) continue;
    found.add(Invariant(field.name, _predicate(field.name, raw)));
  }
  return found;
}

String _predicate(String column, Object? raw) {
  Object? decoded = raw;
  if (raw is String) {
    try {
      decoded = jsonDecode(raw);
    } on FormatException {
      // Not JSON at all. Guessing that it is a bare predicate could enforce
      // something the table never declared, so refuse to interpret it.
      throw FormatException(
        'Column "$column" declares delta.invariants that is not valid JSON: '
        '$raw',
      );
    }
  }
  if (decoded is Map) {
    final expression = decoded['expression'];
    if (expression is String) return expression;
    if (expression is Map && expression['expression'] is String) {
      return expression['expression'] as String;
    }
  }
  throw FormatException(
    'Column "$column" declares delta.invariants in an unrecognized shape '
    '(expected {"expression":{"expression":"<sql>"}}): $raw',
  );
}
