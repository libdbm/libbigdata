part of 'query_planner.dart';

/// Pushes WHERE conjuncts down to the table scans they provably belong to as a
/// row-group pruning [RowFilter]. The [logical.Filter] node is left in place
/// (the exact predicate is re-applied), so pruning only ever over-keeps: joins,
/// outer/null-producing sides, and ambiguous columns are all safe. Runs before
/// [PushDownProjection] so each scan's full `tableSchema` is available for
/// column provenance.
///
/// Only the scans beneath a filter can own its columns. A scan in another
/// set-operation branch, or in an enclosing query, is a different relation
/// even when it reads the same file, so it never receives the filter. A
/// filter does not reach through a [logical.Limit] or a [logical.Sample]
/// (pruning would change the rows they keep), a projection that computes or renames a column (the
/// filter's name may then mean a different scan column) or a set operation
/// (its branches are matched by position, not name). Through a projection it
/// reaches only the columns the projection keeps, and through an aggregate
/// only its column group keys: any other name there is an output of the
/// aggregate (`SUM(salary) AS salary`), not the scan's column. A subquery
/// is a boundary it does not cross; a registered DataFrame ([logical.Frame])
/// is not, so its filters and a query's reach its scans.
class PushDownFilter {
  logical.LogicalPlan optimize(logical.LogicalPlan plan) {
    _push(plan);
    return plan;
  }

  /// Collect into [out] each scan under [node] a filter above it may prune,
  /// with the names it may prune that scan on: the names that reach the
  /// filter unchanged through every projection and aggregate between the
  /// two ([names], null when nothing narrows them).
  void _collectScans(
    logical.LogicalPlan node,
    Map<logical.TableScan, Set<String>?> out, [
    Set<String>? names,
  ]) {
    if (node is logical.AliasScope && node is! logical.Frame) {
      return; // derived relation: an opaque optimization boundary
    }
    if (node is logical.TableScan) {
      out[node] = names;
      return;
    }
    if (node is logical.Limit || node is logical.Sample) return;
    if (node is logical.Union || node is logical.SetOperation) return;
    var through = names;
    if (node is logical.Projection) {
      if (!_passes(node)) return;
      through = _narrow(names, node.aliases);
    } else if (node is logical.Aggregate) {
      through = _narrow(names, [
        for (final key in node.groupBy)
          if (key is logical.ColumnExpression) key.name,
      ]);
    }
    for (final c in node.children) {
      _collectScans(c, out, through);
    }
  }

  /// [names] narrowed to [kept] (all of [kept] when [names] is null).
  static Set<String> _narrow(Set<String>? names, List<String> kept) => {
    for (final name in kept)
      if (names == null || names.contains(name)) name,
  };

  /// Whether every output of [projection] is an input column under its own
  /// name.
  static bool _passes(logical.Projection projection) {
    for (int i = 0; i < projection.expressions.length; i++) {
      final expression = projection.expressions[i];
      if (expression is! logical.ColumnExpression ||
          expression.name != projection.aliases[i]) {
        return false;
      }
    }
    return true;
  }

  void _push(logical.LogicalPlan node) {
    if (node is logical.Filter) {
      final reach = <logical.TableScan, Set<String>?>{};
      for (final child in node.children) {
        _collectScans(child, reach);
      }
      final scans = reach.keys.toList();
      // How many scans below hold each column: a conjunct is only pushed
      // when all its columns are unambiguous (count 1), so cross-side/join-key
      // predicates stay above the join.
      final counts = <String, int>{};
      for (final s in scans) {
        for (final f in s.tableSchema.fields) {
          counts[f.name] = (counts[f.name] ?? 0) + 1;
        }
      }
      for (final conjunct in logical.conjuncts(node.predicate)) {
        final columns = <String>{};
        if (!_columns(conjunct, columns) || columns.isEmpty) continue;
        // The conjunct belongs to a scan iff every column is unambiguously that
        // scan's. Ambiguous (multi-scan) columns leave it above the join.
        if (!columns.every((c) => counts[c] == 1)) continue;
        final target = _owner(columns, reach);
        if (target == null) continue;
        final filter = logicalToRowFilter(conjunct);
        if (filter == null) continue;
        target.pushedFilter = target.pushedFilter == null
            ? filter
            : RowFilter.and([target.pushedFilter!, filter]);
      }
    }
    if (node is logical.AliasScope && node is! logical.Frame) return;
    for (final c in node.children) {
      _push(c);
    }
  }

  /// The single scan whose schema contains every column in [columns], each
  /// one a name the filter reaches it on ([_collectScans]), or null.
  logical.TableScan? _owner(
    Set<String> columns,
    Map<logical.TableScan, Set<String>?> scans,
  ) {
    for (final MapEntry(key: s, value: reached) in scans.entries) {
      final names = {for (final f in s.tableSchema.fields) f.name};
      if (columns.every(
        (c) => names.contains(c) && (reached == null || reached.contains(c)),
      )) {
        return s;
      }
    }
    return null;
  }

  /// Collect referenced column names; false if the expression is not a simple
  /// column/literal/binary tree (so it is left unpushed).
  bool _columns(logical.LogicalExpression e, Set<String> columns) {
    if (e is logical.ColumnExpression) {
      columns.add(e.name);
      return true;
    }
    if (e is logical.LiteralExpression) return true;
    if (e is logical.BinaryExpression) {
      return _columns(e.left, columns) && _columns(e.right, columns);
    }
    return false;
  }
}

/// Pushes column projection into table scans: each [logical.TableScan] is
/// restricted to the columns actually referenced anywhere in the plan, so a
/// lazy Parquet scan decodes (and an in-memory scan slices) fewer columns.
///
/// Safe only when the plan's outputs are enumerated by a Projection or Aggregate;
/// otherwise it is `SELECT *` (no Projection node) and every column is needed.
/// A join or set operation enumerates its outputs only when every input does:
/// a projection on one side says nothing about the columns the other keeps.
/// The set of referenced columns is collected across the whole plan; any
/// construct that can't be fully analyzed (e.g. window functions) aborts the
/// pass (read all columns). A subquery is analyzed as opaque; a registered
/// DataFrame ([logical.Frame]) as the plan it holds.
class PushDownProjection {
  logical.LogicalPlan optimize(logical.LogicalPlan plan) {
    if (!_enumeratesOutputs(plan)) return plan;
    final refs = <String>{};
    if (!_collect(plan, refs)) return plan;
    _apply(plan, refs);
    return plan;
  }

  bool _enumeratesOutputs(logical.LogicalPlan node) {
    // A derived relation is opaque: its inner Projection does not enumerate the
    // outer query's outputs.
    if (node is logical.AliasScope && node is! logical.Frame) return false;
    if (node is logical.Projection || node is logical.Aggregate) return true;
    if (node is logical.Join ||
        node is logical.Union ||
        node is logical.SetOperation) {
      return node.children.every(_enumeratesOutputs);
    }
    return node.children.any(_enumeratesOutputs);
  }

  /// Collect referenced bare column names; returns false if anything is
  /// unanalyzable (caller then skips the optimization).
  bool _collect(logical.LogicalPlan node, Set<String> refs) {
    if (node is logical.AliasScope && node is! logical.Frame) {
      return true; // opaque boundary: don't analyze inside
    }
    if (node is logical.Projection) {
      for (final e in node.expressions) {
        if (!_expression(e, refs)) return false;
      }
    } else if (node is logical.Filter) {
      if (!_expression(node.predicate, refs)) return false;
    } else if (node is logical.Join) {
      if (node.condition != null && !_expression(node.condition!, refs)) {
        return false;
      }
      if (node.usingColumns != null) refs.addAll(node.usingColumns!);
    } else if (node is logical.Aggregate) {
      for (final e in node.groupBy) {
        if (!_expression(e, refs)) return false;
      }
      for (final a in node.aggregates) {
        if (!_expression(a, refs)) return false;
      }
    } else if (node is logical.Sort) {
      for (final s in node.sortExpressions) {
        if (!_expression(s.expression, refs)) return false;
      }
    } else if (node is logical.Distinct) {
      // Rows are distinct over every input column, so each is needed.
      refs.addAll(node.input.schema.fieldNames);
    } else if (node is logical.Values) {
      for (final row in node.rows) {
        for (final e in row) {
          if (!_expression(e, refs)) return false;
        }
      }
    }
    for (final child in node.children) {
      if (!_collect(child, refs)) return false;
    }
    return true;
  }

  bool _expression(logical.LogicalExpression e, Set<String> refs) {
    if (e is logical.ColumnExpression) {
      refs.add(e.name);
      return true;
    }
    if (e is logical.LiteralExpression) return true;
    if (e is logical.BinaryExpression) {
      return _expression(e.left, refs) && _expression(e.right, refs);
    }
    if (e is logical.AggregateExpression) {
      return e.argument == null || _expression(e.argument!, refs);
    }
    if (e is logical.FunctionExpression) {
      for (final a in e.arguments) {
        if (!_expression(a, refs)) return false;
      }
      return true;
    }
    if (e is logical.CaseExpression) {
      for (final w in e.whenClauses) {
        if (!_expression(w.condition, refs) || !_expression(w.result, refs)) {
          return false;
        }
      }
      return e.elseExpression == null || _expression(e.elseExpression!, refs);
    }
    if (e is logical.CastExpression) return _expression(e.expression, refs);
    return false; // unknown expression type -> bail
  }

  void _apply(logical.LogicalPlan node, Set<String> refs) {
    if (node is logical.AliasScope && node is! logical.Frame) {
      return; // opaque boundary: don't push projection inside
    }
    if (node is logical.TableScan) {
      var columns = [
        for (final f in node.tableSchema.fields)
          if (refs.contains(f.name)) f.name,
      ];
      // No referenced column on this scan (e.g. COUNT(*)): keep one column so
      // the reader still reports row counts (a 0-column scan loses them).
      if (columns.isEmpty && node.tableSchema.fields.isNotEmpty) {
        columns = [node.tableSchema.fields.first.name];
      }
      node.projection = columns;
    }
    for (final child in node.children) {
      _apply(child, refs);
    }
  }
}
