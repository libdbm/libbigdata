part of 'query_planner.dart';

/// Pushes WHERE conjuncts down to the table scans they provably belong to as a
/// row-group pruning [RowFilter]. The [logical.Filter] node is left in place
/// (the exact predicate is re-applied), so pruning only ever over-keeps — joins,
/// outer/null-producing sides, and ambiguous columns are all safe. Runs before
/// [PushDownProjection] so each scan's full `tableSchema` is available for
/// column provenance.
class PushDownFilter {
  logical.LogicalPlan optimize(logical.LogicalPlan plan) {
    final scans = <logical.TableScan>[];
    _collectScans(plan, scans);
    if (scans.isEmpty) return plan;
    // How many scans expose each column — a conjunct is only pushed when all its
    // columns are unambiguous (count 1), so cross-side/join-key predicates stay
    // above the join.
    final counts = <String, int>{};
    for (final s in scans) {
      for (final f in s.tableSchema.fields) {
        counts[f.name] = (counts[f.name] ?? 0) + 1;
      }
    }
    _push(plan, scans, counts);
    return plan;
  }

  void _collectScans(logical.LogicalPlan node, List<logical.TableScan> out) {
    if (node is logical.AliasScope) {
      return; // derived relation — an opaque optimization boundary
    }
    if (node is logical.TableScan) {
      out.add(node);
      return;
    }
    for (final c in node.children) {
      _collectScans(c, out);
    }
  }

  void _push(
    logical.LogicalPlan node,
    List<logical.TableScan> scans,
    Map<String, int> counts,
  ) {
    if (node is logical.Filter) {
      for (final conj in _conjuncts(node.predicate)) {
        final cols = <String>{};
        if (!_columns(conj, cols) || cols.isEmpty) continue;
        // The conjunct belongs to a scan iff every column is unambiguously that
        // scan's. Ambiguous (multi-scan) columns leave it above the join.
        if (!cols.every((c) => counts[c] == 1)) continue;
        final target = _owner(cols, scans);
        if (target == null) continue;
        final rf = logicalToRowFilter(conj);
        if (rf == null) continue;
        target.pushedFilter = target.pushedFilter == null
            ? rf
            : RowFilter.and([target.pushedFilter!, rf]);
      }
    }
    if (node is logical.AliasScope) return;
    for (final c in node.children) {
      _push(c, scans, counts);
    }
  }

  /// The single scan whose schema contains every column in [cols], or null.
  logical.TableScan? _owner(Set<String> cols, List<logical.TableScan> scans) {
    for (final s in scans) {
      final names = {for (final f in s.tableSchema.fields) f.name};
      if (cols.every(names.contains)) return s;
    }
    return null;
  }

  List<logical.LogicalExpression> _conjuncts(logical.LogicalExpression e) {
    if (e is logical.BinaryExpression &&
        e.operator == logical.BinaryOperator.and) {
      return [..._conjuncts(e.left), ..._conjuncts(e.right)];
    }
    return [e];
  }

  /// Collect referenced column names; false if the expression is not a simple
  /// column/literal/binary tree (so it is left unpushed).
  bool _columns(logical.LogicalExpression e, Set<String> cols) {
    if (e is logical.ColumnExpression) {
      cols.add(e.name);
      return true;
    }
    if (e is logical.LiteralExpression) return true;
    if (e is logical.BinaryExpression) {
      return _columns(e.left, cols) && _columns(e.right, cols);
    }
    return false;
  }
}

/// Pushes column projection into table scans: each [logical.TableScan] is
/// restricted to the columns actually referenced anywhere in the plan, so a
/// lazy Parquet scan decodes (and an in-memory scan slices) fewer columns.
///
/// Safe only when the plan's outputs are enumerated by a Projection or Aggregate
/// — otherwise it is `SELECT *` (no Projection node) and every column is needed.
/// The set of referenced columns is collected across the whole plan; any
/// construct that can't be fully analyzed (e.g. window functions) aborts the
/// pass (read all columns).
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
    if (node is logical.AliasScope) return false;
    if (node is logical.Projection || node is logical.Aggregate) return true;
    return node.children.any(_enumeratesOutputs);
  }

  /// Collect referenced bare column names; returns false if anything is
  /// unanalyzable (caller then skips the optimization).
  bool _collect(logical.LogicalPlan node, Set<String> refs) {
    if (node is logical.Window) return false; // function args not modeled
    if (node is logical.AliasScope) {
      return true; // opaque boundary — don't analyze inside
    }
    if (node is logical.Projection) {
      for (final e in node.expressions) {
        if (!_expr(e, refs)) return false;
      }
    } else if (node is logical.Filter) {
      if (!_expr(node.predicate, refs)) return false;
    } else if (node is logical.Join) {
      if (node.condition != null && !_expr(node.condition!, refs)) return false;
      if (node.usingColumns != null) refs.addAll(node.usingColumns!);
    } else if (node is logical.Aggregate) {
      for (final e in node.groupBy) {
        if (!_expr(e, refs)) return false;
      }
      for (final a in node.aggregates) {
        if (!_expr(a, refs)) return false;
      }
    } else if (node is logical.Sort) {
      for (final s in node.sortExpressions) {
        if (!_expr(s.expression, refs)) return false;
      }
    } else if (node is logical.Values) {
      for (final row in node.rows) {
        for (final e in row) {
          if (!_expr(e, refs)) return false;
        }
      }
    }
    for (final child in node.children) {
      if (!_collect(child, refs)) return false;
    }
    return true;
  }

  bool _expr(logical.LogicalExpression e, Set<String> refs) {
    if (e is logical.ColumnExpression) {
      refs.add(e.name);
      return true;
    }
    if (e is logical.LiteralExpression) return true;
    if (e is logical.BinaryExpression) {
      return _expr(e.left, refs) && _expr(e.right, refs);
    }
    if (e is logical.AggregateExpression) {
      return e.argument == null || _expr(e.argument!, refs);
    }
    if (e is logical.FunctionExpression) {
      for (final a in e.arguments) {
        if (!_expr(a, refs)) return false;
      }
      return true;
    }
    if (e is logical.CaseExpression) {
      for (final w in e.whenClauses) {
        if (!_expr(w.condition, refs) || !_expr(w.result, refs)) return false;
      }
      return e.elseExpression == null || _expr(e.elseExpression!, refs);
    }
    if (e is logical.CastExpression) return _expr(e.expression, refs);
    return false; // unknown expression type -> bail
  }

  void _apply(logical.LogicalPlan node, Set<String> refs) {
    if (node is logical.AliasScope) {
      return; // opaque boundary — don't push projection inside
    }
    if (node is logical.TableScan) {
      var cols = [
        for (final f in node.tableSchema.fields)
          if (refs.contains(f.name)) f.name,
      ];
      // No referenced column on this scan (e.g. COUNT(*)): keep one column so
      // the reader still reports row counts (a 0-column scan loses them).
      if (cols.isEmpty && node.tableSchema.fields.isNotEmpty) {
        cols = [node.tableSchema.fields.first.name];
      }
      node.projection = cols;
    }
    for (final child in node.children) {
      _apply(child, refs);
    }
  }
}
