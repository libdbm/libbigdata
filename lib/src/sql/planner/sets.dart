part of 'query_planner.dart';

/// Set operations: UNION, INTERSECT and EXCEPT folding and branch binding.
extension _Sets on LogicalPlanBuilder {
  (logical.SetOpType, bool) _setOpType(ast.SetOperationType type) =>
      switch (type) {
        ast.SetOperationType.union => (logical.SetOpType.union, false),
        ast.SetOperationType.unionAll => (logical.SetOpType.union, true),
        ast.SetOperationType.intersect => (logical.SetOpType.intersect, false),
        ast.SetOperationType.except => (logical.SetOpType.except, false),
      };

  /// Fold a flat left-to-right list of set-operation [tails] onto [base] honoring
  /// SQL precedence: INTERSECT binds tighter than UNION/EXCEPT, and both are
  /// left-associative. Collapse each INTERSECT into the current term as it
  /// appears, start a new term on UNION/EXCEPT, then left-fold the terms, so
  /// `A UNION B INTERSECT C` is `A UNION (B INTERSECT C)` and `A EXCEPT B EXCEPT C`
  /// is `(A EXCEPT B) EXCEPT C`.
  logical.LogicalPlan _foldOperations(
    logical.LogicalPlan base,
    List<ast.SetOperation> tails,
  ) {
    var term = base;
    final terms = <logical.LogicalPlan>[];
    final kinds = <(logical.SetOpType, bool)>[];
    for (final tail in tails) {
      final (type, all) = _setOpType(tail.type);
      final right = tail.right.accept(this);
      if (type == logical.SetOpType.intersect) {
        term = _bindSetOperation(term, right, type, all);
      } else {
        terms.add(term);
        kinds.add((type, all));
        term = right;
      }
    }
    terms.add(term);
    // Left to right, a run of UNION / UNION ALL branches gathers into one
    // n-ary Union. A distinct UNION removes duplicates from everything to its
    // left, so the run is deduplicated once, up to its LAST distinct UNION,
    // and the rest is appended: `a UNION ALL b UNION c UNION ALL d` is
    // `Union[Distinct(Union[a, b, c]), d]`. An EXCEPT closes the run.
    var run = [terms.first];
    var last = -1; // index in [run] of the last distinct UNION's branch
    for (int i = 0; i < kinds.length; i++) {
      final (type, all) = kinds[i];
      if (type == logical.SetOpType.union) {
        run.add(terms[i + 1]);
        if (!all) last = run.length - 1;
      } else {
        run = [
          _bindSetOperation(_collapse(run, last), terms[i + 1], type, all),
        ];
        last = -1;
      }
    }
    return _collapse(run, last);
  }

  /// The UNION run [branches] as one plan, deduplicated up to index [last]
  /// (none when it is negative).
  logical.LogicalPlan _collapse(List<logical.LogicalPlan> branches, int last) {
    if (branches.length == 1) return branches.single;
    if (last < 0) return _bindUnion(branches);
    final head = logical.Distinct(_bindUnion(branches.sublist(0, last + 1)));
    if (last == branches.length - 1) return head;
    return _bindUnion([head, ...branches.sublist(last + 1)]);
  }

  /// [branches] as one [logical.Union]: equal arity, and every branch after the
  /// first stamped with the first's column names (as [_bindSetOperation]).
  logical.LogicalPlan _bindUnion(List<logical.LogicalPlan> branches) {
    final fields = branches.first.schema.fields;
    final names = [for (final f in fields) f.name];
    return logical.Union([
      branches.first,
      for (final branch in branches.skip(1))
        _renameBranch(branch, _arity(fields, branch.schema.fields), names),
    ]);
  }

  /// [fields] when they have as many columns as [first]; otherwise the set
  /// operation fails with [Kind.schema].
  List<Field> _arity(List<Field> first, List<Field> fields) {
    if (first.length != fields.length) {
      throw Failure.of(
        Kind.schema,
        'Set operation branches have different column counts '
        '(${first.length} vs ${fields.length})',
      );
    }
    return fields;
  }

  /// Bind a set operation to SQL semantics: equal arity, a common type resolved
  /// per position (with a CAST inserted on the differing branch), and the RIGHT
  /// branch stamped with the LEFT output NAMES (SQL derives set-op output names
  /// from the left). Without this the operator forwards raw batches: a wider
  /// right side silently loses columns, and a valid positional union with
  /// different names fails downstream on a name mismatch.
  logical.LogicalPlan _bindSetOperation(
    logical.LogicalPlan left,
    logical.LogicalPlan right,
    logical.SetOpType type,
    bool all,
  ) {
    final leftFields = left.schema.fields;
    final rightFields = _arity(leftFields, right.schema.fields);
    // Types are NOT reconciled here: a logical column ref carries the `string`
    // PLACEHOLDER type, so commonType() against it lies: pairing a literal/
    // CAST/comparison branch with a bare-column branch injected a stringifying
    // CAST into a valid all-numeric set operation (`SELECT 1 UNION SELECT n`
    // failed at the int64→string cast). visitSetOperation reconciles on the
    // REAL physical types (including decimal scale); only the arity check and
    // the left-name stamping belong at this layer.
    final leftNames = [for (final f in leftFields) f.name];
    return logical.SetOperation(
      left: left,
      right: _renameBranch(right, rightFields, leftNames),
      type: type,
      all: all,
    );
  }

  /// Project [input] so its columns carry [names] (the LEFT branch's names).
  /// Returns [input] unchanged when the names already match.
  logical.LogicalPlan _renameBranch(
    logical.LogicalPlan input,
    List<Field> fields,
    List<String> names,
  ) {
    var needs = false;
    for (int i = 0; i < fields.length; i++) {
      if (names[i] != fields[i].name) needs = true;
    }
    if (!needs) return input;
    return logical.Projection(
      input: input,
      expressions: [
        for (final f in fields)
          logical.ColumnExpression(name: f.name, columnType: f.dataType),
      ],
      aliases: names,
    );
  }
}
