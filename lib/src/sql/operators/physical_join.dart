part of 'physical_operators.dart';

/// Hash join physical operator
class PhysicalHashJoin extends PhysicalPlan {
  final PhysicalPlan left;
  final PhysicalPlan right;
  // Equi-join keys, one entry per ANDed `left = right` equality (a single-column
  // join is the common one-entry case; multi-column USING and ANDed ON produce
  // several). The four lists are PARALLEL. Empty means no key: a cross join.
  final List<String> leftKeys;
  final List<String> rightKeys;
  final List<String?> leftQualifiers;
  final List<String?> rightQualifiers;
  final JoinType joinType;
  final EngineOptions options;

  PhysicalHashJoin({
    required this.left,
    required this.right,
    required this.leftKeys,
    required this.rightKeys,
    List<String?>? leftQualifiers,
    List<String?>? rightQualifiers,
    required this.joinType,
    this.options = const EngineOptions(),
  }) : leftQualifiers = leftQualifiers ?? List.filled(leftKeys.length, null),
       rightQualifiers = rightQualifiers ?? List.filled(rightKeys.length, null);

  /// Right-side key fields to drop (kept once from the left). Only de-dups a key
  /// column that would actually collide: same name AND same qualifier (e.g. an
  /// unqualified in-memory join, or USING). A qualified cross-file `a.id = b.id`
  /// keeps both `a.id` and `b.id`, so chained joins can still reference either.
  static Set<int> dropIndices(
    Schema right,
    List<String> leftKeys,
    List<String> rightKeys,
    List<String?> leftQualifiers,
    List<String?> rightQualifiers,
  ) {
    final drop = <int>{};
    for (int j = 0; j < leftKeys.length; j++) {
      if (leftKeys[j] == rightKeys[j] &&
          leftQualifiers[j] == rightQualifiers[j]) {
        final index = right.resolve(
          rightKeys[j],
          qualifier: rightQualifiers[j],
        );
        if (index >= 0) drop.add(index);
      }
    }
    return drop;
  }

  /// Each key column's decimal scale, or null when it isn't decimal: see
  /// [scaledKey]. A key that doesn't resolve is caught later by the batch lookup.
  static List<int?> keyScales(
    Schema schema,
    List<String> keys,
    List<String?> qualifiers,
  ) => [
    for (int j = 0; j < keys.length; j++)
      () {
        final index = schema.resolve(keys[j], qualifier: qualifiers[j]);
        if (index < 0) return null;
        final field = schema.fields[index];
        return field.dataType == DataType.decimal ? field.scale : null;
      }(),
  ];

  /// The joined schema, widening the NULL-PRODUCING side's fields to nullable.
  ///
  /// An outer join manufactures nulls for unmatched rows (`Kernels.gather`
  /// null-pads), so a field the source declared non-nullable is no longer. A
  /// Parquet column that merely happened to hold no nulls comes back `required`,
  /// and copying that through made `Table._validate` reject the join's own
  /// output ("contains nulls but is declared non-nullable") instead of
  /// returning rows.
  static Schema joinSchema(
    Schema left,
    Schema right,
    Set<int> drop,
    JoinType joinType,
  ) {
    // A semi/anti join FILTERS the left side by whether the right matched; it
    // emits no right column, so there is nothing to pad or drop.
    if (semiLike(joinType)) return left;
    // LEFT keeps every left row and nulls the right; RIGHT is the mirror; FULL
    // does both. INNER/CROSS emit only matched pairs, so nothing is padded.
    final nullLeft = joinType == JoinType.right || joinType == JoinType.full;
    final nullRight = joinType == JoinType.left || joinType == JoinType.full;
    final fields = <Field>[
      for (final field in left.fields)
        nullLeft ? field.copyWith(nullable: true) : field,
    ];
    for (int j = 0; j < right.fields.length; j++) {
      if (drop.contains(j)) continue;
      final field = right.fields[j];
      fields.add(nullRight ? field.copyWith(nullable: true) : field);
    }
    return Schema(fields: fields);
  }

  @override
  Schema get schema => joinSchema(
    left.schema,
    right.schema,
    dropIndices(
      right.schema,
      leftKeys,
      rightKeys,
      leftQualifiers,
      rightQualifiers,
    ),
    joinType,
  );

  @override
  int? get estimatedRows {
    final leftRows = left.estimatedRows;
    final rightRows = right.estimatedRows;
    if (leftRows == null || rightRows == null) return null;

    // Estimate based on join type. Exhaustive on purpose: a `default` here once
    // let the semi/anti types estimate null instead of failing to compile.
    switch (joinType) {
      case JoinType.inner:
        return (leftRows * rightRows * 0.1).round(); // Assume 10% match rate
      case JoinType.left:
        return leftRows;
      case JoinType.right:
        return rightRows;
      case JoinType.full:
        return leftRows + rightRows;
      case JoinType.cross:
        return leftRows * rightRows;
      // A semi/anti join filters the left side, so it is bounded by it, never
      // the product. Same 10% match rate as inner, and its complement.
      case JoinType.semi:
        return (leftRows * 0.1).round();
      case JoinType.anti:
      case JoinType.antiNullAware:
        return (leftRows * 0.9).round();
    }
  }

  /// Whether to build the hash table on the left input: an inner join whose
  /// left side is estimated smaller than its right. Outer, semi and anti
  /// joins keep the build on the right, where their unmatched-row handling
  /// lives.
  bool get buildLeft {
    if (joinType != JoinType.inner) return false;
    final leftRows = left.estimatedRows;
    final rightRows = right.estimatedRows;
    return leftRows != null && rightRows != null && leftRows < rightRows;
  }

  @override
  RecordBatchIterator execute() {
    return HashJoinIterator(
      left: left.execute(),
      right: right.execute(),
      leftKeys: leftKeys,
      rightKeys: rightKeys,
      leftQualifiers: leftQualifiers,
      rightQualifiers: rightQualifiers,
      joinType: joinType,
      options: options,
      buildLeft: buildLeft,
    );
  }
}

class HashJoinIterator extends RecordBatchIterator {
  final RecordBatchIterator left;
  final RecordBatchIterator right;
  final List<String> leftKeys;
  final List<String> rightKeys;
  final List<String?> leftQualifiers;
  final List<String?> rightQualifiers;
  final JoinType joinType;
  final EngineOptions options;

  /// Build the hash table on the left input and stream the right through it
  /// (an inner join whose left side is the smaller; see
  /// [PhysicalHashJoin.buildLeft]).
  final bool buildLeft;

  /// Left batches read while trying to build on the left side, replayed
  /// first when that side proves too large and the join builds on the right
  /// after all.
  final List<RecordBatch> _replay = [];

  late final Schema _schema;
  late final Set<int> _drop;
  // For a RIGHT or FULL join, each kept left key column (by index) whose right
  // twin was dropped (by index): the one output column must hold the right
  // key for rows only the right side has, as `USING` requires.
  late final Map<int, int> _merged;
  // The key columns' decimal scales, or null when they aren't decimal. Taken
  // from the PLAN's schemas, once: a spilled batch's schema has lost them
  // (schemaOf rebuilds fields from dataTypes alone). Both sides are needed
  // SEPARATELY: the whole defect is that the two scales can differ.
  late final List<int?> _leftScales;
  late final List<int?> _rightScales;
  StreamIterator<RecordBatch>? _output;
  final List<SpillRun> _runs = [];
  // Grace-partition writers open but not yet finalized into [_runs]; tracked so
  // close() can abort+delete them if the join is cancelled or throws during the
  // build/probe-partition phase (otherwise their fds + temp files leak).
  final List<SpillWriter> _writers = [];
  // Shared-budget reservation for the in-memory build (right) buffer.
  int _reserved = 0;
  // Running emitted-row count for the CROSS JOIN maxOutputRows guard.
  int _crossEmitted = 0;
  // Running emitted-row count for the equi/hash-join maxOutputRows guard.
  int _probeEmitted = 0;
  // Empty, real-typed template columns for null-side gathers (so unmatched-row
  // batches have the same column types as matched batches across partitions).
  List<Column>? _leftEmpty;
  List<Column>? _rightEmpty;

  HashJoinIterator({
    required this.left,
    required this.right,
    required this.leftKeys,
    required this.rightKeys,
    required this.leftQualifiers,
    required this.rightQualifiers,
    required this.joinType,
    this.options = const EngineOptions(),
    this.buildLeft = false,
  }) {
    _drop = PhysicalHashJoin.dropIndices(
      right.schema,
      leftKeys,
      rightKeys,
      leftQualifiers,
      rightQualifiers,
    );
    _schema = PhysicalHashJoin.joinSchema(
      left.schema,
      right.schema,
      _drop,
      joinType,
    );
    _merged = joinType == JoinType.right || joinType == JoinType.full
        ? _twins(left.schema, right.schema)
        : const {};
    _leftScales = PhysicalHashJoin.keyScales(
      left.schema,
      leftKeys,
      leftQualifiers,
    );
    _rightScales = PhysicalHashJoin.keyScales(
      right.schema,
      rightKeys,
      rightQualifiers,
    );
    _reconcileScales();
  }

  /// Reconcile a key position that is DECIMAL on one side and an INTEGER or
  /// FLOAT on the other: give that side scale 0 so [_keyAt] wraps its value as
  /// a [Decimal] too (a double becomes the decimal its shortest form
  /// represents). A raw `int` and a `Decimal` never hash-equal, so without this
  /// every `int = decimal` match silently drops; once both are Decimals,
  /// `Decimal`'s cross-scale `==` matches `5` with `5.00`. Decimal/decimal
  /// (both non-null) and integer/integer (both null) positions are left
  /// untouched.
  void _reconcileScales() {
    for (int j = 0; j < _leftScales.length; j++) {
      final leftDecimal = _leftScales[j] != null;
      final rightDecimal = _rightScales[j] != null;
      if (leftDecimal == rightDecimal) continue; // both or neither: no lift
      if (leftDecimal &&
          _isNumber(right.schema, rightKeys[j], rightQualifiers[j])) {
        _rightScales[j] = 0;
      } else if (rightDecimal &&
          _isNumber(left.schema, leftKeys[j], leftQualifiers[j])) {
        _leftScales[j] = 0;
      }
    }
  }

  static bool _isNumber(Schema schema, String key, String? qualifier) {
    final index = schema.resolve(key, qualifier: qualifier);
    if (index < 0) return false;
    final type = schema.fields[index].dataType;
    return type.isInteger ||
        type == DataType.float32 ||
        type == DataType.float64;
  }

  @override
  Schema get schema => _schema;

  String get _spillDir => options.spillDir ?? Directory.systemTemp.path;

  /// The key columns of [b] for one side, in key order. A text column is
  /// decoded once per distinct value, so [_keyAt] reads a shared [String]
  /// rather than decoding every row.
  List<Column> _keys(
    RecordBatch b,
    List<String> keys,
    List<String?> qualifiers,
  ) => [
    for (int j = 0; j < keys.length; j++)
      switch (b.column(keys[j], qualifier: qualifiers[j])!) {
        final StringColumn text => ListColumn<String>(
          name: text.name,
          dataType: text.dataType,
          data: text.interned(),
          nullable: text.nullable,
        ),
        final column => column,
      },
  ];

  /// The join key for one row: a single canonicalized value for a one-column key
  /// (the common, fast case) or a [RowKey] over the columns for a composite key.
  /// Null if ANY key column is null: a null never equi-matches, so such a row is
  /// treated exactly as the old single-null-key case (no match; emitted only as
  /// an unmatched-outer row). [canonicalKey] makes a binary/list key compare by
  /// CONTENT and a decimal by VALUE (its scale differs per side).
  Object? _keyAt(List<Column> columns, List<int?> scales, int row) {
    if (columns.length == 1) {
      final v = columns[0][row];
      return v == null ? null : canonicalKey(v, scale: scales[0]);
    }
    final parts = <Object?>[];
    for (int j = 0; j < columns.length; j++) {
      final v = columns[j][row];
      if (v == null) return null;
      parts.add(canonicalKey(v, scale: scales[j]));
    }
    return RowKey(parts);
  }

  @override
  Future<RecordBatch?> next() async {
    await pause();
    final output = _output ??= StreamIterator(_generate());
    return (await output.moveNext()) ? output.current : null;
  }

  Stream<RecordBatch> _generate() async* {
    if (joinType == JoinType.cross) {
      yield* _crossJoin();
      return;
    }
    if (buildLeft && await _drainLeft()) {
      yield* _probeRight();
      return;
    }

    // Drain the build (right) side, buffering until the budget is exceeded; past
    // that, grace-partition both sides by key hash so each partition fits.
    final rightBuffer = <RecordBatch>[];
    var bytes = 0;
    List<SpillWriter>? rightParts;
    List<SpillWriter>? leftParts;
    var partitions = 0;
    RecordBatch? batch;
    // Shared budget applies only when spilling is enabled (forces grace
    // partitioning earlier when the global pool is exhausted).
    final memory = options.spills ? options.memory : null;

    // `x NOT IN (S)` is UNKNOWN for EVERY row when S holds a null: a GLOBAL
    // property that can't be decided per grace partition, so track it while the
    // whole build side is consumed and short-circuit below.
    var buildHasNull = false;

    while ((batch = await right.next()) != null) {
      _rightEmpty ??= [for (final c in batch!.columns) c.slice(0, 0)];
      if (joinType == JoinType.antiNullAware && !buildHasNull) {
        final columns = _keys(batch!, rightKeys, rightQualifiers);
        for (int i = 0; i < batch.rowCount; i++) {
          if (_keyAt(columns, _rightScales, i) == null) {
            buildHasNull = true;
            break;
          }
        }
      }
      final usage = batch!.memoryUsage;
      if (rightParts == null) {
        final localOver =
            options.spills && bytes + usage > options.spillThresholdBytes;
        final globalOver =
            !localOver && memory != null && !memory.reserve(usage);
        if (!localOver && !globalOver && memory != null) _reserved += usage;
        if (localOver || globalOver) {
          partitions = math.max(
            16,
            (bytes ~/ options.spillThresholdBytes + 1) * 2,
          );
          // Create the spill dir once, off the hot path.
          await Directory(_spillDir).create(recursive: true);
          // The build (right) partition is read fully into the hash table → Arrow
          // when types allow; the left/probe partition is streamed during
          // probing, so it stays Parquet (lazy read).
          rightParts = [
            for (int i = 0; i < partitions; i++)
              SpillWriter(
                _spillDir,
                _rightEmpty!.schemaOf(template: right.schema),
                format: SpillFormat.arrow,
                budget: options.spillBudget,
              ),
          ];
          leftParts = [
            for (int i = 0; i < partitions; i++)
              SpillWriter(_spillDir, left.schema, budget: options.spillBudget),
          ];
          _writers
            ..addAll(rightParts)
            ..addAll(leftParts); // track until finished into _runs
          for (final b in rightBuffer) {
            await _route(
              b,
              rightParts,
              partitions,
              rightKeys,
              rightQualifiers,
              _rightScales,
            );
          }
          rightBuffer.clear();
          bytes = 0;
          memory?.release(_reserved); // buffered build rows are on disk now
          _reserved = 0;
          await _route(
            batch,
            rightParts,
            partitions,
            rightKeys,
            rightQualifiers,
            _rightScales,
          );
          continue;
        }
        rightBuffer.add(batch);
        bytes += usage;
      } else {
        await _route(
          batch,
          rightParts,
          partitions,
          rightKeys,
          rightQualifiers,
          _rightScales,
        );
      }
    }

    // NOT IN is null-aware over the WHOLE build side (see [buildHasNull]): an
    // empty build makes it TRUE for every left row (emit all, even null keys); a
    // build containing a null makes it UNKNOWN for every row (emit none). Neither
    // is decidable per partition, so resolve both here, once the build is fully
    // consumed. Otherwise it reduces to a plain anti join, which IS per-partition.
    if (joinType == JoinType.antiNullAware) {
      if (rightParts == null && rightBuffer.isEmpty) {
        yield* _emitAllLeft(_leftStream());
        return;
      }
      if (buildHasNull) return;
    }

    if (rightParts == null) {
      // Build fits the budget: hash it once, stream-probe the left side.
      final rightBuild = rightBuffer.isEmpty
          ? null
          : RecordBatch.concatAll(rightBuffer, rightBuffer.first.schema);
      yield* _buildAndProbe(rightBuild, _leftStream());
      return;
    }

    // Grace: partition the left (probe) side too, then join partition pairs.
    while ((batch = await _nextLeft()) != null) {
      _leftEmpty ??= [for (final c in batch!.columns) c.slice(0, 0)];
      await _route(
        batch!,
        leftParts!,
        partitions,
        leftKeys,
        leftQualifiers,
        _leftScales,
      );
    }
    final rightRuns = [for (final w in rightParts) await w.finish()];
    final leftRuns = [for (final w in leftParts!) await w.finish()];
    _runs
      ..addAll(rightRuns)
      ..addAll(leftRuns);
    _writers.clear(); // all finished → now owned by _runs

    for (int p = 0; p < partitions; p++) {
      yield* _joinPartition(rightRuns[p], leftRuns[p], 0);
    }
  }

  static const _maxJoinRepartitionDepth = 6;

  /// A build partition under this absolute size is always built in memory, even
  /// when the (possibly tiny) spill budget is smaller: recursion is a skew
  /// mitigation for genuinely large partitions, not a hard fit-the-budget rule.
  static const _minJoinBuildBytes = 1 << 20; // 1 MB

  /// Join one build/probe partition pair. If the build side fits the budget it's
  /// hashed and probed directly; if a skewed partition still exceeds it, BOTH
  /// sides are re-partitioned with the same salted hash (so matching keys
  /// co-locate) and joined recursively. At the recursion cap, where salted
  /// repartitioning can no longer shrink an irreducible single-key partition;
  /// it falls back to a memory-bounded block-nested-loop rather than
  /// materializing the whole partition (which would OOM on a hot key).
  Stream<RecordBatch> _joinPartition(
    SpillRun rightRun,
    SpillRun leftRun,
    int depth,
  ) async* {
    final buildBudget = options.spillThresholdBytes > _minJoinBuildBytes
        ? options.spillThresholdBytes
        : _minJoinBuildBytes;
    final canRepartition = depth < _maxJoinRepartitionDepth;

    // A semi/anti build is a DISTINCT-KEY set. It is usually far smaller than the
    // rows (a hot key costs one entry), but a high-cardinality partition can
    // still exceed the budget, so meter it byte-by-byte and repartition (not the
    // block-nested-loop, whose per-block membership is wrong for semi/anti); fail
    // loud at the recursion cap rather than grow unbounded.
    if (semiLike(joinType)) {
      final keys = <Object?>{};
      var keyBytes = 0;
      var reserved = 0;
      final memory = options.spills ? options.memory : null;
      var over = false;
      await for (final b in rightRun.read()) {
        final columns = _keys(b, rightKeys, rightQualifiers);
        for (int j = 0; j < b.rowCount; j++) {
          final k = _keyAt(columns, _rightScales, j);
          if (k == null || !keys.add(k)) continue;
          final size = keyValueBytes(k);
          keyBytes += size;
          final granted = memory == null || memory.reserve(size);
          if (granted && memory != null) reserved += size;
          if (keyBytes > buildBudget || !granted) {
            over = true;
            break;
          }
        }
        if (over) break;
      }
      if (!over) {
        try {
          yield* _probeSet(keys, leftRun.read());
        } finally {
          memory?.release(reserved);
        }
        return;
      }
      // Over budget: drop the partial set, then repartition below (or fail loud
      // at the cap: salted repartitioning can no longer shrink it).
      memory?.release(reserved);
      if (!canRepartition) {
        throw Failure.of(
          Kind.limit,
          'semi/anti join distinct-key set exceeds the memory budget at the '
          'repartition cap',
        );
      }
      // fall through to the shared repartition tail.
    } else {
      final partBatches = <RecordBatch>[];
      var bytes = 0;
      var overBudget = false;
      await for (final b in rightRun.read()) {
        partBatches.add(b);
        bytes += b.memoryUsage;
        // Detect over-budget even at the cap so we stop short of reading (and
        // materializing) the whole partition and take the streaming fallback.
        if (bytes > buildBudget) {
          overBudget = true;
          break;
        }
      }
      if (!overBudget) {
        final rightBuild = partBatches.isEmpty
            ? null
            : RecordBatch.concatAll(partBatches, partBatches.first.schema);
        yield* _buildAndProbe(rightBuild, leftRun.read());
        return;
      }
      partBatches.clear(); // release the partial build before re-partitioning

      if (!canRepartition) {
        // Irreducible skew at the recursion cap: stream the build side in bounded
        // blocks instead of concatenating it all into memory.
        yield* _blockNestedLoopJoin(rightRun, leftRun, buildBudget);
        return;
      }
    }
    const fanout = 16;
    final salt = depth + 1;
    final rightSchema = (_rightEmpty ?? _empty(right.schema)).schemaOf(
      template: right.schema,
    );
    final leftSchema = (_leftEmpty ?? _empty(left.schema)).schemaOf(
      template: left.schema,
    );

    await Directory(_spillDir).create(recursive: true);
    final rightParts = _newParts(fanout, rightSchema, SpillFormat.arrow);
    await for (final b in rightRun.read()) {
      await _route(
        b,
        rightParts,
        fanout,
        rightKeys,
        rightQualifiers,
        _rightScales,
        salt: salt,
      );
    }
    final leftParts = _newParts(fanout, leftSchema, SpillFormat.parquet);
    await for (final b in leftRun.read()) {
      await _route(
        b,
        leftParts,
        fanout,
        leftKeys,
        leftQualifiers,
        _leftScales,
        salt: salt,
      );
    }

    final rightRuns = <SpillRun>[];
    for (final w in rightParts) {
      final r = await w.finish();
      _writers.remove(w);
      _runs.add(r);
      rightRuns.add(r);
    }
    final leftRuns = <SpillRun>[];
    for (final w in leftParts) {
      final r = await w.finish();
      _writers.remove(w);
      _runs.add(r);
      leftRuns.add(r);
    }
    for (int p = 0; p < fanout; p++) {
      yield* _joinPartition(rightRuns[p], leftRuns[p], depth + 1);
      await rightRuns[p].delete();
      _runs.remove(rightRuns[p]);
      await leftRuns[p].delete();
      _runs.remove(leftRuns[p]);
    }
  }

  /// Allocate [n] tracked spill partition writers of [schema].
  List<SpillWriter> _newParts(int n, Schema schema, SpillFormat format) {
    final parts = [
      for (int i = 0; i < n; i++)
        SpillWriter(
          _spillDir,
          schema,
          format: format,
          budget: options.spillBudget,
        ),
    ];
    _writers.addAll(parts);
    return parts;
  }

  /// Hash [rightBuild] by key, then probe a stream of left batches, emitting one
  /// output batch per left batch plus (for outer joins) the unmatched rows.
  ///
  /// [forceUnmatchedLeft] overrides whether unmatched-left rows are emitted here
  /// (the block-nested-loop fallback suppresses them per block and emits them
  /// once in a separate pass); [collectKeys], when given, receives this build's
  /// distinct keys so that pass knows which left rows never matched.
  Stream<RecordBatch> _buildAndProbe(
    RecordBatch? rightBuild,
    Stream<RecordBatch> leftStream, {
    bool? forceUnmatchedLeft,
    Set<Object?>? collectKeys,
  }) async* {
    // A semi/anti join emits LEFT rows only, by membership in the build's
    // distinct key set, not paired rows. (antiNullAware's global empty/null
    // cases are resolved in _generate before reaching here.)
    if (semiLike(joinType)) {
      final keys = <Object?>{};
      if (rightBuild != null) {
        final columns = _keys(rightBuild, rightKeys, rightQualifiers);
        for (int j = 0; j < rightBuild.rowCount; j++) {
          final k = _keyAt(columns, _rightScales, j);
          if (k != null) keys.add(k); // a null key matches nothing
        }
      }
      yield* _probeSet(keys, leftStream);
      return;
    }

    final count = rightBuild?.rowCount ?? 0;
    final rightKeyColumns = rightBuild == null
        ? null
        : _keys(rightBuild, rightKeys, rightQualifiers);
    final keyToRows = <Object?, List<int>>{};
    for (int j = 0; j < count; j++) {
      // A null (any null key part) never equi-matches, so it is not indexed.
      final k = _keyAt(rightKeyColumns!, _rightScales, j);
      if (k != null) {
        keyToRows.putIfAbsent(k, () => []).add(j);
      }
    }
    if (collectKeys != null) collectKeys.addAll(keyToRows.keys);
    final emitUnmatchedRight =
        joinType == JoinType.right || joinType == JoinType.full;
    final matchedRight = emitUnmatchedRight
        ? List<bool>.filled(count, false)
        : null;
    final emitUnmatchedLeft =
        forceUnmatchedLeft ??
        (joinType == JoinType.left || joinType == JoinType.full);
    final rightColumns =
        rightBuild?.columns ?? _rightEmpty ?? _empty(right.schema);

    final max = options.maxOutputRows;
    await for (final leftBatch in leftStream) {
      _leftEmpty ??= [for (final c in leftBatch.columns) c.slice(0, 0)];
      final leftKeyColumns = _keys(leftBatch, leftKeys, leftQualifiers);
      final leftIndices = <int>[];
      final rightIndices = <int>[];
      for (int i = 0; i < leftBatch.rowCount; i++) {
        final k = _keyAt(leftKeyColumns, _leftScales, i);
        final matches = k == null ? null : keyToRows[k];
        if (matches == null) {
          if (emitUnmatchedLeft) {
            leftIndices.add(i);
            rightIndices.add(-1);
          }
        } else {
          for (final j in matches) {
            leftIndices.add(i);
            rightIndices.add(j);
            if (matchedRight != null) matchedRight[j] = true;
            // Flush INSIDE the match loop: a single hot key can produce far more
            // than one batch of pairs, so buffering the whole left batch's
            // matches would allocate the full (skewed) product before yielding.
            if (leftIndices.length >= defaultBatchSize) {
              if (max != null && _probeEmitted + leftIndices.length > max) {
                _probeOverflow(max);
              }
              _probeEmitted += leftIndices.length;
              yield _assemble(
                leftBatch.columns,
                rightColumns,
                leftIndices,
                rightIndices,
              );
              leftIndices.clear();
              rightIndices.clear();
            }
          }
        }
      }
      if (leftIndices.isNotEmpty) {
        if (max != null && _probeEmitted + leftIndices.length > max) {
          _probeOverflow(max);
        }
        _probeEmitted += leftIndices.length;
        yield _assemble(
          leftBatch.columns,
          rightColumns,
          leftIndices,
          rightIndices,
        );
      }
    }

    if (matchedRight != null) {
      final leftColumns = _leftEmpty ?? _empty(left.schema);
      final leftIndices = <int>[];
      final rightIndices = <int>[];
      for (int j = 0; j < count; j++) {
        if (!matchedRight[j]) {
          leftIndices.add(-1);
          rightIndices.add(j);
          if (leftIndices.length >= defaultBatchSize) {
            if (max != null && _probeEmitted + leftIndices.length > max) {
              _probeOverflow(max);
            }
            _probeEmitted += leftIndices.length;
            yield _assemble(
              leftColumns,
              rightColumns,
              leftIndices,
              rightIndices,
            );
            leftIndices.clear();
            rightIndices.clear();
          }
        }
      }
      if (leftIndices.isNotEmpty) {
        if (max != null && _probeEmitted + leftIndices.length > max) {
          _probeOverflow(max);
        }
        _probeEmitted += leftIndices.length;
        yield _assemble(leftColumns, rightColumns, leftIndices, rightIndices);
      }
    }
  }

  /// Read the left input into [_replay] while it fits the spill threshold
  /// and the shared memory pool. True when it all fit, so the join can build
  /// on it; false leaves what was read in [_replay] for the usual right-side
  /// build to probe with first.
  Future<bool> _drainLeft() async {
    final memory = options.spills ? options.memory : null;
    var bytes = 0;
    RecordBatch? batch;
    while ((batch = await left.next()) != null) {
      final usage = batch!.memoryUsage;
      _replay.add(batch);
      bytes += usage;
      final granted = memory == null || memory.reserve(usage);
      if (granted && memory != null) _reserved += usage;
      if (!granted || (options.spills && bytes > options.spillThresholdBytes)) {
        memory?.release(_reserved);
        _reserved = 0;
        return false;
      }
    }
    return true;
  }

  /// The next left batch, replaying [_replay] first.
  Future<RecordBatch?> _nextLeft() async =>
      _replay.isNotEmpty ? _replay.removeAt(0) : left.next();

  /// The left input as a stream, replaying [_replay] first.
  Stream<RecordBatch> _leftStream() async* {
    RecordBatch? batch;
    while ((batch = await _nextLeft()) != null) {
      yield batch!;
    }
  }

  /// Inner join built on the left batches in [_replay]: index their rows by
  /// key, then stream the right input through the index. Columns come out as
  /// left then right, as when building on the right.
  Stream<RecordBatch> _probeRight() async* {
    if (_replay.isEmpty) return;
    final build = RecordBatch.concatAll(_replay, _replay.first.schema);
    _replay.clear();
    final keyColumns = _keys(build, leftKeys, leftQualifiers);
    final keyToRows = <Object?, List<int>>{};
    for (int j = 0; j < build.rowCount; j++) {
      // A null (any null key part) never equi-matches, so it is not indexed.
      final k = _keyAt(keyColumns, _leftScales, j);
      if (k != null) keyToRows.putIfAbsent(k, () => []).add(j);
    }
    final max = options.maxOutputRows;
    final leftIndices = <int>[];
    final rightIndices = <int>[];
    RecordBatch? batch;
    while ((batch = await right.next()) != null) {
      final probe = batch!;
      final probeKeys = _keys(probe, rightKeys, rightQualifiers);
      for (int i = 0; i < probe.rowCount; i++) {
        final k = _keyAt(probeKeys, _rightScales, i);
        final matches = k == null ? null : keyToRows[k];
        if (matches == null) continue;
        for (final j in matches) {
          leftIndices.add(j);
          rightIndices.add(i);
          if (leftIndices.length >= defaultBatchSize) {
            if (max != null && _probeEmitted + leftIndices.length > max) {
              _probeOverflow(max);
            }
            _probeEmitted += leftIndices.length;
            yield _assemble(
              build.columns,
              probe.columns,
              leftIndices,
              rightIndices,
            );
            leftIndices.clear();
            rightIndices.clear();
          }
        }
      }
      if (leftIndices.isNotEmpty) {
        if (max != null && _probeEmitted + leftIndices.length > max) {
          _probeOverflow(max);
        }
        _probeEmitted += leftIndices.length;
        yield _assemble(
          build.columns,
          probe.columns,
          leftIndices,
          rightIndices,
        );
        leftIndices.clear();
        rightIndices.clear();
      }
    }
  }

  Never _probeOverflow(int max) =>
      throw Failure.of(Kind.limit, 'JOIN exceeds maxOutputRows=$max');

  /// Whether a semi/anti join emits a left row, given whether its key is null
  /// and whether it is a member of the build's distinct key set.
  ///
  /// SEMI (IN / EXISTS): a match exists → emit; a null key matches nothing.
  /// ANTI (NOT EXISTS): NO match → emit, INCLUDING a null key (which matches
  /// nothing, so no matching row exists). antiNullAware (NOT IN): a null key is
  /// UNKNOWN, so NOT emitted; its global empty/null cases are handled in
  /// _generate, leaving the plain non-null-not-in-build case here.
  bool _emitLeft(bool isNull, bool inBuild) => switch (joinType) {
    JoinType.semi => !isNull && inBuild,
    JoinType.anti => isNull || !inBuild,
    JoinType.antiNullAware => !isNull && !inBuild,
    _ => throw StateError('not a semi/anti join: $joinType'),
  };

  /// Probe [leftStream] against a build's distinct key set, emitting LEFT rows
  /// only (a semi/anti join's schema is the left schema), per [_emitLeft].
  Stream<RecordBatch> _probeSet(
    Set<Object?> buildKeys,
    Stream<RecordBatch> leftStream,
  ) async* {
    final max = options.maxOutputRows;
    await for (final leftBatch in leftStream) {
      _leftEmpty ??= [for (final c in leftBatch.columns) c.slice(0, 0)];
      final leftKeyColumns = _keys(leftBatch, leftKeys, leftQualifiers);
      final indices = <int>[];
      for (int i = 0; i < leftBatch.rowCount; i++) {
        final k = _keyAt(leftKeyColumns, _leftScales, i);
        if (_emitLeft(k == null, k != null && buildKeys.contains(k))) {
          indices.add(i);
        }
      }
      if (indices.isEmpty) continue;
      if (max != null && _probeEmitted + indices.length > max) {
        _probeOverflow(max);
      }
      _probeEmitted += indices.length;
      yield _assembleLeft(leftBatch.columns, indices);
    }
  }

  /// Emit every left row (a semi/anti join's schema is the left schema). Used for
  /// `x NOT IN (empty)`, which is TRUE for every row.
  Stream<RecordBatch> _emitAllLeft(Stream<RecordBatch> leftStream) async* {
    final max = options.maxOutputRows;
    await for (final leftBatch in leftStream) {
      _leftEmpty ??= [for (final c in leftBatch.columns) c.slice(0, 0)];
      if (leftBatch.rowCount == 0) continue;
      if (max != null && _probeEmitted + leftBatch.rowCount > max) {
        _probeOverflow(max);
      }
      _probeEmitted += leftBatch.rowCount;
      yield RecordBatch(
        columns: leftBatch.columns,
        schema: _schema,
        rowCount: leftBatch.rowCount,
      );
    }
  }

  /// Gather the left columns at [indices] into an output batch (the semi/anti
  /// output is the left schema; no right column is emitted).
  RecordBatch _assembleLeft(List<Column> leftColumns, List<int> indices) {
    final leftSelection = Int32List.fromList(indices);
    return RecordBatch(
      columns: [for (final c in leftColumns) Kernels.gather(c, leftSelection)],
      schema: _schema,
      rowCount: indices.length,
    );
  }

  /// Memory-bounded join for a partition that still exceeds [buildBudget] at the
  /// recursion cap (irreducible single-key skew: salting can't split it). Reads
  /// the re-readable build run in blocks of ≤ [buildBudget] and probes the whole
  /// (re-readable) probe run against each block via [_buildAndProbe]; matched
  /// pairs and per-block unmatched-RIGHT rows fall out of that call directly.
  ///
  /// Unmatched-LEFT rows (LEFT/FULL) can't be decided per block: a left row is
  /// globally unmatched only if NO block held its key. Rather than track a
  /// per-left-row flag (∝ probe size), we accumulate the set of distinct build
  /// keys and emit unmatched-left in one extra probe pass. That set is ~1 under
  /// genuine single-key skew and always smaller than materializing the build
  /// rows. Peak memory ≈ one block + one probe batch + the distinct-key set.
  Stream<RecordBatch> _blockNestedLoopJoin(
    SpillRun rightRun,
    SpillRun leftRun,
    int buildBudget,
  ) async* {
    final emitUnmatchedLeft =
        joinType == JoinType.left || joinType == JoinType.full;
    final seenKeys = emitUnmatchedLeft ? <Object?>{} : null;

    final block = <RecordBatch>[];
    var bytes = 0;
    await for (final b in rightRun.read()) {
      if (block.isNotEmpty && bytes + b.memoryUsage > buildBudget) {
        yield* _buildAndProbe(
          RecordBatch.concatAll(block, block.first.schema),
          leftRun.read(),
          forceUnmatchedLeft: false,
          collectKeys: seenKeys,
        );
        block.clear();
        bytes = 0;
      }
      block.add(b);
      bytes += b.memoryUsage;
    }
    if (block.isNotEmpty) {
      yield* _buildAndProbe(
        RecordBatch.concatAll(block, block.first.schema),
        leftRun.read(),
        forceUnmatchedLeft: false,
        collectKeys: seenKeys,
      );
    }

    if (seenKeys == null) return;
    // Extra pass: emit each probe row whose key is null or was never a build key.
    final rightColumns = _rightEmpty ?? _empty(right.schema);
    await for (final leftBatch in leftRun.read()) {
      _leftEmpty ??= [for (final c in leftBatch.columns) c.slice(0, 0)];
      final leftKeyColumns = _keys(leftBatch, leftKeys, leftQualifiers);
      final leftIndices = <int>[];
      final rightIndices = <int>[];
      for (int i = 0; i < leftBatch.rowCount; i++) {
        final k = _keyAt(leftKeyColumns, _leftScales, i);
        if (k == null || !seenKeys.contains(k)) {
          leftIndices.add(i);
          rightIndices.add(-1);
        }
      }
      if (leftIndices.isNotEmpty) {
        yield _assemble(
          leftBatch.columns,
          rightColumns,
          leftIndices,
          rightIndices,
        );
      }
    }
  }

  /// CROSS join. The left side is streamed batch-by-batch; the right (build) side
  /// is buffered under the memory budget and, if it doesn't fit, spilled to one
  /// re-readable Parquet run and re-scanned from disk for each left batch, so a
  /// large right input never has to be held in memory. The optional maxOutputRows
  /// guard is enforced as a running emitted-row count.
  Stream<RecordBatch> _crossJoin() async* {
    final memory = options.spills ? options.memory : null;
    final rightBuffer = <RecordBatch>[];
    var bytes = 0;
    SpillWriter? writer; // non-null once the right side overflows to disk
    RecordBatch? batch;

    while ((batch = await right.next()) != null) {
      _rightEmpty ??= [for (final c in batch!.columns) c.slice(0, 0)];
      final usage = batch!.memoryUsage;
      if (writer == null) {
        final localOver =
            options.spills && bytes + usage > options.spillThresholdBytes;
        final globalOver =
            !localOver && memory != null && !memory.reserve(usage);
        if (!localOver && !globalOver && memory != null) _reserved += usage;
        if (localOver || globalOver) {
          await Directory(_spillDir).create(recursive: true);
          writer = SpillWriter(
            _spillDir,
            _rightEmpty!.schemaOf(template: right.schema),
            budget: options.spillBudget,
          );
          _writers.add(writer);
          for (final b in rightBuffer) {
            await writer.add(b);
          }
          rightBuffer.clear();
          bytes = 0;
          memory?.release(_reserved); // buffered right rows are on disk now
          _reserved = 0;
          await writer.add(batch);
          continue;
        }
        rightBuffer.add(batch);
        bytes += usage;
      } else {
        await writer.add(batch);
      }
    }

    if (writer == null) {
      // In-memory fast path (unchanged): empty right → empty product.
      if (rightBuffer.isEmpty) {
        memory?.release(_reserved);
        _reserved = 0;
        return;
      }
      final rightBatch = RecordBatch.concatAll(rightBuffer, right.schema);
      memory?.release(_reserved);
      _reserved = 0;
      await for (final leftBatch in _streamOf(left)) {
        yield* _crossPair(leftBatch, rightBatch);
      }
      return;
    }

    // Spilled: re-scan the right run from disk for each left batch.
    final run = await writer.finish();
    _writers.remove(writer);
    _runs.add(run);
    await for (final leftBatch in _streamOf(left)) {
      await for (final rightSub in run.read()) {
        yield* _crossPair(leftBatch, rightSub);
      }
    }
  }

  /// Emit the product of one left batch and one right batch in batch-sized
  /// chunks, enforcing the running maxOutputRows guard.
  Stream<RecordBatch> _crossPair(
    RecordBatch leftBatch,
    RecordBatch right,
  ) async* {
    final max = options.maxOutputRows;
    final leftIndices = <int>[];
    final rightIndices = <int>[];

    Never overflow() =>
        throw Failure.of(Kind.limit, 'CROSS JOIN exceeds maxOutputRows=$max');

    for (int i = 0; i < leftBatch.rowCount; i++) {
      for (int j = 0; j < right.rowCount; j++) {
        leftIndices.add(i);
        rightIndices.add(j);
        if (leftIndices.length >= defaultBatchSize) {
          if (max != null && _crossEmitted + leftIndices.length > max) {
            overflow();
          }
          _crossEmitted += leftIndices.length;
          yield _assemble(
            leftBatch.columns,
            right.columns,
            leftIndices,
            rightIndices,
          );
          leftIndices.clear();
          rightIndices.clear();
        }
      }
    }
    if (leftIndices.isNotEmpty) {
      if (max != null && _crossEmitted + leftIndices.length > max) overflow();
      _crossEmitted += leftIndices.length;
      yield _assemble(
        leftBatch.columns,
        right.columns,
        leftIndices,
        rightIndices,
      );
    }
  }

  /// Gather left+right columns at the paired indices into one output batch
  /// (negative index -> null), dropping the de-duplicated right key column.
  RecordBatch _assemble(
    List<Column> leftColumns,
    List<Column> rightColumns,
    List<int> leftIndices,
    List<int> rightIndices,
  ) {
    final leftSelection = Int32List.fromList(leftIndices);
    final rightSelection = Int32List.fromList(rightIndices);
    final out = <Column>[
      for (final (i, c) in leftColumns.indexed)
        switch (_merged[i]) {
          final int j => _coalesce(
            Kernels.gather(c, leftSelection),
            Kernels.gather(rightColumns[j], rightSelection),
          ),
          _ => Kernels.gather(c, leftSelection),
        },
      for (int j = 0; j < rightColumns.length; j++)
        if (!_drop.contains(j)) Kernels.gather(rightColumns[j], rightSelection),
    ];
    return RecordBatch(
      columns: out,
      schema: _schema,
      rowCount: leftIndices.length,
    );
  }

  /// Left key index to right key index for each key kept once (see
  /// [PhysicalHashJoin.dropIndices]) whose two columns hold values in the
  /// same form: same type and, for decimals, the same scale.
  Map<int, int> _twins(Schema leftSchema, Schema rightSchema) {
    final twins = <int, int>{};
    for (int j = 0; j < leftKeys.length; j++) {
      if (leftKeys[j] != rightKeys[j] ||
          leftQualifiers[j] != rightQualifiers[j]) {
        continue;
      }
      final l = leftSchema.resolve(leftKeys[j], qualifier: leftQualifiers[j]);
      final r = rightSchema.resolve(
        rightKeys[j],
        qualifier: rightQualifiers[j],
      );
      if (l < 0 || r < 0) continue;
      final a = leftSchema.fields[l];
      final b = rightSchema.fields[r];
      if (a.dataType == b.dataType && a.scale == b.scale) twins[l] = r;
    }
    return twins;
  }

  /// [left] where it has a value, else [right], keeping [left]'s name and type.
  static Column _coalesce(Column left, Column right) => Kernels.fromValues(
    left.name,
    left.dataType,
    [for (var i = 0; i < left.length; i++) left[i] ?? right[i]],
  );

  /// Route a batch's rows to partition spill files by join-key hash (null keys
  /// hash consistently, so null-left and null-right co-locate and are handled by
  /// [_buildAndProbe] exactly like the in-memory join). Spill files derive their
  /// schema from the actual column dataTypes, keeping each batch self-consistent.
  /// [scales] MUST be the scales of the side [keys] belong to (see [scaledKey]).
  /// Routing and matching have to key identically, or two equal composite/decimal
  /// keys land in different partitions and are never compared. [_keyAt] has
  /// already canonicalized, so [partitionOf] is passed the finished key.
  Future<void> _route(
    RecordBatch batch,
    List<SpillWriter> parts,
    int partitions,
    List<String> keys,
    List<String?> qualifiers,
    List<int?> scales, {
    int salt = 0,
  }) async {
    final keyColumns = _keys(batch, keys, qualifiers);
    // A null key (any null part) hashes consistently, so null-left and
    // null-right co-locate.
    await route(
      batch,
      parts,
      (row) => _keyAt(keyColumns, scales, row),
      salt: salt,
      schema: batch.columns.schemaOf(template: batch.schema),
    );
  }

  List<Column> _empty(Schema s) => [
    for (final f in s.fields) Kernels.fromValues(f.name, f.dataType, const []),
  ];

  Stream<RecordBatch> _streamOf(RecordBatchIterator iterator) async* {
    RecordBatch? b;
    while ((b = await iterator.next()) != null) {
      _leftEmpty ??= [for (final c in b!.columns) c.slice(0, 0)];
      yield b!;
    }
  }

  @override
  Future<void> close() async {
    try {
      await closeAll([
        // Stop the output first: a consumer that stopped early (LIMIT, a
        // cancelled stream) leaves it suspended mid-read, holding a spill
        // file open and local reservations its finally blocks return.
        () async => _output?.cancel(),
        () => left.close(),
        () => right.close(),
        () async {
          options.memory?.release(_reserved); // return any held reservation
          _reserved = 0;
        },
        // Abort writers still open (cancel/throw during partitioning) + delete.
        for (final writer in _writers) () => writer.abort(),
        for (final run in _runs) () => run.delete(),
      ]);
    } finally {
      _writers.clear();
    }
  }
}
