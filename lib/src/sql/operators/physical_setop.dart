part of 'physical_operators.dart';

/// Physical set operation (UNION / UNION ALL / INTERSECT / EXCEPT) with full-row
/// value-equality semantics. UNION ALL streams; the deduplicating variants buffer
/// under the memory budget and, once it is exceeded, grace-partition both inputs
/// to disk (recursively re-partitioning a skewed partition) so a large set op
/// never has to hold both inputs in memory at once.
class PhysicalSetOperation extends PhysicalPlan {
  final PhysicalPlan left;
  final PhysicalPlan right;
  final SetOpType type;
  final bool all;
  final EngineOptions options;

  final Schema _schema;
  // The branches' own schemas, carrying the REAL decimal scales the output
  // schema reconciled away — the iterator restates each branch FROM these.
  final Schema leftSchema;
  final Schema rightSchema;

  PhysicalSetOperation({
    required this.left,
    required this.right,
    required this.type,
    required this.all,
    required Schema schema,
    required this.leftSchema,
    required this.rightSchema,
    this.options = const EngineOptions(),
  }) : _schema = schema;

  @override
  Schema get schema => _schema;

  @override
  int? get estimatedRows => null;

  @override
  RecordBatchIterator execute() => SetOpIterator(
    left.execute(),
    right.execute(),
    schema,
    type,
    all,
    options,
    leftSchema,
    rightSchema,
  );
}

class SetOpIterator extends RecordBatchIterator {
  final RecordBatchIterator left;
  final RecordBatchIterator right;
  @override
  final Schema schema;
  final SetOpType type;
  final bool all;
  final EngineOptions options;
  // Per-branch source schemas — the origin scales a decimal column is restated
  // FROM. The output [schema] holds the reconciled (widest) scale it is restated
  // TO. Both are needed because the whole defect is that the two can differ.
  final Schema leftSchema;
  final Schema rightSchema;

  static const _fanout = 16;
  static const _maxDepth = 6;

  /// Whether the distinct-key set's estimated resident BYTES (not a flat
  /// per-key count) exceed the spill threshold — the set, not the input batches,
  /// is the peak here, and a wide string/binary/nested key costs far more than a
  /// fixed slot. Uses the shared [rowKeyBytes] estimator.
  bool _bytesOverBudget(int bytes) => bytes > options.spillThresholdBytes;

  final List<SpillRun> _runs = [];
  final List<SpillWriter> _writers = [];
  int _reserved = 0;
  int _emitted = 0;
  StreamIterator<RecordBatch>? _output;

  SetOpIterator(
    this.left,
    this.right,
    this.schema,
    this.type,
    this.all,
    this.options,
    this.leftSchema,
    this.rightSchema,
  );

  String get _spillDir => options.spillDir ?? Directory.systemTemp.path;

  /// Align an incoming batch from [source] to the reconciled output [schema]:
  /// cast each column to the common type (int-width / int→float promotion),
  /// restate a decimal from its source scale to the output's, and stamp the left
  /// names, so every downstream concat/dedup/emit sees uniform values.
  RecordBatch _coerce(RecordBatch b, Schema source) {
    final fields = schema.fields;
    final columns = <Column>[
      for (int i = 0; i < fields.length; i++)
        _align(b.columns[i], source.fields[i], fields[i]),
    ];
    return RecordBatch(columns: columns, schema: schema, rowCount: b.rowCount);
  }

  Column _align(Column c, Field from, Field to) {
    // Restate to a decimal output at its scale. A decimal→decimal `castTo` is a
    // no-op (same dataType), so scale alignment must happen here (1.5 is
    // BigInt(15) at scale 1, BigInt(150) at scale 2); an INTEGER branch first
    // casts to a scale-0 decimal, then both restate to `to.scale` so `5` becomes
    // BigInt(500) at scale 2 and matches `5.00` (not `0.05`).
    if (to.dataType == DataType.decimal && to.scale != null) {
      final fromScale = from.dataType == DataType.decimal
          ? from.scale
          : (from.dataType.isInteger ? 0 : null);
      if (fromScale != null) {
        if (from.dataType != DataType.decimal) c = c.castTo(DataType.decimal);
        if (fromScale != to.scale) c = c.rescaleDecimal(fromScale, to.scale!);
        if (c.name != to.name) c = c.withName(to.name);
        return c;
      }
    }
    if (c.dataType != to.dataType) c = c.castTo(to.dataType);
    if (c.name != to.name) c = c.withName(to.name);
    return c;
  }

  @override
  Future<RecordBatch?> next() async {
    final output = _output ??= StreamIterator(_generate());
    return (await output.moveNext()) ? output.current : null;
  }

  /// Enforce maxOutputRows on the running emitted-row count (so the guard is
  /// effective without first materialising the whole result).
  RecordBatch _emit(RecordBatch b) {
    _emitted += b.rowCount;
    final max = options.maxOutputRows;
    if (max != null && _emitted > max) {
      throw StateError(
        'Set operation produced more than maxOutputRows=$max rows',
      );
    }
    return b;
  }

  Stream<RecordBatch> _generate() async* {
    // UNION ALL keeps every row: pure streaming, no buffering.
    if (type == SetOpType.union && all) {
      RecordBatch? b;
      while ((b = await left.next()) != null) {
        yield _emit(_coerce(b!, leftSchema));
      }
      while ((b = await right.next()) != null) {
        yield _emit(_coerce(b!, rightSchema));
      }
      return;
    }

    // Buffer both sides under the shared budget. If we stay under, combine in
    // memory (unchanged fast path); otherwise grace-partition to disk.
    final mem = options.spills ? options.memory : null;
    final leftBuf = <RecordBatch>[];
    final rightBuf = <RecordBatch>[];
    var bytes = 0;

    Future<bool> fill(
      RecordBatchIterator iter,
      List<RecordBatch> buffer,
      Schema source,
    ) async {
      RecordBatch? b;
      while ((b = await iter.next()) != null) {
        b = _coerce(b!, source);
        final mu = b.memoryUsage;
        // Reserve BEFORE retaining the batch, so a hard cap sees the buffer
        // grow only after the reservation accounted for it.
        final granted = mem == null || mem.reserve(mu);
        buffer.add(b);
        bytes += mu;
        if (granted && mem != null) _reserved += mu;
        if ((options.spills && bytes > options.spillThresholdBytes) ||
            !granted) {
          return true; // over budget — caller switches to the spill path
        }
      }
      return false;
    }

    final overLeft = await fill(left, leftBuf, leftSchema);
    final overRight = overLeft
        ? true
        : await fill(right, rightBuf, rightSchema);

    // Take the in-memory fast path only if the concat transient fits.
    // `concatAll` builds a second copy (~[bytes]) while the input buffers are
    // still live, so the peak is inputs + copies. Reserve that transient up
    // front; under a hard cap that can't fit it, fall through to the on-disk
    // partition path rather than overshooting the advertised ceiling.
    if (!overLeft && !overRight && (mem == null || mem.reserve(bytes))) {
      if (mem != null) _reserved += bytes;
      final l = RecordBatch.concatAll(leftBuf, schema);
      final r = RecordBatch.concatAll(rightBuf, schema);
      // The concatenated batches now hold the data; free the input buffers and
      // release THEIR reservation. The copies' reservation ([bytes]) stays held
      // — they, the distinct-key set (charged in _distinct/_keys) and the
      // gathered output are all resident through _inMemory; close() releases it.
      leftBuf.clear();
      rightBuf.clear();
      if (mem != null) {
        mem.release(_reserved - bytes); // the input portion is now freed
        _reserved = bytes; // keep the live copies' reservation for close()
      }
      yield* _inMemory(l, r);
      return;
    }

    // Over budget: partition both inputs by full-row hash and process pairwise.
    final leftRuns = await _partition(
      leftBuf,
      left,
      overLeft,
      _fanout,
      0,
      leftSchema,
    );
    final rightRuns = await _partition(
      rightBuf,
      right,
      overRight,
      _fanout,
      0,
      rightSchema,
    );
    // The buffers are on disk now; drop the references before releasing so the
    // release matches the memory actually freed (mirrors the hash join).
    leftBuf.clear();
    rightBuf.clear();
    mem?.release(_reserved);
    _reserved = 0;
    for (int p = 0; p < _fanout; p++) {
      yield* _processPartition(leftRuns[p], rightRuns[p], 0);
    }
  }

  /// Route [buffered] (and, when [hasRest], the remainder of [iter]) into [parts]
  /// on-disk runs by salted full-row hash. Equal rows always co-locate, so each
  /// partition can be deduplicated independently. Writers derive a self-consistent
  /// schema from the actual column types.
  Future<List<SpillRun>> _partition(
    List<RecordBatch> buffered,
    RecordBatchIterator? iter,
    bool hasRest,
    int parts,
    int salt,
    Schema source,
  ) async {
    await Directory(_spillDir).create(recursive: true);
    List<SpillWriter>? writers;

    Future<void> route(RecordBatch b) async {
      if (b.rowCount == 0) return;
      writers ??= _makeWriters(b, parts);
      final buckets = [for (var i = 0; i < parts; i++) <int>[]];
      for (int i = 0; i < b.rowCount; i++) {
        buckets[partitionOf(_keyOf(b, i), parts, salt: salt)].add(i);
      }
      for (int p = 0; p < parts; p++) {
        if (buckets[p].isNotEmpty) {
          await writers![p].add(b.gather(Int32List.fromList(buckets[p])));
        }
      }
    }

    // [buffered] is already coerced (fill did it); the un-read remainder is raw,
    // so coerce it too or its decimals keep the source scale and never match the
    // coerced rows they should dedup against.
    for (final b in buffered) {
      await route(b);
    }
    if (hasRest && iter != null) {
      RecordBatch? b;
      while ((b = await iter.next()) != null) {
        await route(_coerce(b!, source));
      }
    }

    if (writers == null) {
      // The side was empty: every partition is an empty (file-less) run.
      return [
        for (var i = 0; i < parts; i++)
          SpillRun('$_spillDir/_empty', schema, hasFile: false),
      ];
    }
    final runs = [for (final w in writers!) await w.finish()];
    _runs.addAll(runs);
    for (final w in writers!) {
      _writers.remove(w);
    }
    return runs;
  }

  List<SpillWriter> _makeWriters(RecordBatch sample, int parts) {
    final real = sample.columns.schemaOf(template: sample.schema);
    final ws = [
      for (var i = 0; i < parts; i++)
        SpillWriter(_spillDir, real, budget: options.spillBudget),
    ];
    _writers.addAll(ws);
    return ws;
  }

  /// Combine one pair of partitions WITHOUT materialising either side. The
  /// bounding structure is the distinct-key set (built by streaming the runs),
  /// not the input rows — so a duplicate-heavy partition (few distinct rows, huge
  /// multiplicity) costs one [RowKey] per distinct row instead of blowing up
  /// `lbatches`/`rbatches`. Runs are re-readable, so we scan them again to size
  /// the recursion decision and to emit. If still over budget and we can recurse,
  /// re-partition with a fresh salt to break skew.
  Stream<RecordBatch> _processPartition(
    SpillRun lrun,
    SpillRun rrun,
    int depth,
  ) async* {
    final canRecurse = depth < _maxDepth && options.spills;
    final mem = options.spills ? options.memory : null;

    if (type == SetOpType.union) {
      // Distinct UNION: one seen-set across BOTH runs; batches discarded as read.
      // Bound the distinct-key set WHILE inserting — the moment its estimated
      // bytes exceed the budget (or the shared MemoryManager denies), stop and
      // repartition (breaking skew). At the recursion cap this can't repartition
      // further, so it FAILS LOUD instead of growing unbounded.
      final seen = <RowKey>{};
      var seenBytes = 0;
      var reserved = 0;
      var over = false;
      try {
        for (final run in [lrun, rrun]) {
          if (over) break;
          await for (final b in run.read()) {
            var globalOver = false;
            for (int i = 0; i < b.rowCount; i++) {
              final k = _keyOf(b, i);
              if (seen.add(k)) {
                final kb = rowKeyBytes(k);
                seenBytes += kb;
                final granted = mem == null || mem.reserve(kb);
                if (granted && mem != null) reserved += kb;
                if (!granted) globalOver = true;
              }
            }
            if (_bytesOverBudget(seenBytes) || globalOver) {
              if (!canRecurse) _keyBudgetExceeded();
              over = true;
              break;
            }
          }
        }
        if (over) {
          // The set is discarded before recursing — release its budget so it
          // isn't double-counted against the sub-partitions' sets.
          seen.clear();
          if (mem != null && reserved > 0) {
            mem.release(reserved);
            reserved = 0;
          }
          final salt = depth + 1;
          final lr = await _repartitionRun(lrun, _fanout, salt);
          final rr = await _repartitionRun(rrun, _fanout, salt);
          for (int p = 0; p < _fanout; p++) {
            yield* _processPartition(lr[p], rr[p], depth + 1);
          }
        } else {
          // Emit each row the first time its key is seen (`seen.remove` dedups and
          // yields first-occurrence, left-then-right, matching the old _distinct).
          yield* _emitFirst(lrun, seen);
          yield* _emitFirst(rrun, seen);
        }
      } finally {
        if (mem != null && reserved > 0) mem.release(reserved);
      }
    } else {
      // INTERSECT / EXCEPT: build the right key-set (budget-bounded), then stream
      // the left. If the key set would exceed the budget, repartition first; at
      // the recursion cap, fail loud rather than grow unbounded.
      final rightKeys = <RowKey>{};
      var rightBytes = 0;
      var reserved = 0;
      var over = false;
      try {
        await for (final b in rrun.read()) {
          var globalOver = false;
          for (int i = 0; i < b.rowCount; i++) {
            final k = _keyOf(b, i);
            if (rightKeys.add(k)) {
              final kb = rowKeyBytes(k);
              rightBytes += kb;
              final granted = mem == null || mem.reserve(kb);
              if (granted && mem != null) reserved += kb;
              if (!granted) globalOver = true;
            }
          }
          if (_bytesOverBudget(rightBytes) || globalOver) {
            if (!canRecurse) _keyBudgetExceeded();
            over = true;
            break;
          }
        }
        if (over) {
          rightKeys.clear();
          if (mem != null && reserved > 0) {
            mem.release(reserved);
            reserved = 0;
          }
          final salt = depth + 1;
          final lr = await _repartitionRun(lrun, _fanout, salt);
          final rr = await _repartitionRun(rrun, _fanout, salt);
          for (int p = 0; p < _fanout; p++) {
            yield* _processPartition(lr[p], rr[p], depth + 1);
          }
        } else {
          final want = type == SetOpType.intersect;
          // The output dedup set grows with the DISTINCT OUTPUT, so it needs the
          // same bound-or-repartition treatment as the two input key sets above
          // — its check was gated on `!canRecurse`, which is false at every
          // depth an ordinary query reaches, so it never fired and the set grew
          // without limit.
          //
          // It is built in a FIRST pass rather than while emitting: recursion is
          // only a recovery while nothing has been yielded, and repartitioning
          // after a partial emit would duplicate rows. Deduping per sub-
          // partition is correct because `partitionOf` routes by KEY, so every
          // row sharing a key lands in one partition. The second pass costs one
          // extra read of an already-spilled run and consumes the set as it goes
          // (`remove` emits a key's first occurrence and shrinks the set).
          final emitted = <RowKey>{};
          var emittedBytes = 0;
          var emittedOver = false;
          await for (final b in lrun.read()) {
            for (int i = 0; i < b.rowCount; i++) {
              final k = _keyOf(b, i);
              if (rightKeys.contains(k) == want && emitted.add(k)) {
                final kb = rowKeyBytes(k);
                emittedBytes += kb;
                final granted = mem == null || mem.reserve(kb);
                if (granted && mem != null) reserved += kb;
                // Weigh the TOTAL resident bytes (right key set + emitted set):
                // a small right side must not let a huge left output through.
                if (!granted || _bytesOverBudget(rightBytes + emittedBytes)) {
                  if (!canRecurse) _emittedBudgetExceeded();
                  emittedOver = true;
                  break;
                }
              }
            }
            if (emittedOver) break;
          }

          if (emittedOver) {
            emitted.clear();
            rightKeys.clear();
            if (mem != null && reserved > 0) {
              mem.release(reserved);
              reserved = 0;
            }
            final salt = depth + 1;
            final lr = await _repartitionRun(lrun, _fanout, salt);
            final rr = await _repartitionRun(rrun, _fanout, salt);
            for (int p = 0; p < _fanout; p++) {
              yield* _processPartition(lr[p], rr[p], depth + 1);
            }
          } else {
            await for (final b in lrun.read()) {
              final index = <int>[];
              for (int i = 0; i < b.rowCount; i++) {
                // `remove` is the dedup: only a key's FIRST occurrence is still
                // present, and the set shrinks as the output drains.
                if (emitted.remove(_keyOf(b, i))) index.add(i);
              }
              if (index.isNotEmpty) yield _stamp(b, index);
            }
          }
        }
      } finally {
        if (mem != null && reserved > 0) mem.release(reserved);
      }
    }
    await lrun.delete();
    await rrun.delete();
  }

  Never _emittedBudgetExceeded() => throw StateError(
    'Set operation output dedup set exceeds the '
    '${options.spillThresholdBytes}-byte budget. Unlike the input key sets this '
    'cannot be recovered by repartitioning — rows have already been emitted — '
    'so raise spillThresholdBytes / memoryBudgetBytes, or reduce the distinct '
    'output size.',
  );

  Never _keyBudgetExceeded() => throw StateError(
    'Set operation key set exceeds the ${options.spillThresholdBytes}-byte '
    'budget at the maximum repartition depth ($_maxDepth); the input is too '
    'skewed to bound in memory',
  );

  /// Re-partition a re-readable [run] into [parts] salted sub-runs (the recursion
  /// step; mirrors [_partition]'s routing but sources from disk, holding only one
  /// batch at a time).
  Future<List<SpillRun>> _repartitionRun(
    SpillRun run,
    int parts,
    int salt,
  ) async {
    await Directory(_spillDir).create(recursive: true);
    List<SpillWriter>? writers;
    await for (final b in run.read()) {
      if (b.rowCount == 0) continue;
      writers ??= _makeWriters(b, parts);
      final buckets = [for (var i = 0; i < parts; i++) <int>[]];
      for (int i = 0; i < b.rowCount; i++) {
        buckets[partitionOf(_keyOf(b, i), parts, salt: salt)].add(i);
      }
      for (int p = 0; p < parts; p++) {
        if (buckets[p].isNotEmpty) {
          await writers[p].add(b.gather(Int32List.fromList(buckets[p])));
        }
      }
    }
    if (writers == null) {
      return [
        for (var i = 0; i < parts; i++)
          SpillRun('$_spillDir/_empty', schema, hasFile: false),
      ];
    }
    final runs = [for (final w in writers) await w.finish()];
    _runs.addAll(runs);
    for (final w in writers) {
      _writers.remove(w);
    }
    return runs;
  }

  /// Stream [run], emitting each row whose key is still in [seen] (removing it so
  /// each distinct key is emitted exactly once across successive calls).
  Stream<RecordBatch> _emitFirst(SpillRun run, Set<RowKey> seen) async* {
    await for (final b in run.read()) {
      final index = <int>[];
      for (int i = 0; i < b.rowCount; i++) {
        if (seen.remove(_keyOf(b, i))) index.add(i);
      }
      if (index.isNotEmpty) yield _stamp(b, index);
    }
  }

  /// Gather [index] rows of [b] and re-label to the planner [schema] (matching the
  /// old in-memory combine, which emitted under the planner schema).
  RecordBatch _stamp(RecordBatch b, List<int> index) {
    final g = b.gather(Int32List.fromList(index));
    return _emit(
      RecordBatch(columns: g.columns, schema: schema, rowCount: g.rowCount),
    );
  }

  /// In-memory combine of one (sub-)partition or the whole inputs. A side with no
  /// rows arrives as a column-less batch (from `concatAll([])`); handle those
  /// explicitly so they are never fed to `concat` (which keys width off the first
  /// batch and would drop columns).
  Stream<RecordBatch> _inMemory(RecordBatch l, RecordBatch r) async* {
    final lEmpty = l.columns.isEmpty;
    final rEmpty = r.columns.isEmpty;
    switch (type) {
      case SetOpType.union: // distinct (UNION ALL handled in _generate)
        if (lEmpty && rEmpty) return;
        if (lEmpty) {
          yield _emit(_distinct(r, (_) => true));
        } else if (rEmpty) {
          yield _emit(_distinct(l, (_) => true));
        } else {
          final combined = RecordBatch.concatAll([l, r], schema);
          yield _emit(_distinct(combined, (_) => true));
        }
      case SetOpType.intersect:
        if (lEmpty || rEmpty) return; // intersect with nothing → nothing
        final rightKeys = _keys(r);
        yield _emit(_distinct(l, rightKeys.contains));
      case SetOpType.except:
        if (lEmpty) return; // nothing on the left → nothing
        if (rEmpty) {
          yield _emit(
            _distinct(l, (_) => true),
          ); // except nothing → distinct(l)
        } else {
          final rightKeys = _keys(r);
          yield _emit(_distinct(l, (k) => !rightKeys.contains(k)));
        }
    }
  }

  RowKey _keyOf(RecordBatch batch, int row) =>
      RowKey([for (final c in batch.columns) c[row]]);

  /// Charge one newly-distinct key's estimated bytes to the shared budget, so the
  /// in-memory dedup set is metered the way the spill path's is (physical_setop
  /// spill path). Held on [_reserved] and released in close(); fail loud when the
  /// shared pool is exhausted rather than grow the set unbounded. No-op when the
  /// memory manager is off (the caller opted out of bounding).
  void _chargeKey(RowKey key) {
    final mem = options.spills ? options.memory : null;
    if (mem == null) return;
    final kb = rowKeyBytes(key);
    if (!mem.reserve(kb)) {
      throw StateError(
        'Set operation exceeded the shared memory budget building its '
        'distinct-key set',
      );
    }
    _reserved += kb;
  }

  Set<RowKey> _keys(RecordBatch batch) {
    final keys = <RowKey>{};
    for (int i = 0; i < batch.rowCount; i++) {
      final k = _keyOf(batch, i);
      if (keys.add(k)) _chargeKey(k);
    }
    return keys;
  }

  /// Distinct rows of [batch] (first occurrence) passing [keep].
  RecordBatch _distinct(RecordBatch batch, bool Function(RowKey) keep) {
    final seen = <RowKey>{};
    final indices = <int>[];
    for (int i = 0; i < batch.rowCount; i++) {
      final key = _keyOf(batch, i);
      if (keep(key) && seen.add(key)) {
        _chargeKey(key);
        indices.add(i);
      }
    }
    return batch.gather(Int32List.fromList(indices));
  }

  @override
  Future<void> close() async {
    try {
      await closeAll([
        () => left.close(),
        () => right.close(),
        () async {
          options.memory?.release(_reserved);
          _reserved = 0;
        },
        for (final w in _writers) () => w.abort(),
        for (final r in _runs) () => r.delete(),
      ]);
    } finally {
      _writers.clear();
    }
  }
}
