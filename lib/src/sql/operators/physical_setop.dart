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
  // schema reconciled away: the iterator restates each branch FROM these.
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
  // Per-branch source schemas: the origin scales a decimal column is restated
  // FROM. The output [schema] holds the reconciled (widest) scale it is restated
  // TO. Both are needed because the whole defect is that the two can differ.
  final Schema leftSchema;
  final Schema rightSchema;

  static const _fanout = 16;
  static const _maxDepth = 6;

  /// Whether the distinct-key set's estimated resident BYTES (not a flat
  /// per-key count) exceed the spill threshold: the set, not the input batches,
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

  RecordBatch _coerce(RecordBatch b, Schema source) =>
      _alignBatch(b, source, schema);

  @override
  Future<RecordBatch?> next() async {
    await pause();
    final output = _output ??= StreamIterator(_generate());
    return (await output.moveNext()) ? output.current : null;
  }

  /// Enforce maxOutputRows on the running emitted-row count (so the guard is
  /// effective without first materialising the whole result).
  RecordBatch _emit(RecordBatch b) {
    _emitted += b.rowCount;
    final max = options.maxOutputRows;
    if (max != null && _emitted > max) {
      throw Failure.of(
        Kind.limit,
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
    final memory = options.spills ? options.memory : null;
    final leftBuffer = <RecordBatch>[];
    final rightBuffer = <RecordBatch>[];
    var bytes = 0;

    Future<bool> fill(
      RecordBatchIterator iterator,
      List<RecordBatch> buffer,
      Schema source,
    ) async {
      RecordBatch? b;
      while ((b = await iterator.next()) != null) {
        b = _coerce(b!, source);
        final usage = b.memoryUsage;
        // Reserve BEFORE retaining the batch, so a hard cap sees the buffer
        // grow only after the reservation accounted for it.
        final granted = memory == null || memory.reserve(usage);
        buffer.add(b);
        bytes += usage;
        if (granted && memory != null) _reserved += usage;
        if ((options.spills && bytes > options.spillThresholdBytes) ||
            !granted) {
          return true; // over budget: caller switches to the spill path
        }
      }
      return false;
    }

    final overLeft = await fill(left, leftBuffer, leftSchema);
    final overRight = overLeft
        ? true
        : await fill(right, rightBuffer, rightSchema);

    // Take the in-memory fast path only if the concat transient fits.
    // `concatAll` builds a second copy (~[bytes]) while the input buffers are
    // still live, so the peak is inputs + copies. Reserve that transient up
    // front; under a hard cap that can't fit it, fall through to the on-disk
    // partition path rather than overshooting the advertised ceiling.
    if (!overLeft && !overRight && (memory == null || memory.reserve(bytes))) {
      if (memory != null) _reserved += bytes;
      final l = RecordBatch.concatAll(leftBuffer, schema);
      final r = RecordBatch.concatAll(rightBuffer, schema);
      // The concatenated batches now hold the data; free the input buffers and
      // release THEIR reservation. The copies' reservation ([bytes]) stays held;
      // they, the distinct-key set (charged in _distinct/_keys) and the
      // gathered output are all resident through _inMemory; close() releases it.
      leftBuffer.clear();
      rightBuffer.clear();
      if (memory != null) {
        memory.release(_reserved - bytes); // the input portion is now freed
        _reserved = bytes; // keep the live copies' reservation for close()
      }
      yield* _inMemory(l, r);
      return;
    }

    // Over budget: partition both inputs by full-row hash and process pairwise.
    final leftRuns = await _partition(
      leftBuffer,
      left,
      overLeft,
      _fanout,
      0,
      leftSchema,
    );
    final rightRuns = await _partition(
      rightBuffer,
      right,
      overRight,
      _fanout,
      0,
      rightSchema,
    );
    // The buffers are on disk now; drop the references before releasing so the
    // release matches the memory actually freed (mirrors the hash join).
    leftBuffer.clear();
    rightBuffer.clear();
    memory?.release(_reserved);
    _reserved = 0;
    for (int p = 0; p < _fanout; p++) {
      yield* _processPartition(leftRuns[p], rightRuns[p], 0);
    }
  }

  /// Route [buffered] (and, when [hasRest], the remainder of [iterator]) into
  /// [parts] on-disk runs by salted full-row hash. Equal rows always co-locate,
  /// so each partition can be deduplicated independently. Writers derive a
  /// self-consistent schema from the actual column types.
  Future<List<SpillRun>> _partition(
    List<RecordBatch> buffered,
    RecordBatchIterator? iterator,
    bool hasRest,
    int parts,
    int salt,
    Schema source,
  ) async {
    await Directory(_spillDir).create(recursive: true);
    List<SpillWriter>? writers;

    Future<void> send(RecordBatch b) async {
      if (b.rowCount == 0) return;
      await route(
        b,
        writers ??= _makeWriters(b, parts),
        (row) => _keyOf(b, row),
        salt: salt,
      );
    }

    // [buffered] is already coerced (fill did it); the un-read remainder is raw,
    // so coerce it too or its decimals keep the source scale and never match the
    // coerced rows they should dedup against.
    for (final b in buffered) {
      await send(b);
    }
    if (hasRest && iterator != null) {
      RecordBatch? b;
      while ((b = await iterator.next()) != null) {
        await send(_coerce(b!, source));
      }
    }

    if (writers == null) {
      // The side was empty: every partition is an empty (file-less) run.
      return [
        for (var i = 0; i < parts; i++)
          SpillRun('$_spillDir/_empty', schema, hasFile: false),
      ];
    }
    final runs = await _finish(writers!);
    return runs;
  }

  List<SpillWriter> _makeWriters(RecordBatch sample, int parts) {
    final real = sample.columns.schemaOf(template: sample.schema);
    final writers = [
      for (var i = 0; i < parts; i++)
        SpillWriter(_spillDir, real, budget: options.spillBudget),
    ];
    _writers.addAll(writers);
    return writers;
  }

  /// Combine one pair of partitions WITHOUT materialising either side. The
  /// bounding structure is the distinct-key set (built by streaming the runs),
  /// not the input rows, so a duplicate-heavy partition (few distinct rows, huge
  /// multiplicity) costs one [RowKey] per distinct row instead of blowing up
  /// `lbatches`/`rbatches`. Runs are re-readable, so we scan them again to size
  /// the recursion decision and to emit. If still over budget and we can recurse,
  /// re-partition with a fresh salt to break skew.
  Stream<RecordBatch> _processPartition(
    SpillRun leftRun,
    SpillRun rightRun,
    int depth,
  ) async* {
    final canRecurse = depth < _maxDepth && options.spills;
    final memory = options.spills ? options.memory : null;

    if (type == SetOpType.union) {
      // Distinct UNION: one seen-set across BOTH runs; batches discarded as read.
      // Bound the distinct-key set WHILE inserting: the moment its estimated
      // bytes exceed the budget (or the shared MemoryManager denies), stop and
      // repartition (breaking skew). At the recursion cap this can't repartition
      // further, so it FAILS LOUD instead of growing unbounded.
      final seen = <RowKey>{};
      var seenBytes = 0;
      var reserved = 0;
      var over = false;
      try {
        for (final run in [leftRun, rightRun]) {
          if (over) break;
          await for (final b in run.read()) {
            var globalOver = false;
            for (int i = 0; i < b.rowCount; i++) {
              final k = _keyOf(b, i);
              if (seen.add(k)) {
                final size = rowKeyBytes(k);
                seenBytes += size;
                final granted = memory == null || memory.reserve(size);
                if (granted && memory != null) reserved += size;
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
          // The set is discarded before recursing: release its budget so it
          // isn't double-counted against the sub-partitions' sets.
          seen.clear();
          if (memory != null && reserved > 0) {
            memory.release(reserved);
            reserved = 0;
          }
          final salt = depth + 1;
          final leftRuns = await _repartitionRun(leftRun, _fanout, salt);
          final rightRuns = await _repartitionRun(rightRun, _fanout, salt);
          for (int p = 0; p < _fanout; p++) {
            yield* _processPartition(leftRuns[p], rightRuns[p], depth + 1);
          }
        } else {
          // Emit each row the first time its key is seen (`seen.remove` dedups and
          // yields first-occurrence, left-then-right, matching the old _distinct).
          yield* _emitFirst(leftRun, seen);
          yield* _emitFirst(rightRun, seen);
        }
      } finally {
        if (memory != null && reserved > 0) memory.release(reserved);
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
        await for (final b in rightRun.read()) {
          var globalOver = false;
          for (int i = 0; i < b.rowCount; i++) {
            final k = _keyOf(b, i);
            if (rightKeys.add(k)) {
              final size = rowKeyBytes(k);
              rightBytes += size;
              final granted = memory == null || memory.reserve(size);
              if (granted && memory != null) reserved += size;
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
          if (memory != null && reserved > 0) {
            memory.release(reserved);
            reserved = 0;
          }
          final salt = depth + 1;
          final leftRuns = await _repartitionRun(leftRun, _fanout, salt);
          final rightRuns = await _repartitionRun(rightRun, _fanout, salt);
          for (int p = 0; p < _fanout; p++) {
            yield* _processPartition(leftRuns[p], rightRuns[p], depth + 1);
          }
        } else {
          final want = type == SetOpType.intersect;
          // The output dedup set grows with the DISTINCT OUTPUT, so it needs the
          // same bound-or-repartition treatment as the two input key sets above;
          // its check depended on `!canRecurse`, which is false at every
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
          await for (final b in leftRun.read()) {
            for (int i = 0; i < b.rowCount; i++) {
              final k = _keyOf(b, i);
              if (rightKeys.contains(k) == want && emitted.add(k)) {
                final size = rowKeyBytes(k);
                emittedBytes += size;
                final granted = memory == null || memory.reserve(size);
                if (granted && memory != null) reserved += size;
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
            if (memory != null && reserved > 0) {
              memory.release(reserved);
              reserved = 0;
            }
            final salt = depth + 1;
            final leftRuns = await _repartitionRun(leftRun, _fanout, salt);
            final rightRuns = await _repartitionRun(rightRun, _fanout, salt);
            for (int p = 0; p < _fanout; p++) {
              yield* _processPartition(leftRuns[p], rightRuns[p], depth + 1);
            }
          } else {
            await for (final b in leftRun.read()) {
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
        if (memory != null && reserved > 0) memory.release(reserved);
      }
    }
    await leftRun.delete();
    await rightRun.delete();
  }

  Never _emittedBudgetExceeded() => throw Failure.of(
    Kind.limit,
    'Set operation output dedup set exceeds the '
    '${options.spillThresholdBytes}-byte budget. Unlike the input key sets this '
    'cannot be recovered by repartitioning (rows have already been emitted) '
    'so raise spillThresholdBytes / memoryBudgetBytes, or reduce the distinct '
    'output size.',
  );

  Never _keyBudgetExceeded() => throw Failure.of(
    Kind.limit,
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
      await route(b, writers, (row) => _keyOf(b, row), salt: salt);
    }
    if (writers == null) {
      return [
        for (var i = 0; i < parts; i++)
          SpillRun('$_spillDir/_empty', schema, hasFile: false),
      ];
    }
    final runs = await _finish(writers);
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
    final memory = options.spills ? options.memory : null;
    if (memory == null) return;
    final size = rowKeyBytes(key);
    if (!memory.reserve(size)) {
      throw Failure.of(
        Kind.limit,
        'Set operation exceeded the shared memory budget building its '
        'distinct-key set',
      );
    }
    _reserved += size;
  }

  Set<RowKey> _keys(RecordBatch batch) {
    final keys = <RowKey>{};
    for (final i in _rows(batch)) {
      final k = _keyOf(batch, i);
      if (keys.add(k)) _chargeKey(k);
    }
    return keys;
  }

  /// Distinct rows of [batch] (first occurrence) passing [keep].
  RecordBatch _distinct(RecordBatch batch, bool Function(RowKey) keep) {
    final seen = <RowKey>{};
    final indices = <int>[];
    for (final i in _rows(batch)) {
      final key = _keyOf(batch, i);
      if (keep(key) && seen.add(key)) {
        _chargeKey(key);
        indices.add(i);
      }
    }
    return batch.gather(Int32List.fromList(indices));
  }

  /// The rows of [batch] worth keying, in order. A one-column text or integer
  /// key gives only the first row of each distinct value, found without
  /// making a [RowKey] per row; any other key gives every row.
  List<int> _rows(RecordBatch batch) {
    final count = batch.rowCount;
    final column = batch.columns.length == 1 ? batch.columns.first : null;
    if (column is StringVector) return column.encode().firsts;
    if (column is NumericVector && column.raw is List<int>) {
      final values = column.raw as List<int>;
      final seen = <int>{};
      final firsts = <int>[];
      var nulls = false;
      for (var i = 0; i < count; i++) {
        if (column.isNullAt(i)) {
          if (!nulls) firsts.add(i);
          nulls = true;
        } else if (seen.add(values[i])) {
          firsts.add(i);
        }
      }
      return firsts;
    }
    return Int32List(count)..setAll(0, Iterable.generate(count));
  }

  /// Finish [writers] one at a time, handing each run to [_runs] as soon as
  /// its own writer finishes, so a later failure cannot strand earlier runs'
  /// files and budget.
  Future<List<SpillRun>> _finish(List<SpillWriter> writers) async {
    final runs = <SpillRun>[];
    for (final writer in writers) {
      final run = await writer.finish();
      _runs.add(run);
      _writers.remove(writer);
      runs.add(run);
    }
    return runs;
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

/// [b], from a branch with schema [source], aligned to the reconciled output
/// [schema]: each column cast to the common type (int-width / int→float
/// promotion), a decimal restated from its source scale to the output's, and
/// the output names stamped, so every downstream concat/dedup/emit sees
/// uniform values.
RecordBatch _alignBatch(RecordBatch b, Schema source, Schema schema) {
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

/// UNION ALL of any number of [inputs], read one after another: each batch is
/// aligned to the reconciled [schema] and passed on, so the plan stays one
/// level deep however many branches the query has.
class PhysicalUnion extends PhysicalPlan {
  final List<PhysicalPlan> inputs;
  final EngineOptions options;
  final Schema _schema;

  PhysicalUnion({
    required this.inputs,
    required Schema schema,
    this.options = const EngineOptions(),
  }) : _schema = schema;

  @override
  Schema get schema => _schema;

  @override
  int? get estimatedRows => null;

  @override
  RecordBatchIterator execute() => UnionIterator(inputs, schema, options);
}

class UnionIterator extends RecordBatchIterator {
  final List<PhysicalPlan> _inputs;
  @override
  final Schema schema;
  final EngineOptions options;

  /// The branch being read: opened only when the previous one is exhausted,
  /// so a thousand-branch union holds one open input at a time.
  RecordBatchIterator? _current;
  int _index = 0;
  int _emitted = 0;

  UnionIterator(this._inputs, this.schema, this.options);

  @override
  Future<RecordBatch?> next() async {
    while (_index < _inputs.length) {
      await pause();
      final current = _current ??= _inputs[_index].execute();
      final batch = await current.next();
      if (batch != null) {
        _emitted += batch.rowCount;
        final max = options.maxOutputRows;
        if (max != null && _emitted > max) {
          throw Failure.of(
            Kind.limit,
            'Set operation produced more than maxOutputRows=$max rows',
          );
        }
        return _alignBatch(batch, _inputs[_index].schema, schema);
      }
      _current = null;
      _index++;
      await current.close();
    }
    return null;
  }

  @override
  Future<void> close() async {
    final current = _current;
    _current = null;
    _index = _inputs.length;
    await current?.close();
  }
}
