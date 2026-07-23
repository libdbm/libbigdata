part of 'physical_operators.dart';

/// Physical SORT operator
class PhysicalSort extends PhysicalPlan {
  final PhysicalPlan input;
  final List<SortExpression> sortExpressions;
  final EngineOptions options;

  PhysicalSort({
    required this.input,
    required this.sortExpressions,
    this.options = const EngineOptions(),
  });

  @override
  Schema get schema => input.schema;

  @override
  int? get estimatedRows => input.estimatedRows;

  @override
  RecordBatchIterator execute() {
    return SortIterator(
      input: input.execute(),
      sortExpressions: sortExpressions,
      options: options,
    );
  }
}

class SortIterator extends RecordBatchIterator {
  final RecordBatchIterator input;
  final List<SortExpression> sortExpressions;
  final EngineOptions options;

  StreamIterator<RecordBatch>? _output;
  final List<SpillRun> _runs = [];
  final List<_RunCursor> _cursors = [];
  // Spill writers open but not yet finalized into [_runs]; aborted in close() so
  // a sort that throws or is cancelled mid-spill doesn't leak its fd + temp file.
  final List<SpillWriter> _writers = [];

  // Shared-budget reservation for the current in-memory buffer.
  int _reserved = 0;

  SortIterator({
    required this.input,
    required this.sortExpressions,
    this.options = const EngineOptions(),
  });

  @override
  Schema get schema => input.schema;

  @override
  Future<RecordBatch?> next() async {
    final output = _output ??= StreamIterator(_generate());
    return (await output.moveNext()) ? output.current : null;
  }

  /// Sub-budget input is sorted entirely in memory (single batch, as before).
  /// Once buffered input exceeds the budget, sorted runs are spilled and merged.
  Stream<RecordBatch> _generate() async* {
    final buffer = <RecordBatch>[];
    var bufferBytes = 0;
    RecordBatch? batch;
    // Shared budget applies only when spilling is enabled (forces a run to spill
    // earlier when the global pool is exhausted).
    final mem = options.spills ? options.memory : null;
    while ((batch = await input.next()) != null) {
      final mu = batch!.memoryUsage;
      buffer.add(batch);
      bufferBytes += mu;
      final granted = mem == null || mem.reserve(mu);
      if (granted && mem != null) _reserved += mu;
      if ((options.spills && bufferBytes > options.spillThresholdBytes) ||
          !granted) {
        _runs.add(await _spillRun(buffer));
        buffer.clear();
        bufferBytes = 0;
        mem?.release(_reserved); // run is on disk now
        _reserved = 0;
      }
    }

    if (_runs.isEmpty) {
      // Pure in-memory path — unchanged behavior.
      if (buffer.isEmpty) return;
      final merged = RecordBatch.concatAll(buffer, buffer.first.schema);
      yield await _sortBatch(merged);
      return;
    }

    if (buffer.isNotEmpty) _runs.add(await _spillRun(buffer));
    yield* _mergeRuns();
  }

  String get _spillDir => options.spillDir ?? Directory.systemTemp.path;

  /// Sort an in-memory buffer and write it as one sorted run on disk. The run is
  /// written with a schema derived from the columns' actual dataTypes, so the
  /// spilled run round-trips and the merge comparison stay type-exact.
  Future<SpillRun> _spillRun(List<RecordBatch> batches) async {
    final merged = RecordBatch.concatAll(batches, batches.first.schema);
    final sorted = await _sortBatch(merged);
    final realSchema = sorted.columns.schemaOf(template: sorted.schema);
    await Directory(_spillDir).create(recursive: true);
    final writer = SpillWriter(
      _spillDir,
      realSchema,
      budget: options.spillBudget,
    );
    _writers.add(writer); // track until finished, so a throw here can't leak it
    await writer.add(
      RecordBatch(
        columns: sorted.columns,
        schema: realSchema,
        rowCount: sorted.rowCount,
      ),
    );
    final run = await writer.finish();
    _writers.remove(writer); // finished → now owned by _runs
    return run;
  }

  /// k-way merge of the sorted runs, emitting [defaultBatchSize]-row batches.
  Stream<RecordBatch> _mergeRuns() async* {
    for (var i = 0; i < _runs.length; i++) {
      final cursor = _RunCursor(i, _runs[i], sortExpressions);
      if (await cursor.advance()) _cursors.add(cursor);
    }
    if (_cursors.isEmpty) return;

    // Type the merged output from the spilled batches' actual column types, so
    // the k-way merge output stays self-consistent (equal to the plan schema).
    final width = schema.fields.length;
    final types = [
      for (final column in _cursors.first.batch!.columns) column.dataType,
    ];

    // Ordered by (key, seq), so each emitted row costs log(runs) instead of a
    // scan of every cursor — sort is the only spilling operator with no fan-in
    // bound (set-op/aggregate/join all merge at most _fanout runs), and a
    // contended MemoryManager produces its runs small and numerous, so the run
    // count is exactly what is not bounded here. `seq` is the run index and is
    // constant per cursor, so it remains the same stability tiebreak.
    final queue = HeapPriorityQueue<_RunCursor>((a, b) {
      final cmp = compareSortKeys(sortExpressions, a.key, b.key);
      return cmp != 0 ? cmp : a.seq.compareTo(b.seq);
    })..addAll(_cursors);

    var cols = [for (var i = 0; i < width; i++) <Object?>[]];
    var rows = 0;
    while (queue.isNotEmpty) {
      final cursor = queue.removeFirst();
      for (var j = 0; j < width; j++) {
        cols[j].add(cursor.batch!.columns[j][cursor.row]);
      }
      rows++;
      // advance() moves the cursor's key, which is its position in the heap —
      // a heap cannot observe an in-place key change, so the cursor is only ever
      // re-added after advancing (it was removed above).
      if (await cursor.advance()) {
        queue.add(cursor);
      } else {
        _cursors.remove(cursor); // exhausted: nothing left to cancel on close
      }

      if (rows >= defaultBatchSize) {
        yield _buildBatch(cols, rows, types);
        cols = [for (var i = 0; i < width; i++) <Object?>[]];
        rows = 0;
      }
    }
    if (rows > 0) yield _buildBatch(cols, rows, types);
  }

  RecordBatch _buildBatch(
    List<List<Object?>> cols,
    int rows,
    List<DataType> types,
  ) {
    final out = [
      for (var j = 0; j < schema.fields.length; j++)
        Kernels.fromValues(schema.fields[j].name, types[j], cols[j]),
    ];
    return RecordBatch(columns: out, schema: schema, rowCount: rows);
  }

  Future<RecordBatch> _sortBatch(RecordBatch batch) async {
    final n = batch.rowCount;

    // Evaluate each sort key once (vectorized) instead of re-evaluating per
    // comparison.
    final keyCols = [
      for (final se in sortExpressions) se.expression.evalColumn(batch),
    ];

    final indices = List<int>.generate(n, (i) => i);
    indices.sort((a, b) {
      for (int k = 0; k < sortExpressions.length; k++) {
        final cmp = compareSortKey(
          sortExpressions[k],
          keyCols[k][a],
          keyCols[k][b],
        );
        if (cmp != 0) return cmp;
      }
      return a - b; // stable tie-break by original position
    });

    return batch.gather(Int32List.fromList(indices));
  }

  @override
  Future<void> close() async {
    try {
      await closeAll([
        () => input.close(),
        () async {
          options.memory?.release(_reserved); // return any held reservation
          _reserved = 0;
        },
        for (final cursor in _cursors) () => cursor.cancel(),
        // Abort any writer still open (cancel/throw mid-spill) before deleting.
        for (final writer in _writers) () => writer.abort(),
        for (final run in _runs) () => run.delete(),
      ]);
    } finally {
      _writers.clear();
    }
  }
}

class SortExpression {
  final PhysicalExpression expression;
  final bool ascending;
  final bool nullsFirst;

  SortExpression({
    required this.expression,
    this.ascending = true,
    this.nullsFirst = false,
  });
}

/// Compare two values under one sort key ([se]): ASC/DESC direction + nulls
/// placement, non-null values by [Comparable.compareTo]. The SINGLE source of
/// truth for sort ordering, shared by the in-memory sort ([SortIterator.
/// _sortBatch]) and the spilled k-way merge ([compareSortKeys]) so the two can
/// never drift.
int compareSortKey(SortExpression se, Object? va, Object? vb) {
  // NULL placement is defined by [nullsFirst] INDEPENDENTLY of ASC/DESC — only
  // the value comparison flips for descending. (Negating the whole result,
  // including the null term, silently inverts NULLS FIRST/LAST under DESC.)
  if (va == null && vb == null) return 0;
  if (va == null) return se.nullsFirst ? -1 : 1;
  if (vb == null) return se.nullsFirst ? 1 : -1;
  final cmp = (va as Comparable).compareTo(vb);
  return se.ascending ? cmp : -cmp;
}

/// Compare two sort-key tuples under the given [sortExpressions] (used by the
/// spilled k-way merge; the caller tie-breaks equal keys by run sequence).
int compareSortKeys(
  List<SortExpression> sortExpressions,
  List<Object?> a,
  List<Object?> b,
) {
  for (int k = 0; k < sortExpressions.length; k++) {
    final cmp = compareSortKey(sortExpressions[k], a[k], b[k]);
    if (cmp != 0) return cmp;
  }
  return 0;
}

/// A read cursor over one sorted spill run, exposing the current row's sort key.
class _RunCursor {
  final int seq;
  final List<SortExpression> sortExpressions;
  final StreamIterator<RecordBatch> _it;

  RecordBatch? batch;
  int row = 0;
  List<Column> _keyCols = const [];
  List<Object?> key = const [];

  _RunCursor(this.seq, SpillRun run, this.sortExpressions)
    : _it = StreamIterator(run.read());

  /// Move to the next row (loading the next batch as needed). Returns false at
  /// end of the run.
  Future<bool> advance() async {
    if (batch != null && row + 1 < batch!.rowCount) {
      row++;
      _setKey();
      return true;
    }
    while (await _it.moveNext()) {
      final next = _it.current;
      if (next.rowCount == 0) continue;
      batch = next;
      row = 0;
      _keyCols = [
        for (final se in sortExpressions) se.expression.evalColumn(next),
      ];
      _setKey();
      return true;
    }
    batch = null;
    return false;
  }

  void _setKey() {
    key = [for (final c in _keyCols) c[row]];
  }

  Future<void> cancel() => _it.cancel();
}
