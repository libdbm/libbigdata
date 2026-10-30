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

/// Physical top-N: the first [count] rows of [input] in [sortExpressions]
/// order, the same rows a full sort cut to [count] gives, ties included. It
/// replaces a sort under a small LIMIT, holding [count] rows rather than the
/// whole input.
class PhysicalTopN extends PhysicalPlan {
  final PhysicalPlan input;
  final List<SortExpression> sortExpressions;
  final int count;

  PhysicalTopN({
    required this.input,
    required this.sortExpressions,
    required this.count,
  });

  @override
  Schema get schema => input.schema;

  @override
  int? get estimatedRows {
    final rows = input.estimatedRows;
    return rows == null ? count : math.min(rows, count);
  }

  @override
  RecordBatchIterator execute() => TopNIterator(
    input: input.execute(),
    sortExpressions: sortExpressions,
    count: count,
  );
}

/// Keeps the best [count] rows seen so far, sorted. A row of a new batch is a
/// candidate only if it sorts strictly before the last kept row; later equal
/// rows lose, as they do in a stable full sort. Kept rows and candidates are
/// then sorted together, kept rows first so ties stay in input order, and cut
/// back to [count].
class TopNIterator extends RecordBatchIterator {
  final RecordBatchIterator input;
  final List<SortExpression> sortExpressions;
  final int count;
  bool _done = false;

  /// The most rows held between batches; at most [count].
  int peak = 0;

  TopNIterator({
    required this.input,
    required this.sortExpressions,
    required this.count,
  });

  @override
  Schema get schema => input.schema;

  @override
  Future<RecordBatch?> next() async {
    await pause();
    if (_done) return null;
    _done = true;
    RecordBatch? kept;
    List<Column> keptKeys = const [];
    RecordBatch? batch;
    while ((batch = await input.next()) != null) {
      await pause();
      final rows = batch!.rowCount;
      if (rows == 0) continue;
      final keys = [
        for (final sort in sortExpressions) sort.expression.evalColumn(batch),
      ];
      final Int32List candidates;
      if (kept == null || kept.rowCount < count) {
        candidates = Int32List.fromList(List.generate(rows, (i) => i));
      } else {
        final last = kept.rowCount - 1;
        final picked = <int>[];
        for (int i = 0; i < rows; i++) {
          if (compareKeyRows(sortExpressions, keys, i, keptKeys, last) < 0) {
            picked.add(i);
          }
        }
        if (picked.isEmpty) continue;
        candidates = Int32List.fromList(picked);
      }
      final chosen = batch.gather(candidates);
      final pool = kept == null
          ? chosen
          : RecordBatch.concatAll([kept, chosen], kept.schema);
      final poolKeys = [
        for (final sort in sortExpressions) sort.expression.evalColumn(pool),
      ];
      final order = List<int>.generate(pool.rowCount, (i) => i);
      order.sort((a, b) {
        final comparison = compareKeyRows(
          sortExpressions,
          poolKeys,
          a,
          poolKeys,
          b,
        );
        return comparison != 0 ? comparison : a - b;
      });
      final size = math.min(count, order.length);
      final top = Int32List.fromList(order.sublist(0, size));
      kept = pool.gather(top);
      keptKeys = [for (final column in poolKeys) Kernels.gather(column, top)];
      peak = math.max(peak, kept.rowCount);
    }
    return kept;
  }

  @override
  Future<void> close() => input.close();
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
    await pause();
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
    final memory = options.spills ? options.memory : null;
    while ((batch = await input.next()) != null) {
      final usage = batch!.memoryUsage;
      buffer.add(batch);
      bufferBytes += usage;
      final granted = memory == null || memory.reserve(usage);
      if (granted && memory != null) _reserved += usage;
      if ((options.spills && bufferBytes > options.spillThresholdBytes) ||
          !granted) {
        _runs.add(await _spillRun(buffer));
        buffer.clear();
        bufferBytes = 0;
        memory?.release(_reserved); // run is on disk now
        _reserved = 0;
      }
    }

    if (_runs.isEmpty) {
      // Pure in-memory path: unchanged behavior.
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

  /// The most runs merged at once: each holds an open file while merging.
  static const _fanIn = 64;

  /// Merge the sorted runs into one sorted stream. Past [_fanIn] runs, groups
  /// of consecutive runs are first merged into larger runs, pass by pass, so
  /// the number of open files stays bounded however many runs a contended
  /// memory pool produced. Consecutive groups keep the input order, so ties
  /// stay stable.
  Stream<RecordBatch> _mergeRuns() async* {
    while (_runs.length > _fanIn) {
      final next = <SpillRun>[];
      final pass = List.of(_runs);
      for (var start = 0; start < pass.length; start += _fanIn) {
        final group = pass.sublist(
          start,
          math.min(start + _fanIn, pass.length),
        );
        next.add(group.length == 1 ? group.single : await _combine(group));
      }
      _runs
        ..clear()
        ..addAll(next);
    }
    yield* _merge(_runs);
  }

  /// Merge [group] into one new run, then delete its runs.
  Future<SpillRun> _combine(List<SpillRun> group) async {
    final writer = SpillWriter(
      _spillDir,
      group.first.schema,
      budget: options.spillBudget,
    );
    _writers.add(writer);
    await for (final batch in _merge(group)) {
      await writer.add(batch);
    }
    final run = await writer.finish();
    _writers.remove(writer);
    _runs.add(run); // owned for cleanup until the pass replaces the list
    for (final old in group) {
      await old.delete();
      _runs.remove(old);
    }
    return run;
  }

  /// k-way merge of [runs], emitting [defaultBatchSize]-row batches.
  Stream<RecordBatch> _merge(List<SpillRun> runs) async* {
    final cursors = <_RunCursor>[];
    for (var i = 0; i < runs.length; i++) {
      final cursor = _RunCursor(i, runs[i], sortExpressions);
      if (await cursor.advance()) cursors.add(cursor);
    }
    _cursors.addAll(cursors);
    if (cursors.isEmpty) return;

    // Type the merged output from the spilled batches' actual column types, so
    // the k-way merge output stays self-consistent (equal to the plan schema).
    final width = schema.fields.length;
    final types = [
      for (final column in cursors.first.batch!.columns) column.dataType,
    ];

    // Ordered by (key, sequence), so each emitted row costs log(runs) instead
    // of a scan of every cursor. `sequence` is the run index and is constant
    // per cursor, so it remains the stability tiebreak.
    final queue = HeapPriorityQueue<_RunCursor>((a, b) {
      final comparison = compareSortKeys(sortExpressions, a.key, b.key);
      return comparison != 0 ? comparison : a.sequence.compareTo(b.sequence);
    })..addAll(cursors);

    var columns = [for (var i = 0; i < width; i++) <Object?>[]];
    var rows = 0;
    while (queue.isNotEmpty) {
      final cursor = queue.removeFirst();
      for (var j = 0; j < width; j++) {
        columns[j].add(cursor.batch!.columns[j][cursor.row]);
      }
      rows++;
      // advance() moves the cursor's key, which is its position in the heap:
      // a heap cannot observe an in-place key change, so the cursor is only ever
      // re-added after advancing (it was removed above).
      if (await cursor.advance()) {
        queue.add(cursor);
      } else {
        _cursors.remove(cursor); // exhausted: nothing left to cancel on close
      }

      if (rows >= defaultBatchSize) {
        yield _buildBatch(columns, rows, types);
        columns = [for (var i = 0; i < width; i++) <Object?>[]];
        rows = 0;
      }
    }
    if (rows > 0) yield _buildBatch(columns, rows, types);
  }

  RecordBatch _buildBatch(
    List<List<Object?>> columns,
    int rows,
    List<DataType> types,
  ) {
    final out = [
      for (var j = 0; j < schema.fields.length; j++)
        Kernels.fromValues(schema.fields[j].name, types[j], columns[j]),
    ];
    return RecordBatch(columns: out, schema: schema, rowCount: rows);
  }

  Future<RecordBatch> _sortBatch(RecordBatch batch) async {
    final n = batch.rowCount;

    // Evaluate each sort key once (vectorized) instead of re-evaluating per
    // comparison.
    final keys = [
      for (final sort in sortExpressions) sort.expression.evalColumn(batch),
    ];

    final indices = List<int>.generate(n, (i) => i);
    indices.sort((a, b) {
      final comparison = compareKeyRows(sortExpressions, keys, a, keys, b);
      return comparison != 0
          ? comparison
          : a - b; // stable tie-break by original position
    });

    return batch.gather(Int32List.fromList(indices));
  }

  @override
  Future<void> close() async {
    try {
      await closeAll([
        // Stop the output first: a consumer that stopped early (LIMIT, a
        // cancelled stream) leaves it suspended mid-read, holding a spill
        // file open and local reservations its finally blocks return.
        () async => _output?.cancel(),
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

/// Compare two values under one sort key ([sort]): ASC/DESC direction + nulls
/// placement, non-null values by [Kernels.compare]. The SINGLE source of
/// truth for sort ordering, shared by the in-memory sort ([SortIterator.
/// _sortBatch]) and the spilled k-way merge ([compareSortKeys]) so the two can
/// never drift.
int compareSortKey(SortExpression sort, Object? left, Object? right) {
  // NULL placement is defined by [nullsFirst] INDEPENDENTLY of ASC/DESC, only
  // the value comparison flips for descending. (Negating the whole result,
  // including the null term, silently inverts NULLS FIRST/LAST under DESC.)
  if (left == null && right == null) return 0;
  if (left == null) return sort.nullsFirst ? -1 : 1;
  if (right == null) return sort.nullsFirst ? 1 : -1;
  final comparison = Kernels.compare(left, right);
  return sort.ascending ? comparison : -comparison;
}

/// Compare row [a] of key columns [x] with row [b] of key columns [y] under
/// [sortExpressions], with no tie-break. A text key compares its bytes in
/// place rather than decoding both values on every comparison.
int compareKeyRows(
  List<SortExpression> sortExpressions,
  List<Column> x,
  int a,
  List<Column> y,
  int b,
) {
  for (int k = 0; k < sortExpressions.length; k++) {
    final left = x[k];
    final right = y[k];
    final comparison = left is StringColumn && right is StringColumn
        ? _compareText(sortExpressions[k], left, a, right, b)
        : compareSortKey(sortExpressions[k], left[a], right[b]);
    if (comparison != 0) return comparison;
  }
  return 0;
}

/// [compareSortKey] for row [a] of [x] and row [b] of [y], read from their
/// bytes in the same order.
int _compareText(
  SortExpression sort,
  StringColumn x,
  int a,
  StringColumn y,
  int b,
) {
  final nullA = x.isNullAt(a);
  final nullB = y.isNullAt(b);
  if (nullA && nullB) return 0;
  if (nullA) return sort.nullsFirst ? -1 : 1;
  if (nullB) return sort.nullsFirst ? 1 : -1;
  final comparison = x.compareAt(a, y, b);
  return sort.ascending ? comparison : -comparison;
}

/// Compare two sort-key tuples under the given [sortExpressions] (used by the
/// spilled k-way merge; the caller tie-breaks equal keys by run sequence).
int compareSortKeys(
  List<SortExpression> sortExpressions,
  List<Object?> a,
  List<Object?> b,
) {
  for (int k = 0; k < sortExpressions.length; k++) {
    final comparison = compareSortKey(sortExpressions[k], a[k], b[k]);
    if (comparison != 0) return comparison;
  }
  return 0;
}

/// A read cursor over one sorted spill run, exposing the current row's sort key.
class _RunCursor {
  final int sequence;
  final List<SortExpression> sortExpressions;
  final StreamIterator<RecordBatch> _iterator;

  RecordBatch? batch;
  int row = 0;
  List<Column> _keys = const [];
  List<Object?> key = const [];

  _RunCursor(this.sequence, SpillRun run, this.sortExpressions)
    : _iterator = StreamIterator(run.read());

  /// Move to the next row (loading the next batch as needed). Returns false at
  /// end of the run.
  Future<bool> advance() async {
    if (batch != null && row + 1 < batch!.rowCount) {
      row++;
      _setKey();
      return true;
    }
    while (await _iterator.moveNext()) {
      final next = _iterator.current;
      if (next.rowCount == 0) continue;
      batch = next;
      row = 0;
      _keys = [
        for (final sort in sortExpressions) sort.expression.evalColumn(next),
      ];
      _setKey();
      return true;
    }
    batch = null;
    return false;
  }

  void _setKey() {
    key = [for (final c in _keys) c[row]];
  }

  Future<void> cancel() => _iterator.cancel();
}
