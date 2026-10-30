part of 'physical_operators.dart';

/// Field name for a group-by expression (its column name, or a positional name).
String _groupName(PhysicalExpression expression, int i) =>
    expression is PhysicalColumnExpression ? expression.columnName : 'group$i';

/// Output schema for an aggregate: one field per group-by expression followed
/// by one per aggregate.
Schema _aggregateSchema(
  List<PhysicalExpression> groupBy,
  List<PhysicalAggregateExpression> aggregates,
  List<String> aliases,
) {
  final fields = <Field>[
    for (int i = 0; i < groupBy.length; i++)
      Field(
        name: _groupName(groupBy[i], i),
        // Keeping the table qualifier lets `GROUP BY t.id, u.id` emit two
        // `id` keys; without it the two names collided.
        qualifier: switch (groupBy[i]) {
          final PhysicalColumnExpression column => column.qualifier,
          _ => null,
        },
        dataType: groupBy[i].dataType,
        nullable: true,
        // The group KEY carries precision/scale for the same reason the
        // aggregates below do. Omitting it made a decimal key bind at scale 0
        // everywhere downstream: HAVING then compared an unscaled 15 against
        // 1.5 and kept the row that WHERE: binding against the scan, which
        // does carry the scale: correctly dropped. Same predicate, two answers.
        precision: groupBy[i].precision,
        scale: groupBy[i].scale,
      ),
    for (int i = 0; i < aggregates.length; i++)
      Field(
        name: aliases[i],
        dataType: aggregates[i].dataType,
        nullable: true,
        precision: aggregates[i].precision,
        scale: aggregates[i].scale,
      ),
  ];
  return Schema(fields: fields);
}

/// Incremental aggregate accumulator over typed values (no row-map materialization).
abstract class _Accumulator {
  void add(Object? value);
  Object? result();

  factory _Accumulator.of(
    PhysicalAggregateExpression aggregate,
    _DistinctBudget budget,
  ) {
    switch (aggregate.function) {
      case 'COUNT':
        return _CountAccumulator(
          aggregate.argument == null,
          aggregate.distinct,
          budget,
        );
      case 'SUM':
        return _SumAccumulator(
          aggregate.distinct,
          budget,
          aggregate.argument?.scale ?? 0,
        );
      case 'AVG':
        return _AverageAccumulator(
          aggregate.distinct,
          budget,
          aggregate.argument?.scale ?? 0,
        );
      case 'MIN':
        return _MinMaxAccumulator(true);
      case 'MAX':
        return _MinMaxAccumulator(false);
      default:
        throw UnsupportedError('aggregate ${aggregate.function}');
    }
  }
}

/// Bounds the TOTAL distinct values held across ALL of an aggregation's DISTINCT
/// accumulators (a single group's `seen` set is otherwise unbounded: grouping
/// by an expression with mixed/multiple COUNT(DISTINCT) bypasses the planner
/// rewrites). Charged once per newly-seen distinct value; fails loud past the
/// cap (`EngineOptions.maxOutputRows`; null opts out).
class _DistinctBudget {
  _DistinctBudget(this.countCap, this.byteCap);
  final int? countCap;

  /// Resident-byte ceiling on all DISTINCT sets combined. The count cap alone is
  /// blind to size: a handful of very large string/binary/nested distinct
  /// values exhausts memory far below the row count, so the payload bytes are
  /// weighed too (via [keyValueBytes], the shared honest estimator).
  final int byteCap;
  int _count = 0;
  int _bytes = 0;
  void charge(Object? value) {
    final c = countCap;
    if (c != null && ++_count > c) {
      throw Failure.of(
        Kind.limit,
        'DISTINCT aggregate exceeds maxOutputRows=$c distinct values across all '
        'groups; pass maxOutputRows: null to allow it (risking OOM) or reduce '
        'the query',
      );
    }
    _bytes += keyValueBytes(value);
    if (_bytes > byteCap) {
      throw Failure.of(
        Kind.limit,
        'DISTINCT aggregate exceeds the $byteCap-byte budget across all groups '
        '(a few large distinct values); raise spillThresholdBytes or reduce the '
        'query',
      );
    }
  }
}

class _CountAccumulator implements _Accumulator {
  final bool star;
  final bool distinct;
  final Set<Object?>? seen;
  final _DistinctBudget budget;
  int n = 0;
  _CountAccumulator(this.star, this.distinct, this.budget)
    : seen = distinct ? <Object?>{} : null;
  @override
  void add(Object? value) {
    if (star) {
      n++;
      return;
    }
    if (value == null) return;
    if (distinct) {
      // Key on the CANONICAL value: a Uint8List/List/Map hashes by identity in
      // Dart, so each row's freshly decoded blob looked distinct and every row
      // counted. Same helper the hash join and the grace router use.
      if (!seen!.add(canonicalKey(value))) return; // already counted
      budget.charge(value);
    }
    n++;
  }

  @override
  Object? result() => n;
}

/// The exact accumulation SUM and AVG share: integers (and decimals, held as
/// unscaled BigInt at [scale]) add into a BigInt, so nothing overflows 2^63 or
/// rounds; floating values add into a double. A non-numeric argument is a
/// caller error naming [name], not something to swallow (accepting only `num`
/// once dropped every decimal).
abstract class _NumericAccumulator implements _Accumulator {
  final String name;
  final bool distinct;
  final Set<Object?>? seen;
  final _DistinctBudget budget;

  /// The argument's decimal scale (0 otherwise). [ints] holds the UNSCALED
  /// sum at this scale.
  final int scale;

  BigInt ints = BigInt.zero;
  double doubles = 0;
  bool float = false;

  /// Non-null values added (after DISTINCT).
  int count = 0;

  _NumericAccumulator(this.name, this.distinct, this.budget, this.scale)
    : seen = distinct ? <Object?>{} : null;

  @override
  void add(Object? value) {
    if (value == null) return;
    if (value is! num && value is! BigInt) {
      throw ArgumentError(
        '$name expects a numeric argument, got ${value.runtimeType}',
      );
    }
    if (distinct) {
      if (!seen!.add(canonicalKey(value))) return;
      budget.charge(value);
    }
    count++;
    if (value is BigInt) {
      ints += value; // already unscaled at `scale`
    } else if (value is int) {
      ints += BigInt.from(value);
    } else {
      float = true;
      doubles += value as double;
    }
  }
}

class _SumAccumulator extends _NumericAccumulator {
  _SumAccumulator(bool distinct, _DistinctBudget budget, int scale)
    : super('SUM', distinct, budget, scale);

  // SQL: SUM over no (non-null) rows is NULL, not 0, matching MIN/MAX/AVG.
  // A pure-int sum is returned as BigInt, a decimal's at its scale.
  @override
  Object? result() {
    if (count == 0) return null;
    return float ? doubles + ints.toDouble() : ints;
  }
}

class _AverageAccumulator extends _NumericAccumulator {
  _AverageAccumulator(bool distinct, _DistinctBudget budget, int scale)
    : super('AVG', distinct, budget, scale);

  @override
  Object? result() {
    if (count == 0) return null;
    // AVG is float-typed, so scale the exact unscaled sum back to its real
    // value before dividing: 1.50 + 2.50 accumulates as 400 at scale 2, whose
    // average is 2.0, not 200. Only this division rounds.
    final total = ints.toDouble() / math.pow(10, scale).toDouble() + doubles;
    return total / count;
  }
}

class _MinMaxAccumulator implements _Accumulator {
  final bool min;
  Object? best;
  _MinMaxAccumulator(this.min);
  @override
  void add(Object? value) {
    if (value is Comparable || value is bool || value is Uint8List) {
      final current = best;
      if (current == null) {
        best = value;
        return;
      }
      final c = Kernels.compare(value!, current);
      if (min ? c < 0 : c > 0) best = value;
    }
  }

  @override
  Object? result() => best;
}

/// Estimated bytes of one group's accumulators for [columns] keys and
/// aggregates.
int _groupBytes(int columns) => 256 + 64 * columns;

/// The in-memory table of a hash aggregate: each group's key values and
/// accumulators, and the bytes its keys hold.
class _Groups {
  final List<PhysicalExpression> groupBy;
  final List<PhysicalAggregateExpression> aggregates;

  /// One budget across every group's DISTINCT sets, shared by the caller
  /// across partitions so the cap really is "across all groups".
  final _DistinctBudget budget;

  final groupId = <RowKey, int>{};
  late final keyValues = [for (final _ in groupBy) <Object?>[]];
  final accumulators = <List<_Accumulator>>[];

  /// Resident bytes of the group KEYS (weighed honestly via rowKeyBytes, not a
  /// fixed per-group estimate), so a wide string/binary/nested key cannot pass
  /// the budget under the group count.
  int keyBytes = 0;

  /// The shared pool each new group reserves from, when spilling is on. A
  /// denied reserve marks the group map [full] (later new keys go to the
  /// returned rest), except for the first group, which is kept so the
  /// aggregate always makes progress.
  final MemoryManager? memory;

  /// Bytes reserved from [memory], returned by [free].
  int reserved = 0;

  /// Set once [memory] refuses a group. It stays set: a group routed to a
  /// partition must not later be created here too when the pool frees up, or
  /// it would be emitted twice.
  bool full = false;

  _Groups(this.groupBy, this.aggregates, this.budget, [this.memory]);

  /// Return every reservation to [memory].
  void free() {
    memory?.release(reserved);
    reserved = 0;
  }

  /// Fold [batch] in. Once the table holds [maxGroups] groups or [maxBytes]
  /// of keys, a row whose group is new is left out; returns those rows, or
  /// null when every row was folded. No [maxGroups] means no cap.
  Int32List? fold(RecordBatch batch, {int? maxGroups, required int maxBytes}) {
    final n = batch.rowCount;
    final keys = [for (final e in groupBy) e.evalColumn(batch)];
    final arguments = [
      for (final a in aggregates) a.argument?.evalColumn(batch),
    ];
    List<int>? rest;

    // The group of key (from parts), made if new and there is room; -1 when
    // the table is full. Called only on a miss, so the per-row lookup stays
    // inline.
    int create(RowKey key, List<Object?> parts) {
      if (maxGroups != null &&
          (groupId.length >= maxGroups || keyBytes >= maxBytes)) {
        return -1;
      }
      if (full) return -1;
      final bytes = rowKeyBytes(key);
      final pool = memory;
      if (pool != null) {
        final size = bytes + _groupBytes(groupBy.length + aggregates.length);
        if (pool.reserve(size)) {
          reserved += size;
        } else if (groupId.isNotEmpty) {
          full = true;
          return -1;
        }
      }
      groupId[key] = accumulators.length;
      keyBytes += bytes;
      for (int g = 0; g < groupBy.length; g++) {
        keyValues[g].add(parts[g]);
      }
      accumulators.add([
        for (final a in aggregates) _Accumulator.of(a, budget),
      ]);
      return accumulators.length - 1;
    }

    void add(int id, int row) {
      if (id < 0) {
        (rest ??= <int>[]).add(row);
        return;
      }
      final group = accumulators[id];
      for (int j = 0; j < aggregates.length; j++) {
        group[j].add(arguments[j]?[row]);
      }
    }

    final single = keys.length == 1 ? keys.first : null;
    if (single is StringVector) {
      // One text key: look each distinct value of the batch up once, then
      // find a row's group through its code, with no key made per row.
      final (:codes, :firsts) = single.encode();
      final ids = Int32List(firsts.length);
      for (int c = 0; c < firsts.length; c++) {
        final parts = [single[firsts[c]]];
        final key = RowKey(parts);
        ids[c] = groupId[key] ?? create(key, parts);
      }
      for (int row = 0; row < n; row++) {
        add(ids[codes[row]], row);
      }
    } else {
      for (int row = 0; row < n; row++) {
        final parts = [for (final c in keys) c[row]];
        final key = RowKey(parts);
        add(groupId[key] ?? create(key, parts), row);
      }
    }
    final left = rest;
    return left == null ? null : Int32List.fromList(left);
  }
}

/// Physical aggregation operator
class PhysicalAggregate extends PhysicalPlan {
  final PhysicalPlan input;
  final List<PhysicalExpression> groupBy;
  final List<PhysicalAggregateExpression> aggregates;
  final List<String> aliases;
  final EngineOptions options;

  PhysicalAggregate({
    required this.input,
    required this.groupBy,
    required this.aggregates,
    required this.aliases,
    this.options = const EngineOptions(),
  });

  @override
  Schema get schema => _aggregateSchema(groupBy, aggregates, aliases);

  @override
  int? get estimatedRows => groupBy.isEmpty ? 1 : null;

  @override
  RecordBatchIterator execute() {
    return AggregateIterator(
      input: input.execute(),
      groupBy: groupBy,
      aggregates: aggregates,
      aliases: aliases,
      options: options,
    );
  }
}

class AggregateIterator extends RecordBatchIterator {
  final RecordBatchIterator input;
  final List<PhysicalExpression> groupBy;
  final List<PhysicalAggregateExpression> aggregates;
  final List<String> aliases;
  final EngineOptions options;

  late final Schema _schema;

  AggregateIterator({
    required this.input,
    required this.groupBy,
    required this.aggregates,
    required this.aliases,
    this.options = const EngineOptions(),
  }) {
    _schema = _aggregateSchema(groupBy, aggregates, aliases);
  }

  @override
  Schema get schema => _schema;

  StreamIterator<RecordBatch>? _output;
  final List<SpillRun> _partRuns = [];

  // Spill writers that are open but not yet finalized into [_partRuns]. Tracked
  // so close() can abort+delete them if the query is cancelled or throws during
  // the build/drain phase (otherwise their fds + temp files would leak).
  final List<SpillWriter> _writers = [];

  // The group table holding reservations, freed by close() if the query is
  // cancelled or throws before the table is.
  _Groups? _table;

  String get _spillDir => options.spillDir ?? Directory.systemTemp.path;

  @override
  Future<RecordBatch?> next() async {
    await pause();
    final output = _output ??= StreamIterator(_generate());
    return (await output.moveNext()) ? output.current : null;
  }

  Stream<RecordBatch> _generate() async* {
    if (groupBy.isEmpty) {
      if (_isSimpleCountStar()) {
        yield await _computeStreamingCountStar();
        return;
      }
      yield await _computeGlobalStreaming();
      return;
    }
    yield* _computeGrouped();
  }

  /// Global aggregate (no GROUP BY): stream the input, feeding accumulators per
  /// batch: no full materialization. Always yields exactly one row, even over
  /// empty input (COUNT → 0, SUM/MIN/MAX/AVG → NULL), per SQL semantics.
  Future<RecordBatch> _computeGlobalStreaming() async {
    final distinctBudget = _DistinctBudget(
      options.maxOutputRows,
      options.spillThresholdBytes,
    );
    final accumulators = [
      for (final a in aggregates) _Accumulator.of(a, distinctBudget),
    ];
    RecordBatch? batch;
    while ((batch = await input.next()) != null) {
      for (int i = 0; i < aggregates.length; i++) {
        final aggregate = aggregates[i];
        final column = aggregate.argument?.evalColumn(batch!);
        for (int row = 0; row < batch!.rowCount; row++) {
          accumulators[i].add(column?[row]);
        }
      }
    }
    final columns = [
      for (int i = 0; i < aggregates.length; i++)
        Kernels.fromValues(aliases[i], aggregates[i].dataType, [
          accumulators[i].result(),
        ]),
    ];
    return RecordBatch(columns: columns, schema: schema, rowCount: 1);
  }

  /// Grouped aggregate. Folds each input batch into an in-memory group table
  /// as it arrives, so only the groups are held, never the rows. When
  /// spilling is on and the table reaches its group or key-byte cap, rows of
  /// groups already in the table still fold in place, and only rows of new
  /// groups are partitioned by key hash to disk. Those groups are not in the
  /// table, so each partition aggregates on its own and no partial state is
  /// ever merged.
  Stream<RecordBatch> _computeGrouped() async* {
    // ONE distinct budget across the table and every partition drain: the doc
    // contract is "across ALL groups", and a per-partition budget let N
    // partitions hold N x cap distinct values at once.
    final distinctBudget = _DistinctBudget(
      options.maxOutputRows,
      options.spillThresholdBytes,
    );
    final groups = _table = _Groups(
      groupBy,
      aggregates,
      distinctBudget,
      options.spills ? options.memory : null,
    );
    final maxGroups = options.spills ? _maxGroups : null;
    List<SpillWriter>? parts;
    var partitions = 0;
    RecordBatch? batch;

    while ((batch = await input.next()) != null) {
      final rest = groups.fold(
        batch!,
        maxGroups: maxGroups,
        maxBytes: options.spillThresholdBytes,
      );
      if (rest == null) continue;
      if (parts == null) {
        partitions = 16;
        await Directory(_spillDir).create(recursive: true);
        // Aggregate partitions are read fully → Arrow when types allow.
        final realSchema = _realSchema(batch);
        parts = [
          for (int i = 0; i < partitions; i++)
            SpillWriter(
              _spillDir,
              realSchema,
              format: SpillFormat.arrow,
              budget: options.spillBudget,
            ),
        ];
        _writers.addAll(parts); // track for cleanup until finished
      }
      await _route(batch.gather(rest), parts, partitions);
    }

    if (groups.accumulators.isNotEmpty) {
      yield _result(groups.keyValues, groups.accumulators);
    }
    groups.free();
    if (parts == null) return;

    for (final writer in parts) {
      _partRuns.add(await writer.finish());
      _writers.remove(writer); // finished → now owned by _partRuns
    }
    for (final run in List.of(_partRuns)) {
      yield* _drainPartition(run, 0, distinctBudget);
    }
  }

  /// Route a batch's rows to partition spill files by group-key hash, keeping
  /// each group whole. Spill files derive their schema from the columns' actual
  /// dataTypes, so each spilled batch is self-consistent (this now matches the
  /// planner schema too, which resolves real expression types).
  Future<void> _route(
    RecordBatch batch,
    List<SpillWriter> parts,
    int partitions, {
    int salt = 0,
  }) async {
    final keys = [for (final e in groupBy) e.evalColumn(batch)];
    await route(
      batch,
      parts,
      (row) => RowKey([for (final c in keys) c[row]]),
      salt: salt,
      schema: _realSchema(batch),
    );
  }

  Schema _realSchema(RecordBatch batch) =>
      batch.columns.schemaOf(template: batch.schema);

  bool _isSimpleCountStar() {
    // Check if all aggregates are COUNT(*) (no argument means COUNT(*))
    return aggregates.every(
      (aggregate) =>
          aggregate is PhysicalCount &&
          aggregate.argument == null &&
          !aggregate.distinct,
    );
  }

  Future<RecordBatch> _computeStreamingCountStar() async {
    // Stream through batches and count rows
    int totalCount = 0;
    while (true) {
      final batch = await input.next();
      if (batch == null) break;
      totalCount += batch.rowCount;
    }

    // Build result with COUNT(*) value(s)
    final columns = <Column>[];
    for (int i = 0; i < aggregates.length; i++) {
      columns.add(
        ListColumn(
          name: aliases[i],
          data: [totalCount],
          dataType: DataType.int64,
        ),
      );
    }

    return RecordBatch(columns: columns, schema: schema, rowCount: 1);
  }

  /// Hash-aggregate a stream of batches, holding only the per-group accumulators
  /// (not the rows), so a skewed key (few groups, many rows) stays bounded.
  /// Returns null without finishing if the live group map would exceed
  /// [maxGroups] (genuinely high cardinality), signalling the caller to
  /// re-partition; with no [maxGroups] it always produces a result.
  Future<RecordBatch?> _aggregateStream(
    Stream<RecordBatch> batches, {
    int? maxGroups,
    required _DistinctBudget budget,
  }) async {
    final groups = _table = _Groups(
      groupBy,
      aggregates,
      budget,
      options.spills ? options.memory : null,
    );
    await for (final batch in batches) {
      final rest = groups.fold(
        batch,
        maxGroups: maxGroups,
        maxBytes: options.spillThresholdBytes,
      );
      if (rest != null) {
        groups.free();
        return null;
      }
    }
    final result = _result(groups.keyValues, groups.accumulators);
    groups.free();
    return result;
  }

  /// Build the output batch from accumulated group keys + accumulators. Group
  /// keys are typed by the output schema field (the planner now resolves real
  /// expression types, so this matches the evaluated key column exactly).
  RecordBatch _result(
    List<List<Object?>> keyValues,
    List<List<_Accumulator>> accumulators,
  ) {
    final columns = <Column>[];
    for (int g = 0; g < groupBy.length; g++) {
      columns.add(
        Kernels.fromValues(
          schema.fields[g].name,
          schema.fields[g].dataType,
          keyValues[g],
        ),
      );
    }
    for (int j = 0; j < aggregates.length; j++) {
      final values = [for (final group in accumulators) group[j].result()];
      columns.add(
        Kernels.fromValues(aliases[j], aggregates[j].dataType, values),
      );
    }
    return RecordBatch(
      columns: columns,
      schema: schema,
      rowCount: accumulators.length,
    );
  }

  /// Aggregate one spilled partition. Stream-aggregates first (bounded for
  /// skew); if the partition has too many distinct keys to hold, re-partitions
  /// it with a salted hash and recurses so each sub-partition fits.
  Stream<RecordBatch> _drainPartition(
    SpillRun run,
    int depth,
    _DistinctBudget budget,
  ) async* {
    // Snapshot the shared budget: an attempt that bails on maxGroups discards
    // its accumulators, and its charges must not double-count when the
    // re-partitioned recursion re-aggregates the same rows.
    final count = budget._count;
    final bytes = budget._bytes;
    final result = await _aggregateStream(
      run.read(),
      maxGroups: _maxGroups,
      budget: budget,
    );
    if (result == null) {
      budget._count = count;
      budget._bytes = bytes;
    }
    if (result != null) {
      if (result.rowCount > 0) yield result;
      return;
    }
    if (depth >= _maxRepartitionDepth) {
      throw Failure.of(
        Kind.limit,
        'GROUP BY exceeds the memory budget: a partition still has too many '
        'distinct keys after $depth re-partitions (raise spillThresholdBytes)',
      );
    }
    // Re-partition with a salt so keys that collided in the parent spread out.
    const fanout = 16;
    List<SpillWriter>? writers;
    await for (final b in run.read()) {
      if (writers == null) {
        await Directory(_spillDir).create(recursive: true);
        writers = [
          for (int i = 0; i < fanout; i++)
            SpillWriter(
              _spillDir,
              b.schema,
              format: SpillFormat.arrow,
              budget: options.spillBudget,
            ),
        ];
        _writers.addAll(writers); // track until finished
      }
      await _route(b, writers, fanout, salt: depth + 1);
    }
    if (writers == null) return; // empty run
    for (final writer in writers) {
      final child = await writer.finish();
      _writers.remove(writer);
      _partRuns.add(
        child,
      ); // own it so a cancel during the drain still cleans it
      yield* _drainPartition(child, depth + 1, budget);
      await child.delete(); // drained → free disk now (happy path)
      _partRuns.remove(child);
    }
  }

  // Bound on the live group map per partition (≈ accumulator footprint within
  // the spill budget); and a recursion cap against pathological hash collisions.
  int get _maxGroups {
    final perGroup = _groupBytes(groupBy.length + aggregates.length);
    return math.max(1024, options.spillThresholdBytes ~/ perGroup);
  }

  static const _maxRepartitionDepth = 8;

  @override
  Future<void> close() async {
    try {
      await closeAll([
        // Stop the output first: a consumer that stopped early (LIMIT, a
        // cancelled stream) leaves it suspended mid-read, holding a spill
        // file open and local reservations its finally blocks return.
        () async => _output?.cancel(),
        () => input.close(),
        () async => _table?.free(),
        // Abort any writer still open and delete every spilled run: neither
        // must leak on early termination.
        for (final writer in _writers) () => writer.abort(),
        for (final run in _partRuns) () => run.delete(),
      ]);
    } finally {
      _writers.clear();
      _partRuns.clear();
    }
  }
}

/// Physical aggregate expression. Carries the function name, optional argument
/// and DISTINCT flag; the actual accumulation is done by [AggregateIterator]
/// via typed [_Accumulator] accumulators.
abstract class PhysicalAggregateExpression extends PhysicalExpression {
  final String function;
  final PhysicalExpression? argument;
  final bool distinct;

  PhysicalAggregateExpression({
    required this.function,
    this.argument,
    this.distinct = false,
  });

  @override
  Future<dynamic> evaluate(RecordBatch batch, int rowIndex) async {
    throw UnsupportedError('Cannot evaluate aggregate in row context');
  }
}

/// COUNT aggregate
class PhysicalCount extends PhysicalAggregateExpression {
  PhysicalCount({super.argument, super.distinct}) : super(function: 'COUNT');

  @override
  DataType get dataType => DataType.int64;
}

/// SUM aggregate. An integer SUM is exact: it accumulates in BigInt and is
/// surfaced as a lossless `decimal(38,0)` (the same choice Spark makes), so a
/// sum past 2^63 neither overflows nor loses precision to float64. A floating
/// SUM stays float64.
class PhysicalSum extends PhysicalAggregateExpression {
  PhysicalSum({required PhysicalExpression super.argument, super.distinct})
    : super(function: 'SUM');

  /// True when the sum is EXACT: an integer argument (accumulated losslessly in
  /// BigInt, so 2^63 can't overflow) or a decimal one (already unscaled BigInt).
  /// A float argument sums as float64, as SQL expects.
  bool get _exactInput => switch (argument?.dataType) {
    DataType.int8 ||
    DataType.int16 ||
    DataType.int32 ||
    DataType.int64 ||
    DataType.decimal => true,
    _ => false,
  };

  @override
  DataType get dataType => _exactInput ? DataType.decimal : DataType.float64;

  @override
  int? get precision => _exactInput ? 38 : null;

  /// Summing unscaled values keeps the argument's scale (an int argument is
  /// scale 0), so the result column must declare it: else a decimal sum reads
  /// back as a scale-0 integer.
  @override
  int? get scale => _exactInput ? (argument?.scale ?? 0) : null;
}

/// AVG aggregate
class PhysicalAvg extends PhysicalAggregateExpression {
  PhysicalAvg({required PhysicalExpression super.argument, super.distinct})
    : super(function: 'AVG');

  @override
  DataType get dataType => DataType.float64;
}

/// MIN aggregate
class PhysicalMin extends PhysicalAggregateExpression {
  PhysicalMin({required PhysicalExpression argument})
    : super(function: 'MIN', argument: argument);

  @override
  DataType get dataType => argument!.dataType;
  // MIN/MAX return an element of the input, so preserve the argument's decimal
  // precision/scale (else a decimal result is misread as scale 0).
  @override
  int? get precision => argument!.precision;
  @override
  int? get scale => argument!.scale;
}

/// MAX aggregate
class PhysicalMax extends PhysicalAggregateExpression {
  PhysicalMax({required PhysicalExpression argument})
    : super(function: 'MAX', argument: argument);

  @override
  DataType get dataType => argument!.dataType;
  @override
  int? get precision => argument!.precision;
  @override
  int? get scale => argument!.scale;
}
