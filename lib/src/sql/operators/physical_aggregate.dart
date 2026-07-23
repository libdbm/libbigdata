part of 'physical_operators.dart';

/// Field name for a group-by expression (its column name, or a positional name).
String _groupName(PhysicalExpression expr, int i) =>
    expr is PhysicalColumnExpression ? expr.columnName : 'group$i';

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
        dataType: groupBy[i].dataType,
        nullable: true,
        // The group KEY carries precision/scale for the same reason the
        // aggregates below do. Omitting it made a decimal key bind at scale 0
        // everywhere downstream: HAVING then compared an unscaled 15 against
        // 1.5 and kept the row that WHERE — binding against the scan, which
        // does carry the scale — correctly dropped. Same predicate, two answers.
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

/// A composite group key with value-equality over its parts.
/// Incremental aggregate accumulator over typed values (no row-map materialization).
abstract class _Acc {
  void add(Object? value);
  Object? result();

  factory _Acc.of(PhysicalAggregateExpression agg, _DistinctBudget budget) {
    switch (agg.function) {
      case 'COUNT':
        return _CountAcc(agg.argument == null, agg.distinct, budget);
      case 'SUM':
        return _SumAcc(agg.distinct, budget, agg.argument?.scale ?? 0);
      case 'AVG':
        return _AvgAcc(agg.distinct, budget, agg.argument?.scale ?? 0);
      case 'MIN':
        return _MinMaxAcc(true);
      case 'MAX':
        return _MinMaxAcc(false);
      default:
        throw UnsupportedError('aggregate ${agg.function}');
    }
  }
}

/// Bounds the TOTAL distinct values held across ALL of an aggregation's DISTINCT
/// accumulators (a single group's `seen` set is otherwise unbounded — grouping
/// by an expression with mixed/multiple COUNT(DISTINCT) bypasses the planner
/// rewrites). Charged once per newly-seen distinct value; fails loud past the
/// cap (`EngineOptions.maxOutputRows`; null opts out).
class _DistinctBudget {
  _DistinctBudget(this.countCap, this.byteCap);
  final int? countCap;

  /// Resident-byte ceiling on all DISTINCT sets combined. The count cap alone is
  /// blind to size — a handful of very large string/binary/nested distinct
  /// values exhausts memory far below the row count — so the payload bytes are
  /// weighed too (via [keyValueBytes], the shared honest estimator).
  final int byteCap;
  int _n = 0;
  int _bytes = 0;
  void charge(Object? value) {
    final c = countCap;
    if (c != null && ++_n > c) {
      throw StateError(
        'DISTINCT aggregate exceeds maxOutputRows=$c distinct values across all '
        'groups; pass maxOutputRows: null to allow it (risking OOM) or reduce '
        'the query',
      );
    }
    _bytes += keyValueBytes(value);
    if (_bytes > byteCap) {
      throw StateError(
        'DISTINCT aggregate exceeds the $byteCap-byte budget across all groups '
        '(a few large distinct values); raise spillThresholdBytes or reduce the '
        'query',
      );
    }
  }
}

class _CountAcc implements _Acc {
  final bool star;
  final bool distinct;
  final Set<Object?>? seen;
  final _DistinctBudget budget;
  int n = 0;
  _CountAcc(this.star, this.distinct, this.budget)
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

class _SumAcc implements _Acc {
  final bool distinct;
  final Set<Object?>? seen;
  final _DistinctBudget budget;
  // Integers accumulate in BigInt (exact, no 2^63 overflow); a floating value
  // promotes the whole sum to double. A pure-int sum is returned as BigInt.
  BigInt _ints = BigInt.zero;
  double _doubles = 0;
  bool _float = false;
  bool _any = false;

  /// The argument's decimal scale (0 otherwise). A decimal column holds unscaled
  /// BigInt, so summing the unscaled values yields the unscaled sum AT THIS
  /// SCALE — which is why the result keeps it rather than dividing here.
  final int scale;

  _SumAcc(this.distinct, this.budget, this.scale)
    : seen = distinct ? <Object?>{} : null;
  @override
  void add(Object? value) {
    if (value == null) return;
    // A decimal value is a BigInt, not a num — accepting only `num` silently
    // dropped every decimal and returned NULL. A non-numeric argument is a
    // caller error, not something to swallow.
    if (value is! num && value is! BigInt) {
      throw ArgumentError(
        'SUM expects a numeric argument, got ${value.runtimeType}',
      );
    }
    if (distinct) {
      if (!seen!.add(canonicalKey(value))) return;
      budget.charge(value);
    }
    _any = true;
    if (value is BigInt) {
      _ints += value; // already unscaled at `scale`
    } else if (value is int) {
      _ints += BigInt.from(value);
    } else {
      _float = true;
      _doubles += value as double;
    }
  }

  // SQL: SUM over no (non-null) rows is NULL, not 0 — matching MIN/MAX/AVG.
  @override
  Object? result() {
    if (!_any) return null;
    return _float ? _doubles + _ints.toDouble() : _ints;
  }
}

class _AvgAcc implements _Acc {
  final bool distinct;
  final Set<Object?>? seen;
  final _DistinctBudget budget;
  // Accumulate the integer part EXACTLY (BigInt) and only the float part as a
  // double, so AVG over large integers isn't rounded during accumulation (SUM is
  // exact BigInt; AVG's numerator must be too). Only the final division rounds.
  BigInt intSum = BigInt.zero;
  double dblSum = 0;
  int count = 0;

  /// The argument's decimal scale (0 otherwise). [intSum] accumulates the
  /// UNSCALED values, so the result must be brought back down by it.
  final int scale;

  _AvgAcc(this.distinct, this.budget, this.scale)
    : seen = distinct ? <Object?>{} : null;
  @override
  void add(Object? value) {
    if (value == null) return;
    if (value is! num && value is! BigInt) {
      throw ArgumentError(
        'AVG expects a numeric argument, got ${value.runtimeType}',
      );
    }
    if (distinct) {
      if (!seen!.add(canonicalKey(value))) return;
      budget.charge(value);
    }
    if (value is BigInt) {
      intSum += value; // unscaled at `scale`
    } else if (value is int) {
      intSum += BigInt.from(value);
    } else {
      dblSum += (value as double).toDouble();
    }
    count++;
  }

  @override
  Object? result() {
    if (count == 0) return null;
    // AVG is float-typed, so scale the exact unscaled sum back to its real
    // value before dividing — 1.50 + 2.50 accumulates as 400 at scale 2, whose
    // average is 2.0, not 200.
    final total = intSum.toDouble() / math.pow(10, scale).toDouble() + dblSum;
    return total / count;
  }
}

class _MinMaxAcc implements _Acc {
  final bool min;
  Comparable? best;
  _MinMaxAcc(this.min);
  @override
  void add(Object? value) {
    if (value is Comparable) {
      if (best == null ||
          (min ? value.compareTo(best!) < 0 : value.compareTo(best!) > 0)) {
        best = value;
      }
    }
  }

  @override
  Object? result() => best;
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

  // Bytes reserved from the shared [MemoryManager] for the in-memory input
  // buffer; released when that buffer spills to disk or the operator closes.
  int _reserved = 0;

  // Spill writers that are open but not yet finalized into [_partRuns]. Tracked
  // so close() can abort+delete them if the query is cancelled or throws during
  // the build/drain phase (otherwise their fds + temp files would leak).
  final List<SpillWriter> _writers = [];

  String get _spillDir => options.spillDir ?? Directory.systemTemp.path;

  @override
  Future<RecordBatch?> next() async {
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
  /// batch — no full materialization. Always yields exactly one row, even over
  /// empty input (COUNT → 0, SUM/MIN/MAX/AVG → NULL), per SQL semantics.
  Future<RecordBatch> _computeGlobalStreaming() async {
    final distinctBudget = _DistinctBudget(
      options.maxOutputRows,
      options.spillThresholdBytes,
    );
    final accs = [for (final a in aggregates) _Acc.of(a, distinctBudget)];
    RecordBatch? batch;
    while ((batch = await input.next()) != null) {
      for (int i = 0; i < aggregates.length; i++) {
        final agg = aggregates[i];
        final argCol = agg.argument?.evalColumn(batch!);
        for (int row = 0; row < batch!.rowCount; row++) {
          accs[i].add(argCol?[row]);
        }
      }
    }
    final columns = [
      for (int i = 0; i < aggregates.length; i++)
        Kernels.fromValues(aliases[i], aggregates[i].dataType, [
          accs[i].result(),
        ]),
    ];
    return RecordBatch(columns: columns, schema: schema, rowCount: 1);
  }

  /// Grouped aggregate. Buffers input until [EngineOptions.spillThresholdBytes];
  /// past that, partitions raw input rows by key hash to disk (so each group
  /// lands wholly in one partition) and aggregates each partition independently.
  Stream<RecordBatch> _computeGrouped() async* {
    final buffer = <RecordBatch>[];
    var bytes = 0;
    List<SpillWriter>? parts;
    var partitions = 0;
    RecordBatch? batch;
    // The shared budget only applies when spilling is enabled (it forces a spill
    // earlier when the global pool is exhausted); with spilling off it is ignored.
    final mem = options.spills ? options.memory : null;

    while ((batch = await input.next()) != null) {
      final mu = batch!.memoryUsage;
      if (parts == null) {
        final localOver =
            options.spills && bytes + mu > options.spillThresholdBytes;
        // Consult the shared pool only if we're not already spilling locally;
        // a granted reserve is tracked so it can be released on spill/close.
        final globalOver = !localOver && mem != null && !mem.reserve(mu);
        if (!localOver && !globalOver && mem != null) {
          _reserved += mu; // reserve(mu) above succeeded
        }
        if (localOver || globalOver) {
          partitions = math.max(
            16,
            (bytes ~/ options.spillThresholdBytes + 1) * 2,
          );
          final realSchema = _realSchema(
            buffer.isNotEmpty ? buffer.first : batch,
          );
          // Create the spill dir once, off the hot path (SpillWriter no longer
          // does a blocking createSync per writer).
          await Directory(_spillDir).create(recursive: true);
          // Aggregate partitions are read fully → Arrow when types allow.
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
          for (final b in buffer) {
            await _route(b, parts, partitions);
          }
          buffer.clear();
          bytes = 0;
          mem?.release(_reserved); // buffered rows are now on disk
          _reserved = 0;
          await _route(batch, parts, partitions);
          continue;
        }
        buffer.add(batch);
        bytes += mu;
      } else {
        await _route(batch, parts, partitions);
      }
    }

    if (parts == null) {
      if (buffer.isNotEmpty) yield await _aggregateBatches(buffer);
      return;
    }

    for (final writer in parts) {
      _partRuns.add(await writer.finish());
      _writers.remove(writer); // finished → now owned by _partRuns
    }
    // ONE distinct budget across every partition drain: the doc contract is
    // "across ALL groups", and a per-partition budget let N partitions hold
    // N x cap distinct values at once.
    final distinctBudget = _DistinctBudget(
      options.maxOutputRows,
      options.spillThresholdBytes,
    );
    for (final run in _partRuns) {
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
    final keyCols = [for (final e in groupBy) e.evalColumn(batch)];
    final realSchema = _realSchema(batch);
    final buckets = [for (int i = 0; i < partitions; i++) <int>[]];
    for (int row = 0; row < batch.rowCount; row++) {
      final key = RowKey([for (final c in keyCols) c[row]]);
      // Shared content-hash router (salt redistributes on re-partition).
      buckets[partitionOf(key, partitions, salt: salt)].add(row);
    }
    for (int p = 0; p < partitions; p++) {
      if (buckets[p].isEmpty) continue;
      final index = Int32List.fromList(buckets[p]);
      await parts[p].add(
        RecordBatch(
          columns: [for (final c in batch.columns) Kernels.gather(c, index)],
          schema: realSchema,
          rowCount: index.length,
        ),
      );
    }
  }

  Schema _realSchema(RecordBatch batch) =>
      batch.columns.schemaOf(template: batch.schema);

  bool _isSimpleCountStar() {
    // Check if all aggregates are COUNT(*) (no argument means COUNT(*))
    return aggregates.every(
      (agg) => agg is PhysicalCount && agg.argument == null && !agg.distinct,
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

  /// Aggregate a fully-buffered set of batches (in-memory path). Streams them
  /// through the accumulator with no group cap, so it never returns null.
  Future<RecordBatch> _aggregateBatches(List<RecordBatch> batches) async =>
      (await _aggregateStream(Stream.fromIterable(batches)))!;

  /// Hash-aggregate a stream of batches, holding only the per-group accumulators
  /// (not the rows) — so a skewed key (few groups, many rows) stays bounded.
  /// Returns null without finishing if the live group map would exceed
  /// [maxGroups] (genuinely high cardinality), signalling the caller to
  /// re-partition; with no [maxGroups] it always produces a result.
  Future<RecordBatch?> _aggregateStream(
    Stream<RecordBatch> batches, {
    int? maxGroups,
    _DistinctBudget? budget,
  }) async {
    final groupId = <RowKey, int>{};
    final keyValues = [for (final _ in groupBy) <Object?>[]];
    final accs = <List<_Acc>>[];
    // One budget across every group's DISTINCT sets — shared across ALL spill
    // partitions by the caller (see _computeGrouped), so the cap really is
    // "across all groups", not per partition.
    final distinctBudget =
        budget ??
        _DistinctBudget(options.maxOutputRows, options.spillThresholdBytes);
    // Resident bytes of the group KEYS (weighed honestly via rowKeyBytes, not a
    // fixed per-group estimate) — a spill/repartition is triggered by BYTES too,
    // so a wide string/binary/nested key can't blow the budget under the count.
    var keyBytes = 0;

    await for (final merged in batches) {
      final n = merged.rowCount;
      final keyCols = [for (final e in groupBy) e.evalColumn(merged)];
      final argCols = [
        for (final a in aggregates) a.argument?.evalColumn(merged),
      ];
      for (int row = 0; row < n; row++) {
        final parts = [for (final c in keyCols) c[row]];
        final key = RowKey(parts);
        var id = groupId[key];
        if (id == null) {
          // Signal re-partition when EITHER the group count OR the resident
          // group-key bytes would exceed the budget.
          if (maxGroups != null &&
              (groupId.length >= maxGroups ||
                  keyBytes >= options.spillThresholdBytes)) {
            return null;
          }
          id = accs.length;
          groupId[key] = id;
          keyBytes += rowKeyBytes(key);
          for (int g = 0; g < groupBy.length; g++) {
            keyValues[g].add(parts[g]);
          }
          accs.add([for (final a in aggregates) _Acc.of(a, distinctBudget)]);
        }
        final groupAccs = accs[id];
        for (int j = 0; j < aggregates.length; j++) {
          groupAccs[j].add(argCols[j]?[row]);
        }
      }
    }
    return _result(keyValues, accs);
  }

  /// Build the output batch from accumulated group keys + accumulators. Group
  /// keys are typed by the output schema field (the planner now resolves real
  /// expression types, so this matches the evaluated key column exactly).
  RecordBatch _result(List<List<Object?>> keyValues, List<List<_Acc>> accs) {
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
      final values = [for (final group in accs) group[j].result()];
      columns.add(
        Kernels.fromValues(aliases[j], aggregates[j].dataType, values),
      );
    }
    return RecordBatch(columns: columns, schema: schema, rowCount: accs.length);
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
    final snapN = budget._n;
    final snapBytes = budget._bytes;
    final result = await _aggregateStream(
      run.read(),
      maxGroups: _maxGroups,
      budget: budget,
    );
    if (result == null) {
      budget._n = snapN;
      budget._bytes = snapBytes;
    }
    if (result != null) {
      if (result.rowCount > 0) yield result;
      return;
    }
    if (depth >= _maxRepartitionDepth) {
      throw StateError(
        'GROUP BY exceeds the memory budget: a partition still has too many '
        'distinct keys after $depth re-partitions (raise spillThresholdBytes)',
      );
    }
    // Re-partition with a salt so keys that collided in the parent spread out.
    const fanout = 16;
    List<SpillWriter>? sub;
    await for (final b in run.read()) {
      if (sub == null) {
        await Directory(_spillDir).create(recursive: true);
        sub = [
          for (int i = 0; i < fanout; i++)
            SpillWriter(
              _spillDir,
              b.schema,
              format: SpillFormat.arrow,
              budget: options.spillBudget,
            ),
        ];
        _writers.addAll(sub); // track until finished
      }
      await _route(b, sub, fanout, salt: depth + 1);
    }
    if (sub == null) return; // empty run
    for (final writer in sub) {
      final sr = await writer.finish();
      _writers.remove(writer);
      _partRuns.add(sr); // own it so a cancel during the drain still cleans it
      yield* _drainPartition(sr, depth + 1, budget);
      await sr.delete(); // drained → free disk now (happy path)
      _partRuns.remove(sr);
    }
  }

  // Bound on the live group map per partition (≈ accumulator footprint within
  // the spill budget); and a recursion cap against pathological hash collisions.
  int get _maxGroups {
    final perGroup = 256 + 64 * (groupBy.length + aggregates.length);
    return math.max(1024, options.spillThresholdBytes ~/ perGroup);
  }

  static const _maxRepartitionDepth = 8;

  @override
  Future<void> close() async {
    try {
      await closeAll([
        () => input.close(),
        // Return any still-held shared-budget reservation (normal completion,
        // early cancel, and throw) so a later query isn't starved.
        () async {
          options.memory?.release(_reserved);
          _reserved = 0;
        },
        // Abort any writer still open and delete every spilled run — neither
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
/// via typed [_Acc] accumulators.
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
  /// scale 0), so the result column must declare it — else a decimal sum reads
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
