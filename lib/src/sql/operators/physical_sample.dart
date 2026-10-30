part of 'physical_operators.dart';

/// Physical sample: buffers its whole input, then emits exactly
/// `round(n * fraction)` of the n rows, chosen by a partial Fisher-Yates
/// shuffle seeded by [seed] (unseeded when null), in shuffled order.
///
/// The exact count needs n, so the input is held whole: under [bytes] (a
/// [Kind.limit] failure past it, as the eager reads fail) and under the
/// shared [EngineOptions.memory] budget, which a sample cannot spill to
/// relieve, so a refused reservation is a [Kind.limit] failure too.
class PhysicalSample extends PhysicalPlan {
  final PhysicalPlan input;
  final double fraction;
  final int? seed;

  /// Byte cap on the buffered input.
  final int bytes;
  final EngineOptions options;

  PhysicalSample({
    required this.input,
    required this.fraction,
    required this.seed,
    required this.bytes,
    this.options = const EngineOptions(),
  }) {
    if (!(fraction >= 0 && fraction <= 1)) {
      throw Failure.of(
        Kind.invalid,
        'sample fraction must be between 0 and 1, got $fraction',
      );
    }
  }

  @override
  Schema get schema => input.schema;

  @override
  int? get estimatedRows {
    final rows = input.estimatedRows;
    return rows == null ? null : (rows * fraction).round();
  }

  @override
  RecordBatchIterator execute() => SampleIterator(this, input.execute());
}

class SampleIterator extends RecordBatchIterator {
  final PhysicalSample plan;
  final RecordBatchIterator input;

  /// The buffered input, once read.
  RecordBatch? _rows;

  /// The kept row indices, in emission order, and how many are emitted.
  Int32List? _kept;
  int _emitted = 0;

  /// Bytes reserved from [EngineOptions.memory], returned by [close].
  int _reserved = 0;

  SampleIterator(this.plan, this.input);

  @override
  Schema get schema => plan.schema;

  @override
  Future<RecordBatch?> next() async {
    await pause();
    final kept = _kept ?? await _choose();
    if (_emitted >= kept.length) return null;
    final end = math.min(_emitted + defaultBatchSize, kept.length);
    final batch = _rows!.gather(Int32List.sublistView(kept, _emitted, end));
    _emitted = end;
    return batch;
  }

  /// Buffer the input and choose the rows to keep.
  Future<Int32List> _choose() async {
    final batches = <RecordBatch>[];
    final memory = plan.options.memory;
    var rows = 0;
    var bytes = 0;
    RecordBatch? batch;
    while ((batch = await input.next()) != null) {
      final usage = batch!.memoryUsage;
      rows += batch.rowCount;
      bytes += usage;
      enforceMaterializationCaps(rows, bytes, null, plan.bytes, 'sample input');
      if (memory != null) {
        if (!memory.reserve(usage)) {
          throw Failure.of(
            Kind.limit,
            'sample input exceeds the memory budget',
          );
        }
        _reserved += usage;
      }
      batches.add(batch);
      await pause();
    }
    final all = RecordBatch.concatAll(batches, plan.schema);
    final count = (all.rowCount * plan.fraction).round();
    // Typed, so the index scratch is 4 bytes/row instead of a boxed word.
    final indices = Int32List(all.rowCount);
    for (var i = 0; i < indices.length; i++) {
      indices[i] = i;
    }
    // Partial Fisher-Yates: only the `count` indices kept need shuffling.
    final seed = plan.seed;
    final random = seed != null ? math.Random(seed) : math.Random();
    for (var i = 0; i < count; i++) {
      final j = i + random.nextInt(indices.length - i);
      final swap = indices[i];
      indices[i] = indices[j];
      indices[j] = swap;
    }
    _rows = all;
    return _kept = Int32List.sublistView(indices, 0, count);
  }

  @override
  Future<void> close() async {
    plan.options.memory?.release(_reserved);
    _reserved = 0;
    _rows = null;
    await input.close();
  }
}
