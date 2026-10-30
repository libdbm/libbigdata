part of 'physical_operators.dart';

/// Physical LIMIT/OFFSET operator
class PhysicalLimit extends PhysicalPlan {
  final PhysicalPlan input;
  final int? limit;
  final int? offset;

  PhysicalLimit({required this.input, this.limit, this.offset}) {
    // A negative LIMIT/OFFSET is invalid SQL; fail loud rather than silently
    // returning an empty result (a `>= -1` check would match immediately).
    if (limit != null && limit! < 0) {
      throw ArgumentError('LIMIT must be non-negative, got $limit');
    }
    if (offset != null && offset! < 0) {
      throw ArgumentError('OFFSET must be non-negative, got $offset');
    }
  }

  @override
  Schema get schema => input.schema;

  @override
  int? get estimatedRows {
    final inputRows = input.estimatedRows;
    if (inputRows == null) return limit;

    final afterOffset = math.max(0, inputRows - (offset ?? 0));
    return limit != null ? math.min(limit!, afterOffset) : afterOffset;
  }

  @override
  RecordBatchIterator execute() {
    return LimitIterator(
      input: input.execute(),
      limit: limit,
      offset: offset ?? 0,
    );
  }
}

class LimitIterator extends RecordBatchIterator {
  final RecordBatchIterator input;
  final int? limit;
  final int offset;

  int _rowsReturned = 0;
  int _rowsSkipped = 0;

  LimitIterator({required this.input, this.limit, required this.offset});

  @override
  Schema get schema => input.schema;

  @override
  Future<RecordBatch?> next() async {
    await pause();
    // Check if we've hit the limit
    if (limit != null && _rowsReturned >= limit!) {
      return null;
    }

    while (true) {
      final batch = await input.next();
      if (batch == null) return null;

      // Handle offset
      if (_rowsSkipped < offset) {
        final toSkip = math.min(batch.rowCount, offset - _rowsSkipped);
        _rowsSkipped += toSkip;

        if (toSkip >= batch.rowCount) {
          continue; // Skip entire batch
        }

        // Partial skip - slice the batch (typed)
        final slicedBatch = batch.slice(toSkip);

        // Apply limit if needed
        if (limit != null) {
          return _applyLimit(slicedBatch);
        }

        return slicedBatch;
      }

      // Apply limit if needed
      if (limit != null) {
        return _applyLimit(batch);
      }

      return batch;
    }
  }

  RecordBatch? _applyLimit(RecordBatch batch) {
    final remainingLimit = limit! - _rowsReturned;
    if (remainingLimit <= 0) return null;

    if (batch.rowCount <= remainingLimit) {
      _rowsReturned += batch.rowCount;
      return batch;
    }

    // Slice the batch to fit the limit (typed)
    _rowsReturned += remainingLimit;
    return batch.slice(0, remainingLimit);
  }

  @override
  Future<void> close() async {
    await input.close();
  }
}
