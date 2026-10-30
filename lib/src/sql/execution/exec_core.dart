import 'dart:async';
import 'dart:typed_data';

import '../../core/column.dart';
import '../../core/schema.dart';
import '../exec/kernels.dart';

/// Execution-core contracts shared by the planner and the physical operators:
/// the [PhysicalPlan] node, its [RecordBatchIterator], and the [RecordBatch] they
/// pass. Extracted into its own module so BOTH `planner/` and `operators/` (and
/// `spill.dart`) depend DOWNWARD on it: breaking the previous
/// `physical_plan.dart` ↔ `spill.dart` import cycle. This module depends only on
/// core (`Column`/`Schema`) + kernels, never on the planner or operators.

/// A query's deadline: [timeout] after construction, or none when null.
/// Carried as a zone value so every operator and spill loop under [run] sees it
/// without threading it through each constructor; they enforce it
/// cooperatively through [checkDeadline], once per batch.
class Deadline {
  final Duration? timeout;
  final Stopwatch _watch = Stopwatch()..start();

  Deadline(this.timeout);

  static final _key = Object();

  /// The deadline in force for the current query, if any.
  static Deadline? get current => Zone.current[_key] as Deadline?;

  /// Time left before the deadline (zero once past it), or null without one.
  Duration? get remaining {
    final limit = timeout;
    if (limit == null) return null;
    final left = limit - _watch.elapsed;
    return left.isNegative ? Duration.zero : left;
  }

  /// Run [body] with this deadline in force.
  Future<T> run<T>(Future<T> Function() body) =>
      timeout == null ? body() : runZoned(body, zoneValues: {_key: this});
}

/// Throws [TimeoutException] once the current query has run past its deadline.
/// Measured with a [Stopwatch] rather than a timer: the operator pipeline runs
/// in microtasks, where a timer would never get to fire.
void checkDeadline() {
  final deadline = Zone.current[Deadline._key] as Deadline?;
  if (deadline != null && deadline._watch.elapsed > deadline.timeout!) {
    throw TimeoutException('Query exceeded its deadline', deadline.timeout);
  }
}

/// Time since the event loop last had a turn during query execution.
final _turn = Stopwatch()..start();

/// The longest a query runs before giving the event loop a turn.
const _slice = Duration(milliseconds: 10);

/// [checkDeadline], then a turn of the event loop once [_slice] has passed
/// since the last one. The pipeline otherwise runs entirely in microtasks,
/// which starve timers and I/O: another query's lock wait and deadline, and
/// the rest of a server on this isolate, would wait for the whole query.
/// Operators await it once per batch.
Future<void> pause() async {
  checkDeadline();
  if (_turn.elapsed < _slice) return;
  await Future<void>.delayed(Duration.zero);
  _turn.reset();
}

/// Physical plan nodes represent executable operations
/// They use an iterator pattern for memory-efficient batch processing
abstract class PhysicalPlan {
  /// Schema that this plan produces
  Schema get schema;

  /// Create an iterator to execute this plan
  RecordBatchIterator execute();

  /// Estimated row count (for query planning)
  int? get estimatedRows;
}

/// Iterator for record batches
abstract class RecordBatchIterator {
  /// Get the next batch of records
  /// Returns null when no more batches
  Future<RecordBatch?> next();

  /// Close the iterator and release resources
  Future<void> close();

  /// Schema of the batches
  Schema get schema;
}

/// A batch of records with columnar storage
class RecordBatch {
  final List<Column> columns;
  final Schema schema;
  final int rowCount;

  RecordBatch({
    required this.columns,
    required this.schema,
    required this.rowCount,
  });

  /// Estimated in-memory size of this batch (sum of column buffers).
  int get memoryUsage {
    var total = 0;
    for (final c in columns) {
      total += c.memoryUsage;
    }
    return total;
  }

  /// Get a column by name, optionally disambiguated by table [qualifier].
  /// Throws if a bare [name] is ambiguous across joined inputs.
  Column? column(String name, {String? qualifier}) {
    final index = schema.resolve(name, qualifier: qualifier);
    if (index < 0) return null;
    return columns[index];
  }

  /// Create a new batch with selected columns
  RecordBatch select(List<String> columnNames) {
    final selectedColumns = <Column>[];
    final selectedFields = <Field>[];

    for (final name in columnNames) {
      // Resolve like RecordBatch.column: fail loud on an unknown column AND on
      // an ambiguous bare name (a post-join collision) rather than silently
      // picking the first match: indexOf would quietly return the left input.
      final index = schema.resolve(name);
      if (index < 0) {
        throw ArgumentError('Field not found: $name');
      }
      selectedColumns.add(columns[index]);
      selectedFields.add(schema.fields[index]);
    }

    return RecordBatch(
      columns: selectedColumns,
      schema: Schema(fields: selectedFields),
      rowCount: rowCount,
    );
  }

  /// Filter rows based on a boolean mask, preserving typed columns.
  RecordBatch filter(List<bool> mask) {
    final indices = <int>[];
    for (int i = 0; i < mask.length; i++) {
      if (mask[i]) indices.add(i);
    }
    return gather(Int32List.fromList(indices));
  }

  /// Keep only rows at [indices], preserving typed (numeric) buffers.
  RecordBatch gather(Int32List indices) {
    return RecordBatch(
      columns: [for (final c in columns) Kernels.gather(c, indices)],
      schema: schema,
      rowCount: indices.length,
    );
  }

  /// Rows [start, end), preserving typed columns via [Column.slice].
  RecordBatch slice(int start, [int? end]) {
    final stop = (end ?? rowCount).clamp(0, rowCount);
    final from = start.clamp(0, stop);
    return RecordBatch(
      columns: [for (final c in columns) c.slice(from, stop)],
      schema: schema,
      rowCount: stop - from,
    );
  }

  /// Concatenate many batches in a single pass (avoids the O(n·batches) cost of
  /// folding [concat] pairwise). Returns an empty batch for an empty input.
  static RecordBatch concatAll(List<RecordBatch> batches, Schema schema) {
    if (batches.isEmpty) {
      return RecordBatch(columns: const [], schema: schema, rowCount: 0);
    }
    if (batches.length == 1) return batches.first;
    final width = batches.first.columns.length;
    final merged = <Column>[
      for (int i = 0; i < width; i++)
        Kernels.concat([for (final b in batches) b.columns[i]]),
    ];
    return RecordBatch(
      columns: merged,
      schema: schema,
      rowCount: batches.fold(0, (s, b) => s + b.rowCount),
    );
  }

  /// Concatenate with another batch (must have same schema), keeping typed
  /// columns where possible.
  RecordBatch concat(RecordBatch other) {
    if (schema != other.schema) {
      throw ArgumentError('Cannot concat batches with different schemas');
    }
    final merged = <Column>[
      for (int i = 0; i < columns.length; i++)
        Kernels.concat([columns[i], other.columns[i]]),
    ];
    return RecordBatch(
      columns: merged,
      schema: schema,
      rowCount: rowCount + other.rowCount,
    );
  }
}

/// Build a [Schema] from a list of columns' ACTUAL (name, dtype, nullable):
/// used wherever an operator must recover the real schema of a batch it built
/// (post-projection/gather) rather than trust a declared one. One shared home so
/// the planner and every physical operator can't drift.
///
/// [template] carries FIELD metadata the columns themselves cannot express:
/// decimal precision/scale, nested children, field id: copied by NAME from
/// the operator's real plan schema. Spill writers need it: a nested column
/// spilled under a children-less field made the Parquet spill emitter crash
/// (`children.first` on empty) only once a query crossed the spill threshold.
/// Dtype/nullable still come from the actual columns (width promotion). When
/// [template] has one field per column, fields are matched by position and keep
/// their qualifier, so a join's `t.id` and `u.id` stay two distinct fields.
extension ColumnListSchema on List<Column> {
  Schema schemaOf({Schema? template}) {
    final positional = template != null && template.fields.length == length;
    return Schema(
      fields: [
        for (final (index, c) in indexed)
          positional
              ? template.fields[index].copyWith(
                  dataType: c.dataType,
                  nullable: c.nullable,
                )
              : _metadata(
                  Field(
                    name: c.name,
                    dataType: c.dataType,
                    nullable: c.nullable,
                  ),
                  template?.field(c.name),
                ),
      ],
    );
  }

  static Field _metadata(Field field, Field? source) {
    if (source == null) return field;
    return field.copyWith(
      precision: source.precision,
      scale: source.scale,
      children: source.children,
      id: source.id,
    );
  }
}

/// Run every cleanup [steps] even if an earlier one throws, then rethrow the
/// FIRST failure, so one child-close error never strands the remaining budget
/// reservation, spill writers (FDs), or spill files. Shared by every blocking
/// operator's `close()`.
Future<void> closeAll(Iterable<Future<void> Function()> steps) async {
  (Object, StackTrace)? first;
  for (final step in steps) {
    try {
      await step();
    } catch (error, trace) {
      first ??= (error, trace);
    }
  }
  if (first case (final error, final trace)) {
    Error.throwWithStackTrace(error, trace);
  }
}
