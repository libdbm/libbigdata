import 'dart:async';
import 'dart:convert';
import 'dart:math' as math;
import 'dart:typed_data';
import 'package:meta/meta.dart';
import '../core/table.dart';
import '../core/schema.dart';
import '../core/data_type.dart';
import '../core/limits.dart';
import '../sql/exec/kernels.dart';
import '../sql/execution/engine_options.dart';
import '../sql/planner/physical_plan.dart';
import '../sql/planner/file_scan.dart';
import '../sql/operators/physical_operators.dart';
import '../sql/planner/logical_plan.dart'
    as plan
    show JoinType, SetOpType, BinaryOperator;

part 'operations.dart';
part 'compilers.dart';

/// Represents a lazy DataFrame with chainable operations
class DataFrame {
  final Table? _table;
  final List<Operation> _operations;
  final Schema? _schema;

  DataFrame._({Table? table, List<Operation>? operations, Schema? schema})
    : _table = table,
      _operations = operations ?? [],
      _schema = schema ?? table?.schema;

  /// Create DataFrame from a table
  factory DataFrame.fromTable(Table table) {
    return DataFrame._(table: table);
  }

  /// Create DataFrame from rows
  factory DataFrame.fromRows(
    List<Map<String, dynamic>> rows, {
    Schema? schema,
  }) {
    // Pass [schema] straight through: Table.fromRows already infers over ALL
    // rows when none is given, so inferring here would only make an inferred
    // schema look explicit — and an explicit schema is held to stricter rules.
    final table = Table.fromRows('dataframe', rows, schema: schema);
    return DataFrame._(table: table);
  }

  /// Create empty DataFrame with schema
  factory DataFrame.empty(Schema schema) {
    final table = Table.empty('dataframe', schema);
    return DataFrame._(table: table);
  }

  /// A LAZY DataFrame over a file [path] (any format `openScan` supports). The
  /// source is not read until a terminal op runs, and it is rooted at a streaming
  /// [PhysicalFileScan], so `readX(path).filter(...).head(n)` streams and stops
  /// early instead of decoding the whole file. [schema] is the source schema
  /// (read cheaply from the footer/header by the I/O factory). The placeholder
  /// empty table is never scanned — the leading [ScanOperation] is the plan root.
  factory DataFrame.scan(String path, Schema schema) => DataFrame._(
    table: Table.empty('scan', schema),
    operations: [ScanOperation(path, schema)],
    schema: schema,
  );

  /// Get the schema
  Schema? get schema => _schema;

  /// Select columns
  DataFrame select(List<String> columns) {
    return DataFrame._(
      table: _table,
      operations: [..._operations, SelectOperation(columns)],
      schema: _schema?.select(columns),
    );
  }

  /// Alias for select
  DataFrame operator [](List<String> columns) => select(columns);

  /// Filter rows
  DataFrame filter(String predicate) {
    return DataFrame._(
      table: _table,
      operations: [..._operations, FilterOperation(predicate)],
      schema: _schema,
    );
  }

  /// Alias for filter
  DataFrame where(String predicate) => filter(predicate);

  /// Limit rows
  DataFrame limit(int n, {int offset = 0}) {
    return DataFrame._(
      table: _table,
      operations: [..._operations, LimitOperation(n, offset)],
      schema: _schema,
    );
  }

  /// Order by columns
  DataFrame orderBy(String column, {bool ascending = true}) {
    return DataFrame._(
      table: _table,
      operations: [..._operations, OrderByOperation(column, ascending)],
      schema: _schema,
    );
  }

  /// Group by columns
  DataFrame groupBy(List<String> columns) {
    return DataFrame._(
      table: _table,
      operations: [..._operations, GroupByOperation(columns)],
      schema: _schema,
    );
  }

  /// Join with another DataFrame
  DataFrame join(DataFrame other, String on, {JoinType type = JoinType.inner}) {
    final op = JoinOperation(other, on, type);
    return DataFrame._(
      table: _table,
      operations: [..._operations, op],
      // Derive the joined schema eagerly (left + right fields) so `columns`/
      // `select` on the result aren't stuck on the original left schema.
      schema: op.outputSchema(_schema, const []),
    );
  }

  /// Aggregate functions
  DataFrame agg(Map<String, String> aggregations) {
    final op = AggregateOperation(aggregations);
    // A preceding groupBy carries the group keys for the output schema.
    final keys = _operations.isNotEmpty && _operations.last is GroupByOperation
        ? (_operations.last as GroupByOperation).columns
        : const <String>[];
    return DataFrame._(
      table: _table,
      operations: [..._operations, op],
      schema: op.outputSchema(_schema, keys),
    );
  }

  /// Add a computed column
  DataFrame withColumn(String name, String expression) {
    final op = WithColumnOperation(name, expression);
    return DataFrame._(
      table: _table,
      operations: [..._operations, op],
      // Derive the schema eagerly (input fields + the new/replaced column) so a
      // later select(['x']) sees the added column instead of throwing.
      schema: op.outputSchema(_schema, const []),
    );
  }

  /// Drop columns
  DataFrame drop(List<String> columns) {
    return DataFrame._(
      table: _table,
      operations: [..._operations, DropOperation(columns)],
      schema: _schema != null
          ? Schema(
              fields: _schema.fields
                  .where((f) => !columns.contains(f.name))
                  .toList(),
            )
          : null,
    );
  }

  /// Rename columns
  DataFrame rename(Map<String, String> renames) {
    return DataFrame._(
      table: _table,
      operations: [..._operations, RenameOperation(renames)],
      schema: _schema?.rename(renames),
    );
  }

  /// Get distinct rows
  DataFrame distinct() {
    return DataFrame._(
      table: _table,
      operations: [..._operations, DistinctOperation()],
      schema: _schema,
    );
  }

  /// Union with another DataFrame
  DataFrame union(DataFrame other) {
    return DataFrame._(
      table: _table,
      operations: [..._operations, UnionOperation(other)],
      schema: _schema,
    );
  }

  /// Sample rows
  DataFrame sample(double fraction, {int? seed}) {
    return DataFrame._(
      table: _table,
      operations: [..._operations, SampleOperation(fraction, seed)],
      schema: _schema,
    );
  }

  /// Execute operations and return table. All operations are fused into ONE
  /// physical plan and streamed once (no per-op intermediate materialization;
  /// a trailing `limit` short-circuits the stream instead of materializing the
  /// whole intermediate first).
  ///
  /// Materialization is bounded like SQL's `execute()`: it throws once the
  /// result exceeds [maxRows] (default [defaultMaxMaterializedRows]) or
  /// [maxBytes] (default [defaultMaxMaterializedBytes]); pass `null` to opt out.
  Future<Table> execute({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    EngineOptions? options,
  }) async {
    if (_table == null) {
      throw StateError('DataFrame has no source table');
    }
    if (_operations.isEmpty) {
      // A no-op plan still returns a materialized table, so it must honor the
      // caps like every other path — otherwise callers using execute() as a
      // resource-policy boundary get unbounded results whenever no op happens to
      // be present.
      _checkCaps(_table, maxRows, maxBytes);
      return _table;
    }
    // One execution context carries the caller's limits through EVERY
    // materialization boundary (unfused intermediates + the final result), so a
    // small cap actually bounds a wide intermediate join/union instead of only
    // the final table.
    final ctx = ExecutionContext(
      maxRows: maxRows,
      maxBytes: maxBytes,
      options: options ?? const EngineOptions(),
    );
    return _runPlan(
      await _buildPlan(ctx),
      _table.name,
      maxRows: maxRows,
      maxBytes: maxBytes,
    );
  }

  /// Throw if [table] already exceeds [maxRows]/[maxBytes] — the no-op
  /// materialization gate, so `execute()` bounds a no-op plan like every other.
  static void _checkCaps(Table table, int? maxRows, int? maxBytes) {
    enforceMaterializationCaps(
      table.rowCount,
      table.memoryUsage,
      maxRows,
      maxBytes,
      'DataFrame result',
    );
  }

  /// Stream the result one batch at a time WITHOUT materializing the whole
  /// table — for exports (CSV/JSON) of production-sized results. Each yielded
  /// [Table] is one batch (disambiguated like [execute]); the consumer writes it
  /// out and drops it. An empty result still yields a single empty [Table] so a
  /// header/schema is available.
  ///
  /// One-batch peak memory holds for a FUSIBLE plan (select/filter/limit/
  /// orderBy/join/aggregate…). An unfusible operator — one with no [Operation.
  /// toPhysical] — has to materialize everything upstream of it before it can
  /// run, and a blocking operator buffers its input by nature; neither is
  /// bounded here, because a stream is asked for precisely when the caller does
  /// not want a row/byte ceiling. Use [execute] with [maxRows]/[maxBytes] when
  /// you want the policy enforced.
  Stream<Table> executeStream({EngineOptions? options}) async* {
    if (_table == null) {
      throw StateError('DataFrame has no source table');
    }
    if (_operations.isEmpty) {
      // Slice the source into bounded batches rather than yielding it whole, so
      // the consumer processes one batch at a time (the streaming contract) even
      // on a no-op plan. slice() is zero-copy, so this adds no materialization.
      if (_table.rowCount == 0) {
        yield _table;
        return;
      }
      const batch = 8192;
      for (var start = 0; start < _table.rowCount; start += batch) {
        yield _table.slice(start, math.min(start + batch, _table.rowCount));
      }
      return;
    }
    final plan = await _buildPlan(
      ExecutionContext(
        maxRows: null,
        maxBytes: null,
        options: options ?? const EngineOptions(),
      ),
    );
    final iterator = plan.execute();
    var any = false;
    try {
      RecordBatch? batch;
      while ((batch = await iterator.next()) != null) {
        any = true;
        yield Kernels.disambiguate(
          Table(
            name: _table.name,
            columns: batch!.columns,
            schema: batch.schema,
          ),
        );
      }
    } finally {
      await iterator.close();
    }
    if (!any) {
      yield Kernels.disambiguate(Table.empty(_table.name, plan.schema));
    }
  }

  /// Fold every operation into a single [PhysicalPlan] rooted at a scan of the
  /// source table. An operation that can't be a plan node (e.g. `sample`)
  /// materializes the plan so far, applies imperatively, then fusion resumes
  /// from a fresh scan — so nothing regresses.
  Future<PhysicalPlan> _buildPlan(ExecutionContext ctx) async {
    // A DataFrame from an I/O factory carries a leading ScanOperation: root at a
    // streaming PhysicalFileScan (lazy, one batch at a time) and fold the rest of
    // the operations on top. Otherwise root at the in-memory table.
    var operations = _operations;
    final String rescanName;
    PhysicalPlan plan;
    if (operations.isNotEmpty && operations.first is ScanOperation) {
      final scan = operations.first as ScanOperation;
      plan = PhysicalFileScan(path: scan.path, schema: scan.tableSchema);
      operations = operations.sublist(1);
      rescanName = 'scan';
    } else {
      final table = _table!;
      plan = PhysicalTableScan(tableName: table.name, table: table);
      rescanName = table.name;
    }
    var groupKeys = const <String>[];
    for (final operator in operations) {
      if (operator is GroupByOperation) {
        groupKeys = operator.columns; // carried to the following aggregate
        continue;
      }
      final next = await operator.toPhysical(plan, groupKeys, ctx);
      groupKeys = const [];
      if (next != null) {
        plan = next;
      } else {
        // Unfusible operator: materialize the current plan (bounded by the
        // memory-safety byte cap — NOT maxRows, since a downstream LIMIT may trim
        // rows), apply it, rescan.
        final materialized = await _runPlan(
          plan,
          rescanName,
          maxBytes: ctx.maxBytes,
        );
        final applied = await operator.apply(materialized, ctx);
        plan = PhysicalTableScan(tableName: applied.name, table: applied);
      }
    }
    return plan;
  }

  /// Transform the source table synchronously. Operations are async, so this
  /// only applies when there are no pending operations (otherwise call
  /// [execute] first) — it no longer silently drops them.
  DataFrame transform(Table Function(Table) transformer) {
    if (_table == null) {
      throw StateError('DataFrame has no source table');
    }
    if (_operations.isNotEmpty) {
      throw StateError(
        'transform() cannot apply pending operations synchronously; '
        'await execute() first',
      );
    }
    final transformed = transformer(_table);
    return DataFrame._(table: transformed, schema: transformed.schema);
  }

  /// Return the source table synchronously. Throws if operations are pending
  /// (call [execute] for those) rather than silently ignoring them.
  Table toTable() {
    if (_table == null) {
      throw StateError('DataFrame has no source table');
    }
    if (_operations.isNotEmpty) {
      throw StateError(
        'toTable() cannot apply pending operations; await execute() instead',
      );
    }
    return _table;
  }

  /// Execute and convert to list of rows
  Future<List<Map<String, dynamic>>> toList() async {
    final table = await execute();
    return table.toRows();
  }

  /// Execute and convert to JSON string. Uses [jsonEncode] so all strings and
  /// keys are escaped correctly (backslashes, control characters, quotes);
  /// `DateTime` serializes as an ISO-8601 string and any other non-JSON value
  /// (e.g. `BigInt`) as its string form. Nested maps/lists are encoded natively.
  Future<String> toJsonString() async {
    final rows = await toList();
    return jsonEncode(
      rows,
      toEncodable: (value) =>
          value is DateTime ? value.toIso8601String() : value.toString(),
    );
  }

  /// Get first n rows
  Future<Table> head([int n = 5]) async {
    final limited = limit(n);
    return limited.execute();
  }

  /// Get last n rows
  Future<Table> tail([int n = 5]) async {
    final table = await execute();
    return table.tail(n);
  }

  /// Count rows. Streams the plan and sums batch row counts WITHOUT retaining
  /// columns, so counting a lazy file source (or after a filter) doesn't
  /// materialize the whole result.
  Future<int> count() async {
    final ctx = ExecutionContext(
      maxRows: null,
      maxBytes: null,
      options: const EngineOptions(),
    );
    final iterator = (await _buildPlan(ctx)).execute();
    var rows = 0;
    try {
      for (
        var batch = await iterator.next();
        batch != null;
        batch = await iterator.next()
      ) {
        rows += batch.rowCount;
      }
    } finally {
      await iterator.close();
    }
    return rows;
  }

  /// Show DataFrame info with up to [rows] preview rows. Honors [rows] by
  /// limiting BEFORE execution (so it doesn't materialize the whole source) and
  /// previewing that many rows (rather than a fixed 5).
  Future<void> show([int rows = 20]) async {
    final table = await limit(rows).execute();
    print(table.describe(rows));
  }

  /// Get column names
  List<String>? get columns => _schema?.fieldNames;

  /// Get data types
  Map<String, DataType>? get dtypes {
    if (_schema == null) return null;
    final types = <String, DataType>{};
    for (final field in _schema.fields) {
      types[field.name] = field.dataType;
    }
    return types;
  }

  /// Cache the DataFrame
  Future<DataFrame> cache() async {
    final table = await execute();
    return DataFrame._(table: table);
  }

  /// Clone the DataFrame
  DataFrame clone() {
    return DataFrame._(
      table: _table,
      operations: List.from(_operations),
      schema: _schema,
    );
  }
}

/// Carries a query's resource limits through every materialization boundary
/// (unfused intermediates AND the final result), so an entry-point cap is
/// consistently enforced. Policy: [maxBytes] is a hard memory-safety ceiling
/// applied to EVERY materialization; [maxRows] applies to the FINAL result only
/// (a downstream LIMIT may trim an intermediate's rows, so a row cap there would
/// wrongly reject it).
@immutable
/// The caller's resource policy for one DataFrame execution, threaded through
/// EVERY materialization boundary and into every physical operator.
///
/// [options] reaches the blocking operators (sort/join/aggregate/set-op) — the
/// same ones SQL uses, which DO spill. A DataFrame has no session object, so
/// unlike [SqlEngine] it cannot build shared budgets for you: pass an
/// [EngineOptions] with a `memory`/`spillBudget` to bound the COMBINED footprint
/// of a pipeline's operators, or leave the default and each operator is bounded
/// only by its own `spillThresholdBytes`.
class ExecutionContext {
  final int? maxRows;
  final int? maxBytes;
  final EngineOptions options;
  const ExecutionContext({
    this.maxRows,
    this.maxBytes,
    this.options = const EngineOptions(),
  });
}

/// Join types
enum JoinType { inner, left, right, full, cross }

/// Execute a physical [plan] over an in-memory source and collect the result
/// into a [Table]. This is the bridge that lets DataFrame operations run on the
/// same vectorized engine as SQL.
Future<Table> _runPlan(
  PhysicalPlan plan,
  String name, {
  int? maxRows,
  int? maxBytes,
}) async {
  final iterator = plan.execute();
  final batches = <RecordBatch>[];
  var rows = 0;
  var bytes = 0;
  try {
    RecordBatch? batch;
    while ((batch = await iterator.next()) != null) {
      // Match SQL's bounded collector so DataFrame.execute() has the SAME
      // materialization safety as an equivalent query (a wide join/projection
      // would otherwise OOM through DataFrame while SQL stops). Caps are opt-in
      // per caller; intermediate materializations pass null so a large
      // intermediate feeding a LIMIT isn't wrongly rejected.
      rows += batch!.rowCount;
      bytes += batch.memoryUsage;
      enforceMaterializationCaps(
        rows,
        bytes,
        maxRows,
        maxBytes,
        'DataFrame result',
      );
      batches.add(batch);
    }
  } finally {
    await iterator.close();
  }
  if (batches.isEmpty) {
    return Kernels.disambiguate(Table.empty(name, plan.schema));
  }
  final merged = RecordBatch.concatAll(batches, plan.schema);
  // Disambiguate join-duplicated column names the same way SQL does, so an
  // equivalent DataFrame and query return identically-named columns.
  return Kernels.disambiguate(
    Table(name: name, columns: merged.columns, schema: merged.schema),
  );
}
