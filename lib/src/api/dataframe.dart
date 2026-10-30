import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math' as math;
import 'package:logging/logging.dart';
import 'package:meta/meta.dart';
import '../core/failure.dart';
import '../core/result.dart';
import '../core/table.dart';
import '../core/schema.dart';
import '../core/data_type.dart';
import '../core/limits.dart';
import '../core/scan.dart';
import '../core/paths.dart' show tableNameFromPath;
import '../sql/exec/kernels.dart';
import '../sql/execution/engine_options.dart';
import '../sql/catalog/table_catalog.dart';
import '../sql/parser/sql_parser.dart';
import '../sql/planner/physical_plan.dart';
import '../sql/planner/query_planner.dart'
    show PhysicalPlanBuilder, QueryPlanner;
import '../sql/planner/file_scan.dart';
import '../sql/operators/spill.dart'
    show defaultSpillBase, newSpillSession, sweepOrphanedSpills;
import '../sql/planner/logical_plan.dart' as logical;
import '../util/logging_config.dart' show Loggers;

part 'operations.dart';

/// Represents a lazy DataFrame with chainable operations.
///
/// The terminal operations ([execute], [executeStream], [toList],
/// [toJsonString], [head], [tail], [count], [show], [cache]) return a
/// [Result]: predicates and expressions are caller-supplied strings, so a
/// failure while running the plan is reported as an [Err], never thrown, and
/// mapped as `SqlEngine.execute` maps it.
final class DataFrame {
  static final Logger _log = Logger(Loggers.dataframe);

  final Table? _table;
  final List<Operation> _operations;

  /// The schema the plan produces, its fields still qualified by join side
  /// (`name` of `p` and `name` of `d`); [schema] reports it as a result names
  /// it (`p.name` and `d.name`).
  final Schema? _schema;

  /// A frame over [table] and [operations]. With no operations the schema is
  /// the table's; a derived frame passes the [schema] its operations produce,
  /// and null when that cannot be worked out (the terminal then reports why),
  /// never the source table's.
  DataFrame._({Table? table, List<Operation>? operations, Schema? schema})
    : _table = table,
      _operations = operations ?? [],
      _schema =
          schema ?? ((operations ?? const []).isEmpty ? table?.schema : null);

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
    // schema look explicit, and an explicit schema is held to stricter rules.
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
  /// empty table is never scanned: the leading [ScanOperation] is the plan root.
  factory DataFrame.scan(String path, Schema schema) => DataFrame._(
    table: Table.empty('scan', schema),
    operations: [ScanOperation(path, schema)],
    schema: schema,
  );

  /// The schema, named as a result is: a column more than one side of a join
  /// has is reported under its side's name, `p.name` and `d.name`.
  Schema? get schema => switch (_schema) {
    final Schema schema => Kernels.disambiguate(
      Table.empty('schema', schema),
    ).schema,
    null => null,
  };

  /// Select columns. Every operation that takes column names takes them as
  /// [columns] reports them: `p.name` after a join, and a bare `name` that
  /// more than one side has is [Kind.invalid].
  DataFrame select(List<String> columns) {
    final operation = SelectOperation(columns);
    return DataFrame._(
      table: _table,
      operations: [..._operations, operation],
      schema: _derive(() => operation.outputSchema(_schema)),
    );
  }

  /// Alias for select
  DataFrame operator [](List<String> columns) => select(columns);

  /// Filter rows by [predicate], a SQL expression as in a WHERE clause:
  /// `"First Name" = 'Alice' AND (age > 30 OR dept IS NULL)`. Strings take
  /// single quotes, names that need quoting take double quotes, and `==` is
  /// accepted as `=`. A non-boolean predicate is cast to BOOLEAN.
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

  /// Order by [column]. NULLs sort last in both directions, as in SQL.
  DataFrame orderBy(String column, {bool ascending = true}) {
    return DataFrame._(
      table: _table,
      operations: [..._operations, OrderByOperation(column, ascending)],
      schema: _schema,
    );
  }

  /// Group by [columns]. The next operation must be [agg]; any other makes
  /// every terminal fail with [Kind.invalid].
  DataFrame groupBy(List<String> columns) {
    return DataFrame._(
      table: _table,
      operations: [..._operations, GroupByOperation(columns)],
      schema: _schema,
    );
  }

  /// Join with another DataFrame, as SQL `USING (on)`: the key is one
  /// coalesced column, and a non-key column both sides have is qualified by
  /// its side's name (`p.name` and `d.name`), or `left` and `right` when both
  /// sides have the same name. A cross join ignores [on].
  DataFrame join(DataFrame other, String on, {JoinType type = JoinType.inner}) {
    final operation = JoinOperation(other, on, type, _name);
    return DataFrame._(
      table: _table,
      operations: [..._operations, operation],
      // Derive the joined schema eagerly (left + right fields) so `columns`/
      // `select` on the result aren't stuck on the original left schema.
      schema: _derive(() => operation.outputSchema(_schema)),
    );
  }

  /// The name this frame's columns are qualified by on a join side: a file
  /// root's base name, as SQL qualifies a file, or `dataset` when that is
  /// empty or a glob pattern (`*.parquet`); otherwise its table's name.
  String get _name => switch (_operations.firstOrNull) {
    final ScanOperation scan => switch (tableNameFromPath(scan.path)) {
      final name when name.isEmpty || name.contains(RegExp(r'[*?[]')) =>
        'dataset',
      final name => name,
    },
    _ => _table!.name,
  };

  /// The schema [derive] computes ahead of execution, or null (unknown)
  /// when it fails: a builder never throws, and [execute] reports the same
  /// problem as an [Err] (an unknown aggregate function or column threw
  /// ArgumentError from the builder itself).
  static Schema? _derive(Schema? Function() derive) {
    try {
      return derive();
    } on ArgumentError {
      return null;
    } on FormatException {
      return null;
    } on UnsupportedError {
      return null;
    } on Failure {
      return null;
    }
  }

  /// Aggregate [aggregations], a map from column to function (`count`, `sum`,
  /// `avg` or `mean`, `min`, `max`), grouped by a preceding [groupBy]. Each
  /// result keeps its input column's name: `{'salary': 'sum'}` gives
  /// `salary`.
  DataFrame agg(Map<String, String> aggregations) {
    // A preceding groupBy gives the group keys.
    final keys = switch (_operations.lastOrNull) {
      final GroupByOperation group => group.columns,
      _ => const <String>[],
    };
    final operation = AggregateOperation(aggregations, keys);
    return DataFrame._(
      table: _table,
      operations: [..._operations, operation],
      schema: _derive(() => operation.outputSchema(_schema)),
    );
  }

  /// Add the column [name] computed by [expression], a SQL expression with
  /// standard precedence (`a + 10 * 2` is `a + 20`). A column already named
  /// [name] is replaced in place.
  DataFrame withColumn(String name, String expression) {
    final operation = WithColumnOperation(name, expression);
    return DataFrame._(
      table: _table,
      operations: [..._operations, operation],
      // Derive the schema eagerly (input fields + the new/replaced column) so a
      // later select(['x']) sees the added column instead of throwing.
      schema: _derive(() => operation.outputSchema(_schema)),
    );
  }

  /// Drop columns
  DataFrame drop(List<String> columns) {
    final operation = DropOperation(columns);
    return DataFrame._(
      table: _table,
      operations: [..._operations, operation],
      schema: _derive(() => operation.outputSchema(_schema)),
    );
  }

  /// Rename columns
  DataFrame rename(Map<String, String> renames) {
    final operation = RenameOperation(renames);
    return DataFrame._(
      table: _table,
      operations: [..._operations, operation],
      schema: _derive(() => operation.outputSchema(_schema)),
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

  /// Union with another DataFrame, as SQL `UNION ALL`, matched by position. A
  /// different column count is a [Kind.schema] failure.
  DataFrame union(DataFrame other) {
    final operation = UnionOperation(other);
    return DataFrame._(
      table: _table,
      operations: [..._operations, operation],
      schema: _derive(() => operation.outputSchema(_schema)),
    );
  }

  /// Sample exactly `round(rowCount * fraction)` rows, reproducibly when
  /// [seed] is given. The input is buffered under the memory budget; a
  /// [fraction] outside `[0, 1]` is [Kind.invalid].
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
  /// Materialization is bounded like SQL's `execute()`: it fails with
  /// [Kind.limit] once the result exceeds [maxRows] (default
  /// [defaultMaxMaterializedRows]) or [maxBytes] (default
  /// [defaultMaxMaterializedBytes]); pass `null` to opt out.
  @useResult
  Future<Result<Table, ErrorDetails>> execute({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    EngineOptions? options,
  }) => report(
    () async => Ok(
      await _execute(maxRows: maxRows, maxBytes: maxBytes, options: options),
    ),
    _log,
  );

  Future<Table> _execute({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    EngineOptions? options,
  }) async {
    if (_table == null) {
      throw StateError('DataFrame has no source table');
    }
    if (_operations.isEmpty) {
      // A no-op plan still returns a materialized table, so it must honor the
      // caps like every other path; otherwise callers using execute() as a
      // resource-policy boundary get unbounded results whenever no op happens to
      // be present.
      _checkCaps(_table, maxRows, maxBytes);
      return _table;
    }
    // A reader's caps bound the result of every frame built on it.
    final (rows, bytes) = capsOf(this, maxRows, maxBytes);
    // One execution context carries the caller's limits through EVERY
    // materialization boundary (unfused intermediates + the final result), so a
    // small cap actually bounds a wide intermediate join/union instead of only
    // the final table.
    final original = options ?? const EngineOptions();
    final used = _session(original);
    final context = ExecutionContext(
      maxRows: rows,
      maxBytes: bytes,
      options: used,
    );
    try {
      return await _runPlan(
        await _buildPlan(context),
        _table.name,
        maxRows: rows,
        maxBytes: bytes,
      );
    } finally {
      await _release(original, used);
    }
  }

  /// The file roots of this frame and of every frame it joins or unions.
  Iterable<ScanOperation> get _roots sync* {
    for (final operation in _operations) {
      switch (operation) {
        case ScanOperation():
          yield operation;
        case JoinOperation(:final other) || UnionOperation(:final other):
          yield* other._roots;
        default:
      }
    }
  }

  /// Whether this process has swept the default spill base for sessions that
  /// crashed runs left behind.
  static bool _swept = false;

  /// [options] for one execution, with a spill session of its own when the
  /// caller set no spill directory: runs then sit inside a session directory,
  /// which [sweepOrphanedSpills] reclaims if the process dies, rather than
  /// loose in the system temp, where nothing would.
  static EngineOptions _session(EngineOptions options) {
    if (options.spillDir != null || !options.spills) return options;
    if (!_swept) {
      _swept = true;
      unawaited(sweepOrphanedSpills(defaultSpillBase));
    }
    return EngineOptions(
      spillThresholdBytes: options.spillThresholdBytes,
      spillDir: newSpillSession(defaultSpillBase),
      maxOutputRows: options.maxOutputRows,
      memory: options.memory,
      spillBudget: options.spillBudget,
      maxMaterializedRows: options.maxMaterializedRows,
      maxMaterializedBytes: options.maxMaterializedBytes,
      timeout: options.timeout,
    );
  }

  /// Remove the session directory [_session] gave [used], if it made one.
  static Future<void> _release(
    EngineOptions original,
    EngineOptions used,
  ) async {
    if (identical(original, used)) return;
    try {
      final directory = Directory(used.spillDir!);
      if (await directory.exists()) await directory.delete(recursive: true);
    } on FileSystemException {
      // Best effort: a leftover session is reclaimed by a later sweep.
    }
  }

  /// Throw if [table] already exceeds [maxRows]/[maxBytes]: the materialization
  /// check for a no-op plan, so `execute()` bounds it like every other.
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
  /// table, for exports (CSV/JSON) of production-sized results. Each yielded
  /// [Table] is one batch (disambiguated like [execute]); the consumer writes it
  /// out and drops it. An empty result still yields a single empty [Table] so a
  /// header/schema is available.
  ///
  /// One-batch peak memory holds for a streaming plan (select/filter/limit/
  /// projection…). A blocking operator (orderBy, join, aggregate) buffers its
  /// input by nature, under its spill threshold and the options' memory
  /// budget, and `sample` holds its whole input, under the options'
  /// `maxMaterializedBytes` (the library default when unset). The result
  /// itself is not bounded here, because a stream is asked for precisely when
  /// the caller does not want a row/byte ceiling. Use [execute] with
  /// [maxRows]/[maxBytes] when you want the policy enforced.
  ///
  /// Each batch arrives as an [Ok]; a failure arrives as one [Err] and ends the
  /// stream, which never errors itself.
  Stream<Result<Table, ErrorDetails>> executeStream({EngineOptions? options}) =>
      reportStream(() => _stream(options: options), _log);

  Stream<Table> _stream({EngineOptions? options}) async* {
    if (_table == null) {
      throw StateError('DataFrame has no source table');
    }
    if (_operations.isEmpty) {
      // Slice the source into bounded batches rather than yielding it whole, so
      // the consumer processes one batch at a time (the streaming contract) even
      // on a no-op plan. slice() copies only its own rows, so each batch holds
      // at most one batch of extra memory.
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
    final original = options ?? const EngineOptions();
    final used = _session(original);
    try {
      final plan = await _buildPlan(
        ExecutionContext(maxRows: null, maxBytes: null, options: used),
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
    } finally {
      await _release(original, used);
    }
  }

  /// Lower every operation into a single [PhysicalPlan]: [_logical] builds
  /// the logical plan in a catalog of its own, and [QueryPlanner.lower] runs
  /// the optimizer rules and the physical builder over it, so a filter and a
  /// projection reach a file scan on either side of a join or union.
  Future<PhysicalPlan> _buildPlan(ExecutionContext context) async {
    final catalog = TableCatalog();
    final plan = _logical(catalog);
    return QueryPlanner(catalog, _capped(context)).lower(plan);
  }

  /// This frame's operations as a logical plan over a scan of its source,
  /// registered in [catalog] (the in-memory table, or a file root as a lazy
  /// source, under a key of its own). A join or union lowers the other frame
  /// into the same catalog and combines the two plans.
  logical.LogicalPlan _logical(TableCatalog catalog) {
    var operations = _operations;
    final key = _free(catalog, _name);
    logical.LogicalPlan plan;
    if (operations.firstOrNull case final ScanOperation scan) {
      catalog.registerSource(
        key,
        LazySource(scan.path, scan.tableSchema, open: scan.open),
      );
      plan = logical.TableScan(tableName: key, tableSchema: scan.tableSchema);
      operations = operations.sublist(1);
    } else {
      final table = _table!;
      catalog.registerTable(key, table);
      plan = logical.TableScan(tableName: key, tableSchema: table.schema);
    }
    // groupBy has no plan node of its own: its keys go to the agg that must
    // follow it directly. Anything else would silently drop the grouping.
    for (int i = 0; i < operations.length; i++) {
      if (operations[i] is GroupByOperation &&
          (i + 1 == operations.length ||
              operations[i + 1] is! AggregateOperation)) {
        throw Failure.of(
          Kind.invalid,
          'groupBy must be followed directly by agg',
        );
      }
    }
    for (final operation in operations) {
      plan = switch (operation) {
        GroupByOperation() => plan,
        JoinOperation(:final other) => operation.combine(
          plan,
          other._logical(catalog),
        ),
        UnionOperation(:final other) => operation.combine(
          plan,
          other._logical(catalog),
        ),
        _ => operation.lower(plan)!,
      };
    }
    return plan;
  }

  /// [name], or [name] with the first number suffix that makes it, a key
  /// [catalog] does not hold yet: two frames over different tables of one
  /// name (every `fromRows` frame is `dataframe`) each get a key of their own.
  static String _free(TableCatalog catalog, String name) {
    var key = name;
    for (
      var i = 1;
      catalog.table(key) != null || catalog.source(key) != null;
      i++
    ) {
      key = '$name$i';
    }
    return key;
  }

  /// [context]'s options with [EngineOptions.maxMaterializedBytes] set to
  /// the byte cap on an intermediate held whole (a sample's input): the
  /// tighter of the caller's [ExecutionContext.maxBytes] and the options'
  /// own cap, and the library default when neither is set (`count()` and
  /// `executeStream()` set no result cap, but the intermediate is still held
  /// in memory whole). The intermediate is not bounded by maxRows, since a
  /// later limit may trim its rows.
  static EngineOptions _capped(ExecutionContext context) {
    final options = context.options;
    final caps = [?context.maxBytes, ?options.maxMaterializedBytes];
    return EngineOptions(
      spillThresholdBytes: options.spillThresholdBytes,
      spillDir: options.spillDir,
      maxOutputRows: options.maxOutputRows,
      memory: options.memory,
      spillBudget: options.spillBudget,
      maxMaterializedRows: options.maxMaterializedRows,
      maxMaterializedBytes: caps.isEmpty
          ? defaultMaxMaterializedBytes
          : caps.reduce(math.min),
      timeout: options.timeout,
    );
  }

  /// Transform the source table synchronously. Operations are async, so this
  /// only applies when there are no pending operations (otherwise call
  /// [execute] first); it no longer silently drops them.
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
  @useResult
  Future<Result<List<Map<String, dynamic>>, ErrorDetails>> toList() =>
      report(() async => Ok((await _execute()).toRows()), _log);

  /// Execute and convert to JSON string. Uses [jsonEncode] so all strings and
  /// keys are escaped correctly (backslashes, control characters, quotes);
  /// `DateTime` serializes as an ISO-8601 string and any other non-JSON value
  /// (e.g. `BigInt`) as its string form. Nested maps/lists are encoded natively.
  @useResult
  Future<Result<String, ErrorDetails>> toJsonString() => report(
    () async => Ok(
      jsonEncode(
        (await _execute()).toRows(),
        toEncodable: (value) =>
            value is DateTime ? value.toIso8601String() : value.toString(),
      ),
    ),
    _log,
  );

  /// Get first n rows
  @useResult
  Future<Result<Table, ErrorDetails>> head([int n = 5]) => limit(n).execute();

  /// Get last n rows
  @useResult
  Future<Result<Table, ErrorDetails>> tail([int n = 5]) =>
      report(() async => Ok((await _execute()).tail(n)), _log);

  /// Count rows. Streams the plan and sums batch row counts WITHOUT retaining
  /// columns, so counting a lazy file source (or after a filter) doesn't
  /// materialize the whole result.
  @useResult
  Future<Result<int, ErrorDetails>> count() =>
      report(() async => Ok(await _count()), _log);

  Future<int> _count() async {
    const original = EngineOptions();
    final used = _session(original);
    try {
      final context = ExecutionContext(
        maxRows: null,
        maxBytes: null,
        options: used,
      );
      final iterator = (await _buildPlan(context)).execute();
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
    } finally {
      await _release(original, used);
    }
  }

  /// Show DataFrame info with up to [rows] preview rows. Honors [rows] by
  /// limiting BEFORE execution (so it doesn't materialize the whole source) and
  /// previewing that many rows (rather than a fixed 5).
  @useResult
  Future<Result<void, ErrorDetails>> show([int rows = 20]) => report(() async {
    print((await limit(rows)._execute()).describe(rows));
    return const Ok(null);
  }, _log);

  /// Get column names
  List<String>? get columns => schema?.fieldNames;

  /// Get data types
  Map<String, DataType>? get dtypes {
    final schema = this.schema;
    if (schema == null) return null;
    final types = <String, DataType>{};
    for (final field in schema.fields) {
      types[field.name] = field.dataType;
    }
    return types;
  }

  /// Cache the DataFrame
  @useResult
  Future<Result<DataFrame, ErrorDetails>> cache() =>
      report(() async => Ok(DataFrame._(table: await _execute())), _log);

  /// Clone the DataFrame
  DataFrame clone() {
    return DataFrame._(
      table: _table,
      operations: List.from(_operations),
      schema: _schema,
    );
  }
}

/// The caller's resource policy for one DataFrame execution, threaded through
/// EVERY materialization boundary (unfused intermediates AND the final result)
/// and into every physical operator, so an entry-point cap is consistently
/// enforced. Policy: [maxBytes] is a hard memory-safety ceiling applied to
/// EVERY materialization; [maxRows] applies to the FINAL result only (a
/// downstream LIMIT may trim an intermediate's rows, so a row cap there would
/// wrongly reject it).
///
/// [options] reaches the blocking operators (sort/join/aggregate/set-op): the
/// same ones SQL uses, which DO spill. A DataFrame has no session object, so
/// unlike [SqlEngine] it cannot build shared budgets for you: pass an
/// [EngineOptions] with a `memory`/`spillBudget` to bound the COMBINED footprint
/// of a pipeline's operators, or leave the default and each operator is bounded
/// only by its own `spillThresholdBytes`.
@immutable
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
      // per caller.
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

/// A lazy DataFrame over [path], as [DataFrame.scan] is, that [open]s its scan
/// with the options its reader was called with and bounds a materialized
/// result by [maxRows]/[maxBytes] as well as the terminal's caps. Not
/// exported: the readers in io_extensions.dart build their frames with it.
DataFrame lazy(
  String path,
  Schema schema,
  Scan Function() open, {
  int? maxRows,
  int? maxBytes,
}) => DataFrame._(
  table: Table.empty('scan', schema),
  operations: [
    ScanOperation(
      path,
      schema,
      open: open,
      maxRows: maxRows,
      maxBytes: maxBytes,
    ),
  ],
  schema: schema,
);

/// The caps on a result read from [frame]: [maxRows]/[maxBytes] joined with
/// the caps of the readers it is built on (its own, and those of the frames
/// it joins or unions), the tighter winning. Not exported.
(int?, int?) capsOf(DataFrame frame, int? maxRows, int? maxBytes) => (
  frame._roots.fold(maxRows, (cap, root) => tighter(cap, root.maxRows)),
  frame._roots.fold(maxBytes, (cap, root) => tighter(cap, root.maxBytes)),
);

/// [frame] as a SQL query reads it once registered: its logical plan, its
/// sources registered in [catalog] under keys of their own, with its columns
/// named as [DataFrame.columns] names them (`p.name` and `d.name` after a
/// join). Not exported: `SqlEngine.registerDataFrame` registers it.
logical.LogicalPlan viewOf(DataFrame frame, TableCatalog catalog) {
  final plan = frame._logical(catalog);
  final fields = plan.schema.fields;
  final names = Kernels.disambiguate(
    Table.empty('view', plan.schema),
  ).schema.fieldNames;
  if (names.indexed.every((entry) => entry.$2 == fields[entry.$1].name)) {
    return plan;
  }
  return logical.Projection(
    input: plan,
    expressions: [
      for (final field in fields)
        logical.ColumnExpression(
          name: field.name,
          table: field.qualifier,
          columnType: field.dataType,
          isNullable: field.nullable,
        ),
    ],
    aliases: names,
  );
}

/// The physical plan [DataFrame.execute] runs for [frame], for tests that
/// inspect its operators. Not exported: the package export shows only
/// [DataFrame] and [JoinType].
Future<PhysicalPlan> planOf(DataFrame frame) =>
    frame._buildPlan(const ExecutionContext());
