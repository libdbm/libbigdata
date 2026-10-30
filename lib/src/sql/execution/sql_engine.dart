import 'dart:async';
import 'dart:io';
import 'package:logging/logging.dart';
import 'package:meta/meta.dart';
import 'package:path/path.dart' as p;
import '../catalog/table_catalog.dart';
import '../../core/table.dart';
import '../../core/schema.dart';
import '../../core/limits.dart';
import '../../core/failure.dart';
import '../../core/paths.dart';
import '../../core/result.dart';
import '../../api/dataframe.dart';
import '../../formats/scan.dart';
import '../parser/sql_parser.dart';
import '../planner/query_planner.dart';
import 'dml.dart';
import 'references.dart';
import 'engine_options.dart';
import 'memory_manager.dart';
import '../operators/spill.dart';
import '../planner/physical_plan.dart';
import '../exec/kernels.dart';
import '../ast/ast.dart';
import '../../core/column.dart';
import '../../core/data_type.dart';
import '../../core/row_filter.dart';
import '../../util/logging_config.dart' show Loggers;

/// SQL execution engine
final class SqlEngine {
  static final Logger _log = Logger(Loggers.sql);

  final TableCatalog _catalog = TableCatalog();
  final SqlParser _parser = SqlParser();
  late final QueryPlanner _planner;
  late final String _session;

  /// Executes UPDATE, INSERT and MERGE with this engine's planner, path
  /// confinement and file reader. A MERGE source query runs as a SELECT does
  /// and its temporaries are released once the source is materialized.
  late final DML _dml = DML(
    planner: _planner,
    resolve: _resolvePath,
    load: _loadFileAsTable,
    select: (query, {required maxRows, required maxBytes}) async {
      try {
        return await _runSelect(query, maxRows: maxRows, maxBytes: maxBytes);
      } finally {
        _cleanup();
      }
    },
  );

  /// True once [dispose] has begun: new queries are rejected so a caller can't
  /// keep using an engine whose spill dir is being reclaimed.
  bool _disposed = false;

  /// In-flight `execute`/`executeStream` operations. [dispose] waits on [_idle]
  /// (completed when this hits zero) before deleting [_session], so it never
  /// yanks a spill dir out from under an active query.
  int _active = 0;

  /// Non-null only while [dispose] is waiting for in-flight operations to drain;
  /// completed by [_release] when [_active] reaches zero.
  Completer<void>? _idle;

  void _checkNotDisposed() {
    if (_disposed) {
      throw StateError('SqlEngine used after dispose()');
    }
  }

  /// Finish one in-flight operation, signalling a waiting [dispose] when the last
  /// one drains (a completer, not a busy-poll).
  void _release() {
    _active--;
    if (_active <= 0 && _idle != null && !_idle!.isCompleted) {
      _idle!.complete();
    }
  }

  final Set<String> _temporaryFileTables = {};
  // CTEs materialized for the current statement (name -> the exact Table this
  // query registered, so cleanup can remove it BY IDENTITY), plus any catalog
  // table they shadow (restored on cleanup).
  final Map<String, Table> _temporaryCteTables = {};
  // What each such name held before this query shadowed it (a table, a lazy
  // file source, or both), keyed in lower case; restored on cleanup.
  final Map<String, (Table?, LazySource?)> _shadowed = {};
  // Views currently being materialized: guards against reference cycles.
  final Set<String> _materializingViews = {};

  // Per-query work is serialized: the engine mutates shared catalog/temp state
  // (file-table registration, CTE/view materialization) and cleans it up per
  // query, so concurrent queries on ONE engine must not interleave their awaits.
  // This makes a single engine safe to call concurrently (queries queue); use
  // separate SqlEngine instances for isolated catalogs / true parallelism.
  Future<void> _queue = Future<void>.value();

  /// Await the in-flight query's lock. When [lockTimeout] is set, a predecessor
  /// whose SETUP runs pathologically long (e.g. a huge CTE materialization)
  /// surfaces a loud [Kind.timeout] here instead of stalling later queries. (An
  /// undrained `executeStream` no longer holds the lock; it is released after
  /// planning, so it can't strand successors regardless of this timeout.)
  ///
  /// A query's own deadline ([Deadline.current]) bounds the wait too, since
  /// its time counts from the call.
  Future<void> _acquire(Future<void> previous) {
    final deadline = Deadline.current;
    final left = deadline?.remaining;
    if (left != null && (lockTimeout == null || left < lockTimeout!)) {
      return previous.timeout(
        left,
        onTimeout: () => throw TimeoutException(
          'Query exceeded its deadline waiting for an earlier query on this '
          'SqlEngine',
          deadline!.timeout,
        ),
      );
    }
    if (lockTimeout == null) return previous;
    return previous.timeout(
      lockTimeout!,
      onTimeout: () => throw Failure.of(
        Kind.timeout,
        'Timed out waiting for a prior query on this SqlEngine to finish after '
        '$lockTimeout; an executeStream result may not have been drained or '
        'cancelled.',
      ),
    );
  }

  /// Run [body] after any in-flight query on this engine completes, so their
  /// shared-state mutations never interleave. Under a query deadline the
  /// caller is answered at the deadline; [late] disposes of a result [body]
  /// produces after that (see [_bounded]).
  Future<T> _serialized<T>(
    Future<T> Function() body, {
    void Function(T value)? late,
  }) {
    final previous = _queue;
    final done = Completer<void>();
    _queue = done.future;
    return _acquire(previous).then(
      // Future.sync so a (hypothetical) synchronously-throwing body still yields
      // a Future that whenComplete can attach to; otherwise the throw would
      // skip done.complete() and strand the queue. Current callers are all async
      // (never throw sync), so this is defensive; the onError path below is
      // unchanged (a timed-out successor must NOT release the slot early).
      (_) => _bounded(Future.sync(body).whenComplete(done.complete), late),
      onError: (Object e, StackTrace stack) {
        // We never ran body (timeout, or the predecessor itself errored). Do
        // NOT release the slot yet on timeout: the predecessor is still in
        // flight and mutating shared state, so tie this slot to its REAL
        // completion. Successors keep waiting for the predecessor (or time out
        // themselves) instead of interleaving on the shared catalog.
        previous.whenComplete(done.complete);
        throw e;
      },
    );
  }

  /// [work] bounded by the current query's deadline. Operators check the
  /// deadline themselves, but an await that makes no operator progress, such
  /// as a slow read, does not. Dart cannot cancel [work], so past the deadline
  /// it runs on: it keeps the engine lock until it ends, counts as in flight
  /// for [dispose], and [late] disposes of a result that arrives after the
  /// caller was answered.
  Future<T> _bounded<T>(Future<T> work, void Function(T value)? late) {
    final deadline = Deadline.current;
    final left = deadline?.remaining;
    if (left == null) return work;
    return work.timeout(
      left,
      onTimeout: () {
        _active++;
        work
            .then((value) => late?.call(value), onError: (Object _) {})
            .whenComplete(_release);
        throw TimeoutException(
          'Query exceeded its deadline during setup',
          deadline!.timeout,
        );
      },
    );
  }

  /// File-path access policy for paths embedded in SQL strings (the untrusted
  /// surface). By default absolute paths and `..`-escaping paths are rejected.
  /// Set [sandboxRoot] to confine reads/writes to a directory, or
  /// [allowExternalPaths] to disable the check entirely (e.g. trusted callers).
  final String? sandboxRoot;
  final bool allowExternalPaths;

  /// Reject every statement that writes: UPDATE, INSERT and MERGE (which
  /// modify tables on disk) and CREATE/DROP VIEW (which modify this engine's
  /// catalog). Defaults to `true` when [sandboxRoot] is set, since a sandbox
  /// signals untrusted SQL; pass `readOnly: false` to allow writes.
  final bool readOnly;

  /// Optional safety valve: the longest a query will wait for a prior query's
  /// SETUP (parse/materialize/plan: the only work that holds the lock) before
  /// failing with [Kind.timeout]. Defaults to `null` (wait indefinitely). Not
  /// needed to bound undrained streams (those release the lock after
  /// planning); a server may still set it as defense-in-depth against a
  /// pathologically slow setup.
  final Duration? lockTimeout;

  /// Service-safe fail-closed defaults (used unless [unbounded] or an explicit
  /// value is passed): a shared in-memory budget and an on-disk spill budget that
  /// bound a runaway query without failing ordinary ones. Generous on purpose:
  /// small queries never touch them; tune per deployment.
  static const _defaultSharedMemoryBytes =
      1 << 30; // 1 GiB across all operators
  static const _defaultSpillBudgetBytes = 32 << 30; // 32 GiB combined on disk

  /// Open an engine. Each engine has its own catalog and its own spill session
  /// directory under [spillDir]; [dispose] it when done. Returns [Kind.io] when
  /// the session directory cannot be created.
  ///
  /// Blocking operators (ORDER BY, GROUP BY) spill to disk once they buffer more
  /// than [spillThresholdBytes]; set it to `0` to keep everything in memory.
  /// [spillDir] defaults to a `libbigdata-spill` subdir of the system temp dir
  /// (a dedicated base so the orphan sweep lists only our own sessions).
  static Result<SqlEngine, ErrorDetails> open({
    String? sandboxRoot,
    bool allowExternalPaths = false,
    bool? readOnly,
    Duration? lockTimeout,
    int spillThresholdBytes = 256 << 20,
    String? spillDir,
    int? maxOutputRows,
    int? memoryBudgetBytes,
    int? maxSpillBytes,
    int? maxMaterializedRows,
    int? maxMaterializedBytes,
    bool unbounded = false,
    bool strictMemory = false,
    Duration? timeout,
  }) {
    try {
      return Ok(
        SqlEngine._(
          sandboxRoot: sandboxRoot,
          allowExternalPaths: allowExternalPaths,
          readOnly: readOnly,
          lockTimeout: lockTimeout,
          spillThresholdBytes: spillThresholdBytes,
          spillDir: spillDir,
          maxOutputRows: maxOutputRows,
          memoryBudgetBytes: memoryBudgetBytes,
          maxSpillBytes: maxSpillBytes,
          maxMaterializedRows: maxMaterializedRows,
          maxMaterializedBytes: maxMaterializedBytes,
          unbounded: unbounded,
          strictMemory: strictMemory,
          timeout: timeout,
        ),
      );
    } catch (error, stack) {
      return Err(caught(error, stack, Kind.io));
    }
  }

  SqlEngine._({
    this.sandboxRoot,
    this.allowExternalPaths = false,
    bool? readOnly,
    this.lockTimeout,
    int spillThresholdBytes = 256 << 20,
    String? spillDir,
    int? maxOutputRows,
    int? memoryBudgetBytes,
    int? maxSpillBytes,
    int? maxMaterializedRows,
    int? maxMaterializedBytes,
    bool unbounded = false,
    bool strictMemory = false,
    Duration? timeout,
  }) : readOnly = readOnly ?? sandboxRoot != null {
    // Fail-closed by default: an explicitly-passed limit always wins; otherwise a
    // bounded service-safe default applies unless the caller opts into unbounded
    // (offline/analytics) use. maxMaterialized* are already bounded downstream.
    final rows =
        maxOutputRows ?? (unbounded ? null : defaultMaxMaterializedRows);
    final memory =
        memoryBudgetBytes ?? (unbounded ? null : _defaultSharedMemoryBytes);
    final spill =
        maxSpillBytes ?? (unbounded ? null : _defaultSpillBudgetBytes);
    // Isolate this engine's spill files in a per-session subdir with a live
    // marker (kept fresh on spill activity), so no other engine's sweep can
    // delete our still-in-use runs: the P0 that silently dropped a partition.
    //
    // The base is a DEDICATED subdir, not the shared temp root: sweepOrphanedSpills
    // lists the base on every engine construction, so pointing it at
    // Directory.systemTemp made every engine enumerate the ENTIRE system temp
    // (thousands of unrelated entries on a busy machine, plus a stat per accrued
    // session): O(temp size) per engine, which starved concurrent spill I/O and
    // could make a spill query take tens of seconds. Confining sessions to
    // `<temp>/libbigdata-spill/` bounds the sweep to this library's own sessions.
    final spillBase = spillDir ?? defaultSpillBase;
    final session = newSpillSession(spillBase);
    // Fail FAST if the session dir can't be created: silently proceeding would
    // hand the planner a spillDir that doesn't exist, so the first spill would
    // die deep inside a blocking operator (mid-query) instead of at construction.
    try {
      Directory(session).createSync(recursive: true);
      File('$session/.live').writeAsStringSync('');
    } on FileSystemException catch (e) {
      throw Failure.of(
        Kind.io,
        'Failed to create spill session directory "$session" '
        '(set spillDir to a writable location): $e',
        e,
      );
    }
    _session = session;
    _planner = QueryPlanner(
      _catalog,
      EngineOptions(
        spillThresholdBytes: spillThresholdBytes,
        spillDir: session,
        maxOutputRows: rows,
        maxMaterializedRows: maxMaterializedRows,
        maxMaterializedBytes: maxMaterializedBytes,
        timeout: timeout,
        // A shared budget bounding combined in-memory use of all blocking
        // operators across this engine's queries.
        // strictMemory enables a HARD ceiling (an oversized-when-empty request is
        // denied → the operator spills, which is safe) instead of the advisory
        // progress-guaranteeing default.
        memory: memory == null
            ? null
            : MemoryManager(memory, hardCap: strictMemory),
        // A shared budget bounding combined on-disk spill across this engine.
        spillBudget: spill == null ? null : SpillBudget(spill),
      ),
    );
    // Reclaim spill files orphaned by a prior crashed session (a SIGKILL exits
    // before any `finally`). Best-effort, age-checked, and unawaited so it
    // never blocks construction or a query.
    unawaited(sweepOrphanedSpills(spillBase));
  }

  /// Release this engine's per-session spill directory. Individual spill files
  /// are removed as their operators close; this removes the now-empty session
  /// dir (and any straggler) so a long-lived process creating a per-request
  /// engine doesn't accumulate one dir per request. Idempotent: rejects further
  /// queries, waits for in-flight ones to settle, then deletes the dir: an
  /// already-gone dir is fine, but a permission/other failure now propagates
  /// instead of being silently swallowed.
  ///
  /// Waits at most [timeout] for in-flight operations to drain (a leaked/undrained
  /// `executeStream` subscription would otherwise pin the wait forever); on
  /// expiry it returns [Kind.timeout] naming the still-active count rather than
  /// hanging or busy-polling. A failure deleting the directory is [Kind.io].
  Future<Result<void, ErrorDetails>> dispose({
    Duration timeout = const Duration(seconds: 30),
  }) => capture(() => _dispose(timeout));

  Future<void> _dispose(Duration timeout) async {
    _disposed = true; // reject NEW operations immediately
    // Never delete the spill dir under an active query/stream: await the last
    // one draining, bounded by [timeout]. (No await separates the guard from the
    // completer assignment, so no decrement can slip through unsignalled.)
    if (_active > 0) {
      final idle = _idle ??= Completer<void>();
      try {
        await idle.future.timeout(timeout);
      } on TimeoutException {
        throw Failure.of(
          Kind.timeout,
          'SqlEngine.dispose() timed out after $timeout with $_active '
          'operation(s) still active (likely an undrained executeStream); drain '
          'or cancel them before disposing.',
        );
      }
    }
    final directory = Directory(_session);
    try {
      if (await directory.exists()) await directory.delete(recursive: true);
    } on PathNotFoundException {
      // A concurrent sweep already removed it; nothing to do.
    }
  }

  /// Validate and resolve a file path from a SQL statement. Throws a
  /// [Kind.denied] failure if the path escapes the sandbox / working directory.
  String _resolvePath(String filePath) {
    if (allowExternalPaths) return filePath;
    final normalized = p.normalize(filePath);
    // [confine] resolves symlinks, so a link inside the boundary that points
    // outside it is rejected, and returns the CANONICAL path it validated, so
    // the file opened is the one checked rather than one a symlink swap could
    // redirect between check and open. canonicalize fails CLOSED (throws) on
    // an unresolvable link. NOTE: pure Dart has no openat/O_NOFOLLOW, so a
    // residual check-then-open TOCTOU on the resolved path remains; a hardened
    // deployment needs an external file-access broker.
    const hint = 'set sandboxRoot or allowExternalPaths to permit';
    if (sandboxRoot != null) {
      final root = p.normalize(sandboxRoot!);
      return confine(
        root,
        p.isAbsolute(normalized)
            ? normalized
            : p.normalize(p.join(root, normalized)),
        label: filePath,
        hint: hint,
        canonical: true,
      );
    }
    if (p.isAbsolute(normalized) || p.split(normalized).contains('..')) {
      throw Failure.of(
        Kind.denied,
        'Access denied: absolute or parent-traversal path "$filePath" '
        '($hint)',
      );
    }
    // The working directory is the boundary.
    final root = Directory.current.path;
    return confine(
      root,
      p.normalize(p.join(root, normalized)),
      label: filePath,
      hint: hint,
      canonical: true,
    );
  }

  /// Execute SQL query and return DataFrame. Like [execute], this materializes
  /// the whole result and reports failures the same way; for large scans
  /// prefer [executeStream].
  @useResult
  Future<Result<DataFrame, ErrorDetails>> sql(
    String query, {
    Duration? timeout,
  }) async => switch (await execute(query, timeout: timeout)) {
    Ok(:final value) => Ok(DataFrame.fromTable(value)),
    Err(:final error) => Err(error),
  }; // execute() is serialized

  /// Execute a SQL query and return the full result as one [Table].
  ///
  /// This materializes every output batch into a single Table. To avoid an
  /// accidental unbounded load OOMing the process, materialization STOPS with
  /// [Kind.limit] once the result exceeds [maxRows] (default
  /// [defaultMaxMaterializedRows]); pass `maxRows: null` to allow an unbounded
  /// result, or use [executeStream] for bounded-memory batches.
  ///
  /// The query fails with [Kind.timeout] once it runs past [timeout] (default
  /// [EngineOptions.timeout]), counted from this call, including any wait for
  /// an earlier query on this engine.
  ///
  /// SQL is untrusted input, so every failure is returned as an [Err], none
  /// thrown: [Kind.invalid] for SQL that does not parse or type-check,
  /// [Kind.unsupported] for SQL this engine does not implement,
  /// [Kind.missing] for an unknown table or file, [Kind.denied] for a path
  /// outside the sandbox or a write on a read-only engine, [Kind.corrupt] for
  /// an unreadable data file, and the writer's kinds for UPDATE, INSERT and
  /// MERGE. Only using a disposed engine throws.
  @useResult
  Future<Result<Table, ErrorDetails>> execute(
    String query, {
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    Duration? timeout,
  }) {
    _checkNotDisposed();
    _active++;
    final deadline = Deadline(timeout ?? _planner.options.timeout);
    return report(
      () => deadline.run(
        () => _serialized<Result<Table, ErrorDetails>>(() async {
          final parsed = _parser.parse(query);
          _checkWritable(parsed);
          if (await _direct(parsed) case final table?) return Ok(table);
          try {
            final (ctes, select) = _unwrap(parsed);
            await _materializeCtes(ctes);
            return Ok(
              await _runSelect(select, maxRows: maxRows, maxBytes: maxBytes),
            );
          } finally {
            _cleanup();
          }
        }),
      ),
      _log,
    ).whenComplete(_release);
  }

  /// The result of [statement] when it is DML or DDL, which run directly
  /// rather than through a query plan, or null for a query. [execute] and
  /// [executeStream] both dispatch through it; a DML failure throws.
  Future<Table?> _direct(Statement statement) async {
    if (statement is UpdateStatement ||
        statement is InsertStatement ||
        statement is MergeStatement) {
      return switch (await _dml.run(statement)) {
        Ok(:final value) => value,
        Err(:final error) => throw Failure(error),
      };
    }
    if (statement is CreateViewStatement || statement is DropViewStatement) {
      return _runDdl(statement);
    }
    return null;
  }

  /// Throws [Kind.denied] when [readOnly] is set and [statement] writes.
  void _checkWritable(Statement statement) {
    if (!readOnly) return;
    if (statement is UpdateStatement ||
        statement is InsertStatement ||
        statement is MergeStatement ||
        statement is CreateViewStatement ||
        statement is DropViewStatement) {
      throw Failure.of(
        Kind.denied,
        'Access denied: ${statement.runtimeType} on a read-only engine',
      );
    }
  }

  /// Split a parsed query into its CTE list and the inner SELECT.
  (List<CommonTableExpression>, SelectStatement) _unwrap(Statement s) =>
      s is QueryStatement
      ? (s.ctes, s.select)
      : (const <CommonTableExpression>[], s as SelectStatement);

  /// Preprocess → plan → execute one SELECT into a materialized Table.
  /// [maxRows]/[maxBytes] cap the materialized result (default caps for the
  /// top-level result and for CTE/view intermediates).
  Future<Table> _runSelect(
    SelectStatement select, {
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) async {
    final (rows, bytes) = await _preprocessStatement(select);
    return _executePhysicalPlan(
      _planner.plan(select),
      maxRows: tighter(maxRows, rows),
      maxBytes: tighter(maxBytes, bytes),
    );
  }

  /// Materialize each CTE once into a temporary catalog table (in order, so a
  /// later CTE can reference an earlier one). References then resolve as plain
  /// tables, getting alias qualification for free. RECURSIVE is rejected.
  Future<void> _materializeCtes(List<CommonTableExpression> ctes) async {
    final names = <String>{};
    for (final cte in ctes) {
      if (!names.add(cte.name.toLowerCase())) {
        throw Failure.of(Kind.invalid, 'Duplicate CTE name: ${cte.name}');
      }
    }
    for (final cte in ctes) {
      if (cte.recursive) {
        throw UnsupportedError('WITH RECURSIVE is not supported');
      }
      var table = await _runSelect(cte.query);
      if (cte.columns.isNotEmpty) {
        table = _relabel(table, cte.columns);
      }
      _shadow(cte.name, table);
    }
  }

  /// Register [table] under [name] for this query, hiding whatever the name
  /// held (a table, or a lazily read file) until [_clearTemporaryCtes]. Only
  /// the first shadow of a name records the original.
  void _shadow(String name, Table table) {
    _shadowed.putIfAbsent(
      name.toLowerCase(),
      () => (_catalog.table(name), _catalog.source(name)),
    );
    _catalog.removeSource(name);
    _catalog.registerTable(name, table);
    _temporaryCteTables[name.toLowerCase()] = table;
  }

  /// Rename a table's columns positionally (for `WITH t(a, b) AS ...`).
  Table _relabel(Table table, List<String> names) {
    final fields = table.schema.fields;
    if (names.length != fields.length) {
      throw ArgumentError(
        'CTE column list has ${names.length} names for ${fields.length} columns',
      );
    }
    return Table(
      name: table.name,
      schema: Schema(
        fields: [
          for (int i = 0; i < names.length; i++)
            Field(
              name: names[i],
              dataType: fields[i].dataType,
              nullable: fields[i].nullable,
            ),
        ],
      ),
      columns: [
        for (int i = 0; i < names.length; i++)
          Kernels.rename(table.columns[i], names[i]),
      ],
    );
  }

  void _cleanup() {
    _clearTemporaryFileTables();
    _clearTemporaryCtes();
    _materializingViews.clear();
  }

  /// Execute a DDL statement (CREATE/DROP VIEW) against the persistent view
  /// registry; returns a one-row status result.
  Table _runDdl(Statement statement) {
    if (statement is CreateViewStatement) {
      if (_catalog.hasView(statement.name)) {
        if (statement.ifNotExists) {
          return _status('view exists: ${statement.name}');
        }
        if (!statement.orReplace) {
          throw ArgumentError('View already exists: ${statement.name}');
        }
      }
      _catalog.registerView(statement.name, statement);
      return _status('CREATE VIEW ${statement.name}');
    }
    if (statement is DropViewStatement) {
      if (!_catalog.hasView(statement.name)) {
        if (statement.ifExists) {
          return _status('view absent: ${statement.name}');
        }
        throw Failure.of(Kind.missing, 'View not found: ${statement.name}');
      }
      _catalog.removeView(statement.name);
      return _status('DROP VIEW ${statement.name}');
    }
    throw UnsupportedError('Unsupported DDL statement: $statement');
  }

  Table _status(String message) => Table(
    name: 'result',
    columns: [
      ListColumn<String>(
        name: 'result',
        dataType: DataType.string,
        data: [message],
        nullable: false,
      ),
    ],
    schema: Schema(
      fields: [
        Field(name: 'result', dataType: DataType.string, nullable: false),
      ],
    ),
  );

  void _clearTemporaryCtes() {
    for (final entry in _temporaryCteTables.entries) {
      // Remove BY IDENTITY, not by name. `registerTable` is synchronous and
      // does not join the query queue, so a caller can replace this name
      // between the CTE's registration and here: removing by name would then
      // silently delete THEIR table (and strand any LazySource registered with
      // it, which only the file-table teardown clears). If the name no longer
      // holds our CTE table, someone else owns it now: leave it, and drop the
      // shadow we would otherwise restore over it.
      final prior = _shadowed.remove(entry.key);
      if (!identical(_catalog.table(entry.key), entry.value)) continue;
      _catalog.removeTable(entry.key);
      if (prior == null) continue;
      final (table, source) = prior;
      if (table != null) _catalog.registerTable(entry.key, table);
      if (source != null) _catalog.registerSource(entry.key, source);
    }
    _temporaryCteTables.clear();
    _shadowed.clear();
  }

  /// Execute a query and stream the result one [Table] batch at a time, without
  /// merging into a single Table. Scan→Filter→Projection→Limit plans stream
  /// end-to-end with bounded memory (Parquet sources read lazily); blocking
  /// operators (aggregate/sort/join/set-op) still buffer their input by nature.
  ///
  /// The engine lock is held only for SETUP (parse/materialize/plan), NOT for the
  /// consumer's drain, so a slow or never-drained stream cannot block other
  /// queries on this engine.
  ///
  /// Each batch arrives as an [Ok]. A failure arrives as one [Err], reported
  /// as [execute] reports it, and ends the stream; the stream itself never
  /// errors. It fails with [Kind.timeout] once it runs past [timeout] (default
  /// [EngineOptions.timeout]), counted from this call and including the
  /// consumer's time between batches. Like [execute], calling it on a
  /// disposed engine throws; an engine disposed after the call but before the
  /// stream is listened to gives one [Err] of [Kind.invalid].
  Stream<Result<Table, ErrorDetails>> executeStream(
    String query, {
    Duration? timeout,
  }) {
    // Here, not in the generator: an async* body only starts when the stream
    // is listened to, so the check and the deadline both waited until then.
    _checkNotDisposed();
    return _streamed(query, Deadline(timeout ?? _planner.options.timeout));
  }

  Stream<Result<Table, ErrorDetails>> _streamed(
    String query,
    Deadline deadline,
  ) async* {
    if (_disposed) {
      yield const Err(
        ErrorDetails(
          Kind.invalid,
          'SqlEngine was disposed before the stream was listened to',
        ),
      );
      return;
    }
    _active++;
    try {
      yield* reportStream(() => _executeStream(query, deadline), _log);
    } finally {
      _release();
    }
  }

  Stream<Table> _executeStream(String query, Deadline deadline) async* {
    // Build the plan + iterator UNDER the engine lock (setup mutates the shared
    // catalog/temp state), then RELEASE the lock before streaming. The physical
    // plan binds CONCRETE sources at plan time (PhysicalTableScan.table /
    // PhysicalFileScan.path) and no operator dereferences the catalog during
    // iteration, so `_cleanup()` is safe to run before the stream and a slow or
    // abandoned consumer cannot hold the lock for the whole drain (which would
    // deadlock every later query on this engine).
    final (result, iterator) = await deadline.run(
      () => _serialized<(Table?, RecordBatchIterator?)>(() async {
        final parsed = _parser.parse(query);
        _checkWritable(parsed);
        if (await _direct(parsed) case final table?) return (table, null);
        try {
          final (ctes, select) = _unwrap(parsed);
          await _materializeCtes(ctes);
          await _preprocessStatement(select);
          return (null, _planner.plan(select).execute());
        } finally {
          _cleanup();
        }
      }, late: (value) => unawaited(value.$2?.close())),
    );
    if (iterator == null) {
      yield result!; // DML/DDL: a single bounded result, lock already released.
      return;
    }
    try {
      RecordBatch? batch;
      while ((batch = await deadline.run(iterator.next)) != null) {
        yield _display(
          Table(name: 'result', schema: batch!.schema, columns: batch.columns),
        );
      }
    } finally {
      // Runs on completion, break, or subscription cancel: cascades through the
      // operator tree to release spill writers / file descriptors.
      await iterator.close();
    }
  }

  /// Register a table.
  ///
  /// Synchronous, so it cannot join the query queue; it lands wherever the
  /// caller's control flow puts it, possibly between an in-flight query's
  /// awaits. That is safe: the query's CTE teardown removes only tables it
  /// still owns (see `_clearTemporaryCtes`), so a name registered here is never
  /// silently dropped. It CAN still shadow a CTE of the same name for a query
  /// that has not finished binding: register before starting the query, or use
  /// separate engines, if that matters.
  void registerTable(String name, Table table) {
    _catalog.removeFrame(name);
    _catalog.registerTable(name, table);
  }

  /// Register a DataFrame as a view: [df] is not run here. Each query that
  /// reads [name] plans the frame's operations with its own, through the same
  /// optimizer, so a query's WHERE prunes a file-backed frame's row groups and
  /// its columns narrow the frame's scans; each such query reads the frame's
  /// sources again. The table's columns are the frame's, named as
  /// [DataFrame.columns] names them, and a query's alias qualifies them. The
  /// frame's sources are held under names of their own, so they neither take
  /// nor hide a name registered in this engine. It replaces a table or file
  /// registered as [name].
  ///
  /// [maxRows]/[maxBytes] bound the result of each [execute] that reads
  /// [name], as a reader's caps bound a frame built on it: the tighter of
  /// these, the caps of the readers [df] is built on and the query's own
  /// applies, and a result past it fails with [Kind.limit]. Pass null to leave
  /// the bound to the query. They bound the result, not the rows read, so
  /// `SELECT COUNT(*)` over a larger frame succeeds; [executeStream] sets no
  /// result cap.
  ///
  /// Serialized against in-flight queries: this mutates the shared catalog, so
  /// letting it land mid-query could shadow a CTE the query is still resolving.
  ///
  /// The frame is planned once here, so an invalid expression or an unknown
  /// column fails here; what only reading shows (a source file removed since)
  /// fails the query. A failure is the DataFrame's own, or [Kind.timeout]
  /// past [lockTimeout].
  @useResult
  Future<Result<void, ErrorDetails>> registerDataFrame(
    String name,
    DataFrame df, {
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => report(() async {
    final roots = TableCatalog();
    QueryPlanner(roots, _planner.options).lower(viewOf(df, roots));
    final (rows, bytes) = capsOf(df, maxRows, maxBytes);
    await _serialized(() async {
      _catalog.removeTable(name);
      _catalog.removeSource(name);
      _catalog.registerFrame(
        name,
        (catalog) => viewOf(df, catalog),
        maxRows: rows,
        maxBytes: bytes,
      );
    });
    return const Ok(null);
  }, _log);

  /// Register a file as a table (like DuckDB). Parquet / CSV / JSON-Lines are
  /// registered lazily (schema only + a streaming source) so a large file isn't
  /// materialized at registration; it streams when queried; JSON arrays and
  /// unknown formats fall back to an eager load. A Delta or Iceberg table
  /// directory is registered lazily too, its scans pruning files by partition
  /// and stats.
  /// The registration persists across queries (it is not a per-query
  /// temporary).
  /// Serialized against in-flight queries: it mutates the shared catalog (a
  /// table AND a LazySource), so landing mid-query could shadow a CTE the query
  /// is still resolving.
  ///
  /// A failure is reported as [execute] reports it: [Kind.denied] for a path
  /// outside the sandbox, [Kind.missing] for a file that does not exist,
  /// [Kind.invalid] for an unsupported extension, [Kind.corrupt] for an
  /// unreadable file, and [Kind.timeout] past [lockTimeout].
  @useResult
  Future<Result<void, ErrorDetails>> registerFile(
    String tableName,
    String filePath,
  ) => report(() async {
    final resolved = _resolvePath(filePath);
    if (!await File(resolved).exists() &&
        !await Directory('$resolved/_delta_log').exists() &&
        !await Directory('$resolved/metadata').exists()) {
      throw Failure.of(Kind.missing, 'File not found: $filePath');
    }
    await _serialized(() {
      _catalog.removeFrame(tableName);
      return _registerLazyOrEager(
        tableName,
        resolved,
        tableName,
        temporary: false,
      );
    });
    return const Ok(null);
  }, _log);

  /// Register [resolved] in the catalog under [catalogKey] (display [label]):
  /// for streamable formats a schema-only [Table.empty] + a [LazySource];
  /// otherwise an eagerly-loaded Table. When [temporary] the key joins
  /// `_temporaryFileTables` (torn down by `_cleanup`); persistent registrations
  /// (e.g. [registerFile]) must NOT, or the next query's cleanup would drop them.
  Future<void> _registerLazyOrEager(
    String catalogKey,
    String resolved,
    String label, {
    required bool temporary,
  }) async {
    // Single format registry (openScan): the same extension set + unknown-ext
    // error as every other entry point. A cheap schema means the source can be
    // scanned lazily; a null schema (empty / not discoverable) falls back to an
    // eager load. A source is shared by every scan of the file in a query, so
    // pushed filters live on each scan node, never here.
    final scan = openScan(resolved);
    final Schema? schema;
    try {
      schema = await scan.readSchema();
    } on FormatException catch (e) {
      throw Failure.of(
        Kind.corrupt,
        'Failed to read the schema of $resolved: ${e.message}',
        e,
      );
    }
    if (schema != null) {
      _catalog.registerTable(catalogKey, Table.empty(label, schema));
      _catalog.registerSource(catalogKey, LazySource(resolved, schema));
    } else {
      _catalog.registerTable(catalogKey, await _loadFileAsTable(resolved));
    }
    if (temporary) _temporaryFileTables.add(catalogKey);
  }

  /// Load a file as a table. [filter] (a Parquet row-group pruning predicate
  /// derived from the query WHERE) is applied only to Parquet files.
  Future<Table> _loadFileAsTable(String filePath, {RowFilter? filter}) async {
    try {
      return await openScan(filePath).read(filter: filter);
    } on Failure {
      rethrow;
    } on ArgumentError {
      rethrow; // unknown extension / bad path: surface as-is.
    } on FileSystemException {
      rethrow; // retryable I/O (missing file, permissions): keep the type.
    } catch (e, stack) {
      // Malformed data, an unsupported codec, or anything else the reader
      // raised: the file is unreadable. Keep the ORIGINAL stack so the
      // failure's origin isn't lost (a bare `throw` here would reset it).
      Error.throwWithStackTrace(
        Failure.of(
          Kind.corrupt,
          'Failed to read ${filePath.split('.').last.toLowerCase()} file '
          '$filePath: ${e is FormatException ? e.message : e}',
          e,
        ),
        stack,
      );
    }
  }

  /// List registered tables
  List<String> listTables() {
    return _catalog.tableNames;
  }

  /// Every registered table, by name.
  Map<String, Table> get tables => _catalog.tables;

  /// The schema of the table registered as [name], or null.
  Schema? schema(String name) => _catalog.table(name)?.schema;

  /// Execute physical plan and return Table
  Future<Table> _executePhysicalPlan(
    PhysicalPlan plan, {
    int? maxRows,
    int? maxBytes,
  }) async {
    final iterator = plan.execute();
    final batches = <RecordBatch>[];

    try {
      // Collect batches, stopping (and closing the iterator) the moment the
      // running row count OR byte size would exceed the cap, so an unbounded
      // result is never fully materialized.
      var rows = 0;
      var bytes = 0;
      RecordBatch? batch;
      while ((batch = await iterator.next()) != null) {
        rows += batch!.rowCount;
        bytes += batch.memoryUsage;
        enforceMaterializationCaps(
          rows,
          bytes,
          maxRows,
          maxBytes,
          'query result',
        );
        batches.add(batch);
      }
    } finally {
      await iterator.close();
    }

    return _display(
      batches.isEmpty
          ? Table.empty('result', plan.schema)
          : _mergeBatches(batches),
    );
  }

  /// Disambiguate a terminal result's column names (shared with DataFrame via
  /// [Kernels.disambiguate]): a bare name shared by more than one field becomes
  /// `qualifier.name`. Applied only at the user-facing boundary.
  Table _display(Table table) => Kernels.disambiguate(table);

  /// Merge record batches into a table. Every batch must have the SAME column
  /// count as the schema: a mismatch is a planning bug (e.g. an unbound set
  /// operation forwarding a wider branch), NOT something to silently mask by
  /// filtering out short batches / dropping extra columns.
  Table _mergeBatches(List<RecordBatch> batches) {
    assert(batches.isNotEmpty, 'Cannot merge empty batches');

    final schema = batches.first.schema;
    final n = schema.fields.length;
    for (final batch in batches) {
      if (batch.columns.length != n) {
        throw StateError(
          'Result batch has ${batch.columns.length} columns but the result '
          'schema has $n; the plan produced inconsistent batch shapes',
        );
      }
    }
    final columns = List.generate(n, (i) {
      final pieces = [for (final batch in batches) batch.columns[i]];
      return pieces.length == 1 ? pieces.first : Kernels.concat(pieces);
    });

    return Table(name: 'result', schema: schema, columns: columns);
  }

  /// Pre-process statement to handle file paths and views, giving the caps
  /// of the registered DataFrames it reads (the tighter of each).
  Future<(int?, int?)> _preprocessStatement(SelectStatement statement) async {
    // Walk the AST for every file path and table name it references.
    final references = References();
    statement.accept(references);

    // Load each unique file path, keyed by the raw path string (so two files
    // sharing a basename coexist). Parquet and CSV/JSON-Lines are read lazily (a
    // streaming scan); JSON arrays (and anything else) are materialized eagerly.
    for (final filePath in references.files) {
      final key = fileKey(filePath);
      if (_catalog.hasTable(key)) continue; // already loaded this path
      final resolved = _resolvePath(filePath);
      final label = tableNameFromPath(filePath); // basename, display only
      // Per-query temporary: keyed by the raw path, torn down by _cleanup.
      await _registerLazyOrEager(key, resolved, label, temporary: true);
    }

    // Materialize any referenced views (like CTEs). A registered table or CTE
    // temp shadows a registered DataFrame, and both shadow a view of the same
    // name.
    int? rows;
    int? bytes;
    for (final name in references.tables) {
      if (_catalog.hasTable(name)) continue;
      final frame = _catalog.frame(name);
      if (frame != null) {
        rows = tighter(rows, frame.maxRows);
        bytes = tighter(bytes, frame.maxBytes);
      } else if (_catalog.hasView(name)) {
        await _materializeView(name);
      }
    }
    return (rows, bytes);
  }

  /// Materialize a view's body into a temporary catalog table (recursively, so a
  /// view referencing another view works), guarding against reference cycles.
  Future<void> _materializeView(String name) async {
    if (!_materializingViews.add(name)) {
      throw StateError('Recursive view reference: $name');
    }
    try {
      final definition = _catalog.view(name)!;
      var table = await _runSelect(definition.query);
      if (definition.columns.isNotEmpty) {
        table = _relabel(table, definition.columns);
      }
      _shadow(name, table);
    } finally {
      _materializingViews.remove(name);
    }
  }

  /// Clear temporary file tables
  void _clearTemporaryFileTables() {
    for (final tableName in _temporaryFileTables) {
      _catalog.removeTable(tableName);
      _catalog.removeSource(tableName);
    }
    _temporaryFileTables.clear();
  }
}

/// SQL extension for DataFrame
extension SqlDataFrame on DataFrame {
  /// Create a SQL engine. NOTE: the caller OWNS the returned engine and must
  /// `await engine.dispose()` when done, or it leaks a per-session spill dir.
  static Result<SqlEngine, ErrorDetails> createEngine() => SqlEngine.open();

  /// Execute SQL query with this DataFrame as the table [tableName], in an
  /// engine of its own that is disposed before this returns, reporting
  /// failures as [SqlEngine.execute] does. The frame is registered as
  /// [SqlEngine.registerDataFrame] registers it, so the query plans the
  /// frame's operations with its own.
  @useResult
  Future<Result<DataFrame, ErrorDetails>> sql(
    String query, {
    String tableName = 'df',
  }) async {
    final SqlEngine engine;
    switch (SqlEngine.open()) {
      case Ok(:final value):
        engine = value;
      case Err(:final error):
        return Err(error);
    }
    try {
      if (await engine.registerDataFrame(tableName, this) case Err(
        :final error,
      )) {
        return Err(error);
      }
      return await engine.sql(query);
    } finally {
      // The result is fully materialized before return, so disposing here is safe
      // and stops this per-call engine from leaking a session dir every call.
      await engine.dispose();
    }
  }
}

/// Run [query] on a shared convenience [SqlEngine], reporting failures as
/// [SqlEngine.execute] does. Queries on it are serialized (safe to call
/// concurrently). It is given a 30-second [SqlEngine.lockTimeout] so a single
/// abandoned query can't stall every later one. For a service, prefer a
/// per-request [SqlEngine.open] (isolated catalog + independent lifecycle).
@useResult
Future<Result<Table, ErrorDetails>> sql(
  String query, {
  Duration? timeout,
}) async => switch (_shared) {
  Ok(:final value) => await value.execute(query, timeout: timeout),
  Err(:final error) => Err(error),
};

/// The engine behind [sql], opened on first use.
final _shared = SqlEngine.open(lockTimeout: const Duration(seconds: 30));
