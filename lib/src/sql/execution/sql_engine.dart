import 'dart:async';
import 'dart:io';
import 'package:path/path.dart' as p;
import '../catalog/table_catalog.dart';
import '../../core/table.dart';
import '../../core/schema.dart';
import '../../core/limits.dart';
import '../../core/paths.dart';
import '../../api/dataframe.dart';
import '../../formats/scan.dart';
import '../parser/sql_parser.dart';
import '../planner/query_planner.dart';
import 'engine_options.dart';
import 'memory_manager.dart';
import '../operators/spill.dart';
import '../../core/ids.dart' as ids;
import '../planner/physical_plan.dart';
import '../exec/kernels.dart';
import '../exec/where_pushdown.dart';
import '../ast/ast.dart';
import '../../core/column.dart';
import '../../core/columnar.dart' show columnsFromRows;
import '../../core/data_type.dart';
import '../../core/decimal.dart';
import '../../core/row_filter.dart';
import '../../formats/delta/delta_writer.dart';
import '../../formats/delta/delta_log.dart' show DeltaLog;
import '../../formats/delta/delta_schema.dart' show DeltaSchema;

/// SQL execution engine
class SqlEngine {
  final TableCatalog _catalog = TableCatalog();
  final SqlParser _parser = SqlParser();
  late final QueryPlanner _planner;
  late final String _sessionDir;

  /// True once [dispose] has begun: new queries are rejected so a caller can't
  /// keep using an engine whose spill dir is being reclaimed.
  bool _disposed = false;

  /// In-flight `execute`/`executeStream` operations. [dispose] waits on [_idle]
  /// (completed when this hits zero) before deleting [_sessionDir], so it never
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
  final Map<String, Table> _shadowedCtes = {};
  // Views currently being materialized — guards against reference cycles.
  final Set<String> _materializingViews = {};

  // Per-query work is serialized: the engine mutates shared catalog/temp state
  // (file-table registration, CTE/view materialization) and cleans it up per
  // query, so concurrent queries on ONE engine must not interleave their awaits.
  // This makes a single engine safe to call concurrently (queries queue); use
  // separate SqlEngine instances for isolated catalogs / true parallelism.
  Future<void> _queue = Future<void>.value();

  /// Await the in-flight query's lock. When [lockTimeout] is set, a predecessor
  /// whose SETUP runs pathologically long (e.g. a huge CTE materialization)
  /// surfaces a loud [StateError] here instead of stalling later queries. (An
  /// undrained `executeStream` no longer holds the lock — it is released after
  /// planning — so it can't strand successors regardless of this timeout.)
  Future<void> _acquire(Future<void> previous) {
    if (lockTimeout == null) return previous;
    return previous.timeout(
      lockTimeout!,
      onTimeout: () => throw StateError(
        'Timed out waiting for a prior query on this SqlEngine to finish after '
        '$lockTimeout; an executeStream result may not have been drained or '
        'cancelled.',
      ),
    );
  }

  /// Run [body] after any in-flight query on this engine completes, so their
  /// shared-state mutations never interleave.
  Future<T> _serialized<T>(Future<T> Function() body) {
    final previous = _queue;
    final done = Completer<void>();
    _queue = done.future;
    return _acquire(previous).then(
      // Future.sync so a (hypothetical) synchronously-throwing body still yields
      // a Future that whenComplete can attach to — otherwise the throw would
      // skip done.complete() and strand the queue. Current callers are all async
      // (never throw sync), so this is defensive; the onError path below is
      // unchanged (a timed-out successor must NOT release the slot early).
      (_) => Future.sync(body).whenComplete(done.complete),
      onError: (Object e, StackTrace st) {
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

  /// File-path access policy for paths embedded in SQL strings (the untrusted
  /// surface). By default absolute paths and `..`-escaping paths are rejected.
  /// Set [sandboxRoot] to confine reads/writes to a directory, or
  /// [allowExternalPaths] to disable the check entirely (e.g. trusted callers).
  final String? sandboxRoot;
  final bool allowExternalPaths;

  /// Optional safety valve: the longest a query will wait for a prior query's
  /// SETUP (parse/materialize/plan — the only work that holds the lock) before
  /// throwing. Defaults to `null` (wait indefinitely). Not needed to bound
  /// undrained streams (those release the lock after planning); a server may
  /// still set it as defense-in-depth against a pathologically slow setup.
  final Duration? lockTimeout;

  /// Blocking operators (ORDER BY, GROUP BY) spill to disk once they buffer more
  /// than [spillThresholdBytes]; set it to `0` to keep everything in memory.
  /// [spillDir] defaults to a `libbigdata-spill` subdir of the system temp dir
  /// (a dedicated base so the orphan sweep lists only our own sessions).
  /// Service-safe fail-closed defaults (used unless [unbounded] or an explicit
  /// value is passed): a shared in-memory budget and an on-disk spill budget that
  /// bound a runaway query without failing ordinary ones. Generous on purpose —
  /// small queries never touch them; tune per deployment.
  static const _defaultSharedMemoryBytes =
      1 << 30; // 1 GiB across all operators
  static const _defaultSpillBudgetBytes = 32 << 30; // 32 GiB combined on disk

  SqlEngine({
    this.sandboxRoot,
    this.allowExternalPaths = false,
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
  }) {
    // Fail-closed by default: an explicitly-passed limit always wins; otherwise a
    // bounded service-safe default applies unless the caller opts into unbounded
    // (offline/analytics) use. maxMaterialized* are already bounded downstream.
    final effMaxOutputRows =
        maxOutputRows ?? (unbounded ? null : defaultMaxMaterializedRows);
    final effMemoryBudget =
        memoryBudgetBytes ?? (unbounded ? null : _defaultSharedMemoryBytes);
    final effSpillBudget =
        maxSpillBytes ?? (unbounded ? null : _defaultSpillBudgetBytes);
    // Isolate this engine's spill files in a per-session subdir with a live
    // marker (kept fresh on spill activity), so no other engine's sweep can
    // delete our still-in-use runs — the P0 that silently dropped a partition.
    //
    // The base is a DEDICATED subdir, not the shared temp root: sweepOrphanedSpills
    // lists the base on every engine construction, so pointing it at
    // Directory.systemTemp made every engine enumerate the ENTIRE system temp
    // (thousands of unrelated entries on a busy machine, plus a stat per accrued
    // session) — O(temp size) per engine, which starved concurrent spill I/O and
    // could make a spill query take tens of seconds. Confining sessions to
    // `<temp>/libbigdata-spill/` bounds the sweep to this library's own sessions.
    final spillBase =
        spillDir ?? '${Directory.systemTemp.path}/libbigdata-spill';
    final sessionDir = '$spillBase/$sessionDirPrefix${ids.token()}';
    // Fail FAST if the session dir can't be created: silently proceeding would
    // hand the planner a spillDir that doesn't exist, so the first spill would
    // die deep inside a blocking operator (mid-query) instead of at construction.
    try {
      Directory(sessionDir).createSync(recursive: true);
      File('$sessionDir/.live').writeAsStringSync('');
    } on FileSystemException catch (e) {
      throw StateError(
        'Failed to create spill session directory "$sessionDir" '
        '(set spillDir to a writable location): $e',
      );
    }
    _sessionDir = sessionDir;
    _planner = QueryPlanner(
      _catalog,
      EngineOptions(
        spillThresholdBytes: spillThresholdBytes,
        spillDir: sessionDir,
        maxOutputRows: effMaxOutputRows,
        maxMaterializedRows: maxMaterializedRows,
        maxMaterializedBytes: maxMaterializedBytes,
        // A shared budget bounding combined in-memory use of all blocking
        // operators across this engine's queries.
        // strictMemory enables a HARD ceiling (an oversized-when-empty request is
        // denied → the operator spills, which is safe) instead of the advisory
        // progress-guaranteeing default.
        memory: effMemoryBudget == null
            ? null
            : MemoryManager(effMemoryBudget, hardCap: strictMemory),
        // A shared budget bounding combined on-disk spill across this engine.
        spillBudget: effSpillBudget == null
            ? null
            : SpillBudget(effSpillBudget),
      ),
    );
    // Reclaim spill files orphaned by a prior crashed session (a SIGKILL exits
    // before any `finally`). Best-effort, age-gated, and unawaited so it never
    // blocks construction or a query.
    unawaited(sweepOrphanedSpills(spillBase));
  }

  /// Release this engine's per-session spill directory. Individual spill files
  /// are removed as their operators close; this removes the now-empty session
  /// dir (and any straggler) so a long-lived process creating a per-request
  /// engine doesn't accumulate one dir per request. Idempotent: rejects further
  /// queries, waits for in-flight ones to settle, then deletes the dir — an
  /// already-gone dir is fine, but a permission/other failure now propagates
  /// instead of being silently swallowed.
  ///
  /// Waits at most [timeout] for in-flight operations to drain (a leaked/undrained
  /// `executeStream` subscription would otherwise pin the wait forever); on
  /// expiry it throws a [StateError] naming the still-active count rather than
  /// hanging or busy-polling.
  Future<void> dispose({Duration timeout = const Duration(seconds: 30)}) async {
    _disposed = true; // reject NEW operations immediately
    // Never delete the spill dir under an active query/stream — await the last
    // one draining, bounded by [timeout]. (No await separates the guard from the
    // completer assignment, so no decrement can slip through unsignalled.)
    if (_active > 0) {
      final idle = _idle ??= Completer<void>();
      try {
        await idle.future.timeout(timeout);
      } on TimeoutException {
        throw StateError(
          'SqlEngine.dispose() timed out after $timeout with $_active '
          'operation(s) still active (likely an undrained executeStream); drain '
          'or cancel them before disposing.',
        );
      }
    }
    final dir = Directory(_sessionDir);
    try {
      if (await dir.exists()) await dir.delete(recursive: true);
    } on PathNotFoundException {
      // A concurrent sweep already removed it — nothing to do.
    }
  }

  /// Validate and resolve a file path from a SQL statement. Throws
  /// [ArgumentError] if the path escapes the sandbox / working directory.
  String _resolvePath(String filePath) {
    if (allowExternalPaths) return filePath;
    final normalized = p.normalize(filePath);
    if (sandboxRoot != null) {
      final root = p.normalize(sandboxRoot!);
      final resolved = p.isAbsolute(normalized)
          ? normalized
          : p.normalize(p.join(root, normalized));
      // Lexical gate (cheap): reject obvious `..` escapes.
      if (resolved != root && !p.isWithin(root, resolved)) {
        throw ArgumentError(
          'Access denied: "$filePath" escapes sandbox "$root"',
        );
      }
      // Authoritative gate: resolve symlinks so a link INSIDE the sandbox that
      // points outside it is rejected (the lexical check can't see through it).
      // canonicalize fails CLOSED (throws) on an unresolvable link.
      final canonicalRoot = canonicalize(root);
      final canonical = canonicalize(resolved);
      if (canonical != canonicalRoot && !p.isWithin(canonicalRoot, canonical)) {
        throw ArgumentError(
          'Access denied: "$filePath" resolves through a symlink outside '
          'sandbox "$root"',
        );
      }
      // Return the CANONICAL path we actually validated, so the file opened is
      // the one checked — not the pre-resolution path a symlink swap could
      // redirect between check and open. NOTE: pure Dart has no openat/
      // O_NOFOLLOW, so a residual check-then-open TOCTOU on the resolved path
      // remains; a hardened deployment needs an external file-access broker.
      return canonical;
    }
    if (p.isAbsolute(normalized) || p.split(normalized).contains('..')) {
      throw ArgumentError(
        'Access denied: absolute or parent-traversal path "$filePath" '
        '(set sandboxRoot or allowExternalPaths to permit)',
      );
    }
    return normalized;
  }

  /// Execute SQL query and return DataFrame. Like [execute], this materializes
  /// the whole result; for large scans prefer [executeStream].
  Future<DataFrame> sql(String query) async =>
      DataFrame.fromTable(await execute(query)); // execute() is serialized

  /// Execute a SQL query and return the full result as one [Table].
  ///
  /// This materializes every output batch into a single Table. To avoid an
  /// accidental unbounded load OOMing the process, materialization STOPS and
  /// throws once the result exceeds [maxRows] (default
  /// [defaultMaxMaterializedRows]); pass `maxRows: null` to allow an unbounded
  /// result, or use [executeStream] for bounded-memory batches.
  Future<Table> execute(
    String query, {
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) {
    _checkNotDisposed();
    _active++;
    return _serialized(() async {
      final parsed = _parser.parse(query);
      if (parsed is UpdateStatement ||
          parsed is InsertStatement ||
          parsed is MergeStatement) {
        return _runDml(parsed);
      }
      if (parsed is CreateViewStatement || parsed is DropViewStatement) {
        return _runDdl(parsed);
      }
      try {
        final (ctes, select) = _unwrap(parsed);
        await _materializeCtes(ctes);
        return await _runSelect(select, maxRows: maxRows, maxBytes: maxBytes);
      } finally {
        _cleanup();
      }
    }).whenComplete(_release);
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
    await _preprocessStatement(select);
    return _executePhysicalPlan(
      _planner.plan(select),
      maxRows: maxRows,
      maxBytes: maxBytes,
    );
  }

  /// Materialize each CTE once into a temporary catalog table (in order, so a
  /// later CTE can reference an earlier one). References then resolve as plain
  /// tables, getting alias qualification for free. RECURSIVE is rejected.
  Future<void> _materializeCtes(List<CommonTableExpression> ctes) async {
    for (final cte in ctes) {
      if (cte.recursive) {
        throw UnsupportedError('WITH RECURSIVE is not supported');
      }
      var table = await _runSelect(cte.query);
      if (cte.columns.isNotEmpty) {
        table = _relabel(table, cte.columns);
      }
      final prior = _catalog.table(cte.name);
      if (prior != null) _shadowedCtes[cte.name] = prior;
      _catalog.registerTable(cte.name, table);
      _temporaryCteTables[cte.name] = table;
    }
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
        throw ArgumentError('View not found: ${statement.name}');
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
      // between the CTE's registration and here — removing by name would then
      // silently delete THEIR table (and strand any LazySource registered with
      // it, which only the file-table teardown clears). If the name no longer
      // holds our CTE table, someone else owns it now: leave it, and drop the
      // shadow we would otherwise restore over it.
      if (identical(_catalog.table(entry.key), entry.value)) {
        _catalog.removeTable(entry.key);
      } else {
        _shadowedCtes.remove(entry.key);
      }
    }
    _temporaryCteTables.clear();
    _shadowedCtes.forEach(_catalog.registerTable); // restore shadowed tables
    _shadowedCtes.clear();
  }

  /// Execute a query and stream the result one [Table] batch at a time, without
  /// merging into a single Table. Scan→Filter→Projection→Limit plans stream
  /// end-to-end with bounded memory (Parquet sources read lazily); blocking
  /// operators (aggregate/sort/join/set-op) still buffer their input by nature.
  ///
  /// The engine lock is held only for SETUP (parse/materialize/plan), NOT for the
  /// consumer's drain — so a slow or never-drained stream cannot block other
  /// queries on this engine.
  Stream<Table> executeStream(String query) async* {
    _checkNotDisposed();
    _active++;
    try {
      yield* _executeStream(query);
    } finally {
      _release();
    }
  }

  Stream<Table> _executeStream(String query) async* {
    // Build the plan + iterator UNDER the engine lock (setup mutates the shared
    // catalog/temp state), then RELEASE the lock before streaming. The physical
    // plan binds CONCRETE sources at plan time (PhysicalTableScan.table /
    // PhysicalFileScan.path) and no operator dereferences the catalog during
    // iteration, so `_cleanup()` is safe to run before the stream and a slow or
    // abandoned consumer can no longer hold the lock for the whole drain — which
    // previously deadlocked every later query on this engine.
    final (
      result,
      iterator,
    ) = await _serialized<(Table?, RecordBatchIterator?)>(() async {
      final parsed = _parser.parse(query);
      if (parsed is UpdateStatement ||
          parsed is InsertStatement ||
          parsed is MergeStatement) {
        return (await _runDml(parsed), null);
      }
      if (parsed is CreateViewStatement || parsed is DropViewStatement) {
        return (_runDdl(parsed), null);
      }
      try {
        final (ctes, select) = _unwrap(parsed);
        await _materializeCtes(ctes);
        await _preprocessStatement(select);
        return (null, _planner.plan(select).execute());
      } finally {
        _cleanup();
      }
    });
    if (iterator == null) {
      yield result!; // DML/DDL: a single bounded result, lock already released.
      return;
    }
    try {
      RecordBatch? batch;
      while ((batch = await iterator.next()) != null) {
        yield _display(
          Table(name: 'result', schema: batch!.schema, columns: batch.columns),
        );
      }
    } finally {
      // Runs on completion, break, or subscription cancel — cascades through the
      // operator tree to release spill writers / file descriptors.
      await iterator.close();
    }
  }

  /// Register a table.
  ///
  /// Synchronous, so it cannot join the query queue — it lands wherever the
  /// caller's control flow puts it, possibly between an in-flight query's
  /// awaits. That is safe: the query's CTE teardown removes only tables it
  /// still owns (see `_clearTemporaryCtes`), so a name registered here is never
  /// silently dropped. It CAN still shadow a CTE of the same name for a query
  /// that has not finished binding — register before starting the query, or use
  /// separate engines, if that matters.
  void registerTable(String name, Table table) {
    _catalog.registerTable(name, table);
  }

  /// Register a DataFrame as a table.
  ///
  /// The DataFrame is fully materialized (a registered table is resident so it
  /// can be queried repeatedly); [maxRows]/[maxBytes] fail loud past the cap so a
  /// huge frame doesn't OOM silently — pass null to opt out, or use
  /// [registerFile] to register large data lazily.
  ///
  /// Serialized against in-flight queries: this mutates the shared catalog, so
  /// letting it land mid-query could shadow a CTE the query is still resolving.
  Future<void> registerDataFrame(
    String name,
    DataFrame df, {
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) async {
    final table = await df.execute(maxRows: maxRows, maxBytes: maxBytes);
    return _serialized(() async => _catalog.registerTable(name, table));
  }

  /// Register a file as a table (like DuckDB). Parquet / CSV / JSON-Lines are
  /// registered lazily (schema only + a streaming source) so a large file isn't
  /// materialized at registration — it streams when queried; JSON arrays and
  /// unknown formats fall back to an eager load. The registration persists
  /// across queries (it is not a per-query temporary).
  /// Serialized against in-flight queries: it mutates the shared catalog (a
  /// table AND a LazySource), so landing mid-query could shadow a CTE the query
  /// is still resolving.
  Future<void> registerFile(String tableName, String filePath) async {
    final resolved = _resolvePath(filePath);
    if (!await File(resolved).exists()) {
      throw ArgumentError('File not found: $filePath');
    }
    return _serialized(
      () => _registerLazyOrEager(
        tableName,
        resolved,
        tableName,
        temporary: false,
      ),
    );
  }

  /// Register [resolved] in the catalog under [catalogKey] (display [label]):
  /// for streamable formats a schema-only [Table.empty] + a [LazySource];
  /// otherwise an eagerly-loaded Table. When [temporary] the key joins
  /// `_temporaryFileTables` (torn down by `_cleanup`); persistent registrations
  /// (e.g. [registerFile]) must NOT, or the next query's cleanup would drop them.
  Future<void> _registerLazyOrEager(
    String catalogKey,
    String resolved,
    String label, {
    RowFilter? pushFilter,
    required bool temporary,
  }) async {
    // Single format registry (openScan): the same extension set + unknown-ext
    // error as every other entry point. A cheap schema means the source can be
    // scanned lazily; a null schema (empty / not discoverable) falls back to an
    // eager load. The pushed filter is applied best-effort by the reader and
    // exactly by the plan, so forwarding it for every format is safe.
    final scan = openScan(resolved);
    final schema = await scan.readSchema();
    if (schema != null) {
      _catalog.registerTable(catalogKey, Table.empty(label, schema));
      _catalog.registerSource(
        catalogKey,
        LazySource(resolved, schema, filter: pushFilter),
      );
    } else {
      _catalog.registerTable(catalogKey, await scan.read(filter: pushFilter));
    }
    if (temporary) _temporaryFileTables.add(catalogKey);
  }

  /// Execute a DML statement (UPDATE / MERGE) against a Delta table; returns a
  /// one-row result with the committed `version` (-1 if nothing changed).
  Future<Table> _runDml(Statement statement) async {
    if (statement is UpdateStatement) {
      final path = _resolvePath(statement.targetPath);
      final schema = DeltaSchema.fromJson(
        (await DeltaLog(path).snapshot()).metadata.schemaString,
      );
      final where = statement.where;
      final wherePhys = where == null
          ? null
          : _planner.compileScalar(where, inputSchema: schema);
      // Build the 1-row batch ONCE per row and share it across the WHERE
      // predicate and every SET assignment: a wide table with several SET
      // columns otherwise rebuilds the whole-schema batch (1 + N) times per row.
      // The rewrite hands all N setters the SAME row instance consecutively, so
      // a one-entry identity memo collapses that to one build per row.
      Map<String, dynamic>? lastRow;
      RecordBatch? lastBatch;
      RecordBatch batchFor(Map<String, dynamic> row) {
        if (!identical(row, lastRow)) {
          lastRow = row;
          lastBatch = _rowBatch(row, schema);
        }
        return lastBatch!;
      }

      Future<bool> predicate(Map<String, dynamic> row) async {
        if (wherePhys == null) return true;
        final result = await _evalDml(wherePhys, batchFor(row));
        // Collapse SQL three-valued logic at the predicate boundary: only a
        // definite TRUE matches; NULL (unknown) and FALSE do not. A non-boolean
        // WHERE is a type error, not a silent no-match.
        if (result == null) return false;
        if (result is bool) return result;
        throw FormatException('Non-boolean WHERE in UPDATE: $result');
      }

      final assignments = {
        for (final a in statement.assignments)
          a.column: _setter(a.value, a.column, schema, batchFor),
      };
      final version = await DeltaWriter(path).update(predicate, assignments);
      return _versionResult(version);
    }
    if (statement is InsertStatement) {
      return _runInsert(statement);
    }
    if (statement is MergeStatement) {
      final Table source;
      if (statement.sourcePath != null) {
        source = await _loadFileAsTable(_resolvePath(statement.sourcePath!));
      } else {
        try {
          await _preprocessStatement(statement.sourceQuery!);
          // Cap the MERGE source like any other materialized result: it is built
          // into a full in-memory hash by the Delta writer, so an unbounded feed
          // would OOM. Stops at the cap (fail-loud); pass null caps via
          // EngineOptions to opt out. Streaming/spilling merge is future work.
          source = await _executePhysicalPlan(
            _planner.plan(statement.sourceQuery!),
            maxRows:
                _planner.options.maxMaterializedRows ??
                defaultMaxMaterializedRows,
            maxBytes:
                _planner.options.maxMaterializedBytes ??
                defaultMaxMaterializedBytes,
          );
        } finally {
          _cleanup();
        }
      }
      final matchedUpdate = statement.matchedUpdateAll
          ? {
              for (final f in source.schema.fields)
                f.name: (Map<String, dynamic> t, Map<String, dynamic> s) =>
                    s[f.name],
            }
          : null;
      final version = await DeltaWriter(_resolvePath(statement.targetPath))
          .merge(
            source,
            on: statement.onKeys,
            whenMatchedUpdate: matchedUpdate,
            whenMatchedDelete: statement.matchedDelete,
            whenNotMatchedInsert: statement.notMatchedInsert,
          );
      return _versionResult(version);
    }
    throw UnsupportedError('Unsupported DML statement: $statement');
  }

  /// Appends literal rows to an existing Delta table.
  ///
  /// The row schema is taken WHOLESALE from the table's committed metadata and
  /// never inferred: a Delta append compares the serialized schema by string
  /// equality, so field order, exact type and the `nullable` flag must all
  /// match. Inferring from the literals would differ on every count.
  Future<Table> _runInsert(InsertStatement statement) async {
    final path = _resolvePath(statement.targetPath);
    final log = DeltaLog(path);
    // `DeltaWriter.write` CREATES a table when the location is empty, so a
    // typo'd path would silently produce a stray table instead of failing.
    if ((await log.versions()).isEmpty) {
      throw StateError(
        'INSERT target is not a Delta table: $path '
        '(INSERT appends to an existing table; it does not create one)',
      );
    }
    final snapshot = await log.snapshot();
    final schema = DeltaSchema.fromJson(snapshot.metadata.schemaString);
    final fields = schema.fields;

    // No column list means the rows are positional over the full schema.
    final columns = statement.columns.isEmpty
        ? [for (final f in fields) f.name]
        : statement.columns;
    for (final column in columns) {
      if (schema.field(column) == null) {
        throw ArgumentError(
          'INSERT names column "$column", which is not a column of $path '
          '(${fields.map((f) => f.name).join(', ')})',
        );
      }
    }
    final duplicates = columns.toSet();
    if (duplicates.length != columns.length) {
      throw ArgumentError('INSERT names a column more than once: $columns');
    }

    // Constant value expressions evaluate against an empty 1-row batch through
    // the shared SELECT evaluator; a column reference fails loud (no columns to
    // resolve), matching INSERT VALUES' constant-only contract.
    final emptyBatch = RecordBatch(
      columns: const [],
      schema: schema,
      rowCount: 1,
    );
    final rows = <Map<String, dynamic>>[];
    for (final row in statement.values) {
      if (row.length != columns.length) {
        throw ArgumentError(
          'INSERT supplies ${row.length} value(s) for ${columns.length} '
          'column(s)',
        );
      }
      final values = <String, dynamic>{};
      for (int i = 0; i < row.length; i++) {
        values[columns[i]] = _coerce(
          await _evalDml(
            _planner.compileScalar(row[i], inputSchema: schema),
            emptyBatch,
          ),
          schema.field(columns[i])!,
        );
      }
      // Columns omitted by the column list default to NULL, which a
      // non-nullable field rejects downstream.
      for (final field in fields) {
        values.putIfAbsent(field.name, () => null);
      }
      rows.add(values);
    }

    final version = await DeltaWriter(path).write(
      Table.fromRows('insert', rows, schema: schema),
      mode: DeltaWriteMode.append,
      // A partitioned table rejects an append that doesn't repeat its layout.
      partitionBy: snapshot.metadata.partitionColumns.isEmpty
          ? null
          : snapshot.metadata.partitionColumns,
    );
    return _versionResult(version);
  }

  static final _integers = {
    DataType.int8,
    DataType.int16,
    DataType.int32,
    DataType.int64,
  };
  // NB `decimal` is deliberately absent: it reports `dartType: double` but is
  // stored as an unscaled BigInt, so widening a literal into it by dartType
  // would put a double in a BigInt column.
  static final _floats = {DataType.float32, DataType.float64};

  /// Widens a SQL literal to the target field's exact storage type. Column
  /// validation demands exact dtype equality, so `1` into a float64 column has
  /// to become `1.0` here.
  ///
  /// The accepted widenings are whitelisted rather than delegated to
  /// [DataType.commonType]: that falls back to `string` for unrelated types, so
  /// it would accept `9` into a string column — and column validation checks a
  /// column's declared dtype, not each value, so the int would be stored inside
  /// a string column and only surface much later. Anything not whitelisted is
  /// rejected NAMING the column, rather than surfacing as a bare
  /// `String is not a subtype of int?` from deep inside column building.
  Object? _coerce(Object? value, Field field) {
    if (value == null) return null;
    final type = field.dataType;
    final valueType = DataType.fromValue(value);
    if (valueType == type) return value;
    if (value is int) {
      // `fromValue` narrows an int by magnitude, so a common type equal to the
      // field's means the literal fits it — 999 into an int8 column does not.
      if (_integers.contains(type) &&
          DataType.commonType(valueType, type) == type) {
        return value;
      }
      if (_floats.contains(type)) return value.toDouble();
      // An integer is EXACT in a decimal column: lift to a scale-0 decimal and
      // let column building rescale to the field's scale. A double into a decimal
      // column is NOT accepted (it would round) and falls through to the throw.
      if (type == DataType.decimal) return Decimal(BigInt.from(value), 0);
    }
    if (value is double && _floats.contains(type)) return value;
    throw ArgumentError(
      'INSERT value $value ($valueType) cannot be stored in column '
      '"${field.name}" of type $type',
    );
  }

  Table _versionResult(int version) => Table(
    name: 'result',
    columns: [
      NumericColumn.int64(name: 'version', values: [version], nullable: false),
    ],
    schema: Schema(
      fields: [
        Field(name: 'version', dataType: DataType.int64, nullable: false),
      ],
    ),
  );

  /// A one-row [RecordBatch] built from a DML [row] map against [schema], so a
  /// DML SET/WHERE expression evaluates through the SAME physical evaluator the
  /// SELECT path uses (Kleene three-valued logic, checked integer arithmetic,
  /// divide-by-zero→NULL) rather than a separate row evaluator that drifts. The
  /// schema dtype is authoritative for a null cell; strict construction rejects
  /// a stray key so a typo can't vanish.
  RecordBatch _rowBatch(Map<String, dynamic> row, Schema schema) => RecordBatch(
    columns: columnsFromRows([row], schema, strict: true),
    schema: schema,
    rowCount: 1,
  );

  /// Evaluate a compiled DML scalar on a one-row [batch], presenting the
  /// evaluator's low-level failures (an unknown column, a type-mismatched
  /// operand) as a domain [FormatException] rather than leaking a raw
  /// `StateError`/`TypeError`/`RangeError` — DML expressions are user-supplied
  /// and must fail closed with a clear message.
  Future<Object?> _evalDml(PhysicalExpression phys, RecordBatch batch) async {
    try {
      return phys.evaluate(batch, 0);
    } on FormatException {
      rethrow;
    } on StateError catch (e) {
      throw FormatException('Invalid DML expression: ${e.message}');
    } on TypeError catch (e) {
      throw FormatException('DML type error: $e');
    } on RangeError catch (e) {
      throw FormatException('Invalid DML expression: $e');
    }
  }

  /// Build the per-row SET-value closure for [column]: compile [value] once,
  /// then evaluate it per row on a one-row batch. NULL is preserved as NULL
  /// (SQL). A decimal column receives an unscaled [BigInt] at the expression's
  /// scale, re-wrapped as a [Decimal] so the write path rescales it to the
  /// column's declared scale. A float result (decimal division — always float —
  /// or a float-typed operand) has no exact fixed-scale form, so it fails loud
  /// rather than silently rounding into a decimal column.
  Future<Object?> Function(Map<String, dynamic>) _setter(
    Expression value,
    String column,
    Schema schema,
    RecordBatch Function(Map<String, dynamic>) batchFor,
  ) {
    final phys = _planner.compileScalar(value, inputSchema: schema);
    final field = schema.field(column);
    return (Map<String, dynamic> row) async {
      final result = await _evalDml(phys, batchFor(row));
      if (field?.dataType == DataType.decimal && result != null) {
        if (result is Decimal) return result;
        if (result is BigInt) return Decimal(result, phys.scale ?? 0);
        // An integer is EXACT — lift it to a scale-0 decimal (the write path
        // rescales to the column's declared scale), so `SET price = 20` stores
        // 20.00. Only a double (decimal division, a float operand) is genuinely
        // lossy at a fixed scale and fails loud.
        if (result is int) return Decimal(BigInt.from(result), 0);
        throw FormatException(
          'Cannot store non-exact value $result (${result.runtimeType}) into '
          'decimal column "$column": decimal division and float operands are '
          'not exactly representable at a fixed scale — CAST explicitly',
        );
      }
      return result;
    };
  }

  /// Load a file as a table. [filter] (a Parquet row-group pruning predicate
  /// derived from the query WHERE) is applied only to Parquet files.
  Future<Table> _loadFileAsTable(String filePath, {RowFilter? filter}) async {
    try {
      return await openScan(filePath).read(filter: filter);
    } on ArgumentError {
      rethrow; // unknown extension / bad path — surface as-is.
    } on FileSystemException {
      rethrow; // retryable I/O (missing file, permissions) — keep the type.
    } on FormatException {
      rethrow; // malformed data / unsupported codec — keep the type.
    } catch (e, stack) {
      // Wrap anything else, but preserve the ORIGINAL stack so the failure's
      // origin isn't lost (a bare `throw` here would reset it).
      Error.throwWithStackTrace(
        StateError(
          'Failed to read ${filePath.split('.').last.toLowerCase()} file: $e',
        ),
        stack,
      );
    }
  }

  /// List registered tables
  List<String> listTables() {
    return _catalog.tableNames;
  }

  /// Get all tables (for CLI)
  Map<String, Table> getTables() {
    return _catalog.tables;
  }

  /// Get table schema
  Schema? getTableSchema(String tableName) {
    final table = _catalog.table(tableName);
    return table?.schema;
  }

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
      // running row count OR byte size would exceed the cap — so an unbounded
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
  /// count as the schema — a mismatch is a planning bug (e.g. an unbound set
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

  /// Pre-process statement to handle file paths
  Future<void> _preprocessStatement(dynamic statement) async {
    // We need to walk the AST and find SimpleTable nodes with isFilePath = true
    // For now, let's use a visitor pattern
    final filePathFinder = _FilePathFinder();
    statement.accept(filePathFinder);

    // Predicate pushdown: for a single file-backed table with a WHERE clause,
    // derive a row-group pruning RowFilter from the predicate. The plan still
    // applies the exact filter, so this only shrinks the scan.
    final pushFilter = _pushdownFilter(statement, filePathFinder.filePaths);

    // Load each unique file path, keyed by the raw path string (so two files
    // sharing a basename coexist). Parquet and CSV/JSON-Lines are read lazily (a
    // streaming scan); JSON arrays (and anything else) are materialized eagerly.
    for (final filePath in filePathFinder.filePaths) {
      if (_catalog.hasTable(filePath)) continue; // already loaded this path
      final resolved = _resolvePath(filePath);
      final label = tableNameFromPath(filePath); // basename, display only
      // Per-query temporary: keyed by the raw path, torn down by _cleanup.
      await _registerLazyOrEager(
        filePath,
        resolved,
        label,
        pushFilter: pushFilter,
        temporary: true,
      );
    }

    // Materialize any referenced views (like CTEs). A registered table or CTE
    // temp shadows a view of the same name.
    for (final name in filePathFinder.tableNames) {
      if (_catalog.hasView(name) && !_catalog.hasTable(name)) {
        await _materializeView(name);
      }
    }
  }

  /// Materialize a view's body into a temporary catalog table (recursively, so a
  /// view referencing another view works), guarding against reference cycles.
  Future<void> _materializeView(String name) async {
    if (!_materializingViews.add(name)) {
      throw StateError('Recursive view reference: $name');
    }
    try {
      final def = _catalog.view(name)!;
      var table = await _runSelect(def.query);
      if (def.columns.isNotEmpty) {
        table = _relabel(table, def.columns);
      }
      final prior = _catalog.table(name);
      if (prior != null) _shadowedCtes[name] = prior;
      _catalog.registerTable(name, table);
      _temporaryCteTables[name] = table;
    } finally {
      _materializingViews.remove(name);
    }
  }

  /// Derive a Parquet pruning [RowFilter] from the statement's WHERE, but only
  /// for the unambiguous single-file case (one file table, no joins) so the
  /// predicate provably references that file's columns.
  RowFilter? _pushdownFilter(dynamic statement, Set<String> filePaths) {
    if (filePaths.length != 1) return null;
    final select = statement is QueryStatement ? statement.select : statement;
    if (select is! SelectStatement) return null;
    if (select.where == null || select.from.length != 1) return null;
    final from = select.from.first;
    if (from is! SimpleTable || !from.isFilePath) return null;
    return whereToRowFilter(select.where!);
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

/// Helper class to find file paths in AST
class _FilePathFinder extends ASTVisitor<void> {
  final Set<String> filePaths = {};
  // Non-file table references (candidate view/CTE/table names).
  final Set<String> tableNames = {};

  @override
  void visitSimpleTable(SimpleTable node) {
    if (node.isFilePath) {
      filePaths.add(node.table);
    } else {
      tableNames.add(node.table);
    }
  }

  // Default implementations for other visit methods
  @override
  void visitQueryStatement(Statement statement) {
    if (statement is QueryStatement) {
      statement.select.accept(this);
    }
  }

  @override
  void visitSelectStatement(Statement statement) {
    if (statement is SelectStatement) {
      for (final table in statement.from) {
        table.accept(this);
      }
      // A `col IN (SELECT …)` in WHERE/HAVING becomes a semi/anti join whose
      // subquery is part of the SAME plan, so its files/views must register too.
      statement.where?.accept(this);
      statement.having?.accept(this);
    }
  }

  @override
  void visitInsertStatement(Statement statement) {}
  void visitUpdateStatement(Statement statement) {}
  void visitDeleteStatement(Statement statement) {}
  @override
  void visitCreateTableStatement(Statement statement) {}
  void visitDropTableStatement(Statement statement) {}
  void visitColumnReference(Expression expression) {}
  @override
  void visitLiteralExpression(Expression expression) {}
  // Recurse through predicate containers so a nested `col IN (SELECT …)` is
  // reached (AND/OR chains, a NOT).
  @override
  void visitBinaryExpression(Expression expression) {
    if (expression is BinaryExpression) {
      expression.left.accept(this);
      expression.right.accept(this);
    }
  }

  @override
  void visitUnaryExpression(Expression expression) {
    if (expression is UnaryExpression) expression.operand.accept(this);
  }

  void visitFunctionCall(Expression expression) {}
  void visitAggregateFunction(Expression expression) {}
  @override
  void visitCaseExpression(Expression expression) {}
  // Descend into an IN subquery so its own files/views register (it becomes part
  // of the outer plan as a semi/anti join).
  @override
  void visitInExpression(Expression expression) {
    if (expression is InExpression) expression.subquery?.accept(this);
  }

  @override
  void visitBetweenExpression(Expression expression) {}
  void visitLikeExpression(Expression expression) {}
  void visitIsNullExpression(Expression expression) {}
  @override
  void visitExistsExpression(Expression expression) {
    if (expression is ExistsExpression) expression.subquery.accept(this);
  }

  @override
  void visitSubqueryExpression(Expression expression) {}
  void visitStarExpression(SelectItem item) {}
  void visitQualifiedStarExpression(SelectItem item) {}
  void visitExpressionSelectItem(SelectItem item) {}
  @override
  void visitTableReference(TableReference table) {}
  @override
  void visitJoinedTable(TableReference table) {
    if (table is JoinedTable) {
      table.left.accept(this);
      for (final j in table.joins) {
        j.right.accept(this);
      }
    }
  }

  @override
  void visitSubqueryTable(TableReference table) {
    if (table is SubqueryTable) table.subquery.accept(this);
  }

  @override
  void visitTableFunction(TableReference table) {}
  @override
  void visitValuesTable(TableReference table) {}
  @override
  void visitCastExpression(Expression expression) {}
  @override
  void visitWindowExpression(Expression expression) {}
  @override
  void visitColumnExpression(Expression expression) {}
  @override
  void visitExpression(Expression expression) {}
  @override
  void visitFunctionExpression(Expression expression) {}
}

/// SQL extension for DataFrame
extension SqlDataFrame on DataFrame {
  /// Create a SQL engine. NOTE: the caller OWNS the returned engine and must
  /// `await engine.dispose()` when done, or it leaks a per-session spill dir.
  static SqlEngine createEngine() => SqlEngine();

  /// Execute SQL query with this DataFrame as a table.
  Future<DataFrame> sql(String query, {String tableName = 'df'}) async {
    final engine = SqlEngine();
    try {
      await engine.registerDataFrame(tableName, this);
      return await engine.sql(query);
    } finally {
      // The result is fully materialized before return, so disposing here is safe
      // and stops this per-call engine from leaking a session dir every call.
      await engine.dispose();
    }
  }
}

/// A shared convenience [SqlEngine]. Queries on it are serialized (safe to call
/// concurrently), but it has ONE shared catalog — registered tables are visible
/// to every caller. It is given a 30-second [SqlEngine.lockTimeout] so a single
/// abandoned/never-drained `executeStream` can't stall every later query on it
/// forever (it self-heals once the predecessor settles). For a service, prefer a
/// per-request `SqlEngine()` (isolated catalog + independent lifecycle) rather
/// than this singleton.
final sql = SqlEngine(lockTimeout: const Duration(seconds: 30));
