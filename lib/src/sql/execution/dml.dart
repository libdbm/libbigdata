import 'dart:async';

import '../../core/column.dart';
import '../../core/columnar.dart' show columnsFromRows;
import '../../core/data_type.dart';
import '../../core/decimal.dart';
import '../../core/failure.dart';
import '../../core/limits.dart';
import '../../core/result.dart';
import '../../core/schema.dart';
import '../../core/table.dart';
import '../../formats/delta/delta_log.dart' show DeltaLog;
import '../../formats/delta/delta_schema.dart' show DeltaSchema;
import '../../formats/delta/delta_writer.dart';
import '../ast/ast.dart';
import '../planner/physical_plan.dart' show PhysicalExpression;
import '../planner/query_planner.dart';
import 'exec_core.dart';

/// The row-level writes of one table format at one location, as SQL DML uses
/// them. Each operation commits atomically and returns the committed version
/// (-1 when nothing changed), or an [Err] with the format writer's kind.
///
/// Rows are `column -> value` maps, and predicates and assignments are
/// callbacks over them, so an implementation needs no knowledge of the SQL
/// AST: [DML] compiles the statement's expressions into these callbacks.
abstract interface class Mutations {
  /// The implementation for the table at [path]. Every location is Delta
  /// today, including a location that holds no table yet.
  static Mutations of(String path) => DeltaMutations(path);

  /// The format's name, as messages name it.
  String get format;

  /// Whether [format] has a table at this location. Throws [Kind.missing]
  /// when the location is not one this format recognises at all.
  Future<bool> exists();

  /// The schema of the table's current version. Throws [Kind.missing] when
  /// there is no table.
  Future<Schema> schema();

  /// Append [rows], which carry the table's own [schema].
  Future<Result<int, ErrorDetails>> insert(Table rows);

  /// Set each column of [assignments] to its callback's value in every row
  /// [predicate] matches.
  Future<Result<int, ErrorDetails>> update(
    FutureOr<bool> Function(Map<String, dynamic> row) predicate,
    Map<String, FutureOr<Object?> Function(Map<String, dynamic> row)>
    assignments,
  );

  /// Remove every row [predicate] matches.
  Future<Result<int, ErrorDetails>> delete(
    bool Function(Map<String, dynamic> row) predicate,
  );

  /// Merge [source] on the equi-key columns [on], which name target columns:
  /// a matched target row takes [update]'s values or, when [delete], is
  /// removed; an unmatched source row is inserted when [insert].
  Future<Result<int, ErrorDetails>> merge(
    Table source, {
    required List<String> on,
    Map<
      String,
      Object? Function(Map<String, dynamic> target, Map<String, dynamic> src)
    >?
    update,
    bool delete = false,
    bool insert = true,
  });
}

/// [Mutations] for a Delta table, over [DeltaWriter] with its defaults.
///
/// One instance serves one statement: its [DeltaLog] caches the version list,
/// so [exists], [schema] and [insert] all see the same version.
final class DeltaMutations implements Mutations {
  DeltaMutations(this.path) : _log = DeltaLog(path);

  final String path;
  final DeltaLog _log;

  @override
  String get format => 'Delta';

  @override
  Future<bool> exists() async => (await _log.versions()).isNotEmpty;

  @override
  Future<Schema> schema() async =>
      DeltaSchema.fromJson((await _log.snapshot()).metadata.schemaString);

  @override
  Future<Result<int, ErrorDetails>> insert(Table rows) => guard(() async {
    final snapshot = await _log.snapshot();
    return DeltaWriter(path).write(
      rows,
      mode: DeltaWriteMode.append,
      // A partitioned table rejects an append that doesn't repeat its layout.
      partitionBy: snapshot.metadata.partitionColumns.isEmpty
          ? null
          : snapshot.metadata.partitionColumns,
    );
  });

  @override
  Future<Result<int, ErrorDetails>> update(
    FutureOr<bool> Function(Map<String, dynamic> row) predicate,
    Map<String, FutureOr<Object?> Function(Map<String, dynamic> row)>
    assignments,
  ) => DeltaWriter(path).update(predicate, assignments);

  @override
  Future<Result<int, ErrorDetails>> delete(
    bool Function(Map<String, dynamic> row) predicate,
  ) => DeltaWriter(path).delete(predicate);

  @override
  Future<Result<int, ErrorDetails>> merge(
    Table source, {
    required List<String> on,
    Map<
      String,
      Object? Function(Map<String, dynamic> target, Map<String, dynamic> src)
    >?
    update,
    bool delete = false,
    bool insert = true,
  }) => DeltaWriter(path).merge(
    source,
    on: on,
    whenMatchedUpdate: update,
    whenMatchedDelete: delete,
    whenNotMatchedInsert: insert,
  );
}

/// Executes SQL DML (UPDATE, INSERT, MERGE) against the [Mutations] of its
/// target, compiling SET/WHERE and VALUES expressions with [planner].
///
/// The engine supplies its path confinement as [resolve], its file reader as
/// [load] (given a resolved path), and [select], which runs a MERGE source
/// query to a table under the given caps and releases that query's
/// temporaries.
final class DML {
  DML({
    required this.planner,
    required this.resolve,
    required this.load,
    required this.select,
  });

  final QueryPlanner planner;
  final String Function(String path) resolve;
  final Future<Table> Function(String path) load;
  final Future<Table> Function(
    SelectStatement query, {
    required int maxRows,
    required int maxBytes,
  })
  select;

  /// Execute a DML statement (UPDATE / INSERT / MERGE); returns a one-row
  /// result with the committed `version` (-1 if nothing changed), or the
  /// writer's [Err]. Other failures throw, for the caller's boundary to report.
  Future<Result<Table, ErrorDetails>> run(Statement statement) async {
    if (statement is UpdateStatement) {
      final mutations = Mutations.of(resolve(statement.targetPath));
      final schema = await mutations.schema();
      final where = statement.where;
      final condition = where == null
          ? null
          : planner.compileScalar(where, inputSchema: schema);
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
        if (condition == null) return true;
        final result = await _evaluate(condition, batchFor(row));
        // Collapse SQL three-valued logic at the predicate boundary: only a
        // definite TRUE matches; NULL (unknown) and FALSE do not. A non-boolean
        // WHERE is a type error, not a silent no-match.
        if (result == null) return false;
        if (result is bool) return result;
        throw Failure.of(Kind.invalid, 'Non-boolean WHERE in UPDATE: $result');
      }

      final assignments = {
        for (final a in statement.assignments)
          a.column: _setter(a.value, a.column, schema, batchFor),
      };
      final version = await mutations.update(predicate, assignments);
      return _versionResult(version);
    }
    if (statement is InsertStatement) {
      return _insert(statement);
    }
    if (statement is MergeStatement) {
      Table source;
      if (statement.sourcePath != null) {
        source = await load(resolve(statement.sourcePath!));
      } else {
        // Cap the MERGE source like any other materialized result: it is built
        // into a full in-memory hash by the Delta writer, so an unbounded feed
        // would OOM. Stops at the cap (fail-loud); pass null caps via
        // EngineOptions to opt out. Streaming/spilling merge is future work.
        source = await select(
          statement.sourceQuery!,
          maxRows:
              planner.options.maxMaterializedRows ?? defaultMaxMaterializedRows,
          maxBytes:
              planner.options.maxMaterializedBytes ??
              defaultMaxMaterializedBytes,
        );
      }
      // `ON t.id = s.key`: the source's key takes the target's name, which
      // the Delta merge matches on (it merged on the target name alone).
      final renames = {
        for (var i = 0; i < statement.onKeys.length; i++)
          if (statement.sourceKeys[i] != statement.onKeys[i])
            statement.sourceKeys[i]: statement.onKeys[i],
      };
      for (final MapEntry(key: from, value: to) in renames.entries) {
        if (source.schema.field(from) == null) {
          throw Failure.of(Kind.invalid, 'MERGE source has no column $from');
        }
        if (source.schema.field(to) != null && !renames.containsKey(to)) {
          throw Failure.of(
            Kind.invalid,
            'MERGE source key $from cannot take the target name $to: the '
            'source already has a column $to',
          );
        }
      }
      if (renames.isNotEmpty) source = source.renameColumns(renames);
      final matchedUpdate = statement.matchedUpdateAll
          ? {
              for (final f in source.schema.fields)
                f.name: (Map<String, dynamic> t, Map<String, dynamic> s) =>
                    s[f.name],
            }
          : null;
      final version = await Mutations.of(resolve(statement.targetPath)).merge(
        source,
        on: statement.onKeys,
        update: matchedUpdate,
        delete: statement.matchedDelete,
        insert: statement.notMatchedInsert,
      );
      return _versionResult(version);
    }
    throw UnsupportedError('Unsupported DML statement: $statement');
  }

  /// Appends literal rows to an existing table.
  ///
  /// The row schema is taken WHOLESALE from the table's committed metadata and
  /// never inferred: a Delta append compares the serialized schema by string
  /// equality, so field order, exact type and the `nullable` flag must all
  /// match. Inferring from the literals would differ on every count.
  Future<Result<Table, ErrorDetails>> _insert(InsertStatement statement) async {
    final path = resolve(statement.targetPath);
    final mutations = Mutations.of(path);
    // A Delta write CREATES a table when the location is empty, so a typo'd
    // path would silently produce a stray table instead of failing.
    if (!await mutations.exists()) {
      throw Failure.of(
        Kind.missing,
        'INSERT target is not a ${mutations.format} table: $path '
        '(INSERT appends to an existing table; it does not create one)',
      );
    }
    final schema = await mutations.schema();
    final fields = schema.fields;

    // No column list means the rows are positional over the full schema.
    final columns = statement.columns.isEmpty
        ? [for (final f in fields) f.name]
        : statement.columns;
    for (final column in columns) {
      if (schema.field(column) == null) {
        throw Failure.of(
          Kind.schema,
          'INSERT names column "$column", which is not a column of $path '
          '(${fields.map((f) => f.name).join(', ')})',
        );
      }
    }
    final duplicates = columns.toSet();
    if (duplicates.length != columns.length) {
      throw Failure.of(
        Kind.schema,
        'INSERT names a column more than once: $columns',
      );
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
        throw Failure.of(
          Kind.schema,
          'INSERT supplies ${row.length} value(s) for ${columns.length} '
          'column(s)',
        );
      }
      final values = <String, dynamic>{};
      for (int i = 0; i < row.length; i++) {
        values[columns[i]] = _coerce(
          await _evaluate(
            planner.compileScalar(row[i], inputSchema: schema),
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

    // Building the rows against the table's schema is where a NULL meets a
    // non-nullable column; that is the table's schema refusing the row.
    final Table table;
    try {
      table = Table.fromRows('insert', rows, schema: schema);
    } on ArgumentError catch (e) {
      throw Failure.of(Kind.schema, 'INSERT into $path: ${e.message}', e);
    }
    final version = await mutations.insert(table);
    return _versionResult(version);
  }

  // NB `decimal` is not a float, and `isInteger` excludes it too: it is stored
  // as an unscaled BigInt whose scale lives on the field, so a literal cannot
  // be widened into it without that scale.
  static final _floats = {DataType.float32, DataType.float64};

  /// Widens a SQL literal to the target field's exact storage type. Column
  /// validation demands exact dtype equality, so `1` into a float64 column has
  /// to become `1.0` here.
  ///
  /// The accepted widenings are whitelisted rather than delegated to
  /// [DataType.commonType]: that falls back to `string` for unrelated types, so
  /// it would accept `9` into a string column, and column validation checks a
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
      // field's means the literal fits it: 999 into an int8 column does not.
      if (type.isInteger && DataType.commonType(valueType, type) == type) {
        return value;
      }
      if (_floats.contains(type)) return value.toDouble();
      // An integer is EXACT in a decimal column: lift to a scale-0 decimal and
      // let column building rescale to the field's scale. A double into a decimal
      // column is NOT accepted (it would round) and falls through to the throw.
      if (type == DataType.decimal) return Decimal(BigInt.from(value), 0);
    }
    if (value is double && _floats.contains(type)) return value;
    throw Failure.of(
      Kind.schema,
      'INSERT value $value ($valueType) cannot be stored in column '
      '"${field.name}" of type $type',
    );
  }

  /// The one-row `version` result of a DML [outcome], or its [Err].
  Result<Table, ErrorDetails> _versionResult(
    Result<int, ErrorDetails> outcome,
  ) => switch (outcome) {
    Ok(:final value) => Ok(
      Table(
        name: 'result',
        columns: [
          NumericColumn.int64(
            name: 'version',
            values: [value],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'version', dataType: DataType.int64, nullable: false),
          ],
        ),
      ),
    ),
    Err(:final error) => Err(error),
  };

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
  /// operand) as a [Kind.invalid] failure rather than leaking a raw
  /// `StateError`/`TypeError`/`RangeError`: DML expressions are user-supplied
  /// and must fail closed with a clear message. It runs inside the Delta
  /// writer's callbacks, whose boundary would report a bare [FormatException]
  /// as [Kind.corrupt].
  Future<Object?> _evaluate(
    PhysicalExpression compiled,
    RecordBatch batch,
  ) async {
    try {
      return compiled.evaluate(batch, 0);
    } on Failure {
      rethrow;
    } on FormatException catch (e) {
      throw Failure.of(Kind.invalid, 'Invalid DML expression: ${e.message}', e);
    } on StateError catch (e) {
      throw Failure.of(Kind.invalid, 'Invalid DML expression: ${e.message}', e);
    } on TypeError catch (e) {
      throw Failure.of(Kind.invalid, 'DML type error: $e', e);
    } on RangeError catch (e) {
      throw Failure.of(Kind.invalid, 'Invalid DML expression: $e', e);
    }
  }

  /// Build the per-row SET-value closure for [column]: compile [value] once,
  /// then evaluate it per row on a one-row batch. NULL is preserved as NULL
  /// (SQL). A decimal column receives an unscaled [BigInt] at the expression's
  /// scale, re-wrapped as a [Decimal] so the write path rescales it to the
  /// column's declared scale. A float result (decimal division: always float,
  /// or a float-typed operand) has no exact fixed-scale form, so it fails loud
  /// rather than silently rounding into a decimal column.
  /// The SQL type name of each integer column type.
  static final _integers = {
    DataType.int8: 'TINYINT',
    DataType.int16: 'SMALLINT',
    DataType.int32: 'INTEGER',
    DataType.int64: 'BIGINT',
  };

  Future<Object?> Function(Map<String, dynamic>) _setter(
    Expression value,
    String column,
    Schema schema,
    RecordBatch Function(Map<String, dynamic>) batchFor,
  ) {
    final field = schema.field(column);
    // An integer column takes the value as CAST gives it: a decimal or double
    // result is rounded half away from zero, and one out of range fails.
    final type = _integers[field?.dataType];
    final compiled = planner.compileScalar(
      type == null
          ? value
          : CastExpression(expression: value, targetType: type),
      inputSchema: schema,
    );
    return (Map<String, dynamic> row) async {
      final result = await _evaluate(compiled, batchFor(row));
      if (field?.dataType == DataType.decimal && result != null) {
        if (result is Decimal) return result;
        if (result is BigInt) return Decimal(result, compiled.scale ?? 0);
        // An integer is EXACT: lift it to a scale-0 decimal (the write path
        // rescales to the column's declared scale), so `SET price = 20` stores
        // 20.00. Only a double (decimal division, a float operand) is genuinely
        // lossy at a fixed scale and fails loud.
        if (result is int) return Decimal(BigInt.from(result), 0);
        throw Failure.of(
          Kind.invalid,
          'Cannot store non-exact value $result (${result.runtimeType}) into '
          'decimal column "$column": decimal division and float operands are '
          'not exactly representable at a fixed scale: CAST explicitly',
        );
      }
      return result;
    };
  }
}
