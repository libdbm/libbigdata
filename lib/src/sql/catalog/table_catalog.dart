import '../../core/scan.dart';
import '../../core/schema.dart';
import '../../core/table.dart';
import '../ast/statements.dart';
import '../planner/logical_plan.dart' show LogicalPlan;

/// A file-backed table read lazily (streamed by the scan operator) instead of
/// materialized up front. Shared by every scan of the file, so it carries no
/// filter; a pushed filter belongs to one scan node.
class LazySource {
  final String path;
  final Schema schema;

  /// Opens the scan of [path] each time it is read, carrying the options its
  /// reader was called with; null opens `openScan(path)`.
  final Scan Function()? open;

  const LazySource(this.path, this.schema, {this.open});
}

/// The catalog key of the file table read from [path]. A path matches
/// exactly (on a case-sensitive file system `A.csv` and `a.csv` are two
/// files), where a table name matches in any case; the leading NUL cannot
/// begin a table name, so the two never meet.
String fileKey(String path) => '\u0000$path';

/// Catalog for managing tables
class TableCatalog {
  /// [name] as a key: a [fileKey] as is, a table name in lower case.
  static String _key(String name) =>
      name.startsWith('\u0000') ? name : name.toLowerCase();

  final Map<String, Table> _tables = {};
  final Map<String, LazySource> _sources = {};
  // Persistent view definitions (the unplanned CREATE VIEW AST). Unlike CTEs and
  // temp file tables, views survive across statements until dropped.
  final Map<String, CreateViewStatement> _views = {};
  // Registered DataFrames, read as views: each builds its plan afresh for
  // every reference, so they survive across statements as views do.
  final Map<
    String,
    ({
      LogicalPlan Function(TableCatalog catalog) plan,
      int? maxRows,
      int? maxBytes,
    })
  >
  _frames = {};

  /// Register a lazy (streamed) file source.
  void registerSource(String name, LazySource source) {
    _sources[_key(name)] = source;
  }

  /// Get a lazy source by name (null if not a lazy source).
  LazySource? source(String name) => _sources[_key(name)];

  /// Remove a lazy source.
  void removeSource(String name) => _sources.remove(_key(name));

  /// Register a table
  void registerTable(String name, Table table) {
    _tables[_key(name)] = table;
  }

  /// Get a table by name
  Table? table(String name) {
    return _tables[_key(name)];
  }

  /// Check if table exists
  bool hasTable(String name) {
    return _tables.containsKey(_key(name));
  }

  /// Remove a table
  void removeTable(String name) {
    _tables.remove(_key(name));
  }

  /// Get all table names
  List<String> get tableNames => _tables.keys.toList();

  /// Get all tables
  Map<String, Table> get tables => Map.unmodifiable(_tables);

  /// Register (or replace) a persistent view definition.
  void registerView(String name, CreateViewStatement definition) {
    _views[_key(name)] = definition;
  }

  /// Get a view definition by name (null if none).
  CreateViewStatement? view(String name) => _views[_key(name)];

  /// Whether [name] resolves to a view.
  bool hasView(String name) => _views.containsKey(_key(name));

  /// Remove a view definition.
  void removeView(String name) => _views.remove(_key(name));

  /// Register (or replace) a DataFrame read as a view. [plan] builds the
  /// frame's logical plan, registering its sources in the catalog it is
  /// given (the frame's own); [maxRows]/[maxBytes] bound a result read from
  /// it.
  void registerFrame(
    String name,
    LogicalPlan Function(TableCatalog catalog) plan, {
    int? maxRows,
    int? maxBytes,
  }) {
    _frames[_key(name)] = (plan: plan, maxRows: maxRows, maxBytes: maxBytes);
  }

  /// The DataFrame registered as [name] (null if none).
  ({
    LogicalPlan Function(TableCatalog catalog) plan,
    int? maxRows,
    int? maxBytes,
  })?
  frame(String name) => _frames[_key(name)];

  /// Remove a registered DataFrame.
  void removeFrame(String name) => _frames.remove(_key(name));
}
