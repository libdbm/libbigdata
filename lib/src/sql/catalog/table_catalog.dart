import '../../core/schema.dart';
import '../../core/table.dart';
import '../../core/row_filter.dart';
import '../ast/statements.dart';

/// A file-backed table read lazily (streamed by the scan operator) instead of
/// materialized up front. Carries an optional pushed-down row-group pruning
/// [filter].
class LazySource {
  final String path;
  final Schema schema;
  final RowFilter? filter;
  const LazySource(this.path, this.schema, {this.filter});
}

/// Catalog for managing tables
class TableCatalog {
  final Map<String, Table> _tables = {};
  final Map<String, LazySource> _sources = {};
  // Persistent view definitions (the unplanned CREATE VIEW AST). Unlike CTEs and
  // temp file tables, views survive across statements until dropped.
  final Map<String, CreateViewStatement> _views = {};

  /// Register a lazy (streamed) file source.
  void registerSource(String name, LazySource source) {
    _sources[name.toLowerCase()] = source;
  }

  /// Get a lazy source by name (null if not a lazy source).
  LazySource? source(String name) => _sources[name.toLowerCase()];

  /// Whether [name] resolves to a lazy source.
  bool hasSource(String name) => _sources.containsKey(name.toLowerCase());

  /// Remove a lazy source.
  void removeSource(String name) => _sources.remove(name.toLowerCase());

  /// Register a table
  void registerTable(String name, Table table) {
    _tables[name.toLowerCase()] = table;
  }

  /// Get a table by name
  Table? table(String name) {
    return _tables[name.toLowerCase()];
  }

  /// Check if table exists
  bool hasTable(String name) {
    return _tables.containsKey(name.toLowerCase());
  }

  /// Remove a table
  void removeTable(String name) {
    _tables.remove(name.toLowerCase());
  }

  /// Get all table names
  List<String> get tableNames => _tables.keys.toList();

  /// Get all tables
  Map<String, Table> get tables => Map.unmodifiable(_tables);

  /// Register (or replace) a persistent view definition.
  void registerView(String name, CreateViewStatement def) {
    _views[name.toLowerCase()] = def;
  }

  /// Get a view definition by name (null if none).
  CreateViewStatement? view(String name) => _views[name.toLowerCase()];

  /// Whether [name] resolves to a view.
  bool hasView(String name) => _views.containsKey(name.toLowerCase());

  /// Remove a view definition.
  void removeView(String name) => _views.remove(name.toLowerCase());
}
