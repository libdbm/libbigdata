import 'package:meta/meta.dart';
import 'column.dart';
import 'columnar.dart';
import 'schema.dart';

/// Default chunk size for vectorized processing
const int defaultChunkSize = 2048;

/// Represents a chunk of data for vectorized processing
@immutable
class DataChunk {
  final List<Column> columns;
  final Schema schema;
  final int rowCount;

  // Defensively wrap the caller's list (mirroring Table.columns) so a later
  // external mutation can't desync the cached rowCount / schema from the actual
  // columns — this is an @immutable, exported type.
  DataChunk({required List<Column> columns, required this.schema})
    : columns = List.unmodifiable(columns),
      rowCount = columns.isEmpty ? 0 : columns.first.length {
    _validate();
  }

  void _validate() => validateColumns(columns, schema);

  /// Create an empty chunk with the given schema
  factory DataChunk.empty(Schema schema) {
    final columns = schema.fields.map((field) {
      return ListColumn(
        name: field.name,
        dataType: field.dataType,
        data: [],
        nullable: field.nullable,
      );
    }).toList();

    return DataChunk(columns: columns, schema: schema);
  }

  /// Create a chunk from row data
  factory DataChunk.fromRows(List<Map<String, dynamic>> rows, Schema schema) {
    if (rows.isEmpty) {
      return DataChunk.empty(schema);
    }
    // Always explicit-schema → strict: reject a row key that isn't a field.
    return DataChunk(
      columns: columnsFromRows(rows, schema, strict: true),
      schema: schema,
    );
  }

  /// Get a column by name
  Column? column(String name) {
    final index = schema.fieldIndex(name);
    return index >= 0 ? columns[index] : null;
  }

  /// Get a column by index
  Column columnAt(int index) {
    if (index < 0 || index >= columns.length) {
      throw RangeError(
        'Column index $index out of range [0, ${columns.length})',
      );
    }
    return columns[index];
  }

  /// Get a row as a map
  Map<String, dynamic> row(int index) {
    if (index < 0 || index >= rowCount) {
      throw RangeError('Row index $index out of range [0, $rowCount)');
    }

    final row = <String, dynamic>{};
    for (int i = 0; i < columns.length; i++) {
      row[schema.fields[i].name] = columns[i][index];
    }
    return row;
  }

  /// Convert chunk to list of rows
  List<Map<String, dynamic>> toRows() {
    final rows = <Map<String, dynamic>>[];
    for (int i = 0; i < rowCount; i++) {
      rows.add(row(i));
    }
    return rows;
  }

  /// Slice the chunk
  DataChunk slice(int start, [int? end]) {
    final stop = end ?? rowCount;
    if (start < 0 || start > rowCount || stop < start || stop > rowCount) {
      throw RangeError(
        'Invalid slice range [$start, $stop) for chunk of size $rowCount',
      );
    }

    final slicedColumns = columns
        .map((column) => column.slice(start, stop))
        .toList();
    return DataChunk(columns: slicedColumns, schema: schema);
  }

  /// Filter chunk by indices
  DataChunk filter(List<int> indices) {
    final filteredColumns = columns
        .map((column) => column.filter(indices))
        .toList();
    return DataChunk(columns: filteredColumns, schema: schema);
  }

  /// Select subset of columns
  DataChunk select(List<String> columnNames) {
    final schema = this.schema.select(columnNames);
    final selectedColumns = <Column>[];

    for (final name in columnNames) {
      final column = this.column(name);
      if (column == null) {
        throw ArgumentError('Column not found: $name');
      }
      selectedColumns.add(column);
    }

    return DataChunk(columns: selectedColumns, schema: schema);
  }

  /// Append another chunk (must have same schema)
  DataChunk append(DataChunk other) {
    if (!schema.isCompatibleWith(other.schema)) {
      throw ArgumentError('Cannot append chunks with incompatible schemas');
    }

    // Typed bulk concat (NumericColumn → setRange, no boxing; non-numeric falls
    // back to a boxed copy that preserves dtype) instead of materializing both
    // columns into boxed lists and rebuilding.
    final combinedColumns = [
      for (int i = 0; i < columns.length; i++)
        Column.concat([columns[i], other.columns[i]]),
    ];
    // Reflect any width promotion (e.g. int32 + int64 → int64) in the schema so
    // the DataChunk constructor's validation doesn't reject the promoted column
    // (mirrors Table.append).
    return DataChunk(
      columns: combinedColumns,
      schema: promotedSchema(schema, combinedColumns),
    );
  }

  /// Get memory usage in bytes
  int get memoryUsage {
    return columns.fold(0, (sum, column) => sum + column.memoryUsage);
  }

  /// Check if chunk is empty
  bool get isEmpty => rowCount == 0;

  /// Check if chunk is not empty
  bool get isNotEmpty => rowCount > 0;

  /// Split into smaller chunks
  List<DataChunk> split(int chunkSize) {
    if (chunkSize <= 0) {
      throw ArgumentError('Chunk size must be positive');
    }

    final chunks = <DataChunk>[];
    for (int start = 0; start < rowCount; start += chunkSize) {
      final end = (start + chunkSize).clamp(0, rowCount);
      chunks.add(slice(start, end));
    }
    return chunks;
  }

  @override
  String toString() {
    final buffer = StringBuffer();
    buffer.writeln('DataChunk(rows: $rowCount, columns: ${columns.length})');

    if (rowCount > 0 && columns.isNotEmpty) {
      buffer.writeln('Schema:');
      for (final field in schema.fields) {
        buffer.writeln('  ${field.name}: ${field.dataType.name}');
      }

      buffer.writeln('Preview (first 5 rows):');
      final previewRows = rowCount > 5 ? 5 : rowCount;
      for (int i = 0; i < previewRows; i++) {
        buffer.write('  ');
        final row = this.row(i);
        buffer.writeln(row);
      }
      if (rowCount > 5) {
        buffer.writeln('  ... ${rowCount - 5} more rows');
      }
    }

    return buffer.toString();
  }
}

/// Iterator for processing data in chunks
class ChunkIterator {
  final List<DataChunk> _chunks;
  int _currentIndex = 0;

  ChunkIterator(this._chunks);

  bool get hasNext => _currentIndex < _chunks.length;

  DataChunk? next() {
    if (!hasNext) return null;
    return _chunks[_currentIndex++];
  }

  void reset() {
    _currentIndex = 0;
  }

  int get totalChunks => _chunks.length;

  int get currentChunk => _currentIndex;

  int get totalRows => _chunks.fold(0, (sum, chunk) => sum + chunk.rowCount);
}
