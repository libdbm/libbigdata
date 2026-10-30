import 'package:meta/meta.dart';
import 'column.dart';
import 'columnar.dart';
import 'data_type.dart';
import 'decimal.dart';
import 'schema.dart';

/// Row [index] of [columns] as a map keyed by [schema]'s field names.
///
/// A decimal column is stored unscaled with its scale on the field, so a bare
/// value is a meaningless integer (1.50 reads as 150); here it becomes a
/// [Decimal], which round-trips back through `Table.fromRows`.
Map<String, dynamic> rowAt(Schema schema, List<Column> columns, int index) => {
  for (var i = 0; i < columns.length; i++)
    schema.fields[i].name: switch (columns[i][index]) {
      final BigInt value when schema.fields[i].dataType == DataType.decimal =>
        Decimal(value, schema.fields[i].scale ?? 0),
      final value => value,
    },
};

/// Default chunk size for vectorized processing
const int defaultChunkSize = 2048;

/// Represents a chunk of data for vectorized processing
@immutable
final class DataChunk {
  /// The columns, in [schema] order, all of [rowCount] rows.
  final List<Column> columns;

  /// The fields describing [columns].
  final Schema schema;

  /// The number of rows.
  final int rowCount;

  // Defensively wrap the caller's list (mirroring Table.columns) so a later
  // external mutation can't desync the cached rowCount / schema from the actual
  // columns; this is an @immutable, exported type.
  /// A chunk of [columns] described by [schema]; validated as for [Table].
  DataChunk({required List<Column> columns, required this.schema})
    : columns = List.unmodifiable(columns),
      rowCount = columns.isEmpty ? 0 : columns.first.length {
    _validate();
  }

  void _validate() => validateColumns(columns, schema);

  /// Create an empty chunk with the given schema
  factory DataChunk.empty(Schema schema) =>
      DataChunk(columns: emptyColumns(schema), schema: schema);

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

    return rowAt(schema, columns, index);
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

    final sliced = columns.map((column) => column.slice(start, stop)).toList();
    return DataChunk(columns: sliced, schema: schema);
  }

  /// Filter chunk by indices
  DataChunk filter(List<int> indices) {
    final filtered = columns.map((column) => column.filter(indices)).toList();
    return DataChunk(columns: filtered, schema: schema);
  }

  /// Select subset of columns
  DataChunk select(List<String> names) => DataChunk(
    // The schema first, so a missing name fails as it always has.
    schema: schema.select(names),
    columns: selectColumns(names, column),
  );

  /// Append another chunk (must have same schema)
  DataChunk append(DataChunk other) {
    if (!schema.isCompatibleWith(other.schema)) {
      throw ArgumentError('Cannot append chunks with incompatible schemas');
    }

    // Typed bulk concat (NumericColumn → setRange, no boxing; non-numeric falls
    // back to a boxed copy that preserves dtype) instead of materializing both
    // columns into boxed lists and rebuilding.
    final combined = [
      for (int i = 0; i < columns.length; i++)
        Column.concat([columns[i], other.columns[i]]),
    ];
    // Reflect any width promotion (e.g. int32 + int64 → int64) in the schema so
    // the DataChunk constructor's validation doesn't reject the promoted column
    // (mirrors Table.append).
    return DataChunk(
      columns: combined,
      schema: promotedSchema(schema, combined),
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
  List<DataChunk> split(int size) {
    if (size <= 0) {
      throw ArgumentError('Chunk size must be positive');
    }

    final chunks = <DataChunk>[];
    for (int start = 0; start < rowCount; start += size) {
      final end = (start + size).clamp(0, rowCount);
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
      final shown = rowCount > 5 ? 5 : rowCount;
      for (int i = 0; i < shown; i++) {
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
final class ChunkIterator {
  final List<DataChunk> _chunks;
  int _index = 0;

  /// An iterator over the given chunks, positioned at the first.
  ChunkIterator(this._chunks);

  /// Whether [next] has a chunk to return.
  bool get hasNext => _index < _chunks.length;

  /// The next chunk, or null after the last.
  DataChunk? next() {
    if (!hasNext) return null;
    return _chunks[_index++];
  }

  /// Go back to the first chunk.
  void reset() {
    _index = 0;
  }

  /// The number of chunks.
  int get totalChunks => _chunks.length;

  /// How many chunks [next] has returned since the start or the last [reset].
  int get currentChunk => _index;

  /// The number of rows across every chunk.
  int get totalRows => _chunks.fold(0, (sum, chunk) => sum + chunk.rowCount);
}
