import 'package:meta/meta.dart';
import 'column.dart';
import 'columnar.dart';
import 'data_type.dart';
import 'decimal.dart';
import 'schema.dart';
import 'data_chunk.dart';
import 'keys.dart';

/// Concatenate [tables] into one named [name] conformed to [schema], null-filling
/// any column a table lacks.
///
/// Every lakehouse reader needs this: files written before a column was added
/// simply don't have it, so the read must pad rather than fail. Shared so a fix
/// here (null-filling, concat width promotion) can't reach one reader and miss
/// its twin — Delta and Iceberg carried byte-identical copies differing only in
/// the table name.
Table unifyTables(String name, List<Table> tables, Schema schema) => Table(
  name: name,
  columns: [
    for (final field in schema.fields)
      Column.concat([
        for (final table in tables)
          table.column(field.name) ??
              columnFromValues(
                field.name,
                field.dataType,
                List.filled(table.rowCount, null),
              ),
      ]),
  ],
  schema: schema,
);

/// Represents an in-memory table with columnar storage
@immutable
class Table {
  final String name;
  final List<Column> columns;
  final Schema schema;
  final Map<String, dynamic> metadata;

  Table({
    required this.name,
    required List<Column> columns,
    required this.schema,
    Map<String, dynamic> metadata = const {},
  }) : columns = List.unmodifiable(columns),
       metadata = metadata.isEmpty ? const {} : Map.unmodifiable(metadata) {
    _validate();
  }

  void _validate() => validateColumns(columns, schema);

  /// Create an empty table
  factory Table.empty(String name, Schema schema) {
    final columns = schema.fields.map((field) {
      return ListColumn(
        name: field.name,
        dataType: field.dataType,
        data: [],
        nullable: field.nullable,
      );
    }).toList();

    return Table(name: name, columns: columns, schema: schema);
  }

  /// Create table from rows
  factory Table.fromRows(
    String name,
    List<Map<String, dynamic>> rows, {
    Schema? schema,
    bool? strict,
  }) {
    // Infer over ALL rows, not a 100-row sample: columns are built from every
    // row, so a sample that misses a wider int (or a late/differently-typed
    // field) would otherwise truncate values into a too-narrow typed buffer or
    // crash on a cast it never saw.
    final resolved = schema ?? Schema.infer(rows, sampleSize: rows.length);

    if (rows.isEmpty) {
      return Table.empty(name, resolved);
    }
    // An EXPLICIT schema is strict by default: a row key not in the schema is a
    // caller error (e.g. a typo) and is rejected, not silently dropped; an
    // inferred schema is derived from the rows, so every key is already a field.
    // A caller whose "explicit" schema was itself derived from these rows (a
    // format reader inferring then pinning) passes strict: false so a value it
    // already resolved to a wider type (JSON `1` under a string field) is coerced
    // rather than rejected.
    return Table(
      name: name,
      columns: columnsFromRows(
        rows,
        resolved,
        strict: strict ?? (schema != null),
      ),
      schema: resolved,
    );
  }

  /// Create table from chunks
  factory Table.fromChunks(String name, List<DataChunk> chunks) {
    if (chunks.isEmpty) {
      throw ArgumentError('Cannot create table from empty chunks');
    }

    final schema = chunks.first.schema;
    for (final chunk in chunks.skip(1)) {
      if (!schema.isCompatibleWith(chunk.schema)) {
        throw ArgumentError('All chunks must have compatible schemas');
      }
    }

    // Concatenate each column ONCE across all chunks — appending chunk-by-chunk
    // re-copies the growing accumulator every step (O(chunks × rows)).
    final width = chunks.first.columns.length;
    final columns = [
      for (int i = 0; i < width; i++)
        Column.concat([for (final chunk in chunks) chunk.columns[i]]),
    ];

    return Table(name: name, columns: columns, schema: schema);
  }

  /// Number of rows in the table
  int get rowCount => columns.isEmpty ? 0 : columns.first.length;

  /// Number of columns in the table
  int get columnCount => columns.length;

  /// Get column by name (null if absent). Throws on an AMBIGUOUS bare name (a
  /// post-join collision under distinct qualifiers) rather than silently
  /// returning the first match — consistent with RecordBatch.column / resolve.
  Column? column(String name) {
    final index = schema.resolve(name);
    return index >= 0 ? columns[index] : null;
  }

  /// Get column by index
  Column columnAt(int index) {
    if (index < 0 || index >= columns.length) {
      throw RangeError(
        'Column index $index out of range [0, ${columns.length})',
      );
    }
    return columns[index];
  }

  /// Row [index] as a map.
  ///
  /// A decimal column is stored unscaled with its scale on the [Field], so a
  /// bare column value is a meaningless integer (1.50 reads as 150). This is the
  /// boundary that owns the schema, so it is where a decimal becomes a
  /// [Decimal] — the value round-trips back through [Table.fromRows].
  Map<String, dynamic> row(int index) {
    if (index < 0 || index >= rowCount) {
      throw RangeError('Row index $index out of range [0, $rowCount)');
    }

    final row = <String, dynamic>{};
    for (int i = 0; i < columns.length; i++) {
      final field = schema.fields[i];
      final value = columns[i][index];
      row[field.name] = value is BigInt && field.dataType == DataType.decimal
          ? Decimal(value, field.scale ?? 0)
          : value;
    }
    return row;
  }

  /// Convert table to list of rows
  List<Map<String, dynamic>> toRows() {
    final rows = <Map<String, dynamic>>[];
    for (int i = 0; i < rowCount; i++) {
      rows.add(row(i));
    }
    return rows;
  }

  /// Convert table to chunks
  List<DataChunk> toChunks([int chunkSize = defaultChunkSize]) {
    if (chunkSize <= 0) {
      throw ArgumentError.value(chunkSize, 'chunkSize', 'must be positive');
    }
    if (rowCount == 0) {
      return [DataChunk.empty(schema)];
    }

    final chunks = <DataChunk>[];
    for (int start = 0; start < rowCount; start += chunkSize) {
      final end = (start + chunkSize).clamp(0, rowCount);
      final chunkColumns = columns
          .map((column) => column.slice(start, end))
          .toList();
      chunks.add(DataChunk(columns: chunkColumns, schema: schema));
    }
    return chunks;
  }

  /// Get chunk iterator
  ChunkIterator chunkIterator([int chunkSize = defaultChunkSize]) {
    return ChunkIterator(toChunks(chunkSize));
  }

  /// Select subset of columns
  Table select(List<String> columnNames) {
    final schema = this.schema.select(columnNames);
    final selectedColumns = <Column>[];

    for (final name in columnNames) {
      final column = this.column(name);
      if (column == null) {
        throw ArgumentError('Column not found: $name');
      }
      selectedColumns.add(column);
    }

    return Table(
      name: name,
      columns: selectedColumns,
      schema: schema,
      metadata: metadata,
    );
  }

  /// Filter table by indices
  Table filter(List<int> indices) {
    final filteredColumns = columns
        .map((column) => column.filter(indices))
        .toList();
    return Table(
      name: name,
      columns: filteredColumns,
      schema: schema,
      metadata: metadata,
    );
  }

  /// Split into one sub-[Table] per distinct tuple of the [keys] partition
  /// columns, preserving row order within each partition. Each entry carries the
  /// partition key→value map (for the table's partition layout) and its rows.
  ///
  /// LAZY: a generator that yields one copied partition table at a time, so a
  /// partitioned write holds a single partition copy at once instead of every
  /// partition's full column copy simultaneously (the real blow-up on a
  /// high-cardinality key). The O(rows) index map is still built up front.
  ///
  /// [maxPartitions] caps the number of DISTINCT partitions and is enforced as
  /// each new key is discovered — so a mistaken `partitionBy: ['id']` over
  /// millions of unique rows fails loud at the (cap+1)-th key instead of building
  /// a multi-million-entry grouping map first. Null disables the cap.
  Iterable<({Map<String, Object?> values, Table table})> splitByKeys(
    List<String> keys, {
    int? maxPartitions,
  }) sync* {
    final keyCols = [for (final k in keys) column(k)!];
    final order = <RowKey>[];
    final groups = <RowKey, List<int>>{};
    final groupVals = <RowKey, Map<String, Object?>>{};
    for (int i = 0; i < rowCount; i++) {
      final vals = <String, Object?>{
        for (int j = 0; j < keys.length; j++) keys[j]: keyCols[j][i],
      };
      // Structural key: element-wise equality can't collide the way a string
      // join can (a value containing a separator, or a null vs the string
      // "null") — see [RowKey].
      final id = RowKey([for (int j = 0; j < keys.length; j++) keyCols[j][i]]);
      final g = groups[id];
      if (g == null) {
        if (maxPartitions != null && order.length >= maxPartitions) {
          throw StateError(
            'partitioning by ${keys.join(", ")} would create more than '
            '$maxPartitions partitions; a high-cardinality partition column '
            '(e.g. an id) produces millions of tiny files — partition by a '
            'low-cardinality column',
          );
        }
        order.add(id);
        groups[id] = [i];
        groupVals[id] = vals;
      } else {
        g.add(i);
      }
    }
    for (final id in order) {
      yield (values: groupVals[id]!, table: filter(groups[id]!));
    }
  }

  /// Filter table by predicate
  Table where(bool Function(Map<String, dynamic>) predicate) {
    final indices = <int>[];
    for (int i = 0; i < rowCount; i++) {
      if (predicate(row(i))) {
        indices.add(i);
      }
    }
    return filter(indices);
  }

  /// Slice table rows
  Table slice(int start, [int? end]) {
    final stop = end ?? rowCount;
    if (start < 0 || start > rowCount || stop < start || stop > rowCount) {
      throw RangeError(
        'Invalid slice range [$start, $stop) for table of size $rowCount',
      );
    }

    final slicedColumns = columns
        .map((column) => column.slice(start, stop))
        .toList();
    return Table(
      name: name,
      columns: slicedColumns,
      schema: schema,
      metadata: metadata,
    );
  }

  /// Take first n rows
  Table head([int n = 5]) {
    return slice(0, n.clamp(0, rowCount));
  }

  /// Take last n rows
  Table tail([int n = 5]) {
    final start = (rowCount - n).clamp(0, rowCount);
    return slice(start, rowCount);
  }

  /// Add a column to the table. A decimal's scale/precision and a nested type's
  /// children live on the [Field], not the [Column], so pass [field] when adding
  /// one of those — inferring a bare Field would drop the scale (reading `1.50`
  /// back as `150`) or the children. For a plain scalar column [field] is
  /// optional and defaults to the column's own type/nullability.
  Table addColumn(Column column, {Field? field}) {
    if (column.length != rowCount) {
      throw ArgumentError(
        'Column length (${column.length}) must match table row count ($rowCount)',
      );
    }
    final Field newField;
    if (field != null) {
      if (field.name != column.name || field.dataType != column.dataType) {
        throw ArgumentError(
          'addColumn field (${field.name}:${field.dataType}) does not match the '
          'column (${column.name}:${column.dataType})',
        );
      }
      newField = field;
    } else if (column.dataType == DataType.decimal ||
        column.dataType.isNested) {
      throw ArgumentError(
        'addColumn cannot infer the ${column.dataType} metadata from a bare '
        'column (a decimal\'s scale/precision and a nested type\'s children live '
        'on the Field): pass field: to supply it',
      );
    } else {
      newField = Field(
        name: column.name,
        dataType: column.dataType,
        nullable: column.nullable,
      );
    }

    return Table(
      name: name,
      columns: [...columns, column],
      schema: schema.addField(newField),
      metadata: metadata,
    );
  }

  /// Remove a column from the table
  Table removeColumn(String columnName) {
    final index = schema.fieldIndex(columnName);
    if (index < 0) {
      throw ArgumentError('Column not found: $columnName');
    }

    final newColumns = [...columns];
    newColumns.removeAt(index);

    return Table(
      name: name,
      columns: newColumns,
      schema: schema.removeField(columnName),
      metadata: metadata,
    );
  }

  /// Rename columns
  Table renameColumns(Map<String, String> renames) {
    final renamedColumns = [
      for (final column in columns)
        renames[column.name] is String
            ? column.withName(renames[column.name]!)
            : column,
    ];

    return Table(
      name: name,
      columns: renamedColumns,
      schema: schema.rename(renames),
      metadata: metadata,
    );
  }

  /// Append another table. This materializes both tables' rows once (a bulk
  /// operation, not an incremental row-appender — use builders for streaming
  /// ingest). [Column.concat] promotes integer/float width across the pieces, so
  /// a wider column never truncates into a narrower buffer.
  Table append(Table other) {
    if (!schema.isCompatibleWith(other.schema)) {
      throw ArgumentError('Cannot append tables with incompatible schemas');
    }

    final combinedColumns = [
      for (int i = 0; i < columns.length; i++)
        Column.concat([columns[i], other.columns[i]]),
    ];
    // Reflect any width promotion (e.g. int32 + int64 → int64) in the schema so
    // field dtype and column dtype stay in agreement.
    return Table(
      name: name,
      columns: combinedColumns,
      schema: promotedSchema(schema, combinedColumns),
      metadata: metadata,
    );
  }

  /// Concatenate compatible-schema tables with ONE N-way [Column.concat] per
  /// column — O(total rows). Repeated [append] in a loop recopies every prior
  /// row on each call (O(n^2)); callers accumulating a batch stream should use
  /// this instead.
  static Table concat(List<Table> tables) {
    if (tables.isEmpty) {
      return Table.empty('table', Schema(fields: const []));
    }
    if (tables.length == 1) return tables.first;
    final first = tables.first;
    for (final other in tables.skip(1)) {
      if (!first.schema.isCompatibleWith(other.schema)) {
        throw ArgumentError('Cannot concat tables with incompatible schemas');
      }
    }
    final columns = [
      for (int i = 0; i < first.columns.length; i++)
        Column.concat([for (final t in tables) t.columns[i]]),
    ];
    return Table(
      name: first.name,
      columns: columns,
      schema: promotedSchema(first.schema, columns),
      metadata: first.metadata,
    );
  }

  /// Get memory usage in bytes
  int get memoryUsage {
    return columns.fold(0, (sum, column) => sum + column.memoryUsage);
  }

  /// Get table statistics
  Map<String, ColumnStats?> get statistics {
    final stats = <String, ColumnStats?>{};
    for (final column in columns) {
      stats[column.name] = column.stats;
    }
    return stats;
  }

  /// Check if table is empty
  bool get isEmpty => rowCount == 0;

  /// Check if table is not empty
  bool get isNotEmpty => rowCount > 0;

  @override
  String toString() => describe();

  /// Human-readable summary with up to [previewRows] preview rows. `toString`
  /// uses the default 5; callers that want more (e.g. `DataFrame.show(n)`) pass a
  /// larger count instead of being silently capped at 5.
  String describe([int previewRows = 5]) {
    final buffer = StringBuffer();
    buffer.writeln('Table: $name');
    buffer.writeln('Rows: $rowCount, Columns: $columnCount');
    buffer.writeln(
      'Memory: ${(memoryUsage / 1024 / 1024).toStringAsFixed(2)} MB',
    );

    if (isNotEmpty) {
      buffer.writeln('\nSchema:');
      for (final field in schema.fields) {
        buffer.writeln(
          '  ${field.name}: ${field.dataType.name}${field.nullable ? "?" : ""}',
        );
      }

      buffer.writeln('\nPreview (first $previewRows rows):');
      final preview = head(previewRows);
      for (int i = 0; i < preview.rowCount; i++) {
        buffer.writeln('  ${preview.row(i)}');
      }
      if (rowCount > previewRows) {
        buffer.writeln('  ... ${rowCount - previewRows} more rows');
      }
    }

    return buffer.toString();
  }
}
