import 'package:meta/meta.dart';
import 'column.dart';
import 'columnar.dart';
import 'data_type.dart';
import 'decimal.dart';
import 'failure.dart';
import 'result.dart';
import 'schema.dart';
import 'data_chunk.dart';
import 'keys.dart';

/// Concatenate [tables] into one named [name] conformed to [schema], null-filling
/// any column a table lacks.
///
/// Every lakehouse reader needs this: files written before a column was added
/// simply don't have it, so the read must pad rather than fail. Shared so a fix
/// here (null-filling, concat width promotion) can't reach one reader and miss
/// its twin: Delta and Iceberg carried byte-identical copies differing only in
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
final class Table {
  /// The table's name.
  final String name;

  /// The columns, in [schema] order, all of [rowCount] rows.
  final List<Column> columns;

  /// The fields describing [columns].
  final Schema schema;

  /// Free-form key/value annotations carried with the table.
  final Map<String, dynamic> metadata;

  /// A table of [columns] described by [schema]. Throws an [ArgumentError]
  /// when they disagree: a different count, name, length or type, or a null
  /// in a non-nullable field.
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
  factory Table.empty(String name, Schema schema) =>
      Table(name: name, columns: emptyColumns(schema), schema: schema);

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

    // Concatenate each column ONCE across all chunks: appending chunk-by-chunk
    // re-copies the growing accumulator every step (O(chunks × rows)).
    final width = chunks.first.columns.length;
    final columns = [
      for (int i = 0; i < width; i++)
        Column.concat([for (final chunk in chunks) chunk.columns[i]]),
    ];

    // A concat that widened a column (int32 then int64 chunks) carries the
    // wider type into the schema, as [append] and [concat] do.
    return Table(
      name: name,
      columns: columns,
      schema: promotedSchema(schema, columns),
    );
  }

  /// Number of rows in the table
  int get rowCount => columns.isEmpty ? 0 : columns.first.length;

  /// Number of columns in the table
  int get columnCount => columns.length;

  /// Get column by name (null if absent). Throws on an AMBIGUOUS bare name (a
  /// post-join collision under distinct qualifiers) rather than silently
  /// returning the first match: consistent with RecordBatch.column / resolve.
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
  /// [Decimal], and the value round-trips back through [Table.fromRows].
  Map<String, dynamic> row(int index) {
    if (index < 0 || index >= rowCount) {
      throw RangeError('Row index $index out of range [0, $rowCount)');
    }

    return rowAt(schema, columns, index);
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
    return DataChunk(columns: columns, schema: schema).split(chunkSize);
  }

  /// Get chunk iterator
  ChunkIterator chunkIterator([int chunkSize = defaultChunkSize]) {
    return ChunkIterator(toChunks(chunkSize));
  }

  /// Select subset of columns
  Table select(List<String> names) => Table(
    name: name,
    // The schema first, so a missing name fails as it always has.
    schema: schema.select(names),
    columns: selectColumns(names, column),
    metadata: metadata,
  );

  /// Filter table by indices
  Table filter(List<int> indices) {
    final filtered = columns.map((column) => column.filter(indices)).toList();
    return Table(
      name: name,
      columns: filtered,
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
  /// each new key is discovered, so a mistaken `partitionBy: ['id']` over
  /// millions of unique rows fails loud at the (cap+1)-th key instead of building
  /// a multi-million-entry grouping map first. Null disables the cap.
  Iterable<({Map<String, Object?> values, Table table})> splitByKeys(
    List<String> keys, {
    int? maxPartitions,
  }) sync* {
    final sources = [for (final k in keys) column(k)!];
    final order = <RowKey>[];
    final groups = <RowKey, List<int>>{};
    final values = <RowKey, Map<String, Object?>>{};
    for (int i = 0; i < rowCount; i++) {
      final entry = <String, Object?>{
        for (int j = 0; j < keys.length; j++) keys[j]: sources[j][i],
      };
      // Structural key: element-wise equality can't collide the way a string
      // join can (a value containing a separator, or a null vs the string
      // "null"): see [RowKey].
      final id = RowKey([for (int j = 0; j < keys.length; j++) sources[j][i]]);
      final group = groups[id];
      if (group == null) {
        if (maxPartitions != null && order.length >= maxPartitions) {
          throw Failure.of(
            Kind.limit,
            'partitioning by ${keys.join(", ")} would create more than '
            '$maxPartitions partitions; a high-cardinality partition column '
            '(e.g. an id) produces millions of tiny files: partition by a '
            'low-cardinality column',
          );
        }
        order.add(id);
        groups[id] = [i];
        values[id] = entry;
      } else {
        group.add(i);
      }
    }
    for (final id in order) {
      yield (values: values[id]!, table: filter(groups[id]!));
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

    final sliced = columns.map((column) => column.slice(start, stop)).toList();
    return Table(
      name: name,
      columns: sliced,
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
  /// one of those: inferring a bare Field would drop the scale (reading `1.50`
  /// back as `150`) or the children. For a plain scalar column [field] is
  /// optional and defaults to the column's own type/nullability.
  Table addColumn(Column column, {Field? field}) {
    if (column.length != rowCount) {
      throw ArgumentError(
        'Column length (${column.length}) must match table row count ($rowCount)',
      );
    }
    final Field added;
    if (field != null) {
      if (field.name != column.name || field.dataType != column.dataType) {
        throw ArgumentError(
          'addColumn field (${field.name}:${field.dataType}) does not match the '
          'column (${column.name}:${column.dataType})',
        );
      }
      added = field;
    } else if (column.dataType == DataType.decimal ||
        column.dataType.isNested) {
      throw ArgumentError(
        'addColumn cannot infer the ${column.dataType} metadata from a bare '
        'column (a decimal\'s scale/precision and a nested type\'s children live '
        'on the Field): pass field: to supply it',
      );
    } else {
      added = Field(
        name: column.name,
        dataType: column.dataType,
        nullable: column.nullable,
      );
    }

    return Table(
      name: name,
      columns: [...columns, column],
      schema: schema.addField(added),
      metadata: metadata,
    );
  }

  /// Remove a column from the table
  Table removeColumn(String columnName) {
    final index = schema.fieldIndex(columnName);
    if (index < 0) {
      throw ArgumentError('Column not found: $columnName');
    }

    final remaining = [...columns];
    remaining.removeAt(index);

    return Table(
      name: name,
      columns: remaining,
      schema: schema.removeField(columnName),
      metadata: metadata,
    );
  }

  /// Rename columns
  Table renameColumns(Map<String, String> renames) {
    final renamed = [
      for (final column in columns)
        renames[column.name] is String
            ? column.withName(renames[column.name]!)
            : column,
    ];

    return Table(
      name: name,
      columns: renamed,
      schema: schema.rename(renames),
      metadata: metadata,
    );
  }

  /// Append another table. This materializes both tables' rows once (a bulk
  /// operation, not an incremental row-appender: use builders for streaming
  /// ingest). [Column.concat] promotes integer/float width across the pieces, so
  /// a wider column never truncates into a narrower buffer.
  Table append(Table other) {
    if (!schema.isCompatibleWith(other.schema)) {
      throw ArgumentError('Cannot append tables with incompatible schemas');
    }

    final combined = [
      for (int i = 0; i < columns.length; i++)
        Column.concat([columns[i], other.columns[i]]),
    ];
    // Reflect any width promotion (e.g. int32 + int64 → int64) in the schema so
    // field dtype and column dtype stay in agreement.
    return Table(
      name: name,
      columns: combined,
      schema: promotedSchema(schema, combined),
      metadata: metadata,
    );
  }

  /// Concatenate compatible-schema tables with ONE N-way [Column.concat] per
  /// column: O(total rows). Repeated [append] in a loop recopies every prior
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
    for (int i = 0; i < columns.length; i++) {
      final column = columns[i];
      stats[column.name] = column.dataType == DataType.decimal
          ? decimalStats(column, schema.fields[i].scale ?? 0)
          : column.stats;
    }
    return stats;
  }

  /// Check if table is empty
  bool get isEmpty => rowCount == 0;

  /// Check if table is not empty
  bool get isNotEmpty => rowCount > 0;

  @override
  String toString() => describe();

  /// Human-readable summary with up to [rows] preview rows. `toString`
  /// uses the default 5; callers that want more (e.g. `DataFrame.show(n)`) pass a
  /// larger count instead of being silently capped at 5.
  String describe([int rows = 5]) {
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

      buffer.writeln('\nPreview (first $rows rows):');
      final preview = head(rows);
      for (int i = 0; i < preview.rowCount; i++) {
        buffer.writeln('  ${preview.row(i)}');
      }
      if (rowCount > rows) {
        buffer.writeln('  ... ${rowCount - rows} more rows');
      }
    }

    return buffer.toString();
  }
}
