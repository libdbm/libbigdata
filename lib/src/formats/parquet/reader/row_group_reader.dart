import '../thrift/parquet_thrift.dart' as thrift;
import '../parquet_reader.dart';
import '../schema/schema_levels.dart';
import '../../../core/column.dart';
import 'file_reader.dart';
import 'column_reader.dart';
import 'dremel_assembler.dart' show RawLeaf;

/// Reads a single row group from a Parquet file
class RowGroupReader {
  final FileReader reader;
  final thrift.RowGroup rowGroup;
  final List<thrift.SchemaElement> schema;
  final ParquetReaderOptions options;

  RowGroupReader(this.reader, this.rowGroup, this.schema, this.options);

  /// Read all columns in the row group into [Column]s keyed by column name.
  Future<Map<String, Column>> read() async {
    final data = <String, Column>{};
    final infos = columns();

    for (final column in infos) {
      if (options.columns != null && !options.columns!.contains(column.name)) {
        continue;
      }

      final columnReader = ColumnReader(
        reader,
        column.chunk,
        column.element,
        options,
        name: column.name,
        allElements: schema,
        elementIndex: column.index,
      );

      data[column.name] = await columnReader.read();
    }

    return data;
  }

  /// Raw per-leaf decode (values + rep/def levels, no record assembly) keyed by
  /// dotted leaf path — drives the multi-level [DremelAssembler]. Reads ALL leaf
  /// columns (no projection; nested reassembly needs every leaf).
  Future<Map<String, RawLeaf>> readRaw() async {
    final out = <String, RawLeaf>{};
    for (final column in columns()) {
      final columnReader = ColumnReader(
        reader,
        column.chunk,
        column.element,
        options,
        name: column.name,
        allElements: schema,
        elementIndex: column.index,
      );
      out[column.name] = await columnReader.readRaw();
    }
    return out;
  }

  /// A page-bounded cursor over this row group: each [RowGroupCursor.next] yields
  /// a ~`size`-row slice of the selected columns without materializing the whole
  /// group, advancing every column in lockstep so they stay row-aligned.
  RowGroupCursor cursor() {
    final cursors = <({String name, ColumnCursor cursor})>[];
    for (final column in columns()) {
      if (options.columns != null && !options.columns!.contains(column.name)) {
        continue;
      }
      final cr = ColumnReader(
        reader,
        column.chunk,
        column.element,
        options,
        name: column.name,
        allElements: schema,
        elementIndex: column.index,
      );
      cursors.add((name: column.name, cursor: cr.cursor()));
    }
    return RowGroupCursor(
      cursors,
      rowGroup.numRows.toInt(),
      options.tolerateTruncatedPages,
    );
  }

  /// Get column information from schema and chunks
  List<ColumnInfo> columns() {
    final infos = <ColumnInfo>[];

    // Leaf paths line up with column chunks in document order; use them as
    // unique column names so nested leaves don't collide.
    final paths = SchemaLevels.leafPaths(schema);

    int chunkIndex = 0;
    for (int i = 0; i < schema.length; i++) {
      final element = schema[i];

      if (element.type != null) {
        // Primitive elements have a type
        if (chunkIndex >= rowGroup.columns.length) {
          throw FormatException('Column chunk index out of bounds');
        }

        infos.add(
          ColumnInfo(
            name: chunkIndex < paths.length ? paths[chunkIndex] : element.name!,
            element: element,
            chunk: rowGroup.columns[chunkIndex],
            index: i,
          ),
        );

        chunkIndex++;
      }
    }

    return infos;
  }
}

/// Lockstep cursor over a row group's selected column cursors. Emits ~`size`-row
/// column maps until the group is exhausted.
class RowGroupCursor {
  final List<({String name, ColumnCursor cursor})> _cols;
  final int _numRows;
  final bool _tolerant;
  int _emitted = 0;

  RowGroupCursor(this._cols, this._numRows, this._tolerant);

  /// Next ~[size]-row slice of the selected columns, or null when exhausted.
  Future<Map<String, Column>?> next(int size) async {
    if (size <= 0) throw ArgumentError.value(size, 'size', 'must be > 0');
    if (_cols.isEmpty) {
      // No columns selected (e.g. COUNT(*)): still surface the row count as
      // empty-column batches so consumers see the right number of rows.
      if (_emitted >= _numRows) return null;
      final take = (_numRows - _emitted).clamp(0, size);
      _emitted += take;
      return <String, Column>{};
    }
    for (final c in _cols) {
      await c.cursor.fill(size);
    }
    var ready = -1;
    for (final c in _cols) {
      final r = c.cursor.readyRows;
      if (ready < 0 || r < ready) ready = r;
    }
    if (ready <= 0) {
      // All columns exhausted. A group that emitted fewer than its declared
      // rows is a truncated file: fail loud, or — in tolerant mode — pad the
      // remaining tail with NULLs (matching the eager typed reader) so a
      // streaming read reports the same row count as an eager one.
      if (_emitted < _numRows) {
        if (!_tolerant) {
          throw FormatException(
            'Parquet row group truncated: $_emitted of $_numRows rows',
          );
        }
        // Per column: emit its own remaining real rows, then NULL-pad the
        // shortfall. A column whose chunk is intact (still has ready rows) keeps
        // its tail; only genuinely short columns pad — matching the eager reader,
        // which pads each column to its own declared length.
        final take = (_numRows - _emitted).clamp(0, size);
        _emitted += take;
        return <String, Column>{
          for (final c in _cols) c.name: c.cursor.takeOrPad(take),
        };
      }
      return null;
    }
    final take = ready < size ? ready : size;
    final out = <String, Column>{
      for (final c in _cols) c.name: c.cursor.take(take),
    };
    _emitted += take;
    return out;
  }
}

/// Information about a column
class ColumnInfo {
  final String name;
  final thrift.SchemaElement element;
  final thrift.ColumnChunk chunk;
  final int index;

  ColumnInfo({
    required this.name,
    required this.element,
    required this.chunk,
    this.index = 0,
  });
}
