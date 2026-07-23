import '../../../core/column.dart';
import '../../../core/data_type.dart';
import '../../../core/schema.dart' as core;
import '../thrift/parquet_thrift.dart' as thrift;
import '../parquet_writer.dart';
import '../schema/schema_converter.dart';
import 'file_writer.dart';
import 'column_writer.dart';
import 'nested_shredder.dart';
import 'page_writer.dart';

/// Writes a row group to a Parquet file
class RowGroupWriter {
  final FileWriter writer;
  final List<thrift.SchemaElement> schema;
  final ParquetWriterOptions options;

  /// Top-level fields (in order), used to drive nested shredding. When null,
  /// every schema leaf is written as a flat column (legacy behavior).
  final List<core.Field>? coreFields;

  RowGroupWriter(this.writer, this.schema, this.options, {this.coreFields});

  /// Write row group data. [typed] carries flat numeric columns that encode
  /// straight from their typed buffer (no boxing); every other column comes
  /// boxed (and physicalized) in [data].
  Future<thrift.RowGroup> write(
    Map<String, List<dynamic>> data,
    int rows, {
    Map<String, NumericColumn> typed = const {},
  }) async {
    final rowGroup = thrift.RowGroup();
    rowGroup.numRows = rows;
    rowGroup.fileOffset = writer.position;
    rowGroup.columns = [];

    int totalSize = 0;
    int compressedSize = 0;

    void account(thrift.ColumnChunk chunk) {
      rowGroup.columns.add(chunk);
      totalSize += chunk.metaData!.totalUncompressedSize;
      compressedSize += chunk.metaData!.totalCompressedSize;
    }

    final fields = coreFields;
    if (fields == null) {
      // Legacy flat path.
      for (final column in getColumns()) {
        final cw = ColumnWriter(writer, column.element, options);
        final fast = typed[column.name];
        account(
          fast != null
              ? await cw.writeTyped(fast)
              : await cw.write(data[column.name] ?? []),
        );
      }
    } else {
      // The file metadata schema ([schema]) is the single source of truth for a
      // column's repetition type (the writer may refine a no-null column to
      // `required`). The per-column encoder must agree with it — otherwise it
      // writes definition levels the reader won't expect (or vice versa),
      // corrupting the page. So take repetition from [schema], not from
      // `scalarElement`'s field-derived default.
      final repByName = {
        for (final e in schema)
          if (e.type != null && e.repetitionType != null)
            e.name: e.repetitionType!,
      };
      for (final field in fields) {
        if (isNestedField(field)) {
          final column = _asColumn(field.name, data[field.name] ?? const []);
          for (final leaf in shred(field, column)) {
            account(await _writeLeaf(leaf));
          }
        } else {
          final element = SchemaConverter().scalarElement(field);
          final rep = repByName[field.name];
          if (rep != null) element.repetitionType = rep;
          final cw = ColumnWriter(writer, element, options);
          final fast = typed[field.name];
          account(
            fast != null
                ? await cw.writeTyped(fast)
                : await cw.write(data[field.name] ?? []),
          );
        }
      }
    }

    rowGroup.totalByteSize = totalSize;
    rowGroup.totalCompressedSize = compressedSize;
    return rowGroup;
  }

  /// Write one shredded nested leaf (PLAIN-encoded with rep/def levels).
  Future<thrift.ColumnChunk> _writeLeaf(LeafColumn leaf) async {
    final chunk = thrift.ColumnChunk();
    chunk.fileOffset = writer.position;
    final pageWriter = PageWriter(
      writer: writer,
      codecType: options.compression,
      checksums: options.writeChecksums,
      writeStats: options.writeStatistics,
    );
    chunk.metaData = await pageWriter.writeColumn(
      leaf.path.last,
      leaf.physicalType,
      leaf.values,
      thrift.Encoding.plain,
      defLevels: leaf.defLevels,
      defBitWidth: leaf.defBitWidth,
      repLevels: leaf.repLevels,
      repBitWidth: leaf.repBitWidth,
      path: leaf.path,
    );
    return chunk;
  }

  // Wrap raw nested values as a Column for the shredder (dataType is irrelevant
  // — the shredder reads structure from the core Field, only values from here).
  Column _asColumn(String name, List<dynamic> values) => ListColumn(
    name: name,
    dataType: DataType.list,
    data: values,
    nullable: true,
  );

  /// Get column information from schema
  List<ColumnInfo> getColumns() {
    final columns = <ColumnInfo>[];
    for (final element in schema) {
      if (element.type != null) {
        columns.add(ColumnInfo(name: element.name!, element: element));
      }
    }
    return columns;
  }
}

/// Column information
class ColumnInfo {
  final String name;
  final thrift.SchemaElement element;

  ColumnInfo({required this.name, required this.element});
}
