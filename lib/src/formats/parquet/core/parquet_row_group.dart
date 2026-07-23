import '../thrift/parquet_thrift.dart' as thrift;
import 'parquet_column_chunk.dart';

/// Represents a row group in a Parquet file
/// Row groups are the horizontal partitioning of data
class ParquetRowGroup {
  final int ordinal;
  final int numRows;
  final List<ParquetColumnChunk> columns;
  final int totalByteSize;
  final int totalCompressedSize;
  final int? fileOffset;
  final Map<String, dynamic> metadata;

  ParquetRowGroup({
    required this.ordinal,
    required this.numRows,
    required this.columns,
    required this.totalByteSize,
    required this.totalCompressedSize,
    this.fileOffset,
    Map<String, dynamic>? metadata,
  }) : metadata = metadata ?? {};

  /// Create from Thrift representation
  factory ParquetRowGroup.fromThrift(
    thrift.RowGroup thriftRowGroup,
    int ordinal,
  ) {
    return ParquetRowGroup(
      ordinal: ordinal,
      numRows: thriftRowGroup.numRows,
      columns: thriftRowGroup.columns
          .map((column) => ParquetColumnChunk.fromThrift(column))
          .toList(),
      totalByteSize: thriftRowGroup.totalByteSize,
      totalCompressedSize:
          thriftRowGroup.totalCompressedSize ?? thriftRowGroup.totalByteSize,
      fileOffset: thriftRowGroup.fileOffset,
    );
  }

  /// Convert to Thrift representation
  thrift.RowGroup toThrift() {
    final rowGroup = thrift.RowGroup();
    rowGroup.numRows = numRows;
    rowGroup.totalByteSize = totalByteSize;
    rowGroup.totalCompressedSize = totalCompressedSize;
    rowGroup.fileOffset = fileOffset;
    rowGroup.columns = columns.map((column) => column.toThrift()).toList();
    return rowGroup;
  }

  /// Get statistics about this row group
  Map<String, dynamic> summary() {
    return {
      'ordinal': ordinal,
      'numRows': numRows,
      'numColumns': columns.length,
      'totalByteSize': totalByteSize,
      'totalCompressedSize': totalCompressedSize,
      'compressionRatio': totalByteSize > 0
          ? totalCompressedSize / totalByteSize
          : 1.0,
      'avgRowSize': numRows > 0 ? totalByteSize / numRows : 0,
    };
  }

  /// Get a specific column chunk by index
  ParquetColumnChunk? columnAt(int index) {
    if (index < 0 || index >= columns.length) {
      return null;
    }
    return columns[index];
  }

  /// Get a column chunk by name
  ParquetColumnChunk? column(String name) {
    return columns.firstWhere(
      (column) => column.path.contains(name),
      orElse: () => throw StateError('Column $name not found'),
    );
  }
}
