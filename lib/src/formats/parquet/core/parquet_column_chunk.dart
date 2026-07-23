import '../thrift/parquet_thrift.dart' as thrift;
import 'page.dart';

/// Represents a column chunk in a Parquet file
/// Column chunks are the vertical partitioning within a row group
class ParquetColumnChunk {
  final String? filePath;
  final int fileOffset;
  final thrift.ColumnMetaData metaData;
  final int? offsetIndexOffset;
  final int? offsetIndexLength;
  final int? columnIndexOffset;
  final int? columnIndexLength;
  final List<Page> pages;

  ParquetColumnChunk({
    this.filePath,
    required this.fileOffset,
    required this.metaData,
    this.offsetIndexOffset,
    this.offsetIndexLength,
    this.columnIndexOffset,
    this.columnIndexLength,
    List<Page>? pages,
  }) : pages = pages ?? [];

  /// Create from Thrift representation
  factory ParquetColumnChunk.fromThrift(thrift.ColumnChunk thriftChunk) {
    return ParquetColumnChunk(
      filePath: thriftChunk.filePath,
      fileOffset: thriftChunk.fileOffset,
      metaData: thriftChunk.metaData!,
      offsetIndexOffset: thriftChunk.offsetIndexOffset,
      offsetIndexLength: thriftChunk.offsetIndexLength,
      columnIndexOffset: thriftChunk.columnIndexOffset,
      columnIndexLength: thriftChunk.columnIndexLength,
    );
  }

  /// Convert to Thrift representation
  thrift.ColumnChunk toThrift() {
    final chunk = thrift.ColumnChunk();
    chunk.filePath = filePath;
    chunk.fileOffset = fileOffset;
    chunk.metaData = metaData;
    chunk.offsetIndexOffset = offsetIndexOffset;
    chunk.offsetIndexLength = offsetIndexLength;
    chunk.columnIndexOffset = columnIndexOffset;
    chunk.columnIndexLength = columnIndexLength;
    return chunk;
  }

  /// Get the column path in the schema
  List<String> get path => metaData.pathInSchema;

  /// Get the column name
  String get name => metaData.pathInSchema.last;

  /// Get column statistics
  Map<String, dynamic> summary() {
    return {
      'name': name,
      'path': path.join('.'),
      'type': metaData.type.toString(),
      'encodings': metaData.encodings.map((e) => e.toString()).toList(),
      'codec': metaData.codec.toString(),
      'numValues': metaData.numValues,
      'totalUncompressedSize': metaData.totalUncompressedSize,
      'totalCompressedSize': metaData.totalCompressedSize,
      'compressionRatio': metaData.totalUncompressedSize > 0
          ? metaData.totalCompressedSize / metaData.totalUncompressedSize
          : 1.0,
      'numPages': pages.length,
    };
  }

  /// Add a page to this column chunk
  void addPage(Page page) {
    pages.add(page);
  }

  /// Get total number of values across all pages
  int get values => pages.fold(0, (sum, page) => sum + page.numValues);

  /// Check if this column has dictionary encoding
  bool hasDictionaryEncoding() {
    return metaData.encodings.any(
      (e) =>
          e == thrift.Encoding.plainDictionary ||
          e == thrift.Encoding.rleDictionary,
    );
  }

  /// Check if this column has statistics
  bool hasStatistics() {
    return metaData.statistics != null;
  }
}
