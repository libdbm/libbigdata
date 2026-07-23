import 'dart:io';
import '../thrift/parquet_thrift.dart' as thrift;
import '../schema/schema_converter.dart' show ParquetSchema;
import 'parquet_row_group.dart';

/// High-level representation of a Parquet file
class ParquetFile {
  final String filePath;
  final thrift.FileMetaData metadata;
  final List<ParquetRowGroup> rowGroups;
  final ParquetSchema schema;
  final Map<String, dynamic> properties;

  ParquetFile({
    required this.filePath,
    required this.metadata,
    required this.rowGroups,
    required this.schema,
    Map<String, dynamic>? properties,
  }) : properties = properties ?? {};

  /// Get file statistics
  Map<String, dynamic> summary() {
    final totalRows = rowGroups.fold<int>(0, (sum, rg) => sum + rg.numRows);
    final totalSize = rowGroups.fold<int>(
      0,
      (sum, rg) => sum + rg.totalByteSize,
    );
    final compressedSize = rowGroups.fold<int>(
      0,
      (sum, rg) => sum + rg.totalCompressedSize,
    );

    return {
      'filePath': filePath,
      'version': metadata.version,
      'createdBy': metadata.createdBy,
      'numRowGroups': rowGroups.length,
      'totalRows': totalRows,
      'numColumns': schema.leafColumns.length,
      'totalSize': totalSize,
      'compressedSize': compressedSize,
      'compressionRatio': totalSize > 0 ? compressedSize / totalSize : 1.0,
      'avgRowGroupSize': rowGroups.isNotEmpty
          ? totalRows ~/ rowGroups.length
          : 0,
      'keyValueMetadata':
          metadata.keyValueMetadata
              ?.map((kv) => '${kv.key}: ${kv.value}')
              .toList() ??
          [],
    };
  }

  /// Get a specific row group
  ParquetRowGroup? rowGroup(int index) {
    if (index < 0 || index >= rowGroups.length) {
      return null;
    }
    return rowGroups[index];
  }

  /// Get all column names
  List<String> get columnNames =>
      schema.leafColumns.map((column) => column.name).toList();

  /// Check if file is valid
  bool isValid() {
    // Check magic bytes
    final file = File(filePath);
    if (!file.existsSync()) return false;

    try {
      final raf = file.openSync();

      // Check start magic
      raf.setPositionSync(0);
      final startMagic = raf.readSync(4);
      if (String.fromCharCodes(startMagic) != "PAR1") {
        raf.closeSync();
        return false;
      }

      // Check end magic
      final length = raf.lengthSync();
      raf.setPositionSync(length - 4);
      final endMagic = raf.readSync(4);
      if (String.fromCharCodes(endMagic) != "PAR1") {
        raf.closeSync();
        return false;
      }

      raf.closeSync();
      return true;
    } catch (e) {
      return false;
    }
  }
}
