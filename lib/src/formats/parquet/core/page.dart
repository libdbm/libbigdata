import 'dart:typed_data';
import '../thrift/parquet_thrift.dart' as thrift;

/// Base class for all page types in Parquet
abstract class Page {
  final thrift.PageType type;
  final int uncompressedSize;
  final int compressedSize;
  final int numValues;

  Page({
    required this.type,
    required this.uncompressedSize,
    required this.compressedSize,
    required this.numValues,
  });

  /// Get page statistics
  Map<String, dynamic> summary();

  /// Get the raw data
  Uint8List? get data;
}

/// Data page containing actual column values
class DataPage extends Page {
  final thrift.Encoding encoding;
  final thrift.Encoding definitionLevelEncoding;
  final thrift.Encoding repetitionLevelEncoding;
  @override
  final Uint8List? data;
  final thrift.Statistics? statistics;

  DataPage({
    required super.uncompressedSize,
    required super.compressedSize,
    required super.numValues,
    required this.encoding,
    required this.definitionLevelEncoding,
    required this.repetitionLevelEncoding,
    this.data,
    this.statistics,
  }) : super(type: thrift.PageType.dataPage);

  @override
  Map<String, dynamic> summary() {
    final stats = {
      'type': 'DATA_PAGE',
      'numValues': numValues,
      'uncompressedSize': uncompressedSize,
      'compressedSize': compressedSize,
      'encoding': encoding.toString(),
      'definitionLevelEncoding': definitionLevelEncoding.toString(),
      'repetitionLevelEncoding': repetitionLevelEncoding.toString(),
    };

    if (statistics != null) {
      stats['hasStatistics'] = true;
      if (statistics!.nullCount != null) {
        stats['nullCount'] = statistics!.nullCount!;
      }
      if (statistics!.distinctCount != null) {
        stats['distinctCount'] = statistics!.distinctCount!;
      }
    }

    return stats;
  }
}

/// Data page version 2 with improved encoding
class DataPageV2 extends Page {
  final int numNulls;
  final int numRows;
  final thrift.Encoding encoding;
  final int definitionLevelsByteLength;
  final int repetitionLevelsByteLength;
  final bool isCompressed;
  @override
  final Uint8List? data;
  final thrift.Statistics? statistics;

  DataPageV2({
    required super.uncompressedSize,
    required super.compressedSize,
    required super.numValues,
    required this.numNulls,
    required this.numRows,
    required this.encoding,
    required this.definitionLevelsByteLength,
    required this.repetitionLevelsByteLength,
    this.isCompressed = true,
    this.data,
    this.statistics,
  }) : super(type: thrift.PageType.dataPageV2);

  @override
  Map<String, dynamic> summary() {
    final stats = {
      'type': 'DATA_PAGE_V2',
      'numValues': numValues,
      'numRows': numRows,
      'numNulls': numNulls,
      'uncompressedSize': uncompressedSize,
      'compressedSize': compressedSize,
      'encoding': encoding.toString(),
      'definitionLevelsByteLength': definitionLevelsByteLength,
      'repetitionLevelsByteLength': repetitionLevelsByteLength,
      'isCompressed': isCompressed,
    };

    if (statistics != null) {
      stats['hasStatistics'] = true;
      if (statistics!.nullCount != null) {
        stats['nullCount'] = statistics!.nullCount!;
      }
      if (statistics!.distinctCount != null) {
        stats['distinctCount'] = statistics!.distinctCount!;
      }
    }

    return stats;
  }
}

/// Dictionary page containing dictionary values
class DictionaryPage extends Page {
  final thrift.Encoding encoding;
  final bool isSorted;
  @override
  final Uint8List? data;

  DictionaryPage({
    required super.uncompressedSize,
    required super.compressedSize,
    required super.numValues,
    required this.encoding,
    this.isSorted = false,
    this.data,
  }) : super(type: thrift.PageType.dictionaryPage);

  @override
  Map<String, dynamic> summary() {
    return {
      'type': 'DICTIONARY_PAGE',
      'numValues': numValues,
      'uncompressedSize': uncompressedSize,
      'compressedSize': compressedSize,
      'encoding': encoding.toString(),
      'isSorted': isSorted,
    };
  }
}

/// Index page for column indexes
class IndexPage extends Page {
  @override
  final Uint8List? data;

  IndexPage({
    required super.uncompressedSize,
    required super.compressedSize,
    this.data,
  }) : super(type: thrift.PageType.indexPage, numValues: 0);

  @override
  Map<String, dynamic> summary() {
    return {
      'type': 'INDEX_PAGE',
      'uncompressedSize': uncompressedSize,
      'compressedSize': compressedSize,
    };
  }
}
