import 'dart:typed_data';
import '../../../core/column.dart';
import '../thrift/parquet_thrift.dart' as thrift;
import '../parquet_writer.dart';
import 'file_writer.dart';
import 'page_writer.dart';

/// Writes a column to a Parquet file
class ColumnWriter {
  final FileWriter writer;
  final thrift.SchemaElement element;
  final ParquetWriterOptions options;

  ColumnWriter(this.writer, this.element, this.options);

  /// Write column values
  Future<thrift.ColumnChunk> write(List<dynamic> values) async {
    final chunk = thrift.ColumnChunk();
    chunk.fileOffset = writer.position;

    // Choose encoding
    final encoding = chooseEncoding(values);

    // Optional columns carry definition levels (1 = present, 0 = null);
    // required columns carry none.
    final optional =
        element.repetitionType == thrift.FieldRepetitionType.optional;
    final defLevels = optional
        ? [for (final v in values) v == null ? 0 : 1]
        : null;

    // Create page writer
    final pageWriter = PageWriter(
      writer: writer,
      codecType: options.compression,
      checksums: options.writeChecksums,
      writeStats: options.writeStatistics,
    );

    // Write column with pages. Statistics honor writeStatistics inside
    // PageWriter (the single gate for flat and nested columns alike).
    final metadata = await pageWriter.writeColumn(
      element.name!,
      element.type!,
      values,
      encoding,
      defLevels: defLevels,
      defBitWidth: optional ? 1 : 0,
    );
    chunk.metaData = metadata;
    return chunk;
  }

  /// Write a flat numeric column from its typed buffer, skipping the boxing that
  /// [write] does. Low-cardinality numeric columns (dictionary) fall back to the
  /// boxed path; PLAIN/DELTA encode straight from the buffer.
  Future<thrift.ColumnChunk> writeTyped(NumericColumn column) async {
    final encoding = chooseEncodingTyped(column);
    if (encoding == thrift.Encoding.rleDictionary) {
      return write(column.toList());
    }

    final chunk = thrift.ColumnChunk();
    chunk.fileOffset = writer.position;

    final optional =
        element.repetitionType == thrift.FieldRepetitionType.optional;
    List<int>? defLevels;
    if (optional) {
      final n = column.length;
      final levels = Uint8List(n)..fillRange(0, n, 1);
      final bitmap = column.rawNullBitmap;
      if (bitmap != null) {
        for (int i = 0; i < n; i++) {
          if ((bitmap[i >> 3] & (1 << (i & 7))) != 0) levels[i] = 0;
        }
      }
      defLevels = levels;
    }

    final pageWriter = PageWriter(
      writer: writer,
      codecType: options.compression,
      checksums: options.writeChecksums,
      writeStats: options.writeStatistics,
    );
    final metadata = await pageWriter.writeColumnTyped(
      element.name!,
      element.type!,
      column,
      encoding,
      defLevels: defLevels,
      defBitWidth: optional ? 1 : 0,
    );
    chunk.metaData = metadata;
    return chunk;
  }

  /// Cardinality/encoding decision for a typed numeric column, probing the raw
  /// buffer without boxing. Mirrors [chooseEncoding]: dictionary for low-card
  /// integers, DELTA for other integers, PLAIN for floats.
  thrift.Encoding chooseEncodingTyped(NumericColumn column) {
    final type = element.type!;
    final isInt =
        type == thrift.ParquetType.int32 || type == thrift.ParquetType.int64;
    if (!isInt) return thrift.Encoding.plain;

    final data = column.rawBuffer as List<int>;
    final bitmap = column.rawNullBitmap;
    final n = column.length;
    const cap = 4096;
    final unique = <int>{};
    int count = 0;
    bool highCard = false;
    for (int i = 0; i < n; i++) {
      if (bitmap != null && (bitmap[i >> 3] & (1 << (i & 7))) != 0) continue;
      count++;
      if (!highCard) {
        unique.add(data[i]);
        if (unique.length > cap) highCard = true;
      }
    }
    if (!highCard && count > 0 && unique.length <= count ~/ 2) {
      return thrift.Encoding.rleDictionary;
    }
    return thrift.Encoding.deltaBinaryPacked;
  }

  /// Choose an encoding for [values]: dictionary for low-cardinality columns,
  /// DELTA_BINARY_PACKED for other integers, PLAIN otherwise.
  thrift.Encoding chooseEncoding(List<dynamic> values) {
    final type = element.type!;
    final isInt =
        type == thrift.ParquetType.int32 || type == thrift.ParquetType.int64;
    final dictable = isInt || type == thrift.ParquetType.byteArray;
    if (!dictable) return thrift.Encoding.plain;

    // Cheap cardinality probe; bail out once it is clearly high. A few thousand
    // distinct is plenty to decide dictionary-vs-not — a high-cardinality column
    // (id/timestamp) then stops hashing after ~[cap] values instead of 64k.
    const cap = 4096;
    final unique = <dynamic>{};
    int count = 0;
    bool highCard = false;
    for (final v in values) {
      if (v == null) continue;
      count++;
      if (!highCard) {
        unique.add(v);
        if (unique.length > cap) highCard = true;
      }
    }

    if (!highCard && count > 0 && unique.length <= count ~/ 2) {
      return thrift.Encoding.rleDictionary;
    }
    return isInt ? thrift.Encoding.deltaBinaryPacked : thrift.Encoding.plain;
  }

  /// Check if encoding is dictionary-based
  bool isDictionary(thrift.Encoding encoding) {
    return encoding == thrift.Encoding.plainDictionary ||
        encoding == thrift.Encoding.rleDictionary;
  }
}
