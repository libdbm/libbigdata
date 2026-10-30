import 'dart:typed_data';
import '../../../core/column.dart';
import '../../../core/failure.dart';
import '../../../core/result.dart';
import '../compression.dart' show ParquetCodec;
import '../thrift/parquet_thrift.dart' as thrift;
import '../parquet_writer.dart';
import 'file_writer.dart';
import '../encoding/page_encoder.dart' show valueBytes;
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
    if (!optional && values.contains(null)) _required();
    final defLevels = optional
        ? [for (final value in values) value == null ? 0 : 1]
        : null;

    // Create page writer
    final pageWriter = PageWriter(
      writer: writer,
      codecType: options.compression.codec,
      checksums: options.writeChecksums,
      writeStats: options.writeStatistics,
    );

    // Write column with pages. Statistics honor writeStatistics inside
    // PageWriter (the single check for flat and nested columns alike).
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
  Future<thrift.ColumnChunk> writeTyped(NumericVector column) async {
    final encoding = chooseEncodingTyped(column);
    if (encoding == thrift.Encoding.rleDictionary) {
      return write(column.toList());
    }

    final chunk = thrift.ColumnChunk();
    chunk.fileOffset = writer.position;

    final optional =
        element.repetitionType == thrift.FieldRepetitionType.optional;
    final nulls = column.nulls;
    if (!optional && nulls != null && nulls.any((byte) => byte != 0)) {
      _required();
    }
    List<int>? defLevels;
    if (optional) {
      final count = column.length;
      final levels = Uint8List(count)..fillRange(0, count, 1);
      final bitmap = column.nulls;
      if (bitmap != null) {
        for (int i = 0; i < count; i++) {
          if ((bitmap[i >> 3] & (1 << (i & 7))) != 0) levels[i] = 0;
        }
      }
      defLevels = levels;
    }

    final pageWriter = PageWriter(
      writer: writer,
      codecType: options.compression.codec,
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
  thrift.Encoding chooseEncodingTyped(NumericVector column) {
    final type = element.type!;
    final isInt =
        type == thrift.ParquetType.int32 || type == thrift.ParquetType.int64;
    if (!isInt) return thrift.Encoding.plain;

    final data = column.raw as List<int>;
    final bitmap = column.nulls;
    final length = column.length;
    const cap = 4096;
    final unique = <int>{};
    int count = 0;
    bool varied = false;
    for (int i = 0; i < length; i++) {
      if (bitmap != null && (bitmap[i >> 3] & (1 << (i & 7))) != 0) continue;
      count++;
      if (!varied) {
        unique.add(data[i]);
        if (unique.length > cap) varied = true;
      }
    }
    if (!varied && count > 0 && unique.length <= count ~/ 2) {
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
    final eligible = isInt || type == thrift.ParquetType.byteArray;
    if (!eligible) return thrift.Encoding.plain;

    // Cheap cardinality probe; bail out once it is clearly high. A few thousand
    // distinct is plenty to decide dictionary-vs-not: a high-cardinality column
    // (id/timestamp) then stops hashing after ~[cap] values instead of 64k.
    // The dictionary is one page, so its values share the page byte budget.
    const cap = 4096;
    const budget = 1 << 20;
    final unique = <dynamic>{};
    var size = 0;
    int count = 0;
    bool varied = false;
    for (final value in values) {
      if (value == null) continue;
      count++;
      if (!varied && unique.add(value)) {
        size += valueBytes(value);
        if (unique.length > cap || size > budget) varied = true;
      }
    }

    if (!varied && count > 0 && unique.length <= count ~/ 2) {
      return thrift.Encoding.rleDictionary;
    }
    return isInt ? thrift.Encoding.deltaBinaryPacked : thrift.Encoding.plain;
  }

  /// A required column holds no nulls: it is written without definition
  /// levels, so a null would leave the page short of values.
  Never _required() => throw Failure.of(
    Kind.invalid,
    'Column ${element.name} is required but holds a null',
  );
}
