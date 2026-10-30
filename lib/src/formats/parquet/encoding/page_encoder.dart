import 'dart:typed_data';
import 'dart:math' as math;
import '../../../core/column.dart';
import '../thrift/parquet_thrift.dart' as thrift;
import 'rle_bitpacked.dart';
import 'value_encoder.dart';

/// The bytes [value] takes in a PLAIN page, estimated without encoding it:
/// strings count one byte per code unit, which is exact for ASCII.
int valueBytes(Object? value) => switch (value) {
  null => 0,
  String() => 4 + value.length,
  Uint8List() => 4 + value.length,
  List() => 4 + value.length * 8,
  _ => 8,
};

/// Encodes data into Parquet pages
class PageEncoder {
  final thrift.ParquetType type;
  final thrift.Encoding encoding;
  final int pageSize;

  /// Bit widths of the definition / repetition levels (0 = no levels).
  final int defBitWidth;
  final int repBitWidth;

  PageEncoder({
    required this.type,
    required this.encoding,
    this.pageSize = 1024 * 1024, // 1MB default
    this.defBitWidth = 0,
    this.repBitWidth = 0,
  });

  /// Encode values into pages.
  List<EncodedPage> encode(
    List<dynamic> values, {
    List<int>? defLevels,
    List<int>? repLevels,
  }) {
    if (encoding == thrift.Encoding.rleDictionary ||
        encoding == thrift.Encoding.plainDictionary) {
      return encodeDictionary(values, defLevels, repLevels);
    }
    return encodeData(values, defLevels, repLevels);
  }

  /// Encode a column as a dictionary page (PLAIN dictionary values) plus one
  /// data page of RLE/bit-packed dictionary indices, prefixed by the index bit
  /// width: the layout DictionaryDecoder expects.
  List<EncodedPage> encodeDictionary(
    List<dynamic> values,
    List<int>? defLevels,
    List<int>? repLevels,
  ) {
    final dictionary = <dynamic, int>{};
    final indices = <int>[];
    for (final value in values) {
      if (value == null) continue;
      indices.add(dictionary.putIfAbsent(value, () => dictionary.length));
    }
    final keys = dictionary.keys.toList();

    final plain = PlainEncoder(type).encode(keys);
    final dictionaryPage = EncodedPage(
      header: thrift.PageHeader()
        ..type = thrift.PageType.dictionaryPage
        ..uncompressedPageSize = plain.length
        ..compressedPageSize = plain.length
        ..dictionaryPageHeader = (thrift.DictionaryPageHeader()
          ..numValues = keys.length
          ..encoding = thrift.Encoding.plain),
      data: plain,
      isDictionary: true,
    );

    final bitWidth = keys.length <= 1 ? 0 : (keys.length - 1).bitLength;
    final buffer = BytesBuilder();
    if (repLevels != null) buffer.add(encodeLevels(repLevels, repBitWidth));
    if (defLevels != null) buffer.add(encodeLevels(defLevels, defBitWidth));
    buffer.addByte(bitWidth);
    buffer.add(RleBitpackedHybrid.encode(indices, bitWidth));
    final data = buffer.toBytes();

    final dataPage = EncodedPage(
      header: thrift.PageHeader()
        ..type = thrift.PageType.dataPage
        ..uncompressedPageSize = data.length
        ..compressedPageSize = data.length
        ..dataPageHeader = (thrift.DataPageHeader()
          ..numValues = defLevels?.length ?? values.length
          ..encoding = thrift.Encoding.rleDictionary
          ..definitionLevelEncoding = thrift.Encoding.rle
          ..repetitionLevelEncoding = thrift.Encoding.rle),
      data: data,
      isDictionary: false,
    );

    return [dictionaryPage, dataPage];
  }

  /// Encode a flat numeric column straight from its typed buffer: no boxing to
  /// `List<dynamic>`. Only PLAIN and DELTA_BINARY_PACKED are handled here (the
  /// caller keeps dictionary/nested/logical columns on the boxed [encode] path).
  /// Page boundaries and per-page `numValues` match [encodeData] exactly, so the
  /// output is interchangeable with the boxed encoder's.
  List<EncodedPage> encodeTyped(NumericVector column, {List<int>? defLevels}) {
    final count = column.length;
    if (count == 0) {
      return [_encodeTypedPage(column, 0, 0, defLevels)];
    }
    final pages = <EncodedPage>[];
    int offset = 0;
    while (offset < count) {
      int size = pageSize ~/ 8; // matches encodeData
      if (offset + size > count) size = count - offset;
      final levels = defLevels?.sublist(offset, offset + size);
      pages.add(_encodeTypedPage(column, offset, offset + size, levels));
      offset += size;
    }
    return pages;
  }

  EncodedPage _encodeTypedPage(
    NumericVector column,
    int start,
    int end,
    List<int>? defLevels,
  ) {
    final buffer = BytesBuilder();
    if (repBitWidth > 0) {
      // Flat columns never carry repetition levels; guard so a future caller
      // can't silently drop them.
      throw StateError('encodeTyped does not support repetition levels');
    }
    if (defLevels != null) buffer.add(encodeLevels(defLevels, defBitWidth));
    buffer.add(
      encoding == thrift.Encoding.deltaBinaryPacked
          ? _encodeTypedDelta(column, start, end)
          : _encodeTypedPlain(column, start, end),
    );
    final data = buffer.toBytes();
    final header = thrift.PageHeader()
      ..type = thrift.PageType.dataPage
      ..uncompressedPageSize = data.length
      ..compressedPageSize = data.length
      ..dataPageHeader = (thrift.DataPageHeader()
        ..numValues = end - start
        ..encoding = encoding
        ..definitionLevelEncoding = thrift.Encoding.rle
        ..repetitionLevelEncoding = thrift.Encoding.rle);
    return EncodedPage(header: header, data: data, isDictionary: false);
  }

  /// PLAIN-encode non-null values in `[start, end)` into one pre-sized buffer.
  /// Width comes from the physical [type] (int16/int8 columns encode as INT32),
  /// values are read unboxed from the typed buffer, written little-endian.
  Uint8List _encodeTypedPlain(NumericVector column, int start, int end) {
    final width = switch (type) {
      thrift.ParquetType.int32 || thrift.ParquetType.float => 4,
      thrift.ParquetType.int64 || thrift.ParquetType.double => 8,
      _ => 0,
    };
    final bitmap = column.nulls;
    var present = end - start;
    if (bitmap != null) {
      present = 0;
      for (int i = start; i < end; i++) {
        if ((bitmap[i >> 3] & (1 << (i & 7))) == 0) present++;
      }
    }
    final out = Uint8List(present * width);
    final view = ByteData.view(out.buffer);
    var offset = 0;
    final buffer = column.raw;
    if (type == thrift.ParquetType.int32 || type == thrift.ParquetType.int64) {
      final data = buffer as List<int>;
      final int32 = type == thrift.ParquetType.int32;
      for (int i = start; i < end; i++) {
        if (bitmap != null && (bitmap[i >> 3] & (1 << (i & 7))) != 0) continue;
        if (int32) {
          view.setInt32(offset, data[i], Endian.little);
        } else {
          view.setInt64(offset, data[i], Endian.little);
        }
        offset += width;
      }
    } else {
      final data = buffer as List<double>;
      final float32 = type == thrift.ParquetType.float;
      for (int i = start; i < end; i++) {
        if (bitmap != null && (bitmap[i >> 3] & (1 << (i & 7))) != 0) continue;
        if (float32) {
          view.setFloat32(offset, data[i], Endian.little);
        } else {
          view.setFloat64(offset, data[i], Endian.little);
        }
        offset += width;
      }
    }
    return out;
  }

  /// DELTA_BINARY_PACKED-encode non-null ints in `[start, end)`. Fills an
  /// `Int64List` from the raw buffer (unboxed) and hands it to the shared block
  /// packer, so the bytes match the boxed DELTA encoder for the same values.
  Uint8List _encodeTypedDelta(NumericVector column, int start, int end) {
    final bitmap = column.nulls;
    final data = column.raw as List<int>;
    var present = end - start;
    if (bitmap != null) {
      present = 0;
      for (int i = start; i < end; i++) {
        if ((bitmap[i >> 3] & (1 << (i & 7))) == 0) present++;
      }
    }
    final ints = Int64List(present);
    var index = 0;
    for (int i = start; i < end; i++) {
      if (bitmap != null && (bitmap[i >> 3] & (1 << (i & 7))) != 0) continue;
      ints[index++] = data[i];
    }
    return DeltaBinaryPackedEncoder(type).encodeInts(ints);
  }

  /// Encode data pages
  List<EncodedPage> encodeData(
    List<dynamic> values,
    List<int>? defLevels,
    List<int>? repLevels,
  ) {
    final pages = <EncodedPage>[];
    int offset = 0;

    if (values.isEmpty) {
      return [encodePage(const [], defLevels, repLevels)];
    }

    while (offset < values.length) {
      // A page ends at [pageSize] encoded bytes (estimated) or [pageSize] / 8
      // values, whichever comes first, so large values cannot make a page the
      // reader refuses. Nested columns end it on a record boundary.
      var end = offset;
      var bytes = 0;
      final most = math.min(values.length, offset + pageSize ~/ 8);
      while (end < most && (end == offset || bytes < pageSize)) {
        bytes += valueBytes(values[end]);
        end++;
      }
      if (repLevels != null) {
        while (end < values.length && repLevels[end] != 0) {
          end++;
        }
      }
      final size = end - offset;

      // Extract batch
      final batch = values.sublist(offset, offset + size);
      final definitions = defLevels?.sublist(offset, offset + size);
      final repetitions = repLevels?.sublist(offset, offset + size);

      // Encode page
      pages.add(encodePage(batch, definitions, repetitions));

      offset += size;
    }

    return pages;
  }

  /// Encode a single data page
  EncodedPage encodePage(
    List<dynamic> values,
    List<int>? defLevels,
    List<int>? repLevels,
  ) {
    final buffer = BytesBuilder();

    // Repetition levels, then definition levels, each as an RLE/bit-packed
    // hybrid run prefixed by its 4-byte little-endian byte length.
    if (repLevels != null) {
      buffer.add(encodeLevels(repLevels, repBitWidth));
    }
    if (defLevels != null) {
      buffer.add(encodeLevels(defLevels, defBitWidth));
    }

    // Encode values (PlainEncoder skips nulls; levels carry null positions).
    final encoder = EncoderFactory.create(encoding, type);
    buffer.add(encoder.encode(values));

    final data = buffer.toBytes();

    final header = thrift.PageHeader()
      ..type = thrift.PageType.dataPage
      ..uncompressedPageSize = data.length
      ..compressedPageSize = data.length
      ..dataPageHeader = (thrift.DataPageHeader()
        ..numValues = values.length
        ..encoding = encoding
        ..definitionLevelEncoding = thrift.Encoding.rle
        ..repetitionLevelEncoding = thrift.Encoding.rle);

    return EncodedPage(header: header, data: data, isDictionary: false);
  }

  /// Encode a level list as a length-prefixed RLE/bit-packed hybrid run.
  Uint8List encodeLevels(List<int> levels, int bitWidth) {
    final body = RleBitpackedHybrid.encode(levels, bitWidth);
    final out = BytesBuilder();
    final length = Uint8List(4);
    ByteData.view(length.buffer).setInt32(0, body.length, Endian.little);
    out.add(length);
    out.add(body);
    return out.toBytes();
  }
}

/// Represents an encoded page
class EncodedPage {
  final thrift.PageHeader header;
  final Uint8List data;
  final bool isDictionary;

  EncodedPage({
    required this.header,
    required this.data,
    required this.isDictionary,
  });
}
