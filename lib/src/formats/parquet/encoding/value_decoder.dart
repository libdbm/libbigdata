import 'dart:convert';
import 'dart:typed_data';
import '../thrift/parquet_thrift.dart' as thrift;
import 'rle_bitpacked.dart';
import 'delta_binary_packed.dart';
import 'delta_byte_array.dart';

/// Whether [schema] annotates a BYTE_ARRAY as UTF8 text: UTF8, or JSON and
/// ENUM, which the spec defines as UTF8 too.
///
/// The ONE place this is decided. A plain BYTE_ARRAY is binary and a DECIMAL
/// holds two's-complement bytes, so neither may go through utf8, and a column
/// that IS text must not come back as bytes under a `string` dtype, which
/// nothing downstream re-checks (validateColumns compares dtypes, not the values
/// behind them).
bool isUtf8(thrift.SchemaElement? schema) => switch (schema) {
  null => false,
  _ =>
    const {
          thrift.ConvertedType.utf8,
          thrift.ConvertedType.json,
          thrift.ConvertedType.enum_,
        }.contains(schema.convertedType) ||
        schema.logicalType?.string != null ||
        schema.logicalType?.json != null ||
        schema.logicalType?.enum_ != null,
};

/// [bytes] as the value [schema] calls for: text, or the raw bytes.
Object byteArrayValue(Uint8List bytes, thrift.SchemaElement? schema) =>
    isUtf8(schema) ? utf8.decode(bytes, allowMalformed: true) : bytes;

/// Base class for value decoders
abstract class ValueDecoder {
  /// Decode bytes to values
  List<dynamic> decode(Uint8List data, int count);
}

/// Plain decoding for values
class PlainDecoder extends ValueDecoder {
  final thrift.ParquetType type;
  final thrift.SchemaElement? schema;

  /// When false (the default), a page that holds fewer bytes than [count]
  /// requires raises a [FormatException]; when true the partial values decoded
  /// so far are returned (for spec-violating legacy files).
  final bool tolerant;

  PlainDecoder(this.type, {this.schema, this.tolerant = false});

  /// Either return the partial [values] (tolerant) or fail hard (strict).
  List<dynamic> _short(List<dynamic> values, int count) {
    if (tolerant) return values;
    throw FormatException(
      'Truncated page: expected $count values but ran out of data at ${values.length}',
    );
  }

  @override
  List<dynamic> decode(Uint8List data, int count) {
    final values = [];
    final view = ByteData.view(data.buffer, data.offsetInBytes);
    int offset = 0;

    // Special handling for boolean - they are bit-packed
    if (type == thrift.ParquetType.boolean) {
      int byteIndex = 0;
      int bitIndex = 0;

      for (int i = 0; i < count; i++) {
        if (byteIndex >= data.length) {
          return _short(values, count);
        }

        // Get the bit value
        final bit = (data[byteIndex] >> bitIndex) & 1;
        values.add(bit == 1);

        // Move to next bit
        bitIndex++;
        if (bitIndex == 8) {
          bitIndex = 0;
          byteIndex++;
        }
      }

      return values;
    }

    // Decode `count` fixed/variable-width values. A page that runs out of bytes
    // early is corrupt/truncated and fails hard unless tolerant mode is on.
    for (int i = 0; i < count; i++) {
      final remaining = data.length - offset;
      switch (type) {
        case thrift.ParquetType.boolean:
          // Boolean handled above
          throw StateError('Boolean should have been handled above');

        case thrift.ParquetType.int32:
          if (remaining < 4) return _short(values, count);
          values.add(view.getInt32(offset, Endian.little));
          offset += 4;
          break;

        case thrift.ParquetType.int64:
          if (remaining < 8) return _short(values, count);
          values.add(view.getInt64(offset, Endian.little));
          offset += 8;
          break;

        case thrift.ParquetType.float:
          if (remaining < 4) return _short(values, count);
          values.add(view.getFloat32(offset, Endian.little));
          offset += 4;
          break;

        case thrift.ParquetType.double:
          if (remaining < 8) return _short(values, count);
          values.add(view.getFloat64(offset, Endian.little));
          offset += 8;
          break;

        case thrift.ParquetType.byteArray:
          // Read length prefix
          if (remaining < 4) return _short(values, count);
          final length = view.getInt32(offset, Endian.little);
          // The prefix is read SIGNED, so a negative length slips past the
          // bounds check below (`bytesLeft < -1` is false) and reaches
          // `sublist(offset, offset - 1)`: a raw RangeError from deep inside
          // the decoder. It is malformed input, not truncation, so `tolerant`
          // must not swallow it either. (DeltaLengthByteArrayDecoder already
          // rejects this; the PLAIN path was missed.)
          if (length < 0) {
            throw FormatException(
              'Invalid PLAIN BYTE_ARRAY length $length at byte $offset: a '
              'length prefix cannot be negative',
            );
          }
          if (data.length - offset - 4 < length) return _short(values, count);
          offset += 4;
          final bytes = data.sublist(offset, offset + length);
          values.add(byteArrayValue(bytes, schema));
          offset += length;
          break;

        case thrift.ParquetType.fixedLenByteArray:
          // Get fixed length from schema
          final length = schema?.typeLength ?? 0;
          if (length <= 0) {
            throw FormatException(
              'Fixed length byte array requires type_length in schema',
            );
          }
          if (remaining < length) return _short(values, count);
          values.add(data.sublist(offset, offset + length));
          offset += length;
          break;

        case thrift.ParquetType.int96:
          // 12 bytes for INT96
          if (remaining < 12) return _short(values, count);
          values.add(data.sublist(offset, offset + 12));
          offset += 12;
          break;
      }
    }

    return values;
  }
}

/// Dictionary decoding for values
class DictionaryDecoder extends ValueDecoder {
  final thrift.ParquetType type;
  final thrift.SchemaElement? schema;
  List<dynamic> dictionary = [];

  /// [dictionary] as a typed list for the numeric physical types (int32,
  /// int64, float, double), so a typed read gathers values without boxing.
  /// Null for every other type.
  List<num>? typed;

  DictionaryDecoder(this.type, {this.schema});

  /// Set dictionary from dictionary page
  void setDictionary(Uint8List data, int count) {
    final decoder = PlainDecoder(type, schema: schema);
    dictionary = decoder.decode(data, count);
    typed = switch (type) {
      thrift.ParquetType.int32 => Int32List.fromList(dictionary.cast<int>()),
      thrift.ParquetType.int64 => Int64List.fromList(dictionary.cast<int>()),
      thrift.ParquetType.float => Float32List.fromList(
        dictionary.cast<double>(),
      ),
      thrift.ParquetType.double => Float64List.fromList(
        dictionary.cast<double>(),
      ),
      _ => null,
    };
  }

  @override
  List<dynamic> decode(Uint8List data, int count) {
    // Decode indices
    final indices = decodeIndices(data, count);

    // Map indices to dictionary values
    final values = [];
    for (final index in indices) {
      if (index >= 0 && index < dictionary.length) {
        values.add(dictionary[index]);
      } else {
        throw FormatException('Dictionary index out of bounds: $index');
      }
    }

    return values;
  }

  /// Decode up to [count] indices into [into] (index 0 on), returning how many
  /// the page held. The typed counterpart of [decodeIndices].
  int decodeIndicesInto(Uint8List data, int count, Int32List into) {
    if (data.isEmpty) return 0;
    final bitWidth = data[0];
    if (bitWidth > 32) {
      throw FormatException(
        'Corrupt dictionary page: index bit width $bitWidth exceeds 32',
      );
    }
    return RleBitpackedHybrid.decodeInto(
      Uint8List.sublistView(data, 1),
      bitWidth,
      count,
      into,
    );
  }

  /// Decode indices from RLE/Bit-packed hybrid
  List<int> decodeIndices(Uint8List data, int count) {
    // Dictionary indices are encoded using RLE/Bit-packed hybrid
    // First byte is the bit width
    if (data.isEmpty) {
      return [];
    }

    final bitWidth = data[0];
    // A byte can claim up to 255; a dictionary index wider than 32 bits is
    // corrupt. RleBitpackedHybrid would throw a raw ArgumentError: corrupt
    // Parquet input surfaces as FormatException everywhere else.
    if (bitWidth > 32) {
      throw FormatException(
        'Corrupt dictionary page: index bit width $bitWidth exceeds 32',
      );
    }
    final encoded = data.sublist(1);

    // A final run can legitimately overrun [count], so decoding stops at
    // [count] (cutting that run) instead of rejecting it or expanding it.
    return RleBitpackedHybrid.decode(
      encoded,
      bitWidth,
      hasLength:
          false, // No length prefix for dictionary indices in data page v1
      maxValues: maxPageValues,
      limit: count,
    );
  }
}

/// RLE decoding for boolean values
///
/// Boolean values use the RLE/bit-packed hybrid with bit width 1, prefixed by a
/// 4-byte length (in both v1 and v2 data pages).
class RleDecoder extends ValueDecoder {
  @override
  List<dynamic> decode(Uint8List data, int count) {
    final bits = RleBitpackedHybrid.decode(
      data,
      1,
      hasLength: true,
      maxValues: maxPageValues,
      limit: count,
    );
    return [for (int i = 0; i < count && i < bits.length; i++) bits[i] != 0];
  }
}

/// Delta Binary Packed decoder wrapper
class DeltaBinaryPackedDecoderWrapper extends ValueDecoder {
  final thrift.ParquetType type;
  DeltaBinaryPackedDecoderWrapper([this.type = thrift.ParquetType.int64]);

  @override
  List<dynamic> decode(Uint8List data, int count) =>
      DeltaBinaryPackedDecoder.decode(
        data,
        count,
        bits: type == thrift.ParquetType.int32 ? 32 : 64,
      );
}

/// Delta Byte Array decoder wrapper
class DeltaByteArrayDecoderWrapper extends ValueDecoder {
  final thrift.SchemaElement? schema;
  DeltaByteArrayDecoderWrapper({this.schema});

  @override
  List<dynamic> decode(Uint8List data, int count) => [
    for (final bytes in DeltaByteArrayDecoder.decode(data, count))
      byteArrayValue(bytes, schema),
  ];
}

/// Delta Length Byte Array decoder wrapper
class DeltaLengthByteArrayDecoderWrapper extends ValueDecoder {
  final thrift.SchemaElement? schema;
  DeltaLengthByteArrayDecoderWrapper({this.schema});

  @override
  List<dynamic> decode(Uint8List data, int count) => [
    for (final bytes in DeltaLengthByteArrayDecoder.decode(data, count))
      byteArrayValue(bytes, schema),
  ];
}

/// Factory for creating decoders
class DecoderFactory {
  /// Create decoder for the given encoding and type
  static ValueDecoder create(
    thrift.Encoding encoding,
    thrift.ParquetType type, {
    thrift.SchemaElement? schema,
    bool tolerant = false,
  }) {
    switch (encoding) {
      case thrift.Encoding.plain:
        return PlainDecoder(type, schema: schema, tolerant: tolerant);
      case thrift.Encoding.plainDictionary:
      case thrift.Encoding.rleDictionary:
        return DictionaryDecoder(type, schema: schema);
      case thrift.Encoding.rle:
        return RleDecoder();
      case thrift.Encoding.deltaBinaryPacked:
        return DeltaBinaryPackedDecoderWrapper(type);
      case thrift.Encoding.deltaByteArray:
        return DeltaByteArrayDecoderWrapper(schema: schema);
      case thrift.Encoding.deltaLengthByteArray:
        return DeltaLengthByteArrayDecoderWrapper(schema: schema);
      case thrift.Encoding.byteStreamSplit:
        return ByteStreamSplitDecoder(type, schema: schema);
      default:
        // BIT_PACKED (levels only) and anything newer: decoding these bytes
        // as PLAIN would return garbage values.
        throw UnsupportedError(
          'Parquet value encoding ${encoding.name} is not supported',
        );
    }
  }
}

/// BYTE_STREAM_SPLIT (Parquet encoding 9): each value's bytes are stored in
/// W separate column-major planes (W = the fixed type width), so value `i`'s
/// byte `j` lives at `data[j*count + i]`. Reassembles them little-endian into
/// the typed value. Used by Spark 3.4+/pyarrow for floats/doubles (and, per the
/// 2.10 spec, int32/int64/FLBA).
class ByteStreamSplitDecoder extends ValueDecoder {
  final thrift.ParquetType type;
  final thrift.SchemaElement? schema;
  ByteStreamSplitDecoder(this.type, {this.schema});

  int get _width => switch (type) {
    thrift.ParquetType.float || thrift.ParquetType.int32 => 4,
    thrift.ParquetType.double || thrift.ParquetType.int64 => 8,
    thrift.ParquetType.fixedLenByteArray => schema?.typeLength ?? 0,
    _ => throw FormatException(
      'BYTE_STREAM_SPLIT unsupported for physical type $type',
    ),
  };

  /// The type width, after checking [data] holds [count] values of it.
  int _check(Uint8List data, int count) {
    final width = _width;
    if (width <= 0) {
      throw const FormatException('BYTE_STREAM_SPLIT requires a type width');
    }
    if (data.length < width * count) {
      throw FormatException(
        'BYTE_STREAM_SPLIT truncated: need ${width * count} bytes, got ${data.length}',
      );
    }
    return width;
  }

  /// Decode [count] values into [out] from [start] by writing each byte plane
  /// straight into [out]'s bytes. [out] must be the typed list of this type
  /// (Int32List for INT32, Float64List for DOUBLE, ...) on a little-endian
  /// host; anything else throws [UnsupportedError].
  void decodeInto(Uint8List data, int count, List<num> out, int start) {
    final width = _check(data, count);
    final matches = switch (type) {
      thrift.ParquetType.int32 => out is Int32List,
      thrift.ParquetType.int64 => out is Int64List,
      thrift.ParquetType.float => out is Float32List,
      thrift.ParquetType.double => out is Float64List,
      _ => false,
    };
    if (!matches || Endian.host != Endian.little) {
      throw UnsupportedError('typed BYTE_STREAM_SPLIT decode for $type');
    }
    final target = out as TypedData;
    final bytes = target.buffer.asUint8List(
      target.offsetInBytes + start * width,
      count * width,
    );
    for (var j = 0; j < width; j++) {
      final plane = j * count;
      for (var i = 0; i < count; i++) {
        bytes[i * width + j] = data[plane + i];
      }
    }
  }

  @override
  List<dynamic> decode(Uint8List data, int count) {
    final width = _check(data, count);
    final out = List<dynamic>.filled(count, null);
    final scratch = Uint8List(width);
    final view = ByteData.view(scratch.buffer);
    for (int i = 0; i < count; i++) {
      for (int j = 0; j < width; j++) {
        scratch[j] = data[j * count + i];
      }
      out[i] = switch (type) {
        thrift.ParquetType.float => view.getFloat32(0, Endian.little),
        thrift.ParquetType.double => view.getFloat64(0, Endian.little),
        thrift.ParquetType.int32 => view.getInt32(0, Endian.little),
        thrift.ParquetType.int64 => view.getInt64(0, Endian.little),
        _ => Uint8List.fromList(scratch), // FLBA: the raw W bytes
      };
    }
    return out;
  }
}
