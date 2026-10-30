import 'dart:typed_data';
import '../thrift/parquet_thrift.dart' as thrift;
import 'value_decoder.dart';
import 'rle_bitpacked.dart';
import 'delta_binary_packed.dart';

/// A parsed page body: decoded levels plus the still-encoded value bytes.
class PageBody {
  final List<int>? defLevels;
  final List<int>? repLevels;
  final Uint8List valueData;
  final int present; // number of non-null values in valueData
  final thrift.Encoding encoding;
  final int numValues; // total slots (including nulls)

  PageBody({
    required this.defLevels,
    required this.repLevels,
    required this.valueData,
    required this.present,
    required this.encoding,
    required this.numValues,
  });
}

/// Decodes Parquet pages into values
class PageDecoder {
  /// Reject a [kind] level section encoded as anything but RLE.
  ///
  /// A zero bit width means the level is implicit (no section is written), so
  /// the encoding is irrelevant there.
  static void _requireRleLevels(
    String kind,
    int bitWidth,
    thrift.Encoding encoding,
  ) {
    if (bitWidth > 0 && encoding != thrift.Encoding.rle) {
      throw FormatException(
        'Unsupported $kind-level encoding $encoding: only RLE is supported '
        '(BIT_PACKED levels were deprecated before Parquet 1.0). Reading past '
        'them would decode the level bytes as values.',
      );
    }
  }

  final thrift.ParquetType type;
  final thrift.SchemaElement? schema;
  DictionaryDecoder? dictionary;

  // Max levels and their bit widths - calculated from schema
  final int maxDef;
  final int maxRep;
  final int defLevelBitWidth;
  final int repLevelBitWidth;

  /// When true, pages that hold fewer values than they declare are read
  /// best-effort (partial values + null padding) instead of raising. Off by
  /// default so corrupt/truncated pages fail loudly; opt in via
  /// ParquetReaderOptions.tolerateTruncatedPages for legacy files.
  final bool tolerant;

  PageDecoder({
    required this.type,
    this.schema,
    int? maxDefinitionLevel,
    int? maxRepetitionLevel,
    this.tolerant = false,
  }) : maxDef = maxDefinitionLevel ?? 1,
       maxRep = maxRepetitionLevel ?? 0,
       defLevelBitWidth = levelBitWidth(maxDefinitionLevel ?? 1),
       repLevelBitWidth = levelBitWidth(maxRepetitionLevel ?? 0);

  /// Reject a page whose declared value count is absurd before we size level or
  /// value buffers from it: the count is an untrusted thrift header field.
  void _guardNumValues(int count) {
    if (count < 0 || count > maxPageValues) {
      throw FormatException('Implausible Parquet page value count: $count');
    }
  }

  /// Definition levels of the current page, reused from page to page. Callers
  /// copy the levels they keep, so one buffer serves the whole column.
  Uint8List _levels = Uint8List(0);

  /// Decode [count] definition levels from [data] into the reused buffer.
  /// Returns a view of the levels the data held and how many reached [maxDef].
  ({Uint8List levels, int present}) _definitions(Uint8List data, int count) {
    if (_levels.length < count) _levels = Uint8List(count);
    final (:decoded, :present) = RleBitpackedHybrid.decodeLevels(
      data,
      defLevelBitWidth,
      count,
      _levels,
      maxDef,
    );
    return (
      levels: Uint8List.sublistView(_levels, 0, decoded),
      present: present,
    );
  }

  /// Set dictionary from dictionary page
  void setDictionary(Uint8List data, int numValues) {
    dictionary = DictionaryDecoder(type, schema: schema);
    dictionary!.setDictionary(data, numValues);
  }

  /// Decode a data page
  DecodedPage decode(thrift.PageHeader header, Uint8List data) {
    switch (header.type) {
      case thrift.PageType.dictionaryPage:
        return decodeDictionary(header, data);
      case thrift.PageType.dataPage:
        return decodeDataV1(header, data);
      case thrift.PageType.dataPageV2:
        return decodeDataV2(header, data);
      case thrift.PageType.indexPage:
        // Index pages don't contain values, skip them
        return DecodedPage(
          values: [],
          defLevels: null,
          repLevels: null,
          numValues: 0,
        );
    }
  }

  /// Decode dictionary page
  DecodedPage decodeDictionary(thrift.PageHeader header, Uint8List data) {
    final dictionaryHeader =
        header.dictionaryPageHeader ??
        (throw const FormatException('Dictionary page without its header'));

    // Store dictionary for later use
    setDictionary(data, dictionaryHeader.numValues);

    return DecodedPage(
      values: [],
      defLevels: null,
      repLevels: null,
      numValues: 0,
    );
  }

  /// Decode data page v1
  DecodedPage decodeDataV1(thrift.PageHeader header, Uint8List data) =>
      _finishBoxed(_parseV1(header, data));

  /// Parse the v1 page body: repetition/definition levels (each length-prefixed)
  /// then the (still-encoded) value bytes, without decoding the values.
  PageBody _parseV1(thrift.PageHeader header, Uint8List data) {
    final dataHeader =
        header.dataPageHeader ??
        (throw const FormatException('Data page without its header'));
    _guardNumValues(dataHeader.numValues);
    int offset = 0;

    // A level section this decoder cannot read must NOT be skipped: `offset`
    // would stay put and the level bytes would be decoded as VALUES: the same
    // fabricated-data failure as writing levels the reader never reads. Only RLE
    // is supported; BIT_PACKED levels (deprecated, pre-1.0 parquet-mr) are
    // rejected rather than silently mis-decoded.
    _requireRleLevels(
      'repetition',
      repLevelBitWidth,
      dataHeader.repetitionLevelEncoding,
    );
    _requireRleLevels(
      'definition',
      defLevelBitWidth,
      dataHeader.definitionLevelEncoding,
    );

    List<int>? repLevels;
    if (repLevelBitWidth > 0 &&
        dataHeader.repetitionLevelEncoding == thrift.Encoding.rle) {
      if (offset + 4 > data.length) {
        throw FormatException(
          'Corrupt page: no room for the repetition-level length '
          '(${data.length - offset} bytes left)',
        );
      }
      final view = ByteData.view(data.buffer, data.offsetInBytes);
      final length = view.getInt32(offset, Endian.little);
      offset += 4;
      if (length < 0 || offset + length > data.length) {
        throw FormatException(
          'Corrupt page: repetition-level length $length exceeds the page '
          '(${data.length - offset} bytes left)',
        );
      }
      if (length > 0) {
        final bytes = Uint8List.sublistView(data, offset, offset + length);
        repLevels = RleBitpackedHybrid.decode(
          bytes,
          repLevelBitWidth,
          hasLength: false,
          maxValues: dataHeader.numValues,
        );
        offset += length;
      }
    }

    List<int>? defLevels;
    // A leaf value is present only when the definition level reaches its max.
    int present = dataHeader.numValues;
    if (defLevelBitWidth > 0 &&
        dataHeader.definitionLevelEncoding == thrift.Encoding.rle) {
      if (offset + 4 > data.length) {
        throw FormatException(
          'Corrupt page: no room for the definition-level length '
          '(${data.length - offset} bytes left)',
        );
      }
      final view = ByteData.view(data.buffer, data.offsetInBytes);
      final length = view.getInt32(offset, Endian.little);
      offset += 4;
      if (length < 0 || offset + length > data.length) {
        throw FormatException(
          'Corrupt page: definition-level length $length exceeds the page '
          '(${data.length - offset} bytes left)',
        );
      }
      if (length > 0) {
        final bytes = Uint8List.sublistView(data, offset, offset + length);
        if (defLevelBitWidth <= 8) {
          final decoded = _definitions(bytes, dataHeader.numValues);
          defLevels = decoded.levels;
          present = decoded.present;
        } else {
          defLevels = clamp(
            RleBitpackedHybrid.decode(
              bytes,
              defLevelBitWidth,
              hasLength: false,
              maxValues: dataHeader.numValues,
            ),
            dataHeader.numValues,
          );
          present = defLevels!.where((level) => level == maxDef).length;
        }
        offset += length;
      }
    }

    // The RLE/bit-packed decoder rounds up to whole groups, so trim padding.
    repLevels = clamp(repLevels, dataHeader.numValues);

    return PageBody(
      defLevels: defLevels,
      repLevels: repLevels,
      valueData: Uint8List.sublistView(data, offset),
      present: present,
      encoding: dataHeader.encoding,
      numValues: dataHeader.numValues,
    );
  }

  /// Boxed finish shared by v1/v2: decode values, reconstruct nulls, pad.
  DecodedPage _finishBoxed(PageBody body) {
    final decoded = decodeValues(body.valueData, body.present, body.encoding);
    final values = reconstruct(decoded, body.defLevels);
    if (tolerant) pad(values, body.numValues);
    return DecodedPage(
      values: values,
      defLevels: body.defLevels,
      repLevels: body.repLevels,
      numValues: body.numValues,
    );
  }

  /// Trim a decoded level list to at most [count] entries.
  List<int>? clamp(List<int>? levels, int count) =>
      (levels != null && levels.length > count)
      ? levels.sublist(0, count)
      : levels;

  /// Pad [values] with trailing nulls up to [count]. Some legacy pages declare
  /// more values than they store without definition levels to place the nulls.
  void pad(List<dynamic> values, int count) {
    if (values.length < count) {
      values.addAll(List.filled(count - values.length, null));
    }
  }

  /// Reconstruct a value list, inserting nulls where the definition level is
  /// below its maximum.
  List<dynamic> reconstruct(List<dynamic> present, List<int>? defLevels) {
    if (defLevels == null) {
      return present;
    }
    final values = <dynamic>[];
    int index = 0;
    for (final level in defLevels) {
      if (level == maxDef) {
        // A corrupt page whose definition levels claim more present values than
        // the page body actually decoded: fail with a domain error, not a raw
        // RangeError (which reads as an internal VM bug).
        if (index >= present.length) {
          throw FormatException(
            'Corrupt page: definition levels claim more present values '
            '(> $index) than the ${present.length} decoded',
          );
        }
        values.add(present[index]);
        index++;
      } else {
        values.add(null);
      }
    }
    return values;
  }

  /// Decode data page v2
  DecodedPage decodeDataV2(thrift.PageHeader header, Uint8List data) =>
      _finishBoxed(_parseV2(header, data));

  /// Parse the v2 page body: repetition/definition levels (byte-length framed,
  /// no 4-byte prefix) then the value bytes, without decoding the values.
  PageBody _parseV2(thrift.PageHeader header, Uint8List data) {
    final dataHeader =
        header.dataPageHeaderV2 ??
        (throw const FormatException('V2 data page without its header'));
    _guardNumValues(dataHeader.numValues);
    // Header fields are untrusted: a negative null count would inflate the
    // present count into a multi-gigabyte value buffer.
    if (dataHeader.numNulls < 0 ||
        dataHeader.numNulls > dataHeader.numValues ||
        dataHeader.repetitionLevelsByteLength < 0 ||
        dataHeader.definitionLevelsByteLength < 0 ||
        // Nulls with no definition levels: nothing says which values are
        // missing, and the typed paths would read their slots as zeros.
        (dataHeader.numNulls > 0 &&
            dataHeader.definitionLevelsByteLength == 0)) {
      throw FormatException(
        'Corrupt V2 page: ${dataHeader.numNulls} nulls of '
        '${dataHeader.numValues} values, level lengths '
        '${dataHeader.repetitionLevelsByteLength}/'
        '${dataHeader.definitionLevelsByteLength}',
      );
    }
    int offset = 0;

    List<int>? repLevels;
    if (dataHeader.repetitionLevelsByteLength > 0) {
      // The level byte-lengths come from the (untrusted) page header; bound them
      // against the page before slicing, like _parseV1, so a corrupt V2 page is
      // a domain FormatException rather than a raw RangeError.
      if (offset + dataHeader.repetitionLevelsByteLength > data.length) {
        throw FormatException(
          'Corrupt V2 page: repetition-level length '
          '${dataHeader.repetitionLevelsByteLength} exceeds the page '
          '(${data.length - offset} bytes left)',
        );
      }
      final bytes = Uint8List.sublistView(
        data,
        offset,
        offset + dataHeader.repetitionLevelsByteLength,
      );
      repLevels = repLevelBitWidth > 0
          ? RleBitpackedHybrid.decode(
              bytes,
              repLevelBitWidth,
              hasLength: false,
              maxValues: dataHeader.numValues,
            )
          : List.filled(dataHeader.numValues, 0);
      offset += dataHeader.repetitionLevelsByteLength;
    }

    List<int>? defLevels;
    // Count present values from the levels, as V1 does, so the value count
    // always agrees with the levels the reader scatters by.
    var present = dataHeader.numValues - dataHeader.numNulls;
    if (dataHeader.definitionLevelsByteLength > 0) {
      if (offset + dataHeader.definitionLevelsByteLength > data.length) {
        throw FormatException(
          'Corrupt V2 page: definition-level length '
          '${dataHeader.definitionLevelsByteLength} exceeds the page '
          '(${data.length - offset} bytes left)',
        );
      }
      final bytes = Uint8List.sublistView(
        data,
        offset,
        offset + dataHeader.definitionLevelsByteLength,
      );
      if (defLevelBitWidth > 0 && defLevelBitWidth <= 8) {
        final decoded = _definitions(bytes, dataHeader.numValues);
        defLevels = decoded.levels;
        present = decoded.present;
      } else {
        defLevels = clamp(
          defLevelBitWidth > 0
              ? RleBitpackedHybrid.decode(
                  bytes,
                  defLevelBitWidth,
                  hasLength: false,
                  maxValues: dataHeader.numValues,
                )
              : List.filled(dataHeader.numValues, 0),
          dataHeader.numValues,
        );
        present = defLevels!.where((level) => level == maxDef).length;
      }
      offset += dataHeader.definitionLevelsByteLength;
    }

    repLevels = clamp(repLevels, dataHeader.numValues);

    return PageBody(
      defLevels: defLevels,
      repLevels: repLevels,
      valueData: Uint8List.sublistView(data, offset),
      present: present,
      encoding: dataHeader.encoding,
      numValues: dataHeader.numValues,
    );
  }

  /// Parse a data page body (v1 or v2) without decoding values: used by the
  /// typed fast path. Not valid for dictionary/index pages.
  PageBody parseBody(thrift.PageHeader header, Uint8List data) {
    switch (header.type) {
      case thrift.PageType.dataPage:
        return _parseV1(header, data);
      case thrift.PageType.dataPageV2:
        return _parseV2(header, data);
      default:
        throw FormatException('parseBody on ${header.type}');
    }
  }

  /// Decode [present] values of a numeric column directly into typed buffer
  /// [out] at [outStart] (no boxing). Supports PLAIN, dictionary and
  /// DELTA_BINARY_PACKED; throws for anything else so callers can fall back.
  void decodeValuesTyped(
    Uint8List valueData,
    int present,
    thrift.Encoding encoding,
    List<num> out,
    int outStart,
  ) {
    if (type == thrift.ParquetType.boolean) {
      _booleans(valueData, present, encoding, out as Uint8List, outStart);
      return;
    }
    if (encoding == thrift.Encoding.plainDictionary ||
        encoding == thrift.Encoding.rleDictionary) {
      if (dictionary == null) {
        throw FormatException('Dictionary encoding without dictionary page');
      }
      if (_indices.length < present) _indices = Int32List(present);
      final decoded = dictionary!.decodeIndicesInto(
        valueData,
        present,
        _indices,
      );
      // Mirror the boxed path's validation: a corrupt index or a short index
      // run must fail as a structured FormatException, not a raw RangeError.
      if (decoded < present) {
        throw FormatException(
          'Dictionary index run shorter than declared: $decoded < $present',
        );
      }
      final typed = dictionary!.typed;
      if (typed != null) {
        _gather(_indices, present, typed, out, outStart);
        return;
      }
      final boxed = dictionary!.dictionary;
      for (int i = 0; i < present; i++) {
        final index = _indices[i];
        if (index < 0 || index >= boxed.length) {
          throw FormatException('Dictionary index out of bounds: $index');
        }
        out[outStart + i] = boxed[index] as num;
      }
      return;
    }
    if (encoding == thrift.Encoding.deltaBinaryPacked) {
      // Thread the width like the boxed path: an int32 delta chain must
      // reconstruct with 32-bit wrapping, not 64-bit. Correctness happened to
      // hold only because the sink is an Int32List that truncates: passing bits
      // makes it independent of the storage container.
      final bits = type == thrift.ParquetType.int32 ? 32 : 64;
      final direct = out is Int64List;
      if (!direct && _deltas.length < present) _deltas = Int64List(present);
      final target = direct ? out : _deltas;
      final start = direct ? outStart : 0;
      final decoded = DeltaBinaryPackedDecoder.decodeInto(
        valueData,
        present,
        target,
        start,
        bits: bits,
      );
      if (decoded < present) {
        throw FormatException(
          'DELTA_BINARY_PACKED run shorter than declared: $decoded < $present',
        );
      }
      if (!direct) out.setRange(outStart, outStart + present, _deltas);
      return;
    }
    if (encoding == thrift.Encoding.plain) {
      final view = ByteData.view(
        valueData.buffer,
        valueData.offsetInBytes,
        valueData.length,
      );
      // Surface a truncated/corrupt page as a structured error rather than a raw
      // RangeError from the ByteData getters below.
      final width = switch (type) {
        thrift.ParquetType.int32 || thrift.ParquetType.float => 4,
        thrift.ParquetType.int64 || thrift.ParquetType.double => 8,
        _ => 0,
      };
      if (width > 0 && valueData.length < present * width) {
        throw FormatException(
          'Truncated PLAIN page: $present values need ${present * width} bytes, have ${valueData.length}',
        );
      }
      // PLAIN is little-endian fixed width: on a little-endian host it is
      // already the typed buffer's layout, so copy the bytes in one call.
      if (Endian.host == Endian.little &&
          out is TypedData &&
          (out as TypedData).elementSizeInBytes == width &&
          _matches(out, type)) {
        final target = out as TypedData;
        target.buffer
            .asUint8List(
              target.offsetInBytes + outStart * width,
              present * width,
            )
            .setRange(0, present * width, valueData);
        return;
      }
      switch (type) {
        case thrift.ParquetType.int32:
          for (int i = 0; i < present; i++) {
            out[outStart + i] = view.getInt32(i * 4, Endian.little);
          }
          return;
        case thrift.ParquetType.int64:
          for (int i = 0; i < present; i++) {
            out[outStart + i] = view.getInt64(i * 8, Endian.little);
          }
          return;
        case thrift.ParquetType.float:
          for (int i = 0; i < present; i++) {
            out[outStart + i] = view.getFloat32(i * 4, Endian.little);
          }
          return;
        case thrift.ParquetType.double:
          for (int i = 0; i < present; i++) {
            out[outStart + i] = view.getFloat64(i * 8, Endian.little);
          }
          return;
        default:
          throw UnsupportedError('typed PLAIN decode for $type');
      }
    }
    if (encoding == thrift.Encoding.byteStreamSplit) {
      ByteStreamSplitDecoder(
        type,
      ).decodeInto(valueData, present, out, outStart);
      return;
    }
    throw UnsupportedError('typed decode for $encoding');
  }

  /// Decode [present] booleans into [out] from [start], one byte (0 or 1) a
  /// value: PLAIN bit-packs them, RLE is the hybrid of width 1 after a 4-byte
  /// length.
  void _booleans(
    Uint8List data,
    int present,
    thrift.Encoding encoding,
    Uint8List out,
    int start,
  ) {
    if (encoding == thrift.Encoding.plain) {
      if (data.length * 8 < present) {
        throw FormatException(
          'Truncated PLAIN page: $present booleans need '
          '${(present + 7) >> 3} bytes, have ${data.length}',
        );
      }
      for (var i = 0; i < present; i++) {
        out[start + i] = (data[i >> 3] >> (i & 7)) & 1;
      }
      return;
    }
    if (encoding != thrift.Encoding.rle) {
      throw UnsupportedError('typed boolean decode for $encoding');
    }
    final size = data.length < 4
        ? -1
        : ByteData.sublistView(data).getUint32(0, Endian.little);
    if (size < 0 || size > data.length - 4) {
      throw FormatException(
        'RLE boolean run of $size bytes overruns its ${data.length}-byte page',
      );
    }
    if (_indices.length < present) _indices = Int32List(present);
    final decoded = RleBitpackedHybrid.decodeInto(
      Uint8List.sublistView(data, 4, 4 + size),
      1,
      present,
      _indices,
    );
    if (decoded < present) {
      throw FormatException(
        'RLE boolean run shorter than declared: $decoded < $present',
      );
    }
    for (var i = 0; i < present; i++) {
      out[start + i] = _indices[i] != 0 ? 1 : 0;
    }
  }

  /// Dictionary indices of the current page, reused from page to page.
  Int32List _indices = Int32List(0);

  /// DELTA_BINARY_PACKED values of the current page when the output is not an
  /// Int64List, reused from page to page.
  Int64List _deltas = Int64List(0);

  /// Copy `dictionary[indices[i]]` to `out[start + i]` for the first [count]
  /// indices, rejecting an index outside [dictionary]. Specialized per typed list so
  /// the loop reads and writes unboxed values.
  static void _gather(
    Int32List indices,
    int count,
    List<num> dictionary,
    List<num> out,
    int start,
  ) {
    final size = dictionary.length;
    for (var i = 0; i < count; i++) {
      final index = indices[i];
      if (index < 0 || index >= size) {
        throw FormatException('Dictionary index out of bounds: $index');
      }
    }
    switch ((dictionary, out)) {
      case (final Int64List from, final Int64List to):
        for (var i = 0; i < count; i++) {
          to[start + i] = from[indices[i]];
        }
      case (final Float64List from, final Float64List to):
        for (var i = 0; i < count; i++) {
          to[start + i] = from[indices[i]];
        }
      case (final Int32List from, final Int32List to):
        for (var i = 0; i < count; i++) {
          to[start + i] = from[indices[i]];
        }
      case (final Float32List from, final Float32List to):
        for (var i = 0; i < count; i++) {
          to[start + i] = from[indices[i]];
        }
      default:
        for (var i = 0; i < count; i++) {
          out[start + i] = dictionary[indices[i]];
        }
    }
  }

  /// Whether [out] holds [type]'s values in Parquet's own representation, so a
  /// byte copy is a faithful decode (an int32 page into an Int32List, not into a
  /// Float32List of the same width).
  static bool _matches(List<num> out, thrift.ParquetType type) =>
      switch (type) {
        thrift.ParquetType.int32 => out is Int32List,
        thrift.ParquetType.int64 => out is Int64List,
        thrift.ParquetType.float => out is Float32List,
        thrift.ParquetType.double => out is Float64List,
        _ => false,
      };

  /// Decode values based on encoding
  List<dynamic> decodeValues(
    Uint8List data,
    int numValues,
    thrift.Encoding encoding,
  ) {
    // Use dictionary decoder if available for dictionary encoding
    if ((encoding == thrift.Encoding.plainDictionary ||
            encoding == thrift.Encoding.rleDictionary) &&
        dictionary != null) {
      return dictionary!.decode(data, numValues);
    }

    // For plain dictionary without a dictionary, it's likely the encoding
    // was specified wrong or we're dealing with a plain encoding
    if (encoding == thrift.Encoding.plainDictionary && dictionary == null) {
      // This shouldn't happen in a valid file
      throw FormatException('Dictionary encoding without dictionary page');
    }

    // Otherwise use appropriate decoder
    final decoder = DecoderFactory.create(
      encoding,
      type,
      schema: schema,
      tolerant: tolerant,
    );
    return decoder.decode(data, numValues);
  }
}

/// Decoded page data
class DecodedPage {
  final List<dynamic> values;
  final List<int>? defLevels;
  final List<int>? repLevels;
  final int numValues;

  DecodedPage({
    required this.values,
    required this.defLevels,
    required this.repLevels,
    required this.numValues,
  });
}
