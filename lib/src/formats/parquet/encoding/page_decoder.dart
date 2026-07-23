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
  DictionaryDecoder? dictDecoder;

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
  /// value buffers from it — the count is an untrusted thrift header field.
  void _guardNumValues(int n) {
    if (n < 0 || n > maxPageValues) {
      throw FormatException('Implausible Parquet page value count: $n');
    }
  }

  /// Set dictionary from dictionary page
  void setDictionary(Uint8List data, int numValues) {
    dictDecoder = DictionaryDecoder(type, schema: schema);
    dictDecoder!.setDictionary(data, numValues);
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
    final dictHeader = header.dictionaryPageHeader!;

    // Store dictionary for later use
    setDictionary(data, dictHeader.numValues);

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
    final dataHeader = header.dataPageHeader!;
    _guardNumValues(dataHeader.numValues);
    int offset = 0;

    // A level section this decoder cannot read must NOT be skipped: `offset`
    // would stay put and the level bytes would be decoded as VALUES — the same
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
      final view = ByteData.view(data.buffer, data.offsetInBytes);
      final repLength = view.getInt32(offset, Endian.little);
      offset += 4;
      if (repLength < 0 || offset + repLength > data.length) {
        throw FormatException(
          'Corrupt page: repetition-level length $repLength exceeds the page '
          '(${data.length - offset} bytes left)',
        );
      }
      if (repLength > 0) {
        final repData = Uint8List.sublistView(data, offset, offset + repLength);
        repLevels = RleBitpackedHybrid.decode(
          repData,
          repLevelBitWidth,
          hasLength: false,
          maxValues: dataHeader.numValues,
        );
        offset += repLength;
      }
    }

    List<int>? defLevels;
    if (defLevelBitWidth > 0 &&
        dataHeader.definitionLevelEncoding == thrift.Encoding.rle) {
      final view = ByteData.view(data.buffer, data.offsetInBytes);
      final defLength = view.getInt32(offset, Endian.little);
      offset += 4;
      if (defLength < 0 || offset + defLength > data.length) {
        throw FormatException(
          'Corrupt page: definition-level length $defLength exceeds the page '
          '(${data.length - offset} bytes left)',
        );
      }
      if (defLength > 0) {
        final defData = Uint8List.sublistView(data, offset, offset + defLength);
        defLevels = RleBitpackedHybrid.decode(
          defData,
          defLevelBitWidth,
          hasLength: false,
          maxValues: dataHeader.numValues,
        );
        offset += defLength;
      }
    }

    // The RLE/bit-packed decoder rounds up to whole groups, so trim padding.
    repLevels = clamp(repLevels, dataHeader.numValues);
    defLevels = clamp(defLevels, dataHeader.numValues);

    // A leaf value is present only when the definition level reaches its max.
    int present = dataHeader.numValues;
    if (defLevels != null) {
      present = defLevels.where((level) => level == maxDef).length;
    }

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
  DecodedPage _finishBoxed(PageBody b) {
    final actualValues = decodeValues(b.valueData, b.present, b.encoding);
    final values = reconstruct(actualValues, b.defLevels);
    if (tolerant) pad(values, b.numValues);
    return DecodedPage(
      values: values,
      defLevels: b.defLevels,
      repLevels: b.repLevels,
      numValues: b.numValues,
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
        // the page body actually decoded — fail with a domain error, not a raw
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
    final dataHeader = header.dataPageHeaderV2!;
    _guardNumValues(dataHeader.numValues);
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
      final repData = Uint8List.sublistView(
        data,
        offset,
        offset + dataHeader.repetitionLevelsByteLength,
      );
      repLevels = repLevelBitWidth > 0
          ? RleBitpackedHybrid.decode(
              repData,
              repLevelBitWidth,
              hasLength: false,
              maxValues: dataHeader.numValues,
            )
          : List.filled(dataHeader.numValues, 0);
      offset += dataHeader.repetitionLevelsByteLength;
    }

    List<int>? defLevels;
    if (dataHeader.definitionLevelsByteLength > 0) {
      if (offset + dataHeader.definitionLevelsByteLength > data.length) {
        throw FormatException(
          'Corrupt V2 page: definition-level length '
          '${dataHeader.definitionLevelsByteLength} exceeds the page '
          '(${data.length - offset} bytes left)',
        );
      }
      final defData = Uint8List.sublistView(
        data,
        offset,
        offset + dataHeader.definitionLevelsByteLength,
      );
      defLevels = defLevelBitWidth > 0
          ? RleBitpackedHybrid.decode(
              defData,
              defLevelBitWidth,
              hasLength: false,
              maxValues: dataHeader.numValues,
            )
          : List.filled(dataHeader.numValues, 0);
      offset += dataHeader.definitionLevelsByteLength;
    }

    repLevels = clamp(repLevels, dataHeader.numValues);
    defLevels = clamp(defLevels, dataHeader.numValues);

    return PageBody(
      defLevels: defLevels,
      repLevels: repLevels,
      valueData: Uint8List.sublistView(data, offset),
      present: dataHeader.numValues - dataHeader.numNulls,
      encoding: dataHeader.encoding,
      numValues: dataHeader.numValues,
    );
  }

  /// Parse a data page body (v1 or v2) without decoding values — used by the
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
    thrift.Encoding enc,
    List<num> out,
    int outStart,
  ) {
    if (enc == thrift.Encoding.plainDictionary ||
        enc == thrift.Encoding.rleDictionary) {
      if (dictDecoder == null) {
        throw FormatException('Dictionary encoding without dictionary page');
      }
      final indices = dictDecoder!.decodeIndices(valueData, present);
      final dict = dictDecoder!.dictionary;
      // Mirror the boxed path's validation: a corrupt index or a short index
      // run must fail as a structured FormatException, not a raw RangeError.
      if (indices.length < present) {
        throw FormatException(
          'Dictionary index run shorter than declared: '
          '${indices.length} < $present',
        );
      }
      for (int i = 0; i < present; i++) {
        final index = indices[i];
        if (index < 0 || index >= dict.length) {
          throw FormatException('Dictionary index out of bounds: $index');
        }
        out[outStart + i] = dict[index] as num;
      }
      return;
    }
    if (enc == thrift.Encoding.deltaBinaryPacked) {
      // Thread the width like the boxed path: an int32 delta chain must
      // reconstruct with 32-bit wrapping, not 64-bit. Correctness happened to
      // hold only because the sink is an Int32List that truncates — passing bits
      // makes it independent of the storage container.
      final list = DeltaBinaryPackedDecoder.decode(
        valueData,
        present,
        bits: type == thrift.ParquetType.int32 ? 32 : 64,
      );
      for (int i = 0; i < present; i++) {
        out[outStart + i] = list[i];
      }
      return;
    }
    if (enc == thrift.Encoding.plain) {
      final view = ByteData.view(
        valueData.buffer,
        valueData.offsetInBytes,
        valueData.length,
      );
      // Surface a truncated/corrupt page as a structured error rather than a raw
      // RangeError from the ByteData getters below.
      final elementSize = switch (type) {
        thrift.ParquetType.int32 || thrift.ParquetType.float => 4,
        thrift.ParquetType.int64 || thrift.ParquetType.double => 8,
        _ => 0,
      };
      if (elementSize > 0 && valueData.length < present * elementSize) {
        throw FormatException(
          'Truncated PLAIN page: $present values need ${present * elementSize} bytes, have ${valueData.length}',
        );
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
    throw UnsupportedError('typed decode for $enc');
  }

  /// Decode values based on encoding
  List<dynamic> decodeValues(
    Uint8List data,
    int numValues,
    thrift.Encoding encoding,
  ) {
    // Use dictionary decoder if available for dictionary encoding
    if ((encoding == thrift.Encoding.plainDictionary ||
            encoding == thrift.Encoding.rleDictionary) &&
        dictDecoder != null) {
      return dictDecoder!.decode(data, numValues);
    }

    // For plain dictionary without a dictionary, it's likely the encoding
    // was specified wrong or we're dealing with a plain encoding
    if (encoding == thrift.Encoding.plainDictionary && dictDecoder == null) {
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
