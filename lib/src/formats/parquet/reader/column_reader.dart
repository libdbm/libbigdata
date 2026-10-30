import 'dart:convert';
import 'dart:typed_data';
import '../thrift/parquet_thrift.dart' as thrift;
import '../parquet_reader.dart';
import '../encoding/byte_reader.dart';
import '../encoding/delta_binary_packed.dart';
import '../encoding/page_decoder.dart';
import '../encoding/value_decoder.dart' show DictionaryDecoder, isUtf8;
import '../schema/schema_levels.dart';
import '../schema/schema_converter.dart';
import '../../../core/column.dart';
import '../../../core/conversions.dart';
import '../../../core/data_type.dart';
import '../../../core/encoding.dart' show validUtf8;
import 'file_reader.dart';
import 'page_reader.dart';
import 'dremel_assembler.dart' show RawLeaf;

/// Initial/step size (values) for the incremental typed-buffer allocation, so a
/// large or adversarial footer numValues never forces a huge upfront alloc.
const int _step = 1 << 16; // 64K

/// A typed-buffer kind for the numeric fast path. Booleans decode one byte a
/// value and are packed into a [BoolColumn] when taken.
class _TypedKind {
  final DataType type;
  final TypedData Function(int length) make;
  const _TypedKind(this.type, this.make);

  /// The first [length] values of [values] (with [nulls], a packed bitmap
  /// with a bit set for null) as a column named [name].
  Column column(String name, TypedData values, int length, Uint8List? nulls) {
    if (type != DataType.boolean) {
      return NumericVector.adopt(
        name: name,
        dataType: type,
        data: values,
        nullBitmap: nulls,
      );
    }
    final bytes = values as Uint8List;
    final bits = Uint8List((length + 7) >> 3);
    for (var i = 0; i < length; i++) {
      bits[i >> 3] |= bytes[i] << (i & 7);
    }
    return BoolVector.adopt(
      name: name,
      length: length,
      bits: bits,
      nulls: nulls,
    );
  }
}

/// Reads a column from a Parquet file into a [Column].
class ColumnReader {
  final FileReader reader;
  final thrift.ColumnChunk chunk;
  final thrift.SchemaElement element;
  final ParquetReaderOptions options;

  /// The column's max levels, from the file's [SchemaLayout]; null reads it
  /// as a flat required column.
  final LevelInfo? levels;
  final String name;

  ColumnReader(
    this.reader,
    this.chunk,
    this.element,
    this.options, {
    required this.name,
    this.levels,
  });

  /// Read the whole column into a [Column].
  Future<Column> read() async {
    final metadata = chunk.metaData!;
    final startOffset =
        (metadata.dictionaryPageOffset != null &&
            metadata.dictionaryPageOffset! > 0)
        ? metadata.dictionaryPageOffset!
        : metadata.dataPageOffset;
    final totalSize = metadata.totalCompressedSize;

    final maxDef = levels?.maxDefinition;
    final maxRep = levels?.maxRepetition;
    final nullable = (maxDef ?? 0) > 0;

    // Typed fast path: flat (non-repeated) numeric columns decode straight into
    // a typed buffer with no boxing. Anything it can't handle throws and we fall
    // back to the boxed path, so correctness never depends on the fast path.
    // Unsigned columns take the boxed path so _finalize can reinterpret them.
    if ((maxRep ?? 0) == 0 && SchemaConverter.unsignedInfo(element) == null) {
      final kind = _typedKind(element.type!);
      if (kind != null) {
        try {
          return _finalize(
            await _readTyped(
              metadata,
              startOffset,
              totalSize,
              maxDef ?? 0,
              nullable,
              kind,
            ),
          );
        } on UnsupportedError {
          // Only an explicit "fast path can't handle this encoding/type" signal
          // falls back to the boxed path; real decode errors propagate.
        }
      }
    }

    // UTF-8 fast path: flat text columns copy their bytes into a StringColumn
    // with no String per value. It leaves anything unusual (other encodings,
    // malformed text, a count that disagrees with the metadata, tolerant mode)
    // to the boxed path below, which owns those semantics.
    if ((maxRep ?? 0) == 0 &&
        element.type == thrift.ParquetType.byteArray &&
        isUtf8(element) &&
        !options.tolerateTruncatedPages) {
      try {
        return await _readStrings(
          metadata,
          startOffset,
          totalSize,
          maxDef ?? 0,
        );
      } on UnsupportedError {
        // Fall back to the boxed path.
      }
    }

    final values = await _readBoxed(
      metadata,
      startOffset,
      totalSize,
      maxDef,
      maxRep,
    );
    final dataType = _convertType();
    if ((maxRep ?? 0) > 0) {
      return ListColumn(name: name, dataType: dataType, data: values);
    }
    // Tolerant truncation can inject nulls into a declared-required column;
    // the data is authoritative, so mark it nullable when nulls are present.
    final present = nullable || values.any((value) => value == null);
    if (dataType == DataType.string) return _text(values, present);
    return _finalize(
      ListColumn(
        name: name,
        dataType: dataType,
        data: values,
        nullable: present,
      ),
    );
  }

  /// Raw per-leaf decode for the multi-level Dremel assembler: the page-decoded
  /// values (with per-value logical conversion applied) plus repetition and
  /// definition levels, with NO single-level record assembly. maxDef/maxRep are
  /// the leaf's levels.
  Future<RawLeaf> readRaw() async {
    final metadata = chunk.metaData!;
    final startOffset =
        (metadata.dictionaryPageOffset != null &&
            metadata.dictionaryPageOffset! > 0)
        ? metadata.dictionaryPageOffset!
        : metadata.dataPageOffset;
    final totalSize = metadata.totalCompressedSize;
    final maxDef = levels?.maxDefinition ?? 0;
    final maxRep = levels?.maxRepetition ?? 0;

    final values = <dynamic>[];
    final reps = <int>[];
    final defs = <int>[];
    final pageReader = PageReader(
      reader,
      startOffset,
      totalSize,
      metadata.codec,
      verify: options.verifyChecksums,
      maxPageBytes: options.maxPageBytes,
    );
    final pageDecoder = PageDecoder(
      type: element.type!,
      schema: element,
      maxDefinitionLevel: maxDef,
      maxRepetitionLevel: maxRep,
      tolerant: options.tolerateTruncatedPages,
    );
    while (true) {
      final page = await pageReader.readNext();
      if (page == null) break;
      final decoded = pageDecoder.decode(page.header, page.data);
      if (page.header.type == thrift.PageType.dictionaryPage) continue;
      values.addAll(decoded.values);
      reps.addAll(decoded.repLevels ?? List.filled(decoded.values.length, 0));
      defs.addAll(
        decoded.defLevels ?? List.filled(decoded.values.length, maxDef),
      );
      if (maxRep == 0 && values.length >= metadata.numValues) break;
    }
    // Siblings are assembled in step, so a leaf whose pages disagree with its
    // metadata would be indexed past its end.
    if (values.length != metadata.numValues) {
      throw FormatException(
        'Column $name holds ${values.length} values, metadata says '
        '${metadata.numValues}',
      );
    }

    // Apply logical conversion (date/timestamp/decimal) to the leaf values,
    // preserving nulls; the assembler then builds nesting from these.
    final dataType = _convertType();
    final converted = _finalize(
      ListColumn(
        name: name,
        dataType: dataType,
        data: values,
        nullable: maxDef > 0,
      ),
    ).toList();
    return RawLeaf(
      values: converted,
      reps: reps,
      defs: defs,
      maxDef: maxDef,
      maxRep: maxRep,
      dataType: dataType,
    );
  }

  /// A streaming cursor over this column's chunk: decodes pages on demand and
  /// hands out row-range [Column]s, so `readBatches` never materializes a whole
  /// (possibly huge) row group at once. Reuses the same page decode + Dremel
  /// assembly + logical finalize as [read].
  ColumnCursor cursor() {
    final metadata = chunk.metaData!;
    final maxDef = levels?.maxDefinition ?? 0;
    final maxRep = levels?.maxRepetition ?? 0;
    return ColumnCursor(this, metadata, maxDef, maxRep);
  }

  DataType _convertType() => SchemaConverter().convertType(element);

  /// Convert a freshly-read column's physical values to their logical Dart form:
  /// date/timestamp ints (or INT96 bytes) → [DateTime]; decimal int/bytes →
  /// unscaled [BigInt]. Other columns are returned unchanged. A value that cannot
  /// be represented (e.g. an out-of-range INT96) fails loudly with a
  /// [FormatException] unless [ParquetReaderOptions.tolerateTruncatedPages] is set,
  /// in which case it becomes null (the column keeps its logical type rather than
  /// degrading to raw physical values).
  Column _finalize(Column column) {
    final logical = _convertType();
    // Unsigned ints: the physical value was read as signed, so a value with the
    // high bit set came back negative: reinterpret to the unsigned value
    // (uint8/16/32 stay int; uint64 becomes a BigInt decimal: lossless).
    final unsigned = SchemaConverter.unsignedInfo(element);
    if (unsigned != null) {
      return _mapLogical(
        column,
        unsigned.type,
        (value) =>
            SchemaConverter.reinterpretUnsigned(value as int, unsigned.bits),
      );
    }
    if (logical == DataType.date) {
      return _mapLogical(
        column,
        logical,
        (value) => dateFromEpochDays(value as int),
      );
    }
    if (logical == DataType.timestamp) {
      final int96 = element.type == thrift.ParquetType.int96;
      final unit = element.logicalType?.timestamp?.unit;
      final millis =
          element.convertedType == thrift.ConvertedType.timestampMillis ||
          unit?.millis != null;
      // NANOS is the pandas/pyarrow default; without this branch it fell
      // through to micros and every value decoded 1000× too large.
      final nanos = unit?.nanos != null;
      return _mapLogical(
        column,
        logical,
        (value) => int96
            ? int96ToTimestamp(value as List<int>)
            : millis
            ? timestampFromEpochMillis(value as int)
            : nanos
            ? timestampFromEpochNanos(value as int)
            : timestampFromEpochMicros(value as int),
      );
    }
    if (logical == DataType.decimal) {
      return _mapLogical(
        column,
        logical,
        (value) => value is int
            ? BigInt.from(value)
            : bytesToBigInt((value as List).cast<int>(), littleEndian: false),
      );
    }
    // TINYINT/SMALLINT decode through the INT32 physical fast path, so narrow
    // the column to its declared logical width; otherwise the column stays
    // int32 while the schema field says int8/int16 (a type mislabel).
    if ((logical == DataType.int8 || logical == DataType.int16) &&
        column.dataType != logical) {
      return _narrowInt(column, logical);
    }
    return column;
  }

  /// Re-type an int32-decoded [column] to its narrower logical [type] (int8 or
  /// int16), preserving values (they fit by construction) and nulls.
  Column _narrowInt(Column column, DataType type) {
    final count = column.length;
    final buffer = type == DataType.int8 ? Int8List(count) : Int16List(count);
    if (column is NumericVector) {
      buffer.setRange(0, count, column.raw as List<int>);
      return NumericVector.adopt(
        name: column.name,
        dataType: type,
        data: buffer,
        nullBitmap: column.nulls,
      );
    }
    final values = column.toList().cast<int?>();
    return type == DataType.int8
        ? NumericColumn.int8(
            name: column.name,
            values: values,
            nullable: column.hasNulls,
          )
        : NumericColumn.int16(
            name: column.name,
            values: values,
            nullable: column.hasNulls,
          );
  }

  /// Map each non-null value through [convert] into a [logical]-typed column. A
  /// conversion error fails loudly unless tolerant mode coerces it to null.
  Column _mapLogical(
    Column column,
    DataType logical,
    dynamic Function(dynamic) convert,
  ) {
    final count = column.length;
    final out = List<dynamic>.filled(count, null);
    for (int i = 0; i < count; i++) {
      final value = column[i];
      if (value == null) continue;
      try {
        out[i] = convert(value);
      } catch (error) {
        if (!options.tolerateTruncatedPages) {
          throw FormatException(
            'Cannot represent ${logical.name} value at row $i in column '
            '"${column.name}": $error (set tolerateTruncatedPages to coerce to null)',
          );
        }
      }
    }
    return ListColumn(
      name: column.name,
      dataType: logical,
      data: out,
      nullable: column.nullable,
    );
  }

  // ---- typed fast path --------------------------------------------------

  _TypedKind? _typedKind(thrift.ParquetType type) {
    switch (type) {
      case thrift.ParquetType.int32:
        return _TypedKind(DataType.int32, (length) => Int32List(length));
      case thrift.ParquetType.int64:
        return _TypedKind(DataType.int64, (length) => Int64List(length));
      case thrift.ParquetType.float:
        return _TypedKind(DataType.float32, (length) => Float32List(length));
      case thrift.ParquetType.double:
        return _TypedKind(DataType.float64, (length) => Float64List(length));
      case thrift.ParquetType.boolean when !options.tolerateTruncatedPages:
        return _TypedKind(DataType.boolean, (length) => Uint8List(length));
      default:
        return null;
    }
  }

  Future<Column> _readTyped(
    thrift.ColumnMetaData metadata,
    int startOffset,
    int totalSize,
    int maxDef,
    bool nullable,
    _TypedKind kind,
  ) async {
    final total = metadata.numValues;
    if (total < 0) {
      throw FormatException(
        'Parquet column "$name": negative numValues $total',
      );
    }
    // Allocate incrementally rather than trusting the footer's numValues up
    // front: an adversarial numValues (e.g. 2^40 in a tiny file) must not force
    // a huge typed allocation before any page is read. Pages are themselves
    // value-capped, so growth tracks actual decoded data.
    var capacity = total < _step ? total : _step;
    var buffer = kind.make(capacity);
    var out = buffer as List<num>;
    var bitmap = nullable ? Uint8List((capacity + 7) ~/ 8) : null;

    void ensure(int needed) {
      if (needed <= capacity) return;
      var size = capacity == 0 ? _step : capacity * 2;
      while (size < needed) {
        size *= 2;
      }
      if (size > total) size = total; // never exceed the declared chunk size
      final grown = kind.make(size) as List<num>;
      grown.setRange(0, out.length, out);
      buffer = grown as TypedData;
      out = grown;
      if (bitmap != null) {
        final bits = Uint8List((size + 7) ~/ 8);
        bits.setRange(0, bitmap!.length, bitmap!);
        bitmap = bits;
      }
      capacity = size;
    }

    final pageReader = PageReader(
      reader,
      startOffset,
      totalSize,
      metadata.codec,
      verify: options.verifyChecksums,
      maxPageBytes: options.maxPageBytes,
    );
    final decoder = PageDecoder(
      type: element.type!,
      schema: element,
      maxDefinitionLevel: maxDef,
      maxRepetitionLevel: 0,
      tolerant: options.tolerateTruncatedPages,
    );

    int row = 0;
    while (row < total) {
      final page = await pageReader.readNext();
      if (page == null) break;
      if (page.header.type == thrift.PageType.dictionaryPage) {
        decoder.decode(page.header, page.data); // populate dictionary
        continue;
      }
      final body = decoder.parseBody(page.header, page.data);
      final defs = body.defLevels;
      if (defs == null) {
        // Required: every slot present, decode straight into place. A page that
        // declares more values than the column's total would push the write
        // past the (total-capped) buffer: reject it rather than RangeError.
        if (row + body.numValues > total) {
          throw FormatException(
            'Parquet column "$name": page declares ${body.numValues} values '
            'but only ${total - row} of $total remain',
          );
        }
        ensure(row + body.numValues);
        decoder.decodeValuesTyped(
          body.valueData,
          body.present,
          body.encoding,
          out,
          row,
        );
        row += body.numValues;
      } else if (body.present == defs.length && row + defs.length <= total) {
        // An optional column with no nulls on this page: every slot is
        // present, so decode straight into place with no scatter.
        ensure(row + defs.length);
        decoder.decodeValuesTyped(
          body.valueData,
          body.present,
          body.encoding,
          out,
          row,
        );
        row += defs.length;
      } else {
        ensure(row + defs.length);
        final temp = kind.make(body.present) as List<num>;
        decoder.decodeValuesTyped(
          body.valueData,
          body.present,
          body.encoding,
          temp,
          0,
        );
        int taken = 0;
        for (int i = 0; i < defs.length && row < total; i++) {
          if (defs[i] == maxDef) {
            out[row] = temp[taken++];
          } else {
            bitmap![row >> 3] |= 1 << (row & 7);
          }
          row++;
        }
      }
    }

    // Pages ending before numValues means the chunk is truncated; the untouched
    // typed-buffer tail would otherwise read as legitimate zeros.
    if (row != total) {
      if (!options.tolerateTruncatedPages) {
        throw FormatException(
          'Parquet column "$name": decoded $row of $total values (truncated chunk)',
        );
      }
      // Tolerant: the tail is UNKNOWN, not zero. Zeros are indistinguishable
      // from data the file really held, so mark the padding null, which is
      // what the boxed path has always done for the same option. A required
      // column needs a bitmap allocated for it; the reader promotes such a
      // column to nullable because the data is authoritative.
      ensure(total);
      final nulls = bitmap ??= Uint8List((total + 7) ~/ 8);
      for (int i = row; i < total; i++) {
        nulls[i >> 3] |= 1 << (i & 7);
      }
    }

    return kind.column(name, buffer, total, bitmap);
  }

  // ---- UTF-8 fast path ----------------------------------------------------

  /// Boxed [values] of a UTF-8 column as a [StringColumn]. FIXED_LEN_BYTE_ARRAY
  /// values arrive as bytes and are decoded as BYTE_ARRAY text is, as DuckDB
  /// reads them. Any other value that is not a [String] means the annotation
  /// sits on a numeric or boolean type, which the format does not allow.
  StringColumn _text(List<dynamic> values, bool nullable) {
    final fixed = element.type == thrift.ParquetType.fixedLenByteArray;
    return StringColumn(
      name: name,
      values: [
        for (final value in values)
          switch (value) {
            null => null,
            String() => value,
            List<int>() when fixed => utf8.decode(value, allowMalformed: true),
            _ => throw FormatException(
              'Parquet column "$name": a UTF8 annotation on ${element.type}',
            ),
          },
      ],
      nullable: nullable,
    );
  }

  /// Read a flat UTF-8 column into a [StringColumn] through [_TextPages].
  /// Throws [UnsupportedError] for what it leaves to the boxed path (see
  /// [_TextPages]), and when the pages hold a different number of values than
  /// the chunk declares.
  Future<Column> _readStrings(
    thrift.ColumnMetaData metadata,
    int startOffset,
    int totalSize,
    int maxDef,
  ) async {
    final total = metadata.numValues;
    if (total < 0) {
      throw FormatException(
        'Parquet column "$name": negative numValues $total',
      );
    }
    final out = _Strings(nullable: maxDef > 0);
    final pageReader = PageReader(
      reader,
      startOffset,
      totalSize,
      metadata.codec,
      verify: options.verifyChecksums,
      maxPageBytes: options.maxPageBytes,
    );
    final decoder = PageDecoder(
      type: element.type!,
      schema: element,
      maxDefinitionLevel: maxDef,
      maxRepetitionLevel: 0,
      tolerant: false,
    );
    final text = _TextPages(element.type!, maxDef);
    while (out.rows < total) {
      final page = await pageReader.readNext();
      if (page == null) break;
      final header = page.header;
      if (header.type == thrift.PageType.dictionaryPage) {
        text.dictionary(header, page.data);
        continue;
      }
      if (header.type != thrift.PageType.dataPage &&
          header.type != thrift.PageType.dataPageV2) {
        continue;
      }
      final body = decoder.parseBody(header, page.data);
      if (out.rows + _TextPages.slots(body) > total) {
        throw UnsupportedError('page value count disagrees with the chunk');
      }
      text.page(body, out);
    }
    if (out.rows != total) {
      throw UnsupportedError('chunk holds ${out.rows} of $total values');
    }
    return out.build(name);
  }

  // ---- boxed path (fallback for strings/bool/nested/legacy) -------------

  Future<List<dynamic>> _readBoxed(
    thrift.ColumnMetaData metadata,
    int startOffset,
    int totalSize,
    int? maxDef,
    int? maxRep,
  ) async {
    final values = <dynamic>[];
    final pageReader = PageReader(
      reader,
      startOffset,
      totalSize,
      metadata.codec,
      verify: options.verifyChecksums,
      maxPageBytes: options.maxPageBytes,
    );
    final pageDecoder = PageDecoder(
      type: element.type!,
      schema: element,
      maxDefinitionLevel: maxDef,
      maxRepetitionLevel: maxRep,
      tolerant: options.tolerateTruncatedPages,
    );

    final repetitions = <int>[];
    final definitions = <int>[];
    while (true) {
      final page = await pageReader.readNext();
      if (page == null) break;
      final decoded = pageDecoder.decode(page.header, page.data);
      if (page.header.type == thrift.PageType.dictionaryPage) {
        continue;
      }
      values.addAll(decoded.values);
      repetitions.addAll(
        decoded.repLevels ?? List.filled(decoded.values.length, 0),
      );
      definitions.addAll(
        decoded.defLevels ?? List.filled(decoded.values.length, maxDef ?? 0),
      );
      if ((maxRep ?? 0) == 0 && values.length >= metadata.numValues) {
        break;
      }
    }

    if ((maxRep ?? 0) > 0) {
      final optional =
          element.repetitionType == thrift.FieldRepetitionType.optional;
      return assemble(values, repetitions, definitions, maxDef ?? 1, optional);
    }
    if (values.length > metadata.numValues) {
      return values.sublist(0, metadata.numValues);
    }
    // A short flat column means the chunk is truncated. Fail rather than return
    // a silently shortened column, or, in tolerant mode, pad the tail with
    // NULLs to the declared count so the boxed path matches the typed path and
    // both agree on the row count.
    if (values.length < metadata.numValues) {
      if (!options.tolerateTruncatedPages) {
        throw FormatException(
          'Parquet column "$name": decoded ${values.length} of '
          '${metadata.numValues} values (truncated chunk)',
        );
      }
      values.addAll(List.filled(metadata.numValues - values.length, null));
    }
    return values;
  }

  /// Assemble flat leaf [values] into one entry per record (row), grouping by
  /// repetition level. A repetition level of 0 starts a new record. Within a
  /// record, slots whose definition level reaches [maxDef] carry a value; a
  /// definition level one below marks a present-but-null element; lower levels
  /// mean the list itself is empty or null. A first value that does not start
  /// a record is a [FormatException] (corrupt levels).
  static List<dynamic> assemble(
    List<dynamic> values,
    List<int> repetitions,
    List<int> definitions,
    int maxDef,
    bool optional,
  ) {
    final rows = <dynamic>[];
    List<dynamic>? current;
    for (int i = 0; i < values.length; i++) {
      if (repetitions[i] == 0) {
        if (current != null) rows.add(current);
        current = <dynamic>[];
      } else if (current == null) {
        throw FormatException(
          'Parquet repetition level ${repetitions[i]} at value $i continues a '
          'record that never started (corrupt levels)',
        );
      }
      final def = definitions[i];
      if (def == maxDef) {
        current.add(values[i]);
      } else if (optional && def == maxDef - 1) {
        // Present-but-null element (only when the leaf itself is optional; for a
        // required element this level instead means an empty collection).
        current.add(null);
      }
    }
    if (current != null) rows.add(current);
    return rows;
  }
}

/// Streaming, page-bounded reader for one column chunk. Accumulates only the
/// decoded values not yet emitted (≈ one page + the pending batch), reusing the
/// owning [ColumnReader]'s decode/assembly/finalize. Flat columns emit one value
/// per row (nulls already inline from the decoder); nested/repeated columns
/// accumulate per page and emit only COMPLETE records, carrying the partial tail
/// across batch boundaries.
class ColumnCursor {
  final ColumnReader _owner;
  final thrift.ColumnMetaData _metadata;
  final int _maxDef;
  final int _maxRep;
  late final PageReader _pageReader;
  late final PageDecoder _pageDecoder;
  bool _exhausted = false;

  /// Set for a flat numeric column: its pending values are decoded into
  /// [_typed], with a null flag per value in [_nulls].
  final _TypedKind? _kind;
  late List<num> _typed = _kind!.make(0) as List<num>;
  Uint8List _nulls = Uint8List(0);
  int _end = 0;

  /// Set for a flat UTF-8 column (outside tolerant mode): its pending values
  /// are appended to [_strings] as bytes.
  final _TextPages? _text;
  final _Strings? _strings;

  /// The dictionary page, kept until a page falls back to the boxed decode,
  /// which needs it decoded as strings.
  ({thrift.PageHeader header, Uint8List data})? _dictionary;

  /// First pending value of a flat column, in [_typed], [_strings] or
  /// [_values]. Taking rows advances it, so a batch costs its own size rather
  /// than a shift of everything still pending.
  int _start = 0;

  final List<dynamic> _values = [];
  final List<int> _repetitions = []; // nested only
  final List<int> _definitions = []; // nested only
  final List<int> _records =
      []; // value indices where repLevel==0 (nested only)

  ColumnCursor(this._owner, this._metadata, this._maxDef, this._maxRep)
    : _kind =
          _maxRep == 0 && SchemaConverter.unsignedInfo(_owner.element) == null
          ? _owner._typedKind(_owner.element.type!)
          : null,
      _text = _textual(_owner, _maxRep)
          ? _TextPages(_owner.element.type!, _maxDef)
          : null,
      _strings = _textual(_owner, _maxRep)
          ? _Strings(nullable: _maxDef > 0)
          : null {
    final startOffset =
        (_metadata.dictionaryPageOffset != null &&
            _metadata.dictionaryPageOffset! > 0)
        ? _metadata.dictionaryPageOffset!
        : _metadata.dataPageOffset;
    _pageReader = PageReader(
      _owner.reader,
      startOffset,
      _metadata.totalCompressedSize,
      _metadata.codec,
      verify: _owner.options.verifyChecksums,
      maxPageBytes: _owner.options.maxPageBytes,
    );
    _pageDecoder = PageDecoder(
      type: _owner.element.type!,
      schema: _owner.element,
      maxDefinitionLevel: _maxDef,
      maxRepetitionLevel: _maxRep,
      tolerant: _owner.options.tolerateTruncatedPages,
    );
  }

  bool get _nested => _maxRep > 0;

  static bool _textual(ColumnReader owner, int maxRep) =>
      maxRep == 0 &&
      owner.element.type == thrift.ParquetType.byteArray &&
      isUtf8(owner.element) &&
      !owner.options.tolerateTruncatedPages;

  Future<void> _pull() async {
    if (_exhausted) return;
    final page = await _pageReader.readNext();
    if (page == null) {
      _exhausted = true;
      return;
    }
    final kind = _kind;
    if (kind != null &&
        (page.header.type == thrift.PageType.dataPage ||
            page.header.type == thrift.PageType.dataPageV2)) {
      _decodeTyped(page.header, page.data, kind);
      return;
    }
    final text = _text;
    if (text != null) {
      _decodeText(page.header, page.data, text, _strings!);
      return;
    }
    final decoded = _pageDecoder.decode(page.header, page.data);
    if (page.header.type == thrift.PageType.dictionaryPage) return;
    if (!_nested && _start > 0 && _start * 2 >= _values.length) {
      _values.removeRange(0, _start);
      _start = 0;
    }
    final base = _values.length;
    _values.addAll(decoded.values);
    if (_nested) {
      final repetitions =
          decoded.repLevels ?? List.filled(decoded.values.length, 0);
      final definitions =
          decoded.defLevels ?? List.filled(decoded.values.length, _maxDef);
      for (int i = 0; i < repetitions.length; i++) {
        if (repetitions[i] == 0) _records.add(base + i);
      }
      _repetitions.addAll(repetitions);
      _definitions.addAll(definitions);
    }
  }

  /// Rows safe to emit now. Flat: every pending value. Nested: complete records
  /// only: the last started record stays pending until a new record-start or
  /// end-of-chunk arrives.
  int get readyRows {
    if (_kind != null) return _end - _start;
    if (_strings != null) return _strings.rows - _start;
    if (!_nested) return _values.length - _start;
    if (_exhausted) return _records.length;
    return _records.isEmpty ? 0 : _records.length - 1;
  }

  /// Pull pages until [target] rows are ready (or the chunk is exhausted).
  Future<void> fill(int target) async {
    while (readyRows < target && !_exhausted) {
      await _pull();
    }
  }

  /// Emit exactly [rows] rows: this column's remaining real rows first, then
  /// NULL padding for any shortfall. Used by the row-group cursor's tolerant
  /// tail so a column whose chunk is INTACT keeps its real values even when a
  /// SIBLING column's chunk is truncated (padding every column uniformly would
  /// drop this column's tail: the eager reader pads each column to its own
  /// length). When this column has exactly [rows] ready it degenerates to [take];
  /// when it has none, to [padNull].
  Column takeOrPad(int rows) {
    final real = readyRows;
    if (real >= rows) return take(rows);
    if (real == 0) return padNull(rows);
    return Column.concat([take(real), padNull(rows - real)]);
  }

  /// Emit a [Column] of exactly [rows] rows (rows ≤ [readyRows]), dropping them.
  Column take(int rows) {
    final kind = _kind;
    if (kind != null) return _takeTyped(rows, kind);
    final strings = _strings;
    if (strings != null) {
      final column = strings.slice(_start, rows, _owner.name);
      _start += rows;
      return column;
    }
    if (!_nested) {
      final slice = _values.sublist(_start, _start + rows);
      _start += rows;
      return _buildFlat(slice);
    }
    final limit = rows < _records.length ? _records[rows] : _values.length;
    final values = _values.sublist(0, limit);
    final repetitions = _repetitions.sublist(0, limit);
    final definitions = _definitions.sublist(0, limit);
    _values.removeRange(0, limit);
    _repetitions.removeRange(0, limit);
    _definitions.removeRange(0, limit);
    _records.removeRange(0, rows);
    for (int i = 0; i < _records.length; i++) {
      _records[i] -= limit;
    }
    final optional =
        _owner.element.repetitionType == thrift.FieldRepetitionType.optional;
    final assembled = ColumnReader.assemble(
      values,
      repetitions,
      definitions,
      _maxDef,
      optional,
    );
    return ListColumn(
      name: _owner.name,
      dataType: _owner._convertType(),
      data: assembled,
    );
  }

  /// A [Column] of exactly [rows] NULLs matching this column's type, used to
  /// pad the tail of a truncated tolerant chunk up to the row group's declared
  /// row count (the same reconciliation the eager typed reader does) so the
  /// streaming and eager APIs agree on the row count of a malformed file. The
  /// padded column is nullable even for a required column: a truncated tail has
  /// no value, and tolerant mode surfaces that as NULL rather than a fake zero.
  Column padNull(int rows) {
    final nulls = List<dynamic>.filled(rows, null);
    if (_nested) {
      return ListColumn(
        name: _owner.name,
        dataType: _owner._convertType(),
        data: nulls,
        nullable: true,
      );
    }
    final kind = _owner._typedKind(_owner.element.type!);
    final column = kind != null
        ? _numericFrom(kind.type, nulls, true)
        : ListColumn(
            name: _owner.name,
            dataType: _owner._convertType(),
            data: nulls,
            nullable: true,
          );
    return _owner._finalize(column);
  }

  /// Decode a data page of a flat numeric column onto the end of [_typed].
  /// An encoding the typed decode does not support goes through the boxed
  /// decode instead, the same fallback [ColumnReader.read] makes per column.
  void _decodeTyped(thrift.PageHeader header, Uint8List data, _TypedKind kind) {
    final body = _pageDecoder.parseBody(header, data);
    final defs = body.defLevels;
    final count = defs?.length ?? body.numValues;
    _reserve(count, kind);
    try {
      if (defs == null || body.present == count) {
        _pageDecoder.decodeValuesTyped(
          body.valueData,
          body.present,
          body.encoding,
          _typed,
          _end,
        );
        _nulls.fillRange(_end, _end + count, 0);
      } else {
        final temp = kind.make(body.present) as List<num>;
        _pageDecoder.decodeValuesTyped(
          body.valueData,
          body.present,
          body.encoding,
          temp,
          0,
        );
        var present = 0;
        for (var i = 0; i < count; i++) {
          if (defs[i] == _maxDef) {
            _typed[_end + i] = temp[present++];
            _nulls[_end + i] = 0;
          } else {
            _nulls[_end + i] = 1;
          }
        }
      }
      _end += count;
    } on UnsupportedError {
      final values = _pageDecoder.decode(header, data).values;
      _reserve(values.length, kind);
      for (var i = 0; i < values.length; i++) {
        final value = values[i];
        _nulls[_end + i] = value == null ? 1 : 0;
        if (value is bool) {
          _typed[_end + i] = value ? 1 : 0;
        } else if (value != null) {
          _typed[_end + i] = value as num;
        }
      }
      _end += values.length;
    }
  }

  /// Append a page of a flat UTF-8 column to [strings] as bytes, or through
  /// the boxed decode for a page [text] leaves to it.
  void _decodeText(
    thrift.PageHeader header,
    Uint8List data,
    _TextPages text,
    _Strings strings,
  ) {
    if (header.type == thrift.PageType.dictionaryPage) {
      _dictionary = (header: header, data: data);
      try {
        text.dictionary(header, data);
      } on UnsupportedError {
        // Its pages take the boxed decode.
      }
      return;
    }
    if (header.type != thrift.PageType.dataPage &&
        header.type != thrift.PageType.dataPageV2) {
      return;
    }
    if (_start > 0 && _start * 2 >= strings.rows) {
      strings.drop(_start);
      _start = 0;
    }
    try {
      text.page(_pageDecoder.parseBody(header, data), strings);
    } on UnsupportedError {
      final dictionary = _dictionary;
      if (dictionary != null) {
        _pageDecoder.decode(dictionary.header, dictionary.data);
        _dictionary = null;
      }
      for (final value in _pageDecoder.decode(header, data).values) {
        if (value == null) {
          strings.addNull();
        } else {
          strings.addText(value as String);
        }
      }
    }
  }

  /// Make room for [count] more values after [_end]: first by moving the
  /// pending values to the front, then by doubling.
  void _reserve(int count, _TypedKind kind) {
    if (_end + count <= _typed.length) return;
    final pending = _end - _start;
    if (pending + count <= _typed.length) {
      _typed.setRange(0, pending, _typed, _start);
      _nulls.setRange(0, pending, _nulls, _start);
    } else {
      final size = (pending + count) * 2;
      final typed = kind.make(size) as List<num>;
      final nulls = Uint8List(size);
      typed.setRange(0, pending, _typed, _start);
      nulls.setRange(0, pending, _nulls, _start);
      _typed = typed;
      _nulls = nulls;
    }
    _start = 0;
    _end = pending;
  }

  Column _takeTyped(int rows, _TypedKind kind) {
    final values = kind.make(rows) as List<num>;
    values.setRange(0, rows, _typed, _start);
    Uint8List? bitmap = _maxDef > 0 ? Uint8List((rows + 7) >> 3) : null;
    for (var i = 0; i < rows; i++) {
      if (_nulls[_start + i] != 0) {
        (bitmap ??= Uint8List((rows + 7) >> 3))[i >> 3] |= 1 << (i & 7);
      }
    }
    _start += rows;
    return _owner._finalize(
      kind.column(_owner.name, values as TypedData, rows, bitmap),
    );
  }

  Column _buildFlat(List<dynamic> values) {
    final kind = _owner._typedKind(_owner.element.type!);
    final Column column;
    if (_owner._convertType() == DataType.string) {
      return _owner._text(values, _maxDef > 0 || values.contains(null));
    } else if (kind != null) {
      column = _numericFrom(kind.type, values, _maxDef > 0);
    } else {
      column = ListColumn(
        name: _owner.name,
        dataType: _owner._convertType(),
        data: values,
        nullable: _maxDef > 0,
      );
    }
    return _owner._finalize(column);
  }

  Column _numericFrom(DataType type, List<dynamic> values, bool nullable) {
    switch (type) {
      case DataType.int32:
        return NumericColumn.int32(
          name: _owner.name,
          values: values.cast<int?>(),
          nullable: nullable,
        );
      case DataType.int64:
        return NumericColumn.int64(
          name: _owner.name,
          values: values.cast<int?>(),
          nullable: nullable,
        );
      case DataType.float32:
        return NumericColumn.float32(
          name: _owner.name,
          values: values.cast<double?>(),
          nullable: nullable,
        );
      default: // float64
        return NumericColumn.float64(
          name: _owner.name,
          values: values.cast<double?>(),
          nullable: nullable,
        );
    }
  }
}

/// A string column's UTF-8 buffers while it is read, grown by doubling.
class _Strings {
  Int32List offsets = Int32List(1024);
  Uint8List bytes = Uint8List(16384);
  Uint8List? nulls;
  int rows = 0;

  _Strings({required bool nullable}) : nulls = nullable ? Uint8List(128) : null;

  /// Append `source[start, start + length)` as the next value.
  void add(Uint8List source, int start, int length) {
    final at = offsets[rows];
    final end = at + length;
    if (end > 0x7fffffff) {
      throw UnsupportedError('string column past 2 GiB');
    }
    if (end > bytes.length) {
      var size = bytes.length * 2;
      while (size < end) {
        size *= 2;
      }
      bytes = Uint8List(size)..setRange(0, at, bytes);
    }
    bytes.setRange(at, end, source, start);
    _row(end);
  }

  void addNull() {
    final row = rows;
    _row(offsets[row]);
    nulls![row >> 3] |= 1 << (row & 7);
  }

  void _row(int end) {
    if (rows + 2 > offsets.length) {
      offsets = Int32List(offsets.length * 2)..setRange(0, rows + 1, offsets);
      final current = nulls;
      if (current != null) {
        nulls = Uint8List(offsets.length >> 3)
          ..setRange(0, current.length, current);
      }
    }
    rows++;
    offsets[rows] = end;
  }

  /// Append [value], encoded.
  void addText(String value) {
    final encoded = utf8.encode(value);
    add(encoded, 0, encoded.length);
  }

  /// Rows [start] to `start + count` as a column of their own.
  StringColumn slice(int start, int count, String name) {
    final base = offsets[start];
    final rebased = Int32List(count + 1);
    for (var i = 1; i <= count; i++) {
      rebased[i] = offsets[start + i] - base;
    }
    final current = nulls;
    final flags = current == null ? null : Uint8List((count + 7) >> 3);
    if (current != null) {
      for (var i = 0; i < count; i++) {
        final row = start + i;
        if ((current[row >> 3] & (1 << (row & 7))) != 0) {
          flags![i >> 3] |= 1 << (i & 7);
        }
      }
    }
    return StringVector.adopt(
      name: name,
      offsets: rebased,
      bytes: Uint8List.fromList(
        Uint8List.sublistView(bytes, base, offsets[start + count]),
      ),
      nulls: flags,
    );
  }

  /// Remove the first [count] rows, moving the rest to the front.
  void drop(int count) {
    final base = offsets[count];
    final remaining = rows - count;
    bytes.setRange(0, offsets[rows] - base, bytes, base);
    for (var i = 0; i <= remaining; i++) {
      offsets[i] = offsets[count + i] - base;
    }
    final current = nulls;
    if (current != null) {
      for (var i = 0; i < rows; i++) {
        final row = count + i;
        final set =
            i < remaining && (current[row >> 3] & (1 << (row & 7))) != 0;
        if (set) {
          current[i >> 3] |= 1 << (i & 7);
        } else {
          current[i >> 3] &= ~(1 << (i & 7));
        }
      }
    }
    rows = remaining;
  }

  /// The column, its buffers copied down to their used size.
  StringColumn build(String name) {
    final current = nulls;
    return StringVector.adopt(
      name: name,
      offsets: Int32List.fromList(Int32List.sublistView(offsets, 0, rows + 1)),
      bytes: Uint8List.fromList(Uint8List.sublistView(bytes, 0, offsets[rows])),
      nulls: current == null
          ? null
          : Uint8List.fromList(
              Uint8List.sublistView(current, 0, (rows + 7) >> 3),
            ),
    );
  }
}

/// Appends the values of a flat UTF-8 column's pages to a [_Strings] as byte
/// ranges, with no [String] per value. [page] throws [UnsupportedError],
/// before appending anything, for what it leaves to the boxed decode: an
/// encoding other than PLAIN, dictionary or DELTA_LENGTH_BYTE_ARRAY, malformed
/// UTF-8 (which the boxed decode reads as replacement characters), or levels
/// that disagree with the page's value count.
class _TextPages {
  final int _maxDef;
  final DictionaryDecoder _indexer;
  _Strings? _dictionary;

  /// Whether a dictionary page has arrived, so a dictionary-encoded page with
  /// no usable dictionary is malformed text rather than a corrupt chunk.
  bool _seen = false;
  Int32List _indices = Int32List(0);
  Int32List _starts = Int32List(0);
  Int32List _lengths = Int32List(0);

  _TextPages(thrift.ParquetType type, this._maxDef)
    : _indexer = DictionaryDecoder(type);

  /// Rows (values and nulls) [body] holds.
  static int slots(PageBody body) => body.defLevels?.length ?? body.numValues;

  void dictionary(thrift.PageHeader header, Uint8List data) {
    _seen = true;
    _dictionary = null;
    final count =
        header.dictionaryPageHeader?.numValues ??
        (throw const FormatException('Dictionary page without its header'));
    if (count < 0 || count > data.length ~/ 4) {
      throw FormatException('Implausible dictionary size $count');
    }
    _reserve(count);
    _plain(data, count);
    final entries = _Strings(nullable: false);
    for (var i = 0; i < count; i++) {
      entries.add(data, _starts[i], _lengths[i]);
    }
    _dictionary = entries;
  }

  void page(PageBody body, _Strings out) {
    final defs = body.defLevels;
    final count = slots(body);
    final present = body.present;
    var expected = count;
    if (defs != null) {
      expected = 0;
      for (var i = 0; i < count; i++) {
        if (defs[i] == _maxDef) expected++;
      }
    }
    if (expected != present) {
      throw UnsupportedError('page levels disagree with its value count');
    }
    _reserve(present);

    // Locate each present value as a byte range of [source].
    final Uint8List source;
    switch (body.encoding) {
      case thrift.Encoding.plain:
        source = body.valueData;
        _plain(source, present);
      case thrift.Encoding.plainDictionary || thrift.Encoding.rleDictionary:
        final entries =
            _dictionary ??
            (_seen
                ? throw UnsupportedError('dictionary is not usable text')
                : throw const FormatException(
                    'Dictionary encoding without dictionary page',
                  ));
        if (_indices.length < present) _indices = Int32List(present);
        final decoded = _indexer.decodeIndicesInto(
          body.valueData,
          present,
          _indices,
        );
        if (decoded < present) {
          throw FormatException(
            'Dictionary index run shorter than declared: $decoded < $present',
          );
        }
        source = entries.bytes;
        for (var i = 0; i < present; i++) {
          final index = _indices[i];
          if (index < 0 || index >= entries.rows) {
            throw FormatException('Dictionary index out of bounds: $index');
          }
          _starts[i] = entries.offsets[index];
          _lengths[i] = entries.offsets[index + 1] - _starts[i];
        }
      case thrift.Encoding.deltaLengthByteArray:
        source = body.valueData;
        _deltaLengths(source, present);
      default:
        throw UnsupportedError('UTF-8 fast path: ${body.encoding}');
    }

    var next = 0;
    for (var i = 0; i < count; i++) {
      if (defs == null || defs[i] == _maxDef) {
        out.add(source, _starts[next], _lengths[next]);
        next++;
      } else {
        out.addNull();
      }
    }
  }

  void _reserve(int count) {
    if (_starts.length >= count) return;
    _starts = Int32List(count);
    _lengths = Int32List(count);
  }

  /// Ranges of the first [count] PLAIN byte arrays in [data] (each a 4-byte
  /// little-endian length then the bytes), each checked as UTF-8.
  void _plain(Uint8List data, int count) {
    final view = ByteData.sublistView(data);
    var at = 0;
    for (var i = 0; i < count; i++) {
      if (at + 4 > data.length) {
        throw UnsupportedError('PLAIN byte arrays run past the page');
      }
      final length = view.getInt32(at, Endian.little);
      at += 4;
      if (length < 0 || at + length > data.length) {
        throw UnsupportedError('PLAIN byte array length $length');
      }
      if (!validUtf8(data, at, at + length)) {
        throw UnsupportedError('malformed UTF-8');
      }
      _starts[i] = at;
      _lengths[i] = length;
      at += length;
    }
  }

  /// Ranges of the first [count] DELTA_LENGTH_BYTE_ARRAY values in [data]
  /// (a DELTA_BINARY_PACKED stream of lengths, then the bytes end to end),
  /// each checked as UTF-8.
  void _deltaLengths(Uint8List data, int count) {
    if (count == 0) return;
    final stream = ByteReader(data, label: 'DELTA_BINARY_PACKED stream');
    final sizes = DeltaBinaryPackedDecoder.decodeFrom(stream, count);
    if (sizes.length < count) {
      throw UnsupportedError('DELTA_LENGTH_BYTE_ARRAY lengths run short');
    }
    var at = stream.position;
    for (var i = 0; i < count; i++) {
      final length = sizes[i];
      if (length < 0 || at + length > data.length) {
        throw UnsupportedError('DELTA_LENGTH_BYTE_ARRAY length $length');
      }
      if (!validUtf8(data, at, at + length)) {
        throw UnsupportedError('malformed UTF-8');
      }
      _starts[i] = at;
      _lengths[i] = length;
      at += length;
    }
  }
}
