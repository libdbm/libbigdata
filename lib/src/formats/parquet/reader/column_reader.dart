import 'dart:typed_data';
import '../thrift/parquet_thrift.dart' as thrift;
import '../parquet_reader.dart';
import '../encoding/page_decoder.dart';
import '../schema/schema_levels.dart';
import '../schema/schema_converter.dart';
import '../../../core/column.dart';
import '../../../core/conversions.dart';
import '../../../core/data_type.dart';
import 'file_reader.dart';
import 'page_reader.dart';
import 'dremel_assembler.dart' show RawLeaf;

/// Initial/step size (values) for the incremental typed-buffer allocation, so a
/// large or adversarial footer numValues never forces a huge upfront alloc.
const int _allocChunk = 1 << 16; // 64K

/// A typed-buffer kind for the numeric fast path.
class _TypedKind {
  final DataType dtype;
  final TypedData Function(int length) make;
  const _TypedKind(this.dtype, this.make);
}

/// Reads a column from a Parquet file into a [Column].
class ColumnReader {
  final FileReader reader;
  final thrift.ColumnChunk chunk;
  final thrift.SchemaElement element;
  final ParquetReaderOptions options;
  final List<thrift.SchemaElement>? allElements;
  final int? elementIndex;
  final String name;

  ColumnReader(
    this.reader,
    this.chunk,
    this.element,
    this.options, {
    required this.name,
    this.allElements,
    this.elementIndex,
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

    int? maxDef;
    int? maxRep;
    if (allElements != null && elementIndex != null) {
      // A level-calculation failure means we'd mis-read nullability/repetition
      // (silently wrong data), so let it surface rather than defaulting.
      final levels = SchemaLevels.calculateLevels(allElements!, elementIndex!);
      maxDef = levels.maxDefinition;
      maxRep = levels.maxRepetition;
    }
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
    return _finalize(
      ListColumn(
        name: name,
        dataType: dataType,
        data: values,
        // Tolerant truncation can inject nulls into a declared-required column;
        // the data is authoritative, so mark it nullable when nulls are present.
        nullable: nullable || values.any((v) => v == null),
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
    int maxDef = 0, maxRep = 0;
    if (allElements != null && elementIndex != null) {
      final levels = SchemaLevels.calculateLevels(allElements!, elementIndex!);
      maxDef = levels.maxDefinition;
      maxRep = levels.maxRepetition;
    }

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
    int maxDef = 0, maxRep = 0;
    if (allElements != null && elementIndex != null) {
      final levels = SchemaLevels.calculateLevels(allElements!, elementIndex!);
      maxDef = levels.maxDefinition;
      maxRep = levels.maxRepetition;
    }
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
    // high bit set came back negative — reinterpret to the unsigned value
    // (uint8/16/32 stay int; uint64 becomes a BigInt decimal — lossless).
    final unsigned = SchemaConverter.unsignedInfo(element);
    if (unsigned != null) {
      return _mapLogical(
        column,
        unsigned.type,
        (v) => SchemaConverter.reinterpretUnsigned(v as int, unsigned.bits),
      );
    }
    if (logical == DataType.date) {
      return _mapLogical(column, logical, (v) => dateFromEpochDays(v as int));
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
        (v) => int96
            ? int96ToTimestamp(v as List<int>)
            : millis
            ? timestampFromEpochMillis(v as int)
            : nanos
            ? timestampFromEpochNanos(v as int)
            : timestampFromEpochMicros(v as int),
      );
    }
    if (logical == DataType.decimal) {
      return _mapLogical(
        column,
        logical,
        (v) => v is int
            ? BigInt.from(v)
            : bytesToBigInt((v as List).cast<int>(), littleEndian: false),
      );
    }
    // TINYINT/SMALLINT decode through the INT32 physical fast path, so narrow
    // the column to its declared logical width — otherwise the column stays
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
    final n = column.length;
    final buffer = type == DataType.int8 ? Int8List(n) : Int16List(n);
    if (column is NumericColumn) {
      buffer.setRange(0, n, column.rawBuffer as List<int>);
      return NumericColumn.adopt(
        name: column.name,
        dataType: type,
        data: buffer,
        nullBitmap: column.rawNullBitmap,
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

  /// Map each non-null value through [conv] into a [logical]-typed column. A
  /// conversion error fails loudly unless tolerant mode coerces it to null.
  Column _mapLogical(
    Column column,
    DataType logical,
    dynamic Function(dynamic) conv,
  ) {
    final n = column.length;
    final out = List<dynamic>.filled(n, null);
    for (int i = 0; i < n; i++) {
      final v = column[i];
      if (v == null) continue;
      try {
        out[i] = conv(v);
      } catch (e) {
        if (!options.tolerateTruncatedPages) {
          throw FormatException(
            'Cannot represent ${logical.name} value at row $i in column '
            '"${column.name}": $e (set tolerateTruncatedPages to coerce to null)',
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
        return _TypedKind(DataType.int32, (n) => Int32List(n));
      case thrift.ParquetType.int64:
        return _TypedKind(DataType.int64, (n) => Int64List(n));
      case thrift.ParquetType.float:
        return _TypedKind(DataType.float32, (n) => Float32List(n));
      case thrift.ParquetType.double:
        return _TypedKind(DataType.float64, (n) => Float64List(n));
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
    final n = metadata.numValues;
    if (n < 0) {
      throw FormatException('Parquet column "$name": negative numValues $n');
    }
    // Allocate incrementally rather than trusting the footer's numValues up
    // front: an adversarial numValues (e.g. 2^40 in a tiny file) must not force
    // a huge typed allocation before any page is read. Pages are themselves
    // value-capped (round-10), so growth tracks actual decoded data.
    var cap = n < _allocChunk ? n : _allocChunk;
    var buffer = kind.make(cap);
    var out = buffer as List<num>;
    var bitmap = nullable ? Uint8List((cap + 7) ~/ 8) : null;

    void ensure(int needed) {
      if (needed <= cap) return;
      var newCap = cap == 0 ? _allocChunk : cap * 2;
      while (newCap < needed) {
        newCap *= 2;
      }
      if (newCap > n) newCap = n; // never exceed the declared chunk size
      final grown = kind.make(newCap) as List<num>;
      grown.setRange(0, out.length, out);
      buffer = grown as TypedData;
      out = grown;
      if (bitmap != null) {
        final nb = Uint8List((newCap + 7) ~/ 8);
        nb.setRange(0, bitmap!.length, bitmap!);
        bitmap = nb;
      }
      cap = newCap;
    }

    final pageReader = PageReader(
      reader,
      startOffset,
      totalSize,
      metadata.codec,
      verify: options.verifyChecksums,
      maxPageBytes: options.maxPageBytes,
    );
    final pd = PageDecoder(
      type: element.type!,
      schema: element,
      maxDefinitionLevel: maxDef,
      maxRepetitionLevel: 0,
      tolerant: options.tolerateTruncatedPages,
    );

    int row = 0;
    while (row < n) {
      final page = await pageReader.readNext();
      if (page == null) break;
      if (page.header.type == thrift.PageType.dictionaryPage) {
        pd.decode(page.header, page.data); // populate dictionary
        continue;
      }
      final body = pd.parseBody(page.header, page.data);
      final defs = body.defLevels;
      if (defs == null) {
        // Required: every slot present, decode straight into place. A page that
        // declares more values than the column's total would push the write
        // past the (n-capped) buffer — reject it rather than RangeError.
        if (row + body.numValues > n) {
          throw FormatException(
            'Parquet column "$name": page declares ${body.numValues} values '
            'but only ${n - row} of $n remain',
          );
        }
        ensure(row + body.numValues);
        pd.decodeValuesTyped(
          body.valueData,
          body.present,
          body.encoding,
          out,
          row,
        );
        row += body.numValues;
      } else {
        ensure(row + defs.length);
        final temp = kind.make(body.present) as List<num>;
        pd.decodeValuesTyped(
          body.valueData,
          body.present,
          body.encoding,
          temp,
          0,
        );
        int p = 0;
        for (int i = 0; i < defs.length && row < n; i++) {
          if (defs[i] == maxDef) {
            out[row] = temp[p++];
          } else {
            bitmap![row >> 3] |= 1 << (row & 7);
          }
          row++;
        }
      }
    }

    // Pages ending before numValues means the chunk is truncated; the untouched
    // typed-buffer tail would otherwise read as legitimate zeros.
    if (row != n) {
      if (!options.tolerateTruncatedPages) {
        throw FormatException(
          'Parquet column "$name": decoded $row of $n values (truncated chunk)',
        );
      }
      // Tolerant: the tail is UNKNOWN, not zero. Zeros are indistinguishable
      // from data the file really held, so mark the padding null — which is
      // what the boxed path has always done for the same option. A required
      // column needs a bitmap allocated for it; the reader promotes such a
      // column to nullable because the data is authoritative.
      ensure(n);
      final nulls = bitmap ??= Uint8List((n + 7) ~/ 8);
      for (int i = row; i < n; i++) {
        nulls[i >> 3] |= 1 << (i & 7);
      }
    }

    return NumericColumn.adopt(
      name: name,
      dataType: kind.dtype,
      data: buffer,
      nullBitmap: bitmap,
    );
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

    final repLevels = <int>[];
    final defLevels = <int>[];
    while (true) {
      final page = await pageReader.readNext();
      if (page == null) break;
      final decoded = pageDecoder.decode(page.header, page.data);
      if (page.header.type == thrift.PageType.dictionaryPage) {
        continue;
      }
      values.addAll(decoded.values);
      repLevels.addAll(
        decoded.repLevels ?? List.filled(decoded.values.length, 0),
      );
      defLevels.addAll(
        decoded.defLevels ?? List.filled(decoded.values.length, maxDef ?? 0),
      );
      if ((maxRep ?? 0) == 0 && values.length >= metadata.numValues) {
        break;
      }
    }

    if ((maxRep ?? 0) > 0) {
      final elemOptional =
          element.repetitionType == thrift.FieldRepetitionType.optional;
      return assemble(values, repLevels, defLevels, maxDef ?? 1, elemOptional);
    }
    if (values.length > metadata.numValues) {
      return values.sublist(0, metadata.numValues);
    }
    // A short flat column means the chunk is truncated. Fail rather than return
    // a silently shortened column — or, in tolerant mode, pad the tail with
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
  /// mean the list itself is empty or null.
  List<dynamic> assemble(
    List<dynamic> values,
    List<int> repLevels,
    List<int> defLevels,
    int maxDef,
    bool elemOptional,
  ) {
    final rows = <dynamic>[];
    List<dynamic>? current;
    for (int i = 0; i < values.length; i++) {
      if (repLevels[i] == 0) {
        if (current != null) rows.add(current);
        current = <dynamic>[];
      }
      final def = defLevels[i];
      if (def == maxDef) {
        current!.add(values[i]);
      } else if (elemOptional && def == maxDef - 1) {
        // Present-but-null element (only when the leaf itself is optional; for a
        // required element this level instead means an empty collection).
        current!.add(null);
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

  final List<dynamic> _values = [];
  final List<int> _rep = []; // nested only
  final List<int> _def = []; // nested only
  final List<int> _rec0 = []; // value indices where repLevel==0 (nested only)

  ColumnCursor(this._owner, this._metadata, this._maxDef, this._maxRep) {
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

  Future<void> _pull() async {
    if (_exhausted) return;
    final page = await _pageReader.readNext();
    if (page == null) {
      _exhausted = true;
      return;
    }
    final decoded = _pageDecoder.decode(page.header, page.data);
    if (page.header.type == thrift.PageType.dictionaryPage) return;
    final base = _values.length;
    _values.addAll(decoded.values);
    if (_nested) {
      final rl = decoded.repLevels ?? List.filled(decoded.values.length, 0);
      final dl =
          decoded.defLevels ?? List.filled(decoded.values.length, _maxDef);
      for (int i = 0; i < rl.length; i++) {
        if (rl[i] == 0) _rec0.add(base + i);
      }
      _rep.addAll(rl);
      _def.addAll(dl);
    }
  }

  /// Rows safe to emit now. Flat: every pending value. Nested: complete records
  /// only — the last started record stays pending until a new record-start or
  /// end-of-chunk arrives.
  int get readyRows {
    if (!_nested) return _values.length;
    if (_exhausted) return _rec0.length;
    return _rec0.isEmpty ? 0 : _rec0.length - 1;
  }

  bool get done => _exhausted && readyRows == 0;

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
  /// drop this column's tail — the eager reader pads each column to its own
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
    if (!_nested) {
      final slice = _values.sublist(0, rows);
      _values.removeRange(0, rows);
      return _buildFlat(slice);
    }
    final valueEnd = rows < _rec0.length ? _rec0[rows] : _values.length;
    final v = _values.sublist(0, valueEnd);
    final r = _rep.sublist(0, valueEnd);
    final d = _def.sublist(0, valueEnd);
    _values.removeRange(0, valueEnd);
    _rep.removeRange(0, valueEnd);
    _def.removeRange(0, valueEnd);
    _rec0.removeRange(0, rows);
    for (int i = 0; i < _rec0.length; i++) {
      _rec0[i] -= valueEnd;
    }
    final elemOptional =
        _owner.element.repetitionType == thrift.FieldRepetitionType.optional;
    final assembled = _owner.assemble(v, r, d, _maxDef, elemOptional);
    return ListColumn(
      name: _owner.name,
      dataType: _owner._convertType(),
      data: assembled,
    );
  }

  /// A [Column] of exactly [rows] NULLs matching this column's type, used to
  /// pad the tail of a truncated tolerant chunk up to the row group's declared
  /// row count — the same reconciliation the eager typed reader does — so the
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
        ? _numericFrom(kind.dtype, nulls, true)
        : ListColumn(
            name: _owner.name,
            dataType: _owner._convertType(),
            data: nulls,
            nullable: true,
          );
    return _owner._finalize(column);
  }

  Column _buildFlat(List<dynamic> values) {
    final kind = _owner._typedKind(_owner.element.type!);
    final Column column;
    if (kind != null) {
      column = _numericFrom(kind.dtype, values, _maxDef > 0);
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

  Column _numericFrom(DataType dtype, List<dynamic> values, bool nullable) {
    switch (dtype) {
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
