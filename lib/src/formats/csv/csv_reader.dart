import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import '../../core/conversions.dart' show inferDecimalMeta, toUnscaled;
import '../../core/encoding.dart';
import '../../core/column.dart';
import '../../core/data_type.dart';
import '../../core/limits.dart';
import '../../core/schema.dart';
import '../../core/table.dart';
import '../../util/lines.dart';
import 'csv_options.dart';

/// CSV reader that handles both strings and files with a common streaming approach
///
/// Key design principles:
/// 1. Common line stream abstraction for all input sources
/// 2. Upfront schema inference (parse first N lines)
/// 3. Reset and stream through entire dataset with known schema
/// 4. Optimized for both small strings and large files
class CsvReader {
  final CsvOptions options;
  CsvReader({CsvOptions? options}) : options = options ?? const CsvOptions() {
    // The parser reads only the first code unit of delimiter/quote/escape; a
    // multi-char value silently mismatches and an empty one throws a RangeError.
    // Validate up front so a bad config is an actionable error.
    this.options.validate();
  }

  /// Read CSV from file path
  Future<Table> readFile(
    String path, {
    String? filter,
    List<String>? selectColumns,
    int? limit,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) async {
    final file = File(path);
    if (!await file.exists()) {
      throw FileSystemException('File not found', path);
    }

    // Optional byte ceiling on the eager whole-file read.
    final cap = options.maxFileBytes;
    if (cap != null) {
      final size = await file.length();
      if (size > cap) {
        throw FormatException(
          'CSV file is $size bytes, exceeding maxFileBytes ($cap); '
          'use readBatches for streaming',
        );
      }
    }

    // Safe by default: stream via readBatches and stop at the cap so a giant CSV
    // can't OOM. Custom line terminators aren't supported by readBatches, so
    // fall through to the eager whole-file read for those.
    if ((maxRows != null || maxBytes != null) &&
        options.lineTerminator == '\n') {
      final tables = <Table>[];
      var rows = 0;
      var bytes = 0;
      await for (final t in readBatches(
        path,
        filter: filter,
        selectColumns: selectColumns,
        limit: limit,
      )) {
        rows += t.rowCount;
        bytes += t.memoryUsage;
        // The byte gate is overhead-adjusted so the cap predicts true peak
        // (object overhead + transient concat), not just raw buffer bytes.
        enforceMaterializationCaps(rows, bytes, maxRows, maxBytes, 'CSV');
        tables.add(t);
      }
      return _concatTables(tables);
    }

    // Eager whole-file read (custom line terminator — readBatches can't stream
    // it). The streaming caps above don't apply here, so enforce the byte cap up
    // front via the file size; otherwise a huge custom-terminator file would
    // OOM in readAsString despite the protective default cap. (maxRows alone
    // can't be enforced without parsing, so maxBytes is the guard.)
    if (maxBytes != null) {
      final size = await file.length();
      if (size > maxBytes) {
        throw StateError(
          'CSV file is $size bytes, exceeding maxBytes=$maxBytes; pass '
          'maxBytes: null to allow it, or use the default "\\n" terminator for '
          'bounded-memory streaming',
        );
      }
    }

    final content = await file.readAsString(encoding: _encoding());
    return _processWithSource(
      _toRecordsList(content),
      filter: filter,
      selectColumns: selectColumns,
      limit: limit,
    );
  }

  /// Concatenate same-schema CSV batch tables into one (empty schema if none).
  Table _concatTables(List<Table> tables) {
    if (tables.isEmpty) return Table.empty('csv', Schema(fields: const []));
    if (tables.length == 1) return tables.first;
    final fields = tables.first.schema.fields;
    return Table(
      name: 'csv',
      schema: tables.first.schema,
      columns: [
        for (final f in fields)
          Column.concat([for (final t in tables) t.column(f.name)!]),
      ],
    );
  }

  /// Read CSV from string content. The input string is already in the caller's
  /// memory, so [maxRows]/[maxBytes] bound the parsed result (fail loud rather
  /// than materialize an oversized DataFrame). Pass null to opt out.
  Future<Table> readString(
    String csvContent, {
    String? filter,
    List<String>? selectColumns,
    int? limit,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) async {
    if (csvContent.isEmpty) {
      return Table.empty('csv_data', Schema(fields: []));
    }
    if (maxBytes != null && csvContent.length > maxBytes) {
      throw StateError(
        'CSV exceeds maxBytes=$maxBytes; pass maxBytes: null to allow it',
      );
    }

    final records = _toRecordsList(csvContent);
    if (maxRows != null && records.length > maxRows) {
      throw StateError(
        'CSV exceeds maxRows=$maxRows; pass maxRows: null to allow it or use '
        'readBatches for bounded-memory streaming',
      );
    }

    return _processWithSource(
      records,
      filter: filter,
      selectColumns: selectColumns,
      limit: limit,
    );
  }

  /// Count the structural quote characters in [line], skipping a quote escaped
  /// by a distinct escape char (so escaped quotes don't toggle the open-quote
  /// state used to merge quote-spanning records).
  /// Whether [line] leaves a quoted field OPEN at its end, given whether one was
  /// already [open] when the line began.
  ///
  /// This applies the SAME rule the field parser does: a quote only QUOTES at
  /// the start of a field, so `x,5" nails` is two plain fields and its lone
  /// quote is literal text. Counting every quote in the line instead — which is
  /// what this used to do — made the splitter and the parser disagree: an odd
  /// count swallowed the following line into the record (and then threw on the
  /// field count), and a run to EOF threw "Unterminated quoted field", for input
  /// Python/pandas/Excel all read without complaint.
  bool _leavesQuoteOpen(String line, bool open) {
    final quoteCu = options.quote.codeUnitAt(0);
    final escCu = options.escape.codeUnitAt(0);
    final delimCu = options.delimiter.codeUnitAt(0);
    final escapeIsQuote = escCu == quoteCu;
    final length = line.length;
    int pos = 0;

    // Resuming a quoted field carried over from a previous physical line.
    var quoted = open;
    if (!quoted) {
      // At a field start: a leading quote opens a quoted field.
      if (pos < length && line.codeUnitAt(pos) == quoteCu) {
        quoted = true;
        pos++;
      }
    }

    while (pos < length) {
      final cu = line.codeUnitAt(pos);
      if (quoted) {
        if (!escapeIsQuote && cu == escCu && pos + 1 < length) {
          pos += 2; // escape consumes the next char, quote included
          continue;
        }
        if (cu == quoteCu) {
          if (escapeIsQuote &&
              pos + 1 < length &&
              line.codeUnitAt(pos + 1) == quoteCu) {
            pos += 2; // "" is an escaped quote, not a close
            continue;
          }
          quoted = false; // closes the field
        }
        pos++;
        continue;
      }
      // Unquoted: only a delimiter matters; a quote here is literal text.
      pos++;
      if (cu != delimCu) continue;
      if (pos < length && line.codeUnitAt(pos) == quoteCu) {
        quoted = true; // the NEXT field starts quoted
        pos++;
      }
    }
    return quoted;
  }

  /// Split [content] into logical CSV records: a record spans line breaks while
  /// a quoted field is open (RFC 4180 §2.6). A line with no quote char is a
  /// record by itself (the common case), avoiding the StringBuffer merge.
  List<String> _toRecordsList(String content) {
    // LineSplitter handles \n / \r / \r\n; a custom terminator splits literally.
    final lines = options.lineTerminator == '\n'
        ? const LineSplitter().convert(content)
        : content.split(options.lineTerminator);
    final quote = options.quote;
    final records = <String>[];
    final buffer = StringBuffer();
    bool open = false;
    int start = 0;
    int n = 0;
    for (final line in lines) {
      n++;
      if (!open && !line.contains(quote)) {
        records.add(line);
        continue;
      }
      final stillOpen = _leavesQuoteOpen(line, open);
      if (!open && !stillOpen) {
        records.add(line); // a self-contained record, quotes and all
        continue;
      }
      if (open) {
        buffer.write('\n');
      } else {
        start = n;
      }
      buffer.write(line);
      open = stillOpen;
      if (!open) {
        records.add(buffer.toString());
        buffer.clear();
      } else if (buffer.length > options.maxRecordBytes) {
        throw FormatException(
          'Unterminated quoted field starting at line $start exceeds '
          'maxRecordBytes (${options.maxRecordBytes})',
        );
      }
    }
    if (open) {
      throw FormatException(
        'Unterminated quoted field starting at line $start',
      );
    }
    return records;
  }

  /// Process all CSV [records] in memory. The data Table is materialised fully
  /// anyway, so a single bulk read beats streaming and avoids re-reading for
  /// the inference sample.
  Future<Table> _processWithSource(
    List<String> records, {
    String? filter,
    List<String>? selectColumns,
    int? limit,
  }) async {
    records = _applySkip(records);
    limit = _effLimit(limit);

    // Explicit schema: skip inference.
    if (options.schema != null) {
      return _processWithSchema(
        records,
        options.schema!,
        filter: filter,
        selectColumns: selectColumns,
        limit: limit,
      );
    }

    if (records.isEmpty) {
      return Table.empty('csv_data', Schema(fields: []));
    }

    // infer the schema from the first N records.
    final parser = _UnifiedCsvParser(options);
    final sampleCount = options.typeInferenceSampleSize + 1; // +1 for header
    final schemaRows = <List<String?>>[];
    for (int i = 0; i < records.length && i < sampleCount; i++) {
      final row = parser.parseLine(records[i]);
      if (row != null) schemaRows.add(row);
    }

    if (schemaRows.isEmpty) {
      return Table.empty('csv_data', Schema(fields: []));
    }

    // Extract headers and determine schema
    List<String> headers;
    List<List<String?>> sampleDataRows;

    if (options.hasHeader) {
      // Index by POSITION: `indexOf(null)` always returns the first null's index,
      // so two empty header cells would collapse to the same synthetic name.
      final first = schemaRows.first;
      headers = [
        for (int i = 0; i < first.length; i++) first[i] ?? 'column_$i',
      ];
      sampleDataRows = schemaRows.skip(1).toList();
    } else {
      headers = _autoHeaders(schemaRows.first.length);
      sampleDataRows = schemaRows;
    }

    // Apply column selection
    final (selectedHeaders, selectedIndices) = _selectColumns(
      headers,
      selectColumns,
    );

    // Infer schema from sample data. Integer columns widen to int64 so the
    // eager readFile path yields the SAME schema as the streaming readBatches
    // path (which must use int64 since it can't see all rows up front).
    final inferred = _inferSchema(
      selectedHeaders,
      sampleDataRows,
      selectedIndices,
    );
    final schema = inferred;

    // Compile filter predicate
    _FilterPredicate? filterPredicate;
    if (filter != null) {
      filterPredicate = _FilterPredicate.compile(filter);
    }

    // Initialize typed column builders with the inferred schema.
    final columnBuilders = selectedHeaders.asMap().entries.map((entry) {
      final field = schema.fields[entry.key];
      return _ColumnBuilder.forType(
        entry.value,
        field.dataType,
        field.nullable,
        options,
        field.scale ?? 0,
      );
    }).toList();

    // Process all records.
    _consume(
      records,
      parser,
      columnBuilders,
      selectedIndices,
      headers,
      filterPredicate,
      limit,
    );

    // Build final columns; reconcile the schema to the built column types (a
    // numeric column may have widened past the inferred starting width).
    final columns = columnBuilders.map((builder) => builder.build()).toList();
    return Table(
      name: 'csv_data',
      columns: columns,
      schema: _schemaOf(columns, schema.fields),
    );
  }

  /// Build a [Schema] from the actual built columns, keeping each decimal's
  /// scale/precision from its aligned source [fields] entry. Rebuilding from
  /// the columns alone dropped the scale, so the eager paths rendered a
  /// decimal's unscaled BigInt at scale 0 (2.25 read back as 225) while the
  /// streaming path — which keeps the inferred schema — was correct.
  Schema _schemaOf(List<Column> columns, List<Field> fields) => Schema(
    fields: [
      for (int i = 0; i < columns.length; i++)
        Field(
          name: columns[i].name,
          dataType: columns[i].dataType,
          nullable: columns[i].nullable,
          precision: columns[i].dataType == DataType.decimal
              ? fields[i].precision
              : null,
          scale: columns[i].dataType == DataType.decimal
              ? fields[i].scale
              : null,
        ),
    ],
  );

  /// Drive [records] into the column [builders]. Records with no quote char and
  /// no active filter take a fast push path (no per-row List, ints parsed
  /// straight from code units); quoted or filtered records use [parseLine].
  void _consume(
    List<String> records,
    _UnifiedCsvParser parser,
    List<_ColumnBuilder> builders,
    List<int> selectedIndices,
    List<String> allHeaders,
    _FilterPredicate? filterPredicate,
    int? limit,
  ) {
    final colToBuilder = _colToBuilder(selectedIndices);
    int processed = 0;
    bool isFirstLine = options.hasHeader;
    for (final line in records) {
      if (isFirstLine) {
        isFirstLine = false;
        continue;
      }
      if (_pushRecord(
        line,
        parser,
        builders,
        colToBuilder,
        selectedIndices,
        allHeaders,
        filterPredicate,
      )) {
        processed++;
        if (limit != null && processed >= limit) break;
      }
    }
  }

  /// Map each selected source column index -> its builder index.
  List<int> _colToBuilder(List<int> selectedIndices) {
    int maxCol = 0;
    for (final ci in selectedIndices) {
      if (ci + 1 > maxCol) maxCol = ci + 1;
    }
    final colToBuilder = List<int>.filled(maxCol, -1);
    for (int j = 0; j < selectedIndices.length; j++) {
      colToBuilder[selectedIndices[j]] = j;
    }
    return colToBuilder;
  }

  /// Parse one record into the [builders]; returns whether a data row was added
  /// (false for blank/comment/filtered-out records). Shared by the whole-file
  /// and streaming paths.
  bool _pushRecord(
    String line,
    _UnifiedCsvParser parser,
    List<_ColumnBuilder> builders,
    List<int> colToBuilder,
    List<int> selectedIndices,
    List<String> allHeaders,
    _FilterPredicate? filterPredicate,
  ) {
    if (line.isEmpty) {
      if (options.skipEmptyLines) return false;
      // skipEmptyLines:false — emit an all-null row for the blank line.
      for (final b in builders) {
        b.addValue(null);
      }
      return true;
    }
    final comment = options.comment;
    if (comment != null && line.startsWith(comment)) return false;
    final expected = allHeaders.length;

    if (filterPredicate == null && !line.contains(options.quote)) {
      final n = parser.parseUnquotedInto(line, builders, colToBuilder);
      _checkWidth(n, expected, line);
      return true;
    }
    final row = parser.parseLine(line);
    if (row == null) return false;
    if (filterPredicate != null && !filterPredicate.evaluate(row, allHeaders)) {
      return false;
    }
    _checkWidth(row.length, expected, line);
    for (int j = 0; j < selectedIndices.length; j++) {
      final colIndex = selectedIndices[j];
      builders[j].addValue(colIndex < row.length ? row[colIndex] : null);
    }
    return true;
  }

  /// Enforce a fixed row width unless [CsvOptions.allowVariableFields] is set.
  void _checkWidth(int actual, int expected, String line) {
    if (!options.allowVariableFields && expected > 0 && actual != expected) {
      throw FormatException(
        'CSV row has $actual fields, expected $expected '
        '(set allowVariableFields: true to permit ragged rows): $line',
      );
    }
  }

  /// Read using an explicit [schema] (no inference).
  Future<Table> _processWithSchema(
    List<String> records,
    Schema schema, {
    String? filter,
    List<String>? selectColumns,
    int? limit,
  }) async {
    final parser = _UnifiedCsvParser(options);
    final allHeaders = schema.fields.map((f) => f.name).toList();
    final (selectedHeaders, selectedIndices) = _selectColumns(
      allHeaders,
      selectColumns,
    );

    final builders = [
      for (int i = 0; i < selectedIndices.length; i++)
        _ColumnBuilder.forType(
          selectedHeaders[i],
          schema.fields[selectedIndices[i]].dataType,
          schema.fields[selectedIndices[i]].nullable,
          options,
          schema.fields[selectedIndices[i]].scale ?? 0,
        ),
    ];

    final filterPredicate = filter != null
        ? _FilterPredicate.compile(filter)
        : null;

    _consume(
      records,
      parser,
      builders,
      selectedIndices,
      allHeaders,
      filterPredicate,
      limit,
    );

    final columns = [for (final b in builders) b.build()];
    return Table(
      name: 'csv_data',
      columns: columns,
      schema: _schemaOf(columns, [
        for (final i in selectedIndices) schema.fields[i],
      ]),
    );
  }

  /// Stream a CSV file as [Table] batches of ~[size] rows, holding only one
  /// batch in memory. The schema is fixed up front — from [CsvOptions.schema] or
  /// a leading inference sample — and stays stable across every batch; inferred
  /// integer columns use int64 so a later batch can never overflow/drift.
  Stream<Table> readBatches(
    String path, {
    int size = 10000,
    String? filter,
    List<String>? selectColumns,
    int? limit,
  }) async* {
    if (size <= 0) throw ArgumentError.value(size, 'size', 'must be > 0');
    final file = File(path);
    if (!await file.exists()) {
      throw FileSystemException('File not found', path);
    }
    // The streaming line split relies on LineSplitter (\n / \r / \r\n); a custom
    // terminator would need a different splitter, so fail loud rather than
    // silently ignore it (the eager readFile path does honor it).
    if (options.lineTerminator != '\n') {
      throw UnsupportedError(
        'readBatches supports only the default line terminator; use readFile '
        'for a custom lineTerminator',
      );
    }
    final eff = _effLimit(limit);
    final records = StreamIterator(
      // A capped splitter (not LineSplitter, which buffers a whole physical line
      // unbounded) so a single multi-GB unquoted cell can't bypass the record cap
      // and OOM — every physical line is bounded by maxRecordBytes before emission.
      _recordStream(
        cappedLines(
          file.openRead().transform(_encoding().decoder),
          options.maxRecordBytes,
          label: 'CSV record',
        ),
      ),
    );
    final parser = _UnifiedCsvParser(options);
    final filterPredicate = filter != null
        ? _FilterPredicate.compile(filter)
        : null;

    try {
      // Drop preamble rows before the header.
      for (var i = 0; i < options.skipRows; i++) {
        if (!await records.moveNext()) return;
      }
      // Resolve headers (and discard the file header line when a schema is given).
      List<String> headers;
      if (options.schema != null) {
        headers = options.schema!.fields.map((f) => f.name).toList();
        if (options.hasHeader) await records.moveNext();
      } else if (options.hasHeader) {
        if (!await records.moveNext()) return;
        headers = _headerNames(parser.parseLine(records.current));
      } else {
        headers = const [];
      }

      // Leading sample for inference; kept to feed the first batch.
      final sample = <String>[];
      while (sample.length < options.typeInferenceSampleSize &&
          await records.moveNext()) {
        sample.add(records.current);
      }
      if (headers.isEmpty) {
        if (sample.isEmpty) return;
        headers = _autoHeaders(
          (parser.parseLine(sample.first) ?? const []).length,
        );
      }

      final (selHeaders, selIdx) = _selectColumns(headers, selectColumns);
      final selectedFields = _streamSchema(
        headers,
        selHeaders,
        selIdx,
        sample,
        parser,
      );
      final schema = Schema(fields: selectedFields);
      final colToBuilder = _colToBuilder(selIdx);

      var sampleIdx = 0;
      Future<String?> nextRecord() async {
        if (sampleIdx < sample.length) return sample[sampleIdx++];
        return await records.moveNext() ? records.current : null;
      }

      int emitted = 0;
      var done = false;
      while (!done) {
        final builders = [
          for (final f in selectedFields)
            _ColumnBuilder.forType(
              f.name,
              f.dataType,
              f.nullable,
              options,
              f.scale ?? 0,
            ),
        ];
        int batchRows = 0;
        while (batchRows < size) {
          final rec = await nextRecord();
          if (rec == null) {
            done = true;
            break;
          }
          if (_pushRecord(
            rec,
            parser,
            builders,
            colToBuilder,
            selIdx,
            headers,
            filterPredicate,
          )) {
            batchRows++;
            emitted++;
            if (eff != null && emitted >= eff) {
              done = true;
              break;
            }
          }
        }
        if (batchRows > 0) {
          yield Table(
            name: 'csv_data',
            columns: [for (final b in builders) b.build()],
            schema: schema,
          );
        }
      }
    } finally {
      await records.cancel();
    }
  }

  /// Logical-record stream: merges quote-spanning lines (RFC 4180 §2.6) the same
  /// way [_toRecordsList] does, but incrementally over a line stream.
  Stream<String> _recordStream(Stream<String> lines) async* {
    final quote = options.quote;
    final buffer = StringBuffer();
    bool open = false;
    int start = 0;
    int n = 0;
    await for (final line in lines) {
      n++;
      if (!open && !line.contains(quote)) {
        yield line;
        continue;
      }
      final stillOpen = _leavesQuoteOpen(line, open);
      if (!open && !stillOpen) {
        yield line; // a self-contained record, quotes and all
        continue;
      }
      if (open) {
        buffer.write('\n');
      } else {
        start = n;
      }
      buffer.write(line);
      open = stillOpen;
      if (!open) {
        yield buffer.toString();
        buffer.clear();
      } else if (buffer.length > options.maxRecordBytes) {
        // Fail fast: an unclosed quote would otherwise accumulate the rest of
        // the file before erroring at EOF.
        throw FormatException(
          'Unterminated quoted field starting at line $start exceeds '
          'maxRecordBytes (${options.maxRecordBytes})',
        );
      }
    }
    if (open) {
      throw FormatException(
        'Unterminated quoted field starting at line $start',
      );
    }
  }

  List<String> _headerNames(List<String?>? row) {
    final r = row ?? const <String?>[];
    return [
      for (int i = 0; i < r.length; i++)
        (r[i] == null || r[i]!.isEmpty) ? 'column_$i' : r[i]!,
    ];
  }

  /// Column names for a headerless file: explicit [CsvOptions.columnNames] when
  /// provided (length must match), otherwise `column_0..column_{n-1}`.
  List<String> _autoHeaders(int n) {
    final names = options.columnNames;
    if (names != null) {
      if (names.length != n) {
        throw ArgumentError(
          'columnNames has ${names.length} entries but the CSV has $n columns',
        );
      }
      return List.of(names);
    }
    return List.generate(n, (i) => 'column_$i');
  }

  /// The effective row cap: the smaller of the method `limit` and
  /// [CsvOptions.maxRows] (either may be null = unbounded).
  int? _effLimit(int? limit) {
    final m = options.maxRows;
    if (m == null) return limit;
    return limit == null ? m : (limit < m ? limit : m);
  }

  /// Drop [CsvOptions.skipRows] leading records (preamble before the header).
  List<String> _applySkip(List<String> records) {
    final s = options.skipRows;
    if (s <= 0) return records;
    return records.length > s ? records.sublist(s) : <String>[];
  }

  /// The fixed, selected schema for a streaming read. Explicit schema is used
  /// as-is; an inferred schema widens integer columns to int64 for stability.
  List<Field> _streamSchema(
    List<String> headers,
    List<String> selHeaders,
    List<int> selIdx,
    List<String> sample,
    _UnifiedCsvParser parser,
  ) {
    if (options.schema != null) {
      return [for (final i in selIdx) options.schema!.fields[i]];
    }
    final rows = [
      for (final rec in sample)
        if (parser.parseLine(rec) case final r?) r,
    ];
    return _inferSchema(selHeaders, rows, selIdx).fields;
  }

  /// Select columns and return headers and indices. A requested column that is
  /// not present is a hard error (no silent drop).
  (List<String>, List<int>) _selectColumns(
    List<String> allHeaders,
    List<String>? selectColumns,
  ) {
    if (selectColumns == null) {
      return (allHeaders, List.generate(allHeaders.length, (i) => i));
    }

    final missing = selectColumns
        .where((c) => !allHeaders.contains(c))
        .toList();
    if (missing.isNotEmpty) {
      throw ArgumentError(
        'Unknown column(s) in selection: ${missing.join(', ')}',
      );
    }
    final selectedIndices = selectColumns.map(allHeaders.indexOf).toList();
    return (selectColumns, selectedIndices);
  }

  /// Infer schema from sample data
  Schema _inferSchema(
    List<String> selectedHeaders,
    List<List<String?>> sampleRows,
    List<int> selectedIndices,
  ) {
    final fields = <Field>[];

    for (int i = 0; i < selectedIndices.length; i++) {
      final header = selectedHeaders[i];
      final columnIndex = selectedIndices[i];

      // Every sampled value counts. `sampleRows` is already bounded by
      // CsvOptions.typeInferenceSampleSize; a second hardcoded cap here made
      // that option inert and let a wider value just past the cap infer a
      // too-narrow type.
      final sampleValues = sampleRows
          .where((row) => columnIndex < row.length && row[columnIndex] != null)
          .map((row) => row[columnIndex]!)
          .where((value) => value.isNotEmpty)
          .toList();

      fields.add(_inferColumn(header, sampleValues));
    }

    return Schema(fields: fields);
  }

  /// Infer the [Field] for a column from its sample values. Returns a Field
  /// rather than a bare type because a decimal also carries precision/scale.
  Field _inferColumn(String name, List<String> sampleValues) {
    Field of(DataType type, bool nullable, {int? precision, int? scale}) =>
        Field(
          name: name,
          dataType: type,
          nullable: nullable,
          precision: precision,
          scale: scale,
        );
    if (sampleValues.isEmpty) {
      return of(DataType.string, true);
    }

    // A SAMPLE can never prove NON-nullability: a column fully populated in the
    // leading inference sample may still have an empty cell in a later row (past
    // the sample), which would then crash against a non-nullable typed builder.
    // Infer nullable; a caller wanting non-nullable pins it with an explicit
    // schema (which is enforced and bypasses inference).
    const nullable = true;

    // inferTypes:false keeps every column as raw strings (an explicit schema is
    // handled separately and bypasses inference entirely).
    if (!options.inferTypes) {
      return of(DataType.string, nullable);
    }

    // Check for boolean
    if (sampleValues.every(
      (s) => s.toLowerCase() == 'true' || s.toLowerCase() == 'false',
    )) {
      return of(DataType.boolean, nullable);
    }

    // Check for integers (locale thousands separators stripped first).
    if (sampleValues.every((s) => _decimalInt(_normNum(s, options)) != null)) {
      // int64 regardless of magnitude. A narrower width is not safe from a
      // SAMPLE: the streaming path cannot see every row, so a wider value past
      // the sample would throw against a too-narrow buffer. Both callers used to
      // widen this back to int64 anyway — the ladder only cost a second parse of
      // every sampled value and read as if the width mattered.
      return of(DataType.int64, nullable);
    }

    // Check for floats (locale decimal/thousands separators normalized first).
    if (sampleValues.every(
      (s) => double.tryParse(_normNum(s, options)) != null,
    )) {
      // A column mixing integers with floats promotes to a LOSSLESS decimal
      // rather than float64, which would round any integer past 2^53 — the same
      // rule Schema.infer applies via DataType.inferredCommon (so CSV and JSON
      // agree on identical data). All-float columns have no integer to lose and
      // stay float64. The scale comes from the values themselves; a non-finite
      // value can't be a decimal, so inferDecimalMeta returns null and the
      // column stays float64.
      final numbers = [
        for (final s in sampleValues) num.parse(_normNum(s, options)),
      ];
      if (numbers.any((v) => v is int)) {
        final meta = inferDecimalMeta(numbers);
        if (meta != null) {
          return of(
            DataType.decimal,
            nullable,
            precision: meta.precision,
            scale: meta.scale,
          );
        }
      }
      return of(DataType.float64, nullable);
    }

    // Temporal, only when a format is configured.
    if (options.timestampFormat != null &&
        sampleValues.every(
          (s) => parseTemporal(s, options.timestampFormat!) != null,
        )) {
      return of(DataType.timestamp, nullable);
    }
    if (options.dateFormat != null &&
        sampleValues.every(
          (s) => parseTemporal(s, options.dateFormat!) != null,
        )) {
      return of(DataType.date, nullable);
    }

    // Default to string
    return of(DataType.string, nullable);
  }

  Encoding _encoding() => resolveEncoding(options.encoding);
}

/// Simplified CSV parser that processes one line at a time
class _UnifiedCsvParser {
  final CsvOptions options;
  final int _quote;
  final int _delimiter;
  final int _escape;
  // When the escape char differs from the quote char, use backslash-style
  // escaping (escape + any char = literal) instead of RFC doubled-quotes.
  final bool _escapeIsQuote;

  _UnifiedCsvParser(this.options)
    : _quote = options.quote.codeUnitAt(0),
      _delimiter = options.delimiter.codeUnitAt(0),
      _escape = options.escape.codeUnitAt(0),
      _escapeIsQuote = options.escape == options.quote;

  /// Fast path for a line known to contain no quote characters: scan fields by
  /// delimiter and push each directly into its builder via a code-unit range
  /// (no per-row List, no substring for integer columns). [colToBuilder] maps a
  /// CSV column index to a builder index (-1 = not selected).
  /// Returns the number of CSV fields the line actually held (for row-width
  /// enforcement); missing trailing selected columns are still null-padded.
  int parseUnquotedInto(
    String line,
    List<_ColumnBuilder> builders,
    List<int> colToBuilder,
  ) {
    final length = line.length;
    final nCols = colToBuilder.length;
    int field = 0;
    int pos = 0;
    while (true) {
      final start = pos;
      while (pos < length && line.codeUnitAt(pos) != _delimiter) {
        pos++;
      }
      if (field < nCols) {
        final bi = colToBuilder[field];
        if (bi >= 0) builders[bi].addField(line, start, pos);
      }
      field++;
      if (pos >= length) break;
      pos++; // skip delimiter
      if (pos >= length) {
        // Trailing delimiter -> one more empty field.
        if (field < nCols) {
          final bi = colToBuilder[field];
          if (bi >= 0) builders[bi].addField(line, pos, pos);
        }
        field++;
        break;
      }
    }
    final seen = field; // actual field count, before null-padding
    // Missing trailing columns -> null.
    for (; field < nCols; field++) {
      final bi = colToBuilder[field];
      if (bi >= 0) builders[bi].addValue(null);
    }
    return seen;
  }

  /// Parse a single line into fields. Unquoted fields are sliced with a single
  /// [String.substring]; a [StringBuffer] is only used for quoted fields that
  /// actually contain escaped quotes. Trimming and nullValues are folded in.
  List<String?>? parseLine(String line) {
    if (line.isEmpty) return null;
    if (options.comment != null && line.startsWith(options.comment!)) {
      return null;
    }

    final fields = <String?>[];
    final length = line.length;
    int pos = 0;

    while (true) {
      if (pos < length && line.codeUnitAt(pos) == _quote) {
        final (value, end) = _parseQuoted(line, pos);
        fields.add(options.trimFields ? value.trim() : value);
        pos = end;
      } else {
        final start = pos;
        while (pos < length && line.codeUnitAt(pos) != _delimiter) {
          pos++;
        }
        fields.add(_finishUnquoted(line.substring(start, pos)));
      }

      if (pos >= length) break;
      // A field ends at a delimiter. Stray characters after a closing quote are
      // a malformed field: reject them in strict mode (the default); otherwise
      // treat them as ending the record (lenient, legacy behavior).
      if (line.codeUnitAt(pos) != _delimiter) {
        if (options.strict) {
          throw FormatException(
            'Malformed CSV: unexpected text after closing quote',
            line,
            pos,
          );
        }
        break;
      }
      pos++;
      if (pos >= length) {
        // Trailing delimiter -> one more empty field.
        fields.add(_finishUnquoted(''));
        break;
      }
    }

    return fields;
  }

  /// Finish an unquoted field: trim, then map nullValues to null.
  String? _finishUnquoted(String value) {
    final v = options.trimFields ? value.trim() : value;
    return options.isNullToken(v) ? null : v;
  }

  /// Parse a quoted field starting at the opening quote; returns the unquoted
  /// content and the position just past the closing quote.
  (String, int) _parseQuoted(String line, int start) {
    if (!_escapeIsQuote) return _parseQuotedCustomEscape(line, start);
    final length = line.length;
    final contentStart = start + 1;
    int pos = contentStart;
    while (pos < length) {
      if (line.codeUnitAt(pos) == _quote) {
        if (pos + 1 < length && line.codeUnitAt(pos + 1) == _quote) {
          // Escaped quote: fall back to the buffer path.
          return _parseQuotedEscaped(line, contentStart, pos);
        }
        return (line.substring(contentStart, pos), pos + 1);
      }
      pos++;
    }
    return (line.substring(contentStart), pos); // unclosed
  }

  /// Quoted field using a distinct escape char: `escape` + any char emits that
  /// char literally; an unescaped quote closes the field.
  (String, int) _parseQuotedCustomEscape(String line, int start) {
    final length = line.length;
    final buffer = StringBuffer();
    int pos = start + 1;
    while (pos < length) {
      final cu = line.codeUnitAt(pos);
      if (cu == _escape && pos + 1 < length) {
        buffer.writeCharCode(line.codeUnitAt(pos + 1));
        pos += 2;
      } else if (cu == _quote) {
        return (buffer.toString(), pos + 1);
      } else {
        buffer.writeCharCode(cu);
        pos++;
      }
    }
    return (buffer.toString(), pos); // unclosed
  }

  (String, int) _parseQuotedEscaped(
    String line,
    int contentStart,
    int firstQuote,
  ) {
    final length = line.length;
    final buffer = StringBuffer(line.substring(contentStart, firstQuote));
    int pos = firstQuote;
    while (pos < length) {
      final cu = line.codeUnitAt(pos);
      if (cu == _quote) {
        if (pos + 1 < length && line.codeUnitAt(pos + 1) == _quote) {
          buffer.writeCharCode(_quote);
          pos += 2;
        } else {
          return (buffer.toString(), pos + 1);
        }
      } else {
        buffer.writeCharCode(cu);
        pos++;
      }
    }
    return (buffer.toString(), pos); // unclosed
  }
}

/// Builds one column from CSV cell strings. Numeric columns accumulate into
/// typed buffers (no boxing) and produce a [NumericColumn] directly; integer
/// columns auto-widen (Int8 -> Int32 -> Int64) so a value beyond the starting
/// width never wraps. A null or unparseable numeric cell becomes null when the
/// column is nullable, otherwise throws.
abstract class _ColumnBuilder {
  final String name;
  final bool nullable;
  final CsvOptions options;
  _ColumnBuilder(this.name, this.nullable, this.options);

  void addValue(String? value);
  Column build();

  /// A cell that could not be parsed to the column type. In strict mode (the
  /// default) this is an ingestion error; a genuine null token never reaches
  /// here (it is mapped to `null` upstream). Throwing here keeps "nullable" from
  /// meaning "parse failures silently become null".
  Never _parseError(String raw, String type) =>
      throw FormatException('Column "$name": cannot parse "$raw" as $type');

  /// Add a field given its code-unit range in [line] (unquoted, no escapes).
  /// Default: materialise the substring (trim + nullValues) then [addValue].
  /// Numeric builders override to parse from the range without a substring.
  void addField(String line, int start, int end) {
    final raw = line.substring(start, end);
    final v = options.trimFields ? raw.trim() : raw;
    addValue(options.isNullToken(v) ? null : v);
  }

  static _ColumnBuilder forType(
    String name,
    DataType type,
    bool nullable,
    CsvOptions options, [
    int scale = 0,
  ]) {
    switch (type) {
      case DataType.int8:
        return _IntBuilder(name, nullable, options, 1);
      case DataType.int16:
        return _IntBuilder(name, nullable, options, 2);
      case DataType.int32:
        return _IntBuilder(name, nullable, options, 4);
      case DataType.int64:
        return _IntBuilder(name, nullable, options, 8);
      case DataType.float32:
        return _DoubleBuilder(name, nullable, options, true);
      case DataType.float64:
        return _DoubleBuilder(name, nullable, options, false);
      default:
        return _BoxedBuilder(name, type, nullable, options, scale);
    }
  }
}

/// Boxed builder for booleans, strings and any non-numeric type.
class _BoxedBuilder extends _ColumnBuilder {
  final DataType dataType;

  /// Decimal scale (0 for every other type): a decimal column stores an
  /// unscaled BigInt, so the text has to be lifted to the field's scale.
  final int scale;
  final List<dynamic> _data = [];

  _BoxedBuilder(
    super.name,
    this.dataType,
    super.nullable,
    super.options, [
    this.scale = 0,
  ]);

  @override
  void addValue(String? value) {
    if (value == null) {
      _data.add(null);
    } else if (dataType == DataType.boolean) {
      if (value.isEmpty) {
        _missing(); // empty cell = missing value, not garbage
        return;
      }
      final b = _parseBool(value);
      if (b == null) {
        _bad(value, 'boolean'); // was: silently false — a data-corruption bug
        return;
      }
      _data.add(b);
    } else if (dataType == DataType.date || dataType == DataType.timestamp) {
      if (value.isEmpty) {
        _missing();
        return;
      }
      final fmt = dataType == DataType.date
          ? options.dateFormat
          : options.timestampFormat;
      final dt = fmt != null
          ? parseTemporal(value, fmt)
          : DateTime.tryParse(value);
      if (dt == null) {
        _bad(value, dataType.name);
        return;
      }
      _data.add(dt);
    } else if (dataType == DataType.decimal) {
      if (value.isEmpty) {
        _missing();
        return;
      }
      final n = num.tryParse(_normNum(value, options));
      // A value needing MORE scale than inference derived would be truncated by
      // toUnscaled, which throws — surface it as a parse error like any other
      // bad cell rather than a raw ArgumentError.
      final unscaled = n == null || (n is double && !n.isFinite)
          ? null
          : _tryUnscaled(n, scale);
      if (unscaled == null) {
        _bad(value, 'decimal');
        return;
      }
      _data.add(unscaled);
    } else {
      _data.add(value); // string: empty stays an empty string
    }
  }

  static BigInt? _tryUnscaled(num value, int scale) {
    try {
      return toUnscaled(value, scale);
    } on ArgumentError {
      return null; // needs more scale than the column declares
    }
  }

  /// An empty cell for a non-string typed column: null when nullable, else an
  /// error (a non-nullable column can't hold a missing value).
  void _missing() {
    if (!nullable) _parseError('', dataType.name);
    _data.add(null);
  }

  static bool? _parseBool(String v) {
    switch (v.toLowerCase()) {
      case 'true':
      case '1':
        return true;
      case 'false':
      case '0':
        return false;
      default:
        return null; // unrecognized → not silently false
    }
  }

  /// A boxed cell that failed to parse: error in strict mode or a non-nullable
  /// column; otherwise (permissive + nullable) record null.
  void _bad(String raw, String type) {
    if (options.strict || !nullable) _parseError(raw, type);
    _data.add(null);
  }

  @override
  Column build() => ListColumn(
    name: name,
    data: _data,
    dataType: dataType,
    nullable: nullable,
  );
}

/// Growable null bitmap shared by the typed builders.
Uint8List _growBitmap(Uint8List old, int newCapBits) {
  final nb = Uint8List((newCapBits + 7) >> 3);
  nb.setRange(0, old.length, old);
  return nb;
}

/// Base-10 integer parse. `int.tryParse` also accepts `0x` HEX literals, so a
/// column of '0xFF' strings silently inferred int64 and stored the converted
/// values (255), losing the source text.
int? _decimalInt(String s) {
  if (s.isEmpty) return null;
  final first = s.codeUnitAt(0);
  final start = (first == 0x2d || first == 0x2b) ? 1 : 0; // - +
  if (start == s.length) return null;
  for (int i = start; i < s.length; i++) {
    final c = s.codeUnitAt(i);
    if (c < 0x30 || c > 0x39) return null;
  }
  return int.tryParse(s); // digits only; still null on int64 overflow
}

class _IntBuilder extends _ColumnBuilder {
  TypedData _buf;
  final int _width; // bytes per element: 1, 2, 4, or 8
  int _len = 0;
  Uint8List? _nulls;

  /// Whether any configured null token LOOKS numeric (e.g. `nullValues:
  /// {'-1'}`): the digit fast path must then check tokens before storing —
  /// with the default (non-numeric) tokens it skips that check entirely.
  final bool _numericNullTokens;

  _IntBuilder(super.name, super.nullable, super.options, this._width)
    : _buf = _alloc(_width, 16),
      _nulls = nullable ? Uint8List(2) : null,
      _numericNullTokens = options.nullValues.any(
        (t) => _decimalInt(t) != null,
      );

  static TypedData _alloc(int width, int cap) => width == 1
      ? Int8List(cap)
      : width == 2
      ? Int16List(cap)
      : width == 4
      ? Int32List(cap)
      : Int64List(cap);

  void _store(int n) {
    _ensure(_len + 1);
    // A narrow width only ever comes from an EXPLICIT caller schema (inference
    // always types integers int64), so an out-of-range value violates that
    // declared contract — fail loud rather than silently widening the column
    // past the type the caller pinned.
    if (!_fits(n)) {
      throw FormatException(
        'Column "$name": value $n out of range for the declared '
        'int${_width * 8} schema type',
      );
    }
    (_buf as List<int>)[_len] = n;
    _len++;
  }

  @override
  void addValue(String? value) {
    if (value == null) {
      _addNull('');
      return;
    }
    // An empty cell is a missing value (null), not garbage — matches the
    // addField fast path and never trips strict mode.
    if (value.isEmpty) {
      _addNull(value);
      return;
    }
    final n = _decimalInt(_normNum(value, options));
    if (n == null) {
      if (options.strict) _parseError(value, 'integer');
      _addNull(value);
      return;
    }
    _store(n);
  }

  @override
  void addField(String line, int start, int end) {
    // Trimming or a locale thousands separator needs full-substring semantics;
    // defer to the base path (which routes through addValue → normalize).
    if (options.trimFields || options.thousandsSeparator != null) {
      super.addField(line, start, end);
      return;
    }
    // Fast path: parse a plain decimal integer straight from the code units
    // (<=18 digits stays within int64). Anything else falls back. A NUMERIC
    // custom null token (`nullValues: {'-1'}`) must be honored here too — the
    // fast path used to store it as the integer.
    if (_numericNullTokens && options.isNullToken(line.substring(start, end))) {
      _addNull(line.substring(start, end));
      return;
    }
    if (end > start && end - start <= 18) {
      int i = start;
      bool neg = false;
      final c0 = line.codeUnitAt(i);
      if (c0 == 0x2d) {
        neg = true;
        i++;
      } else if (c0 == 0x2b) {
        i++;
      }
      if (i < end) {
        int value = 0;
        bool ok = true;
        for (; i < end; i++) {
          final d = line.codeUnitAt(i) - 0x30;
          if (d < 0 || d > 9) {
            ok = false;
            break;
          }
          value = value * 10 + d;
        }
        if (ok) {
          _store(neg ? -value : value);
          return;
        }
      }
    }
    final string = line.substring(start, end);
    if (string.isEmpty || options.isNullToken(string)) {
      _addNull(string);
      return;
    }
    final n = _decimalInt(string);
    if (n == null) {
      if (options.strict) _parseError(string, 'integer');
      _addNull(string);
      return;
    }
    _store(n);
  }

  void _addNull(String raw) {
    if (!nullable) {
      throw FormatException(
        'Column "$name": cannot parse "$raw" as integer (non-nullable)',
      );
    }
    _ensure(_len + 1);
    _nulls![_len >> 3] |= 1 << (_len & 7);
    _len++;
  }

  bool _fits(int n) => _width == 8
      ? true
      : _width == 4
      ? (n >= -2147483648 && n <= 2147483647)
      : _width == 2
      ? (n >= -32768 && n <= 32767)
      : (n >= -128 && n <= 127);

  void _ensure(int needed) {
    final cap = (_buf as List<int>).length;
    if (needed <= cap) return;
    int newCap = cap * 2;
    while (newCap < needed) {
      newCap *= 2;
    }
    final grown = _alloc(_width, newCap) as List<int>;
    grown.setRange(0, _len, _buf as List<int>);
    _buf = grown as TypedData;
    if (_nulls != null) _nulls = _growBitmap(_nulls!, newCap);
  }

  @override
  Column build() {
    final dataType = _width == 1
        ? DataType.int8
        : _width == 2
        ? DataType.int16
        : _width == 4
        ? DataType.int32
        : DataType.int64;
    final TypedData trimmed = _width == 1
        ? Int8List.sublistView(_buf, 0, _len)
        : _width == 2
        ? Int16List.sublistView(_buf, 0, _len)
        : _width == 4
        ? Int32List.sublistView(_buf, 0, _len)
        : Int64List.sublistView(_buf, 0, _len);
    final bitmap = _nulls == null
        ? null
        : Uint8List.sublistView(_nulls!, 0, (_len + 7) >> 3);
    return NumericColumn.adopt(
      name: name,
      dataType: dataType,
      data: trimmed,
      nullBitmap: bitmap,
    );
  }
}

class _DoubleBuilder extends _ColumnBuilder {
  final bool isF32;
  TypedData _buf;
  int _len = 0;
  Uint8List? _nulls;

  _DoubleBuilder(super.name, super.nullable, super.options, this.isF32)
    : _buf = isF32 ? Float32List(16) : Float64List(16),
      _nulls = nullable ? Uint8List(2) : null;

  @override
  void addValue(String? value) {
    if (value == null) {
      _addNull('');
      return;
    }
    // An empty cell is a missing value (null), not garbage.
    if (value.isEmpty) {
      _addNull(value);
      return;
    }
    final d = double.tryParse(_normNum(value, options));
    if (d == null) {
      if (options.strict) _parseError(value, 'double');
      _addNull(value);
      return;
    }
    _ensure(_len + 1);
    (_buf as List<double>)[_len] = d;
    _len++;
  }

  void _addNull(String raw) {
    if (!nullable) {
      throw FormatException(
        'Column "$name": cannot parse "$raw" as double (non-nullable)',
      );
    }
    _ensure(_len + 1);
    _nulls![_len >> 3] |= 1 << (_len & 7);
    _len++;
  }

  void _ensure(int needed) {
    final cap = (_buf as List<double>).length;
    if (needed <= cap) return;
    int newCap = cap * 2;
    while (newCap < needed) {
      newCap *= 2;
    }
    final grown =
        (isF32 ? Float32List(newCap) : Float64List(newCap)) as List<double>;
    grown.setRange(0, _len, _buf as List<double>);
    _buf = grown as TypedData;
    if (_nulls != null) _nulls = _growBitmap(_nulls!, newCap);
  }

  @override
  Column build() {
    final dataType = isF32 ? DataType.float32 : DataType.float64;
    final TypedData trimmed = isF32
        ? Float32List.sublistView(_buf, 0, _len)
        : Float64List.sublistView(_buf, 0, _len);
    final bitmap = _nulls == null
        ? null
        : Uint8List.sublistView(_nulls!, 0, (_len + 7) >> 3);
    return NumericColumn.adopt(
      name: name,
      dataType: dataType,
      data: trimmed,
      nullBitmap: bitmap,
    );
  }
}

/// Compiled filter predicate (reused from existing implementation)
class _FilterPredicate {
  final String column;
  final String operator;
  final String value;
  final dynamic compiledValue;

  /// [column]'s index, cached against the header list it was resolved from.
  /// [evaluate] runs per ROW, so an `indexOf` there costs the header count on
  /// every line of the file; one read passes one header list throughout, so this
  /// resolves once.
  List<String>? _headers;
  int _index = -1;

  _FilterPredicate._(
    this.column,
    this.operator,
    this.value,
    this.compiledValue,
  );

  static _FilterPredicate compile(String filter) {
    final operatorPattern = RegExp(r'\s*(==|!=|<>|>=|<=|>|<|=)\s*');
    final match = operatorPattern.firstMatch(filter);

    if (match == null) {
      throw ArgumentError('Invalid filter format: $filter');
    }

    final column = filter.substring(0, match.start).trim();
    final operator = match.group(1)!;
    String value = filter.substring(match.end).trim();

    // Remove quotes from value
    if ((value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'"))) {
      value = value.substring(1, value.length - 1);
    }

    // Pre-compile value for faster comparison. INT IS TRIED FIRST: double first
    // would claim every int64 literal and round it (9223372036854775807 becomes
    // 9223372036854776000.0, which then compares EQUAL to its neighbours), and
    // it does not recognise the hex literals int.tryParse accepts.
    dynamic compiledValue = value;
    if (int.tryParse(value) != null) {
      compiledValue = int.parse(value);
    } else if (double.tryParse(value) != null) {
      compiledValue = double.parse(value);
    } else if (value.toLowerCase() == 'true' ||
        value.toLowerCase() == 'false') {
      compiledValue = value.toLowerCase() == 'true';
    }

    return _FilterPredicate._(column, operator, value, compiledValue);
  }

  /// Resolve [column] against [headers] once per read, rejecting a name the file
  /// does not have. Returning "no match" for an unknown column (the old
  /// behavior) silently yields an EMPTY result — and unlike the SQL path, this
  /// API has no query layer above it to catch the mistake.
  int _resolve(List<String> headers) {
    if (identical(_headers, headers)) return _index;
    final index = headers.indexOf(column);
    if (index < 0) {
      throw ArgumentError(
        'CSV filter references unknown column "$column" (the file has: '
        '${headers.join(', ')})',
      );
    }
    _headers = headers;
    _index = index;
    return index;
  }

  bool evaluate(List<String?> row, List<String> headers) {
    final columnIndex = _resolve(headers);
    if (columnIndex >= row.length) return false;

    final cellValue = row[columnIndex];
    if (cellValue == null) return false;

    // Convert cell value to same type as compiled value
    dynamic convertedCellValue = cellValue;
    if (compiledValue is num) {
      // Int first, for the same reason as in compile().
      convertedCellValue =
          int.tryParse(cellValue) ?? double.tryParse(cellValue);
      if (convertedCellValue == null) return false;
    } else if (compiledValue is bool) {
      // Only the two boolean literals. The old `lower == 'true' || lower == '1'`
      // turned every OTHER cell into a legitimate-looking `false`, so `flag ==
      // false` matched "banana". An uninterpretable cell now matches nothing —
      // the same answer the numeric branch above already gives.
      final lower = cellValue.toLowerCase();
      if (lower != 'true' && lower != 'false') return false;
      convertedCellValue = lower == 'true';
    }

    // Perform comparison
    switch (operator) {
      case '=':
      case '==':
        return convertedCellValue == compiledValue;
      case '!=':
      case '<>':
        return convertedCellValue != compiledValue;
      case '>':
        return convertedCellValue is Comparable &&
            compiledValue is Comparable &&
            convertedCellValue.compareTo(compiledValue) > 0;
      case '>=':
        return convertedCellValue is Comparable &&
            compiledValue is Comparable &&
            convertedCellValue.compareTo(compiledValue) >= 0;
      case '<':
        return convertedCellValue is Comparable &&
            compiledValue is Comparable &&
            convertedCellValue.compareTo(compiledValue) < 0;
      case '<=':
        return convertedCellValue is Comparable &&
            compiledValue is Comparable &&
            convertedCellValue.compareTo(compiledValue) <= 0;
      default:
        return false;
    }
  }
}

/// Normalize a numeric string for the configured locale: strip the thousands
/// separator and map a non-`.` decimal separator to `.`. A no-op for the default
/// (`null` thousands, `.` decimal), so the common path pays nothing.
String _normNum(String v, CsvOptions o) {
  var s = v;
  final th = o.thousandsSeparator;
  if (th != null && th.isNotEmpty) s = s.replaceAll(th, '');
  if (o.decimalSeparator != '.') s = s.replaceAll(o.decimalSeparator, '.');
  return s;
}

/// Parse [value] against a [format] of the supported tokens — `yyyy`, `yy`,
/// `MM`, `M`, `dd`, `d`, `HH`, `mm`, `ss` — with any other character matched
/// literally. Returns null if [value] doesn't match (so inference can fall
/// through). Not a full ICU/strptime implementation; unknown letter runs throw.
DateTime? parseTemporal(String value, String format) {
  int y = 1, mo = 1, d = 1, h = 0, mi = 0, s = 0;
  int fi = 0, vi = 0;

  int? digits(int count) {
    // Fixed-width tokens read exactly [count]; 1-char tokens (M/d) read 1-2.
    var n = 0, read = 0;
    while (read < count && vi < value.length) {
      final c = value.codeUnitAt(vi);
      if (c < 0x30 || c > 0x39) break;
      n = n * 10 + (c - 0x30);
      vi++;
      read++;
    }
    return read == 0 ? null : n;
  }

  while (fi < format.length) {
    final ch = format[fi];
    if ('yMdHms'.contains(ch)) {
      int run = 1;
      while (fi + run < format.length && format[fi + run] == ch) {
        run++;
      }
      fi += run;
      final v = digits(run >= 2 ? run : 2); // 1-char token reads up to 2
      if (v == null) return null;
      switch (ch) {
        case 'y':
          y = run <= 2 ? 2000 + (v % 100) : v;
          break;
        case 'M':
          mo = v;
          break;
        case 'd':
          d = v;
          break;
        case 'H':
          h = v;
          break;
        case 'm':
          mi = v;
          break;
        case 's':
          s = v;
          break;
      }
    } else {
      // Literal: must match the next input character exactly.
      if (vi >= value.length || value[vi] != ch) return null;
      vi++;
      fi++;
    }
  }
  if (vi != value.length) return null; // trailing junk
  if (mo < 1 || mo > 12 || d < 1 || d > 31 || h > 23 || mi > 59 || s > 59) {
    return null;
  }
  final dt = DateTime(y, mo, d, h, mi, s);
  // DateTime NORMALIZES an out-of-range day (Feb 31 → Mar 3); reject rather than
  // silently store the wrong date.
  if (dt.year != y || dt.month != mo || dt.day != d) return null;
  return dt;
}
