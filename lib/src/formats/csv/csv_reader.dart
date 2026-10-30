import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';
import 'package:meta/meta.dart';

import '../../core/conversions.dart'
    show dateOf, inferDecimalMeta, parseUtc, toUnscaled;
import '../../core/encoding.dart';
import '../../core/column.dart';
import '../../core/data_type.dart';
import '../../core/failure.dart';
import '../../core/limits.dart';
import '../../core/result.dart';
import '../../core/row_filter.dart';
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
final class CsvReader {
  final CsvOptions options;
  CsvReader({CsvOptions? options}) : options = options ?? const CsvOptions() {
    // The parser reads only the first code unit of delimiter/quote/escape; a
    // multi-char value silently mismatches and an empty one throws a RangeError.
    // Validate up front so a bad config is an actionable error.
    this.options.validate();
  }

  /// Read CSV from file path. A failure is reported as an [Err]: [Kind.missing]
  /// for a file that does not exist, [Kind.corrupt] for malformed CSV or a value
  /// that does not parse as its column type, [Kind.limit] past a cap, and
  /// [Kind.invalid] for an unknown selected column or filter.
  @useResult
  Future<Result<Table, ErrorDetails>> readFile(
    String path, {
    RowFilter? filter,
    List<String>? columns,
    int? limit,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => capture(
    () => _readFile(
      path,
      filter: filter,
      columns: columns,
      limit: limit,
      maxRows: maxRows,
      maxBytes: maxBytes,
    ),
    fallback: Kind.corrupt,
  );

  Future<Table> _readFile(
    String path, {
    RowFilter? filter,
    List<String>? columns,
    int? limit,
    int? maxRows,
    int? maxBytes,
  }) async {
    final file = await _existing(path);

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

    // The default terminators stream, and stop at the caps so a giant CSV
    // can't OOM. A custom terminator needs the whole text to split it, so its
    // size is checked against maxBytes up front (maxRows alone can't be
    // enforced without parsing).
    final Stream<Line> lines;
    if (options.lineTerminator == '\n') {
      lines = _lines(file.openRead().transform(_encoding().decoder));
    } else {
      final size = await file.length();
      if (maxBytes != null && size > maxBytes) {
        throw Failure.of(
          Kind.limit,
          'CSV file is $size bytes, exceeding maxBytes=$maxBytes; pass '
          'maxBytes: null to allow it, or use the default "\\n" terminator for '
          'bounded-memory streaming',
        );
      }
      lines = _custom(await file.readAsString(encoding: _encoding()));
    }
    return _collect(
      _batches(
        lines,
        size: 10000,
        filter: filter,
        columns: columns,
        limit: limit,
      ),
      maxRows,
      maxBytes,
    );
  }

  /// [path] as a file, or [PathNotFoundException] when it does not exist.
  Future<File> _existing(String path) async {
    final file = File(path);
    if (!await file.exists()) {
      throw PathNotFoundException(
        path,
        const OSError('No such file or directory', 2),
        'File not found',
      );
    }
    return file;
  }

  /// Decoded text [chunks] as lines on `\n`, `\r\n` or `\r`, each bounded by
  /// [CsvOptions.maxRecordBytes].
  Stream<Line> _lines(Stream<String> chunks) =>
      splitLines(chunks, options.maxRecordBytes, label: 'CSV record');

  /// [content] split on the custom [CsvOptions.lineTerminator].
  Stream<Line> _custom(String content) {
    final end = options.lineTerminator;
    final parts = content.split(end);
    return Stream.fromIterable([
      for (final (index, text) in parts.indexed)
        (text: text, end: index < parts.length - 1 ? end : ''),
    ]);
  }

  /// The batches of [tables] as one table, failing with [Kind.limit] past
  /// [maxRows] data rows or [maxBytes].
  Future<Table> _collect(
    Stream<Table> tables,
    int? maxRows,
    int? maxBytes,
  ) async {
    final all = <Table>[];
    var rows = 0;
    var bytes = 0;
    await for (final table in tables) {
      rows += table.rowCount;
      bytes += table.memoryUsage;
      // The byte check is overhead-adjusted so the cap predicts true peak
      // (object overhead + transient concat), not just raw buffer bytes.
      enforceMaterializationCaps(rows, bytes, maxRows, maxBytes, 'CSV');
      all.add(table);
    }
    return _concatTables(all);
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
  /// than materialize an oversized DataFrame). Pass null to opt out. Failures
  /// are reported as [readFile] reports them.
  @useResult
  Future<Result<Table, ErrorDetails>> readString(
    String csvContent, {
    RowFilter? filter,
    List<String>? columns,
    int? limit,
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => capture(
    () => _readString(
      csvContent,
      filter: filter,
      columns: columns,
      limit: limit,
      maxRows: maxRows,
      maxBytes: maxBytes,
    ),
    fallback: Kind.corrupt,
  );

  Future<Table> _readString(
    String csvContent, {
    RowFilter? filter,
    List<String>? columns,
    int? limit,
    int? maxRows,
    int? maxBytes,
  }) async {
    if (csvContent.isEmpty) {
      return Table.empty('csv_data', Schema(fields: []));
    }
    if (maxBytes != null && csvContent.length > maxBytes) {
      throw Failure.of(
        Kind.limit,
        'CSV exceeds maxBytes=$maxBytes; pass maxBytes: null to allow it',
      );
    }
    return _collect(
      _batches(
        options.lineTerminator == '\n'
            ? _lines(Stream.value(csvContent))
            : _custom(csvContent),
        size: 10000,
        filter: filter,
        columns: columns,
        limit: limit,
      ),
      maxRows,
      maxBytes,
    );
  }

  /// Whether [line] leaves a quoted field OPEN at its end, given whether one was
  /// already [open] when the line began.
  ///
  /// This applies the SAME rule the field parser does: a quote only QUOTES at
  /// the start of a field, so `x,5" nails` is two plain fields and its lone
  /// quote is literal text. Counting every quote in the line instead would make
  /// the splitter and the parser disagree: an odd count would swallow the
  /// following line into the record (and then fail on the field count), and a
  /// run to EOF would fail with "Unterminated quoted field", for input
  /// Python/pandas/Excel all read without complaint.
  bool _leavesQuoteOpen(String line, bool open) {
    final quote = options.quote.codeUnitAt(0);
    final escape = options.escape.codeUnitAt(0);
    final delimiter = options.delimiter.codeUnitAt(0);
    final doubled = escape == quote;
    final length = line.length;
    int position = 0;

    // Resuming a quoted field carried over from a previous physical line.
    var quoted = open;
    if (!quoted) {
      // At a field start: a leading quote opens a quoted field.
      if (position < length && line.codeUnitAt(position) == quote) {
        quoted = true;
        position++;
      }
    }

    while (position < length) {
      final unit = line.codeUnitAt(position);
      if (quoted) {
        if (!doubled && unit == escape && position + 1 < length) {
          position += 2; // escape consumes the next char, quote included
          continue;
        }
        if (unit == quote) {
          if (doubled &&
              position + 1 < length &&
              line.codeUnitAt(position + 1) == quote) {
            position += 2; // "" is an escaped quote, not a close
            continue;
          }
          quoted = false; // closes the field
        }
        position++;
        continue;
      }
      // Unquoted: only a delimiter matters; a quote here is literal text.
      position++;
      if (unit != delimiter) continue;
      if (position < length && line.codeUnitAt(position) == quote) {
        quoted = true; // the NEXT field starts quoted
        position++;
      }
    }
    return quoted;
  }

  /// Map each selected source column index -> its builder index.
  List<int> _slots(List<int> selected) {
    int width = 0;
    for (final index in selected) {
      if (index + 1 > width) width = index + 1;
    }
    final slots = List<int>.filled(width, -1);
    for (int j = 0; j < selected.length; j++) {
      slots[selected[j]] = j;
    }
    return slots;
  }

  /// Parse one record into the [builders]; returns whether a data row was added
  /// (false for blank/comment/filtered-out records). Shared by the whole-file
  /// and streaming paths.
  bool _pushRecord(
    String line,
    _UnifiedCsvParser parser,
    List<_ColumnBuilder> builders,
    List<int> slots,
    List<int> selected,
    List<String> headers,
    _RowMatch? match,
  ) {
    if (line.isEmpty) {
      if (options.skipEmptyLines) return false;
      // skipEmptyLines:false: emit an all-null row for the blank line.
      for (final builder in builders) {
        builder.addValue(null);
      }
      return true;
    }
    final comment = options.comment;
    if (comment != null && line.startsWith(comment)) return false;
    final expected = headers.length;

    if (match == null && !line.contains(options.quote)) {
      final n = parser.parseUnquotedInto(line, builders, slots);
      _checkWidth(n, expected, line);
      return true;
    }
    final row = parser.parseLine(line);
    if (row == null) return false;
    if (match != null && !match.matches(row)) return false;
    _checkWidth(row.length, expected, line);
    for (int j = 0; j < selected.length; j++) {
      final index = selected[j];
      builders[j].addValue(index < row.length ? row[index] : null);
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

  /// Stream a CSV file as [Table] batches of ~[batchSize] rows, holding only one
  /// batch in memory. The schema is fixed up front, from [CsvOptions.schema] or
  /// a leading inference sample, and stays stable across every batch; inferred
  /// integer columns use int64 so a later batch can never overflow/drift.
  ///
  /// Each batch arrives as an [Ok]; a failure as one [Err], reported as
  /// [readFile] reports it, which ends the stream.
  ///
  /// A [batchSize] below 1 is a programming error and throws at the call.
  Stream<Result<Table, ErrorDetails>> readBatches(
    String path, {
    int batchSize = 10000,
    RowFilter? filter,
    List<String>? columns,
    int? limit,
  }) {
    if (batchSize <= 0) {
      throw ArgumentError.value(batchSize, 'batchSize', 'must be > 0');
    }
    return captureStream(() async* {
      final file = await _existing(path);
      // Streaming splits on the default terminators only; a custom one needs
      // the whole text (readFile does that).
      if (options.lineTerminator != '\n') {
        throw Failure.of(
          Kind.unsupported,
          'readBatches supports only the default line terminator; use '
          'readFile for a custom lineTerminator',
        );
      }
      yield* _batches(
        _lines(file.openRead().transform(_encoding().decoder)),
        size: batchSize,
        filter: filter,
        columns: columns,
        limit: limit,
      );
    }, fallback: Kind.corrupt);
  }

  /// The one CSV pipeline: [lines] as [Table] batches of up to [size] rows.
  /// Every read path feeds it, so they agree on the header, inference and
  /// values. When no row comes out (a header-only file, or a filter matching
  /// nothing), one empty table still carries the schema.
  Stream<Table> _batches(
    Stream<Line> lines, {
    required int size,
    RowFilter? filter,
    List<String>? columns,
    int? limit,
  }) async* {
    final cap = _cap(limit);
    // The splitter bounds every physical line by maxRecordBytes before it is
    // buffered, so a single multi-GB unquoted cell can't OOM.
    final records = StreamIterator(_recordStream(lines));
    final parser = _UnifiedCsvParser(options);

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
        // The header is the first record that is not blank or a comment.
        List<String?>? row;
        do {
          if (!await records.moveNext()) return;
          row = parser.parseLine(records.current);
        } while (row == null);
        headers = _headerNames(row);
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
        final first = [
          for (final record in sample) ?parser.parseLine(record),
        ].firstOrNull;
        if (first == null) return;
        headers = _autoHeaders(first.length);
      }

      final match = filter == null ? null : _RowMatch(filter, headers);
      final (names, selected) = _selectColumns(headers, columns);
      final fields = _streamSchema(headers, names, selected, sample, parser);
      final schema = Schema(fields: fields);
      final slots = _slots(selected);

      var cursor = 0;
      Future<String?> next() async {
        if (cursor < sample.length) return sample[cursor++];
        return await records.moveNext() ? records.current : null;
      }

      int emitted = 0;
      var done = false;
      while (!done) {
        final builders = [
          for (final f in fields)
            _ColumnBuilder.forType(
              f.name,
              f.dataType,
              f.nullable,
              options,
              f.scale ?? 0,
            ),
        ];
        int count = 0;
        while (count < size) {
          final record = await next();
          if (record == null) {
            done = true;
            break;
          }
          if (_pushRecord(
            record,
            parser,
            builders,
            slots,
            selected,
            headers,
            match,
          )) {
            count++;
            emitted++;
            if (cap != null && emitted >= cap) {
              done = true;
              break;
            }
          }
        }
        if (count > 0 || (done && emitted == 0)) {
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

  /// Logical-record stream: merges quote-spanning lines (RFC 4180 §2.6),
  /// keeping the line break inside a quoted field as written (`\r\n` stays
  /// `\r\n`).
  Stream<String> _recordStream(Stream<Line> lines) async* {
    final quote = options.quote;
    final buffer = StringBuffer();
    bool open = false;
    var end = '';
    int start = 0;
    int n = 0;
    await for (final line in lines) {
      n++;
      final text = line.text;
      if (!open && !text.contains(quote)) {
        yield text;
        continue;
      }
      final unclosed = _leavesQuoteOpen(text, open);
      if (!open && !unclosed) {
        yield text; // a self-contained record, quotes and all
        continue;
      }
      if (open) {
        buffer.write(end.isEmpty ? '\n' : end);
      } else {
        start = n;
      }
      buffer.write(text);
      end = line.end;
      open = unclosed;
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
    final cells = row ?? const <String?>[];
    return [
      for (int i = 0; i < cells.length; i++)
        (cells[i] == null || cells[i]!.isEmpty) ? 'column_$i' : cells[i]!,
    ];
  }

  /// Column names for a headerless file: explicit [CsvOptions.columnNames] when
  /// provided (length must match), otherwise `column_0..column_{n-1}`.
  List<String> _autoHeaders(int n) {
    final names = options.columnNames;
    if (names != null) {
      if (names.length != n) {
        throw Failure.of(
          Kind.invalid,
          'columnNames has ${names.length} entries but the CSV has $n columns',
        );
      }
      return List.of(names);
    }
    return List.generate(n, (i) => 'column_$i');
  }

  /// The effective row cap: the smaller of the method `limit` and
  /// [CsvOptions.maxRows] (either may be null = unbounded).
  int? _cap(int? limit) {
    final rows = options.maxRows;
    if (rows == null) return limit;
    return limit == null ? rows : (limit < rows ? limit : rows);
  }

  /// The fixed, selected schema for a streaming read. Explicit schema is used
  /// as-is; an inferred schema widens integer columns to int64 for stability.
  List<Field> _streamSchema(
    List<String> headers,
    List<String> names,
    List<int> selected,
    List<String> sample,
    _UnifiedCsvParser parser,
  ) {
    if (options.schema != null) {
      return [for (final i in selected) options.schema!.fields[i]];
    }
    final rows = [for (final record in sample) ?parser.parseLine(record)];
    return _inferSchema(names, rows, selected).fields;
  }

  /// Select columns and return headers and indices. A requested column that is
  /// not present is a hard error (no silent drop).
  (List<String>, List<int>) _selectColumns(
    List<String> headers,
    List<String>? columns,
  ) {
    if (columns == null) {
      return (headers, List.generate(headers.length, (i) => i));
    }

    final missing = columns.where((c) => !headers.contains(c)).toList();
    if (missing.isNotEmpty) {
      throw Failure.of(
        Kind.invalid,
        'Unknown column(s) in selection: ${missing.join(', ')}',
      );
    }
    final indices = columns.map(headers.indexOf).toList();
    return (columns, indices);
  }

  /// Infer schema from sample data
  Schema _inferSchema(
    List<String> names,
    List<List<String?>> rows,
    List<int> indices,
  ) {
    final fields = <Field>[];

    for (int i = 0; i < indices.length; i++) {
      final name = names[i];
      final index = indices[i];

      // Every sampled value counts. `rows` is already bounded by
      // CsvOptions.typeInferenceSampleSize; a second hardcoded cap here made
      // that option inert and let a wider value just past the cap infer a
      // too-narrow type.
      final values = rows
          .where((row) => index < row.length && row[index] != null)
          .map((row) => row[index]!)
          .where((value) => value.isNotEmpty)
          .toList();

      fields.add(_inferColumn(name, values));
    }

    return Schema(fields: fields);
  }

  /// Infer the [Field] for a column from its sample values. Returns a Field
  /// rather than a bare type because a decimal also carries precision/scale.
  Field _inferColumn(String name, List<String> values) {
    Field of(DataType type, bool nullable, {int? precision, int? scale}) =>
        Field(
          name: name,
          dataType: type,
          nullable: nullable,
          precision: precision,
          scale: scale,
        );
    if (values.isEmpty) {
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
    if (values.every(
      (s) => s.toLowerCase() == 'true' || s.toLowerCase() == 'false',
    )) {
      return of(DataType.boolean, nullable);
    }

    // Check for integers (locale thousands separators stripped first).
    if (values.every((s) => _decimalInt(_normalize(s, options)) != null)) {
      // int64 regardless of magnitude. A narrower width is not safe from a
      // SAMPLE: the streaming path cannot see every row, so a wider value past
      // the sample would throw against a too-narrow buffer.
      return of(DataType.int64, nullable);
    }

    // Check for floats (locale decimal/thousands separators normalized first).
    if (values.every((s) => double.tryParse(_normalize(s, options)) != null)) {
      // A column mixing integers with floats promotes to a LOSSLESS decimal
      // rather than float64, which would round any integer past 2^53: the same
      // rule Schema.infer applies via DataType.inferredCommon (so CSV and JSON
      // agree on identical data). All-float columns have no integer to lose and
      // stay float64. The scale comes from the values themselves; a non-finite
      // value can't be a decimal, so inferDecimalMeta returns null and the
      // column stays float64.
      final numbers = [
        for (final s in values) num.parse(_normalize(s, options)),
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
        values.every(
          (s) => parseTemporal(s, options.timestampFormat!) != null,
        )) {
      return of(DataType.timestamp, nullable);
    }
    if (options.dateFormat != null &&
        values.every((s) => parseTemporal(s, options.dateFormat!) != null)) {
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
  final bool _doubled;

  _UnifiedCsvParser(this.options)
    : _quote = options.quote.codeUnitAt(0),
      _delimiter = options.delimiter.codeUnitAt(0),
      _escape = options.escape.codeUnitAt(0),
      _doubled = options.escape == options.quote;

  /// Fast path for a line known to contain no quote characters: scan fields by
  /// delimiter and push each directly into its builder via a code-unit range
  /// (no per-row List, no substring for integer columns). [slots] maps a
  /// CSV column index to a builder index (-1 = not selected).
  /// Returns the number of CSV fields the line actually held (for row-width
  /// enforcement); missing trailing selected columns are still null-padded.
  int parseUnquotedInto(
    String line,
    List<_ColumnBuilder> builders,
    List<int> slots,
  ) {
    final length = line.length;
    final width = slots.length;
    int field = 0;
    int position = 0;
    while (true) {
      final start = position;
      while (position < length && line.codeUnitAt(position) != _delimiter) {
        position++;
      }
      if (field < width) {
        final slot = slots[field];
        if (slot >= 0) builders[slot].addField(line, start, position);
      }
      field++;
      if (position >= length) break;
      position++; // skip delimiter
      if (position >= length) {
        // Trailing delimiter -> one more empty field.
        if (field < width) {
          final slot = slots[field];
          if (slot >= 0) builders[slot].addField(line, position, position);
        }
        field++;
        break;
      }
    }
    final seen = field; // actual field count, before null-padding
    // Missing trailing columns -> null.
    for (; field < width; field++) {
      final slot = slots[field];
      if (slot >= 0) builders[slot].addValue(null);
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
    int position = 0;

    while (true) {
      if (position < length && line.codeUnitAt(position) == _quote) {
        final (value, end) = _parseQuoted(line, position);
        fields.add(options.trimFields ? value.trim() : value);
        position = end;
      } else {
        final start = position;
        while (position < length && line.codeUnitAt(position) != _delimiter) {
          position++;
        }
        fields.add(_finishUnquoted(line.substring(start, position)));
      }

      if (position >= length) break;
      // A field ends at a delimiter. Stray characters after a closing quote are
      // a malformed field: reject them in strict mode (the default); otherwise
      // treat them as ending the record (lenient, legacy behavior).
      if (line.codeUnitAt(position) != _delimiter) {
        if (options.strict) {
          throw FormatException(
            'Malformed CSV: unexpected text after closing quote',
            line,
            position,
          );
        }
        break;
      }
      position++;
      if (position >= length) {
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
    if (!_doubled) return _parseQuotedCustomEscape(line, start);
    final length = line.length;
    final begin = start + 1;
    int position = begin;
    while (position < length) {
      if (line.codeUnitAt(position) == _quote) {
        if (position + 1 < length && line.codeUnitAt(position + 1) == _quote) {
          // Escaped quote: fall back to the buffer path.
          return _parseQuotedEscaped(line, begin, position);
        }
        return (line.substring(begin, position), position + 1);
      }
      position++;
    }
    return (line.substring(begin), position); // unclosed
  }

  /// Quoted field using a distinct escape char: `escape` + any char emits that
  /// char literally; an unescaped quote closes the field.
  (String, int) _parseQuotedCustomEscape(String line, int start) {
    final length = line.length;
    final buffer = StringBuffer();
    int position = start + 1;
    while (position < length) {
      final unit = line.codeUnitAt(position);
      if (unit == _escape && position + 1 < length) {
        buffer.writeCharCode(line.codeUnitAt(position + 1));
        position += 2;
      } else if (unit == _quote) {
        return (buffer.toString(), position + 1);
      } else {
        buffer.writeCharCode(unit);
        position++;
      }
    }
    return (buffer.toString(), position); // unclosed
  }

  (String, int) _parseQuotedEscaped(String line, int begin, int first) {
    final length = line.length;
    final buffer = StringBuffer(line.substring(begin, first));
    int position = first;
    while (position < length) {
      final unit = line.codeUnitAt(position);
      if (unit == _quote) {
        if (position + 1 < length && line.codeUnitAt(position + 1) == _quote) {
          buffer.writeCharCode(_quote);
          position += 2;
        } else {
          return (buffer.toString(), position + 1);
        }
      } else {
        buffer.writeCharCode(unit);
        position++;
      }
    }
    return (buffer.toString(), position); // unclosed
  }
}

/// Builds one column from CSV cell strings. Numeric columns accumulate into
/// typed buffers (no boxing) and produce a [NumericColumn] directly; integer
/// columns keep their declared width and reject a value beyond it rather than
/// wrap (inference always types integers int64). A null or unparseable numeric cell becomes null when the
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
      final flag = _parseBool(value);
      if (flag == null) {
        _bad(value, 'boolean'); // never a silent false
        return;
      }
      _data.add(flag);
    } else if (dataType == DataType.date || dataType == DataType.timestamp) {
      if (value.isEmpty) {
        _missing();
        return;
      }
      final format = dataType == DataType.date
          ? options.dateFormat
          : options.timestampFormat;
      // A value without a zone is UTC, as everywhere else in the library.
      final at = format != null
          ? parseTemporal(value, format)
          : parseUtc(value);
      final moment = at == null || dataType == DataType.timestamp
          ? at
          : dateOf(at);
      if (moment == null) {
        _bad(value, dataType.name);
        return;
      }
      _data.add(moment);
    } else if (dataType == DataType.decimal) {
      if (value.isEmpty) {
        _missing();
        return;
      }
      final n = num.tryParse(_normalize(value, options));
      // A value needing MORE scale than inference derived would be truncated by
      // toUnscaled, which throws: surface it as a parse error like any other
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
Uint8List _grow(Uint8List old, int bits) {
  final grown = Uint8List((bits + 7) >> 3);
  grown.setRange(0, old.length, old);
  return grown;
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
  TypedData _buffer;
  final int _width; // bytes per element: 1, 2, 4, or 8
  int _length = 0;
  Uint8List? _nulls;

  /// Whether any configured null token LOOKS numeric (e.g. `nullValues:
  /// {'-1'}`): the digit fast path must then check tokens before storing,
  /// with the default (non-numeric) tokens it skips that check entirely.
  final bool _numericNullTokens;

  _IntBuilder(super.name, super.nullable, super.options, this._width)
    : _buffer = _allocate(_width, 16),
      _nulls = nullable ? Uint8List(2) : null,
      _numericNullTokens = options.nullValues.any(
        (t) => _decimalInt(t) != null,
      );

  static TypedData _allocate(int width, int capacity) => width == 1
      ? Int8List(capacity)
      : width == 2
      ? Int16List(capacity)
      : width == 4
      ? Int32List(capacity)
      : Int64List(capacity);

  void _store(int n) {
    _ensure(_length + 1);
    // A narrow width only ever comes from an EXPLICIT caller schema (inference
    // always types integers int64), so an out-of-range value violates that
    // declared contract: fail loud rather than silently widening the column
    // past the type the caller pinned.
    if (!_fits(n)) {
      throw FormatException(
        'Column "$name": value $n out of range for the declared '
        'int${_width * 8} schema type',
      );
    }
    (_buffer as List<int>)[_length] = n;
    _length++;
  }

  @override
  void addValue(String? value) {
    if (value == null) {
      _addNull('');
      return;
    }
    // An empty cell is a missing value (null), not garbage: matches the
    // addField fast path and never trips strict mode.
    if (value.isEmpty) {
      _addNull(value);
      return;
    }
    final n = _decimalInt(_normalize(value, options));
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
    // custom null token (`nullValues: {'-1'}`) must be honored here too, before
    // the fast path stores it as the integer.
    if (_numericNullTokens && options.isNullToken(line.substring(start, end))) {
      _addNull(line.substring(start, end));
      return;
    }
    if (end > start && end - start <= 18) {
      int i = start;
      bool negative = false;
      final first = line.codeUnitAt(i);
      if (first == 0x2d) {
        negative = true;
        i++;
      } else if (first == 0x2b) {
        i++;
      }
      if (i < end) {
        int value = 0;
        bool ok = true;
        for (; i < end; i++) {
          final digit = line.codeUnitAt(i) - 0x30;
          if (digit < 0 || digit > 9) {
            ok = false;
            break;
          }
          value = value * 10 + digit;
        }
        if (ok) {
          _store(negative ? -value : value);
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
    _ensure(_length + 1);
    _nulls![_length >> 3] |= 1 << (_length & 7);
    _length++;
  }

  bool _fits(int n) => _width == 8
      ? true
      : _width == 4
      ? (n >= -2147483648 && n <= 2147483647)
      : _width == 2
      ? (n >= -32768 && n <= 32767)
      : (n >= -128 && n <= 127);

  void _ensure(int needed) {
    final capacity = (_buffer as List<int>).length;
    if (needed <= capacity) return;
    int size = capacity * 2;
    while (size < needed) {
      size *= 2;
    }
    final grown = _allocate(_width, size) as List<int>;
    grown.setRange(0, _length, _buffer as List<int>);
    _buffer = grown as TypedData;
    if (_nulls != null) _nulls = _grow(_nulls!, size);
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
        ? Int8List.sublistView(_buffer, 0, _length)
        : _width == 2
        ? Int16List.sublistView(_buffer, 0, _length)
        : _width == 4
        ? Int32List.sublistView(_buffer, 0, _length)
        : Int64List.sublistView(_buffer, 0, _length);
    final bitmap = _nulls == null
        ? null
        : Uint8List.sublistView(_nulls!, 0, (_length + 7) >> 3);
    return NumericVector.adopt(
      name: name,
      dataType: dataType,
      data: trimmed,
      nullBitmap: bitmap,
    );
  }
}

class _DoubleBuilder extends _ColumnBuilder {
  final bool isF32;
  TypedData _buffer;
  int _length = 0;
  Uint8List? _nulls;

  _DoubleBuilder(super.name, super.nullable, super.options, this.isF32)
    : _buffer = isF32 ? Float32List(16) : Float64List(16),
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
    final d = double.tryParse(_normalize(value, options));
    if (d == null) {
      if (options.strict) _parseError(value, 'double');
      _addNull(value);
      return;
    }
    _ensure(_length + 1);
    (_buffer as List<double>)[_length] = d;
    _length++;
  }

  void _addNull(String raw) {
    if (!nullable) {
      throw FormatException(
        'Column "$name": cannot parse "$raw" as double (non-nullable)',
      );
    }
    _ensure(_length + 1);
    _nulls![_length >> 3] |= 1 << (_length & 7);
    _length++;
  }

  void _ensure(int needed) {
    final capacity = (_buffer as List<double>).length;
    if (needed <= capacity) return;
    int size = capacity * 2;
    while (size < needed) {
      size *= 2;
    }
    final grown =
        (isF32 ? Float32List(size) : Float64List(size)) as List<double>;
    grown.setRange(0, _length, _buffer as List<double>);
    _buffer = grown as TypedData;
    if (_nulls != null) _nulls = _grow(_nulls!, size);
  }

  @override
  Column build() {
    final dataType = isF32 ? DataType.float32 : DataType.float64;
    final TypedData trimmed = isF32
        ? Float32List.sublistView(_buffer, 0, _length)
        : Float64List.sublistView(_buffer, 0, _length);
    final bitmap = _nulls == null
        ? null
        : Uint8List.sublistView(_nulls!, 0, (_length + 7) >> 3);
    return NumericVector.adopt(
      name: name,
      dataType: dataType,
      data: trimmed,
      nullBitmap: bitmap,
    );
  }
}

/// A [RowFilter] applied exactly to each CSV row: its columns resolved once
/// against the file's headers (an unknown one is [Kind.invalid], since a
/// filter that silently matched nothing would yield an empty result), and the
/// raw cells read in each literal's type by [RowFilter.matches].
class _RowMatch {
  final RowFilter filter;
  final Map<String, int> _indices;

  _RowMatch(this.filter, List<String> headers)
    : _indices = {
        for (final column in filter.columns)
          column: headers.contains(column)
              ? headers.indexOf(column)
              : throw Failure.of(
                  Kind.invalid,
                  'CSV filter references unknown column "$column" (the file '
                  'has: ${headers.join(', ')})',
                ),
      };

  bool matches(List<String?> row) => filter.matches({
    for (final MapEntry(key: column, value: index) in _indices.entries)
      column: index < row.length ? row[index] : null,
  });
}

/// Normalize a numeric string for the configured locale: strip the thousands
/// separator and map a non-`.` decimal separator to `.`. A no-op for the default
/// (`null` thousands, `.` decimal), so the common path pays nothing.
String _normalize(String value, CsvOptions options) {
  var text = value;
  final separator = options.thousandsSeparator;
  if (separator != null && separator.isNotEmpty && text.contains(separator)) {
    // Only digits grouped in threes are a number: `1.2.3` with `.` as the
    // separator is text, not 123. An empty string never parses as a number.
    final group = RegExp.escape(separator);
    final point = RegExp.escape(options.decimalSeparator);
    if (!RegExp(
      '^[+-]?\\d{1,3}($group\\d{3})+($point\\d*)?\$',
    ).hasMatch(text)) {
      return '';
    }
    text = text.replaceAll(separator, '');
  }
  if (options.decimalSeparator != '.') {
    text = text.replaceAll(options.decimalSeparator, '.');
  }
  return text;
}

/// Parse [value] against a [format] of the supported tokens: `yyyy`, `yy`,
/// `MM`, `M`, `dd`, `d`, `HH`, `mm`, `ss`, with any other character matched
/// literally. Returns null if [value] doesn't match (so inference can fall
/// through). Not a full ICU/strptime implementation; unknown letter runs throw.
DateTime? parseTemporal(String value, String format) {
  int year = 1, month = 1, day = 1, hour = 0, minute = 0, second = 0;
  int index = 0, offset = 0;

  int? digits(int count) {
    // Fixed-width tokens read exactly [count]; 1-char tokens (M/d) read 1-2.
    var n = 0, read = 0;
    while (read < count && offset < value.length) {
      final unit = value.codeUnitAt(offset);
      if (unit < 0x30 || unit > 0x39) break;
      n = n * 10 + (unit - 0x30);
      offset++;
      read++;
    }
    return read == 0 ? null : n;
  }

  while (index < format.length) {
    final symbol = format[index];
    if ('yMdHms'.contains(symbol)) {
      int run = 1;
      while (index + run < format.length && format[index + run] == symbol) {
        run++;
      }
      index += run;
      final number = digits(run >= 2 ? run : 2); // 1-char token reads up to 2
      if (number == null) return null;
      switch (symbol) {
        case 'y':
          year = run <= 2 ? 2000 + (number % 100) : number;
          break;
        case 'M':
          month = number;
          break;
        case 'd':
          day = number;
          break;
        case 'H':
          hour = number;
          break;
        case 'm':
          minute = number;
          break;
        case 's':
          second = number;
          break;
      }
    } else {
      // Literal: must match the next input character exactly.
      if (offset >= value.length || value[offset] != symbol) return null;
      offset++;
      index++;
    }
  }
  if (offset != value.length) return null; // trailing junk
  if (month < 1 ||
      month > 12 ||
      day < 1 ||
      day > 31 ||
      hour > 23 ||
      minute > 59 ||
      second > 59) {
    return null;
  }
  final date = DateTime.utc(year, month, day, hour, minute, second);
  // DateTime NORMALIZES an out-of-range day (Feb 31 → Mar 3); reject rather than
  // silently store the wrong date.
  if (date.year != year || date.month != month || date.day != day) return null;
  return date;
}
