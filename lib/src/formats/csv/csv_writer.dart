import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import '../../core/encoding.dart';
import '../../core/table.dart';
import '../../core/schema.dart';
import '../../core/data_type.dart';
import '../../core/conversions.dart';
import '../../core/paths.dart';
import 'csv_options.dart';
import 'package:meta/meta.dart';
import '../../core/failure.dart';
import '../../core/result.dart';

/// Optimized CSV writer for exporting data to CSV format
final class CsvWriter {
  final CsvOptions options;
  final Converter<String, List<int>> _encoder;
  final Uint8List _terminator;
  final String _quote;
  final String _escape;

  CsvWriter([CsvOptions? options])
    : options = options ?? const CsvOptions(),
      _encoder = resolveEncoding(options?.encoding).encoder,
      _terminator = Uint8List.fromList(
        resolveEncoding(
          options?.encoding,
        ).encoder.convert(options?.lineTerminator ?? '\n'),
      ),
      _quote = options?.quote ?? '"',
      _escape = options?.escape ?? '"' {
    // Single-char delimiter/quote/escape + a never-quoted null sentinel (the
    // reader assumes both). Fail loud at construction rather than with a
    // RangeError or a silent multi-char truncation mid-write.
    this.options.validate();
  }

  /// Write DataFrame to CSV file, STREAMING the result batch-by-batch so a
  /// production-sized result is never fully materialized (was `execute(maxRows:
  /// null, maxBytes: null)`). Atomic: a temp file is replaced onto [path] on
  /// success, deleted on failure.
  ///
  /// Like every write here, a failure is reported as an [Err]: [Kind.io] when
  /// the file cannot be written, [Kind.schema] for a later batch whose schema
  /// differs from the first.
  @useResult
  Future<Result<void, ErrorDetails>> writeFile(
    Stream<Table> batches,
    String path,
  ) => capture(() => _writeFile(batches, path));

  Future<void> _writeFile(Stream<Table> batches, String path) =>
      publishSink(path, (sink) => _writeStreaming(batches, sink));

  /// Write DataFrame to a caller-provided sink, streaming batch-by-batch.
  @useResult
  Future<Result<void, ErrorDetails>> writeStream(
    Stream<Table> batches,
    IOSink sink,
  ) => capture(() => _writeStreaming(batches, sink));

  Future<void> _writeStreaming(Stream<Table> batches, IOSink sink) async {
    Schema? frozen;
    var wrote = false;
    await for (final table in batches) {
      if (frozen == null) {
        frozen = table.schema;
        if (options.hasHeader) {
          _writeHeader(sink, frozen.fields);
        }
      } else if (!frozen.encodesAs(table.schema)) {
        // The header + column positions are fixed by the first batch; a later
        // batch that reorders/renames/retypes columns would silently write
        // values under the wrong headers (Arrow/Parquet enforce the same
        // exact-encoding invariant).
        throw Failure.of(
          Kind.schema,
          'CSV streaming write: a later batch schema is not encoding-compatible '
          'with the first batch (column names, types, and order must match)',
        );
      }
      wrote = _appendRows(sink, table, wrote);
    }
  }

  /// Emit [table]'s rows (no header), each already CSV-encoded, to [emit] in
  /// ~64 KiB chunks. A row terminator precedes every row except the first UNLESS
  /// [leading] is set: it is used when a header or a prior batch already wrote a
  /// trailing terminator, so the first row still needs a leading one; without it,
  /// writing a terminator before row 0 prepends a spurious blank line. Single
  /// loop shared by all three write entry points ([_appendRows], [writeSink],
  /// [writeTableToString]) so the row/column skeleton can't drift.
  void _emitRows(
    void Function(String) emit,
    Table table, {
    required bool leading,
  }) {
    final fields = table.schema.fields;
    final columns = table.columns;
    final types = [for (final f in fields) f.dataType];
    final buffer = StringBuffer();
    var wrote = leading;
    for (int r = 0; r < table.rowCount; r++) {
      if (wrote) buffer.write(options.lineTerminator);
      wrote = true;
      for (int c = 0; c < columns.length; c++) {
        if (c > 0) buffer.write(options.delimiter);
        buffer.write(_cell(columns[c][r], types[c], fields[c].scale));
      }
      if (buffer.length >= 65536) {
        emit(buffer.toString());
        buffer.clear();
      }
    }
    if (buffer.isNotEmpty) emit(buffer.toString());
  }

  /// The escaped, delimiter-joined header line for [fields] (no terminator).
  String _headerLine(List<Field> fields) =>
      [for (final f in fields) _quoted(f.name)].join(options.delimiter);

  /// Append [table]'s rows to [sink] (no header). [wrote] threads across
  /// batches so a row terminator is placed only BETWEEN rows (including at a
  /// batch boundary); returns the updated flag.
  bool _appendRows(IOSink sink, Table table, bool wrote) {
    _emitRows((s) => sink.add(_encoder.convert(s)), table, leading: wrote);
    return wrote || table.rowCount > 0;
  }

  /// Write [table] (with optional header) to [sink], chunk-flushed.
  @useResult
  Future<Result<void, ErrorDetails>> writeSink(Table table, IOSink sink) =>
      capture(() => _writeSink(table, sink));

  Future<void> _writeSink(Table table, IOSink sink) async {
    if (options.hasHeader) {
      _writeHeader(sink, table.schema.fields);
    }
    _emitRows((s) => sink.add(_encoder.convert(s)), table, leading: false);
  }

  /// Write the header line for [fields] and its terminator to [sink].
  void _writeHeader(IOSink sink, List<Field> fields) {
    sink.add(_encoder.convert(_headerLine(fields)));
    sink.add(_terminator);
  }

  /// Write a materialized [table] to a CSV string.
  Future<String> writeString(Table table) async => writeTableToString(table);

  /// Write Table to CSV file. Writes to a temp file and atomically replaces the
  /// destination on success, so a mid-write failure can't destroy the previous
  /// good output.
  @useResult
  Future<Result<void, ErrorDetails>> writeTable(Table table, String path) =>
      capture(() => _writeTable(table, path));

  Future<void> _writeTable(Table table, String path) =>
      publishSink(path, (sink) => _writeSink(table, sink));

  /// Write Table to a CSV string (with optional header).
  String writeTableToString(Table table) {
    final out = StringBuffer();
    if (options.hasHeader) {
      out.write(_headerLine(table.schema.fields));
      out.write(options.lineTerminator);
    }
    _emitRows(out.write, table, leading: false);
    return out.toString();
  }

  /// Format one output cell, already escaped. With [CsvOptions.nullOutput] set,
  /// null becomes the sentinel token and an empty string is force-quoted (`""`)
  /// so a single-column null/empty row is never an ambiguous blank line; without
  /// it, null writes an empty field (RFC-4180 default, indistinguishable from an
  /// empty string: the documented lossy behavior).
  String _cell(Object? value, DataType type, int? scale) {
    final sentinel = options.nullOutput;
    if (value == null) {
      // Validated to need no quoting, so emit it raw: an UNQUOTED sentinel is
      // what the reader maps back to null.
      return sentinel ?? '';
    }
    final formatted = _format(value, type, scale);
    if ((sentinel != null && formatted.isEmpty) ||
        options.isNullToken(formatted)) {
      // Force-quote an empty string OR a real value that a reader would take
      // for null (the sentinel or any null token, such as `NA`): a QUOTED
      // occurrence reads back as the literal string. Null tokens hold no
      // quote char, so no inner escaping is needed.
      return '$_quote$formatted$_quote';
    }
    return _quoted(formatted);
  }

  /// Optimized value formatting with less string operations
  String _format(dynamic value, DataType type, [int? scale]) {
    switch (type) {
      case DataType.boolean:
      case DataType.int8:
      case DataType.int16:
      case DataType.int32:
      case DataType.int64:
        return value.toString();

      case DataType.decimal:
        // Decimal values are unscaled BigInt; apply the field's scale so the
        // CSV shows 123.45, not the raw 12345.
        final string = value is BigInt
            ? formatDecimal(value, scale ?? 0)
            : value.toString();
        return (options.decimalSeparator != '.')
            ? string.replaceAll('.', options.decimalSeparator)
            : string;

      case DataType.float32:
      case DataType.float64:
        final string = value.toString();
        return (options.decimalSeparator != '.')
            ? string.replaceAll('.', options.decimalSeparator)
            : string;

      case DataType.date:
        if (value is DateTime) {
          if (options.dateFormat != null) {
            return _temporal(value, options.dateFormat!);
          }
          // Optimized date formatting without string concatenation
          final year = value.year.toString().padLeft(4, '0');
          final month = value.month.toString().padLeft(2, '0');
          final day = value.day.toString().padLeft(2, '0');
          return '$year-$month-$day';
        }
        return value.toString();

      case DataType.timestamp:
        if (value is DateTime) {
          if (options.timestampFormat != null) {
            return _temporal(value, options.timestampFormat!);
          }
          return value.toIso8601String();
        }
        return value.toString();

      case DataType.string:
      default:
        return value.toString();
    }
  }

  /// Optimized field escaping with early return for common cases
  String _quoted(String value) {
    // Fast path: check if escaping is needed at all
    final delimiter = options.delimiter;
    final quote = _quote;

    // Quick scan for special characters
    bool quoting = false;
    bool quotes = false;

    for (int i = 0; i < value.length; i++) {
      final char = value[i];
      if (char == delimiter || char == '\n' || char == '\r') {
        quoting = true;
        if (quotes) break; // Both conditions met
      } else if (char == quote) {
        quoting = true;
        quotes = true;
        if (quoting) break; // Both conditions met
      }
    }

    // Also check for leading/trailing spaces if trimFields is set
    if (!quoting && options.trimFields && value != value.trim()) {
      quoting = true;
    }

    // Fast path: no escaping needed
    if (!quoting) {
      return value;
    }

    // Inside quotes a distinct escape char escapes any char, itself included,
    // so it is escaped first; then the quotes.
    final escape = _escape;
    final literal = escape != quote && value.contains(escape)
        ? value.replaceAll(escape, escape + escape)
        : value;
    final escaped = quotes
        ? literal.replaceAll(quote, escape + quote)
        : literal;

    // Wrap in quotes
    return '$quote$escaped$quote';
  }

  /// Format DateTime according to format string
  String _temporal(DateTime moment, String format) {
    // Simple date/time formatting
    return format
        .replaceAll('yyyy', moment.year.toString().padLeft(4, '0'))
        .replaceAll('yy', (moment.year % 100).toString().padLeft(2, '0'))
        .replaceAll('MM', moment.month.toString().padLeft(2, '0'))
        .replaceAll('M', moment.month.toString())
        .replaceAll('dd', moment.day.toString().padLeft(2, '0'))
        .replaceAll('d', moment.day.toString())
        .replaceAll('HH', moment.hour.toString().padLeft(2, '0'))
        .replaceAll('H', moment.hour.toString())
        .replaceAll('mm', moment.minute.toString().padLeft(2, '0'))
        .replaceAll('m', moment.minute.toString())
        .replaceAll('ss', moment.second.toString().padLeft(2, '0'))
        .replaceAll('s', moment.second.toString())
        .replaceAll('SSS', moment.millisecond.toString().padLeft(3, '0'));
  }
}
