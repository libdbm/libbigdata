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
import '../../core/ids.dart' as ids;
import 'csv_options.dart';

/// Optimized CSV writer for exporting data to CSV format
class CsvWriter {
  final CsvOptions options;
  final Converter<String, List<int>> _encoder;
  final Uint8List _lineTerminatorBytes;
  final String _quoteStr;
  final String _escapeStr;

  CsvWriter([CsvOptions? options])
    : options = options ?? const CsvOptions(),
      _encoder = resolveEncoding(options?.encoding).encoder,
      _lineTerminatorBytes = Uint8List.fromList(
        resolveEncoding(
          options?.encoding,
        ).encoder.convert(options?.lineTerminator ?? '\n'),
      ),
      _quoteStr = options?.quote ?? '"',
      _escapeStr = options?.escape ?? '"' {
    // Single-char delimiter/quote/escape + a never-quoted null sentinel (the
    // reader assumes both). Fail loud at construction rather than with a
    // RangeError or a silent multi-char truncation mid-write.
    this.options.validate();
  }

  /// Write DataFrame to CSV file, STREAMING the result batch-by-batch so a
  /// production-sized result is never fully materialized (was `execute(maxRows:
  /// null, maxBytes: null)`). Atomic: a temp file is replaced onto [path] on
  /// success, deleted on failure.
  Future<void> writeFile(Stream<Table> batches, String path) async {
    final temp = '$path.${ids.token()}.tmp';
    final sink = File(temp).openWrite();
    try {
      await _writeStreaming(batches, sink);
      await sink.flush();
      await sink.close();
      await syncFile(temp); // IOSink.flush is not an fsync
      await replaceAtomic(File(temp), path);
    } catch (_) {
      try {
        await sink.close();
      } catch (_) {
        /* already closing/closed */
      }
      final f = File(temp);
      if (await f.exists()) await f.delete();
      rethrow;
    }
  }

  /// Write DataFrame to a caller-provided sink, streaming batch-by-batch.
  Future<void> writeStream(Stream<Table> batches, IOSink sink) =>
      _writeStreaming(batches, sink);

  Future<void> _writeStreaming(Stream<Table> batches, IOSink sink) async {
    Schema? frozen;
    var wroteRow = false;
    await for (final table in batches) {
      if (frozen == null) {
        frozen = table.schema;
        if (options.hasHeader) {
          _writeHeader(sink, frozen.fields.map((f) => f.name).toList());
        }
      } else if (!frozen.encodesAs(table.schema)) {
        // The header + column positions are fixed by the first batch; a later
        // batch that reorders/renames/retypes columns would silently write
        // values under the wrong headers (Arrow/Parquet enforce the same
        // exact-encoding invariant).
        throw StateError(
          'CSV streaming write: a later batch schema is not encoding-compatible '
          'with the first batch (column names, types, and order must match)',
        );
      }
      wroteRow = _appendRows(sink, table, wroteRow);
    }
  }

  /// Emit [table]'s rows (no header), each already CSV-encoded, to [emit] in
  /// ~64 KiB chunks. A row terminator precedes every row except the first UNLESS
  /// [leading] — used when a header or a prior batch already wrote a trailing
  /// terminator, so the first row still needs a leading one; without it, writing
  /// a terminator before row 0 prepends a spurious blank line. Single loop shared
  /// by all three write entry points ([_appendRows], [writeTableToStream],
  /// [writeTableToString]) so the row/column skeleton can't drift.
  void _emitRows(
    void Function(String) emit,
    Table table, {
    required bool leading,
  }) {
    final fields = table.schema.fields;
    final columns = table.columns;
    final dataTypes = [for (final f in fields) f.dataType];
    final buffer = StringBuffer();
    var wrote = leading;
    for (int r = 0; r < table.rowCount; r++) {
      if (wrote) buffer.write(options.lineTerminator);
      wrote = true;
      for (int c = 0; c < columns.length; c++) {
        if (c > 0) buffer.write(options.delimiter);
        buffer.write(_cell(columns[c][r], dataTypes[c], fields[c].scale));
      }
      if (buffer.length >= 65536) {
        emit(buffer.toString());
        buffer.clear();
      }
    }
    if (buffer.isNotEmpty) emit(buffer.toString());
  }

  /// The escaped, delimiter-joined header line for [table] (no terminator).
  String _headerLine(Table table) => [
    for (final f in table.schema.fields) _escapeFieldOptimized(f.name),
  ].join(options.delimiter);

  /// Append [table]'s rows to [sink] (no header). [wroteRow] threads across
  /// batches so a row terminator is placed only BETWEEN rows (including at a
  /// batch boundary); returns the updated flag.
  bool _appendRows(IOSink sink, Table table, bool wroteRow) {
    _emitRows((s) => sink.add(_encoder.convert(s)), table, leading: wroteRow);
    return wroteRow || table.rowCount > 0;
  }

  /// Write [table] (with optional header) to [sink], chunk-flushed.
  Future<void> writeTableToStream(Table table, IOSink sink) async {
    if (options.hasHeader) {
      _writeHeader(sink, [for (final f in table.schema.fields) f.name]);
    }
    _emitRows((s) => sink.add(_encoder.convert(s)), table, leading: false);
  }

  /// Optimized header writing
  void _writeHeader(IOSink sink, List<String> columnNames) {
    final headerLine = columnNames
        .map((name) => _escapeFieldOptimized(name))
        .join(options.delimiter);
    sink.add(_encoder.convert(headerLine));
    sink.add(_lineTerminatorBytes);
  }

  /// Write a materialized [table] to a CSV string.
  Future<String> writeString(Table table) async => writeTableToString(table);

  /// Write Table to CSV file. Writes to a temp file and atomically replaces the
  /// destination on success, so a mid-write failure can't destroy the previous
  /// good output.
  Future<void> writeTable(Table table, String path) async {
    final temp = '$path.${ids.token()}.tmp';
    final sink = File(temp).openWrite();

    try {
      await writeTableToStream(table, sink);
      await sink.flush();
      await sink.close();
      await syncFile(temp); // IOSink.flush is not an fsync
      await replaceAtomic(File(temp), path);
    } catch (_) {
      try {
        await sink.close();
      } catch (_) {
        /* already closing/closed */
      }
      final f = File(temp);
      if (await f.exists()) await f.delete();
      rethrow;
    }
  }

  /// Write Table to a CSV string (with optional header).
  String writeTableToString(Table table) {
    final out = StringBuffer();
    if (options.hasHeader) {
      out.write(_headerLine(table));
      out.write(options.lineTerminator);
    }
    _emitRows(out.write, table, leading: false);
    return out.toString();
  }

  /// Format one output cell, already escaped. With [CsvOptions.nullOutput] set,
  /// null becomes the sentinel token and an empty string is force-quoted (`""`)
  /// so a single-column null/empty row is never an ambiguous blank line; without
  /// it, null writes an empty field (RFC-4180 default, indistinguishable from an
  /// empty string — the documented lossy behavior).
  String _cell(Object? value, DataType type, int? scale) {
    final sentinel = options.nullOutput;
    if (value == null) {
      // Validated to need no quoting, so emit it raw — an UNQUOTED sentinel is
      // what the reader maps back to null.
      return sentinel ?? '';
    }
    final formatted = _formatValueOptimized(value, type, scale);
    if (sentinel != null && (formatted.isEmpty || formatted == sentinel)) {
      // Force-quote an empty string OR a real value that collides with the
      // sentinel: a QUOTED occurrence reads back as the literal string, never
      // null (the sentinel has no quote char, so no inner escaping is needed).
      return '$_quoteStr$formatted$_quoteStr';
    }
    return _escapeFieldOptimized(formatted);
  }

  /// Optimized value formatting with less string operations
  String _formatValueOptimized(dynamic value, DataType type, [int? scale]) {
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
            return _formatDateTime(value, options.dateFormat!);
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
            return _formatDateTime(value, options.timestampFormat!);
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
  String _escapeFieldOptimized(String value) {
    // Fast path: check if escaping is needed at all
    final delimiter = options.delimiter;
    final quote = _quoteStr;

    // Quick scan for special characters
    bool needsQuoting = false;
    bool hasQuotes = false;

    for (int i = 0; i < value.length; i++) {
      final char = value[i];
      if (char == delimiter || char == '\n' || char == '\r') {
        needsQuoting = true;
        if (hasQuotes) break; // Both conditions met
      } else if (char == quote) {
        needsQuoting = true;
        hasQuotes = true;
        if (needsQuoting) break; // Both conditions met
      }
    }

    // Also check for leading/trailing spaces if trimFields is set
    if (!needsQuoting && options.trimFields && value != value.trim()) {
      needsQuoting = true;
    }

    // Fast path: no escaping needed
    if (!needsQuoting) {
      return value;
    }

    // Escape quotes if present
    final escaped = hasQuotes
        ? value.replaceAll(quote, _escapeStr + quote)
        : value;

    // Wrap in quotes
    return '$quote$escaped$quote';
  }

  /// Format DateTime according to format string
  String _formatDateTime(DateTime dt, String format) {
    // Simple date/time formatting
    return format
        .replaceAll('yyyy', dt.year.toString().padLeft(4, '0'))
        .replaceAll('yy', (dt.year % 100).toString().padLeft(2, '0'))
        .replaceAll('MM', dt.month.toString().padLeft(2, '0'))
        .replaceAll('M', dt.month.toString())
        .replaceAll('dd', dt.day.toString().padLeft(2, '0'))
        .replaceAll('d', dt.day.toString())
        .replaceAll('HH', dt.hour.toString().padLeft(2, '0'))
        .replaceAll('H', dt.hour.toString())
        .replaceAll('mm', dt.minute.toString().padLeft(2, '0'))
        .replaceAll('m', dt.minute.toString())
        .replaceAll('ss', dt.second.toString().padLeft(2, '0'))
        .replaceAll('s', dt.second.toString())
        .replaceAll('SSS', dt.millisecond.toString().padLeft(3, '0'));
  }
}
