import 'dart:async';
import 'dart:convert';
import 'dart:io';

import '../../core/table.dart';
import '../../core/schema.dart';
import '../../core/decimal.dart';
import '../../core/encoding.dart' show resolveEncoding;
import '../../core/paths.dart';
import 'json_reader.dart' show JsonOptions;
import 'package:meta/meta.dart';
import '../../core/failure.dart';
import '../../core/result.dart';

/// Make non-JSON-native values encodable: temporal columns hold [DateTime]
/// (→ ISO-8601) and a decimal arrives from [Table.row] as a [Decimal]
/// (→ "123.45"). A bare [BigInt] is the fallback for anything the schema didn't
/// mark decimal (e.g. nested) → its string form.
///
/// Decimals and BigInts are emitted as QUOTED STRINGS, on purpose: a JSON number
/// is IEEE-754 double on essentially every consumer, so `12345678901234567890`
/// or `0.30` would silently round. Dart's [JsonEncoder] also cannot emit a raw
/// unquoted decimal token (only a double) so a string is the sole exact wire
/// form. A consumer that wants a number parses the string; a round-trip back
/// through [JsonReader] re-infers the field as `string` (it has no schema to say
/// otherwise). This is a deliberate exactness-over-type-fidelity tradeoff.
Object? _toEncodable(Object? value) {
  // JSON has no NaN or infinity; they write as null.
  if (value is double && !value.isFinite) return null;
  if (value is DateTime) return value.toUtc().toIso8601String();
  if (value is Decimal) return value.toString();
  if (value is BigInt) return value.toString();
  return value;
}

/// JSON writer for exporting data to JSON format
final class JsonWriter {
  final JsonOptions options;
  late final JsonEncoder _encoder;

  JsonWriter([JsonOptions? options])
    : options = options ?? const JsonOptions() {
    _encoder =
        this.options.encoder ??
        (this.options.prettyPrint
            ? JsonEncoder.withIndent('  ', _toEncodable)
            : JsonEncoder(_toEncodable));
  }

  /// Write DataFrame to JSON file, STREAMING the result batch-by-batch (was
  /// `execute(maxRows: null, maxBytes: null)` + one giant encoded string). JSON
  /// Lines and compact arrays stream; a PRETTY-PRINTED array keeps the buffered
  /// path (human-readable output, typically small).
  ///
  /// Like every write here, a failure is reported as an [Err] ([Kind.io] when
  /// the file cannot be written), with no partial file left behind.
  @useResult
  Future<Result<void, ErrorDetails>> writeFile(
    Stream<Table> batches,
    String path,
  ) => capture(() => _writeFile(batches, path));

  /// Whether [path] gets JSON Lines: by the options, or by its `.jsonl` or
  /// `.ndjson` extension, which the reader goes by too.
  bool _lines(String path) =>
      options.jsonLines || path.endsWith('.jsonl') || path.endsWith('.ndjson');

  Future<void> _writeFile(Stream<Table> batches, String path) async {
    final delimited = _lines(path);
    if (!delimited && options.prettyPrint) {
      await _write(await _collect(batches), path, false);
      return;
    }
    await publishSink(
      path,
      (sink) => _stream(batches, sink, jsonLines: delimited),
      encoding: _encoding,
    );
  }

  /// Write DataFrame as JSON Lines to a caller-provided sink, streaming.
  @useResult
  Future<Result<void, ErrorDetails>> writeJsonLinesToStream(
    Stream<Table> batches,
    IOSink sink,
  ) => capture(() => _stream(batches, sink, jsonLines: true));

  /// Stream a DataFrame to [sink] as JSON Lines (one compact object per line) or
  /// a compact array (`[`, comma-separated elements, `]`), batch-by-batch so the
  /// whole result is never materialized.
  Future<void> _stream(
    Stream<Table> batches,
    IOSink sink, {
    required bool jsonLines,
  }) async {
    final encoder = JsonEncoder(_toEncodable); // compact (line/array-safe)
    final buffer = StringBuffer();
    var wrote = false;
    if (!jsonLines) buffer.write('[');
    await for (final table in batches) {
      for (int i = 0; i < table.rowCount; i++) {
        if (wrote) buffer.write(jsonLines ? '\n' : ',');
        wrote = true;
        buffer.write(encoder.convert(_row(table, i)));
        if (buffer.length >= 1 << 20) {
          sink.write(buffer.toString());
          buffer.clear();
        }
      }
    }
    if (!jsonLines) buffer.write(']');
    if (buffer.isNotEmpty) sink.write(buffer.toString());
  }

  /// Write [table] rows as JSON Lines to [sink]. Encodes compactly (pretty-print
  /// would break the line-delimited format) and flushes in ~1MB batches: a
  /// per-row IOSink.write is ~25x slower for large files.
  void _writeLines(Table table, IOSink sink) {
    final encoder = JsonEncoder(_toEncodable);
    final buffer = StringBuffer();
    final n = table.rowCount;
    for (int i = 0; i < n; i++) {
      if (i > 0) buffer.write('\n');
      buffer.write(encoder.convert(_row(table, i)));
      if (buffer.length >= 1 << 20) {
        sink.write(buffer.toString());
        buffer.clear();
      }
    }
    if (buffer.isNotEmpty) sink.write(buffer.toString());
  }

  /// Build row [i] as a JSON object, dropping nulls per [options]. Decimal and
  /// DateTime are rendered by the encoder ([_toEncodable]).
  Map<String, dynamic> _row(Table table, int i) {
    final row = table.row(i);
    return options.includeNulls ? row : _removeNulls(row);
  }

  /// Write Table to JSON file
  @useResult
  Future<Result<void, ErrorDetails>> writeTable(Table table, String path) =>
      capture(() => _write(table, path, _lines(path)));

  /// Write [table] to [path], crash-safely: encode into a temp file and
  /// atomically replace the destination on success, so a mid-write failure can't
  /// destroy the previous good output.
  Future<void> _write(Table table, String path, bool jsonLines) async {
    if (jsonLines) {
      await publishSink(
        path,
        (sink) async => _writeLines(table, sink),
        encoding: _encoding,
      );
    } else {
      await publish(
        path,
        (temp) => File(
          temp,
        ).writeAsString(_encoder.convert(_rows(table)), encoding: _encoding),
      );
    }
  }

  /// Collect a batch stream into one table (for the buffered pretty path). Uses
  /// one N-way [Table.concat] rather than pairwise [Table.append] in a loop,
  /// which recopies all prior rows each step (O(n^2) over the stream).
  Future<Table> _collect(Stream<Table> batches) async {
    final tables = <Table>[];
    await for (final batch in batches) {
      tables.add(batch);
    }
    return tables.isEmpty
        ? Table.empty('json_data', Schema(fields: const []))
        : Table.concat(tables);
  }

  /// Convert a materialized [table] to a JSON string.
  String writeString(Table table) => writeTableToString(table);

  /// Convert Table to JSON string
  String writeTableToString(Table table) {
    if (options.jsonLines) {
      final buffer = StringBuffer();
      final encoder = JsonEncoder(_toEncodable);
      for (int i = 0; i < table.rowCount; i++) {
        if (i > 0) buffer.write('\n');
        buffer.write(encoder.convert(_row(table, i)));
      }
      return buffer.toString();
    } else {
      final rows = _rows(table);
      return _encoder.convert(rows);
    }
  }

  /// Convert table to list of JSON objects
  List<Map<String, dynamic>> _rows(Table table) {
    final rows = <Map<String, dynamic>>[];
    for (int i = 0; i < table.rowCount; i++) {
      rows.add(_row(table, i));
    }
    return rows;
  }

  /// The configured [JsonOptions.encoding].
  Encoding get _encoding => resolveEncoding(options.encoding);

  /// Remove null values from a row
  Map<String, dynamic> _removeNulls(Map<String, dynamic> row) {
    return Map.fromEntries(row.entries.where((e) => e.value != null));
  }
}

extension TableJsonWriterExtension on Table {
  /// Write this table to a JSON file, reporting a failure as
  /// [JsonWriter.writeTable] does.
  @useResult
  Future<Result<void, ErrorDetails>> writeJson(
    String filePath, [
    JsonOptions? options,
  ]) => JsonWriter(options).writeTable(this, filePath);

  /// Write this table to a JSON Lines file
  @useResult
  Future<Result<void, ErrorDetails>> writeJsonLines(
    String filePath, [
    JsonOptions? options,
  ]) =>
      JsonWriter(options ?? JsonOptions.jsonLines()).writeTable(this, filePath);

  /// Convert this table to JSON string
  String toJson([JsonOptions? options]) {
    final writer = JsonWriter(options);
    return writer.writeTableToString(this);
  }

  /// Convert this table to JSON Lines string
  String toJsonLines([JsonOptions? options]) {
    final writer = JsonWriter(options ?? JsonOptions.jsonLines());
    return writer.writeTableToString(this);
  }
}
