import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:logging/logging.dart';
import 'package:meta/meta.dart';

import '../../core/encoding.dart';
import '../../core/data_type.dart';
import '../../core/failure.dart';
import '../../core/limits.dart';
import '../../core/result.dart';
import '../../core/schema.dart';
import '../../core/table.dart';
import '../../util/lines.dart';
import '../../util/logging_config.dart' show Loggers;

/// Options for JSON reading and writing
final class JsonOptions {
  /// Whether to read/write as JSON Lines (one JSON object per line)
  final bool jsonLines;

  /// Whether to flatten nested objects (reading)
  final bool flattenNested;

  /// Separator for flattened field names (reading)
  final String flattenSeparator;

  /// Maximum depth for flattening (reading)
  final int maxFlattenDepth;

  /// Whether to infer types (reading)
  final bool inferTypes;

  /// Sample size for type inference (reading)
  final int typeInferenceSampleSize;

  /// Whether to handle arrays (reading)
  final bool expandArrays;

  /// Whether to pretty print JSON (writing, only for array format)
  final bool prettyPrint;

  /// Custom JSON encoder (writing)
  final JsonEncoder? encoder;

  /// Whether to include null values in output (writing)
  final bool includeNulls;

  /// Encoding for reading/writing
  final String encoding;

  /// Reading: reject malformed or non-object JSON Lines records with a
  /// line-numbered error (default). When false, such lines are skipped and a
  /// warning with the rejected count is logged.
  final bool strict;

  /// Streaming ([JsonReader.readBatches]): cap on the size (in characters) of a single
  /// JSON Lines record, so a malformed file with one unterminated multi-GB
  /// "line" fails loud instead of OOMing. Mirrors `CsvOptions.maxRecordBytes`.
  final int maxRecordBytes;

  const JsonOptions({
    this.jsonLines = false,
    this.flattenNested = true,
    this.flattenSeparator = '.',
    this.maxFlattenDepth = 3,
    this.inferTypes = true,
    this.typeInferenceSampleSize = 1000,
    this.expandArrays = false,
    this.prettyPrint = false,
    this.encoder,
    this.includeNulls = true,
    this.encoding = 'utf-8',
    this.strict = true,
    this.maxRecordBytes = 16 << 20, // 16 MB
  });

  JsonOptions copyWith({bool? jsonLines, bool? strict}) => JsonOptions(
    jsonLines: jsonLines ?? this.jsonLines,
    flattenNested: flattenNested,
    flattenSeparator: flattenSeparator,
    maxFlattenDepth: maxFlattenDepth,
    inferTypes: inferTypes,
    typeInferenceSampleSize: typeInferenceSampleSize,
    expandArrays: expandArrays,
    prettyPrint: prettyPrint,
    encoder: encoder,
    includeNulls: includeNulls,
    encoding: encoding,
    strict: strict ?? this.strict,
    maxRecordBytes: maxRecordBytes,
  );

  /// Create options for JSON Lines format
  factory JsonOptions.jsonLines() {
    return const JsonOptions(jsonLines: true);
  }

  /// Create options for pretty printed JSON array
  factory JsonOptions.prettyArray() =>
      const JsonOptions(jsonLines: false, prettyPrint: true);
}

/// Marks an integer too large for int64 while decoding (see [_decode]). Holds
/// NULs, which a decoded JSON string only has when its source escaped them.
const _bigint = '\u0000bigint\u0000';

final _longDigits = RegExp(r'\d{19}');

/// [text] decoded as JSON, with integers beyond int64 read exactly as [BigInt]
/// rather than rounded through a double (as `json.decode` does to a uint64
/// id). Only text holding a run of 19 or more digits is scanned.
Object? _decode(String text) {
  if (!_longDigits.hasMatch(text)) return json.decode(text);
  final out = StringBuffer();
  var quoted = false;
  var escaped = false;
  var i = 0;
  while (i < text.length) {
    final unit = text.codeUnitAt(i);
    if (quoted) {
      out.writeCharCode(unit);
      if (escaped) {
        escaped = false;
      } else if (unit == 0x5c) {
        escaped = true;
      } else if (unit == 0x22) {
        quoted = false;
      }
      i++;
      continue;
    }
    if (unit == 0x22) {
      quoted = true;
      out.writeCharCode(unit);
      i++;
      continue;
    }
    if (unit == 0x2d || (unit >= 0x30 && unit <= 0x39)) {
      // A number token: digits, sign, point and exponent.
      var end = i + 1;
      while (end < text.length) {
        final next = text.codeUnitAt(end);
        if ((next >= 0x30 && next <= 0x39) ||
            next == 0x2e ||
            next == 0x65 ||
            next == 0x45 ||
            next == 0x2b ||
            next == 0x2d) {
          end++;
        } else {
          break;
        }
      }
      final token = text.substring(i, end);
      final integer = !token.contains(RegExp('[.eE]'));
      if (integer && int.tryParse(token) == null) {
        // Escaped in the source; decoding turns it back into [_bigint].
        out.write('"\\u0000bigint\\u0000$token"');
      } else {
        out.write(token);
      }
      i = end;
      continue;
    }
    out.writeCharCode(unit);
    i++;
  }
  return json.decode(
    out.toString(),
    reviver: (_, value) => value is String && value.startsWith(_bigint)
        ? BigInt.tryParse(value.substring(_bigint.length)) ?? value
        : value,
  );
}

/// Stream the elements of a single top-level JSON array from a decoded character
/// [chunks] stream, yielding each element's raw JSON one at a time so a multi-GB
/// array is never fully buffered. A depth-tracking scanner (honoring strings and
/// escapes) finds element boundaries; each element's buffer is capped by
/// [maxBytes] so one oversized element can't OOM either.
Stream<String> _elements(Stream<String> chunks, int maxBytes) async* {
  var started = false; // seen the opening '['
  var closed = false; // seen the matching top-level ']'
  var active = false; // accumulating an element
  var expecting = false; // last separator was ',' → an element MUST follow
  var depth = 0; // {}/[] nesting within the current element
  var single = false; // the input is one top-level object, not an array
  var quoted = false, escaped = false;
  final element = StringBuffer();

  await for (final chunk in chunks) {
    for (var i = 0; i < chunk.length; i++) {
      final char = chunk[i];

      // Once the array closes, only trailing whitespace is valid through EOF:
      // `[{}] garbage` must be rejected, not silently truncated.
      if (closed) {
        if (char.trim().isEmpty) continue;
        throw const FormatException('Unexpected content after JSON array');
      }

      if (!started) {
        if (char.trim().isEmpty) continue;
        if (char == '{') {
          // A single top-level object: one element, closed by its own brace.
          started = true;
          single = true;
          active = true;
        } else if (char != '[') {
          throw const FormatException('Expected a top-level JSON array');
        } else {
          started = true;
          continue;
        }
      }

      if (!active) {
        if (char.trim().isEmpty) continue; // whitespace between tokens
        if (char == ']') {
          if (expecting) {
            throw const FormatException('Trailing comma in JSON array');
          }
          closed = true; // keep scanning to validate trailing input
          continue;
        }
        if (char == ',') {
          // A separator comma is consumed inline (below); reaching one here is a
          // leading comma (`[,…`) or a double comma (`[1,,…`).
          throw const FormatException('Unexpected comma in JSON array');
        }
        active = true; // first char of a new element (fall through)
        expecting = false;
      }
      element.write(char);
      if (element.length > maxBytes) {
        throw FormatException(
          'JSON array element exceeds maxRecordBytes ($maxBytes)',
        );
      }
      if (quoted) {
        if (escaped) {
          escaped = false;
        } else if (char == '\\') {
          escaped = true;
        } else if (char == '"') {
          quoted = false;
        }
        continue;
      }
      if (char == '"') {
        quoted = true;
      } else if (char == '{' || char == '[') {
        depth++;
      } else if (char == '}' || char == ']') {
        depth--;
        if (single && depth == 0) {
          // The single object closed.
          final text = element.toString();
          element.clear();
          active = false;
          closed = true;
          yield text;
        } else if (depth < 0) {
          // Only the array's ']' can close it; a stray '}' is malformed.
          if (char != ']') {
            throw const FormatException('Unexpected "}" in JSON array');
          }
          final text = element.toString();
          element.clear();
          active = false;
          closed = true;
          yield text.substring(0, text.length - 1); // drop the ']'
        }
      } else if (char == ',' && depth == 0 && !single) {
        final text = element.toString();
        element.clear();
        active = false;
        expecting = true; // an element must follow this separator
        yield text.substring(0, text.length - 1); // drop the trailing ','
      }
    }
  }
  // EOF: input must have opened AND closed a top-level array. An empty/never-
  // opened file or an unterminated array is truncated/invalid: fail loud.
  if (!started) {
    throw const FormatException(
      'Expected a top-level JSON array (empty input)',
    );
  }
  if (!closed) {
    throw const FormatException(
      'Unterminated JSON array (missing closing "]")',
    );
  }
}

/// JSON reader for parsing JSON and JSON Lines files
final class JsonReader {
  final JsonOptions options;

  JsonReader([JsonOptions? options]) : options = options ?? const JsonOptions();

  /// Read a JSON file into a [Table]. To stop an accidental full load from
  /// OOMing, the read STOPS with [Kind.limit] once it would exceed [maxRows] (default
  /// [defaultMaxMaterializedRows]); both JSON Lines AND a single large JSON
  /// array are streamed (via [readBatches]) so the whole file is never buffered.
  /// Pass `maxRows: null` to read the whole file at once (unbounded), only then
  /// is the entire file buffered, and whole-file (not sampled) schema inference
  /// is used.
  ///
  /// A failure is reported as an [Err]: [Kind.missing] for a file that does not
  /// exist, [Kind.corrupt] for invalid JSON, [Kind.limit] past a cap, and
  /// [Kind.invalid] for an unknown projected column.
  @useResult
  Future<Result<Table, ErrorDetails>> readFile(
    String path, {
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => capture(
    () => _readFile(path, maxRows: maxRows, maxBytes: maxBytes),
    fallback: Kind.corrupt,
  );

  Future<Table> _readFile(String path, {int? maxRows, int? maxBytes}) async {
    final file = File(path);
    if (!await file.exists()) {
      throw PathNotFoundException(
        path,
        const OSError('No such file or directory', 2),
        'File not found',
      );
    }

    // Auto-detect JSON Lines format from extension (preserving other options).
    final delimited = path.endsWith('.jsonl') || path.endsWith('.ndjson');
    final effective = delimited ? options.copyWith(jsonLines: true) : options;
    final reader = JsonReader(effective);

    if (maxRows != null || maxBytes != null) {
      // Bounded: stream batches (JSON Lines or array) and stop at the cap, so a
      // multi-GB file/array is never fully buffered.
      final rows = <Map<String, dynamic>>[];
      Schema? schema;
      var count = 0;
      var bytes = 0;
      await for (final batch in reader._batches(path)) {
        schema ??= batch.schema;
        bytes += batch.memoryUsage;
        // Check the overhead-adjusted size (row maps + column buffers coexist,
        // plus String/Map object overhead) so the cap predicts true peak.
        if (overByteCap(bytes, maxBytes)) {
          throw Failure.of(
            Kind.limit,
            'JSON exceeds maxBytes=$maxBytes; pass maxBytes: null to allow it '
            'or use readBatches for bounded-memory streaming',
          );
        }
        for (final row in batch.toRows()) {
          if (maxRows != null && ++count > maxRows) {
            throw Failure.of(
              Kind.limit,
              'JSON exceeds maxRows=$maxRows; pass maxRows: null to allow it '
              'or use readBatches for bounded-memory streaming',
            );
          }
          rows.add(row);
        }
      }
      if (schema == null) return Table.empty('json_data', Schema(fields: []));
      return Table.fromRows('json_data', rows, schema: schema);
    }

    // Reached only when the caller opted out of both caps: keep it unbounded.
    final content = await file.readAsString(encoding: _encoding());
    return reader._readString(content, maxRows: null, maxBytes: null);
  }

  /// Read JSON from stream. Buffers the whole stream in memory, so it enforces
  /// [maxBytes] as it accumulates (stop-at-cap): an unbounded request body
  /// can't OOM the process, and [maxRows] on the parsed result. Pass null to
  /// opt out.
  /// Failures are reported as [readFile] reports them.
  @useResult
  Future<Result<Table, ErrorDetails>> readStream(
    Stream<List<int>> stream, {
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => capture(
    () => _readStream(stream, maxRows: maxRows, maxBytes: maxBytes),
    fallback: Kind.corrupt,
  );

  Future<Table> _readStream(
    Stream<List<int>> stream, {
    int? maxRows,
    int? maxBytes,
  }) async {
    final decoder = _encoding().decoder;
    final chars = stream.transform(decoder);
    final buffer = StringBuffer();

    await for (final chunk in chars) {
      buffer.write(chunk);
      if (maxBytes != null && buffer.length > maxBytes) {
        throw Failure.of(
          Kind.limit,
          'JSON stream exceeds maxBytes=$maxBytes; pass maxBytes: null to '
          'allow it or use readBatches for bounded-memory streaming',
        );
      }
    }

    return _readString(buffer.toString(), maxRows: maxRows, maxBytes: maxBytes);
  }

  /// Read JSON from string. The input string is already in the caller's memory,
  /// so [maxRows]/[maxBytes] bound the parsed result (fail loud rather than
  /// materialize an oversized DataFrame). Pass null to opt out.
  /// Failures are reported as [readFile] reports them.
  @useResult
  Future<Result<Table, ErrorDetails>> readString(
    String jsonContent, {
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) => capture(
    () => _readString(jsonContent, maxRows: maxRows, maxBytes: maxBytes),
    fallback: Kind.corrupt,
  );

  Future<Table> _readString(
    String content, {
    int? maxRows,
    int? maxBytes,
  }) async {
    if (maxBytes != null && content.length > maxBytes) {
      throw Failure.of(
        Kind.limit,
        'JSON exceeds maxBytes=$maxBytes; pass maxBytes: null to allow it',
      );
    }
    List<Map<String, dynamic>> records;

    if (options.jsonLines) {
      records = _lines(content);
    } else {
      final decoded = _decode(content);
      if (decoded is List) {
        // Per ELEMENT, not a lazy `.cast()`: a cast defers the type check to
        // whoever reads the list, so a non-object element surfaced later as a
        // bare TypeError from inside _flatten/Schema.infer, never the
        // FormatException naming the element, and `strict: false` (skip + warn)
        // was silently ignored on this path. readFile() and
        // readFile(maxRows: null) disagreed on the very same file.
        records = [
          for (var i = 0; i < decoded.length; i++)
            ?_asRecord(decoded[i], 'at array element $i'),
        ];
      } else if (decoded is Map<String, dynamic>) {
        records = [decoded];
      } else {
        throw FormatException('JSON must be an object or array of objects');
      }
    }

    if (maxRows != null && records.length > maxRows) {
      throw Failure.of(
        Kind.limit,
        'JSON exceeds maxRows=$maxRows; pass maxRows: null to allow it or use '
        'readBatches for bounded-memory streaming',
      );
    }

    if (records.isEmpty) {
      return Table.empty('json_data', Schema(fields: []));
    }

    // Flatten nested objects if requested
    if (options.flattenNested) {
      records = records.map((r) => _flatten(r)).toList();
    }

    // Expand arrays if requested. The cap above bounded the RECORD count, but
    // one record expands into as many rows as its longest array, so maxRows is
    // re-applied inside the expansion, which is where the row count is actually
    // decided.
    if (options.expandArrays) {
      records = _expand(records, maxRows);
    }

    // Infer schema. The records are already fully materialized here, so scan
    // ALL of them: a leading sample would silently drop fields that first
    // appear later (data loss). (typeInferenceSampleSize only bounds the
    // streaming reader, where the whole input isn't held in memory.)
    final schema = options.inferTypes
        ? Schema.infer(records, sampleSize: records.length)
        : _stringSchema(records);

    // The schema was inferred FROM these records, so it is not a strict caller
    // contract: a value resolved to a wider type (a field mixing JSON `1` and
    // `"a"` infers string) is coerced to it, not rejected. strict: false.
    return Table.fromRows('json_data', records, schema: schema, strict: false);
  }

  /// Stream a JSON file as [Table] batches of ~[size] rows, holding only one
  /// batch in memory. Supports both JSON Lines (`options.jsonLines` or a
  /// `.jsonl`/`.ndjson` path) and a single top-level JSON array (streamed
  /// element-by-element via a depth-tracking scanner, so a multi-GB array does
  /// not OOM). The schema is inferred from a leading sample and pinned for every
  /// batch. [columns] pushes a projection down, only those keys are kept per
  /// record (unselected columns are never allocated). `expandArrays` is not
  /// supported here (it would break fixed batch sizing).
  ///
  /// Pass an explicit [schema] to skip leading-sample inference entirely and pin
  /// it up front: records are conformed to it (unknown fields are dropped,
  /// missing fields are null), so a late record with a new field can never fail
  /// the stream after earlier batches were already yielded.
  ///
  /// Each batch arrives as an [Ok]; a failure as one [Err], reported as
  /// [readFile] reports it, which ends the stream.
  /// A [batchSize] below 1 is a programming error and throws at the call.
  Stream<Result<Table, ErrorDetails>> readBatches(
    String path, {
    int batchSize = 10000,
    List<String>? columns,
    Schema? schema,
  }) {
    if (batchSize <= 0) {
      throw ArgumentError.value(batchSize, 'batchSize', 'must be > 0');
    }
    return captureStream(
      () => _batches(path, size: batchSize, columns: columns, schema: schema),
      fallback: Kind.corrupt,
    );
  }

  Stream<Table> _batches(
    String path, {
    int size = 10000,
    List<String>? columns,
    Schema? schema,
  }) async* {
    if (options.expandArrays) {
      throw Failure.of(
        Kind.unsupported,
        'expandArrays is not supported by readBatches',
      );
    }
    final file = File(path);
    if (!await file.exists()) {
      throw PathNotFoundException(
        path,
        const OSError('No such file or directory', 2),
        'File not found',
      );
    }
    final records = StreamIterator(_records(file));
    try {
      // Leading sample → pinned schema (kept to feed the first batch).
      final sample = <Map<String, dynamic>>[];
      while (sample.length < options.typeInferenceSampleSize &&
          await records.moveNext()) {
        sample.add(records.current);
      }
      if (sample.isEmpty) return;
      // An explicit schema pins up front; otherwise infer from the sample.
      final pinned = schema != null;
      var resolved =
          schema ??
          (options.inferTypes
              ? Schema.infer(
                  sample,
                  sampleSize: options.typeInferenceSampleSize,
                )
              : _stringSchema(sample));
      // Projection pushdown: restrict the schema to the requested columns. A
      // requested column absent from the (sample-inferred) schema fails loud:
      // consistent with Parquet/CSV, rather than silently narrowing.
      final project = columns != null;
      if (project) {
        final present = {for (final f in resolved.fields) f.name};
        final missing = columns.where((c) => !present.contains(c)).toList();
        if (missing.isNotEmpty) {
          throw Failure.of(
            Kind.invalid,
            'Unknown column(s) in projection: ${missing.join(', ')}',
          );
        }
        final keep = columns.toSet();
        resolved = Schema(
          fields: [
            for (final f in resolved.fields)
              if (keep.contains(f.name)) f,
          ],
        );
      }
      // A sample-inferred schema can't be widened retroactively for
      // already-yielded batches, so a later record with a new field is a
      // fail-closed error. When projecting OR pinned to an explicit schema, an
      // unknown field is simply dropped (the schema is the caller's contract).
      final drop = project || pinned;
      final allowed = {for (final f in resolved.fields) f.name};

      var cursor = 0;
      Future<Map<String, dynamic>?> next() async {
        Map<String, dynamic>? record;
        if (cursor < sample.length) {
          record = sample[cursor++];
        } else if (await records.moveNext()) {
          record = records.current;
        } else {
          return null;
        }
        if (drop) {
          // Keep only the allowed keys (drop the rest without allocating them).
          return {
            for (final key in record.keys)
              if (allowed.contains(key)) key: record[key],
          };
        }
        for (final key in record.keys) {
          if (!allowed.contains(key)) {
            throw FormatException(
              'JSON field "$key" is absent from the schema inferred from the '
              'first ${sample.length} record(s); increase '
              'typeInferenceSampleSize or use readFile for whole-file inference',
            );
          }
        }
        return record;
      }

      var done = false;
      while (!done) {
        final batch = <Map<String, dynamic>>[];
        while (batch.length < size) {
          final row = await next();
          if (row == null) {
            done = true;
            break;
          }
          batch.add(row);
        }
        if (batch.isNotEmpty) {
          // Only a caller-PINNED schema is a strict contract; a sample-inferred
          // one is derived from these records, so coerce (don't reject) a value
          // that resolved to a wider type.
          yield Table.fromRows(
            'json_data',
            batch,
            schema: resolved,
            strict: pinned,
          );
        }
      }
    } finally {
      await records.cancel();
    }
  }

  /// Stream decoded record maps from [file]: JSON Lines via the byte-capped line
  /// splitter, or a single top-level JSON array via the streaming array scanner.
  Stream<Map<String, dynamic>> _records(File file) async* {
    final chars = file.openRead().transform(_encoding().decoder);
    if (options.jsonLines) {
      var number = 0;
      await for (final line in cappedLines(
        chars,
        options.maxRecordBytes,
        label: 'JSON line',
      )) {
        number++;
        final text = line.trim();
        final record = text.isEmpty ? null : _record(text, 'on line $number');
        if (record != null) yield record;
      }
    } else {
      var count = 0;
      await for (final element in _elements(chars, options.maxRecordBytes)) {
        final record = _record(element, 'at array element ${++count}');
        if (record != null) yield record;
      }
    }
  }

  /// [text] decoded as one record (flattened when configured and [flatten]),
  /// or null when permissive mode skips it: malformed JSON, or a value that is
  /// not an object. [where] places it in the error or warning (`on line 3`,
  /// `at array element 2`). The one rule for every JSON read path.
  Map<String, dynamic>? _record(
    String text,
    String where, {
    bool flatten = true,
  }) {
    final Object? decoded;
    try {
      decoded = _decode(text);
    } on FormatException catch (e) {
      if (options.strict) {
        throw FormatException('Invalid JSON $where: ${e.message}');
      }
      _log.warning('JSON: skipped a malformed record $where');
      return null;
    }
    return _asRecord(decoded, where, flatten: flatten);
  }

  /// [decoded] as a record (flattened when configured and [flatten]), or null
  /// when permissive mode skips a non-object; [where] is as for [_record].
  Map<String, dynamic>? _asRecord(
    Object? decoded,
    String where, {
    bool flatten = false,
  }) {
    if (decoded is Map<String, dynamic>) {
      return flatten && options.flattenNested ? _flatten(decoded) : decoded;
    }
    if (options.strict) {
      throw FormatException(
        'Expected a JSON object $where, got ${decoded.runtimeType}',
      );
    }
    _log.warning('JSON: skipped a non-object $where');
    return null;
  }

  static final Logger _log = Logger(Loggers.json);

  /// Parse JSON Lines format. In strict mode (default) a malformed or non-object
  /// line raises a line-numbered [FormatException]; otherwise such lines are
  /// skipped and the rejected count is logged (no silent data loss).
  List<Map<String, dynamic>> _lines(String content) {
    final lines = content.split('\n');
    return [
      for (int i = 0; i < lines.length; i++)
        if (lines[i].trim() case final text when text.isNotEmpty)
          ?_record(text, 'on line ${i + 1}', flatten: false),
    ];
  }

  /// Flatten nested objects.
  ///
  /// Joining nested keys with [JsonOptions.flattenSeparator] is NOT injective:
  /// a literal key that already contains the separator can produce the same
  /// flattened name as a nested path (`{"a.b": 1, "a": {"b": 2}}`, both become
  /// `a.b`). Flattening is lossy and one-way (there is no un-flatten on the
  /// write path), so a collision is rejected rather than resolved: silently
  /// keeping whichever value happened to come last would discard valid source
  /// data based on nothing but key order in the document.
  Map<String, dynamic> _flatten(
    Map<String, dynamic> object, [
    String prefix = '',
    int depth = 0,
  ]) {
    if (depth >= options.maxFlattenDepth) {
      return {prefix: object};
    }

    final result = <String, dynamic>{};

    void put(String key, Object? value) {
      if (result.containsKey(key)) {
        throw FormatException(
          'JSON flattening produced the duplicate field "$key": a literal key '
          'and a nested path collide on it. Read with '
          'JsonOptions(flattenNested: false), or rename the field.',
        );
      }
      result[key] = value;
    }

    for (final entry in object.entries) {
      final key = prefix.isEmpty
          ? entry.key
          : '$prefix${options.flattenSeparator}${entry.key}';
      final value = entry.value;

      if (value is Map<String, dynamic>) {
        // The nested call may return several keys (or, at the depth limit, the
        // prefix itself), each must clear the same collision check.
        _flatten(value, key, depth + 1).forEach(put);
      } else {
        // Arrays are kept as-is; expansion happens after flattening.
        put(key, value);
      }
    }

    return result;
  }

  /// Expand arrays into multiple rows
  /// Expand each record's array fields into one row per element, stopping the
  /// moment the result would exceed [maxRows] (null opts out). The check is per
  /// ROW rather than after the fact: expansion amplifies, so building the whole
  /// list and then rejecting it would materialize the very blow-up the cap
  /// exists to prevent.
  List<Map<String, dynamic>> _expand(
    List<Map<String, dynamic>> records,
    int? maxRows,
  ) {
    final expanded = <Map<String, dynamic>>[];

    void add(Map<String, dynamic> row) {
      if (maxRows != null && expanded.length == maxRows) {
        throw Failure.of(
          Kind.limit,
          'JSON exceeds maxRows=$maxRows once expandArrays expands its arrays '
          'into rows; pass maxRows: null to allow it',
        );
      }
      expanded.add(row);
    }

    for (final record in records) {
      final arrays = <String, List>{};
      final scalars = <String, dynamic>{};

      // Separate array and scalar fields
      for (final entry in record.entries) {
        if (entry.value is List) {
          arrays[entry.key] = entry.value as List;
        } else {
          scalars[entry.key] = entry.value;
        }
      }

      if (arrays.isEmpty) {
        add(record);
        continue;
      }

      // Find the maximum array length
      int longest = 0;
      for (final array in arrays.values) {
        if (array.length > longest) {
          longest = array.length;
        }
      }

      // Create expanded rows
      for (int i = 0; i < longest; i++) {
        final row = Map<String, dynamic>.from(scalars);

        for (final entry in arrays.entries) {
          final array = entry.value;
          row[entry.key] = i < array.length ? array[i] : null;
        }

        add(row);
      }
    }

    return expanded;
  }

  /// Create a schema with all string fields
  Schema _stringSchema(List<Map<String, dynamic>> records) {
    final names = <String>{};
    for (final record in records) {
      names.addAll(record.keys);
    }

    return Schema(
      fields: names
          .map(
            (name) =>
                Field(name: name, dataType: DataType.string, nullable: true),
          )
          .toList(),
    );
  }

  /// Get encoding
  Encoding _encoding() => resolveEncoding(options.encoding);
}
