import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:logging/logging.dart';

import '../../core/encoding.dart';
import '../../core/data_type.dart';
import '../../core/limits.dart';
import '../../core/schema.dart';
import '../../core/table.dart';
import '../../util/lines.dart';

/// Options for JSON reading and writing
class JsonOptions {
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

  /// Streaming ([readBatches]): cap on the size (in characters) of a single
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

/// Stream the elements of a single top-level JSON array from a decoded character
/// [chunks] stream, yielding each element's raw JSON one at a time so a multi-GB
/// array is never fully buffered. A depth-tracking scanner (honoring strings and
/// escapes) finds element boundaries; each element's buffer is capped by
/// [maxBytes] so one oversized element can't OOM either.
Stream<String> _streamJsonArray(Stream<String> chunks, int maxBytes) async* {
  var started = false; // seen the opening '['
  var closed = false; // seen the matching top-level ']'
  var inElem = false; // accumulating an element
  var afterComma = false; // last separator was ',' → an element MUST follow
  var depth = 0; // {}/[] nesting within the current element
  var inStr = false, esc = false;
  final element = StringBuffer();

  await for (final chunk in chunks) {
    for (var i = 0; i < chunk.length; i++) {
      final ch = chunk[i];

      // Once the array closes, only trailing whitespace is valid through EOF —
      // `[{}] garbage` must be rejected, not silently truncated.
      if (closed) {
        if (ch.trim().isEmpty) continue;
        throw const FormatException('Unexpected content after JSON array');
      }

      if (!started) {
        if (ch.trim().isEmpty) continue;
        if (ch != '[') {
          throw const FormatException('Expected a top-level JSON array');
        }
        started = true;
        continue;
      }

      if (!inElem) {
        if (ch.trim().isEmpty) continue; // whitespace between tokens
        if (ch == ']') {
          if (afterComma) {
            throw const FormatException('Trailing comma in JSON array');
          }
          closed = true; // keep scanning to validate trailing input
          continue;
        }
        if (ch == ',') {
          // A separator comma is consumed inline (below); reaching one here is a
          // leading comma (`[,…`) or a double comma (`[1,,…`).
          throw const FormatException('Unexpected comma in JSON array');
        }
        inElem = true; // first char of a new element (fall through)
        afterComma = false;
      }
      element.write(ch);
      if (element.length > maxBytes) {
        throw FormatException(
          'JSON array element exceeds maxRecordBytes ($maxBytes)',
        );
      }
      if (inStr) {
        if (esc) {
          esc = false;
        } else if (ch == '\\') {
          esc = true;
        } else if (ch == '"') {
          inStr = false;
        }
        continue;
      }
      if (ch == '"') {
        inStr = true;
      } else if (ch == '{' || ch == '[') {
        depth++;
      } else if (ch == '}' || ch == ']') {
        depth--;
        if (depth < 0) {
          // The closing ']' of the array ended this element.
          final s = element.toString();
          element.clear();
          inElem = false;
          closed = true;
          yield s.substring(0, s.length - 1); // drop the ']'
        }
      } else if (ch == ',' && depth == 0) {
        final s = element.toString();
        element.clear();
        inElem = false;
        afterComma = true; // an element must follow this separator
        yield s.substring(0, s.length - 1); // drop the trailing ','
      }
    }
  }
  // EOF: input must have opened AND closed a top-level array. An empty/never-
  // opened file or an unterminated array is truncated/invalid — fail loud.
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
class JsonReader {
  final JsonOptions options;

  JsonReader([JsonOptions? options]) : options = options ?? const JsonOptions();

  /// Read a JSON file into a [DataFrame]. To stop an accidental full load from
  /// OOMing, the read STOPS and throws once it would exceed [maxRows] (default
  /// [defaultMaxMaterializedRows]); both JSON Lines AND a single large JSON
  /// array are streamed (via [readBatches]) so the whole file is never buffered.
  /// Pass `maxRows: null` to read the whole file at once (unbounded) — only then
  /// is the entire file buffered, and whole-file (not sampled) schema inference
  /// is used.
  Future<Table> readFile(
    String path, {
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) async {
    final file = File(path);
    if (!await file.exists()) {
      throw FileSystemException('File not found', path);
    }

    // Auto-detect JSON Lines format from extension (preserving other options).
    final useJsonLines = path.endsWith('.jsonl') || path.endsWith('.ndjson');
    final effectiveOptions = useJsonLines
        ? options.copyWith(jsonLines: true)
        : options;
    final reader = JsonReader(effectiveOptions);

    if (maxRows != null || maxBytes != null) {
      // Bounded: stream batches (JSON Lines or array) and stop at the cap, so a
      // multi-GB file/array is never fully buffered.
      final rows = <Map<String, dynamic>>[];
      Schema? schema;
      var n = 0;
      var bytes = 0;
      await for (final batch in reader.readBatches(path)) {
        schema ??= batch.schema;
        bytes += batch.memoryUsage;
        // Gate on the overhead-adjusted size (row maps + column buffers coexist,
        // plus String/Map object overhead) so the cap predicts true peak.
        if (overByteCap(bytes, maxBytes)) {
          throw StateError(
            'JSON exceeds maxBytes=$maxBytes; pass maxBytes: null to allow it '
            'or use readBatches for bounded-memory streaming',
          );
        }
        for (final r in batch.toRows()) {
          if (maxRows != null && ++n > maxRows) {
            throw StateError(
              'JSON exceeds maxRows=$maxRows; pass maxRows: null to allow it '
              'or use readBatches for bounded-memory streaming',
            );
          }
          rows.add(r);
        }
      }
      if (schema == null) return Table.empty('json_data', Schema(fields: []));
      return Table.fromRows('json_data', rows, schema: schema);
    }

    // Reached only when the caller opted out of both caps — keep it unbounded.
    final content = await file.readAsString(encoding: _encoding());
    return reader.readString(content, maxRows: null, maxBytes: null);
  }

  /// Read JSON from stream. Buffers the whole stream in memory, so it enforces
  /// [maxBytes] as it accumulates (stop-at-cap) — an unbounded request body
  /// can't OOM the process — and [maxRows] on the parsed result. Pass null to
  /// opt out.
  Future<Table> readStream(
    Stream<List<int>> stream, {
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) async {
    final decoder = _encoding().decoder;
    final stringStream = stream.transform(decoder);
    final buffer = StringBuffer();

    await for (final chunk in stringStream) {
      buffer.write(chunk);
      if (maxBytes != null && buffer.length > maxBytes) {
        throw StateError(
          'JSON stream exceeds maxBytes=$maxBytes; pass maxBytes: null to '
          'allow it or use readBatches for bounded-memory streaming',
        );
      }
    }

    return readString(buffer.toString(), maxRows: maxRows, maxBytes: maxBytes);
  }

  /// Read JSON from string. The input string is already in the caller's memory,
  /// so [maxRows]/[maxBytes] bound the parsed result (fail loud rather than
  /// materialize an oversized DataFrame). Pass null to opt out.
  Future<Table> readString(
    String jsonContent, {
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
  }) async {
    if (maxBytes != null && jsonContent.length > maxBytes) {
      throw StateError(
        'JSON exceeds maxBytes=$maxBytes; pass maxBytes: null to allow it',
      );
    }
    List<Map<String, dynamic>> records;

    if (options.jsonLines) {
      records = _parseJsonLines(jsonContent);
    } else {
      final decoded = json.decode(jsonContent);
      if (decoded is List) {
        // Per ELEMENT, not a lazy `.cast()`: a cast defers the type check to
        // whoever reads the list, so a non-object element surfaced later as a
        // bare TypeError from inside _flattenObject/Schema.infer — never the
        // FormatException naming the element, and `strict: false` (skip + warn)
        // was silently ignored on this path. readFile() and
        // readFile(maxRows: null) disagreed on the very same file.
        records = [
          for (var i = 0; i < decoded.length; i++)
            if (_asRecord(decoded[i], i) case final record?) record,
        ];
      } else if (decoded is Map<String, dynamic>) {
        records = [decoded];
      } else {
        throw FormatException('JSON must be an object or array of objects');
      }
    }

    if (maxRows != null && records.length > maxRows) {
      throw StateError(
        'JSON exceeds maxRows=$maxRows; pass maxRows: null to allow it or use '
        'readBatches for bounded-memory streaming',
      );
    }

    if (records.isEmpty) {
      return Table.empty('json_data', Schema(fields: []));
    }

    // Flatten nested objects if requested
    if (options.flattenNested) {
      records = records.map((r) => _flattenObject(r)).toList();
    }

    // Expand arrays if requested. The cap above bounded the RECORD count, but
    // one record expands into as many rows as its longest array — so maxRows is
    // re-applied inside the expansion, which is where the row count is actually
    // decided.
    if (options.expandArrays) {
      records = _expandArrays(records, maxRows);
    }

    // Infer schema. The records are already fully materialized here, so scan
    // ALL of them — a leading sample would silently drop fields that first
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
  /// batch. [columns] pushes a projection down — only those keys are kept per
  /// record (unselected columns are never allocated). `expandArrays` is not
  /// supported here (it would break fixed batch sizing).
  ///
  /// Pass an explicit [schema] to skip leading-sample inference entirely and pin
  /// it up front: records are conformed to it (unknown fields are dropped,
  /// missing fields are null), so a late record with a new field can never fail
  /// the stream after earlier batches were already yielded.
  Stream<Table> readBatches(
    String path, {
    int size = 10000,
    List<String>? columns,
    Schema? schema,
  }) async* {
    if (size <= 0) throw ArgumentError.value(size, 'size', 'must be > 0');
    if (options.expandArrays) {
      throw UnsupportedError('expandArrays is not supported by readBatches');
    }
    final file = File(path);
    if (!await file.exists()) {
      throw FileSystemException('File not found', path);
    }
    final records = StreamIterator(_recordStream(file));
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
      // requested column absent from the (sample-inferred) schema fails loud —
      // consistent with Parquet/CSV — rather than silently narrowing.
      final project = columns != null;
      if (project) {
        final present = {for (final f in resolved.fields) f.name};
        final missing = columns.where((c) => !present.contains(c)).toList();
        if (missing.isNotEmpty) {
          throw ArgumentError(
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
      final dropUnknown = project || pinned;
      final allowed = {for (final f in resolved.fields) f.name};

      var sampleIdx = 0;
      Future<Map<String, dynamic>?> nextRow() async {
        Map<String, dynamic>? rec;
        if (sampleIdx < sample.length) {
          rec = sample[sampleIdx++];
        } else if (await records.moveNext()) {
          rec = records.current;
        } else {
          return null;
        }
        if (dropUnknown) {
          // Keep only the allowed keys (drop the rest without allocating them).
          return {
            for (final k in rec.keys)
              if (allowed.contains(k)) k: rec[k],
          };
        }
        for (final key in rec.keys) {
          if (!allowed.contains(key)) {
            throw FormatException(
              'JSON field "$key" is absent from the schema inferred from the '
              'first ${sample.length} record(s); increase '
              'typeInferenceSampleSize or use readFile for whole-file inference',
            );
          }
        }
        return rec;
      }

      var done = false;
      while (!done) {
        final batch = <Map<String, dynamic>>[];
        while (batch.length < size) {
          final row = await nextRow();
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
  Stream<Map<String, dynamic>> _recordStream(File file) async* {
    final chars = file.openRead().transform(_encoding().decoder);
    if (options.jsonLines) {
      var lineNo = 0;
      await for (final line in cappedLines(
        chars,
        options.maxRecordBytes,
        label: 'JSON line',
      )) {
        final rec = _decodeLine(line, ++lineNo);
        if (rec != null) yield rec;
      }
    } else {
      var n = 0;
      await for (final element in _streamJsonArray(
        chars,
        options.maxRecordBytes,
      )) {
        final rec = _decodeElement(element, ++n);
        if (rec != null) yield rec;
      }
    }
  }

  /// One already-decoded array element as a record, or null when a non-object is
  /// skipped in permissive mode. The eager twin of [_decodeElement]'s type
  /// check — that path decodes the text itself; here `json.decode` has already
  /// run over the whole array.
  Map<String, dynamic>? _asRecord(Object? decoded, int n) {
    if (decoded is Map<String, dynamic>) return decoded;
    if (options.strict) {
      throw FormatException(
        'Expected a JSON object at array element $n, got ${decoded.runtimeType}',
      );
    }
    _log.warning('JSON array: skipped non-object element $n');
    return null;
  }

  /// Decode one streamed array element (flattened when configured), or null for
  /// a skipped non-object in permissive mode.
  Map<String, dynamic>? _decodeElement(String raw, int n) {
    Object? decoded;
    try {
      decoded = json.decode(raw);
    } on FormatException catch (e) {
      if (options.strict) {
        throw FormatException('Invalid JSON array element $n: ${e.message}');
      }
      _log.warning('JSON array: skipped malformed element $n');
      return null;
    }
    if (decoded is Map<String, dynamic>) {
      return options.flattenNested ? _flattenObject(decoded) : decoded;
    }
    if (options.strict) {
      throw FormatException(
        'Expected a JSON object at array element $n, got ${decoded.runtimeType}',
      );
    }
    _log.warning('JSON array: skipped non-object element $n');
    return null;
  }

  /// Decode one JSON Lines record (flattened when configured), or null for a
  /// blank/skipped line. Strict mode throws with the 1-based line number.
  Map<String, dynamic>? _decodeLine(String line, int lineNo) {
    final trimmed = line.trim();
    if (trimmed.isEmpty) return null;
    Object? decoded;
    try {
      decoded = json.decode(trimmed);
    } on FormatException catch (e) {
      if (options.strict) {
        throw FormatException('Invalid JSON on line $lineNo: ${e.message}');
      }
      _log.warning('JSON Lines: skipped malformed line $lineNo');
      return null;
    }
    if (decoded is Map<String, dynamic>) {
      return options.flattenNested ? _flattenObject(decoded) : decoded;
    }
    if (options.strict) {
      throw FormatException(
        'Expected a JSON object on line $lineNo, got ${decoded.runtimeType}',
      );
    }
    _log.warning('JSON Lines: skipped non-object line $lineNo');
    return null;
  }

  static final Logger _log = Logger('JsonReader');

  /// Parse JSON Lines format. In strict mode (default) a malformed or non-object
  /// line raises a line-numbered [FormatException]; otherwise such lines are
  /// skipped and the rejected count is logged (no silent data loss).
  List<Map<String, dynamic>> _parseJsonLines(String content) {
    final lines = content.split('\n');
    final records = <Map<String, dynamic>>[];
    int rejected = 0;

    for (int i = 0; i < lines.length; i++) {
      final trimmed = lines[i].trim();
      if (trimmed.isEmpty) continue;

      Object? decoded;
      try {
        decoded = json.decode(trimmed);
      } on FormatException catch (e) {
        if (options.strict) {
          throw FormatException('Invalid JSON on line ${i + 1}: ${e.message}');
        }
        rejected++;
        continue;
      }
      if (decoded is Map<String, dynamic>) {
        records.add(decoded);
      } else if (options.strict) {
        throw FormatException(
          'Expected a JSON object on line ${i + 1}, got ${decoded.runtimeType}',
        );
      } else {
        rejected++;
      }
    }

    if (rejected > 0) {
      _log.warning(
        'JSON Lines: skipped $rejected malformed/non-object line(s)',
      );
    }
    return records;
  }

  /// Flatten nested objects.
  ///
  /// Joining nested keys with [JsonOptions.flattenSeparator] is NOT injective:
  /// a literal key that already contains the separator can produce the same
  /// flattened name as a nested path (`{"a.b": 1, "a": {"b": 2}}` — both become
  /// `a.b`). Flattening is lossy and one-way (there is no un-flatten on the
  /// write path), so a collision is rejected rather than resolved: silently
  /// keeping whichever value happened to come last would discard valid source
  /// data based on nothing but key order in the document.
  Map<String, dynamic> _flattenObject(
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
        // prefix itself) — each must clear the same collision check.
        _flattenObject(value, key, depth + 1).forEach(put);
      } else if (value is List && !options.expandArrays) {
        // Keep arrays as-is unless expanding
        put(key, value);
      } else {
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
  List<Map<String, dynamic>> _expandArrays(
    List<Map<String, dynamic>> records,
    int? maxRows,
  ) {
    final expanded = <Map<String, dynamic>>[];

    void add(Map<String, dynamic> row) {
      if (maxRows != null && expanded.length == maxRows) {
        throw StateError(
          'JSON exceeds maxRows=$maxRows once expandArrays expands its arrays '
          'into rows; pass maxRows: null to allow it',
        );
      }
      expanded.add(row);
    }

    for (final record in records) {
      final arrayFields = <String, List>{};
      final scalarFields = <String, dynamic>{};

      // Separate array and scalar fields
      for (final entry in record.entries) {
        if (entry.value is List) {
          arrayFields[entry.key] = entry.value as List;
        } else {
          scalarFields[entry.key] = entry.value;
        }
      }

      if (arrayFields.isEmpty) {
        add(record);
        continue;
      }

      // Find the maximum array length
      int maxLength = 0;
      for (final array in arrayFields.values) {
        if (array.length > maxLength) {
          maxLength = array.length;
        }
      }

      // Create expanded rows
      for (int i = 0; i < maxLength; i++) {
        final expandedRow = Map<String, dynamic>.from(scalarFields);

        for (final entry in arrayFields.entries) {
          final array = entry.value;
          expandedRow[entry.key] = i < array.length ? array[i] : null;
        }

        add(expandedRow);
      }
    }

    return expanded;
  }

  /// Create a schema with all string fields
  Schema _stringSchema(List<Map<String, dynamic>> records) {
    final fieldNames = <String>{};
    for (final record in records) {
      fieldNames.addAll(record.keys);
    }

    return Schema(
      fields: fieldNames
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
