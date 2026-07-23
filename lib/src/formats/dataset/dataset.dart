import 'dart:io';
import 'dart:isolate';

import '../../core/column.dart';
import '../../core/partitions.dart';
import '../../core/data_type.dart';
import '../../core/limits.dart';
import '../../core/schema.dart';
import '../../core/table.dart';
import '../../core/columnar.dart';
import '../../util/parallel.dart';
import '../../core/scan.dart' show Scan;
import '../scan.dart';
import '../../core/row_filter.dart';
import '../parallel_read.dart';

/// A multi-file dataset: a directory, a glob, or a single file read as one
/// logical table. Discovers Hive-style `key=value` partition columns from the
/// path, prunes whole files via a [RowFilter] over partition values (and pushes
/// the same filter into Parquet row-group pruning), and unifies the schema
/// across files (union of fields, null-filling absentees).
class Dataset {
  /// A directory, a glob (`dir/**`, `dir/*.parquet`), or a single file.
  final String source;

  /// Optional pruning predicate ([RowFilter]). Applied to partition
  /// values to skip files, and forwarded to the Parquet reader per file.
  final RowFilter? filter;

  /// Optional column projection on the unified result.
  final List<String>? columns;

  /// Max independent file reads in flight. `1` (default) reads serially on the
  /// main isolate (unchanged); `>1` reads files in parallel across isolates.
  /// Only used on the unbounded (`maxRows: null`) eager path.
  final int concurrency;

  /// Safety caps on the materialized result for [read] (defaults
  /// [defaultMaxMaterializedRows] / [defaultMaxMaterializedBytes]). When either
  /// is set, [read] streams via [readBatches] and stops at the cap, so an
  /// oversized dataset can't OOM. Both `null` reads everything (the eager
  /// `_unify` path, which can use [concurrency]).
  final int? maxRows;
  final int? maxBytes;

  Dataset(
    this.source, {
    this.filter,
    this.columns,
    this.concurrency = 1,
    this.maxRows = defaultMaxMaterializedRows,
    this.maxBytes = defaultMaxMaterializedBytes,
  });

  static const _exts = [
    '.parquet',
    '.csv',
    '.jsonl',
    '.ndjson',
    '.json',
    '.arrow',
  ];

  Future<Table> read() async {
    // Safe by default: stream and stop at the cap so a giant dataset/file can't
    // OOM. Both caps null opts into the unbounded eager path below.
    if (maxRows != null || maxBytes != null) return _readBounded();

    // Prune whole files by partition predicate first, so pruned files never
    // spawn read work. Kept files retain their input order.
    final kept = <(String, Map<String, Object?>)>[];
    for (final file in await _resolve(source)) {
      final parts = _partitions(file);
      if (filter != null && !_partitionKeep(filter!, parts)) continue;
      kept.add((file, parts));
    }
    if (kept.isEmpty) {
      return Table.empty('dataset', Schema(fields: const []));
    }

    final List<Table> raw;
    if (concurrency <= 1) {
      // Project and read through ONE scan per file, so the footer parsed to
      // work out the projection is reused by the read instead of being parsed
      // again. Interleaved rather than projecting every file up front: only one
      // file's footer is alive at a time, which matters at a million files.
      raw = [for (final e in kept) await _readOne(e.$1, e.$2)];
    } else {
      // Per-file column projection: push the requested columns into each Parquet
      // read (intersected with the file's own schema, minus partition columns)
      // instead of reading every column and projecting after unification. The
      // isolate re-opens the file itself (a Scan can't cross an isolate), so
      // this path still parses each footer twice.
      final projections = [
        for (final e in kept) await _fileProjection(openScan(e.$1), e.$1, e.$2),
      ];
      final localFilter = filter; // bind locally so the isolate closure
      // captures only sendable values, not `this`.
      final localMaxRows = maxRows;
      final localMaxBytes = maxBytes;
      raw = await parallelMap(
        [for (int i = 0; i < kept.length; i++) (kept[i].$1, projections[i])],
        (e) => Isolate.run(
          () => readTableTask(
            ReadSpec(
              e.$1,
              filter: localFilter,
              columns: e.$2,
              maxRows: localMaxRows,
              maxBytes: localMaxBytes,
            ),
          ),
        ),
        concurrency: concurrency,
      );
    }

    final tables = [
      for (int i = 0; i < kept.length; i++)
        _appendPartitions(raw[i], kept[i].$2),
    ];
    return _unify(tables);
  }

  /// Bounded eager read: drain [readBatches] (which streams each file and stops
  /// short of loading a giant one) and stop the moment the running row count or
  /// byte size would exceed the cap, so the result is never larger than the cap
  /// in memory.
  Future<Table> _readBounded() async {
    final batches = <Table>[];
    var rows = 0;
    var bytes = 0;
    await for (final b in readBatches()) {
      rows += b.rowCount;
      bytes += b.memoryUsage;
      enforceMaterializationCaps(rows, bytes, maxRows, maxBytes, 'dataset');
      batches.add(b);
    }
    if (batches.isEmpty) {
      return Table.empty('dataset', Schema(fields: const []));
    }
    if (batches.length == 1) return batches.first;
    // Every batch is already conformed to the same (unified, projected) schema.
    final fields = batches.first.schema.fields;
    return Table(
      name: 'dataset',
      schema: batches.first.schema,
      columns: [
        for (final f in fields)
          Column.concat([for (final b in batches) b.column(f.name)!]),
      ],
    );
  }

  // --- file resolution -----------------------------------------------------

  Future<List<String>> _resolve(String source) async {
    final type = await FileSystemEntity.type(source);
    if (type == FileSystemEntityType.file) return [source];
    if (type == FileSystemEntityType.directory) return _listDir(source);
    if (source.contains('*')) return _glob(source);
    throw ArgumentError('Dataset path not found: $source');
  }

  Future<List<String>> _listDir(String dir) async {
    final out = <String>[];
    await for (final e in Directory(dir).list(recursive: true)) {
      if (e is File && _supported(e.path)) out.add(e.path);
    }
    out.sort();
    return out;
  }

  Future<List<String>> _glob(String pattern) async {
    final segments = pattern.split('/');
    final base = <String>[];
    for (final s in segments) {
      if (s.contains('*')) break;
      base.add(s);
    }
    final dir = base.isEmpty ? '.' : base.join('/');
    final regex = _globRegex(pattern);
    final out = <String>[];
    await for (final e in Directory(dir).list(recursive: true)) {
      if (e is File && regex.hasMatch(e.path)) out.add(e.path);
    }
    out.sort();
    return out;
  }

  RegExp _globRegex(String pattern) {
    final buffer = StringBuffer('^');
    for (int i = 0; i < pattern.length; i++) {
      final c = pattern[i];
      if (c == '*') {
        if (i + 1 < pattern.length && pattern[i + 1] == '*') {
          buffer.write('.*');
          i++;
        } else {
          buffer.write('[^/]*');
        }
      } else if (c == '?') {
        buffer.write('[^/]');
      } else if ('.+(){}[]^\$|\\'.contains(c)) {
        buffer.write('\\$c');
      } else {
        buffer.write(c);
      }
    }
    buffer.write(r'$');
    return RegExp(buffer.toString());
  }

  bool _supported(String path) {
    final lower = path.toLowerCase();
    return _exts.any(lower.endsWith);
  }

  // --- partitions ----------------------------------------------------------

  /// Parse `key=value` segments anywhere in [path] into typed partition values.
  Map<String, Object?> _partitions(String path) {
    final out = <String, Object?>{};
    for (final seg
        in path.split('/').expand((s) => s.split(Platform.pathSeparator))) {
      final eq = seg.indexOf('=');
      if (eq > 0 && eq < seg.length - 1) {
        final key = seg.substring(0, eq);
        if (key.isNotEmpty && !key.contains('.')) {
          out[key] = _coerce(seg.substring(eq + 1));
        }
      }
    }
    return out;
  }

  Object? _coerce(String value) {
    if (isNullPartition(value)) return null;
    // Hive/Spark (and this library's Delta writer) escape special characters
    // in the segment value as %XX — `a b` is written `a b` but `50%` is
    // `50%25`; taking the text literally returned the ESCAPED form.
    final raw = unescapePathName(value);
    final i = int.tryParse(raw);
    if (i != null) return i;
    final d = double.tryParse(raw);
    if (d != null) return d;
    if (raw == 'true') return true;
    if (raw == 'false') return false;
    return raw;
  }

  bool _partitionKeep(RowFilter filter, Map<String, Object?> parts) {
    final bounds = <String, ColumnBounds>{
      for (final e in parts.entries)
        e.key: ColumnBounds(
          min: e.value,
          max: e.value,
          nullCount: e.value == null ? 1 : 0,
          rowCount: 1,
          hasStats: true,
        ),
    };
    return filter.keep(bounds);
  }

  Table _appendPartitions(Table table, Map<String, Object?> parts) {
    if (parts.isEmpty) return table;
    final columns = [...table.columns];
    final fields = [...table.schema.fields];
    final existing = table.schema.fields.map((f) => f.name).toSet();
    parts.forEach((key, value) {
      if (existing.contains(key)) return; // file column wins over partition
      final type = _typeOf(value);
      columns.add(
        columnFromValues(key, type, List.filled(table.rowCount, value)),
      );
      fields.add(Field(name: key, dataType: type, nullable: value == null));
    });
    return Table(
      name: table.name,
      columns: columns,
      schema: Schema(fields: fields),
    );
  }

  /// The type of a partition value decoded from a path segment.
  ///
  /// Deliberately NOT [DataType.fromValue]: that narrows an integer by
  /// magnitude, and partition types are derived PER FILE — `part=5` (int8) and
  /// `part=300` (int16) would then be "incompatible types" in [_unionFields].
  /// The widths here are the widest of each family so every file agrees.
  ///
  /// A null partition (the `__HIVE_DEFAULT_PARTITION__` sentinel this library
  /// itself writes) MUST be [DataType.null_] — that is the escape hatch
  /// [_unionFields] uses to let a null column sit under a sibling file's real
  /// type. Typing it `string` made that hatch unreachable, so a perfectly
  /// ordinary Hive layout failed to read.
  DataType _typeOf(Object? value) {
    if (value == null) return DataType.null_;
    if (value is int) return DataType.int64;
    if (value is double) return DataType.float64;
    if (value is bool) return DataType.boolean;
    return DataType.string;
  }

  // --- per-file read -------------------------------------------------------

  /// Columns to read from [file]: the requested projection intersected with the
  /// file's own schema and minus partition columns (which come from the path),
  /// or null to read all. Only Parquet projection is pushed down today; an empty
  /// intersection keeps one column so the file's row count survives null-filling.
  /// Read one file through a single [Scan], so the footer parsed for the
  /// projection is reused by the read (see [_ParquetScan]).
  Future<Table> _readOne(String file, Map<String, Object?> parts) async {
    final scan = openScan(file);
    return _readFile(scan, await _fileProjection(scan, file, parts));
  }

  Future<List<String>?> _fileProjection(
    Scan scan,
    String file,
    Map<String, Object?> parts,
  ) async {
    final cols = columns;
    if (cols == null || !file.toLowerCase().endsWith('.parquet')) return null;
    final schema = await scan.readSchema();
    final fileNames = schema == null
        ? const <String>[]
        : schema.fields.map((f) => f.name).toList();
    final partKeys = parts.keys.toSet();
    final proj = [
      for (final c in cols)
        if (fileNames.contains(c) && !partKeys.contains(c)) c,
    ];
    if (proj.isNotEmpty) return proj;
    return fileNames.isEmpty ? null : [fileNames.first];
  }

  Future<Table> _readFile(Scan scan, [List<String>? projection]) async {
    // Format selection came from the single registry (openScan); forward the
    // dataset-level caps so `null` (opt-out) is truly unbounded per file — not
    // silently re-defaulted to 10M / 1 GiB — and a smaller dataset cap is
    // honoured. (The cap bounds each file, not the dataset total.)
    return scan.read(
      maxRows: maxRows,
      maxBytes: maxBytes,
      projection: projection,
      filter: filter,
    );
  }

  // --- schema unification --------------------------------------------------

  /// First-seen-order union of fields across [perFile] schemas (no data). Data
  /// fields are marked nullable: a field may be absent from some file (null-filled
  /// there), and per-file schemas come from a bounded peek that can't prove a
  /// later row isn't null — so non-nullness can't be guaranteed across a streaming
  /// heterogeneous dataset.
  List<Field> _unionFields(Iterable<List<Field>> perFile) {
    final fieldByName = <String, Field>{};
    final order = <String>[];
    for (final fields in perFile) {
      for (final field in fields) {
        final seen = fieldByName[field.name];
        if (seen == null) {
          fieldByName[field.name] = field.nullable
              ? field
              : field.copyWith(nullable: true);
          order.add(field.name);
          continue;
        }
        // null_ carries no shape — promote to the first CONCRETE type. The
        // null-partition sentinel starts with '_', so its file can sort (and
        // be seen) FIRST; first-seen-wins then froze the field as null_ and
        // every concrete column failed validation.
        if (seen.dataType == DataType.null_) {
          if (field.dataType != DataType.null_) {
            fieldByName[field.name] = field.copyWith(nullable: true);
          }
          continue;
        }
        if (field.dataType == DataType.null_) continue;
        if (seen.dataType != field.dataType) {
          // First-seen-wins would otherwise seat a later file's differently
          // typed column under this field's type and silently mislabel data.
          throw ArgumentError(
            'Incompatible types for column "${field.name}" across dataset '
            'files: ${seen.dataType} vs ${field.dataType}',
          );
        }
        // Same DataType is NOT the whole story: decimal scale and nested
        // children live on the FIELD, and merging across a mismatch corrupts
        // values (unscaled BigInts reinterpreted) or mis-pairs struct members.
        if (seen.dataType == DataType.decimal) {
          final unified = _unifyDecimal(seen, field);
          if (unified != null) fieldByName[field.name] = unified;
          continue;
        }
        if (!seen.isCompatibleWith(field)) {
          throw ArgumentError(
            'Incompatible ${field.dataType.name} shapes for column '
            '"${field.name}" across dataset files (nested children differ)',
          );
        }
      }
    }
    return [for (final n in order) fieldByName[n]!];
  }

  /// The unified decimal field for [seen] + [next]: the WIDER scale wins
  /// (values are rescaled to it in [_conform]/[_unify]) and precision keeps
  /// room for the widest integer part at that scale. Null when [seen] already
  /// covers [next].
  Field? _unifyDecimal(Field seen, Field next) {
    final seenScale = seen.scale ?? 0;
    final nextScale = next.scale ?? 0;
    final scale = seenScale > nextScale ? seenScale : nextScale;
    final sp = seen.precision;
    final np = next.precision;
    final int? precision;
    if (sp == null || np == null) {
      precision = sp ?? np;
    } else {
      final ints = (sp - seenScale) > (np - nextScale)
          ? (sp - seenScale)
          : (np - nextScale);
      final wanted = ints + scale;
      precision = wanted > 38 ? 38 : wanted;
    }
    if (scale == seenScale && precision == seen.precision) return null;
    return seen.copyWith(scale: scale, precision: precision);
  }

  /// [batch]'s column for [field], rescaled if the file wrote the decimal at
  /// a narrower scale than the unified field carries.
  Column? _aligned(Table batch, Field field) {
    final column = batch.column(field.name);
    if (column == null || field.dataType != DataType.decimal) return column;
    final own = batch.schema.field(field.name)?.scale ?? 0;
    final want = field.scale ?? 0;
    return own == want ? column : column.rescaleDecimal(own, want);
  }

  /// Reorder [batch]'s columns to [unified], null-filling any field the batch
  /// lacks. Per-batch (no cross-file concat), so it stays bounded for streaming.
  Table _conform(Table batch, List<Field> unified) {
    return Table(
      name: 'dataset',
      schema: Schema(fields: unified),
      columns: [
        for (final field in unified)
          _aligned(batch, field) ??
              columnFromValues(
                field.name,
                field.dataType,
                List.filled(batch.rowCount, null),
              ),
      ],
    );
  }

  Table _unify(List<Table> tables) {
    final unifiedFields = _unionFields([
      for (final t in tables) t.schema.fields,
    ]);
    final names = unifiedFields.map((f) => f.name).toSet();
    final outColumns = <Column>[
      for (final field in unifiedFields)
        Column.concat([
          for (final table in tables)
            _aligned(table, field) ??
                columnFromValues(
                  field.name,
                  field.dataType,
                  List.filled(table.rowCount, null),
                ),
        ]),
    ];

    var result = Table(
      name: 'dataset',
      columns: outColumns,
      schema: Schema(fields: unifiedFields),
    );
    final select = columns;
    if (select != null) {
      _checkProjection(select, names);
      result = result.select(select);
    }
    return result;
  }

  void _checkProjection(List<String> select, Set<String> available) {
    final missing = select.where((c) => !available.contains(c)).toList();
    if (missing.isNotEmpty) {
      throw ArgumentError(
        'Unknown column(s) in projection: ${missing.join(', ')}',
      );
    }
  }

  // --- streaming read ------------------------------------------------------

  /// Stream the dataset as a sequence of [size]-row(-ish) batches conformed to
  /// one unified schema, without ever holding all files (or a second unified
  /// copy) in memory. Async file discovery + partition pruning are shared with
  /// [read]. Single-isolate (a `Stream` cannot cross `Isolate.run`, so the
  /// parallel-read option is only available on the eager [read]). Plain `.json`
  /// (not line-delimited) has no streaming reader and is emitted as one batch.
  /// [schema], when supplied, PINS the unified schema and SKIPS the per-file
  /// metadata pre-pass entirely — a million-file dataset then costs no up-front
  /// O(files) schema reads (each file's reader still conforms to it). Without it,
  /// the schema is derived by peeking every file's fields (the historical path).
  Stream<Table> readBatches({int size = 10000, Schema? schema}) async* {
    if (size <= 0) throw ArgumentError.value(size, 'size', 'must be > 0');
    final kept = <(String, Map<String, Object?>)>[];
    for (final file in await _resolve(source)) {
      final parts = _partitions(file);
      if (filter != null && !_partitionKeep(filter!, parts)) continue;
      kept.add((file, parts));
    }
    if (kept.isEmpty) return;

    List<Field> unified;
    // Only when we peek: each file's PUSHED PROJECTION (the small list of column
    // names to read), NOT the full per-file Field list — so the stream doesn't
    // retain O(files) worth of schema metadata after unification is computed.
    List<List<String>?>? projections;
    if (schema != null) {
      unified = schema.fields;
    } else {
      // Cheap metadata pre-pass: derive the unified schema (file fields + their
      // partition columns) up front so every emitted batch can be conformed to
      // it — a stream can't retroactively widen already-emitted batches. The
      // peeked file fields are also reused to compute each file's projection.
      // Overlap the per-file schema reads up to [concurrency] (order
      // preserved): serially, this pre-pass was the whole time-to-first-row on
      // wide datasets over slow storage. Pass [schema] to skip it entirely.
      final peeked = concurrency > 1
          ? await parallelMap(
              [for (final (file, _) in kept) file],
              _fileFields,
              concurrency: concurrency,
            )
          : [for (final (file, _) in kept) await _fileFields(file)];
      final perFile = <List<Field>>[];
      projections = <List<String>?>[];
      for (int i = 0; i < kept.length; i++) {
        final (_, parts) = kept[i];
        final fileFields = peeked[i];
        projections.add(
          _projectionFor(fileFields),
        ); // small; fileFields dropped
        final withParts = [...fileFields];
        final have = fileFields.map((f) => f.name).toSet();
        parts.forEach((key, value) {
          if (!have.contains(key)) {
            withParts.add(
              Field(
                name: key,
                dataType: _typeOf(value),
                nullable: value == null,
              ),
            );
          }
        });
        perFile.add(withParts);
      }
      unified = _unionFields(perFile);
    }
    final select = columns;
    if (select != null) {
      _checkProjection(select, unified.map((f) => f.name).toSet());
    }

    for (int i = 0; i < kept.length; i++) {
      final (file, parts) = kept[i];
      // Push the projection into the reader (all formats) so unselected columns
      // are never parsed/allocated. Only available when we peeked file fields; a
      // pinned schema reads all columns and projects via the trailing select.
      final projection = projections?[i];
      await for (final batch in _streamFile(file, projection, size)) {
        final conformed = _conform(_appendPartitions(batch, parts), unified);
        yield select == null ? conformed : conformed.select(select);
      }
    }
  }

  /// The requested columns that exist in [fileFields] (partition columns come
  /// from the path, not the reader), or null when no projection. When the
  /// intersection is empty, keep one column so the file's row count survives.
  List<String>? _projectionFor(List<Field> fileFields) {
    final cols = columns;
    if (cols == null) return null;
    final names = fileFields.map((f) => f.name).toSet();
    final proj = [
      for (final c in cols)
        if (names.contains(c)) c,
    ];
    if (proj.isNotEmpty) return proj;
    return fileFields.isEmpty ? null : [fileFields.first.name];
  }

  /// Cheap per-file field list for the unification pre-pass, via the single
  /// registry: Parquet from the footer; other formats by peeking the first
  /// streamed batch's schema. Empty file: contributes no fields.
  ///
  /// Deliberately does NOT hold its [Scan] to share the parsed footer with the
  /// later stream: the pre-pass visits every file before the first batch is
  /// emitted, so keeping the scans alive would hold every file's footer at once
  /// — worse, at a million files, than the parse it saves. Pass `schema:` to
  /// skip the pre-pass entirely.
  Future<List<Field>> _fileFields(String path) async =>
      (await openScan(path).readSchema())?.fields ?? const [];

  /// Stream one file as batches via its format's streaming reader (openScan).
  Stream<Table> _streamFile(String path, List<String>? projection, int size) =>
      openScan(
        path,
      ).batches(size: size, projection: projection, filter: filter);
}
