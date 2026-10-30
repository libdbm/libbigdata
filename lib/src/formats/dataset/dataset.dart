import 'dart:io';
import 'dart:isolate';

import 'package:meta/meta.dart';

import '../../core/column.dart';
import '../../core/partitions.dart';
import '../../core/data_type.dart';
import '../../core/failure.dart';
import '../../core/limits.dart';
import '../../core/result.dart';
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
final class Dataset {
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

  static const _extensions = [
    '.parquet',
    '.csv',
    '.jsonl',
    '.ndjson',
    '.json',
    '.arrow',
  ];

  /// Read the whole dataset. A failure is reported as an [Err]:
  /// [Kind.missing] for a path that does not exist, [Kind.schema] for column
  /// types that differ across files, [Kind.invalid] for an unknown projected
  /// column, [Kind.limit] past a cap, and the file readers' kinds otherwise.
  @useResult
  Future<Result<Table, ErrorDetails>> read() =>
      capture(_read, fallback: Kind.corrupt);

  Future<Table> _read() async {
    // Safe by default: stream and stop at the cap so a giant dataset/file can't
    // OOM. Both caps null opts into the unbounded eager path below.
    if (maxRows != null || maxBytes != null) return _readBounded();

    // Prune whole files by partition predicate first, so pruned files never
    // spawn read work. Kept files retain their input order.
    final kept = await _kept();
    if (kept.isEmpty) {
      return Table.empty('dataset', Schema(fields: const []));
    }

    final List<Table> raw;
    if (concurrency <= 1) {
      // Project and read through ONE scan per file, so the footer parsed to
      // work out the projection is reused by the read instead of being parsed
      // again. Interleaved rather than projecting every file up front: only one
      // file's footer is alive at a time, which matters at a million files.
      raw = [for (final entry in kept) await _readOne(entry.$1, entry.$2)];
    } else {
      // Per-file column projection: push the requested columns into each Parquet
      // read (intersected with the file's own schema, minus partition columns)
      // instead of reading every column and projecting after unification. The
      // isolate re-opens the file itself (a Scan can't cross an isolate), so
      // this path still parses each footer twice.
      final projections = [
        for (final entry in kept)
          await _fileProjection(openScan(entry.$1), entry.$1, entry.$2),
      ];
      final localFilter = filter; // bind locally so the isolate closure
      // captures only sendable values, not `this`.
      final localMaxRows = maxRows;
      final localMaxBytes = maxBytes;
      raw = await parallelMap(
        [for (int i = 0; i < kept.length; i++) (kept[i].$1, projections[i])],
        (entry) => Isolate.run(
          () => readTableTask(
            ReadSpec(
              entry.$1,
              filter: localFilter,
              columns: entry.$2,
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
    await for (final batch in _batches()) {
      rows += batch.rowCount;
      bytes += batch.memoryUsage;
      enforceMaterializationCaps(rows, bytes, maxRows, maxBytes, 'dataset');
      batches.add(batch);
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
        for (final field in fields)
          Column.concat([
            for (final batch in batches) batch.column(field.name)!,
          ]),
      ],
    );
  }

  // --- file resolution -----------------------------------------------------

  /// The files to read, each with its partition values, minus those the
  /// [filter] prunes by partition.
  Future<List<(String, Map<String, Object?>)>> _kept() async {
    final (root, files) = await _resolve(source);
    final parts = _partitions(root, files);
    return [
      for (int i = 0; i < files.length; i++)
        if (filter == null || _partitionKeep(filter!, parts[i]))
          (files[i], parts[i]),
    ];
  }

  /// The directory partitions are read below, and the files. A Delta or
  /// Iceberg table directory is one "file" that [openScan] reads as the
  /// table; listing its data files would read removed and uncommitted ones.
  Future<(String, List<String>)> _resolve(String source) async {
    final type = await FileSystemEntity.type(source);
    if (type == FileSystemEntityType.file) return (source, [source]);
    if (type == FileSystemEntityType.directory) {
      if (isTable(source)) return (source, [source]);
      return (source, await _list(source, (path) => _supported(path)));
    }
    if (source.contains('*')) {
      final base = <String>[];
      for (final segment in source.split('/')) {
        if (segment.contains('*')) break;
        base.add(segment);
      }
      final directory = base.isEmpty ? '.' : base.join('/');
      final regex = _globRegex(source);
      return (directory, await _list(directory, regex.hasMatch));
    }
    throw Failure.of(Kind.missing, 'Dataset path not found: $source');
  }

  /// The files below [directory] that [match], sorted. A file or directory whose
  /// name starts with `_` or `.` is skipped, as Spark and Arrow do
  /// (`_SUCCESS`, `_temporary`, `_delta_log`, `.crc` files). A Delta or
  /// Iceberg table below [directory] is [Kind.unsupported]: reading its data files
  /// directly would include removed ones.
  Future<List<String>> _list(
    String directory,
    bool Function(String) match,
  ) async {
    final out = <String>[];
    await for (final entry in Directory(directory).list(recursive: true)) {
      final names = _names(entry.path.substring(directory.length));
      if (names.any((name) => name.startsWith('_') || name.startsWith('.'))) {
        continue;
      }
      if (entry is Directory && isTable(entry.path)) {
        throw Failure.of(
          Kind.unsupported,
          'Dataset $source holds the table ${entry.path}; read the table '
          'directly or list it as its own source',
        );
      }
      if (entry is File && match(entry.path)) out.add(entry.path);
    }
    out.sort();
    return out;
  }

  RegExp _globRegex(String pattern) {
    final buffer = StringBuffer('^');
    for (int i = 0; i < pattern.length; i++) {
      final character = pattern[i];
      if (character == '*') {
        if (i + 1 < pattern.length && pattern[i + 1] == '*') {
          buffer.write('.*');
          i++;
        } else {
          buffer.write('[^/]*');
        }
      } else if (character == '?') {
        buffer.write('[^/]');
      } else if ('.+(){}[]^\$|\\'.contains(character)) {
        buffer.write('\\$character');
      } else {
        buffer.write(character);
      }
    }
    buffer.write(r'$');
    return RegExp(buffer.toString());
  }

  bool _supported(String path) {
    final lower = path.toLowerCase();
    return _extensions.any(lower.endsWith);
  }

  // --- partitions ----------------------------------------------------------

  /// The non-empty names in the [relative] path, split on `/` and the
  /// platform's separator.
  static List<String> _names(String relative) => [
    for (final name in relative.split('/'))
      ...name.split(Platform.pathSeparator).where((part) => part.isNotEmpty),
  ];

  /// The directory names in the [relative] path of a file: all but the last.
  static List<String> _directories(String relative) {
    final names = _names(relative);
    return names.isEmpty ? names : names.sublist(0, names.length - 1);
  }

  /// The partition values of each of [files]: its `key=value` directories
  /// below [root] (an ancestor of the root, or the file name, is not one),
  /// typed alike across files. A key whose values all read as integers is an
  /// integer, as finite numbers a double, as `true`/`false` a boolean, and
  /// otherwise text: typing each file alone made `k=1` and `k=1.5` a schema
  /// conflict, and `NaN` a number.
  List<Map<String, Object?>> _partitions(String root, List<String> files) {
    final texts = [
      for (final file in files)
        <String, String?>{
          for (final segment in _directories(file.substring(root.length)))
            if (segment.indexOf('=') case final index
                when index > 0 &&
                    index < segment.length - 1 &&
                    !segment.substring(0, index).contains('.'))
              segment.substring(
                0,
                index,
              ): isNullPartition(segment.substring(index + 1))
                  ? null
                  // Hive/Spark (and this library's Delta writer) escape
                  // special characters in the segment value as %XX.
                  : unescapePathName(segment.substring(index + 1)),
        },
    ];
    final types = <String, DataType>{};
    for (final key in {for (final text in texts) ...text.keys}) {
      final values = [for (final text in texts) ?text[key]];
      types[key] = values.every((value) => int.tryParse(value) != null)
          ? DataType.int64
          : values.every((value) => double.tryParse(value)?.isFinite ?? false)
          ? DataType.float64
          : values.every((value) => value == 'true' || value == 'false')
          ? DataType.boolean
          : DataType.string;
    }
    return [
      for (final text in texts)
        {
          for (final MapEntry(:key, :value) in text.entries)
            key: value == null
                ? null
                : switch (types[key]!) {
                    DataType.int64 => int.parse(value),
                    DataType.float64 => double.parse(value),
                    DataType.boolean => value == 'true',
                    _ => value,
                  },
        },
    ];
  }

  bool _partitionKeep(RowFilter filter, Map<String, Object?> parts) {
    final bounds = <String, ColumnBounds>{
      for (final entry in parts.entries)
        entry.key: ColumnBounds.exact(entry.value),
    };
    return filter.keep(bounds);
  }

  Table _appendPartitions(Table table, Map<String, Object?> parts) {
    if (parts.isEmpty) return table;
    final columns = [...table.columns];
    final fields = [...table.schema.fields];
    final existing = table.schema.fields.map((field) => field.name).toSet();
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
  /// magnitude, and partition types are derived PER FILE: `part=5` (int8) and
  /// `part=300` (int16) would then be "incompatible types" in [_unionFields].
  /// The widths here are the widest of each family so every file agrees.
  ///
  /// A null partition (the `__HIVE_DEFAULT_PARTITION__` sentinel this library
  /// itself writes) MUST be [DataType.null_]; that is the escape hatch
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

  /// Read one file through a single [Scan], so the footer parsed for the
  /// projection is reused by the read (see [_ParquetScan]).
  Future<Table> _readOne(String file, Map<String, Object?> parts) async {
    final scan = openScan(file);
    return _readFile(scan, await _fileProjection(scan, file, parts));
  }

  /// Columns to read from [file]: the requested projection intersected with the
  /// file's own schema and minus partition columns (which come from the path),
  /// or null to read all. Only Parquet projection is pushed down today; an empty
  /// intersection keeps one column so the file's row count survives null-filling.
  Future<List<String>?> _fileProjection(
    Scan scan,
    String file,
    Map<String, Object?> parts,
  ) async {
    final wanted = columns;
    if (wanted == null || !file.toLowerCase().endsWith('.parquet')) return null;
    final schema = await scan.readSchema();
    final names = schema == null
        ? const <String>[]
        : schema.fields.map((field) => field.name).toList();
    final keys = parts.keys.toSet();
    final projection = [
      for (final column in wanted)
        if (names.contains(column) && !keys.contains(column)) column,
    ];
    if (projection.isNotEmpty) return projection;
    return names.isEmpty ? null : [names.first];
  }

  Future<Table> _readFile(Scan scan, [List<String>? projection]) async {
    // Format selection came from the single registry (openScan); forward the
    // dataset-level caps so `null` (opt-out) is truly unbounded per file, not
    // silently re-defaulted to 10M / 1 GiB, and a smaller dataset cap is
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
  /// later row isn't null, so non-nullness can't be guaranteed across a streaming
  /// heterogeneous dataset.
  List<Field> _unionFields(Iterable<List<Field>> perFile) {
    final byName = <String, Field>{};
    final order = <String>[];
    for (final fields in perFile) {
      for (final field in fields) {
        final seen = byName[field.name];
        if (seen == null) {
          byName[field.name] = field.nullable
              ? field
              : field.copyWith(nullable: true);
          order.add(field.name);
          continue;
        }
        // null_ carries no shape: promote to the first CONCRETE type. The
        // null-partition sentinel starts with '_', so its file can sort (and
        // be seen) FIRST; first-seen-wins then froze the field as null_ and
        // every concrete column failed validation.
        if (seen.dataType == DataType.null_) {
          if (field.dataType != DataType.null_) {
            byName[field.name] = field.copyWith(nullable: true);
          }
          continue;
        }
        if (field.dataType == DataType.null_) continue;
        if (seen.dataType != field.dataType) {
          // First-seen-wins would otherwise seat a later file's differently
          // typed column under this field's type and silently mislabel data.
          throw Failure.of(
            Kind.schema,
            'Incompatible types for column "${field.name}" across dataset '
            'files: ${seen.dataType} vs ${field.dataType}',
          );
        }
        // Same DataType is NOT the whole story: decimal scale and nested
        // children live on the FIELD, and merging across a mismatch corrupts
        // values (unscaled BigInts reinterpreted) or mis-pairs struct members.
        if (seen.dataType == DataType.decimal) {
          final unified = _unifyDecimal(seen, field);
          if (unified != null) byName[field.name] = unified;
          continue;
        }
        if (!seen.isCompatibleWith(field)) {
          throw Failure.of(
            Kind.schema,
            'Incompatible ${field.dataType.name} shapes for column '
            '"${field.name}" across dataset files (nested children differ)',
          );
        }
      }
    }
    return [for (final name in order) byName[name]!];
  }

  /// The unified decimal field for [seen] + [next]: the WIDER scale wins
  /// (values are rescaled to it in [_conform]/[_unify]) and precision keeps
  /// room for the widest integer part at that scale. Null when [seen] already
  /// covers [next].
  Field? _unifyDecimal(Field seen, Field next) {
    final seenScale = seen.scale ?? 0;
    final nextScale = next.scale ?? 0;
    final scale = seenScale > nextScale ? seenScale : nextScale;
    final seenPrecision = seen.precision;
    final nextPrecision = next.precision;
    final int? precision;
    if (seenPrecision == null || nextPrecision == null) {
      precision = seenPrecision ?? nextPrecision;
    } else {
      final integral = (seenPrecision - seenScale) > (nextPrecision - nextScale)
          ? (seenPrecision - seenScale)
          : (nextPrecision - nextScale);
      final wanted = integral + scale;
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
    final unified = _unionFields([
      for (final table in tables) table.schema.fields,
    ]);
    final names = unified.map((field) => field.name).toSet();
    final merged = <Column>[
      for (final field in unified)
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
      columns: merged,
      schema: Schema(fields: unified),
    );
    final select = columns;
    if (select != null) {
      _checkProjection(select, names);
      result = result.select(select);
    }
    return result;
  }

  void _checkProjection(List<String> select, Set<String> available) {
    final missing = select
        .where((column) => !available.contains(column))
        .toList();
    if (missing.isNotEmpty) {
      throw Failure.of(
        Kind.invalid,
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
  /// metadata pre-pass entirely: a million-file dataset then costs no up-front
  /// O(files) schema reads (each file's reader still conforms to it). Without it,
  /// the schema is derived by peeking every file's fields.
  ///
  /// Each batch arrives as an [Ok]; a failure as one [Err], reported as [read]
  /// reports it, which ends the stream.
  /// A [batchSize] below 1 is a programming error and throws at the call.
  Stream<Result<Table, ErrorDetails>> readBatches({
    int batchSize = 10000,
    Schema? schema,
  }) {
    if (batchSize <= 0) {
      throw ArgumentError.value(batchSize, 'batchSize', 'must be > 0');
    }
    return captureStream(
      () => _batches(size: batchSize, schema: schema),
      fallback: Kind.corrupt,
    );
  }

  Stream<Table> _batches({int size = 10000, Schema? schema}) async* {
    final kept = await _kept();
    if (kept.isEmpty) return;

    List<Field> unified;
    // Only when we peek: each file's PUSHED PROJECTION (the small list of column
    // names to read), NOT the full per-file Field list, so the stream doesn't
    // retain O(files) worth of schema metadata after unification is computed.
    List<List<String>?>? projections;
    if (schema != null) {
      unified = schema.fields;
    } else {
      // Cheap metadata pre-pass: derive the unified schema (file fields + their
      // partition columns) up front so every emitted batch can be conformed to
      // it: a stream can't retroactively widen already-emitted batches. The
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
        final fields = peeked[i];
        projections.add(_projectionFor(fields)); // small; fields dropped
        final extended = [...fields];
        final have = fields.map((field) => field.name).toSet();
        parts.forEach((key, value) {
          if (!have.contains(key)) {
            extended.add(
              Field(
                name: key,
                dataType: _typeOf(value),
                nullable: value == null,
              ),
            );
          }
        });
        perFile.add(extended);
      }
      unified = _unionFields(perFile);
    }
    final select = columns;
    if (select != null) {
      _checkProjection(select, unified.map((field) => field.name).toSet());
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

  /// The requested columns that exist in [fields] (partition columns come
  /// from the path, not the reader), or null when no projection. When the
  /// intersection is empty, keep one column so the file's row count survives.
  List<String>? _projectionFor(List<Field> fields) {
    final wanted = columns;
    if (wanted == null) return null;
    final names = fields.map((field) => field.name).toSet();
    final projection = [
      for (final column in wanted)
        if (names.contains(column)) column,
    ];
    if (projection.isNotEmpty) return projection;
    return fields.isEmpty ? null : [fields.first.name];
  }

  /// Cheap per-file field list for the unification pre-pass, via the single
  /// registry: Parquet from the footer; other formats by peeking the first
  /// streamed batch's schema. Empty file: contributes no fields.
  ///
  /// Deliberately does NOT hold its [Scan] to share the parsed footer with the
  /// later stream: the pre-pass visits every file before the first batch is
  /// emitted, so keeping the scans alive would hold every file's footer at once:
  /// worse, at a million files, than the parse it saves. Pass `schema:` to
  /// skip the pre-pass entirely.
  Future<List<Field>> _fileFields(String path) async =>
      (await openScan(path).readSchema())?.fields ?? const [];

  /// Stream one file as batches via its format's streaming reader (openScan).
  Stream<Table> _streamFile(String path, List<String>? projection, int size) =>
      openScan(
        path,
      ).batches(size: size, projection: projection, filter: filter);
}
