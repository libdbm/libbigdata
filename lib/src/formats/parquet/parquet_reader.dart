import 'dart:isolate';

import '../../core/table.dart';
import '../../core/column.dart';
import '../../core/data_type.dart';
import 'package:meta/meta.dart';

import '../../core/failure.dart';
import '../../core/limits.dart';
import '../../core/result.dart';
import '../../core/schema.dart' as core;
import 'reader/page_reader.dart' show defaultMaxParquetPageBytes;
import 'filter/row_filter.dart';
import 'reader/file_reader.dart';
import 'reader/metadata_reader.dart';
import 'reader/nested_assembler.dart';
import 'reader/dremel_assembler.dart';
import 'reader/row_group_reader.dart';
import 'schema/schema_converter.dart';
import 'schema/schema_levels.dart';
import 'thrift/parquet_thrift.dart' as thrift;
import '../../util/parallel.dart';

/// The columns [pick] chooses from the fields of the file [reader] opens, as
/// a projection: null (read every column) for a nested file, whose columns
/// are not projected, and the first column when [pick] chooses none, so the
/// read still yields the file's row count. The Delta and Iceberg scans differ
/// only in how they pick.
Future<List<String>?> projected(
  ParquetReader reader,
  List<String> Function(List<core.Field> fields) pick,
) async {
  final fields = (await reader.schema().unwrap()).fields;
  if (fields.any((field) => field.children.isNotEmpty)) return null;
  final chosen = pick(fields);
  return chosen.isEmpty && fields.isNotEmpty ? [fields.first.name] : chosen;
}

/// Parquet file reader.
final class ParquetReader {
  /// Diagnostic counter: total row groups skipped by filter pruning across all
  /// reads. Useful for tests/benchmarks to confirm pushdown took effect.
  @internal
  static int rowGroupsPruned = 0;

  /// Diagnostic counter: total (row-group × column) reads performed, i.e. it
  /// rises by the number of selected columns for each row group actually read.
  /// Lets tests confirm column projection reached the reader.
  @internal
  static int columnsRead = 0;

  /// Diagnostic counter: footers actually read + thrift-parsed. Lets tests
  /// confirm a caller that asks for the schema and then reads pays for the
  /// footer ONCE (see the [footer] parameter).
  @internal
  static int footersParsed = 0;

  final String filePath;
  final ParquetReaderOptions options;

  /// A footer already parsed from this same file, so a second read of it can be
  /// skipped. Supplied only by a caller that parsed it itself and knows the file
  /// has not changed since: Parquet files are immutable once published, which
  /// is what makes this safe.
  final thrift.FileMetaData? _footer;

  late thrift.FileMetaData _metadata;

  /// [_metadata]'s schema, converted to fields.
  late List<ParquetSchemaField> _fields;
  late FileReader _fileReader;

  /// Leaf paths and levels of [_metadata]'s schema, built once per file.
  late SchemaLayout _layout;

  ParquetReader(this.filePath, {ParquetReaderOptions? options, footer})
    : options = options ?? const ParquetReaderOptions(),
      _footer = footer;

  /// The parsed footer, available after any read. Pass it to a later
  /// [ParquetReader] over the same file (see [footer]) to skip re-parsing it.
  thrift.FileMetaData get metadata => _metadata;

  /// Initialize the reader and read metadata
  Future<void> _initialize() async {
    _fileReader = FileReader(filePath);
    await _fileReader.open();
    // The fd is held now; a corrupt/zero-byte file makes validation or metadata
    // parsing throw, and this runs before the callers' try/finally, so close
    // the fd here on any failure rather than leaking it.
    try {
      final cached = _footer;
      // A caller that already parsed this footer skips the magic check, the
      // footer preads and the thrift parse: the fd is still needed for column
      // data, so this saves the metadata round-trips, not the open.
      final metadata = cached ?? await _readFooter();

      _metadata = metadata;
      _fields = SchemaConverter().fromParquetSchema(metadata.schema).fields;
      _layout = SchemaLayout(metadata.schema);
    } catch (_) {
      await _fileReader.close();
      rethrow;
    }
  }

  Future<thrift.FileMetaData> _readFooter() async {
    footersParsed++;
    // Read and validate magic bytes
    if (!await _fileReader.validateMagic()) {
      throw FormatException(
        'Invalid Parquet file: missing or incorrect magic bytes',
      );
    }
    return MetadataReader(_fileReader).readMetadata();
  }

  /// Read only the schema from the file footer: no column data is decoded.
  /// Honors [ParquetReaderOptions.columns] (same fields/order as
  /// [read]/[readBatches]).
  ///
  /// For a nested file this is the REASSEMBLED schema (top-level list/struct/
  /// map fields with children): exactly what [read]/[readBatches] emit, so a
  /// metadata-first consumer (the Dataset pre-pass) unifies the same names the
  /// batches carry.
  ///
  /// Like [read] and [readBatches], it reports a failure as an [Err]:
  /// [Kind.corrupt] for a malformed file, [Kind.missing] or [Kind.io] when it
  /// cannot be opened.
  @useResult
  Future<Result<core.Schema, ErrorDetails>> schema() =>
      capture(_schema, fallback: Kind.corrupt);

  Future<core.Schema> _schema() async {
    await _initialize();
    try {
      if (options.columns == null &&
          DremelAssembler.hasNested(_metadata.schema)) {
        return core.Schema(fields: DremelAssembler.fieldsOf(_metadata.schema));
      }
      return core.Schema(
        fields: [
          for (final field in _outputFields())
            core.Field(
              name: field.name,
              dataType: field.dataType,
              nullable: field.nullable,
              precision: field.precision,
              scale: field.scale,
              id: field.id,
            ),
        ],
      );
    } finally {
      await _fileReader.close();
    }
  }

  /// Read the whole file into one [Table]. To stop an accidental full load from
  /// OOMing, materialization STOPS with [Kind.limit] once it would exceed
  /// [maxRows] (default [defaultMaxMaterializedRows]) or [maxBytes] (default
  /// [defaultMaxMaterializedBytes]); pass `null` for either to opt out, or use
  /// the page-bounded [readBatches] for streaming.
  ///
  /// [concurrency] > 1 decodes up to that many row groups at once, each in
  /// its own isolate, and assembles them in file order; the result is the
  /// same as the serial read. A file with nested columns reads serially.
  ///
  /// Past a cap the result is [Kind.limit]; an unknown projected column is
  /// [Kind.invalid].
  @useResult
  Future<Result<Table, ErrorDetails>> read({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    int concurrency = 1,
  }) => capture(
    () => _read(maxRows: maxRows, maxBytes: maxBytes, concurrency: concurrency),
    fallback: Kind.corrupt,
  );

  Future<Table> _read({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    int concurrency = 1,
  }) async {
    await _initialize();

    try {
      // Files with nested (group) columns and no projection are reassembled to
      // ANY depth via the Dremel assembler over raw per-leaf rep/def levels.
      if (options.columns == null &&
          DremelAssembler.hasNested(_metadata.schema)) {
        return await _readNested(maxRows, maxBytes);
      }

      final fields = _outputFields();
      final types = {for (final field in fields) field.name: field.dataType};

      // Collect each (selected) column's per-row-group pieces, stopping the
      // moment the accumulated rows/bytes would exceed the cap so a huge file
      // (e.g. an Iceberg delete file) can't OOM the process.
      final parts = <String, List<Column>>{
        for (final field in fields) field.name: <Column>[],
      };
      var rows = 0;
      var bytes = 0;
      final groups = <int>[]; // row groups left to decode in parallel
      for (int i = 0; i < _metadata.rowGroups.length; i++) {
        final rowGroup = _metadata.rowGroups[i];
        final reader = RowGroupReader(
          _fileReader,
          rowGroup,
          _metadata.schema,
          options,
          _layout,
        );
        if (_skip(reader, rowGroup, types)) {
          rowGroupsPruned++;
          continue;
        }
        columnsRead += fields.length;
        if (concurrency > 1) {
          // Check the declared size now, in file order, as the serial read
          // does before each decode; the exact sizes are charged as the
          // decoded groups arrive.
          enforceMaterializationCaps(
            rows + rowGroup.numRows.toInt(),
            bytes + rowGroup.totalByteSize,
            maxRows,
            maxBytes,
            'Parquet result',
          );
          rows += rowGroup.numRows.toInt();
          bytes += rowGroup.totalByteSize;
          groups.add(i);
          continue;
        }
        // Pre-check the row group's DECLARED size against the budget BEFORE
        // materializing it: read() below allocates the whole group into memory,
        // so a single oversized row group would OOM before the post-read checks
        // (below) ever ran. totalByteSize is the on-disk size (a lower bound on
        // in-memory), so an over-cap group is rejected here; the exact
        // in-memory check still runs after decode for anything that slips under.
        enforceMaterializationCaps(
          rows + rowGroup.numRows.toInt(),
          bytes + rowGroup.totalByteSize,
          maxRows,
          maxBytes,
          'Parquet result',
        );
        final data = await reader.read();
        rows += rowGroup.numRows.toInt();
        for (final column in data.values) {
          bytes += column.memoryUsage;
        }
        enforceMaterializationCaps(
          rows,
          bytes,
          maxRows,
          maxBytes,
          'Parquet result',
        );
        data.forEach((name, column) {
          parts[name]?.add(column);
        });
      }
      if (groups.isNotEmpty) {
        final path = filePath;
        final footer = _metadata;
        var decodedBytes = 0;
        final decoded = await parallelMap(
          groups,
          (index) => _spawn(path, options, footer, index),
          concurrency: concurrency,
          onEach: (group) {
            for (final column in group.values) {
              decodedBytes += column.memoryUsage;
            }
            enforceMaterializationCaps(
              rows,
              decodedBytes,
              maxRows,
              maxBytes,
              'Parquet result',
            );
          },
        );
        for (final group in decoded) {
          group.forEach((name, column) {
            parts[name]?.add(column);
          });
        }
      }

      // One column per selected field: use the single piece directly
      // (zero-copy) or concatenate across row groups.
      final tableColumns = <Column>[];
      for (final field in fields) {
        final pieces = parts[field.name] ?? const <Column>[];
        if (pieces.isEmpty) {
          tableColumns.add(
            ListColumn(
              name: field.name,
              dataType: field.dataType,
              data: const [],
              nullable: field.nullable,
            ),
          );
        } else if (pieces.length == 1) {
          tableColumns.add(pieces.first);
        } else {
          tableColumns.add(Column.concat(pieces));
        }
      }

      var outFields = [
        for (int i = 0; i < fields.length; i++)
          core.Field(
            name: fields[i].name,
            dataType: fields[i].dataType,
            // The data is authoritative: a column that actually contains nulls
            // (e.g. from tolerant truncation) is nullable regardless of the
            // file's declared repetition.
            nullable: fields[i].nullable || tableColumns[i].hasNulls,
            precision: fields[i].precision,
            scale: fields[i].scale,
            id: fields[i].id,
          ),
      ];
      var outColumns = tableColumns;
      // Reassemble dotted leaf columns into nested struct/list/map columns
      // (skipped when projecting specific columns, where dotted paths differ).
      if (options.columns == null) {
        (outFields, outColumns) = NestedAssembler(
          _metadata.schema,
          outFields,
          outColumns,
        ).run();
      }

      return Table(
        name: 'parquet_data',
        columns: outColumns,
        schema: core.Schema(fields: outFields),
      );
    } finally {
      await _fileReader.close();
    }
  }

  /// Decode row group [index] of the file at [path] in a new isolate. Static,
  /// so the closure carries only its arguments (the options, which hold no
  /// closures, and the parsed [footer]) and never a reader's open file.
  static Future<Map<String, Column>> _spawn(
    String path,
    ParquetReaderOptions options,
    thrift.FileMetaData footer,
    int index,
  ) => Isolate.run(
    () => ParquetReader(path, options: options, footer: footer)._group(index),
  );

  /// Decode row group [index] alone.
  Future<Map<String, Column>> _group(int index) async {
    await _initialize();
    try {
      return await RowGroupReader(
        _fileReader,
        _metadata.rowGroups[index],
        _metadata.schema,
        options,
        _layout,
      ).read();
    } finally {
      await _fileReader.close();
    }
  }

  /// Read a nested-containing file: collect raw per-leaf triples across row
  /// groups and reassemble to arbitrary depth via [DremelAssembler].
  Future<Table> _readNested(int? maxRows, int? maxBytes) async {
    final pieces = <String, List<RawLeaf>>{};
    var rows = 0;
    var bytes = 0;
    for (final rowGroup in _metadata.rowGroups) {
      final reader = RowGroupReader(
        _fileReader,
        rowGroup,
        _metadata.schema,
        options,
        _layout,
      );
      // Prune by the pushed filter's column stats FIRST, as the flat path does.
      if (_skip(reader, rowGroup, _leafTypes(reader))) {
        rowGroupsPruned++;
        continue;
      }
      columnsRead += _fields.length;
      // Pre-check the row group's DECLARED size BEFORE readRaw() materializes it,
      // mirroring the non-nested path; otherwise a single oversized nested row
      // group is fully assembled in memory before the post-read caps below fire.
      enforceMaterializationCaps(
        rows + rowGroup.numRows.toInt(),
        bytes + rowGroup.totalByteSize,
        maxRows,
        maxBytes,
        'Parquet result',
      );
      final raw = await reader.readRaw();
      raw.forEach((path, leaf) {
        (pieces[path] ??= []).add(leaf);
        bytes += leaf.bytes;
      });
      rows += rowGroup.numRows.toInt();
      // Stop-at-cap on BOTH dimensions per row group, BEFORE buffering the next:
      // a wide/deep nested file's raw rep/def overhead can blow memory long
      // before assembly (mirrors the non-nested path).
      enforceMaterializationCaps(
        rows,
        bytes,
        maxRows,
        maxBytes,
        'Parquet result',
      );
    }
    final leaves = {
      for (final entry in pieces.entries)
        entry.key: RawLeaf.concat(entry.value),
    };
    final (fields, columns) = DremelAssembler(_metadata.schema, leaves).run();
    if (maxBytes != null) {
      final bytes = columns.fold(0, (sum, column) => sum + column.memoryUsage);
      if (bytes > maxBytes) {
        throw Failure.of(
          Kind.limit,
          'Parquet result exceeds maxBytes=$maxBytes; pass maxBytes: null to '
          'allow it or use readBatches for bounded-memory streaming',
        );
      }
    }
    return Table(
      name: 'parquet_data',
      columns: columns,
      schema: core.Schema(fields: fields),
    );
  }

  /// The fields to emit: all of them, or just
  /// [ParquetReaderOptions.columns] (in file order).
  /// A requested name not present in the file is a hard error (no silent drop).
  List<ParquetSchemaField> _outputFields() {
    final all = _fields;
    final requested = options.columns;
    if (requested == null) return all;
    final names = {for (final field in all) field.name};
    final missing = requested.where((name) => !names.contains(name)).toList();
    if (missing.isNotEmpty) {
      throw Failure.of(
        Kind.invalid,
        'Unknown column(s) in projection: ${missing.join(', ')}',
      );
    }
    return [
      for (final field in all)
        if (requested.contains(field.name)) field,
    ];
  }

  /// Whether [rowGroup] can be skipped entirely under the configured filter.
  /// Evaluates [options.filter] against the group's per-column statistics; a
  /// group is skipped only when the stats prove no row can match.
  bool _skip(
    RowGroupReader reader,
    thrift.RowGroup rowGroup,
    Map<String, DataType> types,
  ) {
    final filter = options.filter;
    if (filter == null) return false;

    final columns = reader.columns();
    final byName = {for (final column in columns) column.name: column};
    // A column whose declared sort order this reader does not know has
    // statistics it cannot interpret.
    final orders = metadata.columnOrders;
    final bounds = <String, ColumnBounds>{};
    for (final name in filter.columns) {
      final info = byName[name];
      if (info == null) continue; // column absent -> no bounds -> keep
      final index = columns.indexOf(info);
      if (orders != null &&
          index < orders.length &&
          orders[index].typeOrder == null) {
        continue;
      }
      // Reinterpret an UNSIGNED column's stats using ITS OWN leaf element: a
      // short-name lookup could pick a different (nested) leaf that shares the
      // name and apply the wrong sign, corrupting the bounds and mis-pruning.
      bounds[name] = statsToBounds(
        info.chunk.metaData?.statistics,
        types[name] ?? DataType.string,
        rowGroup.numRows.toInt(),
        unsignedBits: SchemaConverter.unsignedInfo(info.element)?.bits,
      );
    }
    return !filter.keep(bounds);
  }

  /// Leaf-column dtypes for [reader], so [_skip] can interpret a NESTED file's
  /// per-column statistics correctly (the flat path passes output-field types;
  /// nested output fields aren't known until after assembly). Only the filter's
  /// columns are consulted, and an unmatched filter column keeps the row group,
  /// so a leaf name that doesn't line up never wrongly prunes.
  Map<String, DataType> _leafTypes(RowGroupReader reader) {
    final converter = SchemaConverter();
    return {
      for (final column in reader.columns())
        column.name: converter.convertType(column.element),
    };
  }

  /// Each batch arrives as an [Ok]; a failure as one [Err], reported as [read]
  /// reports it, which ends the stream.
  /// A [batchSize] below 1 is a programming error and throws at the call.
  Stream<Result<Table, ErrorDetails>> readBatches({int batchSize = 10000}) {
    if (batchSize <= 0) {
      throw ArgumentError.value(batchSize, 'batchSize', 'must be > 0');
    }
    return captureStream(() => _batches(batchSize), fallback: Kind.corrupt);
  }

  Stream<Table> _batches(int size) async* {
    await _initialize();

    try {
      // Nested (struct/list/map) schemas can't stream leaf-by-leaf, but they CAN
      // be reassembled one ROW GROUP at a time: readRaw() already materializes a
      // single row group's leaves, so running the Dremel assembler per row group
      // yields bounded batches with the SAME reassembled schema as read(): no
      // whole-file materialization. (A column projection takes the flat leaf path
      // in read() too, so it stays on the leaf-streaming path below.)
      if (options.columns == null &&
          DremelAssembler.hasNested(_metadata.schema)) {
        for (int i = 0; i < _metadata.rowGroups.length; i++) {
          final rowGroup = _metadata.rowGroups[i];
          final reader = RowGroupReader(
            _fileReader,
            rowGroup,
            _metadata.schema,
            options,
            _layout,
          );
          // Prune by the pushed filter's column stats FIRST (as the flat path
          // does). Ahead of the byte-cap check so a pruned-out group never fails on its size.
          if (_skip(reader, rowGroup, _leafTypes(reader))) {
            rowGroupsPruned++;
            continue;
          }
          // Backstop the RAW-leaf load (readRaw materializes one row group's
          // rep/def/value arrays): reject a pathological row group before OOMing
          // on the levels. The assembled output below is separately bounded by
          // `size` (windowed assembly).
          if (overByteCap(
            rowGroup.totalByteSize,
            defaultMaxMaterializedBytes,
          )) {
            throw Failure.of(
              Kind.limit,
              'Parquet nested row group is ${rowGroup.totalByteSize} bytes, over '
              'the $defaultMaxMaterializedBytes-byte streaming ceiling; the file '
              'needs smaller row groups for bounded-memory nested reads',
            );
          }
          columnsRead += _fields.length;
          final raw = await reader.readRaw();
          // Assemble in `size`-record windows so a huge nested row group yields
          // multiple bounded batches (honoring readBatches's `size`) instead of
          // one giant table: the assembled peak is one window, not the group.
          final assembler = DremelAssembler(_metadata.schema, raw);
          var batch = 0;
          while (assembler.hasMore) {
            final (fields, columns) = assembler.assembleWindow(size);
            yield Table(
              name: 'parquet_nested_batch_${i}_${batch++}',
              columns: columns,
              schema: core.Schema(fields: fields),
            );
          }
        }
        return;
      }

      final fields = _outputFields();
      final types = {for (final field in fields) field.name: field.dataType};
      final schema = core.Schema(
        fields: [
          for (final field in fields)
            core.Field(
              name: field.name,
              dataType: field.dataType,
              nullable: field.nullable,
              precision: field.precision,
              scale: field.scale,
              id: field.id,
            ),
        ],
      );

      for (int i = 0; i < _metadata.rowGroups.length; i++) {
        final rowGroup = _metadata.rowGroups[i];
        final reader = RowGroupReader(
          _fileReader,
          rowGroup,
          _metadata.schema,
          options,
          _layout,
        );
        if (_skip(reader, rowGroup, types)) {
          rowGroupsPruned++;
          continue;
        }
        columnsRead += fields.length;

        // Stream the row group page-by-page so a single (possibly huge) row
        // group is never fully materialized: peak memory ≈ one batch.
        final cursor = reader.cursor();
        var batch = 0;
        Map<String, Column>? data;
        while ((data = await cursor.next(size)) != null) {
          final columns = [
            for (final field in fields)
              data![field.name] ??
                  ListColumn(
                    name: field.name,
                    dataType: field.dataType,
                    data: const [],
                    nullable: field.nullable,
                  ),
          ];
          yield Table(
            name: 'parquet_batch_${i}_${batch++}',
            columns: columns,
            schema: schema,
          );
        }
      }
    } finally {
      await _fileReader.close();
    }
  }
}

/// Options for reading Parquet files
final class ParquetReaderOptions {
  /// Columns to read (null = all columns)
  final List<String>? columns;

  /// Whether to verify page checksums (CRC32) when present; mismatches throw.
  /// On by default (production-safe); set `false` for a fast, unsafe read that
  /// skips integrity checks. CRC-less files read fine either way.
  final bool verifyChecksums;

  /// Read pages best-effort when they declare more values than they store
  /// (a spec-violating legacy quirk). Off by default so truncated/corrupt
  /// pages fail loudly instead of yielding silently wrong data.
  final bool tolerateTruncatedPages;

  /// Optional predicate used to prune row groups via column statistics. It is a
  /// pruning hint only: groups proven not to match are skipped, but surviving
  /// groups are returned whole (exact row filtering happens in the query layer).
  final RowFilter? filter;

  /// Per-page ceiling (compressed AND decompressed), so a memory-bounded service
  /// can lower it below the [defaultMaxParquetPageBytes] default. A page over
  /// this is rejected before allocation.
  final int maxPageBytes;

  const ParquetReaderOptions({
    this.columns,
    this.verifyChecksums = true,
    this.tolerateTruncatedPages = false,
    this.filter,
    this.maxPageBytes = defaultMaxParquetPageBytes,
  });
}
