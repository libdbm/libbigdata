import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/formats/avro/avro_ocf_reader.dart';
import 'package:libbigdata/src/formats/avro/avro_ocf_writer.dart';
import 'package:libbigdata/src/formats/iceberg/iceberg_commit_coordinator.dart'
    show
        FilesystemCommitCoordinator,
        IcebergCommitConflict,
        IcebergCommitIndeterminate;
import 'package:libbigdata/src/formats/iceberg/iceberg_schema.dart';
import 'package:libbigdata/src/formats/scan.dart';
import 'package:libbigdata/src/formats/iceberg/manifest_schemas.dart';
import 'package:test/test.dart';

import 'support/results.dart';

String _strip(String uri) => uri.startsWith('file://') ? uri.substring(7) : uri;

/// Delegates to the real filesystem backend but records that it was invoked and
/// the versions it returned. Inherits the filesystem `current` (the SAME read
/// its CAS uses).
final class _Recording extends FilesystemIcebergCatalog
    implements IcebergBackend {
  final versions = <int>[];
  final _inner = const FilesystemIcebergBackend();

  @override
  Future<Result<int, ErrorDetails>> commit({
    required String root,
    required Future<IcebergMetadataPublish> Function(
      Map<String, dynamic>? previous,
      int version,
    )
    build,
    bool allowOversized = false,
  }) async {
    final outcome = await _inner.commit(
      root: root,
      build: build,
      allowOversized: allowOversized,
    );
    if (outcome case Ok(:final value)) versions.add(value);
    return outcome;
  }
}

/// Always loses the CAS: fails before publishing anything.
final class _Conflicting extends FilesystemIcebergCatalog
    implements IcebergBackend {
  @override
  Future<Result<int, ErrorDetails>> commit({
    required String root,
    required Future<IcebergMetadataPublish> Function(
      Map<String, dynamic>? previous,
      int version,
    )
    build,
    bool allowOversized = false,
  }) async => Err(IcebergCommitConflict(root, 0).details);
}

/// Loses the CAS after making the data directory read-only, so the writer's
/// cleanup of its staged data files fails.
final class _Undeletable extends FilesystemIcebergCatalog
    implements IcebergBackend {
  @override
  Future<Result<int, ErrorDetails>> commit({
    required String root,
    required Future<IcebergMetadataPublish> Function(
      Map<String, dynamic>? previous,
      int version,
    )
    build,
    bool allowOversized = false,
  }) async {
    Process.runSync('chmod', ['555', '$root/data']);
    addTearDown(() => Process.runSync('chmod', ['755', '$root/data']));
    return Err(IcebergCommitConflict(root, 0).details);
  }
}

/// Publishes normally, then makes [directory] (the registry's) read-only so the
/// registry write that follows the publish fails.
final class _SealAfterPublish extends FilesystemCommitCoordinator {
  const _SealAfterPublish(this.directory);

  final String directory;

  @override
  Future<Result<int, ErrorDetails>> commit({
    required String root,
    required Future<Map<String, dynamic>?> Function() readCurrent,
    required Future<IcebergMetadataPublish> Function(
      Map<String, dynamic>? previous,
      int version,
    )
    build,
  }) async {
    final outcome = await super.commit(
      root: root,
      readCurrent: readCurrent,
      build: build,
    );
    Process.runSync('chmod', ['555', directory]);
    return outcome;
  }
}

/// A minimal alternate backend that drives commits WITHOUT the O_EXCL marker:
/// proving a custom CAS can install the pointer through the boundary. Reads through
/// its own inherited `current`, so read and CAS cannot diverge.
final class _NoMarker extends FilesystemIcebergCatalog
    implements IcebergBackend {
  @override
  Future<Result<int, ErrorDetails>> commit({
    required String root,
    required Future<IcebergMetadataPublish> Function(
      Map<String, dynamic>? previous,
      int version,
    )
    build,
    bool allowOversized = false,
  }) async {
    final previous = await current(root, allowOversized: allowOversized).ok;
    final version = ((previous?['_version'] as int?) ?? -1) + 1;
    final pub = await build(previous, version);
    final metaPath = '$root/metadata/${pub.metadataFileName}';
    final tmp = File('$metaPath.tmp');
    await tmp.writeAsString(pub.metadataJson, flush: true);
    await tmp.rename(metaPath);
    return Ok(version);
  }
}

void main() {
  group('metadata history', () {
    late Directory dir;
    late String path;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('iceberg_history_');
      path = '${dir.path}/t';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Table ids(List<int> values) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: values, nullable: false),
      ],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    test(
      'metadata-log lists earlier files and snapshots name parents',
      () async {
        await IcebergWriter(path).write(ids([1])).ok;
        await IcebergWriter(path).write(ids([2])).ok;
        await IcebergWriter(
          path,
        ).write(ids([3]), mode: IcebergWriteMode.overwrite).ok;
        final files =
            Directory('$path/metadata')
                .listSync()
                .whereType<File>()
                .where((f) => f.path.endsWith('.metadata.json'))
                .toList()
              ..sort((a, b) => a.path.compareTo(b.path));
        expect(files, hasLength(3));
        final meta =
            jsonDecode(files.last.readAsStringSync()) as Map<String, dynamic>;
        final log = (meta['metadata-log'] as List).cast<Map>();
        expect(
          [for (final e in log) _strip(e['metadata-file'] as String)],
          [for (final f in files.take(2)) f.absolute.path],
        );
        expect(log.every((e) => e['timestamp-ms'] is int), isTrue);
        final snapshots = (meta['snapshots'] as List).cast<Map>();
        expect(snapshots.first.containsKey('parent-snapshot-id'), isFalse);
        for (var i = 1; i < snapshots.length; i++) {
          expect(
            snapshots[i]['parent-snapshot-id'],
            snapshots[i - 1]['snapshot-id'],
          );
        }
        // Internal bookkeeping never leaks into published metadata.
        expect(meta.keys.where((k) => k.startsWith('_')), isEmpty);
      },
    );
  });

  group('Iceberg spec conformance', () {
    late Directory dir;
    late String path;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('iceberg_spec_');
      path = '${dir.path}/t';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Table rows(List<int> id, List<String> p) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: id, nullable: false),
        ListColumn<String>(
          name: 'p',
          dataType: DataType.string,
          data: p,
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'p', dataType: DataType.string, nullable: false),
        ],
      ),
    );

    File latest() => Directory('$path/metadata')
        .listSync()
        .whereType<File>()
        .where((f) => f.path.endsWith('.metadata.json'))
        .reduce((a, b) => a.path.compareTo(b.path) > 0 ? a : b);

    /// Publish [meta] as the next metadata version.
    void publish(Map<String, dynamic> meta, String tag) {
      final n = int.parse(latest().uri.pathSegments.last.substring(0, 5)) + 1;
      File(
        '$path/metadata/${n.toString().padLeft(5, '0')}-$tag.metadata.json',
      ).writeAsStringSync(jsonEncode(meta));
    }

    Map<String, Object?> entry(String file, int content, String format) => {
      'status': 1,
      'snapshot_id': 1,
      'sequence_number': 999,
      'file_sequence_number': 999,
      'data_file': {
        'content': content,
        'file_path': 'file://$file',
        'file_format': format,
        'partition': <String, Object?>{},
        'record_count': 1,
        'file_size_in_bytes': File(file).lengthSync(),
        'column_sizes': null,
        'value_counts': null,
        'null_value_counts': null,
        'nan_value_counts': null,
        'lower_bounds': null,
        'upper_bounds': null,
        'key_metadata': null,
        'split_offsets': null,
        'equality_ids': content == 2 ? [1] : null,
        'sort_order_id': null,
      },
    };

    /// Add a snapshot listing the current manifests plus one extra manifest
    /// holding [extra] (unpartitioned, spec 0, [content] 0 data / 1 deletes).
    Future<void> addManifest(
      List<Map<String, Object?>> extra, {
      required int content,
    }) async {
      final meta =
          jsonDecode(latest().readAsStringSync()) as Map<String, dynamic>;
      final snap = (meta['snapshots'] as List)
          .cast<Map<String, dynamic>>()
          .last;
      final list = await AvroOcfReader.readPathAsync(
        _strip(snap['manifest-list'] as String),
      );
      final manifest = '$path/metadata/extra-m0.avro';
      await AvroOcfWriter.writePathAsync(manifest, manifestEntrySchema, extra);
      final seq = (meta['last-sequence-number'] as int? ?? 0) + 1000;
      final listPath = '$path/metadata/snap-extra.avro';
      await AvroOcfWriter.writePathAsync(listPath, manifestListSchema, [
        ...list,
        {
          ...list.first,
          'manifest_path': 'file://$manifest',
          'manifest_length': File(manifest).lengthSync(),
          'partition_spec_id': 0,
          'content': content,
          'sequence_number': seq,
          'min_sequence_number': seq,
          'partitions': null,
        },
      ]);
      final newId = (snap['snapshot-id'] as int) + 1;
      meta['snapshots'] = [
        ...(meta['snapshots'] as List),
        {
          ...snap,
          'snapshot-id': newId,
          'sequence-number': seq,
          'manifest-list': 'file://$listPath',
        },
      ];
      meta['current-snapshot-id'] = newId;
      meta['last-sequence-number'] = seq;
      meta['refs'] = {
        'main': {'snapshot-id': newId, 'type': 'branch'},
      };
      publish(meta, 'extra');
    }

    test('a format-version 3 table is unsupported', () async {
      await IcebergWriter(path).write(rows([1], ['a'])).ok;
      final meta =
          jsonDecode(latest().readAsStringSync()) as Map<String, dynamic>;
      publish({...meta, 'format-version': 3}, 'v3');
      await expectLater(
        IcebergTable(path).read(),
        failsWith(Kind.unsupported, contains('format-version 3')),
      );
    });

    test('an ORC data file is unsupported', () async {
      await IcebergWriter(path).write(rows([1], ['a'])).ok;
      final orc = '$path/data/x.orc';
      File(orc).writeAsBytesSync(List.filled(8, 0));
      await addManifest([entry(orc, 0, 'ORC')], content: 0);
      await expectLater(
        IcebergTable(path).read(),
        failsWith(Kind.unsupported, contains('ORC')),
      );
    });

    test('an unpartitioned-spec equality delete applies after partition '
        'evolution', () async {
      await IcebergWriter(
        path,
      ).write(rows([1], ['a'])).ok; // spec 0, unpartitioned
      await IcebergWriter(path)
          .write(
            rows([7, 8, 7], ['a', 'a', 'b']),
            mode: IcebergWriteMode.overwrite,
            partitionBy: ['p'],
          )
          .ok; // spec 1
      final deletes = '$path/data/del-eq.parquet';
      await ParquetWriter(deletes)
          .write(
            Table(
              name: 'd',
              columns: [
                NumericColumn.int64(name: 'id', values: [7], nullable: false),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'id', dataType: DataType.int64, nullable: false),
                ],
              ),
            ),
          )
          .ok;
      await addManifest([entry(deletes, 2, 'PARQUET')], content: 1);
      final back = await IcebergTable(path).read().ok;
      expect(back.column('id')!.toList(), [8]);
    });
  });

  group('concurrent claims', () {
    late Directory dir;
    late String path;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('iceberg_claims_');
      path = '${dir.path}/t';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Table ids(List<int> values) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: values, nullable: false),
      ],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    test('concurrent appends publish distinct versions', () async {
      await IcebergWriter(path).write(ids([0])).ok;
      await Future.wait([
        for (var i = 1; i <= 8; i++) IcebergWriter(path).write(ids([i])),
      ]);
      final versions = [
        for (final f in Directory('$path/metadata').listSync())
          if (f.path.endsWith('.metadata.json'))
            f.uri.pathSegments.last.substring(0, 5),
      ];
      expect(versions.toSet().length, versions.length, reason: '$versions');
      expect(
        (await IcebergTable(path).read().ok).column('id')!.toList()..sort(),
        List.generate(9, (i) => i),
      );
    });

    test('a claim on a version that moved retries rather than publish it '
        'twice', () async {
      await IcebergWriter(path).write(ids([1])).ok; // version 0
      final root = Directory(path).absolute.path;
      const backend = FilesystemIcebergBackend();
      // Writer B read version 0 ...
      final stale = await backend.current(root).ok;
      // ... then writer A published version 1 and released its claim.
      await IcebergWriter(path).write(ids([2])).ok;
      var reads = 0;
      final built = <int>[];
      final published = await const FilesystemCommitCoordinator()
          .commit(
            root: root,
            readCurrent: () async =>
                reads++ == 0 ? stale : await backend.current(root).ok,
            build: (previous, version) async {
              built.add(version);
              // Republish the current state unchanged under the new version.
              final copy = {...previous!}..remove('_version');
              return IcebergMetadataPublish(
                '${version.toString().padLeft(5, '0')}-copy.metadata.json',
                jsonEncode(copy),
              );
            },
          )
          .ok;
      expect(published, 2);
      expect(built, [2], reason: 'never built on the stale version 0');
      final versions = [
        for (final f in Directory('$root/metadata').listSync())
          if (f.path.endsWith('.metadata.json'))
            f.uri.pathSegments.last.substring(0, 5),
      ];
      expect(versions.toSet().length, versions.length, reason: '$versions');
      expect(
        (await IcebergTable(path).read().ok).column('id')!.toList()..sort(),
        [1, 2],
      );
    });
  });

  group('overwrite field ids', () {
    late Directory dir;
    late String path;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('iceberg_ids_');
      path = '${dir.path}/t';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Table table(Map<String, DataType> types, List<List<Object>> rows) => Table(
      name: 't',
      schema: Schema(
        fields: [
          for (final e in types.entries)
            Field(name: e.key, dataType: e.value, nullable: false),
        ],
      ),
      columns: [
        for (final (i, e) in types.entries.indexed)
          e.value == DataType.string
              ? ListColumn<String>(
                  name: e.key,
                  dataType: DataType.string,
                  data: [for (final r in rows) r[i] as String],
                  nullable: false,
                )
              : NumericColumn.int64(
                  name: e.key,
                  values: [for (final r in rows) r[i] as int],
                  nullable: false,
                ),
      ],
    );

    Future<Map<String, dynamic>> metadata() async {
      final file = Directory('$path/metadata')
          .listSync()
          .whereType<File>()
          .map((f) => f.path)
          .where((p) => p.endsWith('.metadata.json'))
          .reduce((a, b) => a.compareTo(b) > 0 ? a : b);
      return jsonDecode(File(file).readAsStringSync()) as Map<String, dynamic>;
    }

    Map<String, int> currentIds(Map<String, dynamic> meta) {
      final schema = (meta['schemas'] as List).cast<Map>().firstWhere(
        (s) => s['schema-id'] == meta['current-schema-id'],
      );
      return {
        for (final f in (schema['fields'] as List).cast<Map>())
          f['name'] as String: f['id'] as int,
      };
    }

    test('overwrite keeps the ids of matching columns', () async {
      await IcebergWriter(path)
          .write(
            table(
              {'a': DataType.int64, 'b': DataType.string},
              [
                [1, 'x'],
              ],
            ),
          )
          .ok;
      await IcebergWriter(path)
          .write(
            table(
              {'c': DataType.string, 'a': DataType.int64},
              [
                ['y', 2],
              ],
            ),
            mode: IcebergWriteMode.overwrite,
          )
          .ok;
      final meta = await metadata();
      expect(currentIds(meta), {'c': 3, 'a': 1});
      expect(meta['last-column-id'], 3);
      final back = await IcebergTable(path).read().ok;
      expect(back.toRows(), [
        {'c': 'y', 'a': 2},
      ]);
    });

    test('a column whose type changed gets a new id', () async {
      await IcebergWriter(path)
          .write(
            table(
              {'a': DataType.int64, 'b': DataType.int64},
              [
                [1, 2],
              ],
            ),
          )
          .ok;
      await IcebergWriter(path)
          .write(
            table(
              {'a': DataType.string},
              [
                ['x'],
              ],
            ),
            mode: IcebergWriteMode.overwrite,
          )
          .ok;
      final meta = await metadata();
      expect(currentIds(meta), {'a': 3});
      expect(meta['last-column-id'], 3);
      expect((await IcebergTable(path).read().ok).toRows(), [
        {'a': 'x'},
      ]);
    });
  });

  group('narrow int columns', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('iceberg_int_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('an inferred small int round-trips as Iceberg int', () async {
      final path = '${dir.path}/t';
      final frame = DataFrame.fromRows([
        {'id': 1},
        {'id': 2},
      ]);
      expect(frame.dtypes!['id'], isNot(DataType.int64));
      await frame.writeIceberg(path).ok;
      final back = await (await DataFrameIO.readIceberg(path).ok).execute().ok;
      expect(back.column('id')!.toList(), [1, 2]);
      expect(back.schema.field('id')!.dataType, DataType.int32);
    });

    test('int8 and int16 columns widen to int32', () async {
      final path = '${dir.path}/narrow';
      await IcebergWriter(path)
          .write(
            Table(
              name: 't',
              schema: Schema(
                fields: [
                  Field(name: 'a', dataType: DataType.int8),
                  Field(name: 'b', dataType: DataType.int16),
                ],
              ),
              columns: [
                NumericColumn.int8(
                  name: 'a',
                  values: [-128, 127],
                  nullable: false,
                ),
                NumericColumn.int16(
                  name: 'b',
                  values: [-32768, 32767],
                  nullable: false,
                ),
              ],
            ),
          )
          .ok;
      final back = await IcebergTable(path).read().ok;
      expect(back.column('a')!.toList(), [-128, 127]);
      expect(back.column('b')!.toList(), [-32768, 32767]);
      expect(back.schema.field('a')!.dataType, DataType.int32);
    });
  });

  group('core: write / read / time-travel & concurrency', () {
    Table table(List<int> ids, [List<String>? names]) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: ids, nullable: false),
        ListColumn<String>(
          name: 'name',
          dataType: DataType.string,
          data: names ?? [for (final i in ids) 'n$i'],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'name', dataType: DataType.string, nullable: false),
        ],
      ),
    );

    Future<Map<int, String?>> read(String path, {int? snapshotId}) async {
      final t = await (await DataFrameIO.readIceberg(
        path,
        snapshotId: snapshotId,
      ).ok).execute().ok;
      return {for (final r in t.toRows()) r['id'] as int: r['name'] as String?};
    }

    group('Iceberg write/read round-trip', () {
      late Directory dir;
      late String tbl;
      setUp(() {
        dir = Directory.systemTemp.createTempSync('ice_');
        tbl = '${dir.path}/t';
      });
      tearDown(() => dir.deleteSync(recursive: true));

      test('create then read', () async {
        await IcebergWriter(tbl).write(table([1, 2, 3])).ok;
        expect(await read(tbl), {1: 'n1', 2: 'n2', 3: 'n3'});
      });

      test('append accumulates across snapshots', () async {
        final w = IcebergWriter(tbl);
        await w.write(table([1, 2, 3])).ok;
        await w.write(table([4, 5])).ok;
        expect(await read(tbl), {1: 'n1', 2: 'n2', 3: 'n3', 4: 'n4', 5: 'n5'});
      });

      test(
        'commit writes metadata.json atomically (no temp left, all valid)',
        () async {
          final w = IcebergWriter(tbl);
          await w.write(table([1, 2, 3])).ok;
          await w.write(table([4, 5])).ok;
          final metas = Directory('$tbl/metadata').listSync().whereType<File>();
          expect(metas.where((f) => f.path.endsWith('.tmp')), isEmpty);
          for (final f in metas.where(
            (f) => f.path.endsWith('.metadata.json'),
          )) {
            expect(() => jsonDecode(f.readAsStringSync()), returnsNormally);
          }
          expect(await read(tbl), {
            1: 'n1',
            2: 'n2',
            3: 'n3',
            4: 'n4',
            5: 'n5',
          });
        },
      );

      test('overwrite replaces visible data', () async {
        final w = IcebergWriter(tbl);
        await w.write(table([1, 2, 3])).ok;
        await w.write(table([9], ['z']), mode: IcebergWriteMode.overwrite).ok;
        expect(await read(tbl), {9: 'z'});
      });

      test('time travel by snapshot id', () async {
        final w = IcebergWriter(tbl);
        await w.write(table([1, 2, 3])).ok;
        await w.write(table([4, 5])).ok;
        // First snapshot = the initial 3 rows.
        final meta =
            jsonDecode(
                  File(
                    Directory('$tbl/metadata')
                        .listSync()
                        .whereType<File>()
                        .map((f) => f.path)
                        .where((p) => p.endsWith('.metadata.json'))
                        .reduce((a, b) => a.compareTo(b) > 0 ? a : b),
                  ).readAsStringSync(),
                )
                as Map<String, dynamic>;
        final firstSnap =
            (meta['snapshots'] as List).first['snapshot-id'] as int;
        expect(await read(tbl, snapshotId: firstSnap), {
          1: 'n1',
          2: 'n2',
          3: 'n3',
        });
      });

      String metaFile() => Directory('$tbl/metadata')
          .listSync()
          .whereType<File>()
          .map((f) => f.path)
          .where((p) => p.endsWith('.metadata.json'))
          .reduce((a, b) => a.compareTo(b) > 0 ? a : b);
      void patchSnapshots(void Function(Map<String, dynamic>) edit) {
        final f = File(metaFile());
        final meta = jsonDecode(f.readAsStringSync()) as Map<String, dynamic>;
        for (final s
            in (meta['snapshots'] as List).cast<Map<String, dynamic>>()) {
          edit(s);
        }
        f.writeAsStringSync(jsonEncode(meta));
      }

      test('a snapshot with an unresolvable schema-id fails closed', () async {
        await IcebergWriter(tbl).write(table([1, 2])).ok;
        patchSnapshots((s) => s['schema-id'] = 999); // not in schemas[]
        await expectLater(IcebergTable(tbl).read(), failsWith(Kind.corrupt));
      });

      test('a snapshot with NO schema-id reads (legacy fallback)', () async {
        await IcebergWriter(tbl).write(table([1, 2])).ok;
        patchSnapshots((s) => s.remove('schema-id'));
        expect(await read(tbl), {1: 'n1', 2: 'n2'});
      });
    });

    test('reads a pyiceberg-authored table (checked-in fixture)', () async {
      const path = 'test/fixtures/iceberg/pyiceberg_written';
      expect(await read(path), {1: 'a', 2: 'b', 3: 'c', 4: 'd', 5: 'e'});
    });

    group('concurrency + streaming', () {
      late Directory dir;
      late String tbl;
      setUp(() {
        dir = Directory.systemTemp.createTempSync('ice_cc_');
        tbl = '${dir.path}/t';
      });
      tearDown(() => dir.deleteSync(recursive: true));

      test('concurrent appends both commit (no lost snapshot)', () async {
        await IcebergWriter(tbl).write(table([0])).ok;
        // Two writers race the next version; the exclusive version claim makes the
        // loser retry, so both snapshots survive.
        await Future.wait([
          IcebergWriter(tbl).write(table([1])),
          IcebergWriter(tbl).write(table([2])),
        ]);
        expect((await read(tbl)).keys.toList()..sort(), [0, 1, 2]);
      });

      test('readBatches streams the table one data file at a time', () async {
        await IcebergWriter(tbl).write(table([0, 1])).ok;
        await IcebergWriter(tbl).write(table([2, 3])).ok;
        var batches = 0;
        final ids = <int>[];
        await for (final b in IcebergTable(tbl).readBatches().ok) {
          batches++;
          ids.addAll(b.column('id')!.toList().cast<int>());
        }
        expect(batches, greaterThan(1)); // not one whole-table materialization
        expect(ids..sort(), [0, 1, 2, 3]);
      });

      test('a successful commit leaves no .commit marker', () async {
        await IcebergWriter(tbl).write(table([0])).ok;
        await IcebergWriter(tbl).write(table([1])).ok;
        final markers = Directory(
          '$tbl/metadata',
        ).listSync().where((e) => e.path.endsWith('.commit')).toList();
        expect(markers, isEmpty);
      });

      test('snapshot ids are positive and distinct across snapshots', () async {
        final w = IcebergWriter(tbl);
        await w.write(table([0])).ok;
        await w.write(table([1])).ok;
        await w.write(table([2])).ok;
        final meta =
            jsonDecode(
                  File(
                    Directory('$tbl/metadata')
                        .listSync()
                        .whereType<File>()
                        .map((f) => f.path)
                        .where((p) => p.endsWith('.metadata.json'))
                        .reduce((a, b) => a.compareTo(b) > 0 ? a : b),
                  ).readAsStringSync(),
                )
                as Map<String, dynamic>;
        final ids = [
          for (final s in meta['snapshots'] as List) s['snapshot-id'] as int,
        ];
        expect(ids.length, 3);
        expect(ids.toSet().length, 3); // distinct
        expect(
          ids.every((id) => id > 0),
          isTrue,
        ); // positive, no double truncation
      });

      test(
        'a stale marker (crashed writer) is reclaimed, not deadlocked',
        () async {
          await IcebergWriter(tbl).write(table([0])).ok; // v0 committed
          // Simulate a writer that claimed v1 then was SIGKILLed: an orphan marker
          // with a long-past mtime and no v1 metadata.
          final marker = File('$tbl/metadata/v00001.commit')..createSync();
          marker.setLastModifiedSync(
            DateTime.now().subtract(const Duration(hours: 1)),
          );
          // The next write must reclaim it and succeed (not spin/throw forever).
          await IcebergWriter(tbl).write(table([1])).ok;
          expect((await read(tbl)).keys.toList()..sort(), [0, 1]);
          expect(marker.existsSync(), isFalse); // reclaimed
        },
      );

      test('append with a mismatched schema is rejected, leaving the table '
          'unchanged', () async {
        await IcebergWriter(tbl).write(table([1, 2, 3])).ok;
        final dataBefore = Directory('$tbl/data').listSync().length;

        // A different schema (extra column) must not append.
        final wrong = Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'id', values: [9], nullable: false),
            NumericColumn.int64(name: 'extra', values: [9], nullable: false),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
              Field(name: 'extra', dataType: DataType.int64, nullable: false),
            ],
          ),
        );
        await expectLater(
          IcebergWriter(tbl).write(wrong),
          failsWith(Kind.schema),
        );
        // Table data unchanged and no orphaned data file written.
        expect(await read(tbl), {1: 'n1', 2: 'n2', 3: 'n3'});
        expect(Directory('$tbl/data').listSync().length, dataBefore);
      });

      test('overwrite may change the schema', () async {
        await IcebergWriter(tbl).write(table([1, 2, 3])).ok;
        final wide = Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'id', values: [7], nullable: false),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
            ],
          ),
        );
        await IcebergWriter(
          tbl,
        ).write(wide, mode: IcebergWriteMode.overwrite).ok;
        final t = await (await DataFrameIO.readIceberg(tbl).ok).execute().ok;
        expect(t.toRows(), [
          {'id': 7},
        ]);
      });
    });
  });

  group('position & equality deletes', () {
    late Directory dir;
    late String tbl;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('icedel_');
      tbl = '${dir.path}/t';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Future<Map<String, dynamic>> readJson(String path) async =>
        jsonDecode(await File(path).readAsString()) as Map<String, dynamic>;

    Table idsTable(List<int> ids) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: ids, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    test('data-file manifest carries per-column counts and bounds', () async {
      await IcebergWriter(tbl)
          .write(
            Table(
              name: 't',
              columns: [
                NumericColumn.int64(
                  name: 'id',
                  values: [10, 20, 30, 40],
                  nullable: false,
                ),
                ListColumn<String>(
                  name: 'name',
                  dataType: DataType.string,
                  data: ['a', 'b', null, 'd'], // one null
                  nullable: true,
                ),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'id', dataType: DataType.int64, nullable: false),
                  Field(
                    name: 'name',
                    dataType: DataType.string,
                    nullable: true,
                  ),
                ],
              ),
            ),
          )
          .ok;

      final metaFile = Directory('$tbl/metadata')
          .listSync()
          .whereType<File>()
          .map((f) => f.path)
          .firstWhere((p) => p.endsWith('.metadata.json'));
      final meta = await readJson(metaFile);
      final snap = (meta['snapshots'] as List)
          .cast<Map<String, dynamic>>()
          .last;
      final mlist = await AvroOcfReader.readPathAsync(
        _strip(snap['manifest-list'] as String),
      );
      final entries = await AvroOcfReader.readPathAsync(
        _strip(mlist.first['manifest_path'] as String),
      );
      final df = entries.first['data_file'] as Map;

      final schemaFields =
          ((meta['schemas'] as List).last as Map)['fields'] as List;
      final idOf = {
        for (final f in schemaFields.cast<Map>())
          f['name'] as String: f['id'] as int,
      };
      Map<int, Object?> asMap(dynamic v) => {
        for (final e in (v as List).cast<Map>()) e['key'] as int: e['value'],
      };

      final valueCounts = asMap(df['value_counts']);
      expect(valueCounts[idOf['id']], 4);
      expect(valueCounts[idOf['name']], 4);

      final nullCounts = asMap(df['null_value_counts']);
      expect(nullCounts[idOf['id']], 0);
      expect(nullCounts[idOf['name']], 1); // the one null name

      int decodeLong(List<int> b) => ByteData.view(
        Uint8List.fromList(b).buffer,
      ).getInt64(0, Endian.little);
      final lower = asMap(df['lower_bounds']);
      final upper = asMap(df['upper_bounds']);
      expect(decodeLong((lower[idOf['id']] as List).cast<int>()), 10);
      expect(decodeLong((upper[idOf['id']] as List).cast<int>()), 40);
      expect(utf8.decode((lower[idOf['name']] as List).cast<int>()), 'a');
      expect(utf8.decode((upper[idOf['name']] as List).cast<int>()), 'd');
    });

    test('bounds use code-point order and keep infinity', () async {
      await IcebergWriter(tbl)
          .write(
            Table(
              name: 't',
              columns: [
                ListColumn<String>(
                  name: 's',
                  dataType: DataType.string,
                  data: ['\uFF01', '\u{1F600}'],
                  nullable: false,
                ),
                NumericColumn.float64(
                  name: 'x',
                  values: [1.0, double.infinity],
                  nullable: false,
                ),
              ],
              schema: Schema(
                fields: [
                  Field(name: 's', dataType: DataType.string, nullable: false),
                  Field(name: 'x', dataType: DataType.float64, nullable: false),
                ],
              ),
            ),
          )
          .ok;
      final metaFile = Directory('$tbl/metadata')
          .listSync()
          .whereType<File>()
          .map((f) => f.path)
          .firstWhere((p) => p.endsWith('.metadata.json'));
      final meta = await readJson(metaFile);
      final snap = (meta['snapshots'] as List)
          .cast<Map<String, dynamic>>()
          .last;
      final mlist = await AvroOcfReader.readPathAsync(
        _strip(snap['manifest-list'] as String),
      );
      final entries = await AvroOcfReader.readPathAsync(
        _strip(mlist.first['manifest_path'] as String),
      );
      final df = entries.first['data_file'] as Map;
      final idOf = {
        for (final f
            in (((meta['schemas'] as List).last as Map)['fields'] as List)
                .cast<Map>())
          f['name'] as String: f['id'] as int,
      };
      Map<int, Object?> asMap(dynamic v) => {
        for (final e in (v as List).cast<Map>()) e['key'] as int: e['value'],
      };
      final upper = asMap(df['upper_bounds']);
      expect(utf8.decode((upper[idOf['s']] as List).cast<int>()), '\u{1F600}');
      final x = ByteData.view(
        Uint8List.fromList((upper[idOf['x']] as List).cast<int>()).buffer,
      ).getFloat64(0, Endian.little);
      expect(x, double.infinity);
      expect(asMap(df['nan_value_counts'])[idOf['x']], 0);
    });

    test('position deletes filter rows (streamed delete read)', () async {
      // 1. A base table of four rows (offsets 0..3).
      await IcebergWriter(tbl).write(idsTable([10, 20, 30, 40])).ok;

      // 2. Walk v0 metadata -> manifest-list -> data manifest -> the data file.
      final metaFile = Directory('$tbl/metadata')
          .listSync()
          .whereType<File>()
          .map((f) => f.path)
          .firstWhere((p) => p.endsWith('.metadata.json'));
      final meta = await readJson(metaFile);
      final snap = (meta['snapshots'] as List)
          .cast<Map<String, dynamic>>()
          .last;
      final mlist = await AvroOcfReader.readPathAsync(
        _strip(snap['manifest-list'] as String),
      );
      final dataManifestUri = mlist.first['manifest_path'] as String;
      final dataEntries = await AvroOcfReader.readPathAsync(
        _strip(dataManifestUri),
      );
      final dataFileUri =
          (dataEntries.first['data_file'] as Map)['file_path'] as String;

      // 3. Position-delete file removing offsets 1 and 3 (ids 20 and 40).
      final delParquet = '$tbl/data/del-pos.parquet';
      await ParquetWriter(delParquet)
          .write(
            Table(
              name: 'd',
              columns: [
                ListColumn<String>(
                  name: 'file_path',
                  dataType: DataType.string,
                  data: [dataFileUri, dataFileUri],
                  nullable: false,
                ),
                NumericColumn.int64(
                  name: 'pos',
                  values: [1, 3],
                  nullable: false,
                ),
              ],
              schema: Schema(
                fields: [
                  Field(
                    name: 'file_path',
                    dataType: DataType.string,
                    nullable: false,
                  ),
                  Field(name: 'pos', dataType: DataType.int64, nullable: false),
                ],
              ),
            ),
          )
          .ok;

      // 4. A delete manifest (content == 1) referencing that delete file.
      final delManifest = '$tbl/metadata/del-m0.avro';
      await AvroOcfWriter.writePathAsync(delManifest, manifestEntrySchema, [
        {
          'status': 1,
          'snapshot_id': 1,
          'sequence_number': null,
          'file_sequence_number': null,
          'data_file': {
            'content': 1, // POSITION DELETES
            'file_path': 'file://$delParquet',
            'file_format': 'PARQUET',
            'partition': <String, dynamic>{},
            'record_count': 2,
            'file_size_in_bytes': await File(delParquet).length(),
            'column_sizes': null,
            'value_counts': null,
            'null_value_counts': null,
            'nan_value_counts': null,
            'lower_bounds': null,
            'upper_bounds': null,
            'key_metadata': null,
            'split_offsets': null,
            'equality_ids': null,
            'sort_order_id': null,
          },
        },
      ]);

      // 5. A fresh snapshot listing both manifests directly (no manifest-list),
      //    written as the new current metadata.json.
      final newId = (snap['snapshot-id'] as int) + 1;
      final seq = (meta['last-sequence-number'] as int? ?? 0) + 1;
      final newSnap = {
        'snapshot-id': newId,
        'sequence-number': seq,
        'timestamp-ms': DateTime.now().millisecondsSinceEpoch,
        'manifests': [dataManifestUri, 'file://$delManifest'],
        'summary': {'operation': 'overwrite'},
        'schema-id': 0,
      };
      meta['snapshots'] = [...(meta['snapshots'] as List), newSnap];
      meta['current-snapshot-id'] = newId;
      meta['last-sequence-number'] = seq;
      meta['refs'] = {
        'main': {'snapshot-id': newId, 'type': 'branch'},
      };
      await File(
        '$tbl/metadata/00001-deletes.metadata.json',
      ).writeAsString(jsonEncode(meta));

      // 6. The read applies the position deletes via the streamed delete index.
      final t = await (await DataFrameIO.readIceberg(tbl).ok).execute().ok;
      final ids = [for (final r in t.toRows()) r['id'] as int]..sort();
      expect(ids, [10, 30]);
    });

    test('an equality delete naming no field ids is rejected (F5)', () async {
      // A base table of three rows.
      await IcebergWriter(tbl).write(idsTable([10, 20, 30])).ok;

      final metaFile = Directory('$tbl/metadata')
          .listSync()
          .whereType<File>()
          .map((f) => f.path)
          .firstWhere((p) => p.endsWith('.metadata.json'));
      final meta = await readJson(metaFile);
      final snap = (meta['snapshots'] as List)
          .cast<Map<String, dynamic>>()
          .last;
      final dataManifestUri =
          (await AvroOcfReader.readPathAsync(
                _strip(snap['manifest-list'] as String),
              )).first['manifest_path']
              as String;

      // A delete file naming id=10, but the manifest declares NO equality_ids.
      // Left unchecked this builds an empty RowKey that matches EVERY row, so
      // every row is silently dropped instead of just id=10.
      final delParquet = '$tbl/data/del-eq-empty.parquet';
      await ParquetWriter(delParquet)
          .write(
            Table(
              name: 'd',
              columns: [
                NumericColumn.int64(name: 'id', values: [10], nullable: false),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'id', dataType: DataType.int64, nullable: false),
                ],
              ),
            ),
          )
          .ok;

      final delManifest = '$tbl/metadata/del-eq-empty.avro';
      await AvroOcfWriter.writePathAsync(delManifest, manifestEntrySchema, [
        {
          'status': 1,
          'snapshot_id': 1,
          'sequence_number': 999,
          'file_sequence_number': 999,
          'data_file': {
            'content': 2, // EQUALITY DELETES
            'file_path': 'file://$delParquet',
            'file_format': 'PARQUET',
            'partition': <String, dynamic>{},
            'record_count': 1,
            'file_size_in_bytes': await File(delParquet).length(),
            'column_sizes': null,
            'value_counts': null,
            'null_value_counts': null,
            'nan_value_counts': null,
            'lower_bounds': null,
            'upper_bounds': null,
            'key_metadata': null,
            'split_offsets': null,
            'equality_ids': <int>[], // MALFORMED: names no fields
            'sort_order_id': null,
          },
        },
      ]);

      final newId = (snap['snapshot-id'] as int) + 1;
      final seq = (meta['last-sequence-number'] as int? ?? 0) + 1000;
      meta['snapshots'] = [
        ...(meta['snapshots'] as List),
        {
          'snapshot-id': newId,
          'sequence-number': seq,
          'timestamp-ms': DateTime.now().millisecondsSinceEpoch,
          'manifests': [dataManifestUri, 'file://$delManifest'],
          'summary': {'operation': 'overwrite'},
          'schema-id': 0,
        },
      ];
      meta['current-snapshot-id'] = newId;
      meta['last-sequence-number'] = seq;
      meta['refs'] = {
        'main': {'snapshot-id': newId, 'type': 'branch'},
      };
      await File(
        '$tbl/metadata/00001-eqempty.metadata.json',
      ).writeAsString(jsonEncode(meta));

      // The frame is lazy: the delete files are read by its terminal.
      final frame = await DataFrameIO.readIceberg(tbl).ok;
      await expectLater(frame.execute(), failsWith(Kind.corrupt));
    });

    test('an equality delete applies when its key is not selected', () async {
      await IcebergWriter(tbl)
          .write(
            Table.fromRows('t', [
              for (final id in [10, 20, 30]) {'id': id, 'v': 'v$id'},
            ]),
          )
          .ok;

      final metaFile = Directory('$tbl/metadata')
          .listSync()
          .whereType<File>()
          .map((f) => f.path)
          .firstWhere((p) => p.endsWith('.metadata.json'));
      final meta = await readJson(metaFile);
      final snap = (meta['snapshots'] as List)
          .cast<Map<String, dynamic>>()
          .last;
      final idFieldId =
          ((meta['schemas'] as List).first['fields'] as List).firstWhere(
                (f) => f['name'] == 'id',
              )['id']
              as int;
      final dataManifestUri =
          (await AvroOcfReader.readPathAsync(
                _strip(snap['manifest-list'] as String),
              )).first['manifest_path']
              as String;

      // A delete file for id=10, keyed on the id field.
      final delParquet = '$tbl/data/del-eq-unselected.parquet';
      await ParquetWriter(delParquet)
          .write(
            Table(
              name: 'd',
              columns: [
                NumericColumn.int64(name: 'id', values: [10], nullable: false),
              ],
              schema: Schema(
                fields: [
                  Field(
                    name: 'id',
                    dataType: DataType.int64,
                    nullable: false,
                    id: idFieldId,
                  ),
                ],
              ),
            ),
          )
          .ok;

      final delManifest = '$tbl/metadata/del-eq-unselected.avro';
      await AvroOcfWriter.writePathAsync(delManifest, manifestEntrySchema, [
        {
          'status': 1,
          'snapshot_id': 1,
          'sequence_number': 999,
          'file_sequence_number': 999,
          'data_file': {
            'content': 2, // EQUALITY DELETES
            'file_path': 'file://$delParquet',
            'file_format': 'PARQUET',
            'partition': <String, dynamic>{},
            'record_count': 1,
            'file_size_in_bytes': await File(delParquet).length(),
            'column_sizes': null,
            'value_counts': null,
            'null_value_counts': null,
            'nan_value_counts': null,
            'lower_bounds': null,
            'upper_bounds': null,
            'key_metadata': null,
            'split_offsets': null,
            'equality_ids': [idFieldId],
            'sort_order_id': null,
          },
        },
      ]);

      final newId = (snap['snapshot-id'] as int) + 1;
      final seq = (meta['last-sequence-number'] as int? ?? 0) + 1000;
      meta['snapshots'] = [
        ...(meta['snapshots'] as List),
        {
          'snapshot-id': newId,
          'sequence-number': seq,
          'timestamp-ms': DateTime.now().millisecondsSinceEpoch,
          'manifests': [dataManifestUri, 'file://$delManifest'],
          'summary': {'operation': 'overwrite'},
          'schema-id': 0,
        },
      ];
      meta['current-snapshot-id'] = newId;
      meta['last-sequence-number'] = seq;
      meta['refs'] = {
        'main': {'snapshot-id': newId, 'type': 'branch'},
      };
      await File(
        '$tbl/metadata/00001-equnselected.metadata.json',
      ).writeAsString(jsonEncode(meta));

      final batches = await IcebergTable(
        tbl,
      ).readBatches(columns: ['v']).ok.toList();
      for (final batch in batches) {
        expect(batch.schema.fields.map((f) => f.name), ['v']);
      }
      expect(
        [for (final b in batches) ...b.column('v')!.toList()],
        ['v20', 'v30'],
      );
    });

    test(
      'equality delete matches by FIELD ID across a column rename',
      () async {
        // Iceberg identity is the field id: files written before a rename keep
        // the old physical name. Matching by current-schema NAME read a null
        // column from those files, so rows that should be deleted were silently
        // returned (audit 7).
        await IcebergWriter(tbl).write(idsTable([7, 8, 9])).ok;

        final metaFile = Directory('$tbl/metadata')
            .listSync()
            .whereType<File>()
            .map((f) => f.path)
            .firstWhere((p) => p.endsWith('.metadata.json'));
        final meta = await readJson(metaFile);
        final schema = (meta['schemas'] as List)
            .cast<Map<String, dynamic>>()
            .first;
        final fields = (schema['fields'] as List).cast<Map<String, dynamic>>();
        final idFieldId =
            fields.firstWhere((f) => f['name'] == 'id')['id'] as int;
        // Simulate an EXTERNAL rename: same field id, new name. The data file
        // on disk keeps the old physical name 'id' (with its field id).
        fields.firstWhere((f) => f['name'] == 'id')['name'] = 'ident';

        final snap = (meta['snapshots'] as List)
            .cast<Map<String, dynamic>>()
            .last;
        final mlist = await AvroOcfReader.readPathAsync(
          _strip(snap['manifest-list'] as String),
        );
        final dataManifestUri = mlist.first['manifest_path'] as String;

        // An equality-delete file for id=7, written BEFORE the rename: its
        // parquet column carries the OLD name plus the field id.
        final delParquet = '$tbl/data/del-eq-rename.parquet';
        await ParquetWriter(delParquet)
            .write(
              Table(
                name: 'd',
                columns: [
                  NumericColumn.int64(name: 'id', values: [7], nullable: false),
                ],
                schema: Schema(
                  fields: [
                    Field(
                      name: 'id',
                      dataType: DataType.int64,
                      nullable: false,
                      id: idFieldId,
                    ),
                  ],
                ),
              ),
            )
            .ok;

        final delManifest = '$tbl/metadata/del-eq-rename-m0.avro';
        await AvroOcfWriter.writePathAsync(delManifest, manifestEntrySchema, [
          {
            'status': 1,
            'snapshot_id': 1,
            'sequence_number': 999,
            'file_sequence_number': 999,
            'data_file': {
              'content': 2, // EQUALITY DELETES
              'file_path': 'file://$delParquet',
              'file_format': 'PARQUET',
              'partition': <String, dynamic>{},
              'record_count': 1,
              'file_size_in_bytes': await File(delParquet).length(),
              'column_sizes': null,
              'value_counts': null,
              'null_value_counts': null,
              'nan_value_counts': null,
              'lower_bounds': null,
              'upper_bounds': null,
              'key_metadata': null,
              'split_offsets': null,
              'equality_ids': [idFieldId],
              'sort_order_id': null,
            },
          },
        ]);

        final newId = (snap['snapshot-id'] as int) + 1;
        final seq = (meta['last-sequence-number'] as int? ?? 0) + 1000;
        meta['snapshots'] = [
          ...(meta['snapshots'] as List),
          {
            'snapshot-id': newId,
            'sequence-number': seq,
            'timestamp-ms': DateTime.now().millisecondsSinceEpoch,
            'manifests': [dataManifestUri, 'file://$delManifest'],
            'summary': {'operation': 'overwrite'},
            'schema-id': 0,
          },
        ];
        meta['current-snapshot-id'] = newId;
        meta['last-sequence-number'] = seq;
        meta['refs'] = {
          'main': {'snapshot-id': newId, 'type': 'branch'},
        };
        await File(
          '$tbl/metadata/00001-eqrename.metadata.json',
        ).writeAsString(jsonEncode(meta));

        final t = await (await DataFrameIO.readIceberg(tbl).ok).execute().ok;
        final ids = [for (final r in t.toRows()) r['ident'] as int]..sort();
        expect(ids, [8, 9], reason: 'id=7 must be deleted despite the rename');
      },
    );
  });

  group('partition-scoped deletes', () {
    // manifestEntrySchema, but with the partition record carrying the `p` field so a
    // hand-authored delete entry can declare its partition (the built-in schema has
    // an empty partition record).
    final partManifest = manifestEntrySchema.replaceFirst(
      '"name":"partition","field-id":102,"type":{"type":"record","fields":[],"name":"r102"}',
      '"name":"partition","field-id":102,"type":{"type":"record","name":"r102",'
          '"fields":[{"name":"p","field-id":1000,"type":["null","string"],"default":null}]}',
    );

    Table rows(List<int> id, List<String> p) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: id, nullable: false),
        ListColumn<String>(name: 'p', dataType: DataType.string, data: p),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'p', dataType: DataType.string),
        ],
      ),
    );

    test('equality delete is scoped to its partition', () async {
      final dir = Directory.systemTemp.createTempSync('icepart_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final tbl = '${dir.path}/t';

      // Partition A = {7,8}, partition B = {7,9}.
      await IcebergWriter(
        tbl,
      ).write(rows([7, 8, 7, 9], ['A', 'A', 'B', 'B']), partitionBy: ['p']).ok;

      final metaFile = Directory('$tbl/metadata')
          .listSync()
          .whereType<File>()
          .map((f) => f.path)
          .firstWhere((p) => p.endsWith('.metadata.json'));
      final meta =
          jsonDecode(await File(metaFile).readAsString())
              as Map<String, dynamic>;
      final schema = (meta['schemas'] as List)
          .cast<Map<String, dynamic>>()
          .first;
      final idFieldId =
          (schema['fields'] as List).cast<Map<String, dynamic>>().firstWhere(
                (f) => f['name'] == 'id',
              )['id']
              as int;

      final snap = (meta['snapshots'] as List)
          .cast<Map<String, dynamic>>()
          .last;
      final mlist = await AvroOcfReader.readPathAsync(
        _strip(snap['manifest-list'] as String),
      );
      final dataManifests = [
        for (final m in mlist) m['manifest_path'] as String,
      ];

      // An equality-delete file removing id=7.
      final delParquet = '$tbl/data/del-eq.parquet';
      await ParquetWriter(delParquet)
          .write(
            Table(
              name: 'd',
              columns: [
                NumericColumn.int64(name: 'id', values: [7], nullable: false),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'id', dataType: DataType.int64, nullable: false),
                ],
              ),
            ),
          )
          .ok;

      // A delete manifest (content == 2) scoped to partition p=A only.
      final delManifest = '$tbl/metadata/del-eq-m0.avro';
      await AvroOcfWriter.writePathAsync(delManifest, partManifest, [
        {
          'status': 1,
          'snapshot_id': 1,
          'sequence_number': 999, // higher than the data files' seq
          'file_sequence_number': 999,
          'data_file': {
            'content': 2, // EQUALITY DELETES
            'file_path': 'file://$delParquet',
            'file_format': 'PARQUET',
            'partition': {'p': 'A'},
            'record_count': 1,
            'file_size_in_bytes': await File(delParquet).length(),
            'column_sizes': null,
            'value_counts': null,
            'null_value_counts': null,
            'nan_value_counts': null,
            'lower_bounds': null,
            'upper_bounds': null,
            'key_metadata': null,
            'split_offsets': null,
            'equality_ids': [idFieldId],
            'sort_order_id': null,
          },
        },
      ]);

      final newId = (snap['snapshot-id'] as int) + 1;
      final seq = (meta['last-sequence-number'] as int? ?? 0) + 1000;
      meta['snapshots'] = [
        ...(meta['snapshots'] as List),
        {
          'snapshot-id': newId,
          'sequence-number': seq,
          'timestamp-ms': DateTime.now().millisecondsSinceEpoch,
          'manifests': [...dataManifests, 'file://$delManifest'],
          'summary': {'operation': 'overwrite'},
          'schema-id': 0,
        },
      ];
      meta['current-snapshot-id'] = newId;
      meta['last-sequence-number'] = seq;
      meta['refs'] = {
        'main': {'snapshot-id': newId, 'type': 'branch'},
      };
      await File(
        '$tbl/metadata/00001-eqdel.metadata.json',
      ).writeAsString(jsonEncode(meta));

      final t = await (await DataFrameIO.readIceberg(tbl).ok).execute().ok;
      final ids = [for (final r in t.toRows()) r['id'] as int]..sort();
      // id=7 removed from A only; it survives in B. (Global matching → [8,9].)
      expect(ids, [7, 8, 9]);
    });
  });

  group('partition commit cleanup', () {
    Table t() => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: [1, 2, 3], nullable: false),
        ListColumn<String>(
          name: 'p',
          dataType: DataType.string,
          data: ['a', 'b', 'c'],
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'p', dataType: DataType.string),
        ],
      ),
    );

    int dataFileCount(String tbl) => Directory(
      '$tbl/data',
    ).listSync().where((e) => e.path.endsWith('.parquet')).length;

    test('a failed partitioned commit leaves no orphan data files', () async {
      final dir = Directory.systemTemp.createTempSync('iceclean_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final tbl = '${dir.path}/t';

      // Commit 0: 3 partitions → 3 data files.
      await IcebergWriter(tbl).write(t(), partitionBy: ['p']).ok;
      expect(dataFileCount(tbl), 3);

      // Block version 1 with a fresh commit marker so the next write writes its 3
      // partition files, then fails to claim the version and cleans up. Inject a
      // short-deadline coordinator so permanent contention fails FAST and
      // deterministically instead of waiting out the full backoff budget.
      await File('$tbl/metadata/v00001.commit').create();

      await expectLater(
        IcebergWriter(
          tbl,
          backend: const FilesystemIcebergBackend(
            loop: FilesystemCommitCoordinator(
              maxElapsed: Duration(milliseconds: 200),
            ),
          ),
        ).write(t(), partitionBy: ['p']),
        failsWith(Kind.conflict),
      );

      // The 3 orphans from the failed commit are gone, only commit 0's remain.
      expect(dataFileCount(tbl), 3);
    });
  });

  group('nested schema', () {
    final schema = Schema(
      fields: [
        Field(name: 'id', dataType: DataType.int64, nullable: false),
        Field(
          name: 'tags',
          dataType: DataType.list,
          nullable: true,
          children: [
            Field(name: 'item', dataType: DataType.int64, nullable: true),
          ],
        ),
        Field(
          name: 'info',
          dataType: DataType.struct,
          nullable: true,
          children: [
            Field(name: 'a', dataType: DataType.int64, nullable: true),
            Field(name: 'b', dataType: DataType.string, nullable: true),
          ],
        ),
      ],
    );

    test('build emits nested type objects with field-ids', () {
      final r = IcebergSchema.build(schema);
      final fields = r.json['fields'] as List;
      expect(fields[0]['type'], 'long');
      expect((fields[1]['type'] as Map)['type'], 'list');
      expect((fields[1]['type'] as Map).containsKey('element-id'), isTrue);
      expect((fields[2]['type'] as Map)['type'], 'struct');
      // The data-file id-schema carries matching nested ids.
      expect(r.idSchema.fields[1].id, fields[1]['id']);
      expect(r.idSchema.fields[1].children.first.id, isNotNull);
    });

    test('fromJson round-trips the nested shape', () {
      final back = IcebergSchema.fromJson(IcebergSchema.toJson(schema));
      expect(back.fields[1].dataType, DataType.list);
      expect(back.fields[1].children.first.dataType, DataType.int64);
      expect(back.fields[2].dataType, DataType.struct);
      expect(back.fields[2].children.map((f) => f.name), ['a', 'b']);
    });
  });

  group('commit coordinator', () {
    late Directory dir;
    late String tbl;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('ice_coord_');
      tbl = '${dir.path}/t';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Table t(List<int> ids) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: ids, nullable: false),
        ListColumn<String>(
          name: 'name',
          dataType: DataType.string,
          data: [for (final i in ids) 'n$i'],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'name', dataType: DataType.string, nullable: false),
        ],
      ),
    );

    Future<Set<int>> read(String path) async {
      final t = await (await DataFrameIO.readIceberg(path).ok).execute().ok;
      return {for (final r in t.toRows()) r['id'] as int};
    }

    test(
      'a custom coordinator is invoked; versions increment; table reads back',
      () async {
        final coord = _Recording();
        await IcebergWriter(tbl, backend: coord).write(t([1, 2])).ok;
        await IcebergWriter(tbl, backend: coord).write(t([3])).ok;
        expect(coord.versions, [0, 1]);
        expect(await read(tbl), {1, 2, 3});
      },
    );

    test('a lost CAS returns a conflict and leaves no orphans', () async {
      await expectLater(
        IcebergWriter(tbl, backend: _Conflicting()).write(t([1, 2, 3])),
        failsWith(Kind.conflict),
      );
      // The writer's outer cleanup removed the staged data/manifest files.
      final dataDir = Directory('$tbl/data');
      final data = dataDir.existsSync()
          ? dataDir.listSync().where((f) => f.path.endsWith('.parquet'))
          : const <FileSystemEntity>[];
      expect(data, isEmpty);
      final metaDir = Directory('$tbl/metadata');
      final manifests = metaDir.existsSync()
          ? metaDir.listSync().where((f) => f.path.endsWith('.avro'))
          : const <FileSystemEntity>[];
      expect(manifests, isEmpty);
      // And no metadata.json was published.
      await expectLater(read(tbl), throwsA(anything));
    });

    test(
      'a cleanup that cannot remove staged files is an io error caused by the '
      'conflict',
      () async {
        final outcome = await IcebergWriter(
          tbl,
          backend: _Undeletable(),
        ).write(t([1, 2, 3])).err;
        expect(outcome.kind, Kind.io);
        expect(outcome.message, contains('could not remove'));
        expect(
          outcome.cause,
          isA<ErrorDetails>().having(
            (cause) => cause.kind,
            'kind',
            Kind.conflict,
          ),
        );
      },
      testOn: '!windows',
    );

    test(
      'a registry write that fails after publish is indeterminate, caused by '
      'the write error',
      () async {
        final registry = Directory('${dir.path}/registry')..createSync();
        final backend = LocalCatalogIcebergBackend(
          LocalCatalog('${registry.path}/reg.json'),
          inner: _SealAfterPublish(registry.path),
        );
        addTearDown(() => Process.runSync('chmod', ['755', registry.path]));
        final outcome = await IcebergWriter(
          tbl,
          backend: backend,
        ).write(t([1, 2])).err;
        expect(outcome.kind, Kind.indeterminate);
        // The cause is the registry write's own report.
        expect(
          outcome.cause,
          isA<ErrorDetails>().having((cause) => cause.kind, 'kind', Kind.io),
        );
        // The commit landed, so its staged files were kept and it reads back.
        expect(await read(tbl), {1, 2});
      },
      testOn: '!windows',
    );

    test('an alternate (marker-free) coordinator drives two commits', () async {
      await IcebergWriter(tbl, backend: _NoMarker()).write(t([1, 2])).ok;
      await IcebergWriter(tbl, backend: _NoMarker()).write(t([3, 4])).ok;
      expect(await read(tbl), {1, 2, 3, 4});
    });

    test('FileLockIcebergBackend commits and reads back', () async {
      await IcebergWriter(
        tbl,
        backend: FileLockIcebergBackend(),
      ).write(t([1, 2])).ok;
      await IcebergWriter(
        tbl,
        backend: FileLockIcebergBackend(),
      ).write(t([3])).ok;
      expect(await read(tbl), {1, 2, 3});
    });

    test('FileLockIcebergBackend serializes overlapping writers', () async {
      await IcebergWriter(tbl).write(t([1])).ok; // v0
      // Overlapping writers through the file lock: the read → build → publish is
      // serialized, so neither clobbers the other and BOTH commits survive.
      await Future.wait([
        IcebergWriter(tbl, backend: FileLockIcebergBackend()).write(t([2])),
        IcebergWriter(tbl, backend: FileLockIcebergBackend()).write(t([3])),
      ]);
      expect(await read(tbl), {1, 2, 3});
    });

    test(
      'one backend owns both the commit CAS and the current read (review 5)',
      () async {
        // The backend is the SOLE pointer authority: its own `current` feeds both
        // the writer's CAS and a resolve; there is no separate coordinator/catalog
        // pair that could resolve different pointers (the split-brain the old API
        // allowed).
        final registry = LocalCatalog('${dir.path}/reg.json');
        final backend = LocalCatalogIcebergBackend(registry);
        await IcebergWriter(tbl, backend: backend).write(t([1, 2])).ok;
        await IcebergWriter(tbl, backend: backend).write(t([3])).ok;
        final cur = await backend.current(Directory(tbl).absolute.path).ok;
        expect(cur, isNotNull);
        expect((cur!['snapshots'] as List).length, 2);
        expect(await read(tbl), {1, 2, 3});
      },
    );

    test(
      'a registry backend adopts an existing filesystem table (no 2nd history)',
      () async {
        // Table created WITHOUT the registry (filesystem head on disk), then a
        // registry backend with an EMPTY registry opens it. Its `current` must
        // consult the on-disk head instead of returning null and letting the
        // writer rebuild version 0 over it (a second 00000-*.metadata.json).
        await IcebergWriter(
          tbl,
          backend: const FilesystemIcebergBackend(),
        ).write(t([1, 2])).ok;
        final root = Directory(tbl).absolute.path;
        final backend = LocalCatalogIcebergBackend(
          LocalCatalog('${dir.path}/reg.json'),
        );
        expect(
          await backend.current(root).ok,
          isNotNull,
        ); // sees the on-disk head
        await IcebergWriter(
          tbl,
          backend: backend,
        ).write(t([3])).ok; // appends v1
        final v0 = Directory('$root/metadata')
            .listSync()
            .map((f) => f.path)
            .where((p) => p.endsWith('.metadata.json') && p.contains('00000-'))
            .length;
        expect(
          v0,
          1,
          reason: 'exactly one version-0 metadata (no split-brain)',
        );
        expect(await read(tbl), {1, 2, 3});
      },
    );

    test(
      'the default writer is fenced (file-lock), matching filesystem output',
      () async {
        final a = '${dir.path}/a';
        final b = '${dir.path}/b';
        await IcebergWriter(
          a,
        ).write(t([1, 2])).ok; // default = FileLockIcebergBackend
        await IcebergWriter(
          b,
          backend: const FilesystemIcebergBackend(),
        ).write(t([1, 2])).ok;
        expect(await read(a), await read(b)); // same committed data
        // The default took the commit lock (fenced); the filesystem backend didn't.
        expect(File('$a/metadata/_commit.lock').existsSync(), isTrue);
        expect(File('$b/metadata/_commit.lock').existsSync(), isFalse);
      },
    );
  });

  group('Iceberg read-path', icebergTests);

  group('#2 Iceberg fails closed on corrupt metadata', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('r50_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table rows(List<int> ids) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: ids, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    test(
      'append with a missing manifest list throws (data preserved)',
      () async {
        final path = '${dir.path}/ice';
        await IcebergWriter(path).write(rows([1, 2])).ok;
        // Delete the current snapshot's manifest list to simulate corruption.
        final meta = Directory('$path/metadata');
        final list = meta.listSync().whereType<File>().firstWhere(
          (f) => f.uri.pathSegments.last.startsWith('snap-'),
        );
        list.deleteSync();
        await expectLater(
          IcebergWriter(path).write(rows([3])),
          failsWith(Kind.corrupt),
        );
      },
    );
  });

  group('Iceberg spec-compliance (round 52)', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('r52_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table intTable(String col, List<int> v) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: col, values: v, nullable: false)],
      schema: Schema(
        fields: [Field(name: col, dataType: DataType.int64, nullable: false)],
      ),
    );

    Map<String, dynamic> currentMeta(String root) {
      final f =
          Directory('$root/metadata')
              .listSync()
              .whereType<File>()
              .where((f) => f.path.endsWith('.metadata.json'))
              .toList()
            ..sort((a, b) => a.path.compareTo(b.path));
      return jsonDecode(f.last.readAsStringSync()) as Map<String, dynamic>;
    }

    group('#1 Iceberg field-id preservation', () {
      test(
        'build reuses existing ids and assigns fresh ones for new fields',
        () {
          final schema = Schema(
            fields: [
              Field(name: 'a', dataType: DataType.int64),
              Field(name: 'b', dataType: DataType.string),
              Field(name: 'c', dataType: DataType.int64), // new: no reuse entry
            ],
          );
          final built = IcebergSchema.build(
            schema,
            reuseIds: {'a': 10, 'b': 20},
          );
          final ids = {for (final f in built.idSchema.fields) f.name: f.id};
          expect(ids['a'], 10);
          expect(ids['b'], 20);
          expect(ids['c'], 21); // max(existing)+1, no collision
        },
      );

      test('idMap round-trips the ids build assigned', () {
        final schema = Schema(
          fields: [Field(name: 'x', dataType: DataType.int64)],
        );
        final json = IcebergSchema.build(schema, reuseIds: {'x': 7}).json;
        expect(IcebergSchema.idMap(json)['x'], 7);
      });

      test('append reuses the table field-ids in the name-mapping', () async {
        final path = '${dir.path}/ice';
        await IcebergWriter(path).write(intTable('id', [1, 2])).ok;
        // Rewrite the metadata schema ids to Spark-style 10 (simulating an
        // externally-created table) so the append must reuse them.
        final metaFile = Directory('$path/metadata')
            .listSync()
            .whereType<File>()
            .firstWhere((f) => f.path.endsWith('.metadata.json'));
        final meta =
            jsonDecode(metaFile.readAsStringSync()) as Map<String, dynamic>;
        (meta['schemas'][0]['fields'][0] as Map)['id'] = 10;
        metaFile.writeAsStringSync(jsonEncode(meta));

        await IcebergWriter(path).write(intTable('id', [3])).ok;
        final nm =
            jsonDecode(
                  currentMeta(path)['properties']['schema.name-mapping.default']
                      as String,
                )
                as List;
        expect(
          (nm.first as Map)['field-id'],
          10,
        ); // reused, was regenerated to 1
      });
    });

    group('#2 Iceberg metadata evolution', () {
      test('append preserves properties, tags, and snapshot history', () async {
        final path = '${dir.path}/ice2';
        await IcebergWriter(path).write(intTable('id', [1])).ok;
        // Inject a user property + a tag ref, as an external engine would.
        final metaFile = Directory('$path/metadata')
            .listSync()
            .whereType<File>()
            .firstWhere((f) => f.path.endsWith('.metadata.json'));
        final meta =
            jsonDecode(metaFile.readAsStringSync()) as Map<String, dynamic>;
        final firstSnap = (meta['snapshots'] as List).first['snapshot-id'];
        (meta['properties'] as Map)['owner'] = 'alice';
        (meta['refs'] as Map)['v1'] = {'snapshot-id': firstSnap, 'type': 'tag'};
        metaFile.writeAsStringSync(jsonEncode(meta));

        await IcebergWriter(path).write(intTable('id', [2])).ok;
        final m = currentMeta(path);
        expect((m['properties'] as Map)['owner'], 'alice'); // property survived
        expect((m['refs'] as Map).containsKey('v1'), isTrue); // tag survived
        expect((m['snapshots'] as List).length, 2); // history retained
      });

      test('overwrite retains prior snapshots (time travel)', () async {
        final path = '${dir.path}/ice3';
        await IcebergWriter(path).write(intTable('id', [1])).ok;
        final firstSnap = (currentMeta(path)['snapshots'] as List).first;
        await IcebergWriter(
          path,
        ).write(intTable('id', [2]), mode: IcebergWriteMode.overwrite).ok;
        final snaps = currentMeta(path)['snapshots'] as List;
        expect(snaps.length, 2); // was 1 (history dropped)
        expect(
          snaps.any((s) => s['snapshot-id'] == firstSnap['snapshot-id']),
          isTrue,
        );
      });
    });
  });

  group('Iceberg spec-completeness (round 53)', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('r53_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Map<String, dynamic> currentMeta(String root) {
      final f =
          Directory('$root/metadata')
              .listSync()
              .whereType<File>()
              .where((f) => f.path.endsWith('.metadata.json'))
              .toList()
            ..sort((a, b) => a.path.compareTo(b.path));
      return jsonDecode(f.last.readAsStringSync()) as Map<String, dynamic>;
    }

    group(
      '#1 Iceberg overwrite preserves per-snapshot schema (time travel)',
      () {
        test(
          'a schema-changing overwrite keeps the prior schema + binds by it',
          () async {
            final path = Directory('${dir.path}/ice').absolute.path;
            // v0: {id:long, name:string}
            await IcebergWriter(path)
                .write(
                  Table(
                    name: 't',
                    columns: [
                      NumericColumn.int64(
                        name: 'id',
                        values: [1],
                        nullable: false,
                      ),
                      ListColumn<String>(
                        name: 'name',
                        dataType: DataType.string,
                        data: ['a'],
                        nullable: false,
                      ),
                    ],
                    schema: Schema(
                      fields: [
                        Field(
                          name: 'id',
                          dataType: DataType.int64,
                          nullable: false,
                        ),
                        Field(
                          name: 'name',
                          dataType: DataType.string,
                          nullable: false,
                        ),
                      ],
                    ),
                  ),
                )
                .ok;
            final oldSnap =
                (currentMeta(path)['snapshots'] as List).first['snapshot-id'];

            // overwrite with a DIFFERENT schema {value:double}
            await IcebergWriter(path)
                .write(
                  Table(
                    name: 't',
                    columns: [
                      NumericColumn.float64(
                        name: 'value',
                        values: [3.5],
                        nullable: false,
                      ),
                    ],
                    schema: Schema(
                      fields: [
                        Field(
                          name: 'value',
                          dataType: DataType.float64,
                          nullable: false,
                        ),
                      ],
                    ),
                  ),
                  mode: IcebergWriteMode.overwrite,
                )
                .ok;

            final meta = currentMeta(path);
            // Both schemas retained; current points at the new one.
            expect((meta['schemas'] as List).length, 2);
            expect(meta['current-schema-id'], isNot(0));

            // Time travel to the OLD snapshot decodes under the OLD schema.
            final old = await IcebergTable(
              path,
            ).read(snapshotId: oldSnap as int).ok;
            expect(old.schema.fields.map((f) => f.name), ['id', 'name']);
            expect(old.column('name')!.toList(), ['a']);

            // Current read uses the NEW schema.
            final now = await IcebergTable(path).read().ok;
            expect(now.schema.fields.single.name, 'value');
          },
        );
      },
    );

    group('#3/#4/#5 Iceberg metadata spec completeness', () {
      Future<void> writeNested(String path) => IcebergWriter(path).write(
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'id', values: [1], nullable: false),
            ListColumn(
              name: 'tags',
              dataType: DataType.list,
              data: [
                [10, 20],
              ],
              nullable: true,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
              Field(
                name: 'tags',
                dataType: DataType.list,
                nullable: true,
                children: [
                  Field(name: 'item', dataType: DataType.int64, nullable: true),
                ],
              ),
            ],
          ),
        ),
      );

      test('#3 last-column-id counts nested ids, not top-level fields', () async {
        final path = Directory('${dir.path}/ice3').absolute.path;
        await writeNested(path);
        final meta = currentMeta(path);
        // Depth-first ids: id=1, tags=2, tags.element=3 → last-column-id=3, NOT the
        // 2 top-level fields.
        expect(meta['last-column-id'], 3);
      });

      test('#4 name-mapping recurses into the list element', () async {
        final path = Directory('${dir.path}/ice4').absolute.path;
        await writeNested(path);
        final meta = currentMeta(path);
        final mapping =
            jsonDecode(
                  (meta['properties'] as Map)['schema.name-mapping.default']
                      as String,
                )
                as List;
        final tags = mapping.cast<Map<String, dynamic>>().firstWhere(
          (m) => (m['names'] as List).contains('tags'),
        );
        // The list entry carries a nested 'element' with its own field-id.
        final nested = (tags['fields'] as List).cast<Map<String, dynamic>>();
        expect(nested.single['names'], ['element']);
        expect(nested.single['field-id'], 3);
      });

      test('#5 snapshot-log accumulates one entry per commit', () async {
        final path = Directory('${dir.path}/ice5').absolute.path;
        await writeNested(path);
        expect((currentMeta(path)['snapshot-log'] as List).length, 1);
        await writeNested(path); // identical-schema append
        final log = currentMeta(path)['snapshot-log'] as List;
        expect(log.length, 2);
        // Each entry pairs a timestamp with the snapshot it installed.
        expect(log.last.keys, containsAll(['timestamp-ms', 'snapshot-id']));
      });
    });
  });

  group('review round 43 Iceberg', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('round43_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table ids(List<int> v, {bool nullableId = false}) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: v, nullable: nullableId),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: nullableId),
        ],
      ),
    );

    group('#1 Iceberg indeterminate commit', () {
      test(
        'an indeterminate outcome RETAINS staged data (commit may have landed)',
        () async {
          final tbl = '${dir.path}/t';
          await expectLater(
            IcebergWriter(
              tbl,
              backend: const _IndeterminateAfterPublish(),
            ).write(ids([1, 2, 3])),
            failsWith(Kind.indeterminate),
          );
          // The coordinator published before the lost response, so the data files
          // must NOT have been deleted: the snapshot is readable.
          final t = await IcebergTable(tbl).read().ok;
          expect(t.rowCount, 3);
        },
      );
    });

    group('#2 Iceberg schema requiredness', () {
      test(
        'appending a nullable column to a required column is rejected',
        () async {
          final tbl = '${dir.path}/t';
          await IcebergWriter(tbl).write(ids([1, 2])).ok; // required id
          await expectLater(
            // Same name/type but nullable=true → must not be accepted as an append.
            IcebergWriter(tbl).write(ids([3], nullableId: true)),
            failsWith(Kind.schema),
          );
        },
      );
    });

    group('#3 nested Iceberg read', () {
      test('a struct/list table written by Iceberg reads back', () async {
        final tbl = '${dir.path}/t';
        final table = Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'id', values: [1], nullable: false),
            ListColumn(
              name: 'tags',
              dataType: DataType.list,
              data: [
                [10, 20, 30],
              ],
              nullable: true,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
              Field(
                name: 'tags',
                dataType: DataType.list,
                nullable: true,
                children: [
                  Field(name: 'item', dataType: DataType.int64, nullable: true),
                ],
              ),
            ],
          ),
        );
        await IcebergWriter(tbl).write(table).ok;
        final back = await IcebergTable(tbl).read().ok;
        expect(back.rowCount, 1);
        expect(back.schema.field('tags')!.dataType, DataType.list);
        expect(back.column('tags')!.toList().first, equals([10, 20, 30]));
      });
    });
  });

  group('#3 nested Iceberg read honours caps', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('round44_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table nested() => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: [1, 2], nullable: false),
        ListColumn(
          name: 'tags',
          dataType: DataType.list,
          data: [
            [10, 20],
            [30],
          ],
          nullable: true,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(
            name: 'tags',
            dataType: DataType.list,
            nullable: true,
            children: [
              Field(name: 'item', dataType: DataType.int64, nullable: true),
            ],
          ),
        ],
      ),
    );

    test(
      'a maxBytes cap now applies to a nested table (no unbounded read)',
      () async {
        final tbl = '${dir.path}/t';
        await IcebergWriter(tbl).write(nested()).ok;
        // Nested reads now stream per row group, so the Iceberg per-batch cap fires
        // instead of materializing the whole file unbounded.
        await expectLater(
          IcebergTable(tbl).read(maxBytes: 1),
          failsWith(Kind.limit),
        );
        // Unbounded still round-trips.
        final t = await IcebergTable(tbl).read(maxBytes: null).ok;
        expect(t.rowCount, 2);
      },
    );
  });

  group('#3 Iceberg field-ID resolution', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('round45_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('a renamed column reads its data via field-id (not null)', () async {
      final tbl = '${dir.path}/t';
      await IcebergWriter(tbl)
          .write(
            Table(
              name: 't',
              columns: [
                NumericColumn.int64(
                  name: 'id',
                  values: [1, 2],
                  nullable: false,
                ),
                NumericColumn.int64(
                  name: 'val',
                  values: [10, 20],
                  nullable: false,
                ),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'id', dataType: DataType.int64, nullable: false),
                  Field(name: 'val', dataType: DataType.int64, nullable: false),
                ],
              ),
            ),
          )
          .ok;
      // Simulate a column RENAME in the current metadata: 'val' → 'renamed',
      // keeping its field-id. The old data file still names the column 'val'.
      final metaDir = Directory('$tbl/metadata');
      final metaFile = metaDir.listSync().whereType<File>().firstWhere(
        (f) => f.path.endsWith('.metadata.json'),
      );
      final meta =
          jsonDecode(await metaFile.readAsString()) as Map<String, dynamic>;
      for (final s in (meta['schemas'] as List).cast<Map<String, dynamic>>()) {
        for (final f in (s['fields'] as List).cast<Map<String, dynamic>>()) {
          if (f['name'] == 'val') f['name'] = 'renamed';
        }
      }
      await metaFile.writeAsString(jsonEncode(meta));

      final t = await IcebergTable(tbl).read().ok;
      expect(t.schema.field('renamed'), isNotNull);
      // Resolved BY ID → the old-named physical column's data, not null-fill.
      expect(t.column('renamed')!.toList(), [10, 20]);
      // A projection names the current field and reads the old column.
      final batches = await IcebergTable(
        tbl,
      ).readBatches(columns: ['renamed']).ok.toList();
      expect(
        [for (final b in batches) ...b.column('renamed')!.toList()],
        [10, 20],
      );
      expect(batches.first.schema.fields.map((f) => f.name), ['renamed']);
    });
  });

  group('#5 Iceberg writer split-brain', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('round46_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('an append fails closed on two same-version metadata files', () async {
      final tbl = '${dir.path}/t';
      await IcebergWriter(tbl)
          .write(
            Table(
              name: 't',
              columns: [
                NumericColumn.int64(name: 'id', values: [1], nullable: false),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'id', dataType: DataType.int64, nullable: false),
                ],
              ),
            ),
          )
          .ok;
      // Duplicate the current metadata under a second name at the SAME version.
      final metaDir = Directory('$tbl/metadata');
      final meta = metaDir.listSync().whereType<File>().firstWhere(
        (f) => f.path.endsWith('.metadata.json'),
      );
      final name = meta.uri.pathSegments.last; // e.g. 00000-uuid.metadata.json
      final version = name.split('-').first;
      meta.copySync('$tbl/metadata/$version-duplicate.metadata.json');
      await expectLater(
        IcebergWriter(tbl).write(
          Table(
            name: 't',
            columns: [
              NumericColumn.int64(name: 'id', values: [2], nullable: false),
            ],
            schema: Schema(
              fields: [
                Field(name: 'id', dataType: DataType.int64, nullable: false),
              ],
            ),
          ),
        ),
        failsWith(Kind.corrupt),
      );
    });
  });

  group('finding 7: Iceberg reader fails loud on version collision', () {
    late Directory dir;
    setUp(() async {
      dir = await Directory.systemTemp.createTemp('review_findings');
    });
    tearDown(() async {
      if (await dir.exists()) await dir.delete(recursive: true);
    });

    test(
      'two metadata files sharing a version throw instead of a silent pick',
      () async {
        final tbl = '${dir.path}/iceberg7';
        await IcebergWriter(tbl).write(_ids([1])).ok;
        final metaDir = Directory('$tbl/metadata');
        final names =
            metaDir
                .listSync()
                .whereType<File>()
                .map((f) => f.uri.pathSegments.last)
                .where((n) => RegExp(r'^\d+-.*\.metadata\.json$').hasMatch(n))
                .toList()
              ..sort();
        final top = names.last;
        final prefix = RegExp(r'^(\d+)-').firstMatch(top)!.group(1)!;
        // A second file with the SAME numeric version but a different uuid.
        await File(
          '${metaDir.path}/$prefix-'
          '00000000000000000000000000000000.metadata.json',
        ).writeAsString(await File('${metaDir.path}/$top').readAsString());
        await expectLater(
          IcebergTable(tbl).read(),
          failsWith(Kind.corrupt, contains('split-brain')),
        );
      },
    );
  });

  // Changing a table's partitioning registers a NEW spec. Every manifest names
  // the spec its partition records were written under, and readers project
  // `data_file.partition` by that spec's FIELD-IDS, so the id in the manifest
  // list and the field-ids in the spec both have to be real.
  group('partition spec evolution', () {
    late Directory dir;
    late String tbl;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('ice_spec_');
      tbl = Directory('${dir.path}/t').absolute.path;
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Table rows(String partition, List<String> values) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(
          name: 'id',
          values: [for (var i = 0; i < values.length; i++) i],
          nullable: false,
        ),
        ListColumn<String>(
          name: partition,
          dataType: DataType.string,
          data: values,
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: partition, dataType: DataType.string, nullable: false),
        ],
      ),
    );

    Future<Map<String, dynamic>> current() async {
      final latest =
          Directory('$tbl/metadata')
              .listSync()
              .whereType<File>()
              .map((f) => f.path)
              .where((p) => p.endsWith('.metadata.json'))
              .toList()
            ..sort();
      return jsonDecode(File(latest.last).readAsStringSync())
          as Map<String, dynamic>;
    }

    /// Every manifest-list entry's declared spec id, newest snapshot first.
    Future<List<int>> declaredSpecIds() async {
      final meta = await current();
      final out = <int>[];
      for (final snapshot
          in (meta['snapshots'] as List).cast<Map<String, dynamic>>()) {
        for (final entry in await AvroOcfReader.readPathAsync(
          _strip(snapshot['manifest-list'] as String),
        )) {
          out.add(entry['partition_spec_id'] as int);
        }
      }
      return out;
    }

    test('a repartitioning overwrite registers a second spec', () async {
      await IcebergWriter(
        tbl,
      ).write(rows('region', ['eu']), partitionBy: ['region']).ok;
      await IcebergWriter(tbl)
          .write(
            rows('country', ['de']),
            mode: IcebergWriteMode.overwrite,
            partitionBy: ['country'],
          )
          .ok;
      final meta = await current();
      final specs = (meta['partition-specs'] as List)
          .cast<Map<String, dynamic>>();
      expect(specs, hasLength(2));
      expect(meta['default-spec-id'], 1);
    });

    test('each manifest declares the spec it was written under', () async {
      await IcebergWriter(
        tbl,
      ).write(rows('region', ['eu']), partitionBy: ['region']).ok;
      await IcebergWriter(tbl)
          .write(
            rows('country', ['de']),
            mode: IcebergWriteMode.overwrite,
            partitionBy: ['country'],
          )
          .ok;
      // The new snapshot's manifest belongs to spec 1, not the hardcoded 0.
      expect(await declaredSpecIds(), contains(1));
    });

    test('the second spec gets fresh partition field-ids', () async {
      await IcebergWriter(
        tbl,
      ).write(rows('region', ['eu']), partitionBy: ['region']).ok;
      await IcebergWriter(tbl)
          .write(
            rows('country', ['de']),
            mode: IcebergWriteMode.overwrite,
            partitionBy: ['country'],
          )
          .ok;
      final meta = await current();
      final specs = (meta['partition-specs'] as List)
          .cast<Map<String, dynamic>>();
      final ids = [
        for (final spec in specs)
          for (final f in (spec['fields'] as List).cast<Map<String, dynamic>>())
            f['field-id'] as int,
      ];
      // Reusing 1000 across specs is what makes one spec's value decode as the
      // other's column.
      expect(ids.toSet(), hasLength(ids.length), reason: 'field-ids: $ids');
      expect(meta['last-partition-id'], ids.reduce((a, b) => a > b ? a : b));
    });

    test('re-partitioning back to the original REUSES its spec', () async {
      // `region` stays in the schema throughout, so it keeps its field id. (A
      // column dropped and added back is a new column with a new id, and so a
      // new spec.)
      Table both(String region, String country) => Table(
        name: 't',
        columns: [
          ListColumn<String>(
            name: 'region',
            dataType: DataType.string,
            data: [region],
            nullable: false,
          ),
          ListColumn<String>(
            name: 'country',
            dataType: DataType.string,
            data: [country],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'region', dataType: DataType.string, nullable: false),
            Field(name: 'country', dataType: DataType.string, nullable: false),
          ],
        ),
      );
      await IcebergWriter(
        tbl,
      ).write(both('eu', 'fr'), partitionBy: ['region']).ok;
      await IcebergWriter(tbl)
          .write(
            both('eu', 'de'),
            mode: IcebergWriteMode.overwrite,
            partitionBy: ['country'],
          )
          .ok;
      await IcebergWriter(tbl)
          .write(
            both('us', 'us'),
            mode: IcebergWriteMode.overwrite,
            partitionBy: ['region'],
          )
          .ok;
      final meta = await current();
      // A spec is matched by (source-id, transform, name): no third spec.
      expect((meta['partition-specs'] as List), hasLength(2));
      expect(meta['default-spec-id'], 0);
    });

    test(
      'a decimal partition is typed as an Avro decimal, not a string',
      () async {
        final table = Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'id', values: [0, 1], nullable: false),
            ListColumn<BigInt>(
              name: 'amount',
              dataType: DataType.decimal,
              data: [BigInt.from(12345), BigInt.from(670)], // 123.45, 6.70
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
              Field(
                name: 'amount',
                dataType: DataType.decimal,
                nullable: false,
                precision: 10,
                scale: 2,
              ),
            ],
          ),
        );
        await IcebergWriter(tbl).write(table, partitionBy: ['amount']).ok;

        // Iceberg requires data_file.partition's field types to be the spec's
        // RESULT types. Writing a decimal as an Avro string invites a reader's
        // string<->bytes promotion to decode "123.45" as unscaled decimal bytes.
        final meta = await current();
        final snapshot = (meta['snapshots'] as List)
            .cast<Map<String, dynamic>>()
            .last;
        final entry = (await AvroOcfReader.readPathAsync(
          _strip(snapshot['manifest-list'] as String),
        )).single;
        final manifest = _strip(entry['manifest_path'] as String);
        final values = {
          for (final record in await AvroOcfReader.readPathAsync(manifest))
            ((record['data_file'] as Map)['partition'] as Map)['amount'],
        };
        // A decimal-typed field decodes back to its unscaled BigInt; an Avro
        // `string` field would hand back the text "12345".
        expect(values, {BigInt.from(12345), BigInt.from(670)});
      },
    );

    test('an unpartitioned table still reports spec 0', () async {
      await IcebergWriter(tbl).write(rows('region', ['eu'])).ok;
      final meta = await current();
      expect(meta['default-spec-id'], 0);
      expect(await declaredSpecIds(), [0]);
    });

    test('repeated appends keep one spec and read back', () async {
      await IcebergWriter(
        tbl,
      ).write(rows('region', ['eu']), partitionBy: ['region']).ok;
      await IcebergWriter(
        tbl,
      ).write(rows('region', ['us']), partitionBy: ['region']).ok;
      final meta = await current();
      expect((meta['partition-specs'] as List), hasLength(1));
      final table = await (await DataFrameIO.readIceberg(tbl).ok).execute().ok;
      expect(table.rowCount, 2);
    });
  });

  // expireSnapshots is the remedy the 128MiB metadata cap's error message and
  // IcebergCatalog's doc both already tell users to run. It DELETES files, so
  // the tests care most about what must SURVIVE.
  group('snapshot selection and metadata names', () {
    late Directory dir;
    late String tbl;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('ice_select_');
      tbl = Directory('${dir.path}/t').absolute.path;
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Table rows(List<int> ids) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: ids, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    String latest() =>
        (Directory('$tbl/metadata').listSync().map((f) => f.path).toList()
              ..sort())
            .lastWhere((p) => p.endsWith('.metadata.json'));

    Future<Set<int>> ids({int? asOf}) async {
      final t = await IcebergTable(tbl).read(asOf: asOf).ok;
      return {for (final r in t.toRows()) r['id'] as int};
    }

    /// Two appends, then a rollback to the first recorded in snapshot-log.
    Future<({int first, int second, int rollback})> rolledBack() async {
      await IcebergWriter(tbl).write(rows([1])).ok;
      await IcebergWriter(tbl).write(rows([2])).ok;
      final path = latest();
      final json =
          jsonDecode(File(path).readAsStringSync()) as Map<String, dynamic>;
      final snapshots = (json['snapshots'] as List).cast<Map>();
      final first = snapshots.first['timestamp-ms'] as int;
      final second = snapshots.last['timestamp-ms'] as int;
      final rollback = second + 1000;
      json['current-snapshot-id'] = snapshots.first['snapshot-id'];
      json['snapshot-log'] = [
        ...(json['snapshot-log'] as List),
        {
          'timestamp-ms': rollback,
          'snapshot-id': snapshots.first['snapshot-id'],
        },
      ];
      File(path).writeAsStringSync(jsonEncode(json));
      return (first: first, second: second, rollback: rollback);
    }

    test('asOf follows snapshot-log through a rollback', () async {
      final times = await rolledBack();
      expect(await ids(asOf: times.first), {1});
      expect(await ids(asOf: times.second), {1, 2});
      expect(await ids(asOf: times.rollback + 1), {1});
    });

    test('asOf before the first snapshot is missing', () async {
      final times = await rolledBack();
      await expectLater(
        IcebergTable(tbl).read(asOf: times.first - 1),
        failsWith(Kind.missing, contains('older than')),
      );
    });

    /// Rename the metadata files to the Hadoop catalog's `v<n>` form.
    void hadoop() {
      final files =
          Directory('$tbl/metadata')
              .listSync()
              .map((f) => f.path)
              .where((p) => p.endsWith('.metadata.json'))
              .toList()
            ..sort();
      for (var i = 0; i < files.length; i++) {
        File(files[i]).renameSync('$tbl/metadata/v${i + 1}.metadata.json');
      }
      File(
        '$tbl/metadata/version-hint.text',
      ).writeAsStringSync('${files.length}');
    }

    test('Hadoop catalog v<n>.metadata.json files read', () async {
      await IcebergWriter(tbl).write(rows([1])).ok;
      await IcebergWriter(tbl).write(rows([2])).ok;
      hadoop();
      expect(await ids(), {1, 2});
      final scanned = await openScan(tbl).read();
      expect(scanned.rowCount, 2);
    });

    test('gzip-compressed metadata reads', () async {
      await IcebergWriter(tbl).write(rows([1])).ok;
      await IcebergWriter(tbl).write(rows([2])).ok;
      hadoop();
      final plain = File('$tbl/metadata/v2.metadata.json');
      File(
        '$tbl/metadata/v2.gz.metadata.json',
      ).writeAsBytesSync(gzip.encode(plain.readAsBytesSync()));
      plain.deleteSync();
      expect(await ids(), {1, 2});
      final scanned = await openScan(tbl).read();
      expect(scanned.rowCount, 2);
    });
  });

  group('expireSnapshots', () {
    late Directory dir;
    late String tbl;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('ice_expire_');
      tbl = Directory('${dir.path}/t').absolute.path;
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Table rows(List<int> ids) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: ids, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    Future<Map<String, dynamic>> current() async {
      final latest =
          Directory('$tbl/metadata')
              .listSync()
              .whereType<File>()
              .map((f) => f.path)
              .where((p) => p.endsWith('.metadata.json'))
              .toList()
            ..sort();
      return jsonDecode(File(latest.last).readAsStringSync())
          as Map<String, dynamic>;
    }

    Future<List<int>> snapshots() async => [
      for (final s in (await current())['snapshots'] as List)
        s['snapshot-id'] as int,
    ];

    Future<Set<int>> ids([int? snapshotId]) async {
      final t = await (await DataFrameIO.readIceberg(
        tbl,
        snapshotId: snapshotId,
      ).ok).execute().ok;
      return {for (final r in t.toRows()) r['id'] as int};
    }

    /// Four appends -> four snapshots, each adding one id.
    Future<void> build() async {
      for (var i = 1; i <= 4; i++) {
        await IcebergWriter(tbl).write(rows([i])).ok;
      }
    }

    test('retains the newest N and drops the rest', () async {
      await build();
      final before = await snapshots();
      expect(before, hasLength(4));

      final expired = await IcebergWriter(tbl).expireSnapshots(retain: 2).ok;
      expect(expired, 2);

      final after = await snapshots();
      expect(after, before.sublist(2));
      // The table itself is untouched: expiry drops HISTORY, not data.
      expect(await ids(), {1, 2, 3, 4});
    });

    test('time travel still works for a retained snapshot', () async {
      await build();
      final before = await snapshots();
      await IcebergWriter(tbl).expireSnapshots(retain: 2).ok;
      // The 3rd snapshot saw ids 1..3 and is retained.
      expect(await ids(before[2]), {1, 2, 3});
    });

    test('retain-N ranks by timestamp, not array position (audit 7)', () async {
      // The spec does not guarantee the snapshots array is chronological; an
      // externally-written table may append out of order. Positional retention
      // could then expire a genuinely recent snapshot.
      await build();
      final meta = await current();
      final all = ((meta['snapshots'] as List).cast<Map<String, dynamic>>());
      // Stamp distinct times and shuffle the array so the NEWEST non-current
      // snapshot sits FIRST (positional retention would expire it).
      for (var i = 0; i < all.length; i++) {
        all[i]['timestamp-ms'] = 1000 * (i + 1);
      }
      final reordered = [all[2], all[0], all[1], all[3]];
      meta['snapshots'] = reordered;
      final latest =
          Directory('$tbl/metadata')
              .listSync()
              .whereType<File>()
              .map((f) => f.path)
              .where((p) => p.endsWith('.metadata.json'))
              .toList()
            ..sort();
      File(latest.last).writeAsStringSync(jsonEncode(meta));

      // Keep the 2 newest by TIME: snapshots 3 (ts 3000) and 4 (ts 4000,
      // current). Positional retention would keep the array's last two:
      // snapshots 2 and 4: expiring the newer snapshot 3.
      final expired = await IcebergWriter(tbl).expireSnapshots(retain: 2).ok;
      expect(expired, 2);
      final kept = await snapshots();
      expect(kept.toSet(), {all[2]['snapshot-id'], all[3]['snapshot-id']});
    });

    test('reading an expired snapshot says so, naming the id', () async {
      await build();
      final before = await snapshots();
      await IcebergWriter(tbl).expireSnapshots(retain: 2).ok;
      await expectLater(
        DataFrameIO.readIceberg(tbl, snapshotId: before.first),
        failsWith(
          Kind.missing,
          allOf(contains('${before.first}'), contains('expired')),
        ),
      );
    });

    test('a retained snapshot never loses a file it reaches', () async {
      await build();
      await IcebergWriter(tbl).expireSnapshots(retain: 2).ok;
      // Every file the retained snapshots reference must still exist: an
      // append carries prior manifests forward, so the newest snapshot reaches
      // manifests first written for snapshots that are now expired.
      final meta = await current();
      for (final snapshot
          in (meta['snapshots'] as List).cast<Map<String, dynamic>>()) {
        final list = _strip(snapshot['manifest-list'] as String);
        expect(File(list).existsSync(), isTrue, reason: list);
        for (final entry in await AvroOcfReader.readPathAsync(list)) {
          final manifest = _strip(entry['manifest_path'] as String);
          expect(File(manifest).existsSync(), isTrue, reason: manifest);
          for (final record in await AvroOcfReader.readPathAsync(manifest)) {
            final data = _strip(
              (record['data_file'] as Map)['file_path'] as String,
            );
            expect(File(data).existsSync(), isTrue, reason: data);
          }
        }
      }
    });

    test(
      'an expired snapshot\'s unreachable manifest list is deleted',
      () async {
        await build();
        final meta = await current();
        final doomed = _strip(
          ((meta['snapshots'] as List)
                  .cast<Map<String, dynamic>>()
                  .first)['manifest-list']
              as String,
        );
        expect(File(doomed).existsSync(), isTrue);
        await IcebergWriter(tbl).expireSnapshots(retain: 2).ok;
        expect(File(doomed).existsSync(), isFalse);
      },
    );

    test('the snapshot-log is pruned to the retained snapshots', () async {
      await build();
      await IcebergWriter(tbl).expireSnapshots(retain: 2).ok;
      final meta = await current();
      final retained = await snapshots();
      final logged = [
        for (final e in (meta['snapshot-log'] as List? ?? const []))
          (e as Map)['snapshot-id'] as int,
      ];
      expect(logged.toSet().difference(retained.toSet()), isEmpty);
    });

    test('before= protects snapshots newer than the cutoff', () async {
      await build();
      final before = await snapshots();
      // Nothing predates the epoch, so nothing may expire however small retain
      // is: retain is a floor on top of the age cutoff, not an override.
      expect(
        await IcebergWriter(tbl)
            .expireSnapshots(
              retain: 1,
              before: DateTime.fromMillisecondsSinceEpoch(0),
            )
            .ok,
        0,
      );
      expect(await snapshots(), before);
    });

    test('before= expires only what predates the cutoff', () async {
      await build();
      final before = await snapshots();
      final expired = await IcebergWriter(
        tbl,
      ).expireSnapshots(retain: 1, before: DateTime.now()).ok;
      expect(expired, 3);
      expect(await snapshots(), [before.last]);
      expect(await ids(), {1, 2, 3, 4});
    });

    test('the current snapshot is never expired', () async {
      await build();
      final before = await snapshots();
      expect(await IcebergWriter(tbl).expireSnapshots(retain: 1).ok, 3);
      expect(await snapshots(), [before.last]);
      expect(await ids(), {1, 2, 3, 4});
    });

    test('retain below 1 is rejected', () async {
      await build();
      expect(
        () => IcebergWriter(tbl).expireSnapshots(retain: 0),
        throwsArgumentError,
      );
    });

    test('nothing to expire is a no-op', () async {
      await build();
      final before = await snapshots();
      Set<String> metaFiles() => Directory('$tbl/metadata')
          .listSync()
          .where((f) => f.path.endsWith('.metadata.json'))
          .map((f) => f.path)
          .toSet();
      final metaBefore = metaFiles();
      expect(await IcebergWriter(tbl).expireSnapshots(retain: 10).ok, 0);
      expect(await snapshots(), before);
      // A genuine no-op publishes NO new metadata version (no churn).
      expect(metaFiles(), metaBefore);
    });

    /// Rewrite the current metadata file in place with [change] applied.
    Future<void> edit(void Function(Map<String, dynamic>) change) async {
      final latest =
          (Directory('$tbl/metadata').listSync().map((f) => f.path).toList()
                ..sort())
              .lastWhere((p) => p.endsWith('.metadata.json'));
      final json =
          jsonDecode(File(latest).readAsStringSync()) as Map<String, dynamic>;
      change(json);
      File(latest).writeAsStringSync(jsonEncode(json));
    }

    test('a snapshot a tag or branch names survives with its files', () async {
      await build();
      final before = await snapshots();
      await edit((json) {
        json['refs'] = {
          ...(json['refs'] as Map? ?? const {}),
          'v1': {'snapshot-id': before[0], 'type': 'tag'},
          'dev': {'snapshot-id': before[1], 'type': 'branch'},
        };
      });
      expect(await IcebergWriter(tbl).expireSnapshots(retain: 1).ok, 1);
      expect(await snapshots(), [before[0], before[1], before[3]]);
      expect(await ids(before[0]), {1});
      expect(await ids(before[1]), {1, 2});
    });

    test('a branch keeps min-snapshots-to-keep of its ancestors', () async {
      await build();
      final before = await snapshots();
      await edit((json) {
        json['refs'] = {
          ...(json['refs'] as Map? ?? const {}),
          'dev': {
            'snapshot-id': before[2],
            'type': 'branch',
            'min-snapshots-to-keep': 2,
          },
        };
      });
      expect(await IcebergWriter(tbl).expireSnapshots(retain: 1).ok, 1);
      expect(await snapshots(), before.sublist(1));
    });

    test('metadata-log keeps write.metadata.previous-versions-max', () async {
      await IcebergWriter(tbl).write(rows([0])).ok;
      await edit((json) {
        json['properties'] = {
          ...(json['properties'] as Map? ?? const {}),
          'write.metadata.previous-versions-max': '2',
        };
      });
      await build();
      final log = ((await current())['metadata-log'] as List).cast<Map>();
      expect(log, hasLength(2));
      // The newest entries are kept.
      final files = [for (final e in log) e['metadata-file'] as String];
      expect(files, [...files]..sort());
      await IcebergWriter(tbl).expireSnapshots(retain: 1).ok;
      expect((await current())['metadata-log'] as List, hasLength(2));
    });

    test(
      'published metadata never leaks the internal _version field',
      () async {
        await build();
        // Force a real expiry so a fresh metadata.json is published.
        await IcebergWriter(tbl).expireSnapshots(retain: 1).ok;
        final latest =
            (Directory('$tbl/metadata').listSync().map((f) => f.path).toList()
                  ..sort())
                .lastWhere((p) => p.endsWith('.metadata.json'));
        final json = jsonDecode(File(latest).readAsStringSync()) as Map;
        expect(json.containsKey('_version'), isFalse);
      },
    );

    test('expiring a table that does not exist fails loud', () async {
      await expectLater(
        IcebergWriter('${dir.path}/absent').expireSnapshots(retain: 1),
        failsWith(Kind.missing),
      );
    });

    test('expiry on a RELOCATED table deletes the expired data files', () async {
      // Iceberg records absolute URIs, so a table copied elsewhere still names
      // its old location. Manifest LISTS re-root via /metadata/, but DATA files
      // need /data/, which the writer's resolver did not handle, and the
      // writer's is what expireSnapshots uses to decide what is unreachable. An
      // OVERWRITE is what makes old data files genuinely unreachable once their
      // snapshot expires (an append keeps them live).
      await IcebergWriter(tbl).write(rows([1])).ok;
      await IcebergWriter(tbl).write(rows([2])).ok;
      await IcebergWriter(
        tbl,
      ).write(rows([9]), mode: IcebergWriteMode.overwrite).ok;

      final moved = Directory('${dir.path}/moved').absolute.path;
      Directory(tbl).renameSync(moved);
      expect(Directory('$moved/data').listSync().length, 3);

      expect(await IcebergWriter(moved).expireSnapshots(retain: 1).ok, 2);

      // The overwrite's file is the only one any retained snapshot reaches; the
      // other two are unreachable and must actually be gone, not orphaned.
      expect(
        Directory('$moved/data').listSync().length,
        1,
        reason: 'expired data files must be deleted, not orphaned',
      );
      final live = await (await DataFrameIO.readIceberg(moved).ok).execute().ok;
      expect(live.column('id')!.toList(), [9]);
    });

    test('an append after an expiry still reads every row', () async {
      await build();
      await IcebergWriter(tbl).expireSnapshots(retain: 1).ok;
      await IcebergWriter(tbl).write(rows([5])).ok;
      expect(await ids(), {1, 2, 3, 4, 5});
    });

    test('a missing manifest aborts expiry, fail closed (F4)', () async {
      await build();
      // Delete a manifest a snapshot reaches (not its manifest list). Its files'
      // reachability can no longer be PROVEN, so expiry must abort rather than
      // under-count `live` and delete a file a retained snapshot still uses.
      final meta = await current();
      final snap = (meta['snapshots'] as List)
          .cast<Map<String, dynamic>>()
          .last;
      final list = _strip(snap['manifest-list'] as String);
      final manifest = _strip(
        (await AvroOcfReader.readPathAsync(list)).first['manifest_path']
            as String,
      );
      expect(File(manifest).existsSync(), isTrue);
      File(manifest).deleteSync();

      await expectLater(
        IcebergWriter(tbl).expireSnapshots(retain: 2),
        failsWith(Kind.corrupt),
      );
    });

    test('will not delete a data file outside the table root (F1)', () async {
      // append then overwrite: the overwrite drops the first snapshot's manifest
      // from the current metadata, so once the first snapshot expires its files
      // are genuinely unreachable: the case expiry deletes.
      await IcebergWriter(tbl).write(rows([1])).ok;
      await IcebergWriter(
        tbl,
      ).write(rows([2]), mode: IcebergWriteMode.overwrite).ok;

      final meta = await current();
      final snap0 = (meta['snapshots'] as List)
          .cast<Map<String, dynamic>>()
          .first;
      final m0 = _strip(
        (await AvroOcfReader.readPathAsync(
              _strip(snap0['manifest-list'] as String),
            )).first['manifest_path']
            as String,
      );

      // A file OUTSIDE the table root that a tampered manifest names.
      final external = File('${dir.path}/external.keep')
        ..writeAsStringSync('secret');

      // Rewrite the expired snapshot's manifest so its data file points there:
      // the reviewer's repro of a crafted/corrupt manifest.
      final rec = Map<String, dynamic>.from(
        (await AvroOcfReader.readPathAsync(m0)).first,
      );
      final df = Map<String, dynamic>.from(rec['data_file'] as Map);
      df['file_path'] = 'file://${external.path}';
      rec['data_file'] = df;
      await AvroOcfWriter.writePathAsync(m0, manifestEntrySchema, [rec]);

      // Expiry must refuse to resolve (let alone delete) a file outside root.
      await expectLater(
        IcebergWriter(tbl).expireSnapshots(retain: 1),
        failsWith(Kind.denied),
      );
      expect(
        external.existsSync(),
        isTrue,
        reason: 'an external file must never be deleted by expiry',
      );

      // allowExternalPaths opts back into the old, unconfined behavior.
      await IcebergWriter(
        tbl,
        allowExternalPaths: true,
      ).expireSnapshots(retain: 1).ok;
      expect(external.existsSync(), isFalse);
    });
  });

  // A partitioned write emits one data file AND one manifest entry per distinct
  // key, so partitioning on a high-cardinality column turns one write into a
  // small-file catastrophe with the whole manifest held in memory. Nothing
  // bounded it (the 128 MiB metadata cap is read-side only).
  group('partition-count guard', () {
    late Directory pdir;
    setUp(() => pdir = Directory.systemTemp.createTempSync('icepart_'));
    tearDown(() => pdir.deleteSync(recursive: true));

    Table keyed(int rows) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(
          name: 'k',
          values: [for (var i = 0; i < rows; i++) i],
          nullable: false,
        ),
        NumericColumn.int64(
          name: 'v',
          values: [for (var i = 0; i < rows; i++) i * 2],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'k', dataType: DataType.int64, nullable: false),
          Field(name: 'v', dataType: DataType.int64, nullable: false),
        ],
      ),
    );

    test('too many partitions fails loud', () async {
      await expectLater(
        IcebergWriter(
          '${pdir.path}/t',
        ).write(keyed(20), partitionBy: ['k'], maxPartitions: 5),
        failsWith(Kind.limit),
      );
    });

    test('the error names the partition column and the cap', () async {
      await expectLater(
        IcebergWriter(
          '${pdir.path}/t2',
        ).write(keyed(20), partitionBy: ['k'], maxPartitions: 5),
        failsWith(Kind.limit, allOf(contains('k'), contains('5'))),
      );
    });

    test('a write within the cap succeeds', () async {
      final tbl = '${pdir.path}/t3';
      expect(
        await IcebergWriter(
          tbl,
        ).write(keyed(3), partitionBy: ['k'], maxPartitions: 5).ok,
        0,
      );
      expect((await IcebergTable(tbl).read().ok).rowCount, 3);
    });

    test('maxPartitions: null opts out', () async {
      final tbl = '${pdir.path}/t4';
      expect(
        await IcebergWriter(
          tbl,
        ).write(keyed(12), partitionBy: ['k'], maxPartitions: null).ok,
        0,
      );
      expect((await IcebergTable(tbl).read().ok).rowCount, 12);
    });

    test('an unpartitioned write is unaffected', () async {
      final tbl = '${pdir.path}/t5';
      await IcebergWriter(tbl).write(keyed(50)).ok;
      expect((await IcebergTable(tbl).read().ok).rowCount, 50);
    });
  });

  group('scan with partition and stats prune', () {
    late Directory dir;
    late String path;
    late SqlEngine engine;

    setUp(() {
      dir = Directory.systemTemp.createTempSync('iceberg_prune_');
      path = '${dir.path}/t';
      engine = SqlEngine.open(allowExternalPaths: true).ok;
    });
    tearDown(() async {
      await engine.dispose();
      dir.deleteSync(recursive: true);
    });

    Table rows(String part, List<int> ids) => Table.fromRows('t', [
      for (final id in ids) {'id': id, 'part': part, 'v': 'v$id'},
    ]);

    /// Three appends, one partition and one manifest each: a holds ids 0..9,
    /// b 10..19, c 20..29.
    Future<void> partitioned() async {
      for (final (i, part) in ['a', 'b', 'c'].indexed) {
        await IcebergWriter(path)
            .write(
              rows(part, [for (var id = i * 10; id < i * 10 + 10; id++) id]),
              partitionBy: ['part'],
            )
            .ok;
      }
    }

    Future<({List<Map<String, dynamic>> rows, int manifests, int files})> query(
      String sql,
    ) async {
      IcebergTable.manifestsRead = 0;
      ParquetReader.footersParsed = 0;
      final table = await engine.execute(sql).ok;
      return (
        rows: table.toRows(),
        manifests: IcebergTable.manifestsRead,
        files: ParquetReader.footersParsed,
      );
    }

    Future<({List<Map<String, dynamic>> rows, int manifests, int files})>
    select(String where) =>
        query("SELECT id, part, v FROM '$path' WHERE $where ORDER BY id");

    Future<List<Map<String, dynamic>>> eager(
      bool Function(Map row) where,
    ) async {
      final table = await IcebergTable(path).read().ok;
      final all = [
        for (final row in table.toRows())
          if (where(row)) {'id': row['id'], 'part': row['part'], 'v': row['v']},
      ];
      all.sort((x, y) => (x['id'] as int).compareTo(y['id'] as int));
      return all;
    }

    test('the manifest list records partition summaries', () async {
      await partitioned();
      final meta = await const FilesystemIcebergBackend().current(path).ok;
      final snapshot = (meta!['snapshots'] as List).last as Map;
      final list = await AvroOcfReader.readPathAsync(
        _strip(snapshot['manifest-list'] as String),
      );
      final bounds = [
        for (final manifest in list)
          for (final summary in manifest['partitions'] as List)
            [
              (summary as Map)['contains_null'],
              utf8.decode(summary['lower_bound'] as List<int>),
              utf8.decode(summary['upper_bound'] as List<int>),
            ],
      ]..sort((x, y) => '$x'.compareTo('$y'));
      expect(bounds, [
        [false, 'a', 'a'],
        [false, 'b', 'b'],
        [false, 'c', 'c'],
      ]);
    });

    test('a partition filter skips manifests and files', () async {
      await partitioned();
      final result = await select("part = 'a'");
      expect(result.manifests, 1);
      expect(result.files, 1);
      expect(result.rows, await eager((row) => row['part'] == 'a'));
      expect(result.rows, hasLength(10));
    });

    test('column bounds prune files whose range cannot match', () async {
      await partitioned();
      final result = await select('id >= 25');
      expect(result.manifests, 3);
      expect(result.files, 1);
      expect(result.rows, await eager((row) => (row['id'] as int) >= 25));
    });

    test('a filter nothing can match reads no manifest or file', () async {
      await partitioned();
      final result = await select("part = 'z'");
      expect(result.manifests, 0);
      expect(result.files, 0);
      expect(result.rows, isEmpty);
    });

    test('an unprunable filter reads every file and matches', () async {
      await partitioned();
      final result = await select("v <> 'v3'");
      expect(result.files, 3);
      expect(result.rows, await eager((row) => row['v'] != 'v3'));
    });

    test('readBatches reads only the requested columns', () async {
      await partitioned();
      final batches = await IcebergTable(
        path,
      ).readBatches(columns: ['v', 'part']).ok.toList();
      for (final batch in batches) {
        expect(batch.schema.fields.map((f) => f.name).toSet(), {'v', 'part'});
      }
      expect(
        [
          for (final b in batches)
            for (final row in b.toRows()) '${row['part']}${row['v']}',
        ]..sort(),
        [
          for (final (i, part) in ['a', 'b', 'c'].indexed)
            for (var id = i * 10; id < i * 10 + 10; id++) '${part}v$id',
        ]..sort(),
      );
    });

    test('a filter on an unselected column still filters', () async {
      await partitioned();
      final result = await query(
        "SELECT v FROM '$path' WHERE id >= 25 ORDER BY v",
      );
      expect(result.rows, [
        for (var id = 25; id < 30; id++) {'v': 'v$id'},
      ]);
    });

    test('a NaN row survives pruning for <>', () async {
      // The first file holds [5.0, NaN]; the second only 5.0, with a NaN
      // count of 0, so only the second can be skipped.
      for (final values in [
        [5.0, double.nan],
        [5.0, 5.0],
      ]) {
        await IcebergWriter(path)
            .write(
              Table.fromRows('t', [
                for (final value in values) {'x': value},
              ]),
            )
            .ok;
      }
      final result = await query("SELECT x FROM '$path' WHERE x <> 5");
      expect(result.files, 1);
      expect(result.rows, hasLength(1));
      expect((result.rows.single['x'] as double).isNaN, isTrue);
    });

    test('a manifest summary prunes <> only when it rules out NaN', () async {
      // Partitioned by x, each append writes one manifest whose summary for
      // x is [5, 5]; only the second records contains_nan false.
      for (final values in [
        [5.0, double.nan],
        [5.0, 5.0],
      ]) {
        await IcebergWriter(path)
            .write(
              Table.fromRows('t', [
                for (final (i, value) in values.indexed) {'id': i, 'x': value},
              ]),
              partitionBy: ['x'],
            )
            .ok;
      }
      final result = await query("SELECT x FROM '$path' WHERE x <> 5");
      expect(result.manifests, 1);
      expect(result.rows, hasLength(1));
      expect((result.rows.single['x'] as double).isNaN, isTrue);
    });

    test('a registered Iceberg table scans with pruning', () async {
      await partitioned();
      await engine.registerFile('i', path).ok;
      final result = await query(
        "SELECT id FROM i WHERE part = 'c' ORDER BY id",
      );
      expect(result.files, 1);
      expect(result.rows.map((row) => row['id']), [
        for (var id = 20; id < 30; id++) id,
      ]);
    });
  });
}

/// Iceberg read-path findings (#5 fail-closed metadata, #7 reader-catalog,
/// #8 global plan budget).
void icebergTests() {
  late Directory dir;
  late String tbl;
  setUp(() {
    dir = Directory.systemTemp.createTempSync('r47ice_');
    tbl = '${dir.path}/t';
  });
  tearDown(() => dir.deleteSync(recursive: true));

  Table sample() => Table(
    name: 't',
    columns: [
      NumericColumn.int64(name: 'id', values: [1, 2, 3], nullable: false),
    ],
    schema: Schema(
      fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
    ),
  );

  File latestMetadata() {
    final metas =
        Directory('$tbl/metadata')
            .listSync()
            .whereType<File>()
            .where((f) => f.path.endsWith('.metadata.json'))
            .toList()
          ..sort((a, b) => a.path.compareTo(b.path));
    return metas.last;
  }

  test('#5 a dangling current-snapshot-id fails closed', () async {
    await IcebergWriter(tbl).write(sample()).ok;
    final meta = latestMetadata();
    final json = jsonDecode(meta.readAsStringSync()) as Map<String, dynamic>;
    json['current-snapshot-id'] = 999999999; // matches no snapshot
    meta.writeAsStringSync(jsonEncode(json));
    await expectLater(IcebergTable(tbl).read(), failsWith(Kind.corrupt));
  });

  test('#7 the reader resolves through an injected catalog', () async {
    await IcebergWriter(tbl).write(sample()).ok;
    // A catalog that reports "no metadata" makes the reader fail as not-a-table,
    // proving the reader routes current-metadata resolution through the catalog.
    await expectLater(
      IcebergTable(tbl, catalog: const _EmptyCatalog()).read(),
      failsWith(Kind.missing),
    );
  });

  test('#8 a scan exceeding maxPlanEntries fails loud', () async {
    await IcebergWriter(tbl).write(sample()).ok;
    await expectLater(
      IcebergTable(tbl, maxPlanEntries: 0).read(),
      failsWith(Kind.limit),
    );
  });
}

Table _ids(List<int> ids) => Table(
  name: 't',
  schema: Schema(
    fields: [Field(name: 'id', dataType: DataType.int64)],
  ),
  columns: [ListColumn<int>(name: 'id', dataType: DataType.int64, data: ids)],
);

/// An Iceberg catalog that always reports no current metadata.
class _EmptyCatalog implements IcebergCatalog {
  const _EmptyCatalog();
  @override
  Future<Result<Map<String, dynamic>?, ErrorDetails>> current(
    String root, {
    bool allowOversized = false,
  }) async => const Ok(null);
}

/// A backend that PUBLISHES the commit for real (via the default filesystem
/// backend) and then returns [Kind.indeterminate], simulating a catalog
/// CAS that succeeded but whose response was lost to a timeout/reset.
final class _IndeterminateAfterPublish extends FilesystemIcebergCatalog
    implements IcebergBackend {
  const _IndeterminateAfterPublish();

  @override
  Future<Result<int, ErrorDetails>> commit({
    required String root,
    required Future<IcebergMetadataPublish> Function(
      Map<String, dynamic>? previous,
      int version,
    )
    build,
    bool allowOversized = false,
  }) async {
    await const FilesystemIcebergBackend()
        .commit(root: root, build: build, allowOversized: allowOversized)
        .ok;
    return Err(
      IcebergCommitIndeterminate(root, 'simulated lost CAS response').details,
    );
  }
}
