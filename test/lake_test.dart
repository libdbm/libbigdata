import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/formats/delta/actions.dart';
import 'package:libbigdata/src/formats/delta/deletion_vector.dart';
import 'package:libbigdata/src/formats/iceberg/iceberg_catalog.dart';
import 'package:libbigdata/src/formats/scan.dart';
import 'package:test/test.dart';

import 'support/results.dart';

/// Cross-format lake concerns that span Delta AND Iceberg: read caps and
/// identity partitioning.
void main() {
  // Delta/Iceberg reads cap total materialized rows/bytes across files (per-file
  // Parquet caps don't bound a table of many files), and expose bounded-memory
  // streaming via readBatches.
  group('read caps', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('caps_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table t(List<int> ids) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: ids, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    test('Delta read caps total rows across files; null opts out', () async {
      final tbl = '${dir.path}/d';
      // Two data files (one per partition) → 4 rows total.
      await DeltaWriter(tbl)
          .write(
            Table(
              name: 't',
              columns: [
                NumericColumn.int64(
                  name: 'id',
                  values: [1, 2, 3, 4],
                  nullable: false,
                ),
                ListColumn<String>(
                  name: 'p',
                  dataType: DataType.string,
                  data: ['a', 'a', 'b', 'b'],
                ),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'id', dataType: DataType.int64, nullable: false),
                  Field(name: 'p', dataType: DataType.string),
                ],
              ),
            ),
            partitionBy: ['p'],
          )
          .ok;
      expect(DeltaTable(tbl).read(maxRows: 1), failsWith(Kind.limit));
      final full = await DeltaTable(tbl).read(maxRows: null).ok;
      expect(full.rowCount, 4);
      // readBatches streams without the cap (one file at a time).
      final batches = await DeltaTable(tbl).readBatches().ok.toList();
      expect(batches.length, greaterThanOrEqualTo(2));
      expect(batches.fold<int>(0, (n, b) => n + b.rowCount), 4);
    });

    test('Iceberg read caps total rows; null opts out', () async {
      final tbl = '${dir.path}/i';
      await DataFrame.fromTable(t([1, 2, 3])).writeIceberg(tbl).ok;
      await DataFrame.fromTable(t([4, 5, 6])).writeIceberg(tbl).ok;
      expect(IcebergTable(tbl).read(maxRows: 1), failsWith(Kind.limit));
      final full = await IcebergTable(tbl).read(maxRows: null).ok;
      expect(full.rowCount, 6);
    });
  });

  // Partitioned Delta + Iceberg writes (identity partitions) round-trip through
  // our own readers; externally validated against delta-rs/pyiceberg in the
  // interop suite.
  group('partitioning', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('part_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table t() => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: [1, 2, 3, 4], nullable: false),
        ListColumn<String>(
          name: 'region',
          dataType: DataType.string,
          data: ['us', 'eu', 'us', 'eu'],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'region', dataType: DataType.string, nullable: false),
        ],
      ),
    );

    Future<Map<int, String>> rows(DataFrame df) async {
      final t = await df.execute().ok;
      return {
        for (final r in t.toRows()) r['id'] as int: r['region'] as String,
      };
    }

    test(
      'Delta partitioned write → Hive dirs + read reconstructs region',
      () async {
        final tbl = '${dir.path}/d';
        await DeltaWriter(tbl).write(t(), partitionBy: ['region']).ok;
        // Hive-style partition directories, partition column not in the data file.
        expect(Directory('$tbl/region=us').existsSync(), isTrue);
        expect(Directory('$tbl/region=eu').existsSync(), isTrue);
        expect(await rows(await DataFrameIO.readDelta(tbl).ok), {
          1: 'us',
          2: 'eu',
          3: 'us',
          4: 'eu',
        });
      },
    );

    test(
      'Iceberg partitioned write → spec + read keeps region in-file',
      () async {
        final tbl = '${dir.path}/i';
        await IcebergWriter(tbl).write(t(), partitionBy: ['region']).ok;
        expect(await rows(await DataFrameIO.readIceberg(tbl).ok), {
          1: 'us',
          2: 'eu',
          3: 'us',
          4: 'eu',
        });
      },
    );
  });

  // Delta + Iceberg commit through ONE shared local CAS registry (round 52 #4).
  group('#4 shared local CAS registry', () {
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

    test('different table keys commit concurrently (finding 13)', () async {
      final catalog = LocalCatalog('${dir.path}/shard.json');
      final aEntered = Completer<void>();
      final release = Completer<void>();
      // A holds its per-key lock across a blocked body.
      final a = catalog.guarded('a', (txn) async {
        aEntered.complete();
        await release.future;
        txn.put('a', version: 1);
      });
      await aEntered.future;
      // B (a DIFFERENT key) must not block on A's held key lock, with the old
      // single global lock it would, and this times out.
      await catalog
          .guarded('b', (txn) async => txn.put('b', version: 2))
          .timeout(const Duration(seconds: 3));
      expect((await catalog.lookup('b').ok)!['version'], 2);
      release.complete();
      await a;
      expect((await catalog.lookup('a').ok)!['version'], 1);
    });

    test('a failed registry write leaves no temp (finding 13)', () async {
      final path = '${dir.path}/reg.json';
      // Put a DIRECTORY at the registry path so the publish rename fails.
      Directory(path).createSync(recursive: true);
      final catalog = LocalCatalog(path);
      await expectLater(
        catalog.guarded('k', (txn) async => txn.put('k', version: 1)),
        failsWith(Kind.io),
      );
      final temps = Directory(
        dir.path,
      ).listSync().whereType<File>().where((f) => f.path.endsWith('.tmp'));
      expect(temps, isEmpty);
    });

    // A registry that exists but can't be read is NOT an empty registry: one
    // file backs every Delta AND Iceberg table registered here, so silently
    // treating it as fresh would re-register each at version 0 and publish a
    // stale current-pointer for all of them.
    group('a corrupt registry fails closed', () {
      for (final (what, content) in [
        ('a zero-length', ''),
        ('a truncated', '{"a": {"version": 1}'),
        ('a non-object', '[1, 2, 3]'),
      ]) {
        test('$what registry names itself and what to do', () async {
          final path = '${dir.path}/catalog.json';
          File(path).writeAsStringSync(content);
          await expectLater(
            LocalCatalog(path).lookup('a'),
            failsWith(
              Kind.corrupt,
              allOf(contains(path), contains('Restore it')),
            ),
          );
        });
      }

      test('a missing registry is still a fresh one', () async {
        final catalog = LocalCatalog('${dir.path}/absent.json');
        expect(await catalog.lookup('a').ok, isNull);
        await catalog.guarded('a', (txn) async => txn.put('a', version: 3)).ok;
        expect((await catalog.lookup('a').ok)!['version'], 3);
      });

      test('a registry lost to a crashed Windows swap is recovered', () async {
        // dest missing, the only intact copy at the backup: guarded() heals it
        // before reading, so the recorded pointers are not lost as "fresh empty".
        final path = '${dir.path}/registry.json';
        File('$path.bak-tok').writeAsStringSync('{"a": {"version": 5}}');
        final catalog = LocalCatalog(path);
        final version = await catalog
            .guarded('a', (txn) async => txn.entry('a')?['version'])
            .ok;
        expect(version, 5);
        expect(File('$path.bak-tok').existsSync(), isFalse);
      });
    });

    test('Delta + Iceberg commit through one registry; O(1) current', () async {
      final registry = LocalCatalog('${dir.path}/catalog.json');

      // Delta via the registry coordinator.
      final dpath = '${dir.path}/d';
      await DeltaWriter(
        dpath,
        coordinator: LocalCatalogDeltaCommitCoordinator(registry),
      ).write(intTable('id', [1])).ok;
      await DeltaWriter(
        dpath,
        coordinator: LocalCatalogDeltaCommitCoordinator(registry),
      ).write(intTable('id', [2])).ok;
      final back = await DeltaTable(dpath).read().ok;
      expect({for (final r in back.toRows()) r['id'] as int}, {1, 2});

      // Iceberg via ONE registry backend: it owns both the O(1) registry
      // current-pointer read and the registry-fenced commit CAS, so read and
      // write cannot resolve different pointers.
      final ipath = Directory('${dir.path}/i').absolute.path;
      final backend = LocalCatalogIcebergBackend(registry);
      await IcebergWriter(
        ipath,
        backend: backend,
      ).write(intTable('id', [1])).ok;
      await IcebergWriter(
        ipath,
        backend: backend,
      ).write(intTable('id', [2])).ok;
      // The backend resolves current from its registry pointer (no dir scan).
      final cur = await backend.current(ipath).ok;
      expect(cur, isNotNull);
      expect((cur!['snapshots'] as List).length, 2);
      // The registry recorded both tables' current versions.
      expect((await registry.lookup(dpath).ok)!['version'], 1);
      expect((await registry.lookup(ipath).ok)!['pointer'], isNotNull);
    });
  });

  group('lake scan details', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('lake_scan_'));
    tearDown(() => dir.deleteSync(recursive: true));

    /// Five rows of a small id and a wide text column.
    Table wide() => Table(
      name: 't',
      columns: [
        NumericColumn.int64(
          name: 'id',
          values: [1, 2, 3, 4, 5],
          nullable: false,
        ),
        ListColumn<String>(
          name: 'text',
          dataType: DataType.string,
          data: [for (var i = 0; i < 5; i++) 'x' * 10000],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'text', dataType: DataType.string, nullable: false),
        ],
      ),
    );

    Future<Map<String, String>> tables() async {
      final delta = '${dir.path}/delta';
      final iceberg = '${dir.path}/iceberg';
      await DeltaWriter(delta).write(wide()).ok;
      await IcebergWriter(iceberg).write(wide()).ok;
      return {'Delta': delta, 'Iceberg': iceberg};
    }

    test('read projects before the byte cap applies', () async {
      for (final MapEntry(key: format, value: path)
          in (await tables()).entries) {
        final t = await openScan(
          path,
        ).read(projection: ['id'], maxBytes: 10000);
        expect(t.schema.fields.map((f) => f.name), ['id'], reason: format);
      }
    });

    test('Iceberg batches honour the batch size', () async {
      final path = (await tables())['Iceberg']!;
      final sizes = [
        await for (final batch in openScan(path).batches(size: 2))
          batch.rowCount,
      ];
      expect(sizes, [2, 2, 1]);
    });

    test('an unknown projected column is invalid', () async {
      final paths = await tables();
      await expectLater(
        DeltaTable(paths['Delta']!).readBatches(columns: ['nope']).first,
        completion(
          isA<Err>().having((e) => e.error.kind, 'kind', Kind.invalid),
        ),
      );
      await expectLater(
        IcebergTable(paths['Iceberg']!).readBatches(columns: ['nope']).first,
        completion(
          isA<Err>().having((e) => e.error.kind, 'kind', Kind.invalid),
        ),
      );
    });

    test('a relocated Iceberg path re-roots at its last /data/', () async {
      final root = '${dir.path}/data/t';
      File('$root/data/f.parquet').createSync(recursive: true);
      expect(
        await resolveIcebergPath(root, '/elsewhere/data/t/data/f.parquet'),
        '$root/data/f.parquet',
      );
    });

    test('a short deletion-vector path is corrupt', () {
      expect(
        () => DeletionVector.absolutePath(
          dir.path,
          DeletionVectorDescriptor(
            storageType: 'u',
            pathOrInlineDv: 'abc',
            sizeInBytes: 1,
            cardinality: 1,
          ),
        ),
        throwsFailure(Kind.corrupt),
      );
    });

    test('the Iceberg writer refuses a dangling current-schema-id', () async {
      final path = (await tables())['Iceberg']!;
      final latest =
          (Directory('$path/metadata').listSync().map((f) => f.path).toList()
                ..sort())
              .lastWhere((p) => p.endsWith('.metadata.json'));
      final json =
          jsonDecode(File(latest).readAsStringSync()) as Map<String, dynamic>;
      json['current-schema-id'] = 99;
      File(latest).writeAsStringSync(jsonEncode(json));
      await expectLater(
        IcebergWriter(path).write(wide()),
        failsWith(Kind.corrupt, contains('current-schema-id 99')),
      );
    });
  });
}
