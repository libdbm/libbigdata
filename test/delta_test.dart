import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/core/paths.dart';
import 'package:libbigdata/src/formats/delta/actions.dart';
import 'package:libbigdata/src/formats/delta/deletion_vector.dart';
import 'package:libbigdata/src/formats/delta/delta_log.dart';
import 'package:libbigdata/src/formats/delta/delta_schema.dart';
import 'package:libbigdata/src/formats/delta/roaring_bitmap.dart';
import 'package:test/test.dart';

void main() {
  group('core: write / read / time-travel', () {
    Table ids(List<int> ids) => Table(
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

    group('Roaring bitmap portable round-trip', () {
      void roundTrip(Set<int> values, String reason) {
        final bytes = Roaring.serializeTreemap(values);
        expect(
          Roaring.deserializeTreemap(bytes),
          equals(values),
          reason: reason,
        );
      }

      test('sparse (array container)', () {
        roundTrip({0, 5, 7, 100, 65535}, 'sparse');
      });
      test('dense (bitmap container, > 4096 in one container)', () {
        roundTrip({for (int i = 0; i < 5000; i++) i * 2}, 'dense');
      });
      test('multi-container (spans 16-bit keys)', () {
        roundTrip({1, 70000, 140000, 1 << 20}, 'multi-container');
      });
      test('64-bit spanning (multiple treemap buckets)', () {
        roundTrip({5, (1 << 32) + 7, (1 << 33) + 9}, '64-bit');
      });
      test('empty', () {
        roundTrip(<int>{}, 'empty');
      });
    });

    group('Delta write / read', () {
      late Directory dir;
      late String path;
      setUp(() {
        dir = Directory.systemTemp.createTempSync('delta_');
        path = '${dir.path}/tbl';
      });
      tearDown(() => dir.deleteSync(recursive: true));

      test('create then read round-trips', () async {
        await DeltaWriter(path).write(ids([1, 2, 3]));
        final t = await (await DataFrameIO.readDelta(path)).execute();
        expect(t.rowCount, 3);
        expect(t.column('id')!.toList(), [1, 2, 3]);
        expect(t.column('name')!.toList(), ['n1', 'n2', 'n3']);
        expect(t.column('id'), isA<NumericColumn>());
      });

      test('append accumulates files; time travel sees each version', () async {
        final w = DeltaWriter(path);
        await w.write(ids([1, 2])); // v0
        await w.write(ids([3, 4])); // v1 (append)

        final latest = await (await DataFrameIO.readDelta(path)).execute();
        expect(latest.rowCount, 4);
        expect((latest.column('id')!.toList().cast<int>()..sort()), [
          1,
          2,
          3,
          4,
        ]);

        final v0 = await (await DataFrameIO.readDelta(
          path,
          version: 0,
        )).execute();
        expect(v0.rowCount, 2);
        expect(v0.column('id')!.toList(), [1, 2]);
      });

      test('overwrite replaces all data', () async {
        final w = DeltaWriter(path);
        await w.write(ids([1, 2, 3]));
        await w.write(ids([9]), mode: DeltaWriteMode.overwrite);
        final t = await (await DataFrameIO.readDelta(path)).execute();
        expect(t.column('id')!.toList(), [9]);
      });
    });

    group('Delta delete via deletion vectors', () {
      late Directory dir;
      late String path;
      setUp(() {
        dir = Directory.systemTemp.createTempSync('delta_dv_');
        path = '${dir.path}/tbl';
      });
      tearDown(() => dir.deleteSync(recursive: true));

      test(
        'delete marks rows; read excludes them; time travel restores',
        () async {
          final w = DeltaWriter(path);
          await w.write(ids(List.generate(1000, (i) => i))); // v0
          final v = await w.delete(
            (row) => (row['id'] as int) >= 500,
          ); // v1, DV
          expect(v, 1);

          final after = await (await DataFrameIO.readDelta(path)).execute();
          expect(after.rowCount, 500);
          final vals = after.column('id')!.toList().cast<int>();
          expect(vals.first, 0);
          expect(vals.last, 499);

          // Time travel to before the delete still sees all rows.
          final before = await (await DataFrameIO.readDelta(
            path,
            version: 0,
          )).execute();
          expect(before.rowCount, 1000);
        },
      );

      test(
        'a second delete merges with the existing deletion vector',
        () async {
          final w = DeltaWriter(path);
          await w.write(ids(List.generate(10, (i) => i))); // v0
          await w.delete((row) => (row['id'] as int) == 0); // v1
          await w.delete((row) => (row['id'] as int) == 9); // v2 (merges)
          final t = await (await DataFrameIO.readDelta(path)).execute();
          expect(t.column('id')!.toList(), [1, 2, 3, 4, 5, 6, 7, 8]);
        },
      );
    });
  });

  group('transactions & schema validation', () {
    Table t(List<int> ids, [List<String>? names]) => Table(
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

    Table wide(List<int> ids) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: ids, nullable: false),
        NumericColumn.int64(name: 'extra', values: ids, nullable: false),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'extra', dataType: DataType.int64, nullable: false),
        ],
      ),
    );

    late Directory dir;
    late String tbl;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('delta_txn_');
      tbl = '${dir.path}/tbl';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    group('append schema validation', () {
      test('a matching-schema append succeeds', () async {
        await DeltaWriter(tbl).write(t([0, 1]));
        await DeltaWriter(tbl).write(t([2, 3]));
        expect((await DeltaTable(tbl).read()).rowCount, 4);
      });

      test('a mismatched-schema append throws', () async {
        await DeltaWriter(tbl).write(t([0, 1]));
        await expectLater(
          DeltaWriter(tbl).write(wide([2])),
          throwsA(isA<StateError>()),
        );
        expect((await DeltaTable(tbl).read()).rowCount, 2); // unchanged
      });

      test(
        'an append revalidates schema against the latest snapshot at commit time',
        () async {
          await DeltaWriter(tbl).write(t([0, 1])); // v0, schema {id,name}

          // Simulate an EXTERNAL writer evolving the schema (a Metadata-only
          // commit at v1). The append below claims v2, so it must revalidate
          // against THIS latest snapshot — not the schema it saw earlier.
          await DeltaLog(tbl).commit(1, [
            Metadata(
              id: 'external',
              schemaString: DeltaSchema.toJson(wide([0]).schema),
              createdTime: 0,
            ).toJson(),
          ]);

          await expectLater(
            DeltaWriter(tbl).write(t([2, 3])), // still schema {id,name}
            throwsA(isA<StateError>()),
          );

          // The rejected append must leave no orphaned data file behind (only
          // v0's part remains).
          final parts = Directory(tbl)
              .listSync()
              .whereType<File>()
              .where((f) => f.path.endsWith('.parquet'))
              .length;
          expect(parts, 1);
        },
      );
    });

    group('mutation conflict detection', () {
      // Two concurrent MERGEs that each insert the same not-yet-present key must
      // never both commit it: whichever reads a stale basis aborts, and if they
      // serialize the second sees the key and UPDATEs. Either way the key is
      // present exactly once (without the conflict check, a race duplicates it).
      test('concurrent upserts of the same key never duplicate it', () async {
        await DeltaWriter(tbl).write(t([0, 1, 2]));
        final source = t([9], ['s9']);
        Future<int?> upsert() => DeltaWriter(tbl)
            .merge(source, on: ['id'], whenNotMatchedInsert: true)
            .catchError((e) => e is StateError ? -1 : throw e);

        await Future.wait([upsert(), upsert()]);

        final rows = (await DeltaTable(tbl).read()).toRows();
        expect(rows.where((r) => r['id'] == 9).length, 1);
        expect(rows.length, 4); // 0,1,2 + a single 9
      });
    });

    group('0-byte (crashed) commit recovery', () {
      // Simulate a writer that claimed a version (create exclusive) then was
      // SIGKILLed before publishing content via temp+rename.
      void crashClaim(int version) {
        File(
          '$tbl/_delta_log/${DeltaLog.commitFileName(version)}',
        ).createSync();
      }

      test('a trailing 0-byte commit is ignored on read', () async {
        await DeltaWriter(tbl).write(t([0, 1])); // v0
        crashClaim(1); // 0-byte v1
        final rows = (await DeltaTable(tbl).read()).toRows();
        expect(rows.map((r) => r['id']).toList()..sort(), [
          0,
          1,
        ]); // v1 invisible
      });

      test('a fresh 0-byte tail is contended, not advanced past', () async {
        await DeltaWriter(tbl).write(t([0, 1])); // v0
        // A fresh claim is indistinguishable from a live in-flight commit;
        // advancing to v2 would gap the log and commit against stale state, so
        // the writer contends and fails rather than skipping it.
        crashClaim(1);
        await expectLater(
          DeltaWriter(tbl, maxRetries: 2).write(t([2])),
          throwsA(isA<StateError>()),
        );
      });

      test('the table self-heals once a stale crash orphan ages out', () async {
        await DeltaWriter(tbl).write(t([0, 1])); // v0
        crashClaim(1);
        // Age the orphan past the reclaim window (a real crash), so the next
        // write reclaims v1 and appends contiguously.
        final claim = '$tbl/_delta_log/${DeltaLog.commitFileName(1)}';
        final r = await Process.run('touch', ['-t', '202001010000', claim]);
        expect(r.exitCode, 0, reason: 'touch failed: ${r.stderr}');
        await DeltaWriter(tbl).write(t([2]));
        final rows = (await DeltaTable(tbl).read()).toRows();
        expect(rows.map((r) => r['id']).toList()..sort(), [0, 1, 2]);
      });

      test('explicitly reading an incomplete version throws', () async {
        await DeltaWriter(tbl).write(t([0])); // v0
        crashClaim(1);
        await expectLater(
          DeltaTable(tbl).read(version: 1),
          throwsA(isA<StateError>()),
        );
      });

      test(
        'checkpoint sweeps a stale 0-byte orphan but keeps a recent claim',
        () async {
          await DeltaWriter(tbl).write(t([0, 1])); // v0
          crashClaim(1); // will be backdated → stale, should be swept
          crashClaim(2); // fresh → within TTL, must be preserved
          final stale = '$tbl/_delta_log/${DeltaLog.commitFileName(1)}';
          final recent = '$tbl/_delta_log/${DeltaLog.commitFileName(2)}';
          // Backdate the stale claim well past the 15-minute TTL.
          final r = await Process.run('touch', ['-t', '202001010000', stale]);
          expect(r.exitCode, 0, reason: 'touch failed: ${r.stderr}');

          await DeltaLog(tbl).checkpoint(0);

          expect(File(stale).existsSync(), isFalse); // stale orphan swept
          expect(File(recent).existsSync(), isTrue); // recent claim untouched
        },
      );
    });
  });

  group('mutations (update/merge)', () {
    Table t(List<int> ids, [List<String>? names]) => Table(
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

    Future<Map<int, String?>> read(String path, {int? version}) async {
      final t = await (await DataFrameIO.readDelta(
        path,
        version: version,
      )).execute();
      return {for (final r in t.toRows()) r['id'] as int: r['name'] as String?};
    }

    late Directory dir;
    late String path;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('delta_mut_');
      path = '${dir.path}/tbl';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    test('the default writer is fenced (holds the commit lock)', () async {
      await DeltaWriter(
        path,
      ).write(t([1, 2])); // default = FileLock coordinator
      expect(await read(path), {1: 'n1', 2: 'n2'});
      expect(File('$path/_delta_log/_commit.lock').existsSync(), isTrue);
    });

    group('DeltaWriter.update', () {
      test(
        'updates matching rows; time travel preserves the original',
        () async {
          final w = DeltaWriter(path);
          await w.write(t([0, 1, 2, 3, 4])); // v0
          final v = await w.update((r) => (r['id'] as int) >= 3, {
            'name': (r) => 'X',
          });
          expect(v, 1);

          final after = await read(path);
          expect(after, {0: 'n0', 1: 'n1', 2: 'n2', 3: 'X', 4: 'X'});
          final before = await read(path, version: 0);
          expect(before[3], 'n3');
        },
      );

      test('no match is a no-op (-1)', () async {
        final w = DeltaWriter(path);
        await w.write(t([0, 1, 2]));
        expect(
          await w.update((r) => (r['id'] as int) > 100, {'name': (r) => 'X'}),
          -1,
        );
      });

      test('the predicate is evaluated exactly once per live row', () async {
        // A single pass caches each row's decision so a non-deterministic
        // predicate can't decide differently between the "is this file affected"
        // scan and the rewrite (the old two-pass design ran it twice).
        final w = DeltaWriter(path);
        await w.write(t([0, 1, 2, 3, 4]));
        final calls = <int, int>{};
        await w.update((r) {
          final id = r['id'] as int;
          calls[id] = (calls[id] ?? 0) + 1;
          return id >= 3;
        }, {'name': (r) => 'X'});
        expect(calls, {0: 1, 1: 1, 2: 1, 3: 1, 4: 1});
      });

      test('a no-match update leaves the data files byte-identical', () async {
        final w = DeltaWriter(path);
        await w.write(t([0, 1, 2]));
        Map<String, List<int>> dataFiles() => {
          for (final f in Directory(path).listSync().whereType<File>())
            if (f.path.endsWith('.parquet')) f.path: f.readAsBytesSync(),
        };
        final before = dataFiles();
        await w.update((r) => (r['id'] as int) > 100, {'name': (r) => 'X'});
        final after = dataFiles();
        expect(after.keys.toSet(), before.keys.toSet());
        for (final entry in before.entries) {
          expect(after[entry.key], entry.value); // unaffected file untouched
        }
      });

      test('update applies on top of a deletion vector', () async {
        final w = DeltaWriter(path);
        await w.write(t([0, 1, 2, 3, 4]));
        await w.delete((r) => (r['id'] as int) == 2); // DV removes id 2
        await w.update((r) => (r['id'] as int) >= 3, {'name': (r) => 'X'});
        expect(await read(path), {0: 'n0', 1: 'n1', 3: 'X', 4: 'X'});
      });

      test(
        'predicate fires once per live row across MULTIPLE batches + a DV',
        () async {
          // The cached-decision bitmap is keyed by live-row ordinal; with the
          // stream spanning several readBatches AND a deletion vector filtering
          // rows out, a misaligned index would update the WRONG rows or double-
          // evaluate. >10000 rows forces multiple batches in _liveStream.
          final w = DeltaWriter(path);
          await w.write(t([for (var i = 0; i < 25000; i++) i]));
          await w.delete((r) => (r['id'] as int) % 100 == 0); // 250 deleted
          final calls = <int, int>{};
          await w.update((r) {
            final id = r['id'] as int;
            calls[id] = (calls[id] ?? 0) + 1;
            return id % 3 == 0;
          }, {'name': (r) => 'X'});

          // Exactly the 24750 LIVE rows, each evaluated exactly once.
          expect(calls.length, 24750);
          expect(calls.values.every((c) => c == 1), isTrue);
          expect(
            calls.keys.any((id) => id % 100 == 0),
            isFalse,
          ); // DV rows never seen

          final rows = await read(path);
          expect(rows.length, 24750);
          expect(rows[3], 'X'); // matched + live
          expect(rows[1], 'n1'); // unmatched + live
          expect(
            rows.containsKey(300),
            isFalse,
          ); // deleted (and would've matched)
        },
      );

      test(
        'rejects update on a partitioned table (would null partitions)',
        () async {
          final w = DeltaWriter(path);
          await w.write(
            _regioned([1, 2, 3], ['US', 'US', 'EU']),
            partitionBy: ['region'],
          );
          // The copy-on-write rewrite would null-fill the region column for the
          // whole rewritten partition; fail loud instead of corrupting it.
          await expectLater(
            w.update((r) => (r['id'] as int) == 1, {'name': (r) => 'X'}),
            throwsA(isA<UnsupportedError>()),
          );
        },
      );
    });

    group('DeltaWriter.merge', () {
      test('upsert: update matched + insert not-matched', () async {
        final w = DeltaWriter(path);
        await w.write(t([0, 1, 2, 3, 4])); // v0
        final src = t([3, 5], ['s3', 's5']);
        await w.merge(
          src,
          on: ['id'],
          whenMatchedUpdate: {'name': (t, s) => s['name']},
          whenNotMatchedInsert: true,
        );
        expect(await read(path), {
          0: 'n0',
          1: 'n1',
          2: 'n2',
          3: 's3',
          4: 'n4',
          5: 's5',
        });
      });

      test('matched delete + no insert', () async {
        final w = DeltaWriter(path);
        await w.write(t([0, 1, 2, 3, 4]));
        final src = t([2, 3], ['x', 'y']);
        await w.merge(
          src,
          on: ['id'],
          whenMatchedDelete: true,
          whenNotMatchedInsert: false,
        );
        expect((await read(path)).keys.toList()..sort(), [0, 1, 4]);
      });

      test('insert-only when nothing matches', () async {
        final w = DeltaWriter(path);
        await w.write(t([0, 1]));
        await w.merge(t([7, 8], ['s7', 's8']), on: ['id']);
        expect(await read(path), {0: 'n0', 1: 'n1', 7: 's7', 8: 's8'});
      });

      test(
        'rejects merge on a partitioned table (would null partitions)',
        () async {
          final w = DeltaWriter(path);
          await w.write(
            _regioned([1, 2, 3], ['US', 'US', 'EU']),
            partitionBy: ['region'],
          );
          await expectLater(
            w.merge(
              _regioned([1], ['US']),
              on: ['id'],
              whenMatchedUpdate: {'name': (t, s) => s['name']},
            ),
            throwsA(isA<UnsupportedError>()),
          );
        },
      );
    });

    // The streamed mutation paths read a parquet file one batch (~10k rows) at a
    // time; these exercise predicates/rewrites that cross a batch boundary so the
    // physical-row indexing the deletion vector relies on stays aligned.
    group('streamed mutations across batch boundaries (#8)', () {
      test(
        'delete spanning a batch boundary excludes exactly the right rows',
        () async {
          final w = DeltaWriter(path);
          await w.write(t([for (int i = 0; i < 15000; i++) i]));
          await w.delete(
            (r) => (r['id'] as int) >= 9000 && (r['id'] as int) < 11000,
          );
          final rows = await read(path);
          expect(rows.length, 13000);
          expect(rows.containsKey(8999), isTrue);
          expect(rows.containsKey(9000), isFalse);
          expect(rows.containsKey(10999), isFalse);
          expect(rows.containsKey(11000), isTrue);
        },
      );

      test(
        'update across a batch boundary rewrites exactly the matching rows',
        () async {
          final w = DeltaWriter(path);
          await w.write(t([for (int i = 0; i < 15000; i++) i]));
          final v = await w.update(
            (r) => (r['id'] as int) >= 9500 && (r['id'] as int) < 10500,
            {'name': (r) => 'X'},
          );
          expect(v, 1);
          final rows = await read(path);
          expect(rows.length, 15000);
          expect(rows[9499], 'n9499');
          expect(rows[9500], 'X');
          expect(rows[10499], 'X');
          expect(rows[10500], 'n10500');
        },
      );

      test(
        'merge delete that empties a file tombstones with no replacement',
        () async {
          final w = DeltaWriter(path);
          await w.write(t([1, 2, 3]));
          await w.merge(
            t([1, 2, 3]),
            on: ['id'],
            whenMatchedDelete: true,
            whenNotMatchedInsert: false,
          );
          // Empty result means the original file was tombstoned with no replacement.
          expect(await read(path), isEmpty);
        },
      );
    });
  });

  group('robustness', () {
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

    Future<Set<int>> readIds(String path, {int? version}) async {
      final t = await (await DataFrameIO.readDelta(
        path,
        version: version,
      )).execute();
      return {for (final r in t.toRows()) r['id'] as int};
    }

    late Directory dir;
    late String path;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('delta_rob_');
      path = '${dir.path}/tbl';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    group('concurrency- and crash-safe commits (#1)', () {
      test(
        'commit throws CommitConflict when the version is already claimed',
        () async {
          await DeltaWriter(path).write(t([0])); // v0 exists
          final log = DeltaLog(path);
          // Version 0 is taken: a second claim must conflict, not clobber.
          await expectLater(
            log.commit(0, const []),
            throwsA(isA<CommitConflict>()),
          );
        },
      );

      test(
        'the FileLock coordinator honors its lock timeout (finding 4)',
        () async {
          final log = DeltaLog(path);
          await Directory(log.logDir).create(recursive: true);
          final entered = Completer<void>();
          final release = Completer<void>();
          // Hold the commit lock (via the in-isolate mutex) until released.
          final holder = withFileLock('${log.logDir}/_commit.lock', () async {
            entered.complete();
            await release.future;
          });
          await entered.future;
          const coordinator = FileLockDeltaCommitCoordinator(
            timeout: Duration(milliseconds: 50),
          );
          // With the lock held, the coordinator fails loud instead of hanging
          // forever — its build never runs.
          await expectLater(
            coordinator.commit(
              log: log,
              label: 'x',
              build: (v) async => const [],
            ),
            throwsA(isA<TimeoutException>()),
          );
          release.complete();
          await holder;
        },
      );

      test('a failed checkpoint leaves no temp file (finding 9)', () async {
        await DeltaWriter(path).write(t([0, 1])); // v0
        final log = DeltaLog(path);
        // Block the checkpoint's final rename by putting a DIRECTORY at the
        // checkpoint path, so replaceAtomic(temp, checkpointPath) throws.
        Directory(log.checkpointPath(0)).createSync(recursive: true);
        await expectLater(log.checkpoint(0), throwsA(anything));
        final temps = Directory(
          log.logDir,
        ).listSync().whereType<File>().where((f) => f.path.endsWith('.tmp'));
        expect(temps, isEmpty);
      });

      test('a terminal commit failure leaves no orphan data file', () async {
        // Block version 0 with a *directory* at its commit path: the exclusive
        // create fails with a non-conflict FileSystemException.
        Directory(
          '$path/_delta_log/${DeltaLog.commitFileName(0)}',
        ).createSync(recursive: true);

        await expectLater(
          DeltaWriter(path).write(t([1, 2, 3])),
          throwsA(isA<Exception>()),
        );

        final orphans = Directory(path).listSync().whereType<File>().where(
          (f) => f.path.endsWith('.parquet'),
        );
        expect(orphans, isEmpty);
        // ... and no stranded commit temp either: nothing ever sweeps
        // `_delta_log/*.tmp`, so a leak there is permanent (external review).
        final temps = Directory(
          '$path/_delta_log',
        ).listSync().whereType<File>().where((f) => f.path.endsWith('.tmp'));
        expect(temps, isEmpty);
      });

      test('commits and mutations leave no _delta_log temp files', () async {
        await DeltaWriter(path).write(t([1, 2, 3]));
        await DeltaWriter(path).delete((r) => r['id'] == 2);
        await DeltaWriter(path).write(t([4]), mode: DeltaWriteMode.overwrite);
        final temps = Directory(
          '$path/_delta_log',
        ).listSync().whereType<File>().where((f) => f.path.endsWith('.tmp'));
        expect(temps, isEmpty);
      });

      test(
        'concurrent appends produce distinct versions and the full union',
        () async {
          final w = DeltaWriter(path);
          await w.write(t([0])); // v0
          final versions = await Future.wait([
            w.write(t([1])),
            w.write(t([2])),
            w.write(t([3])),
          ]);
          expect(versions.toSet().length, 3); // no two appends shared a version
          expect(await readIds(path), {0, 1, 2, 3});
        },
      );
    });

    group('checkpoint-bounded replay (#7)', () {
      Future<void> appendMany(int n) async {
        final w = DeltaWriter(path);
        for (int i = 0; i < n; i++) {
          await w.write(t([i]));
        }
      }

      test('a checkpoint is written every 10 commits', () async {
        await appendMany(12); // versions 0..11 → checkpoint at v10
        final pointer = File('$path/_delta_log/_libbigdata_last_checkpoint');
        expect(pointer.existsSync(), isTrue);
        expect(pointer.readAsStringSync(), contains('"version":10'));
        expect(
          File(
            '$path/_delta_log/${'10'.padLeft(20, '0')}.checkpoint.json',
          ).existsSync(),
          isTrue,
        );
        expect(await readIds(path), {for (int i = 0; i < 12; i++) i});
      });

      test(
        'snapshot is reconstructed from the checkpoint, not version 0',
        () async {
          await appendMany(12);
          // Remove every commit JSON the checkpoint subsumes (0..9). A from-scratch
          // fold would now fail; success proves the checkpoint was consulted.
          for (int v = 0; v < 10; v++) {
            File('$path/_delta_log/${DeltaLog.commitFileName(v)}').deleteSync();
          }
          expect(await readIds(path), {for (int i = 0; i < 12; i++) i});
        },
      );

      test('time travel and checkpoint boundary read correctly', () async {
        await appendMany(12);
        expect(await readIds(path, version: 3), {0, 1, 2, 3});
        expect(await readIds(path, version: 10), {
          for (int i = 0; i <= 10; i++) i,
        });
      });
    });
  });

  // Lifecycle operations that DESTROY or REPLACE data. Both bugs here were
  // invisible to the existing tests because each feature was only ever tested
  // alone: overwrite on a table with no deletion vector, and vacuum on a table
  // too short to have checkpointed.
  group('overwrite + deletion vectors', () {
    Table t(List<int> ids) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: ids, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    late Directory dir;
    late String path;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('delta_ow_');
      path = '${dir.path}/tbl';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Future<Set<int>> ids() async {
      final table = await (await DataFrameIO.readDelta(path)).execute();
      return {for (final r in table.toRows()) r['id'] as int};
    }

    test('overwrite after a delete replaces ALL prior data', () async {
      final w = DeltaWriter(path);
      await w.write(t([0, 1, 2])); // v0
      await w.delete((row) => (row['id'] as int) == 0); // v1: file gains a DV
      expect(await ids(), {1, 2});
      await w.write(t([9]), mode: DeltaWriteMode.overwrite); // v2
      // A tombstone is matched by (path, deletionVector); one built without the
      // DV matches nothing, leaving the old file live next to the new one.
      expect(await ids(), {9});
    });

    test('overwrite after a delete still time-travels', () async {
      final w = DeltaWriter(path);
      await w.write(t([0, 1, 2]));
      await w.delete((row) => (row['id'] as int) == 0);
      await w.write(t([9]), mode: DeltaWriteMode.overwrite);
      final v1 = await (await DataFrameIO.readDelta(
        path,
        version: 1,
      )).execute();
      expect({for (final r in v1.toRows()) r['id'] as int}, {1, 2});
    });

    test('overwrite with no deletion vector still replaces data', () async {
      final w = DeltaWriter(path);
      await w.write(t([0, 1, 2]));
      await w.write(t([9]), mode: DeltaWriteMode.overwrite);
      expect(await ids(), {9});
    });
  });

  group('vacuum vs time travel', () {
    Table t(List<int> ids) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: ids, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    late Directory dir;
    late String path;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('delta_vac_');
      path = '${dir.path}/tbl';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    test('a file tombstoned before a checkpoint survives vacuum', () async {
      final w = DeltaWriter(path);
      await w.write(t([0, 1, 2])); // v0: creates the file time travel needs
      await w.update((row) => (row['id'] as int) == 0, {
        'id': (row) => 100,
      }); // v1: tombstones v0's file (rewrite)
      // Commit past the checkpoint interval (10) so _maybeCheckpoint fires and
      // the checkpoint — which records live adds only — hides v1's tombstone.
      for (var i = 0; i < 10; i++) {
        await w.write(t([10 + i]));
      }
      expect(
        File(DeltaLog(path).checkpointPath(10)).existsSync(),
        isTrue,
        reason: 'the checkpoint is what hides the tombstone',
      );

      // Any TTL: the tombstoned file is not an orphan, it is history.
      await DeltaLog(path).vacuumOrphans(olderThan: Duration.zero);

      final v0 = await (await DataFrameIO.readDelta(
        path,
        version: 0,
      )).execute();
      expect({for (final r in v0.toRows()) r['id'] as int}, {0, 1, 2});
    });

    test('a genuine orphan is still removed after a checkpoint', () async {
      final w = DeltaWriter(path);
      for (var i = 0; i < 11; i++) {
        await w.write(t([i]));
      }
      final orphan = File('$path/part-00000-orphan.parquet')
        ..writeAsBytesSync([1, 2, 3]);
      await DeltaLog(path).vacuumOrphans(olderThan: Duration.zero);
      expect(orphan.existsSync(), isFalse);
    });
  });

  // The Delta protocol's "Partition Value Serialization" governs the
  // partitionValues MAP; the Hive `col=value` PATH is a different convention.
  // One encoder served both, so our writer and reader agreed with each other
  // while both deviated from the spec — a closed loop only a real engine breaks.
  group('partition value serialization', () {
    late Directory dir;
    late String path;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('delta_pv_');
      path = '${dir.path}/tbl';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Map<String, dynamic> addAction() {
      final commit = File('$path/_delta_log/${'0'.padLeft(20, '0')}.json');
      for (final line in commit.readAsLinesSync()) {
        final action = jsonDecode(line) as Map<String, dynamic>;
        if (action['add'] != null) return action['add'] as Map<String, dynamic>;
      }
      throw StateError('no add action');
    }

    Table partitioned(String name, DataType type, List<Object?> values) =>
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(
              name: 'id',
              values: [for (var i = 0; i < values.length; i++) i],
              nullable: false,
            ),
            ListColumn<Object>(
              name: name,
              dataType: type,
              data: values,
              nullable: true,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
              Field(name: name, dataType: type, nullable: true),
            ],
          ),
        );

    test(
      'partitioning by EVERY column fails loud (would lose all rows)',
      () async {
        // Partition columns live in the path, not the data file, so partitioning
        // by every column would write zero-column/zero-row files and silently
        // commit a version containing none of the input rows.
        await expectLater(
          DeltaWriter(path).write(
            partitioned('region', DataType.string, ['US', 'EU']),
            partitionBy: ['id', 'region'], // both columns
          ),
          throwsArgumentError,
        );
        // Nothing was committed.
        expect(Directory('$path/_delta_log').existsSync(), isFalse);
      },
    );

    test('partitioning by a strict subset still round-trips', () async {
      await DeltaWriter(path).write(
        partitioned('region', DataType.string, ['US', 'EU']),
        partitionBy: ['region'], // id remains a data column
      );
      final rows = await DeltaTable(path).read();
      expect(rows.rowCount, 2);
    });

    test(
      'a rejected partitioned append deletes its staged files (finding 1)',
      () async {
        int parquets() => Directory(path)
            .listSync(recursive: true)
            .whereType<File>()
            .where((f) => f.path.endsWith('.parquet'))
            .length;

        // v0: two partitions (region=US, region=EU), one file each.
        await DeltaWriter(path).write(
          partitioned('region', DataType.string, ['US', 'EU']),
          partitionBy: ['region'],
        );
        expect(parquets(), 2);

        // A schema-mismatched partitioned append is rejected at commit time. Its
        // staged file lives on disk at `region=EU/...` but is LOGGED at the
        // URI-encoded `region%3DEU/...`; cleanup must resolve the PHYSICAL path
        // and delete it, not check the encoded path, find nothing, and silently
        // orphan the real file (the finding-1 bug).
        final wide = Table(
          name: 't',
          columns: [
            ...partitioned('region', DataType.string, ['EU']).columns,
            NumericColumn.int64(name: 'extra', values: [9], nullable: false),
          ],
          schema: Schema(
            fields: [
              ...partitioned('region', DataType.string, ['EU']).schema.fields,
              Field(name: 'extra', dataType: DataType.int64, nullable: false),
            ],
          ),
        );
        await expectLater(
          DeltaWriter(path).write(wide, partitionBy: ['region']),
          throwsA(isA<StateError>()),
        );
        // Only v0's two files remain — the rejected append left no orphan.
        expect(parquets(), 2);
      },
    );

    test(
      'partitioned add actions carry precomputed stats (finding 3)',
      () async {
        // Stats are computed eagerly per partition (so each partition Table is
        // released right after its file is written) rather than from a Table
        // retained until commit — the add action must still carry real stats.
        await DeltaWriter(path).write(
          partitioned('region', DataType.string, ['US', 'EU']),
          partitionBy: ['region'],
        );
        expect(addAction()['stats'], contains('numRecords'));
      },
    );

    test('a null partition value serializes as the empty string', () async {
      // Spec: "An empty string for any type translates to a null partition
      // value." __HIVE_DEFAULT_PARTITION__ is the PATH convention, not this one:
      // Spark casts the map's string straight to the column type, so the
      // sentinel reads back as the literal text.
      await DeltaWriter(path).write(
        partitioned('region', DataType.string, [null]),
        partitionBy: ['region'],
      );
      expect(addAction()['partitionValues'], {'region': ''});
      // The directory segment keeps the Hive marker.
      expect(addAction()['path'], contains('__HIVE_DEFAULT_PARTITION__'));
    });

    test('a null partition value round-trips back to null', () async {
      await DeltaWriter(path).write(
        partitioned('region', DataType.string, ['eu', null]),
        partitionBy: ['region'],
      );
      final table = await (await DataFrameIO.readDelta(path)).execute();
      expect(
        {for (final r in table.toRows()) r['id'] as int: r['region']},
        {0: 'eu', 1: null},
      );
    });

    group('special-character partition values round-trip (audit 7)', () {
      // The writer emitted the RAW value into the directory name and add.path,
      // but the reader (and the Delta spec) URL-decode add.path — a `50%`
      // partition wrote fine and then EVERY read threw FormatException, and
      // `a/b` silently became a nested directory. Values now get Hive path
      // escaping on disk and URL encoding in the log.
      for (final value in ['50%', 'a b', 'a/b', 'ünïcode', 'a=b', 'x%2Fy']) {
        test("'$value' writes, reads, deletes, and appends", () async {
          Table two() => partitioned('c', DataType.string, [value, 'plain']);
          await DeltaWriter(path).write(two(), partitionBy: ['c']);
          final first = await DeltaTable(path).read();
          expect(
            first.toRows().map((r) => r['c']).toSet(),
            {value, 'plain'},
            reason: 'read-back after write',
          );
          // Mutations resolve add.path too.
          await DeltaWriter(path).delete((row) => row['c'] == 'plain');
          await DeltaWriter(
            path,
          ).write(two(), partitionBy: ['c'], mode: DeltaWriteMode.append);
          final after = await DeltaTable(path).read();
          expect(
            after.toRows().where((r) => r['c'] == value).length,
            2,
            reason: 'both appends of the special value survive',
          );
        });
      }

      test('delete sees partition values at their LOGICAL types '
          '(external review)', () async {
        // The predicate row used to receive the log's raw STRING map, so
        // `row['p'] == 5` compared 5 against '5', matched nothing, and the
        // delete silently returned -1 with all rows intact.
        Table t(DataType type, List<Object?> partitions) => Table(
          name: 't',
          columns: [
            NumericColumn.int64(
              name: 'id',
              values: [for (var i = 0; i < partitions.length; i++) i],
              nullable: false,
            ),
            ListColumn<Object>(
              name: 'p',
              dataType: type,
              data: partitions,
              nullable: true,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
              Field(name: 'p', dataType: type, nullable: true),
            ],
          ),
        );

        // int partition
        await DeltaWriter(
          path,
        ).write(t(DataType.int64, [5, 5, 7]), partitionBy: ['p']);
        final v = await DeltaWriter(path).delete((r) => r['p'] == 5);
        expect(v, greaterThanOrEqualTo(0));
        expect(
          (await DeltaTable(path).read()).toRows().map((r) => r['p']).toSet(),
          {7},
        );

        // date partition
        final datePath = '${dir.path}/tbl_date';
        await DeltaWriter(datePath).write(
          t(DataType.date, [
            DateTime.utc(2024, 1, 15),
            DateTime.utc(2024, 2, 20),
          ]),
          partitionBy: ['p'],
        );
        final dv = await DeltaWriter(
          datePath,
        ).delete((r) => r['p'] == DateTime.utc(2024, 1, 15));
        expect(dv, greaterThanOrEqualTo(0));
        expect(
          (await DeltaTable(datePath).read()).toRows().single['p'],
          DateTime.utc(2024, 2, 20),
        );

        // null partition (the map serializes null as '')
        final nullPath = '${dir.path}/tbl_null';
        await DeltaWriter(
          nullPath,
        ).write(t(DataType.string, [null, 'keep']), partitionBy: ['p']);
        final nv = await DeltaWriter(nullPath).delete((r) => r['p'] == null);
        expect(nv, greaterThanOrEqualTo(0));
        expect(
          (await DeltaTable(nullPath).read()).toRows().single['p'],
          'keep',
        );
      });

      test(
        'a corrupt partition value fails loud on read (external review)',
        () async {
          // tryParse-to-null / non-'true'→false fabricated data from corrupt
          // metadata; the codec now rejects it naming column/type/value.
          Table t() => Table(
            name: 't',
            columns: [
              NumericColumn.int64(name: 'id', values: [1], nullable: false),
              NumericColumn.int64(name: 'n', values: [5], nullable: false),
            ],
            schema: Schema(
              fields: [
                Field(name: 'id', dataType: DataType.int64, nullable: false),
                Field(name: 'n', dataType: DataType.int64, nullable: false),
              ],
            ),
          );
          await DeltaWriter(path).write(t(), partitionBy: ['n']);
          // Corrupt the committed add action's partition value in place.
          final commit = File('$path/_delta_log/${'0'.padLeft(20, '0')}.json');
          final lines = commit.readAsLinesSync();
          final patched = [
            for (final line in lines)
              line.contains('"add"')
                  ? line.replaceFirst('"n":"5"', '"n":"12x"')
                  : line,
          ];
          commit.writeAsStringSync('${patched.join('\n')}\n');
          await expectLater(
            DeltaTable(path).read(),
            throwsA(
              isA<FormatException>().having(
                (e) => e.message,
                'message',
                allOf(contains('12x'), contains('n')),
              ),
            ),
          );
        },
      );

      test('the logged add.path URL-decodes to the on-disk file', () async {
        await DeltaWriter(
          path,
        ).write(partitioned('c', DataType.string, ['50%']), partitionBy: ['c']);
        final logged = addAction()['path'] as String;
        expect(File('$path/${Uri.decodeFull(logged)}').existsSync(), isTrue);
        // On disk the Hive-escaped form: `c=50%25/...`.
        expect(Uri.decodeFull(logged), startsWith('c=50%25/'));
      });
    });

    test('add.stats serialize date/timestamp/decimal as typed strings '
        '(audit 7)', () async {
      // Raw storage ints (epoch days/micros) and unscaled BigInts in the JSON
      // stats made external engines parse garbage bounds.
      await DeltaWriter(path).write(
        Table(
          name: 't',
          columns: [
            ListColumn<DateTime>(
              name: 'd',
              dataType: DataType.date,
              data: [DateTime.utc(2024, 1, 15), DateTime.utc(2024, 2, 20)],
              nullable: false,
            ),
            ListColumn<DateTime>(
              name: 'ts',
              dataType: DataType.timestamp,
              data: [
                DateTime.utc(2024, 1, 15, 10, 30),
                DateTime.utc(2024, 2, 20, 8, 5),
              ],
              nullable: false,
            ),
            ListColumn<BigInt>(
              name: 'amt',
              dataType: DataType.decimal,
              data: [BigInt.from(12345), BigInt.from(99)], // 123.45, 0.99
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'd', dataType: DataType.date, nullable: false),
              Field(name: 'ts', dataType: DataType.timestamp, nullable: false),
              Field(
                name: 'amt',
                dataType: DataType.decimal,
                nullable: false,
                precision: 10,
                scale: 2,
              ),
            ],
          ),
        ),
      );
      final stats =
          jsonDecode(addAction()['stats'] as String) as Map<String, dynamic>;
      final min = stats['minValues'] as Map<String, dynamic>;
      final max = stats['maxValues'] as Map<String, dynamic>;
      expect(min['d'], '2024-01-15');
      expect(max['d'], '2024-02-20');
      expect(min['ts'], startsWith('2024-01-15T10:30'));
      expect(max['ts'], startsWith('2024-02-20T08:05'));
      expect(min['amt'], '0.99');
      expect(max['amt'], '123.45');
    });

    test('a date partition serializes as yyyy-MM-dd', () async {
      await DeltaWriter(path).write(
        partitioned('d', DataType.date, [DateTime.utc(2024, 1, 15)]),
        partitionBy: ['d'],
      );
      expect(addAction()['partitionValues'], {'d': '2024-01-15'});
    });

    test('a date partition reads back as a DateTime, not a String', () async {
      await DeltaWriter(path).write(
        partitioned('d', DataType.date, [DateTime.utc(2024, 1, 15)]),
        partitionBy: ['d'],
      );
      final table = await (await DataFrameIO.readDelta(path)).execute();
      final value = table.toRows().single['d'];
      expect(
        value,
        isA<DateTime>(),
        reason: 'declared date; got ${value.runtimeType}',
      );
      expect(value, DateTime.utc(2024, 1, 15));
    });

    test('a boolean partition serializes as true/false', () async {
      await DeltaWriter(
        path,
      ).write(partitioned('b', DataType.boolean, [true]), partitionBy: ['b']);
      expect(addAction()['partitionValues'], {'b': 'true'});
    });

    test('a string partition is unchanged', () async {
      await DeltaWriter(path).write(
        partitioned('region', DataType.string, ['eu-west']),
        partitionBy: ['region'],
      );
      expect(addAction()['partitionValues'], {'region': 'eu-west'});
    });

    test('a high-precision decimal partition round-trips exactly', () async {
      // Reading the partition string through a double would round this past 2^53
      // (or brick the read when the rounded double can't restate at scale 0).
      final unscaled = BigInt.parse('12345678901234567890');
      await DeltaWriter(path).write(
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'id', values: [0], nullable: false),
            ListColumn<BigInt>(
              name: 'p',
              dataType: DataType.decimal,
              data: [unscaled],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
              Field(
                name: 'p',
                dataType: DataType.decimal,
                nullable: false,
                precision: 38,
                scale: 0,
              ),
            ],
          ),
        ),
        partitionBy: ['p'],
      );
      final table = await (await DataFrameIO.readDelta(path)).execute();
      expect(table.column('p')!.toList(), [unscaled]);
    });

    test('a table written with the legacy sentinel still reads', () async {
      // Tables this library already wrote carry __HIVE_DEFAULT_PARTITION__ in
      // the map; the reader must keep accepting it.
      await DeltaWriter(path).write(
        partitioned('region', DataType.string, ['eu', null]),
        partitionBy: ['region'],
      );
      final commit = File('$path/_delta_log/${'0'.padLeft(20, '0')}.json');
      commit.writeAsStringSync(
        commit.readAsStringSync().replaceAll(
          '"region":""',
          '"region":"__HIVE_DEFAULT_PARTITION__"',
        ),
      );
      final table = await (await DataFrameIO.readDelta(path)).execute();
      expect(
        {for (final r in table.toRows()) r['id'] as int: r['region']},
        {0: 'eu', 1: null},
      );
    });
  });

  group('vacuum', () {
    Table t() => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: [1, 2, 3], nullable: false),
      ],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    List<File> parts(String path) => Directory(path)
        .listSync()
        .whereType<File>()
        .where((f) => f.path.contains('part-') && f.path.endsWith('.parquet'))
        .toList();

    Future<Set<int>> read(String path) async {
      final t = await (await DataFrameIO.readDelta(path)).execute();
      return {for (final r in t.toRows()) r['id'] as int};
    }

    late Directory dir;
    late String path;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('delta_vac_');
      path = '${dir.path}/tbl';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    test(
      'vacuum removes a crash-orphaned part file, keeps the committed one',
      () async {
        await DeltaWriter(path).write(t());
        final committed = parts(path).single.path;
        // A writer that wrote a data file then died before committing.
        final orphan = File('$path/part-99999-deadbeef.parquet')
          ..writeAsBytesSync(List.filled(64, 0));

        await DeltaLog(path).vacuumOrphans(olderThan: Duration.zero);

        expect(orphan.existsSync(), isFalse, reason: 'orphan reclaimed');
        expect(
          File(committed).existsSync(),
          isTrue,
          reason: 'committed file kept',
        );
        expect(await read(path), {1, 2, 3});
      },
    );

    test('vacuum preserves a within-TTL (in-flight) orphan', () async {
      await DeltaWriter(path).write(t());
      final fresh = File('$path/part-88888-inflight.parquet')
        ..writeAsBytesSync(List.filled(64, 0));

      await DeltaLog(path).vacuumOrphans(); // default 1h TTL

      expect(fresh.existsSync(), isTrue, reason: 'a fresh orphan is in-flight');
    });

    test('vacuum keeps an orphaned DV that is still referenced', () async {
      // Write + delete so a deletion-vector .bin exists and is referenced.
      await DeltaWriter(path).write(t());
      await DeltaWriter(path).delete((row) => row['id'] == 2);
      final dvs = Directory(path)
          .listSync()
          .whereType<File>()
          .where((f) => f.path.contains('deletion_vector_'))
          .toList();
      expect(dvs, isNotEmpty, reason: 'a DV .bin was written');

      await DeltaLog(path).vacuumOrphans(olderThan: Duration.zero);

      for (final dv in dvs) {
        expect(dv.existsSync(), isTrue, reason: 'referenced DV preserved');
      }
      expect(await read(path), {1, 3});
    });
  });

  group('cross-engine fixtures', () {
    Future<List<int>> ids(String path) async {
      final t = await (await DataFrameIO.readDelta(path)).execute();
      return t.column('id')!.toList().cast<int>()..sort();
    }

    group('Delta fixtures', () {
      test('reads this library\'s own deletion-vector table', () async {
        const path = 'test/fixtures/delta/dart_written';
        final t = await (await DataFrameIO.readDelta(path)).execute();
        expect(t.column('id')!.toList().cast<int>()..sort(), [
          1,
          3,
          5,
          7,
          9,
          100,
          101,
          102,
          103,
          104,
        ]);
        expect(t.column('id'), isA<NumericColumn>());
        expect(t.column('name')!.toList(), contains('n100'));
      });

      test('time travel on the fixture sees pre-delete rows', () async {
        const path = 'test/fixtures/delta/dart_written';
        final v0 = await (await DataFrameIO.readDelta(
          path,
          version: 0,
        )).execute();
        expect(v0.column('id')!.toList().cast<int>()..sort(), [
          0,
          1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          9,
        ]);
      });

      // Externally-authored deletion-vector tables (Spark + delta-spark). Each
      // activates once its fixture is copied in from the Docker interop run; both
      // delete id >= 5 from ids 0..9, leaving 0..4.
      for (final engine in ['spark_written', 'spark4_written']) {
        final path = 'test/fixtures/delta/$engine';
        test(
          'reads a $engine deletion-vector table',
          () async {
            expect(await ids(path), [0, 1, 2, 3, 4]);
          },
          skip: Directory(path).existsSync()
              ? false
              : '$engine fixture not generated yet (run the Docker interop step)',
        );
      }
    });
  });

  group('writer merge cap', () {
    Table t(List<int> ids) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: ids, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    late Directory dir;
    late String tbl;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('merge_cap_');
      tbl = '${dir.path}/t';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    test('a merge source over the cap throws StateError', () async {
      await DeltaWriter(tbl).write(t([1, 2, 3]));
      await expectLater(
        DeltaWriter(tbl).merge(t([2, 3, 4, 5]), on: ['id'], maxSourceRows: 2),
        throwsA(isA<StateError>()),
      );
    });

    test('under the cap the merge upserts normally', () async {
      await DeltaWriter(tbl).write(t([1, 2, 3]));
      await DeltaWriter(tbl).merge(t([3, 4]), on: ['id'], maxSourceRows: 100);
      final t2 = await DeltaTable(tbl).read();
      expect({for (final r in t2.toRows()) r['id'] as int}, {1, 2, 3, 4});
    });
  });

  group('parallel read', () {
    Table t(List<int> ids, List<String> regions) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: ids, nullable: false),
        ListColumn<String>(
          name: 'region',
          dataType: DataType.string,
          data: regions,
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

    Future<List<Map<String, Object?>>> rows(
      String path, {
      int concurrency = 1,
    }) async {
      final t = await (await DataFrameIO.readDelta(
        path,
        concurrency: concurrency,
      )).execute();
      return [
        for (final r in t.toRows()) {'id': r['id'], 'region': r['region']},
      ];
    }

    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('delta_par_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('partitioned multi-file table: concurrency:4 == sequential', () async {
      final tbl = '${dir.path}/t';
      // partitionBy writes one data file per distinct region → several files.
      await DeltaWriter(tbl).write(
        t([1, 2, 3, 4, 5, 6], ['us', 'eu', 'us', 'ap', 'eu', 'ap']),
        partitionBy: ['region'],
      );
      final seq = await rows(tbl);
      final par = await rows(tbl, concurrency: 4);
      expect(par, seq);
      expect(seq.length, 6);
    });

    test('deletion-vector table reads identically under concurrency', () async {
      final tbl = '${dir.path}/dv';
      await DeltaWriter(tbl).write(t([1, 2, 3, 4], ['us', 'eu', 'us', 'eu']));
      await DeltaWriter(tbl).delete((row) => row['id'] == 2 || row['id'] == 3);
      final seq = await rows(tbl);
      final par = await rows(tbl, concurrency: 4);
      expect(par, seq);
      expect({for (final r in seq) r['id']}, {1, 4});
    });
  });

  group('readBatches streaming', () {
    Table t(List<int> ids) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: ids, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    test('readBatches applies the deletion vector across batches', () async {
      final dir = Directory.systemTemp.createTempSync('dbatch_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final tbl = '${dir.path}/t';

      await DeltaWriter(tbl).write(t([10, 20, 30, 40, 50]));
      // Deletion vector removes positions 1 (20) and 3 (40).
      await DeltaWriter(tbl).delete((r) => r['id'] == 20 || r['id'] == 40);

      // Small batch size forces multiple batches, exercising the offset tracking.
      final batches = await DeltaTable(tbl).readBatches(size: 2).toList();
      final rows = [
        for (final b in batches)
          for (final r in b.toRows()) r['id'] as int,
      ];
      expect(rows..sort(), [10, 30, 50]);
      expect(batches.length, greaterThan(1)); // actually streamed, not one blob

      // Matches the eager read.
      final eager = await DeltaTable(tbl).read();
      expect({for (final r in eager.toRows()) r['id'] as int}, {10, 30, 50});
    });
  });

  group('SQL DML', () {
    Table t(List<int> ids, [List<String>? names]) => Table(
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

    Future<Map<int, String?>> read(String path) async {
      final t = await DeltaTable(path).read();
      return {for (final r in t.toRows()) r['id'] as int: r['name'] as String?};
    }

    late Directory dir;
    late String tbl;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('delta_sql_');
      tbl = '${dir.path}/tbl';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    test('SQL UPDATE … SET … WHERE on a Delta table', () async {
      await DeltaWriter(tbl).write(t([0, 1, 2, 3, 4]));
      final engine = SqlEngine(allowExternalPaths: true);
      final res = await engine.execute(
        "UPDATE '$tbl' SET name = 'X' WHERE id >= 3",
      );
      expect(res.column('version')!.toList(), [1]);
      expect(await read(tbl), {0: 'n0', 1: 'n1', 2: 'n2', 3: 'X', 4: 'X'});
    });

    test('SQL UPDATE SET with a CASE expression', () async {
      await DeltaWriter(tbl).write(t([1, 2, 3]));
      final engine = SqlEngine(allowExternalPaths: true);
      await engine.execute(
        "UPDATE '$tbl' SET name = CASE WHEN id >= 3 THEN 'hi' ELSE name END",
      );
      expect(await read(tbl), {1: 'n1', 2: 'n2', 3: 'hi'});
    });

    test('SQL UPDATE SET with a scalar function', () async {
      await DeltaWriter(tbl).write(t([1, 2]));
      final engine = SqlEngine(allowExternalPaths: true);
      await engine.execute("UPDATE '$tbl' SET name = UPPER(name) WHERE id = 1");
      expect(await read(tbl), {1: 'N1', 2: 'n2'});
    });

    test('SQL UPDATE SET COALESCE fills a NULL', () async {
      await DeltaWriter(tbl).write(
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'id', values: [1, 2], nullable: false),
            ListColumn<String>(
              name: 'name',
              dataType: DataType.string,
              data: ['a', null],
              nullable: true,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
              Field(name: 'name', dataType: DataType.string, nullable: true),
            ],
          ),
        ),
      );
      final engine = SqlEngine(allowExternalPaths: true);
      await engine.execute("UPDATE '$tbl' SET name = COALESCE(name, 'X')");
      expect(await read(tbl), {1: 'a', 2: 'X'});
    });

    test('SQL INSERT VALUES with expressions evaluates them', () async {
      await DeltaWriter(tbl).write(t([1]));
      final engine = SqlEngine(allowExternalPaths: true);
      await engine.execute("INSERT INTO '$tbl' VALUES (1 + 1, UPPER('a'))");
      expect(await read(tbl), {1: 'n1', 2: 'A'});
    });

    test('SQL UPDATE with an arithmetic SET expression', () async {
      await DeltaWriter(tbl).write(
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'id', values: [1, 2, 3], nullable: false),
            NumericColumn.int64(
              name: 'v',
              values: [10, 20, 30],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
              Field(name: 'v', dataType: DataType.int64, nullable: false),
            ],
          ),
        ),
      );
      final engine = SqlEngine(allowExternalPaths: true);
      await engine.execute("UPDATE '$tbl' SET v = v + 100 WHERE id = 2");
      final t2 = await DeltaTable(tbl).read();
      final m = {for (final r in t2.toRows()) r['id'] as int: r['v']};
      expect(m, {1: 10, 2: 120, 3: 30});
    });

    group('SQL UPDATE SET shares SELECT evaluator semantics (review 1+2)', () {
      // The DML SET value now evaluates through the SAME physical evaluator as
      // SELECT, so it inherits Kleene 3VL, checked int overflow and /0->NULL —
      // the hand-rolled row evaluator collapsed NULL AND TRUE to false, wrapped
      // int64 overflow, and produced Infinity on divide-by-zero.
      late SqlEngine engine;
      setUp(() => engine = SqlEngine(allowExternalPaths: true));
      tearDown(() => engine.dispose());

      test('AND with a NULL operand keeps NULL, not false (3VL)', () async {
        await DeltaWriter(tbl).write(
          Table(
            name: 't',
            columns: [
              NumericColumn.int64(name: 'id', values: [1], nullable: false),
              ListColumn<bool>(
                name: 'flag',
                dataType: DataType.boolean,
                data: [null],
                nullable: true,
              ),
              ListColumn<bool>(
                name: 'out',
                dataType: DataType.boolean,
                data: [null],
                nullable: true,
              ),
            ],
            schema: Schema(
              fields: [
                Field(name: 'id', dataType: DataType.int64, nullable: false),
                Field(name: 'flag', dataType: DataType.boolean, nullable: true),
                Field(name: 'out', dataType: DataType.boolean, nullable: true),
              ],
            ),
          ),
        );
        await engine.execute("UPDATE '$tbl' SET out = flag AND TRUE");
        final row = (await DeltaTable(tbl).read()).toRows().single;
        expect(row['out'], isNull); // NULL AND TRUE = NULL, not false
      });

      test(
        'int64 overflow in a SET expression fails loud, not wrapped',
        () async {
          await DeltaWriter(tbl).write(
            Table(
              name: 't',
              columns: [
                NumericColumn.int64(name: 'v', values: [1], nullable: false),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'v', dataType: DataType.int64, nullable: false),
                ],
              ),
            ),
          );
          await expectLater(
            engine.execute("UPDATE '$tbl' SET v = 9223372036854775807 + 1"),
            throwsA(anyOf(isA<FormatException>(), isA<StateError>())),
          );
          expect((await DeltaTable(tbl).read()).toRows().single['v'], 1);
        },
      );

      test(
        'divide by zero in a SET expression yields NULL, not Infinity',
        () async {
          await DeltaWriter(tbl).write(
            Table(
              name: 't',
              columns: [
                NumericColumn.int64(name: 'id', values: [1], nullable: false),
                ListColumn<double>(
                  name: 'x',
                  dataType: DataType.float64,
                  data: [1.0],
                  nullable: true,
                ),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'id', dataType: DataType.int64, nullable: false),
                  Field(name: 'x', dataType: DataType.float64, nullable: true),
                ],
              ),
            ),
          );
          await engine.execute("UPDATE '$tbl' SET x = 10.0 / 0");
          expect((await DeltaTable(tbl).read()).toRows().single['x'], isNull);
        },
      );
    });

    group('SQL UPDATE over a decimal column (audit 7)', () {
      // DML rows surface decimals as Decimal, which the num-only evaluator
      // mishandled: `price = 1.50` silently matched NOTHING, `<` threw from
      // Decimal.compareTo(double), and `price + 1` threw from _num.
      Table prices() => Table(
        name: 't',
        columns: [
          NumericColumn.int64(name: 'id', values: [1, 2, 3], nullable: false),
          ListColumn<BigInt>(
            name: 'price',
            dataType: DataType.decimal,
            // 1.50, 2.50, 3.75 at scale 2
            data: [BigInt.from(150), BigInt.from(250), BigInt.from(375)],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'id', dataType: DataType.int64, nullable: false),
            Field(
              name: 'price',
              dataType: DataType.decimal,
              nullable: false,
              precision: 10,
              scale: 2,
            ),
          ],
        ),
      );

      late SqlEngine engine;
      setUp(() async {
        await DeltaWriter(tbl).write(prices());
        engine = SqlEngine(allowExternalPaths: true);
      });
      tearDown(() => engine.dispose());

      Future<Map<int, Object?>> byId() async {
        final t = await DeltaTable(tbl).read();
        return {for (final r in t.toRows()) r['id'] as int: r['price']};
      }

      test('WHERE price = <literal> matches by value', () async {
        await engine.execute("UPDATE '$tbl' SET id = 99 WHERE price = 1.50");
        final t = await DeltaTable(tbl).read();
        final ids = t.column('id')!.toList()..sort();
        expect(ids, [2, 3, 99]);
      });

      test('WHERE price < <literal> compares by value', () async {
        await engine.execute("UPDATE '$tbl' SET id = 99 WHERE price < 2.00");
        final t = await DeltaTable(tbl).read();
        final ids = t.column('id')!.toList()..sort();
        expect(ids, [2, 3, 99]);
      });

      test('SET price = price + 1 keeps exact scale', () async {
        await engine.execute(
          "UPDATE '$tbl' SET price = price + 1 WHERE id = 1",
        );
        expect(await byId(), {
          1: Decimal(BigInt.from(250), 2),
          2: Decimal(BigInt.from(250), 2),
          3: Decimal(BigInt.from(375), 2),
        });
      });

      test('SET decimal division fails loud, not rounded storage', () async {
        await expectLater(
          engine.execute("UPDATE '$tbl' SET price = price / 3 WHERE id = 1"),
          throwsA(isA<FormatException>()),
        );
        // Nothing was committed.
        expect((await byId())[1], Decimal(BigInt.from(150), 2));
      });

      test('SET decimal = int literal stores the exact value', () async {
        // An integer is exact in a decimal column — it must store, not fail
        // loud like a lossy float division.
        await engine.execute("UPDATE '$tbl' SET price = 20 WHERE id = 1");
        expect((await byId())[1], Decimal(BigInt.from(2000), 2)); // 20.00
      });

      test(
        'SET decimal = int arithmetic stores exactly at column scale',
        () async {
          await engine.execute("UPDATE '$tbl' SET price = 2 + 3 WHERE id = 1");
          expect((await byId())[1], Decimal(BigInt.from(500), 2)); // 5.00
        },
      );

      test(
        'INSERT an int into a decimal column stores the exact value',
        () async {
          await engine.execute("INSERT INTO '$tbl' VALUES (4, 20)");
          expect((await byId())[4], Decimal(BigInt.from(2000), 2)); // 20.00
        },
      );
    });

    test(
      'arithmetic on a non-numeric column fails loud (FormatException)',
      () async {
        // a mistyped operand yields a domain error, not a raw TypeError.
        await DeltaWriter(tbl).write(t([1, 2]));
        final engine = SqlEngine(allowExternalPaths: true);
        await expectLater(
          engine.execute("UPDATE '$tbl' SET name = name + 1 WHERE id = 1"),
          throwsA(isA<FormatException>()),
        );
      },
    );

    test('SQL MERGE INTO … UPDATE SET * / INSERT * (upsert)', () async {
      await DeltaWriter(tbl).write(t([0, 1, 2, 3, 4]));
      final src = '${dir.path}/src.parquet';
      await ParquetWriter(src).write(t([3, 5], ['s3', 's5']));
      final engine = SqlEngine(allowExternalPaths: true);
      await engine.execute(
        "MERGE INTO '$tbl' t USING '$src' s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *",
      );
      expect(await read(tbl), {
        0: 'n0',
        1: 'n1',
        2: 'n2',
        3: 's3',
        4: 'n4',
        5: 's5',
      });
    });

    test('SQL MERGE … WHEN MATCHED THEN DELETE', () async {
      await DeltaWriter(tbl).write(t([0, 1, 2, 3, 4]));
      final src = '${dir.path}/d.parquet';
      await ParquetWriter(src).write(t([1, 3]));
      final engine = SqlEngine(allowExternalPaths: true);
      await engine.execute(
        "MERGE INTO '$tbl' USING '$src' ON id = id WHEN MATCHED THEN DELETE",
      );
      expect((await read(tbl)).keys.toList()..sort(), [0, 2, 4]);
    });

    group('SQL INSERT INTO … VALUES', () {
      test('appends a single row and returns the version', () async {
        await DeltaWriter(tbl).write(t([0]));
        final engine = SqlEngine(allowExternalPaths: true);
        final res = await engine.execute("INSERT INTO '$tbl' VALUES (1, 'a')");
        expect(res.column('version')!.toList(), [1]);
        expect(await read(tbl), {0: 'n0', 1: 'a'});
      });

      test('appends several rows in one commit', () async {
        await DeltaWriter(tbl).write(t([0]));
        final engine = SqlEngine(allowExternalPaths: true);
        final res = await engine.execute(
          "INSERT INTO '$tbl' VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        );
        expect(res.column('version')!.toList(), [1], reason: 'one commit');
        expect(await read(tbl), {0: 'n0', 1: 'a', 2: 'b', 3: 'c'});
      });

      test('a column list may reorder the columns', () async {
        await DeltaWriter(tbl).write(t([0]));
        final engine = SqlEngine(allowExternalPaths: true);
        await engine.execute("INSERT INTO '$tbl' (name, id) VALUES ('a', 1)");
        expect(await read(tbl), {0: 'n0', 1: 'a'});
      });

      test('an omitted nullable column defaults to NULL', () async {
        final nullable = Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'id', values: [0], nullable: false),
            ListColumn<String>(
              name: 'name',
              dataType: DataType.string,
              data: ['n0'],
              nullable: true,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
              Field(name: 'name', dataType: DataType.string, nullable: true),
            ],
          ),
        );
        await DeltaWriter(tbl).write(nullable);
        final engine = SqlEngine(allowExternalPaths: true);
        await engine.execute("INSERT INTO '$tbl' (id) VALUES (1)");
        await engine.execute("INSERT INTO '$tbl' VALUES (2, NULL)");
        expect(await read(tbl), {0: 'n0', 1: null, 2: null});
      });

      test('an int literal widens into a float column', () async {
        await DeltaWriter(tbl).write(
          Table(
            name: 't',
            columns: [
              NumericColumn.int64(name: 'id', values: [0], nullable: false),
              NumericColumn.float64(
                name: 'amt',
                values: [1.5],
                nullable: false,
              ),
            ],
            schema: Schema(
              fields: [
                Field(name: 'id', dataType: DataType.int64, nullable: false),
                Field(name: 'amt', dataType: DataType.float64, nullable: false),
              ],
            ),
          ),
        );
        final engine = SqlEngine(allowExternalPaths: true);
        await engine.execute("INSERT INTO '$tbl' VALUES (1, 3)");
        final rows = (await DeltaTable(tbl).read()).toRows();
        expect(rows.map((r) => r['amt']), [1.5, 3.0]);
      });

      test('works through executeStream', () async {
        // executeStream has its own statement dispatch; without INSERT there it
        // falls through to the SELECT path and dies on a cast.
        await DeltaWriter(tbl).write(t([0]));
        final engine = SqlEngine(allowExternalPaths: true);
        await engine
            .executeStream("INSERT INTO '$tbl' VALUES (1, 'a')")
            .drain();
        expect(await read(tbl), {0: 'n0', 1: 'a'});
      });

      test('appends to a partitioned table', () async {
        // The append has to repeat the table's partition layout, which is
        // recoverable only from the committed metadata.
        await DeltaWriter(tbl).write(t([0, 1]), partitionBy: ['name']);
        final engine = SqlEngine(allowExternalPaths: true);
        await engine.execute("INSERT INTO '$tbl' VALUES (2, 'n2')");
        expect(await read(tbl), {0: 'n0', 1: 'n1', 2: 'n2'});
      });

      group('INSERT fails closed (table untouched)', () {
        Future<void> rejects(String sql, Matcher matcher) async {
          await DeltaWriter(tbl).write(t([0, 1]));
          final engine = SqlEngine(allowExternalPaths: true);
          await expectLater(engine.execute(sql), throwsA(matcher));
          expect(await read(tbl), {0: 'n0', 1: 'n1'}); // untouched
        }

        test('too few values', () async {
          await rejects("INSERT INTO '$tbl' VALUES (2)", isA<ArgumentError>());
        });

        test('too many values', () async {
          await rejects(
            "INSERT INTO '$tbl' VALUES (2, 'a', 'extra')",
            isA<ArgumentError>(),
          );
        });

        test('a column list naming an unknown column', () async {
          await rejects(
            "INSERT INTO '$tbl' (nope) VALUES (2)",
            isA<ArgumentError>(),
          );
        });

        test('a column named twice', () async {
          await rejects(
            "INSERT INTO '$tbl' (id, id) VALUES (2, 3)",
            isA<ArgumentError>(),
          );
        });

        test('a string into an int column', () async {
          await rejects(
            "INSERT INTO '$tbl' VALUES ('x', 'a')",
            isA<ArgumentError>(),
          );
        });

        test('an int into a string column', () async {
          // commonType would widen int->string; storing it would put an int
          // inside a string column, which column validation does not catch.
          await rejects(
            "INSERT INTO '$tbl' VALUES (2, 9)",
            isA<ArgumentError>(),
          );
        });

        test('NULL into a non-nullable column', () async {
          await rejects(
            "INSERT INTO '$tbl' VALUES (2, NULL)",
            isA<ArgumentError>(),
          );
        });

        test('a non-constant value', () async {
          await rejects(
            "INSERT INTO '$tbl' VALUES (2, name)",
            isA<FormatException>(),
          );
        });
      });

      test('a missing table fails loud instead of creating one', () async {
        // DeltaWriter.write creates a table on an empty location, so a typo'd
        // path must be rejected before it gets there.
        final missing = '${dir.path}/nosuch';
        final engine = SqlEngine(allowExternalPaths: true);
        await expectLater(
          engine.execute("INSERT INTO '$missing' VALUES (1, 'a')"),
          throwsA(anyOf(isA<ArgumentError>(), isA<StateError>())),
        );
        expect(Directory(missing).existsSync(), isFalse);
      });
    });

    group('DML fails closed (no silent degrade)', () {
      test('UPDATE WHERE on an unknown column throws', () async {
        await DeltaWriter(tbl).write(t([0, 1, 2]));
        final engine = SqlEngine(allowExternalPaths: true);
        await expectLater(
          engine.execute("UPDATE '$tbl' SET name = 'X' WHERE missing = 1"),
          throwsA(isA<FormatException>()),
        );
        expect(await read(tbl), {0: 'n0', 1: 'n1', 2: 'n2'}); // untouched
      });

      test('UPDATE SET to an unknown column throws (not null-write)', () async {
        await DeltaWriter(tbl).write(t([0, 1, 2]));
        final engine = SqlEngine(allowExternalPaths: true);
        await expectLater(
          engine.execute("UPDATE '$tbl' SET name = missing WHERE id = 1"),
          throwsA(isA<FormatException>()),
        );
        expect(await read(tbl), {0: 'n0', 1: 'n1', 2: 'n2'});
      });

      test('UPDATE WHERE that is a non-boolean expression throws', () async {
        await DeltaWriter(tbl).write(t([0, 1, 2]));
        final engine = SqlEngine(allowExternalPaths: true);
        await expectLater(
          engine.execute("UPDATE '$tbl' SET name = 'X' WHERE id"),
          throwsA(isA<FormatException>()),
        );
      });
    });
  });

  group('deletion vector robustness', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('dv_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('a valid deletion vector round-trips', () async {
      final w = await DeletionVector.write(dir.path, {1, 5, 9});
      final loaded = await DeletionVector.load(w.dv, dir.path);
      expect(loaded.values.toSet(), {1, 5, 9});
      expect(loaded.contains(5), isTrue);
      expect(loaded.contains(2), isFalse);
      expect(loaded.isNotEmpty, isTrue);
    });

    test('a truncated .bin file throws FormatException', () async {
      final w = await DeletionVector.write(dir.path, {2, 4, 6});
      final f = File(w.path);
      final bytes = await f.readAsBytes();
      await f.writeAsBytes(
        bytes.sublist(0, bytes.length - 6),
      ); // chop payload+crc
      await expectLater(
        DeletionVector.load(w.dv, dir.path),
        throwsA(isA<FormatException>()),
      );
    });

    test('a CRC-corrupted deletion vector throws FormatException', () async {
      final w = await DeletionVector.write(dir.path, {3, 7});
      final f = File(w.path);
      final bytes = await f.readAsBytes();
      bytes[bytes.length - 6] ^= 0xFF; // flip a payload byte → CRC mismatch
      await f.writeAsBytes(bytes);
      await expectLater(
        DeletionVector.load(w.dv, dir.path),
        throwsA(isA<FormatException>()),
      );
    });

    test(
      'an oversized declared length is rejected before allocation',
      () async {
        // A tiny .bin whose 4-byte BE length header declares a payload far over
        // the cap must fail BEFORE the huge single-buffer read.
        final bin = File('${dir.path}/huge.bin');
        const huge = 1 << 30; // 1 GiB, over the 256 MiB cap
        final header = ByteData(4)..setUint32(0, huge, Endian.big);
        await bin.writeAsBytes(header.buffer.asUint8List());
        final dv = DeletionVectorDescriptor(
          storageType: 'p',
          pathOrInlineDv: bin.path,
          offset: 0,
          sizeInBytes: huge, // matches the header so the cross-check passes
          cardinality: 0,
        );
        await expectLater(
          DeletionVector.load(dv, dir.path),
          throwsA(
            isA<FormatException>().having(
              (e) => e.message,
              'message',
              contains('over the'),
            ),
          ),
        );
      },
    );

    test('a malformed Z85 descriptor throws FormatException', () async {
      // A 20-char UUID descriptor (the 'u' path slices the last 20 chars) whose
      // last char is a space — not in the Z85 alphabet.
      final dv = DeletionVectorDescriptor(
        storageType: 'u',
        pathOrInlineDv: '${'0' * 19} ',
        offset: 1,
        sizeInBytes: 0,
        cardinality: 0,
      );
      await expectLater(
        DeletionVector.load(dv, dir.path),
        throwsA(isA<FormatException>()),
      );
    });
  });

  // A bitmap container stores its bits in a Uint64List, so bit 63 of a word
  // reads back as a NEGATIVE Dart int. Iterating it must still yield 63.
  // `add`/`contains` only mask bits and were always right — the bug lived
  // solely where a bit position is converted back into a number, which is why a
  // test that builds a bitmap container but never iterates it passed.
  group('roaring bitmap word-boundary iteration', () {
    /// A bitmap container needs >4096 values in one 16-bit window; [extra]
    /// values well clear of [probe] force the promotion without touching it.
    RoaringBitmap dense(int probe) {
      final bm = RoaringBitmap();
      bm.add(probe);
      for (var i = 1000; i < 5200; i++) {
        bm.add(i);
      }
      return bm;
    }

    test('bit 63 iterates as 63, not 62', () {
      final bm = dense(63);
      expect(bm.contains(63), isTrue);
      expect(bm.contains(62), isFalse);
      final values = bm.values.toList();
      expect(values, contains(63));
      expect(values, isNot(contains(62)));
    });

    test('every bit of a full word iterates exactly once', () {
      // All 64 bits set: the buggy shift yielded 62 twice and 63 never, so the
      // values collapsed and cardinality no longer matched.
      final bm = RoaringBitmap();
      for (var i = 0; i < 64; i++) {
        bm.add(i);
      }
      for (var i = 1000; i < 5200; i++) {
        bm.add(i);
      }
      final values = bm.values.toList();
      expect(values.toSet().length, values.length, reason: 'no duplicates');
      expect(values.where((v) => v < 64).toList(), [
        for (var i = 0; i < 64; i++) i,
      ]);
      expect(values.length, bm.length);
    });

    test('values agrees with contains across word boundaries', () {
      for (final probe in [0, 62, 63, 64, 127, 128]) {
        final bm = dense(probe);
        expect(bm.values.toList().toSet(), {
          probe,
          for (var i = 1000; i < 5200; i++) i,
        }, reason: 'probe $probe');
      }
    });

    test('a bit-63 deletion vector survives serialize -> deserialize', () {
      final values = {63, for (var i = 1000; i < 5200; i++) i};
      expect(
        Roaring.deserializeTreemap(Roaring.serializeTreemap(values)),
        values,
      );
    });
  });

  group('roaring bitmap', () {
    test('membership matches a reference Set over mixed inputs', () {
      final bm = RoaringBitmap();
      final ref = <int>{};

      void put(int v) {
        bm.add(v);
        ref.add(v);
      }

      // Sparse, scattered low values.
      for (final v in [0, 5, 100, 65535, 65536, 70000]) {
        put(v);
      }
      // A dense contiguous range that forces an array container to promote to a
      // bitmap container (> 4096 entries in one 16-bit key window).
      for (int v = 200000; v < 210000; v++) {
        put(v);
      }
      // Values in higher 32-bit buckets.
      for (final v in [1 << 32, (1 << 32) + 7, (5 << 32) + 123456]) {
        put(v);
      }

      expect(bm.length, ref.length);
      // Present values report true.
      for (final v in ref) {
        expect(bm.contains(v), isTrue, reason: 'missing $v');
      }
      // A spread of absent values report false.
      for (final v in [
        1,
        4,
        99,
        199999,
        210000,
        65537,
        (1 << 32) + 1,
        1 << 40,
      ]) {
        expect(bm.contains(v), isFalse, reason: 'false positive $v');
      }
    });

    test('adding a duplicate does not grow length', () {
      final bm = RoaringBitmap();
      bm.add(42);
      bm.add(42);
      bm.add(4242);
      expect(bm.length, 2);
      expect(bm.contains(42), isTrue);
      expect(bm.contains(43), isFalse);
    });
  });

  group('SQL merge cap', () {
    Table t(List<int> ids) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: ids, nullable: false),
        NumericColumn.int64(
          name: 'v',
          values: [for (final i in ids) i * 10],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'v', dataType: DataType.int64, nullable: false),
        ],
      ),
    );

    late Directory dir;
    late String tbl;
    late String src;
    setUp(() async {
      dir = Directory.systemTemp.createTempSync('merge_cap_');
      tbl = '${dir.path}/tbl';
      src = '${dir.path}/src.parquet';
      await DeltaWriter(tbl).write(t([0, 1, 2]));
      await ParquetWriter(src).write(t([2, 3, 4, 5, 6])); // 5-row source
    });
    tearDown(() => dir.deleteSync(recursive: true));

    test('oversized MERGE subquery source fails loud (no OOM)', () async {
      final engine = SqlEngine(
        allowExternalPaths: true,
        maxMaterializedRows: 2,
      );
      expect(
        engine.execute(
          "MERGE INTO '$tbl' t USING (SELECT * FROM '$src') s ON t.id = s.id "
          "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
        ),
        throwsA(
          isA<StateError>().having(
            (e) => e.toString(),
            'message',
            contains('maxRows'),
          ),
        ),
      );
    });

    test('a within-cap MERGE subquery source still works', () async {
      final engine = SqlEngine(
        allowExternalPaths: true,
        maxMaterializedRows: 100,
      );
      await engine.execute(
        "MERGE INTO '$tbl' t USING (SELECT * FROM '$src') s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
      );
      final t2 = await (await DataFrameIO.readDelta(tbl)).execute();
      expect(
        {for (final r in t2.toRows()) r['id'] as int},
        {0, 1, 2, 3, 4, 5, 6},
      );
    });
  });

  group('path confinement', () {
    Table t(List<int> ids) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: ids, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    test('add.path outside the table root is confined by default', () async {
      final dir = Directory.systemTemp.createTempSync('confine_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final tbl = '${dir.path}/t';

      // A normal table (commit 0, ids 1..3).
      await DeltaWriter(tbl).write(t([1, 2, 3]));

      // A data file OUTSIDE the table dir, referenced by an absolute path.
      final outside = '${dir.path}/outside.parquet';
      await ParquetWriter(outside).write(t([99]));

      // Hand-author commit 1 adding the external file.
      final commit1 = '$tbl/_delta_log/${'0' * 19}1.json';
      await File(commit1).writeAsString(
        '${jsonEncode({
          'add': {
            'path': outside, // absolute, outside the table root
            'partitionValues': <String, String>{},
            'size': await File(outside).length(),
            'modificationTime': 0,
            'dataChange': true,
          },
        })}\n',
      );

      // Default: the escape is rejected.
      await expectLater(DeltaTable(tbl).read(), throwsA(isA<ArgumentError>()));

      // Opt-in: the external file is read (ids 1,2,3,99).
      final t2 = await DeltaTable(tbl, allowExternalPaths: true).read();
      expect({for (final r in t2.toRows()) r['id'] as int}, {1, 2, 3, 99});
    });
  });

  group('W2 Delta write survives NaN/Inf', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('phaseA_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table floats(List<double?> v) => Table(
      name: 't',
      columns: [NumericColumn.float64(name: 'f', values: v, nullable: true)],
      schema: Schema(
        fields: [Field(name: 'f', dataType: DataType.float64, nullable: true)],
      ),
    );

    test('write does not throw and the column round-trips', () async {
      final path = '${dir.path}/tbl';
      await DeltaWriter(path).write(floats([1.0, double.nan, 3.0]));
      final t = await (await DataFrameIO.readDelta(path)).execute();
      final back = t.column('f')!.toList();
      expect(back[0], 1.0);
      expect((back[1] as double).isNaN, isTrue);
      expect(back[2], 3.0);
    });
  });

  group('#1 Delta commit coordinator seam', () {
    late Directory dir;
    late String path;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('r47delta_');
      path = '${dir.path}/tbl';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Table ids(List<int> v) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: v, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    test('an injected coordinator is used for the commit', () async {
      final probe = _CountingCoordinator();
      await DeltaWriter(path, coordinator: probe).write(ids([1, 2, 3]));
      expect(probe.commits, greaterThan(0));
      // And the write is a real, readable commit.
      final t = await (await DataFrameIO.readDelta(path)).execute();
      expect(t.column('id')!.toList(), [1, 2, 3]);
    });

    test('sequential writes advance versions contiguously', () async {
      final w = DeltaWriter(path);
      await w.write(ids([1]));
      await w.write(ids([2]));
      await w.write(ids([3]));
      expect(await DeltaLog(path).versions(), [0, 1, 2]);
    });
  });

  group('R1 Delta overwrite with a new schema', () {
    test('overwrite replaces the schema and reads back correctly', () async {
      final dir = Directory.systemTemp.createTempSync('r48delta_');
      final path = '${dir.path}/tbl';
      try {
        // v0: schema {id int64}
        await DeltaWriter(path).write(
          Table(
            name: 't',
            columns: [
              NumericColumn.int64(name: 'id', values: [1, 2], nullable: false),
            ],
            schema: Schema(
              fields: [
                Field(name: 'id', dataType: DataType.int64, nullable: false),
              ],
            ),
          ),
        );
        // overwrite with a DIFFERENT schema {name string}
        await DeltaWriter(path).write(
          Table(
            name: 't',
            columns: [
              ListColumn<String>(
                name: 'name',
                dataType: DataType.string,
                data: ['x', 'y'],
                nullable: false,
              ),
            ],
            schema: Schema(
              fields: [
                Field(name: 'name', dataType: DataType.string, nullable: false),
              ],
            ),
          ),
          mode: DeltaWriteMode.overwrite,
        );
        final t = await (await DataFrameIO.readDelta(path)).execute();
        expect(t.schema.fields.map((f) => f.name), ['name']);
        expect(t.column('name')!.toList(), ['x', 'y']);
      } finally {
        dir.deleteSync(recursive: true);
      }
    });
  });

  group('#2 explicit-schema strictness (Delta)', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('r49_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Schema idName() => Schema(
      fields: [
        Field(name: 'id', dataType: DataType.int64, nullable: false),
        Field(name: 'name', dataType: DataType.string, nullable: true),
      ],
    );

    Future<void> seed(String path) async {
      await DeltaWriter(path).write(
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'id', values: [1, 2], nullable: false),
            ListColumn<String>(
              name: 'name',
              dataType: DataType.string,
              data: ['a', 'b'],
              nullable: true,
            ),
          ],
          schema: idName(),
        ),
      );
    }

    test(
      'Delta update to a typo\'d column throws (was a silent no-op)',
      () async {
        final path = '${dir.path}/tbl';
        await seed(path);
        await expectLater(
          DeltaWriter(path).update(
            (row) => row['id'] == 1,
            {'nmae': (row) => 'X'}, // typo: column is 'name'
          ),
          throwsArgumentError,
        );
      },
    );

    test('Delta merge with a source missing a join key throws', () async {
      final path = '${dir.path}/tbl';
      await seed(path);
      // source has no 'id' column.
      final source = Table(
        name: 's',
        columns: [
          ListColumn<String>(
            name: 'name',
            dataType: DataType.string,
            data: ['z'],
            nullable: true,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'name', dataType: DataType.string, nullable: true),
          ],
        ),
      );
      await expectLater(
        DeltaWriter(path).merge(source, on: ['id']),
        throwsArgumentError,
      );
    });
  });

  group('#6 bounded / streaming writers (Delta)', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('r49_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('Delta merge rejects a source over the byte budget', () async {
      final path = '${dir.path}/tbl';
      await DeltaWriter(path).write(
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
      );
      final big = 'x' * 4096;
      final source = Table(
        name: 's',
        columns: [
          NumericColumn.int64(
            name: 'id',
            values: [for (var i = 0; i < 200; i++) i],
            nullable: false,
          ),
          ListColumn<String>(
            name: 'name',
            dataType: DataType.string,
            data: [for (var i = 0; i < 200; i++) big],
            nullable: true,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'id', dataType: DataType.int64, nullable: false),
            Field(name: 'name', dataType: DataType.string, nullable: true),
          ],
        ),
      );
      await expectLater(
        DeltaWriter(path).merge(source, on: ['id'], maxSourceBytes: 4096),
        throwsStateError,
      );
    });
  });

  group('#7 Delta log replay is streamed and bounded', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('r49_'));
    tearDown(() => dir.deleteSync(recursive: true));

    // A single commit over the 1 GiB metadata cap can't be fabricated cheaply;
    // the streamed replay (_forEachLine) is exercised on every read, so assert a
    // multi-commit log folds correctly through the public read path.
    test('a multi-commit log replays correctly via streamed lines', () async {
      final path = '${dir.path}/tbl';
      Table one(int id) => Table(
        name: 't',
        columns: [
          NumericColumn.int64(name: 'id', values: [id], nullable: false),
        ],
        schema: Schema(
          fields: [
            Field(name: 'id', dataType: DataType.int64, nullable: false),
          ],
        ),
      );
      final writer = DeltaWriter(path);
      await writer.write(one(1));
      for (var i = 2; i <= 6; i++) {
        await writer.write(one(i));
      }
      final back = await DataFrameIO.readDelta(path);
      final table = await back.execute(maxRows: null);
      expect(table.rowCount, 6);
      final ids = (table.column('id')!.toList()..sort());
      expect(ids, [1, 2, 3, 4, 5, 6]);
    });
  });

  group('#7 file-lock CAS coordinator', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('r50_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table one(int id) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: [id], nullable: false),
      ],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    test('sequential commits under the lock work (functional)', () async {
      final path = '${dir.path}/lk1';
      const coord = FileLockDeltaCommitCoordinator();
      await DeltaWriter(path, coordinator: coord).write(one(1));
      await DeltaWriter(path, coordinator: coord).write(one(2));
      final back = await DeltaTable(path).read();
      expect({for (final r in back.toRows()) r['id'] as int}, {1, 2});
      expect(File('$path/_delta_log/_commit.lock').existsSync(), isTrue);
    });

    test(
      'concurrent writer PROCESSES lose no commits',
      () async {
        final path = '${dir.path}/lk2';
        // Seed v0 with a single writer, then race N separate processes appending.
        await DeltaWriter(
          path,
          coordinator: const FileLockDeltaCommitCoordinator(),
        ).write(one(0));
        const n = 4;
        final results = await Future.wait([
          for (var i = 1; i <= n; i++)
            Process.run('dart', [
              'run',
              'test/support/delta_lock_append.dart',
              path,
              '$i',
            ]),
        ]);
        for (final r in results) {
          expect(r.exitCode, 0, reason: '${r.stdout}\n${r.stderr}');
        }
        final back = await DeltaTable(path).read();
        final ids = {for (final r in back.toRows()) r['id'] as int};
        expect(ids, {0, 1, 2, 3, 4}); // seed + every append, none lost
      },
      timeout: const Timeout(Duration(minutes: 2)),
    );
  });

  group('#F2 registry failure after publish is indeterminate', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('f2_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('a post-publish registry write failure retains staged data', () async {
      final path = '${dir.path}/t';
      // Point the registry at an existing DIRECTORY. The lock (a sibling) still
      // works and _read sees no registry FILE (a fresh registry, so the body
      // runs), but the final temp→registry rename fails AFTER the Delta commit
      // has published — the exact "committed but pointer unrecorded" window.
      final registry = '${dir.path}/registry.json';
      Directory(registry).createSync();
      final catalog = LocalCatalog(registry);
      final coord = LocalCatalogDeltaCommitCoordinator(catalog);

      // Raw, the writer would treat this as a pre-publish failure and DELETE the
      // staged files. The coordinator must remap it to an indeterminate outcome.
      await expectLater(
        DeltaWriter(path, coordinator: coord).write(_ids([1, 2, 3])),
        throwsA(isA<DeltaCommitIndeterminate>()),
      );

      // The commit published (v0 in the log) and its data files SURVIVE.
      expect(await DeltaLog(path).versions(), [0]);
      final parquet = Directory(path)
          .listSync(recursive: true)
          .whereType<File>()
          .where((f) => f.path.endsWith('.parquet'))
          .toList();
      expect(
        parquet,
        isNotEmpty,
        reason: 'staged data must not be deleted after the commit published',
      );
      final back = await DeltaTable(path).read();
      expect({for (final r in back.toRows()) r['id'] as int}, {1, 2, 3});
    });
  });

  group('#3 Delta append partition validation', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('r52_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('append without partitionBy to a partitioned table throws', () async {
      final path = '${dir.path}/dt';
      final t = Table(
        name: 't',
        columns: [
          NumericColumn.int64(name: 'id', values: [1], nullable: false),
          ListColumn<String>(
            name: 'p',
            dataType: DataType.string,
            data: ['x'],
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
      await DeltaWriter(path).write(t, partitionBy: ['p']);
      await expectLater(
        DeltaWriter(path).write(t), // no partitionBy — mismatch
        throwsA(isA<StateError>()),
      );
    });
  });

  group('#10 Delta zero-byte intermediate commit fails closed', () {
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

    test('a truncated mid-history commit throws on read', () async {
      final path = '${dir.path}/dtz';
      await DeltaWriter(path).write(intTable('id', [0]));
      await DeltaWriter(path).write(intTable('id', [1]));
      await DeltaWriter(path).write(intTable('id', [2]));
      // Truncate version 1 (an intermediate commit) to 0 bytes.
      File(
        '$path/_delta_log/${'1'.padLeft(20, '0')}.json',
      ).writeAsBytesSync([]);
      await expectLater(DeltaTable(path).read(), throwsA(isA<StateError>()));
    });
  });

  group('review round 42 Delta DML', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('round42_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table rows(List<int> ids, List<int?> v, List<String> name) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: ids, nullable: false),
        NumericColumn.int64(name: 'v', values: v, nullable: true),
        ListColumn<String>(
          name: 'name',
          dataType: DataType.string,
          data: name,
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'v', dataType: DataType.int64, nullable: true),
          Field(name: 'name', dataType: DataType.string, nullable: false),
        ],
      ),
    );

    Future<Map<int, String>> names(String path) async {
      final t = await DeltaTable(path).read();
      return {for (final r in t.toRows()) r['id'] as int: r['name'] as String};
    }

    test(
      '#9 DeltaTable.read(maxRows: null) reads a file over the per-file default',
      () async {
        final tbl = '${dir.path}/tbl';
        await DeltaWriter(
          tbl,
        ).write(rows([0, 1, 2], [1, 2, 3], ['a', 'b', 'c']));
        // A cap below the row count throws; opting out (null) reads it fully.
        expect(
          () => DeltaTable(tbl).read(maxRows: 1),
          throwsA(isA<StateError>()),
        );
        final t = await DeltaTable(tbl).read(maxRows: null);
        expect(t.rowCount, 3);
      },
    );

    group('#1 DML three-valued logic', () {
      test(
        'UPDATE … WHERE v <> 5 leaves NULL and equal rows untouched',
        () async {
          final tbl = '${dir.path}/tbl';
          await DeltaWriter(
            tbl,
          ).write(rows([0, 1, 2], [10, null, 5], ['a', 'b', 'c']));
          final engine = SqlEngine(allowExternalPaths: true);
          await engine.execute("UPDATE '$tbl' SET name = 'X' WHERE v <> 5");
          // v=10 matches; v=NULL is UNKNOWN (not matched); v=5 is false.
          expect(await names(tbl), {0: 'X', 1: 'b', 2: 'c'});
        },
      );

      test(
        'UPDATE … WHERE v = 5 matches the equal row only (NULL excluded)',
        () async {
          final tbl = '${dir.path}/tbl';
          await DeltaWriter(
            tbl,
          ).write(rows([0, 1, 2], [10, null, 5], ['a', 'b', 'c']));
          final engine = SqlEngine(allowExternalPaths: true);
          await engine.execute("UPDATE '$tbl' SET name = 'X' WHERE v = 5");
          expect(await names(tbl), {0: 'a', 1: 'b', 2: 'X'});
        },
      );
    });

    group('#1 MERGE cardinality', () {
      Table src(List<int?> ids, List<String> name) => Table(
        name: 's',
        columns: [
          NumericColumn.int64(name: 'id', values: ids, nullable: true),
          ListColumn<String>(
            name: 'name',
            dataType: DataType.string,
            data: name,
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'id', dataType: DataType.int64, nullable: true),
            Field(name: 'name', dataType: DataType.string, nullable: false),
          ],
        ),
      );

      Table base(List<int> ids, {bool nullableId = false}) => Table(
        name: 't',
        columns: [
          NumericColumn.int64(name: 'id', values: ids, nullable: nullableId),
          ListColumn<String>(
            name: 'name',
            dataType: DataType.string,
            data: [for (final i in ids) 'n$i'],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'id', dataType: DataType.int64, nullable: nullableId),
            Field(name: 'name', dataType: DataType.string, nullable: false),
          ],
        ),
      );

      test(
        'duplicate source keys throw when a MATCHED action is present',
        () async {
          final tbl = '${dir.path}/tbl';
          await DeltaWriter(tbl).write(base([1, 2]));
          expect(
            () => DeltaWriter(tbl).merge(
              src([1, 1], ['x', 'y']),
              on: ['id'],
              whenMatchedUpdate: {'name': (t, s) => s['name']},
            ),
            throwsA(isA<StateError>()),
          );
        },
      );

      test(
        'a NULL join key never matches — it is inserted, not merged',
        () async {
          final tbl = '${dir.path}/tbl';
          await DeltaWriter(tbl).write(base([1, 2], nullableId: true));
          // The null-keyed source row must fall to NOT-MATCHED (insert), not error
          // and not collide with the target.
          await DeltaWriter(tbl).merge(
            src([null], ['fresh']),
            on: ['id'],
            whenMatchedUpdate: {'name': (t, s) => s['name']},
          );
          final t = await DeltaTable(tbl).read();
          final ns = [for (final r in t.toRows()) r['name'] as String]..sort();
          expect(ns, ['fresh', 'n1', 'n2']);
        },
      );
    });
  });

  group('#1 Delta mutation vs a fresh incomplete claim', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('round44_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table ids(List<int> v) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: v, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    test('a mutation does not commit over a concurrent 0-byte claim', () async {
      final tbl = '${dir.path}/t';
      await DeltaWriter(tbl).write(ids([1, 2, 3])); // v0
      // Plant a FRESH 0-byte claim for v1 (a concurrent writer mid-commit).
      await File('$tbl/_delta_log/${'1'.padLeft(20, '0')}.json').create();
      // The UPDATE must contend (back off) and abort rather than jump to v2 over
      // the live claim — bounded by maxRetries so the test terminates.
      await expectLater(
        DeltaWriter(
          tbl,
          maxRetries: 2,
        ).update((r) => (r['id'] as int) == 1, {'id': (r) => 9}),
        throwsA(isA<StateError>()),
      );
      // v2 must NOT exist (the mutation never jumped the claim).
      expect(
        File('$tbl/_delta_log/${'2'.padLeft(20, '0')}.json').existsSync(),
        isFalse,
      );
    });
  });

  group('review round 45 Delta', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('round45_'));
    tearDown(() => dir.deleteSync(recursive: true));

    group('#2 Delta protocol gate', () {
      test('an unknown reader feature is rejected fail-closed', () {
        // Unit: the capability matrix.
        expect(Protocol(1, 2).readerUnsupported(), isNull);
        expect(
          Protocol(
            3,
            7,
            readerFeatures: ['deletionVectors'],
          ).readerUnsupported(),
          isNull,
        );
        expect(
          Protocol(3, 7, readerFeatures: ['columnMapping']).readerUnsupported(),
          isNotNull,
        );
        expect(Protocol(2, 5).readerUnsupported(), isNotNull); // column mapping
      });

      test('reading a table with an unsupported protocol throws', () async {
        final tbl = '${dir.path}/t';
        await DeltaWriter(tbl).write(
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
        );
        // Add commit v1 upgrading the protocol to an unsupported reader feature.
        await File(
          '$tbl/_delta_log/${'1'.padLeft(20, '0')}.json',
        ).writeAsString(
          '${jsonEncode({
            'protocol': {
              'minReaderVersion': 3,
              'minWriterVersion': 7,
              'readerFeatures': ['columnMapping'],
              'writerFeatures': <String>[],
            },
          })}\n',
        );
        await expectLater(
          DeltaTable(tbl).read(),
          throwsA(isA<UnsupportedError>()),
        );
      });
    });

    group('#4 Delta log contiguity', () {
      test('a missing intermediate commit is reported as corruption', () async {
        final tbl = '${dir.path}/t';
        Table one(int v) => Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'id', values: [v], nullable: false),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
            ],
          ),
        );
        final w = DeltaWriter(tbl);
        await w.write(one(0)); // v0
        await w.write(one(1)); // v1
        await w.write(one(2)); // v2
        // Delete the intermediate commit.
        File('$tbl/_delta_log/${'1'.padLeft(20, '0')}.json').deleteSync();
        await expectLater(DeltaTable(tbl).read(), throwsA(isA<StateError>()));
      });
    });
  });

  group('finding 6: Delta reclaims a stale empty version contiguously', () {
    late Directory dir;
    setUp(() async {
      dir = await Directory.systemTemp.createTemp('review_findings');
    });
    tearDown(() async {
      if (await dir.exists()) await dir.delete(recursive: true);
    });

    test('a crash-orphaned 0-byte tail version is reused, no gap', () async {
      final tbl = '${dir.path}/delta6';
      final writer = DeltaWriter(tbl);
      await writer.write(_ids([1])); // v0
      await writer.write(_ids([2])); // v1
      final log = DeltaLog(tbl);
      // Simulate a writer that crashed after the O_EXCL claim of v2 but before
      // publishing content, aged past the 15-minute orphan TTL.
      final v2 = File(log.commitPath(2));
      await v2.create();
      await v2.setLastModified(
        DateTime.now().subtract(const Duration(minutes: 20)),
      );
      await writer.write(_ids([3])); // must reclaim v2, not skip to v3
      expect(await log.versions(), [0, 1, 2]);
      expect((await DeltaTable(tbl).read()).rowCount, 3);
    });
  });

  group('Delta commit durability + timestamp travel', () {
    late Directory dir;
    late String tbl;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('delta_dur_');
      tbl = '${dir.path}/t';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Table ids(List<int> v) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: v, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    test(
      'commits are atomic (no .tmp leftovers) and carry a commitInfo',
      () async {
        await DeltaWriter(tbl).write(ids([1, 2, 3]));
        final logDir = Directory('$tbl/_delta_log');
        final names = logDir
            .listSync()
            .map((e) => e.uri.pathSegments.last)
            .toList();
        expect(names.any((n) => n.endsWith('.tmp')), isFalse);
        final commit = File('$tbl/_delta_log/${'0'.padLeft(20, '0')}.json');
        final first = jsonDecode(commit.readAsLinesSync().first) as Map;
        expect(first['commitInfo']?['timestamp'], isA<int>());
      },
    );

    test('timestamp travel uses commit metadata, not file mtime', () async {
      final w = DeltaWriter(tbl);
      await w.write(ids([1, 2, 3]));
      await w.write(ids([4, 5]));
      // A timestamp before any commit -> error; far-future -> latest snapshot.
      expect(
        () => DeltaTable(tbl).read(timestamp: 0),
        throwsA(isA<ArgumentError>()),
      );
      final latest = await DeltaTable(tbl).read(timestamp: 1 << 62);
      expect(latest.rowCount, 5);
    });
  });

  // Table properties another engine set that this library must honor or refuse.
  // Nothing here writes `configuration` or field metadata itself, so each test
  // patches version 0's metaData the way Spark/delta-rs would have written it.
  group('table properties', () {
    Table t(List<int> ids) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: ids, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    Table nullable(List<int?> ids) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: ids, nullable: true)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: true)],
      ),
    );

    late Directory dir;
    late String path;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('delta_props_');
      path = '${dir.path}/tbl';
    });
    tearDown(() => dir.deleteSync(recursive: true));

    void patch(Map<String, dynamic> Function(Map<String, dynamic>) edit) {
      final file = File('$path/_delta_log/${'0'.padLeft(20, '0')}.json');
      final patched = file.readAsLinesSync().map((line) {
        final action = jsonDecode(line) as Map<String, dynamic>;
        final metadata = action['metaData'] as Map<String, dynamic>?;
        if (metadata == null) return line;
        return jsonEncode({'metaData': edit(metadata)});
      });
      file.writeAsStringSync('${patched.join('\n')}\n');
    }

    void appendOnly() => patch(
      (metadata) => {
        ...metadata,
        'configuration': {'delta.appendOnly': 'true'},
      },
    );

    void invariant() => patch(
      (metadata) => {
        ...metadata,
        'schemaString': jsonEncode({
          'type': 'struct',
          'fields': [
            {
              'name': 'id',
              'type': 'long',
              'nullable': false,
              'metadata': {
                'delta.invariants': '{"expression":{"expression":"id > 0"}}',
              },
            },
          ],
        }),
      },
    );

    group('delta.appendOnly', () {
      test('append still commits', () async {
        final w = DeltaWriter(path);
        await w.write(t([0, 1, 2]));
        appendOnly();
        await w.write(t([3, 4]));
        final after = await (await DataFrameIO.readDelta(path)).execute();
        expect(after.rowCount, 5);
      });

      test('delete is refused', () async {
        final w = DeltaWriter(path);
        await w.write(t([0, 1, 2]));
        appendOnly();
        await expectLater(
          w.delete((row) => (row['id'] as int) == 0),
          throwsA(isA<UnsupportedError>()),
        );
      });

      test('update is refused', () async {
        final w = DeltaWriter(path);
        await w.write(t([0, 1, 2]));
        appendOnly();
        await expectLater(
          w.update((row) => true, {'id': (row) => 9}),
          throwsA(isA<UnsupportedError>()),
        );
      });

      test('merge is refused', () async {
        final w = DeltaWriter(path);
        await w.write(t([0, 1, 2]));
        appendOnly();
        await expectLater(
          w.merge(t([2, 3]), on: ['id'], whenNotMatchedInsert: true),
          throwsA(isA<UnsupportedError>()),
        );
      });

      test('overwrite is refused', () async {
        final w = DeltaWriter(path);
        await w.write(t([0, 1, 2]));
        appendOnly();
        await expectLater(
          w.write(t([9]), mode: DeltaWriteMode.overwrite),
          throwsA(isA<UnsupportedError>()),
        );
      });

      test('the table is left untouched by a refused mutation', () async {
        final w = DeltaWriter(path);
        await w.write(t([0, 1, 2]));
        appendOnly();
        await expectLater(
          w.delete((row) => (row['id'] as int) == 0),
          throwsA(isA<UnsupportedError>()),
        );
        final after = await (await DataFrameIO.readDelta(path)).execute();
        expect(after.column('id')!.toList().cast<int>()..sort(), [0, 1, 2]);
        expect(await DeltaLog(path).versions(), [0]);
      });
    });

    group('delta.invariants', () {
      // Until the invariants are actually evaluated, a write that could violate
      // one must be refused rather than silently accepted.
      test('append is refused', () async {
        final w = DeltaWriter(path);
        await w.write(t([1, 2]));
        invariant();
        await expectLater(w.write(t([3])), throwsA(isA<UnsupportedError>()));
      });

      test('update is refused', () async {
        final w = DeltaWriter(path);
        await w.write(t([1, 2]));
        invariant();
        await expectLater(
          w.update((row) => true, {'id': (row) => -1}),
          throwsA(isA<UnsupportedError>()),
        );
      });

      test('reading is still allowed', () async {
        await DeltaWriter(path).write(t([1, 2]));
        invariant();
        final after = await (await DataFrameIO.readDelta(path)).execute();
        expect(after.rowCount, 2);
      });

      // With an evaluator wired, invariants move from "rejected" to "enforced".
      group('enforced', () {
        DeltaWriter writer() =>
            DeltaWriter(path, invariants: const SqlInvariantEvaluator());

        test('a satisfying append commits', () async {
          await DeltaWriter(path).write(t([1, 2]));
          invariant(); // id > 0
          await writer().write(t([3, 4]));
          final after = await (await DataFrameIO.readDelta(path)).execute();
          expect(after.column('id')!.toList().cast<int>()..sort(), [
            1,
            2,
            3,
            4,
          ]);
        });

        test('a violating append throws and commits nothing', () async {
          await DeltaWriter(path).write(t([1, 2]));
          invariant();
          await expectLater(
            writer().write(t([3, -1])),
            throwsA(
              isA<StateError>().having(
                (e) => e.message,
                'message',
                allOf(contains('id > 0'), contains('invariant')),
              ),
            ),
          );
          final after = await (await DataFrameIO.readDelta(path)).execute();
          expect(after.column('id')!.toList().cast<int>()..sort(), [1, 2]);
          expect(await DeltaLog(path).versions(), [0]);
        });

        test('a violating overwrite throws and commits nothing', () async {
          await DeltaWriter(path).write(t([1, 2]));
          invariant();
          await expectLater(
            writer().write(t([-5]), mode: DeltaWriteMode.overwrite),
            throwsA(isA<StateError>()),
          );
          final after = await (await DataFrameIO.readDelta(path)).execute();
          expect(after.column('id')!.toList().cast<int>()..sort(), [1, 2]);
        });

        test('a NULL does not violate (SQL CHECK semantics)', () async {
          await DeltaWriter(path).write(nullable([1]));
          // id > 0 is NULL for a null id: unknown is not false, so it passes.
          patch(
            (metadata) => {
              ...metadata,
              'schemaString': jsonEncode({
                'type': 'struct',
                'fields': [
                  {
                    'name': 'id',
                    'type': 'long',
                    'nullable': true,
                    'metadata': {
                      'delta.invariants':
                          '{"expression":{"expression":"id > 0"}}',
                    },
                  },
                ],
              }),
            },
          );
          await writer().write(nullable([2, null]));
          final after = await (await DataFrameIO.readDelta(path)).execute();
          expect(after.rowCount, 3);
        });

        test('a table with no invariants is unaffected', () async {
          final w = writer();
          await w.write(t([1]));
          await w.write(t([2]));
          final after = await (await DataFrameIO.readDelta(path)).execute();
          expect(after.column('id')!.toList().cast<int>()..sort(), [1, 2]);
        });

        test('update still refuses: it cannot enforce them yet', () async {
          await DeltaWriter(path).write(t([1, 2]));
          invariant();
          await expectLater(
            writer().update((row) => true, {'id': (row) => -1}),
            throwsA(isA<UnsupportedError>()),
          );
        });

        test('an unreadable invariant is refused, not guessed at', () async {
          await DeltaWriter(path).write(t([1]));
          patch(
            (metadata) => {
              ...metadata,
              'schemaString': jsonEncode({
                'type': 'struct',
                'fields': [
                  {
                    'name': 'id',
                    'type': 'long',
                    'nullable': false,
                    'metadata': {'delta.invariants': 'not json at all'},
                  },
                ],
              }),
            },
          );
          await expectLater(
            writer().write(t([2])),
            throwsA(isA<FormatException>()),
          );
        });
      });
    });

    group('field metadata', () {
      test('survives a schema round-trip', () {
        final schema = Schema(
          fields: [
            Field(
              name: 'id',
              dataType: DataType.int64,
              nullable: false,
              metadata: const {'comment': 'the key'},
            ),
          ],
        );
        final back = DeltaSchema.fromJson(DeltaSchema.toJson(schema));
        expect(back.fields.single.metadata, {'comment': 'the key'});
      });

      test('survives an overwrite that changes the schema', () async {
        final w = DeltaWriter(path);
        await w.write(t([0, 1]));
        // Another engine annotates the column, then we overwrite with a wider
        // schema: the surviving column's metadata must not be dropped.
        patch(
          (metadata) => {
            ...metadata,
            'schemaString': jsonEncode({
              'type': 'struct',
              'fields': [
                {
                  'name': 'id',
                  'type': 'long',
                  'nullable': false,
                  'metadata': {'comment': 'the key'},
                },
              ],
            }),
          },
        );
        final annotated = DeltaSchema.fromJson(
          (await DeltaLog(path).snapshot()).metadata.schemaString,
        );
        expect(annotated.fields.single.metadata, {'comment': 'the key'});

        await w.write(
          Table(
            name: 't',
            columns: [
              NumericColumn.int64(name: 'id', values: [9], nullable: false),
            ],
            schema: annotated,
          ),
          mode: DeltaWriteMode.overwrite,
        );
        final after = DeltaSchema.fromJson(
          (await DeltaLog(path).snapshot()).metadata.schemaString,
        );
        expect(after.fields.single.metadata, {'comment': 'the key'});
      });
    });
  });
}

/// Delegates to the default filesystem coordinator, counting invocations to
/// prove the injection seam is honored.
class _CountingCoordinator implements DeltaCommitCoordinator {
  int commits = 0;
  final _inner = const FilesystemDeltaCommitCoordinator();

  @override
  Future<int> commit({
    required DeltaLog log,
    required Future<List<Map<String, dynamic>>> Function(int version) build,
    String label = 'commit',
  }) {
    commits++;
    return _inner.commit(log: log, build: build, label: label);
  }
}

Table _regioned(List<int> ids, List<String> regions) => Table(
  name: 't',
  columns: [
    NumericColumn.int64(name: 'id', values: ids, nullable: false),
    ListColumn<String>(
      name: 'name',
      dataType: DataType.string,
      data: [for (final i in ids) 'n$i'],
      nullable: false,
    ),
    ListColumn<String>(
      name: 'region',
      dataType: DataType.string,
      data: regions,
      nullable: false,
    ),
  ],
  schema: Schema(
    fields: [
      Field(name: 'id', dataType: DataType.int64, nullable: false),
      Field(name: 'name', dataType: DataType.string, nullable: false),
      Field(name: 'region', dataType: DataType.string, nullable: false),
    ],
  ),
);

Table _ids(List<int> ids) => Table(
  name: 't',
  schema: Schema(
    fields: [Field(name: 'id', dataType: DataType.int64)],
  ),
  columns: [ListColumn<int>(name: 'id', dataType: DataType.int64, data: ids)],
);
