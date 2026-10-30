import 'dart:async';
import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/core/limits.dart';
import 'package:libbigdata/src/formats/arrow/arrow_reader.dart';
import 'package:libbigdata/src/formats/arrow/arrow_writer.dart';
import 'package:libbigdata/src/sql/catalog/table_catalog.dart';
import 'package:libbigdata/src/sql/operators/spill.dart';
import 'package:libbigdata/src/util/parallel.dart';
import 'package:test/test.dart';

import 'support/results.dart';

void main() {
  /// a shared opt-in MemoryManager bounds the COMBINED
  /// in-memory footprint of blocking operators. A small `memoryBudgetBytes` forces
  /// spilling even when each operator's local `spillThresholdBytes` is huge, and
  /// results still match the in-memory run.
  group('memory budget', () {
    group('MemoryManager', () {
      test('grants within budget and denies over it', () {
        final m = MemoryManager(100);
        expect(m.reserve(60), isTrue);
        expect(m.used, 60);
        expect(m.reserve(50), isFalse); // 60+50 > 100
        expect(m.used, 60);
        expect(m.reserve(40), isTrue); // 60+40 == 100
        expect(m.used, 100);
      });

      test('allows a single oversized request when the pool is empty', () {
        final m = MemoryManager(100);
        expect(m.reserve(1000), isTrue); // progress, not deadlock
        expect(m.reserve(1), isFalse); // now full
      });

      test('hardCap denies an oversized-when-empty request', () {
        final m = MemoryManager(100, hardCap: true);
        expect(m.reserve(1000), isFalse); // strict ceiling → caller spills
        expect(m.reserve(80), isTrue); // still grants within budget
      });

      test('release frees the pool and clamps at zero', () {
        final m = MemoryManager(100);
        m.reserve(80);
        m.release(80);
        expect(m.used, 0);
        m.release(50); // double/over-release
        expect(m.used, 0);
        expect(m.reserve(100), isTrue);
      });
    });

    group('fail-closed EngineOptions defaults (#8)', () {
      test('the default bounds output rows', () {
        expect(const EngineOptions().maxOutputRows, defaultMaxMaterializedRows);
      });

      test('unbounded() opts out but keeps spilling on', () {
        final u = EngineOptions.unbounded();
        expect(u.maxOutputRows, isNull);
        expect(u.memory, isNull);
        expect(u.spillBudget, isNull);
        expect(u.spills, isTrue); // RAM still relieved to disk
      });
    });

    group('global budget forces spilling', () {
      late Directory root;
      const rows = 8000;
      setUp(() => root = Directory.systemTemp.createTempSync('membudget_'));
      tearDown(() => root.deleteSync(recursive: true));

      Table data(int n) => Table(
        name: 't',
        columns: [
          NumericColumn.int64(
            name: 'id',
            values: [for (var i = 0; i < n; i++) (i * 2654435761) & 0x7fffffff],
            nullable: false,
          ),
          NumericColumn.int64(
            name: 'k',
            values: [for (var i = 0; i < n; i++) i % 50],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'id', dataType: DataType.int64, nullable: false),
            Field(name: 'k', dataType: DataType.int64, nullable: false),
          ],
        ),
      );

      // In-memory reference (no spilling at all).
      Future<List<Map<String, dynamic>>> inMemory(String sql) async {
        final e = SqlEngine.open(spillThresholdBytes: 0).ok;
        e.registerTable('t', data(rows));
        return (await e.execute(sql).ok).toRows();
      }

      // Budgeted: local threshold huge (never trips), tiny shared budget forces
      // spilling. spillDir is a not-yet-existing subdir, created only when the
      // spill path actually engages.
      Future<({List<Map<String, dynamic>> rows, bool spilled})> budgeted(
        String sql,
      ) async {
        final spillDir = '${root.path}/sub';
        final e = SqlEngine.open(
          spillThresholdBytes: 1 << 30, // 1 GiB → local spill never triggers
          memoryBudgetBytes: 4096, // tiny shared pool → forces spilling
          spillDir: spillDir,
        ).ok;
        e.registerTable('t', data(rows));
        final out = (await e.execute(sql).ok).toRows();
        return (rows: out, spilled: Directory(spillDir).existsSync());
      }

      test('ORDER BY spills under the budget and matches in-memory', () async {
        const sql = 'SELECT id FROM t ORDER BY id';
        final b = await budgeted(sql);
        expect(b.spilled, isTrue, reason: 'budget should have forced a spill');
        expect(b.rows, await inMemory(sql));
      });

      test('GROUP BY spills under the budget and matches in-memory', () async {
        const sql = 'SELECT k, COUNT(*) AS n FROM t GROUP BY k ORDER BY k';
        final b = await budgeted(sql);
        expect(b.spilled, isTrue);
        expect(b.rows, await inMemory(sql));
      });

      test('reservations are released so a later query still spills', () async {
        // If the first query leaked its reservation the pool would stay full;
        // both queries must succeed and match in-memory.
        final spillDir = '${root.path}/sub2';
        final e = SqlEngine.open(
          spillThresholdBytes: 1 << 30,
          memoryBudgetBytes: 4096,
          spillDir: spillDir,
        ).ok;
        e.registerTable('t', data(rows));
        const sql = 'SELECT id FROM t ORDER BY id';
        final ref = await inMemory(sql);
        expect((await e.execute(sql).ok).toRows(), ref);
        expect(
          (await e.execute(sql).ok).toRows(),
          ref,
        ); // second run, post-release
      });
    });
  });

  /// crash-orphaned spill files (left by a SIGKILL that runs no
  /// `finally`) are reclaimed by a startup sweep limited by age, so it is
  /// safe over the shared system-temp spill dir (never deletes a live sibling
  /// engine's in-use file).
  group('spill orphan sweep', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('spillsweep_'));
    tearDown(() => dir.deleteSync(recursive: true));

    // A per-session spill dir with a spill file and a `.live` marker.
    Directory session(String token, {bool marker = true}) {
      final d = Directory('${dir.path}/$sessionPrefix$token')..createSync();
      File('${d.path}/spill-x.parquet').writeAsBytesSync([0]);
      if (marker) File('${d.path}/.live').writeAsBytesSync([0]);
      return d;
    }

    test(
      'ttl: zero reclaims a session dir (marker treated as stale)',
      () async {
        final s = session('aaa');
        await sweepOrphanedSpills(dir.path, ttl: Duration.zero);
        expect(s.existsSync(), isFalse);
      },
    );

    test('a generous ttl keeps a live session dir (fresh marker)', () async {
      final s = session('aaa');
      await sweepOrphanedSpills(dir.path, ttl: const Duration(hours: 1));
      expect(s.existsSync(), isTrue); // fresh marker -> live -> preserved
    });

    test('a session dir with no live marker is reclaimed', () async {
      final s = session('nomarker', marker: false);
      await sweepOrphanedSpills(dir.path, ttl: const Duration(hours: 1));
      expect(s.existsSync(), isFalse);
    });

    test('non-session dirs and files are never touched', () async {
      final other = Directory('${dir.path}/other')..createSync();
      File('${dir.path}/important.txt').writeAsBytesSync([0]);
      await sweepOrphanedSpills(dir.path, ttl: Duration.zero);
      expect(other.existsSync(), isTrue);
      expect(File('${dir.path}/important.txt').existsSync(), isTrue);
    });

    test('a missing directory is a no-op (no throw)', () async {
      await sweepOrphanedSpills(
        '${dir.path}/does-not-exist',
        ttl: Duration.zero,
      );
    });
  });

  group('parallel', () {
    Future<void> writeParquet(String path, List<int> ids) async {
      await ParquetWriter(path)
          .write(
            Table(
              name: 't',
              columns: [
                NumericColumn.int64(name: 'id', values: ids, nullable: false),
                ListColumn<String>(
                  name: 'v',
                  dataType: DataType.string,
                  data: [for (final i in ids) 'v$i'],
                  nullable: false,
                ),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'id', dataType: DataType.int64, nullable: false),
                  Field(name: 'v', dataType: DataType.string, nullable: false),
                ],
              ),
            ),
          )
          .ok;
    }

    group('parallelMap', () {
      test('preserves input order regardless of completion order', () async {
        final out = await parallelMap<int, int>([5, 1, 4, 2, 3], (x) async {
          await Future.delayed(Duration(milliseconds: 10 * x));
          return x * 10;
        }, concurrency: 5);
        expect(out, [50, 10, 40, 20, 30]);
      });

      test('respects the concurrency bound', () async {
        var inFlight = 0;
        var peak = 0;
        await parallelMap<int, int>(List.generate(20, (i) => i), (x) async {
          inFlight++;
          if (inFlight > peak) peak = inFlight;
          await Future.delayed(const Duration(milliseconds: 5));
          inFlight--;
          return x;
        }, concurrency: 4);
        expect(peak, lessThanOrEqualTo(4));
        expect(peak, greaterThan(1));
      });

      test('propagates a task error', () async {
        expect(
          () => parallelMap<int, int>(
            [1, 2, 3],
            (x) async => x == 2 ? throw StateError('boom') : x,
            concurrency: 3,
          ),
          throwsA(isA<StateError>()),
        );
      });

      test('empty input returns empty', () async {
        expect(await parallelMap<int, int>([], (x) async => x), isEmpty);
      });

      test('a task error stops siblings from pulling new items', () async {
        var started = 0;
        await expectLater(
          () => parallelMap<int, int>(List.generate(100, (i) => i), (x) async {
            started++;
            // Worker 0 fails fast; the rest are slow so, without a cancel flag,
            // siblings would keep looping and start all 100 items.
            if (x == 0) throw StateError('boom');
            await Future.delayed(const Duration(milliseconds: 5));
            return x;
          }, concurrency: 4),
          throwsA(isA<StateError>()),
        );
        // With the cancel flag, only the in-flight items are started, not all 100.
        expect(started, lessThan(100));
      });
    });

    group('parallel Dataset.read', () {
      late Directory dir;
      setUp(() => dir = Directory.systemTemp.createTempSync('pds_'));
      tearDown(() => dir.deleteSync(recursive: true));

      test(
        'concurrency 1 and 4 give identical results (with partitions)',
        () async {
          // Hive-partitioned layout: region=us/.. , region=eu/..
          for (final region in ['us', 'eu']) {
            Directory('${dir.path}/region=$region').createSync();
          }
          await writeParquet('${dir.path}/region=us/a.parquet', [1, 2, 3]);
          await writeParquet('${dir.path}/region=us/b.parquet', [4, 5]);
          await writeParquet('${dir.path}/region=eu/c.parquet', [6, 7, 8]);

          final serial = await (await DataFrameIO.readDataset(
            dir.path,
          ).ok).execute().ok;
          final parallel = await (await DataFrameIO.readDataset(
            dir.path,
            concurrency: 4,
          ).ok).execute().ok;

          expect(parallel.rowCount, serial.rowCount);
          expect(
            parallel.schema.fields.map((f) => f.name),
            serial.schema.fields.map((f) => f.name),
          );
          for (final f in serial.schema.fields) {
            expect(
              parallel.column(f.name)!.toList(),
              serial.column(f.name)!.toList(),
              reason: 'column ${f.name} must match',
            );
          }
          // partition column present
          expect(serial.column('region'), isNotNull);
        },
      );

      test('parallel preserves input (file) order', () async {
        await writeParquet('${dir.path}/a.parquet', [1, 2]);
        await writeParquet('${dir.path}/b.parquet', [3, 4]);
        await writeParquet('${dir.path}/c.parquet', [5, 6]);
        final t = await (await DataFrameIO.readDataset(
          dir.path,
          concurrency: 3,
        ).ok).execute().ok;
        expect(t.column('id')!.toList(), [1, 2, 3, 4, 5, 6]);
      });
    });
  });

  /// every public streaming reader rejects size <= 0 up front,
  /// so a bad batch size can't spin the isolate forever (a tight, await-free loop
  /// that never makes progress and never reaches EOF).
  group('streaming size guard', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('sizeguard_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('CSV readBatches(batchSize: 0) throws at the call', () async {
      final p = '${dir.path}/a.csv';
      File(p).writeAsStringSync('id\n1\n2\n');
      expect(
        () => CsvReader().readBatches(p, batchSize: 0),
        throwsArgumentError,
      );
    });

    test('JSON readBatches(batchSize: 0) throws at the call', () async {
      final p = '${dir.path}/a.jsonl';
      File(p).writeAsStringSync('{"id":1}\n{"id":2}\n');
      expect(
        () => JsonReader(
          const JsonOptions(jsonLines: true),
        ).readBatches(p, batchSize: 0),
        throwsArgumentError,
      );
    });

    test('Parquet readBatches(batchSize: 0) throws at the call', () async {
      final p = '${dir.path}/a.parquet';
      await ParquetWriter(p)
          .write(
            Table(
              name: 't',
              columns: [
                NumericColumn.int64(
                  name: 'id',
                  values: [1, 2],
                  nullable: false,
                ),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'id', dataType: DataType.int64, nullable: false),
                ],
              ),
            ),
          )
          .ok;
      expect(
        () => ParquetReader(p).readBatches(batchSize: 0),
        throwsArgumentError,
      );
      expect(
        () => ParquetReader(p).readBatches(batchSize: -5),
        throwsArgumentError,
      );
    });

    test('Dataset readBatches(batchSize: 0) throws at the call', () async {
      final p = '${dir.path}/a.csv';
      File(p).writeAsStringSync('id\n1\n');
      expect(() => Dataset(p).readBatches(batchSize: 0), throwsArgumentError);
    });
  });

  /// eager materializing APIs are OOM-bounded by default
  /// (opt out with maxRows: null); the shared `sql` singleton has a lock timeout.
  group('eager caps', () {
    Table ids(int n) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(
          name: 'id',
          values: [for (var i = 0; i < n; i++) i],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    test('defaultMaxMaterializedRows is a sane non-null cap', () {
      expect(defaultMaxMaterializedRows, greaterThan(0));
    });

    group('SqlEngine.execute', () {
      test('throws past maxRows, returns full result when null', () async {
        final e = SqlEngine.open().ok;
        e.registerTable('t', ids(100));
        await expectLater(
          e.execute('SELECT * FROM t', maxRows: 10),
          failsWith(Kind.limit),
        );
        final all = await e.execute('SELECT * FROM t', maxRows: null).ok;
        expect(all.rowCount, 100);
      });

      test(
        'throws past maxBytes (memory cap), independent of row count',
        () async {
          final e = SqlEngine.open().ok;
          e.registerTable('t', ids(100)); // 100 int64 rows = ~800 bytes
          await expectLater(
            e.execute('SELECT * FROM t', maxBytes: 64),
            failsWith(Kind.limit),
          );
          final all = await e.execute('SELECT * FROM t', maxBytes: null).ok;
          expect(all.rowCount, 100);
        },
      );
    });

    group('maxBytes overhead factor', () {
      test('eagerReadOverheadFactor is a sane >1 multiplier', () {
        expect(eagerReadOverheadFactor, greaterThan(1.0));
      });

      test('CSV maxBytes applies to the overhead-adjusted size', () async {
        final dir = Directory.systemTemp.createTempSync('capf_');
        addTearDown(() => dir.deleteSync(recursive: true));
        final p = '${dir.path}/n.csv';
        final sb = StringBuffer('id\n');
        for (var i = 0; i < 1000; i++) {
          sb.writeln(i);
        }
        File(p).writeAsStringSync(sb.toString());

        final full = await CsvReader().readFile(p, maxBytes: null).ok;
        final mu = full.memoryUsage;
        // A cap ABOVE raw memoryUsage but BELOW raw×factor is now rejected:
        // the check predicts the transient peak, not just raw buffer bytes.
        await expectLater(
          CsvReader().readFile(p, maxBytes: (mu * 1.5).toInt()),
          failsWith(Kind.limit),
        );
        // Comfortably above raw×factor → still succeeds.
        final ok = await CsvReader().readFile(p, maxBytes: mu * 3).ok;
        expect(ok.rowCount, 1000);
      });
    });

    group('JsonReader.readFile', () {
      late Directory dir;
      setUp(() => dir = Directory.systemTemp.createTempSync('jcap_'));
      tearDown(() => dir.deleteSync(recursive: true));

      test('JSON Lines is bounded by maxRows', () async {
        final p = '${dir.path}/a.jsonl';
        File(p).writeAsStringSync(
          [for (var i = 0; i < 100; i++) '{"id":$i}'].join('\n'),
        );
        await expectLater(
          JsonReader(
            const JsonOptions(jsonLines: true),
          ).readFile(p, maxRows: 10),
          failsWith(Kind.limit),
        );
        final df = await JsonReader(
          const JsonOptions(jsonLines: true),
        ).readFile(p, maxRows: null).ok;
        expect(df.rowCount, 100);
      });

      test('a plain JSON array is streamed and bounded by maxRows', () async {
        final p = '${dir.path}/a.json';
        File(p).writeAsStringSync(
          '[${[for (var i = 0; i < 100; i++) '{"id":$i}'].join(',')}]',
        );
        // Bounded: must throw without buffering the whole array.
        await expectLater(
          JsonReader().readFile(p, maxRows: 10),
          failsWith(Kind.limit),
        );
        // Unbounded opt-out reads it all.
        final df = await JsonReader().readFile(p, maxRows: null).ok;
        expect(df.rowCount, 100);
      });

      test(
        'streaming a plain JSON array round-trips via readBatches',
        () async {
          final p = '${dir.path}/b.json';
          File(p).writeAsStringSync(
            '[{"a":1,"b":"x"},{"a":2,"b":"y"},{"a":3,"b":"z"}]',
          );
          final rows = [
            for (final batch
                in await JsonReader().readBatches(p, batchSize: 2).ok.toList())
              ...batch.toRows(),
          ];
          expect(rows.length, 3);
          expect(rows.map((r) => r['a']), [1, 2, 3]);
          expect(rows.last['b'], 'z');
        },
      );
    });

    group('CsvReader.readFile (now bounded)', () {
      late Directory dir;
      setUp(() => dir = Directory.systemTemp.createTempSync('ccap_'));
      tearDown(() => dir.deleteSync(recursive: true));

      test('bounded by maxRows by default; null reads all', () async {
        final p = '${dir.path}/a.csv';
        File(p).writeAsStringSync(
          'id\n${[for (var i = 0; i < 100; i++) i].join('\n')}\n',
        );
        await expectLater(
          CsvReader().readFile(p, maxRows: 10),
          failsWith(Kind.limit),
        );
        final df = await CsvReader()
            .readFile(p, maxRows: null, maxBytes: null)
            .ok;
        expect(df.rowCount, 100);
      });

      test('custom terminator: byte cap still enforced', () async {
        // A custom line terminator takes the eager whole-file path (readBatches
        // can't stream it); the byte cap must still fire before readAsString.
        final p = '${dir.path}/crlf.csv';
        final body = [for (var i = 0; i < 100; i++) i].join('\r\n');
        File(p).writeAsStringSync('id\r\n$body\r\n');
        const opts = CsvOptions(lineTerminator: '\r\n');
        await expectLater(
          CsvReader(options: opts).readFile(p, maxBytes: 8),
          failsWith(Kind.limit),
        );
        final df = await CsvReader(
          options: opts,
        ).readFile(p, maxRows: null, maxBytes: null).ok;
        expect(df.rowCount, 100);
      });
    });

    group('shared sql function', () {
      test('runs a query and reports a failure as an Err', () async {
        final table = await sql('SELECT 1 AS x').ok;
        expect(table.column('x')!.toList(), [1]);
        await expectLater(
          sql('SELECT * FROM nowhere'),
          failsWith(Kind.missing),
        );
      });
    });
  });

  /// out-of-core I/O: Arrow `readBatches` reads each
  /// record-batch block on demand (not the whole file), and CSV `readFile` honors
  /// an opt-in size ceiling.
  group('out of core', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('ooc_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table batch(int start, int n) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(
          name: 'id',
          values: [for (var i = 0; i < n; i++) start + i],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    group('Arrow lazy readBatches', () {
      test(
        'yields one table per record-batch block with correct data',
        () async {
          final path = '${dir.path}/multi.arrow';
          final w = ArrowStreamWriter(path, batch(0, 1).schema);
          await w.add(batch(0, 100));
          await w.add(batch(100, 100));
          await w.add(batch(200, 50));
          await w.close();

          final tables = await ArrowReader(path).readBatches().toList();
          expect(tables.length, 3); // one per record-batch block
          expect(tables.map((t) => t.rowCount), [100, 100, 50]);
          final all = [
            for (final t in tables) ...t.column('id')!.toList().cast<int>(),
          ];
          expect(all, [for (var i = 0; i < 250; i++) i]);
        },
      );

      test('matches whole-file read()', () async {
        final path = '${dir.path}/cmp.arrow';
        final w = ArrowStreamWriter(path, batch(0, 1).schema);
        await w.add(batch(0, 100));
        await w.add(batch(100, 100));
        await w.close();

        final whole = await ArrowReader(path).read();
        final streamed = [
          for (final t in await ArrowReader(path).readBatches().toList())
            ...t.column('id')!.toList().cast<int>(),
        ];
        expect(streamed, whole.column('id')!.toList());
      });
    });

    group('CSV readFile size guard', () {
      String writeCsv(int rows) {
        final p = '${dir.path}/big.csv';
        File(p).writeAsStringSync(
          'id,v\n${[for (var i = 0; i < rows; i++) '$i,${i * 2}'].join('\n')}\n',
        );
        return p;
      }

      test('throws when the file exceeds maxFileBytes', () async {
        final path = writeCsv(5000);
        final size = File(path).lengthSync();
        await expectLater(
          CsvReader(
            options: CsvOptions(maxFileBytes: size ~/ 2),
          ).readFile(path),
          failsWith(Kind.corrupt),
        );
      });

      test('reads normally when maxFileBytes is unset (default)', () async {
        final path = writeCsv(100);
        final df = await CsvReader().readFile(path).ok;
        expect(df.rowCount, 100);
      });

      test('reads normally when under maxFileBytes', () async {
        final path = writeCsv(100);
        final df = await CsvReader(
          options: const CsvOptions(maxFileBytes: 1 << 20),
        ).readFile(path).ok;
        expect(df.rowCount, 100);
      });
    });
  });

  group('#7 SqlEngine lifecycle (dispose removes spill dir)', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('round42_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('dispose() removes the per-session spill dir', () async {
      final base = Directory('${dir.path}/spill')..createSync();
      final engine = SqlEngine.open(spillDir: base.path).ok;
      final sessions = base.listSync().whereType<Directory>().where(
        (d) => d.path.contains('libbigdata-session-'),
      );
      expect(sessions, isNotEmpty);
      await engine.dispose();
      final after = base.listSync().whereType<Directory>().where(
        (d) => d.path.contains('libbigdata-session-'),
      );
      expect(after, isEmpty);
    });

    test('a non-creatable spill dir fails fast at construction', () {
      // A regular file where a directory is expected: createSync under it throws.
      final file = File('${dir.path}/not-a-dir')..writeAsStringSync('');
      expect(
        SqlEngine.open(spillDir: file.path),
        isA<Err<SqlEngine, ErrorDetails>>().having(
          (err) => err.error.kind,
          'kind',
          Kind.io,
        ),
      );
    });
  });

  group('#7 SqlEngine lifecycle (use-after-dispose, idempotent)', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('round43_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('use-after-dispose throws', () async {
      final engine = SqlEngine.open(spillDir: dir.path).ok;
      await engine.dispose();
      expect(() => engine.execute('SELECT 1'), throwsStateError);
    });

    test('dispose is idempotent', () async {
      final engine = SqlEngine.open(spillDir: dir.path).ok;
      await engine.dispose();
      await engine.dispose(); // no throw on the second call
    });
  });

  group('#10 SqlEngine dispose has a bounded timeout', () {
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

    test('dispose fails fast when an operation is pinned', () async {
      final engine = SqlEngine.open(spillDir: dir.path).ok;
      engine.registerTable('t', ids(List.generate(5000, (i) => i)));
      // Subscribe but pause: the stream suspends at a yield with the operation
      // still in flight, so _active stays > 0.
      final sub = engine.executeStream('SELECT * FROM t').ok.listen((_) {});
      sub.pause();
      await Future<void>.delayed(const Duration(milliseconds: 50));
      await expectLater(
        engine.dispose(timeout: const Duration(milliseconds: 100)),
        failsWith(Kind.timeout),
      );
      await sub.cancel();
    });
  });

  group('#12 spill session marker is recreated', () {
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

    test('a live session that spills re-establishes its .live marker', () async {
      // A small positive threshold enables spilling and trips almost immediately
      // (0 would DISABLE spilling: spills => spillThresholdBytes > 0).
      final engine = SqlEngine.open(
        spillThresholdBytes: 1,
        spillDir: dir.path,
      ).ok;
      final session = Directory(dir.path)
          .listSync()
          .whereType<Directory>()
          .firstWhere((d) => d.path.contains('libbigdata-session-'));
      File('${session.path}/.live').deleteSync(); // simulate a prior sweep
      engine.registerTable(
        't',
        intTable('a', [for (var i = 0; i < 5000; i++) 5000 - i]),
      );
      await engine
          .execute('SELECT a FROM t ORDER BY a')
          .ok; // spills (threshold 0)
      expect(File('${session.path}/.live').existsSync(), isTrue); // recreated
      engine.dispose();
    });
  });

  group('engine lock timeout safety valve', () {
    Table nums() => Table(
      name: 'a',
      columns: [
        NumericColumn.int64(
          name: 'v',
          values: [for (var i = 0; i < 5; i++) i],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [Field(name: 'v', dataType: DataType.int64, nullable: false)],
      ),
    );

    test('an undrained executeStream does not block later queries', () async {
      // The deadlock cause is gone: executeStream releases the engine
      // lock after planning, not after the consumer drains. So an undrained
      // stream no longer strands later queries; they run promptly, without
      // relying on a lockTimeout safety valve. (Richer concurrency/cancel
      // coverage lives in sql_stream_test.dart.)
      final engine = SqlEngine.open().ok;
      engine.registerTable('a', nums());

      final started = Completer<void>();
      late StreamSubscription<Table> sub;
      sub = engine.executeStream('SELECT v FROM a').ok.listen((_) {
        sub.pause(); // never drain, never cancel
        if (!started.isCompleted) started.complete();
      });
      await started.future;

      // A second query runs immediately rather than hanging/timing out.
      final t = await engine
          .execute('SELECT COUNT(*) AS n FROM a')
          .ok
          .timeout(const Duration(seconds: 5));
      expect(t.toRows().single['n'], 5);
      await sub.cancel();
    });
  });

  group('finding 10: logging shutdown cancels subscriptions', () {
    test(
      're-initialize after shutdown does not throw or double-listen',
      () async {
        LoggingConfig.initialize();
        await LoggingConfig.shutdown();
        LoggingConfig.initialize();
        await LoggingConfig.shutdown();
        // No StateError from writing to a closed sink; the re-init succeeded.
      },
    );
  });

  // Queries are serialized because they mutate shared catalog/CTE state, but
  // the registration API sat outside that boundary. A registration landing
  // between an in-flight query's awaits was silently destroyed by the query's
  // CTE teardown, which removed by NAME with no check that the table was still
  // the one it had registered.
  group('catalog registration vs an in-flight query', () {
    late Directory dir;
    late SqlEngine engine;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('catrace_');
      engine = SqlEngine.open(spillDir: dir.path, allowExternalPaths: true).ok;
      engine.registerTable('base', _ints('base', [1, 2, 3]));
    });
    tearDown(() async {
      await engine.dispose();
      dir.deleteSync(recursive: true);
    });

    test('registerTable during a CTE query is not dropped by cleanup', () async {
      // The CTE 'x' materializes from an in-memory table (immediately), then the
      // main query does real file I/O over many rows: a wide window in which
      // the synchronous registration lands AFTER the CTE was registered but
      // BEFORE the query's teardown. 'x' deliberately collides with the CTE.
      final csv = '${dir.path}/big.csv';
      File(
        csv,
      ).writeAsStringSync('m\n${List.generate(20000, (i) => i).join('\n')}\n');
      final query = engine.execute(
        "WITH x AS (SELECT n FROM base) SELECT m FROM '$csv' LIMIT 1",
      );
      await Future<void>.delayed(const Duration(milliseconds: 1));
      engine.registerTable('x', _ints('x', [9]));
      await query;
      expect(engine.listTables(), contains('x'));
      expect(
        (await engine.execute('SELECT n FROM x').ok).column('n')!.toList(),
        [9],
      );
    });

    test(
      'registerFile during a CTE query survives and stays queryable',
      () async {
        // Same wide window: registerFile lands while the main query is still
        // scanning the file, i.e. after the CTE 'x' was registered.
        final big = '${dir.path}/big2.csv';
        File(big).writeAsStringSync(
          'm\n${List.generate(20000, (i) => i).join('\n')}\n',
        );
        final csv = '${dir.path}/x.csv';
        File(csv).writeAsStringSync('n\n7\n');
        final query = engine.execute(
          "WITH x AS (SELECT n FROM base) SELECT m FROM '$big' LIMIT 1",
        );
        await Future<void>.delayed(const Duration(milliseconds: 1));
        await engine.registerFile('x', csv).ok;
        await query;
        expect(engine.listTables(), contains('x'));
        // The LazySource registered alongside the table must still resolve; the
        // old teardown removed the table but stranded the source.
        expect(
          (await engine.execute('SELECT n FROM x').ok).column('n')!.toList(),
          [7],
        );
      },
    );

    test('a CTE still shadows a pre-existing table and restores it', () async {
      // The normal case must be untouched: registered BEFORE the query, the
      // table is shadowed for the query's duration, then restored.
      engine.registerTable('x', _ints('x', [42]));
      final r = await engine
          .execute('WITH x AS (SELECT n FROM base) SELECT n FROM x')
          .ok;
      expect(r.column('n')!.toList(), [1, 2, 3]); // the CTE won
      expect(
        (await engine.execute('SELECT n FROM x').ok).column('n')!.toList(),
        [42], // restored
      );
    });

    test('an ordinary CTE is still torn down', () async {
      await engine
          .execute('WITH tmp AS (SELECT n FROM base) SELECT n FROM tmp')
          .ok;
      expect(engine.listTables(), isNot(contains('tmp')));
    });
  });

  group('SqlEngine reports every failure as an Err', () {
    late SqlEngine engine;
    setUp(() {
      engine = SqlEngine.open(spillThresholdBytes: 0).ok;
      engine.registerTable('t', _ints('t', [1, 2]));
    });
    tearDown(() => engine.dispose());

    for (final (query, kind) in [
      ('SELEC n FROM t', Kind.invalid),
      ("SELECT * FROM '/etc/hosts'", Kind.denied),
      ('SELECT * FROM nowhere', Kind.missing),
      ('SELECT 9223372036854775807 + n FROM t', Kind.invalid),
      (
        'WITH RECURSIVE r AS (SELECT n FROM t) SELECT * FROM r',
        Kind.unsupported,
      ),
    ]) {
      test('$kind: $query', () async {
        await expectLater(engine.execute(query), failsWith(kind));
        await expectLater(engine.executeStream(query).last, failsWith(kind));
      });
    }

    test('a result over maxRows is a limit', () async {
      await expectLater(
        engine.execute('SELECT n FROM t', maxRows: 1),
        failsWith(Kind.limit),
      );
    });

    test('a query past its deadline is a timeout', () async {
      engine.registerTable('a', _ints('a', List.generate(1000, (i) => i)));
      await expectLater(
        engine.execute(
          'SELECT x.n FROM a x, a y, a z',
          timeout: const Duration(milliseconds: 50),
        ),
        failsWith(Kind.timeout),
      );
    });

    test('a runtime StateError keeps the error as its cause', () async {
      final error = await engine
          .execute('SELECT 9223372036854775807 + n FROM t')
          .err;
      expect(error.cause, isA<StateError>());
    });
  });
  group('engine details', () {
    test('file tables are keyed by exact path, table names by any case', () {
      final catalog = TableCatalog();
      final upper = Table.fromRows('u', [
        {'a': 1},
      ]);
      final lower = Table.fromRows('l', [
        {'a': 2},
      ]);
      catalog.registerTable(fileKey('/data/A.csv'), upper);
      catalog.registerTable(fileKey('/data/a.csv'), lower);
      expect(catalog.table(fileKey('/data/A.csv')), same(upper));
      expect(catalog.table(fileKey('/data/a.csv')), same(lower));
      catalog.registerTable('People', upper);
      expect(catalog.table('PEOPLE'), same(upper));
      expect(catalog.table(fileKey('/data/people')), isNull);
    });

    test('an executeStream deadline counts from the call', () async {
      final engine = SqlEngine.open().ok;
      addTearDown(engine.dispose);
      final stream = engine.executeStream(
        'SELECT 1 AS n',
        timeout: const Duration(milliseconds: 20),
      );
      await Future<void>.delayed(const Duration(milliseconds: 60));
      final first = await stream.first;
      expect(first, isA<Err>());
      expect((first as Err).error.kind, Kind.timeout);
    });

    test(
      'executeStream on a disposed engine throws, as execute does',
      () async {
        final engine = SqlEngine.open().ok;
        await engine.dispose();
        expect(() => engine.executeStream('SELECT 1'), throwsStateError);
      },
    );

    test(
      'an engine disposed before the stream is listened to is an Err',
      () async {
        final engine = SqlEngine.open().ok;
        final stream = engine.executeStream('SELECT 1');
        await engine.dispose();
        final first = await stream.first;
        expect(first, isA<Err>());
        expect((first as Err).error.kind, Kind.invalid);
      },
    );
  });
}

Table _ints(String name, List<int> values) => Table(
  name: name,
  columns: [NumericColumn.int64(name: 'n', values: values, nullable: false)],
  schema: Schema(
    fields: [Field(name: 'n', dataType: DataType.int64, nullable: false)],
  ),
);
