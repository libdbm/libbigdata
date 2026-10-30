// Regressions from a production-safety review of the lakehouse writers and the
// export/registration APIs.
import 'dart:async';
import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:test/test.dart';

import 'support/results.dart';

/// A coordinator that records it was used, then delegates to the real one.
class _SpyDeltaCoordinator implements DeltaCommitCoordinator {
  final _inner = const FilesystemDeltaCommitCoordinator();
  var used = false;
  @override
  Future<Result<int, ErrorDetails>> commit({
    required DeltaLog log,
    required Future<List<Map<String, dynamic>>> Function(int version) build,
    String label = 'commit',
  }) {
    used = true;
    return _inner.commit(log: log, build: build, label: label);
  }
}

Table _ints(String name, String col, List<int> values) => Table(
  name: name,
  columns: [NumericColumn.int64(name: col, values: values, nullable: false)],
  schema: Schema(
    fields: [Field(name: col, dataType: DataType.int64, nullable: false)],
  ),
);

void main() {
  late Directory dir;
  late SqlEngine engine;
  setUp(() {
    dir = Directory.systemTemp.createTempSync('review_');
    engine = SqlEngine.open().ok;
  });
  tearDown(() async {
    await engine.dispose();
    dir.deleteSync(recursive: true);
  });

  group('F1: DataFrame write forwards a coordinator', () {
    test('writeDelta routes through the passed coordinator', () async {
      engine.registerTable('t', _ints('t', 'a', [1, 2, 3]));
      final df = await engine.sql('SELECT * FROM t').ok;
      final spy = _SpyDeltaCoordinator();
      await df.writeDelta('${dir.path}/tbl', coordinator: spy).ok;
      expect(spy.used, isTrue);
    });
  });

  group('F2: the commit-outcome taxonomy is public', () {
    test('conflict and indeterminate are Kind values a caller can match', () {
      // A commit's outcome is its Result; a caller tells a definitely-failed
      // commit from an unknown one by kind, with no reach into src/.
      String handle(ErrorDetails error) => switch (error.kind) {
        Kind.conflict => 'retry',
        Kind.indeterminate => 'inspect',
        _ => 'fail',
      };
      expect(handle(const ErrorDetails(Kind.conflict, 'lost')), 'retry');
      expect(
        handle(const ErrorDetails(Kind.indeterminate, 'unknown')),
        'inspect',
      );
    });
  });

  group('F3: partition explosion is capped in the grouping', () {
    test('splitByKeys fails at the (cap+1)-th distinct key', () {
      final t = _ints('t', 'k', [0, 1, 2, 3, 4]);
      expect(
        () => t.splitByKeys(['k'], maxPartitions: 2).toList(),
        throwsFailure(Kind.limit),
      );
    });

    test('splitByKeys within the cap yields all partitions', () {
      final t = _ints('t', 'k', [0, 1, 0, 1]);
      expect(t.splitByKeys(['k'], maxPartitions: 5).length, 2);
    });

    test('a Delta partitioned write over the cap fails loud', () async {
      final t = Table.fromRows('t', [
        for (int i = 0; i < 5; i++) {'id': i, 'v': i},
      ]);
      await expectLater(
        DeltaWriter(
          '${dir.path}/d',
        ).write(t, partitionBy: ['id'], maxPartitions: 2),
        failsWith(Kind.limit),
      );
    });

    test('an Iceberg partitioned write over the cap fails loud', () async {
      final t = Table.fromRows('t', [
        for (int i = 0; i < 5; i++) {'id': i, 'v': i},
      ]);
      await expectLater(
        IcebergWriter(
          '${dir.path}/i',
        ).write(t, partitionBy: ['id'], maxPartitions: 2),
        failsWith(Kind.limit),
      );
    });
  });

  group('F4: export/registration APIs cap materialization', () {
    test('toCsv over the cap fails loud; null opts out', () async {
      engine.registerTable(
        'b',
        _ints('b', 'n', [for (int i = 0; i < 10; i++) i]),
      );
      final df = await engine.sql('SELECT * FROM b').ok;
      await expectLater(df.toCsv(maxRows: 3), failsWith(Kind.limit));
      final ok = await (await engine.sql('SELECT * FROM b').ok)
          .toCsv(maxRows: null)
          .ok;
      expect(ok.trim().split('\n').length, 11); // header + 10
    });

    test('toJson over the cap fails loud', () async {
      engine.registerTable(
        'b',
        _ints('b', 'n', [for (int i = 0; i < 10; i++) i]),
      );
      final df = await engine.sql('SELECT * FROM b').ok;
      await expectLater(df.toJson(maxRows: 3), failsWith(Kind.limit));
    });

    test(
      'a result read from a registered DataFrame over its cap fails loud',
      () async {
        engine.registerTable(
          'b',
          _ints('b', 'n', [for (int i = 0; i < 10; i++) i]),
        );
        final df = await engine.sql('SELECT * FROM b').ok;
        await engine.registerDataFrame('r', df, maxRows: 3).ok;
        await expectLater(
          engine.execute('SELECT * FROM r'),
          failsWith(Kind.limit),
        );
      },
    );

    test('writeArrowStream streams and round-trips', () async {
      engine.registerTable(
        'b',
        _ints('b', 'n', [for (int i = 0; i < 100; i++) i]),
      );
      final df = await engine.sql('SELECT * FROM b').ok;
      final path = '${dir.path}/s.arrows';
      await df.writeArrowStream(path, batchSize: 16).ok;
      final back = await DataFrameIO.readArrowStream(path).ok;
      final table = await back.execute().ok;
      expect(table.rowCount, 100);
    });
  });

  group('F6: Iceberg refuses to publish unreadable metadata', () {
    test('a commit whose metadata exceeds the ceiling fails loud', () async {
      final t = _ints('t', 'a', [1, 2, 3]);
      // A tiny ceiling makes even the first commit's metadata too large.
      final w = IcebergWriter('${dir.path}/i', maxMetadataBytes: 100);
      await expectLater(w.write(t), failsWith(Kind.limit));
    });

    test('under the ceiling the table stays readable', () async {
      final t = _ints('t', 'a', [1, 2, 3]);
      await IcebergWriter('${dir.path}/i2').write(t).ok; // default ceiling
      final back = await IcebergTable('${dir.path}/i2').read().ok;
      expect(back.rowCount, 3);
    });

    test('expireSnapshots accepts the recovery override', () async {
      final path = '${dir.path}/i3';
      final w = IcebergWriter(path);
      await w.write(_ints('t', 'a', [1])).ok;
      await w.write(_ints('t', 'a', [2])).ok;
      // Threaded through readMetadataJson → current → expireSnapshots.
      final expired = await w
          .expireSnapshots(retain: 1, allowOversizedMetadata: true)
          .ok;
      expect(expired, greaterThanOrEqualTo(0));
    });

    test('readMetadataJson allowOversized reads a normal file', () async {
      final path = '${dir.path}/i4';
      await IcebergWriter(path).write(_ints('t', 'a', [1])).ok;
      final meta = await const FilesystemIcebergCatalog().current(
        Directory(path).absolute.path,
        allowOversized: true,
      );
      expect(meta, isNotNull);
    });
  });

  group('F7: Delta orphan-cleanup callback', () {
    test('a successful write does not fire onCleanupFailure', () async {
      var fired = false;
      final w = DeltaWriter(
        '${dir.path}/d',
        onCleanupFailure: (_) => fired = true,
      );
      await w.write(_ints('t', 'a', [1, 2, 3])).ok;
      expect(fired, isFalse);
    });

    test('a terminal failure with a clean cleanup does not fire it', () async {
      // Append a mismatched schema → terminal schema Err → cleanup runs and
      // (on a writable dir) succeeds, so the callback must NOT fire.
      final path = '${dir.path}/d2';
      var fired = false;
      final w = DeltaWriter(path, onCleanupFailure: (_) => fired = true);
      await w.write(_ints('t', 'a', [1])).ok;
      await expectLater(
        w.write(
          Table.fromRows('t', [
            {'b': 1},
          ]),
        ),
        failsWith(Kind.schema),
      );
      expect(fired, isFalse); // cleanup succeeded → no orphan reported
    });
  });
}
