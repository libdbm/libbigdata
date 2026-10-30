@Tags(['perf'])
library;

import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:test/test.dart';

import 'support/results.dart';

import '../benchmark/synth.dart';

/// >1M-row profiling tests. They assert CORRECTNESS at scale and print timings;
/// they intentionally do NOT assert absolute time/throughput thresholds (those
/// are machine-dependent). Tagged `perf`: run with:
///   dart test -t perf --run-skipped
void main() {
  const rows = 1200000; // > 1M, crosses the 100k Parquet row-group boundary

  int ms(void Function() body) {
    final sw = Stopwatch()..start();
    body();
    return sw.elapsedMilliseconds;
  }

  Future<int> msAsync(Future<void> Function() body) async {
    final sw = Stopwatch()..start();
    await body();
    return sw.elapsedMilliseconds;
  }

  late Table fact;
  late SqlEngine engine;

  setUp(() {
    fact = synthTable(rows);
    engine = SqlEngine.open(allowExternalPaths: true).ok;
    engine.registerTable('fact', fact);
    engine.registerTable('dim', synthDim(1000));
  });

  test('COUNT(*) over >1M rows', () async {
    late Table r;
    final t = await msAsync(() async {
      r = await engine.execute('SELECT COUNT(*) AS n FROM fact').ok;
    });
    expect(r.column('n')!.toList().first, rows);
    print('count(*): ${t}ms');
  });

  test('filtered count matches a reference', () async {
    final amounts = fact.column('amount')!.toList().cast<double>();
    final expected = amounts.where((a) => a > 500).length;
    late Table r;
    final t = await msAsync(() async {
      r = await engine
          .execute('SELECT COUNT(*) AS n FROM fact WHERE amount > 500')
          .ok;
    });
    expect(r.column('n')!.toList().first, expected);
    print('filter count: ${t}ms ($expected rows)');
  });

  test('GROUP BY: group count + sum total are consistent', () async {
    late Table grouped;
    final t = await msAsync(() async {
      grouped = await engine
          .execute(
            'SELECT category, SUM(amount) AS s FROM fact GROUP BY category',
          )
          .ok;
    });
    expect(grouped.rowCount, categoryCardinality);
    final groupTotal = (grouped.column('s')!.toList().cast<num>()).fold<double>(
      0,
      (a, b) => a + b.toDouble(),
    );
    final global = await engine.execute('SELECT SUM(amount) AS s FROM fact').ok;
    final globalSum = (global.column('s')!.toList().first as num).toDouble();
    expect(groupTotal, closeTo(globalSum, globalSum.abs() * 1e-9 + 1e-6));
    print('group by: ${t}ms (${grouped.rowCount} groups)');
  });

  test('ORDER BY ... LIMIT returns sorted prefix', () async {
    late Table r;
    final t = await msAsync(() async {
      r = await engine
          .execute('SELECT amount FROM fact ORDER BY amount LIMIT 100')
          .ok;
    });
    final got = r.column('amount')!.toList().cast<double>();
    expect(got.length, 100);
    for (int i = 1; i < got.length; i++) {
      expect(got[i] >= got[i - 1], isTrue);
    }
    print('order by limit: ${t}ms');
  });

  test('hash JOIN fact (>1M) x dim', () async {
    // Every fact.key is in [0, 1000) and dim has keys 0..999 -> all rows match.
    late Table r;
    final t = await msAsync(() async {
      r = await engine
          .execute(
            'SELECT COUNT(*) AS n FROM fact f JOIN dim d ON f.key = d.key',
          )
          .ok;
    });
    expect(r.column('n')!.toList().first, rows);
    print('join: ${t}ms');
  });

  test('CTE + view over >1M rows', () async {
    final cte = await engine
        .execute(
          'WITH t AS (SELECT category, amount FROM fact WHERE amount > 100) '
          'SELECT category, COUNT(*) AS n FROM t GROUP BY category',
        )
        .ok;
    expect(cte.rowCount, categoryCardinality);

    await engine
        .execute(
          "CREATE VIEW v AS (SELECT category, amount FROM fact WHERE flag = true)",
        )
        .ok;
    final view = await engine.execute('SELECT COUNT(*) AS n FROM v').ok;
    final flags = fact.column('flag')!.toList().cast<bool>();
    expect(view.column('n')!.toList().first, flags.where((f) => f).length);
  });

  test('Parquet round-trip + pushdown at >1M rows', () async {
    final dir = Directory.systemTemp.createTempSync('perf_pq_');
    try {
      final path = '${dir.path}/fact.parquet';
      final wrote = await msAsync(() => synthParquet(path, rows));
      print('parquet write: ${wrote}ms');

      late Table back;
      final read = await msAsync(() async {
        back = await ParquetReader(path).read().ok;
      });
      expect(back.rowCount, rows);
      print('parquet read: ${read}ms');

      // id is sequential 0..rows-1 -> a high-id filter prunes most row groups.
      ParquetReader.rowGroupsPruned = 0;
      final r = await engine
          .execute(
            "SELECT COUNT(*) AS n FROM '$path' WHERE id >= ${rows - 50000}",
          )
          .ok;
      expect(r.column('n')!.toList().first, 50000);
      expect(ParquetReader.rowGroupsPruned, greaterThan(0));
      print('pushdown pruned ${ParquetReader.rowGroupsPruned} row groups');
    } finally {
      dir.deleteSync(recursive: true);
    }
  });

  test('executeStream over a >1M-row Parquet stays row-complete', () async {
    final dir = Directory.systemTemp.createTempSync('perf_stream_');
    try {
      final path = '${dir.path}/fact.parquet';
      await synthParquet(path, rows);
      var total = 0;
      final t = await msAsync(() async {
        await for (final batch
            in engine.executeStream("SELECT id FROM '$path'").ok) {
          total += batch.rowCount;
        }
      });
      expect(total, rows);
      print('executeStream: ${t}ms');
    } finally {
      dir.deleteSync(recursive: true);
    }
  });

  test('synthesis uses unboxed typed buffers', () {
    final t = ms(() => synthTable(rows));
    expect(fact.rowCount, rows);
    expect(fact.column('id'), isA<NumericColumn>());
    print('synth $rows rows: ${t}ms');
  });

  // Force spill at scale with a small budget (~8 MB << the ~50 MB of data) and
  // confirm results match the in-memory path and nothing OOMs.
  test('GROUP BY spills at >1M rows and matches in-memory', () async {
    Future<List<Map<String, dynamic>>> group(int budget) async {
      final engine = SqlEngine.open(spillThresholdBytes: budget).ok;
      engine.registerTable('fact', synthTable(rows));
      final r = await engine
          .execute(
            'SELECT category, SUM(amount) AS s FROM fact GROUP BY category',
          )
          .ok;
      final out = r.toRows()
        ..sort((a, b) => '${a['category']}'.compareTo('${b['category']}'));
      return out;
    }

    final memory = await group(0);
    late List<Map<String, dynamic>> spilled;
    final t = await msAsync(() async => spilled = await group(8 << 20));
    expect(spilled.length, categoryCardinality);
    expect(
      spilled.map((r) => r.toString()).toList(),
      memory.map((r) => r.toString()).toList(),
    );
    print('spilled GROUP BY ($rows rows): ${t}ms');
  });

  test('hash JOIN spills at >1M rows and matches in-memory', () async {
    Future<int> joinCount(int budget) async {
      final engine = SqlEngine.open(spillThresholdBytes: budget).ok;
      engine.registerTable('fact', synthTable(rows));
      engine.registerTable('dim', synthDim(1000));
      final r = await engine
          .execute(
            'SELECT COUNT(*) AS n FROM fact f JOIN dim d ON f.key = d.key',
          )
          .ok;
      return r.column('n')!.toList().first as int;
    }

    final memory = await joinCount(0);
    late int spilled;
    final t = await msAsync(() async => spilled = await joinCount(8 << 20));
    expect(spilled, memory);
    expect(spilled, rows); // every fact row matches exactly one dim row
    print('spilled JOIN ($rows rows): ${t}ms');
  });

  test('ORDER BY spills at >1M rows and stays sorted', () async {
    final engine = SqlEngine.open(spillThresholdBytes: 8 << 20).ok;
    engine.registerTable('fact', synthTable(rows));
    late Table r;
    final t = await msAsync(() async {
      r = await engine
          .execute('SELECT id, amount FROM fact ORDER BY amount ASC, id ASC')
          .ok;
    });
    expect(r.rowCount, rows);
    final amount = r.column('amount')!;
    final id = r.column('id')!;
    var ok = true;
    for (var i = 1; i < r.rowCount; i++) {
      final pa = (amount[i - 1] as num).toDouble(),
          ca = (amount[i] as num).toDouble();
      if (ca < pa || (ca == pa && (id[i] as int) < (id[i - 1] as int))) {
        ok = false;
        break;
      }
    }
    expect(ok, isTrue, reason: 'spilled ORDER BY must be globally sorted');
    print('spilled ORDER BY ($rows rows): ${t}ms');
  });
}
