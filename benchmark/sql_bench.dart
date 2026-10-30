import 'dart:io';

import 'package:libbigdata/libbigdata.dart';

import 'results.dart';
import 'synth.dart';

/// End-to-end SQL-engine profiler over >1M-row synthesized tables. Covers the
/// streaming and blocking operators (filter, GROUP BY, ORDER BY, JOIN, UNION,
/// CTE, view, multi-file join, Parquet pushdown, executeStream).
///
///   dart run benchmark/sql_bench.dart [rows=1000000] [reps=3]
Future<void> main(List<String> args) async {
  final rows = args.isNotEmpty ? int.parse(args[0]) : 1000000;
  final reps = args.length > 1 ? int.parse(args[1]) : 3;

  final work = Directory('benchmark/work')..createSync(recursive: true);
  final factPath = '${work.path}/fact.parquet';
  final fact2Path = '${work.path}/fact2.parquet';

  stderr.writeln('synthesizing $rows rows (+ parquet)...');
  final engine = SqlEngine.open(allowExternalPaths: true).ok;
  engine.registerTable('fact', synthTable(rows));
  engine.registerTable('dim', synthDim(1000));
  await synthParquet(factPath, rows);
  await synthParquet(fact2Path, rows, seed: 99);

  Future<void> bench(String label, Future<void> Function() op) async {
    final ms = <int>[];
    for (int r = 0; r <= reps; r++) {
      final sw = Stopwatch()..start();
      await op();
      sw.stop();
      if (r > 0) ms.add(sw.elapsedMilliseconds); // discard warm-up
    }
    final m = median(ms);
    final mrps = m == 0 ? double.infinity : rows / m / 1000.0;
    print(
      '${label.padRight(26)} ${m.toString().padLeft(6)} ms   '
      '${mrps.toStringAsFixed(2)} M rows/s',
    );
    print('RESULT libbigdata-sql $label $m $rows');
  }

  Future<void> run(String sql) async {
    await engine.execute(sql).ok; // materialize
  }

  print('=== libbigdata SQL bench: $rows rows, $reps reps (median) ===');

  await bench('count(*)', () => run('SELECT COUNT(*) AS n FROM fact'));
  await bench(
    'filter count',
    () => run('SELECT COUNT(*) AS n FROM fact WHERE amount > 500'),
  );
  await bench(
    'group by',
    () => run(
      'SELECT category, COUNT(*) AS n, SUM(amount) AS s '
      'FROM fact GROUP BY category',
    ),
  );
  await bench(
    'order by limit',
    () => run('SELECT id FROM fact ORDER BY amount LIMIT 100'),
  );
  await bench(
    'hash join',
    () => run('SELECT COUNT(*) AS n FROM fact f JOIN dim d ON f.key = d.key'),
  );
  await bench(
    'union',
    () => run('SELECT key FROM fact UNION SELECT key FROM dim'),
  );
  await bench(
    'upper projection',
    () => run('SELECT UPPER(category) AS u FROM fact'),
  );
  await bench(
    'upper + group by',
    () => run(
      'SELECT u, COUNT(*) AS n FROM (SELECT UPPER(category) AS u FROM fact) AS s '
      'GROUP BY u',
    ),
  );
  await bench(
    'cte + group by',
    () => run(
      'WITH t AS (SELECT category, amount FROM fact WHERE amount > 100) '
      'SELECT category, SUM(amount) AS s FROM t GROUP BY category',
    ),
  );

  await engine
      .execute(
        "CREATE OR REPLACE VIEW v AS (SELECT category, amount FROM fact WHERE flag = true)",
      )
      .ok;
  await bench(
    'view + group by',
    () => run('SELECT category, COUNT(*) AS n FROM v GROUP BY category'),
  );

  await bench(
    'multi-file join',
    () => run(
      "SELECT COUNT(*) AS n FROM '$factPath' a "
      "JOIN '$fact2Path' b ON a.id = b.id",
    ),
  );

  await bench(
    'parquet scan count',
    () => run("SELECT COUNT(*) AS n FROM '$factPath'"),
  );

  // Parquet pushdown: id is sequential, so a high-id filter prunes most groups.
  await bench('parquet pushdown', () async {
    ParquetReader.rowGroupsPruned = 0;
    await engine
        .execute(
          "SELECT COUNT(*) AS n FROM '$factPath' WHERE id >= ${rows - 50000}",
        )
        .ok;
  });
  ParquetReader.rowGroupsPruned = 0;
  ParquetReader.columnsRead = 0;
  await engine
      .execute("SELECT id FROM '$factPath' WHERE id >= ${rows - 50000}")
      .ok;
  print(
    '  pushdown: rowGroupsPruned=${ParquetReader.rowGroupsPruned} '
    'columnsRead=${ParquetReader.columnsRead}',
  );

  // Memory-bounded streaming scan+filter over the >1M-row parquet.
  await bench('executeStream scan', () async {
    var total = 0;
    await for (final batch
        in engine
            .executeStream("SELECT id FROM '$factPath' WHERE amount > 900")
            .ok) {
      total += batch.rowCount;
    }
    if (total < 0) throw StateError('unreachable');
  });

  for (final path in [factPath, fact2Path]) {
    File(path).deleteSync();
  }
}
