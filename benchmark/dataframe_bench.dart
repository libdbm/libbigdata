import 'dart:io';

import 'package:libbigdata/libbigdata.dart';

import 'results.dart';
import 'synth.dart';

/// DataFrame pipeline profiler: in-memory pipelines over a synthesized table
/// and Parquet pipelines over `benchmark/work/gen.snappy.parquet` (when it is
/// present; `gen_data.dart` writes it). The Phase 19 baseline: the same
/// pipelines must not get slower on in-memory data once DataFrame runs
/// through the SQL planner, and should get faster on selective file scans.
///
///   dart run benchmark/dataframe_bench.dart [rows=1000000] [reps=3]
Future<void> main(List<String> args) async {
  final rows = args.isNotEmpty ? int.parse(args[0]) : 1000000;
  final reps = args.length > 1 ? int.parse(args[1]) : 3;

  Future<void> bench(
    String label,
    int count,
    Future<void> Function() op,
  ) async {
    final times = <int>[];
    for (var run = 0; run <= reps; run++) {
      final watch = Stopwatch()..start();
      await op();
      watch.stop();
      if (run > 0) times.add(watch.elapsedMilliseconds); // discard warm-up
    }
    final middle = median(times);
    final rate = middle == 0 ? double.infinity : count / middle / 1000.0;
    print(
      '${label.padRight(30)} ${middle.toString().padLeft(6)} ms   '
      '${rate.toStringAsFixed(2)} M rows/s',
    );
    print('RESULT libbigdata-dataframe $label $middle $count');
  }

  Future<void> run(DataFrame frame) async {
    await frame.execute(maxRows: null, maxBytes: null).ok; // materialize
  }

  stderr.writeln('synthesizing $rows rows...');
  final fact = DataFrame.fromTable(synthTable(rows));
  final dim = DataFrame.fromTable(synthDim(1000));

  print('=== libbigdata DataFrame bench: $rows rows, $reps reps (median) ===');

  await bench(
    'filter + select',
    rows,
    () => run(fact.filter('amount > 500').select(['id', 'amount'])),
  );
  await bench(
    'groupBy + agg',
    rows,
    () => run(fact.groupBy(['category']).agg({'amount': 'sum', 'id': 'count'})),
  );
  await bench(
    'orderBy + limit',
    rows,
    () => run(fact.orderBy('amount').limit(100)),
  );
  await bench(
    'join',
    rows,
    () => run(fact.join(dim, 'key').select(['id', 'label'])),
  );
  await bench(
    'withColumn',
    rows,
    () => run(fact.withColumn('twice', 'amount * 2').select(['id', 'twice'])),
  );

  const path = 'benchmark/work/gen.snappy.parquet';
  if (!File(path).existsSync()) {
    stderr.writeln('$path not found; skipping the Parquet pipelines');
    return;
  }
  // Each run opens the file, as each SQL query over a path does.
  Future<DataFrame> open() => DataFrameIO.readParquet(path).ok;
  final size = await (await open()).count().ok;

  await bench(
    'parquet filter + select',
    size,
    () async =>
        run((await open()).filter('amount > 9000').select(['id', 'amount'])),
  );
  await bench(
    'parquet groupBy + agg',
    size,
    () async => run(
      (await open()).groupBy(['category']).agg({
        'amount': 'sum',
        'quantity': 'max',
      }),
    ),
  );
  await bench(
    'parquet orderBy + limit',
    size,
    () async =>
        run((await open()).orderBy('amount', ascending: false).limit(100)),
  );
}
