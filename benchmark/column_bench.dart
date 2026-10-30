import 'dart:io';

import 'package:libbigdata/libbigdata.dart';

import 'results.dart';

/// Per-column Parquet read times: each column of [path] is read alone (column
/// projection), [reps] times, and the median is reported. Isolates the decode
/// cost of one physical type and encoding, which the whole-file read blurs.
///
///   `dart run benchmark/column_bench.dart <path> [reps]`
Future<void> main(List<String> args) async {
  final path = args[0];
  final reps = args.length > 1 ? int.parse(args[1]) : 5;
  final schema = await ParquetReader(path).schema().ok;
  stdout.writeln('| column | type | nullable | median ms |');
  stdout.writeln('|---|---|---|--:|');
  for (final field in schema.fields) {
    final times = <int>[];
    for (var r = 0; r <= reps; r++) {
      final watch = Stopwatch()..start();
      final table = await ParquetReader(
        path,
        options: ParquetReaderOptions(columns: [field.name]),
      ).read(maxRows: null, maxBytes: null).ok;
      watch.stop();
      if (table.rowCount < 0) throw StateError('unreachable');
      if (r > 0) times.add(watch.elapsedMicroseconds); // first is warm-up
    }
    times.sort();
    final median = times[times.length ~/ 2] / 1000;
    stdout.writeln(
      '| ${field.name} | ${field.dataType.name} | ${field.nullable} | '
      '${median.toStringAsFixed(1)} |',
    );
  }
}
