import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
// CompressionCodec isn't re-exported by the barrel.

import 'results.dart';

/// Intra-library format comparison: round-trip a dataset through CSV, JSON,
/// JSON Lines, Arrow IPC (real Apache Arrow binary) and Parquet, measuring
/// write time, read time and file size.
///
///   dart run benchmark/format_bench.dart [rows] [reps] [csvPath]
Future<void> main(List<String> args) async {
  final csvPath = args.length > 2 ? args[2] : 'benchmark/work/fmt.csv';
  final reps = args.length > 1 ? int.parse(args[1]) : 3;
  final dir = Directory('benchmark/work/fmt');
  if (!dir.existsSync()) dir.createSync(recursive: true);

  // Source data: load the generated CSV once.
  final df = await DataFrameIO.readCsv(csvPath).ok;
  final table = await df.execute().ok;
  final rows = table.rowCount;
  stderr.writeln('loaded $rows rows from $csvPath');

  final results = <_Result>[];

  Future<int> median(Future<void> Function() op) async {
    final times = <int>[];
    for (var i = 0; i < reps; i++) {
      final sw = Stopwatch()..start();
      await op();
      times.add(sw.elapsedMicroseconds);
    }
    times.sort();
    return times[times.length ~/ 2];
  }

  Future<void> bench(
    String name,
    String path,
    Future<void> Function() write,
    Future<int> Function() readCount,
  ) async {
    final w = await median(write);
    final size = File(path).lengthSync();
    int count = 0;
    int r =
        -1; // -1 => read unavailable (e.g. libcompress can't decode it back)
    try {
      r = await median(() async => count = await readCount());
    } catch (_) {
      count = -1;
    }
    results.add(_Result(name, w, r, size, count));
    stderr.writeln('  $name done');
  }

  final csv = '${dir.path}/d.csv';
  await bench(
    'CSV',
    csv,
    () => CsvWriter().writeFile(df.executeStream().ok, csv),
    () async =>
        (await (await DataFrameIO.readCsv(csv).ok).execute().ok).rowCount,
  );

  final json = '${dir.path}/d.json';
  await bench(
    'JSON (array)',
    json,
    () => JsonWriter().writeFile(df.executeStream().ok, json),
    () async =>
        (await (await DataFrameIO.readJson(json).ok).execute().ok).rowCount,
  );

  final jsonl = '${dir.path}/d.jsonl';
  await bench(
    'JSON Lines',
    jsonl,
    () => JsonWriter(
      JsonOptions.jsonLines(),
    ).writeFile(df.executeStream().ok, jsonl),
    () async => (await (await DataFrameIO.readJsonLines(
      jsonl,
    ).ok).execute().ok).rowCount,
  );

  for (final ac in [
    ArrowCompression.none,
    ArrowCompression.lz4,
    ArrowCompression.zstd,
  ]) {
    final label = ac == ArrowCompression.none
        ? 'Arrow IPC'
        : 'Arrow IPC (${ac.name})';
    final arrow = '${dir.path}/d.${ac.name}.arrow';
    await bench(
      label,
      arrow,
      () => df.writeArrow(arrow, compression: ac),
      () async =>
          (await (await DataFrameIO.readArrow(arrow).ok).execute().ok).rowCount,
    );
  }

  for (final codec in [
    ParquetCompression.none,
    ParquetCompression.snappy,
    ParquetCompression.zstd,
  ]) {
    final path = '${dir.path}/d.${codec.name}.parquet';
    await bench(
      'Parquet (${codec.name})',
      path,
      () => ParquetWriter(
        path,
        options: ParquetWriterOptions(compression: codec),
      ).write(table),
      () async => (await ParquetReader(path).read().ok).rowCount,
    );
  }

  _report(rows, reps, results);
}

class _Result {
  final String name;
  final int writeUs;
  final int readUs;
  final int bytes;
  final int rowCount;
  _Result(this.name, this.writeUs, this.readUs, this.bytes, this.rowCount);
}

void _report(int rows, int reps, List<_Result> results) {
  String ms(int us) => us < 0 ? 'n/a' : (us / 1000).toStringAsFixed(1);
  String mb(int b) => (b / (1024 * 1024)).toStringAsFixed(2);
  // rows/s is format-independent (MB/s would penalise compact formats).
  String rps(int us) =>
      us <= 0 ? '-' : (rows / (us / 1e6) / 1e6).toStringAsFixed(1);

  final buf = StringBuffer();
  buf.writeln('# Format comparison ($rows rows, median of $reps)\n');
  buf.writeln(
    '| format | write ms | read ms | size MB | '
    'write M rows/s | read M rows/s |',
  );
  buf.writeln('|---|--:|--:|--:|--:|--:|');
  for (final r in results) {
    buf.writeln(
      '| ${r.name} | ${ms(r.writeUs)} | ${ms(r.readUs)} | '
      '${mb(r.bytes)} | ${rps(r.writeUs)} | ${rps(r.readUs)} |',
    );
  }
  if (results.any((r) => r.readUs < 0)) {
    buf.writeln(
      '\nread n/a = the format could not be decoded back; the '
      'written file is still valid.',
    );
  }
  print(buf);
}
