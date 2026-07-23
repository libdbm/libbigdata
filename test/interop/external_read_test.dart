@Tags(['interop'])
library;

import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/formats/avro/avro_ocf_reader.dart';
import 'package:libbigdata/src/formats/scan.dart';
import 'package:libbigdata/src/core/conversions.dart';
import 'package:test/test.dart';

/// the REVERSE direction — external engines (pyarrow, Spark, delta-rs,
/// pyiceberg, fastavro) WRITE each format and libbigdata READS it, asserting
/// values. Gated behind the `interop` tag (needs local engines); run with:
///   dart test -t interop --run-skipped
void main() {
  late Directory dir;
  late Set<String> wrote;

  setUpAll(() async {
    dir = Directory.systemTemp.createTempSync('libbd_extread_');
    final r = await Process.run('python3', [
      'tool/interop/gen_external.py',
      dir.path,
    ]);
    final out = '${r.stdout}';
    if (!out.contains('DONE')) {
      markTestSkipped('gen_external.py not runnable: ${r.stderr}');
      wrote = {};
      return;
    }
    wrote = RegExp(
      r'^WROTE (\w+)$',
      multiLine: true,
    ).allMatches(out).map((m) => m.group(1)!).toSet();
  });

  tearDownAll(() => dir.deleteSync(recursive: true));

  void need(String artifact) {
    if (!wrote.contains(artifact)) markTestSkipped('$artifact not generated');
  }

  String firstFile(String sub, String ext) => Directory('${dir.path}/$sub')
      .listSync(recursive: true)
      .whereType<File>()
      .firstWhere((f) => f.path.endsWith(ext))
      .path;

  test(
    'libbigdata reads pyarrow nested Parquet (element named "item")',
    () async {
      need('pa_nested');
      final t = await ParquetReader('${dir.path}/pa_nested.parquet').read();
      final rows = t.toRows();
      expect(rows.map((r) => r['tags']), [
        [1, 2],
        [],
        [9],
      ]);
      expect(rows[0]['info'], {'a': 1, 'b': 'x'});
      expect(rows[0]['props'], {'k1': 1});
    },
  );

  test('libbigdata reads pyarrow BYTE_STREAM_SPLIT floats/doubles', () async {
    need('pa_bss');
    final t = await ParquetReader('${dir.path}/pa_bss.parquet').read();
    expect(t.column('f')!.toList().cast<double>(), [
      closeTo(1.5, 1e-6),
      closeTo(2.5, 1e-6),
      closeTo(3.5, 1e-6),
    ]);
    expect(t.column('g')!.toList(), [1.25, 2.5, 3.75]);
    // nullable BSS column exercises the present-count path.
    expect(t.column('h')!.toList(), [
      closeTo(1.5, 1e-6),
      isNull,
      closeTo(3.5, 1e-6),
    ]);
  });

  test('libbigdata reads pyarrow unsigned ints (positive)', () async {
    need('pa_uint');
    final t = await ParquetReader('${dir.path}/pa_uint.parquet').read();
    expect(t.column('u32')!.toList().first, 4000000000);
  });

  test('stats pruning keeps a uint32 row group with high-bit values', () async {
    // The spec stores UINT_* stats as raw physical bytes in unsigned order;
    // decoding them signed made the min/max read NEGATIVE, so a filter on the
    // high range "proved" no match and silently dropped every row (audit 7).
    need('pa_uint');
    final t = await openScan(
      '${dir.path}/pa_uint.parquet',
    ).read(filter: RowFilter.compare('u32', FilterOp.eq, 3000000000));
    expect(t.column('u32')!.toList(), contains(3000000000));
    final ge = await openScan(
      '${dir.path}/pa_uint.parquet',
    ).read(filter: RowFilter.compare('u32', FilterOp.ge, 4000000000));
    expect(ge.column('u32')!.toList(), contains(4000000000));
  });

  test('libbigdata reads Spark nested + INT96 Parquet', () async {
    need('spark_nested');
    final t = await ParquetReader(firstFile('spark_nested', '.parquet')).read();
    final byId = {for (final r in t.toRows()) r['id']: r};
    expect(byId[1]!['tags'], [1, 2]);
    expect(byId[1]!['info'], {'a': 1, 'b': 'x'});
    expect(byId[1]!['ts'], isA<DateTime>());
  });

  test('libbigdata reads delta-rs partitioned Delta', () async {
    need('drs_delta');
    final t = await (await DataFrameIO.readDelta(
      '${dir.path}/drs_delta',
    )).execute();
    expect(
      {for (final r in t.toRows()) r['id']: r['region']},
      {1: 'us', 2: 'eu', 3: 'us', 4: 'eu'},
    );
  });

  test(
    'libbigdata reads pyiceberg typed Iceberg (decimal+timestamp)',
    () async {
      need('pyice');
      final meta = firstFile('pyice', '.metadata.json');
      final tablePath = Directory(
        meta,
      ).parent.parent.path; // .../t (above metadata/)
      final t = await (await DataFrameIO.readIceberg(tablePath)).execute();
      final amts = t.column('amt')!.toList();
      expect(amts.first, BigInt.from(12345)); // 123.45 unscaled
      expect(t.column('ts')!.toList().first, isA<DateTime>());
    },
  );

  test('libbigdata reads fastavro logical-typed OCF', () async {
    need('avro');
    final rows = AvroOcfReader.read(
      File('${dir.path}/logical.avro').readAsBytesSync(),
    );
    final r = rows.first;
    expect(r['d'], isA<DateTime>());
    expect(r['ts'], isA<DateTime>());
    expect(formatDecimal(r['amt'] as BigInt, 2), '123.45');
  });
}
