@Tags(['interop'])
library;

import 'dart:convert';
import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/formats/avro/avro_ocf_reader.dart';
import 'package:libbigdata/src/formats/scan.dart';
import 'package:libbigdata/src/core/conversions.dart';
import 'package:test/test.dart';

import '../support/results.dart';

/// the REVERSE direction: external engines (pyarrow, Spark, delta-rs,
/// pyiceberg, fastavro) WRITE each format and libbigdata READS it, asserting
/// values. Tagged `interop` (needs local engines); run with:
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
      final t = await ParquetReader('${dir.path}/pa_nested.parquet').read().ok;
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
    final t = await ParquetReader('${dir.path}/pa_bss.parquet').read().ok;
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
    final t = await ParquetReader('${dir.path}/pa_uint.parquet').read().ok;
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
    final t = await ParquetReader(
      firstFile('spark_nested', '.parquet'),
    ).read().ok;
    final byId = {for (final r in t.toRows()) r['id']: r};
    expect(byId[1]!['tags'], [1, 2]);
    expect(byId[1]!['info'], {'a': 1, 'b': 'x'});
    expect(byId[1]!['ts'], isA<DateTime>());
  });

  test('libbigdata reads delta-rs partitioned Delta', () async {
    need('drs_delta');
    final t = await (await DataFrameIO.readDelta(
      '${dir.path}/drs_delta',
    ).ok).execute().ok;
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
      final t = await (await DataFrameIO.readIceberg(
        tablePath,
      ).ok).execute().ok;
      final amts = t.column('amt')!.toList();
      expect(amts.first, BigInt.from(12345)); // 123.45 unscaled
      expect(t.column('ts')!.toList().first, isA<DateTime>());
    },
  );

  test(
    'a column dropped and re-added reads NULL for older files (pyiceberg)',
    () async {
      // The old files hold a physical column `a` under the DROPPED field id;
      // the re-added `a` has a new id, so pyiceberg reads NULL for them.
      // Matching by name attached the dropped column's data to the new one.
      need('pyice_readd');
      final table = Directory(
        firstFile('pyice_readd', '.metadata.json'),
      ).parent.parent.path;
      final frame = await DataFrameIO.readIceberg(table).ok;
      final t = await frame.execute().ok;
      expect(
        {for (final r in t.toRows()) r['id']: r['a']},
        {1: null, 2: null, 3: 'new3'},
      );
      final hits = await frame.filter("a = 'old1'").execute().ok;
      expect(hits.rowCount, 0);
    },
  );

  test('a file without field ids reads through the name mapping', () async {
    // pyiceberg added a plain Parquet file (column `a`, no field ids), then
    // renamed `a` to `b`; schema.name-mapping.default maps both names to the
    // field. Matching by the current name read NULL.
    need('pyice_mapped');
    final table = Directory(
      firstFile('pyice_mapped', '.metadata.json'),
    ).parent.parent.path;
    // add_files leaves the file where it was, outside the table directory.
    final t = await IcebergTable(table, allowExternalPaths: true).read().ok;
    expect({for (final r in t.toRows()) r['id']: r['b']}, {1: 'x', 2: 'y'});
  });

  test('pyiceberg reads a tag after libbigdata expires snapshots', () async {
    // The tag names the first snapshot, which an overwrite then replaced.
    // Expiry used to drop it and delete its files, leaving the tag pointing
    // at nothing. pyiceberg's overwrite commits two snapshots (delete, then
    // append); those two expire.
    need('pyice_tag');
    final table = Directory(
      firstFile('pyice_tag', '.metadata.json'),
    ).parent.parent.path;
    expect(await IcebergWriter(table).expireSnapshots(retain: 1).ok, 2);
    final r = await Process.run('python3', [
      '-c',
      '''
import glob, json, sys
from pyiceberg.table import StaticTable
latest = sorted(glob.glob(sys.argv[1] + "/metadata/*.metadata.json"))[-1]
table = StaticTable.from_metadata(latest)
tag = table.metadata.refs["v1"].snapshot_id
print(json.dumps({
  "current": sorted(table.scan().to_arrow()["id"].to_pylist()),
  "tag": sorted(table.scan(snapshot_id=tag).to_arrow()["id"].to_pylist()),
}))
''',
      table,
    ]);
    expect(r.exitCode, 0, reason: '${r.stderr}');
    expect(jsonDecode('${r.stdout}'), {
      'current': [2, 3],
      'tag': [1],
    });
  });

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
