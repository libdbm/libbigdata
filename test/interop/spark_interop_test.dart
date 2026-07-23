@Tags(['interop'])
library;

import 'dart:convert';
import 'dart:io';

import 'package:test/test.dart';

/// real external-engine round-trip compatibility. libbigdata
/// writes sample files (tool/interop/write_samples.dart); Apache Spark reads the
/// Parquet/CSV/JSON, delta-rs reads the Delta table, and pyiceberg reads the
/// Iceberg table (tool/interop/validate_all.py). Gated behind the `interop` tag
/// (needs a local python + spark/delta-rs/pyiceberg) — run with:
///   dart test -t interop --run-skipped
/// Skips gracefully when those engines aren't installed.
void main() {
  late Directory dir;
  late Map<String, dynamic> result;

  setUpAll(() async {
    dir = Directory.systemTemp.createTempSync('libbd_interop_');

    final wrote = await Process.run('dart', [
      'run',
      'tool/interop/write_samples.dart',
      dir.path,
    ]);
    if (wrote.exitCode != 0 || !'${wrote.stdout}'.contains('DONE')) {
      fail('write_samples.dart failed: ${wrote.stdout}\n${wrote.stderr}');
    }

    final read = await Process.run('python3', [
      'tool/interop/validate_all.py',
      dir.path,
    ]);
    final stdout = '${read.stdout}';
    final skip = RegExp(r'^SKIP (.*)$', multiLine: true).firstMatch(stdout);
    if (skip != null) {
      markTestSkipped('interop engines unavailable: ${skip.group(1)}');
      result = const {};
      return;
    }
    final line = RegExp(r'^RESULT (.*)$', multiLine: true).firstMatch(stdout);
    if (line == null) {
      markTestSkipped('python/interop not runnable: ${read.stderr}');
      result = const {};
      return;
    }
    result = jsonDecode(line.group(1)!) as Map<String, dynamic>;
  });

  tearDownAll(() => dir.deleteSync(recursive: true));

  Map<String, dynamic> fmt(String k) {
    if (result.isEmpty) markTestSkipped('interop skipped');
    return (result[k] as Map).cast<String, dynamic>();
  }

  test('Spark reads flat Parquet (all types incl. decimal)', () {
    final r = fmt('parquet_flat');
    expect(r['ok'], isTrue, reason: '${r['error']}');
    final rows = (r['rows'] as List).cast<Map>();
    final byId = {for (final m in rows) m['i64']: m};
    expect(byId[10]!['dec'].toString(), '123.45');
    expect(byId[20]!['dec'].toString(), '-0.99');
    expect(byId[10]!['s'], 'alpha');
  });

  test('Spark reads nested Parquet (list/struct/map)', () {
    final r = fmt('parquet_nested');
    expect(r['ok'], isTrue, reason: '${r['error']}');
    final schema = r['schema'] as String;
    expect(schema, contains('array'));
    expect(schema, contains('struct'));
    expect(schema, contains('map'));
  });

  test('Spark reads multi-level nested Parquet', () {
    final r = fmt('parquet_deep');
    expect(r['ok'], isTrue, reason: '${r['error']}');
    // matrix: array<array<bigint>>; people: array<struct<a,b>>.
    final schema = r['schema'] as String;
    expect(schema, contains('array<array<'));
    final rows = (r['rows'] as List).cast<Map>();
    final byId = {for (final m in rows) m['id']: m};
    expect(byId[1]!['matrix'], [
      [1, 2],
      [3],
    ]);
  });

  test('delta-rs reads partitioned Delta', () {
    final r = fmt('delta_partitioned');
    expect(r['ok'], isTrue, reason: '${r['error']}');
    expect((r['cols'] as List), contains('region')); // partition reconstructed
    expect(r['rows'], 4);
  });

  // expireSnapshots DELETES files. A real engine reading the pruned table is the
  // only proof it deleted the right ones.
  // A struct that is REQUIRED all the way to the leaf carries no definition
  // levels. Writing a zero-width level section anyway shifted every value; our
  // own reader skipped those bytes the same way we wrote them, so only Spark
  // can tell us the bytes are right.
  // Our reader and writer both do 64-bit delta arithmetic, so they agree with
  // each other regardless. pyarrow reads INT32 deltas through a uint32 VLQ.
  test('pyarrow reads int32 DELTA with a delta over 2^31', () {
    final r = fmt('parquet_int32_delta');
    expect(r['ok'], isTrue, reason: '${r['error']}');
    expect(r['type'], 'int32');
    expect(r['v'], [
      -2000000000,
      2000000000,
      -1999999999,
      1999999998,
      -1999999997,
      1999999996,
      -2147483648,
      2147483647,
    ]);
  });

  test('Spark reads an all-required nested struct', () {
    final r = fmt('parquet_required_nested');
    expect(r['ok'], isTrue, reason: '${r['error']}');
    expect(r['a'], [100, 200, 300, 400, 500, 600, 700, 800]);
  });

  test('pyiceberg reads an Iceberg table whose snapshots were expired', () {
    final r = fmt('iceberg_expired');
    expect(r['ok'], isTrue, reason: '${r['error']}');
    expect(r['nsnapshots'], 1); // three appends expired down to one
    expect(r['rows'], 12); // ...but every row of all three still reads
  });

  // A null partition value must reach delta-rs as null, not as the literal text
  // "__HIVE_DEFAULT_PARTITION__", and a date as a real date. Our reader
  // recognised our own sentinel, so the round trip looked fine from inside.
  test('delta-rs reads null and date partition values', () {
    final r = fmt('delta_partition_values');
    expect(r['ok'], isTrue, reason: '${r['error']}');
    expect(r['region'], ['eu', null]);
    expect(r['d'], ['2024-01-15', '2024-01-15']);
  });

  // The manifest of the repartitioned snapshot must declare spec 1; hardcoding 0
  // made pyiceberg project the partition record under the WRONG spec.
  test('pyiceberg reads a repartitioned Iceberg table', () {
    final r = fmt('iceberg_repartitioned');
    expect(r['ok'], isTrue, reason: '${r['error']}');
    expect(r['nspecs'], 2);
    expect(r['default_spec'], 1);
    expect(r['rows'], 3);
    expect(r['country'], ['de', 'fr', 'ca']);
    // Two rows have region='eu'. A manifest mislabelled spec 0 makes pyiceberg
    // read country's value as region and prune them away.
    expect(r['region_eu'], 2);
  });

  test('pyiceberg reads partitioned Iceberg', () {
    final r = fmt('iceberg_partitioned');
    expect(r['ok'], isTrue, reason: '${r['error']}');
    expect(r['nparts'], 1); // one identity partition field
    expect(r['rows'], 4);
  });

  test('Spark reads CSV with scaled decimals', () {
    final r = fmt('csv');
    expect(r['ok'], isTrue, reason: '${r['error']}');
    final decs = (r['rows'] as List).map((m) => (m as Map)['dec'].toString());
    expect(decs, contains('123.45'));
  });

  test('Spark reads JSON Lines (no DateTime crash)', () {
    final r = fmt('json');
    expect(r['ok'], isTrue, reason: '${r['error']}');
  });

  test('delta-rs reads Delta with decimal(10,2)', () {
    final r = fmt('delta');
    expect(r['ok'], isTrue, reason: '${r['error']}');
    expect(r['dec_type'].toString(), contains('decimal128(10, 2)'));
    expect((r['dec'] as List).map((e) => e.toString()), contains('123.45'));
  });

  test('pyiceberg reads Iceberg with decimal(10,2)', () {
    final r = fmt('iceberg');
    expect(r['ok'], isTrue, reason: '${r['error']}');
    expect(r['dec_type'].toString(), contains('decimal128(10, 2)'));
    expect(r['rows'], 4);
  });
}
