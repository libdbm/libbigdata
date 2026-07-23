@Tags(['interop'])
library;

import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:test/test.dart';

/// libbigdata READS Parquet written by DuckDB. DuckDB's level encoder flushes
/// definition levels in fixed bit-packed blocks and pads the final block, which
/// overruns the reader's declared-count guard on a NULLABLE column — a shape the
/// pyarrow/Spark fixtures don't produce. This writes that exact shape with the
/// `duckdb` CLI and asserts a correct read. Gated behind `interop` (needs the
/// duckdb CLI); skips gracefully when it's absent.
void main() {
  bool duckdb() {
    try {
      return Process.runSync('duckdb', ['--version']).exitCode == 0;
    } catch (_) {
      return false;
    }
  }

  group(
    'libbigdata reads DuckDB-written Parquet',
    () {
      late Directory tmp;
      setUp(() => tmp = Directory.systemTemp.createTempSync('ddbread_'));
      tearDown(() => tmp.deleteSync(recursive: true));

      test('a nullable wide-dictionary DOUBLE column (bit-packed def levels)', () async {
        final path = '${tmp.path}/ddb.parquet';
        // Scattered nulls + a wide dictionary over enough rows for DuckDB's default
        // ~122880-value pages, so the def levels are bit-packed with a padded final
        // block — the case that used to throw "RLE run length ... exceeds page
        // value bound".
        final gen = await Process.run('duckdb', [
          '-c',
          // hash()-scattered nulls force BIT-PACKED (not RLE) definition levels;
          // the non-round /7.0 keeps it a genuine DOUBLE (dictionary-encoded).
          "COPY (SELECT i AS id, "
              "CASE WHEN hash(i) % 8 = 0 THEN NULL "
              "ELSE (hash(i) % 10000) / 7.0 END AS score "
              "FROM range(300000) t(i)) "
              "TO '$path' (FORMAT parquet, COMPRESSION snappy);",
        ]);
        expect(gen.exitCode, 0, reason: gen.stderr.toString());

        // Ground truth from DuckDB.
        final truth = await Process.run('duckdb', [
          '-noheader',
          '-list',
          '-c',
          "SELECT round(SUM(score), 4), COUNT(score) "
              "FROM read_parquet('$path');",
        ]);
        final parts = (truth.stdout as String).trim().split('|');
        final expectSum = double.parse(parts[0]);
        final expectCount = int.parse(parts[1]);

        // libbigdata must read it (previously threw) with matching values.
        final table = await ParquetReader(path).read();
        expect(table.rowCount, 300000);
        final score = table.column('score')!;
        num sum = 0;
        var nonNull = 0;
        for (final v in score.toList()) {
          if (v is num) {
            sum += v;
            nonNull++;
          }
        }
        expect(nonNull, expectCount);
        // Loose tolerance: the two engines sum ~262k doubles in different orders,
        // so the ~188M total can differ in the last few ulps.
        expect(sum, closeTo(expectSum, 1.0));
      });
    },
    skip: duckdb() ? false : 'duckdb CLI not installed',
  );
}
