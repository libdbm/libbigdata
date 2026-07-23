import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:test/test.dart';

/// Planner group-by-expression + order-by-group-key binding (Round 48 deferred).
void main() {
  late Directory dir;
  late SqlEngine engine;
  setUp(() {
    dir = Directory.systemTemp.createTempSync('gbplan_');
    engine = SqlEngine(spillDir: dir.path);
    engine.registerTable(
      't',
      Table(
        name: 't',
        columns: [
          NumericColumn.int64(
            name: 'k',
            values: [1, 2, 3, 4, 5, 6],
            nullable: false,
          ),
          NumericColumn.int64(
            name: 'v',
            values: [10, 20, 30, 40, 50, 60],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'k', dataType: DataType.int64, nullable: false),
            Field(name: 'v', dataType: DataType.int64, nullable: false),
          ],
        ),
      ),
    );
  });
  tearDown(() async {
    await engine.dispose();
    dir.deleteSync(recursive: true);
  });

  Future<List<Map<String, Object?>>> rows(String sql) async =>
      (await engine.execute(sql)).toRows();

  test('GROUP BY <expr> with the expr in SELECT binds', () async {
    final r = await rows(
      'SELECT k % 2 AS g, COUNT(*) AS c FROM t GROUP BY k % 2',
    );
    final byG = {for (final row in r) row['g']: row['c']};
    expect(byG, {1: 3, 0: 3}); // odd k: 1,3,5 ; even k: 2,4,6
  });

  test('GROUP BY <expr>, SUM over it', () async {
    final r = await rows(
      'SELECT k % 2 AS g, SUM(v) AS s FROM t GROUP BY k % 2',
    );
    final byG = {for (final row in r) row['g']: row['s']};
    expect(byG[1], Decimal(BigInt.from(90), 0)); // 10+30+50
    expect(byG[0], Decimal(BigInt.from(120), 0)); // 20+40+60
  });

  test('ORDER BY a group key NOT in the SELECT list (S3)', () async {
    // region=k, but SELECT only has COUNT(*): ORDER BY k must still bind.
    final r = await rows(
      'SELECT COUNT(*) AS c FROM t GROUP BY k ORDER BY k DESC',
    );
    expect(r.length, 6);
    // 6 singleton groups; the count is 1 each — the assertion is that it BINDS
    // and orders (no "Unknown column: k").
    expect(r.every((row) => row['c'] == 1), isTrue);
    expect(r.first.containsKey('k'), isFalse); // k trimmed from the output
  });

  test('ORDER BY a group EXPRESSION not in SELECT', () async {
    final r = await rows(
      'SELECT COUNT(*) AS c FROM t GROUP BY k % 2 ORDER BY k % 2',
    );
    expect(r.length, 2);
  });

  test('a plain-column GROUP BY + ORDER BY still works (regression)', () async {
    final r = await rows(
      'SELECT k, COUNT(*) AS c FROM t GROUP BY k ORDER BY k',
    );
    expect(r.map((row) => row['k']).toList(), [1, 2, 3, 4, 5, 6]);
  });

  test('ORDER BY an aggregate alias still works', () async {
    final r = await rows(
      'SELECT k % 2 AS g, SUM(v) AS s FROM t GROUP BY k % 2 ORDER BY s DESC',
    );
    expect(
      r.first['s'],
      Decimal(BigInt.from(120), 0),
    ); // even group sums higher
  });

  test('ORDER BY the selected group expression by its alias', () async {
    final r = await rows(
      'SELECT k % 2 AS g, COUNT(*) AS c FROM t GROUP BY k % 2 ORDER BY g',
    );
    expect(r.map((row) => row['g']).toList(), [0, 1]);
  });

  test('HAVING over a plain-column group + aggregate still works', () async {
    final r = await rows(
      'SELECT k, COUNT(*) AS c FROM t GROUP BY k HAVING COUNT(*) >= 1 ORDER BY k',
    );
    expect(r.length, 6);
  });

  test('GROUP BY expr, expr in SELECT, ordered by the expr', () async {
    final r = await rows(
      'SELECT k % 3 AS g, COUNT(*) AS c FROM t GROUP BY k % 3 ORDER BY k % 3',
    );
    expect(r.map((row) => row['g']).toList(), [0, 1, 2]);
  });
}
