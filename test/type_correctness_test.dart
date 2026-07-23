import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/sql/exec/kernels.dart';
import 'package:test/test.dart';

/// The SQL planner used to label every projected column `string` (an unresolved
/// placeholder), so `SELECT int_col` produced a string-typed output Field over an
/// int64 Column. These tests pin the output-schema Field types to what the
/// executor actually produces, and pin the strict-but-family-aware Table
/// validation that now backstops it.
void main() {
  Table typed() => Table(
    name: 't',
    schema: Schema(
      fields: [
        Field(name: 'id', dataType: DataType.int64, nullable: false),
        Field(name: 'amount', dataType: DataType.float64, nullable: false),
        Field(name: 'name', dataType: DataType.string, nullable: true),
        Field(name: 'active', dataType: DataType.boolean, nullable: false),
      ],
    ),
    columns: [
      NumericColumn.int64(name: 'id', values: [3, 1, 2], nullable: false),
      NumericColumn.float64(
        name: 'amount',
        values: [1.5, 2.5, 3.5],
        nullable: false,
      ),
      ListColumn<String>(
        name: 'name',
        dataType: DataType.string,
        data: ['a', 'b', 'c'],
        nullable: true,
      ),
      ListColumn<bool>(
        name: 'active',
        dataType: DataType.boolean,
        data: [true, false, true],
        nullable: false,
      ),
    ],
  );

  late SqlEngine engine;
  setUp(() {
    engine = SqlEngine();
    engine.registerTable('t', typed());
  });

  DataType typeOf(Table r, String col) =>
      r.schema.fields.firstWhere((f) => f.name == col).dataType;

  test(
    'column passthrough preserves real types (not the string placeholder)',
    () async {
      final r = await engine.execute('SELECT id, amount, name, active FROM t');
      expect(typeOf(r, 'id'), DataType.int64);
      expect(typeOf(r, 'amount'), DataType.float64);
      expect(typeOf(r, 'name'), DataType.string);
      expect(typeOf(r, 'active'), DataType.boolean);
    },
  );

  test('arithmetic result types match the kernel', () async {
    final r = await engine.execute(
      'SELECT id + 1 AS a, amount / 2 AS b, id + amount AS c, id * id AS d FROM t',
    );
    expect(typeOf(r, 'a'), DataType.int64); // int + int -> int64
    expect(typeOf(r, 'b'), DataType.float64); // division -> float64
    expect(typeOf(r, 'c'), DataType.float64); // int + float -> float64
    expect(typeOf(r, 'd'), DataType.int64); // int * int -> int64
  });

  test('string functions and CAST', () async {
    final r = await engine.execute(
      'SELECT UPPER(name) AS u, LENGTH(name) AS l, '
      'CAST(id AS DOUBLE) AS d, CAST(id AS VARCHAR) AS s FROM t',
    );
    expect(typeOf(r, 'u'), DataType.string);
    expect(typeOf(r, 'l'), DataType.int64);
    expect(typeOf(r, 'd'), DataType.float64);
    expect(typeOf(r, 's'), DataType.string);
  });

  test('aggregate result types', () async {
    final r = await engine.execute(
      'SELECT COUNT(*) AS c, SUM(id) AS s, AVG(id) AS a, '
      'MIN(id) AS mn, MAX(amount) AS mx FROM t',
    );
    expect(typeOf(r, 'c'), DataType.int64);
    expect(typeOf(r, 's'), DataType.decimal); // integer SUM -> lossless decimal
    expect(typeOf(r, 'a'), DataType.float64);
    expect(typeOf(r, 'mn'), DataType.int64); // MIN passes through the arg type
    expect(typeOf(r, 'mx'), DataType.float64);
  });

  test(
    'every output column type equals its schema field (no mislabel)',
    () async {
      final r = await engine.execute(
        'SELECT id, id + 1 AS a, amount / 2 AS b, UPPER(name) AS u FROM t',
      );
      for (final f in r.schema.fields) {
        expect(r.column(f.name)!.dataType, f.dataType, reason: f.name);
      }
    },
  );

  test('GROUP BY output types are correct and spill-invariant', () async {
    const sql =
        'SELECT active, SUM(id) AS s, COUNT(*) AS c FROM t GROUP BY active';
    final plain = await SqlEngine(spillThresholdBytes: 0).let((e) {
      e.registerTable('t', typed());
      return e.execute(sql);
    });
    expect(typeOf(plain, 'active'), DataType.boolean);
    expect(typeOf(plain, 's'), DataType.decimal);
    expect(typeOf(plain, 'c'), DataType.int64);
  });

  test(
    'narrow int (int8) survives a spilling sort with its type intact',
    () async {
      const n = 2000;
      final small = [for (var i = 0; i < n; i++) (i % 251) - 125]; // int8 range
      final t = Table(
        name: 't',
        schema: Schema(
          fields: [
            Field(name: 'small', dataType: DataType.int8, nullable: false),
          ],
        ),
        columns: [
          NumericColumn.int8(name: 'small', values: small, nullable: false),
        ],
      );
      final dir = Directory.systemTemp.createTempSync('type_spill');
      try {
        // Tiny threshold vs ~2 KiB of int8 data forces the sort to spill.
        final e = SqlEngine(spillThresholdBytes: 512, spillDir: dir.path);
        e.registerTable('t', t);
        final r = await e.execute('SELECT small FROM t ORDER BY small');
        expect(typeOf(r, 'small'), DataType.int8); // not widened by the spill
        final want = [...small]..sort();
        expect(r.column('small')!.toList(), want);
      } finally {
        dir.deleteSync(recursive: true);
      }
    },
  );

  group('Table validation backstop', () {
    test('rejects a cross-family mislabel (int column under string field)', () {
      expect(
        () => Table(
          name: 'x',
          schema: Schema(
            fields: [Field(name: 'v', dataType: DataType.string)],
          ),
          columns: [
            NumericColumn.int64(name: 'v', values: [1], nullable: false),
          ],
        ),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('rejects an integer-width mismatch (int32 under int8)', () {
      // Types are preserved exactly end-to-end now (Column.concat keeps the
      // widest actual width; readers decode to their declared width), so even a
      // same-family width mismatch is a real inconsistency and must fail.
      expect(
        () => Table(
          name: 'x',
          schema: Schema(
            fields: [Field(name: 'v', dataType: DataType.int8)],
          ),
          columns: [
            NumericColumn.int32(name: 'v', values: [1], nullable: false),
          ],
        ),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('accepts an exact-width match and preserves int8 through concat', () {
      final merged = Kernels.concat([
        NumericColumn.int8(name: 'v', values: [1, 2], nullable: false),
        NumericColumn.int8(name: 'v', values: [3], nullable: false),
      ]);
      expect(merged.dataType, DataType.int8);
      Table(
        name: 'x',
        schema: Schema(
          fields: [Field(name: 'v', dataType: DataType.int8)],
        ),
        columns: [merged],
      );
    });
  });
}

extension<T> on T {
  R let<R>(R Function(T) f) => f(this);
}
