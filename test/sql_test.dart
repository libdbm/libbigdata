import 'dart:typed_data';
import 'dart:async';
import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/sql/operators/spill.dart';
import 'package:libbigdata/src/sql/planner/physical_plan.dart';
import 'package:test/test.dart';

/// Leaked `spill-*` files under [d] (recursive). Each SqlEngine now spills into
/// a per-session subdir with a `.live` marker (expected infrastructure), so a
/// clean query leaves the subdir but no spill files.
List<FileSystemEntity> _leakedSpills(Directory d) => d
    .listSync(recursive: true)
    .where((f) => f is File && f.uri.pathSegments.last.startsWith('spill-'))
    .toList();

void main() {
  group('joins', () {
    Table t(List<int> ids, String pfx) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: ids, nullable: false),
        ListColumn<String>(
          name: 'name',
          dataType: DataType.string,
          data: [for (final i in ids) '$pfx$i'],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'name', dataType: DataType.string, nullable: false),
        ],
      ),
    );

    late Directory dir;
    late String a, b, c;
    late SqlEngine engine;

    setUp(() async {
      dir = Directory.systemTemp.createTempSync('sqljoin_');
      a = '${dir.path}/a.parquet';
      b = '${dir.path}/b.parquet';
      c = '${dir.path}/c.parquet';
      await ParquetWriter(a).write(t([1, 2, 3], 'a'));
      await ParquetWriter(b).write(t([2, 3, 4], 'b'));
      await ParquetWriter(c).write(t([3, 4, 5], 'c'));
      engine = SqlEngine(allowExternalPaths: true);
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Future<List<Map<String, dynamic>>> rows(String sql) async =>
        (await engine.execute(sql)).toRows();

    group('SKIP file-read error typing (finding 11)', skip: true, () {});
    group('_unused', () {
      test('a missing file surfaces FileSystemException', () async {
        await expectLater(
          engine.execute("SELECT * FROM '${dir.path}/missing.parquet'"),
          throwsA(isA<FileSystemException>()),
        );
      });

      test('a malformed file surfaces FormatException', () async {
        final bad = '${dir.path}/bad.parquet';
        await File(bad).writeAsBytes(List.filled(64, 0x00));
        await expectLater(
          engine.execute("SELECT * FROM '$bad'"),
          throwsA(isA<FormatException>()),
        );
      });
    });

    group('JOIN over file paths (#multi-file FROM)', () {
      test('qualified columns from both sides resolve correctly', () async {
        final r = await engine.execute(
          "SELECT a.name, b.name FROM '$a' a JOIN '$b' b ON a.id = b.id",
        );
        expect(r.schema.fieldNames, ['a.name', 'b.name']);
        expect(r.toRows(), [
          {'a.name': 'a2', 'b.name': 'b2'},
          {'a.name': 'a3', 'b.name': 'b3'},
        ]);
      });

      test('SELECT * keeps both ON keys and qualifies collisions', () async {
        final r = await engine.execute(
          "SELECT * FROM '$a' a JOIN '$b' b ON a.id = b.id",
        );
        expect(r.schema.fieldNames, ['a.id', 'a.name', 'b.id', 'b.name']);
        expect(r.rowCount, 2);
      });

      test('reversed ON order gives the same result', () async {
        final r = await rows(
          "SELECT a.name FROM '$a' a JOIN '$b' b ON b.id = a.id",
        );
        expect(r, [
          {'name': 'a2'},
          {'name': 'a3'},
        ]);
      });

      test('WHERE on the left side filters only that side', () async {
        final r = await rows(
          "SELECT a.name FROM '$a' a JOIN '$b' b ON a.id=b.id WHERE a.name='a2'",
        );
        expect(r, [
          {'name': 'a2'},
        ]);
      });

      test('a bare ambiguous column throws', () {
        expect(
          engine.execute("SELECT name FROM '$a' a JOIN '$b' b ON a.id=b.id"),
          throwsA(isA<StateError>()),
        );
      });

      test(
        'LEFT JOIN keeps unmatched left rows with null right columns',
        () async {
          final r = await rows(
            "SELECT a.id, b.name FROM '$a' a LEFT JOIN '$b' b ON a.id=b.id",
          );
          final m = {for (final row in r) row['id'] as int: row['name']};
          expect(m, {1: null, 2: 'b2', 3: 'b3'});
        },
      );

      test('self-join of one file aliased twice', () async {
        final r = await rows(
          "SELECT a.name, b.name FROM '$a' a JOIN '$a' b ON a.id=b.id",
        );
        expect(r.length, 3);
        expect(r.first, {'a.name': 'a1', 'b.name': 'a1'});
      });

      test('three-way join sharing the key name', () async {
        final r = await rows(
          "SELECT a.name, b.name, c.name FROM '$a' a JOIN '$b' b ON a.id=b.id "
          "JOIN '$c' c ON b.id=c.id",
        );
        expect(r, [
          {'a.name': 'a3', 'b.name': 'b3', 'c.name': 'c3'},
        ]);
      });

      test(
        'two files sharing a basename (different dirs) join under aliases',
        () async {
          final d1 = Directory('${dir.path}/d1')..createSync();
          final d2 = Directory('${dir.path}/d2')..createSync();
          await ParquetWriter(
            '${d1.path}/data.parquet',
          ).write(t([1, 2, 3], 'x'));
          await ParquetWriter(
            '${d2.path}/data.parquet',
          ).write(t([2, 3, 4], 'y'));
          final r = await engine.execute(
            "SELECT p.name, q.name FROM '${d1.path}/data.parquet' p "
            "JOIN '${d2.path}/data.parquet' q ON p.id = q.id",
          );
          expect(r.schema.fieldNames, ['p.name', 'q.name']);
          expect(r.toRows(), [
            {'p.name': 'x2', 'q.name': 'y2'},
            {'p.name': 'x3', 'q.name': 'y3'},
          ]);
        },
      );

      test('USING (id) yields a single key column', () async {
        final r = await engine.execute(
          "SELECT * FROM '$a' a JOIN '$b' b USING (id)",
        );
        expect(r.schema.fieldNames.where((n) => n == 'id').length, 1);
        expect(r.rowCount, 2);
      });
    });

    group('comma cross-join (#multi-file FROM)', () {
      test('comma FROM is a cartesian product', () async {
        final r = await engine.execute("SELECT * FROM '$a', '$b'");
        expect(r.rowCount, 9);
        expect(r.schema.fieldNames, ['a.id', 'a.name', 'b.id', 'b.name']);
      });

      test('comma FROM with a WHERE behaves like an equi-join', () async {
        final r = await rows(
          "SELECT a.name, b.name FROM '$a' a, '$b' b WHERE a.id = b.id",
        );
        expect(r, [
          {'a.name': 'a2', 'b.name': 'b2'},
          {'a.name': 'a3', 'b.name': 'b3'},
        ]);
      });
    });

    group('regression: single-table queries unchanged', () {
      test('single file SELECT * keeps bare names', () async {
        final r = await engine.execute("SELECT * FROM '$a'");
        expect(r.schema.fieldNames, ['id', 'name']);
        expect(r.rowCount, 3);
      });

      test('single file qualified and bare refs both work', () async {
        final r = await rows("SELECT a.id, name FROM '$a' a WHERE id > 1");
        expect(r, [
          {'id': 2, 'name': 'a2'},
          {'id': 3, 'name': 'a3'},
        ]);
      });
    });
  });

  group('block nested loop join', () {
    Table hot(int n) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(
          name: 'k',
          values: [for (var i = 0; i < n; i++) 0], // one hot key
          nullable: false,
        ),
        NumericColumn.int64(
          name: 'v',
          values: [for (var i = 0; i < n; i++) i],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'k', dataType: DataType.int64, nullable: false),
          Field(name: 'v', dataType: DataType.int64, nullable: false),
        ],
      ),
    );

    // One matching row (key 0), one non-matching (key 5), one null key.
    Table probe() => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'k', values: [0, 5, null], nullable: true),
        NumericColumn.int64(
          name: 'v',
          values: [100, 101, 102],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'k', dataType: DataType.int64, nullable: true),
          Field(name: 'v', dataType: DataType.int64, nullable: false),
        ],
      ),
    );

    test(
      'hot-key join hits the bounded block-nested-loop and matches in-memory',
      () async {
        final dir = Directory.systemTemp.createTempSync('joinbnl_default_');
        addTearDown(() => dir.deleteSync(recursive: true));
        final right = hot(80000); // ~1.28 MB single-key build > 1 MB floor
        final left = probe();

        Future<List<List<Object?>>> run(String type, int budget) async {
          final e = budget == 0
              ? SqlEngine(spillThresholdBytes: 0)
              : SqlEngine(spillThresholdBytes: budget, spillDir: dir.path);
          e.registerTable('l', left);
          e.registerTable('r', right);
          final t = await e.execute(
            'SELECT l.k AS lk, l.v AS lv, r.v AS rv '
            'FROM l $type JOIN r ON l.k = r.k',
          );
          final rows = t
              .toRows()
              .map((r) => [r['lk'], r['lv'], r['rv']])
              .toList();
          rows.sort((a, b) => a.toString().compareTo(b.toString()));
          return rows;
        }

        for (final type in ['INNER', 'LEFT']) {
          final inMemory = await run(type, 0);
          final bnl = await run(type, 256 << 10); // 256 KB → grace + cap + BNL
          expect(bnl, inMemory, reason: '$type join under BNL must match');
        }

        final leftover = dir
            .listSync()
            .where((f) => f.path.contains('spill-'))
            .toList();
        expect(leftover, isEmpty);
      },
    );
  });

  group('multi-key joins', () {
    late SqlEngine engine;

    setUp(() {
      engine = SqlEngine();
      engine.registerTable(
        'o',
        Table.fromRows('o', [
          {'a': 1, 'b': 10, 'v': 'x'},
          {'a': 1, 'b': 20, 'v': 'y'},
          {'a': 2, 'b': 10, 'v': 'z'},
        ]),
      );
      engine.registerTable(
        'p',
        Table.fromRows('p', [
          {'a': 1, 'b': 10, 'w': 'P'},
          {'a': 2, 'b': 10, 'w': 'Q'},
          {'a': 1, 'b': 99, 'w': 'R'},
        ]),
      );
    });

    tearDown(() => engine.dispose());

    test('an ANDed ON matches on the composite key', () async {
      // Only (1,10) and (2,10) exist on both sides; (1,20) and (1,99) must not
      // cross-match on a single column.
      final r = await engine.execute(
        'SELECT o.v, p.w FROM o JOIN p ON o.a = p.a AND o.b = p.b',
      );
      expect(r.toRows(), [
        {'v': 'x', 'w': 'P'},
        {'v': 'z', 'w': 'Q'},
      ]);
    });

    test('a reversed ANDed ON binds each key to its own side', () async {
      final r = await engine.execute(
        'SELECT o.v FROM o JOIN p ON p.a = o.a AND p.b = o.b',
      );
      expect(r.toRows(), [
        {'v': 'x'},
        {'v': 'z'},
      ]);
    });

    test('multi-column USING coalesces the shared keys', () async {
      final r = await engine.execute('SELECT * FROM o JOIN p USING (a, b)');
      expect(r.schema.fieldNames, ['a', 'b', 'v', 'w']);
      expect(r.toRows(), [
        {'a': 1, 'b': 10, 'v': 'x', 'w': 'P'},
        {'a': 2, 'b': 10, 'v': 'z', 'w': 'Q'},
      ]);
    });

    test('a LEFT join on a composite key nulls the unmatched side', () async {
      final r = await engine.execute(
        'SELECT o.v, p.w FROM o LEFT JOIN p ON o.a = p.a AND o.b = p.b '
        'ORDER BY o.v',
      );
      expect(r.toRows(), [
        {'v': 'x', 'w': 'P'},
        {'v': 'y', 'w': null},
        {'v': 'z', 'w': 'Q'},
      ]);
    });

    test('a composite-key join survives a spill', () async {
      // A tiny positive threshold forces grace partitioning; 0 would DISABLE
      // spilling (`spills` is `spillThresholdBytes > 0`).
      final spilling = SqlEngine(spillThresholdBytes: 256);
      try {
        spilling.registerTable(
          'o',
          Table.fromRows('o', [
            for (int i = 0; i < 500; i++) {'a': i, 'b': i % 7, 'v': 'v$i'},
          ]),
        );
        spilling.registerTable(
          'p',
          Table.fromRows('p', [
            for (int i = 0; i < 500; i++) {'a': i, 'b': i % 7, 'w': 'w$i'},
          ]),
        );
        final r = await spilling.execute(
          'SELECT COUNT(*) AS n FROM o JOIN p ON o.a = p.a AND o.b = p.b',
        );
        expect(r.toRows(), [
          {'n': 500},
        ]);
      } finally {
        await spilling.dispose();
      }
    });

    test('a non-equi join condition fails loud', () async {
      await expectLater(
        engine.execute('SELECT * FROM o JOIN p ON o.a = p.a AND o.b > p.b'),
        throwsA(isA<UnsupportedError>()),
      );
    });
  });

  group('decimal join keys', () {
    // A decimal column stores an UNSCALED BigInt with its scale on the Field, so
    // the SAME value has a different BigInt per scale: 1.5 is 15@scale1 but
    // 150@scale2. Joining on the raw BigInt therefore both MISSED equal values
    // and MATCHED unequal ones — 2.5@scale1 and 0.25@scale2 are both BigInt(25),
    // a false match the join silently emitted.
    Table decimals(String name, List<int> unscaled, int scale) => Table(
      name: name,
      columns: [
        ListColumn<BigInt>(
          name: 'd',
          dataType: DataType.decimal,
          data: [for (final u in unscaled) BigInt.from(u)],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(
            name: 'd',
            dataType: DataType.decimal,
            nullable: false,
            precision: 10,
            scale: scale,
          ),
        ],
      ),
    );

    late SqlEngine engine;

    setUp(() {
      engine = SqlEngine();
      // t: 1.5, 2.5 (scale 1)   u: 1.50, 0.25 (scale 2)
      // The only true match is 1.5 = 1.50.
      engine.registerTable('t', decimals('t', [15, 25], 1));
      engine.registerTable('u', decimals('u', [150, 25], 2));
    });

    tearDown(() => engine.dispose());

    test('a decimal join matches by VALUE across differing scales', () async {
      final r = await engine.execute('SELECT t.d FROM t JOIN u ON t.d = u.d');
      expect(r.rowCount, 1);
      expect(r.toRows(), [
        {'d': Decimal(BigInt.from(15), 1)},
      ]);
    });

    test('decimal column vs int column compares exactly above 2^53', () async {
      final big = BigInt.parse('9007199254740993'); // 2^53 + 1
      final w = Table(
        name: 'w',
        columns: [
          ListColumn<BigInt>(
            name: 'd',
            dataType: DataType.decimal,
            data: [big, big + BigInt.one], // 2^53+1, 2^53+2
            nullable: false,
          ),
          NumericColumn.int64(
            name: 'i',
            values: [big.toInt(), big.toInt()], // both 2^53+1
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(
              name: 'd',
              dataType: DataType.decimal,
              nullable: false,
              precision: 20,
              scale: 0,
            ),
            Field(name: 'i', dataType: DataType.int64, nullable: false),
          ],
        ),
      );
      final e = SqlEngine();
      e.registerTable('w', w);
      // Only row 0 (d == i) matches. The lossy double path rounds 2^53+1 to
      // 2^53 and would return NO match (false negative).
      final r = await e.execute('SELECT d FROM w WHERE d = i');
      await e.dispose();
      expect(r.rowCount, 1);
      expect(r.toRows(), [
        {'d': Decimal(big, 0)},
      ]);
    });

    test(
      'a decimal join does not match unequal values sharing a BigInt',
      () async {
        // 2.5 (25@1) must NOT match 0.25 (25@2) — the false-positive direction,
        // which an ANTI join would turn into an emitted row that should be gone.
        final r = await engine.execute(
          "SELECT t.d FROM t JOIN u ON t.d = u.d WHERE t.d > 2",
        );
        expect(r.rowCount, 0);
      },
    );

    test('a decimal join key survives a spill', () async {
      // Grace partitioning hashes the key: routing and matching must
      // canonicalize identically, or equal values land in different partitions
      // and are never compared at all. A tiny positive threshold forces it (0
      // would DISABLE spilling).
      final spilling = SqlEngine(spillThresholdBytes: 256);
      try {
        spilling.registerTable('t', decimals('t', [15, 25], 1));
        spilling.registerTable('u', decimals('u', [150, 25], 2));
        final r = await spilling.execute(
          'SELECT t.d FROM t JOIN u ON t.d = u.d',
        );
        expect(r.rowCount, 1);
        expect(r.toRows(), [
          {'d': Decimal(BigInt.from(15), 1)},
        ]);
      } finally {
        await spilling.dispose();
      }
    });
  });

  group('mixed int/decimal reconciliation', () {
    Table ints(String name, List<int> vs) => Table(
      name: name,
      columns: [NumericColumn.int64(name: 'd', values: vs, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'd', dataType: DataType.int64, nullable: false)],
      ),
    );
    Table decs(String name, List<int> unscaled, int scale) => Table(
      name: name,
      columns: [
        ListColumn<BigInt>(
          name: 'd',
          dataType: DataType.decimal,
          data: [for (final u in unscaled) BigInt.from(u)],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(
            name: 'd',
            dataType: DataType.decimal,
            nullable: false,
            precision: 10,
            scale: scale,
          ),
        ],
      ),
    );

    late SqlEngine engine;
    setUp(() => engine = SqlEngine());
    tearDown(() => engine.dispose());

    test(
      'an int = decimal join matches by value (was: every match dropped)',
      () async {
        engine.registerTable('a', ints('a', [5, 6]));
        engine.registerTable('b', decs('b', [500], 2)); // 5.00
        final r = await engine.execute('SELECT a.d FROM a JOIN b ON a.d = b.d');
        expect(r.rowCount, 1);
        expect(r.toRows(), [
          {'d': 5},
        ]);
      },
    );

    test(
      'int = decimal does not falsely match a differently-scaled value',
      () async {
        engine.registerTable('a', ints('a', [5]));
        engine.registerTable('b', decs('b', [5], 2)); // 0.05, not 5
        final r = await engine.execute('SELECT a.d FROM a JOIN b ON a.d = b.d');
        expect(r.rowCount, 0);
      },
    );

    test(
      'int UNION decimal keeps distinct values (was: 5 and 0.05 collided)',
      () async {
        engine.registerTable('i', ints('i', [5]));
        engine.registerTable('d', decs('d', [5], 2)); // 0.05
        final r = await engine.execute('SELECT d FROM i UNION SELECT d FROM d');
        expect(r.rowCount, 2);
        expect(r.toRows().map((e) => e['d']).toSet(), {
          Decimal(BigInt.from(500), 2),
          Decimal(BigInt.from(5), 2),
        });
      },
    );

    test('int UNION decimal dedups an equal value to one row', () async {
      engine.registerTable('i', ints('i', [5]));
      engine.registerTable('d', decs('d', [500], 2)); // 5.00
      final r = await engine.execute('SELECT d FROM i UNION SELECT d FROM d');
      expect(r.rowCount, 1);
      expect(r.toRows(), [
        {'d': Decimal(BigInt.from(500), 2)},
      ]);
    });

    test(
      'int > 2^24 UNION float32 fails loud (was: silent truncation)',
      () async {
        engine.registerTable(
          'i',
          Table(
            name: 'i',
            columns: [
              NumericColumn.int32(
                name: 'x',
                values: [16777217],
                nullable: false,
              ),
            ],
            schema: Schema(
              fields: [
                Field(name: 'x', dataType: DataType.int32, nullable: false),
              ],
            ),
          ),
        );
        engine.registerTable(
          'f',
          Table(
            name: 'f',
            columns: [
              NumericColumn.float32(name: 'x', values: [1.5], nullable: false),
            ],
            schema: Schema(
              fields: [
                Field(name: 'x', dataType: DataType.float32, nullable: false),
              ],
            ),
          ),
        );
        await expectLater(
          engine.execute('SELECT x FROM i UNION SELECT x FROM f'),
          throwsA(isA<ArgumentError>()),
        );
      },
    );
  });

  group('SQL semantics (phase 3)', () {
    late SqlEngine engine;
    Table ints(String n, List<int> vs) => Table(
      name: n,
      columns: [NumericColumn.int64(name: 'n', values: vs, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'n', dataType: DataType.int64, nullable: false)],
      ),
    );
    setUp(() {
      engine = SqlEngine();
      engine.registerTable('a', ints('a', [1, 2, 3]));
      engine.registerTable('b', ints('b', [1]));
      engine.registerTable('c', ints('c', [2]));
      engine.registerTable('u1', ints('u1', [3, 1]));
      engine.registerTable('u2', ints('u2', [2, 4]));
      engine.registerTable(
        't',
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'id', values: [1, 2, 3], nullable: false),
            ListColumn<int>(
              name: 'amt',
              dataType: DataType.int64,
              data: [30, null, 10],
            ),
            ListColumn<bool>(
              name: 'flag',
              dataType: DataType.boolean,
              data: [true, false, null],
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
              Field(name: 'amt', dataType: DataType.int64, nullable: true),
              Field(name: 'flag', dataType: DataType.boolean, nullable: true),
            ],
          ),
        ),
      );
    });
    tearDown(() => engine.dispose());

    Future<List> ns(String sql) async =>
        (await engine.execute(sql)).toRows().map((r) => r['n']).toList();
    Future<List> ids(String sql) async =>
        (await engine.execute(sql)).toRows().map((r) => r['id']).toList();

    test('EXCEPT chains are left-associative', () async {
      // (a EXCEPT b) EXCEPT c = {3}, not a EXCEPT (b EXCEPT c) = {2,3}.
      final r = await ns(
        'SELECT n FROM a EXCEPT SELECT n FROM b EXCEPT SELECT n FROM c',
      );
      expect(r..sort(), [3]);
    });

    test('INTERSECT binds tighter than UNION', () async {
      // a UNION (b INTERSECT c) = {1,2,3} ∪ {} = {1,2,3}.
      final r = await ns(
        'SELECT n FROM a UNION SELECT n FROM b INTERSECT SELECT n FROM c',
      );
      expect(r..sort(), [1, 2, 3]);
    });

    test('ORDER BY after a set operation sorts the whole result', () async {
      expect(await ns('SELECT n FROM u1 UNION SELECT n FROM u2 ORDER BY n'), [
        1,
        2,
        3,
        4,
      ]);
    });

    test('ORDER BY + LIMIT after a set operation apply to the union', () async {
      expect(
        await ns(
          'SELECT n FROM u1 UNION SELECT n FROM u2 ORDER BY n DESC LIMIT 2',
        ),
        [4, 3],
      );
    });

    test('IS TRUE / IS FALSE are 2-valued (a NULL is not TRUE)', () async {
      expect(await ids('SELECT id FROM t WHERE flag IS TRUE'), [1]);
      expect(await ids('SELECT id FROM t WHERE flag IS FALSE'), [2]);
      expect((await ids('SELECT id FROM t WHERE flag IS NOT TRUE'))..sort(), [
        2,
        3,
      ]);
      expect((await ids('SELECT id FROM t WHERE flag IS NOT FALSE'))..sort(), [
        1,
        3,
      ]);
      // IS NULL / IS NOT NULL still work.
      expect(await ids('SELECT id FROM t WHERE flag IS NULL'), [3]);
    });

    test('ORDER BY a unary/CASE/CAST over a non-selected column', () async {
      expect(await ids('SELECT id FROM t ORDER BY -id'), [3, 2, 1]);
      expect(
        await ids(
          'SELECT id FROM t ORDER BY CASE WHEN id=2 THEN 0 ELSE id END',
        ),
        [2, 1, 3],
      );
      expect(await ids('SELECT id FROM t ORDER BY CAST(amt AS DOUBLE)'), [
        3,
        1,
        2,
      ]);
    });

    test('NULLS FIRST / LAST control null ordering (ASC and DESC)', () async {
      expect(await ids('SELECT id FROM t ORDER BY amt NULLS FIRST'), [2, 3, 1]);
      expect(await ids('SELECT id FROM t ORDER BY amt NULLS LAST'), [3, 1, 2]);
      // DESC: NULL placement is INDEPENDENT of direction (it used to invert).
      expect(await ids('SELECT id FROM t ORDER BY amt DESC NULLS LAST'), [
        1,
        3,
        2,
      ]);
      expect(await ids('SELECT id FROM t ORDER BY amt DESC NULLS FIRST'), [
        2,
        1,
        3,
      ]);
      // Defaults (Postgres/DuckDB): NULLS LAST for ASC, NULLS FIRST for DESC.
      expect(await ids('SELECT id FROM t ORDER BY amt'), [3, 1, 2]);
      expect(await ids('SELECT id FROM t ORDER BY amt DESC'), [2, 1, 3]);
    });

    test('two unaliased computed columns get distinct names', () async {
      final r = await engine.execute('SELECT n + 1, n * 10 FROM a');
      expect(r.schema.fieldNames.toSet().length, 2);
    });

    test(
      'COALESCE into a string renders non-string branches to string',
      () async {
        final r = await engine.execute(
          "SELECT COALESCE(CAST(amt AS VARCHAR), 'none') AS r FROM t",
        );
        expect(r.schema.field('r')!.dataType, DataType.string);
        expect(r.toRows().every((e) => e['r'] is String), isTrue);
      },
    );
  });

  group('skew join', () {
    Table build(int n, int keyMod) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(
          name: 'k',
          values: [for (var i = 0; i < n; i++) i % keyMod],
          nullable: false,
        ),
        NumericColumn.int64(
          name: 'v',
          values: [for (var i = 0; i < n; i++) i],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'k', dataType: DataType.int64, nullable: false),
          Field(name: 'v', dataType: DataType.int64, nullable: false),
        ],
      ),
    );

    // A probe table mixing rows that match the hot key (0), a non-matching key
    // (5, absent from the single-key build), and null keys — to exercise both the
    // matched path and the unmatched-left pass of the block-nested-loop fallback.
    Table probe() => Table(
      name: 't',
      columns: [
        NumericColumn.int64(
          name: 'k',
          values: [0, 0, 0, 5, 5, null, null],
          nullable: true,
        ),
        NumericColumn.int64(
          name: 'v',
          values: [10, 11, 12, 13, 14, 15, 16],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'k', dataType: DataType.int64, nullable: true),
          Field(name: 'v', dataType: DataType.int64, nullable: false),
        ],
      ),
    );

    test('single-hot-key partition at the recursion cap uses the bounded '
        'block-nested-loop and matches in-memory for every join type', () async {
      final dir = Directory.systemTemp.createTempSync('joinbnl_');
      // The build (right) side is ONE key repeated: salted repartitioning can
      // never split it, so it recurses to the cap. ~80k rows × 16 bytes ≈ 1.28
      // MB > the 1 MB build floor, forcing the streaming fallback (not a
      // full-partition concatAll, which would OOM under a real hot key).
      final right = build(80000, 1); // all k == 0
      final left = probe();

      Future<List<List<Object?>>> run(String type, int budget) async {
        final e = budget == 0
            ? SqlEngine(spillThresholdBytes: 0)
            : SqlEngine(spillThresholdBytes: budget, spillDir: dir.path);
        e.registerTable('l', left);
        e.registerTable('r', right);
        final t = await e.execute(
          'SELECT l.k AS lk, l.v AS lv, r.v AS rv '
          'FROM l $type JOIN r ON l.k = r.k',
        );
        // Row order differs (BNL groups by build block), so sort before compare.
        final rows = t
            .toRows()
            .map((r) => [r['lk'], r['lv'], r['rv']])
            .toList();
        rows.sort((a, b) => a.toString().compareTo(b.toString()));
        return rows;
      }

      for (final type in ['INNER', 'LEFT', 'RIGHT', 'FULL']) {
        final inMemory = await run(type, 0);
        final bnl = await run(type, 256 << 10); // 256 KB → grace + cap + BNL
        expect(
          bnl,
          inMemory,
          reason: '$type join under BNL must match in-memory',
        );
      }

      final leftover = dir
          .listSync()
          .where((f) => f.path.contains('spill-'))
          .toList();
      expect(leftover, isEmpty); // BNL sub-partitions cleaned up
      dir.deleteSync(recursive: true);
    });

    test(
      'skewed grace join recursively repartitions and matches in-memory',
      () async {
        final dir = Directory.systemTemp.createTempSync('joinskew_');
        // ~2.4M build rows over few keys → a 16-way partition still exceeds 1 MB,
        // forcing the recursive (salted) re-partition path.
        final left = build(2400000, 64);
        final right = build(2000, 64);

        Future<int> run(int budget) async {
          // This skew stress-join legitimately streams well over 10M matched
          // pairs into a COUNT, so opt out of the service-safe caps (incl. the
          // new equi-join maxOutputRows guard) that would otherwise reject it.
          final e = budget == 0
              ? SqlEngine(spillThresholdBytes: 0, unbounded: true)
              : SqlEngine(
                  spillThresholdBytes: budget,
                  spillDir: dir.path,
                  unbounded: true,
                );
          e.registerTable('l', left);
          e.registerTable('r', right);
          return (await e.execute(
                'SELECT COUNT(*) AS n FROM l JOIN r ON l.k = r.k',
              )).column('n')!.toList().first
              as int;
        }

        final inMemory = await run(0);
        final spilled = await run(1 << 20); // 1 MB budget → grace + recursion
        expect(spilled, inMemory);

        final leftover = dir
            .listSync()
            .where((f) => f.path.contains('spill-'))
            .toList();
        expect(leftover, isEmpty); // recursive sub-partitions cleaned up
        dir.deleteSync(recursive: true);
      },
    );
  }, tags: 'perf');

  group('join spill', () {
    Table left(int n) {
      final id = <int>[];
      final k = <int?>[];
      final lv = <String>[];
      var x = 11;
      for (var i = 0; i < n; i++) {
        x = (x * 1103515245 + 12345) & 0x7fffffff;
        id.add(i);
        k.add(x % 7 == 0 ? null : x % 40); // ~14% null keys
        lv.add('l${x % 5}');
      }
      return Table(
        name: 'l',
        columns: [
          NumericColumn.int64(name: 'id', values: id, nullable: false),
          NumericColumn.int64(name: 'k', values: k, nullable: true),
          ListColumn<String>(
            name: 'lv',
            dataType: DataType.string,
            data: lv,
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'id', dataType: DataType.int64, nullable: false),
            Field(name: 'k', dataType: DataType.int64, nullable: true),
            Field(name: 'lv', dataType: DataType.string, nullable: false),
          ],
        ),
      );
    }

    Table right(int n) {
      final rk = <int?>[];
      final rv = <String>[];
      var x = 99;
      for (var i = 0; i < n; i++) {
        x = (x * 1103515245 + 12345) & 0x7fffffff;
        rk.add(x % 9 == 0 ? null : x % 50);
        rv.add('r${x % 6}');
      }
      return Table(
        name: 'r',
        columns: [
          NumericColumn.int64(name: 'rk', values: rk, nullable: true),
          ListColumn<String>(
            name: 'rv',
            dataType: DataType.string,
            data: rv,
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'rk', dataType: DataType.int64, nullable: true),
            Field(name: 'rv', dataType: DataType.string, nullable: false),
          ],
        ),
      );
    }

    late Directory spillDir;
    const ln = 3000, rn = 2000;

    setUp(() => spillDir = Directory.systemTemp.createTempSync('joinspill_'));
    tearDown(() => spillDir.deleteSync(recursive: true));

    Future<List<String>> run(int budget, String sql) async {
      final engine = budget == 0
          ? SqlEngine(spillThresholdBytes: 0)
          : SqlEngine(spillThresholdBytes: budget, spillDir: spillDir.path);
      engine.registerTable('l', left(ln));
      engine.registerTable('r', right(rn));
      final rows = (await engine.execute(sql)).toRows();
      return rows.map((r) => r.toString()).toList()..sort();
    }

    // Tiny budget => multi-partition grace join on both sides.
    Future<void> matchesInMemory(String sql) async {
      final memory = await run(0, sql);
      final spilled = await run(2048, sql);
      expect(spilled.length, memory.length, reason: 'row count for: $sql');
      expect(spilled, memory, reason: sql);
    }

    group('grace hash join matches in-memory', () {
      const select = 'SELECT l.id, l.k, r.rk, r.rv FROM l';
      const on = 'ON l.k = r.rk';
      test('inner', () => matchesInMemory('$select JOIN r $on'));
      test('left', () => matchesInMemory('$select LEFT JOIN r $on'));
      test('right', () => matchesInMemory('$select RIGHT JOIN r $on'));
      test('full', () => matchesInMemory('$select FULL JOIN r $on'));
    });

    test('USING (k) dedups the key column under spill', () async {
      // Both sides share key column `k`, so USING coalesces it to one column.
      Table dim() => Table(
        name: 'd',
        columns: [
          NumericColumn.int64(
            name: 'k',
            values: [for (var i = 0; i < 50; i++) i],
            nullable: false,
          ),
          ListColumn<String>(
            name: 'dv',
            dataType: DataType.string,
            data: [for (var i = 0; i < 50; i++) 'd$i'],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'k', dataType: DataType.int64, nullable: false),
            Field(name: 'dv', dataType: DataType.string, nullable: false),
          ],
        ),
      );
      List<String> sorted(Table t) =>
          t.toRows().map((r) => r.toString()).toList()..sort();

      final mem = SqlEngine(spillThresholdBytes: 0)
        ..registerTable('l', left(ln))
        ..registerTable('d', dim());
      final spill =
          SqlEngine(spillThresholdBytes: 2048, spillDir: spillDir.path)
            ..registerTable('l', left(ln))
            ..registerTable('d', dim());
      const sql = 'SELECT * FROM l JOIN d USING (k)';
      final memRows = sorted(await mem.execute(sql));
      final spillRows = sorted(await spill.execute(sql));
      expect(
        (await spill.execute(
          sql,
        )).schema.fieldNames.where((n) => n == 'k').length,
        1,
      );
      expect(spillRows, memRows);
    });

    test(
      'build side larger than budget spills to multiple partitions',
      () async {
        // 2048-byte budget vs a multi-thousand-row build => many partitions.
        await matchesInMemory('SELECT l.id, r.rv FROM l JOIN r ON l.k = r.rk');
      },
    );

    test('spill files are cleaned up after the query', () async {
      final engine = SqlEngine(
        spillThresholdBytes: 2048,
        spillDir: spillDir.path,
      );
      engine.registerTable('l', left(ln));
      engine.registerTable('r', right(rn));
      await engine.execute(
        'SELECT l.id, r.rv FROM l FULL JOIN r ON l.k = r.rk',
      );
      final leftover = spillDir
          .listSync()
          .where((f) => f.path.contains('spill-'))
          .toList();
      expect(leftover, isEmpty);
    });

    test('CROSS JOIN stays correct (in-memory path)', () async {
      final engine = SqlEngine(
        spillThresholdBytes: 2048,
        spillDir: spillDir.path,
      );
      engine.registerTable('l', left(50));
      engine.registerTable('r', right(40));
      final r = await engine.execute(
        'SELECT COUNT(*) AS n FROM l CROSS JOIN r',
      );
      expect(r.column('n')!.toList().first, 50 * 40);
    });
  });

  group('cross join spill', () {
    Table t(String name, String col, int n, int base) {
      final v = <int>[for (var i = 0; i < n; i++) base + i];
      return Table(
        name: name,
        columns: [NumericColumn.int64(name: col, values: v, nullable: false)],
        schema: Schema(
          fields: [Field(name: col, dataType: DataType.int64, nullable: false)],
        ),
      );
    }

    late Directory spillDir;
    setUp(() => spillDir = Directory.systemTemp.createTempSync('xspill_'));
    tearDown(() => spillDir.deleteSync(recursive: true));

    test('CROSS JOIN with a spilled right side matches in-memory', () async {
      const left = 20;
      const right = 4000;

      Future<List<Map<String, dynamic>>> go(int budget) async {
        final e = budget == 0
            ? SqlEngine(spillThresholdBytes: 0)
            : SqlEngine(spillThresholdBytes: budget, spillDir: spillDir.path);
        e.registerTable('l', t('l', 'x', left, 0));
        e.registerTable('r', t('r', 'y', right, 1000));
        return (await e.execute('SELECT x, y FROM l CROSS JOIN r')).toRows();
      }

      final memory = await go(0);
      final spilled = await go(2048);

      expect(spilled.length, left * right);
      final a = spilled.map((r) => r.toString()).toList()..sort();
      final b = memory.map((r) => r.toString()).toList()..sort();
      expect(a, b);
      expect(_leakedSpills(spillDir), isEmpty); // no leaked spill files
    });

    test('CROSS JOIN still honors maxOutputRows under spill', () async {
      final e = SqlEngine(
        spillThresholdBytes: 2048,
        spillDir: spillDir.path,
        maxOutputRows: 100,
      );
      e.registerTable('l', t('l', 'x', 20, 0));
      e.registerTable('r', t('r', 'y', 4000, 1000));
      await expectLater(
        e.execute('SELECT x, y FROM l CROSS JOIN r'),
        throwsA(isA<StateError>()),
      );
    });
  });

  group('set operations', () {
    /// Data for the spill group: overlapping, duplicate-bearing rows across two
    /// tables so UNION/INTERSECT/EXCEPT have real work under a tiny budget.
    Table data(String name, int n, int seed, int mod) {
      final a = <int>[];
      final b = <String>[];
      final c = <int?>[];
      var x = seed;
      for (var i = 0; i < n; i++) {
        x = (x * 1103515245 + 12345) & 0x7fffffff;
        a.add(x % mod);
        b.add('v_${x % 7}');
        c.add(x % 13 == 0 ? null : x % 50);
      }
      return Table(
        name: name,
        columns: [
          NumericColumn.int64(name: 'a', values: a, nullable: false),
          ListColumn<String>(
            name: 'b',
            dataType: DataType.string,
            data: b,
            nullable: false,
          ),
          NumericColumn.int64(name: 'c', values: c, nullable: true),
        ],
        schema: Schema(
          fields: [
            Field(name: 'a', dataType: DataType.int64, nullable: false),
            Field(name: 'b', dataType: DataType.string, nullable: false),
            Field(name: 'c', dataType: DataType.int64, nullable: true),
          ],
        ),
      );
    }

    Table ids(String name, List<int> ids) => Table(
      name: name,
      columns: [NumericColumn.int64(name: 'id', values: ids, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    Future<List<int>> ids2(SqlEngine e, String q) async =>
        (await e.execute(q)).column('id')!.toList().cast<int>()..sort();

    late SqlEngine engine;
    setUp(() {
      engine = SqlEngine();
      engine.registerTable('t1', ids('t1', [1, 2, 3]));
      engine.registerTable('t2', ids('t2', [2, 3, 4]));
      engine.registerTable('dups', ids('dups', [1, 1, 2]));
    });

    test('UNION ALL keeps duplicates', () async {
      expect(
        await ids2(engine, 'SELECT id FROM t1 UNION ALL SELECT id FROM t2'),
        [1, 2, 2, 3, 3, 4],
      );
    });

    test('UNION deduplicates', () async {
      expect(await ids2(engine, 'SELECT id FROM t1 UNION SELECT id FROM t2'), [
        1,
        2,
        3,
        4,
      ]);
    });

    test('UNION dedups within a single side too', () async {
      expect(
        await ids2(engine, 'SELECT id FROM dups UNION SELECT id FROM t2'),
        [1, 2, 3, 4],
      );
    });

    test('INTERSECT keeps common distinct rows', () async {
      expect(
        await ids2(engine, 'SELECT id FROM t1 INTERSECT SELECT id FROM t2'),
        [2, 3],
      );
    });

    test('EXCEPT keeps left-only distinct rows', () async {
      expect(await ids2(engine, 'SELECT id FROM t1 EXCEPT SELECT id FROM t2'), [
        1,
      ]);
    });

    test('chained set operations (right-associative)', () async {
      engine.registerTable('t3', ids('t3', [4, 5]));
      // t1 UNION t2 UNION t3 -> {1,2,3,4,5}
      expect(
        await ids2(
          engine,
          'SELECT id FROM t1 UNION SELECT id FROM t2 UNION SELECT id FROM t3',
        ),
        [1, 2, 3, 4, 5],
      );
    });

    test('multi-column UNION matches on whole rows', () async {
      Table kv(String n, List<List<dynamic>> rows) => Table(
        name: n,
        columns: [
          NumericColumn.int64(
            name: 'k',
            values: [for (final r in rows) r[0] as int],
            nullable: false,
          ),
          ListColumn<String>(
            name: 'v',
            dataType: DataType.string,
            data: [for (final r in rows) r[1] as String],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'k', dataType: DataType.int64, nullable: false),
            Field(name: 'v', dataType: DataType.string, nullable: false),
          ],
        ),
      );
      engine.registerTable(
        'a',
        kv('a', [
          [1, 'x'],
          [2, 'y'],
        ]),
      );
      engine.registerTable(
        'b',
        kv('b', [
          [2, 'y'],
          [2, 'z'],
        ]),
      );
      final t = await engine.execute(
        'SELECT k, v FROM a UNION SELECT k, v FROM b',
      );
      final rows = {for (final r in t.toRows()) '${r['k']}-${r['v']}'};
      expect(rows, {'1-x', '2-y', '2-z'}); // (2,y) deduped
    });

    // A tiny budget forces UNION / INTERSECT / EXCEPT onto disk (grace
    // partitioning, recursive on skew). Results must match the in-memory path
    // (budget 0) and leave no spill files behind.
    group('spill', () {
      late Directory spillDir;
      setUp(() => spillDir = Directory.systemTemp.createTempSync('setspill_'));
      tearDown(() => spillDir.deleteSync(recursive: true));

      Future<List<Map<String, dynamic>>> run(int budget, String sql) async {
        final engine = budget == 0
            ? SqlEngine(spillThresholdBytes: 0)
            : SqlEngine(spillThresholdBytes: budget, spillDir: spillDir.path);
        engine.registerTable('t1', data('t1', 6000, 7919, 800));
        engine.registerTable('t2', data('t2', 6000, 104729, 800));
        return (await engine.execute(sql)).toRows();
      }

      Future<void> matches(String sql) async {
        final memory = await run(0, sql);
        final spilled = await run(2048, sql);
        final a = spilled.map((r) => r.toString()).toList()..sort();
        final b = memory.map((r) => r.toString()).toList()..sort();
        expect(a, b, reason: sql);
        expect(
          _leakedSpills(spillDir),
          isEmpty,
          reason: 'spill dir not clean: $sql',
        );
      }

      test('UNION (distinct) spills and matches memory', () {
        return matches('SELECT a, b, c FROM t1 UNION SELECT a, b, c FROM t2');
      });

      test('UNION ALL streams (no spill) and matches memory', () {
        return matches(
          'SELECT a, b, c FROM t1 UNION ALL SELECT a, b, c FROM t2',
        );
      });

      test('INTERSECT spills and matches memory', () {
        return matches(
          'SELECT a, b, c FROM t1 INTERSECT SELECT a, b, c FROM t2',
        );
      });

      test('EXCEPT spills and matches memory', () {
        return matches('SELECT a, b, c FROM t1 EXCEPT SELECT a, b, c FROM t2');
      });

      // Duplicate-heavy: many rows but very few DISTINCT rows. The streaming
      // combine holds only one RowKey per distinct row, so this stays bounded.
      Table dup(String name, int n, int variants) => Table(
        name: name,
        columns: [
          NumericColumn.int64(
            name: 'a',
            values: [for (var i = 0; i < n; i++) i % variants],
            nullable: false,
          ),
          ListColumn<String>(
            name: 'b',
            dataType: DataType.string,
            data: [for (var i = 0; i < n; i++) 'v_${i % variants}'],
            nullable: false,
          ),
          NumericColumn.int64(
            name: 'c',
            values: [for (var i = 0; i < n; i++) i % variants],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'a', dataType: DataType.int64, nullable: false),
            Field(name: 'b', dataType: DataType.string, nullable: false),
            Field(name: 'c', dataType: DataType.int64, nullable: false),
          ],
        ),
      );

      test(
        'duplicate-heavy UNION/INTERSECT/EXCEPT stay bounded and correct',
        () async {
          for (final op in ['UNION', 'INTERSECT', 'EXCEPT']) {
            final sql = 'SELECT a, b, c FROM t1 $op SELECT a, b, c FROM t2';
            final e = SqlEngine(
              spillThresholdBytes: 512,
              spillDir: spillDir.path,
            );
            e.registerTable('t1', dup('t1', 20000, 3));
            e.registerTable('t2', dup('t2', 20000, 5));
            final spilled = (await e.execute(sql)).toRows();

            final m = SqlEngine(spillThresholdBytes: 0);
            m.registerTable('t1', dup('t1', 20000, 3));
            m.registerTable('t2', dup('t2', 20000, 5));
            final memory = (await m.execute(sql)).toRows();

            final a = spilled.map((r) => r.toString()).toList()..sort();
            final b = memory.map((r) => r.toString()).toList()..sort();
            expect(a, b, reason: op);
            expect(
              _leakedSpills(spillDir),
              isEmpty,
              reason: 'spill dir not clean: $op',
            );
          }
        },
      );

      test('high-cardinality UNION forces recursive re-partition', () async {
        final engine = SqlEngine(
          spillThresholdBytes: 1024,
          spillDir: spillDir.path,
        );
        engine.registerTable('t1', data('t1', 6000, 7919, 100000));
        engine.registerTable('t2', data('t2', 6000, 104729, 100000));
        final spilled = (await engine.execute(
          'SELECT a, b, c FROM t1 UNION SELECT a, b, c FROM t2',
        )).toRows();

        final mem = SqlEngine(spillThresholdBytes: 0);
        mem.registerTable('t1', data('t1', 6000, 7919, 100000));
        mem.registerTable('t2', data('t2', 6000, 104729, 100000));
        final memory = (await mem.execute(
          'SELECT a, b, c FROM t1 UNION SELECT a, b, c FROM t2',
        )).toRows();

        expect(spilled.length, memory.length);
        expect(_leakedSpills(spillDir), isEmpty);
      });
    });

    group('decimal scale reconciliation', () {
      // Two branches hold the SAME values at DIFFERENT scales: a decimal stores
      // an unscaled BigInt with its scale on the Field, so 1.5 is BigInt(15) at
      // scale 1 but BigInt(150) at scale 2. A set op compared/concatenated the
      // raw BigInts — missing equal values AND matching unequal ones (2.5 and
      // 0.25 are both BigInt(25)).
      Table decimals(String name, List<int> unscaled, int scale) => Table(
        name: name,
        columns: [
          ListColumn<BigInt>(
            name: 'd',
            dataType: DataType.decimal,
            data: [for (final u in unscaled) BigInt.from(u)],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(
              name: 'd',
              dataType: DataType.decimal,
              nullable: false,
              precision: 10,
              scale: scale,
            ),
          ],
        ),
      );

      Future<List<Decimal>> run(SqlEngine e, String q) async {
        final r = await e.execute(q);
        return [for (final row in r.toRows()) row['d'] as Decimal];
      }

      SqlEngine seed([int threshold = 256 << 20]) {
        final e = SqlEngine(spillThresholdBytes: threshold);
        // t: 1.5, 2.5 (scale 1)   u: 1.50, 0.25 (scale 2)
        e.registerTable('t', decimals('t', [15, 25], 1));
        e.registerTable('u', decimals('u', [150, 25], 2));
        return e;
      }

      test('INTERSECT matches by value across differing scales', () async {
        final e = seed();
        // Only 1.5 is common. Rendered at the wider scale (2).
        expect(await run(e, 'SELECT d FROM t INTERSECT SELECT d FROM u'), [
          Decimal(BigInt.from(150), 2),
        ]);
        await e.dispose();
      });

      test('EXCEPT excludes by value across differing scales', () async {
        final e = seed();
        // 2.5 is in t and not u; 1.5 IS in u so it's excluded. 2.5 must NOT be
        // dropped by a false match against 0.25 (both raw BigInt(25)).
        expect(await run(e, 'SELECT d FROM t EXCEPT SELECT d FROM u'), [
          Decimal(BigInt.from(250), 2),
        ]);
        await e.dispose();
      });

      test('UNION dedups by value and yields one scale', () async {
        final e = seed();
        final r = await e.execute('SELECT d FROM t UNION SELECT d FROM u');
        expect(r.schema.fields.first.scale, 2);
        // {1.5, 2.5, 0.25} — 1.5 and 1.50 collapse to one row. Order is not
        // guaranteed (ORDER BY binds to the left branch), so compare as a set.
        expect(
          {for (final row in r.toRows()) row['d']},
          {
            Decimal(BigInt.from(25), 2),
            Decimal(BigInt.from(150), 2),
            Decimal(BigInt.from(250), 2),
          },
        );
        await e.dispose();
      });

      test('the reconciliation survives a spill', () async {
        final e = seed(
          256,
        ); // force the grace-partition path (0 disables spill)
        expect(await run(e, 'SELECT d FROM t INTERSECT SELECT d FROM u'), [
          Decimal(BigInt.from(150), 2),
        ]);
        await e.dispose();
      });

      // F5: rescaling the narrower branch UP adds integer→unscaled digits, so
      // result precision = widest integer digits + result scale. decimal(3,1) ∪
      // decimal(3,2) is decimal(4,2), not decimal(3,2) — the latter can't
      // represent 99.90 (a Parquet/Arrow write would emit an invalid contract).
      test('F5: reconciled precision widens for the added digits', () async {
        Table one(String name, int unscaled, int precision, int scale) => Table(
          name: name,
          columns: [
            ListColumn<BigInt>(
              name: 'd',
              dataType: DataType.decimal,
              data: [BigInt.from(unscaled)],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(
                name: 'd',
                dataType: DataType.decimal,
                nullable: false,
                precision: precision,
                scale: scale,
              ),
            ],
          ),
        );
        final e = SqlEngine();
        e.registerTable('a', one('a', 999, 3, 1)); // 99.9  decimal(3,1)
        e.registerTable('b', one('b', 125, 3, 2)); // 1.25  decimal(3,2)
        final r = await e.execute('SELECT d FROM a UNION SELECT d FROM b');
        final f = r.schema.fields.first;
        expect(f.scale, 2);
        expect(f.precision, 4); // was 3 — too narrow to hold 99.90
        await e.dispose();
      });
    });
  });

  group('CTEs', () {
    Table t(List<int> ids, String pfx) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: ids, nullable: false),
        ListColumn<String>(
          name: 'name',
          dataType: DataType.string,
          data: [for (final i in ids) '$pfx$i'],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'name', dataType: DataType.string, nullable: false),
        ],
      ),
    );

    late Directory dir;
    late String a, b;
    late SqlEngine engine;

    setUp(() async {
      dir = Directory.systemTemp.createTempSync('sqlcte_');
      a = '${dir.path}/a.parquet';
      b = '${dir.path}/b.parquet';
      await ParquetWriter(a).write(t([1, 2, 3], 'a'));
      await ParquetWriter(b).write(t([2, 3, 4], 'b'));
      engine = SqlEngine(allowExternalPaths: true);
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Future<List<Map<String, dynamic>>> rows(String sql) async =>
        (await engine.execute(sql)).toRows();

    group('CTE materialization', () {
      test('CTE over a file body loads and qualifies', () async {
        final r = await engine.execute(
          "WITH t AS (SELECT id, name FROM '$a') SELECT t.name FROM t WHERE t.id > 1",
        );
        expect(r.schema.fieldNames, ['name']);
        expect(r.toRows(), [
          {'name': 'a2'},
          {'name': 'a3'},
        ]);
      });

      test('CTE SELECT * keeps bare column names', () async {
        final r = await engine.execute(
          "WITH t AS (SELECT * FROM '$a') SELECT * FROM t",
        );
        expect(r.schema.fieldNames, ['id', 'name']);
        expect(r.rowCount, 3);
      });

      test('a later CTE can reference an earlier one', () async {
        final r = await rows(
          "WITH t AS (SELECT id, name FROM '$a'), u AS (SELECT id FROM t WHERE id > 1) SELECT * FROM u",
        );
        expect(r, [
          {'id': 2},
          {'id': 3},
        ]);
      });

      test('self-join of a CTE under two aliases', () async {
        final r = await engine.execute(
          "WITH t AS (SELECT id, name FROM '$a') SELECT x.name, y.name FROM t x JOIN t y ON x.id = y.id",
        );
        expect(r.schema.fieldNames, ['x.name', 'y.name']);
        expect(r.rowCount, 3);
        expect(r.toRows().first, {'x.name': 'a1', 'y.name': 'a1'});
      });

      test('CTE joined to a file', () async {
        final r = await engine.execute(
          "WITH t AS (SELECT id, name FROM '$a') SELECT t.name, b.name FROM t JOIN '$b' b ON t.id = b.id",
        );
        expect(r.schema.fieldNames, ['t.name', 'b.name']);
        expect(r.toRows(), [
          {'t.name': 'a2', 'b.name': 'b2'},
          {'t.name': 'a3', 'b.name': 'b3'},
        ]);
      });

      test('WITH t(p, q) renames the CTE columns', () async {
        final r = await rows(
          "WITH t(p, q) AS (SELECT id, name FROM '$a') SELECT t.p, t.q FROM t WHERE t.p = 2",
        );
        expect(r, [
          {'p': 2, 'q': 'a2'},
        ]);
      });

      test('WITH RECURSIVE is rejected', () {
        expect(
          engine.execute(
            "WITH RECURSIVE t AS (SELECT id, name FROM '$a') SELECT * FROM t",
          ),
          throwsA(isA<UnsupportedError>()),
        );
      });

      test('a CTE shadows a registered table only for that query', () async {
        engine.registerTable('t', t([99], 'real'));
        final inside = await rows(
          "WITH t AS (SELECT id, name FROM '$a') SELECT id FROM t ORDER BY id",
        );
        expect(inside.map((r) => r['id']).toList(), [
          1,
          2,
          3,
        ]); // CTE, not the real table
        final after = await rows('SELECT id FROM t');
        expect(after, [
          {'id': 99},
        ]); // registered table restored
      });
    });

    group('subquery in FROM', () {
      test('subquery alias qualifies its columns', () async {
        final r = await rows(
          "SELECT s.name FROM (SELECT id, name FROM '$a') s WHERE s.id > 1",
        );
        expect(r, [
          {'name': 'a2'},
          {'name': 'a3'},
        ]);
      });

      test('subquery joined to a file', () async {
        final r = await engine.execute(
          "SELECT s.name, b.name FROM (SELECT id, name FROM '$a') s JOIN '$b' b ON s.id = b.id",
        );
        expect(r.schema.fieldNames, ['s.name', 'b.name']);
        expect(r.rowCount, 2);
      });
    });
  });

  group('views', () {
    Table t(List<int> ids, String pfx) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: ids, nullable: false),
        ListColumn<String>(
          name: 'name',
          dataType: DataType.string,
          data: [for (final i in ids) '$pfx$i'],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'name', dataType: DataType.string, nullable: false),
        ],
      ),
    );

    late Directory dir;
    late String a, b;
    late SqlEngine engine;

    setUp(() async {
      dir = Directory.systemTemp.createTempSync('sqlview_');
      a = '${dir.path}/a.parquet';
      b = '${dir.path}/b.parquet';
      await ParquetWriter(a).write(t([1, 2, 3], 'a'));
      await ParquetWriter(b).write(t([2, 3, 4], 'b'));
      engine = SqlEngine(allowExternalPaths: true);
    });
    tearDown(() => dir.deleteSync(recursive: true));

    Future<List<Map<String, dynamic>>> rows(String sql) async =>
        (await engine.execute(sql)).toRows();

    test('create then select, with alias qualification', () async {
      await engine.execute("CREATE VIEW v AS (SELECT id, name FROM '$a')");
      final r = await engine.execute("SELECT v.name FROM v WHERE v.id > 1");
      expect(r.schema.fieldNames, ['name']);
      expect(r.toRows(), [
        {'name': 'a2'},
        {'name': 'a3'},
      ]);
    });

    test('a view persists across separate statements', () async {
      await engine.execute("CREATE VIEW v AS (SELECT id, name FROM '$a')");
      expect(await rows("SELECT count(*) AS n FROM v"), [
        {'n': 3},
      ]);
      expect(await rows("SELECT id FROM v WHERE id = 2"), [
        {'id': 2},
      ]);
    });

    test('view column aliases (CREATE VIEW v(p, q))', () async {
      await engine.execute(
        "CREATE VIEW v(p, q) AS (SELECT id, name FROM '$a')",
      );
      expect(await rows("SELECT v.p, v.q FROM v WHERE v.p = 2"), [
        {'p': 2, 'q': 'a2'},
      ]);
    });

    test('view joined to a file', () async {
      await engine.execute("CREATE VIEW v AS (SELECT id, name FROM '$a')");
      final r = await engine.execute(
        "SELECT v.name, b.name FROM v JOIN '$b' b ON v.id = b.id",
      );
      expect(r.schema.fieldNames, ['v.name', 'b.name']);
      expect(r.rowCount, 2);
    });

    test('self-join of a view under two aliases', () async {
      await engine.execute("CREATE VIEW v AS (SELECT id, name FROM '$a')");
      final r = await engine.execute(
        "SELECT x.name, y.name FROM v x JOIN v y ON x.id = y.id",
      );
      expect(r.schema.fieldNames, ['x.name', 'y.name']);
      expect(r.rowCount, 3);
    });

    test('view referencing another view', () async {
      await engine.execute("CREATE VIEW v AS (SELECT id, name FROM '$a')");
      await engine.execute("CREATE VIEW w AS (SELECT * FROM v)");
      final r = await engine.execute("SELECT * FROM w");
      expect(r.schema.fieldNames, ['id', 'name']);
      expect(r.rowCount, 3);
    });

    test('CREATE OR REPLACE swaps the definition', () async {
      await engine.execute("CREATE VIEW v AS (SELECT id, name FROM '$a')");
      await engine.execute(
        "CREATE OR REPLACE VIEW v AS (SELECT id, name FROM '$b')",
      );
      expect(await rows("SELECT v.name FROM v WHERE v.id = 2"), [
        {'name': 'b2'},
      ]);
    });

    test('duplicate CREATE without OR REPLACE throws', () async {
      await engine.execute("CREATE VIEW v AS (SELECT id FROM '$a')");
      expect(
        engine.execute("CREATE VIEW v AS (SELECT id FROM '$b')"),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('DROP VIEW removes it; referencing it afterwards fails', () async {
      await engine.execute("CREATE VIEW v AS (SELECT id FROM '$a')");
      await engine.execute("DROP VIEW v");
      expect(engine.execute("SELECT * FROM v"), throwsA(isA<StateError>()));
    });

    test('DROP VIEW absent throws; IF EXISTS is a no-op', () async {
      expect(engine.execute("DROP VIEW nope"), throwsA(isA<ArgumentError>()));
      await engine.execute("DROP VIEW IF EXISTS nope"); // does not throw
    });

    test('a self-referential view is rejected at reference time', () async {
      await engine.execute("CREATE VIEW r AS (SELECT * FROM r)");
      expect(engine.execute("SELECT * FROM r"), throwsA(isA<StateError>()));
    });

    test('a registered table shadows a view of the same name', () async {
      await engine.execute("CREATE VIEW t AS (SELECT id FROM '$a')");
      engine.registerTable('t', t([99], 'real'));
      expect(await rows("SELECT id FROM t"), [
        {'id': 99},
      ]); // the real table wins
    });
  });

  group('expressions', () {
    late SqlEngine engine;
    setUp(() {
      engine = SqlEngine();
      engine.registerTable(
        't',
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(
              name: 'x',
              values: [-10, -3, 2, 7],
              nullable: false,
            ),
            ListColumn<String>(
              name: 's',
              dataType: DataType.string,
              data: ['a.b', 'acb', 'axb', 'ab'],
              nullable: false,
            ),
            ListColumn<bool>(
              name: 'f',
              dataType: DataType.boolean,
              data: [true, false, true, false],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'x', dataType: DataType.int64, nullable: false),
              Field(name: 's', dataType: DataType.string, nullable: false),
              Field(name: 'f', dataType: DataType.boolean, nullable: false),
            ],
          ),
        ),
      );
    });

    Future<List<dynamic>> col(String sql, String name) async =>
        (await engine.execute(sql)).column(name)!.toList();

    group('unary expressions work (not silent NULL)', () {
      test('WHERE with a negative literal', () async {
        expect(await col('SELECT x FROM t WHERE x > -5', 'x'), [-3, 2, 7]);
      });

      test('WHERE NOT <bool column>', () async {
        expect(await col('SELECT x FROM t WHERE NOT f', 'x'), [-3, 7]);
      });

      test('NOT over a comparison', () async {
        expect(await col('SELECT x FROM t WHERE NOT (x > 0)', 'x'), [-10, -3]);
      });
    });

    group('fail closed on unsupported expressions / functions', () {
      test('an unknown scalar function throws', () async {
        await expectLater(
          engine.execute('SELECT bogusfn(x) FROM t'),
          throwsA(isA<UnsupportedError>()),
        );
      });
    });

    group('LIKE escapes regex metacharacters', () {
      test("'a.b' matches only the literal dot, not any char", () async {
        expect(await col("SELECT s FROM t WHERE s LIKE 'a.b'", 's'), ['a.b']);
      });

      test(
        "'a_b' single-char wildcard matches a.b/acb/axb but not ab",
        () async {
          expect(await col("SELECT s FROM t WHERE s LIKE 'a_b'", 's'), [
            'a.b',
            'acb',
            'axb',
          ]);
        },
      );

      test("'a%b' any-run wildcard matches all a…b including empty", () async {
        expect(await col("SELECT s FROM t WHERE s LIKE 'a%b'", 's'), [
          'a.b',
          'acb',
          'axb',
          'ab',
        ]);
      });
    });
  });

  group('semantics', () {
    // A table with a group column and a nullable value column.
    Table sales() => Table(
      name: 't',
      columns: [
        ListColumn<String>(
          name: 'region',
          dataType: DataType.string,
          data: ['e', 'e', 'w'],
          nullable: false,
        ),
        ListColumn<int>(
          name: 'amt',
          dataType: DataType.int64,
          data: [10, 20, null],
          nullable: true,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'region', dataType: DataType.string, nullable: false),
          Field(name: 'amt', dataType: DataType.int64, nullable: true),
        ],
      ),
    );

    SqlEngine engine() {
      final e = SqlEngine();
      e.registerTable('t', sales());
      return e;
    }

    group('empty / all-null aggregates', () {
      test('global aggregate over empty input yields one NULL/0 row', () async {
        final t = await engine().execute(
          'SELECT COUNT(*) AS c, MAX(amt) AS m, SUM(amt) AS s '
          'FROM t WHERE amt > 100000',
        );
        expect(t.rowCount, 1);
        final row = t.toRows().single;
        expect(row['c'], 0);
        expect(row['m'], isNull);
        expect(row['s'], isNull);
      });

      test('SUM over an all-NULL group is NULL, not 0', () async {
        final t = await engine().execute(
          'SELECT region, SUM(amt) AS s FROM t GROUP BY region ORDER BY region',
        );
        final rows = {for (final r in t.toRows()) r['region']: r['s']};
        expect(
          rows['e'],
          Decimal(BigInt.from(30), 0),
        ); // integer SUM is a lossless decimal
        expect(rows['w'], isNull); // the only w row has a NULL amount
      });
    });

    group('arithmetic edges', () {
      test('integer modulo by zero is NULL (no crash)', () async {
        final t = await engine().execute('SELECT amt % 0 AS r FROM t');
        expect(t.toRows().every((row) => row['r'] == null), isTrue);
      });

      test('modulo takes the sign of the dividend (SQL semantics)', () async {
        final e = SqlEngine();
        e.registerTable(
          'neg',
          Table(
            name: 'neg',
            columns: [
              NumericColumn.int64(
                name: 'v',
                values: [-7, 7, -8],
                nullable: false,
              ),
            ],
            schema: Schema(
              fields: [
                Field(name: 'v', dataType: DataType.int64, nullable: false),
              ],
            ),
          ),
        );
        final r = (await e.execute(
          'SELECT v % 3 AS m FROM neg',
        )).toRows().map((x) => x['m']).toList();
        await e.dispose();
        // remainder (SQL): sign of the dividend, not Dart's Euclidean %.
        expect(r, [-1, 1, -2]);
      });

      test('numeric vs string ordered comparison fails loud', () async {
        await expectLater(
          engine().execute("SELECT region FROM t WHERE amt > 'x'"),
          throwsA(isA<Object>()),
        );
      });
    });

    group('aggregate output column order + aliases', () {
      test('columns follow the SELECT list, not [keys, aggs]', () async {
        final t = await engine().execute(
          'SELECT COUNT(*) AS c, region FROM t GROUP BY region',
        );
        expect(t.schema.fieldNames, ['c', 'region']);
      });

      test('a group-key alias is honored', () async {
        final t = await engine().execute(
          'SELECT region AS r, COUNT(*) AS c FROM t GROUP BY region',
        );
        expect(t.schema.fieldNames, ['r', 'c']);
      });
    });

    group('IS NULL / IS NOT NULL', () {
      test('IS NULL selects the null rows', () async {
        final t = await engine().execute(
          'SELECT region FROM t WHERE amt IS NULL',
        );
        expect(t.toRows().map((r) => r['region']), ['w']);
      });

      test('IS NOT NULL selects the non-null rows', () async {
        final t = await engine().execute(
          'SELECT region FROM t WHERE amt IS NOT NULL ORDER BY region',
        );
        expect(t.rowCount, 2);
      });
    });
  });

  group('distinct bounding', () {
    // Small table with known distinct structure.
    //   cat=A: x∈{1,1,2}  y∈{10,20,30}
    //   cat=B: x∈{5,5}    y∈{40,50}
    Table small() => Table(
      name: 't',
      columns: [
        ListColumn<String>(
          name: 'cat',
          dataType: DataType.string,
          data: ['A', 'A', 'A', 'B', 'B'],
          nullable: false,
        ),
        NumericColumn.int64(
          name: 'x',
          values: [1, 1, 2, 5, 5],
          nullable: false,
        ),
        NumericColumn.int64(
          name: 'y',
          values: [10, 20, 30, 40, 50],
          nullable: false,
        ),
        // xn is entirely NULL for group A, set for group B.
        NumericColumn.int64(
          name: 'xn',
          values: [null, null, null, 7, 7],
          nullable: true,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'cat', dataType: DataType.string, nullable: false),
          Field(name: 'x', dataType: DataType.int64, nullable: false),
          Field(name: 'y', dataType: DataType.int64, nullable: false),
          Field(name: 'xn', dataType: DataType.int64, nullable: true),
        ],
      ),
    );

    Future<Map<String, List<Object?>>> rows(
      String sql,
      List<String> keys,
    ) async {
      final e = SqlEngine();
      e.registerTable('t', small());
      final t = await e.execute(sql);
      return {
        for (final r in t.toRows())
          r['cat'].toString(): [for (final k in keys) r[k]],
      };
    }

    test('multiple distinct columns (Tier 2)', () async {
      final r = await rows(
        'SELECT cat, COUNT(DISTINCT x) AS dx, COUNT(DISTINCT y) AS dy '
        'FROM t GROUP BY cat',
        ['dx', 'dy'],
      );
      expect(r['A'], [2, 3]);
      expect(r['B'], [1, 2]);
    });

    test('mixed distinct + non-distinct incl. COUNT(*) (Tier 2)', () async {
      final r = await rows(
        'SELECT cat, COUNT(DISTINCT x) AS dx, SUM(y) AS sy, COUNT(*) AS n '
        'FROM t GROUP BY cat',
        ['dx', 'sy', 'n'],
      );
      // SUM over an int column returns a lossless BigInt.
      expect(r['A'], [2, Decimal(BigInt.from(60), 0), 3]);
      expect(r['B'], [1, Decimal(BigInt.from(90), 0), 2]);
    });

    test(
      'multiple distinct aggregates over different columns w/ SUM',
      () async {
        final r = await rows(
          'SELECT cat, COUNT(DISTINCT x) AS dx, SUM(DISTINCT y) AS sy '
          'FROM t GROUP BY cat',
          ['dx', 'sy'],
        );
        expect(r['A'], [
          2,
          Decimal(BigInt.from(60), 0),
        ]); // distinct y = {10,20,30}
        expect(r['B'], [
          1,
          Decimal(BigInt.from(90), 0),
        ]); // distinct y = {40,50}
      },
    );

    test('global (no GROUP BY) multi-distinct', () async {
      final e = SqlEngine();
      e.registerTable('t', small());
      final t = await e.execute(
        'SELECT COUNT(DISTINCT x) AS dx, COUNT(DISTINCT y) AS dy FROM t',
      );
      final row = t.toRows().single;
      expect(row['dx'], 3); // {1,2,5}
      expect(row['dy'], 5); // {10,20,30,40,50}
    });

    test('all-NULL distinct group yields COUNT 0, not NULL', () async {
      // Group A's distinct arg (xn) is entirely NULL; COUNT must be 0, and the
      // group must still be present (kept by the raw COUNT(*) lane).
      final e = SqlEngine();
      e.registerTable('t', small());
      final t = await e.execute(
        'SELECT cat, COUNT(DISTINCT xn) AS dx, COUNT(*) AS n FROM t GROUP BY cat',
      );
      final r = {for (final row in t.toRows()) row['cat'].toString(): row};
      expect(r['A']!['dx'], 0); // all NULL → 0 distinct
      expect(r['A']!['n'], 3);
      expect(r['B']!['dx'], 1); // {7}
    });

    // Tier 1: distinct over an EXPRESSION rides the bounded two-level rewrite.
    test('COUNT(DISTINCT expr) is correct (Tier 1)', () async {
      final r = await rows(
        'SELECT cat, COUNT(DISTINCT x + 100) AS dx FROM t GROUP BY cat',
        ['dx'],
      );
      expect(r['A'], [2]); // {101,102}
      expect(r['B'], [1]); // {105}
    });

    // Boundedness: a tiny budget forces spilling; results must still match memory.
    group('bounded under spill', () {
      late Directory dir;
      setUp(() => dir = Directory.systemTemp.createTempSync('distinct_'));
      tearDown(() => dir.deleteSync(recursive: true));

      Table big(int n) {
        final cat = <String>[];
        final x = <int>[];
        final y = <double>[];
        var s = 7919;
        for (var i = 0; i < n; i++) {
          s = (s * 1103515245 + 12345) & 0x7fffffff;
          cat.add('c${s % 10}');
          x.add(s % 5000); // many distinct
          y.add((s % 3000).toDouble());
        }
        return Table(
          name: 't',
          columns: [
            ListColumn<String>(
              name: 'cat',
              dataType: DataType.string,
              data: cat,
              nullable: false,
            ),
            NumericColumn.int64(name: 'x', values: x, nullable: false),
            NumericColumn.float64(name: 'y', values: y, nullable: false),
          ],
          schema: Schema(
            fields: [
              Field(name: 'cat', dataType: DataType.string, nullable: false),
              Field(name: 'x', dataType: DataType.int64, nullable: false),
              Field(name: 'y', dataType: DataType.float64, nullable: false),
            ],
          ),
        );
      }

      Future<void> matches(String sql) async {
        Future<List<String>> run(int budget) async {
          final e = budget == 0
              ? SqlEngine(spillThresholdBytes: 0)
              : SqlEngine(spillThresholdBytes: budget, spillDir: dir.path);
          e.registerTable('t', big(6000));
          return (await e.execute(
            sql,
          )).toRows().map((r) => r.toString()).toList()..sort();
        }

        expect(await run(4096), await run(0), reason: sql);
        expect(_leakedSpills(dir), isEmpty, reason: 'spill not clean: $sql');
      }

      test('multi distinct columns spill and match', () {
        return matches(
          'SELECT cat, COUNT(DISTINCT x) AS dx, COUNT(DISTINCT y) AS dy '
          'FROM t GROUP BY cat',
        );
      });

      test('mixed distinct + non-distinct spill and match', () {
        return matches(
          'SELECT cat, COUNT(DISTINCT x) AS dx, SUM(y) AS sy, COUNT(*) AS n '
          'FROM t GROUP BY cat',
        );
      });
    });
  });

  group('binding', () {
    Table t() => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: [3, 1, 2], nullable: false),
        NumericColumn.float64(
          name: 'amount',
          values: [30, 10, 20],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'amount', dataType: DataType.float64, nullable: false),
        ],
      ),
    );

    late SqlEngine engine;
    setUp(() {
      engine = SqlEngine();
      engine.registerTable('t', t());
    });

    group('unknown columns fail loud', () {
      test('SELECT of an unknown column throws', () {
        expect(
          engine.execute('SELECT nope FROM t'),
          throwsA(isA<FormatException>()),
        );
      });

      test('WHERE on an unknown column throws (not silently filter all)', () {
        expect(
          engine.execute('SELECT id FROM t WHERE nope = 1'),
          throwsA(isA<FormatException>()),
        );
      });

      test('an unknown column inside an expression throws', () {
        expect(
          engine.execute('SELECT id + nope AS x FROM t'),
          throwsA(isA<FormatException>()),
        );
      });
    });

    group('valid queries still work', () {
      test('ORDER BY a non-projected column sorts correctly', () async {
        // `amount` is not in the SELECT list; the engine must still sort by it
        // (previously it silently sorted by an all-null key).
        final r = await engine.execute('SELECT id FROM t ORDER BY amount DESC');
        expect(r.column('id')!.toList(), [3, 2, 1]);
        expect(r.schema.fields.map((f) => f.name), ['id']); // amount trimmed
      });

      test('ORDER BY a projected column still works', () async {
        final r = await engine.execute(
          'SELECT id, amount FROM t ORDER BY id ASC',
        );
        expect(r.column('id')!.toList(), [1, 2, 3]);
      });
    });

    group('outer-join null synthesis is unaffected', () {
      test(
        'LEFT JOIN null-fills unmatched right columns without error',
        () async {
          final left = Table(
            name: 'l',
            columns: [
              NumericColumn.int64(name: 'k', values: [1, 2], nullable: false),
            ],
            schema: Schema(
              fields: [
                Field(name: 'k', dataType: DataType.int64, nullable: false),
              ],
            ),
          );
          final right = Table(
            name: 'r',
            columns: [
              NumericColumn.int64(name: 'k', values: [1], nullable: false),
              NumericColumn.int64(name: 'v', values: [99], nullable: false),
            ],
            schema: Schema(
              fields: [
                Field(name: 'k', dataType: DataType.int64, nullable: false),
                Field(name: 'v', dataType: DataType.int64, nullable: true),
              ],
            ),
          );
          final e = SqlEngine();
          e.registerTable('l', left);
          e.registerTable('r', right);
          final out = await e.execute(
            'SELECT l.k, r.v FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.k',
          );
          final rows = out.toRows();
          expect(rows.length, 2);
          expect(rows[1]['v'], isNull); // k=2 unmatched → null, not an error
        },
      );
    });
  });

  group('unsupported features', () {
    SqlEngine engine() {
      final e = SqlEngine();
      e.registerTable(
        't',
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'id', values: [1, 2], nullable: false),
          ],
          schema: Schema(
            fields: [Field(name: 'id', dataType: DataType.int64)],
          ),
        ),
      );
      return e;
    }

    test('an unsupported planner feature throws UnsupportedError', () {
      // A non-equi join condition has no hash-join path → a clear
      // UnsupportedError, not an internal-defect UnimplementedError.
      expect(
        engine().execute('SELECT * FROM t a JOIN t b ON a.id > b.id'),
        throwsA(isA<UnsupportedError>()),
      );
    });
  });

  group('security', () {
    Table t(List<int> ids) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: ids, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    group('SQL file-path sandbox (#3.3)', () {
      test('default-deny rejects an absolute path', () {
        final engine = SqlEngine();
        expect(
          engine.execute("SELECT * FROM '/etc/hosts'"),
          throwsA(isA<ArgumentError>()),
        );
      });

      test('default-deny rejects parent traversal', () {
        final engine = SqlEngine();
        expect(
          engine.execute("SELECT * FROM '../secret.parquet'"),
          throwsA(isA<ArgumentError>()),
        );
      });

      test('a relative path under the working dir is allowed', () async {
        final engine = SqlEngine();
        final t2 = await engine.execute(
          "SELECT COUNT(*) AS n FROM 'test/fixtures/iris.parquet'",
        );
        expect((t2.column('n')!.toList().first as int) > 0, isTrue);
      });

      test('allowExternalPaths permits an absolute path', () async {
        final dir = Directory.systemTemp.createTempSync('sec_');
        try {
          final path = '${dir.path}/x.parquet';
          await ParquetWriter(path).write(t([1, 2, 3]));
          final engine = SqlEngine(allowExternalPaths: true);
          final t2 = await engine.execute("SELECT COUNT(*) AS n FROM '$path'");
          expect(t2.column('n')!.toList().first, 3);
        } finally {
          dir.deleteSync(recursive: true);
        }
      });

      test('sandboxRoot confines reads to the root', () async {
        final dir = Directory.systemTemp.createTempSync('sec_root_');
        try {
          await ParquetWriter('${dir.path}/in.parquet').write(t([1, 2]));
          final engine = SqlEngine(sandboxRoot: dir.path);
          // A path inside the root resolves and reads.
          final t2 = await engine.execute(
            "SELECT COUNT(*) AS n FROM 'in.parquet'",
          );
          expect(t2.column('n')!.toList().first, 2);
          // Escaping the root is denied.
          expect(
            engine.execute("SELECT * FROM '../outside.parquet'"),
            throwsA(isA<ArgumentError>()),
          );
        } finally {
          dir.deleteSync(recursive: true);
        }
      });
    });
  });

  group('sandbox symlinks', () {
    late Directory sandbox;
    late Directory outside;
    setUp(() {
      sandbox = Directory.systemTemp.createTempSync('sbx_in_');
      outside = Directory.systemTemp.createTempSync('sbx_out_');
    });
    tearDown(() {
      sandbox.deleteSync(recursive: true);
      outside.deleteSync(recursive: true);
    });

    test('a symlink inside the sandbox pointing outside is denied', () async {
      File('${outside.path}/secret.csv').writeAsStringSync('id\n1\n');
      Link('${sandbox.path}/link.csv').createSync('${outside.path}/secret.csv');
      final engine = SqlEngine(sandboxRoot: sandbox.path);
      await expectLater(
        engine.execute("SELECT * FROM 'link.csv'"),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('a genuine in-sandbox file still reads', () async {
      File('${sandbox.path}/ok.csv').writeAsStringSync('id\n7\n');
      final engine = SqlEngine(sandboxRoot: sandbox.path);
      final t = await engine.execute("SELECT * FROM 'ok.csv'");
      expect(t.toRows().single['id'], 7);
    });

    test('a not-yet-existing nested in-sandbox path is NOT a denial', () async {
      // The canonical gate must follow a symlinked sandbox ROOT (e.g. macOS /tmp →
      // /private/tmp) and still permit a legitimate nested path whose leaf does not
      // exist yet (the tail-walk). Such a path gets PAST the gate and fails only at
      // read time — a read error, not an "Access denied" ArgumentError.
      final engine = SqlEngine(sandboxRoot: sandbox.path);
      try {
        await engine.execute("SELECT * FROM 'sub/missing.csv'");
      } on ArgumentError catch (e) {
        fail('a legitimate nested in-sandbox path was falsely denied: $e');
      } catch (_) {
        // A read error (file not found) is fine — it got PAST the sandbox gate,
        // which is all this test asserts.
      }
    });
  });

  group('output guard', () {
    Table nums(String name, int n) => Table(
      name: name,
      columns: [
        NumericColumn.int64(
          name: 'v',
          values: [for (var i = 0; i < n; i++) i],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [Field(name: 'v', dataType: DataType.int64, nullable: false)],
      ),
    );

    test('CROSS JOIN past maxOutputRows throws', () async {
      final engine = SqlEngine(maxOutputRows: 50);
      engine.registerTable('a', nums('a', 20));
      engine.registerTable('b', nums('b', 20)); // 400 > 50
      await expectLater(
        engine.execute('SELECT a.v FROM a, b'),
        throwsA(isA<StateError>()),
      );
    });

    test('CROSS JOIN under maxOutputRows succeeds', () async {
      final engine = SqlEngine(maxOutputRows: 1000);
      engine.registerTable('a', nums('a', 20));
      engine.registerTable('b', nums('b', 20)); // 400 <= 1000
      final t = await engine.execute('SELECT a.v FROM a, b');
      expect(t.rowCount, 400);
    });

    test('a product under the fail-closed default succeeds', () async {
      // The default now bounds output at defaultMaxMaterializedRows (10M); a 900-
      // row product is far below it, so it runs unaffected.
      final engine = SqlEngine();
      engine.registerTable('a', nums('a', 30));
      engine.registerTable('b', nums('b', 30));
      final t = await engine.execute('SELECT a.v FROM a, b');
      expect(t.rowCount, 900);
    });

    test('unbounded: true opts out of the output-row cap', () async {
      final engine = SqlEngine(unbounded: true);
      engine.registerTable('a', nums('a', 30));
      engine.registerTable('b', nums('b', 30));
      final t = await engine.execute('SELECT a.v FROM a, b');
      expect(t.rowCount, 900);
    });

    test('set operation past maxOutputRows throws', () async {
      final engine = SqlEngine(maxOutputRows: 10);
      engine.registerTable('a', nums('a', 20));
      engine.registerTable('b', nums('b', 20)); // 40 buffered > 10
      await expectLater(
        engine.execute('SELECT v FROM a UNION ALL SELECT v FROM b'),
        throwsA(isA<StateError>()),
      );
    });
  });

  group('concurrency', () {
    Table fact(int base, int n) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(
          name: 'id',
          values: [for (var i = 0; i < n; i++) base + i],
          nullable: false,
        ),
        ListColumn<String>(
          name: 'cat',
          dataType: DataType.string,
          data: [for (var i = 0; i < n; i++) 'c${(base + i) % 5}'],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'cat', dataType: DataType.string, nullable: false),
        ],
      ),
    );

    test('concurrent CTE queries on one engine stay consistent', () async {
      final engine = SqlEngine(spillThresholdBytes: 0);
      engine.registerTable('a', fact(0, 3000));

      // Each query materializes a CTE named `t` (shared-name temp table) — the
      // classic interleaving hazard — plus a GROUP BY. Fire them all at once.
      final futures = [
        for (int i = 0; i < 24; i++)
          engine.execute(
            'WITH t AS (SELECT id, cat FROM a WHERE id >= ${i * 10}) '
            'SELECT cat, COUNT(*) AS n FROM t GROUP BY cat',
          ),
      ];
      final results = await Future.wait(futures);

      for (int i = 0; i < results.length; i++) {
        final total = results[i]
            .column('n')!
            .toList()
            .cast<num>()
            .fold<int>(0, (s, v) => s + v.toInt());
        expect(total, 3000 - i * 10, reason: 'query $i row total');
      }

      // The shared CTE temp table is cleaned up — `t` isn't left registered.
      expect(() => engine.execute('SELECT * FROM t'), throwsA(anything));
    });

    test(
      'concurrent CREATE VIEW + SELECT do not corrupt the catalog',
      () async {
        final engine = SqlEngine(spillThresholdBytes: 0);
        engine.registerTable('a', fact(0, 1000));
        await engine.execute(
          'CREATE VIEW v AS (SELECT id FROM a WHERE id < 500)',
        );
        final futures = [
          for (int i = 0; i < 16; i++)
            engine.execute('SELECT COUNT(*) AS n FROM v'),
        ];
        final results = await Future.wait(futures);
        for (final r in results) {
          expect(r.column('n')!.toList().first, 500);
        }
      },
    );
  });

  group('streaming', () {
    group('lazy file scan', () {
      late Directory dir;
      setUp(() => dir = Directory.systemTemp.createTempSync('streamscan_'));
      tearDown(() => dir.deleteSync(recursive: true));

      String csv(int n) {
        final f = '${dir.path}/d.csv';
        File(f).writeAsStringSync(
          'id,name,v\n${[for (var i = 0; i < n; i++) '$i,n$i,${i * 2}'].join('\n')}\n',
        );
        return f;
      }

      String jsonl(int n) {
        final f = '${dir.path}/d.jsonl';
        File(f).writeAsStringSync(
          [
            for (var i = 0; i < n; i++) '{"id":$i,"name":"n$i","v":${i * 2}}',
          ].join('\n'),
        );
        return f;
      }

      test('CSV scan with WHERE + projection returns correct rows', () async {
        final e = SqlEngine(allowExternalPaths: true);
        final t = await e.execute(
          "SELECT id, v FROM '${csv(2500)}' WHERE id >= 2498 ORDER BY id",
        );
        expect(t.schema.fieldNames, ['id', 'v']);
        expect(t.toRows(), [
          {'id': 2498, 'v': 4996},
          {'id': 2499, 'v': 4998},
        ]);
      });

      test('JSON-Lines aggregate over a lazy scan is correct', () async {
        final e = SqlEngine(allowExternalPaths: true);
        final t = await e.execute(
          "SELECT COUNT(*) AS n, SUM(v) AS s FROM '${jsonl(2500)}'",
        );
        expect(t.toRows().single, {
          'n': 2500,
          's': Decimal(BigInt.from(6247500), 0),
        });
      });

      test(
        'executeStream over CSV yields multiple batches (bounded input)',
        () async {
          final e = SqlEngine(allowExternalPaths: true);
          var batches = 0, rows = 0;
          await for (final b in e.executeStream(
            "SELECT id FROM '${csv(2500)}'",
          )) {
            batches++;
            rows += b.rowCount;
          }
          expect(rows, 2500);
          expect(batches, greaterThan(1)); // streamed, not one whole-file batch
        },
      );
    });

    group('executeStream lock and lifecycle', () {
      late Directory dir;
      setUp(() => dir = Directory.systemTemp.createTempSync('streamlock_'));
      tearDown(() => dir.deleteSync(recursive: true));

      String csv(String name, int n) {
        final f = '${dir.path}/$name.csv';
        File(f).writeAsStringSync(
          'id,v\n${[for (var i = 0; i < n; i++) '$i,${i * 2}'].join('\n')}\n',
        );
        return f;
      }

      test('an undrained executeStream does NOT block a later query', () async {
        final engine = SqlEngine(allowExternalPaths: true);
        // Pull ONE batch then stop (never drain) — the old code held the lock here.
        final it = StreamIterator(
          engine.executeStream("SELECT id FROM '${csv('big', 3000)}'"),
        );
        expect(await it.moveNext(), isTrue);
        // A different query must still run promptly rather than deadlocking.
        final t = await engine
            .execute("SELECT COUNT(*) AS n FROM '${csv('small', 10)}'")
            .timeout(const Duration(seconds: 5));
        expect(t.toRows().single['n'], 10);
        await it.cancel();
      });

      test(
        'two concurrent stream consumers interleave to full counts',
        () async {
          final engine = SqlEngine(allowExternalPaths: true);
          final a = StreamIterator(
            engine.executeStream("SELECT id FROM '${csv('a', 1500)}'"),
          );
          final b = StreamIterator(
            engine.executeStream("SELECT id FROM '${csv('b', 1700)}'"),
          );
          var ca = 0, cb = 0;
          var more = true;
          while (more) {
            more = false;
            if (await a.moveNext()) {
              ca += a.current.rowCount;
              more = true;
            }
            if (await b.moveNext()) {
              cb += b.current.rowCount;
              more = true;
            }
          }
          expect(ca, 1500);
          expect(cb, 1700);
          await a.cancel();
          await b.cancel();
        },
      );

      test('cancel mid-stream leaves no spill files', () async {
        final spill = Directory('${dir.path}/spill')..createSync();
        final engine = SqlEngine(
          allowExternalPaths: true,
          spillThresholdBytes: 64 << 10, // force ORDER BY to spill
          spillDir: spill.path,
        );
        final it = StreamIterator(
          engine.executeStream(
            "SELECT id FROM '${csv('s', 60000)}' ORDER BY id",
          ),
        );
        expect(await it.moveNext(), isTrue); // sort completed (spilled+merged)
        await it.cancel(); // close() must reclaim spill files
        final leftover = spill
            .listSync()
            .whereType<File>()
            .where((f) => f.path.contains('spill-'))
            .toList();
        expect(leftover, isEmpty);
      });
    });
  });

  group('lazy scans', () {
    Future<String> writeParquet(String path, int rows, int rowGroupSize) async {
      final t = Table(
        name: 't',
        columns: [
          NumericColumn.int64(
            name: 'id',
            values: [for (int i = 0; i < rows; i++) i],
            nullable: false,
          ),
          ListColumn<String>(
            name: 'name',
            dataType: DataType.string,
            data: [for (int i = 0; i < rows; i++) 'n$i'],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'id', dataType: DataType.int64, nullable: false),
            Field(name: 'name', dataType: DataType.string, nullable: false),
          ],
        ),
      );
      await ParquetWriter(
        path,
        options: ParquetWriterOptions(rowGroupSize: rowGroupSize),
      ).write(t);
      return path;
    }

    late Directory dir;
    late SqlEngine engine;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('sql_lazy_');
      engine = SqlEngine(allowExternalPaths: true);
    });
    tearDown(() => dir.deleteSync(recursive: true));

    test(
      'SELECT over a parquet file (lazy scan) returns correct rows',
      () async {
        final p = await writeParquet('${dir.path}/d.parquet', 1000, 200);
        final t = await engine.execute(
          "SELECT id, name FROM '$p' WHERE id < 3 ORDER BY id",
        );
        expect(t.column('id')!.toList(), [0, 1, 2]);
        expect(t.column('name')!.toList(), ['n0', 'n1', 'n2']);
      },
    );

    test('executeStream yields multiple batches (one per row group)', () async {
      final p = await writeParquet('${dir.path}/s.parquet', 1000, 200);
      final batches = await engine.executeStream("SELECT * FROM '$p'").toList();
      expect(
        batches.length,
        greaterThan(1),
        reason: 'should stream, not one merged table',
      );
      final total = batches.fold<int>(0, (s, b) => s + b.rowCount);
      expect(total, 1000);
      final ids = [
        for (final b in batches) ...b.column('id')!.toList().cast<int>(),
      ];
      expect(ids, List.generate(1000, (i) => i));
    });

    test('WHERE pushdown prunes row groups from SQL', () async {
      final p = await writeParquet('${dir.path}/f.parquet', 1000, 200);
      ParquetReader.rowGroupsPruned = 0;
      final t = await engine.execute("SELECT id FROM '$p' WHERE id >= 800");
      expect(t.rowCount, 200); // 800..999
      expect(
        ParquetReader.rowGroupsPruned,
        greaterThan(0),
        reason: 'pushed-down filter must prune row groups at the reader',
      );
    });

    test(
      'aggregate over a lazy parquet scan is correct (blocking op)',
      () async {
        final p = await writeParquet('${dir.path}/a.parquet', 1000, 200);
        final t = await engine.execute(
          "SELECT count(*) AS c FROM '$p' WHERE id < 500",
        );
        expect(t.column('c')!.toList().first, 500);
      },
    );

    test('projection pushdown decodes only referenced columns', () async {
      final p = await writeParquet(
        '${dir.path}/proj.parquet',
        1000,
        200,
      ); // 5 groups, 2 cols

      ParquetReader.columnsRead = 0;
      final one = await engine.execute("SELECT id FROM '$p'");
      final readForOne = ParquetReader.columnsRead; // 5 groups x 1 col = 5
      expect(one.schema.fieldNames, ['id']);

      ParquetReader.columnsRead = 0;
      await engine.execute("SELECT * FROM '$p'");
      final readForStar = ParquetReader.columnsRead; // 5 groups x 2 cols = 10

      expect(
        readForOne,
        lessThan(readForStar),
        reason: 'SELECT id must decode fewer columns than SELECT *',
      );
      expect(readForOne, 5);
      expect(readForStar, 10);
    });

    test(
      'WHERE-only columns are projected into the scan (correct results)',
      () async {
        final p = await writeParquet('${dir.path}/pw.parquet', 100, 50);
        // name is selected, id only used in WHERE -> scan must still read id.
        final t = await engine.execute("SELECT name FROM '$p' WHERE id = 7");
        expect(t.column('name')!.toList(), ['n7']);
      },
    );
  });

  group('parquet queries', () {
    test('SQL engine queries a registered Parquet file', () async {
      final engine = SqlEngine();
      await engine.registerFile('flights', 'test/fixtures/flights-1m.parquet');

      final count = await engine.execute('SELECT COUNT(*) AS n FROM flights');
      expect(count.toRows().first['n'], 1000000);
    });

    test(
      'a registered file persists across queries (not a per-query temp)',
      () async {
        final engine = SqlEngine();
        await engine.registerFile(
          'flights',
          'test/fixtures/flights-1m.parquet',
        );
        // The lazy registration must survive the first query's _cleanup and remain
        // usable for the next query (regression: _clearTemporaryFileTables drop).
        await engine.execute('SELECT DISTANCE FROM flights LIMIT 1');
        final again = await engine.execute('SELECT COUNT(*) AS n FROM flights');
        expect(again.toRows().first['n'], 1000000);
        expect(engine.listTables(), contains('flights'));
      },
    );

    test('SQL engine reports unsupported file types clearly', () async {
      final engine = SqlEngine();
      expect(
        () => engine.registerFile('x', 'data.xlsx'),
        throwsA(isA<ArgumentError>()),
      );
    });
  });

  group('spill', () {
    Table data(int n) {
      final id = <int>[];
      final amount = <double>[];
      final cat = <String>[];
      final score = <double?>[];
      var x = 7919;
      for (var i = 0; i < n; i++) {
        x = (x * 1103515245 + 12345) & 0x7fffffff;
        id.add(i);
        amount.add((x % 1000).toDouble());
        cat.add('cat_${x % 11}');
        score.add(x % 9 == 0 ? null : (x % 500).toDouble());
      }
      return Table(
        name: 't',
        columns: [
          NumericColumn.int64(name: 'id', values: id, nullable: false),
          NumericColumn.float64(
            name: 'amount',
            values: amount,
            nullable: false,
          ),
          ListColumn<String>(
            name: 'cat',
            dataType: DataType.string,
            data: cat,
            nullable: false,
          ),
          NumericColumn.float64(name: 'score', values: score, nullable: true),
        ],
        schema: Schema(
          fields: [
            Field(name: 'id', dataType: DataType.int64, nullable: false),
            Field(name: 'amount', dataType: DataType.float64, nullable: false),
            Field(name: 'cat', dataType: DataType.string, nullable: false),
            Field(name: 'score', dataType: DataType.float64, nullable: true),
          ],
        ),
      );
    }

    late Directory spillDir;
    const rows = 8000;

    setUp(() => spillDir = Directory.systemTemp.createTempSync('spill_'));
    tearDown(() => spillDir.deleteSync(recursive: true));

    Future<List<Map<String, dynamic>>> run(int budget, String sql) async {
      final engine = budget == 0
          ? SqlEngine(spillThresholdBytes: 0)
          : SqlEngine(spillThresholdBytes: budget, spillDir: spillDir.path);
      engine.registerTable('t', data(rows));
      return (await engine.execute(sql)).toRows();
    }

    // Tiny budget => many runs/partitions.
    Future<void> expectSpillMatchesMemory(
      String sql, {
      bool ordered = true,
    }) async {
      final memory = await run(0, sql);
      final spilled = await run(4096, sql);
      expect(spilled.length, memory.length, reason: 'row count for: $sql');
      if (ordered) {
        expect(
          spilled.map((r) => r.toString()).toList(),
          memory.map((r) => r.toString()).toList(),
          reason: sql,
        );
      } else {
        final a = spilled.map((r) => r.toString()).toList()..sort();
        final b = memory.map((r) => r.toString()).toList()..sort();
        expect(a, b, reason: sql);
      }
    }

    group('ORDER BY (external merge-sort)', () {
      test(
        'numeric key, ascending',
        () => expectSpillMatchesMemory(
          'SELECT id, amount FROM t ORDER BY amount ASC, id ASC',
        ),
      );

      test(
        'string key, descending',
        () => expectSpillMatchesMemory(
          'SELECT id, cat FROM t ORDER BY cat DESC, id ASC',
        ),
      );

      test(
        'nullable key (nulls present)',
        () => expectSpillMatchesMemory(
          'SELECT id, score FROM t ORDER BY score ASC, id ASC',
        ),
      );

      test(
        'multi-column mixed direction',
        () => expectSpillMatchesMemory(
          'SELECT cat, amount, id FROM t ORDER BY cat ASC, amount DESC, id ASC',
        ),
      );
    });

    group('GROUP BY (partitioned hash aggregation)', () {
      test(
        'count/sum/avg/min/max',
        () => expectSpillMatchesMemory(
          'SELECT cat, COUNT(*) AS n, SUM(amount) AS s, AVG(amount) AS a, '
          'MIN(amount) AS mn, MAX(amount) AS mx FROM t GROUP BY cat',
          ordered: false,
        ),
      );

      test(
        'count distinct',
        () => expectSpillMatchesMemory(
          'SELECT cat, COUNT(DISTINCT amount) AS d FROM t GROUP BY cat',
          ordered: false,
        ),
      );

      test(
        'high-cardinality COUNT(DISTINCT) spills (few groups, many distinct)',
        () => expectSpillMatchesMemory(
          // Few group keys (cat ~11) but many distinct ids — the case where the
          // old per-group `seen` set grew unbounded; the two-level rewrite spills.
          'SELECT cat, COUNT(DISTINCT id) AS d FROM t GROUP BY cat',
          ordered: false,
        ),
      );

      test(
        'global COUNT(DISTINCT) spills',
        () => expectSpillMatchesMemory('SELECT COUNT(DISTINCT id) AS d FROM t'),
      );

      test(
        'group by two columns',
        () => expectSpillMatchesMemory(
          'SELECT cat, amount, COUNT(*) AS n FROM t GROUP BY cat, amount',
          ordered: false,
        ),
      );

      test('skewed key + high cardinality (recursive repartition)', () async {
        // 90% of rows share one HOT key (a skewed partition that must not be
        // materialized whole), plus a long tail of distinct keys (forces a
        // partition over the group cap → salted recursive repartition).
        const n = 40000;
        final skew = Table(
          name: 't',
          columns: [
            ListColumn<String>(
              name: 'k',
              dataType: DataType.string,
              data: [
                for (var i = 0; i < n; i++) i % 10 == 0 ? 'tail$i' : 'HOT',
              ],
              nullable: false,
            ),
            NumericColumn.int64(
              name: 'v',
              values: [for (var i = 0; i < n; i++) i],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'k', dataType: DataType.string, nullable: false),
              Field(name: 'v', dataType: DataType.int64, nullable: false),
            ],
          ),
        );
        Future<List<Map<String, dynamic>>> g(int budget) async {
          final engine = budget == 0
              ? SqlEngine(spillThresholdBytes: 0)
              : SqlEngine(spillThresholdBytes: budget, spillDir: spillDir.path);
          engine.registerTable('t', skew);
          return (await engine.execute(
            'SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k',
          )).toRows();
        }

        final memory = await g(0);
        final spilled = await g(4096);
        expect(spilled.length, memory.length);
        final a = spilled.map((r) => r.toString()).toList()..sort();
        final b = memory.map((r) => r.toString()).toList()..sort();
        expect(a, b);
        // The hot group is correct and present.
        expect(
          spilled.firstWhere((r) => r['k'] == 'HOT')['c'],
          memory.firstWhere((r) => r['k'] == 'HOT')['c'],
        );
      });
    });

    group('global aggregate (streaming, no GROUP BY)', () {
      test(
        'sum/avg/count match in-memory',
        () => expectSpillMatchesMemory(
          'SELECT SUM(amount) AS s, AVG(amount) AS a, COUNT(*) AS n, '
          'MIN(amount) AS mn, MAX(amount) AS mx FROM t',
        ),
      );
    });

    test('spill files are cleaned up after the query', () async {
      final engine = SqlEngine(
        spillThresholdBytes: 4096,
        spillDir: spillDir.path,
      );
      engine.registerTable('t', data(rows));
      await engine.execute('SELECT id FROM t ORDER BY amount, id');
      await engine.execute('SELECT cat, SUM(amount) AS s FROM t GROUP BY cat');
      final leftover = spillDir
          .listSync()
          .where((f) => f.path.contains('spill-'))
          .toList();
      expect(leftover, isEmpty);
    });

    test('spilling disabled (budget 0) keeps everything in memory', () async {
      final rowsOut = await run(
        0,
        'SELECT cat, SUM(amount) AS s FROM t GROUP BY cat',
      );
      expect(rowsOut.length, 11);
    });

    group('spill files are not leaked on early cancellation', () {
      int spillFiles() =>
          spillDir.listSync().where((f) => f.path.contains('spill-')).length;

      test('cancelled GROUP BY stream leaves no spill files', () async {
        final engine = SqlEngine(
          spillThresholdBytes: 4096,
          spillDir: spillDir.path,
        );
        engine.registerTable('t', data(rows));
        // Consume one batch then abandon the subscription mid-drain.
        await for (final _
            in engine
                .executeStream('SELECT cat, COUNT(*) AS n FROM t GROUP BY cat')
                .take(1)) {
          break;
        }
        expect(spillFiles(), 0);
      });

      test('cancelled JOIN stream leaves no spill files', () async {
        final engine = SqlEngine(
          spillThresholdBytes: 4096,
          spillDir: spillDir.path,
        );
        engine.registerTable('a', data(rows));
        engine.registerTable('b', data(rows));
        await for (final _
            in engine
                .executeStream('SELECT a.id FROM a JOIN b ON a.id = b.id')
                .take(1)) {
          break;
        }
        expect(spillFiles(), 0);
      });

      test('cancelled ORDER BY stream leaves no spill files', () async {
        final engine = SqlEngine(
          spillThresholdBytes: 4096,
          spillDir: spillDir.path,
        );
        engine.registerTable('t', data(rows));
        await for (final _
            in engine
                .executeStream('SELECT id, amount FROM t ORDER BY amount, id')
                .take(1)) {
          break;
        }
        expect(spillFiles(), 0);
      });
    });

    group('Arrow spill format', () {
      test('supported types write a .arrow file and round-trip', () async {
        final batch = RecordBatch(
          columns: [
            NumericColumn.int64(name: 'k', values: [1, 2, 3], nullable: false),
            ListColumn<String>(
              name: 'v',
              dataType: DataType.string,
              data: ['a', 'b', 'c'],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'k', dataType: DataType.int64, nullable: false),
              Field(name: 'v', dataType: DataType.string, nullable: false),
            ],
          ),
          rowCount: 3,
        );
        final w = SpillWriter(
          spillDir.path,
          batch.schema,
          format: SpillFormat.arrow,
        );
        await w.add(batch);
        final run = await w.finish();
        expect(run.path.endsWith('.arrow'), isTrue);
        final back = <RecordBatch>[];
        await for (final b in run.read()) {
          back.add(b);
        }
        final merged = RecordBatch.concatAll(back, batch.schema);
        expect(merged.rowCount, 3);
        expect(merged.column('k')!.toList(), [1, 2, 3]);
        expect(merged.column('v')!.toList(), ['a', 'b', 'c']);
      });

      test('an Arrow-unsupported type selects the Parquet backend', () {
        // The type gate decides the backend (and file extension) at construction.
        // Nested types are not yet Arrow-encodable -> Parquet fallback.
        final schema = Schema(
          fields: [Field(name: 'l', dataType: DataType.list, nullable: false)],
        );
        final w = SpillWriter(spillDir.path, schema, format: SpillFormat.arrow);
        expect(
          w.path.endsWith('.parquet'),
          isTrue,
          reason: 'nested list is not Arrow-encodable -> Parquet fallback',
        );
      });
    });

    group('maxSpillBytes cap', () {
      test('a tiny cap fails a spilling query fast', () async {
        final engine = SqlEngine(
          spillThresholdBytes: 4096,
          spillDir: spillDir.path,
          maxSpillBytes: 4096, // smaller than the total that must spill
        );
        engine.registerTable('t', data(rows));
        await expectLater(
          engine.execute('SELECT id FROM t ORDER BY amount, id'),
          throwsA(isA<StateError>()),
        );
        // Partial spill files are cleaned up by operator close on the failure.
        final leftover = spillDir
            .listSync()
            .where((f) => f.path.contains('spill-'))
            .toList();
        expect(leftover, isEmpty);
      });

      test('null (default) leaves spilling unbounded', () async {
        final engine = SqlEngine(
          spillThresholdBytes: 4096,
          spillDir: spillDir.path,
        );
        engine.registerTable('t', data(rows));
        final out = await engine.execute(
          'SELECT id FROM t ORDER BY amount, id',
        );
        expect(out.rowCount, rows);
      });

      test('SpillBudget charge/release accounting', () {
        final b = SpillBudget(100);
        b.charge(60);
        expect(b.used, 60);
        expect(() => b.charge(50), throwsA(isA<StateError>())); // 60+50 > 100
        expect(b.used, 60, reason: 'a rejected charge counts nothing');
        b.release(60);
        expect(b.used, 0);
        b.release(999); // clamps at zero, never negative
        expect(b.used, 0);
      });
    });
  });

  // ---- Relocated: join-column naming parity (from audit_phaseb_test.dart) ----
  group('CB7 join-column naming parity (DataFrame vs SQL)', () {
    late SqlEngine engine;
    late Directory dir;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('phaseB_');
      engine = SqlEngine(spillDir: dir.path);
    });
    tearDown(() async {
      await engine.dispose();
      dir.deleteSync(recursive: true);
    });

    Table t(String name, List<Map<String, Object?>> rows) {
      final cols = rows.first.keys.toList();
      return Table(
        name: name,
        columns: [
          for (final c in cols)
            ListColumn(
              name: c,
              dataType: rows.first[c] is int ? DataType.int64 : DataType.string,
              data: [for (final r in rows) r[c]],
              nullable: false,
            ),
        ],
        schema: Schema(
          fields: [
            for (final c in cols)
              Field(
                name: c,
                dataType: rows.first[c] is int
                    ? DataType.int64
                    : DataType.string,
                nullable: false,
              ),
          ],
        ),
      );
    }

    test('SQL join qualifies collided names via shared disambiguate', () async {
      final a = t('a', [
        {'k': 1, 'v': 'a1'},
        {'k': 2, 'v': 'a2'},
      ]);
      final b = t('b', [
        {'k': 1, 'v': 'b1'},
        {'k': 2, 'v': 'b2'},
      ]);
      engine.registerTable('a', a);
      engine.registerTable('b', b);

      // The shared Kernels.disambiguate (used by both SqlEngine._display and
      // DataFrame.execute) turns the two collided `v` fields into a.v / b.v.
      final sql = await engine.execute('SELECT * FROM a JOIN b ON a.k = b.k');
      final names = sql.schema.fields.map((f) => f.name).toSet();
      expect(names.contains('a.v'), isTrue);
      expect(names.contains('b.v'), isTrue);
    });

    test('DataFrame join leaves non-colliding names unqualified', () async {
      final a = t('a', [
        {'k': 1, 'x': 10},
        {'k': 2, 'x': 20},
      ]);
      final b = t('b', [
        {'k': 1, 'y': 100},
        {'k': 2, 'y': 200},
      ]);
      // The coalesced key plus distinct payload columns collide on nothing, so
      // the shared disambiguate is a no-op — names stay bare.
      final df = await DataFrame.fromTable(
        a,
      ).join(DataFrame.fromTable(b), 'k').execute();
      expect(df.schema.fields.map((f) => f.name).toList(), ['k', 'x', 'y']);
    });
  });

  // ---- Relocated: set-operation schema binding (from audit_round49_test.dart) ----
  group('audit_round49 relocated', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('r49_'));
    tearDown(() => dir.deleteSync(recursive: true));

    group('#3 SQL set-operation schema binding', () {
      late SqlEngine engine;
      setUp(() {
        engine = SqlEngine(spillDir: dir.path);
        Table one(String col, DataType t, List<int> v) => Table(
          name: 'x',
          columns: [
            t == DataType.int32
                ? NumericColumn.int32(name: col, values: v, nullable: false)
                : NumericColumn.int64(name: col, values: v, nullable: false),
          ],
          schema: Schema(
            fields: [Field(name: col, dataType: t, nullable: false)],
          ),
        );
        engine.registerTable('one', one('a', DataType.int64, [1, 2]));
        engine.registerTable('diff', one('x', DataType.int64, [3]));
        engine.registerTable(
          'two',
          Table(
            name: 'y',
            columns: [
              NumericColumn.int64(name: 'a', values: [9], nullable: false),
              NumericColumn.int64(name: 'b', values: [8], nullable: false),
            ],
            schema: Schema(
              fields: [
                Field(name: 'a', dataType: DataType.int64, nullable: false),
                Field(name: 'b', dataType: DataType.int64, nullable: false),
              ],
            ),
          ),
        );
        engine.registerTable('wide', one('a', DataType.int32, [7]));
      });
      tearDown(() => engine.dispose());

      test('mismatched arity throws (was: b silently discarded)', () async {
        await expectLater(
          engine.execute('SELECT a FROM one UNION ALL SELECT a, b FROM two'),
          throwsA(isA<ArgumentError>()),
        );
      });

      test(
        'valid positional union with different names uses left names',
        () async {
          final t = await engine.execute(
            'SELECT a FROM one UNION ALL SELECT x FROM diff',
          );
          expect(t.schema.fields.single.name, 'a'); // left name stamped
          expect(t.column('a')!.toList()..sort(), [1, 2, 3]);
        },
      );

      test('same-type positional union works', () async {
        final t = await engine.execute(
          'SELECT a FROM one UNION ALL SELECT x FROM diff',
        );
        expect(t.column('a')!.toList()..sort(), [1, 2, 3]);
      });

      test(
        'a cross-width union promotes to the common type (Round 51 A1)',
        () async {
          // int32 ∪ int64: the physical set-op now reconciles real branch types and
          // coerces to the common type (int64), so this succeeds with promotion
          // instead of crashing at concat/validate.
          final t = await engine.execute(
            'SELECT a FROM wide UNION ALL SELECT a FROM one',
          );
          expect(t.schema.fields.single.dataType, DataType.int64);
          expect(t.column('a')!.toList()..sort(), [1, 2, 7]);
        },
      );
    });
  });

  // ---- Relocated: set-op promotion + SQL correctness (from audit_round51_test.dart) ----
  group('audit_round51 relocated', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('r51_'));
    tearDown(() => dir.deleteSync(recursive: true));

    group('#A1 set-operation type promotion (was a crash)', () {
      late SqlEngine engine;
      setUp(() => engine = SqlEngine(spillDir: dir.path));
      tearDown(() => engine.dispose());

      Table col(String name, DataType t, List<num> v) => Table(
        name: 'x',
        columns: [
          t == DataType.int32
              ? NumericColumn.int32(
                  name: name,
                  values: v.cast<int>(),
                  nullable: false,
                )
              : t == DataType.int64
              ? NumericColumn.int64(
                  name: name,
                  values: v.cast<int>(),
                  nullable: false,
                )
              : NumericColumn.float64(
                  name: name,
                  values: v.cast<double>(),
                  nullable: false,
                ),
        ],
        schema: Schema(
          fields: [Field(name: name, dataType: t, nullable: false)],
        ),
      );

      test('int32 UNION ALL int64 promotes to int64', () async {
        engine.registerTable('a', col('n', DataType.int32, [1, 2]));
        engine.registerTable('b', col('n', DataType.int64, [1 << 40]));
        final t = await engine.execute(
          'SELECT n FROM a UNION ALL SELECT n FROM b',
        );
        expect(t.column('n')!.toList()..sort(), [1, 2, 1 << 40]);
        expect(t.schema.fields.single.dataType, DataType.int64);
      });

      test('int UNION float promotes to float64', () async {
        engine.registerTable('a', col('n', DataType.int64, [1, 2]));
        engine.registerTable('b', col('n', DataType.float64, [2.5]));
        final t = await engine.execute('SELECT n FROM a UNION SELECT n FROM b');
        expect((t.column('n')!.toList().cast<num>()..sort()), [1.0, 2.0, 2.5]);
        expect(t.schema.fields.single.dataType, DataType.float64);
      });

      // Unifying int64 with float64 is float64 (standard SQL), but float64
      // cannot hold every int64: 2^53+1 rounds DOWN to 2^53. A 64-bit id or
      // counter would be silently rewritten, so the cast refuses rather than
      // round. The alternative — decimal — needs a scale that only the DATA can
      // supply, and a set-op's schema is fixed before any data is read.
      test('an int64 too large for float64 fails loud, not rounds', () async {
        engine.registerTable(
          'a',
          col('n', DataType.int64, [9007199254740993]), // 2^53 + 1
        );
        engine.registerTable('b', col('n', DataType.float64, [0.5]));
        await expectLater(
          engine.execute('SELECT n FROM a UNION ALL SELECT n FROM b'),
          throwsA(isA<ArgumentError>()),
        );
      });

      test('the error names the column and the value', () async {
        engine.registerTable('a', col('n', DataType.int64, [9007199254740993]));
        engine.registerTable('b', col('n', DataType.float64, [0.5]));
        await expectLater(
          engine.execute('SELECT n FROM a UNION ALL SELECT n FROM b'),
          throwsA(
            isA<ArgumentError>().having(
              (e) => e.message.toString(),
              'message',
              allOf(contains('n'), contains('9007199254740993')),
            ),
          ),
        );
      });

      test('an int64 ABOVE 2^53 that is exact still promotes', () async {
        // 2^54 is exactly representable — reject only genuine loss, not size.
        engine.registerTable(
          'a',
          col('n', DataType.int64, [18014398509481984]),
        );
        engine.registerTable('b', col('n', DataType.float64, [0.5]));
        final t = await engine.execute(
          'SELECT n FROM a UNION ALL SELECT n FROM b',
        );
        expect((t.column('n')!.toList().cast<num>()..sort()), [
          0.5,
          18014398509481984.0,
        ]);
      });

      test('2^53 itself promotes (the boundary is inclusive)', () async {
        engine.registerTable('a', col('n', DataType.int64, [9007199254740992]));
        engine.registerTable('b', col('n', DataType.float64, [0.5]));
        final t = await engine.execute(
          'SELECT n FROM a UNION ALL SELECT n FROM b',
        );
        expect(t.column('n')!.toList().contains(9007199254740992.0), isTrue);
      });
    });

    group('#A3/A4/A5 SQL correctness', () {
      late SqlEngine engine;
      setUp(() => engine = SqlEngine(spillDir: dir.path));
      tearDown(() => engine.dispose());

      Table ints(String name, List<int> v) => Table(
        name: 't',
        columns: [NumericColumn.int64(name: name, values: v, nullable: false)],
        schema: Schema(
          fields: [
            Field(name: name, dataType: DataType.int64, nullable: false),
          ],
        ),
      );

      test(
        'A4: overflowing int64 multiply fails loud (no silent wrap)',
        () async {
          engine.registerTable('t', ints('a', [1 << 40]));
          await expectLater(
            engine.execute('SELECT a * a AS p FROM t'),
            throwsA(isA<StateError>()),
          );
        },
      );

      test('A4: non-overflowing int64 arithmetic stays int64', () async {
        engine.registerTable('t', ints('a', [3, 4]));
        final r = await engine.execute('SELECT a * 2 AS p FROM t');
        expect(r.column('p')!.toList()..sort(), [6, 8]);
        expect(r.schema.fields.single.dataType, DataType.int64);
      });

      test('A4: decimal operands compose in per-row arithmetic', () async {
        // A decimal column in per-row arithmetic exercises the kernel's BigInt
        // path (previously `x as num` crashed on a BigInt).
        engine.registerTable(
          't',
          Table(
            name: 't',
            columns: [
              ListColumn<BigInt>(
                name: 'd',
                dataType: DataType.decimal,
                data: [BigInt.from(3), BigInt.from(4)],
                nullable: false,
              ),
            ],
            schema: Schema(
              fields: [
                Field(
                  name: 'd',
                  dataType: DataType.decimal,
                  precision: 10,
                  scale: 0,
                  nullable: false,
                ),
              ],
            ),
          ),
        );
        final r = await engine.execute('SELECT d + d AS s FROM t');
        expect(r.column('s')!.toList()..sort(), [
          BigInt.from(6),
          BigInt.from(8),
        ]);
      });

      test('A3: AVG of large ints uses an exact numerator', () async {
        const big = 9007199254740993; // 2^53+1
        engine.registerTable('t', ints('a', [big, big]));
        final r = await engine.execute('SELECT AVG(a) AS m FROM t');
        expect(r.column('m')!.toList().single, closeTo(big.toDouble(), 2.0));
      });

      test('A5: CASE with a true-OR-null condition matches (3VL)', () async {
        engine.registerTable('t', ints('a', [1]));
        final r = await engine.execute(
          "SELECT CASE WHEN (a = 1 OR NULL) THEN 'y' ELSE 'n' END AS c FROM t",
        );
        expect(r.column('c')!.toList().single, 'y');
      });
    });
  });

  // ---- Relocated: scaled-decimal arithmetic + cross-type UNION (from audit_round53_test.dart) ----
  group('audit_round53 relocated', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('r53_'));
    tearDown(() => dir.deleteSync(recursive: true));

    group('#2 scaled-decimal arithmetic is scale-correct', () {
      // A decimal(10,2) column: 1.50 and 2.30 stored unscaled as 150 and 230.
      Table dec(String name, List<BigInt> unscaled) => Table(
        name: 't',
        columns: [
          ListColumn<BigInt>(
            name: name,
            dataType: DataType.decimal,
            data: unscaled,
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(
              name: name,
              dataType: DataType.decimal,
              precision: 10,
              scale: 2,
              nullable: false,
            ),
          ],
        ),
      );

      late SqlEngine engine;
      setUp(() => engine = SqlEngine(spillDir: dir.path));
      tearDown(() => engine.dispose());

      test('add keeps scale 2 (unscaled add, result scale 2)', () async {
        engine.registerTable('t', dec('a', [BigInt.from(150)]));
        final r = await engine.execute('SELECT a + a AS s FROM t');
        expect(r.schema.fields.single.scale, 2);
        expect(r.column('s')!.toList().single, BigInt.from(300)); // 3.00
      });

      test('multiply adds scales (result scale 4)', () async {
        final t = Table(
          name: 't',
          columns: [
            ListColumn<BigInt>(
              name: 'a',
              dataType: DataType.decimal,
              data: [BigInt.from(150)],
              nullable: false,
            ),
            ListColumn<BigInt>(
              name: 'b',
              dataType: DataType.decimal,
              data: [BigInt.from(230)],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(
                name: 'a',
                dataType: DataType.decimal,
                precision: 10,
                scale: 2,
              ),
              Field(
                name: 'b',
                dataType: DataType.decimal,
                precision: 10,
                scale: 2,
              ),
            ],
          ),
        );
        engine.registerTable('t', t);
        final r = await engine.execute('SELECT a * b AS p FROM t');
        expect(r.schema.fields.single.scale, 4);
        expect(r.column('p')!.toList().single, BigInt.from(34500)); // 3.4500
      });

      test('decimal + int literal aligns scale', () async {
        engine.registerTable('t', dec('a', [BigInt.from(150)])); // 1.50
        final r = await engine.execute('SELECT a + 3 AS s FROM t');
        expect(r.schema.fields.single.scale, 2);
        expect(r.column('s')!.toList().single, BigInt.from(450)); // 4.50
      });
    });

    group('#A2 cross-type UNION promotes (was crash/corruption)', () {
      test(
        'int column UNION a decimal (SUM) promotes to decimal BigInt',
        () async {
          final engine = SqlEngine(spillDir: dir.path);
          addTearDown(engine.dispose);
          engine.registerTable(
            't',
            Table(
              name: 't',
              columns: [
                NumericColumn.int64(name: 'n', values: [5, 6], nullable: false),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'n', dataType: DataType.int64, nullable: false),
                ],
              ),
            ),
          );
          // SUM(n) is decimal(38,0); UNION with the int column n → decimal common
          // type. Previously: castTo(int→decimal) threw ArgumentError mid-query.
          final r = await engine.execute(
            'SELECT n FROM t UNION ALL SELECT SUM(n) FROM t',
          );
          expect(r.schema.fields.single.dataType, DataType.decimal);
          // All values are BigInt (no double-in-decimal corruption).
          expect(r.column('n')!.toList().every((v) => v is BigInt), isTrue);
          expect(r.column('n')!.toList().cast<BigInt>()..sort(), [
            BigInt.from(5),
            BigInt.from(6),
            BigInt.from(11),
          ]);
        },
      );
    });
  });

  // ---- Relocated: function value types + CASE (from review_defects_test.dart) ----
  group('review_defects relocated', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('review_defects'));
    tearDown(() => dir.deleteSync(recursive: true));

    DataType typeOf(Table r, String col) =>
        r.schema.fields.firstWhere((f) => f.name == col).dataType;

    group('#4 function value types match their declared type', () {
      late SqlEngine engine;
      setUp(() {
        engine = SqlEngine();
        engine.registerTable(
          't',
          Table(
            name: 't',
            schema: Schema(
              fields: [
                Field(name: 'i', dataType: DataType.int32, nullable: true),
                Field(name: 'd', dataType: DataType.float64, nullable: false),
              ],
            ),
            columns: [
              NumericColumn.int32(
                name: 'i',
                values: [5, null, -3],
                nullable: true,
              ),
              NumericColumn.float64(
                name: 'd',
                values: [1.5, 2.5, 3.5],
                nullable: false,
              ),
            ],
          ),
        );
      });

      test('ABS(int)->int64, MOD(int,int)->int64, COALESCE(int,double)->float64,'
          ' all Parquet-writable', () async {
        final r = await engine.execute(
          'SELECT ABS(i) AS a, MOD(i, 2) AS m, COALESCE(i, d) AS c FROM t',
        );
        expect(typeOf(r, 'a'), DataType.int64);
        expect(typeOf(r, 'm'), DataType.int64);
        expect(typeOf(r, 'c'), DataType.float64);
        // The COALESCE column must hold doubles (int coerced), not raw ints.
        expect(r.column('c')!.toList().whereType<int>(), isEmpty);
        // Round-trip through Parquet (previously threw 'int is not a subtype of
        // double').
        final path = '${dir.path}/fn.parquet';
        await ParquetWriter(path).write(r);
        final back = await ParquetReader(path).read();
        expect(back.rowCount, 3);
      });
    });

    test('#8 a CASE SQL statement parses and executes', () async {
      final engine = SqlEngine();
      engine.registerTable(
        't',
        Table(
          name: 't',
          schema: Schema(
            fields: [Field(name: 'id', dataType: DataType.int64)],
          ),
          columns: [
            NumericColumn.int64(name: 'id', values: [0, 2, 5], nullable: false),
          ],
        ),
      );
      final r = await engine.execute(
        "SELECT CASE WHEN id > 1 THEN 'big' ELSE 'small' END AS c FROM t",
      );
      expect(r.column('c')!.toList(), ['small', 'big', 'big']);
    });
  });

  // ---- Relocated: SQL DISTINCT over a binary column (from review_round45_test.dart) ----
  group('#1 binary/composite value equality', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('round45_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('SQL DISTINCT over a binary column dedups equal values', () async {
      final engine = SqlEngine(spillDir: dir.path);
      engine.registerTable(
        't',
        Table(
          name: 't',
          columns: [
            ListColumn<List<int>>(
              name: 'k',
              dataType: DataType.binary,
              data: [
                [1, 2],
                [1, 2], // equal bytes, distinct objects
                [3, 4],
              ],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [Field(name: 'k', dataType: DataType.binary)],
          ),
        ),
      );
      final r = await engine.execute('SELECT DISTINCT k FROM t');
      expect(r.rowCount, 2); // identity equality would have kept 3
      await engine.dispose();
    });
  });

  // A decimal column stores an unscaled BigInt plus a scale on its field, while
  // a SQL literal arrives as a plain int or double. Nothing reconciled the two,
  // so equality was silently false and ordering threw outright.
  group('decimal comparison and arithmetic against literals', () {
    late SqlEngine engine;

    /// A decimal(10,[scale]) column named `v` holding [values] UNSCALED, i.e.
    /// `dec([100, 250], scale: 2)` holds 1.00 and 2.50.
    void register(String name, List<int> values, {int scale = 2}) {
      engine.registerTable(
        name,
        Table(
          name: name,
          columns: [
            ListColumn<BigInt>(
              name: 'v',
              dataType: DataType.decimal,
              data: [for (final v in values) BigInt.from(v)],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(
                name: 'v',
                dataType: DataType.decimal,
                precision: 10,
                scale: scale,
                nullable: false,
              ),
            ],
          ),
        ),
      );
    }

    setUp(() {
      engine = SqlEngine();
      register('d', [100, 250, 1000]); // 1.00, 2.50, 10.00
    });
    tearDown(() => engine.dispose());

    Future<List<Object?>> truth(String sql) async =>
        (await engine.execute(sql)).column('r')!.toList();
    Future<List<Object?>> kept(String sql) async =>
        (await engine.execute(sql)).column('v')!.toList();

    test('= matches a float literal of the same value', () async {
      expect(await truth('SELECT v = 1.00 AS r FROM d'), [true, false, false]);
      expect(await truth('SELECT v = 2.5 AS r FROM d'), [false, true, false]);
      expect(await truth('SELECT v = 10.0 AS r FROM d'), [false, false, true]);
    });

    test('= matches an int literal of the same value', () async {
      // 1.00 == 1 and 10.00 == 10 — the int literal is scale 0.
      expect(await truth('SELECT v = 1 AS r FROM d'), [true, false, false]);
      expect(await truth('SELECT v = 10 AS r FROM d'), [false, false, true]);
    });

    test('<> is the inverse of =', () async {
      expect(await truth('SELECT v <> 1.00 AS r FROM d'), [false, true, true]);
    });

    test('a literal finer than the column scale matches nothing', () async {
      // No decimal(_,2) value equals 1.005, so = is false and <> is true.
      expect(await truth('SELECT v = 1.005 AS r FROM d'), [
        false,
        false,
        false,
      ]);
      expect(await truth('SELECT v <> 1.005 AS r FROM d'), [true, true, true]);
    });

    test('ordering works against float literals', () async {
      expect(await truth('SELECT v > 2.00 AS r FROM d'), [false, true, true]);
      expect(await truth('SELECT v < 3.00 AS r FROM d'), [true, true, false]);
      expect(await truth('SELECT v >= 2.50 AS r FROM d'), [false, true, true]);
      expect(await truth('SELECT v <= 2.50 AS r FROM d'), [true, true, false]);
    });

    test('ordering works against int literals', () async {
      expect(await truth('SELECT v > 2 AS r FROM d'), [false, true, true]);
      expect(await truth('SELECT v <= 2 AS r FROM d'), [true, false, false]);
    });

    test('ordering against a finer literal rounds the right way', () async {
      // 1.005 sits between 1.00 and 1.01: only 1.00 is below it.
      expect(await truth('SELECT v < 1.005 AS r FROM d'), [true, false, false]);
      expect(await truth('SELECT v > 1.005 AS r FROM d'), [false, true, true]);
      // >= and <= agree with > and < because no value equals 1.005.
      expect(await truth('SELECT v >= 1.005 AS r FROM d'), [false, true, true]);
      expect(await truth('SELECT v <= 1.005 AS r FROM d'), [
        true,
        false,
        false,
      ]);
    });

    test('WHERE / BETWEEN / IN filter correctly', () async {
      expect(await kept('SELECT v FROM d WHERE v = 1.00'), [BigInt.from(100)]);
      expect(await kept('SELECT v FROM d WHERE v BETWEEN 1.00 AND 3.00'), [
        BigInt.from(100),
        BigInt.from(250),
      ]);
      expect(await kept('SELECT v FROM d WHERE v IN (1.00, 2.50)'), [
        BigInt.from(100),
        BigInt.from(250),
      ]);
      expect(await kept('SELECT v FROM d WHERE v NOT IN (1.00)'), [
        BigInt.from(250),
        BigInt.from(1000),
      ]);
    });

    test('two decimal columns of different scales compare by value', () async {
      // e holds 1.0000 and 2.5000 at scale 4 — the same values as d's first two.
      register('e', [10000, 25000], scale: 4);
      expect(
        await truth('SELECT d.v = e.v AS r FROM d, e WHERE d.v = 1.00'),
        [true, false], // 1.00 vs 1.0000 then 1.00 vs 2.5000
      );
    });

    test('arithmetic with a float literal yields a float', () async {
      // decimal OP float is float-typed (any float operand floats the result),
      // so the decimal side must convert rather than crash.
      final t = await engine.execute('SELECT v + 1.5 AS r FROM d');
      expect(t.schema.field('r')!.dataType, DataType.float64);
      expect(t.column('r')!.toList(), [2.5, 4.0, 11.5]);
    });

    test('arithmetic with an int literal stays an exact decimal', () async {
      // Unchanged behaviour: int literals are scale 0 and align exactly.
      final t = await engine.execute('SELECT v + 3 AS r FROM d');
      expect(t.schema.field('r')!.dataType, DataType.decimal);
      expect(t.column('r')!.toList(), [
        BigInt.from(400),
        BigInt.from(550),
        BigInt.from(1300),
      ]);
    });

    test('a NULL decimal stays UNKNOWN', () async {
      engine.registerTable(
        'n',
        Table(
          name: 'n',
          columns: [
            ListColumn<BigInt>(
              name: 'v',
              dataType: DataType.decimal,
              data: [BigInt.from(100), null],
              nullable: true,
            ),
          ],
          schema: Schema(
            fields: [
              Field(
                name: 'v',
                dataType: DataType.decimal,
                precision: 10,
                scale: 2,
                nullable: true,
              ),
            ],
          ),
        ),
      );
      expect(await truth('SELECT v = 1.00 AS r FROM n'), [true, null]);
      expect(await truth('SELECT v > 0.50 AS r FROM n'), [true, null]);
    });
  });

  // A SQL integer literal must be typed by its RANGE. Typing every int as int32
  // makes the column's declared dtype a lie for anything past 2^31, and a boxed
  // column carries that lie all the way to a fixed-width encoder, which
  // truncates it silently.
  group('integer literal width', () {
    late SqlEngine engine;
    setUp(() => engine = SqlEngine());
    tearDown(() => engine.dispose());

    Future<Field> field(String literal) async =>
        (await engine.execute('SELECT $literal AS v')).schema.field('v')!;
    Future<Object?> value(String literal) async => (await engine.execute(
      'SELECT $literal AS v',
    )).column('v')!.toList().first;

    test('a literal is typed by its range, not always int32', () async {
      expect((await field('1')).dataType, DataType.int8);
      expect((await field('200')).dataType, DataType.int16);
      expect((await field('100000')).dataType, DataType.int32);
      expect((await field('2147483648')).dataType, DataType.int64);
      expect((await field('-2147483649')).dataType, DataType.int64);
      expect((await field('9223372036854775807')).dataType, DataType.int64);
    });

    test('int32 boundaries fall on the right side', () async {
      expect((await field('2147483647')).dataType, DataType.int32);
      expect((await field('-2147483648')).dataType, DataType.int32);
    });

    test('values past 2^31 survive intact', () async {
      expect(await value('2147483648'), 2147483648);
      expect(await value('9223372036854775807'), 9223372036854775807);
    });

    test('SELECT and VALUES agree on a wide literal', () async {
      // These diverged: VALUES range-checked and threw, SELECT silently
      // mislabelled the column int32.
      expect((await field('2147483648')).dataType, DataType.int64);
      final t = await engine.execute(
        'SELECT n FROM VALUES (2147483648) AS t(n)',
      );
      expect(t.schema.field('n')!.dataType, DataType.int64);
      expect(t.column('n')!.toList(), [2147483648]);
    });
  });

  // A DISTINCT accumulator keys a Set on raw column values, and a
  // Uint8List/List/Map hashes by IDENTITY in Dart — so every row's freshly
  // decoded blob looked distinct. `canonicalKey` exists for exactly this and is
  // used by the hash join and the grace router; the aggregate was the one site
  // that kept the raw value.
  group('DISTINCT aggregates over by-identity values', () {
    late SqlEngine engine;
    setUp(() => engine = SqlEngine());
    tearDown(() => engine.dispose());

    /// 100 rows in one group whose `v` is the SAME 4 bytes each time, decoded
    /// into a separate object per row.
    void registerBinary() {
      engine.registerTable(
        'b',
        Table(
          name: 'b',
          columns: [
            ListColumn<String>(
              name: 'g',
              dataType: DataType.string,
              data: List.filled(100, 'k'),
              nullable: false,
            ),
            ListColumn<Uint8List>(
              name: 'v',
              dataType: DataType.binary,
              data: [
                for (var i = 0; i < 100; i++) Uint8List.fromList([1, 2, 3, 4]),
              ],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'g', dataType: DataType.string, nullable: false),
              Field(name: 'v', dataType: DataType.binary, nullable: false),
            ],
          ),
        ),
      );
    }

    // Grouping by an EXPRESSION with a mixed aggregate bypasses the planner's
    // DISTINCT rewrites, so this reaches the raw accumulator.
    test('COUNT(DISTINCT binary) counts by content, not identity', () async {
      registerBinary();
      final t = await engine.execute(
        'SELECT LOWER(g) AS k, COUNT(DISTINCT v) AS n, COUNT(*) AS c '
        'FROM b GROUP BY LOWER(g)',
      );
      final row = t.toRows().single;
      expect(row['c'], 100);
      expect(row['n'], 1, reason: '100 identical blobs are ONE distinct value');
    });

    test('genuinely distinct blobs still count separately', () async {
      engine.registerTable(
        'b2',
        Table(
          name: 'b2',
          columns: [
            ListColumn<String>(
              name: 'g',
              dataType: DataType.string,
              data: List.filled(6, 'k'),
              nullable: false,
            ),
            ListColumn<Uint8List>(
              name: 'v',
              dataType: DataType.binary,
              data: [
                for (var i = 0; i < 6; i++) Uint8List.fromList([i % 3]),
              ],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'g', dataType: DataType.string, nullable: false),
              Field(name: 'v', dataType: DataType.binary, nullable: false),
            ],
          ),
        ),
      );
      final t = await engine.execute(
        'SELECT LOWER(g) AS k, COUNT(DISTINCT v) AS n FROM b2 GROUP BY LOWER(g)',
      );
      expect(t.toRows().single['n'], 3);
    });

    // Found while writing the tests above. The all-DISTINCT rewrite stacks two
    // aggregates and reads the inner's OUTPUT, but it remapped only the distinct
    // ARGUMENT to the inner's emitted column — the group keys still referenced
    // base columns the inner no longer emits. A mixed aggregate bails out of the
    // rewrite and silently worked, which hid it.
    test('GROUP BY an expression with an all-DISTINCT aggregate works', () async {
      engine.registerTable(
        'e',
        Table(
          name: 'e',
          columns: [
            ListColumn<String>(
              name: 'g',
              dataType: DataType.string,
              data: ['a', 'a', 'b', 'b'],
              nullable: false,
            ),
            NumericColumn.int64(
              name: 'v',
              values: [1, 1, 2, 3],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'g', dataType: DataType.string, nullable: false),
              Field(name: 'v', dataType: DataType.int64, nullable: false),
            ],
          ),
        ),
      );
      final t = await engine.execute(
        'SELECT LOWER(g) AS k, COUNT(DISTINCT v) AS n FROM e GROUP BY LOWER(g)',
      );
      final byKey = {for (final r in t.toRows()) r['k']: r['n']};
      expect(byKey, {'a': 1, 'b': 2});
    });

    test('the same shape with SUM(DISTINCT) works', () async {
      engine.registerTable(
        'e2',
        Table(
          name: 'e2',
          columns: [
            ListColumn<String>(
              name: 'g',
              dataType: DataType.string,
              data: ['a', 'a', 'b'],
              nullable: false,
            ),
            NumericColumn.int64(name: 'v', values: [5, 5, 7], nullable: false),
          ],
          schema: Schema(
            fields: [
              Field(name: 'g', dataType: DataType.string, nullable: false),
              Field(name: 'v', dataType: DataType.int64, nullable: false),
            ],
          ),
        ),
      );
      final t = await engine.execute(
        'SELECT LOWER(g) AS k, SUM(DISTINCT v) AS s FROM e2 GROUP BY LOWER(g)',
      );
      final byKey = {for (final r in t.toRows()) r['k']: r['s'].toString()};
      expect(byKey, {'a': '5', 'b': '7'});
    });

    test('a plain-column group key still works', () async {
      engine.registerTable(
        'e3',
        Table(
          name: 'e3',
          columns: [
            ListColumn<String>(
              name: 'g',
              dataType: DataType.string,
              data: ['a', 'a', 'b'],
              nullable: false,
            ),
            NumericColumn.int64(name: 'v', values: [1, 1, 2], nullable: false),
          ],
          schema: Schema(
            fields: [
              Field(name: 'g', dataType: DataType.string, nullable: false),
              Field(name: 'v', dataType: DataType.int64, nullable: false),
            ],
          ),
        ),
      );
      final t = await engine.execute(
        'SELECT g, COUNT(DISTINCT v) AS n FROM e3 GROUP BY g',
      );
      expect({for (final r in t.toRows()) r['g']: r['n']}, {'a': 1, 'b': 1});
    });

    test('SUM/AVG(DISTINCT) dedup by content too', () async {
      // The same raw-Set bug lives in _SumAcc/_AvgAcc.
      engine.registerTable(
        'n',
        Table(
          name: 'n',
          columns: [
            ListColumn<String>(
              name: 'g',
              dataType: DataType.string,
              data: List.filled(4, 'k'),
              nullable: false,
            ),
            NumericColumn.int64(
              name: 'v',
              values: [5, 5, 7, 7],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'g', dataType: DataType.string, nullable: false),
              Field(name: 'v', dataType: DataType.int64, nullable: false),
            ],
          ),
        ),
      );
      final t = await engine.execute(
        'SELECT LOWER(g) AS k, SUM(DISTINCT v) AS s FROM n GROUP BY LOWER(g)',
      );
      expect(t.toRows().single['s'], Decimal(BigInt.from(12), 0)); // 5 + 7
    });
  });

  // An expression's column must actually HOLD values of its declared dtype.
  // CASE widened its branches' types but never coerced their values, so a
  // float64-declared column held Dart ints — a lie that a Parquet write rejects
  // and that splits one decimal value into two DISTINCT groups (BigInt.from(0)
  // != 0). Coercion belongs in `evaluate`, which both the vectorized and the
  // per-row paths go through, so they cannot drift apart.
  group('CASE / COALESCE value coercion', () {
    late SqlEngine engine;
    late Directory dir;
    setUp(() {
      dir = Directory.systemTemp.createTempSync('coerce_');
      engine = SqlEngine(spillDir: dir.path);
      engine.registerTable(
        't',
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'id', values: [1, 2, 3], nullable: false),
            ListColumn<BigInt>(
              name: 'd',
              dataType: DataType.decimal,
              data: [BigInt.from(150), BigInt.from(250), BigInt.from(350)],
              nullable: true,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
              Field(
                name: 'd',
                dataType: DataType.decimal,
                precision: 10,
                scale: 2,
                nullable: true,
              ),
            ],
          ),
        ),
      );
    });
    tearDown(() async {
      await engine.dispose();
      dir.deleteSync(recursive: true);
    });

    test('a mixed int/float CASE holds doubles, not ints', () async {
      final t = await engine.execute(
        'SELECT CASE WHEN id > 1 THEN 1 ELSE 2.5 END AS c FROM t',
      );
      expect(t.schema.field('c')!.dataType, DataType.float64);
      expect(
        t.column('c')!.toList().map((v) => v.runtimeType).toSet(),
        {double},
        reason: 'every value must match the declared dtype',
      );
      expect(t.column('c')!.toList(), [2.5, 1.0, 1.0]);
    });

    test('a mixed int/float CASE survives a Parquet write', () async {
      // The dtype lie surfaced here as "int is not a subtype of double".
      final t = await engine.execute(
        'SELECT CASE WHEN id > 1 THEN 1 ELSE 2.5 END AS c FROM t',
      );
      final path = '${dir.path}/case.parquet';
      await ParquetWriter(path).write(t);
      expect((await ParquetReader(path).read()).column('c')!.toList(), [
        2.5,
        1.0,
        1.0,
      ]);
    });

    test('a decimal CASE keeps its scale', () async {
      final t = await engine.execute(
        'SELECT CASE WHEN id > 0 THEN d ELSE d END AS c FROM t',
      );
      expect(t.schema.field('c')!.dataType, DataType.decimal);
      expect(t.schema.field('c')!.scale, 2, reason: 'else it reads as scale 0');
      expect(t.toRows().first['c'].toString(), '1.50'); // was 150
    });

    test('a decimal CASE with an int branch holds BigInt', () async {
      // `ELSE 0` put a raw int in a decimal column; BigInt.from(0) != 0, so a
      // later DISTINCT saw two different values.
      final t = await engine.execute(
        'SELECT CASE WHEN id > 2 THEN d ELSE 0 END AS c FROM t',
      );
      expect(t.schema.field('c')!.dataType, DataType.decimal);
      expect(t.column('c')!.toList().map((v) => v.runtimeType).toSet(), {
        BigInt.zero.runtimeType,
      });
      expect(t.toRows().map((r) => r['c'].toString()), [
        '0.00',
        '0.00',
        '3.50',
      ]);
    });

    test('DISTINCT over a decimal CASE does not split one value', () async {
      final t = await engine.execute(
        'SELECT DISTINCT CASE WHEN id > 2 THEN d ELSE 0 END AS c FROM t',
      );
      expect(t.rowCount, 2); // {0.00, 3.50}, not 3
    });

    test('COALESCE coerces per-row too, not just vectorized', () async {
      // The float fix lived only in evalColumn, so the per-row path (a filter
      // predicate) still compared an uncoerced value.
      final t = await engine.execute(
        'SELECT COALESCE(id, 2.5) AS c FROM t WHERE COALESCE(id, 2.5) > 1',
      );
      expect(t.column('c')!.toList().map((v) => v.runtimeType).toSet(), {
        double,
      });
      expect(t.column('c')!.toList(), [2.0, 3.0]);
    });

    test('COALESCE over a decimal keeps its scale and holds BigInt', () async {
      // Same defect as CASE: the widened type without the widened VALUES, so a
      // decimal column held a raw int and rendered 1.50 as 150.
      final t = await engine.execute('SELECT COALESCE(d, 0) AS c FROM t');
      expect(t.schema.field('c')!.dataType, DataType.decimal);
      expect(t.schema.field('c')!.scale, 2);
      expect(t.column('c')!.toList().map((v) => v.runtimeType).toSet(), {
        BigInt.zero.runtimeType,
      });
      expect(t.toRows().first['c'].toString(), '1.50');
    });

    test('a plain CASE with one type is unaffected', () async {
      final t = await engine.execute(
        "SELECT CASE WHEN id > 1 THEN 'hi' ELSE 'lo' END AS c FROM t",
      );
      expect(t.column('c')!.toList(), ['lo', 'hi', 'hi']);
    });

    test('a CASE with no ELSE still yields NULL', () async {
      final t = await engine.execute(
        'SELECT CASE WHEN id > 2 THEN 1.5 END AS c FROM t',
      );
      expect(t.column('c')!.toList(), [null, null, 1.5]);
    });
  });

  // A decimal column holds unscaled BigInt, which is not a `num` — so the SUM
  // and AVG accumulators dropped every value and returned NULL. MIN/MAX worked
  // (BigInt is Comparable), which is what marked this as an oversight.
  group('aggregates over a decimal column', () {
    late SqlEngine engine;

    /// decimal(10,[scale]) column `v` holding [values] UNSCALED, i.e.
    /// `register([150, 250], scale: 2)` holds 1.50 and 2.50.
    void register(List<int?> values, {int scale = 2}) {
      engine.registerTable(
        'd',
        Table(
          name: 'd',
          columns: [
            ListColumn<BigInt>(
              name: 'v',
              dataType: DataType.decimal,
              data: [for (final v in values) v == null ? null : BigInt.from(v)],
              nullable: true,
            ),
          ],
          schema: Schema(
            fields: [
              Field(
                name: 'v',
                dataType: DataType.decimal,
                precision: 10,
                scale: scale,
                nullable: true,
              ),
            ],
          ),
        ),
      );
    }

    setUp(() => engine = SqlEngine());
    tearDown(() => engine.dispose());

    Future<Object?> one(String sql) async =>
        (await engine.execute(sql)).toRows().single['r'];

    test('SUM keeps the value and the scale', () async {
      register([150, 250]); // 1.50 + 2.50 = 4.00
      final t = await engine.execute('SELECT SUM(v) AS r FROM d');
      expect(t.schema.field('r')!.dataType, DataType.decimal);
      expect(
        t.schema.field('r')!.scale,
        2,
        reason: 'summing unscaled keeps it',
      );
      expect(t.toRows().single['r'], Decimal(BigInt.from(400), 2));
      expect(t.toRows().single['r'].toString(), '4.00');
    });

    test('AVG scales back down before dividing', () async {
      // Unscaled 150+250 = 400; the average is 2.00, NOT 200.
      register([150, 250]);
      expect(await one('SELECT AVG(v) AS r FROM d'), 2.0);
    });

    test('SUM stays exact past 2^63', () async {
      register([
        8000000000000000000,
        8000000000000000000,
        8000000000000000000,
      ], scale: 0);
      expect(
        await one('SELECT SUM(v) AS r FROM d'),
        Decimal(BigInt.parse('24000000000000000000'), 0),
      );
    });

    test('nulls are skipped, not counted', () async {
      register([150, null, 250]);
      expect(
        await one('SELECT SUM(v) AS r FROM d'),
        Decimal(BigInt.from(400), 2),
      );
      expect(await one('SELECT AVG(v) AS r FROM d'), 2.0); // 2 rows, not 3
    });

    test('an all-NULL decimal column aggregates to NULL', () async {
      register([null, null]);
      expect(await one('SELECT SUM(v) AS r FROM d'), isNull);
      expect(await one('SELECT AVG(v) AS r FROM d'), isNull);
    });

    test('MIN/MAX still work (they always did)', () async {
      register([150, 250]);
      expect(
        await one('SELECT MIN(v) AS r FROM d'),
        Decimal(BigInt.from(150), 2),
      );
      expect(
        await one('SELECT MAX(v) AS r FROM d'),
        Decimal(BigInt.from(250), 2),
      );
    });

    test('SUM over a non-numeric column fails loud', () async {
      // Previously swallowed to NULL by the same `is! num` guard.
      engine.registerTable(
        's',
        Table(
          name: 's',
          columns: [
            ListColumn<String>(
              name: 'v',
              dataType: DataType.string,
              data: ['a', 'b'],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [Field(name: 'v', dataType: DataType.string)],
          ),
        ),
      );
      await expectLater(
        engine.execute('SELECT SUM(v) AS r FROM s'),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('an integer SUM is unchanged', () async {
      engine.registerTable(
        'i',
        Table(
          name: 'i',
          columns: [
            NumericColumn.int64(name: 'v', values: [1, 2, 3], nullable: false),
          ],
          schema: Schema(
            fields: [Field(name: 'v', dataType: DataType.int64)],
          ),
        ),
      );
      expect(
        (await engine.execute(
          'SELECT SUM(v) AS r FROM i',
        )).toRows().single['r'],
        Decimal(BigInt.from(6), 0),
      );
    });
  });

  // A decimal is an unscaled BigInt on the Column with its SCALE on the Field,
  // so any site that rebuilds a Field (or an expression) by hand and forgets
  // `scale` silently renders 1.5 as "15" and compares it as 15. These pin every
  // such site.
  // An outer join manufactures NULLs on the null-producing side, so that side's
  // fields must become nullable. They were copied verbatim, which only shows up
  // when a source declares a column non-nullable — as Parquet does for any
  // column that happens to hold no nulls.
  group('outer join widens nullability', () {
    late Directory dir;
    late SqlEngine engine;
    setUp(() async {
      dir = Directory.systemTemp.createTempSync('sqljoin_');
      engine = SqlEngine();
    });
    tearDown(() async {
      await engine.dispose();
      dir.deleteSync(recursive: true);
    });

    /// Register [name] from a Parquet round trip, which is what declares a
    /// no-null column REQUIRED (the writer marks a leaf required when the column
    /// holds no nulls, and the reader reads it back non-nullable).
    Future<void> parquet(String name, List<Map<String, Object?>> rows) async {
      final path = '${dir.path}/$name.parquet';
      await ParquetWriter(path).write(Table.fromRows(name, rows));
      final table = await ParquetReader(path).read();
      // The `${name}n` string column must come back REQUIRED or this fixture
      // proves nothing. (The int key does NOT — NumericColumn.hasNulls reports
      // "is nullable", so the writer marks it optional. See audit #3.)
      expect(
        table.schema.field('${name}n')!.nullable,
        isFalse,
        reason: 'the fixture needs a required column to be meaningful',
      );
      engine.registerTable(name, table);
    }

    test(
      'LEFT JOIN over a required column returns the unmatched row',
      () async {
        await parquet('a', [
          {'k': 1, 'an': 'x'},
          {'k': 2, 'an': 'y'},
        ]);
        await parquet('b', [
          {'k': 1, 'bn': 'p'},
        ]);
        // k=2 has no match, so `bn` — declared required — is null.
        final r = await engine.execute(
          'SELECT * FROM a LEFT JOIN b ON a.k = b.k',
        );
        // Both keys survive, qualified — the unmatched left row keeps its own.
        final byKey = {
          for (final row in r.toRows()) row['a.k'] as int: row['bn'],
        };
        expect(byKey, {1: 'p', 2: null});
        expect(r.schema.field('bn')!.nullable, isTrue);
      },
    );

    test('RIGHT JOIN widens the left side', () async {
      await parquet('a', [
        {'k': 1, 'an': 'x'},
      ]);
      await parquet('b', [
        {'k': 1, 'bn': 'p'},
        {'k': 2, 'bn': 'q'},
      ]);
      final r = await engine.execute(
        'SELECT * FROM a RIGHT JOIN b ON a.k = b.k',
      );
      expect(r.rowCount, 2);
      expect(r.schema.field('an')!.nullable, isTrue);
    });

    test('FULL JOIN widens both sides', () async {
      await parquet('a', [
        {'k': 1, 'an': 'x'},
        {'k': 3, 'an': 'z'},
      ]);
      await parquet('b', [
        {'k': 1, 'bn': 'p'},
        {'k': 2, 'bn': 'q'},
      ]);
      final r = await engine.execute(
        'SELECT * FROM a FULL JOIN b ON a.k = b.k',
      );
      expect(r.rowCount, 3);
      expect(r.schema.field('an')!.nullable, isTrue);
      expect(r.schema.field('bn')!.nullable, isTrue);
    });

    test('INNER JOIN does NOT widen — nothing is null-padded', () async {
      await parquet('a', [
        {'k': 1, 'an': 'x'},
      ]);
      await parquet('b', [
        {'k': 1, 'bn': 'p'},
      ]);
      final r = await engine.execute(
        'SELECT * FROM a INNER JOIN b ON a.k = b.k',
      );
      expect(r.rowCount, 1);
      expect(r.schema.field('bn')!.nullable, isFalse);
      expect(r.schema.field('an')!.nullable, isFalse);
    });
  });

  // SQL's `%` matches ANY sequence of characters and `_` any single one — a
  // newline is not special. The LIKE regex is built without dotAll, where `.`
  // excludes line terminators and `$` anchors at end-of-input, so every pattern
  // silently failed against a value containing one.
  group('LIKE spans line terminators', () {
    late SqlEngine engine;
    setUp(() {
      engine = SqlEngine();
      engine.registerTable(
        't',
        Table.fromRows('t', [
          {'id': 1, 's': 'line1\nline2'},
          {'id': 2, 's': 'plain'},
          {'id': 3, 's': 'a\nb'},
          {'id': 4, 's': 'a\rb'},
        ]),
      );
    });
    tearDown(() => engine.dispose());

    Future<List<int>> ids(String predicate) async {
      final r = await engine.execute('SELECT id FROM t WHERE $predicate');
      return r.column('id')!.toList().cast<int>();
    }

    test("'%' matches every value, newlines included", () async {
      expect(await ids("s LIKE '%'"), [1, 2, 3, 4]);
    });

    test('%substring% spans a newline', () async {
      expect(await ids("s LIKE '%line2%'"), [1]);
    });

    test('_ matches a newline as a single character', () async {
      expect(await ids("s LIKE 'a_b'"), [3, 4]);
    });

    test('a carriage return is matched too', () async {
      expect(await ids("s LIKE '%\rb'"), [4]);
    });

    test('NOT LIKE stays the complement', () async {
      expect(await ids("s NOT LIKE '%'"), isEmpty);
    });

    test('a pattern that should not match still does not', () async {
      expect(await ids("s LIKE 'zzz%'"), isEmpty);
    });
  });

  group('decimal scale threading', () {
    // Mixed int/float infers decimal(2,1): unscaled 15, 35, 20.
    Table source() => Table.fromRows('t', [
      {'d': 1.5},
      {'d': 3.5},
      {'d': 2},
    ]);

    late SqlEngine engine;
    setUp(() {
      engine = SqlEngine();
      engine.registerTable('t', source());
    });
    tearDown(() => engine.dispose());

    Future<Table> run(String sql) => engine.execute(sql);

    test('HAVING over a decimal group key agrees with WHERE', () async {
      // The same predicate must not depend on which operator binds it.
      final where = await run('SELECT d FROM t WHERE d > 1.5');
      final having = await run(
        'SELECT d, COUNT(*) AS n FROM t GROUP BY d HAVING d > 1.5',
      );
      expect(where.rowCount, 2);
      expect(having.rowCount, 2);
    });

    test('GROUP BY keeps the decimal scale', () async {
      final result = await run('SELECT d, COUNT(*) AS n FROM t GROUP BY d');
      expect(result.schema.field('d')!.scale, 1);
      expect(
        {for (final r in result.toRows()) r['d'].toString()},
        {'1.5', '3.5', '2.0'},
      );
    });

    test('SELECT DISTINCT keeps the decimal scale', () async {
      final result = await run('SELECT DISTINCT d FROM t');
      expect(result.schema.field('d')!.scale, 1);
      expect(
        {for (final r in result.toRows()) r['d'].toString()},
        {'1.5', '3.5', '2.0'},
      );
    });

    test('CAST off a decimal respects its scale', () async {
      final result = await run(
        'SELECT CAST(d AS DOUBLE) AS c, CAST(d AS BIGINT) AS i, '
        'CAST(d AS VARCHAR) AS s FROM t WHERE d = 1.5',
      );
      final row = result.toRows().single;
      expect(row['c'], closeTo(1.5, 1e-9));
      expect(row['i'], 1);
      expect(row['s'], '1.5');
    });

    test('CASE comparing a decimal to a literal matches', () async {
      final result = await run(
        "SELECT CASE WHEN d = 3.5 THEN 'hit' ELSE 'miss' END AS c FROM t",
      );
      expect(result.toRows().map((r) => r['c']).toList(), [
        'miss',
        'hit',
        'miss',
      ]);
    });

    test('CASE ordering a decimal against a literal does not throw', () async {
      final result = await run(
        "SELECT CASE WHEN d > 1.5 THEN 'big' ELSE 'small' END AS c FROM t",
      );
      expect(result.toRows().map((r) => r['c']).toList(), [
        'small',
        'big',
        'big',
      ]);
    });

    test('a name-colliding join keeps the decimal scale', () async {
      // disambiguate() only fires when a name collides, and then rewrites EVERY
      // field — so an untouched decimal column loses its scale too.
      engine.registerTable(
        'b',
        Table.fromRows('b', [
          {'k': 1, 'x': 9},
        ]),
      );
      engine.registerTable(
        'a',
        Table.fromRows('a', [
          {'k': 1, 'amt': 1.5},
          {'k': 1, 'amt': 2},
        ]),
      );
      final result = await run('SELECT * FROM a JOIN b ON a.k = b.k');
      expect(result.schema.field('amt')!.scale, 1);
      expect(result.toRows().map((r) => r['amt'].toString()).toList(), [
        '1.5',
        '2.0',
      ]);
    });

    test('DataFrame.distinct keeps the decimal scale', () async {
      final frame = DataFrame.fromTable(source()).distinct();
      final result = await frame.execute();
      expect(result.schema.field('d')!.scale, 1);
      expect(
        {for (final r in result.toRows()) r['d'].toString()},
        {'1.5', '3.5', '2.0'},
      );
    });
  });

  group('VALUES', () {
    late SqlEngine engine;
    setUp(() => engine = SqlEngine());
    tearDown(() => engine.dispose());

    Future<Table> run(String sql) => engine.execute(sql);
    Future<List<Object?>> column(String sql, String name) async =>
        (await run(sql)).column(name)!.toList();

    test('rows default to column1..N', () async {
      final t = await run('SELECT * FROM VALUES (1), (2)');
      expect(t.schema.fieldNames, ['column1']);
      expect(t.toRows().map((r) => r['column1']), [1, 2]);
    });

    test('a column alias list names the columns', () async {
      final t = await run(
        "SELECT n, s FROM VALUES (1, 'a'), (2, 'b') AS t(n, s)",
      );
      expect(t.schema.fieldNames, ['n', 's']);
      expect(t.toRows(), [
        {'n': 1, 's': 'a'},
        {'n': 2, 's': 'b'},
      ]);
    });

    test('the alias qualifies its columns', () async {
      // Pins that VALUES gets an AliasScope like a subquery does; without it
      // `t.n` would not resolve.
      expect(await column('SELECT t.n FROM VALUES (1), (2) AS t(n)', 'n'), [
        1,
        2,
      ]);
    });

    test('a column is typed over ALL rows, not just the first', () async {
      // Typing from row 0 alone would make this int and then feed 2.5 into an
      // int buffer.
      final t = await run('SELECT n FROM VALUES (1), (2.5) AS t(n)');
      expect(t.schema.field('n')!.dataType, DataType.float64);
      expect(t.column('n')!.toList(), [1.0, 2.5]);
    });

    test('a NULL anywhere in a column makes it nullable', () async {
      for (final sql in [
        'SELECT n FROM VALUES (1), (NULL) AS t(n)',
        'SELECT n FROM VALUES (NULL), (1) AS t(n)',
      ]) {
        final t = await run(sql);
        expect(t.schema.field('n')!.nullable, isTrue, reason: sql);
        expect(t.column('n')!.toList().contains(null), isTrue, reason: sql);
      }
    });

    test('cells may be constant expressions', () async {
      expect(await column('SELECT n FROM VALUES (1 + 1) AS t(n)', 'n'), [2]);
    });

    test('VALUES composes with the rest of the query', () async {
      expect(
        await column(
          'SELECT n FROM VALUES (1), (2), (3) AS t(n) WHERE n > 1',
          'n',
        ),
        [2, 3],
      );
      expect(
        (await run(
          'SELECT SUM(n) AS s FROM VALUES (1), (2), (3) AS t(n)',
        )).column('s')!.toList().first,
        BigInt.from(6),
      );
    });

    test('SELECT with no FROM works', () async {
      // The no-FROM path builds the same Values node, so it threw until VALUES
      // was implemented.
      expect((await run('SELECT 1 AS v')).column('v')!.toList(), [1]);
      expect((await run('SELECT 1 + 1 AS v')).column('v')!.toList(), [2]);
      expect((await run("SELECT 'hi' AS v")).column('v')!.toList(), ['hi']);
    });

    test('ragged rows fail loud', () async {
      await expectLater(
        run('SELECT n FROM VALUES (1), (2, 3) AS t(n)'),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('an alias list of the wrong arity fails loud', () async {
      await expectLater(
        run('SELECT n FROM VALUES (1) AS t(n, extra)'),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('a cell referencing a column fails loud', () async {
      // VALUES has no input rows, so a column reference cannot resolve — it
      // must throw rather than quietly produce NULL.
      await expectLater(
        run('SELECT n FROM VALUES (x) AS t(n)'),
        throwsA(isA<StateError>()),
      );
    });
  });

  // SQL three-valued logic for IN/NOT IN. These pin BEHAVIOUR, not the tree
  // shape, so they survive IN becoming a native node: a NULL in the list makes a
  // non-match NULL (not false), which WHERE then excludes — the two are only
  // distinguishable by selecting the expression, which is what these do.
  group('IN / NOT IN three-valued logic', () {
    late SqlEngine engine;

    Table ints(String name, List<int?> values) => Table(
      name: name,
      columns: [
        ListColumn<int>(
          name: 'n',
          dataType: DataType.int64,
          data: values,
          nullable: true,
        ),
      ],
      schema: Schema(
        fields: [Field(name: 'n', dataType: DataType.int64, nullable: true)],
      ),
    );

    setUp(() {
      engine = SqlEngine();
      engine.registerTable('t', ints('t', [1, 3]));
      engine.registerTable('u', ints('u', [1, null]));
    });
    tearDown(() => engine.dispose());

    /// The IN expression's value per row — `true` / `false` / `null` (UNKNOWN).
    Future<List<Object?>> truth(String sql) async =>
        (await engine.execute(sql)).column('r')!.toList();

    test('a NULL in the list makes a non-match UNKNOWN, not false', () async {
      // 1 matches -> TRUE; 3 matches nothing but the list holds NULL -> UNKNOWN.
      expect(await truth('SELECT n IN (1, 2, NULL) AS r FROM t'), [true, null]);
    });

    test('without a NULL the list behaves classically', () async {
      expect(await truth('SELECT n IN (1, 2) AS r FROM t'), [true, false]);
      expect(await truth('SELECT n NOT IN (1, 2) AS r FROM t'), [false, true]);
    });

    test('NOT IN: a match is FALSE even when the list holds NULL', () async {
      // 1 NOT IN (1, NULL) is FALSE (a definite match dominates), but
      // 3 NOT IN (1, NULL) is UNKNOWN — this asymmetry is the classic trap.
      expect(await truth('SELECT n NOT IN (1, NULL) AS r FROM t'), [
        false,
        null,
      ]);
    });

    test('a NULL probe is UNKNOWN against any list', () async {
      expect(await truth('SELECT n IN (1, 2) AS r FROM u'), [true, null]);
      expect(await truth('SELECT n NOT IN (1, 2) AS r FROM u'), [false, null]);
    });

    test('WHERE keeps only TRUE — UNKNOWN is excluded like false', () async {
      final kept = await engine.execute(
        'SELECT n FROM t WHERE n IN (1, 2, NULL)',
      );
      expect(kept.toRows().map((r) => r['n']), [1]);
      // Every row is either FALSE or UNKNOWN, so NOT IN keeps nothing.
      final none = await engine.execute(
        'SELECT n FROM t WHERE n NOT IN (1, NULL)',
      );
      expect(none.rowCount, 0);
    });

    test('IN over strings', () async {
      engine.registerTable(
        's',
        Table(
          name: 's',
          columns: [
            ListColumn<String>(
              name: 'n',
              dataType: DataType.string,
              data: ['a', 'b', 'c'],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [Field(name: 'n', dataType: DataType.string)],
          ),
        ),
      );
      expect(await truth("SELECT n IN ('a', 'c') AS r FROM s"), [
        true,
        false,
        true,
      ]);
    });

    test('an int literal matches a float column', () async {
      engine.registerTable(
        'f',
        Table(
          name: 'f',
          columns: [
            NumericColumn.float64(
              name: 'n',
              values: [1.0, 2.5],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [Field(name: 'n', dataType: DataType.float64)],
          ),
        ),
      );
      // 1 vs 1.0 must match — a hash-set probe relies on Dart's int/double
      // hashCode agreeing here, so this pins the fast path's key assumption.
      expect(await truth('SELECT n IN (1) AS r FROM f'), [true, false]);
      expect(await truth('SELECT n IN (2.5) AS r FROM f'), [false, true]);
    });

    test('a non-literal list element still evaluates', () async {
      // `n IN (n, 2)` is trivially TRUE — pins that the list is not restricted
      // to constants (a hash-set fast path must fall back here).
      expect(await truth('SELECT n IN (n, 2) AS r FROM t'), [true, true]);
    });

    test('IN matches a decimal column, and agrees with =', () async {
      // The hash-set fast path deliberately skips decimal (BigInt.from(1) != 1),
      // so IN falls back to comparisons — pin that it lands on the same answer
      // as plain equality and cannot silently diverge from it.
      engine.registerTable(
        'd',
        Table(
          name: 'd',
          columns: [
            ListColumn<BigInt>(
              name: 'n',
              dataType: DataType.decimal,
              data: [BigInt.from(100), BigInt.from(250)],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(
                name: 'n',
                dataType: DataType.decimal,
                precision: 10,
                scale: 2,
                nullable: false,
              ),
            ],
          ),
        ),
      );
      // d holds 1.00 and 2.50 (unscaled 100 / 250 at scale 2).
      expect(await truth('SELECT n IN (1.00) AS r FROM d'), [true, false]);
      expect(
        await truth('SELECT n IN (1.00) AS r FROM d'),
        await truth('SELECT n = 1.00 AS r FROM d'),
      );
      expect(await truth('SELECT n IN (1.00, 2.5) AS r FROM d'), [true, true]);
      expect(await truth('SELECT n NOT IN (1.00) AS r FROM d'), [false, true]);
    });

    test('a large IN list stays correct', () async {
      final list = List.generate(1000, (i) => i + 100).join(', ');
      // Only 1 and 3 exist in t, neither is >= 100.
      expect(await truth('SELECT n IN ($list) AS r FROM t'), [false, false]);
      expect(await truth('SELECT n IN (1, $list) AS r FROM t'), [true, false]);
    });

    test('an empty IN list is a parse error', () async {
      await expectLater(
        engine.execute('SELECT n FROM t WHERE n IN ()'),
        throwsA(isA<FormatException>()),
      );
    });

    test('IN (SELECT ...) is a semi-join', () async {
      // u = {1, null}; t's 1 matches, 3 does not. A null in the build matches
      // nothing (so it neither adds nor removes a semi match).
      final r = await engine.execute(
        'SELECT n FROM t WHERE n IN (SELECT n FROM u)',
      );
      expect(r.toRows(), [
        {'n': 1},
      ]);
    });

    test(
      'NOT IN (SELECT ...) with a NULL in the result yields nothing',
      () async {
        // u contains a null → `x NOT IN (S)` is UNKNOWN for EVERY row.
        final r = await engine.execute(
          'SELECT n FROM t WHERE n NOT IN (SELECT n FROM u)',
        );
        expect(r.rowCount, 0);
      },
    );

    test(
      'NOT IN (SELECT ...) over a null-free result is a plain anti-join',
      () async {
        // The subquery filters the null away, leaving {1}; t's 3 is not in it.
        final r = await engine.execute(
          'SELECT n FROM t WHERE n NOT IN (SELECT n FROM u WHERE n = 1)',
        );
        expect(r.toRows(), [
          {'n': 3},
        ]);
      },
    );

    test('IN (SELECT ...) alongside another conjunct', () async {
      final r = await engine.execute(
        'SELECT n FROM t WHERE n IN (SELECT n FROM u) AND n < 2',
      );
      expect(r.toRows(), [
        {'n': 1},
      ]);
    });

    test('a correlated IN subquery decorrelates', () async {
      // t = {1,3}, u = {1, null}. Per t-row: u where u.n = t.n. For 1 → {1} so
      // 1 IN {1}; for 3 → {} so 3 not in. (Correlated coverage lives in its own
      // group; this pins the fixture behaviour through the semi-join path.)
      final r = await engine.execute(
        'SELECT n FROM t WHERE n IN (SELECT n FROM u WHERE u.n = t.n)',
      );
      expect(r.toRows(), [
        {'n': 1},
      ]);
    });

    test('a multi-column IN subquery fails loud', () async {
      await expectLater(
        engine.execute('SELECT n FROM t WHERE n IN (SELECT n, n FROM u)'),
        throwsA(isA<ArgumentError>()),
      );
    });
  });

  group('correlated IN subqueries', () {
    late SqlEngine engine;

    setUp(() {
      engine = SqlEngine();
      // orders (cust, region); customers (cust, region, tier).
      engine.registerTable(
        'o',
        Table.fromRows('o', [
          {'cust': 1, 'region': 'E'},
          {'cust': 2, 'region': 'W'},
          {'cust': 3, 'region': 'E'},
          {'cust': 1, 'region': 'W'},
        ]),
      );
      engine.registerTable(
        'c',
        Table.fromRows('c', [
          {'cust': 1, 'region': 'E', 'tier': 'gold'},
          {'cust': 1, 'region': 'W', 'tier': 'silver'},
          {'cust': 3, 'region': 'E', 'tier': 'gold'},
        ]),
      );
    });

    tearDown(() => engine.dispose());

    test('decorrelates to a multi-key semi-join', () async {
      // A cust that is a gold customer IN THE SAME REGION. The same cust in a
      // different region (1/W is silver) must NOT match on cust alone.
      final r = await engine.execute(
        "SELECT cust, region FROM o WHERE cust IN "
        "(SELECT cust FROM c WHERE c.region = o.region AND c.tier = 'gold') "
        "ORDER BY cust, region",
      );
      expect(r.toRows(), [
        {'cust': 1, 'region': 'E'},
        {'cust': 3, 'region': 'E'},
      ]);
    });

    test('equals the explicit JOIN + DISTINCT it decorrelates to', () async {
      final correlated = await engine.execute(
        "SELECT cust, region FROM o WHERE cust IN "
        "(SELECT cust FROM c WHERE c.region = o.region AND c.tier = 'gold') "
        "ORDER BY cust, region",
      );
      final join = await engine.execute(
        "SELECT DISTINCT o.cust, o.region FROM o "
        "JOIN c ON o.cust = c.cust AND o.region = c.region "
        "WHERE c.tier = 'gold' ORDER BY o.cust, o.region",
      );
      expect(correlated.toRows(), join.toRows());
    });

    test('a correlated NOT IN fails loud (per-row null semantics)', () async {
      await expectLater(
        engine.execute(
          'SELECT cust FROM o WHERE cust NOT IN '
          '(SELECT cust FROM c WHERE c.region = o.region)',
        ),
        throwsA(isA<UnsupportedError>()),
      );
    });

    test('a non-equi correlation fails loud', () async {
      await expectLater(
        engine.execute(
          'SELECT cust FROM o WHERE cust IN '
          '(SELECT cust FROM c WHERE c.cust > o.cust)',
        ),
        throwsA(isA<UnsupportedError>()),
      );
    });

    test('a correlated aggregate subquery fails loud', () async {
      await expectLater(
        engine.execute(
          'SELECT cust FROM o WHERE cust IN '
          '(SELECT MAX(cust) FROM c WHERE c.region = o.region)',
        ),
        throwsA(isA<UnsupportedError>()),
      );
    });
  });

  group('EXISTS / NOT EXISTS', () {
    late SqlEngine engine;

    setUp(() {
      engine = SqlEngine();
      engine.registerTable(
        'o',
        Table.fromRows('o', [
          {'cust': 1, 'region': 'E'},
          {'cust': 2, 'region': 'W'},
          {'cust': 3, 'region': 'E'},
        ]),
      );
      engine.registerTable(
        'c',
        Table.fromRows('c', [
          {'cust': 1, 'region': 'E', 'tier': 'gold'},
          {'cust': 3, 'region': 'E', 'tier': 'silver'},
        ]),
      );
    });

    tearDown(() => engine.dispose());

    test('correlated EXISTS is a semi-join', () async {
      final r = await engine.execute(
        'SELECT cust FROM o WHERE EXISTS '
        '(SELECT 1 FROM c WHERE c.cust = o.cust) ORDER BY cust',
      );
      expect(r.toRows(), [
        {'cust': 1},
        {'cust': 3},
      ]);
    });

    test('correlated NOT EXISTS is a plain anti-join', () async {
      final r = await engine.execute(
        'SELECT cust FROM o WHERE NOT EXISTS '
        '(SELECT 1 FROM c WHERE c.cust = o.cust)',
      );
      expect(r.toRows(), [
        {'cust': 2},
      ]);
    });

    test('EXISTS with a local filter and two correlation keys', () async {
      final r = await engine.execute(
        "SELECT cust FROM o WHERE EXISTS "
        "(SELECT 1 FROM c WHERE c.cust = o.cust AND c.region = o.region "
        "AND c.tier = 'gold')",
      );
      expect(r.toRows(), [
        {'cust': 1},
      ]);
    });

    test('an uncorrelated EXISTS fails loud', () async {
      await expectLater(
        engine.execute('SELECT cust FROM o WHERE EXISTS (SELECT 1 FROM c)'),
        throwsA(isA<UnsupportedError>()),
      );
    });
  });

  // Regressions from a code review of the subquery / ORDER-BY work.
  group('review fixes', () {
    late SqlEngine engine;
    setUp(() {
      engine = SqlEngine();
      engine.registerTable(
        't',
        Table.fromRows('t', [
          {'n': 1},
          {'n': 2},
          {'n': 3},
          {'n': 4},
        ]),
      );
      engine.registerTable(
        'u',
        Table.fromRows('u', [
          {'n': 1},
          {'n': 2},
          {'n': 2},
          {'n': 4},
        ]),
      );
      engine.registerTable(
        'g',
        Table.fromRows('g', [
          {'n': 1, 'grp': 'a'},
          {'n': 2, 'grp': 'a'},
          {'n': 3, 'grp': 'b'},
          {'n': 4, 'grp': 'b'},
        ]),
      );
      engine.registerTable(
        'a',
        Table.fromRows('a', [
          {'n': 1},
        ]),
      );
      engine.registerTable(
        'b',
        Table.fromRows('b', [
          {'n': 2},
        ]),
      );
    });
    tearDown(() => engine.dispose());

    // F1: an uncorrelated IN subquery must preserve GROUP BY / LIMIT / set ops
    // (the decorrelation rebuild used to drop them, a silent wrong answer).
    test('F1: IN subquery preserves GROUP BY', () async {
      // per group max: a→2, b→4. t IN {2,4}.
      final r = await engine.execute(
        'SELECT n FROM t WHERE n IN (SELECT MAX(n) FROM g GROUP BY grp) '
        'ORDER BY n',
      );
      expect(r.toRows(), [
        {'n': 2},
        {'n': 4},
      ]);
    });

    test('F1: IN subquery preserves LIMIT', () async {
      // u LIMIT 1 = {1}; only t.n = 1 matches (not all four rows).
      final r = await engine.execute(
        'SELECT n FROM t WHERE n IN (SELECT n FROM u LIMIT 1)',
      );
      expect(r.toRows(), [
        {'n': 1},
      ]);
    });

    test('F1: IN subquery preserves a set operation', () async {
      final r = await engine.execute(
        'SELECT n FROM t WHERE n IN (SELECT n FROM a UNION SELECT n FROM b) '
        'ORDER BY n',
      );
      expect(r.toRows(), [
        {'n': 1},
        {'n': 2},
      ]);
    });

    // F2: a qualifier the schema doesn't carry is a typo, not a bare match.
    test('F2: an unknown table qualifier fails loud', () async {
      await expectLater(
        engine.execute('SELECT typo.n FROM t'),
        throwsA(isA<FormatException>()),
      );
      await expectLater(
        engine.execute('SELECT n FROM t WHERE typo.n = 1'),
        throwsA(isA<FormatException>()),
      );
    });

    // F6: the IN column being the correlation column must not duplicate a field.
    test(
      'F6: correlated IN where the IN column is the correlation column',
      () async {
        engine.registerTable(
          'o',
          Table.fromRows('o', [
            {'cust': 1},
            {'cust': 2},
            {'cust': 3},
          ]),
        );
        engine.registerTable(
          'c',
          Table.fromRows('c', [
            {'cust': 1},
            {'cust': 2},
          ]),
        );
        final r = await engine.execute(
          'SELECT cust FROM o WHERE cust IN '
          '(SELECT c.cust FROM c WHERE c.cust = o.cust) ORDER BY cust',
        );
        expect(r.toRows(), [
          {'cust': 1},
          {'cust': 2},
        ]);
      },
    );

    // F7: an aggregate only in ORDER BY / HAVING makes it a global aggregate.
    test('F7: an aggregate only in ORDER BY is a global aggregate', () async {
      final r = await engine.execute('SELECT 1 AS x FROM t ORDER BY SUM(n)');
      expect(r.toRows(), [
        {'x': 1},
      ]);
    });

    test('F7: an aggregate only in HAVING is a global aggregate', () async {
      expect(
        (await engine.execute(
          'SELECT 1 AS x FROM t HAVING SUM(n) > 5',
        )).rowCount,
        1,
      );
      expect(
        (await engine.execute(
          'SELECT 1 AS x FROM t HAVING SUM(n) > 500',
        )).rowCount,
        0,
      );
    });

    test('F7: SELECT * with a global aggregate fails loud', () async {
      await expectLater(
        engine.execute('SELECT * FROM t ORDER BY SUM(n)'),
        throwsA(isA<UnsupportedError>()),
      );
    });
  });

  // The grammar folds `a - b - c` from a separated list of operands and
  // separators. Every operator here is non-commutative or precedence-sensitive,
  // so a fold that groups right instead of left changes the ANSWER rather than
  // just the tree — 10 - 3 - 2 would be 9, not 5. Values verified against the
  // engine before any of this was touched.
  group('arithmetic associativity and precedence', () {
    late SqlEngine engine;
    setUp(() {
      engine = SqlEngine();
      engine.registerTable(
        't',
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'n', values: [1], nullable: false),
          ],
          schema: Schema(
            fields: [
              Field(name: 'n', dataType: DataType.int64, nullable: false),
            ],
          ),
        ),
      );
    });
    tearDown(() => engine.dispose());

    Future<Object?> value(String expression) async => (await engine.execute(
      'SELECT $expression AS v FROM t',
    )).column('v')!.toList().first;

    test('chained subtraction groups left', () async {
      expect(await value('10 - 3 - 2'), 5); // right-assoc would give 9
    });

    test('chained division groups left', () async {
      expect(await value('100 / 5 / 2'), 10.0); // right-assoc would give 40.0
    });

    test('chained modulo groups left', () async {
      expect(await value('20 % 7 % 2'), 0); // right-assoc would give 20 % 1 = 0
    });

    test('a four-operand subtraction chain groups left', () async {
      expect(await value('100 - 10 - 5 - 2'), 83); // right-assoc gives 93
    });

    test('mixed + and - fold in source order', () async {
      expect(await value('1 + 2 - 3 + 4'), 4);
    });

    test('* binds tighter than -', () async {
      expect(await value('10 - 2 * 3'), 4); // left-to-right would give 24
    });

    test('mixed precedence across all four operators', () async {
      // 1 + (2*3) - (4/2) = 5
      expect(await value('1 + 2 * 3 - 4 / 2'), 5.0);
    });

    test('chained multiplication is stable', () async {
      expect(await value('2 * 3 * 4'), 24);
    });

    test('parentheses override the fold', () async {
      expect(await value('10 - (3 - 2)'), 9);
      expect(await value('(10 - 3) - 2'), 5);
    });

    test('a lone operand is unwrapped', () async {
      expect(await value('7'), 7);
    });
  });

  // An aggregate may sit anywhere in a SELECT item or in HAVING; the planner
  // hoists each one into the aggregate step and rewrites the expression around
  // it to read the column that step emits. Previously any aggregate not written
  // as a whole SELECT item was rejected (`SUM(x)+1`) or silently misresolved
  // (HAVING).
  group('aggregate expressions', () {
    // k=a: x=[1,1,3] -> COUNT=3, COUNT(DISTINCT)=2, SUM=5, SUM(x+1)=8, SUM(x*100)=500
    // k=b: x=[10]    -> COUNT=1, COUNT(DISTINCT)=1, SUM=10, SUM(x+1)=11, SUM(x*100)=1000
    Table rows() => Table.fromRows('t', [
      {'k': 'a', 'x': 1},
      {'k': 'a', 'x': 1},
      {'k': 'a', 'x': 3},
      {'k': 'b', 'x': 10},
    ]);

    SqlEngine engine() {
      final e = SqlEngine();
      e.registerTable('t', rows());
      return e;
    }

    Future<List<Map<String, dynamic>>> run(String sql) async =>
        (await engine().execute(sql)).toRows();

    Future<dynamic> value(String select) async =>
        (await run('SELECT $select FROM t')).single.values.first;

    group('nesting shapes', () {
      test('arithmetic around an aggregate', () async {
        expect(await value('SUM(x) + 1 AS r'), Decimal(BigInt.from(16), 0));
      });

      test('two different aggregates in one expression', () async {
        // SUM(x)=15, COUNT(*)=4 -> 3.75
        expect(await value('SUM(x) / COUNT(*) AS r'), 3.75);
      });

      test('an aggregate inside a scalar function', () async {
        expect(await value('ROUND(AVG(x), 2) AS r'), 3.75);
      });

      test('an aggregate inside CASE', () async {
        expect(await value('CASE WHEN SUM(x) > 5 THEN 1 ELSE 0 END AS r'), 1);
      });

      test('an aggregate under unary minus', () async {
        expect(await value('-SUM(x) AS r'), Decimal(BigInt.from(-15), 0));
      });

      test('an aggregate inside CAST', () async {
        expect(await value('CAST(SUM(x) AS DOUBLE) AS r'), 15.0);
      });

      test('an aggregate under NOT', () async {
        expect(await value('NOT (SUM(x) > 5) AS r'), false);
      });

      test('an aggregate inside IN', () async {
        // ast.InExpression is a node _containsAggregate did not walk, so this
        // slipped past the old reject and planned SUM as a scalar function
        // ("Unknown SQL function: SUM").
        expect(await value('SUM(x) IN (14, 15) AS r'), true);
      });

      test('an expression over a grouped aggregate', () async {
        expect(
          await run('SELECT k, SUM(x) * 2 AS d FROM t GROUP BY k ORDER BY k'),
          [
            {'k': 'a', 'd': Decimal(BigInt.from(10), 0)},
            {'k': 'b', 'd': Decimal(BigInt.from(20), 0)},
          ],
        );
      });

      test('a group key and an aggregate in one expression', () async {
        expect(
          await run(
            'SELECT x % 2 AS m, SUM(x) + 1 AS s FROM t '
            'GROUP BY x % 2 ORDER BY m',
          ),
          [
            {'m': 0, 's': Decimal(BigInt.from(11), 0)},
            {'m': 1, 's': Decimal(BigInt.from(6), 0)},
          ],
        );
      });

      test('a bare aggregate and a nested one over the same input', () async {
        expect(await run('SELECT SUM(x) AS s, SUM(x) + 1 AS s1 FROM t'), [
          {'s': Decimal(BigInt.from(15), 0), 's1': Decimal(BigInt.from(16), 0)},
        ]);
      });

      test('a nested DISTINCT aggregate', () async {
        expect(
          await run(
            'SELECT k, COUNT(DISTINCT x) + 1 AS c FROM t '
            'GROUP BY k ORDER BY k',
          ),
          [
            {'k': 'a', 'c': 3},
            {'k': 'b', 'c': 2},
          ],
        );
      });
    });

    // The hoisted plan is the same shape the planner's old error message told
    // users to hand-write, so the subquery form is the semantic reference.
    group('matches the equivalent subquery', () {
      Future<void> same(String inline, String sub) async {
        expect(await run(inline), await run(sub));
      }

      test('SUM(x) + 1', () async {
        await same(
          'SELECT SUM(x) + 1 AS r FROM t',
          'SELECT s + 1 AS r FROM (SELECT SUM(x) AS s FROM t) sub',
        );
      });

      test('ROUND(AVG(x), 2)', () async {
        await same(
          'SELECT ROUND(AVG(x), 2) AS r FROM t',
          'SELECT ROUND(a, 2) AS r FROM (SELECT AVG(x) AS a FROM t) sub',
        );
      });

      test('grouped SUM(x) * 2', () async {
        await same(
          'SELECT k, SUM(x) * 2 AS r FROM t GROUP BY k ORDER BY k',
          'SELECT k, s * 2 AS r FROM (SELECT k, SUM(x) AS s FROM t GROUP BY k) '
              'sub ORDER BY k',
        );
      });
    });

    group('HAVING', () {
      test('an aggregate absent from the SELECT list resolves', () async {
        // Was "Unknown column: x" — HAVING's aggregate was never collected.
        expect(await run('SELECT k FROM t GROUP BY k HAVING SUM(x) > 5'), [
          {'k': 'b'},
        ]);
      });

      test('a nested aggregate resolves', () async {
        expect(await run('SELECT k FROM t GROUP BY k HAVING SUM(x) + 1 > 6'), [
          {'k': 'b'},
        ]);
      });

      test('aggregates over different expressions do not collide', () async {
        // The old string key rendered EVERY non-column argument as 'expr', so
        // SUM(x+1) and SUM(x*100) shared the key 'SUM(expr)' and the last one
        // registered won: this returned k=b instead of no rows.
        expect(
          await run(
            'SELECT k, SUM(x + 1) AS a, SUM(x * 100) AS b FROM t '
            'GROUP BY k HAVING SUM(x + 1) > 500',
          ),
          isEmpty,
        );
      });

      test('a DISTINCT aggregate does not collide with a plain one', () async {
        // The old string key ignored DISTINCT, so COUNT(x) resolved to the
        // selected COUNT(DISTINCT x): this returned no rows instead of k=a.
        expect(
          await run(
            'SELECT k, COUNT(DISTINCT x) AS c FROM t '
            'GROUP BY k HAVING COUNT(x) > 2',
          ),
          [
            {'k': 'a', 'c': 2},
          ],
        );
      });

      test('an aggregate shared with the SELECT list still resolves', () async {
        expect(
          await run(
            'SELECT k, SUM(x) AS s FROM t GROUP BY k HAVING SUM(x) > 5',
          ),
          [
            {'k': 'b', 's': Decimal(BigInt.from(10), 0)},
          ],
        );
      });

      test('an aggregate under SELECT * does not leak its column', () async {
        // `SELECT *` under GROUP BY means the group keys, and skips the
        // projection that otherwise hides the hoisted `__agg$i` columns — so
        // the aggregate HAVING needs must be trimmed back off explicitly.
        final t = await engine().execute(
          'SELECT * FROM t GROUP BY k HAVING SUM(x) > 5',
        );
        expect(t.schema.fieldNames, ['k']);
        expect(t.toRows(), [
          {'k': 'b'},
        ]);
      });
    });

    group('unchanged behaviour', () {
      test('a bare aggregate is still named for its function', () async {
        expect(
          (await engine().execute('SELECT SUM(x) FROM t')).schema.fieldNames,
          ['SUM'],
        );
      });

      test('an explicit alias still wins', () async {
        expect(
          (await engine().execute(
            'SELECT SUM(x) AS total FROM t',
          )).schema.fieldNames,
          ['total'],
        );
      });

      test('ORDER BY an aggregate alias still works', () async {
        expect(
          await run('SELECT k, SUM(x) AS s FROM t GROUP BY k ORDER BY s DESC'),
          [
            {'k': 'b', 's': Decimal(BigInt.from(10), 0)},
            {'k': 'a', 's': Decimal(BigInt.from(5), 0)},
          ],
        );
      });

      test('ORDER BY an aggregate written out in full', () async {
        expect(
          await run('SELECT k, SUM(x) AS s FROM t GROUP BY k ORDER BY SUM(x)'),
          [
            {'k': 'a', 's': Decimal(BigInt.from(5), 0)},
            {'k': 'b', 's': Decimal(BigInt.from(10), 0)},
          ],
        );
      });

      test('ORDER BY an aggregate that is not selected', () async {
        // The aggregate is computed and sorted on, then trimmed back out: it
        // must not leak its internal `__agg$i` column into the output.
        final t = await engine().execute(
          'SELECT k FROM t GROUP BY k ORDER BY SUM(x) DESC',
        );
        expect(t.schema.fieldNames, ['k']);
        expect(t.toRows(), [
          {'k': 'b'},
          {'k': 'a'},
        ]);
      });

      test('ORDER BY an aggregate under SELECT * does not leak it', () async {
        final t = await engine().execute(
          'SELECT * FROM t GROUP BY k ORDER BY SUM(x) DESC',
        );
        expect(t.schema.fieldNames, ['k']);
        expect(t.toRows(), [
          {'k': 'b'},
          {'k': 'a'},
        ]);
      });

      test('ORDER BY an aggregate expression', () async {
        expect(await run('SELECT k FROM t GROUP BY k ORDER BY SUM(x) * -1'), [
          {'k': 'b'},
          {'k': 'a'},
        ]);
      });

      test('ORDER BY COUNT(*) and a group key together', () async {
        expect(await run('SELECT k FROM t GROUP BY k ORDER BY COUNT(*), k'), [
          {'k': 'b'},
          {'k': 'a'},
        ]);
      });

      test('ORDER BY an aggregate with DISTINCT fails loud', () async {
        // The carry is unavailable under DISTINCT, so this stays loud rather
        // than leaking an opaque "Unknown column: __agg$0".
        await expectLater(
          engine().execute(
            'SELECT DISTINCT k FROM t GROUP BY k ORDER BY SUM(x)',
          ),
          throwsA(
            isA<UnsupportedError>().having(
              (e) => e.message,
              'message',
              contains('ORDER BY s'),
            ),
          ),
        );
      });
    });
  });

  group('nested columns under memory pressure (external review)', () {
    // The spill schema was rebuilt from bare columns (name+dtype+nullable),
    // losing nested children — the Parquet spill writer then crashed on
    // `children.first`, but only once a query crossed the spill threshold.
    // These force spilling with a tiny budget.
    Table nested(String name, String prefix, int n) => Table(
      name: name,
      columns: [
        NumericColumn.int64(
          name: '${prefix}k',
          values: [for (int i = n; i > 0; i--) i],
          nullable: false,
        ),
        ListColumn(
          name: '${prefix}payload',
          dataType: DataType.list,
          data: [
            for (int i = 0; i < n; i++) [i, i + 1],
          ],
          nullable: true,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: '${prefix}k', dataType: DataType.int64, nullable: false),
          Field(
            name: '${prefix}payload',
            dataType: DataType.list,
            nullable: true,
            children: [Field(name: 'element', dataType: DataType.int64)],
          ),
        ],
      ),
    );

    late SqlEngine engine;
    setUp(() {
      engine = SqlEngine(spillThresholdBytes: 256);
      engine.registerTable('a', nested('a', 'a', 400));
      engine.registerTable('b', nested('b', 'b', 400));
    });
    tearDown(() => engine.dispose());

    test('ORDER BY spills a list payload and round-trips it', () async {
      final r = await engine.execute('SELECT * FROM a ORDER BY ak');
      expect(r.rowCount, 400);
      final first = r.toRows().first;
      expect(first['ak'], 1);
      expect(first['apayload'], [399, 400]);
    });

    test('UNION dedup spills a list payload and round-trips it', () async {
      final r = await engine.execute(
        'SELECT apayload FROM a UNION SELECT bpayload FROM b',
      );
      expect(r.rowCount, 400); // identical payloads dedup across branches
      expect(r.toRows().first['apayload'], isA<List<dynamic>>());
    });

    test('a grace join carries a build-side list payload', () async {
      final r = await engine.execute(
        'SELECT ak, bpayload FROM a JOIN b ON a.ak = b.bk',
      );
      expect(r.rowCount, 400);
      expect(
        r.toRows().every((row) => row['bpayload'] is List),
        isTrue,
        reason: 'payload survives the spilled build side',
      );
    });
  });

  group('comma-join equi-extraction (audit 7)', () {
    // `FROM a, b WHERE a.id = b.id` used to execute as a full cartesian
    // product with a filter on top (O(n·m) rows materialized); the equality
    // conjuncts now become the fold step's hash-join condition. Anything the
    // extractor is unsure about stays in the WHERE, so results never change.
    Table ints(String name, String c, List<int> v) => Table(
      name: name,
      columns: [NumericColumn.int64(name: c, values: v, nullable: false)],
      schema: Schema(
        fields: [Field(name: c, dataType: DataType.int64, nullable: false)],
      ),
    );

    late SqlEngine engine;
    setUp(() {
      engine = SqlEngine();
      engine.registerTable('a', ints('a', 'id', [1, 2, 3, 4]));
      engine.registerTable('b', ints('b', 'bid', [2, 3, 5]));
      engine.registerTable('c', ints('c', 'cid', [3, 9]));
    });
    tearDown(() => engine.dispose());

    test('comma join matches the explicit JOIN result', () async {
      final comma = await engine.execute(
        'SELECT a.id FROM a, b WHERE a.id = b.bid',
      );
      final join = await engine.execute(
        'SELECT a.id FROM a JOIN b ON a.id = b.bid',
      );
      expect(
        comma.column('id')!.toList()..sort(),
        join.column('id')!.toList()..sort(),
      );
      expect(comma.column('id')!.toList().toSet(), {2, 3});
    });

    test('residual conjuncts still filter', () async {
      final r = await engine.execute(
        'SELECT a.id FROM a, b WHERE a.id = b.bid AND a.id > 2',
      );
      expect(r.column('id')!.toList(), [3]);
    });

    test('a three-table comma chain joins each step', () async {
      final r = await engine.execute(
        'SELECT a.id FROM a, b, c WHERE a.id = b.bid AND b.bid = c.cid',
      );
      expect(r.column('id')!.toList(), [3]);
    });

    test('unqualified unambiguous columns extract too', () async {
      final r = await engine.execute('SELECT id FROM a, b WHERE id = bid');
      expect(r.column('id')!.toList().toSet(), {2, 3});
    });

    test('a large comma join completes at hash-join speed', () async {
      const n = 20000;
      engine.registerTable(
        'big1',
        ints('big1', 'x', [for (int i = 0; i < n; i++) i]),
      );
      engine.registerTable(
        'big2',
        ints('big2', 'y', [for (int i = 0; i < n; i++) i]),
      );
      // As a cartesian product this is 400M pairs — it would time out (or trip
      // caps) long before finishing; as a hash join it is instant.
      final sw = Stopwatch()..start();
      final r = await engine.execute(
        'SELECT x FROM big1, big2 WHERE x = y AND x < 3',
      );
      sw.stop();
      expect(r.column('x')!.toList()..sort(), [0, 1, 2]);
      expect(
        sw.elapsedMilliseconds,
        lessThan(5000),
        reason: 'hash join, not a 400M-row cartesian product',
      );
    });

    test('no extractable equality keeps cross-join semantics', () async {
      final r = await engine.execute(
        'SELECT a.id FROM a, b WHERE a.id < b.bid',
      );
      // Plain cross + filter: pairs where id < bid.
      expect(r.rowCount, 7); // (1,2)(1,3)(1,5)(2,3)(2,5)(3,5)(4,5)
    });
  });

  group('decimal per-row evaluation (audit 7)', () {
    // The per-row `evaluate` path (CASE branches, COALESCE, scalar functions)
    // only scale-aligned decimals for COMPARISONS; arithmetic fell through to
    // raw operators — unaligned unscaled adds (wrong magnitude), `BigInt + int`
    // TypeErrors, and `BigInt as num` crashes in math functions. It now
    // delegates to Kernels.arithScalar, the vectorized kernel's per-pair twin.
    Table decimals(Map<String, (List<int>, int)> columns) => Table(
      name: 't',
      columns: [
        for (final e in columns.entries)
          ListColumn<BigInt>(
            name: e.key,
            dataType: DataType.decimal,
            data: [for (final u in e.value.$1) BigInt.from(u)],
            nullable: false,
          ),
      ],
      schema: Schema(
        fields: [
          for (final e in columns.entries)
            Field(
              name: e.key,
              dataType: DataType.decimal,
              nullable: false,
              precision: 10,
              scale: e.value.$2,
            ),
        ],
      ),
    );

    late SqlEngine engine;
    setUp(() {
      engine = SqlEngine();
      // a = 1.50 (150@2), b = 0.0025 (25@4), price = 1.50 (150@2)
      engine.registerTable(
        't',
        decimals({
          'a': ([150], 2),
          'b': ([25], 4),
          'price': ([150], 2),
        }),
      );
    });
    tearDown(() => engine.dispose());

    test(
      'cross-scale addition inside CASE matches the vectorized path',
      () async {
        final vectorized = await engine.execute('SELECT a + b AS v FROM t');
        final perRow = await engine.execute(
          'SELECT CASE WHEN price > 0 THEN a + b END AS v FROM t',
        );
        // 1.50 + 0.0025 = 1.5025 (unscaled 15025 at scale 4) — NOT 0.0175, the
        // unaligned 150 + 25 the raw fall-through produced.
        expect(vectorized.column('v')!.toList().single, BigInt.from(15025));
        expect(vectorized.schema.fields.single.scale, 4);
        expect(
          perRow.column('v')!.toList().single,
          vectorized.column('v')!.toList().single,
        );
        expect(perRow.schema.fields.single.scale, 4);
      },
    );

    test('decimal + int inside COALESCE evaluates (was a TypeError)', () async {
      final r = await engine.execute(
        'SELECT COALESCE(price * 2, 0) AS v FROM t',
      );
      expect(r.column('v')!.toList().single, BigInt.from(300)); // 3.00
      expect(r.schema.fields.single.scale, 2);
    });

    test('decimal + int inside CASE evaluates (was a TypeError)', () async {
      final r = await engine.execute(
        'SELECT CASE WHEN price > 0 THEN price + 1 END AS v FROM t',
      );
      expect(r.column('v')!.toList().single, BigInt.from(250)); // 2.50
    });

    test(
      'math functions accept a decimal argument (was a TypeError)',
      () async {
        final abs = await engine.execute('SELECT ABS(a) AS v FROM t');
        expect(abs.column('v')!.toList().single, 1.5);
        final round = await engine.execute('SELECT ROUND(price) AS v FROM t');
        expect(round.column('v')!.toList().single, 2.0);
        final floor = await engine.execute('SELECT FLOOR(price) AS v FROM t');
        expect(floor.column('v')!.toList().single, 1.0);
      },
    );

    test('ABS over SUM(int) evaluates (SUM surfaces decimal)', () async {
      final ints = Table(
        name: 'n',
        columns: [
          NumericColumn.int64(name: 'x', values: [-3, -4], nullable: false),
        ],
        schema: Schema(
          fields: [Field(name: 'x', dataType: DataType.int64, nullable: false)],
        ),
      );
      engine.registerTable('n', ints);
      final r = await engine.execute('SELECT ABS(SUM(x)) AS v FROM n');
      expect(r.column('v')!.toList().single, 7.0);
    });

    test('MOD over decimals stays exact decimal at the wider scale', () async {
      // 1.50 % 0.0025 = 0 exactly (unscaled: 15000 % 25 at scale 4).
      final r = await engine.execute('SELECT MOD(a, b) AS v FROM t');
      expect(r.schema.fields.single.dataType, DataType.decimal);
      expect(r.column('v')!.toList().single, BigInt.zero);
    });
  });
}
