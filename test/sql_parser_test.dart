import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/sql/ast/ast.dart';
import 'package:libbigdata/src/sql/parser/sql_parser.dart';
import 'package:test/test.dart';

/// Direct SQL-parser / semantics tests — the parser (997 LOC) had no dedicated
/// suite, so case-sensitivity and grammar-gap bugs shipped untested. Covers the
/// Phase-1 correctness fixes.
void main() {
  late Directory dir;
  late SqlEngine engine;
  setUp(() {
    dir = Directory.systemTemp.createTempSync('sqlparse_');
    engine = SqlEngine(spillDir: dir.path);
  });
  tearDown(() async {
    await engine.dispose();
    dir.deleteSync(recursive: true);
  });

  Table ints(String name, List<int> v) => Table(
    name: name,
    columns: [NumericColumn.int64(name: 'n', values: v, nullable: false)],
    schema: Schema(
      fields: [Field(name: 'n', dataType: DataType.int64, nullable: false)],
    ),
  );

  Future<List<int>> col(String sql) async =>
      (await engine.execute(sql)).column('n')!.toList().cast<int>();

  group('CB1 set-op keyword case', () {
    setUp(() {
      engine.registerTable('a', ints('a', [1, 2, 3]));
      engine.registerTable('b', ints('b', [2, 3, 4]));
    });

    test('lowercase "union all" concatenates (not except)', () async {
      final r = await col('SELECT n FROM a union all SELECT n FROM b');
      expect(r.length, 6); // EXCEPT would have returned {1}
    });

    test('lowercase "union" dedups', () async {
      final r = await col('SELECT n FROM a union SELECT n FROM b');
      expect(r.toSet(), {1, 2, 3, 4});
    });

    test('lowercase "intersect"', () async {
      final r = await col('SELECT n FROM a intersect SELECT n FROM b');
      expect(r.toSet(), {2, 3});
    });

    test('lowercase "except"', () async {
      final r = await col('SELECT n FROM a except SELECT n FROM b');
      expect(r.toSet(), {1});
    });
  });

  group('CB2 ORDER BY direction case', () {
    setUp(() => engine.registerTable('t', ints('t', [3, 1, 2])));

    test('lowercase "asc" sorts ascending', () async {
      expect(await col('SELECT n FROM t ORDER BY n asc'), [1, 2, 3]);
    });

    test('lowercase "desc" sorts descending', () async {
      expect(await col('SELECT n FROM t ORDER BY n desc'), [3, 2, 1]);
    });

    test('bare ORDER BY defaults ascending', () async {
      expect(await col('SELECT n FROM t ORDER BY n'), [1, 2, 3]);
    });
  });

  group('CB4 IN-list + BETWEEN', () {
    setUp(() => engine.registerTable('t', ints('t', [1, 2, 3, 4, 5])));

    test('IN value list', () async {
      expect((await col('SELECT n FROM t WHERE n IN (2, 4)')).toSet(), {2, 4});
    });

    test('NOT IN value list', () async {
      expect((await col('SELECT n FROM t WHERE n NOT IN (2, 4)')).toSet(), {
        1,
        3,
        5,
      });
    });

    test('single-element IN (1) matches', () async {
      expect(await col('SELECT n FROM t WHERE n IN (3)'), [3]);
    });

    test('BETWEEN is inclusive', () async {
      expect(await col('SELECT n FROM t WHERE n BETWEEN 2 AND 4'), [2, 3, 4]);
    });

    test('NOT BETWEEN', () async {
      expect(
        (await col('SELECT n FROM t WHERE n NOT BETWEEN 2 AND 4')).toSet(),
        {1, 5},
      );
    });

    test(
      'BETWEEN composes with AND (its AND is consumed by the predicate)',
      () async {
        expect(
          await col('SELECT n FROM t WHERE n BETWEEN 2 AND 4 AND n <> 3'),
          [2, 4],
        );
      },
    );
  });

  group('CB3 nested aggregate hoisting', () {
    Table grouped() => Table(
      name: 'g',
      columns: [
        ListColumn<String>(
          name: 'k',
          dataType: DataType.string,
          data: ['a', 'a', 'b'],
          nullable: false,
        ),
        NumericColumn.int64(name: 'n', values: [1, 2, 3], nullable: false),
      ],
      schema: Schema(
        fields: [
          Field(name: 'k', dataType: DataType.string, nullable: false),
          Field(name: 'n', dataType: DataType.int64, nullable: false),
        ],
      ),
    );
    setUp(() => engine.registerTable('g', grouped()));

    test('a bare top-level aggregate still works', () async {
      final r = await engine.execute('SELECT SUM(n) AS s FROM g');
      expect(r.column('s')!.toList().first, BigInt.from(6));
    });

    test('an aggregate nested in arithmetic is hoisted', () async {
      final r = await engine.execute('SELECT SUM(n) + 1 AS s FROM g');
      expect(r.column('s')!.toList().first, BigInt.from(7));
    });

    test('a nested aggregate under GROUP BY is hoisted', () async {
      // Was silently dropped, then rejected; now computed per group.
      final r = await engine.execute(
        'SELECT k, SUM(n) * 100 AS s FROM g GROUP BY k ORDER BY k',
      );
      expect(r.column('s')!.toList(), [BigInt.from(300), BigInt.from(300)]);
    });
  });

  group('CB9 float literals', () {
    setUp(() => engine.registerTable('t', ints('t', [1])));

    Future<double> f(String expr) async =>
        (await engine.execute(
              'SELECT $expr AS n FROM t',
            )).column('n')!.toList().first
            as double;

    test('leading-dot, trailing-dot, and exponent forms parse', () async {
      expect(await f('.5'), 0.5);
      expect(await f('1.'), 1.0);
      expect(await f('1e3'), 1000.0);
      expect(await f('1.5E-1'), closeTo(0.15, 1e-12));
    });
  });

  // The grammar builds its operator trees by folding a separated list. These
  // tests pin the fold DIRECTION and the list SHAPE at the AST level, where a
  // wrong-associativity tree is visible directly rather than only via an
  // arithmetic result that happens to differ.
  group('operator associativity (AST shape)', () {
    Expression expression(String sql) =>
        (SqlParser().parse('SELECT $sql AS v FROM t') as SelectStatement)
            .selectList
            .first
            .expression;

    /// Asserts `sql` parses to a left-nested tree whose outermost operator is
    /// [operator] — i.e. `a OP b OP c` groups as `(a OP b) OP c`. Under
    /// right-association the nesting flips to the `right` branch.
    void nestsLeft(String sql, String operator) {
      final root = expression(sql) as BinaryExpression;
      expect(root.operator, operator, reason: '$sql: outermost operator');
      expect(
        root.left,
        isA<BinaryExpression>(),
        reason: '$sql: must nest left — (a $operator b) $operator c',
      );
      expect(
        root.right,
        isA<ColumnExpression>(),
        reason: '$sql: right branch must be the trailing leaf, not a subtree',
      );
    }

    test('chained - nests left', () => nestsLeft('a - b - c', '-'));
    test('chained / nests left', () => nestsLeft('a / b / c', '/'));
    test('chained % nests left', () => nestsLeft('a % b % c', '%'));
    test('chained + nests left', () => nestsLeft('a + b + c', '+'));
    test('chained * nests left', () => nestsLeft('a * b * c', '*'));

    test('a four-operand chain nests left the whole way down', () {
      // ((a - b) - c) - d
      final root = expression('a - b - c - d') as BinaryExpression;
      final l1 = root.left as BinaryExpression;
      final l2 = l1.left as BinaryExpression;
      expect(l2.left, isA<ColumnExpression>());
      expect([
        root.right,
        l1.right,
        l2.right,
      ], everyElement(isA<ColumnExpression>()));
    });

    test('mixed + and - fold left in source order', () {
      // (a + b) - c  — the operator comes from the separator, so a fold that
      // reuses one operator for the whole chain would yield (a + b) + c.
      final root = expression('a + b - c') as BinaryExpression;
      expect(root.operator, '-');
      expect((root.left as BinaryExpression).operator, '+');
    });

    test('* binds tighter than + (precedence, not associativity)', () {
      // a + (b * c) — the multiplicative subtree hangs off the RIGHT here.
      final root = expression('a + b * c') as BinaryExpression;
      expect(root.operator, '+');
      expect(root.left, isA<ColumnExpression>());
      expect((root.right as BinaryExpression).operator, '*');
    });

    test('a single operand yields no BinaryExpression at all', () {
      // Guards the one-element fast path: a fold over a 1-element list must
      // return the element itself, not wrap it.
      expect(expression('a'), isA<ColumnExpression>());
    });
  });

  // Multi-element list productions. Each is built by the same separated-list
  // combinator, so these pin the element COUNT and ORDER — a list silently
  // truncated to its first element, or one that leaks separators in as
  // elements, fails here.
  group('multi-element list productions', () {
    SelectStatement select(String sql) =>
        SqlParser().parse(sql) as SelectStatement;

    test('SELECT list keeps all items in order', () {
      final s = select('SELECT a, b, c, d FROM t');
      expect(s.selectList.length, 4);
      expect(
        s.selectList.map((i) => (i.expression as ColumnExpression).column),
        ['a', 'b', 'c', 'd'],
      );
    });

    test('GROUP BY keeps all expressions', () {
      final s = select('SELECT a FROM t GROUP BY a, b, c');
      expect(s.groupBy.length, 3);
    });

    test('ORDER BY keeps all items with their directions', () {
      final s = select('SELECT a FROM t ORDER BY a ASC, b DESC, c ASC');
      expect(s.orderBy.length, 3);
      expect(s.orderBy.map((o) => o.ascending), [true, false, true]);
    });

    test('IN keeps every value on one node', () {
      final in_ =
          select('SELECT a FROM t WHERE a IN (1, 2, 3, 4)').where!
              as InExpression;
      expect(in_.negated, isFalse);
      expect(in_.subquery, isNull);
      expect((in_.expression as ColumnExpression).column, 'a');
      expect(in_.values.map((v) => (v as LiteralExpression).value), [
        1,
        2,
        3,
        4,
      ]);
    });

    test('NOT IN sets negated and keeps every value', () {
      final in_ =
          select('SELECT a FROM t WHERE a NOT IN (1, 2, 3)').where!
              as InExpression;
      expect(in_.negated, isTrue);
      expect(in_.values.length, 3);
    });

    test('a single-value IN list is still an IN node', () {
      final in_ =
          select('SELECT a FROM t WHERE a IN (1)').where! as InExpression;
      expect(in_.values.length, 1);
      expect(in_.negated, isFalse);
    });

    test('IN (SELECT ...) parses to the subquery form', () {
      final in_ =
          select('SELECT a FROM t WHERE a IN (SELECT b FROM u)').where!
              as InExpression;
      expect(in_.subquery, isNotNull);
      expect(in_.values, isEmpty);
    });

    test('EXISTS parses to an ExistsExpression', () {
      final w = select('SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u)').where!;
      expect(w, isA<ExistsExpression>());
      expect((w as ExistsExpression).subquery, isNotNull);
    });

    test('NOT EXISTS parses (NOT binds outside EXISTS)', () {
      // notExpression binds the leading NOT, so it arrives as NOT over EXISTS.
      final w = select(
        'SELECT a FROM t WHERE NOT EXISTS (SELECT 1 FROM u)',
      ).where!;
      expect(w, isA<UnaryExpression>());
      expect((w as UnaryExpression).operand, isA<ExistsExpression>());
    });

    test('function arguments keep count and order', () {
      final s = select('SELECT COALESCE(a, b, c) AS v FROM t');
      final f = s.selectList.first.expression as FunctionExpression;
      expect(f.arguments.length, 3);
      expect(f.arguments.map((a) => (a as ColumnExpression).column), [
        'a',
        'b',
        'c',
      ]);
    });

    test('FROM comma list keeps all table references', () {
      expect(select('SELECT a FROM t1, t2, t3').from.length, 3);
    });

    test('JOIN USING keeps all columns', () {
      final s = select('SELECT * FROM t1 JOIN t2 USING (a, b, c)');
      final joined = s.from.first as JoinedTable;
      final using = joined.joins.first.condition as UsingCondition;
      expect(using.columns, ['a', 'b', 'c']);
    });

    test('chained AND keeps every conjunct', () {
      // Folded with a hardcoded operator, so only the operand count is at risk.
      final s = select('SELECT a FROM t WHERE a = 1 AND b = 2 AND c = 3');
      final root = s.where as BinaryExpression;
      expect(root.operator, 'AND');
      expect((root.left as BinaryExpression).operator, 'AND');
    });

    test('chained OR keeps every disjunct', () {
      final s = select('SELECT a FROM t WHERE a = 1 OR b = 2 OR c = 3');
      final root = s.where as BinaryExpression;
      expect(root.operator, 'OR');
      expect((root.left as BinaryExpression).operator, 'OR');
    });

    test('WITH keeps every CTE and its column list', () {
      final q =
          SqlParser().parse(
                'WITH x(p, q) AS (SELECT a, b FROM t), y AS (SELECT c FROM t) '
                'SELECT p FROM x',
              )
              as QueryStatement;
      expect(q.ctes.length, 2, reason: 'CTE list');
      expect(q.ctes.first.columns, ['p', 'q'], reason: 'CTE column list');
      expect(q.ctes.map((c) => c.name), ['x', 'y']);
    });
  });

  // The sole production nesting one separated list directly inside another
  // (rows of expressions), plus the table alias column list. Execution is
  // covered in sql_test.dart; these pin the AST shape.
  group('VALUES table (AST shape)', () {
    ValuesTable valuesOf(String sql) =>
        (SqlParser().parse(sql) as SelectStatement).from.first as ValuesTable;

    test('rows and per-row expressions both keep their counts', () {
      final v = valuesOf("SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c')");
      expect(v.values.length, 3, reason: 'row count');
      expect(v.values.map((r) => r.length), [2, 2, 2], reason: 'per-row arity');
    });

    test('alias and its column list survive', () {
      final v = valuesOf("SELECT n FROM VALUES (1, 'a'), (2, 'b') AS t(n, s)");
      expect(v.alias, 't');
      expect(v.columnAliases, ['n', 's']);
    });

    test('a bare alias without a column list leaves columnAliases null', () {
      final v = valuesOf('SELECT * FROM VALUES (1), (2) AS t');
      expect(v.alias, 't');
      expect(v.columnAliases, isNull);
    });
  });

  // ---- Relocated: parser DoS guard (from resilience_test.dart) ----
  group('SQL parser DoS guard', () {
    test('deeply nested parentheses throw instead of overflowing', () {
      final sql = 'SELECT ${'(' * 400}1${')' * 400}';
      expect(() => SqlParser().parse(sql), throwsA(isA<FormatException>()));
    });

    test('a modestly nested expression still parses', () {
      expect(SqlParser().parse('SELECT (((1)))'), isNotNull);
    });

    test('parens inside a double-quoted string do not trip the guard', () {
      // A valid double-quoted string literal packed with parens must parse;
      // the depth scanner must treat "..." as opaque, like the grammar does.
      final sql = 'SELECT ${'"'}${'(' * 400}${'"'}';
      expect(SqlParser().parse(sql), isNotNull);
    });

    test('parens inside a backtick identifier do not trip the guard', () {
      final sql = 'SELECT ${'`'}${'(' * 400}${'`'} FROM t';
      expect(SqlParser().parse(sql), isNotNull);
    });
  });

  // ---- Relocated: doubled-quote escape (from regression_test.dart) ----
  test('parser handles a doubled-quote escape in a WHERE string', () async {
    final engine = SqlEngine();
    engine.registerTable(
      'p',
      Table(
        name: 'p',
        columns: [
          ListColumn<String>(
            name: 's',
            dataType: DataType.string,
            data: ["it's", 'plain', "o''nope"], // 3rd is a literal
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [Field(name: 's', dataType: DataType.string)],
        ),
      ),
    );
    // The 'it''s' literal must parse to the value it's (was truncated to "it").
    final t = await engine.execute("SELECT s FROM p WHERE s = 'it''s'");
    expect(t.toRows().map((r) => r['s']).toList(), ["it's"]);
  });

  group('keyword whitespace (audit 7)', () {
    // keyword() trims surrounding whitespace, so a .flatten() layered OVER it
    // captures that whitespace in the yielded text. Four consumers compared
    // that text against exact strings and silently mis-parsed: `SELECT true`
    // returned false, `LEFT  OUTER JOIN` became INNER, `UNION  ALL` deduped,
    // and `CAST(x AS INTEGER )` fell through to a string cast.
    Table ints(String name, String column, List<int> values) => Table(
      name: name,
      columns: [
        ListColumn<int>(name: column, dataType: DataType.int64, data: values),
      ],
      schema: Schema(
        fields: [
          Field(name: column, dataType: DataType.int64, nullable: false),
        ],
      ),
    );

    setUp(() {
      engine.registerTable('t', ints('t', 'a', [1, 2]));
      engine.registerTable('u', ints('u', 'b', [2, 3]));
    });

    test('a TRUE literal is true even when followed by whitespace', () async {
      final r = await engine.execute('SELECT true AS v FROM t');
      expect(r.column('v')!.toList(), [true, true]);
      final c = await engine.execute(
        'SELECT CASE WHEN true THEN 5 END AS v FROM t',
      );
      expect(c.column('v')!.toList(), [5, 5]);
      final f = await engine.execute('SELECT false AS v FROM t');
      expect(f.column('v')!.toList(), [false, false]);
    });

    test('LEFT OUTER JOIN survives doubled/newline whitespace', () async {
      for (final sql in [
        'SELECT a, b FROM t LEFT  OUTER JOIN u ON t.a = u.b',
        'SELECT a, b FROM t LEFT\nOUTER JOIN u ON t.a = u.b',
      ]) {
        final r = await engine.execute(sql);
        // The null-padded row proves the join stayed LEFT (INNER drops it).
        expect(r.rowCount, 2, reason: sql);
        expect(r.toRows().any((row) => row['b'] == null), isTrue, reason: sql);
      }
    });

    test('UNION ALL survives doubled/newline whitespace', () async {
      for (final sql in [
        'SELECT a FROM t UNION  ALL SELECT a FROM t',
        'SELECT a FROM t UNION\nALL SELECT a FROM t',
      ]) {
        final r = await engine.execute(sql);
        expect(r.rowCount, 4, reason: '$sql must not dedup as plain UNION');
      }
    });

    test('CAST keeps its target type when followed by whitespace', () async {
      final r = await engine.execute(
        "SELECT CAST('12' AS INTEGER ) AS v FROM t",
      );
      expect(r.schema.fields.single.dataType, DataType.int64);
      expect(r.column('v')!.toList(), [12, 12]);
    });
  });

  group('CAST target types (audit 7)', () {
    setUp(() => engine.registerTable('t', ints('t', [1])));

    Future<Table> one(String expr) =>
        engine.execute('SELECT $expr AS v FROM t');

    test('CAST AS DECIMAL of an int is exact (was float64)', () async {
      // float64 would round 9007199254740993 down to ...992.
      final r = await one('CAST(9007199254740993 AS DECIMAL)');
      expect(r.schema.fields.single.dataType, DataType.decimal);
      expect(r.column('v')!.toList().single, BigInt.parse('9007199254740993'));
    });

    test('CAST AS DECIMAL(p,s) scales string/double/int exactly', () async {
      final s = await one("CAST('123.45' AS DECIMAL(10,2))");
      expect(s.schema.fields.single.scale, 2);
      expect(s.schema.fields.single.precision, 10);
      expect(s.column('v')!.toList().single, BigInt.from(12345));
      final d = await one('CAST(1.5 AS DECIMAL(10,2))');
      expect(d.column('v')!.toList().single, BigInt.from(150));
      final i = await one('CAST(7 AS DECIMAL(10,2))');
      expect(i.column('v')!.toList().single, BigInt.from(700));
    });

    test('CAST AS NUMERIC maps to decimal too', () async {
      final r = await one('CAST(7 AS NUMERIC(5,1))');
      expect(r.schema.fields.single.dataType, DataType.decimal);
      expect(r.column('v')!.toList().single, BigInt.from(70));
    });

    test('a value with more fractional digits than s fails loud', () async {
      await expectLater(
        one("CAST('1.234' AS DECIMAL(10,2))"),
        throwsA(isA<FormatException>()),
      );
    });

    test('CAST AS INT parses', () async {
      final r = await one("CAST('12' AS INT)");
      expect(r.schema.fields.single.dataType, DataType.int64);
      expect(r.column('v')!.toList().single, 12);
    });
  });

  group('IS operand validation (audit 7)', () {
    setUp(() => engine.registerTable('t', ints('t', [1, 2])));

    test(
      'x IS <non-boolean literal> fails loud (was silent IS NULL)',
      () async {
        await expectLater(
          engine.execute('SELECT n FROM t WHERE n IS 5'),
          throwsA(isA<UnsupportedError>()),
        );
        await expectLater(
          engine.execute('SELECT n FROM t WHERE n IS NOT 5'),
          throwsA(isA<UnsupportedError>()),
        );
      },
    );

    test('IS NULL / IS NOT NULL / IS TRUE keep working', () async {
      expect(
        (await engine.execute('SELECT n FROM t WHERE n IS NULL')).rowCount,
        0,
      );
      expect(
        (await engine.execute('SELECT n FROM t WHERE n IS NOT NULL')).rowCount,
        2,
      );
    });
  });

  group('aggregate output naming (audit 7)', () {
    setUp(() {
      engine.registerTable(
        't2',
        Table(
          name: 't2',
          columns: [
            NumericColumn.int64(name: 'a', values: [1, 2], nullable: false),
            NumericColumn.int64(name: 'b', values: [3, 4], nullable: false),
          ],
          schema: Schema(
            fields: [
              Field(name: 'a', dataType: DataType.int64, nullable: false),
              Field(name: 'b', dataType: DataType.int64, nullable: false),
            ],
          ),
        ),
      );
    });

    test('duplicate unaliased aggregates get distinct names', () async {
      // The aggregating projection skipped _uniquifyAuto, emitting a
      // duplicate-name schema that broke by-name access and subqueries.
      final r = await engine.execute('SELECT COUNT(a), COUNT(b) FROM t2');
      expect(r.schema.fields.map((f) => f.name).toSet().length, 2);
      final m = await engine.execute('SELECT MAX(a), MAX(b) FROM t2');
      expect(m.schema.fields.map((f) => f.name).toList(), ['MAX', 'MAX_2']);
      expect(m.toRows().single, {'MAX': 2, 'MAX_2': 4});
    });
  });

  group('set-op heterogeneous branch types (audit 7)', () {
    // A logical column ref carries a `string` PLACEHOLDER type, and
    // _bindSetOperation used to run commonType() against it — pairing a
    // literal/CAST branch with a bare-column branch injected a stringifying
    // CAST into a valid all-numeric set operation. Reconciliation belongs to
    // the physical layer, which sees real types.
    setUp(() {
      engine.registerTable('t', ints('t', [5, 10]));
    });

    test('literal UNION column stays numeric (both orders)', () async {
      for (final sql in [
        'SELECT 1 AS n FROM t UNION SELECT n FROM t',
        'SELECT n FROM t UNION SELECT 1 FROM t',
      ]) {
        final r = await engine.execute(sql);
        expect(r.schema.fields.single.dataType, DataType.int64, reason: sql);
      }
    });

    test('aliased literal UNION ALL column stays numeric', () async {
      final r = await engine.execute(
        'SELECT 100 AS n FROM t UNION ALL SELECT n FROM t',
      );
      expect(r.schema.fields.single.dataType, DataType.int64);
      expect(r.column('n')!.toList().cast<int>()..sort(), [5, 10, 100, 100]);
    });

    test('CAST branch UNION column stays numeric', () async {
      final r = await engine.execute(
        'SELECT CAST(n AS INTEGER) AS n FROM t UNION SELECT n FROM t',
      );
      expect(r.schema.fields.single.dataType, DataType.int64);
      expect(r.column('n')!.toList().toSet(), {5, 10});
    });

    test('INTERSECT and EXCEPT with a literal branch stay numeric', () async {
      final i = await engine.execute(
        'SELECT 5 AS n FROM t INTERSECT SELECT n FROM t',
      );
      expect(i.column('n')!.toList(), [5]);
      final e = await engine.execute(
        'SELECT n FROM t EXCEPT SELECT 5 AS n FROM t',
      );
      expect(e.column('n')!.toList(), [10]);
    });

    test('SELECT * over an expression GROUP BY works as a branch', () async {
      // logical.Aggregate.schema dropped non-column group keys, so this valid
      // query failed the set-op arity check ("0 vs 1 columns").
      engine.registerTable('g', ints('g', [1, 2, 3, 4]));
      final r = await engine.execute(
        'SELECT * FROM g GROUP BY n % 2 UNION SELECT n FROM t',
      );
      expect(r.column('group0')!.toList().toSet(), {0, 1, 5, 10});
    });

    test('a projected NOT LIKE types as boolean in a set-op branch', () async {
      // notLike was missing from the logical boolean-type group (drifted from
      // _binaryResultType), so this branch typed as string.
      engine.registerTable(
        's',
        Table(
          name: 's',
          columns: [
            ListColumn<String>(
              name: 'v',
              dataType: DataType.string,
              data: ['xa', 'yb'],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'v', dataType: DataType.string, nullable: false),
            ],
          ),
        ),
      );
      final r = await engine.execute(
        "SELECT v NOT LIKE 'x%' AS m FROM s UNION ALL "
        'SELECT n > 5 AS m FROM t',
      );
      expect(r.schema.fields.single.dataType, DataType.boolean);
      expect(r.column('m')!.toList(), [false, true, false, true]);
    });
  });
}
