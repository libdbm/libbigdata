import 'package:libbigdata/libbigdata.dart' hide JoinType;
import 'package:libbigdata/src/sql/planner/physical_plan.dart';
import 'package:libbigdata/src/sql/operators/physical_operators.dart';
import 'package:libbigdata/src/sql/planner/logical_plan.dart'
    show BinaryOperator, JoinType;

/// Vectorized vs per-row execution micro-benchmark for filter and projection.
/// Compares the new column-wise kernels against the previous row-at-a-time
/// interpreted path on identical data.
///
///   dart run benchmark/exec_bench.dart [rows] [reps]
Future<void> main(List<String> args) async {
  final n = args.isNotEmpty ? int.parse(args[0]) : 2000000;
  final reps = args.length > 1 ? int.parse(args[1]) : 5;

  final id = NumericColumn.int64(
    name: 'id',
    values: List.generate(n, (i) => i),
    nullable: false,
  );
  final v = NumericColumn.float64(
    name: 'v',
    values: List.generate(n, (i) => i.toDouble()),
    nullable: false,
  );
  final schema = Schema(
    fields: [
      Field(name: 'id', dataType: DataType.int64, nullable: false),
      Field(name: 'v', dataType: DataType.float64, nullable: false),
    ],
  );
  final batch = RecordBatch(columns: [id, v], schema: schema, rowCount: n);

  final pred = PhysicalBinaryExpression(
    left: PhysicalColumnExpression('id', DataType.int64),
    operator: BinaryOperator.gt,
    right: PhysicalLiteralExpression(n ~/ 2, DataType.int64),
    resultType: DataType.boolean,
  );
  final proj = PhysicalBinaryExpression(
    left: PhysicalColumnExpression('id', DataType.int64),
    operator: BinaryOperator.add,
    right: PhysicalLiteralExpression(1, DataType.int64),
    resultType: DataType.int64,
  );

  Future<int> median(Future<void> Function() op) async {
    final t = <int>[];
    for (var i = 0; i < reps; i++) {
      final sw = Stopwatch()..start();
      await op();
      t.add(sw.elapsedMicroseconds);
    }
    t.sort();
    return t[t.length ~/ 2];
  }

  // Filter: per-row evaluate -> List<bool>  vs  vectorized evalMask.
  final filterScalar = await median(() async {
    final mask = <bool>[];
    for (int i = 0; i < n; i++) {
      mask.add(pred.evaluate(batch, i) == true);
    }
  });
  final filterVector = await median(() async {
    final m = pred.evalMask(batch);
    m.selected();
  });

  // Projection: per-row evaluate -> boxed list  vs  vectorized evalColumn.
  final projScalar = await median(() async {
    final out = [];
    for (int i = 0; i < n; i++) {
      out.add(proj.evaluate(batch, i));
    }
  });
  final projVector = await median(() async {
    proj.evalColumn(batch);
  });

  String ms(int us) => (us / 1000).toStringAsFixed(1);
  String x(int scalar, int vector) =>
      '${(scalar / vector).toStringAsFixed(1)}x';

  print('# Vectorized vs per-row execution ($n rows, median of $reps)\n');
  print('| op | per-row ms | vectorized ms | speedup |');
  print('|---|--:|--:|--:|');
  print(
    '| filter (id > n/2) | ${ms(filterScalar)} | ${ms(filterVector)} | '
    '${x(filterScalar, filterVector)} |',
  );
  print(
    '| projection (id + 1) | ${ms(projScalar)} | ${ms(projVector)} | '
    '${x(projScalar, projVector)} |',
  );

  // Blocking-operator throughput (group-by / sort / join) on the vectorized path.
  final grp = NumericColumn.int64(
    name: 'g',
    values: List.generate(n, (i) => i % 100),
    nullable: false,
  );
  final src = Table(
    name: 't',
    columns: [id, v, grp],
    schema: Schema(
      fields: [
        Field(name: 'id', dataType: DataType.int64, nullable: false),
        Field(name: 'v', dataType: DataType.float64, nullable: false),
        Field(name: 'g', dataType: DataType.int64, nullable: false),
      ],
    ),
  );

  Future<void> drain(PhysicalPlan plan) async {
    final it = plan.execute();
    while (await it.next() != null) {}
    await it.close();
  }

  final groupBy = await median(
    () => drain(
      PhysicalAggregate(
        input: PhysicalTableScan(tableName: 't', table: src),
        groupBy: [PhysicalColumnExpression('g', DataType.int64)],
        aggregates: [
          PhysicalCount(),
          PhysicalSum(
            argument: PhysicalColumnExpression('v', DataType.float64),
          ),
        ],
        aliases: ['c', 's'],
      ),
    ),
  );
  final sort = await median(
    () => drain(
      PhysicalSort(
        input: PhysicalTableScan(tableName: 't', table: src),
        sortExpressions: [
          SortExpression(
            expression: PhysicalColumnExpression('g', DataType.int64),
            ascending: true,
          ),
        ],
      ),
    ),
  );
  // Self-join on the low-cardinality key (n rows x ~n/100 matches each).
  final small = Table(
    name: 'd',
    columns: [
      NumericColumn.int64(
        name: 'g',
        values: List.generate(100, (i) => i),
        nullable: false,
      ),
    ],
    schema: Schema(
      fields: [Field(name: 'g', dataType: DataType.int64, nullable: false)],
    ),
  );
  final join = await median(
    () => drain(
      PhysicalHashJoin(
        left: PhysicalTableScan(tableName: 't', table: src),
        right: PhysicalTableScan(tableName: 'd', table: small),
        leftKeys: ['g'],
        rightKeys: ['g'],
        joinType: JoinType.inner,
      ),
    ),
  );

  print('\n## Blocking operators (vectorized, $n rows)\n');
  print('| op | ms | M rows/s |');
  print('|---|--:|--:|');
  String rps(int us) => (n / (us / 1e6) / 1e6).toStringAsFixed(1);
  print('| group by (100 groups) | ${ms(groupBy)} | ${rps(groupBy)} |');
  print('| sort | ${ms(sort)} | ${rps(sort)} |');
  print('| inner join | ${ms(join)} | ${rps(join)} |');
}
