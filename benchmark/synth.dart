import 'dart:typed_data';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/core/column.dart' show NumericVector;

import 'results.dart';

/// Deterministic data synthesis for >1M-row profiling. Numeric columns are built
/// straight into typed buffers and `NumericVector.adopt`ed (zero-copy, no boxing);
/// strings use `StringColumn`, bools `ListColumn`. Shared by `sql_bench.dart` and
/// `test/perf_test.dart`.

/// Small reproducible xorshift32 PRNG (same family as `gen_data.dart`).
class _Rng {
  int _s;
  _Rng(int seed) : _s = seed == 0 ? 0x9e3779b9 : seed & 0xffffffff;
  int next() {
    var x = _s;
    x ^= (x << 13) & 0xffffffff;
    x ^= x >> 17;
    x ^= (x << 5) & 0xffffffff;
    _s = x & 0xffffffff;
    return _s;
  }

  /// A double in [0, 1).
  double unit() => next() / 0x100000000;
}

/// Number of distinct `category` values produced by [synthTable].
const int categoryCardinality = 12;

/// A wide fact table of [rows] rows: `id` (unique int64), `key` (int64 join/group
/// key in [0, keyCardinality)), `amount` (float64), `score` (float64, ~10% null),
/// `category` (string, [categoryCardinality] distinct), `flag` (bool).
Table synthTable(
  int rows, {
  int seed = 42,
  int keyCardinality = 1000,
  String name = 'fact',
}) {
  final rng = _Rng(seed);

  final id = Int64List(rows);
  final key = Int64List(rows);
  final amount = Float64List(rows);
  final score = Float64List(rows);
  final scoreNulls = Uint8List((rows + 7) >> 3); // bit set = null
  final category = List<String>.filled(rows, '');
  final flag = List<bool>.filled(rows, false);

  for (int i = 0; i < rows; i++) {
    id[i] = i;
    key[i] = rng.next() % keyCardinality;
    amount[i] = rng.unit() * 1000.0;
    if (rng.next() % 10 == 0) {
      scoreNulls[i >> 3] |= (1 << (i & 7)); // ~10% null
    } else {
      score[i] = rng.unit() * 100.0;
    }
    category[i] = 'cat_${rng.next() % categoryCardinality}';
    flag[i] = (rng.next() & 1) == 1;
  }

  return Table(
    name: name,
    columns: [
      NumericVector.adopt(name: 'id', dataType: DataType.int64, data: id),
      NumericVector.adopt(name: 'key', dataType: DataType.int64, data: key),
      NumericVector.adopt(
        name: 'amount',
        dataType: DataType.float64,
        data: amount,
      ),
      NumericVector.adopt(
        name: 'score',
        dataType: DataType.float64,
        data: score,
        nullBitmap: scoreNulls,
      ),
      StringColumn(name: 'category', values: category, nullable: false),
      ListColumn<bool>(
        name: 'flag',
        dataType: DataType.boolean,
        data: flag,
        nullable: false,
      ),
    ],
    schema: Schema(
      fields: [
        Field(name: 'id', dataType: DataType.int64, nullable: false),
        Field(name: 'key', dataType: DataType.int64, nullable: false),
        Field(name: 'amount', dataType: DataType.float64, nullable: false),
        Field(name: 'score', dataType: DataType.float64, nullable: true),
        Field(name: 'category', dataType: DataType.string, nullable: false),
        Field(name: 'flag', dataType: DataType.boolean, nullable: false),
      ],
    ),
  );
}

/// A small dimension table keyed 0..[rows)-1, for join profiling.
Table synthDim(int rows, {int seed = 7, String name = 'dim'}) {
  final key = Int64List(rows);
  final label = List<String>.filled(rows, '');
  for (int i = 0; i < rows; i++) {
    key[i] = i;
    label[i] = 'label_$i';
  }
  return Table(
    name: name,
    columns: [
      NumericVector.adopt(name: 'key', dataType: DataType.int64, data: key),
      StringColumn(name: 'label', values: label, nullable: false),
    ],
    schema: Schema(
      fields: [
        Field(name: 'key', dataType: DataType.int64, nullable: false),
        Field(name: 'label', dataType: DataType.string, nullable: false),
      ],
    ),
  );
}

/// Synthesize [rows] rows and write them to a Parquet file at [path].
Future<void> synthParquet(
  String path,
  int rows, {
  int seed = 42,
  int keyCardinality = 1000,
  ParquetWriterOptions? options,
}) async {
  final table = synthTable(rows, seed: seed, keyCardinality: keyCardinality);
  await ParquetWriter(
    path,
    options: options ?? const ParquetWriterOptions(),
  ).write(table).ok;
}

/// Median of [values] (sorts a copy).
int median(List<int> values) {
  final sorted = [...values]..sort();
  return sorted[sorted.length ~/ 2];
}
