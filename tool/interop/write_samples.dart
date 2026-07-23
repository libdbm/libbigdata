// Writes the interop sample fixtures that external engines (Spark, delta-rs,
// pyiceberg, pyarrow) read back in test/interop/spark_interop_test.dart, via
// tool/interop/validate_all.py. Each fixture is written in its own try/catch so
// one failure doesn't abort the rest; prints DONE last so setUpAll proceeds.
//
// Usage: dart run tool/interop/write_samples.dart <output-dir>
import 'dart:io';

import 'package:libbigdata/libbigdata.dart';

late final String out;

Future<void> attempt(String name, Future<void> Function() fn) async {
  try {
    await fn();
  } catch (e, st) {
    stderr.writeln('[skip $name] $e\n$st');
  }
}

Field f(
  String name,
  DataType t, {
  bool nullable = false,
  int? precision,
  int? scale,
  List<Field>? children,
}) => Field(
  name: name,
  dataType: t,
  nullable: nullable,
  precision: precision,
  scale: scale,
  children: children ?? const [],
);

Table table(List<Field> fields, List<Column> columns) => Table(
  name: 't',
  schema: Schema(fields: fields),
  columns: columns,
);

NumericColumn i64(String name, List<int> v, {bool nullable = false}) =>
    NumericColumn.int64(name: name, values: v, nullable: nullable);

ListColumn<String> str(String name, List<String?> v, {bool nullable = false}) =>
    ListColumn<String>(
      name: name,
      dataType: DataType.string,
      data: v,
      nullable: nullable,
    );

ListColumn<BigInt> dec(String name, List<BigInt?> v, {bool nullable = false}) =>
    ListColumn<BigInt>(
      name: name,
      dataType: DataType.decimal,
      data: v,
      nullable: nullable,
    );

Future<void> main(List<String> args) async {
  out = args.first;
  Directory(out).createSync(recursive: true);

  // 1. flat Parquet, all types incl. decimal(10,2)
  await attempt('parquet_flat', () async {
    final t = table(
      [
        f('i64', DataType.int64),
        f('dec', DataType.decimal, precision: 10, scale: 2),
        f('s', DataType.string),
      ],
      [
        i64('i64', [10, 20]),
        dec('dec', [BigInt.from(12345), BigInt.from(-99)]),
        str('s', ['alpha', 'beta']),
      ],
    );
    await ParquetWriter('$out/flat.parquet').write(t);
  });

  // 2. nested Parquet: list / struct / map
  await attempt('parquet_nested', () async {
    final t = table(
      [
        f(
          'tags',
          DataType.list,
          children: [f('element', DataType.int64, nullable: true)],
        ),
        f(
          'info',
          DataType.struct,
          children: [f('a', DataType.int64), f('b', DataType.string)],
        ),
        f(
          'props',
          DataType.map,
          children: [
            f(
              'key_value',
              DataType.struct,
              children: [f('key', DataType.string), f('value', DataType.int64)],
            ),
          ],
        ),
      ],
      [
        ListColumn(
          name: 'tags',
          dataType: DataType.list,
          data: [
            [1, 2],
            [9],
          ],
          nullable: false,
        ),
        ListColumn(
          name: 'info',
          dataType: DataType.struct,
          data: [
            {'a': 1, 'b': 'x'},
            {'a': 2, 'b': 'y'},
          ],
          nullable: false,
        ),
        ListColumn(
          name: 'props',
          dataType: DataType.map,
          data: [
            {'k1': 1},
            {'k2': 2},
          ],
          nullable: false,
        ),
      ],
    );
    await ParquetWriter('$out/nested.parquet').write(t);
  });

  // 3. multi-level nested: array<array<bigint>>, array<struct<a,b>>
  await attempt('parquet_deep', () async {
    final t = table(
      [
        f('id', DataType.int64),
        f(
          'matrix',
          DataType.list,
          children: [
            f(
              'element',
              DataType.list,
              children: [f('element', DataType.int64, nullable: true)],
            ),
          ],
        ),
        f(
          'people',
          DataType.list,
          children: [
            f(
              'element',
              DataType.struct,
              children: [f('a', DataType.int64), f('b', DataType.string)],
            ),
          ],
        ),
      ],
      [
        i64('id', [1, 2]),
        ListColumn(
          name: 'matrix',
          dataType: DataType.list,
          data: [
            [
              [1, 2],
              [3],
            ],
            [
              [4],
            ],
          ],
          nullable: false,
        ),
        ListColumn(
          name: 'people',
          dataType: DataType.list,
          data: [
            [
              {'a': 1, 'b': 'x'},
            ],
            [
              {'a': 2, 'b': 'y'},
            ],
          ],
          nullable: false,
        ),
      ],
    );
    await ParquetWriter('$out/deep.parquet').write(t);
  });

  // 4. int32 DELTA with a delta over 2^31 (read back by pyarrow's uint32 VLQ)
  await attempt('parquet_int32_delta', () async {
    final v = [
      -2000000000,
      2000000000,
      -1999999999,
      1999999998,
      -1999999997,
      1999999996,
      -2147483648,
      2147483647,
    ];
    final t = table(
      [f('v', DataType.int32)],
      [NumericColumn.int32(name: 'v', values: v, nullable: false)],
    );
    await ParquetWriter('$out/int32delta.parquet').write(t);
  });

  // 5. all-required nested struct (no definition levels to the leaf)
  await attempt('parquet_required_nested', () async {
    final t = table(
      [
        f('s', DataType.struct, children: [f('a', DataType.int64)]),
      ],
      [
        ListColumn(
          name: 's',
          dataType: DataType.struct,
          data: [
            for (var i = 1; i <= 8; i++) {'a': i * 100},
          ],
          nullable: false,
        ),
      ],
    );
    await ParquetWriter('$out/required_nested.parquet').write(t);
  });

  // 6. partitioned Delta (region), 4 rows
  await attempt('delta_partitioned', () async {
    final t = table(
      [f('id', DataType.int64), f('region', DataType.string)],
      [
        i64('id', [1, 2, 3, 4]),
        str('region', ['us', 'eu', 'us', 'eu']),
      ],
    );
    await DeltaWriter('$out/delta_part').write(t, partitionBy: ['region']);
  });

  // 7. Delta with decimal(10,2)
  await attempt('delta', () async {
    final t = table(
      [
        f('id', DataType.int64),
        f('dec', DataType.decimal, precision: 10, scale: 2),
      ],
      [
        i64('id', [1, 2, 3, 4]),
        dec('dec', [
          BigInt.from(12345),
          BigInt.from(200),
          BigInt.from(-50),
          BigInt.from(9999),
        ]),
      ],
    );
    await DeltaWriter('$out/delta_dec').write(t);
  });

  // 8. Delta null + date partition values
  await attempt('delta_partition_values', () async {
    final t = table(
      [
        f('id', DataType.int64),
        f('region', DataType.string, nullable: true),
        f('d', DataType.date),
      ],
      [
        i64('id', [1, 2]),
        str('region', ['eu', null], nullable: true),
        ListColumn<DateTime>(
          name: 'd',
          dataType: DataType.date,
          data: [DateTime.utc(2024, 1, 15), DateTime.utc(2024, 1, 15)],
          nullable: false,
        ),
      ],
    );
    await DeltaWriter('$out/delta_pv').write(t, partitionBy: ['region', 'd']);
  });

  // 9. Iceberg with decimal(10,2), 4 rows
  await attempt('iceberg', () async {
    final t = table(
      [
        f('id', DataType.int64),
        f('dec', DataType.decimal, precision: 10, scale: 2),
      ],
      [
        i64('id', [1, 2, 3, 4]),
        dec('dec', [
          BigInt.from(12345),
          BigInt.from(200),
          BigInt.from(-50),
          BigInt.from(9999),
        ]),
      ],
    );
    await IcebergWriter('$out/ice_dec/t').write(t);
  });

  // 10. Iceberg with expired snapshots: 3 appends of 4 rows -> expire to 1
  await attempt('iceberg_expired', () async {
    final w = IcebergWriter('$out/ice_expired/t');
    for (var b = 0; b < 3; b++) {
      await w.write(
        table(
          [f('id', DataType.int64)],
          [
            i64('id', [for (var i = 0; i < 4; i++) b * 4 + i]),
          ],
        ),
      );
    }
    await w.expireSnapshots(retain: 1);
  });

  // 11. Iceberg repartitioned: region spec (0) then country spec (1)
  await attempt('iceberg_repartitioned', () async {
    final w = IcebergWriter('$out/ice_repart/t');
    Table rows(List<String> country) => table(
      [
        f('id', DataType.int64),
        f('country', DataType.string),
        f('region', DataType.string),
      ],
      [
        i64('id', [for (var i = 0; i < country.length; i++) i]),
        str('country', country),
        str('region', ['eu', 'eu', 'us']),
      ],
    );
    await w.write(rows(['de', 'fr', 'ca']), partitionBy: ['region']);
    await w.write(
      rows(['de', 'fr', 'ca']),
      mode: IcebergWriteMode.overwrite,
      partitionBy: ['country'],
    );
  });

  // 12. Iceberg partitioned by one identity field, 4 rows
  await attempt('iceberg_partitioned', () async {
    final t = table(
      [f('id', DataType.int64), f('p', DataType.string)],
      [
        i64('id', [1, 2, 3, 4]),
        str('p', ['a', 'a', 'b', 'b']),
      ],
    );
    await IcebergWriter('$out/ice_part/t').write(t, partitionBy: ['p']);
  });

  // 13. CSV with scaled decimals
  await attempt('csv', () async {
    final t = table(
      [
        f('id', DataType.int64),
        f('dec', DataType.decimal, precision: 10, scale: 2),
      ],
      [
        i64('id', [1, 2]),
        dec('dec', [BigInt.from(12345), BigInt.from(6789)]),
      ],
    );
    await CsvWriter().writeTable(t, '$out/data.csv');
  });

  // 14. JSON Lines
  await attempt('json', () async {
    final t = table(
      [
        f('id', DataType.int64),
        f('name', DataType.string),
        f('d', DataType.date),
      ],
      [
        i64('id', [1, 2]),
        str('name', ['a', 'b']),
        ListColumn<DateTime>(
          name: 'd',
          dataType: DataType.date,
          data: [DateTime.utc(2024, 1, 15), DateTime.utc(2024, 2, 20)],
          nullable: false,
        ),
      ],
    );
    await JsonWriter(
      JsonOptions.jsonLines(),
    ).writeFile(Stream.value(t), '$out/data.jsonl');
  });

  print('DONE');
}
