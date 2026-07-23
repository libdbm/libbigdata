import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/formats/arrow/arrow_writer.dart';
import 'package:libbigdata/src/formats/arrow/arrow_reader.dart';
import 'package:libbigdata/src/formats/scan.dart';
import 'package:test/test.dart';

Table _ids(List<int> ids) => Table(
  name: 't',
  columns: [
    NumericColumn.int64(name: 'id', values: ids, nullable: false),
    NumericColumn.float64(
      name: 'v',
      values: [for (final i in ids) i.toDouble()],
      nullable: false,
    ),
  ],
  schema: Schema(
    fields: [
      Field(name: 'id', dataType: DataType.int64, nullable: false),
      Field(name: 'v', dataType: DataType.float64, nullable: false),
    ],
  ),
);

Future<void> _write(String path, Table table) async {
  Directory(
    path.substring(0, path.lastIndexOf('/')),
  ).createSync(recursive: true);
  await ParquetWriter(path).write(table);
}

void main() {
  late Directory dir;
  setUp(() => dir = Directory.systemTemp.createTempSync('dataset_'));
  tearDown(() => dir.deleteSync(recursive: true));

  group('partitioned dataset', () {
    setUp(() async {
      await _write(
        '${dir.path}/year=2024/month=01/part.parquet',
        _ids([0, 1, 2, 3, 4]),
      );
      await _write(
        '${dir.path}/year=2024/month=02/part.parquet',
        _ids([5, 6, 7, 8, 9]),
      );
      await _write(
        '${dir.path}/year=2023/month=01/part.parquet',
        _ids([10, 11, 12, 13, 14]),
      );
    });

    test('reads every file and exposes Hive partition columns', () async {
      final t = await (await DataFrameIO.readDataset(dir.path)).execute();
      expect(t.rowCount, 15);
      expect(t.schema.fieldNames, containsAll(['id', 'v', 'year', 'month']));
      // partition values are typed (int) from the path
      expect(t.column('year')!.toList().toSet(), {2024, 2023});
      expect(t.column('month')!.toList().toSet(), {1, 2});
    });

    test('a partition predicate prunes whole files', () async {
      final t = await (await DataFrameIO.readDataset(
        dir.path,
        filter: RowFilter.compare('year', FilterOp.eq, 2024),
      )).execute();
      expect(t.rowCount, 10); // only the two year=2024 files
      expect(t.column('year')!.toList().every((y) => y == 2024), isTrue);
    });

    test('an AND of partition predicates narrows to one file', () async {
      final t = await (await DataFrameIO.readDataset(
        dir.path,
        filter: RowFilter.and([
          RowFilter.compare('year', FilterOp.eq, 2024),
          RowFilter.compare('month', FilterOp.eq, 2),
        ]),
      )).execute();
      expect(t.rowCount, 5);
      expect(t.column('id')!.toList().toSet(), {5, 6, 7, 8, 9});
    });

    test('glob recurses', () async {
      final t = await (await DataFrameIO.readDataset(
        '${dir.path}/**.parquet',
      )).execute();
      expect(t.rowCount, 15);
    });

    test('projection keeps only requested columns', () async {
      final t = await (await DataFrameIO.readDataset(
        dir.path,
        columns: ['id', 'year'],
      )).execute();
      expect(t.schema.fieldNames, ['id', 'year']);
    });
  });

  group('schema unification across heterogeneous files', () {
    test('union of fields with null-fill', () async {
      Directory('${dir.path}/u').createSync(recursive: true);
      await ParquetWriter('${dir.path}/u/a.parquet').write(
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'a', values: [1, 2], nullable: false),
          ],
          schema: Schema(
            fields: [
              Field(name: 'a', dataType: DataType.int64, nullable: false),
            ],
          ),
        ),
      );
      await ParquetWriter('${dir.path}/u/b.parquet').write(
        Table(
          name: 't',
          columns: [
            ListColumn<String>(
              name: 'b',
              dataType: DataType.string,
              data: ['x', 'y'],
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'b', dataType: DataType.string, nullable: false),
            ],
          ),
        ),
      );

      final t = await (await DataFrameIO.readDataset(
        '${dir.path}/u',
      )).execute();
      expect(t.rowCount, 4);
      expect(t.schema.fieldNames, ['a', 'b']);
      // a.parquet sorts before b.parquet -> a rows first
      expect(t.column('a')!.toList(), [1, 2, null, null]);
      expect(t.column('b')!.toList(), [null, null, 'x', 'y']);
    });
  });

  test('single file works through the dataset path', () async {
    await _write('${dir.path}/solo.parquet', _ids([1, 2, 3]));
    final t = await (await DataFrameIO.readDataset(
      '${dir.path}/solo.parquet',
    )).execute();
    expect(t.rowCount, 3);
  });

  group('readBatches (streaming)', () {
    setUp(() async {
      await _write('${dir.path}/year=2024/p.parquet', _ids([0, 1, 2, 3, 4]));
      await _write('${dir.path}/year=2023/p.parquet', _ids([5, 6, 7, 8, 9]));
    });

    test(
      'concatenated batches equal eager read (rows, schema, partitions)',
      () async {
        final eager = await (await DataFrameIO.readDataset(dir.path)).execute();
        final batches = await DataFrameIO.readDatasetBatches(
          dir.path,
          size: 3,
        ).toList();

        // Every batch is bounded by size.
        expect(batches.every((b) => b.rowCount <= 3), isTrue);
        // Same unified schema.
        expect(batches.first.schema.fieldNames, eager.schema.fieldNames);
        // Same total rows + same id set (order matches file discovery order).
        final ids = [for (final b in batches) ...b.column('id')!.toList()];
        expect(ids.length, eager.rowCount);
        expect(ids.toSet(), eager.column('id')!.toList().toSet());
        // Partition column present and typed.
        final years = [for (final b in batches) ...b.column('year')!.toList()];
        expect(years.toSet(), {2023, 2024});
      },
    );

    test(
      'a pinned schema streams correctly (skips the metadata pre-pass)',
      () async {
        // Passing schema: pins the unified schema and skips the per-file peek
        // pre-pass entirely — the recommended path for very wide datasets.
        final schema = Schema(
          fields: [
            Field(name: 'id', dataType: DataType.int64, nullable: false),
            Field(name: 'v', dataType: DataType.float64, nullable: false),
            Field(name: 'year', dataType: DataType.int64, nullable: true),
          ],
        );
        final batches = await Dataset(
          dir.path,
        ).readBatches(schema: schema, size: 3).toList();
        expect(batches.first.schema.fieldNames, ['id', 'v', 'year']);
        final ids = [for (final b in batches) ...b.column('id')!.toList()];
        expect(ids.toSet(), {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
      },
    );

    test('projection + unknown-column parity with eager', () async {
      final batches = await DataFrameIO.readDatasetBatches(
        dir.path,
        columns: ['id', 'year'],
        size: 4,
      ).toList();
      expect(batches.first.schema.fieldNames, ['id', 'year']);

      expect(
        () => DataFrameIO.readDatasetBatches(dir.path, columns: ['nope']).first,
        throwsA(isA<ArgumentError>()),
      );
    });

    test('filter prunes whole partitions from the stream', () async {
      final batches = await DataFrameIO.readDatasetBatches(
        dir.path,
        filter: RowFilter.compare('year', FilterOp.eq, 2024),
      ).toList();
      final years = [for (final b in batches) ...b.column('year')!.toList()];
      expect(years.every((y) => y == 2024), isTrue);
      expect(years.length, 5);
    });

    test('null-fills across heterogeneous files', () async {
      final d = Directory.systemTemp.createTempSync('dsb_');
      try {
        await ParquetWriter('${d.path}/a.parquet').write(
          Table(
            name: 't',
            columns: [
              NumericColumn.int64(name: 'a', values: [1, 2], nullable: false),
            ],
            schema: Schema(
              fields: [
                Field(name: 'a', dataType: DataType.int64, nullable: false),
              ],
            ),
          ),
        );
        await ParquetWriter('${d.path}/b.parquet').write(
          Table(
            name: 't',
            columns: [
              ListColumn<String>(
                name: 'b',
                dataType: DataType.string,
                data: ['x'],
                nullable: false,
              ),
            ],
            schema: Schema(
              fields: [
                Field(name: 'b', dataType: DataType.string, nullable: false),
              ],
            ),
          ),
        );
        final batches = await DataFrameIO.readDatasetBatches(d.path).toList();
        for (final b in batches) {
          expect(b.schema.fieldNames, ['a', 'b']); // unified across files
        }
        final a = [for (final b in batches) ...b.column('a')!.toList()];
        final bb = [for (final b in batches) ...b.column('b')!.toList()];
        expect(a, [1, 2, null]);
        expect(bb, [null, null, 'x']);
      } finally {
        d.deleteSync(recursive: true);
      }
    });
  });

  group('unbounded caps', () {
    Table ids(List<int> values) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: values, nullable: false),
      ],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    test(
      'readDataset with null caps reads via the unbounded per-file path',
      () async {
        final dir = Directory.systemTemp.createTempSync('ds_unbounded_');
        addTearDown(() => dir.deleteSync(recursive: true));
        await ParquetWriter('${dir.path}/a.parquet').write(ids([1, 2, 3]));
        await ParquetWriter('${dir.path}/b.parquet').write(ids([4, 5, 6]));

        final df = await DataFrameIO.readDataset(
          dir.path,
          maxRows: null,
          maxBytes: null,
        );
        final table = await df.execute();
        expect(
          {for (final r in table.toRows()) r['id'] as int},
          {1, 2, 3, 4, 5, 6},
        );
      },
    );

    test(
      'null caps stay unbounded on the parallel (concurrency>1) path too',
      () async {
        final dir = Directory.systemTemp.createTempSync('ds_unbounded_par_');
        addTearDown(() => dir.deleteSync(recursive: true));
        // Two files across parallel isolates; the fix forwards null caps into
        // ReadSpec so the isolate readers don't silently re-cap at the default.
        await ParquetWriter('${dir.path}/a.parquet').write(ids([1, 2, 3]));
        await ParquetWriter('${dir.path}/b.parquet').write(ids([4, 5, 6]));

        final df = await DataFrameIO.readDataset(
          dir.path,
          maxRows: null,
          maxBytes: null,
          concurrency: 2,
        );
        final table = await df.execute();
        expect(
          {for (final r in table.toRows()) r['id'] as int},
          {1, 2, 3, 4, 5, 6},
        );
      },
    );
  });

  group('pushdown', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('dspush_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Future<List<Map<String, dynamic>>> rows(
      String path,
      List<String> cols,
    ) async {
      final batches = await DataFrameIO.readDatasetBatches(
        path,
        columns: cols,
      ).toList();
      return [for (final b in batches) ...b.toRows()];
    }

    test('CSV projection yields only selected columns', () async {
      final p = '${dir.path}/d.csv';
      File(p).writeAsStringSync('a,b,c\n1,x,9\n2,y,8\n');
      final out = await rows(p, ['a', 'c']);
      expect(out, [
        {'a': 1, 'c': 9},
        {'a': 2, 'c': 8},
      ]);
    });

    test('JSON Lines projection yields only selected columns', () async {
      final p = '${dir.path}/d.jsonl';
      File(
        p,
      ).writeAsStringSync('{"a":1,"b":"x","c":9}\n{"a":2,"b":"y","c":8}\n');
      final out = await rows(p, ['a', 'c']);
      expect(out, [
        {'a': 1, 'c': 9},
        {'a': 2, 'c': 8},
      ]);
    });

    test('plain JSON array projection yields only selected columns', () async {
      final p = '${dir.path}/d.json';
      File(
        p,
      ).writeAsStringSync('[{"a":1,"b":"x","c":9},{"a":2,"b":"y","c":8}]');
      final out = await rows(p, ['a', 'c']);
      expect(out, [
        {'a': 1, 'c': 9},
        {'a': 2, 'c': 8},
      ]);
    });

    test('Arrow projection yields only selected columns', () async {
      final p = '${dir.path}/d.arrow';
      await ArrowWriter(p).write(
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'a', values: [1, 2], nullable: false),
            ListColumn<String>(
              name: 'b',
              dataType: DataType.string,
              data: ['x', 'y'],
              nullable: false,
            ),
            NumericColumn.int64(name: 'c', values: [9, 8], nullable: false),
          ],
          schema: Schema(
            fields: [
              Field(name: 'a', dataType: DataType.int64, nullable: false),
              Field(name: 'b', dataType: DataType.string, nullable: false),
              Field(name: 'c', dataType: DataType.int64, nullable: false),
            ],
          ),
        ),
      );
      final out = await rows(p, ['a', 'c']);
      expect(out, [
        {'a': 1, 'c': 9},
        {'a': 2, 'c': 8},
      ]);
    });

    test(
      'Arrow skip-decode keeps later columns aligned past a nested one',
      () async {
        // selecting a scalar AFTER a skipped nested (list) column must
        // still decode correctly — skipTree has to consume the nested field's nodes
        // and buffers exactly, or the trailing column desyncs.
        final p = '${dir.path}/n.arrow';
        await ArrowWriter(p).write(
          Table(
            name: 't',
            columns: [
              NumericColumn.int64(name: 'a', values: [1, 2], nullable: false),
              ListColumn(
                name: 'tags',
                dataType: DataType.list,
                data: [
                  [1, 2],
                  [3],
                ],
                nullable: true,
              ),
              NumericColumn.int64(name: 'z', values: [70, 80], nullable: false),
            ],
            schema: Schema(
              fields: [
                Field(name: 'a', dataType: DataType.int64, nullable: false),
                Field(
                  name: 'tags',
                  dataType: DataType.list,
                  nullable: true,
                  children: [
                    Field(
                      name: 'item',
                      dataType: DataType.int64,
                      nullable: true,
                    ),
                  ],
                ),
                Field(name: 'z', dataType: DataType.int64, nullable: false),
              ],
            ),
          ),
        );
        // Select a + z, skipping the nested 'tags' in between.
        final out = await rows(p, ['a', 'z']);
        expect(out, [
          {'a': 1, 'z': 70},
          {'a': 2, 'z': 80},
        ]);
      },
    );

    group('projection of an unknown column fails loud', () {
      test('JSON reader throws (was silent narrowing)', () async {
        final p = '${dir.path}/u.jsonl';
        File(p).writeAsStringSync('{"a":1,"b":2}\n');
        await expectLater(
          JsonReader(
            const JsonOptions(jsonLines: true),
          ).readBatches(p, columns: ['a', 'nope']).toList(),
          throwsA(isA<ArgumentError>()),
        );
      });

      test('Arrow reader throws (was silent narrowing)', () async {
        final p = '${dir.path}/u.arrow';
        await ArrowWriter(p).write(
          Table(
            name: 't',
            columns: [
              NumericColumn.int64(name: 'a', values: [1], nullable: false),
            ],
            schema: Schema(
              fields: [
                Field(name: 'a', dataType: DataType.int64, nullable: false),
              ],
            ),
          ),
        );
        await expectLater(
          ArrowReader(p).readBatches(columns: ['a', 'nope']).toList(),
          throwsA(isA<ArgumentError>()),
        );
      });
    });
  });

  group('finding 2: dataset schema unification rejects type conflicts', () {
    test(
      'same-named column with a different type across files throws',
      () async {
        await File('${dir.path}/a.csv').writeAsString('n\n1\n2\n');
        await File('${dir.path}/b.csv').writeAsString('n\nhello\nworld\n');
        // a.n infers int64, b.n infers string — first-seen-wins would silently
        // mislabel the strings as int64.
        expect(() => Dataset(dir.path).read(), throwsA(isA<ArgumentError>()));
      },
    );
  });

  group('external review: dataset union hardening', () {
    Table dec(int scale, List<int> unscaled) => Table(
      name: 't',
      columns: [
        ListColumn<BigInt>(
          name: 'amt',
          dataType: DataType.decimal,
          data: [for (final u in unscaled) BigInt.from(u)],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(
            name: 'amt',
            dataType: DataType.decimal,
            nullable: false,
            precision: 10,
            scale: scale,
          ),
        ],
      ),
    );

    test('cross-scale decimal partitions rescale, not reinterpret', () async {
      // scale-2 150 (1.50) + scale-3 1500 (1.500): the union used to keep the
      // first file's scale and concat raw unscaled BigInts — 1.500 read back
      // as 15.00, a silent power-of-ten error.
      await ParquetWriter('${dir.path}/f1.parquet').write(dec(2, [150]));
      await ParquetWriter('${dir.path}/f2.parquet').write(dec(3, [1500]));
      final t = await Dataset(dir.path).read();
      expect(t.schema.field('amt')!.scale, 3);
      expect(t.toRows().map((r) => r['amt'].toString()).toSet(), {'1.500'});
      final streamed = await Dataset(dir.path).readBatches().toList();
      expect(
        streamed
            .expand((b) => b.toRows())
            .map((r) => r['amt'].toString())
            .toSet(),
        {'1.500'},
      );
    });

    test(
      'nested Parquet round-trips through Dataset (was null-filled)',
      () async {
        // ParquetReader.schema() used to report flattened dotted leaves
        // (info.a) while the stream emits reassembled top-level fields (info);
        // the pre-pass then unified names no batch carries and _conform
        // null-filled them, silently dropping the real nested columns.
        final nested = Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'id', values: [1, 2], nullable: false),
            ListColumn(
              name: 'tags',
              dataType: DataType.list,
              data: [
                [1, 2],
                [3],
              ],
              nullable: true,
            ),
            ListColumn(
              name: 'info',
              dataType: DataType.struct,
              data: [
                {'a': 1, 'b': 'x'},
                {'a': 2, 'b': 'y'},
              ],
              nullable: true,
            ),
          ],
          schema: Schema(
            fields: [
              Field(name: 'id', dataType: DataType.int64, nullable: false),
              Field(
                name: 'tags',
                dataType: DataType.list,
                nullable: true,
                children: [Field(name: 'element', dataType: DataType.int64)],
              ),
              Field(
                name: 'info',
                dataType: DataType.struct,
                nullable: true,
                children: [
                  Field(name: 'a', dataType: DataType.int64),
                  Field(name: 'b', dataType: DataType.string),
                ],
              ),
            ],
          ),
        );
        final file = '${dir.path}/nested.parquet';
        await ParquetWriter(file).write(nested);

        // The metadata schema must equal the streamed batch's schema.
        final meta = (await openScan(file).readSchema())!;
        final batches = await ParquetReader(file).readBatches().toList();
        expect(
          meta.fields.map((f) => '${f.name}:${f.dataType.name}').toList(),
          batches.first.schema.fields
              .map((f) => '${f.name}:${f.dataType.name}')
              .toList(),
        );

        Map<int, Map<String, Object?>> byId(
          Iterable<Map<String, dynamic>> rows,
        ) => {
          for (final r in rows)
            r['id'] as int: {'tags': r['tags'], 'info': r['info']},
        };
        final expected = {
          1: {
            'tags': [1, 2],
            'info': {'a': 1, 'b': 'x'},
          },
          2: {
            'tags': [3],
            'info': {'a': 2, 'b': 'y'},
          },
        };
        expect(byId((await Dataset(dir.path).read()).toRows()), expected);
        final streamed = await Dataset(dir.path).readBatches().toList();
        expect(byId(streamed.expand((b) => b.toRows())), expected);
      },
    );
  });

  group('finding 9: single format registry', () {
    test('an unknown extension throws instead of routing to Parquet', () {
      expect(
        () => openScan('${dir.path}/mystery.xyz'),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('ndjson is usable as a SQL source', () async {
      final path = '${dir.path}/data.ndjson';
      await File(path).writeAsString('{"n":1}\n{"n":2}\n');
      final engine = SqlEngine(allowExternalPaths: true);
      final result = await engine.execute("SELECT n FROM '$path'");
      expect(result.rowCount, 2);
    });
  });

  group('projection guard (relocated)', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('proj_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table sample() => Table(
      name: 't',
      columns: [
        NumericColumn.int32(
          name: 'id',
          values: List.generate(50, (i) => i),
          nullable: false,
        ),
        ListColumn<String>(
          name: 'name',
          dataType: DataType.string,
          data: List.generate(50, (i) => 'item_$i'),
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int32, nullable: false),
          Field(name: 'name', dataType: DataType.string, nullable: false),
        ],
      ),
    );

    test('Dataset projection of an unknown column throws', () async {
      final path = '${dir.path}/data.parquet';
      await ParquetWriter(path).write(sample());
      expect(
        DataFrameIO.readDataset(path, columns: ['id', 'ghost']),
        throwsA(isA<ArgumentError>()),
      );
    });
  });

  // A projected Parquet read opened each file for its schema and then opened and
  // read it again — two footer preads + two thrift parses per file. On a
  // million-file or network-mounted dataset that is the bulk of the per-file
  // metadata cost.
  group('footer parses per file', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('dsfooter_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Schema schemaOf() => Schema(
      fields: [
        Field(name: 'a', dataType: DataType.int64, nullable: false),
        Field(name: 'b', dataType: DataType.int64, nullable: false),
      ],
    );

    Future<void> write(String path, int base) => ParquetWriter(path).write(
      Table(
        name: 't',
        columns: [
          NumericColumn.int64(name: 'a', values: [base], nullable: false),
          NumericColumn.int64(name: 'b', values: [base * 2], nullable: false),
        ],
        schema: schemaOf(),
      ),
    );

    Future<void> writeAll(int n, String prefix) async {
      for (var i = 0; i < n; i++) {
        await write('${dir.path}/$prefix$i.parquet', i);
      }
    }

    test('the eager projected read parses each footer ONCE', () async {
      await writeAll(3, 'f');
      ParquetReader.footersParsed = 0;
      // Both caps null selects the eager path; a projection is what forces the
      // schema peek that used to cost a second parse.
      final t = await Dataset(
        dir.path,
        columns: ['a'],
        maxRows: null,
        maxBytes: null,
      ).read();
      expect(t.rowCount, 3);
      expect(t.schema.fieldNames, ['a']);
      expect(ParquetReader.footersParsed, 3); // was 6
    });

    test('the projection still reaches the reader', () async {
      await write('${dir.path}/g.parquet', 7);
      ParquetReader.columnsRead = 0;
      await Dataset(
        dir.path,
        columns: ['a'],
        maxRows: null,
        maxBytes: null,
      ).read();
      expect(ParquetReader.columnsRead, 1); // 'b' was never decoded
    });

    test('an explicit schema skips the streaming pre-pass entirely', () async {
      await writeAll(3, 'h');
      ParquetReader.footersParsed = 0;
      final batches = await Dataset(
        dir.path,
      ).readBatches(schema: schemaOf()).toList();
      expect(batches.fold<int>(0, (a, b) => a + b.rowCount), 3);
      expect(ParquetReader.footersParsed, 3); // one per file, stream only
    });

    test('without a schema, streaming pays a documented pre-pass', () async {
      await writeAll(3, 'k');
      ParquetReader.footersParsed = 0;
      final batches = await Dataset(dir.path).readBatches().toList();
      expect(batches.fold<int>(0, (a, b) => a + b.rowCount), 3);
      // The pre-pass must visit EVERY file before the first batch (a stream
      // can't retroactively widen what it already emitted), so its scans are not
      // held to share the footer — that would keep a million footers alive.
      // Pass `schema:` (above) to eliminate this pass.
      expect(ParquetReader.footersParsed, 6);
    });
  });

  // A null Hive partition (`part=__HIVE_DEFAULT_PARTITION__`) is a layout THIS
  // library writes (DeltaWriter.encodePartition). Reading it back threw:
  // `_typeOf` typed the null as `string` while a sibling file's value typed as
  // int64, and `_unionFields`' `null_` escape hatch — which exists for exactly
  // this — was unreachable because `_typeOf` could never return `null_`.
  group('null Hive partitions', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('dsnull_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Future<void> write(String part, int v) async {
      final d = '${dir.path}/part=$part';
      await Directory(d).create(recursive: true);
      await ParquetWriter('$d/f.parquet').write(
        Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'v', values: [v], nullable: false),
          ],
          schema: Schema(
            fields: [
              Field(name: 'v', dataType: DataType.int64, nullable: false),
            ],
          ),
        ),
      );
    }

    test('a null partition reads alongside a valued one', () async {
      await write('5', 1);
      await write('__HIVE_DEFAULT_PARTITION__', 2);
      final rows = (await Dataset(dir.path).read()).toRows();
      final byV = {for (final r in rows) r['v']: r['part']};
      expect(byV, {1: 5, 2: null});
    });

    test('order does not matter', () async {
      // The null file first: previously threw either way, but the two paths
      // differ (first-seen-wins seats the field).
      await write('__HIVE_DEFAULT_PARTITION__', 1);
      await write('9', 2);
      final rows = (await Dataset(dir.path).read()).toRows();
      expect({for (final r in rows) r['v']: r['part']}, {1: null, 2: 9});
    });

    test('a TRULY null-first ordering still reads (external review)', () async {
      // File paths are SORTED before schema union, and the null sentinel
      // starts with '_' (0x5F) — so a concrete value like 'zz' sorts AFTER
      // it and the null partition file is processed FIRST. The union used to
      // freeze the field as null_ (never promoting to the later concrete
      // type), and the concrete column then failed validation. The older
      // "order does not matter" test used part=9, which sorts BEFORE the
      // sentinel and never covered this.
      await write('__HIVE_DEFAULT_PARTITION__', 1);
      await write('zz', 2);
      final eager = (await Dataset(dir.path).read()).toRows();
      expect({for (final r in eager) r['v']: r['part']}, {1: null, 2: 'zz'});
      final streamed = await Dataset(dir.path).readBatches().toList();
      expect(
        {for (final r in streamed.expand((b) => b.toRows())) r['v']: r['part']},
        {1: null, 2: 'zz'},
      );
    });

    test('a Hive-escaped partition segment unescapes (audit 7)', () async {
      // Hive/Spark (and this library's Delta writer) escape special characters
      // as %XX in `col=value` segments; taking the text literally returned the
      // escaped form ('50%25' instead of '50%').
      await write('50%25', 1);
      await write('a%2Fb', 2);
      final rows = (await Dataset(dir.path).read()).toRows();
      expect({for (final r in rows) r['v']: r['part']}, {1: '50%', 2: 'a/b'});
    });

    test('an all-null partition column still reads', () async {
      await write('__HIVE_DEFAULT_PARTITION__', 1);
      expect((await Dataset(dir.path).read()).toRows().single['part'], isNull);
    });

    test('partition values of different magnitudes stay compatible', () async {
      // Partition types are derived per FILE, so they must NOT be narrowed by
      // magnitude — 5 as int8 and 300 as int16 would be "incompatible".
      await write('5', 1);
      await write('300', 2);
      final rows = (await Dataset(dir.path).read()).toRows();
      expect({for (final r in rows) r['v']: r['part']}, {1: 5, 2: 300});
    });

    test('the streaming path agrees with the eager one', () async {
      await write('5', 1);
      await write('__HIVE_DEFAULT_PARTITION__', 2);
      final rows = [
        for (final b in await Dataset(dir.path).readBatches().toList())
          ...b.toRows(),
      ];
      expect({for (final r in rows) r['v']: r['part']}, {1: 5, 2: null});
    });
  });
}
