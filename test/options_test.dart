import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:test/test.dart';

import 'support/results.dart';

/// One option vocabulary across the readers and writers (decision D2):
/// format options and caps are named, batches are `batchSize`, projection is
/// `columns`, a filter is a [RowFilter], and time travel is `asOf`.
void main() {
  late Directory dir;
  setUp(() => dir = Directory.systemTemp.createTempSync('options_'));
  tearDown(() => dir.deleteSync(recursive: true));

  Table rows() => Table.fromRows('t', [
    {'id': 1, 'city': 'oslo', 'n': 10},
    {'id': 2, 'city': 'rome', 'n': 20},
    {'id': 3, 'city': 'oslo', 'n': 30},
  ]);

  String csv() {
    final path = '${dir.path}/t.csv';
    File(
      path,
    ).writeAsStringSync('id,city,n\n1,oslo,10\n2,rome,20\n3,oslo,30\n');
    return path;
  }

  Future<List<Map<String, dynamic>>> collect(
    Stream<Result<DataFrame, ErrorDetails>> stream,
  ) async => [
    for (final frame in await stream.ok.toList())
      ...(await frame.execute().ok).toRows(),
  ];

  group('CSV takes a RowFilter, applied exactly to each row', () {
    final oslo = RowFilter.compare('city', FilterOp.eq, 'oslo');
    final both = RowFilter.and([oslo, RowFilter.compare('n', FilterOp.gt, 15)]);

    test('readCsvBatches', () async {
      final read = await collect(
        DataFrameIO.readCsvBatches(
          csv(),
          batchSize: 1,
          columns: ['id'],
          filter: both,
        ),
      );
      expect(read, [
        {'id': 3},
      ]);
    });

    test('readCsv', () async {
      final frame = await DataFrameIO.readCsv(
        csv(),
        filter: RowFilter.or([both, RowFilter.compare('id', FilterOp.eq, 2)]),
        columns: ['id'],
        batchSize: 2,
      ).ok;
      expect((await frame.execute().ok).toRows(), [
        {'id': 2},
        {'id': 3},
      ]);
    });

    test('CsvReader.readFile and readString', () async {
      final file = await CsvReader().readFile(csv(), filter: oslo).ok;
      expect(file.column('id')!.toList(), [1, 3]);
      final text = await CsvReader()
          .readString(
            'id,n\n1,5\n2,\n',
            filter: RowFilter.compare('n', FilterOp.ne, 5),
          )
          .ok;
      // A null never matches a comparison.
      expect(text.rowCount, 0);
    });

    test('an unknown filter column is invalid', () async {
      await expectLater(
        CsvReader().readFile(
          csv(),
          filter: RowFilter.compare('nope', FilterOp.eq, 1),
        ),
        failsWith(Kind.invalid, contains('nope')),
      );
    });
  });

  test('format options and caps are named everywhere', () async {
    final path = csv();
    final options = const CsvOptions(hasHeader: true);
    expect(
      (await (await DataFrameIO.readCsv(
        path,
        options: options,
      ).ok).execute().ok).rowCount,
      3,
    );
    final text = await DataFrameIO.fromCsvString('a\n1\n', options: options).ok;
    expect(await text.toCsv(options: options, maxRows: 10).ok, contains('a'));
    await expectLater(
      text.toCsv(options: options, maxRows: 0),
      failsWith(Kind.limit),
    );
    await (await DataFrameIO.fromCsvString(
      'a\n1\n',
      options: options,
    ).ok).writeCsv('${dir.path}/out.csv', options: options).ok;
    final json = DataFrame.fromTable(rows());
    await json.writeJson('${dir.path}/t.json', options: const JsonOptions()).ok;
    await json.writeJsonLines('${dir.path}/t.jsonl').ok;
    expect(await json.toJson(maxRows: 10).ok, contains('oslo'));
    expect(await json.toJsonLines(maxRows: 10).ok, contains('rome'));
    await DataFrameIO.readJson(
      '${dir.path}/t.json',
      options: const JsonOptions(),
    ).ok;
  });

  test('every reader takes the caps', () async {
    // A lazy reader's caps bound the frame's result, so its terminal fails;
    // an eager one fails at the call.
    Future<Result<Object?, ErrorDetails>> outcome(
      Future<Result<DataFrame, ErrorDetails>> read,
    ) async => switch (await read) {
      Ok(:final value) => await value.execute(),
      Err(:final error) => Err(error),
    };
    final frame = DataFrame.fromTable(rows());
    await frame.writeJson('${dir.path}/t.json').ok;
    await frame.writeJsonLines('${dir.path}/t.jsonl').ok;
    await frame.writeArrow('${dir.path}/t.arrow').ok;
    await frame.writeArrowStream('${dir.path}/s.arrow').ok;
    for (final read in <Future<Result<DataFrame, ErrorDetails>> Function()>[
      () => DataFrameIO.readJson('${dir.path}/t.json', maxRows: 1),
      () => DataFrameIO.readJsonLines('${dir.path}/t.jsonl', maxRows: 1),
      () => DataFrameIO.readArrow('${dir.path}/t.arrow', maxRows: 1),
      () => DataFrameIO.readArrowStream('${dir.path}/s.arrow', maxRows: 1),
      () => DataFrameIO.fromCsvString('a\n1\n2\n', maxRows: 1),
      () => DataFrameIO.fromJsonString('[{"a":1},{"a":2}]', maxRows: 1),
      () => DataFrameIO.fromJsonLinesString('{"a":1}\n{"a":2}\n', maxRows: 1),
    ]) {
      await expectLater(outcome(read()), failsWith(Kind.limit));
    }
  });

  test('Delta time travel is asOf, as for Iceberg', () async {
    final path = '${dir.path}/delta';
    await DeltaWriter(path).write(rows()).ok;
    final later = DateTime.now().millisecondsSinceEpoch + 60000;
    final frame = await DataFrameIO.readDelta(path, asOf: later).ok;
    expect((await frame.execute().ok).rowCount, 3);
    expect((await DeltaTable(path).read(asOf: later).ok).rowCount, 3);
    expect((await DeltaTable(path).schema(asOf: later).ok).fieldNames, [
      'id',
      'city',
      'n',
    ]);
  });

  group('batchSize everywhere', () {
    Future<List<int>> sizes(Stream<Result<Table, ErrorDetails>> stream) async =>
        [for (final t in await stream.ok.toList()) t.rowCount];

    test('every batch reader honours it', () async {
      await ParquetWriter('${dir.path}/t.parquet').write(rows()).ok;
      await DeltaWriter('${dir.path}/delta').write(rows()).ok;
      await IcebergWriter('${dir.path}/iceberg').write(rows()).ok;
      final frame = DataFrame.fromTable(rows());
      await frame.writeJsonLines('${dir.path}/t.jsonl').ok;
      Directory('${dir.path}/set').createSync();
      await ParquetWriter('${dir.path}/set/a.parquet').write(rows()).ok;
      expect(await sizes(CsvReader().readBatches(csv(), batchSize: 2)), [2, 1]);
      expect(
        await sizes(
          JsonReader(
            JsonOptions.jsonLines(),
          ).readBatches('${dir.path}/t.jsonl', batchSize: 2),
        ),
        [2, 1],
      );
      expect(
        await sizes(
          ParquetReader('${dir.path}/t.parquet').readBatches(batchSize: 2),
        ),
        [2, 1],
      );
      expect(
        await sizes(Dataset('${dir.path}/set').readBatches(batchSize: 2)),
        [2, 1],
      );
      expect(
        await sizes(DeltaTable('${dir.path}/delta').readBatches(batchSize: 2)),
        [2, 1],
      );
      expect(
        await sizes(
          IcebergTable('${dir.path}/iceberg').readBatches(batchSize: 2),
        ),
        [2, 1],
      );
      final frames = await DataFrameIO.readDatasetBatches(
        '${dir.path}/set',
        batchSize: 2,
      ).ok.toList();
      expect(frames, hasLength(2));
      final json = await DataFrameIO.readJsonBatches(
        '${dir.path}/t.jsonl',
        batchSize: 2,
      ).ok.toList();
      expect(json, hasLength(2));
    });

    test('a batchSize below 1 throws at the call, not in the stream', () {
      expect(
        () => CsvReader().readBatches(csv(), batchSize: 0),
        throwsArgumentError,
      );
      expect(
        () => JsonReader().readBatches('${dir.path}/x.jsonl', batchSize: 0),
        throwsArgumentError,
      );
      expect(
        () => ParquetReader('${dir.path}/x.parquet').readBatches(batchSize: 0),
        throwsArgumentError,
      );
      expect(
        () => Dataset(dir.path).readBatches(batchSize: 0),
        throwsArgumentError,
      );
      expect(
        () => DeltaTable(dir.path).readBatches(batchSize: 0),
        throwsArgumentError,
      );
      expect(
        () => IcebergTable(dir.path).readBatches(batchSize: 0),
        throwsArgumentError,
      );
    });
  });
}
