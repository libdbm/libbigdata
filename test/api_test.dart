import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:test/test.dart';

import 'support/results.dart';

/// The public surface, reached through `package:libbigdata/libbigdata.dart`
/// alone: every type a public signature takes is exported, and internals are
/// not.
void main() {
  late Directory dir;
  setUp(() => dir = Directory.systemTemp.createTempSync('api_'));
  tearDown(() => dir.deleteSync(recursive: true));

  Table ids() => Table.fromRows('t', [
    {'id': 1},
    {'id': 2},
  ]);

  test('a Parquet codec is chosen without a src import', () async {
    final path = '${dir.path}/t.parquet';
    await ParquetWriter(
      path,
      options: const ParquetWriterOptions(compression: ParquetCompression.zstd),
    ).write(ids()).ok;
    expect((await ParquetReader(path).read().ok).rowCount, 2);
  });

  test('types in public signatures are exported', () async {
    // A custom Delta coordinator receives a DeltaLog.
    DeltaLog? log;
    expect(log, isNull);
    // The Iceberg backends take a FilesystemCommitCoordinator.
    const FilesystemIcebergBackend(loop: FilesystemCommitCoordinator());
    // RowFilter.keep takes ColumnBounds.
    final filter = RowFilter.compare('id', FilterOp.gt, 5);
    expect(
      filter.keep({
        'id': const ColumnBounds(min: 1, max: 2, rowCount: 2, hasStats: true),
      }),
      isFalse,
    );
    // LocalCatalog.guarded hands its body a CatalogTxn.
    final catalog = LocalCatalog('${dir.path}/catalog.json');
    final version = await catalog.guarded('t', (CatalogTxn txn) async {
      txn.put('t', version: 3);
      return txn.entry('t')!['version'];
    }).ok;
    expect(version, 3);
  });

  test('a DataFrame runs SQL through the exported extension', () async {
    final frame = DataFrame.fromTable(ids());
    final result = await frame.sql('SELECT COUNT(*) AS n FROM df').ok;
    expect((await result.execute().ok).toRows(), [
      {'n': 2},
    ]);
  });

  test(
    'readDatasetBatches yields DataFrames like the other batch readers',
    () async {
      Directory('${dir.path}/d').createSync();
      await ParquetWriter('${dir.path}/d/a.parquet').write(ids()).ok;
      final frames = await DataFrameIO.readDatasetBatches(
        '${dir.path}/d',
      ).toList();
      expect(frames.single.ok, isA<DataFrame>());
    },
  );

  test('internals and deprecated aliases are not exported', () async {
    final probe = File('tool/_api_probe.dart')
      ..writeAsStringSync('''
import 'package:libbigdata/libbigdata.dart';

void main() {
  unifyTables;
  decimalStats;
  parseTemporal;
  distinctStatsCap;
  defaultChunkSize;
  DelimiterDetectionResult;
  DataFrameIO.fromCsv;
  DataFrameIO.readCsvOptimized;
}
''');
    addTearDown(probe.deleteSync);
    final result = await Process.run(Platform.resolvedExecutable, [
      'analyze',
      probe.path,
    ]);
    final out = '${result.stdout}';
    for (final name in [
      'unifyTables',
      'decimalStats',
      'parseTemporal',
      'distinctStatsCap',
      'defaultChunkSize',
      'DelimiterDetectionResult',
      'fromCsv',
      'readCsvOptimized',
    ]) {
      expect(out, contains(name), reason: '$name is still reachable:\n$out');
    }
  });

  test(
    'exported classes cannot be extended or implemented by accident',
    () async {
      final probe = File('tool/_api_modifiers.dart')
        ..writeAsStringSync('''
import 'package:libbigdata/libbigdata.dart';

abstract class MyColumn implements Column<int> {}
abstract class MyFilter implements RowFilter {}
abstract class MyTable implements Table {}
abstract class MyEngine implements SqlEngine {}
abstract class MyReader implements ParquetReader {}
abstract class MyCatalog implements FilesystemIcebergCatalog {}
''');
      addTearDown(probe.deleteSync);
      final result = await Process.run(Platform.resolvedExecutable, [
        'analyze',
        probe.path,
      ]);
      final errors = [
        for (final line in '${result.stdout}'.split('\n'))
          if (line.trimLeft().startsWith('error')) line,
      ];
      // Lines 3 to 8 each implement an exported class (implementing needs no
      // constructor, so an error there is the class modifier's).
      for (var line = 3; line <= 8; line++) {
        expect(
          errors.any((error) => error.contains('_api_modifiers.dart:$line:')),
          isTrue,
          reason: 'line $line is allowed:\n${result.stdout}',
        );
      }
    },
  );

  test('multi-word public names have single-word forms (D4)', () async {
    final engine = SqlEngine.open().ok;
    addTearDown(engine.dispose);
    engine.registerTable('t', ids());
    expect(engine.tables.keys, ['t']);
    expect(engine.schema('t')!.fieldNames, ['id']);
    expect(engine.schema('none'), isNull);

    final file = File('${dir.path}/t.csv');
    final sink = file.openWrite();
    await CsvWriter().writeSink(ids(), sink).ok;
    await sink.close();
    expect(file.readAsStringSync(), 'id\n1\n2');
  });

  group('column internals are not reachable from the package', () {
    Future<String> analyze(String name, String source) async {
      final probe = File('tool/$name.dart')..writeAsStringSync(source);
      addTearDown(probe.deleteSync);
      final result = await Process.run(Platform.resolvedExecutable, [
        'analyze',
        probe.path,
      ]);
      return '${result.stdout}';
    }

    test('the members that expose or adopt buffers do not resolve', () async {
      // One use per line, lines 5 to 16, each of which must be an error.
      final out = await analyze('_api_columns', '''
import 'dart:typed_data';
import 'package:libbigdata/libbigdata.dart';

void probe(NumericColumn<int> n, StringColumn s, BoolColumn b) {
  print(n.rawBuffer);
  print(n.rawNullBitmap);
  print(NumericColumn<int>.adopt(name: 'a', dataType: DataType.int64, data: Int64List(1)));
  print(s.encode());
  print(StringColumn.adopt(name: 's', offsets: Int32List(1), bytes: Uint8List(0)));
  print(b.rawBits);
  print(b.rawNullBitmap);
  print(BoolColumn.adopt(name: 'b', length: 1, bits: Uint8List(1)));
  print(NumericVector);
  print(StringVector);
  print(BoolVector);
  print(ListVector);
}
''');
      final errors = [
        for (final line in out.split('\n'))
          if (line.trimLeft().startsWith('error')) line,
      ];
      for (var line = 5; line <= 16; line++) {
        expect(
          errors.any((error) => error.contains('_api_columns.dart:$line:')),
          isTrue,
          reason: 'line $line resolves:\n$out',
        );
      }
    });

    test(
      'the copying construction and read surface analyzes cleanly',
      () async {
        final out = await analyze('_api_surface', '''
import 'dart:typed_data';
import 'package:libbigdata/libbigdata.dart';

void main() {
  final numbers = NumericColumn.int64(name: 'a', values: [1, null, 3]);
  final floats = NumericColumn.float64(name: 'f', values: [1.5]);
  final small = NumericColumn.int8(name: 'i', values: [1]);
  final text = StringColumn(name: 's', values: ['x', null]);
  final flags = BoolColumn(name: 'b', values: [true, null]);
  final list = ListColumn<int>(
    name: 'l',
    dataType: DataType.int64,
    data: [1, 2],
    nullable: true,
  );
  final Column<Object?> any = numbers;
  print([numbers[0], numbers.length, numbers.hasNulls, numbers.isNullAt(1)]);
  print([numbers.buffer, numbers.nullBitmap, numbers.toList(), numbers.stats]);
  print([numbers.slice(0, 1), numbers.filter([0]), numbers.copy()]);
  print([numbers.withName('b'), numbers.map((v) => v, DataType.int64), numbers.memoryUsage]);
  print([numbers.castTo(DataType.float64), floats, small, any.dataType]);
  print([text.compareAt(0, text, 0), text.interned(), text.gather(Int32List.fromList([0]))]);
  print([flags.gather(Int32List.fromList([0])), flags.isNullAt(1), list.toList()]);
  print(Column.concat([numbers, numbers]));
}
''');
        expect(out, contains('No issues found!'), reason: out);
      },
    );
  });

  group('logging', () {
    tearDown(LoggingConfig.shutdown);

    test('a second initialize applies its settings', () async {
      LoggingConfig.initialize(level: Level.OFF);
      LoggingConfig.initialize(level: Level.FINE);
      expect(Logger.root.level, Level.FINE);
    });

    test('the library logs under the documented names', () async {
      final records = <LogRecord>[];
      final subscription = Logger.root.onRecord.listen(records.add);
      addTearDown(subscription.cancel);
      LoggingConfig.initialize(level: Level.ALL);
      // Permissive JSON skips the element that is not an object and warns.
      await DataFrameIO.fromJsonString(
        '[{"a":1}, 5, {"a":2}]',
        options: const JsonOptions(strict: false),
      ).ok;
      final names = {for (final r in records) r.loggerName};
      expect(names, contains(Loggers.json));
      expect(Loggers.json, startsWith('${Loggers.root}.'));
    });
  });
}
