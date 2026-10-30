import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/core/failure.dart';
import 'package:libbigdata/src/formats/arrow/arrow_reader.dart';
import 'package:libbigdata/src/formats/arrow/arrow_writer.dart';
import 'package:libbigdata/src/formats/avro/avro_ocf_reader.dart';
import 'package:libbigdata/src/formats/avro/avro_ocf_writer.dart';
import 'package:test/test.dart';

import 'support/results.dart';

/// Decoders face untrusted files. Every corrupt input must fail with a domain
/// error, never a raw RangeError, TypeError, StackOverflowError or null check,
/// and never with a runaway allocation or decode time.
///
/// The hostile fixtures run with the normal suite. The mutation runs are tagged
/// `fuzz`: dart test -t fuzz --run-skipped
void main() {
  group('hostile fixtures', () {
    const dir = 'test/fixtures/hostile';
    // Each file is one input that once escaped as a raw error, with the
    // message of the check that now rejects it.
    const expected = {
      'parquet_bad_repetition_enum.parquet':
          'Invalid FieldRepetitionType value',
      // An unknown converted type is ignored now; the file's malformed root
      // element is what rejects it.
      'parquet_bad_converted_enum.parquet': 'Invalid root schema element',
      'parquet_bad_page_type.parquet': 'Invalid PageType value',
      'parquet_page_without_header.parquet': 'Data page without its header',
      'parquet_dictionary_without_header.parquet':
          'Dictionary page without its header',
      'parquet_chunk_without_metadata.parquet':
          'Parquet column chunk without metadata',
      'parquet_leaf_without_chunk.parquet': 'has no column chunk',
      'parquet_utf8_on_boolean.parquet': 'a UTF8 annotation on',
      'parquet_nested_short_leaf.parquet': 'holds 3 values, metadata says 2',
      'avro_bad_schema_shape.avro': 'Avro schema:',
      'avro_value_budget_across_records.avro': 'value budget',
      'avro_deep_schema.avro': 'nesting exceeds depth',
      'arrow_bad_int_width.arrow': 'Arrow int bitWidth',
      'arrow_bad_type_code.arrow': 'Invalid Arrow type code',
      'arrow_bad_float_precision.arrow': 'Arrow float precision',
    };

    test('every fixture has an expectation', () {
      final files = Directory(
        dir,
      ).listSync().map((f) => f.uri.pathSegments.last).toSet();
      expect(files, expected.keys.toSet());
    });

    for (final MapEntry(key: name, value: message) in expected.entries) {
      test('$name is rejected', () async {
        // Avro and Arrow raise FormatException; Parquet reports its Err,
        // which _decode rethrows as the Failure.
        await expectLater(
          _decode('$dir/$name'),
          throwsA(
            anyOf(
              isA<FormatException>().having(
                (error) => error.message,
                'message',
                contains(message),
              ),
              isA<Failure>()
                  .having((error) => error.details.kind, 'kind', Kind.corrupt)
                  .having(
                    (error) => error.details.message,
                    'message',
                    contains(message),
                  ),
            ),
          ),
        );
      });
    }
  });

  group('mutation', () {
    late Directory dir;
    late Map<String, List<Uint8List>> seeds;

    setUpAll(() async {
      dir = Directory.systemTemp.createTempSync('fuzz_');
      seeds = await _seeds(dir);
    });
    tearDownAll(() => dir.deleteSync(recursive: true));

    for (final format in ['parquet', 'avro', 'arrow']) {
      test(
        '10,000 mutated $format files fail only with domain errors',
        () async {
          final random = Random(42);
          final target = '${dir.path}/mutated.$format';
          final watch = Stopwatch();
          for (var run = 0; run < 10000; run++) {
            final group = seeds[format]!;
            final input = _mutate(group[run % group.length], random);
            File(target).writeAsBytesSync(input);
            watch
              ..reset()
              ..start();
            try {
              await _decode(target);
            } on FormatException {
              // corrupt
            } on ArgumentError {
              // invalid
            } on UnsupportedError {
              // a mutated type code can name a real but unsupported type
            } on Failure catch (failure) {
              // the library's own report of corrupt, unsupported, over-cap
              // or invalid input; any other kind is unexpected here
              const accepted = {
                Kind.corrupt,
                Kind.unsupported,
                Kind.limit,
                Kind.invalid,
              };
              if (!accepted.contains(failure.details.kind)) {
                _keep(input, format, run);
                rethrow;
              }
            } on StateError catch (error) {
              // a materialization cap is a limit, any other state is a bug
              if (!error.message.contains('exceeds max')) {
                _keep(input, format, run);
                rethrow;
              }
            } catch (error) {
              _keep(input, format, run);
              fail('run $run: ${error.runtimeType}: $error');
            }
            expect(
              watch.elapsedMilliseconds,
              lessThan(2000),
              reason: 'run $run decoded too slowly',
            );
            expect(
              ProcessInfo.currentRss,
              lessThan(2 << 30),
              reason: 'run $run grew the heap past 2 GiB',
            );
          }
        },
        tags: 'fuzz',
        timeout: const Timeout(Duration(minutes: 10)),
      );
    }
  });
}

/// Decode the file at [path] with the reader its extension names.
Future<Object?> _decode(String path) async {
  if (path.endsWith('.parquet')) {
    return switch (await ParquetReader(path).read()) {
      Ok(:final value) => value,
      // An Error the reader reported is still a decoder bug: surface it, so
      // the harness fails on it as it does for the other readers.
      Err(:final error) when error.cause is Error => throw error.cause!,
      // Reported the way the other readers raise it, for one catch above.
      Err(:final error) => throw Failure(error),
    };
  }
  final bytes = File(path).readAsBytesSync();
  if (path.endsWith('.avro')) return AvroOcfReader.read(bytes);
  return ArrowReader.decode(bytes);
}

/// Save a failing input so it can become a hostile fixture.
void _keep(Uint8List input, String format, int run) {
  final path = '${Directory.systemTemp.path}/fuzz_failure_$run.$format';
  File(path).writeAsBytesSync(input);
  printOnFailure('failing input saved to $path');
}

/// One mutation of [seed]: bit flips, an extreme byte, a truncation, or a
/// 32-bit length field overwritten with an extreme value.
Uint8List _mutate(Uint8List seed, Random random) {
  final out = Uint8List.fromList(seed);
  switch (random.nextInt(4)) {
    case 0:
      for (var k = 0, n = 1 + random.nextInt(8); k < n; k++) {
        out[random.nextInt(out.length)] ^= 1 << random.nextInt(8);
      }
      return out;
    case 1:
      out[random.nextInt(out.length)] = const [
        0,
        0x7F,
        0x80,
        0xFF,
      ][random.nextInt(4)];
      return out;
    case 2:
      return Uint8List.sublistView(out, 0, random.nextInt(out.length));
    default:
      const extremes = [0x7FFFFFFF, 0xFFFFFFFF, 0x80000000, 0, 1, 0x10000];
      final at = random.nextInt(out.length - 3);
      ByteData.sublistView(
        out,
      ).setUint32(at, extremes[random.nextInt(extremes.length)], Endian.little);
      return out;
  }
}

/// Valid files per format: Apache parquet-testing files covering the
/// encodings, plus this library's own writer output for each format.
Future<Map<String, List<Uint8List>>> _seeds(Directory dir) async {
  const names = [
    'rle_boolean_encoding',
    'datapage_v2.snappy',
    'nested_lists.snappy',
    'alltypes_dictionary',
    'delta_length_byte_array',
    'nullable.impala',
    'delta_encoding_optional_column',
    'byte_stream_split.zstd',
  ];
  final parquet = [
    for (final name in names)
      File(
        'test/fixtures/parquet-testing/data/$name.parquet',
      ).readAsBytesSync(),
  ];
  final table = _table(300);
  final own = '${dir.path}/own.parquet';
  await ParquetWriter(own).write(table).ok;
  parquet.add(File(own).readAsBytesSync());

  const schema =
      '{"type":"record","name":"r","fields":['
      '{"name":"a","type":"long"},'
      '{"name":"s","type":["null","string"]},'
      '{"name":"xs","type":{"type":"array","items":"int"}},'
      '{"name":"m","type":{"type":"map","values":"double"}}]}';
  final records = [
    for (var i = 0; i < 50; i++)
      {
        'a': i,
        's': i.isEven ? null : 's$i',
        'xs': [i, i + 1],
        'm': {'k$i': i / 3},
      },
  ];
  final avro = [
    File('test/fixtures/avro/logical_types.avro').readAsBytesSync(),
    AvroOcfWriter.write(schema, records, codec: 'null'),
    AvroOcfWriter.write(schema, records, codec: 'deflate'),
  ];

  final arrow = <Uint8List>[];
  for (final compression in ArrowCompression.values) {
    final path = '${dir.path}/own_${compression.name}.arrow';
    await ArrowWriter(path, compression: compression).write(table);
    arrow.add(File(path).readAsBytesSync());
  }
  return {'parquet': parquet, 'avro': avro, 'arrow': arrow};
}

Table _table(int rows) => Table(
  name: 't',
  schema: Schema(
    fields: [
      Field(name: 'i', dataType: DataType.int64, nullable: true),
      Field(name: 's', dataType: DataType.string, nullable: true),
      Field(name: 'd', dataType: DataType.float64, nullable: true),
      Field(name: 'b', dataType: DataType.boolean, nullable: true),
    ],
  ),
  columns: [
    ListColumn<int>(
      name: 'i',
      dataType: DataType.int64,
      data: [for (var i = 0; i < rows; i++) i % 7 == 0 ? null : i * 1000003],
      nullable: true,
    ),
    ListColumn<String>(
      name: 's',
      dataType: DataType.string,
      data: [for (var i = 0; i < rows; i++) i % 5 == 0 ? null : 'v${i % 13}'],
      nullable: true,
    ),
    ListColumn<double>(
      name: 'd',
      dataType: DataType.float64,
      data: [for (var i = 0; i < rows; i++) i % 3 == 0 ? null : i / 7],
      nullable: true,
    ),
    ListColumn<bool>(
      name: 'b',
      dataType: DataType.boolean,
      data: [for (var i = 0; i < rows; i++) i % 4 == 0 ? null : i.isEven],
      nullable: true,
    ),
  ],
);
