import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:libbigdata/src/core/conversions.dart';
import 'package:libbigdata/src/core/result.dart';
import 'package:libbigdata/src/formats/avro/avro_decoder.dart';
import 'package:libbigdata/src/formats/avro/avro_ocf_reader.dart';
import 'package:libbigdata/src/formats/avro/avro_ocf_writer.dart';
import 'package:test/test.dart';

import 'support/results.dart';

void main() {
  group('OCF read/write', () {
    const schema = '''
{"type":"record","name":"entry","fields":[
  {"name":"status","type":"int"},
  {"name":"path","type":"string"},
  {"name":"count","type":"long"},
  {"name":"ratio","type":"double"},
  {"name":"opt","type":["null","string"]},
  {"name":"tags","type":{"type":"array","items":"string"}},
  {"name":"props","type":{"type":"map","values":"long"}},
  {"name":"nested","type":{"type":"record","name":"sub","fields":[
    {"name":"x","type":"double"},{"name":"flag","type":"boolean"}]}}
]}''';

    List<Map<String, dynamic>> records() => [
      {
        'status': 1,
        'path': 'a/b.parquet',
        'count': 1000,
        'ratio': 1.5,
        'opt': 'present',
        'tags': ['x', 'y'],
        'props': {'k': 7},
        'nested': {'x': 2.5, 'flag': true},
      },
      {
        'status': 2,
        'path': 'c/d.parquet',
        'count': -42,
        'ratio': -0.25,
        'opt': null,
        'tags': <String>[],
        'props': <String, int>{},
        'nested': {'x': 0.0, 'flag': false},
      },
    ];

    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('avro_'));
    tearDown(() => dir.deleteSync(recursive: true));

    for (final codec in ['null', 'deflate']) {
      test('OCF round-trips records ($codec codec)', () {
        final path = '${dir.path}/f_$codec.avro';
        AvroOcfWriter.writePath(path, schema, records(), codec: codec);
        final back = AvroOcfReader.readPath(path);
        expect(back.length, 2);
        expect(back[0]['path'], 'a/b.parquet');
        expect(back[0]['count'], 1000);
        expect(back[0]['opt'], 'present');
        expect(back[0]['tags'], ['x', 'y']);
        expect(back[0]['props'], {'k': 7});
        expect(back[0]['nested'], {'x': 2.5, 'flag': true});
        expect(back[1]['opt'], isNull);
        expect(back[1]['count'], -42);
        expect(back[1]['tags'], isEmpty);
        expect(back[1]['nested'], {'x': 0.0, 'flag': false});
      });
    }

    test('streaming readPath honors a compressed-input budget (#9)', () {
      final path = '${dir.path}/budget.avro';
      AvroOcfWriter.writePath(path, schema, records());
      // A tiny compressed-input budget aborts before decoding the whole file.
      expect(
        () => AvroOcfReader.readPath(path, maxCompressedBytes: 4),
        throwsFailure(Kind.limit, contains('maxCompressedBytes')),
      );
      // Async path streams the same content correctly with the default budget.
      expect(
        AvroOcfReader.readPathAsync(path).then((r) => r.length),
        completion(2),
      );
    });

    group('OCF integrity', () {
      const schema =
          '{"type":"record","name":"r","fields":[{"name":"v","type":"long"}]}';

      test('a corrupted block sync marker is rejected', () {
        final path = '${dir.path}/c.avro';
        AvroOcfWriter.writePath(path, schema, [
          {'v': 1},
          {'v': 2},
        ], codec: 'null');
        final bytes = File(path).readAsBytesSync();
        // Flip a byte in the trailing 16-byte block sync marker.
        bytes[bytes.length - 1] ^= 0xFF;
        final corrupt = '${dir.path}/c2.avro';
        File(corrupt).writeAsBytesSync(bytes);
        expect(
          () => AvroOcfReader.readPath(corrupt),
          throwsA(isA<FormatException>()),
        );
      });

      test('a truncated file fails loud (FormatException, not RangeError)', () {
        final path = '${dir.path}/t.avro';
        AvroOcfWriter.writePath(path, schema, [
          {'v': 1},
        ], codec: 'null');
        final bytes = File(path).readAsBytesSync();
        // Drop the trailing sync (and some block bytes) so a read overruns.
        final truncated = Uint8List.sublistView(bytes, 0, bytes.length - 8);
        final p = '${dir.path}/t2.avro';
        File(p).writeAsBytesSync(truncated);
        expect(
          () => AvroOcfReader.readPath(p),
          throwsA(isA<FormatException>()),
        );
      });
    });

    test('zigzag long boundaries survive a round-trip', () {
      const schema =
          '{"type":"record","name":"r","fields":[{"name":"v","type":"long"}]}';
      final values = [
        0,
        -1,
        1,
        63,
        64,
        -64,
        2147483647,
        -2147483648,
        1 << 50,
        -(1 << 50),
      ];
      final path = '${dir.path}/longs.avro';
      AvroOcfWriter.writePath(path, schema, [
        for (final v in values) {'v': v},
      ], codec: 'null');
      final back = AvroOcfReader.readPath(path);
      expect(back.map((r) => r['v']).toList(), values);
    });
  });

  group('decoder', () {
    /// the Avro binary decoder must fail loud (FormatException)
    /// on truncated / malformed / hostile input instead of throwing a raw RangeError
    /// (or, on JS, spinning on an unterminated varint).
    AvroReader r(List<int> bytes) => AvroReader(Uint8List.fromList(bytes));

    group('varint guards', () {
      test(
        'a truncated varint (continuation, no end) throws FormatException',
        () {
          expect(() => r([0x80]).readLong(), throwsA(isA<FormatException>()));
        },
      );

      test('an over-long varint (all high-bit) throws FormatException', () {
        expect(
          () => r(List.filled(10, 0x80)).readLong(),
          throwsA(isA<FormatException>()),
        );
      });

      test('an empty buffer throws FormatException, not RangeError', () {
        expect(() => r([]).readLong(), throwsA(isA<FormatException>()));
      });
    });

    group('scalar reader guards', () {
      test('readBoolean on an empty buffer throws FormatException', () {
        expect(() => r([]).readBoolean(), throwsA(isA<FormatException>()));
      });

      test('readFloat on a short buffer throws FormatException', () {
        expect(() => r([1, 2]).readFloat(), throwsA(isA<FormatException>()));
      });

      test('readDouble on a short buffer throws FormatException', () {
        expect(
          () => r([1, 2, 3, 4]).readDouble(),
          throwsA(isA<FormatException>()),
        );
      });
    });

    group('branch-index validation', () {
      test('an out-of-range union index throws FormatException', () {
        final schema = AvroSchema.parse('["int","string"]'); // 2 branches
        // zigzag(5) = 0x0A -> branch index 5, out of [0,1]
        expect(() => schema.decode(r([0x0A])), throwsA(isA<FormatException>()));
      });

      test('an out-of-range enum index throws FormatException', () {
        final schema = AvroSchema.parse(
          '{"type":"enum","name":"e","symbols":["A","B"]}',
        );
        // zigzag(9) = 0x12 -> symbol index 9, out of [0,1]
        expect(() => schema.decode(r([0x12])), throwsA(isA<FormatException>()));
      });

      test('a valid union branch still decodes', () {
        final schema = AvroSchema.parse('["null","int"]');
        // branch 1 (int), zigzag(7)=0x0E
        expect(schema.decode(r([0x02, 0x0E])), 7);
      });
    });
  });

  group('caps', () {
    /// the Avro OCF reader caps total records/bytes (Iceberg manifests
    /// are O(#data files), so an unbounded read can OOM) and opts out on null.
    const schema =
        '{"type":"record","name":"r","fields":[{"name":"a","type":"long"}]}';

    test('reader caps total records; null opts out', () {
      final bytes = AvroOcfWriter.write(schema, [
        for (var i = 0; i < 10; i++) {'a': i},
      ], codec: 'null');
      expect(
        () => AvroOcfReader.read(bytes, maxRecords: 5),
        throwsFailure(Kind.limit, contains('maxRecords=5')),
      );
      expect(AvroOcfReader.read(bytes, maxRecords: null).length, 10);
    });

    // a deflate block that would decompress past the byte budget is
    // rejected DURING inflation (a limit), not after it is fully materialized.
    test('a deflate block over the byte budget throws before full inflation', () {
      const strSchema =
          '{"type":"record","name":"r","fields":[{"name":"s","type":"string"}]}';
      // Highly compressible records → a small compressed block, large decompressed.
      final bytes = AvroOcfWriter.write(strSchema, [
        for (var i = 0; i < 200; i++) {'s': 'x' * 200},
      ], codec: 'deflate');
      expect(
        () => AvroOcfReader.read(bytes, maxBytes: 1024),
        throwsFailure(Kind.limit),
      );
      // Uncapped it reads fine.
      expect(AvroOcfReader.read(bytes, maxBytes: null).length, 200);
    });

    test('a highly compressible deflate OCF from the writer reads back', () {
      const strSchema =
          '{"type":"record","name":"r","fields":[{"name":"s","type":"string"}]}';
      // 2 MB of repeated text deflates far past 200x.
      final bytes = AvroOcfWriter.write(strSchema, [
        for (var i = 0; i < 2000; i++) {'s': 'x' * 1000},
      ], codec: 'deflate');
      expect(AvroOcfReader.read(bytes).length, 2000);
    });
  });

  group('logical types', () {
    /// Avro scalar logical types are decoded (was raw int/bytes).
    /// Fixture authored by fastavro (date / timestamp-micros / decimal-bytes).
    test('decodes date, timestamp, decimal from a fastavro OCF', () {
      final bytes = File(
        'test/fixtures/avro/logical_types.avro',
      ).readAsBytesSync();
      final rows = AvroOcfReader.read(bytes);
      expect(rows, hasLength(1));
      final r = rows.first;

      expect(r['d'], isA<DateTime>());
      expect((r['d'] as DateTime).toUtc(), DateTime.utc(2021, 1, 1));

      expect(r['ts'], isA<DateTime>());
      expect(
        (r['ts'] as DateTime).toUtc(),
        DateTime.utc(2021, 3, 15, 10, 30, 15),
      );

      expect(r['amt'], isA<BigInt>());
      // Unscaled BigInt 12345 + scale 2 = 123.45.
      expect(formatDecimal(r['amt'] as BigInt, 2), '123.45');
    });
  });

  // The decoder converts a logical value to its Dart form (int -> DateTime,
  // bytes -> BigInt); the encoder has to convert it back. It used to test only
  // the PHYSICAL type, so a decoded value matched no branch of its own union and
  // the `index < 0 -> 0` fallback wrote it as null.
  group('logical type encoding', () {
    List<Map<String, dynamic>> roundTrip(
      String schema,
      List<Map<String, dynamic>> records,
    ) => AvroOcfReader.read(AvroOcfWriter.write(schema, records));

    String nullable(String name, String logical) =>
        '{"type":"record","name":"r","fields":[{"name":"$name",'
        '"type":["null",$logical]}]}';

    test('a nullable timestamp-micros round-trips', () {
      const schema =
          '{"type":"record","name":"r","fields":[{"name":"ts","type":'
          '["null",{"type":"long","logicalType":"timestamp-micros"}]}]}';
      final when = DateTime.utc(2024, 1, 1, 12, 30);
      final back = roundTrip(schema, [
        {'ts': when},
      ]);
      expect(back.single['ts'], when);
    });

    test('a nullable timestamp-millis round-trips', () {
      final schema = nullable(
        'ts',
        '{"type":"long","logicalType":"timestamp-millis"}',
      );
      final when = DateTime.utc(2024, 1, 1, 12, 30);
      expect(
        roundTrip(schema, [
          {'ts': when},
        ]).single['ts'],
        when,
      );
    });

    test('a nullable date round-trips', () {
      final schema = nullable('d', '{"type":"int","logicalType":"date"}');
      final day = DateTime.utc(2021, 1, 1);
      expect(
        roundTrip(schema, [
          {'d': day},
        ]).single['d'],
        day,
      );
    });

    test('a nullable bytes-backed decimal round-trips', () {
      final schema = nullable(
        'amount',
        '{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}',
      );
      final unscaled = BigInt.from(12345); // 123.45
      expect(
        roundTrip(schema, [
          {'amount': unscaled},
        ]).single['amount'],
        unscaled,
      );
    });

    test('a nullable logical value is still writable as null', () {
      final schema = nullable('d', '{"type":"int","logicalType":"date"}');
      expect(
        roundTrip(schema, [
          {'d': null},
        ]).single['d'],
        isNull,
      );
    });

    test('a non-nullable logical value round-trips', () {
      const schema =
          '{"type":"record","name":"r","fields":[{"name":"d","type":'
          '{"type":"int","logicalType":"date"}}]}';
      final day = DateTime.utc(2021, 6, 15);
      expect(
        roundTrip(schema, [
          {'d': day},
        ]).single['d'],
        day,
      );
    });

    test('a value matching no union branch throws', () {
      // Was silently written as null (branch 0): the worst possible answer for
      // a value the caller believed it had stored.
      final schema = nullable('n', '"int"');
      expect(
        () => AvroOcfWriter.write(schema, [
          {'n': 3.5},
        ]),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('a plain union still selects by physical type', () {
      const schema =
          '{"type":"record","name":"r","fields":[{"name":"v","type":'
          '["null","int","string"]}]}';
      final back = roundTrip(schema, [
        {'v': 7},
        {'v': 'x'},
        {'v': null},
      ]);
      expect(back.map((r) => r['v']).toList(), [7, 'x', null]);
    });
  });

  group('#4 Avro collection cardinality', () {
    // Minimal Avro long (zigzag varint) encoder.
    Uint8List avroLong(int n) {
      var v = (n << 1) ^ (n >> 63); // zigzag
      final out = <int>[];
      while (true) {
        if ((v & ~0x7f) == 0) {
          out.add(v & 0x7f);
          break;
        }
        out.add((v & 0x7f) | 0x80);
        v = v >>> 7;
      }
      return Uint8List.fromList(out);
    }

    test('an array declaring a huge null count throws before expansion', () {
      final schema = AvroSchema.parse('{"type":"array","items":"null"}');
      // A single block declaring 20M null items (0 payload bytes each) must be
      // rejected against the value budget (10M) before the loop runs.
      final reader = AvroReader(avroLong(20000000));
      expect(() => schema.decode(reader), throwsA(isA<FormatException>()));
    });

    test('the value budget spans the file, not each record', () {
      const schema =
          '{"type":"record","name":"r","fields":['
          '{"name":"a","type":{"type":"array","items":"null"}}]}';
      // Two records of 6M nulls each: each fits a per-record budget of 10M,
      // together they exceed the file's.
      final record = [...avroLong(6000000), ...avroLong(0)];
      final block = [...record, ...record];
      final sync = List.filled(16, 7);
      final bytes = Uint8List.fromList([
        0x4F, 0x62, 0x6A, 0x01, // magic
        ...avroLong(1),
        ...avroLong(11), ...'avro.schema'.codeUnits,
        ...avroLong(schema.length), ...schema.codeUnits,
        ...avroLong(0),
        ...sync,
        ...avroLong(2), ...avroLong(block.length), ...block, ...sync,
      ]);
      expect(bytes.length, lessThan(250));
      expect(() => AvroOcfReader.read(bytes), throwsA(isA<FormatException>()));
    });

    group('schema nesting', () {
      String nested(int depth) =>
          '${'{"type":"array","items":' * depth}"null"${'}' * depth}';

      test('a schema nested past the depth limit throws', () {
        expect(
          () => AvroSchema.parse(nested(200)),
          throwsA(isA<FormatException>()),
        );
      });

      test('a very deeply nested schema throws, not stack-overflow', () {
        expect(
          () => AvroSchema.parse(nested(100000)),
          throwsA(isA<FormatException>()),
        );
      });

      test('a schema within the limit parses', () {
        expect(AvroSchema.parse(nested(50)).root, isA<AvroArray>());
      });
    });

    group('recursive schema depth (finding 5)', () {
      // A self-referential record: the shape of optional nested / Iceberg
      // manifest schemas. The depth guard used to cover only array/map blocks,
      // so record→union→ref recursion here would stack-overflow the isolate.
      const linkedList =
          '{"type":"record","name":"Node","fields":['
          '{"name":"next","type":["null","Node"]}]}';

      // Each "next another node" costs one union index (0x02); a final 0x00
      // selects null and terminates. Each hop adds ~3 decode frames, so ~40 hops
      // blows past _maxDepth (100).
      test('a deeply self-referential value throws, not stack-overflow', () {
        final schema = AvroSchema.parse(linkedList);
        final bytes = Uint8List.fromList([...List.filled(40, 0x02), 0x00]);
        expect(
          () => schema.decode(AvroReader(bytes)),
          throwsA(isA<FormatException>()),
        );
      });

      test('a shallow self-referential value still decodes', () {
        final schema = AvroSchema.parse(linkedList);
        // 3 nodes then null: {next:{next:{next:{next:null}}}}
        final decoded = schema.decode(
          AvroReader(Uint8List.fromList([0x02, 0x02, 0x02, 0x00])),
        );
        expect(decoded, isA<Map>());
      });
    });
  });

  group('named types and codecs', () {
    test('named types resolve by full name across namespaces', () {
      const schema = '''
{"type": "record", "name": "Outer", "namespace": "a", "fields": [
  {"name": "x", "type": {"type": "record", "name": "Item",
    "fields": [{"name": "v", "type": "int"}]}},
  {"name": "y", "type": {"type": "record", "name": "Item", "namespace": "b",
    "fields": [{"name": "s", "type": "string"}]}},
  {"name": "z", "type": "Item"},
  {"name": "w", "type": "b.Item"},
  {"name": "e", "type": {"type": "enum", "name": "Color", "namespace": "c",
    "symbols": ["RED"]}},
  {"name": "f", "type": "c.Color"}
]}''';
      final record = {
        'x': {'v': 1},
        'y': {'s': 'p'},
        'z': {'v': 2},
        'w': {'s': 'q'},
        'e': 'RED',
        'f': 'RED',
      };
      final bytes = AvroOcfWriter.write(schema, [record], codec: 'null');
      expect(AvroOcfReader.read(bytes), [record]);
    });

    test('a zstandard OCF reads', () {
      final rows = AvroOcfReader.read(
        File('test/fixtures/avro/zstandard.avro').readAsBytesSync(),
      );
      expect(rows, [
        {'id': 0, 's': 'v0'},
        {'id': 1, 's': 'v1'},
        {'id': 2, 's': 'v2'},
      ]);
    });

    test('an unknown codec is unsupported', () {
      final bytes = AvroOcfWriter.write(
        '{"type":"record","name":"r","fields":[{"name":"a","type":"long"}]}',
        [
          {'a': 1},
        ],
        codec: 'null',
      );
      // Rename the codec in the header from `null` to the unknown `xzzz`,
      // which keeps every length intact.
      final text = latin1.decode(bytes);
      final patched = latin1.encode(
        text.replaceFirst('avro.codec\u0008null', 'avro.codec\u0008xzzz'),
      );
      expect(
        () => AvroOcfReader.read(patched),
        throwsFailure(Kind.unsupported, contains('xzzz')),
      );
    });
  });
}
