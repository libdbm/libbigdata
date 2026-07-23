import 'dart:io';
import 'dart:convert';
import 'package:test/test.dart';
import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/formats/scan.dart';

void main() {
  group('decimal JSON encoding is an exact string (pinned tradeoff)', () {
    test(
      'a decimal value is written as a quoted string, not a lossy number',
      () async {
        final t = Table(
          name: 't',
          columns: [
            ListColumn<BigInt>(
              name: 'price',
              dataType: DataType.decimal,
              data: [BigInt.from(150)], // 1.50 at scale 2
              nullable: false,
            ),
          ],
          schema: Schema(
            fields: [
              Field(
                name: 'price',
                dataType: DataType.decimal,
                nullable: false,
                precision: 10,
                scale: 2,
              ),
            ],
          ),
        );
        final json = JsonWriter().writeString(t);
        // Exact string form (a JSON number would round through IEEE-754 double).
        expect(json, contains('"price":"1.50"'));
      },
    );
  });

  group('cross-record type conflict (inferred schema is not strict)', () {
    test(
      'a field mixing int and string resolves to string, values coerced',
      () async {
        final t = await JsonReader().readString('[{"x": 1}, {"x": "a"}]');
        expect(t.schema.field('x')!.dataType, DataType.string);
        expect(t.toRows().map((r) => r['x']).toList(), ['1', 'a']);
        expect(t.toRows().every((r) => r['x'] is String), isTrue);
      },
    );

    test('bool then int, reversed key order, also coerces to string', () async {
      final t = await JsonReader().readString('[{"y": true}, {"y": 2}]');
      expect(t.schema.field('y')!.dataType, DataType.string);
      expect(t.toRows().map((r) => r['y']).toList(), ['true', '2']);
    });

    test('inferTypes:false renders every scalar as its string form', () async {
      final t = await JsonReader(
        const JsonOptions(inferTypes: false),
      ).readString('[{"x": 1}, {"x": true}]');
      expect(t.toRows().map((r) => r['x']).toList(), ['1', 'true']);
    });

    test(
      'JSON Lines with a cross-record conflict coerces, does not throw',
      () async {
        final t = await JsonReader(
          const JsonOptions(jsonLines: true),
        ).readString('{"x": 1}\n{"x": "a"}');
        expect(t.schema.field('x')!.dataType, DataType.string);
        expect(t.toRows().map((r) => r['x']).toList(), ['1', 'a']);
      },
    );

    test('streaming readBatches coerces a conflict spanning batches', () async {
      final dir = Directory.systemTemp.createTempSync('json_conflict_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final file = File('${dir.path}/c.jsonl')
        ..writeAsStringSync('{"x": 1}\n{"x": "a"}\n{"x": 3}');
      final rows = <Object?>[];
      await for (final batch in JsonReader(
        const JsonOptions(jsonLines: true),
      ).readBatches(file.path, size: 2)) {
        rows.addAll(batch.toRows().map((r) => r['x']));
      }
      expect(rows, ['1', 'a', '3']);
    });
  });

  group('array & core', () {
    group('JSON Reading Tests', () {
      late Directory tempDir;

      setUp(() {
        tempDir = Directory.systemTemp.createTempSync('json_test_');
      });

      tearDown(() {
        if (tempDir.existsSync()) {
          tempDir.deleteSync(recursive: true);
        }
      });

      test('reads basic JSON array file', () async {
        final jsonFile = File('${tempDir.path}/basic.json');
        jsonFile.writeAsStringSync('''[
          {"id": 1, "name": "Alice", "age": 30},
          {"id": 2, "name": "Bob", "age": 25},
          {"id": 3, "name": "Charlie", "age": 35}
        ]''');

        final df = await DataFrameIO.readJson(jsonFile.path);
        final table = await df.execute();

        expect(table.rowCount, equals(3));
        expect(table.columnCount, equals(3));
        expect(table.schema.fieldNames, equals(['id', 'name', 'age']));

        final rows = table.toRows();
        expect(rows[0], equals({'id': 1, 'name': 'Alice', 'age': 30}));
        expect(rows[1], equals({'id': 2, 'name': 'Bob', 'age': 25}));
        expect(rows[2], equals({'id': 3, 'name': 'Charlie', 'age': 35}));
      });

      test('reads JSON Lines format', () async {
        final jsonlFile = File('${tempDir.path}/lines.jsonl');
        jsonlFile.writeAsStringSync(
          '''{"id": 1, "product": "Widget A", "price": 10.99}
{"id": 2, "product": "Widget B", "price": 25.50}
{"id": 3, "product": "Widget C", "price": 15.00}''',
        );

        final df = await DataFrameIO.readJson(jsonlFile.path);
        final table = await df.execute();

        expect(table.rowCount, equals(3));
        final rows = table.toRows();
        expect(rows[0]['product'], equals('Widget A'));
        expect(rows[1]['price'], equals(25.50));
        expect(rows[2]['id'], equals(3));
      });

      // Flattening joins nested keys with '.', so a literal key that already
      // contains '.' can land on the same name as a nested path. The result
      // used to depend on JSON key ORDER, silently discarding one of the two
      // values — valid input, no error, data gone.
      group('flatten name collisions', () {
        Future<Table> read(String body) async {
          final file = File('${tempDir.path}/collide.json');
          file.writeAsStringSync(body);
          return JsonReader().readFile(file.path);
        }

        test(
          'a literal dotted key colliding with a nested path throws',
          () async {
            await expectLater(
              read('[{"a.b": 1, "a": {"b": 2}}]'),
              throwsA(isA<FormatException>()),
            );
          },
        );

        test('the reverse key order throws too (not order-dependent)', () async {
          // Previously this kept 1 while the other order kept 2 — the surviving
          // value depended purely on source key order.
          await expectLater(
            read('[{"a": {"b": 2}, "a.b": 1}]'),
            throwsA(isA<FormatException>()),
          );
        });

        test('the error names the colliding field', () async {
          await expectLater(
            read('[{"a.b": 1, "a": {"b": 2}}]'),
            throwsA(
              isA<FormatException>().having(
                (e) => e.message,
                'message',
                contains('a.b'),
              ),
            ),
          );
        });

        test('two nested paths colliding on one name throw', () async {
          await expectLater(
            read('[{"a": {"b": {"c": 1}}, "a.b": {"c": 2}}]'),
            throwsA(isA<FormatException>()),
          );
        });

        test('flattenNested: false keeps both fields intact', () async {
          final file = File('${tempDir.path}/nocollide.json');
          file.writeAsStringSync('[{"a.b": 1, "a": {"b": 2}}]');
          final t = await JsonReader(
            const JsonOptions(flattenNested: false),
          ).readFile(file.path);
          final row = t.toRows().single;
          expect(row['a.b'], 1);
          expect(row['a'], {'b': 2});
        });

        test('distinct flattened names are unaffected', () async {
          final t = await read('[{"a": {"b": 1, "c": 2}, "d": 3}]');
          final row = t.toRows().single;
          expect(row['a.b'], 1);
          expect(row['a.c'], 2);
          expect(row['d'], 3);
        });
      });

      test('handles nested JSON objects with flattening', () async {
        final jsonFile = File('${tempDir.path}/nested.json');
        jsonFile.writeAsStringSync('''[
          {
            "id": 1,
            "user": {
              "name": "Alice",
              "email": "alice@example.com"
            },
            "scores": [90, 85, 92]
          },
          {
            "id": 2,
            "user": {
              "name": "Bob",
              "email": "bob@example.com"
            },
            "scores": [78, 82, 88]
          }
        ]''');

        // Default behavior flattens nested objects
        final df = await DataFrameIO.readJson(jsonFile.path);
        final table = await df.execute();

        expect(table.rowCount, equals(2));
        final rows = table.toRows();

        // With flattening enabled (default), nested objects become flattened fields
        expect(rows[0]['user.name'], equals('Alice'));
        expect(rows[0]['user.email'], equals('alice@example.com'));
        expect(rows[1]['user.name'], equals('Bob'));

        // Arrays are preserved
        expect(rows[0]['scores'], isA<List>());
        expect((rows[0]['scores'] as List), equals([90, 85, 92]));
      });

      test('preserves nested JSON objects without flattening', () async {
        final jsonFile = File('${tempDir.path}/nested_preserved.json');
        jsonFile.writeAsStringSync('''[
          {
            "id": 1,
            "user": {
              "name": "Alice",
              "email": "alice@example.com"
            },
            "metadata": {"level": 5}
          }
        ]''');

        // Disable flattening to preserve nested structure
        final df = await DataFrameIO.readJson(
          jsonFile.path,
          const JsonOptions(flattenNested: false),
        );
        final table = await df.execute();

        final rows = table.toRows();

        // Nested objects should be preserved as Maps
        expect(rows[0]['user'], isA<Map>());
        expect((rows[0]['user'] as Map)['name'], equals('Alice'));
        expect(rows[0]['metadata'], isA<Map>());
        expect((rows[0]['metadata'] as Map)['level'], equals(5));
      });

      test('handles null values correctly', () async {
        final jsonFile = File('${tempDir.path}/nulls.json');
        jsonFile.writeAsStringSync('''[
          {"id": 1, "name": "Alice", "value": 100},
          {"id": 2, "name": null, "value": 200},
          {"id": 3, "name": "Charlie", "value": null},
          {"id": 4, "name": "David"}
        ]''');

        final df = await DataFrameIO.readJson(jsonFile.path);
        final table = await df.execute();

        final rows = table.toRows();
        expect(rows[1]['name'], isNull);
        expect(rows[2]['value'], isNull);
        expect(rows[3].containsKey('value'), isTrue);
        expect(rows[3]['value'], isNull);
      });

      test('preserves data types from JSON', () async {
        final jsonFile = File('${tempDir.path}/types.json');
        jsonFile.writeAsStringSync('''[
          {
            "int": 42,
            "float": 3.14,
            "bool": true,
            "string": "hello",
            "null": null,
            "array": [1, 2, 3],
            "object": {"key": "value"}
          },
          {
            "int": -100,
            "float": 2.718,
            "bool": false,
            "string": "world",
            "null": null,
            "array": [4, 5, 6],
            "object": {"key": "another"}
          }
        ]''');

        // Use flattenNested: false to preserve objects
        final df = await DataFrameIO.readJson(
          jsonFile.path,
          const JsonOptions(flattenNested: false),
        );
        final table = await df.execute();

        final rows = table.toRows();
        expect(rows[0]['int'], equals(42));
        expect(rows[0]['float'], equals(3.14));
        expect(rows[0]['bool'], equals(true));
        expect(rows[0]['string'], equals('hello'));
        expect(rows[0]['null'], isNull);
        expect(rows[0]['array'], equals([1, 2, 3]));
        expect(rows[0]['object'], isA<Map>());
        expect((rows[0]['object'] as Map)['key'], equals('value'));
      });

      test('handles empty JSON array', () async {
        final jsonFile = File('${tempDir.path}/empty.json');
        jsonFile.writeAsStringSync('[]');

        final df = await DataFrameIO.readJson(jsonFile.path);
        final table = await df.execute();

        expect(table.rowCount, equals(0));
        expect(table.isEmpty, isTrue);
      });

      test('handles sparse JSON data', () async {
        final jsonFile = File('${tempDir.path}/sparse.jsonl');
        jsonFile.writeAsStringSync('''{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob"}
{"id": 3, "age": 35}
{"id": 4, "name": "David", "age": 28, "city": "NYC"}''');

        final df = await DataFrameIO.readJson(jsonFile.path);
        final table = await df.execute();

        expect(table.rowCount, equals(4));
        expect(table.columnCount, equals(4)); // id, name, age, city

        final rows = table.toRows();
        expect(rows[1]['age'], isNull);
        expect(rows[2]['name'], isNull);
        expect(rows[3]['city'], equals('NYC'));
        expect(rows[0]['city'], isNull);
      });

      test('auto-detects format from file extension', () async {
        // Test .json extension (should read as array)
        final jsonFile = File('${tempDir.path}/data.json');
        jsonFile.writeAsStringSync('[{"id": 1}, {"id": 2}]');

        final df1 = await DataFrameIO.readJson(jsonFile.path);
        final table1 = await df1.execute();
        expect(table1.rowCount, equals(2));

        // Test .jsonl extension (should read as lines)
        final jsonlFile = File('${tempDir.path}/data.jsonl');
        jsonlFile.writeAsStringSync('{"id": 1}\n{"id": 2}');

        final df2 = await DataFrameIO.readJson(jsonlFile.path);
        final table2 = await df2.execute();
        expect(table2.rowCount, equals(2));
      });

      test('handles large numbers correctly', () async {
        final jsonFile = File('${tempDir.path}/numbers.json');
        jsonFile.writeAsStringSync('''[
          {
            "small_int": 42,
            "large_int": 9223372036854775807,
            "small_float": 3.14,
            "large_float": 1.7976931348623157e+308,
            "negative": -9223372036854775808
          }
        ]''');

        final df = await DataFrameIO.readJson(jsonFile.path);
        final table = await df.execute();

        final rows = table.toRows();
        expect(rows[0]['small_int'], equals(42));
        expect(rows[0]['large_int'], equals(9223372036854775807));
        expect(rows[0]['small_float'], equals(3.14));
        expect(rows[0]['negative'], equals(-9223372036854775808));
      });
    });

    group('JSON Writing Tests', () {
      late Directory tempDir;

      setUp(() {
        tempDir = Directory.systemTemp.createTempSync('json_write_test_');
      });

      tearDown(() {
        if (tempDir.existsSync()) {
          tempDir.deleteSync(recursive: true);
        }
      });

      test('writes basic JSON array file', () async {
        final data = [
          {'id': 1, 'name': 'Alice', 'age': 30},
          {'id': 2, 'name': 'Bob', 'age': 25},
          {'id': 3, 'name': 'Charlie', 'age': 35},
        ];

        final df = DataFrame.fromRows(data);
        final outputFile = '${tempDir.path}/output.json';
        await df.writeJson(outputFile);

        final content = File(outputFile).readAsStringSync();
        final parsed = jsonDecode(content) as List;

        expect(parsed.length, equals(3));
        expect(parsed[0]['name'], equals('Alice'));
        expect(parsed[1]['age'], equals(25));
        expect(parsed[2]['id'], equals(3));
      });

      test('writes JSON Lines format', () async {
        final data = [
          {'id': 1, 'product': 'Widget A', 'price': 10.99},
          {'id': 2, 'product': 'Widget B', 'price': 25.50},
        ];

        final df = DataFrame.fromRows(data);
        final outputFile = '${tempDir.path}/output.jsonl';
        await df.writeJson(outputFile, JsonOptions.jsonLines());

        final lines = File(outputFile).readAsLinesSync();
        expect(lines.length, equals(2));

        final row1 = jsonDecode(lines[0]);
        expect(row1['product'], equals('Widget A'));

        final row2 = jsonDecode(lines[1]);
        expect(row2['price'], equals(25.50));
      });

      test('writes with pretty printing', () async {
        final data = [
          {'id': 1, 'name': 'Alice'},
          {'id': 2, 'name': 'Bob'},
        ];

        final df = DataFrame.fromRows(data);
        final outputFile = '${tempDir.path}/pretty.json';
        await df.writeJson(outputFile, JsonOptions.prettyArray());

        final content = File(outputFile).readAsStringSync();
        expect(content, contains('[\n'));
        expect(content, contains('  {'));
        expect(content, contains('    "id"'));
        expect(content, contains('    "name"'));
      });

      test('handles null values in output', () async {
        final data = [
          {'id': 1, 'name': 'Alice', 'value': 100},
          {'id': 2, 'name': null, 'value': 200},
          {'id': 3, 'name': 'Charlie', 'value': null},
        ];

        final df = DataFrame.fromRows(data);
        final outputFile = '${tempDir.path}/nulls.json';
        await df.writeJson(outputFile);

        final content = File(outputFile).readAsStringSync();
        final parsed = jsonDecode(content) as List;

        expect(parsed[1]['name'], isNull);
        expect(parsed[2]['value'], isNull);
      });

      test('preserves complex data types', () async {
        final data = [
          {
            'id': 1,
            'nested': {'key': 'value', 'num': 42},
            'array': [1, 2, 3],
            'bool': true,
          },
          {
            'id': 2,
            'nested': {'key': 'other', 'num': 99},
            'array': [4, 5, 6],
            'bool': false,
          },
        ];

        final df = DataFrame.fromRows(data);
        final outputFile = '${tempDir.path}/complex.json';
        await df.writeJson(outputFile);

        final content = File(outputFile).readAsStringSync();
        final parsed = jsonDecode(content) as List;

        expect(parsed[0]['nested']['key'], equals('value'));
        expect(parsed[0]['array'], equals([1, 2, 3]));
        expect(parsed[1]['bool'], equals(false));
      });

      test('auto-detects format from extension', () async {
        final data = [
          {'id': 1, 'value': 'A'},
          {'id': 2, 'value': 'B'},
        ];

        final df = DataFrame.fromRows(data);

        // .json should write as array
        final jsonFile = '${tempDir.path}/data.json';
        await df.writeJson(jsonFile);
        final jsonContent = File(jsonFile).readAsStringSync();
        expect(jsonContent.startsWith('['), isTrue);
        expect(jsonContent.endsWith(']'), isTrue);

        // .jsonl should write as lines
        final jsonlFile = '${tempDir.path}/data.jsonl';
        await df.writeJson(jsonlFile);
        final jsonlLines = File(jsonlFile).readAsLinesSync();
        expect(jsonlLines.length, equals(2));
        expect(jsonlLines[0].startsWith('{'), isTrue);
      });

      test('round-trip preserves data integrity', () async {
        final originalData = [
          {'id': 1, 'name': 'Alice', 'score': 95.5, 'active': true},
          {'id': 2, 'name': 'Bob', 'score': 87.3, 'active': false},
          {'id': 3, 'name': 'Charlie', 'score': 92.0, 'active': true},
        ];

        final df1 = DataFrame.fromRows(originalData);

        // Write to JSON
        final jsonFile = '${tempDir.path}/roundtrip.json';
        await df1.writeJson(jsonFile);

        // Read back
        final df2 = await DataFrameIO.readJson(jsonFile);
        final table = await df2.execute();

        expect(table.rowCount, equals(3));
        final rows = table.toRows();

        for (int i = 0; i < originalData.length; i++) {
          expect(rows[i], equals(originalData[i]));
        }
      });
    });
  });

  group('roundtrip', () {
    group('JSON Round-trip Tests', () {
      late Directory tempDir;

      setUp(() {
        tempDir = Directory.systemTemp.createTempSync('json_roundtrip_test_');
      });

      tearDown(() {
        if (tempDir.existsSync()) {
          tempDir.deleteSync(recursive: true);
        }
      });

      group('CSV to JSON Array', () {
        test('round-trips basic data', () async {
          // Create CSV data
          final csvFile = File('${tempDir.path}/input.csv');
          csvFile.writeAsStringSync('''id,name,age,score
1,Alice,30,95.5
2,Bob,25,87.3
3,Charlie,35,92.0''');

          // Read CSV
          final df = await DataFrameIO.readCsv(csvFile.path);

          // Write to JSON array
          final jsonFile = '${tempDir.path}/output.json';
          await df.writeJson(jsonFile);

          // Read JSON back
          final df2 = await DataFrameIO.readJson(jsonFile);
          final table2 = await df2.execute();

          // Verify data integrity
          expect(table2.rowCount, equals(3));
          expect(table2.columnCount, equals(4));

          final rows = table2.toRows();
          expect(rows[0]['name'], equals('Alice'));
          expect(rows[0]['age'], equals(30));
          expect(rows[0]['score'], equals(95.5));
          expect(rows[2]['name'], equals('Charlie'));

          // Read the JSON file to verify format
          final jsonContent = File(jsonFile).readAsStringSync();
          expect(jsonContent, contains('['));
          expect(jsonContent, contains(']'));
          expect(jsonContent, contains('"name"'));
        });

        test('handles null values', () async {
          final csvFile = File('${tempDir.path}/nulls.csv');
          csvFile.writeAsStringSync('''id,name,value
1,Alice,100
2,,200
3,Charlie,
4,David,0''');

          final df = await DataFrameIO.readCsv(
            csvFile.path,
            CsvOptions(nullValues: const {'', 'NULL', 'null', 'NA'}),
          );

          final jsonFile = '${tempDir.path}/nulls.json';
          await df.writeJson(jsonFile);

          final df2 = await DataFrameIO.readJson(jsonFile);
          final table2 = await df2.execute();

          final rows = table2.toRows();
          expect(rows[1]['name'], isNull);
          expect(rows[2]['value'], isNull);
          expect(rows[3]['value'], equals(0));
        });

        test('preserves data types', () async {
          final csvFile = File('${tempDir.path}/types.csv');
          csvFile.writeAsStringSync('''int_col,float_col,bool_col,string_col
42,3.14,true,hello
100,2.718,false,world
-5,0.0,true,test''');

          final df = await DataFrameIO.readCsv(csvFile.path);
          final jsonFile = '${tempDir.path}/types.json';
          await df.writeJson(jsonFile);

          final df2 = await DataFrameIO.readJson(jsonFile);
          final table2 = await df2.execute();

          final rows = table2.toRows();
          expect(rows[0]['int_col'], equals(42));
          expect(rows[0]['float_col'], equals(3.14));
          expect(rows[0]['bool_col'], equals(true));
          expect(rows[1]['bool_col'], equals(false));
          expect(rows[2]['int_col'], equals(-5));
        });
      });

      group('CSV to JSON Lines', () {
        test('round-trips to .jsonl format', () async {
          final csvFile = File('${tempDir.path}/input.csv');
          csvFile.writeAsStringSync('''id,name,score
1,Alice,95.5
2,Bob,87.3
3,Charlie,92.0''');

          final df = await DataFrameIO.readCsv(csvFile.path);

          // Write to JSON Lines (auto-detected by extension)
          final jsonlFile = '${tempDir.path}/output.jsonl';
          await df.writeJson(jsonlFile);

          // Verify it's JSON Lines format
          final content = File(jsonlFile).readAsStringSync();
          final lines = content.split('\n');
          expect(lines.length, equals(3));
          expect(lines[0], contains('{"id":1'));
          expect(lines[0], contains('"name":"Alice"'));
          expect(lines[0], isNot(contains('[')));

          // Read JSON Lines back
          final df2 = await DataFrameIO.readJson(jsonlFile);
          final table2 = await df2.execute();

          expect(table2.rowCount, equals(3));
          final rows = table2.toRows();
          expect(rows[0]['name'], equals('Alice'));
          expect(rows[2]['score'], equals(92.0));
        });

        test('explicitly uses JSON Lines format', () async {
          final csvFile = File('${tempDir.path}/input.csv');
          csvFile.writeAsStringSync('''product,price,quantity
Widget A,10.99,100
Widget B,25.50,50
Widget C,15.00,75''');

          final df = await DataFrameIO.readCsv(csvFile.path);

          // Explicitly use JSON Lines format
          final jsonFile = '${tempDir.path}/products.json';
          await df.writeJson(jsonFile, JsonOptions.jsonLines());

          // Verify format
          final content = File(jsonFile).readAsStringSync();
          final lines = content.trim().split('\n');
          expect(lines.length, equals(3));

          // Parse each line as separate JSON
          for (final line in lines) {
            final json = jsonDecode(line);
            expect(json, isA<Map<String, dynamic>>());
            expect(json.containsKey('product'), isTrue);
            expect(json.containsKey('price'), isTrue);
          }

          // Read back with JSON Lines
          final df2 = await DataFrameIO.readJsonLines(jsonFile);
          final table2 = await df2.execute();

          expect(table2.rowCount, equals(3));
          final rows = table2.toRows();
          expect(rows[0]['product'], equals('Widget A'));
          expect(rows[1]['price'], equals(25.50));
        });
      });

      group('JSON to CSV', () {
        test('converts JSON array to CSV', () async {
          final jsonFile = File('${tempDir.path}/input.json');
          jsonFile.writeAsStringSync('''[
  {"id": 1, "name": "Alice", "age": 30},
  {"id": 2, "name": "Bob", "age": 25},
  {"id": 3, "name": "Charlie", "age": 35}
]''');

          final df = await DataFrameIO.readJson(jsonFile.path);

          final csvFile = '${tempDir.path}/output.csv';
          await df.writeCsv(csvFile);

          // Read CSV back
          final df2 = await DataFrameIO.readCsv(csvFile);
          final table2 = await df2.execute();

          expect(table2.rowCount, equals(3));
          final rows = table2.toRows();
          expect(rows[0]['name'], equals('Alice'));
          expect(rows[1]['age'], equals(25));

          // Verify CSV format
          final csvContent = File(csvFile).readAsStringSync();
          expect(csvContent, contains('id,name,age'));
          expect(csvContent, contains('1,Alice,30'));
        });

        test('converts JSON Lines to CSV', () async {
          final jsonlFile = File('${tempDir.path}/input.jsonl');
          jsonlFile.writeAsStringSync(
            '''{"id": 1, "product": "Widget A", "price": 10.99}
{"id": 2, "product": "Widget B", "price": 25.50}
{"id": 3, "product": "Widget C", "price": 15.00}''',
          );

          final df = await DataFrameIO.readJson(jsonlFile.path);

          final csvFile = '${tempDir.path}/output.csv';
          await df.writeCsv(csvFile);

          final df2 = await DataFrameIO.readCsv(csvFile);
          final table2 = await df2.execute();

          expect(table2.rowCount, equals(3));
          final rows = table2.toRows();
          expect(rows[0]['product'], equals('Widget A'));
          expect(rows[1]['price'], equals(25.50));
        });

        test('handles missing fields in JSON Lines', () async {
          final jsonlFile = File('${tempDir.path}/sparse.jsonl');
          jsonlFile.writeAsStringSync('''{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob"}
{"id": 3, "age": 35}
{"id": 4, "name": "David", "age": 28, "city": "NYC"}''');

          final df = await DataFrameIO.readJson(jsonlFile.path);
          final table = await df.execute();

          expect(table.rowCount, equals(4));
          expect(table.columnCount, equals(4)); // id, name, age, city

          final rows = table.toRows();
          expect(rows[1]['age'], isNull);
          expect(rows[2]['name'], isNull);
          expect(rows[3]['city'], equals('NYC'));

          // Convert to CSV
          final csvFile = '${tempDir.path}/sparse.csv';
          await df.writeCsv(csvFile);

          // Read back and verify
          final df2 = await DataFrameIO.readCsv(
            csvFile,
            CsvOptions(nullValues: const {'', 'NULL', 'null'}),
          );
          final table2 = await df2.execute();

          final rows2 = table2.toRows();
          expect(rows2[1]['age'], isNull);
          expect(rows2[2]['name'], isNull);
        });
      });

      group('Large dataset round-trips', () {
        test('handles 10K rows in JSON array format', () async {
          // Create large CSV
          final csvFile = File('${tempDir.path}/large.csv');
          final buffer = StringBuffer('id,value,category\n');
          for (int i = 0; i < 10000; i++) {
            buffer.writeln('$i,${i * 2.5},cat${i % 10}');
          }
          csvFile.writeAsStringSync(buffer.toString());

          final df = await DataFrameIO.readCsv(csvFile.path);

          // Convert to JSON array
          final jsonFile = '${tempDir.path}/large.json';
          await df.writeJson(jsonFile);

          // Read back
          final df2 = await DataFrameIO.readJson(jsonFile);
          final table2 = await df2.execute();

          expect(table2.rowCount, equals(10000));
          final rows = table2.toRows();
          expect(rows[0]['id'], equals(0));
          expect(rows[9999]['value'], equals(24997.5));
          expect(rows[500]['category'], equals('cat0'));
        });

        test('handles 10K rows in JSON Lines format', () async {
          // Create large CSV
          final csvFile = File('${tempDir.path}/large.csv');
          final buffer = StringBuffer('id,value,category\n');
          for (int i = 0; i < 10000; i++) {
            buffer.writeln('$i,${i * 2.5},cat${i % 10}');
          }
          csvFile.writeAsStringSync(buffer.toString());

          final df = await DataFrameIO.readCsv(csvFile.path);

          // Convert to JSON Lines
          final jsonlFile = '${tempDir.path}/large.jsonl';
          await df.writeJson(jsonlFile);

          // Verify file size is reasonable
          final fileSize = File(jsonlFile).lengthSync();
          expect(fileSize, greaterThan(100000)); // Should be > 100KB

          // Read back
          final df2 = await DataFrameIO.readJson(jsonlFile);
          final table2 = await df2.execute();

          expect(table2.rowCount, equals(10000));
          final rows = table2.toRows();
          expect(rows[0]['id'], equals(0));
          expect(rows[9999]['value'], equals(24997.5));
        });
      });

      group('Format detection', () {
        test('auto-detects JSON Lines from .jsonl extension', () async {
          final data = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
          ];

          final df = DataFrame.fromRows(data);

          // Save with .jsonl extension
          final jsonlFile = '${tempDir.path}/data.jsonl';
          await df.writeJson(jsonlFile);

          // Verify it's JSON Lines format
          final content = File(jsonlFile).readAsStringSync();
          expect(content, contains('\n'));
          expect(content, isNot(contains('[')));

          // Should be readable as JSON Lines
          final df2 = await DataFrameIO.readJson(jsonlFile);
          final table2 = await df2.execute();
          expect(table2.rowCount, equals(2));
        });

        test('auto-detects JSON array from .json extension', () async {
          final data = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
          ];

          final df = DataFrame.fromRows(data);

          // Save with .json extension
          final jsonFile = '${tempDir.path}/data.json';
          await df.writeJson(jsonFile);

          // Verify it's JSON array format
          final content = File(jsonFile).readAsStringSync();
          expect(content, contains('['));
          expect(content, contains(']'));

          // Should be readable as JSON array
          final df2 = await DataFrameIO.readJson(jsonFile);
          final table2 = await df2.execute();
          expect(table2.rowCount, equals(2));
        });
      });
    });
  });

  group('streaming', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('json_stream_'));
    tearDown(() => dir.deleteSync(recursive: true));

    String write(String name, String content) {
      final p = '${dir.path}/$name';
      File(p).writeAsStringSync(content);
      return p;
    }

    test(
      'streams JSON Lines in bounded batches with a stable schema',
      () async {
        final lines = [for (int i = 0; i < 250; i++) '{"id":$i,"name":"n$i"}'];
        final path = write('d.jsonl', lines.join('\n'));

        final batches = await DataFrameIO.readJsonBatches(
          path,
          size: 100,
        ).toList();
        expect(batches.length, 3);
        final tables = [for (final b in batches) await b.execute()];
        expect(tables.map((t) => t.rowCount).toList(), [100, 100, 50]);

        final schemas = tables
            .map(
              (t) => t.schema.fields
                  .map((f) => '${f.name}:${f.dataType}')
                  .join(','),
            )
            .toSet();
        expect(schemas.length, 1, reason: 'pinned schema across batches');

        final ids = [
          for (final t in tables) ...t.column('id')!.toList().cast<int>(),
        ];
        expect(ids, List.generate(250, (i) => i));
      },
    );

    test(
      'strict (default) throws on a malformed line with its number',
      () async {
        final path = write('bad.jsonl', '{"a":1}\nnotjson\n{"a":2}');
        expect(
          () => DataFrameIO.readJsonBatches(path, size: 10).toList(),
          throwsA(
            isA<FormatException>().having(
              (e) => e.message,
              'message',
              contains('line 2'),
            ),
          ),
        );
      },
    );

    test('permissive mode skips bad/non-object lines', () async {
      final path = write('p.jsonl', '{"a":1}\nnotjson\n5\n{"a":2}');
      final r = JsonReader(const JsonOptions(jsonLines: true, strict: false));
      final rows = <Map<String, dynamic>>[];
      await for (final t in r.readBatches(path, size: 10)) {
        rows.addAll(t.toRows());
      }
      expect(rows.map((m) => m['a']).toList(), [1, 2]);
    });

    test('flattens nested objects', () async {
      final path = write(
        'nest.jsonl',
        '{"id":1,"u":{"name":"a"}}\n{"id":2,"u":{"name":"b"}}',
      );
      final t = await (await DataFrameIO.readJsonBatches(
        path,
        size: 10,
      ).first).execute();
      expect(t.column('u.name')!.toList(), ['a', 'b']);
    });

    test('matches whole-file readJsonLines on values', () async {
      final lines = [for (int i = 0; i < 120; i++) '{"id":$i,"v":"x$i"}'];
      final path = write('m.jsonl', lines.join('\n'));
      final whole = await (await DataFrameIO.readJsonLines(path)).execute();
      final streamed = [
        for (final b in await DataFrameIO.readJsonBatches(
          path,
          size: 40,
        ).toList())
          ...(await b.execute()).column('v')!.toList(),
      ];
      expect(streamed, whole.column('v')!.toList());
    });

    test('readBatches streams a plain JSON array', () async {
      final path = write('arr.json', '[{"a":1},{"a":2},{"a":3}]');
      final rows = [
        for (final b in await JsonReader(
          const JsonOptions(),
        ).readBatches(path).toList())
          ...b.toRows(),
      ];
      expect(rows.map((r) => r['a']), [1, 2, 3]);
    });

    group('truncated JSON array fails loud', () {
      Future<void> expectThrows(String content) async {
        final path = write('trunc.json', content);
        await expectLater(
          JsonReader(const JsonOptions()).readBatches(path).toList(),
          throwsA(isA<FormatException>()),
        );
      }

      test('missing closing bracket after one element', () async {
        await expectThrows('[{"id":1}');
      });
      test('missing closing bracket mid-array', () async {
        await expectThrows('[{"id":1},{"id":2}');
      });
      test('a well-formed array and empty [] still succeed', () async {
        final ok = write('ok.json', '[{"a":1},{"a":2}]');
        expect(
          [
            for (final b in await JsonReader(
              const JsonOptions(),
            ).readBatches(ok).toList())
              ...b.toRows(),
          ].map((r) => r['a']),
          [1, 2],
        );
        final empty = write('empty.json', '[]');
        expect(
          await JsonReader(const JsonOptions()).readBatches(empty).toList(),
          isEmpty,
        );
      });
    });

    group('late-appearing fields (no silent loss)', () {
      // 'extra' first appears at record index 3, beyond a 2-record sample.
      final lines = [
        '{"id":0}',
        '{"id":1}',
        '{"id":2}',
        '{"id":3,"extra":"late"}',
        '{"id":4}',
      ];

      test(
        'unbounded readFile (caps null) infers a field beyond the sample',
        () async {
          final path = write('late.jsonl', lines.join('\n'));
          // Both caps null reads the whole file and infers from ALL records.
          final t = await JsonReader(
            const JsonOptions(jsonLines: true, typeInferenceSampleSize: 2),
          ).readFile(path, maxRows: null, maxBytes: null);
          expect(t.schema.field('extra'), isNotNull);
          expect(t.column('extra')!.toList()[3], 'late');
        },
      );

      test('bounded (default) readFile fails closed on a late field', () async {
        // The default (bounded) readFile streams via readBatches, which infers
        // from the leading sample and fails closed on a later new field.
        final path = write('late.jsonl', lines.join('\n'));
        await expectLater(
          JsonReader(
            const JsonOptions(jsonLines: true, typeInferenceSampleSize: 2),
          ).readFile(path),
          throwsA(isA<FormatException>()),
        );
      });

      test('streaming readBatches fails closed on a late field', () async {
        final path = write('late.jsonl', lines.join('\n'));
        expect(
          () => JsonReader(
            const JsonOptions(jsonLines: true, typeInferenceSampleSize: 2),
          ).readBatches(path, size: 2).toList(),
          throwsA(isA<FormatException>()),
        );
      });
    });
  });

  group('schema pin', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('jpin_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('a late new field fails closed WITHOUT an explicit schema', () async {
      final p = '${dir.path}/a.jsonl';
      File(p).writeAsStringSync('{"a":1}\n{"a":2}\n{"a":3,"b":9}');
      final reader = JsonReader(
        const JsonOptions(jsonLines: true, typeInferenceSampleSize: 1),
      );
      await expectLater(
        reader.readBatches(p, size: 1).toList(),
        throwsA(isA<FormatException>()),
      );
    });

    test(
      'an explicit schema pins up front; a late new field is dropped',
      () async {
        final p = '${dir.path}/a.jsonl';
        File(p).writeAsStringSync('{"a":1}\n{"a":2}\n{"a":3,"b":9}');
        final reader = JsonReader(
          const JsonOptions(jsonLines: true, typeInferenceSampleSize: 1),
        );
        final schema = Schema(
          fields: [Field(name: 'a', dataType: DataType.int64, nullable: true)],
        );
        final batches = await reader
            .readBatches(p, size: 1, schema: schema)
            .toList();
        final all = [for (final t in batches) ...t.toRows()];
        expect(all.map((r) => r['a']).toList(), [1, 2, 3]);
        // 'b' was dropped (not in the pinned schema), so no field surprise.
        expect(all.every((r) => !r.containsKey('b')), isTrue);
      },
    );
  });

  group('line cap', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('jsoncap_'));
    tearDown(() => dir.deleteSync(recursive: true));

    String write(String name, String content) {
      final p = '${dir.path}/$name';
      File(p).writeAsStringSync(content);
      return p;
    }

    test('a normal .jsonl streams in bounded batches', () async {
      final path = write(
        'ok.jsonl',
        [for (var i = 0; i < 50; i++) '{"id":$i,"v":${i * 2}}'].join('\n'),
      );
      final batches = await JsonReader(
        const JsonOptions(jsonLines: true),
      ).readBatches(path, size: 7).toList();
      final rows = [for (final b in batches) ...b.toRows()];
      expect(rows.length, 50);
      expect(rows.first['id'], 0);
      expect(rows.last['v'], 98);
    });

    test('a single over-cap line throws FormatException (no OOM)', () {
      final huge = '{"a":"${'x' * 200000}"}'; // one ~200KB line
      final path = write('big.jsonl', huge);
      expect(
        JsonReader(
          const JsonOptions(jsonLines: true, maxRecordBytes: 1024),
        ).readBatches(path).toList(),
        throwsA(isA<FormatException>()),
      );
    });

    test('\\r\\n line endings parse', () async {
      final path = write('crlf.jsonl', '{"id":1}\r\n{"id":2}\r\n');
      final rows = [
        for (final b in await JsonReader(
          const JsonOptions(jsonLines: true),
        ).readBatches(path).toList())
          ...b.toRows(),
      ];
      expect(rows.map((r) => r['id']), [1, 2]);
    });

    test('the default 16MB cap admits ordinary rows', () async {
      final path = write('def.jsonl', '{"id":1}\n{"id":2}\n{"id":3}');
      final rows = [
        for (final b in await JsonReader(
          const JsonOptions(jsonLines: true),
        ).readBatches(path).toList())
          ...b.toRows(),
      ];
      expect(rows.length, 3);
    });
  });

  // readFile() streams (and honours `strict`); readFile(maxRows: null) goes
  // through readString. They must not disagree about the same file.
  group('array element typing', () {
    const content = '[{"a":1},"not an object",{"a":3}]';

    test('strict readString names the bad element', () async {
      await expectLater(
        JsonReader().readString(content),
        throwsA(
          isA<FormatException>().having(
            (e) => e.message,
            'message',
            allOf(contains('element 1'), contains('object')),
          ),
        ),
      );
    });

    test('permissive readString skips it', () async {
      final table = await JsonReader(
        const JsonOptions(strict: false),
      ).readString(content);
      expect(table.rowCount, 2);
      expect(table.column('a')!.toList(), [1, 3]);
    });

    test('an all-object array is unaffected', () async {
      final table = await JsonReader().readString('[{"a":1},{"a":2}]');
      expect(table.column('a')!.toList(), [1, 2]);
    });

    test('a bare object is still accepted', () async {
      final table = await JsonReader().readString('{"a":1}');
      expect(table.column('a')!.toList(), [1]);
    });
  });

  // readString documents maxRows/maxBytes as bounding the PARSED RESULT, and
  // expandArrays turns one record into as many rows as its longest array — so
  // the cap has to survive the expansion, not just the parse.
  group('expandArrays cap', () {
    final reader = JsonReader(const JsonOptions(expandArrays: true));

    // 2 records x a 10-element array = 20 rows out of 2 records in.
    final content = jsonEncode([
      for (var r = 0; r < 2; r++)
        {
          'id': r,
          'v': [for (var i = 0; i < 10; i++) r * 10 + i],
        },
    ]);

    test('expansion past maxRows throws', () async {
      await expectLater(
        reader.readString(content, maxRows: 5),
        throwsA(isA<StateError>()),
      );
    });

    test('expansion up to maxRows is allowed', () async {
      final table = await reader.readString(content, maxRows: 20);
      expect(table.rowCount, 20);
    });

    test('maxRows: null opts out', () async {
      final table = await reader.readString(content, maxRows: null);
      expect(table.rowCount, 20);
      expect(table.column('v')!.toList().first, 0);
    });

    test('the pre-expansion record count is still capped', () async {
      await expectLater(
        reader.readString(content, maxRows: 1),
        throwsA(isA<StateError>()),
      );
    });
  });

  group('dataframe toJsonString', () {
    test('toJsonString escapes special characters and round-trips', () async {
      final rows = [
        {
          'plain': 'hello',
          'quote': 'a"b',
          'backslash': r'c\d',
          'newline': 'e\nf',
          'tab': 'g\th',
        },
      ];
      final df = DataFrame.fromRows(rows);
      final json = await df.toJsonString();

      // Must be parseable and equal to the original values.
      final decoded = jsonDecode(json) as List;
      expect(decoded, hasLength(1));
      final row = decoded.first as Map<String, dynamic>;
      expect(row['plain'], 'hello');
      expect(row['quote'], 'a"b');
      expect(row['backslash'], r'c\d');
      expect(row['newline'], 'e\nf');
      expect(row['tab'], 'g\th');
    });

    test('toJsonString handles a key containing a quote', () async {
      final df = DataFrame.fromRows([
        {'we"ird': 1},
      ]);
      final decoded = jsonDecode(await df.toJsonString()) as List;
      expect((decoded.first as Map)['we"ird'], 1);
    });
  });

  // ---------------------------------------------------------------------------
  // Relocated from audit/review round suites (JSON-specific coverage).
  // ---------------------------------------------------------------------------
  group('relocated audit/review rounds', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('reloc_json_'));
    tearDown(() => dir.deleteSync(recursive: true));

    group('#6 JSON bounded / streaming export', () {
      DataFrame frame(int rows) => DataFrame.fromRows([
        for (var i = 0; i < rows; i++) {'id': i, 'name': 'row-$i'},
      ]);

      test('JSON Lines export streams and round-trips', () async {
        final path = '${dir.path}/out.jsonl';
        await frame(5000).writeJsonLines(path);
        final lines = await File(path).readAsLines();
        expect(lines.length, 5000);
      });

      test('compact JSON array export streams a valid array', () async {
        final path = '${dir.path}/out.json';
        await frame(2000).writeJson(path);
        final back = await (await DataFrameIO.readJson(
          path,
        )).execute(maxRows: null);
        expect(back.rowCount, 2000);
      });
    });

    group('#7 projection honored by JSON scan read()', () {
      test('read(projection:) returns only the projected columns', () async {
        final path = '${dir.path}/w.json';
        await File(path).writeAsString('[{"a":1,"b":2,"c":3}]');
        final t = await openScan(path).read(projection: ['a', 'c']);
        expect(t.schema.fieldNames, ['a', 'c']); // 'b' not retained
      });
    });

    group('#5 JSON array grammar', () {
      Future<void> read(String content) async {
        final f = File('${dir.path}/x.json')..writeAsStringSync(content);
        await (await DataFrameIO.readJson(f.path)).execute();
      }

      test('malformed arrays are rejected', () async {
        for (final bad in ['[{},]', '[,{}]', '[{}] junk', '', '  ']) {
          await expectLater(
            read(bad),
            throwsA(isA<FormatException>()),
            reason: 'should reject: "$bad"',
          );
        }
      });

      test('valid JSON still reads', () async {
        await read('[{"a":1},{"a":2}]'); // no throw
      });
    });
  });

  // Relocated verbatim from regression_test.dart (self-contained).
  group('JSON Lines strict-by-default', () {
    test(
      'strict (default) throws with a line number on malformed input',
      () async {
        final r = JsonReader(JsonOptions.jsonLines());
        expect(
          () => r.readString('{"a":1}\nnotjson\n{"a":2}'),
          throwsA(
            isA<FormatException>().having(
              (e) => e.message,
              'message',
              contains('line 2'),
            ),
          ),
        );
      },
    );

    test('strict rejects non-object lines', () async {
      final r = JsonReader(JsonOptions.jsonLines());
      expect(
        () => r.readString('{"a":1}\n5\n{"a":2}'),
        throwsA(isA<FormatException>()),
      );
    });

    test('permissive mode skips bad lines and keeps the rest', () async {
      final r = JsonReader(const JsonOptions(jsonLines: true, strict: false));
      final t = await r.readString('{"a":1}\nnotjson\n{"a":2}\n5');
      expect(t.column('a')!.toList(), [1, 2]);
    });

    test('valid input is unaffected', () async {
      final df = await JsonReader(
        JsonOptions.jsonLines(),
      ).readString('{"a":1}\n{"a":2}\n{"a":3}');
      expect(df.rowCount, 3);
    });
  });

  group('crash-safe writers (relocated)', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('writer_safe_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table ints(List<int> ids) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: 'id', values: ids, nullable: false)],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    Table withNan() => Table(
      name: 't',
      columns: [
        NumericColumn.float64(name: 'x', values: [double.nan], nullable: false),
      ],
      schema: Schema(
        fields: [Field(name: 'x', dataType: DataType.float64, nullable: false)],
      ),
    );

    List<FileSystemEntity> temps() =>
        dir.listSync().where((f) => f.path.contains('.tmp')).toList();

    test(
      'a failed JSON write preserves the previous file and leaves no temp',
      () async {
        final path = '${dir.path}/out.json';
        await ints([1, 2, 3]).writeJson(path);
        final good = File(path).readAsStringSync();
        await expectLater(withNan().writeJson(path), throwsA(anything));
        expect(File(path).readAsStringSync(), good);
        expect(temps(), isEmpty);
      },
    );

    test(
      'successful CSV/JSON writes replace atomically with no temp orphan',
      () async {
        final csv = '${dir.path}/out.csv';
        final json = '${dir.path}/out.json';
        await CsvWriter().writeTable(ints([1, 2]), csv);
        await ints([1, 2]).writeJson(json);
        await CsvWriter().writeTable(ints([9]), csv);
        await ints([9]).writeJson(json);
        expect(File(csv).readAsStringSync(), contains('9'));
        expect(File(csv).readAsStringSync(), isNot(contains('1')));
        expect(File(json).readAsStringSync(), contains('9'));
        expect(temps(), isEmpty);
      },
    );
  });
}
