import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:test/test.dart';

import 'support/results.dart';

void main() {
  group('CSV values', () {
    test('a timestamp without a zone is UTC', () async {
      final table = await CsvReader(
        options: CsvOptions(
          schema: Schema(
            fields: [Field(name: 't', dataType: DataType.timestamp)],
          ),
        ),
      ).readString('t\n2024-01-01 10:00:00\n').ok;
      expect(table.column('t')![0], DateTime.utc(2024, 1, 1, 10));
      final formatted = await CsvReader(
        options: CsvOptions(
          timestampFormat: 'yyyy/MM/dd HH:mm',
          schema: Schema(
            fields: [Field(name: 't', dataType: DataType.timestamp)],
          ),
        ),
      ).readString('t\n2024/01/01 10:00\n').ok;
      expect(formatted.column('t')![0], DateTime.utc(2024, 1, 1, 10));
    });

    test(
      'the thousands separator is stripped only from grouped digits',
      () async {
        final table = await CsvReader(
          options: CsvOptions.european(),
        ).readString('a;b\n1.2.3;1.234,5\n10.0.1;2.000,25\n').ok;
        expect(table.schema.field('a')!.dataType, DataType.string);
        expect(table.column('a')!.toList(), ['1.2.3', '10.0.1']);
        expect(table.column('b')!.toList(), [1234.5, 2000.25]);
      },
    );

    test('a custom escape character is itself escaped on write', () async {
      const options = CsvOptions(escape: r'\');
      final table = Table.fromRows('t', [
        {'v': r'a\b,c'},
        {'v': r'x,\'},
      ]);
      final text = CsvWriter(options).writeTableToString(table);
      final back = await CsvReader(options: options).readString(text).ok;
      expect(back.column('v')!.toList(), [r'a\b,c', r'x,\']);
    });

    test('a string equal to a null token survives a round trip', () async {
      final table = Table.fromRows('t', [
        {'c': 'NA', 'n': 'null'},
        {'c': 'x', 'n': 'NaN'},
      ]);
      final text = CsvWriter().writeTableToString(table);
      final back = await CsvReader().readString(text).ok;
      expect(back.toRows(), [
        {'c': 'NA', 'n': 'null'},
        {'c': 'x', 'n': 'NaN'},
      ]);
    });
  });

  group('every read path agrees', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('csv_paths_'));
    tearDown(() => dir.deleteSync(recursive: true));

    /// [content] read through readString, readFile and readBatches, each as
    /// (schema, rows).
    Future<List<(String, List<Map<String, dynamic>>)>> read(
      String content, [
      CsvOptions options = const CsvOptions(),
    ]) async {
      final path = '${dir.path}/in.csv';
      File(path).writeAsStringSync(content);
      final reader = CsvReader(options: options);
      String schema(Table t) => [
        for (final f in t.schema.fields) '${f.name}:${f.dataType.name}',
      ].join(',');
      final text = await reader.readString(content).ok;
      final file = await reader.readFile(path).ok;
      final batches = [await for (final b in reader.readBatches(path)) b.ok];
      final rows = [for (final b in batches) ...b.toRows()];
      return [
        (schema(text), text.toRows()),
        (schema(file), file.toRows()),
        (batches.isEmpty ? '' : schema(batches.first), rows),
      ];
    }

    void same(
      List<(String, List<Map<String, dynamic>>)> results,
      String schema,
      List<Map<String, dynamic>> rows,
    ) {
      for (final (index, (s, r)) in results.indexed) {
        final path = ['readString', 'readFile', 'readBatches'][index];
        expect(s, schema, reason: path);
        expect(r, rows, reason: path);
      }
    }

    test('a comment line before the header', () async {
      same(
        await read('# exported\na,b\n1,2\n', const CsvOptions(comment: '#')),
        'a:int64,b:int64',
        [
          {'a': 1, 'b': 2},
        ],
      );
    });

    test('a blank line before the header', () async {
      same(await read('\na,b\n1,2\n'), 'a:int64,b:int64', [
        {'a': 1, 'b': 2},
      ]);
    });

    test('carriage-return line endings', () async {
      same(await read('a,b\r1,2\r3,4\r'), 'a:int64,b:int64', [
        {'a': 1, 'b': 2},
        {'a': 3, 'b': 4},
      ]);
    });

    test('a header-only file keeps its columns', () async {
      same(await read('a,b\n'), 'a:string,b:string', []);
    });

    test('an empty header cell gets one name everywhere', () async {
      same(await read('a,,c\n1,2,3\n'), 'a:int64,column_1:int64,c:int64', [
        {'a': 1, 'column_1': 2, 'c': 3},
      ]);
    });

    test('a CRLF inside a quoted field is kept', () async {
      same(await read('a,b\r\n"x\r\ny",2\r\n'), 'a:string,b:int64', [
        {'a': 'x\r\ny', 'b': 2},
      ]);
    });

    test('maxRows counts data rows, not the header', () async {
      final text = await CsvReader().readString('a\n1\n2\n', maxRows: 2).ok;
      expect(text.rowCount, 2);
    });
  });

  group('reading & writing', () {
    group('CSV Reading Tests', () {
      late Directory tempDir;

      setUp(() {
        tempDir = Directory.systemTemp.createTempSync('csv_test_');
      });

      tearDown(() {
        if (tempDir.existsSync()) {
          tempDir.deleteSync(recursive: true);
        }
      });

      test('reads basic CSV file', () async {
        final csvFile = File('${tempDir.path}/basic.csv');
        csvFile.writeAsStringSync('''id,name,age
1,Alice,30
2,Bob,25
3,Charlie,35''');

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        expect(table.rowCount, equals(3));
        expect(table.columnCount, equals(3));
        expect(table.schema.fieldNames, equals(['id', 'name', 'age']));

        final rows = table.toRows();
        expect(rows[0], equals({'id': 1, 'name': 'Alice', 'age': 30}));
        expect(rows[1], equals({'id': 2, 'name': 'Bob', 'age': 25}));
        expect(rows[2], equals({'id': 3, 'name': 'Charlie', 'age': 35}));
      });

      test('handles CSV with custom delimiter', () async {
        final csvFile = File('${tempDir.path}/pipe_delimited.csv');
        csvFile.writeAsStringSync('''id|name|value
1|Item A|100.5
2|Item B|200.75''');

        final df = await DataFrameIO.readCsv(
          csvFile.path,
          options: CsvOptions(delimiter: '|'),
        ).ok;
        final table = await df.execute().ok;

        expect(table.rowCount, equals(2));
        final rows = table.toRows();
        expect(rows[0]['name'], equals('Item A'));
        expect(rows[1]['value'], equals(200.75));
      });

      test('handles CSV with quotes and commas', () async {
        final csvFile = File('${tempDir.path}/quoted.csv');
        csvFile.writeAsStringSync('''name,description,notes
"John Doe","A person, with comma","Simple note"
"Jane Smith","Another ""quoted"" value","Has ""quotes"" in notes"
"Bob, Jr.","Name with, comma","Also has, comma here"''');

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        final rows = table.toRows();
        // Test comma handling in quoted fields
        expect(rows[0]['description'], equals('A person, with comma'));
        expect(rows[2]['name'], equals('Bob, Jr.'));
        expect(rows[2]['notes'], equals('Also has, comma here'));

        // Test escaped double quotes (RFC 4180 standard)
        expect(rows[1]['description'], equals('Another "quoted" value'));
        expect(rows[1]['notes'], equals('Has "quotes" in notes'));
      });

      test('handles empty values and nulls', () async {
        final csvFile = File('${tempDir.path}/nulls.csv');
        csvFile.writeAsStringSync('''id,name,value
1,Alice,100
2,,200
3,Charlie,
4,David,0''');

        // Use options that treat empty strings as null for backward compatibility
        final options = CsvOptions(
          nullValues: const {
            '',
            'NULL',
            'null',
            'NA',
            'N/A',
            'n/a',
            'NaN',
            'nan',
          },
        );
        final df = await DataFrameIO.readCsv(csvFile.path, options: options).ok;
        final table = await df.execute().ok;

        final rows = table.toRows();
        expect(rows[1]['name'], isNull);
        expect(rows[2]['value'], isNull);
        expect(rows[3]['value'], equals(0));
      });

      test('infers data types correctly', () async {
        final csvFile = File('${tempDir.path}/types.csv');
        csvFile.writeAsStringSync('''int_col,float_col,bool_col,string_col
42,3.14,true,hello
100,2.718,false,world
-5,0.0,true,test''');

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        final schema = table.schema;
        // Integer columns infer as int64 in both the eager and streaming paths
        // (aligned them; readBatches can't narrow without seeing all rows).
        expect(schema.fields[0].dataType, equals(DataType.int64));
        expect(schema.fields[1].dataType, equals(DataType.float64));
        expect(schema.fields[2].dataType, equals(DataType.boolean));
        expect(schema.fields[3].dataType, equals(DataType.string));
      });

      test('handles large CSV file', () async {
        final csvFile = File('${tempDir.path}/large.csv');
        final buffer = StringBuffer('id,value\n');
        for (int i = 0; i < 10000; i++) {
          buffer.writeln('$i,${i * 2.5}');
        }
        csvFile.writeAsStringSync(buffer.toString());

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        expect(table.rowCount, equals(10000));
        final rows = table.toRows();
        expect(rows[0]['id'], equals(0));
        expect(rows[9999]['value'], equals(24997.5));
      });

      test('handles RFC 4180 quote escaping correctly', () async {
        final csvFile = File('${tempDir.path}/rfc4180_quotes.csv');
        // Test various RFC 4180 quote scenarios
        csvFile.writeAsStringSync('''text,description
"Simple text","No special chars"
"He said ""Hello"" to me","Double quotes escaped by doubling"
"""Already quoted""","Triple quotes: outer are field delimiters, inner doubled become single"
"","Empty quoted field"
,"Empty unquoted field"''');

        final df = await DataFrameIO.readCsv(
          csvFile.path,
          options: CsvOptions(nullValues: {''}),
        ).ok;
        final table = await df.execute().ok;

        final rows = table.toRows();

        // Standard quoted field
        expect(rows[0]['text'], equals('Simple text'));

        // Escaped quotes within quoted field
        expect(rows[1]['text'], equals('He said "Hello" to me'));
        expect(
          rows[1]['description'],
          equals('Double quotes escaped by doubling'),
        );

        // Triple quotes parse as quoted field containing quotes
        // """Already quoted""" -> "Already quoted" (outer quotes are field delimiters, "" becomes ")
        expect(rows[2]['text'], equals('"Already quoted"'));

        // Empty quoted field remains empty string
        expect(rows[3]['text'], equals(''));

        // Empty unquoted field becomes null (with nullValues option)
        expect(rows[4]['text'], isNull);
      });

      test('handles CSV without header', () async {
        final csvFile = File('${tempDir.path}/no_header.csv');
        csvFile.writeAsStringSync('''1,Alice,30
2,Bob,25
3,Charlie,35''');

        final df = await DataFrameIO.readCsv(
          csvFile.path,
          options: CsvOptions(hasHeader: false),
        ).ok;
        final table = await df.execute().ok;

        expect(table.rowCount, equals(3));
        expect(
          table.schema.fieldNames,
          equals(['column_0', 'column_1', 'column_2']),
        );

        final rows = table.toRows();
        expect(rows[0]['column_0'], equals(1));
        expect(rows[0]['column_1'], equals('Alice'));
      });

      test('parses a valid multiline quoted field', () async {
        final csvFile = File('${tempDir.path}/multiline.csv');
        csvFile.writeAsStringSync('id,note\n1,"line one\nline two"\n2,plain');

        final table = await (await DataFrameIO.readCsv(
          csvFile.path,
        ).ok).execute().ok;
        final rows = table.toRows();
        expect(rows[0]['note'], equals('line one\nline two'));
        expect(rows[1]['note'], equals('plain'));
      });

      test('throws a line-numbered error on an unterminated quote', () async {
        final csvFile = File('${tempDir.path}/unterminated.csv');
        csvFile.writeAsStringSync('id,note\n1,"oops never closed\n2,more');

        expect(
          DataFrameIO.readCsv(csvFile.path),
          failsWith(Kind.corrupt, contains('line 2')),
        );
      });

      test('streaming read also rejects an unterminated quote', () async {
        final csvFile = File('${tempDir.path}/unterminated_stream.csv');
        csvFile.writeAsStringSync('id,note\n1,"oops never closed\n2,more');

        expect(
          DataFrameIO.readCsvBatches(csvFile.path).last,
          failsWith(Kind.corrupt),
        );
      });
    });

    // CSV inference is string-parse based and never consulted the shared
    // numeric lattice, so a column mixing an integer with a float inferred
    // float64: rounding any integer past 2^53. Schema.infer (and therefore
    // JSON) already promotes such a column to a lossless decimal; CSV now
    // matches, deriving the scale from the sampled values.
    group('mixed integer/float columns infer a lossless decimal', () {
      late Directory dir;
      setUp(() => dir = Directory.systemTemp.createTempSync('csvdec_'));
      tearDown(() => dir.deleteSync(recursive: true));

      Future<Table> read(String body, [CsvOptions? options]) async {
        final file = File('${dir.path}/t.csv');
        file.writeAsStringSync(body);
        return CsvReader(options: options).readFile(file.path).ok;
      }

      test('a large int mixed with a float is not rounded', () async {
        // float64 would round 9007199254740993 down to ...992.
        final t = await read('v\n9007199254740993\n0.5\n');
        expect(t.schema.field('v')!.dataType, DataType.decimal);
        expect(t.toRows().map((r) => r['v'].toString()), [
          '9007199254740993.0',
          '0.5',
        ]);
      });

      test('the decimal scale comes from the data', () async {
        final t = await read('v\n1\n2.25\n');
        final field = t.schema.field('v')!;
        expect(field.dataType, DataType.decimal);
        expect(field.scale, 2);
        expect(t.toRows().map((r) => r['v'].toString()), ['1.00', '2.25']);
      });

      test('an all-integer column stays an int', () async {
        // int64, not int8: the streaming path can't narrow without seeing every
        // row, and both paths are deliberately aligned on that.
        final t = await read('v\n1\n2\n');
        expect(t.schema.field('v')!.dataType, DataType.int64);
      });

      test('an all-float column stays float64', () async {
        // No integer to lose, so there is nothing to protect.
        final t = await read('v\n1.5\n2.25\n');
        expect(t.schema.field('v')!.dataType, DataType.float64);
      });

      test('a non-finite value keeps the column float64', () async {
        // Infinity cannot be a decimal: fall back rather than throw. (NaN is a
        // default null token, so it never reaches inference as a value.)
        final t = await read('v\n1\nInfinity\n');
        expect(t.schema.field('v')!.dataType, DataType.float64);
      });

      test('nulls are preserved', () async {
        // An empty line is skipped and '' is not a null token (RFC 4180), so
        // use an explicit one.
        final t = await read('v\n1\nNULL\n2.5\n');
        final rows = t.toRows();
        expect(rows[1]['v'], isNull);
        expect(rows.map((r) => r['v']?.toString()), ['1.0', null, '2.5']);
      });

      test('the decimal column round-trips through the writer', () async {
        final t = await read('v\n1\n2.25\n');
        final out = '${dir.path}/out.csv';
        await CsvWriter().writeTable(t, out).ok;
        expect(File(out).readAsStringSync(), contains('2.25'));
      });

      group('eager paths keep the decimal scale (audit 7)', () {
        // The eager returns rebuilt their schema from the built columns via
        // _schemaOf, which dropped scale/precision: the unscaled BigInt then
        // rendered at scale 0 (2.25 as 225, a 100x lie). The streaming
        // readFile path kept the inferred schema and was correct, which is
        // why every existing decimal test missed this.
        void check(Table t) {
          expect(t.schema.field('v')!.dataType, DataType.decimal);
          expect(t.schema.field('v')!.scale, 2);
          expect(t.toRows().map((r) => r['v'].toString()), ['1.00', '2.25']);
        }

        test('readString', () async {
          check(await CsvReader().readString('v\n1\n2.25').ok);
        });

        test('readFile with a custom line terminator (eager path)', () async {
          final file = File('${dir.path}/rs.csv');
          file.writeAsStringSync('v;1;2.25', flush: true);
          check(
            await CsvReader(
              options: const CsvOptions(lineTerminator: ';'),
            ).readFile(file.path).ok,
          );
        });

        test('a numeric custom null token nulls on the fast path', () async {
          // The unquoted-integer fast path skipped isNullToken, storing a
          // configured `-1` as the integer -1 instead of null.
          final t = await CsvReader(
            options: const CsvOptions(nullValues: {'-1'}),
          ).readString('v\n5\n-1\n7').ok;
          expect(t.toRows().map((r) => r['v']), [5, null, 7]);
        });

        test('hex-looking strings stay strings (audit 7)', () async {
          // Dart's int.tryParse accepts 0x literals, so a column of '0xFF'
          // silently inferred int64 holding 255.
          final t = await CsvReader().readString('v\n0xFF\n0x10').ok;
          expect(t.schema.field('v')!.dataType, DataType.string);
          expect(t.toRows().map((r) => r['v']), ['0xFF', '0x10']);
        });

        test('an explicit narrow-int schema fails loud on overflow', () async {
          // The builder silently WIDENED (int8 -> int32 -> int64) past the
          // caller's declared type; narrow widths only ever come from an
          // explicit schema (inference always picks int64), so an
          // out-of-range value violates that contract.
          Schema narrow(DataType t) => Schema(
            fields: [Field(name: 'v', dataType: t, nullable: false)],
          );
          await expectLater(
            CsvReader(
              options: CsvOptions(schema: narrow(DataType.int8)),
            ).readString('v\n300'),
            failsWith(Kind.corrupt),
          );
          await expectLater(
            CsvReader(
              options: CsvOptions(schema: narrow(DataType.int16)),
            ).readString('v\n40000'),
            failsWith(Kind.corrupt),
          );
          // In-range values keep the declared type (int16 was silently int32).
          final ok = await CsvReader(
            options: CsvOptions(schema: narrow(DataType.int16)),
          ).readString('v\n123').ok;
          expect(ok.schema.field('v')!.dataType, DataType.int16);
          expect(ok.toRows().single['v'], 123);
        });

        test('an explicit decimal schema keeps its scale', () async {
          final t = await CsvReader(
            options: CsvOptions(
              schema: Schema(
                fields: [
                  Field(
                    name: 'v',
                    dataType: DataType.decimal,
                    nullable: false,
                    precision: 10,
                    scale: 2,
                  ),
                ],
              ),
            ),
          ).readString('v\n1\n2.25').ok;
          check(t);
        });
      });
    });

    // typeInferenceSampleSize was inert: _inferSchema re-capped every column to
    // a hardcoded 100 values, so raising the option changed nothing and a
    // wider value appearing after row 100 was missed: the exact hazard
    // Table.fromRows refuses to take by inferring over ALL rows.
    group('typeInferenceSampleSize is honoured', () {
      late Directory dir;
      setUp(() => dir = Directory.systemTemp.createTempSync('csvsample_'));
      tearDown(() => dir.deleteSync(recursive: true));

      Future<Table> read(String body, CsvOptions options) async {
        final file = File('${dir.path}/t.csv');
        file.writeAsStringSync(body);
        return CsvReader(options: options).readFile(file.path).ok;
      }

      String wideAt(int row) {
        final b = StringBuffer('v\n');
        for (var i = 0; i < 130; i++) {
          b.writeln(i == row ? '3000000000' : '1');
        }
        return b.toString();
      }

      test('a wide int past row 100 widens the column', () async {
        // Sampling only 100 values would infer int8 and then throw on 3e9.
        final t = await read(wideAt(129), const CsvOptions());
        expect(t.schema.field('v')!.dataType, DataType.int64);
        expect(t.column('v')!.toList()[129], 3000000000);
      });

      test('a float past row 100 is seen', () async {
        final b = StringBuffer('v\n');
        for (var i = 0; i < 130; i++) {
          b.writeln(i == 129 ? '0.5' : '1');
        }
        final t = await read(b.toString(), const CsvOptions());
        expect(t.schema.field('v')!.dataType, DataType.decimal);
      });

      test('a smaller sample size is respected as a bound', () async {
        // With only 10 rows sampled the late float is invisible, so the column
        // infers int and the read fails loud rather than silently truncating.
        await expectLater(
          read(
            'v\n${List.filled(20, '1').join('\n')}\n0.5\n',
            const CsvOptions(typeInferenceSampleSize: 10),
          ),
          throwsA(anything),
        );
      });

      test(
        'a column full in the sample but empty later reads null, not crash',
        () async {
          // Rows 1-2 (the whole sample) populate `n`; row 3 leaves it empty. A
          // sample can't prove non-nullability, so the empty cell must read as null
          // rather than crash a non-nullable int builder.
          final t = await read(
            'n,other\n1,a\n2,b\n,c',
            const CsvOptions(typeInferenceSampleSize: 2),
          );
          expect(t.schema.field('n')!.nullable, isTrue);
          expect(t.column('n')!.toList(), [1, 2, null]);
        },
      );
    });

    group('CSV Writing Tests', () {
      late Directory tempDir;

      setUp(() {
        tempDir = Directory.systemTemp.createTempSync('csv_write_test_');
      });

      tearDown(() {
        if (tempDir.existsSync()) {
          tempDir.deleteSync(recursive: true);
        }
      });

      test('writes basic CSV file', () async {
        final outputFile = File('${tempDir.path}/output.csv');

        final data = [
          {'id': 1, 'name': 'Alice', 'age': 30},
          {'id': 2, 'name': 'Bob', 'age': 25},
          {'id': 3, 'name': 'Charlie', 'age': 35},
        ];

        final df = DataFrame.fromRows(data);
        await df.writeCsv(outputFile.path).ok;

        final content = outputFile.readAsStringSync();
        expect(content, contains('id,name,age'));
        expect(content, contains('1,Alice,30'));
        expect(content, contains('2,Bob,25'));
        expect(content, contains('3,Charlie,35'));
      });

      test('writes CSV with custom delimiter', () async {
        final outputFile = File('${tempDir.path}/output_pipe.csv');

        final data = [
          {'id': 1, 'value': 100.5},
          {'id': 2, 'value': 200.75},
        ];

        final df = DataFrame.fromRows(data);
        await df
            .writeCsv(outputFile.path, options: CsvOptions(delimiter: '|'))
            .ok;

        final content = outputFile.readAsStringSync();
        expect(content, contains('id|value'));
        expect(content, contains('1|100.5'));
        expect(content, contains('2|200.75'));
      });

      test('writes CSV with quotes when needed', () async {
        final outputFile = File('${tempDir.path}/output_quoted.csv');

        final data = [
          {'name': 'John, Doe', 'description': 'Has "quotes" here'},
          {'name': 'Jane Smith', 'description': 'Normal text'},
        ];

        final df = DataFrame.fromRows(data);
        await df.writeCsv(outputFile.path).ok;

        final content = outputFile.readAsStringSync();
        expect(content, contains('"John, Doe"'));
        expect(content, contains('"Has ""quotes"" here"'));
      });

      test('writes CSV with null values', () async {
        final outputFile = File('${tempDir.path}/output_nulls.csv');

        final data = [
          {'id': 1, 'name': 'Alice', 'value': 100},
          {'id': 2, 'name': null, 'value': 200},
          {'id': 3, 'name': 'Charlie', 'value': null},
        ];

        final df = DataFrame.fromRows(data);
        await df.writeCsv(outputFile.path).ok;

        final content = outputFile.readAsStringSync();
        final lines = content.trim().split('\n');
        expect(lines[2], equals('2,,200'));
        expect(lines[3], equals('3,Charlie,'));
      });

      test('writes CSV without header', () async {
        final outputFile = File('${tempDir.path}/output_no_header.csv');

        final data = [
          {'id': 1, 'name': 'Alice'},
          {'id': 2, 'name': 'Bob'},
        ];

        final df = DataFrame.fromRows(data);
        await df
            .writeCsv(outputFile.path, options: CsvOptions(hasHeader: false))
            .ok;

        final content = outputFile.readAsStringSync();
        expect(content, isNot(contains('id,name')));
        // No leading blank line: the first byte is the first data value.
        expect(content.split('\n').first, equals('1,Alice'));
        expect(content.startsWith('1,Alice'), isTrue);
      });

      test('preserves data types in round-trip', () async {
        final outputFile = File('${tempDir.path}/types_roundtrip.csv');

        final data = [
          {'int': 42, 'float': 3.14, 'bool': true, 'string': 'hello'},
          {'int': -100, 'float': 2.718, 'bool': false, 'string': 'world'},
        ];

        final df1 = DataFrame.fromRows(data);
        await df1.writeCsv(outputFile.path).ok;

        final df2 = await DataFrameIO.readCsv(outputFile.path).ok;
        final table = await df2.execute().ok;

        final rows = table.toRows();
        expect(rows[0]['int'], equals(42));
        expect(rows[0]['float'], equals(3.14));
        expect(rows[0]['bool'], equals(true));
        expect(rows[1]['string'], equals('world'));
      });
    });
  });

  group('options', () {
    /// CsvOptions that were previously declared-but-ignored are
    /// now honored (in both the eager and streaming paths).
    Future<List<Map<String, dynamic>>> readRows(Table t) async => t.toRows();

    group('copyWith can clear nullable options (finding 12)', () {
      test('an explicit null clears a set option', () {
        const base = CsvOptions(maxRows: 100, thousandsSeparator: ',');
        final cleared = base.copyWith(maxRows: null, thousandsSeparator: null);
        expect(cleared.maxRows, isNull);
        expect(cleared.thousandsSeparator, isNull);
      });

      test('an omitted option is retained', () {
        const base = CsvOptions(maxRows: 100, maxFileBytes: 500);
        final same = base.copyWith(delimiter: ';');
        expect(same.maxRows, 100);
        expect(same.maxFileBytes, 500);
        expect(same.delimiter, ';');
      });

      test('a new value still replaces the old', () {
        const base = CsvOptions(maxRows: 100);
        expect(base.copyWith(maxRows: 7).maxRows, 7);
      });
    });

    group('control options', () {
      test('skipRows drops preamble before the header', () async {
        final df = await CsvReader(
          options: const CsvOptions(skipRows: 2),
        ).readString('junk1\njunk2\na,b\n1,2\n3,4').ok;
        expect(await readRows(df), [
          {'a': 1, 'b': 2},
          {'a': 3, 'b': 4},
        ]);
      });

      test('maxRows caps the row count', () async {
        final df = await CsvReader(
          options: const CsvOptions(maxRows: 2),
        ).readString('a\n1\n2\n3\n4').ok;
        expect((await readRows(df)).length, 2);
      });

      test('columnNames names a headerless file', () async {
        final df = await CsvReader(
          options: const CsvOptions(hasHeader: false, columnNames: ['x', 'y']),
        ).readString('1,2\n3,4').ok;
        expect(await readRows(df), [
          {'x': 1, 'y': 2},
          {'x': 3, 'y': 4},
        ]);
      });

      test('inferTypes:false keeps every column as strings', () async {
        final df = await CsvReader(
          options: const CsvOptions(inferTypes: false),
        ).readString('a,b\n1,2.5').ok;
        final t = df;
        expect(
          t.schema.fields.every((f) => f.dataType == DataType.string),
          true,
        );
        expect(t.toRows(), [
          {'a': '1', 'b': '2.5'},
        ]);
      });
    });

    group('allowVariableFields (row width)', () {
      test('a ragged row is rejected by default', () {
        expect(
          CsvReader().readString('a,b,c\n1,2,3\n4,5'),
          failsWith(Kind.corrupt),
        );
      });

      test('ragged rows permitted when allowVariableFields is true', () async {
        final df = await CsvReader(
          options: const CsvOptions(allowVariableFields: true),
        ).readString('a,b,c\n1,2,3\n4,5').ok;
        final rows = await readRows(df);
        expect(rows.length, 2);
        expect(rows[1]['c'], isNull); // short row null-padded
      });
    });

    group('locale numerics (european)', () {
      test('comma decimal and dot thousands parse as numbers', () async {
        final df = await CsvReader(
          options: CsvOptions.european(),
        ).readString('price;qty\n1.234,50;3\n9,99;10').ok;
        final t = df;
        expect(t.schema.fields[0].dataType, DataType.float64);
        expect(t.schema.fields[1].dataType, DataType.int64);
        expect(t.toRows(), [
          {'price': 1234.5, 'qty': 3},
          {'price': 9.99, 'qty': 10},
        ]);
      });
    });

    group('temporal', () {
      test('dateFormat infers and parses a date column', () async {
        final df = await CsvReader(
          options: const CsvOptions(dateFormat: 'dd/MM/yyyy'),
        ).readString('d\n31/12/2023\n01/06/2024').ok;
        final t = df;
        expect(t.schema.fields[0].dataType, DataType.date);
        expect(t.column('d')!.toList(), [
          DateTime.utc(2023, 12, 31),
          DateTime.utc(2024, 6, 1),
        ]);
      });

      test('timestampFormat parses date + time', () async {
        final df = await CsvReader(
          options: const CsvOptions(timestampFormat: 'yyyy-MM-dd HH:mm:ss'),
        ).readString('ts\n2023-01-02 03:04:05').ok;
        final t = df;
        expect(t.schema.fields[0].dataType, DataType.timestamp);
        expect(t.column('ts')!.toList(), [DateTime.utc(2023, 1, 2, 3, 4, 5)]);
      });
    });

    group('custom escape', () {
      test('backslash escape inside quotes', () async {
        final df = await CsvReader(
          options: const CsvOptions(escape: '\\'),
        ).readString('a\n"x\\"y"').ok;
        expect((await readRows(df)).single['a'], 'x"y');
      });
    });

    group('eager and streaming agree', () {
      test('readFile and readBatches give identical schema + rows', () async {
        final dir = Directory.systemTemp.createTempSync('csvparity_');
        try {
          final path = '${dir.path}/d.csv';
          File(path).writeAsStringSync(
            'id,v\n${[for (var i = 0; i < 50; i++) '$i,${i * 2}'].join('\n')}\n',
          );
          final eager = await CsvReader().readFile(path).ok;
          final eagerT = eager;
          final batched = <Map<String, dynamic>>[];
          Schema? batchSchema;
          await for (final b
              in CsvReader().readBatches(path, batchSize: 7).ok) {
            batchSchema ??= b.schema;
            batched.addAll(b.toRows());
          }
          expect(
            batchSchema!.fields.map((f) => '${f.name}:${f.dataType.name}'),
            eagerT.schema.fields.map((f) => '${f.name}:${f.dataType.name}'),
          );
          expect(batched, eagerT.toRows());
        } finally {
          dir.deleteSync(recursive: true);
        }
      });
    });

    group('writer options', () {
      test('lineTerminator writes CRLF and round-trips', () async {
        final dir = Directory.systemTemp.createTempSync('csv_crlf_');
        try {
          final path = '${dir.path}/out.csv';
          final df = DataFrame.fromRows([
            {'id': 1, 'name': 'a'},
            {'id': 2, 'name': 'b'},
          ]);
          await df
              .writeCsv(path, options: const CsvOptions(lineTerminator: '\r\n'))
              .ok;

          final text = File(path).readAsStringSync();
          expect(text, contains('\r\n'));
          // Every record boundary is CRLF, so no bare LF is left unpaired.
          expect(text.replaceAll('\r\n', '').contains('\n'), isFalse);

          final back = await readRows(
            await (await DataFrameIO.readCsv(path).ok).execute().ok,
          );
          expect(back, [
            {'id': 1, 'name': 'a'},
            {'id': 2, 'name': 'b'},
          ]);
        } finally {
          dir.deleteSync(recursive: true);
        }
      });
    });
  });

  group('schema inference', () {
    group('CSV explicit schema', () {
      test('uses provided types and skips inference', () async {
        final schema = Schema(
          fields: [
            Field(name: 'a', dataType: DataType.int64, nullable: false),
            Field(name: 'b', dataType: DataType.float64, nullable: false),
            Field(name: 'c', dataType: DataType.string, nullable: false),
          ],
        );
        final df = await CsvReader(
          options: CsvOptions(schema: schema),
        ).readString('a,b,c\n1,2.5,x\n2,3.5,y').ok;
        final table = df;

        // Inference would have called column "a" int8; the schema forces int64.
        expect(table.schema.fields[0].dataType, equals(DataType.int64));
        expect(table.schema.fields[1].dataType, equals(DataType.float64));
        expect(table.schema.fields[2].dataType, equals(DataType.string));

        expect(table.column('a')!.toList(), equals([1, 2]));
        expect(table.column('b')!.toList(), equals([2.5, 3.5]));
        expect(table.column('c')!.toList(), equals(['x', 'y']));
      });
    });

    group('CSV typed builder auto-widen', () {
      test(
        'inferred integer columns are int64 (no width drift / wrap)',
        () async {
          // inferred ints are int64 in both paths, so a value beyond the
          // sampled width never wraps regardless of the sample.
          final df = await CsvReader(
            options: const CsvOptions(typeInferenceSampleSize: 2),
          ).readString('n\n1\n2\n300').ok;
          final table = df;

          expect(table.schema.fields[0].dataType, equals(DataType.int64));
          expect(table.column('n')!.toList(), equals([1, 2, 300])); // no wrap
        },
      );
    });

    group('CSV bad-cell handling', () {
      test('unparseable numeric throws by default (strict)', () async {
        final schema = Schema(
          fields: [Field(name: 'n', dataType: DataType.int32, nullable: true)],
        );
        // Strict is the default: a malformed cell is an ingestion error, not data.
        await expectLater(
          CsvReader(
            options: CsvOptions(schema: schema),
          ).readString('n\n1\nabc\n3'),
          failsWith(Kind.corrupt),
        );
      });

      test(
        'unparseable numeric becomes null when nullable + strict:false',
        () async {
          final schema = Schema(
            fields: [
              Field(name: 'n', dataType: DataType.int32, nullable: true),
            ],
          );
          final df = await CsvReader(
            options: CsvOptions(schema: schema, strict: false),
          ).readString('n\n1\nabc\n3').ok;
          final table = df;

          expect(table.column('n')!.toList(), equals([1, null, 3]));
        },
      );

      test('unparseable numeric throws when non-nullable', () async {
        final schema = Schema(
          fields: [Field(name: 'n', dataType: DataType.int32, nullable: false)],
        );
        // Even permissive mode errors on a non-nullable column.
        await expectLater(
          CsvReader(
            options: CsvOptions(schema: schema, strict: false),
          ).readString('n\n1\nabc'),
          failsWith(Kind.corrupt),
        );
      });

      test('bad boolean is not silently false', () async {
        final schema = Schema(
          fields: [
            Field(name: 'b', dataType: DataType.boolean, nullable: true),
          ],
        );
        // strict default: garbage throws
        await expectLater(
          CsvReader(
            options: CsvOptions(schema: schema),
          ).readString('b\ntrue\ntru'),
          failsWith(Kind.corrupt),
        );
        // permissive: garbage bool -> null (NOT false)
        final df = await CsvReader(
          options: CsvOptions(schema: schema, strict: false),
        ).readString('b\ntrue\ntru\nfalse').ok;
        expect(df.column('b')!.toList(), equals([true, null, false]));
      });
    });
  });

  group('streaming', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('csv_stream_'));
    tearDown(() => dir.deleteSync(recursive: true));

    String write(String name, String content) {
      final p = '${dir.path}/$name';
      File(p).writeAsStringSync(content);
      return p;
    }

    test('streams all rows in bounded batches with a stable schema', () async {
      final rows = [for (int i = 0; i < 250; i++) '$i,name_$i,${i * 1.5}'];
      final path = write('d.csv', 'id,name,score\n${rows.join('\n')}');

      final batches = await DataFrameIO.readCsvBatches(
        path,
        batchSize: 100,
      ).ok.toList();
      expect(batches.length, 3); // 100 + 100 + 50

      final tables = [for (final b in batches) await b.execute().ok];
      expect(tables.map((t) => t.rowCount).toList(), [100, 100, 50]);

      // One stable schema across every batch.
      final schemas = tables
          .map(
            (t) =>
                t.schema.fields.map((f) => '${f.name}:${f.dataType}').join(','),
          )
          .toSet();
      expect(
        schemas.length,
        1,
        reason: 'schema must be identical across batches',
      );

      // Inferred integer column is int64 (stable, overflow-proof).
      expect(tables.first.column('id'), isA<NumericColumn>());
      expect(tables.first.column('id')!.dataType, DataType.int64);

      final ids = [
        for (final t in tables) ...t.column('id')!.toList().cast<int>(),
      ];
      expect(ids, List.generate(250, (i) => i));
    });

    test('a large value beyond the sample does not overflow (int64)', () async {
      // First rows are tiny (would infer int8 in whole-file mode); a late row is huge.
      final rows = [for (int i = 0; i < 50; i++) '$i'];
      rows.add('9000000000'); // > int32, appears after the sample window
      final path = write('big.csv', 'v\n${rows.join('\n')}');

      final tables = [
        for (final b in await DataFrameIO.readCsvBatches(
          path,
          batchSize: 16,
        ).ok.toList())
          await b.execute().ok,
      ];
      final vals = [
        for (final t in tables) ...t.column('v')!.toList().cast<int>(),
      ];
      expect(vals.last, 9000000000);
      expect(
        tables.every((t) => t.column('v')!.dataType == DataType.int64),
        isTrue,
      );
    });

    test('filter and limit are honored across batches', () async {
      final rows = [for (int i = 0; i < 100; i++) '$i,${i % 2}'];
      final path = write('f.csv', 'id,grp\n${rows.join('\n')}');

      final filtered = [
        for (final b in await DataFrameIO.readCsvBatches(
          path,
          batchSize: 30,
          filter: RowFilter.compare('grp', FilterOp.eq, 1),
        ).ok.toList())
          ...(await b.execute().ok).column('id')!.toList().cast<int>(),
      ];
      expect(filtered.every((id) => id.isOdd), isTrue);
      expect(filtered.length, 50);

      final limited = [
        for (final b in await DataFrameIO.readCsvBatches(
          path,
          batchSize: 30,
          limit: 25,
        ).ok.toList())
          ...(await b.execute().ok).column('id')!.toList().cast<int>(),
      ];
      expect(limited.length, 25);
    });

    test('quoted fields spanning newlines stay one record', () async {
      final path = write('q.csv', 'id,note\n1,"line one\nline two"\n2,plain');
      final t = await (await DataFrameIO.readCsvBatches(
        path,
        batchSize: 10,
      ).ok.toList()).map((b) => b.execute().ok).first;
      final table = t;
      expect(table.rowCount, 2);
      expect(table.column('note')!.toList()[0], 'line one\nline two');
    });

    test('matches whole-file readCsv on values', () async {
      final rows = [for (int i = 0; i < 120; i++) '$i,v$i'];
      final path = write('m.csv', 'id,name\n${rows.join('\n')}');

      final whole = await (await DataFrameIO.readCsv(path).ok).execute().ok;
      final streamed = [
        for (final b in await DataFrameIO.readCsvBatches(
          path,
          batchSize: 50,
        ).ok.toList())
          ...(await b.execute().ok).column('name')!.toList(),
      ];
      expect(streamed, whole.column('name')!.toList());
    });
  });

  group('strict quotes', () {
    /// Strict CSV (the default) must reject stray characters after a closing quote
    /// (`"abc"x`) rather than silently truncating the field to `abc`. Lenient mode
    /// keeps the legacy end-the-record behavior.
    const content = 'c\n"abc"x';

    Future<List<Object?>> read(bool strict) async {
      final df = await CsvReader(
        options: CsvOptions(strict: strict),
      ).readString(content).ok;
      final table = df;
      return table.toRows().map((r) => r['c']).toList();
    }

    test('strict mode fails on text after a closing quote', () {
      expect(
        CsvReader(options: CsvOptions(strict: true)).readString(content),
        failsWith(Kind.corrupt),
      );
    });

    test(
      'lenient mode parses the quoted content and ends the record',
      () async {
        expect(await read(false), ['abc']);
      },
    );
  });

  group('RFC 4180 compliance', () {
    group('RFC 4180 Compliance Tests', () {
      late Directory tempDir;

      setUp(() {
        tempDir = Directory.systemTemp.createTempSync('rfc4180_test_');
      });

      tearDown(() {
        if (tempDir.existsSync()) {
          tempDir.deleteSync(recursive: true);
        }
      });

      test('Rule 1: Records separated by CRLF', () async {
        final csvFile = File('${tempDir.path}/crlf.csv');
        // Use explicit CRLF line endings
        csvFile.writeAsBytesSync([
          // Header
          ...'Name,Age'.codeUnits,
          0x0D, 0x0A, // CRLF
          // Row 1
          ...'Alice,30'.codeUnits,
          0x0D, 0x0A, // CRLF
          // Row 2
          ...'Bob,25'.codeUnits,
          0x0D, 0x0A, // CRLF
        ]);

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        expect(table.rowCount, equals(2));
        final rows = table.toRows();
        expect(rows[0]['Name'], equals('Alice'));
        expect(rows[1]['Name'], equals('Bob'));
      });

      test('Rule 2: Last field not followed by comma', () async {
        final csvFile = File('${tempDir.path}/no_trailing_comma.csv');
        csvFile.writeAsStringSync('Name,Age,City\nAlice,30,NYC\nBob,25,LA');

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        expect(table.columnCount, equals(3));
        final rows = table.toRows();
        expect(rows[0]['City'], equals('NYC'));
        expect(rows[1]['City'], equals('LA'));
      });

      test('Rule 3: Optional header line', () async {
        final csvFile = File('${tempDir.path}/with_header.csv');
        csvFile.writeAsStringSync('Name,Age\nAlice,30\nBob,25');

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        expect(table.schema.fieldNames, equals(['Name', 'Age']));
        expect(table.rowCount, equals(2));
      });

      test(
        'Rule 4: Spaces are part of field (not trimmed by default)',
        () async {
          final csvFile = File('${tempDir.path}/spaces.csv');
          csvFile.writeAsStringSync(
            'Name,Description\n Alice , Has spaces \nBob,No extra spaces',
          );

          final df = await DataFrameIO.readCsv(
            csvFile.path,
            options: CsvOptions(trimFields: false),
          ).ok;
          final table = await df.execute().ok;

          final rows = table.toRows();
          expect(rows[0]['Name'], equals(' Alice '));
          expect(rows[0]['Description'], equals(' Has spaces '));
          expect(rows[1]['Name'], equals('Bob'));
        },
      );

      test('Rule 5: Fields may be enclosed in double quotes', () async {
        final csvFile = File('${tempDir.path}/quoted.csv');
        csvFile.writeAsStringSync('''Name,Age,City
"Alice",30,"New York"
Bob,25,LA
"Charlie","35","San Francisco"''');

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        final rows = table.toRows();
        expect(rows[0]['Name'], equals('Alice'));
        expect(rows[0]['City'], equals('New York'));
        expect(rows[1]['Name'], equals('Bob'));
        expect(rows[2]['Name'], equals('Charlie'));
        expect(rows[2]['Age'], equals(35)); // Should be parsed as number
      });

      test('Rule 6: Line breaks within quoted fields', () async {
        final csvFile = File('${tempDir.path}/multiline.csv');
        csvFile.writeAsStringSync('''Name,Address,Age
"Alice","123 Main St
Apt 4B
NYC",30
"Bob","456 Oak Rd",25''');

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        final rows = table.toRows();
        expect(table.rowCount, equals(2));
        expect(rows[0]['Name'], equals('Alice'));
        expect(rows[0]['Address'], equals('123 Main St\nApt 4B\nNYC'));
        expect(rows[0]['Age'], equals(30));
        expect(rows[1]['Name'], equals('Bob'));
        expect(rows[1]['Address'], equals('456 Oak Rd'));
      });

      test('Rule 7: Double quotes escaped by doubling', () async {
        final csvFile = File('${tempDir.path}/escaped_quotes.csv');
        csvFile.writeAsStringSync('''Name,Quote
"Alice","She said ""Hello"" to me"
"Bob","Uses 6"" nails"
"Charlie","No quotes here"''');

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        final rows = table.toRows();
        expect(rows[0]['Quote'], equals('She said "Hello" to me'));
        expect(rows[1]['Quote'], equals('Uses 6" nails'));
        expect(rows[2]['Quote'], equals('No quotes here'));
      });

      test('Commas within quoted fields', () async {
        final csvFile = File('${tempDir.path}/comma_in_field.csv');
        csvFile.writeAsStringSync('''Name,Amount,Description
"Smith, John","1,234.56","Payment for Jan, Feb, Mar"
Alice,567.89,No commas here''');

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        final rows = table.toRows();
        expect(rows[0]['Name'], equals('Smith, John'));
        expect(rows[0]['Amount'], equals('1,234.56'));
        expect(rows[0]['Description'], equals('Payment for Jan, Feb, Mar'));
        expect(rows[1]['Name'], equals('Alice'));
      });

      test('Empty fields', () async {
        final csvFile = File('${tempDir.path}/empty.csv');
        csvFile.writeAsStringSync('''Name,Middle,Last
Alice,,Smith
,Bob,Jones
Carol,Ann,''');

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        final rows = table.toRows();
        expect(rows[0]['Middle'], equals(''));
        expect(rows[1]['Name'], equals(''));
        expect(rows[2]['Last'], equals(''));
      });

      test('Quoted empty fields', () async {
        final csvFile = File('${tempDir.path}/quoted_empty.csv');
        csvFile.writeAsStringSync('''Name,Value
"","text"
"Alice",""
"",""''');

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        final rows = table.toRows();
        expect(rows[0]['Name'], equals(''));
        expect(
          rows[0]['Value'],
          equals('text'),
        ); // Changed from "0" to avoid type inference issues
        expect(rows[1]['Value'], equals(''));
        expect(rows[2]['Name'], equals(''));
        expect(rows[2]['Value'], equals(''));
      });

      test('Mixed LF and CRLF line endings (lenient parsing)', () async {
        final csvFile = File('${tempDir.path}/mixed_endings.csv');
        csvFile.writeAsBytesSync([
          // Header with LF
          ...'Name,Age'.codeUnits,
          0x0A, // LF
          // Row 1 with CRLF
          ...'Alice,30'.codeUnits,
          0x0D, 0x0A, // CRLF
          // Row 2 with LF
          ...'Bob,25'.codeUnits,
          0x0A, // LF
          // Row 3 with CRLF
          ...'Charlie,35'.codeUnits,
          0x0D, 0x0A, // CRLF
        ]);

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        expect(table.rowCount, equals(3));
        final rows = table.toRows();
        expect(rows[0]['Name'], equals('Alice'));
        expect(rows[1]['Name'], equals('Bob'));
        expect(rows[2]['Name'], equals('Charlie'));
      });

      test('Triple quotes (RFC 4180 interpretation)', () async {
        final csvFile = File('${tempDir.path}/triple_quotes.csv');
        csvFile.writeAsStringSync('''ID,Name,Value
"""ABC123""","John",100
"""DEF456""","""Jane Smith""",200''');

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        final rows = table.toRows();
        // According to RFC 4180, """ABC123""" should parse as "ABC123" (with quotes)
        // The outer quotes are field delimiters, the doubled inner quotes ("") become single quotes (")
        expect(rows[0]['ID'], equals('"ABC123"'));
        expect(rows[0]['Name'], equals('John'));
        expect(rows[1]['ID'], equals('"DEF456"'));
        expect(rows[1]['Name'], equals('"Jane Smith"'));
      });

      test('Complex real-world example', () async {
        final csvFile = File('${tempDir.path}/complex.csv');
        csvFile.writeAsStringSync('''Year,Make,Model,Description,Price
1997,Ford,E350,"ac, abs, moon",3000.00
1999,Chevy,"Venture ""Extended Edition""","",4900.00
1999,Chevy,"Venture ""Extended Edition, Very Large""",,5000.00
1996,Jeep,Grand Cherokee,"MUST SELL! air, moon roof, loaded",4799.00''');

        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;

        expect(table.rowCount, equals(4));

        final rows = table.toRows();

        // First row
        expect(rows[0]['Year'], equals(1997));
        expect(rows[0]['Make'], equals('Ford'));
        expect(rows[0]['Model'], equals('E350'));
        expect(rows[0]['Description'], equals('ac, abs, moon'));
        expect(rows[0]['Price'], equals(3000.00));

        // Second row with escaped quotes
        expect(rows[1]['Model'], equals('Venture "Extended Edition"'));
        expect(rows[1]['Description'], equals(''));

        // Third row with complex escaped quotes
        expect(
          rows[2]['Model'],
          equals('Venture "Extended Edition, Very Large"'),
        );
        expect(rows[2]['Price'], equals(5000.00));

        // Fourth row with line break in description
        expect(rows[3]['Description'], contains('MUST SELL!'));
        expect(rows[3]['Description'], contains('air, moon roof, loaded'));
      });

      test('Performance: Large file with quoted fields', tags: 'perf', () async {
        final csvFile = File('${tempDir.path}/large_quoted.csv');
        final buffer = StringBuffer();

        // Header
        buffer.writeln('ID,Name,Description,Value');

        // Generate 10K rows with various quoting patterns
        for (int i = 0; i < 10000; i++) {
          if (i % 3 == 0) {
            // Quoted with commas
            buffer.writeln('"ID-$i","Name, $i","Description with, commas",$i');
          } else if (i % 3 == 1) {
            // Quoted with escaped quotes
            buffer.writeln('"ID-$i","Name ""$i""","Has ""quotes"" inside",$i');
          } else {
            // Unquoted
            buffer.writeln('ID-$i,Name-$i,Description-$i,$i');
          }
        }

        csvFile.writeAsStringSync(buffer.toString());

        final stopwatch = Stopwatch()..start();
        final df = await DataFrameIO.readCsv(csvFile.path).ok;
        final table = await df.execute().ok;
        stopwatch.stop();

        print('\nPerformance for 10K rows with quotes:');
        print('  Time: ${stopwatch.elapsedMilliseconds}ms');
        print(
          '  Rows/sec: ${(10000 * 1000 / stopwatch.elapsedMilliseconds).toStringAsFixed(0)}',
        );

        expect(table.rowCount, equals(10000));

        // Verify some samples
        final rows = table.toRows();
        expect(rows[0]['Name'], equals('Name, 0'));
        expect(rows[1]['Name'], equals('Name "1"'));
        expect(rows[2]['Name'], equals('Name-2'));
      });
    });
  });

  // ---------------------------------------------------------------------------
  // Relocated from audit/review round suites (CSV-specific coverage).
  // ---------------------------------------------------------------------------
  group('relocated audit/review rounds', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('reloc_csv_'));
    tearDown(() => dir.deleteSync(recursive: true));

    group('#3 CSV lossless null vs empty string (opt-in sentinel)', () {
      test(
        'null, empty string, and single-column rows all round-trip',
        () async {
          final path = '${dir.path}/n.csv';
          final df = DataFrame.fromRows([
            {'s': null},
            {'s': ''},
            {'s': 'x'},
          ]);
          const opts = CsvOptions(nullOutput: r'\N');
          await df.writeCsv(path, options: opts).ok;
          final back = await (await DataFrameIO.readCsv(
            path,
            options: opts,
          ).ok).execute().ok;
          expect(back.rowCount, 3); // no row dropped
          expect(back.column('s')!.toList(), [null, '', 'x']);
        },
      );

      test(
        'default (no sentinel) keeps RFC-4180 behavior: null -> empty',
        () async {
          final path = '${dir.path}/d.csv';
          await DataFrame.fromRows([
            {'a': 1, 'b': null},
          ]).writeCsv(path).ok;
          final text = await File(path).readAsString();
          expect(text.trim().split('\n').last, '1,'); // null is an empty field
        },
      );
    });

    group('#R1/R2 CSV null sentinel correctness', () {
      test('a sentinel containing a special char is rejected', () {
        expect(
          () => CsvWriter(const CsvOptions(nullOutput: 'a,b')),
          throwsArgumentError,
        );
        expect(
          () => CsvWriter(const CsvOptions(nullOutput: 'x"y')),
          throwsArgumentError,
        );
        expect(
          () => CsvWriter(const CsvOptions(nullOutput: '')),
          throwsArgumentError,
        );
      });

      test(
        'a real value equal to the sentinel round-trips as a string',
        () async {
          final path = '${dir.path}/s.csv';
          const opts = CsvOptions(nullOutput: r'\N');
          await DataFrame.fromRows([
            {'s': null},
            {'s': r'\N'}, // real value collides with the sentinel
            {'s': 'x'},
          ]).writeCsv(path, options: opts).ok;
          final back = await (await DataFrameIO.readCsv(
            path,
            options: opts,
          ).ok).execute().ok;
          expect(back.column('s')!.toList(), [null, r'\N', 'x']);
        },
      );
    });

    group('#9 CSV streaming schema freeze + options validation', () {
      Table two(String a, String b) => Table(
        name: 't',
        columns: [
          NumericColumn.int64(name: a, values: [1], nullable: false),
          NumericColumn.int64(name: b, values: [2], nullable: false),
        ],
        schema: Schema(
          fields: [
            Field(name: a, dataType: DataType.int64, nullable: false),
            Field(name: b, dataType: DataType.int64, nullable: false),
          ],
        ),
      );

      test('a reordered later batch is rejected', () async {
        final sink = File('${dir.path}/x.csv').openWrite();
        await expectLater(
          CsvWriter().writeStream(
            Stream.fromIterable([two('a', 'b'), two('b', 'a')]),
            sink,
          ),
          failsWith(Kind.schema),
        );
        await sink.close();
      });

      test('multi-character / empty delimiter is rejected', () {
        expect(
          () => CsvReader(options: const CsvOptions(delimiter: '||')),
          throwsArgumentError,
        );
        expect(
          () => CsvWriter(const CsvOptions(delimiter: '')),
          throwsArgumentError,
        );
      });
    });

    group('#8 readCsv with pushdown enforces caps', () {
      test('an over-cap result throws (maxRows)', () async {
        final path = '${dir.path}/o.csv';
        File(path).writeAsStringSync('a\n1\n2\n3\n');
        // The frame is lazy: its terminal fails.
        final capped = await DataFrameIO.readCsv(
          path,
          batchSize: 1,
          maxRows: 1,
        ).ok;
        await expectLater(capped.execute(), failsWith(Kind.limit));
        // Opt-out reads it all.
        final df = await DataFrameIO.readCsv(
          path,
          batchSize: 1,
          maxRows: null,
        ).ok;
        expect((await df.execute(maxRows: null).ok).rowCount, 3);
      });
    });

    group('#6 CSV bounded / streaming export', () {
      DataFrame frame(int rows) => DataFrame.fromRows([
        for (var i = 0; i < rows; i++) {'id': i, 'name': 'row-$i'},
      ]);

      test('CSV export streams and round-trips a large result', () async {
        final path = '${dir.path}/out.csv';
        await frame(5000).writeCsv(path).ok;
        final back = await (await DataFrameIO.readCsv(
          path,
        ).ok).execute(maxRows: null).ok;
        expect(back.rowCount, 5000);
        final ids = back.column('id')!.toList();
        expect(ids.first, 0);
        expect(ids.last, 4999);
      });
    });

    group('#4 CSV record cap covers unquoted lines', () {
      test('a giant unquoted cell throws instead of OOM', () async {
        final path = '${dir.path}/big.csv';
        // One unquoted line far larger than the cap (no quotes → the old path
        // yielded it unchecked).
        await File(path).writeAsString('h\n${'x' * 5000}\n');
        // The STREAMING path (readBatches) is where an unbounded LineSplitter used
        // to buffer the whole physical line before the cap could see it.
        final reader = CsvReader(
          options: const CsvOptions(maxRecordBytes: 1000),
        );
        await expectLater(
          reader.readBatches(path).last,
          failsWith(Kind.corrupt),
        );
      });
    });

    group('projection guards + readCsv pushdown (regression)', () {
      String writeCsv() {
        final path = '${dir.path}/data.csv';
        final rows = ['id,name', for (int i = 0; i < 20; i++) '$i,item_$i'];
        File(path).writeAsStringSync(rows.join('\n'));
        return path;
      }

      group('projection errors on unknown columns', () {
        test('CSV select of an unknown column throws', () {
          expect(
            DataFrameIO.readCsv(writeCsv(), columns: ['id', 'nope']),
            failsWith(Kind.invalid),
          );
        });

        test('a valid projection still works', () async {
          final df = await DataFrameIO.readCsv(writeCsv(), columns: ['id']).ok;
          final table = await df.execute().ok;
          expect(table.schema.fields.map((f) => f.name), ['id']);
          expect(table.rowCount, 20);
        });
      });

      group('readCsv pushdown is bounded and correct', () {
        test('filter pushdown', () async {
          final df = await DataFrameIO.readCsv(
            writeCsv(),
            filter: RowFilter.compare('id', FilterOp.gt, 15),
          ).ok;
          final table = await df.execute().ok;
          expect(table.rowCount, 4); // 16,17,18,19
          expect(table.column('id')!.toList().first, 16);
        });

        test('limit pushdown', () async {
          final df = await DataFrameIO.readCsv(writeCsv(), limit: 5).ok;
          final table = await df.execute().ok;
          expect(table.rowCount, 5);
          expect(table.column('id')!.toList(), [0, 1, 2, 3, 4]);
        });

        test('a small buffer spans batches', () async {
          final df = await DataFrameIO.readCsv(writeCsv(), batchSize: 3).ok;
          final table = await df.execute().ok;
          expect(table.rowCount, 20);
          expect(table.column('id')!.toList().last, 19);
        });
      });
    });
  });

  // The record splitter and the field parser must agree on what a quote MEANS.
  // The parser treats a quote as quoting only at a field START; the splitter used
  // to count every quote in the line, so a bare quote in an unquoted field
  // (inches, minutes) either swallowed the next row or threw. Python/pandas/Excel
  // all read these.
  group('bare quotes in unquoted fields', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('csvbq_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Future<Table> read(String content) async {
      final path = '${dir.path}/f.csv';
      File(path).writeAsStringSync(content);
      return (await DataFrameIO.readCsv(path).ok).execute().ok;
    }

    test('an odd bare quote does not swallow the next row', () async {
      final table = await read('a,b\nx,5" nails\ny,6" screws\n');
      expect(table.rowCount, 2);
      expect(table.column('b')!.toList(), ['5" nails', '6" screws']);
    });

    test('a single row ending in a bare quote parses', () async {
      final table = await read('a,b\nx,5" nails\n');
      expect(table.column('b')!.toList(), ['5" nails']);
    });

    test('an even number of bare quotes parses', () async {
      final table = await read('a,b\nx,5" 6" nails\n');
      expect(table.column('b')!.toList(), ['5" 6" nails']);
    });

    test('a properly quoted field still spans newlines', () async {
      final table = await read('a,b\nx,"line1\nline2"\n');
      expect(table.rowCount, 1);
      expect(table.column('b')!.toList(), ['line1\nline2']);
    });

    test('a quoted field with a doubled quote still parses', () async {
      final table = await read('a,b\nx,"say ""hi"" now"\n');
      expect(table.column('b')!.toList(), ['say "hi" now']);
    });

    test('a quoted field after a bare-quote field still parses', () async {
      final table = await read('a,b,c\nx,5" nails,"a,b"\n');
      expect(table.column('b')!.toList(), ['5" nails']);
      expect(table.column('c')!.toList(), ['a,b']);
    });

    test('a genuinely unterminated quoted field still throws', () async {
      final path = '${dir.path}/f.csv';
      File(path).writeAsStringSync('a,b\nx,"never closed\n');
      await expectLater(DataFrameIO.readCsv(path), failsWith(Kind.corrupt));
    });
  });

  // The standalone CsvReader(filter:) API. SQL never reaches this code: the
  // planner drops the pushdown and filters exactly (scan.dart: "CSV can't prune
  // by stats"), so there is no query layer above it to correct a wrong answer.
  group('reader filter', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('csvfilter_'));
    tearDown(() => dir.deleteSync(recursive: true));

    String write(String content) {
      final path = '${dir.path}/f.csv';
      File(path).writeAsStringSync(content);
      return path;
    }

    Future<Table> read(String content, RowFilter filter) async =>
        (await DataFrameIO.readCsv(
          write(content),
          filter: filter,
        ).ok).execute().ok;

    test('an unknown column throws instead of returning nothing', () async {
      await expectLater(
        DataFrameIO.readCsv(
          write('id,name\n1,a\n2,b\n'),
          filter: RowFilter.compare('nope', FilterOp.eq, 1),
        ),
        failsWith(Kind.invalid, allOf(contains('nope'), contains('id'))),
      );
    });

    test('a known column still filters', () async {
      final table = await read(
        'id,name\n1,a\n2,b\n3,c\n',
        RowFilter.compare('id', FilterOp.gt, 1),
      );
      expect(table.column('id')!.toList(), [2, 3]);
    });

    test('an int64 literal keeps its precision', () async {
      // Parsed as a double first, 9223372036854775807 rounds to ...776000.0 and
      // so does its neighbour, both would match.
      final table = await read(
        'id\n9223372036854775807\n9223372036854775806\n',
        RowFilter.compare('id', FilterOp.eq, 9223372036854775807),
      );
      expect(table.column('id')!.toList(), [9223372036854775807]);
    });

    test('a non-boolean cell matches neither true nor false', () async {
      // 'banana' used to coerce to false, so `flag == false` matched it.
      const content = 'flag\ntrue\nfalse\nbanana\n';
      expect(
        (await read(
          content,
          RowFilter.compare('flag', FilterOp.eq, true),
        )).rowCount,
        1,
      );
      expect(
        (await read(
          content,
          RowFilter.compare('flag', FilterOp.eq, false),
        )).rowCount,
        1,
      );
    });

    test('an unparseable numeric cell matches nothing', () async {
      expect(
        (await read(
          'id\n1\nx\n3\n',
          RowFilter.compare('id', FilterOp.gt, 0),
        )).rowCount,
        2,
      );
    });
  });

  // Relocated verbatim from resilience_test.dart (self-contained setUp).
  group('CSV unclosed-quote cap', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('csvcap_'));
    tearDown(() => dir.deleteSync(recursive: true));

    String write(int lines) {
      final p = '${dir.path}/bad.csv';
      // A header, then an unterminated quote followed by many lines that would
      // otherwise all be merged into one logical record.
      final body = [
        'a,b',
        '"oops,start',
        for (var i = 0; i < lines; i++) 'x$i,y$i',
      ];
      File(p).writeAsStringSync(body.join('\n'));
      return p;
    }

    test('eager readFile throws before buffering the whole file', () async {
      final path = write(20000);
      await expectLater(
        CsvReader(
          options: const CsvOptions(maxRecordBytes: 1024),
        ).readFile(path),
        failsWith(Kind.corrupt),
      );
    });

    test(
      'streaming readBatches throws promptly on the unclosed quote',
      () async {
        final path = write(20000);
        expect(
          CsvReader(
            options: const CsvOptions(maxRecordBytes: 1024),
          ).readBatches(path).last,
          failsWith(Kind.corrupt),
        );
      },
    );
  });
}
