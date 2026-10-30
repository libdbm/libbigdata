import 'dart:io';
import 'dart:typed_data';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/core/failure.dart';
import 'package:libbigdata/src/core/limits.dart';
import 'package:libbigdata/src/formats/arrow/arrow_buffers.dart'
    show decodeColumn;
import 'package:libbigdata/src/formats/arrow/arrow_message.dart'
    show decimalBitsMeta, timeUnitMeta;
import 'package:libbigdata/src/formats/arrow/arrow_reader.dart';
import 'package:libbigdata/src/formats/arrow/arrow_writer.dart';
import 'package:test/test.dart';

import 'support/results.dart';

Table _ids(List<int> ids) => Table(
  name: 't',
  schema: Schema(
    fields: [Field(name: 'id', dataType: DataType.int64)],
  ),
  columns: [ListColumn<int>(name: 'id', dataType: DataType.int64, data: ids)],
);

void main() {
  test('a date file from an earlier version still reads as days', () async {
    // Earlier writers omitted the date unit, which the spec reads as
    // milliseconds; the values are 4-byte days.
    final table = await ArrowReader(
      'test/fixtures/arrow_date_without_unit.arrow',
    ).read();
    expect(table.column('d')!.toList(), [
      DateTime.utc(2020, 1, 1),
      DateTime.utc(1969, 12, 31),
    ]);
  });

  group('IPC stream format', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('arrow_stream_'));
    tearDown(() => dir.deleteSync(recursive: true));

    final frame = DataFrame.fromTable(
      Table.fromRows('t', [
        for (var i = 0; i < 5; i++) {'id': i, 'name': 'n$i'},
      ]),
    );

    test('writeArrowStream writes the stream format and reads back', () async {
      final path = '${dir.path}/t.arrows';
      await frame
          .writeArrowStream(
            path,
            batchSize: 2,
            compression: ArrowCompression.zstd,
          )
          .ok;
      final bytes = File(path).readAsBytesSync();
      // No ARROW1 magic; ends with the end-of-stream marker.
      expect(String.fromCharCodes(bytes.take(6)), isNot('ARROW1'));
      expect(bytes.sublist(bytes.length - 8), [255, 255, 255, 255, 0, 0, 0, 0]);
      final read = await DataFrameIO.readArrowStream(path).ok;
      expect((await read.execute().ok).column('name')!.toList(), [
        'n0',
        'n1',
        'n2',
        'n3',
        'n4',
      ]);
    });

    test('an empty stream keeps its schema', () async {
      final path = '${dir.path}/e.arrows';
      await frame.filter('id > 99').writeArrowStream(path).ok;
      final read = await (await DataFrameIO.readArrowStream(
        path,
      ).ok).execute().ok;
      expect(read.rowCount, 0);
      expect(read.schema.fieldNames, ['id', 'name']);
    });

    test('readArrowStream also reads the file format', () async {
      final path = '${dir.path}/t.arrow';
      await frame.writeArrow(path).ok;
      final read = await DataFrameIO.readArrowStream(path).ok;
      expect((await read.execute().ok).rowCount, 5);
    });

    test('a stream over the row cap is a limit', () async {
      final path = '${dir.path}/t.arrows';
      await frame.writeArrowStream(path, batchSize: 2).ok;
      await expectLater(
        DataFrameIO.readArrowStream(path, maxRows: 3),
        failsWith(Kind.limit),
      );
    });

    test('readArrowBatches honours batchSize', () async {
      final path = '${dir.path}/t.arrow';
      await frame.writeArrow(path, batchSize: 5).ok;
      final sizes = [
        for (final batch in await DataFrameIO.readArrowBatches(
          path,
          batchSize: 2,
        ).ok.toList())
          (await batch.execute().ok).rowCount,
      ];
      expect(sizes, [2, 2, 1]);
    });
  });

  group('IPC', () {
    Table sample(int rows) {
      final id = <int?>[];
      final amount = <double?>[];
      final name = <String?>[];
      final active = <bool?>[];
      final q = <int?>[];
      final score = <double?>[];
      final small = <int?>[];
      for (int i = 0; i < rows; i++) {
        id.add(5000000000 + i);
        amount.add(i * 1.5);
        name.add(i % 7 == 0 ? null : 'name_$i');
        active.add(i % 2 == 0);
        q.add(1 + (i % 100));
        score.add(i % 5 == 0 ? null : i + 0.25);
        small.add(i % 256 - 128);
      }
      final schema = Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'amount', dataType: DataType.float64, nullable: false),
          Field(name: 'name', dataType: DataType.string, nullable: true),
          Field(name: 'active', dataType: DataType.boolean, nullable: false),
          Field(name: 'q', dataType: DataType.int32, nullable: false),
          Field(name: 'score', dataType: DataType.float64, nullable: true),
          Field(name: 'small', dataType: DataType.int8, nullable: false),
        ],
      );
      return Table(
        name: 't',
        columns: [
          NumericColumn.int64(name: 'id', values: id, nullable: false),
          NumericColumn.float64(
            name: 'amount',
            values: amount,
            nullable: false,
          ),
          ListColumn<String>(
            name: 'name',
            dataType: DataType.string,
            data: name,
            nullable: true,
          ),
          ListColumn<bool>(
            name: 'active',
            dataType: DataType.boolean,
            data: active,
            nullable: false,
          ),
          NumericColumn.int32(name: 'q', values: q, nullable: false),
          NumericColumn.float64(name: 'score', values: score, nullable: true),
          NumericColumn.int8(name: 'small', values: small, nullable: false),
        ],
        schema: schema,
      );
    }

    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('arrow_ipc_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('round-trips core types with nulls', () async {
      final table = sample(500);
      final path = '${dir.path}/d.arrow';
      await ArrowWriter(path).write(table);
      final back = await ArrowReader(path).read();

      expect(back.rowCount, equals(500));
      expect(
        back.schema.fields.map((f) => f.dataType).toList(),
        equals(table.schema.fields.map((f) => f.dataType).toList()),
      );
      for (final name in [
        'id',
        'amount',
        'name',
        'active',
        'q',
        'score',
        'small',
      ]) {
        expect(
          back.column(name)!.toList(),
          equals(table.column(name)!.toList()),
          reason: 'column $name',
        );
      }
    });

    for (final codec in [ArrowCompression.lz4, ArrowCompression.zstd]) {
      test('round-trips with ${codec.name} body compression', () async {
        final table = sample(1500);
        final path = '${dir.path}/c_${codec.name}.arrow';
        await ArrowWriter(
          path,
          batchSize: 500,
          compression: codec,
        ).write(table);
        final back = await ArrowReader(path).read();
        expect(back.rowCount, equals(1500));
        for (final name in [
          'id',
          'amount',
          'name',
          'active',
          'q',
          'score',
          'small',
        ]) {
          expect(
            back.column(name)!.toList(),
            equals(table.column(name)!.toList()),
            reason: 'column $name (${codec.name})',
          );
        }
      });
    }

    test('round-trips across multiple record batches', () async {
      final table = sample(2500);
      final path = '${dir.path}/m.arrow';
      await ArrowWriter(path, batchSize: 500).write(table); // 5 batches
      final back = await ArrowReader(path).read();
      expect(back.rowCount, equals(2500));
      expect(back.column('id')!.toList(), equals(table.column('id')!.toList()));
      expect(
        back.column('name')!.toList(),
        equals(table.column('name')!.toList()),
      );
      expect(
        back.column('score')!.toList(),
        equals(table.column('score')!.toList()),
      );
    });

    test('writes valid IPC file framing', () async {
      final path = '${dir.path}/f.arrow';
      await ArrowWriter(path).write(sample(10));
      final bytes = File(path).readAsBytesSync();
      final magic = [65, 82, 82, 79, 87, 49];
      expect(bytes.sublist(0, 6), equals(magic), reason: 'leading magic');
      expect(
        bytes.sublist(bytes.length - 6),
        equals(magic),
        reason: 'trailing magic',
      );
      // first message after the 8-byte magic+pad starts with the continuation marker
      final cont = ByteData.view(
        bytes.buffer,
        8,
        4,
      ).getUint32(0, Endian.little);
      expect(cont, equals(0xFFFFFFFF), reason: 'continuation marker');
    });

    test('empty table round-trips', () async {
      final schema = Schema(
        fields: [
          Field(name: 'a', dataType: DataType.int32, nullable: false),
          Field(name: 'b', dataType: DataType.string, nullable: true),
        ],
      );
      final table = Table(
        name: 't',
        columns: [
          NumericColumn.int32(name: 'a', values: const [], nullable: false),
          ListColumn<String>(
            name: 'b',
            dataType: DataType.string,
            data: const [],
            nullable: true,
          ),
        ],
        schema: schema,
      );
      final path = '${dir.path}/e.arrow';
      await ArrowWriter(path).write(table);
      final back = await ArrowReader(path).read();
      expect(back.rowCount, equals(0));
      expect(back.columnCount, equals(2));
    });

    test('a nested field missing its child raises a clear error', () async {
      final schema = Schema(
        fields: [Field(name: 'l', dataType: DataType.list, nullable: false)],
      );
      final table = Table(
        name: 't',
        columns: [
          ListColumn(
            name: 'l',
            dataType: DataType.list,
            data: [
              [1, 2],
            ],
            nullable: false,
          ),
        ],
        schema: schema,
      );
      expect(
        () => ArrowWriter('${dir.path}/u.arrow').write(table),
        throwsA(isA<ArgumentError>()),
      );
    });

    group('extended types (date/timestamp/decimal/null)', () {
      Table extended() => Table(
        name: 't',
        columns: [
          ListColumn<DateTime>(
            name: 'd',
            dataType: DataType.date,
            data: [DateTime.utc(2021, 1, 1), null, DateTime.utc(1990, 6, 15)],
            nullable: true,
          ),
          ListColumn<DateTime>(
            name: 'ts',
            dataType: DataType.timestamp,
            data: [
              DateTime.utc(2021, 3, 15, 10, 30, 15, 123, 456),
              DateTime.utc(2000, 1, 1),
              null,
            ],
            nullable: true,
          ),
          ListColumn<BigInt>(
            name: 'dec',
            dataType: DataType.decimal,
            data: [BigInt.from(12345), BigInt.from(-99), null],
            nullable: true,
          ),
          ListColumn(
            name: 'nul',
            dataType: DataType.null_,
            data: [null, null, null],
            nullable: true,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'd', dataType: DataType.date, nullable: true),
            Field(name: 'ts', dataType: DataType.timestamp, nullable: true),
            Field(
              name: 'dec',
              dataType: DataType.decimal,
              nullable: true,
              precision: 10,
              scale: 2,
            ),
            Field(name: 'nul', dataType: DataType.null_, nullable: true),
          ],
        ),
      );

      test(
        'whole-file round-trip preserves values + decimal precision/scale',
        () async {
          final t = extended();
          final path = '${dir.path}/ext.arrow';
          await ArrowWriter(path).write(t);
          final back = await ArrowReader(path).read();
          for (final f in ['d', 'ts', 'dec', 'nul']) {
            expect(
              back.column(f)!.toList(),
              equals(t.column(f)!.toList()),
              reason: f,
            );
          }
          // microsecond precision preserved
          expect((back.column('ts')![0] as DateTime).microsecond, 456);
          final dec = back.schema.field('dec')!;
          expect(dec.precision, 10);
          expect(dec.scale, 2);
        },
      );

      test('streaming writer round-trips extended types', () async {
        final t = extended();
        final path = '${dir.path}/ext_s.arrow';
        final w = ArrowStreamWriter(path, t.schema);
        await w.add(t);
        await w.close();
        final back = await ArrowReader(path).read();
        expect(back.column('dec')!.toList(), equals(t.column('dec')!.toList()));
        expect(back.column('d')!.toList(), equals(t.column('d')!.toList()));
      });
    });

    group('nested types (list/struct/map)', () {
      Table nested() => Table(
        name: 't',
        columns: [
          ListColumn(
            name: 'l',
            dataType: DataType.list,
            data: [
              [1, 2],
              [3],
              null,
              <int>[],
            ],
            nullable: true,
          ),
          ListColumn(
            name: 's',
            dataType: DataType.struct,
            data: [
              {'a': 1, 'b': 'x'},
              {'a': 2, 'b': 'y'},
              null,
              {'a': 4, 'b': null},
            ],
            nullable: true,
          ),
          ListColumn(
            name: 'm',
            dataType: DataType.map,
            data: [
              {'k1': 1, 'k2': 2},
              <String, int>{},
              null,
              {'z': 9},
            ],
            nullable: true,
          ),
        ],
        schema: Schema(
          fields: [
            Field(
              name: 'l',
              dataType: DataType.list,
              nullable: true,
              children: [
                Field(name: 'item', dataType: DataType.int64, nullable: true),
              ],
            ),
            Field(
              name: 's',
              dataType: DataType.struct,
              nullable: true,
              children: [
                Field(name: 'a', dataType: DataType.int64, nullable: true),
                Field(name: 'b', dataType: DataType.string, nullable: true),
              ],
            ),
            Field(
              name: 'm',
              dataType: DataType.map,
              nullable: true,
              children: [
                Field(
                  name: 'entries',
                  dataType: DataType.struct,
                  nullable: false,
                  children: [
                    Field(
                      name: 'key',
                      dataType: DataType.string,
                      nullable: false,
                    ),
                    Field(
                      name: 'value',
                      dataType: DataType.int64,
                      nullable: true,
                    ),
                  ],
                ),
              ],
            ),
          ],
        ),
      );

      test('list/struct/map round-trip (whole-file)', () async {
        final t = nested();
        final path = '${dir.path}/nested.arrow';
        await ArrowWriter(path).write(t);
        final back = await ArrowReader(path).read();
        for (final f in ['l', 's', 'm']) {
          expect(
            back.column(f)!.toList().toString(),
            equals(t.column(f)!.toList().toString()),
            reason: f,
          );
        }
        // schema children preserved
        expect(
          back.schema.field('s')!.children.map((c) => c.name).toList(),
          equals(['a', 'b']),
        );
      });

      test('nested round-trips via the streaming writer', () async {
        final t = nested();
        final path = '${dir.path}/nested_s.arrow';
        final w = ArrowStreamWriter(path, t.schema);
        await w.add(t);
        await w.close();
        final back = await ArrowReader(path).read();
        expect(
          back.column('m')!.toList().toString(),
          equals(t.column('m')!.toList().toString()),
        );
      });
    });

    group('ArrowStreamWriter (incremental)', () {
      test('append batch-by-batch round-trips via ArrowReader', () async {
        final table = sample(1300);
        final path = '${dir.path}/s.arrow';
        final w = ArrowStreamWriter(path, table.schema);
        for (var start = 0; start < table.rowCount; start += 500) {
          final end = (start + 500) < table.rowCount
              ? start + 500
              : table.rowCount;
          await w.add(table.slice(start, end));
        }
        await w.close();

        final back = await ArrowReader(path).read();
        expect(back.rowCount, equals(1300));
        expect(back.schema.fieldNames, equals(table.schema.fieldNames));
        for (final f in table.schema.fieldNames) {
          expect(
            back.column(f)!.toList(),
            equals(table.column(f)!.toList()),
            reason: f,
          );
        }
      });

      test('produces valid IPC framing', () async {
        final path = '${dir.path}/f.arrow';
        final w = ArrowStreamWriter(path, sample(1).schema);
        await w.add(sample(10));
        await w.close();
        final bytes = await File(path).readAsBytes();
        const magic = [65, 82, 82, 79, 87, 49];
        expect(bytes.sublist(0, 6), equals(magic), reason: 'leading magic');
        expect(
          bytes.sublist(bytes.length - 6),
          equals(magic),
          reason: 'trailing magic',
        );
        final cont = ByteData.view(
          bytes.buffer,
          8,
          4,
        ).getUint32(0, Endian.little);
        expect(
          cont,
          equals(0xFFFFFFFF),
          reason: 'continuation marker at byte 8',
        );
      });

      test(
        'matches the whole-file writer byte-for-byte (single batch)',
        () async {
          final table = sample(700);
          final streamed = '${dir.path}/stream.arrow';
          final whole = '${dir.path}/whole.arrow';
          final w = ArrowStreamWriter(streamed, table.schema);
          await w.add(table); // one batch
          await w.close();
          // ArrowWriter splits into one batch too (rows < batchSize default).
          await ArrowWriter(whole).write(table);
          expect(
            await File(streamed).readAsBytes(),
            equals(await File(whole).readAsBytes()),
          );
        },
      );

      test('no batches added leaves no file', () async {
        final path = '${dir.path}/empty.arrow';
        await ArrowStreamWriter(path, sample(1).schema).close();
        expect(File(path).existsSync(), isFalse);
      });
    });

    group('ArrowWriter.writeBatches (streams, not buffers)', () {
      test('a stream of batches round-trips in order', () async {
        final path = '${dir.path}/wb.arrow';
        final batches = [
          for (var i = 0; i < 5; i++) sample(200), // 5 record batches
        ];
        await ArrowWriter(path).writeBatches(Stream.fromIterable(batches));
        final back = await ArrowReader(path).read();
        expect(back.rowCount, 1000);

        // And readable batch-by-batch (lazy reader), one block per written batch.
        final blocks = await ArrowReader(path).readBatches().toList();
        expect(blocks.length, 5);
      });

      test(
        'an empty stream with an explicit schema writes a schema-only file',
        () async {
          final path = '${dir.path}/wbempty.arrow';
          final s = sample(1).schema;
          await ArrowWriter(path).writeBatches(const Stream.empty(), schema: s);
          final back = await ArrowReader(path).read();
          expect(back.rowCount, 0);
          expect(back.schema.fields.length, s.fields.length);
        },
      );

      test(
        'a throw mid-stream aborts: no partial file, handle released',
        () async {
          final path = '${dir.path}/wbabort.arrow';
          // First batch opens the file against a 7-field schema; the second batch
          // has too few columns, so encoding it throws inside add().
          final bad = Table(
            name: 't',
            columns: [
              NumericColumn.int64(name: 'id', values: [1], nullable: false),
            ],
            schema: Schema(
              fields: [
                Field(name: 'id', dataType: DataType.int64, nullable: false),
              ],
            ),
          );
          await expectLater(
            ArrowWriter(
              path,
            ).writeBatches(Stream.fromIterable([sample(10), bad])),
            throwsA(anything),
          );
          // abort() must have deleted the partial file (and closed the fd).
          expect(File(path).existsSync(), isFalse);
        },
      );
    });
  });

  group('atomic write', () {
    /// ArrowWriter.write is atomic (temp + rename). A failure
    /// mid-write leaves no partial file at the destination and preserves any
    /// pre-existing file.
    Table good() => Table(
      name: 't',
      columns: [
        NumericColumn.int64(name: 'id', values: [1, 2, 3], nullable: false),
      ],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: false)],
      ),
    );

    // A column whose element type Arrow can't encode → write throws mid-encode.
    Table bad() => Table(
      name: 't',
      columns: [
        ListColumn(
          name: 'l',
          dataType: DataType.list,
          data: [
            [1, 2],
          ],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [Field(name: 'l', dataType: DataType.list, nullable: false)],
      ),
    );

    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('arrowatomic_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('a failed write leaves no file and no .tmp', () async {
      final path = '${dir.path}/out.arrow';
      await expectLater(ArrowWriter(path).write(bad()), throwsA(anything));
      expect(File(path).existsSync(), isFalse);
      // No leftover temp files in the directory.
      final leftover = dir
          .listSync()
          .whereType<File>()
          .where((f) => f.path.endsWith('.tmp'))
          .toList();
      expect(leftover, isEmpty);
    });

    test('a failed overwrite preserves the pre-existing file', () async {
      final path = '${dir.path}/out.arrow';
      await ArrowWriter(path).write(good());
      final before = File(path).readAsBytesSync();

      await expectLater(ArrowWriter(path).write(bad()), throwsA(anything));

      // The original file is intact (atomic temp+rename, not in-place).
      expect(File(path).readAsBytesSync(), before);
      expect((await ArrowReader(path).read()).rowCount, 3);
    });

    test('a failed writeBatches preserves the pre-existing file', () async {
      final path = '${dir.path}/out.arrow';
      await ArrowWriter(path).write(good());
      final before = File(path).readAsBytesSync();

      // A stream whose second batch can't be encoded throws mid-write.
      Stream<Table> batches() async* {
        yield good();
        yield bad();
      }

      await expectLater(
        ArrowWriter(path).writeBatches(batches()),
        throwsA(anything),
      );

      // The original survives (writeBatches is now temp+rename, not in-place).
      expect(File(path).readAsBytesSync(), before);
      expect((await ArrowReader(path).read()).rowCount, 3);
      final leftover = dir
          .listSync()
          .whereType<File>()
          .where((f) => f.path.endsWith('.tmp'))
          .toList();
      expect(leftover, isEmpty);
    });
  });

  group('corrupt handling', () {
    /// a corrupt Arrow IPC file whose footer block runs past the file
    /// fails with a domain FormatException, not a raw RangeError.
    Future<Uint8List> makeValid(Directory dir) async {
      final path = '${dir.path}/t.arrow';
      final t = Table(
        name: 't',
        columns: [
          NumericColumn.int64(
            name: 'id',
            values: [for (var i = 0; i < 50; i++) i],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'id', dataType: DataType.int64, nullable: false),
          ],
        ),
      );
      await ArrowWriter(path).write(t);
      return File(path).readAsBytes();
    }

    test('a footer block past EOF throws FormatException, not RangeError', () async {
      final dir = Directory.systemTemp.createTempSync('acorrupt_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final bytes = await makeValid(dir);

      // Inflate the last block bodyLength in the footer: find the trailing footer
      // (before the int32 length + "ARROW1" magic) and overwrite an i64 there with
      // a huge value so a block's offset+len exceeds the file.
      final corrupt = Uint8List.fromList(bytes);
      // Set the 8 bytes right before the trailing footer-length int32 to a huge
      // value; this lands inside the footer flatbuffer (a Block bodyLength/offset
      // field), driving a block out of range.
      final at = corrupt.length - 10 - 8; // before <i32 len><"ARROW1"> tail
      final bd = ByteData.sublistView(corrupt);
      bd.setUint64(at, 0x7FFFFFFFFFFF, Endian.little);

      expect(
        () => ArrowReader.decode(corrupt),
        throwsA(isA<FormatException>()),
      );
    });

    test(
      'a truncated Arrow file throws FormatException, not RangeError',
      () async {
        final dir = Directory.systemTemp.createTempSync('atrunc_');
        addTearDown(() => dir.deleteSync(recursive: true));
        final bytes = await makeValid(dir);
        expect(
          () => ArrowReader.decode(
            Uint8List.sublistView(bytes, 0, bytes.length ~/ 2),
          ),
          throwsA(isA<FormatException>()),
        );
      },
    );

    test(
      'any single corrupted byte reads or fails with a domain error',
      () async {
        final dir = Directory.systemTemp.createTempSync('afuzz_');
        addTearDown(() => dir.deleteSync(recursive: true));
        final bytes = await makeValid(dir);
        for (var i = 0; i < bytes.length; i++) {
          for (final value in [0x00, 0x7F, 0xFF]) {
            final corrupt = Uint8List.fromList(bytes)..[i] = value;
            try {
              ArrowReader.decode(corrupt);
            } on FormatException {
              // expected for metadata bytes
            } on Failure catch (failure) {
              // a flipped type code can name a real but unsupported type
              expect(failure.details.kind, Kind.unsupported);
            } catch (error) {
              fail('byte $i set to $value threw ${error.runtimeType}: $error');
            }
          }
        }
      },
    );

    test('a decimal buffer shorter than declared throws FormatException', () {
      final field = Field(
        name: 'd',
        dataType: DataType.decimal,
        metadata: {decimalBitsMeta: 128},
      );
      final body = Uint8List(64);
      // Four 128-bit values need 64 bytes; the buffer declares 32.
      expect(
        () => decodeColumn(
          field,
          (length: 4, nullCount: 0),
          [(offset: 0, length: 0), (offset: 0, length: 32)],
          body,
        ),
        throwsA(isA<FormatException>()),
      );
    });
  });

  group('crafted buffers fail as corrupt', () {
    final body = Uint8List(256);

    void corrupt(Field field, int rows, int nulls, List<(int, int)> buffers) {
      expect(
        () => decodeColumn(
          field,
          (length: rows, nullCount: nulls),
          [
            for (final (offset, length) in buffers)
              (offset: offset, length: length),
          ],
          body,
        ),
        throwsA(isA<FormatException>()),
      );
    }

    test('a validity buffer shorter than the rows', () {
      // 64 rows with nulls need 8 bytes of validity; 1 is declared.
      corrupt(Field(name: 'a', dataType: DataType.int64), 64, 3, [
        (0, 1),
        (8, 64 * 8),
      ]);
    });

    test('nulls without a validity buffer', () {
      corrupt(Field(name: 'a', dataType: DataType.int32), 4, 1, [
        (0, 0),
        (0, 16),
      ]);
    });

    test('int16, boolean, date and timestamp buffers too short', () {
      corrupt(Field(name: 'a', dataType: DataType.int16), 16, 0, [
        (0, 0),
        (0, 2),
      ]);
      corrupt(Field(name: 'a', dataType: DataType.boolean), 64, 0, [
        (0, 0),
        (0, 1),
      ]);
      corrupt(Field(name: 'a', dataType: DataType.date), 8, 0, [
        (0, 0),
        (0, 4),
      ]);
      corrupt(Field(name: 'a', dataType: DataType.timestamp), 8, 0, [
        (0, 0),
        (0, 8),
      ]);
    });

    test('a string offset past its data buffer', () {
      // Offsets [0, 100) but a 4-byte data buffer: the bytes would come from
      // whatever follows it in the body.
      final offsets = ByteData(8)
        ..setInt32(0, 0, Endian.little)
        ..setInt32(4, 100, Endian.little);
      body.setRange(0, 8, offsets.buffer.asUint8List());
      corrupt(Field(name: 's', dataType: DataType.string), 1, 0, [
        (0, 0),
        (0, 8),
        (8, 4),
      ]);
    });

    test('a seconds timestamp that overflows microseconds', () {
      final raw = ByteData(8)..setInt64(0, 1 << 62, Endian.little);
      body.setRange(0, 8, raw.buffer.asUint8List());
      corrupt(
        Field(
          name: 't',
          dataType: DataType.timestamp,
          metadata: {timeUnitMeta: 0},
        ),
        1,
        0,
        [(0, 0), (0, 8)],
      );
    });
  });

  test('a wide batch reads with maxRows: null', () async {
    final dir = Directory.systemTemp.createTempSync('awide_');
    addTearDown(() => dir.deleteSync(recursive: true));
    const rows = 1000000;
    final names = [for (var c = 0; c < 11; c++) 'c$c'];
    final values = [for (var i = 0; i < rows; i++) i];
    final path = '${dir.path}/wide.arrow';
    // One batch, so its nodes together declare 11M rows.
    await ArrowWriter(path, batchSize: rows).write(
      Table(
        name: 't',
        schema: Schema(
          fields: [
            for (final name in names)
              Field(name: name, dataType: DataType.int64, nullable: false),
          ],
        ),
        columns: [
          for (final name in names)
            NumericColumn.int64(name: name, values: values, nullable: false),
        ],
      ),
    );
    final table = await ArrowReader(path).read(maxRows: null);
    expect(table.rowCount, rows);
    expect(table.column('c10')![rows - 1], rows - 1);
  }, timeout: const Timeout(Duration(minutes: 2)));

  group('#8 Arrow zero-body read', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('round43_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('an empty table round-trips without a whole-file slurp', () async {
      final path = '${dir.path}/empty.arrow';
      final schema = Schema(
        fields: [Field(name: 'id', dataType: DataType.int64, nullable: true)],
      );
      await ArrowWriter(path).write(Table.empty('t', schema));
      final back = await ArrowReader(path).read();
      expect(back.rowCount, 0);
      expect(back.schema.field('id')!.dataType, DataType.int64);
    });
  });

  test('#9 an Arrow file with an oversized declared footer is rejected', () async {
    // A tiny buffer whose trailing footer-length claims a huge footer: caught by
    // the file-length bound, but the ceiling is the belt-and-suspenders guard.
    expect(defaultMaxFooterBytes, greaterThan(0));
    // Garbage bytes must fail loud (FormatException), not OOM / RangeError.
    expect(
      () => ArrowReader.decode(Uint8List.fromList(List.filled(64, 0))),
      throwsA(isA<FormatException>()),
    );
  });

  group('review findings (Arrow)', () {
    late Directory dir;

    setUp(() async {
      dir = await Directory.systemTemp.createTemp('review_findings');
    });

    tearDown(() async {
      if (await dir.exists()) await dir.delete(recursive: true);
    });

    group('finding 4: Arrow streaming write rejects schema drift', () {
      test(
        'a reordered same-type batch is rejected, not silently swapped',
        () async {
          final path = '${dir.path}/drift.arrow';
          final schema = Schema(
            fields: [
              Field(name: 'a', dataType: DataType.int64),
              Field(name: 'b', dataType: DataType.int64),
            ],
          );
          final writer = ArrowStreamWriter(path, schema);
          await writer.add(
            Table(
              name: 't',
              schema: schema,
              columns: [
                ListColumn<int>(name: 'a', dataType: DataType.int64, data: [1]),
                ListColumn<int>(name: 'b', dataType: DataType.int64, data: [2]),
              ],
            ),
          );
          // Second batch has the two columns reordered (b, a): same types, wrong
          // order: must be rejected rather than stored under the wrong field.
          final reordered = Table(
            name: 't',
            schema: Schema(
              fields: [
                Field(name: 'b', dataType: DataType.int64),
                Field(name: 'a', dataType: DataType.int64),
              ],
            ),
            columns: [
              ListColumn<int>(name: 'b', dataType: DataType.int64, data: [3]),
              ListColumn<int>(name: 'a', dataType: DataType.int64, data: [4]),
            ],
          );
          await expectLater(
            writer.add(reordered),
            throwsA(isA<ArgumentError>()),
          );
          await writer.abort();
        },
      );
    });

    group('finding 5: invalid sizes throw instead of hanging', () {
      test('ArrowWriter(batchSize: 0) throws at construction', () {
        expect(
          () => ArrowWriter('${dir.path}/x.arrow', batchSize: 0),
          throwsA(isA<ArgumentError>()),
        );
      });
    });

    group('finding 8: no temp orphan on a failed rename', () {
      test(
        'Arrow write to a destination that is a directory leaves no .tmp',
        () async {
          // Make the destination path an existing directory so replaceAtomic fails
          // after the temp is finalized.
          final dest = '${dir.path}/dest.arrow';
          await Directory(dest).create();
          final table = Table(
            name: 't',
            schema: Schema(
              fields: [Field(name: 'v', dataType: DataType.int64)],
            ),
            columns: [
              ListColumn<int>(
                name: 'v',
                dataType: DataType.int64,
                data: [1, 2],
              ),
            ],
          );
          await expectLater(
            ArrowWriter(dest).write(table),
            throwsA(isA<Object>()),
          );
          final orphans = await dir
              .list()
              .where((e) => e.path.endsWith('.tmp'))
              .toList();
          expect(orphans, isEmpty);
        },
      );
    });

    group('finding 3: Arrow read bounds a block before allocating', () {
      test(
        'read(maxBytes: tiny) rejects an oversized block pre-decode',
        () async {
          final path = '${dir.path}/big.arrow';
          await ArrowWriter(
            path,
          ).write(_ids([for (var i = 0; i < 1000; i++) i]));
          // The single block is far larger than 1 byte; the ceiling must trip before
          // the block is read/decoded.
          await expectLater(
            ArrowReader(path).read(maxBytes: 1),
            throwsFailure(Kind.limit),
          );
          // Opting out (null) still reads the whole file.
          final full = await ArrowReader(
            path,
          ).read(maxBytes: null, maxRows: null);
          expect(full.rowCount, 1000);
        },
      );
    });

    group('metadata safety (findings 1 & 7)', () {
      Field nest(int depth) {
        Field f = Field(name: 'leaf', dataType: DataType.int64, nullable: true);
        for (var i = 0; i < depth; i++) {
          f = Field(
            name: 's$i',
            dataType: DataType.struct,
            nullable: true,
            children: [f],
          );
        }
        return f;
      }

      test(
        'a schema nested past the depth cap throws, not StackOverflow',
        () async {
          // Finding 1: the recursive FlatBuffers schema parse had no depth guard.
          final path = '${dir.path}/deep.arrow';
          await ArrowWriter(path).write(
            Table.empty(
              't',
              Schema(fields: [nest(defaultMaxSchemaDepth + 30)]),
            ),
          );
          await expectLater(
            ArrowReader(path).readSchema(),
            throwsA(isA<FormatException>()),
          );
        },
      );

      test('a modestly nested schema still reads', () async {
        final path = '${dir.path}/shallow.arrow';
        final schema = Schema(fields: [nest(5)]);
        await ArrowWriter(path).write(Table.empty('t', schema));
        final read = await ArrowReader(path).readSchema();
        expect(read.fields.first.name, 's4');
      });

      test(
        'readSchema returns the footer schema without decoding a batch',
        () async {
          // Finding 7: the scan contract's schema path must not decode a whole
          // record-batch body just to learn the columns.
          final path = '${dir.path}/schema.arrow';
          await ArrowWriter(path).write(
            Table(
              name: 't',
              schema: Schema(
                fields: [
                  Field(name: 'a', dataType: DataType.int64),
                  Field(name: 'b', dataType: DataType.string),
                ],
              ),
              columns: [
                ListColumn<int>(
                  name: 'a',
                  dataType: DataType.int64,
                  data: [1, 2],
                ),
                ListColumn<String>(
                  name: 'b',
                  dataType: DataType.string,
                  data: ['x', 'y'],
                ),
              ],
            ),
          );
          final schema = await ArrowReader(path).readSchema();
          expect(schema.fields.map((f) => f.name), ['a', 'b']);
        },
      );
    });
  });
}
