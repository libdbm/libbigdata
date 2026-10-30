import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:libcompress/libcompress.dart' hide CompressionCodec;
import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/core/limits.dart' show defaultMaxSchemaFields;
import 'package:libbigdata/src/formats/arrow/arrow_writer.dart';
import 'package:libbigdata/src/formats/parquet/compression.dart';
import 'package:libbigdata/src/formats/parquet/encoding/byte_reader.dart';
import 'package:libbigdata/src/formats/parquet/encoding/delta_binary_packed.dart';
import 'package:libbigdata/src/formats/parquet/encoding/delta_byte_array.dart';
import 'package:libbigdata/src/formats/parquet/encoding/page_decoder.dart';
import 'package:libbigdata/src/formats/parquet/reader/column_reader.dart';
import 'package:libbigdata/src/formats/parquet/reader/dremel_assembler.dart';
import 'package:libbigdata/src/formats/parquet/encoding/value_decoder.dart';
import 'package:libbigdata/src/formats/parquet/encoding/value_encoder.dart'
    show DeltaBinaryPackedEncoder;
import 'package:libbigdata/src/formats/parquet/encoding/rle_bitpacked.dart';
import 'package:libbigdata/src/formats/parquet/filter/row_filter.dart';
import 'package:libbigdata/src/formats/parquet/reader/file_reader.dart';
import 'package:libbigdata/src/formats/parquet/reader/metadata_reader.dart';
import 'package:libbigdata/src/formats/parquet/reader/page_reader.dart';
import 'package:libbigdata/src/formats/parquet/schema/schema_converter.dart';
import 'package:libbigdata/src/formats/parquet/schema/schema_levels.dart';
import 'package:libbigdata/src/formats/parquet/thrift/compact_protocol.dart';
import 'package:libbigdata/src/formats/parquet/thrift/parquet_thrift.dart'
    as thrift;
import 'package:test/test.dart';

import 'support/results.dart';

/// ULEB128-encode [v] (test helper for crafting malformed encoder headers).
List<int> _uleb(int v) {
  final out = <int>[];
  while (v >= 0x80) {
    out.add((v & 0x7F) | 0x80);
    v >>= 7;
  }
  out.add(v & 0x7F);
  return out;
}

void main() {
  test('a page that decompresses to zero bytes reads as empty', () {
    // pyarrow writes such a page for an all-null column chunk; libcompress
    // rejects a size bound of 0, so the bound must be at least 1.
    for (final codec in [
      thrift.CompressionCodec.snappy,
      thrift.CompressionCodec.gzip,
      thrift.CompressionCodec.zstd,
      thrift.CompressionCodec.lz4Raw,
    ]) {
      final empty = codecFor(codec).compress(Uint8List(0));
      expect(
        codecFor(codec, maxSize: 0).decompress(empty),
        isEmpty,
        reason: codec.name,
      );
    }
  });

  group('thrift container lengths', () {
    CompactReader reader(List<int> bytes) =>
        CompactReader(Uint8List.fromList(bytes));

    test('a list longer than the bytes left is corrupt', () {
      // 0xF5: a list of i32 whose length follows as a varint; 127 elements
      // cannot fit in the one byte left.
      expect(
        () => reader([0xF5, 0x7F, 0x02]).readListBegin(),
        throwsA(isA<FormatException>()),
      );
      expect(
        () => reader([0xF5, 0xFF, 0xFF, 0xFF, 0x7F]).readSetBegin(),
        throwsA(isA<FormatException>()),
      );
    });

    test('a map longer than the bytes left is corrupt', () {
      expect(
        () => reader([0x64, 0x55, 0x02, 0x04]).readMapBegin(),
        throwsA(isA<FormatException>()),
      );
    });

    test('a length equal to the bytes left still reads', () {
      final list = reader([0x35, 0x02, 0x04, 0x06])..readListBegin();
      expect([list.readI32(), list.readI32(), list.readI32()], [1, 2, 3]);
      final map = reader([0x01, 0x55, 0x02, 0x04]).readMapBegin();
      expect(map.length, 1);
    });
  });

  group('thrift LogicalType writes', () {
    thrift.LogicalType roundTrip(thrift.LogicalType logical) {
      final writer = CompactWriter();
      logical.write(writer);
      return thrift.LogicalType()..read(CompactReader(writer.takeBytes()));
    }

    test('each LogicalType round-trips through write then read', () {
      expect(
        roundTrip(thrift.LogicalType()..string = thrift.StringType()).string,
        isNotNull,
      );
      expect(
        roundTrip(thrift.LogicalType()..date = thrift.DateType()).date,
        isNotNull,
      );
      final decimal = roundTrip(
        thrift.LogicalType()
          ..decimal = (thrift.DecimalType()
            ..scale = 2
            ..precision = 10),
      ).decimal!;
      expect((decimal.scale, decimal.precision), (2, 10));
      for (final (unit, check) in [
        (
          thrift.TimeUnit()..millis = thrift.MilliSeconds(),
          (thrift.TimeUnit u) => u.millis != null,
        ),
        (
          thrift.TimeUnit()..micros = thrift.MicroSeconds(),
          (thrift.TimeUnit u) => u.micros != null,
        ),
        (
          thrift.TimeUnit()..nanos = thrift.NanoSeconds(),
          (thrift.TimeUnit u) => u.nanos != null,
        ),
      ]) {
        final timestamp = roundTrip(
          thrift.LogicalType()
            ..timestamp = (thrift.TimestampType()
              ..isAdjustedToUTC = true
              ..unit = unit),
        ).timestamp!;
        expect(timestamp.isAdjustedToUTC, isTrue);
        expect(check(timestamp.unit!), isTrue);
        final time = roundTrip(
          thrift.LogicalType()
            ..time = (thrift.TimeType()
              ..isAdjustedToUTC = false
              ..unit = unit),
        ).time!;
        expect(time.isAdjustedToUTC, isFalse);
        expect(check(time.unit!), isTrue);
      }
      final integer = roundTrip(
        thrift.LogicalType()
          ..integer = (thrift.IntType()
            ..bitWidth = 16
            ..isSigned = false),
      ).integer!;
      expect((integer.bitWidth, integer.isSigned), (16, false));
    });

    test('footer and page structs round-trip through write then read', () {
      T trip<T extends thrift.ParquetBase>(T value, T empty) {
        final writer = CompactWriter();
        value.write(writer);
        return empty..read(CompactReader(writer.takeBytes()));
      }

      final order = trip(
        thrift.ColumnOrder()..typeOrder = thrift.TypeDefinedOrder(),
        thrift.ColumnOrder(),
      );
      expect(order.typeOrder, isNotNull);
      expect(
        trip(thrift.ColumnOrder(), thrift.ColumnOrder()).typeOrder,
        isNull,
      );
      final sorting = trip(
        thrift.SortingColumn()
          ..columnIdx = 3
          ..descending = true
          ..nullsFirst = true,
        thrift.SortingColumn(),
      );
      expect(
        (sorting.columnIdx, sorting.descending, sorting.nullsFirst),
        (3, true, true),
      );
      final stats = trip(
        thrift.PageEncodingStats()
          ..pageType = thrift.PageType.dictionaryPage
          ..encoding = thrift.Encoding.rleDictionary
          ..count = 7,
        thrift.PageEncodingStats(),
      );
      expect(
        (stats.pageType, stats.encoding, stats.count),
        (thrift.PageType.dictionaryPage, thrift.Encoding.rleDictionary, 7),
      );
      final header = trip(
        thrift.DataPageHeaderV2()
          ..numValues = 10
          ..numNulls = 2
          ..numRows = 9
          ..encoding = thrift.Encoding.deltaBinaryPacked
          ..definitionLevelsByteLength = 4
          ..repetitionLevelsByteLength = 5
          ..isCompressed = false
          ..statistics = (thrift.Statistics()..nullCount = 2),
        thrift.DataPageHeaderV2(),
      );
      expect(
        (
          header.numValues,
          header.numNulls,
          header.numRows,
          header.encoding,
          header.definitionLevelsByteLength,
          header.repetitionLevelsByteLength,
          header.isCompressed,
          header.statistics?.nullCount,
        ),
        (10, 2, 9, thrift.Encoding.deltaBinaryPacked, 4, 5, false, 2),
      );
    });

    test('the writer records a type-defined order for each leaf', () async {
      final dir = Directory.systemTemp.createTempSync('orders_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final path = '${dir.path}/t.parquet';
      await ParquetWriter(path)
          .write(
            Table.fromRows('t', [
              {'i': 1, 's': 'a', 'd': 0.5, 'b': true},
            ]),
          )
          .ok;
      final reader = ParquetReader(path);
      await reader.schema().ok;
      final orders = reader.metadata.columnOrders;
      expect(orders, hasLength(4));
      expect(orders!.every((order) => order.typeOrder != null), isTrue);
    });

    test(
      'the writer annotates strings, decimals, dates and timestamps',
      () async {
        final dir = Directory.systemTemp.createTempSync('logical_');
        addTearDown(() => dir.deleteSync(recursive: true));
        final path = '${dir.path}/t.parquet';
        final instant = DateTime.utc(2024, 1, 2, 3, 4, 5);
        await ParquetWriter(path)
            .write(
              Table(
                name: 't',
                schema: Schema(
                  fields: [
                    Field(name: 's', dataType: DataType.string),
                    Field(
                      name: 'd',
                      dataType: DataType.decimal,
                      precision: 10,
                      scale: 2,
                    ),
                    Field(name: 'day', dataType: DataType.date),
                    Field(name: 'ts', dataType: DataType.timestamp),
                  ],
                ),
                columns: [
                  ListColumn<String>(
                    name: 's',
                    dataType: DataType.string,
                    data: ['x'],
                  ),
                  ListColumn<BigInt>(
                    name: 'd',
                    dataType: DataType.decimal,
                    data: [BigInt.from(150)],
                  ),
                  ListColumn<DateTime>(
                    name: 'day',
                    dataType: DataType.date,
                    data: [DateTime.utc(2024, 1, 2)],
                  ),
                  ListColumn<DateTime>(
                    name: 'ts',
                    dataType: DataType.timestamp,
                    data: [instant],
                  ),
                ],
              ),
            )
            .ok;
        final reader = ParquetReader(path);
        final table = await reader.read().ok;
        final elements = {
          for (final e in reader.metadata.schema.skip(1)) e.name: e,
        };
        expect(elements['s']!.logicalType?.string, isNotNull);
        expect(elements['d']!.logicalType?.decimal?.scale, 2);
        expect(elements['d']!.logicalType?.decimal?.precision, 10);
        expect(elements['day']!.logicalType?.date, isNotNull);
        final timestamp = elements['ts']!.logicalType?.timestamp;
        expect(timestamp?.isAdjustedToUTC, isTrue);
        expect(timestamp?.unit?.micros, isNotNull);
        // The legacy annotations stay for older readers.
        expect(
          elements['ts']!.convertedType,
          thrift.ConvertedType.timestampMicros,
        );
        expect(table.column('ts')![0], instant);
        expect(table.row(0)['d'].toString(), '1.50');
      },
    );
  });

  group('writer', () {
    // Round-trips a table through ParquetWriter -> ParquetReader for each codec.
    late Directory dir;

    setUp(() => dir = Directory.systemTemp.createTempSync('pq_writer_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table sample() => Table(
      name: 't',
      columns: [
        NumericColumn.int32(
          name: 'id',
          values: List.generate(500, (i) => 1000000 + i),
          nullable: false,
        ),
        ListColumn<String>(
          name: 'name',
          dataType: DataType.string,
          data: List.generate(500, (i) => 'item_${i % 7}'),
          nullable: false,
        ),
        ListColumn<bool>(
          name: 'flag',
          dataType: DataType.boolean,
          data: List.generate(500, (i) => i % 2 == 0),
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int32, nullable: false),
          Field(name: 'name', dataType: DataType.string, nullable: false),
          Field(name: 'flag', dataType: DataType.boolean, nullable: false),
        ],
      ),
    );

    for (final codec in ParquetCompression.values) {
      test('round-trips a table with ${codec.name} compression', () async {
        final path = '${dir.path}/out_${codec.name}.parquet';
        await ParquetWriter(
          path,
          options: ParquetWriterOptions(compression: codec),
        ).write(sample()).ok;

        final table = await ParquetReader(path).read().ok;
        expect(table.rowCount, 500);
        expect(table.column('id')!.toList().first, 1000000);
        expect(table.column('name')!.toList().first, 'item_0');
        expect(table.column('flag')!.toList().first, true);

        int sum = 0;
        for (final v in table.column('id')!.toList()) {
          sum += v as int;
        }
        expect(sum, 500 * 1000000 + (499 * 500) ~/ 2);
      });
    }

    // The typed write path encodes flat numeric columns straight from their
    // typed buffer (no boxing to List<dynamic>). These lock in that it matches
    // the boxed path: nulls land in the right rows, int16 widens to INT32, DELTA
    // and PLAIN both survive, and page/row-group splitting stays correct.
    group('typed numeric write path', () {
      test('nullable numerics round-trip with edge/interior nulls', () async {
        const n = 600;
        final ints = [
          for (var i = 0; i < n; i++)
            (i == 0 || i == n - 1 || i == 300) ? null : i * 7 - 100,
        ];
        final dbls = [
          for (var i = 0; i < n; i++)
            (i == 1 || i == n - 1) ? null : i * 1.5 - 3.25,
        ];
        final shorts = [
          for (var i = 0; i < n; i++) (i == 0) ? null : (i % 50) - 25,
        ];
        final table = Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'a', values: ints),
            NumericColumn.float64(name: 'b', values: dbls),
            NumericColumn.int16(name: 'c', values: shorts),
          ],
          schema: Schema(
            fields: [
              Field(name: 'a', dataType: DataType.int64, nullable: true),
              Field(name: 'b', dataType: DataType.float64, nullable: true),
              Field(name: 'c', dataType: DataType.int16, nullable: true),
            ],
          ),
        );
        final path = '${dir.path}/typed_nulls.parquet';
        await ParquetWriter(path).write(table).ok;
        final back = await ParquetReader(path).read().ok;
        expect(back.column('a')!.toList(), ints);
        expect(back.column('b')!.toList(), dbls);
        expect(back.column('c')!.toList(), shorts);
      });

      test('streaming concatenates typed row-group slices', () async {
        final schema = Schema(
          fields: [
            Field(name: 'a', dataType: DataType.int64, nullable: true),
            Field(name: 'b', dataType: DataType.float64, nullable: false),
          ],
        );
        Table batch(int base, int m) => Table(
          name: 't',
          columns: [
            NumericColumn.int64(
              name: 'a',
              values: [
                for (var i = 0; i < m; i++) (i % 7 == 0) ? null : base + i,
              ],
            ),
            NumericColumn.float64(
              name: 'b',
              values: [for (var i = 0; i < m; i++) (base + i) * 0.5],
              nullable: false,
            ),
          ],
          schema: schema,
        );
        final batches = [batch(0, 90), batch(1000, 90), batch(2000, 90)];
        final path = '${dir.path}/typed_stream.parquet';
        // rowGroupSize < batch total, so _flushBuffer concatenates typed slices
        // ACROSS batch boundaries into one numeric column per row group.
        await ParquetWriter(
          path,
          options: ParquetWriterOptions(rowGroupSize: 100),
        ).writeBatches(Stream.fromIterable(batches)).ok;
        final back = await ParquetReader(path).read().ok;
        final expA = [for (final b in batches) ...b.column('a')!.toList()];
        final expB = [for (final b in batches) ...b.column('b')!.toList()];
        expect(back.rowCount, 270);
        expect(back.column('a')!.toList(), expA);
        expect(back.column('b')!.toList(), expB);
      });

      test('typed encoding spans multiple pages', () async {
        // > pageSize~/8 (131072) in ONE row group forces the typed encoder to
        // split pages; a wrong boundary would corrupt values past the first page.
        const n = 200000;
        final ints = [for (var i = 0; i < n; i++) i * 3 - 500];
        final dbls = [for (var i = 0; i < n; i++) i * 0.25];
        final table = Table(
          name: 't',
          columns: [
            NumericColumn.int64(name: 'a', values: ints, nullable: false),
            NumericColumn.float64(name: 'b', values: dbls, nullable: false),
          ],
          schema: Schema(
            fields: [
              Field(name: 'a', dataType: DataType.int64, nullable: false),
              Field(name: 'b', dataType: DataType.float64, nullable: false),
            ],
          ),
        );
        final path = '${dir.path}/typed_pages.parquet';
        await ParquetWriter(
          path,
          options: ParquetWriterOptions(rowGroupSize: 300000),
        ).write(table).ok;
        final back = await ParquetReader(path).read().ok;
        expect(back.rowCount, n);
        final a = back.column('a')!;
        final b = back.column('b')!;
        for (final i in [0, 131071, 131072, 199999]) {
          expect(a[i], ints[i]);
          expect(b[i], dbls[i]);
        }
      });
    });

    test('round-trips non-ASCII (UTF-8) strings', () async {
      final path = '${dir.path}/utf8.parquet';
      final values = ['café', 'naïve', '日本語', 'emoji😀', 'ascii'];
      final table = Table(
        name: 't',
        columns: [
          ListColumn<String>(
            name: 's',
            dataType: DataType.string,
            data: values,
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 's', dataType: DataType.string, nullable: false),
          ],
        ),
      );
      await ParquetWriter(path).write(table).ok;
      final back = await ParquetReader(path).read().ok;
      expect(back.column('s')!.toList(), values);
    });

    // The writer must not produce files it can't read back, and writeStatistics
    // must apply uniformly to flat AND nested-leaf columns.
    group('writer options validation (finding 7)', () {
      test('an unsupported metadata version is rejected', () {
        // The const-constructor assert catches it in dev/test; open() also
        // rejects it in production (asserts stripped), mirroring rowGroupSize.
        expect(
          () => ParquetWriterOptions(version: 3),
          throwsA(isA<AssertionError>()),
        );
      });

      test('versions 1 and 2 write and read back', () async {
        for (final v in [1, 2]) {
          final path = '${dir.path}/v$v.parquet';
          await ParquetWriter(
            path,
            options: ParquetWriterOptions(version: v),
          ).write(sample()).ok;
          expect((await ParquetReader(path).read().ok).rowCount, 500);
        }
      });

      test(
        'writeStatistics:false drops stats on nested leaf columns',
        () async {
          Table nested() => Table(
            name: 't',
            columns: [
              ListColumn(
                name: 's',
                dataType: DataType.struct,
                data: [
                  for (var i = 1; i <= 8; i++) {'a': i},
                ],
                nullable: false,
              ),
            ],
            schema: Schema(
              fields: [
                Field(
                  name: 's',
                  dataType: DataType.struct,
                  nullable: false,
                  children: [
                    Field(name: 'a', dataType: DataType.int64, nullable: false),
                  ],
                ),
              ],
            ),
          );

          Future<List<bool>> leafHasStats(String path) async {
            final reader = FileReader(path);
            await reader.open();
            try {
              final md = await MetadataReader(reader).readMetadata();
              return [
                for (final c in md.rowGroups.first.columns)
                  c.metaData!.statistics != null,
              ];
            } finally {
              await reader.close();
            }
          }

          final off = '${dir.path}/nostats.parquet';
          await ParquetWriter(
            off,
            options: const ParquetWriterOptions(writeStatistics: false),
          ).write(nested()).ok;
          expect(
            (await leafHasStats(off)).every((h) => !h),
            isTrue,
            reason: 'nested leaf columns must carry NO stats',
          );

          final on = '${dir.path}/stats.parquet';
          await ParquetWriter(
            on,
          ).write(nested()).ok; // default: writeStatistics true
          expect(
            (await leafHasStats(on)).any((h) => h),
            isTrue,
            reason: 'control: the default keeps nested stats',
          );
        },
      );
    });

    test('open() rejects re-entry while a write is live (finding 8)', () async {
      final path = '${dir.path}/reopen.parquet';
      final writer = ParquetWriter(path);
      await writer.open(sample().schema).ok;
      // A precondition on the caller, so it throws rather than reports.
      expect(() => writer.open(sample().schema), throwsStateError);
      await writer.abort();
    });

    test('RawLeaf.bytes charges real string payload (finding 5)', () {
      // A flat 8 bytes/value badly undercounts a leaf of long strings, letting a
      // nested file slip past the read byte cap. Charge the real content length.
      final long = 'x' * 10000;
      final leaf = RawLeaf(
        values: [long, long],
        reps: [0, 0],
        defs: [1, 1],
        maxDef: 1,
        maxRep: 0,
        dataType: DataType.string,
      );
      // Flat-8 would report 4*8 = 32; the honest figure includes the payload.
      expect(leaf.bytes, greaterThan(2 * 10000));
    });

    // A fixed-width encoder must never truncate. `ByteData.setInt32` keeps only
    // the low 32 bits, so an int32-declared column carrying a wider value wrote
    // a silently different number. Column validation cannot catch this; it
    // checks a column's DECLARED dtype, never its values, so the encoder is the
    // last line of defence and has to check.
    group('fixed-width encoders reject out-of-range values', () {
      /// A column that LIES: declared [type], holding [values] boxed. This is
      /// what a mistyped literal produces (`Kernels.constColumn` boxes).
      Table lying(DataType type, List<int> values) => Table(
        name: 't',
        columns: [
          ListColumn<int>(
            name: 'v',
            dataType: type,
            data: values,
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [Field(name: 'v', dataType: type, nullable: false)],
        ),
      );

      test(
        'an int32 column holding 2^31 throws instead of truncating',
        () async {
          final path = '${dir.path}/wide32.parquet';
          await expectLater(
            ParquetWriter(path).write(lying(DataType.int32, [2147483648])),
            failsWith(Kind.invalid),
          );
        },
      );

      test('an int32 column holding -2^31-1 throws', () async {
        final path = '${dir.path}/narrow32.parquet';
        await expectLater(
          ParquetWriter(path).write(lying(DataType.int32, [-2147483649])),
          failsWith(Kind.invalid),
        );
      });

      test('int32 boundary values still write and round-trip', () async {
        final path = '${dir.path}/bound32.parquet';
        await ParquetWriter(
          path,
        ).write(lying(DataType.int32, [2147483647, -2147483648, 0])).ok;
        expect((await ParquetReader(path).read().ok).column('v')!.toList(), [
          2147483647,
          -2147483648,
          0,
        ]);
      });

      test('a genuine int64 column round-trips past 2^31', () async {
        final path = '${dir.path}/wide64.parquet';
        await ParquetWriter(
          path,
        ).write(lying(DataType.int64, [2147483648, 9223372036854775807])).ok;
        expect((await ParquetReader(path).read().ok).column('v')!.toList(), [
          2147483648,
          9223372036854775807,
        ]);
      });
    });

    // Integer columns default to DELTA_BINARY_PACKED, which zigzags each value
    // and delta. Zigzag maps int64 onto uint64, so any value or delta >= 2^62
    // sets bit 63 and reads back as a negative Dart int: decoding it with an
    // arithmetic shift smears the sign bit and returns a different number.
    group('int64 extremes survive DELTA_BINARY_PACKED', () {
      Table ints(List<int> values) => Table(
        name: 't',
        columns: [
          NumericColumn.int64(name: 'v', values: values, nullable: false),
        ],
        schema: Schema(
          fields: [Field(name: 'v', dataType: DataType.int64, nullable: false)],
        ),
      );

      Future<List<Object?>> roundTrip(String name, List<int> values) async {
        final path = '${dir.path}/$name.parquet';
        await ParquetWriter(path).write(ints(values)).ok;
        return (await ParquetReader(path).read().ok).column('v')!.toList();
      }

      test('a lone int64 max round-trips', () async {
        // The FIRST value is zigzagged directly, so this needs no delta at all.
        expect(await roundTrip('max', [9223372036854775807]), [
          9223372036854775807,
        ]);
      });

      test('a lone int64 min round-trips', () async {
        expect(await roundTrip('min', [-9223372036854775808]), [
          -9223372036854775808,
        ]);
      });

      test('a delta spanning the full int64 range round-trips', () async {
        // min -> max is a delta of 2^64-1, the worst case for zigzag overflow.
        expect(
          await roundTrip('span', [-9223372036854775808, 9223372036854775807]),
          [-9223372036854775808, 9223372036854775807],
        );
      });

      test('values either side of 2^62 round-trip', () async {
        const v =
            4611686018427387904; // 2^62, where `n << 1` starts to overflow
        expect(await roundTrip('p62', [v - 1, v, v + 1]), [v - 1, v, v + 1]);
      });

      test('a descending int64 sequence round-trips', () async {
        expect(await roundTrip('desc', [9223372036854775807, 0, -5]), [
          9223372036854775807,
          0,
          -5,
        ]);
      });

      // The spec makes `delta - minDelta` a uint64. Computing its bit width with
      // SIGNED arithmetic loses any block whose spread reaches 2^63: the value
      // wraps negative, the running max never sees it, the width stays 0, and
      // the whole block encodes as ZERO BYTES: every delta then decodes as
      // minDelta. A single value never reaches the block loop, which is why the
      // lone-max test above passes while these did not.
      test('a block whose spread exceeds 2^63 round-trips', () async {
        // 0 -> max is a delta of 2^63-1; max -> 0 is its negative, so
        // `delta - minDelta` is 2*(2^63-1), which wraps.
        expect(await roundTrip('spread', [0, 9223372036854775807, 0]), [
          0,
          9223372036854775807,
          0,
        ]);
      });

      test('alternating int64 extremes round-trip', () async {
        expect(
          await roundTrip('alt', [
            -9223372036854775808,
            9223372036854775807,
            -9223372036854775808,
            9223372036854775807,
          ]),
          [
            -9223372036854775808,
            9223372036854775807,
            -9223372036854775808,
            9223372036854775807,
          ],
        );
      });

      test('a spread of exactly 2^62 round-trips', () async {
        const v = 4611686018427387904; // 2^62
        expect(await roundTrip('sp62', [0, v, 0]), [0, v, 0]);
      });

      test('a wide spread inside a longer block round-trips', () async {
        // >32 values, so the block spans several miniblocks and the wide pair
        // sits in only one of them.
        final values = [
          for (var i = 0; i < 40; i++) i,
          9223372036854775807,
          for (var i = 0; i < 40; i++) -i,
        ];
        expect(await roundTrip('long', values), values);
      });
    });

    test(
      'a writer instance can be reused without corrupting metadata',
      () async {
        final path = '${dir.path}/reuse.parquet';
        final writer = ParquetWriter(path);
        await writer.write(sample()).ok; // 500 rows
        await writer.write(sample()).ok; // 500 rows again, NOT 1000
        final table = await ParquetReader(path).read().ok;
        expect(table.rowCount, 500);
      },
    );

    group('extended types', () {
      Future<List<dynamic>> roundTrip(Field field, List<dynamic> data) async {
        final path = '${dir.path}/${field.name}.parquet';
        final col = field.dataType == DataType.decimal
            ? ListColumn<BigInt>(
                name: field.name,
                dataType: field.dataType,
                data: data.cast<BigInt?>(),
                nullable: true,
              )
            : ListColumn<DateTime>(
                name: field.name,
                dataType: field.dataType,
                data: data.cast<DateTime?>(),
                nullable: true,
              );
        await ParquetWriter(path)
            .write(
              Table(
                name: 't',
                columns: [col],
                schema: Schema(fields: [field]),
              ),
            )
            .ok;
        return (await ParquetReader(
          path,
        ).read().ok).column(field.name)!.toList();
      }

      test('date round-trips as DateTime', () async {
        final vals = [DateTime.utc(2021, 5, 1), null, DateTime.utc(1990, 1, 1)];
        expect(
          await roundTrip(Field(name: 'd', dataType: DataType.date), vals),
          equals(vals),
        );
      });

      test('timestamp round-trips at microsecond precision', () async {
        final vals = [DateTime.utc(2021, 3, 15, 10, 30, 15, 123, 456), null];
        final got = await roundTrip(
          Field(name: 'ts', dataType: DataType.timestamp),
          vals,
        );
        expect(got, equals(vals));
        expect((got[0] as DateTime).microsecond, 456);
      });

      for (final p in [9, 18, 38]) {
        test('decimal (precision $p) round-trips as BigInt', () async {
          final vals = <BigInt?>[
            BigInt.from(12345),
            BigInt.from(-99),
            null,
            if (p == 38) BigInt.parse('12345678901234567890123456789'),
          ];
          final path = '${dir.path}/dec$p.parquet';
          final field = Field(
            name: 'v',
            dataType: DataType.decimal,
            precision: p,
            scale: 2,
          );
          await ParquetWriter(path)
              .write(
                Table(
                  name: 't',
                  columns: [
                    ListColumn<BigInt>(
                      name: 'v',
                      dataType: DataType.decimal,
                      data: vals,
                      nullable: true,
                    ),
                  ],
                  schema: Schema(fields: [field]),
                ),
              )
              .ok;
          final back = await ParquetReader(path).read().ok;
          expect(back.column('v')!.toList(), equals(vals));
          expect(back.schema.field('v')!.scale, 2);
          expect(back.schema.field('v')!.precision, p);
        });
      }
    });
  });

  group('codecs', () {
    // Reads a fixture and returns its first column's values.
    Future<List<dynamic>> read(String name) async {
      final table = await ParquetReader(
        'test/fixtures/parquet-testing/data/$name',
      ).read().ok;
      return table.columns.first.toList();
    }

    group('Parquet codec support', () {
      test('uncompressed', () async {
        expect(await read('alltypes_plain.parquet'), hasLength(8));
      });

      test('snappy', () async {
        expect(await read('alltypes_plain.snappy.parquet'), [6, 7]);
      });

      test('gzip', () async {
        expect(
          await read('byte_stream_split_extended.gzip.parquet'),
          hasLength(200),
        );
      });

      test('zstd', () async {
        // BYTE_STREAM_SPLIT (encoding 9): assert VALUES, not just row count; the
        // encoding was previously undecoded (silent garbage). These match pyarrow.
        final f32 = (await read(
          'byte_stream_split.zstd.parquet',
        )).cast<double>();
        expect(f32, hasLength(300));
        expect(f32[0], closeTo(1.764052, 1e-5));
        expect(f32[1], closeTo(0.400157, 1e-5));
        expect(f32[2], closeTo(0.978738, 1e-5));
      });

      test('lz4 (hadoop framing)', () async {
        expect(await read('hadoop_lz4_compressed.parquet'), [
          1593604800,
          1593604800,
          1593604801,
          1593604801,
        ]);
      });

      test('lz4 (non-hadoop framing)', () async {
        expect(await read('non_hadoop_lz4_compressed.parquet'), hasLength(4));
      });

      test('lz4 raw', () async {
        expect(await read('lz4_raw_compressed.parquet'), hasLength(4));
      });

      test('rejects brotli', () {
        expect(
          ParquetReader(
            'test/fixtures/parquet-testing/data/large_string_map.brotli.parquet',
          ).read(),
          failsWith(Kind.corrupt),
        );
      });
    });

    group('Parquet nested types', () {
      test('repeated primitive assembles into per-row lists', () async {
        final values = await read('repeated_primitive_no_list.parquet');
        expect(values, hasLength(4));
        expect(values.first, [0, 1, 2, 3]);
      });

      test('list column', () async {
        expect(await read('list_columns.parquet'), isNotEmpty);
      });

      test('nested map column', () async {
        expect(await read('nested_maps.snappy.parquet'), isNotEmpty);
      });
    });

    group('Parquet encoding support', () {
      test('delta binary packed (required)', () async {
        final values = await read('delta_encoding_required_column.parquet');
        expect(values.take(4), [105, 104, 103, 102]);
      });

      test('delta binary packed (optional)', () async {
        final values = await read('delta_encoding_optional_column.parquet');
        expect(values.take(4), [100, 99, 98, 97]);
      });

      test('delta binary packed, every bit width, matches expected', () async {
        const dir = 'test/fixtures/parquet-testing/data';
        final table = await ParquetReader(
          '$dir/delta_binary_packed.parquet',
        ).read().ok;
        final lines = File(
          '$dir/delta_binary_packed_expect.csv',
        ).readAsLinesSync().where((l) => l.isNotEmpty).toList();
        final names = lines.first.split(',');
        for (var c = 0; c < names.length; c++) {
          final expected = [
            for (final line in lines.skip(1)) int.parse(line.split(',')[c]),
          ];
          expect(table.column(names[c])!.toList(), expected, reason: names[c]);
        }
      });

      test('delta length byte array', () async {
        expect(await read('delta_length_byte_array.parquet'), hasLength(1000));
      });

      test('delta byte array', () async {
        expect(await read('delta_byte_array.parquet'), hasLength(1000));
      });

      // PlainDecoder is the ONLY place in the Parquet tree that utf8-decodes a
      // BYTE_ARRAY, and it does so from the SchemaElement. DecoderFactory hands
      // that schema to PlainDecoder but not to the delta byte-array decoders, so
      // a UTF8-annotated column read through them came back as raw bytes under a
      // `string` dtype, and validateColumns compares dtypes only, so it passed.
      test('a UTF8 delta byte array decodes to String', () async {
        const path =
            'test/fixtures/parquet-testing/data/delta_byte_array.parquet';
        final table = await ParquetReader(path).read().ok;
        final column = table.column('c_first_name')!;
        expect(table.schema.field('c_first_name')!.dataType, DataType.string);
        expect(column.toList().first, isA<String>());
        // Ground truth is the fixture's own expectation file.
        final expected = const LineSplitter()
            .convert(
              File(
                'test/fixtures/parquet-testing/data/delta_byte_array_expect.csv',
              ).readAsStringSync(),
            )
            .skip(1)
            .map((line) => line.split(',')[2].replaceAll('"', ''))
            .take(5)
            .toList();
        expect(column.toList().take(5), expected);
      });

      test('a UTF8 delta LENGTH byte array decodes to String', () async {
        const path =
            'test/fixtures/parquet-testing/data/delta_length_byte_array.parquet';
        final table = await ParquetReader(path).read().ok;
        for (final field in table.schema.fields) {
          if (field.dataType != DataType.string) continue;
          final first = table.column(field.name)!.toList().first;
          expect(first, isA<String>(), reason: field.name);
        }
      });

      test('rle boolean', () async {
        expect(await read('rle_boolean_encoding.parquet'), hasLength(68));
      });

      test('dictionary page offset zero', () async {
        expect(await read('dict-page-offset-zero.parquet'), hasLength(39));
      });

      test('required column with v1 page CRC', () async {
        expect(
          await read('datapage_v1-uncompressed-checksum.parquet'),
          isNotEmpty,
        );
      });

      test('legacy FLBA short pages fail hard by default', () async {
        // parquet-mr wrote this with nulls but no definition levels, so pages
        // declare more values than they store. By default that is treated as a
        // truncated/corrupt page and raises rather than yielding wrong data.
        expect(
          ParquetReader(
            'test/fixtures/parquet-testing/data/fixed_length_byte_array.parquet',
          ).read(),
          failsWith(Kind.corrupt),
        );
      });

      test('legacy FLBA short pages read with tolerateTruncatedPages', () async {
        final table = await ParquetReader(
          'test/fixtures/parquet-testing/data/fixed_length_byte_array.parquet',
          options: const ParquetReaderOptions(tolerateTruncatedPages: true),
        ).read().ok;
        final values = table.columns.first.toList();
        expect(values, hasLength(1000));
        expect(values.first, [0, 0, 0x03, 0xe8]); // 1000 as a 4-byte FLBA
      });
    });

    group('Snappy reading', () {
      const base = 'test/fixtures/parquet-testing/data';

      test('reads a flat DataPageV1 snappy file', () async {
        final table = await ParquetReader(
          '$base/alltypes_plain.snappy.parquet',
        ).read().ok;

        expect(table.columns, hasLength(11));
        expect(table.columns.first.length, 2);
        expect(table.columns.first.name, 'id');
        expect(table.columns.first.toList(), [6, 7]);
      });

      test('reads a DataPageV2 snappy file with an empty data page', () async {
        final table = await ParquetReader(
          '$base/datapage_v2_empty_datapage.snappy.parquet',
        ).read().ok;

        expect(table.columns, isNotEmpty);
      });

      test(
        'decompresses DataPageV2 snappy pages (levels stored uncompressed)',
        () async {
          // DataPageV2 keeps repetition/definition levels uncompressed ahead of
          // the compressed values; the page reader must only run the codec over
          // the value bytes. Drive PageReader directly so the assertion covers
          // decompression independently of higher-level value decoding.
          final reader = FileReader('$base/datapage_v2.snappy.parquet');
          await reader.open();
          try {
            final metadata = await MetadataReader(reader).readMetadata();
            final column = metadata.rowGroups.first.columns.first.metaData!;
            final start = column.dictionaryPageOffset ?? column.dataPageOffset;
            final pages = PageReader(
              reader,
              start,
              column.totalCompressedSize,
              column.codec,
            );

            final page = await pages.readNext();
            expect(page, isNotNull);
            expect(page!.data, isNotEmpty);
          } finally {
            await reader.close();
          }
        },
      );
    });

    group('Hadoop LZ4 decode', () {
      // The Hadoop-framed LZ4 length decode reads unsigned big-endian via
      // ByteData (portable on web; >2GB lengths don't go negative). Verify a
      // valid multi-block Hadoop frame still decodes correctly.
      test('decodes a multi-block Hadoop-framed LZ4 payload', () {
        final lz4 = Lz4Codec(format: Lz4Format.raw);
        final builder = BytesBuilder();

        void be32(int v) {
          final b = ByteData(4)..setUint32(0, v, Endian.big);
          builder.add(b.buffer.asUint8List());
        }

        // Two Hadoop frames, each one uncompressed-block made of a single raw
        // sub-block: [uncompressedSize][compLen][lz4 block].
        final blocks = [
          Uint8List.fromList(List.generate(500, (i) => i % 256)),
          Uint8List.fromList(List.generate(300, (i) => (i * 7) % 256)),
        ];
        for (final raw in blocks) {
          final comp = lz4.compress(raw);
          be32(raw.length); // uncompressed size of this frame
          be32(comp.length); // compressed sub-block length
          builder.add(comp);
        }

        final codec = codecFor(thrift.CompressionCodec.lz4); // hadoop=true
        final out = codec.decompress(builder.toBytes());

        expect(out, [...blocks[0], ...blocks[1]]);
      });

      test('cumulative output over maxSize is rejected (bomb)', () {
        // Two frames summing to 1000 decompressed bytes. Under a generous cap
        // they decode; under a cap between one and two frames the bomb must be
        // rejected (no OOM / no oversized output) rather than returning 1000.
        final lz4 = Lz4Codec(format: Lz4Format.raw);
        Uint8List framed() {
          final builder = BytesBuilder();
          void be32(int v) => builder.add(
            (ByteData(4)..setUint32(0, v, Endian.big)).buffer.asUint8List(),
          );
          for (final raw in [
            Uint8List.fromList(List.generate(500, (i) => i % 256)),
            Uint8List.fromList(List.generate(500, (i) => (i * 3) % 256)),
          ]) {
            final comp = lz4.compress(raw);
            be32(raw.length);
            be32(comp.length);
            builder.add(comp);
          }
          return builder.toBytes();
        }

        // Generous cap: the full 1000 bytes decode.
        expect(
          codecFor(
            thrift.CompressionCodec.lz4,
            maxSize: 100000,
          ).decompress(framed()).length,
          1000,
        );
        // Tight cap below the cumulative total: rejected, not silently returned.
        expect(
          () => codecFor(
            thrift.CompressionCodec.lz4,
            maxSize: 600,
          ).decompress(framed()),
          throwsA(anything),
        );
      });
    });
  });

  group('checksums', () {
    // Covers #5: optional Parquet page CRC32, written when [writeChecksums] is
    // set and verified when [verifyChecksums] is set; a corrupted page is
    // rejected.
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('pq_crc_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table sample() => Table(
      name: 't',
      columns: [
        NumericColumn.int32(
          name: 'id',
          values: List.generate(20000, (i) => i),
          nullable: false,
        ),
        ListColumn<String>(
          name: 'name',
          dataType: DataType.string,
          data: List.generate(20000, (i) => 'item_${i % 11}'),
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

    Future<String> writeChecksummed() async {
      final path = '${dir.path}/crc.parquet';
      await ParquetWriter(
        path,
        options: const ParquetWriterOptions(writeChecksums: true),
      ).write(sample()).ok;
      return path;
    }

    test('round-trips with checksums written and verified', () async {
      final path = await writeChecksummed();
      final table = await ParquetReader(
        path,
        options: const ParquetReaderOptions(verifyChecksums: true),
      ).read().ok;
      expect(table.rowCount, 20000);
      expect(table.column('id')!.toList().last, 19999);
      expect(table.column('name')!.toList().first, 'item_0');
    });

    test('a corrupted page is rejected when verifyChecksums is on', () async {
      final path = await writeChecksummed();
      // Flip a byte well inside the first column's compressed page payload
      // (past the page header, far from the trailing footer metadata).
      final bytes = await File(path).readAsBytes();
      final mut = Uint8List.fromList(bytes);
      const at = 1000;
      mut[at] = mut[at] ^ 0xFF;
      await File(path).writeAsBytes(mut);

      expect(
        ParquetReader(
          path,
          options: const ParquetReaderOptions(verifyChecksums: true),
        ).read(),
        failsWith(Kind.corrupt),
      );
    });

    test('verifyChecksums off ignores a present CRC', () async {
      final path = await writeChecksummed();
      // Default options: verifyChecksums == false → no CRC check at all.
      final table = await ParquetReader(path).read().ok;
      expect(table.rowCount, 20000);
    });
  });

  group('integrity', () {
    // Parquet reads fail loud on corruption/truncation by default, and
    // our own writes carry CRCs that our reads verify.
    Table sample() => Table(
      name: 't',
      columns: [
        NumericColumn.int64(
          name: 'id',
          values: [for (var i = 0; i < 5000; i++) i],
          nullable: false,
        ),
        ListColumn<String>(
          name: 'name',
          dataType: DataType.string,
          data: [for (var i = 0; i < 5000; i++) 'n$i'],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'name', dataType: DataType.string, nullable: false),
        ],
      ),
    );

    group('checksums', () {
      test('a corrupt-checksum fixture throws by default', () async {
        const f =
            'test/fixtures/parquet-testing/data/datapage_v1-corrupt-checksum.parquet';
        expect(ParquetReader(f).read(), failsWith(Kind.corrupt));
      });

      test('verifyChecksums:false skips the check (decodes anyway)', () async {
        const f =
            'test/fixtures/parquet-testing/data/datapage_v1-corrupt-checksum.parquet';
        final t = await ParquetReader(
          f,
          options: const ParquetReaderOptions(verifyChecksums: false),
        ).read().ok;
        expect(t.rowCount, greaterThan(0));
      });

      test('default write emits CRCs that a default read verifies', () async {
        final dir = Directory.systemTemp.createTempSync('pqcrc_');
        final path = '${dir.path}/t.parquet';
        await ParquetWriter(
          path,
        ).write(sample()).ok; // writeChecksums defaults true
        final back = await ParquetReader(
          path,
        ).read().ok; // verifyChecksums defaults true
        expect(back.rowCount, 5000);
        expect(back.column('id')!.toList().last, 4999);
        dir.deleteSync(recursive: true);
      });
    });

    group('truncation', () {
      test('a physically truncated file fails loud', () async {
        final dir = Directory.systemTemp.createTempSync('pqtrunc_');
        final path = '${dir.path}/t.parquet';
        await ParquetWriter(path).write(sample()).ok;
        final bytes = File(path).readAsBytesSync();
        // Lop off the tail of the data (keep the footer-less middle truncated):
        // chop ~40% so a page read runs past EOF.
        final cut = (bytes.length * 0.6).floor();
        File(path).writeAsBytesSync(Uint8List.sublistView(bytes, 0, cut));

        // Footer is gone, so this throws regardless; the point is it never returns
        // fabricated data. Tolerant mode must not paper over a physical short read.
        expect(() => ParquetReader(path).read().ok, throwsA(isA<Exception>()));
        expect(
          () => ParquetReader(
            path,
            options: const ParquetReaderOptions(
              tolerateTruncatedPages: true,
              verifyChecksums: false,
            ),
          ).read().ok,
          throwsA(isA<Exception>()),
        );
        dir.deleteSync(recursive: true);
      });

      // Overstate the declared value count in the footer, leaving the pages
      // untouched: the chunk now promises more values than its pages hold, the
      // shape parquet-mr's legacy short-page files have. Patching only the
      // footer keeps every page (and its CRC) valid, so the read reaches the
      // decoder's truncation handling rather than failing on a short file.
      Future<String> overstated(Directory dir, Table table, int extra) async {
        final path = '${dir.path}/overstated.parquet';
        await ParquetWriter(path).write(table).ok;
        final bytes = File(path).readAsBytesSync();
        final length = ByteData.view(
          bytes.buffer,
          bytes.offsetInBytes,
        ).getInt32(bytes.length - 8, Endian.little);
        final start = bytes.length - 8 - length;
        final metadata = thrift.FileMetaData()
          ..read(
            CompactReader(Uint8List.sublistView(bytes, start, start + length)),
          );
        metadata.numRows += extra;
        for (final group in metadata.rowGroups) {
          group.numRows += extra;
          for (final column in group.columns) {
            column.metaData!.numValues += extra;
          }
        }
        final protocol = CompactWriter();
        metadata.write(protocol);
        final footer = protocol.takeBytes();
        final tail = Uint8List(8);
        ByteData.view(tail.buffer).setInt32(0, footer.length, Endian.little);
        tail.setAll(4, 'PAR1'.codeUnits);
        File(path).writeAsBytesSync(
          (BytesBuilder()
                ..add(Uint8List.sublistView(bytes, 0, start))
                ..add(footer)
                ..add(tail))
              .takeBytes(),
        );
        return path;
      }

      Table numbers({required bool nullable}) => Table(
        name: 't',
        columns: [
          NumericColumn.int64(
            name: 'i',
            values: [1, 2, 3, 4, 5],
            nullable: nullable,
          ),
          NumericColumn.float64(
            name: 'd',
            values: [1.5, 2.5, 3.5, 4.5, 5.5],
            nullable: nullable,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'i', dataType: DataType.int64, nullable: nullable),
            Field(name: 'd', dataType: DataType.float64, nullable: nullable),
          ],
        ),
      );

      for (final nullable in [false, true]) {
        final what = nullable ? 'an optional' : 'a required';

        test('$what short chunk fails loud by default', () async {
          final dir = Directory.systemTemp.createTempSync('pqpad_');
          addTearDown(() => dir.deleteSync(recursive: true));
          final path = await overstated(dir, numbers(nullable: nullable), 3);
          await expectLater(
            ParquetReader(path).read(),
            failsWith(Kind.corrupt),
          );
        });

        test(
          '$what short chunk pads with null, not zero, when tolerated',
          () async {
            final dir = Directory.systemTemp.createTempSync('pqpad_');
            addTearDown(() => dir.deleteSync(recursive: true));
            final path = await overstated(dir, numbers(nullable: nullable), 3);
            final table = await ParquetReader(
              path,
              options: const ParquetReaderOptions(tolerateTruncatedPages: true),
            ).read().ok;
            // A fabricated 0/0.0 tail is indistinguishable from real data; the
            // boxed path has always padded with null, and the typed path must
            // give the same answer for the same option.
            expect(table.column('i')!.toList(), [
              1,
              2,
              3,
              4,
              5,
              null,
              null,
              null,
            ]);
            expect(table.column('d')!.toList(), [
              1.5,
              2.5,
              3.5,
              4.5,
              5.5,
              null,
              null,
              null,
            ]);
            // The data is authoritative: a padded column is nullable however
            // the file declared it.
            for (final field in table.schema.fields) {
              expect(field.nullable, isTrue, reason: field.name);
            }
          },
        );
      }

      test(
        'streaming readBatches pads identically to eager read when tolerated',
        () async {
          final dir = Directory.systemTemp.createTempSync('pqpad_');
          addTearDown(() => dir.deleteSync(recursive: true));
          final path = await overstated(dir, numbers(nullable: true), 3);
          const options = ParquetReaderOptions(tolerateTruncatedPages: true);
          final eager = await ParquetReader(path, options: options).read().ok;
          // A small batch size makes the null tail span a batch boundary.
          final streamed = <Map<String, dynamic>>[];
          await for (final batch in ParquetReader(
            path,
            options: options,
          ).readBatches(batchSize: 3).ok) {
            streamed.addAll(batch.toRows());
          }
          // Both APIs report the DECLARED row count with a null-padded tail:
          // selecting an API must never change the row count of a file.
          expect(streamed.length, eager.rowCount);
          expect(streamed.length, 8); // 5 real + 3 padded
          expect(streamed.map((r) => r['i']).toList(), [
            1,
            2,
            3,
            4,
            5,
            null,
            null,
            null,
          ]);
          expect(streamed.map((r) => r['d']).skip(5), everyElement(isNull));
        },
      );
    });

    group('resource leaks on failure', () {
      test(
        'reading many corrupt files does not exhaust file descriptors',
        () async {
          final dir = Directory.systemTemp.createTempSync('pqfd_');
          final bad = '${dir.path}/bad.parquet';
          File(
            bad,
          ).writeAsBytesSync(Uint8List.fromList([0, 1, 2, 3])); // bad magic
          // If _initialize leaked the fd on the bad-magic throw, this loop would hit
          // EMFILE well before completing; instead every read throws cleanly.
          for (var i = 0; i < 600; i++) {
            await expectLater(
              ParquetReader(bad).read(),
              failsWith(Kind.corrupt),
            );
          }
          dir.deleteSync(recursive: true);
        },
      );

      test('a write whose schema conversion fails leaves no .tmp', () async {
        final dir = Directory.systemTemp.createTempSync('pqwtmp_');
        final path = '${dir.path}/t.parquet';
        // A list field with no child element makes toParquetSchema throw, before
        // any file handle is acquired.
        final bad = Table(
          name: 't',
          columns: [
            ListColumn(
              name: 'l',
              dataType: DataType.list,
              data: [
                [1],
              ],
              nullable: true,
            ),
          ],
          schema: Schema(
            fields: [Field(name: 'l', dataType: DataType.list, nullable: true)],
          ),
        );
        await expectLater(
          ParquetWriter(path).write(bad),
          failsWith(Kind.invalid),
        );
        expect(File('$path.tmp').existsSync(), isFalse);
        expect(File(path).existsSync(), isFalse);
        dir.deleteSync(recursive: true);
      });
    });
  });

  group('resilience', () {
    // Builds a chain of [depth] nested empty structs as compact-protocol bytes.
    Uint8List nested(int depth) {
      final w = CompactWriter();
      void write(int d) {
        w.writeStructBegin(TStruct());
        if (d > 0) {
          w.writeFieldBegin(const TField('f', TType.STRUCT, 1));
          write(d - 1);
          w.writeFieldEnd();
        }
        w.writeFieldStop();
        w.writeStructEnd();
      }

      write(depth);
      return w.takeBytes();
    }

    group('Thrift skip recursion is bounded (#2.4)', () {
      test('a shallow nested struct skips fine', () {
        expect(
          () => TProtocolUtil.skip(CompactReader(nested(10)), TType.STRUCT),
          returnsNormally,
        );
      });

      test('a too-deep nested struct throws instead of overflowing', () {
        expect(
          () => TProtocolUtil.skip(CompactReader(nested(200)), TType.STRUCT),
          throwsA(isA<FormatException>()),
        );
      });
    });

    // The footer's schema is a flat list, but every reader walks it as a tree
    // recursively, so depth and size are bounded when the footer is read.
    group('Parquet schema limits', () {
      late Directory dir;
      setUp(() => dir = Directory.systemTemp.createTempSync('pq_schema_'));
      tearDown(() => dir.deleteSync(recursive: true));

      String craft(List<thrift.SchemaElement> schema) {
        final metadata = thrift.FileMetaData()
          ..version = 1
          ..schema = schema
          ..numRows = 0
          ..rowGroups = [];
        final writer = CompactWriter();
        metadata.write(writer);
        final footer = writer.takeBytes();
        final size = ByteData(4)..setInt32(0, footer.length, Endian.little);
        final path = '${dir.path}/crafted.parquet';
        File(path).writeAsBytesSync([
          ...ascii.encode('PAR1'),
          ...footer,
          ...size.buffer.asUint8List(),
          ...ascii.encode('PAR1'),
        ]);
        return path;
      }

      thrift.SchemaElement group(String name, int children) =>
          thrift.SchemaElement()
            ..name = name
            ..numChildren = children
            ..repetitionType = thrift.FieldRepetitionType.optional;

      thrift.SchemaElement leaf(String name) => thrift.SchemaElement()
        ..name = name
        ..type = thrift.ParquetType.int32
        ..repetitionType = thrift.FieldRepetitionType.required;

      test('a schema nested past the depth limit throws', () async {
        final path = craft([
          group('root', 1),
          for (var i = 0; i < 50000; i++) group('g$i', 1),
          leaf('x'),
        ]);
        await expectLater(
          ParquetReader(path).schema(),
          failsWith(Kind.corrupt),
        );
      });

      test('a schema wider than the field limit throws', () async {
        final path = craft([
          group('root', defaultMaxSchemaFields + 1),
          for (var i = 0; i <= defaultMaxSchemaFields; i++) leaf('c$i'),
        ]);
        await expectLater(
          ParquetReader(path).schema(),
          failsWith(Kind.corrupt),
        );
      });

      test('a child count running past the list throws', () async {
        final path = craft([group('root', 3), leaf('a')]);
        await expectLater(
          ParquetReader(path).schema(),
          failsWith(Kind.corrupt),
        );
      });

      test('a modest nested schema still reads', () async {
        final path = craft([
          group('root', 1),
          for (var i = 0; i < 20; i++) group('g$i', 1),
          leaf('x'),
        ]);
        expect(await ParquetReader(path).schema().ok, isNotNull);
      });
    });

    group('truncated ULEB128', () {
      // A varint whose continuation bit is still set at end-of-buffer is
      // truncated input; returning the bits read so far is a fabricated value.
      test('a varint running off the end throws', () {
        // 0x80 = "more bytes follow", then nothing.
        final reader = ByteReader(Uint8List.fromList([0x80]));
        expect(
          reader.readVarint,
          throwsA(
            isA<FormatException>().having(
              (e) => e.message,
              'message',
              contains('varint'),
            ),
          ),
        );
      });

      test('a complete varint still reads', () {
        // 0xAC 0x02 = 300.
        final reader = ByteReader(Uint8List.fromList([0xAC, 0x02]));
        expect(reader.readVarint(), 300);
      });

      test('a single-byte varint still reads', () {
        expect(ByteReader(Uint8List.fromList([0x7F])).readVarint(), 127);
      });
    });

    group('deprecated BIT_PACKED levels', () {
      // The level guards required Encoding.rle, so a BIT_PACKED level section
      // fell through un-read: `offset` stayed at 0 and the level bytes were
      // decoded as VALUES: fabricated data, no exception.
      thrift.PageHeader page(thrift.Encoding levelEncoding) =>
          thrift.PageHeader()
            ..type = thrift.PageType.dataPage
            ..uncompressedPageSize = 16
            ..compressedPageSize = 16
            ..dataPageHeader = (thrift.DataPageHeader()
              ..numValues = 2
              ..encoding = thrift.Encoding.plain
              ..definitionLevelEncoding = levelEncoding
              ..repetitionLevelEncoding = thrift.Encoding.rle);

      PageDecoder decoder() => PageDecoder(
        type: thrift.ParquetType.int32,
        maxDefinitionLevel: 1,
        maxRepetitionLevel: 0,
      );

      test('a BIT_PACKED definition level is rejected, not skipped', () {
        expect(
          () => decoder().parseBody(
            page(thrift.Encoding.bitPacked),
            Uint8List(16),
          ),
          throwsA(
            isA<FormatException>().having(
              (e) => e.message,
              'message',
              contains('definition-level encoding'),
            ),
          ),
        );
      });

      test('an RLE definition level is still accepted', () {
        expect(
          () => decoder().parseBody(page(thrift.Encoding.rle), Uint8List(16)),
          returnsNormally,
        );
      });
    });

    group('PLAIN typed decode rejects a truncated page (#2.5)', () {
      test('short buffer throws FormatException, not a raw RangeError', () {
        final decoder = PageDecoder(type: thrift.ParquetType.int32);
        final out = List<num>.filled(10, 0);
        // 10 int32 values need 40 bytes; supply only 8.
        final short = Uint8List(8);
        expect(
          () => decoder.decodeValuesTyped(
            short,
            10,
            thrift.Encoding.plain,
            out,
            0,
          ),
          throwsA(isA<FormatException>()),
        );
      });

      test('a full buffer still decodes', () {
        final decoder = PageDecoder(type: thrift.ParquetType.int32);
        final out = List<num>.filled(2, 0);
        final buf = Uint8List(8);
        ByteData.view(buf.buffer)
          ..setInt32(0, 7, Endian.little)
          ..setInt32(4, 9, Endian.little);
        decoder.decodeValuesTyped(buf, 2, thrift.Encoding.plain, out, 0);
        expect(out, [7, 9]);
      });
    });

    group('corrupt page header fails loud', () {
      test('a header larger than the chunk throws FormatException', () async {
        // A compact field header (field 1, type binary) declaring a 100-byte
        // string, with only a few bytes following: the header parse runs off the
        // chunk. It must surface as FormatException, not a raw RangeError.
        final dir = Directory.systemTemp.createTempSync('pghdr_');
        try {
          final path = '${dir.path}/c.bin';
          final bytes = Uint8List.fromList([0x18, 0x64, 0, 0, 0, 0, 0]);
          File(path).writeAsBytesSync(bytes);
          final reader = FileReader(path);
          await reader.open();
          try {
            final pr = PageReader(
              reader,
              0,
              bytes.length,
              thrift.CompressionCodec.uncompressed,
            );
            await expectLater(
              pr.readHeaderWithSize(),
              throwsA(isA<FormatException>()),
            );
          } finally {
            await reader.close();
          }
        } finally {
          dir.deleteSync(recursive: true);
        }
      });
    });

    group('native sqrt stddev (#1.2)', () {
      test('NumericColumn.stats stddev is correct', () {
        final col = NumericColumn.int64(
          name: 'v',
          values: [2, 4, 4, 4, 5, 5, 7, 9],
          nullable: false,
        );
        // population variance = 4 -> stddev = 2.0
        expect(col.stats!.stdDev, closeTo(2.0, 1e-9));
      });
    });
  });

  // A nested column whose every node from the root to the leaf is REQUIRED has
  // maxDef == 0, so there are no definition levels to carry. The writer must
  // then omit the level section entirely: emitting a zero-width one writes
  // bytes the reader (and pyarrow) never read back, shifting every value.
  group('all-required nesting', () {
    Future<Table> roundTrip(Table table) async {
      final dir = Directory.systemTemp.createTempSync('pqreq_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final path = '${dir.path}/t.parquet';
      await ParquetWriter(path).write(table).ok;
      return ParquetReader(path).read().ok;
    }

    test('a required struct of a required leaf round-trips', () async {
      final rows = [
        for (var i = 1; i <= 8; i++)
          {
            's': {'a': i},
          },
      ];
      final table = Table(
        name: 't',
        columns: [
          ListColumn(
            name: 's',
            dataType: DataType.struct,
            data: [for (final r in rows) r['s']],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(
              name: 's',
              dataType: DataType.struct,
              nullable: false,
              children: [
                Field(name: 'a', dataType: DataType.int64, nullable: false),
              ],
            ),
          ],
        ),
      );
      final back = await roundTrip(table);
      expect(back.column('s')!.toList(), [
        for (var i = 1; i <= 8; i++) {'a': i},
      ]);
    });

    test('a required list of required elements round-trips', () async {
      // maxRep > 0 here, so only the def-level side is at issue.
      final data = [
        [1, 2],
        [3],
        [4, 5, 6],
      ];
      final table = Table(
        name: 't',
        columns: [
          ListColumn(
            name: 'l',
            dataType: DataType.list,
            data: data,
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(
              name: 'l',
              dataType: DataType.list,
              nullable: false,
              children: [
                Field(
                  name: 'element',
                  dataType: DataType.int64,
                  nullable: false,
                ),
              ],
            ),
          ],
        ),
      );
      final back = await roundTrip(table);
      expect(back.column('l')!.toList(), data);
    });

    test('a nested read prunes row groups by the pushed filter', () async {
      // A file with a NESTED column (struct) takes the Dremel-assembly read
      // path, which used to decode EVERY row group regardless of the filter.
      final dir = Directory.systemTemp.createTempSync('pqnestprune_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final path = '${dir.path}/nested.parquet';
      final table = Table(
        name: 't',
        columns: [
          NumericColumn.int32(
            name: 'id',
            values: [for (var i = 0; i < 1000; i++) i], // ascending
            nullable: false,
          ),
          ListColumn(
            name: 's',
            dataType: DataType.struct,
            data: [
              for (var i = 0; i < 1000; i++) {'a': i},
            ],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'id', dataType: DataType.int32, nullable: false),
            Field(
              name: 's',
              dataType: DataType.struct,
              nullable: false,
              children: [
                Field(name: 'a', dataType: DataType.int64, nullable: false),
              ],
            ),
          ],
        ),
      );
      await ParquetWriter(
        path,
        options: const ParquetWriterOptions(rowGroupSize: 100),
      ).write(table).ok; // 10 row groups

      ParquetReader.rowGroupsPruned = 0;
      final t = await ParquetReader(
        path,
        options: ParquetReaderOptions(
          filter: RowFilter.compare('id', FilterOp.ge, 500),
        ),
      ).read().ok;
      // Row groups whose id-max < 500 are pruned (5 of 10), so only the
      // surviving groups' rows (id 500..999) are decoded.
      expect(ParquetReader.rowGroupsPruned, greaterThan(0));
      final ids = t.column('id')!.toList().cast<int>();
      expect(ids.first, 500);
      expect(ids.last, 999);
      expect(t.rowCount, 500);
      // The nested struct column survived assembly for the kept groups.
      expect((t.column('s')!.toList().first as Map)['a'], 500);
    });

    test('a nested-leaf filter never mis-prunes (result stays complete)', () async {
      // Filter on the nested leaf `s.a` (dotted). Row-group pruning is a
      // superset filter, so EVERY row group containing a matching value must be
      // kept: the result must contain all rows where s.a >= 500, whether or not
      // any group is actually pruned.
      final dir = Directory.systemTemp.createTempSync('pqnestleaf_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final path = '${dir.path}/nl.parquet';
      final table = Table(
        name: 't',
        columns: [
          NumericColumn.int32(
            name: 'id',
            values: [for (var i = 0; i < 1000; i++) i],
            nullable: false,
          ),
          ListColumn(
            name: 's',
            dataType: DataType.struct,
            data: [
              for (var i = 0; i < 1000; i++) {'a': i},
            ],
            nullable: false,
          ),
        ],
        schema: Schema(
          fields: [
            Field(name: 'id', dataType: DataType.int32, nullable: false),
            Field(
              name: 's',
              dataType: DataType.struct,
              nullable: false,
              children: [
                Field(name: 'a', dataType: DataType.int64, nullable: false),
              ],
            ),
          ],
        ),
      );
      await ParquetWriter(
        path,
        options: const ParquetWriterOptions(rowGroupSize: 100),
      ).write(table).ok;
      final t = await ParquetReader(
        path,
        options: ParquetReaderOptions(
          filter: RowFilter.compare('s.a', FilterOp.ge, 500),
        ),
      ).read().ok;
      // Every row whose s.a >= 500 survives (row-group granularity keeps whole
      // groups, so the count is a multiple of the group size, never fewer).
      final aVals = [for (final v in t.column('s')!.toList()) (v as Map)['a']];
      expect(aVals.where((a) => (a as int) >= 500).length, 500);
    });
  });

  group('nested', () {
    // Parquet nested READ: the reader reassembles dotted leaf columns into nested
    // struct/list/map columns (single-level shapes); deeper shapes fall back to
    // flat leaves without crashing.
    const dir = 'test/fixtures/parquet-testing/data';

    test('LIST columns read back as list columns of their elements', () async {
      final t = await ParquetReader('$dir/list_columns.parquet').read().ok;
      final f = t.schema.field('int64_list');
      expect(f, isNotNull);
      expect(f!.dataType, DataType.list);
      expect(f.children.single.dataType, DataType.int64);
      final first = t.column('int64_list')!.toList().first;
      expect(first, isA<List>());
      expect(first, equals([1, 2, 3]));
    });

    test(
      'readBatches reassembles a nested schema (same shape as read), bounded '
      'per row group',
      () async {
        final batches = await ParquetReader(
          '$dir/list_columns.parquet',
        ).readBatches().ok.toList();
        expect(batches, isNotEmpty);
        // Every streamed batch carries the reassembled nested schema, matching
        // read() rather than the flat leaf shape.
        for (final b in batches) {
          expect(b.schema.field('int64_list')!.dataType, DataType.list);
        }
        final whole = await ParquetReader(
          '$dir/list_columns.parquet',
        ).read().ok;
        final streamedRows = batches.fold(0, (s, b) => s + b.rowCount);
        expect(streamedRows, whole.rowCount);
      },
    );

    test('STRUCT columns read back as struct (Map) columns', () async {
      final t = await ParquetReader(
        '$dir/nested_structs.rust.parquet',
      ).read().ok;
      final f = t.schema.field('roll_num');
      expect(f, isNotNull);
      expect(f!.dataType, DataType.struct);
      expect(f.children.map((c) => c.name), contains('min'));
      final first = t.column('roll_num')!.toList().first;
      expect(first, isA<Map>());
      expect((first as Map).containsKey('min'), isTrue);
    });

    test(
      'a deeply-nested (map-of-map) file still reads without crashing',
      () async {
        final t = await ParquetReader(
          '$dir/nested_maps.snappy.parquet',
        ).read().ok;
        expect(
          t.rowCount,
          greaterThan(0),
        ); // falls back to flat leaves, no throw
      },
    );

    group('write -> read round-trip', () {
      late Directory tmp;
      setUp(() => tmp = Directory.systemTemp.createTempSync('pqnest_'));
      tearDown(() => tmp.deleteSync(recursive: true));

      Future<Table> roundTrip(Table t) async {
        final path = '${tmp.path}/n.parquet';
        await ParquetWriter(path).write(t).ok;
        return ParquetReader(path).read().ok;
      }

      test('list / struct / map of scalars round-trip', () async {
        final t = Table(
          name: 't',
          columns: [
            ListColumn(
              name: 'l',
              dataType: DataType.list,
              data: [
                [1, 2, 3],
                [4],
                <int>[],
                [5, 6],
              ],
              nullable: true,
            ),
            ListColumn(
              name: 's',
              dataType: DataType.struct,
              data: [
                {'a': 1, 'b': 'x'},
                {'a': 2, 'b': 'y'},
                {'a': 3, 'b': null},
                {'a': 4, 'b': 'z'},
              ],
              nullable: true,
            ),
            ListColumn(
              name: 'm',
              dataType: DataType.map,
              data: [
                {'k1': 1, 'k2': 2},
                <String, int>{},
                {'z': 9},
                {'p': 7},
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
        final back = await roundTrip(t);
        for (final f in ['l', 's', 'm']) {
          expect(
            back.column(f)!.toList().toString(),
            equals(t.column(f)!.toList().toString()),
            reason: f,
          );
        }
      });
    });
  });

  group('deep nested', () {
    // arbitrary-depth nested Parquet write + read round-trips
    // through our own reader (Dremel shred + assemble). Validated separately
    // against Spark/pyarrow in the interop suite.
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('deepnest_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Field intf([String n = 'item']) =>
        Field(name: n, dataType: DataType.int64, nullable: true);

    Future<List<dynamic>> roundTrip(Field field, List<dynamic> data) async {
      final p = '${dir.path}/x.parquet';
      await ParquetWriter(p)
          .write(
            Table(
              name: 't',
              columns: [
                ListColumn(
                  name: field.name,
                  dataType: field.dataType,
                  data: data,
                  nullable: true,
                ),
              ],
              schema: Schema(fields: [field]),
            ),
          )
          .ok;
      final back = await ParquetReader(p).read().ok;
      return back.column(field.name)!.toList();
    }

    test('list<list<int>> round-trips (incl. empty + null)', () async {
      final field = Field(
        name: 'll',
        dataType: DataType.list,
        nullable: true,
        children: [
          Field(
            name: 'element',
            dataType: DataType.list,
            nullable: true,
            children: [intf()],
          ),
        ],
      );
      final data = [
        [
          [1, 2],
          [3],
        ],
        [<int>[]],
        null,
        [
          [4],
          [5, 6],
        ],
      ];
      expect((await roundTrip(field, data)).toString(), data.toString());
    });

    test('struct<x:int, tags:list<int>> round-trips', () async {
      final field = Field(
        name: 's',
        dataType: DataType.struct,
        nullable: true,
        children: [
          Field(name: 'x', dataType: DataType.int64, nullable: true),
          Field(
            name: 'tags',
            dataType: DataType.list,
            nullable: true,
            children: [intf()],
          ),
        ],
      );
      final data = [
        {
          'x': 1,
          'tags': [1, 2],
        },
        {'x': 2, 'tags': <int>[]},
      ];
      expect((await roundTrip(field, data)).toString(), data.toString());
    });

    test('list<struct<a:int,b:string>> round-trips', () async {
      final field = Field(
        name: 'ls',
        dataType: DataType.list,
        nullable: true,
        children: [
          Field(
            name: 'element',
            dataType: DataType.struct,
            nullable: true,
            children: [
              Field(name: 'a', dataType: DataType.int64, nullable: true),
              Field(name: 'b', dataType: DataType.string, nullable: true),
            ],
          ),
        ],
      );
      final data = [
        [
          {'a': 1, 'b': 'x'},
          {'a': 2, 'b': null},
        ],
        <Map>[],
      ];
      expect((await roundTrip(field, data)).toString(), data.toString());
    });

    test('map<string, struct<a:int>> round-trips', () async {
      final field = Field(
        name: 'm',
        dataType: DataType.map,
        nullable: true,
        children: [
          Field(
            name: 'key_value',
            dataType: DataType.struct,
            nullable: false,
            children: [
              Field(name: 'key', dataType: DataType.string, nullable: false),
              Field(
                name: 'value',
                dataType: DataType.struct,
                nullable: true,
                children: [
                  Field(name: 'a', dataType: DataType.int64, nullable: true),
                ],
              ),
            ],
          ),
        ],
      );
      final data = [
        {
          'k1': {'a': 1},
          'k2': {'a': 2},
        },
        <String, dynamic>{},
      ];
      expect((await roundTrip(field, data)).toString(), data.toString());
    });
  });

  group('streaming', () {
    Table ids(int start, int count) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(
          name: 'id',
          values: [for (int i = 0; i < count; i++) start + i],
          nullable: false,
        ),
        ListColumn<String>(
          name: 'name',
          dataType: DataType.string,
          data: [for (int i = 0; i < count; i++) 'n${start + i}'],
          nullable: false,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(name: 'name', dataType: DataType.string, nullable: false),
        ],
      ),
    );

    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('pq_stream_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('streaming write accrues batches into full row groups', () async {
      final path = '${dir.path}/s.parquet';
      // 5 batches of 50 rows, rowGroupSize 100 -> 250 rows in 3 row groups.
      final batches = [
        for (int b = 0; b < 5; b++) DataFrame.fromTable(ids(b * 50, 50)),
      ];
      await DataFrameIO.writeParquetStream(
        path,
        Stream.fromIterable(batches),
        options: const ParquetWriterOptions(rowGroupSize: 100),
      ).ok;

      final t = await ParquetReader(path).read().ok;
      expect(t.rowCount, 250);
      expect(t.column('id')!.toList(), List.generate(250, (i) => i));
      expect(t.column('name')!.toList().last, 'n249');
    });

    test('write(Table) still round-trips (memory-fixed path)', () async {
      final path = '${dir.path}/w.parquet';
      await ParquetWriter(
        path,
        options: const ParquetWriterOptions(rowGroupSize: 100),
      ).write(ids(0, 250)).ok;
      final t = await ParquetReader(path).read().ok;
      expect(t.rowCount, 250);
      expect(t.column('id')!.toList().first, 0);
      expect(t.column('id')!.toList().last, 249);
    });

    test('end-to-end: CSV stream -> Parquet stream -> read', () async {
      final csv = '${dir.path}/in.csv';
      final rows = [for (int i = 0; i < 300; i++) '$i,v$i'];
      File(csv).writeAsStringSync('id,name\n${rows.join('\n')}');

      final pq = '${dir.path}/out.parquet';
      await DataFrameIO.writeParquetStream(
        pq,
        DataFrameIO.readCsvBatches(csv, batchSize: 64).ok,
        options: const ParquetWriterOptions(rowGroupSize: 128),
      ).ok;

      final t = await ParquetReader(pq).read().ok;
      expect(t.rowCount, 300);
      expect(t.column('id')!.toList(), List.generate(300, (i) => i));
      expect(t.column('name')!.toList()[299], 'v299');
    });

    test('empty stream writes a valid empty file', () async {
      final path = '${dir.path}/e.parquet';
      await DataFrameIO.writeParquetStream(
        path,
        Stream<DataFrame>.fromIterable([DataFrame.fromTable(ids(0, 0))]),
      ).ok;
      final t = await ParquetReader(path).read().ok;
      expect(t.rowCount, 0);
    });
  });

  group('streaming nulls', () {
    // the Parquet streaming accrual buffer concats COLUMN refs at
    // flush (typed `Column.concat`). This exercises the null-bitmap realignment
    // across pieces + the empty-remainder concat with nullable columns crossing a
    // row-group boundary.
    Table batch(int start, int count) => Table(
      name: 't',
      columns: [
        NumericColumn.int64(
          name: 'id',
          values: [
            for (int i = 0; i < count; i++)
              (start + i).isEven ? null : start + i,
          ],
          nullable: true,
        ),
        ListColumn<String>(
          name: 'name',
          dataType: DataType.string,
          data: [
            for (int i = 0; i < count; i++)
              (start + i) % 3 == 0 ? null : 'n${start + i}',
          ],
          nullable: true,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: true),
          Field(name: 'name', dataType: DataType.string, nullable: true),
        ],
      ),
    );

    test(
      'streamed nullable batches round-trip across row-group boundaries',
      () async {
        final dir = Directory.systemTemp.createTempSync('pq_streamnull_');
        addTearDown(() => dir.deleteSync(recursive: true));
        final path = '${dir.path}/n.parquet';
        // 7 batches of 30 rows, rowGroupSize 100 -> 210 rows, boundaries at 100/200.
        final batches = [
          for (int b = 0; b < 7; b++) DataFrame.fromTable(batch(b * 30, 30)),
        ];
        await DataFrameIO.writeParquetStream(
          path,
          Stream.fromIterable(batches),
          options: const ParquetWriterOptions(rowGroupSize: 100),
        ).ok;

        final t = await ParquetReader(path).read().ok;
        expect(t.rowCount, 210);
        final id = t.column('id')!.toList();
        final name = t.column('name')!.toList();
        for (int i = 0; i < 210; i++) {
          expect(id[i], i.isEven ? null : i, reason: 'id[$i]');
          expect(name[i], i % 3 == 0 ? null : 'n$i', reason: 'name[$i]');
        }
      },
    );
  });

  group('pushdown', () {
    Uint8List i64(int v) {
      final b = Uint8List(8);
      ByteData.view(b.buffer).setInt64(0, v, Endian.little);
      return b;
    }

    Uint8List f64(double v) {
      final b = Uint8List(8);
      ByteData.view(b.buffer).setFloat64(0, v, Endian.little);
      return b;
    }

    // A row group covering [lo, hi] for [column], with [nulls] null rows out of
    // [rows] total.
    Map<String, ColumnBounds> boundsFor(
      String column,
      Object lo,
      Object hi, {
      int nulls = 0,
      int rows = 100,
    }) => {
      column: ColumnBounds(
        min: lo,
        max: hi,
        nullCount: nulls,
        rowCount: rows,
        hasStats: true,
      ),
    };

    group('RowFilter pruning logic', () {
      final g = boundsFor('x', 10, 20);

      test('eq keeps only when the literal is within [min, max]', () {
        expect(RowFilter.compare('x', FilterOp.eq, 15).keep(g), isTrue);
        expect(RowFilter.compare('x', FilterOp.eq, 10).keep(g), isTrue);
        expect(RowFilter.compare('x', FilterOp.eq, 20).keep(g), isTrue);
        expect(RowFilter.compare('x', FilterOp.eq, 5).keep(g), isFalse);
        expect(RowFilter.compare('x', FilterOp.eq, 25).keep(g), isFalse);
      });

      test('lt / le prune against the minimum', () {
        expect(RowFilter.compare('x', FilterOp.lt, 10).keep(g), isFalse);
        expect(RowFilter.compare('x', FilterOp.lt, 11).keep(g), isTrue);
        expect(RowFilter.compare('x', FilterOp.le, 9).keep(g), isFalse);
        expect(RowFilter.compare('x', FilterOp.le, 10).keep(g), isTrue);
      });

      test('gt / ge prune against the maximum', () {
        expect(RowFilter.compare('x', FilterOp.gt, 20).keep(g), isFalse);
        expect(RowFilter.compare('x', FilterOp.gt, 19).keep(g), isTrue);
        expect(RowFilter.compare('x', FilterOp.ge, 21).keep(g), isFalse);
        expect(RowFilter.compare('x', FilterOp.ge, 20).keep(g), isTrue);
      });

      test('ne skips only a uniform, null-free group equal to the literal', () {
        final uniform = boundsFor('x', 7, 7, nulls: 0);
        expect(RowFilter.compare('x', FilterOp.ne, 7).keep(uniform), isFalse);
        expect(RowFilter.compare('x', FilterOp.ne, 8).keep(uniform), isTrue);
        // Nulls present -> cannot prove all rows equal the literal.
        final withNulls = boundsFor('x', 7, 7, nulls: 3);
        expect(RowFilter.compare('x', FilterOp.ne, 7).keep(withNulls), isTrue);
        // Non-uniform range -> keep.
        expect(RowFilter.compare('x', FilterOp.ne, 15).keep(g), isTrue);
      });

      test('ne keeps a uniform floating-point group that may hold NaN', () {
        // min/max exclude NaN, and NaN <> 5 is true, so [5.0, NaN] has the
        // bounds of a uniform group and must still be read.
        ColumnBounds bounds({int? nans}) => ColumnBounds(
          min: 5.0,
          max: 5.0,
          nullCount: 0,
          nanCount: nans,
          rowCount: 2,
          hasStats: true,
        );
        final unknown = {'x': bounds()};
        final some = {'x': bounds(nans: 1)};
        final none = {'x': bounds(nans: 0)};
        expect(RowFilter.compare('x', FilterOp.ne, 5).keep(unknown), isTrue);
        expect(RowFilter.compare('x', FilterOp.ne, 5).keep(some), isTrue);
        expect(RowFilter.compare('x', FilterOp.ne, 5).keep(none), isFalse);
        expect(RowFilter.compare('x', FilterOp.ne, 5.0).keep(none), isFalse);
      });

      test('all-null group is skipped for any comparison', () {
        final allNull = {
          'x': const ColumnBounds(
            nullCount: 100,
            rowCount: 100,
            hasStats: true,
          ),
        };
        expect(RowFilter.compare('x', FilterOp.eq, 1).keep(allNull), isFalse);
        expect(RowFilter.compare('x', FilterOp.gt, 1).keep(allNull), isFalse);
        expect(RowFilter.compare('x', FilterOp.ne, 1).keep(allNull), isFalse);
      });

      test('absent stats or unknown column always keep', () {
        final noStats = {
          'x': const ColumnBounds(rowCount: 100, hasStats: false),
        };
        expect(RowFilter.compare('x', FilterOp.eq, 999).keep(noStats), isTrue);
        expect(RowFilter.compare('y', FilterOp.eq, 999).keep(g), isTrue);
      });

      test('type-mismatched literal keeps the group', () {
        expect(RowFilter.compare('x', FilterOp.gt, 'oops').keep(g), isTrue);
      });

      test(
        'AND skips if any part proves no match; OR keeps if any part matches',
        () {
          final bounds = {
            'a': const ColumnBounds(
              min: 0,
              max: 50,
              nullCount: 0,
              rowCount: 100,
              hasStats: true,
            ),
            'b': const ColumnBounds(
              min: 0,
              max: 50,
              nullCount: 0,
              rowCount: 100,
              hasStats: true,
            ),
          };
          final and = RowFilter.and([
            RowFilter.compare('a', FilterOp.gt, 10), // keep (max 50 > 10)
            RowFilter.compare('b', FilterOp.gt, 99), // skip (max 50 <= 99)
          ]);
          expect(and.keep(bounds), isFalse);
          expect(and.columns, {'a', 'b'});

          final or = RowFilter.or([
            RowFilter.compare('a', FilterOp.gt, 99), // skip
            RowFilter.compare('b', FilterOp.lt, 10), // keep
          ]);
          expect(or.keep(bounds), isTrue);
        },
      );
    });

    group('RowFilter.boundsFrom decoding', () {
      test('decodes int64-encoded min/max (this library and spec int32)', () {
        final s = thrift.Statistics()
          ..minValue = i64(100)
          ..maxValue = i64(200)
          ..nullCount = 3;
        final b = statsToBounds(s, DataType.int32, 100);
        expect(b.min, 100);
        expect(b.max, 200);
        expect(b.nullCount, 3);
        expect(b.hasStats, isTrue);

        // 4-byte little-endian (spec native width for int32) also decodes.
        final spec = thrift.Statistics()
          ..minValue = (Uint8List(4)
            ..buffer.asByteData().setInt32(0, 7, Endian.little))
          ..maxValue = (Uint8List(4)
            ..buffer.asByteData().setInt32(0, 9, Endian.little));
        final sb = statsToBounds(spec, DataType.int32, 10);
        expect(sb.min, 7);
        expect(sb.max, 9);
      });

      test('decodes doubles and strings and bools', () {
        final d = statsToBounds(
          thrift.Statistics()
            ..minValue = f64(1.5)
            ..maxValue = f64(9.5),
          DataType.float64,
          10,
        );
        expect(d.min, 1.5);
        expect(d.max, 9.5);

        final s = statsToBounds(
          thrift.Statistics()
            ..minValue = Uint8List.fromList('apple'.codeUnits)
            ..maxValue = Uint8List.fromList('pear'.codeUnits),
          DataType.string,
          10,
        );
        expect(s.min, 'apple');
        expect(s.max, 'pear');

        final b = statsToBounds(
          thrift.Statistics()
            ..minValue = Uint8List.fromList([0])
            ..maxValue = Uint8List.fromList([1]),
          DataType.boolean,
          10,
        );
        expect(b.min, false);
        expect(b.max, true);
      });

      test('null statistics yield hasStats=false', () {
        final b = statsToBounds(null, DataType.int64, 100);
        expect(b.hasStats, isFalse);
      });

      test('deprecated min/max are used only for signed-order types', () {
        // Old writers (PARQUET-686) order the deprecated fields by signed
        // bytes, so {'a', 'é'} records min 'é' and max 'a'.
        final legacy = thrift.Statistics()
          ..min = Uint8List.fromList(utf8.encode('é'))
          ..max = Uint8List.fromList(utf8.encode('a'));
        final text = statsToBounds(legacy, DataType.string, 2);
        expect(text.min, isNull);
        expect(
          RowFilter.compare('s', FilterOp.eq, 'a').keep({'s': text}),
          isTrue,
        );

        final unsigned = statsToBounds(
          thrift.Statistics()
            ..min = i64(-1)
            ..max = i64(5),
          DataType.int64,
          2,
          unsignedBits: 32,
        );
        expect(unsigned.min, isNull);

        final signed = statsToBounds(
          thrift.Statistics()
            ..min = i64(3)
            ..max = i64(5),
          DataType.int64,
          2,
        );
        expect([signed.min, signed.max], [3, 5]);
      });
    });

    group('end-to-end row-group pruning', () {
      late Directory dir;
      late String path;
      setUp(() async {
        dir = Directory.systemTemp.createTempSync('pq_pushdown_');
        path = '${dir.path}/d.parquet';
        // 1000 sequential ids in 10 row groups of 100 -> group i covers
        // [100*i, 100*i+99], so a predicate on id prunes whole groups exactly.
        final table = Table(
          name: 't',
          columns: [
            NumericColumn.int32(
              name: 'id',
              values: List.generate(1000, (i) => i),
              nullable: false,
            ),
            ListColumn<String>(
              name: 'name',
              dataType: DataType.string,
              data: List.generate(1000, (i) => 'n$i'),
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
        await ParquetWriter(
          path,
          options: const ParquetWriterOptions(rowGroupSize: 100),
        ).write(table).ok;
      });
      tearDown(() => dir.deleteSync(recursive: true));

      test('no filter reads every row', () async {
        final df = await DataFrameIO.readParquet(path).ok;
        expect((await df.execute().ok).rowCount, 1000);
      });

      test(
        'selective filter prunes leading groups (exact on boundaries)',
        () async {
          final df = await DataFrameIO.readParquet(
            path,
            filter: RowFilter.compare('id', FilterOp.ge, 500),
          ).ok;
          final t = await df.execute().ok;
          // Groups 0-4 (ids 0-499) pruned; 5-9 kept whole.
          expect(t.rowCount, 500);
          final ids = t.column('id')!.toList().cast<int>();
          expect(ids.first, 500);
          expect(ids.last, 999);
        },
      );

      test('a column with an unknown sort order is not pruned', () async {
        // Rewrite the footer so the id column declares a sort order this
        // reader does not know (an empty ColumnOrder union).
        final bytes = File(path).readAsBytesSync();
        final length = ByteData.sublistView(
          bytes,
        ).getUint32(bytes.length - 8, Endian.little);
        final start = bytes.length - 8 - length;
        final metadata = thrift.FileMetaData()
          ..read(
            CompactReader(Uint8List.sublistView(bytes, start, start + length)),
          );
        metadata.columnOrders = [
          thrift.ColumnOrder(),
          for (final _ in metadata.schema.skip(2)) thrift.ColumnOrder(),
        ];
        final writer = CompactWriter();
        metadata.write(writer);
        final footer = writer.takeBytes();
        final patched = '${dir.path}/unknown_order.parquet';
        File(patched).writeAsBytesSync([
          ...Uint8List.sublistView(bytes, 0, start),
          ...footer,
          ...(ByteData(
            4,
          )..setUint32(0, footer.length, Endian.little)).buffer.asUint8List(),
          ...'PAR1'.codeUnits,
        ]);
        final df = await DataFrameIO.readParquet(
          patched,
          filter: RowFilter.compare('id', FilterOp.ge, 500),
        ).ok;
        expect((await df.execute().ok).rowCount, 1000);
      });

      test('filter matching nothing prunes all groups', () async {
        final df = await DataFrameIO.readParquet(
          path,
          filter: RowFilter.compare('id', FilterOp.gt, 100000),
        ).ok;
        expect((await df.execute().ok).rowCount, 0);
      });

      test('range AND keeps only the overlapping groups', () async {
        final df = await DataFrameIO.readParquet(
          path,
          filter: RowFilter.and([
            RowFilter.compare('id', FilterOp.ge, 300),
            RowFilter.compare('id', FilterOp.lt, 500),
          ]),
        ).ok;
        final t = await df.execute().ok;
        expect(t.rowCount, 200); // groups 3 and 4
        final ids = t.column('id')!.toList().cast<int>();
        expect(ids.first, 300);
        expect(ids.last, 499);
      });

      test('a NaN row survives pruning for <>', () async {
        final file = '${dir.path}/nan.parquet';
        await ParquetWriter(file)
            .write(
              Table.fromRows('t', [
                {'x': 5.0},
                {'x': double.nan},
              ]),
            )
            .ok;
        final t = await (await DataFrameIO.readParquet(
          file,
          filter: RowFilter.compare('x', FilterOp.ne, 5),
        ).ok).execute().ok;
        // The filter only prunes groups: the group is kept whole.
        expect(t.rowCount, 2);
      });

      test('column projection returns only the requested columns', () async {
        final t = await (await DataFrameIO.readParquet(
          path,
          columns: ['id'],
        ).ok).execute().ok;
        expect(t.schema.fieldNames, ['id']);
        expect(t.rowCount, 1000);
        expect(t.column('id')!.toList().first, 0);
      });

      test('projection and filter combine', () async {
        final t = await (await DataFrameIO.readParquet(
          path,
          columns: ['id'],
          filter: RowFilter.compare('id', FilterOp.ge, 500),
        ).ok).execute().ok;
        expect(t.schema.fieldNames, ['id']);
        expect(t.rowCount, 500);
        expect(t.column('id')!.toList().first, 500);
      });
    });
  });

  group('footer guard', () {
    // a corrupt/adversarial Parquet footer (offsets/sizes
    // outside the file) must fail loud at validation, not drive an out-of-file
    // read or huge allocation.
    thrift.FileMetaData meta({
      required int dataPageOffset,
      required int totalCompressedSize,
    }) {
      final root = thrift.SchemaElement()..numChildren = 1;
      final leaf = thrift.SchemaElement()
        ..name = 'c'
        ..type = thrift.ParquetType.int32;
      final col = thrift.ColumnMetaData()
        ..codec = thrift.CompressionCodec.uncompressed
        ..pathInSchema = ['c']
        ..numValues = 1
        ..dataPageOffset = dataPageOffset
        ..totalCompressedSize = totalCompressedSize;
      final chunk = thrift.ColumnChunk()..metaData = col;
      final rg = thrift.RowGroup()
        ..columns = [chunk]
        ..numRows = 1;
      return thrift.FileMetaData()
        ..version = 1
        ..schema = [root, leaf]
        ..numRows = 1
        ..rowGroups = [rg];
    }

    late Directory dir;
    late FileReader reader;
    setUp(() async {
      dir = Directory.systemTemp.createTempSync('pqguard_');
      final path = '${dir.path}/f';
      File(path).writeAsBytesSync(List.filled(100, 0)); // a 100-byte "file"
      reader = FileReader(path);
      await reader.open();
    });
    tearDown(() async {
      await reader.close();
      dir.deleteSync(recursive: true);
    });

    test('a data-page offset past the file length is rejected', () {
      expect(
        () => MetadataReader(
          reader,
        ).validate(meta(dataPageOffset: 1 << 30, totalCompressedSize: 10)),
        throwsA(isA<FormatException>()),
      );
    });

    test('a chunk size that runs past EOF is rejected', () {
      expect(
        () => MetadataReader(
          reader,
        ).validate(meta(dataPageOffset: 50, totalCompressedSize: 1 << 30)),
        throwsA(isA<FormatException>()),
      );
    });

    test('an in-bounds chunk passes validation', () {
      // offset 10 + size 20 = 30 <= 100; should not throw.
      MetadataReader(
        reader,
      ).validate(meta(dataPageOffset: 10, totalCompressedSize: 20));
    });
  });

  group('large header', () {
    // a valid Parquet page header larger than the initial 1024-byte
    // read window (e.g. rich per-page statistics) must be read by widening the
    // window, not rejected as "truncated". This regressed when the
    // compact protocol began throwing FormatException (not RangeError) on the
    // end-of-window that drives the widen loop.
    Future<HeaderResult?> readHeader(Uint8List headerBytes) async {
      final dir = Directory.systemTemp.createTempSync('pqhdr_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final path = '${dir.path}/page.bin';
      await File(path).writeAsBytes(headerBytes);
      final reader = FileReader(path);
      await reader.open();
      addTearDown(reader.close);
      return PageReader(
        reader,
        0,
        reader.length,
        thrift.CompressionCodec.uncompressed,
      ).readHeaderWithSize();
    }

    Uint8List serialize(thrift.PageHeader h) {
      final w = CompactWriter();
      h.write(w);
      return w.takeBytes();
    }

    test('a page header > 1024 bytes parses (window widens)', () async {
      // Statistics with ~2 KB min/max blows the header past the 1024-byte window.
      final blob = Uint8List(2000)..fillRange(0, 2000, 7);
      final header = thrift.PageHeader()
        ..type = thrift.PageType.dataPage
        ..uncompressedPageSize = 16
        ..compressedPageSize = 16
        ..dataPageHeader = (thrift.DataPageHeader()
          ..numValues = 1
          ..encoding = thrift.Encoding.plain
          ..definitionLevelEncoding = thrift.Encoding.rle
          ..repetitionLevelEncoding = thrift.Encoding.rle
          ..statistics = (thrift.Statistics()
            ..minValue = blob
            ..maxValue = blob));

      final bytes = serialize(header);
      expect(bytes.length, greaterThan(1024)); // precondition: needs widening

      final result = await readHeader(bytes);
      expect(result, isNotNull);
      expect(result!.header.type, thrift.PageType.dataPage);
      expect(result.header.dataPageHeader!.numValues, 1);
    });

    test('a genuinely truncated header still throws FormatException', () async {
      // A large header (with a 2 KB stats blob) chopped mid-blob: the blob's
      // length prefix promises more bytes than exist, so it never parses no matter
      // how far the window widens.
      final blob = Uint8List(2000)..fillRange(0, 2000, 7);
      final header = thrift.PageHeader()
        ..type = thrift.PageType.dataPage
        ..uncompressedPageSize = 16
        ..compressedPageSize = 16
        ..dataPageHeader = (thrift.DataPageHeader()
          ..numValues = 1
          ..encoding = thrift.Encoding.plain
          ..definitionLevelEncoding = thrift.Encoding.rle
          ..repetitionLevelEncoding = thrift.Encoding.rle
          ..statistics = (thrift.Statistics()
            ..minValue = blob
            ..maxValue = blob));
      final full = serialize(header);
      final truncated = Uint8List.sublistView(full, 0, full.length - 800);
      await expectLater(readHeader(truncated), throwsA(isA<FormatException>()));
    });
  });

  group('read cap', () {
    // ParquetReader.read() is memory-bounded by default (like
    // the CSV/JSON/Arrow eager readers), so a huge file (e.g. an Iceberg delete
    // file) can't OOM. Opt out with maxRows/maxBytes: null.
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('pqcap_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Future<String> write(int n) async {
      final p = '${dir.path}/f.parquet';
      await ParquetWriter(p)
          .write(
            Table(
              name: 't',
              columns: [
                NumericColumn.int64(
                  name: 'id',
                  values: [for (var i = 0; i < n; i++) i],
                  nullable: false,
                ),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'id', dataType: DataType.int64, nullable: false),
                ],
              ),
            ),
          )
          .ok;
      return p;
    }

    test('read() throws past maxRows; null reads all', () async {
      final p = await write(1000);
      await expectLater(
        ParquetReader(p).read(maxRows: 100),
        failsWith(Kind.limit),
      );
      expect((await ParquetReader(p).read(maxRows: null).ok).rowCount, 1000);
    });

    test('read() throws past maxBytes (memory cap)', () async {
      final p = await write(1000); // 1000 int64 = ~8000 bytes
      await expectLater(
        ParquetReader(p).read(maxBytes: 64),
        failsWith(Kind.limit),
      );
      expect((await ParquetReader(p).read(maxBytes: null).ok).rowCount, 1000);
    });

    test('default caps clear a normal file', () async {
      final p = await write(1000);
      expect((await ParquetReader(p).read().ok).rowCount, 1000);
    });
  });

  group('batch bounded', () {
    // ParquetReader.readBatches streams a row group page-by-page:
    // peak memory ≈ one `size`-row batch, not the whole group. A single large
    // row group (default rowGroupSize=100k) read with a small `size` must yield many
    // small batches whose concatenation equals read() column-for-column.
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('pqbb_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('flat columns: small-size batches concat to read()', () async {
      final path = '${dir.path}/flat.parquet';
      const n = 1000; // one row group (default 100k)
      await ParquetWriter(path)
          .write(
            Table(
              name: 't',
              columns: [
                NumericColumn.int64(
                  name: 'id',
                  values: [for (var i = 0; i < n; i++) i],
                  nullable: false,
                ),
                NumericColumn.float64(
                  name: 'v',
                  values: [
                    for (var i = 0; i < n; i++) i % 7 == 0 ? null : i * 1.5,
                  ],
                  nullable: true,
                ),
                ListColumn<String>(
                  name: 's',
                  dataType: DataType.string,
                  data: [for (var i = 0; i < n; i++) 'r$i'],
                  nullable: false,
                ),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'id', dataType: DataType.int64, nullable: false),
                  Field(name: 'v', dataType: DataType.float64, nullable: true),
                  Field(name: 's', dataType: DataType.string, nullable: false),
                ],
              ),
            ),
          )
          .ok;

      final whole = await ParquetReader(path).read().ok;
      final batches = await ParquetReader(
        path,
      ).readBatches(batchSize: 100).ok.toList();

      expect(
        batches.length,
        greaterThan(1),
      ); // many small batches, not one group
      expect(batches.every((b) => b.rowCount <= 100), isTrue);
      expect(batches.fold(0, (s, b) => s + b.rowCount), n);

      for (final name in ['id', 'v', 's']) {
        final streamed = [for (final b in batches) ...b.column(name)!.toList()];
        expect(streamed, whole.column(name)!.toList(), reason: name);
      }
    });

    test('many small int batches stay value-aligned across pages', () async {
      // A wider/longer flat file (multiple data pages within the single row group)
      // exercises the cross-page carry: each batch must pick up exactly where the
      // last left off.
      final path = '${dir.path}/pages.parquet';
      const n = 5000;
      await ParquetWriter(path)
          .write(
            Table(
              name: 't',
              columns: [
                NumericColumn.int64(
                  name: 'k',
                  values: [for (var i = 0; i < n; i++) i * 3 - 1],
                  nullable: false,
                ),
              ],
              schema: Schema(
                fields: [
                  Field(name: 'k', dataType: DataType.int64, nullable: false),
                ],
              ),
            ),
          )
          .ok;
      final whole = await ParquetReader(path).read().ok;
      final batches = await ParquetReader(
        path,
      ).readBatches(batchSize: 257).ok.toList();
      expect(batches.length, greaterThan(1));
      final streamed = [for (final b in batches) ...b.column('k')!.toList()];
      expect(streamed, whole.column('k')!.toList());
    });
  });

  group('nested cap', () {
    // the nested-Parquet eager read enforces `maxBytes` INCREMENTALLY
    // during row-group accumulation (stop-at-cap), not only after the whole file is
    // buffered and assembled.
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('nestcap_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Future<String> writeNested() async {
      final p = '${dir.path}/n.parquet';
      await ParquetWriter(p)
          .write(
            Table(
              name: 't',
              columns: [
                ListColumn(
                  name: 'tags',
                  dataType: DataType.list,
                  data: [
                    for (var i = 0; i < 500; i++) [i, i + 1, i + 2],
                  ],
                  nullable: true,
                ),
              ],
              schema: Schema(
                fields: [
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
                ],
              ),
            ),
          )
          .ok;
      return p;
    }

    test('a tiny maxBytes fails loud on a nested read', () async {
      final p = await writeNested();
      await expectLater(
        ParquetReader(p).read(maxBytes: 1),
        failsWith(Kind.limit, contains('maxBytes')),
      );
    });

    test('the same nested file reads fine under the default cap', () async {
      final p = await writeNested();
      final t = await ParquetReader(p).read().ok;
      expect(t.column('tags')!.toList().first, [0, 1, 2]);
      expect(t.rowCount, 500);
    });
  });

  group('concurrent write', () {
    // ParquetWriter uses a per-writer unique temp path, so two
    // writers targeting the same destination can't corrupt or cross-delete each
    // other's in-progress temp: the destination ends up a valid file.
    test(
      'concurrent writers to one destination both produce valid files',
      () async {
        final dir = Directory.systemTemp.createTempSync('pqconc_');
        try {
          final path = '${dir.path}/out.parquet';

          Table table(int base) => Table(
            name: 't',
            columns: [
              NumericColumn.int64(
                name: 'v',
                values: [for (var i = 0; i < 500; i++) base + i],
                nullable: false,
              ),
            ],
            schema: Schema(
              fields: [
                Field(name: 'v', dataType: DataType.int64, nullable: false),
              ],
            ),
          );

          // Two writers race on the same destination path.
          await Future.wait([
            ParquetWriter(path).write(table(0)),
            ParquetWriter(path).write(table(1000)),
          ]);

          // Whichever won the final rename, the file must be valid and complete
          // (not a corrupted interleave of both writers' temps).
          final back = await ParquetReader(path).read().ok;
          expect(back.rowCount, 500);
          final first = back.column('v')!.toList().first as int;
          expect(first == 0 || first == 1000, isTrue);

          // No stray temp files left behind.
          final leftovers = dir
              .listSync()
              .where((f) => f.path.endsWith('.tmp'))
              .toList();
          expect(leftovers, isEmpty);
        } finally {
          dir.deleteSync(recursive: true);
        }
      },
    );
  });

  group('delta encoding', () {
    // a high-cardinality integer column is DELTA_BINARY_PACKED
    // encoded by the writer. The writer marks a no-null column `required` in the
    // file schema, so the per-column encoder must agree (write no definition
    // levels); otherwise the reader reads the delta header at the wrong offset.
    // Regression for the nullable delta round-trip bug.
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('pqdelta_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Future<Table> roundTrip(Table t) async {
      final p = '${dir.path}/d.parquet';
      await ParquetWriter(p).write(t).ok;
      return ParquetReader(p).read().ok;
    }

    Table ints(List<int?> values, {required bool nullable}) => Table(
      name: 't',
      columns: [
        ListColumn<int>(
          name: 'id',
          dataType: DataType.int64,
          data: values,
          nullable: nullable,
        ),
      ],
      schema: Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: nullable),
        ],
      ),
    );

    // Distinct values defeat dictionary encoding → DELTA_BINARY_PACKED.
    List<int?> distinct(int n) => [for (var i = 0; i < n; i++) i * 7919];

    // INT32 delta arithmetic wraps in 32 bits per the spec (parquet-mr and Arrow
    // both read it that way: Arrow's min_delta_ is an int32_t read through a
    // uint32 VLQ capped at 5 bytes). Computing 64-bit deltas emits a varint whose
    // high bits those readers silently drop.
    Table int32s(List<int> values) => Table(
      name: 't',
      columns: [
        NumericColumn.int32(name: 'id', values: values, nullable: false),
      ],
      schema: Schema(
        fields: [Field(name: 'id', dataType: DataType.int32, nullable: false)],
      ),
    );

    test('an int32 delta spanning more than 2^31 round-trips', () async {
      // Every value is in int32 range, all distinct (-> DELTA), but the deltas
      // between them exceed 2^31 and must wrap.
      const values = [
        -2000000000,
        2000000000,
        -1999999999,
        1999999998,
        -1999999997,
        1999999996,
      ];
      final back = await roundTrip(int32s(values));
      expect(back.column('id')!.toList(), values);
    });

    test('int32 extremes round-trip', () async {
      const values = [-2147483648, 2147483647, -2147483648, 2147483647];
      final back = await roundTrip(int32s(values));
      expect(back.column('id')!.toList(), values);
    });

    test('an ordinary int32 column still round-trips', () async {
      final values = [for (var i = 0; i < 300; i++) i * 7919];
      final back = await roundTrip(int32s(values));
      expect(back.column('id')!.toList(), values);
    });

    test('an int64 delta spanning more than 2^63 still round-trips', () async {
      // The 64-bit path must be untouched by the int32 wrapping.
      final back = await roundTrip(
        ints([0, 9223372036854775807, 0], nullable: false),
      );
      expect(back.column('id')!.toList(), [0, 9223372036854775807, 0]);
    });

    test('non-nullable delta column round-trips', () async {
      final back = await roundTrip(ints(distinct(300), nullable: false));
      expect(back.column('id')!.toList(), distinct(300));
    });

    test('nullable delta column with no actual nulls round-trips', () async {
      final back = await roundTrip(ints(distinct(300), nullable: true));
      expect(back.column('id')!.toList(), distinct(300));
    });

    test('nullable delta column with nulls preserves null positions', () async {
      final values = [for (var i = 0; i < 300; i++) i.isEven ? i * 7919 : null];
      final back = await roundTrip(ints(values, nullable: true));
      expect(back.column('id')!.toList(), values);
    });
  });

  group('external read', () {
    // libbigdata reads Parquet written by external engines (pyarrow,
    // Spark, parquet-mr) correctly: unsigned ints, BYTE_STREAM_SPLIT, legacy
    // 2-level LIST, FLBA decimals, INT96 timestamps.
    Future<Table> read(String path, {bool tolerate = false}) => ParquetReader(
      path,
      options: ParquetReaderOptions(tolerateTruncatedPages: tolerate),
    ).read().ok;

    test(
      'unsigned ints read losslessly (uint8/16/32 widen; uint64 → BigInt)',
      () async {
        final t = await read('test/fixtures/parquet/uint_pyarrow.parquet');
        final types = {for (final f in t.schema.fields) f.name: f.dataType};
        expect(types['u8'], DataType.int16);
        expect(types['u16'], DataType.int32);
        expect(types['u32'], DataType.int64);
        // uint64 exceeds int64 → decimal(20,0), BigInt-backed (Spark's mapping).
        expect(types['u64'], DataType.decimal);
        final u64 = t.schema.fields.firstWhere((f) => f.name == 'u64');
        expect((u64.precision, u64.scale), (20, 0));
        final r = t.toRows().first;
        expect(r['u8'], 200);
        expect(r['u16'], 60000);
        expect(r['u32'], 4000000000); // was -294967296 before the fix
        // was the silent wrap -446744073709551616 before the fix.
        expect(r['u64'], Decimal(BigInt.parse('18000000000000000000'), 0));
      },
    );

    test('uint64 BigInt survives a read → write → read round-trip', () async {
      final dir = Directory.systemTemp.createTempSync('u64rt_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final t = await read('test/fixtures/parquet/uint_pyarrow.parquet');
      final out = '${dir.path}/rt.parquet';
      await ParquetWriter(out).write(t).ok;
      final back = await read(out);
      expect(
        back.toRows().first['u64'],
        Decimal(BigInt.parse('18000000000000000000'), 0),
      ); // decimal(20,0) → FLBA(9) write path is lossless
    });

    test(
      'BYTE_STREAM_SPLIT with nulls decodes (present-count) (pyarrow)',
      () async {
        final t = await read(
          'test/fixtures/parquet/bss_nullable_pyarrow.parquet',
        );
        final h = t.column('h')!.toList();
        expect(h, [
          closeTo(1.5, 1e-6),
          isNull,
          closeTo(3.5, 1e-6),
          isNull,
          closeTo(5.5, 1e-6),
        ]);
      },
    );

    test('legacy 2-level LIST reads as lists (parquet-mr)', () async {
      final t = await read(
        'test/fixtures/parquet-testing/data/old_list_structure.parquet',
      );
      expect(t.column('a')!.toList().first, [1, 2, 3, 4]);
    });

    test('FLBA / byte-array / int decimals read as unscaled BigInt', () async {
      for (final f in [
        'fixed_length_decimal.parquet',
        'byte_array_decimal.parquet',
        'int32_decimal.parquet',
        'int64_decimal.parquet',
      ]) {
        final t = await read('test/fixtures/parquet-testing/data/$f');
        final v = t.column('value')!.toList();
        expect(v.first, isA<BigInt>(), reason: f);
        expect(v.first, BigInt.from(100), reason: f);
      }
    });

    test('Spark INT96 timestamps decode (extreme values fail loud)', () async {
      // The fixture mixes normal timestamps with a year-290000 value beyond Dart's
      // DateTime range: fail loud by default, coerce-to-null with tolerate.
      await expectLater(
        ParquetReader(
          'test/fixtures/parquet-testing/data/int96_from_spark.parquet',
        ).read(),
        failsWith(Kind.corrupt),
      );
      final t = await read(
        'test/fixtures/parquet-testing/data/int96_from_spark.parquet',
        tolerate: true,
      );
      final ts = t.column('a')!.toList();
      expect(ts[0], isA<DateTime>());
      expect(
        (ts[0] as DateTime).toUtc(),
        DateTime.utc(2024, 1, 1, 20, 34, 56, 123, 456),
      );
      expect(ts.last, isNull); // out-of-range → null under tolerate
    });
  });

  group('field ids', () {
    // a Field.id is emitted into the Parquet SchemaElement
    // (file-level field-ids enable true Spark-Iceberg schema evolution); absent
    // when no id is set (plain Parquet unchanged).
    test('scalarElement carries fieldId when Field.id is set', () {
      final el = SchemaConverter().scalarElement(
        Field(name: 'x', dataType: DataType.int64, nullable: false, id: 7),
      );
      expect(el.fieldId, 7);
    });

    test('scalarElement leaves fieldId null when Field.id is unset', () {
      final el = SchemaConverter().scalarElement(
        Field(name: 'x', dataType: DataType.int64, nullable: false),
      );
      expect(el.fieldId, isNull);
    });

    test('Field.id is excluded from equality (carried metadata)', () {
      final a = Field(name: 'x', dataType: DataType.int64);
      final b = a.copyWith(id: 9);
      expect(a, b); // id must not affect schema equality / spill concat
    });
  });

  group('nested shred guard', () {
    // a malformed nested Field (a map without its key_value
    // struct child, or a list without an element child) now fails loud with a clear
    // ArgumentError instead of an opaque RangeError from the shredder.
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('shredguard_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Future<Result<void, ErrorDetails>> writeBad(Field bad, Column col) =>
        ParquetWriter('${dir.path}/x.parquet').write(
          Table(
            name: 't',
            columns: [col],
            schema: Schema(fields: [bad]),
          ),
        );

    test('map without a key_value struct child is invalid', () async {
      // The common mistake: children = [key, value] directly (no wrapping struct).
      final bad = Field(
        name: 'm',
        dataType: DataType.map,
        nullable: false,
        children: [
          Field(name: 'key', dataType: DataType.string, nullable: false),
          Field(name: 'value', dataType: DataType.int64, nullable: true),
        ],
      );
      final col = ListColumn(
        name: 'm',
        dataType: DataType.map,
        data: [
          {'a': 1},
        ],
        nullable: false,
      );
      await expectLater(writeBad(bad, col), failsWith(Kind.invalid));
    });
  });

  group('reader string caps', () {
    // the string/stream readers enforce the same opt-out caps as the
    // file readers, so an oversized request body can't OOM the process.
    const csv = 'a,b\n1,2\n3,4\n5,6\n';

    test('CSV readString caps rows; null opts out', () async {
      expect(CsvReader().readString(csv, maxRows: 2), failsWith(Kind.limit));
      final t = await CsvReader().readString(csv, maxRows: null).ok;
      expect(t.rowCount, 3);
    });

    test('CSV readString caps bytes', () async {
      expect(CsvReader().readString(csv, maxBytes: 4), failsWith(Kind.limit));
    });

    test('JSON readString caps rows; null opts out', () async {
      const j = '[{"a":1},{"a":2},{"a":3}]';
      expect(JsonReader().readString(j, maxRows: 2), failsWith(Kind.limit));
      final df = await JsonReader().readString(j, maxRows: null).ok;
      expect(df.rowCount, 3);
    });

    test('JSON readStream stops buffering at the byte cap', () async {
      Stream<List<int>> body() async* {
        yield utf8.encode('[{"a":1},');
        yield utf8.encode('{"a":2},{"a":3}]');
      }

      expect(
        JsonReader().readStream(body(), maxBytes: 4),
        failsWith(Kind.limit),
      );
    });
  });

  group('file reader concurrency', () {
    // FileReader serializes seek+read, so concurrent
    // readBytes() on one shared handle return correct (non-interleaved) bytes.
    test(
      'concurrent readBytes on a shared handle are not interleaved',
      () async {
        final dir = Directory.systemTemp.createTempSync('frconc_');
        try {
          // 256 distinct 16-byte blocks: block i is all-byte-value i.
          final path = '${dir.path}/blocks.bin';
          final bytes = Uint8List(256 * 16);
          for (var i = 0; i < 256; i++) {
            for (var j = 0; j < 16; j++) {
              bytes[i * 16 + j] = i;
            }
          }
          File(path).writeAsBytesSync(bytes);

          final reader = FileReader(path);
          await reader.open();
          try {
            // Fire all 256 reads concurrently against the single shared cursor.
            final reads = await Future.wait([
              for (var i = 0; i < 256; i++) reader.readBytes(i * 16, 16),
            ]);
            for (var i = 0; i < 256; i++) {
              expect(reads[i], everyElement(i), reason: 'block $i corrupted');
            }
          } finally {
            await reader.close();
          }
        } finally {
          dir.deleteSync(recursive: true);
        }
      },
    );
  });

  group('phase A Parquet stats/binary', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('phaseA_'));
    tearDown(() => dir.deleteSync(recursive: true));

    // Read the row-group statistics for the first (only) column of a Parquet file.
    Future<({Uint8List? min, Uint8List? max, int? nulls})> stats(
      String path,
    ) async {
      final reader = FileReader(path);
      await reader.open();
      try {
        final md = await MetadataReader(reader).readMetadata();
        final s = md.rowGroups.first.columns.first.metaData!.statistics;
        return (min: s?.minValue, max: s?.maxValue, nulls: s?.nullCount);
      } finally {
        await reader.close();
      }
    }

    Table floats(List<double?> v) => Table(
      name: 't',
      columns: [NumericColumn.float64(name: 'f', values: v, nullable: true)],
      schema: Schema(
        fields: [Field(name: 'f', dataType: DataType.float64, nullable: true)],
      ),
    );

    group('W1 Parquet float stats ignore NaN but keep ±Infinity', () {
      test('a NaN in a non-first slot does not poison min/max', () async {
        final path = '${dir.path}/nan.parquet';
        await ParquetWriter(path).write(floats([1.0, double.nan, 3.0, 2.0])).ok;
        final s = await stats(path);
        final min = ByteData.sublistView(s.min!).getFloat64(0, Endian.little);
        final max = ByteData.sublistView(s.max!).getFloat64(0, Endian.little);
        expect(min, 1.0);
        expect(max, 3.0);
      });

      test('+Infinity participates in max (only NaN is excluded)', () async {
        // [nan, infinity] is NOT all-NaN: infinity is a real value and MUST be
        // the max, else a `f >= x` filter prunes this row group and the +Inf row
        // silently vanishes (and external readers trust the poisoned stat).
        final path = '${dir.path}/inf.parquet';
        await ParquetWriter(
          path,
        ).write(floats([double.nan, double.infinity])).ok;
        final s = await stats(path);
        final min = ByteData.sublistView(s.min!).getFloat64(0, Endian.little);
        final max = ByteData.sublistView(s.max!).getFloat64(0, Endian.little);
        expect(min, double.infinity);
        expect(max, double.infinity);
      });

      test('-Infinity is the min', () async {
        final path = '${dir.path}/neginf.parquet';
        await ParquetWriter(
          path,
        ).write(floats([1.0, double.negativeInfinity, 2.0])).ok;
        final s = await stats(path);
        final min = ByteData.sublistView(s.min!).getFloat64(0, Endian.little);
        expect(min, double.negativeInfinity);
      });

      test('a genuinely all-NaN column emits no min/max', () async {
        final path = '${dir.path}/allnan.parquet';
        await ParquetWriter(path).write(floats([double.nan, double.nan])).ok;
        final s = await stats(path);
        expect(s.min, isNull);
        expect(s.max, isNull);
      });

      test(
        'a range filter does not prune a row group holding +Infinity',
        () async {
          // The functional consequence of the stats fix: reading with a predicate
          // above every finite value must still return the +Inf row.
          final path = '${dir.path}/prune.parquet';
          await ParquetWriter(
            path,
          ).write(floats([1.0, 2.0, double.infinity])).ok;
          final engine = SqlEngine.open(allowExternalPaths: true).ok;
          final r = await engine
              .execute("SELECT f FROM '$path' WHERE f >= 1e308")
              .ok;
          expect(r.toRows().map((e) => e['f']), [double.infinity]);
          await engine.dispose();
        },
      );

      test('the NaN value itself still round-trips', () async {
        final path = '${dir.path}/rt.parquet';
        await ParquetWriter(path).write(floats([1.0, double.nan, 3.0])).ok;
        final back = (await ParquetReader(
          path,
        ).read().ok).column('f')!.toList();
        expect(back[0], 1.0);
        expect((back[1] as double).isNaN, isTrue);
        expect(back[2], 3.0);
      });
    });

    group('W3 Parquet binary column is not stringified', () {
      test('raw bytes round-trip', () async {
        final path = '${dir.path}/bin.parquet';
        final values = <Uint8List?>[
          Uint8List.fromList([1, 2, 3]),
          Uint8List.fromList([255, 0, 128]),
          null,
        ];
        await ParquetWriter(path)
            .write(
              Table(
                name: 't',
                columns: [
                  ListColumn<Uint8List>(
                    name: 'b',
                    dataType: DataType.binary,
                    data: values,
                    nullable: true,
                  ),
                ],
                schema: Schema(
                  fields: [
                    Field(name: 'b', dataType: DataType.binary, nullable: true),
                  ],
                ),
              ),
            )
            .ok;
        final back = (await ParquetReader(
          path,
        ).read().ok).column('b')!.toList();
        expect(back[0], equals([1, 2, 3]));
        expect(back[1], equals([255, 0, 128]));
        expect(back[2], isNull);
      });
    });
  });

  group('round 49 streaming writers', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('r49_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table intTable(String name, DataType type, List<int> values) => Table(
      name: 't',
      columns: [
        type == DataType.int32
            ? NumericColumn.int32(name: name, values: values, nullable: false)
            : NumericColumn.int64(name: name, values: values, nullable: false),
      ],
      schema: Schema(
        fields: [Field(name: name, dataType: type, nullable: false)],
      ),
    );

    group('#1 streaming writers reject a widened batch (no truncation)', () {
      test('Parquet: int64 batch under a frozen int32 schema throws', () async {
        final w = ParquetWriter('${dir.path}/a.parquet');
        await w
            .writeBatch(intTable('n', DataType.int32, [1]))
            .ok; // freezes int32
        await expectLater(
          w.writeBatch(intTable('n', DataType.int64, [1 << 40])),
          failsWith(Kind.schema),
        );
        await w.abort();
      });

      test('Arrow: int64 batch under a frozen int32 schema throws', () async {
        final int32 = intTable('n', DataType.int32, [1]);
        final w = ArrowStreamWriter('${dir.path}/a.arrow', int32.schema);
        await w.add(int32);
        await expectLater(
          w.add(intTable('n', DataType.int64, [1 << 40])),
          throwsArgumentError,
        );
        await w.abort();
      });

      test(
        'a correctly int64-declared Parquet round-trip preserves 1<<40',
        () async {
          final path = '${dir.path}/b.parquet';
          final w = ParquetWriter(path);
          await w.writeBatch(intTable('n', DataType.int64, [1, 1 << 40])).ok;
          await w.close().ok;
          final back = (await ParquetReader(
            path,
          ).read().ok).column('n')!.toList();
          expect(back, [1, 1 << 40]); // was [1, 0] under the old widening bug
        },
      );
    });

    group('#5 finalization leaves no orphan', () {
      test('ParquetWriter.close() failure self-cleans the temp', () async {
        final path = '${dir.path}/x.parquet';
        Directory(path).createSync(); // rename onto a directory fails
        final w = ParquetWriter(path);
        await w
            .writeBatch(
              Table(
                name: 't',
                columns: [
                  NumericColumn.int64(name: 'n', values: [1], nullable: false),
                ],
                schema: Schema(
                  fields: [
                    Field(name: 'n', dataType: DataType.int64, nullable: false),
                  ],
                ),
              ),
            )
            .ok;
        await expectLater(w.close(), failsWith(Kind.io));
        // No .tmp orphan survives (close() aborted itself).
        final orphans = Directory(
          dir.path,
        ).listSync().where((e) => e.path.endsWith('.tmp')).toList();
        expect(orphans, isEmpty);
      });
    });
  });

  group('#8 Hadoop-LZ4 cumulative bomb throws (no raw fallback)', () {
    Uint8List be32(int v) =>
        Uint8List(4)..buffer.asByteData().setUint32(0, v, Endian.big);

    test('an over-cap multi-sub-block frame surfaces the budget error', () {
      // Two 400-byte sub-blocks (each UNDER a 500-byte per-block cap) sum to 800
      // > 500, only the CUMULATIVE guard can catch it. Before the fix its
      // FormatException was swallowed by the `on Object` fallback, which
      // re-inflated the bomb via the raw decoder (a different error); now the
      // typed exception surfaces the budget message.
      final block = Lz4Codec(
        format: Lz4Format.raw,
      ).compress(Uint8List(400)..fillRange(0, 400, 65));
      final frame = BytesBuilder()
        ..add(be32(800)) // outer uncompressed size
        ..add(be32(block.length))
        ..add(block)
        ..add(be32(block.length))
        ..add(block);
      final codec = codecFor(thrift.CompressionCodec.lz4, maxSize: 500);
      expect(
        () => codec.decompress(frame.takeBytes()),
        throwsA(isA<LimitException>()),
      );
    });
  });

  group('#7 Parquet page cap is configurable', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('r52_'));
    tearDown(() => dir.deleteSync(recursive: true));

    Table intTable(String col, List<int> v) => Table(
      name: 't',
      columns: [NumericColumn.int64(name: col, values: v, nullable: false)],
      schema: Schema(
        fields: [Field(name: col, dataType: DataType.int64, nullable: false)],
      ),
    );

    test('a tiny maxPageBytes rejects an over-cap page', () async {
      final path = '${dir.path}/p.parquet';
      await ParquetWriter(
        path,
      ).write(intTable('id', [for (var i = 0; i < 1000; i++) i])).ok;
      await expectLater(
        ParquetReader(
          path,
          options: const ParquetReaderOptions(maxPageBytes: 8),
        ).read(),
        failsWith(Kind.corrupt),
      );
    });
  });

  group('#3 nested Parquet readBatches honors size', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('round46_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('a nested column streams in multiple bounded batches', () async {
      final path = '${dir.path}/n.parquet';
      final t = Table(
        name: 't',
        columns: [
          ListColumn(
            name: 'tags',
            dataType: DataType.list,
            data: [
              for (var i = 0; i < 5; i++) [i, i + 1],
            ],
            nullable: true,
          ),
        ],
        schema: Schema(
          fields: [
            Field(
              name: 'tags',
              dataType: DataType.list,
              nullable: true,
              children: [
                Field(name: 'item', dataType: DataType.int64, nullable: true),
              ],
            ),
          ],
        ),
      );
      await ParquetWriter(path).write(t).ok;
      final batches = await ParquetReader(
        path,
      ).readBatches(batchSize: 2).ok.toList();
      // 5 rows in windows of 2 → 3 batches (2, 2, 1), each carrying the list.
      expect(batches.length, greaterThan(1));
      for (final b in batches) {
        expect(b.schema.field('tags')!.dataType, DataType.list);
        expect(b.rowCount, lessThanOrEqualTo(2));
      }
      expect(batches.fold(0, (s, b) => s + b.rowCount), 5);
      expect(
        batches.expand((b) => b.column('tags')!.toList()).first,
        equals([0, 1]),
      );
    });
  });

  group('#4c nested Parquet preflight', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('round43_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('an over-cap nested read throws', () async {
      final path = '${dir.path}/n.parquet';
      final t = Table(
        name: 't',
        columns: [
          ListColumn(
            name: 'l',
            dataType: DataType.list,
            data: [
              [1, 2],
              [3, 4],
              [5, 6],
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
          ],
        ),
      );
      await ParquetWriter(path).write(t).ok;
      expect(ParquetReader(path).read(maxRows: 1), failsWith(Kind.limit));
      expect((await ParquetReader(path).read(maxRows: null).ok).rowCount, 3);
    });
  });

  group('Thrift compact reader fails loud', () {
    test(
      'a run of continuation bytes throws (varint cap, no infinite loop)',
      () {
        final reader = CompactReader(Uint8List.fromList(List.filled(16, 0x80)));
        expect(reader.readI32, throwsA(isA<FormatException>()));
      },
    );

    test('a truncated buffer throws FormatException, not RangeError', () {
      final reader = CompactReader(Uint8List(0));
      expect(reader.readByte, throwsA(isA<FormatException>()));
    });

    test('readDouble on a short slice throws (no parent-buffer over-read)', () {
      // A 4-byte slice of a larger parent buffer: readDouble needs 8 and must
      // refuse rather than read 4 adjacent parent bytes.
      final parent = Uint8List.fromList(List.generate(64, (i) => i));
      final slice = Uint8List.sublistView(
        parent,
        8,
        12,
      ); // len 4, parent has more
      expect(CompactReader(slice).readDouble, throwsA(isA<FormatException>()));
    });

    test('readString with an over-long length throws FormatException', () {
      // varint length 100 then only a few bytes → bounded, domain error.
      final reader = CompactReader(Uint8List.fromList([100, 1, 2, 3]));
      expect(reader.readString, throwsA(isA<FormatException>()));
    });
  });

  // a malformed Parquet encoding header must not be able to
  // drive an unbounded allocation (a decompression / run-length "size bomb").
  group('DELTA_BINARY_PACKED rejects crafted headers', () {
    // Header: block size, miniblocks per block, value count (ULEB128), then
    // the first value (zigzag ULEB128).
    Uint8List header(int block, int miniblocks, int count, [List<int>? rest]) =>
        Uint8List.fromList([
          ..._uleb(block),
          ..._uleb(miniblocks),
          ..._uleb(count),
          0x00,
          ...?rest,
        ]);

    for (final MapEntry(key: name, value: data) in {
      // A 14-byte header that asked for a 1 TiB allocation.
      'more miniblocks than bytes': header(1 << 40, 1 << 40, 3),
      'block size not a multiple of 128': header(100, 4, 3),
      'miniblock size not a multiple of 32': header(128, 8, 3),
      'a bit width wider than the type': header(128, 4, 3, [
        0x00, // min delta
        65, 0, 0, 0, // widths
        ...List.filled(1024, 0),
      ]),
    }.entries) {
      test(name, () {
        for (final decode in [
          () => DeltaBinaryPackedDecoder.decodeInto(data, 3, Int64List(3), 0),
          () => DeltaBinaryPackedDecoder.decode(data, 3),
        ]) {
          expect(decode, throwsFormatException);
        }
      });
    }

    test('a 32-bit column rejects a 33-bit width', () {
      final data = header(128, 4, 3, [
        0x00,
        33,
        0,
        0,
        0,
        ...List.filled(1024, 0),
      ]);
      expect(
        () => DeltaBinaryPackedDecoder.decodeInto(
          data,
          3,
          Int64List(3),
          0,
          bits: 32,
        ),
        throwsFormatException,
      );
    });
  });

  group('DELTA_BINARY_PACKED decoded into a buffer', () {
    int state = 7;
    int next() => state = (state * 6364136223846793005 + 1442695040888963407);

    final shapes = <String, List<int>>{
      'sorted ids': [for (var i = 0; i < 5000; i++) 1000 + i],
      'timestamps': [
        for (var i = 0; i < 5000; i++) 1700000000000000 + i * 997 + i % 13,
      ],
      'small random': [for (var i = 0; i < 5000; i++) next() % 1000],
      'wide random': [for (var i = 0; i < 5000; i++) next() >> 9],
      'one value': [42],
      'constant': List.filled(3000, -5),
    };
    for (final MapEntry(key: name, value: values) in shapes.entries) {
      test('agrees with the list decoder on $name', () {
        final data = DeltaBinaryPackedEncoder().encodeInts(values);
        final expected = DeltaBinaryPackedDecoder.decode(data, values.length);
        final into = Int64List(values.length + 3);
        final written = DeltaBinaryPackedDecoder.decodeInto(
          data,
          values.length,
          into,
          3,
        );
        expect(written, values.length);
        expect(into.sublist(3), expected);
        expect(expected, values);
      });
    }

    test('stops at the count and still checks the rest', () {
      final values = [for (var i = 0; i < 1000; i++) i * 3];
      final data = DeltaBinaryPackedEncoder().encodeInts(values);
      final into = Int64List(10);
      expect(DeltaBinaryPackedDecoder.decodeInto(data, 10, into, 0), 10);
      expect(into, values.take(10));
    });

    test('int32 columns wrap in 32 bits', () {
      final values = [2147483647, -2147483648, 0, 2147483647, -1];
      final data = DeltaBinaryPackedEncoder(
        thrift.ParquetType.int32,
      ).encodeInts(values);
      final into = Int64List(values.length);
      DeltaBinaryPackedDecoder.decodeInto(
        data,
        values.length,
        into,
        0,
        bits: 32,
      );
      expect(into, values);
    });

    test('a truncated stream throws', () {
      final values = [for (var i = 0; i < 1000; i++) next() >> 20];
      final data = DeltaBinaryPackedEncoder().encodeInts(values);
      expect(
        () => DeltaBinaryPackedDecoder.decodeInto(
          Uint8List.sublistView(data, 0, data.length ~/ 2),
          values.length,
          Int64List(values.length),
          0,
        ),
        throwsA(isA<FormatException>()),
      );
    });
  });

  group('dictionary indices decoded into a buffer', () {
    List<int> indices(int count, int max, int seed) {
      var state = seed;
      int next() => state = (state * 1103515245 + 12345) & 0x7fffffff;
      final out = <int>[];
      while (out.length < count) {
        final value = next() % (max + 1);
        final run = next() % 3 == 0 ? next() % 100 : 1;
        for (var i = 0; i < run && out.length < count; i++) {
          out.add(run == 1 ? next() % (max + 1) : value);
        }
      }
      return out;
    }

    for (final width in [1, 2, 7, 8, 9, 13, 17, 24, 31]) {
      test('agrees with the list decoder at width $width', () {
        for (final count in [1, 8, 9, 1000, 4099]) {
          final expected = indices(count, (1 << width) - 1, count + width);
          final data = RleBitpackedHybrid.encode(expected, width);
          final into = Int32List(count);
          final decoded = RleBitpackedHybrid.decodeInto(
            data,
            width,
            count,
            into,
          );
          expect(decoded, count);
          expect(into, expected, reason: 'count $count');
        }
      });
    }

    test('a final run is cut at the count', () {
      final data = Uint8List.fromList([..._uleb((1 << 27) << 1), 0x02]);
      final into = Int32List(3);
      expect(RleBitpackedHybrid.decodeInto(data, 2, 3, into), 3);
      expect(into, [2, 2, 2]);
    });

    test('a truncated bit-packed run throws', () {
      final data = Uint8List.fromList([0x03, 0xFF]);
      expect(
        () => RleBitpackedHybrid.decodeInto(data, 9, 8, Int32List(8)),
        throwsA(isA<FormatException>()),
      );
    });

    test(
      'dictionary-encoded int64, double and int32 columns read back',
      () async {
        // Written by DuckDB (our writer does not dictionary-encode): 9000 rows,
        // row groups of 4096, each column also repeated with every fifth null.
        final table = await ParquetReader(
          'test/fixtures/dictionary_numeric.parquet',
        ).read().ok;
        final rows = [for (var i = 0; i < 9000; i++) i];
        final longs = [for (final i in rows) (i % 13) * 1000003];
        final doubles = [for (final i in rows) (i % 11) * 0.25];
        final ints = [for (final i in rows) (i % 17) - 8];
        List<Object?> holes(List<Object> values) => [
          for (final i in rows) i % 5 == 0 ? null : values[i],
        ];
        expect(table.column('l')!.toList(), longs);
        expect(table.column('d')!.toList(), doubles);
        expect(table.column('n')!.toList(), ints);
        expect(table.column('lh')!.toList(), holes(longs));
        expect(table.column('dh')!.toList(), holes(doubles));
        expect(table.column('nh')!.toList(), holes(ints));
      },
    );
  });

  group('definition levels decoded into a buffer', () {
    // Levels as a writer would produce them: long runs (encoded as RLE) mixed
    // with noise (encoded bit-packed).
    List<int> levels(int count, int max, int seed) {
      var state = seed;
      int next() => state = (state * 1103515245 + 12345) & 0x7fffffff;
      final out = <int>[];
      while (out.length < count) {
        final value = next() % (max + 1);
        final run = next() % 3 == 0 ? next() % 200 : 1;
        for (var i = 0; i < run && out.length < count; i++) {
          out.add(run == 1 ? next() % (max + 1) : value);
        }
      }
      return out;
    }

    for (final max in [1, 2, 3, 7]) {
      test('agrees with the list decoder at max level $max', () {
        final width = levelBitWidth(max);
        for (final count in [1, 7, 8, 9, 1000, 4099]) {
          final expected = levels(count, max, count * 31 + max);
          final data = RleBitpackedHybrid.encode(expected, width);
          final into = Uint8List(count);
          final (:decoded, :present) = RleBitpackedHybrid.decodeLevels(
            data,
            width,
            count,
            into,
            max,
          );
          expect(decoded, count);
          expect(into, expected, reason: 'count $count');
          expect(present, expected.where((level) => level == max).length);
        }
      });
    }

    test('stops at the count inside a padded bit-packed block', () {
      // 256 one-bit levels of 1 in one bit-packed run; only 16 are wanted.
      final data = Uint8List.fromList([0x41, ...List.filled(32, 0xFF)]);
      final into = Uint8List(16);
      final (:decoded, :present) = RleBitpackedHybrid.decodeLevels(
        data,
        1,
        16,
        into,
        1,
      );
      expect(decoded, 16);
      expect(present, 16);
    });

    test('an RLE run past the count bound throws', () {
      final data = Uint8List.fromList([..._uleb(5000 << 1), 0x01]);
      expect(
        () => RleBitpackedHybrid.decodeLevels(data, 1, 10, Uint8List(10), 1),
        throwsA(isA<FormatException>()),
      );
    });

    test('a truncated bit-packed run throws', () {
      // Claims 8 values at width 3 (3 bytes) but carries one.
      final data = Uint8List.fromList([0x03, 0xFF]);
      expect(
        () => RleBitpackedHybrid.decodeLevels(data, 3, 8, Uint8List(8), 7),
        throwsA(isA<FormatException>()),
      );
    });

    test('short data reports how many levels it held', () {
      final data = Uint8List.fromList([..._uleb(3 << 1), 0x01]);
      final (:decoded, :present) = RleBitpackedHybrid.decodeLevels(
        data,
        1,
        10,
        Uint8List(10),
        1,
      );
      expect(decoded, 3);
      expect(present, 3);
    });

    test(
      'an optional column with and without nulls reads back exactly',
      () async {
        final dir = Directory.systemTemp.createTempSync('levels_');
        addTearDown(() => dir.deleteSync(recursive: true));
        Future<List<Object?>> roundTrip(List<double?> values) async {
          final path = '${dir.path}/${values.length}-${values.first}.parquet';
          // Streamed batches keep the leaf optional even with no nulls.
          final schema = Schema(
            fields: [Field(name: 'x', dataType: DataType.float64)],
          );
          final writer = ParquetWriter(
            path,
            options: const ParquetWriterOptions(rowGroupSize: 3000),
          );
          await writer.open(schema).ok;
          await writer
              .writeBatch(
                Table(
                  name: 't',
                  schema: schema,
                  columns: [
                    ListColumn<double>(
                      name: 'x',
                      dataType: DataType.float64,
                      data: values,
                      nullable: true,
                    ),
                  ],
                ),
              )
              .ok;
          await writer.close().ok;
          final table = await ParquetReader(path).read().ok;
          return table.column('x')!.toList();
        }

        final full = [for (var i = 0; i < 10000; i++) i * 0.5];
        expect(await roundTrip(full), full);
        final holes = [
          for (var i = 0; i < 10000; i++) i % 7 == 0 ? null : i * 1.5,
        ];
        expect(await roundTrip(holes), holes);
      },
    );
  });

  group('Parquet decoder allocation guards', () {
    test('RLE run length past maxValues throws (not a giant alloc)', () {
      // bitWidth 0, one RLE run claiming 2000 zeros; cap (maxValues 10 + slack)
      // is far below, so it must reject rather than build the list.
      final data = Uint8List.fromList(_uleb(2000 << 1));
      expect(
        () => RleBitpackedHybrid.decode(data, 0, maxValues: 10),
        throwsA(isA<FormatException>()),
      );
    });

    test('a run within bounds still decodes', () {
      final data = Uint8List.fromList(_uleb(5 << 1)); // 5 zeros, bitWidth 0
      expect(RleBitpackedHybrid.decode(data, 0, maxValues: 16).length, 5);
    });

    test('a bit-packed run over maxValues+8 decodes (DuckDB block padding)', () {
      // Writers (e.g. DuckDB) flush definition levels in fixed bit-packed blocks
      // and pad the final block, legitimately overrunning the declared count by
      // up to a block. A bit-packed run is byte-bounded (readBitPacked must read
      // the bytes), so it is allowed past the tight cap; the caller clamps the
      // surplus. Before the fix this threw "exceeds page value bound".
      final section = <int>[];
      for (int run = 0; run < 2; run++) {
        section.add(0x41); // bit-packed header: 32 groups × 8 = 256 values
        section.addAll(List.filled(32, 0xFF)); // 256 one-bit values = 32 bytes
      }
      final data = Uint8List.fromList(section);
      final decoded = RleBitpackedHybrid.decode(data, 1, maxValues: 16);
      expect(decoded.length, 512); // 2×256, far over maxValues(16)+8
      expect(decoded.every((v) => v == 1), isTrue);
    });

    // A data page's index stream may legally overrun its value count, so the
    // decoder must stop at the count rather than expand the whole run.
    test('dictionary indices stop at the requested count', () {
      // bit width 1, then one RLE run of 2^27 copies of index 0: 7 bytes.
      final data = Uint8List.fromList([1, ..._uleb((1 << 27) << 1), 0x00]);
      final decoder = DictionaryDecoder(thrift.ParquetType.int32);
      final watch = Stopwatch()..start();
      expect(decoder.decodeIndices(data, 1), [0]);
      expect(watch.elapsed, lessThan(const Duration(milliseconds: 100)));
    });

    test('an overrunning final run is cut at the count', () {
      final data = Uint8List.fromList([1, ..._uleb(10 << 1), 0x01]);
      final decoder = DictionaryDecoder(thrift.ParquetType.int32);
      expect(decoder.decodeIndices(data, 3), [1, 1, 1]);
    });

    test('a bit-packed run is cut at the limit', () {
      // 8 one-bit values 0b10110101, limit 5.
      final data = Uint8List.fromList([0x03, 0xB5]);
      expect(RleBitpackedHybrid.decode(data, 1, limit: 5), [1, 0, 1, 0, 1]);
    });

    test('boolean RLE stops at the requested count', () {
      // 4-byte length prefix, then one RLE run of 2^27 trues.
      final run = [..._uleb((1 << 27) << 1), 0x01];
      final data = Uint8List.fromList([run.length, 0, 0, 0, ...run]);
      final watch = Stopwatch()..start();
      expect(RleDecoder().decode(data, 2), [true, true]);
      expect(watch.elapsed, lessThan(const Duration(milliseconds: 100)));
    });

    test('an RLE run (bit width >= 1) past maxValues still throws', () {
      // The RLE branch materializes runLength copies from a tiny header with no
      // further reads (a real size bomb) so it keeps the tight cap.
      final data = Uint8List.fromList([..._uleb(5000 << 1), 0x01]);
      expect(
        () => RleBitpackedHybrid.decode(data, 1, maxValues: 10),
        throwsA(isA<FormatException>()),
      );
    });

    // corrupt/truncated input fails with a domain FormatException,
    // not a raw RangeError (which reads as an internal VM bug + can kill isolates).
    test('a truncated RLE/bit-packed ByteReader throws FormatException', () {
      expect(
        ByteReader(Uint8List(0)).readByte,
        throwsA(isA<FormatException>()),
      );
      expect(
        () => ByteReader(Uint8List(2)).readBytes(5),
        throwsA(isA<FormatException>()),
      );
    });

    test('a page over-claiming present values throws FormatException', () {
      // def-levels declare two present values (level == maxDef) but only one was
      // decoded: a corrupt page, not a RangeError.
      final decoder = PageDecoder(
        type: thrift.ParquetType.int32,
        maxDefinitionLevel: 1,
      );
      expect(
        () => decoder.reconstruct([42], [1, 1]),
        throwsA(isA<FormatException>()),
      );
    });

    // a DataPageV2 whose level byte-lengths exceed the page throws
    // a domain FormatException, not a raw RangeError (V1 already guarded this).
    test('a V2 page with over-long level lengths throws FormatException', () {
      final decoder = PageDecoder(
        type: thrift.ParquetType.int32,
        maxDefinitionLevel: 1,
      );
      final header = thrift.PageHeader()
        ..type = thrift.PageType.dataPageV2
        ..uncompressedPageSize = 4
        ..compressedPageSize = 4
        ..dataPageHeaderV2 = (thrift.DataPageHeaderV2()
          ..numValues = 1
          ..numRows = 1
          ..encoding = thrift.Encoding.plain
          ..repetitionLevelsByteLength = 0
          ..definitionLevelsByteLength = 9999); // >> the 4-byte page
      expect(
        () => decoder.decodeDataV2(header, Uint8List(4)),
        throwsA(isA<FormatException>()),
      );
    });

    group('V2 page header validation', () {
      final decoder = PageDecoder(
        type: thrift.ParquetType.int32,
        maxDefinitionLevel: 1,
      );
      // Two values; def levels [1, 0] as one bit-packed group, then one int32.
      final data = Uint8List.fromList([0x03, 0x01, 7, 0, 0, 0]);
      thrift.PageHeader header({int nulls = 1, int rep = 0, int def = 2}) =>
          thrift.PageHeader()
            ..type = thrift.PageType.dataPageV2
            ..uncompressedPageSize = data.length
            ..compressedPageSize = data.length
            ..dataPageHeaderV2 = (thrift.DataPageHeaderV2()
              ..numValues = 2
              ..numNulls = nulls
              ..numRows = 2
              ..encoding = thrift.Encoding.plain
              ..repetitionLevelsByteLength = rep
              ..definitionLevelsByteLength = def);

      test('a negative null count throws FormatException', () {
        expect(
          () => decoder.parseBody(header(nulls: -0x7fffffff), data),
          throwsA(isA<FormatException>()),
        );
      });

      test('a null count above the value count throws FormatException', () {
        expect(
          () => decoder.parseBody(header(nulls: 3), data),
          throwsA(isA<FormatException>()),
        );
      });

      test('a negative level length throws FormatException', () {
        expect(
          () => decoder.parseBody(header(rep: -1), data),
          throwsA(isA<FormatException>()),
        );
        expect(
          () => decoder.parseBody(header(def: -1), data),
          throwsA(isA<FormatException>()),
        );
      });

      test('the present count comes from the definition levels', () {
        // The header claims no nulls; the levels show one.
        expect(decoder.parseBody(header(nulls: 0), data).present, 1);
      });

      test('nulls without definition levels throw FormatException', () {
        // Nothing says which values are null, so the typed paths would read
        // the missing slots as zeros.
        expect(
          () => decoder.parseBody(header(nulls: 1, def: 0), data),
          throwsA(isA<FormatException>()),
        );
      });
    });

    test('a V1 page too short for its level length throws FormatException', () {
      final decoder = PageDecoder(
        type: thrift.ParquetType.int32,
        maxDefinitionLevel: 1,
      );
      final header = thrift.PageHeader()
        ..type = thrift.PageType.dataPage
        ..uncompressedPageSize = 2
        ..compressedPageSize = 2
        ..dataPageHeader = (thrift.DataPageHeader()
          ..numValues = 1
          ..encoding = thrift.Encoding.plain
          ..definitionLevelEncoding = thrift.Encoding.rle
          ..repetitionLevelEncoding = thrift.Encoding.rle);
      expect(
        () => decoder.parseBody(header, Uint8List(2)),
        throwsA(isA<FormatException>()),
      );
    });

    test(
      'DELTA_BINARY_PACKED with zero miniblocks throws (no div-by-zero)',
      () {
        // blockSize=128, miniblocksPerBlock=0, totalValues=10, firstValue=0
        final data = Uint8List.fromList([
          ..._uleb(128),
          ..._uleb(0),
          ..._uleb(10),
          ..._uleb(0),
        ]);
        expect(
          () => DeltaBinaryPackedDecoder.decode(data, 10),
          throwsA(isA<FormatException>()),
        );
      },
    );

    test('DELTA_BINARY_PACKED with an absurd value count throws', () {
      // totalValues far beyond any real page → reject before allocating.
      final data = Uint8List.fromList([
        ..._uleb(128),
        ..._uleb(1),
        ..._uleb((1 << 27) + 1),
        ..._uleb(0),
      ]);
      expect(
        () => DeltaBinaryPackedDecoder.decode(data, 8),
        throwsA(isA<FormatException>()),
      );
    });

    test('DELTA_BINARY_PACKED stops at the requested count', () {
      // 12 bytes: one width-0 miniblock claiming 2^27 values, which reads no
      // further input and so must not be expanded past the requested count.
      final data = Uint8List.fromList([
        ..._uleb(1 << 27), // block size
        ..._uleb(1), // miniblocks per block
        ..._uleb(1 << 27), // total values
        ..._uleb(0), // first value (zigzag)
        ..._uleb(2), // min delta 1 (zigzag)
        0, // bit width
      ]);
      final reader = ByteReader(data);
      final watch = Stopwatch()..start();
      expect(DeltaBinaryPackedDecoder.decodeFrom(reader, 3), [0, 1, 2]);
      expect(watch.elapsed, lessThan(const Duration(milliseconds: 100)));
      expect(reader.position, data.length);
    });

    test('DELTA_BINARY_PACKED skips unread miniblocks to the stream end', () {
      // One 32-value miniblock of width 1 (four bytes), then a sentinel.
      final data = Uint8List.fromList([
        ..._uleb(128), // block size
        ..._uleb(4), // miniblocks per block
        ..._uleb(33), // total values
        ..._uleb(0), // first value
        ..._uleb(0), // min delta 0
        1, 1, 1, 1, // bit widths
        0xFF, 0xFF, 0xFF, 0xFF, // every delta is 1
        0xAB, // sentinel after the stream
      ]);
      final reader = ByteReader(data);
      expect(DeltaBinaryPackedDecoder.decodeFrom(reader, 2), [0, 1]);
      expect(reader.readByte(), 0xAB);
    });
  });

  group('the writer never writes a corrupt file', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('pq_corrupt_'));
    tearDown(() => dir.deleteSync(recursive: true));

    test('a top-level column named like a nested leaf', () async {
      final path = '${dir.path}/collide.parquet';
      final schema = Schema(
        fields: [
          Field(name: 'id', dataType: DataType.int64, nullable: false),
          Field(
            name: 's',
            dataType: DataType.struct,
            children: [Field(name: 'id', dataType: DataType.int64)],
          ),
        ],
      );
      final table = Table(
        name: 't',
        schema: schema,
        columns: [
          NumericColumn.int64(name: 'id', values: [1, 2, 3], nullable: false),
          ListColumn<Map<String, dynamic>?>(
            name: 's',
            dataType: DataType.struct,
            data: [
              {'id': 10},
              {'id': null},
              null,
            ],
          ),
        ],
      );
      await ParquetWriter(path).write(table).ok;
      final back = await ParquetReader(path).read().ok;
      expect(back.toRows(), [
        {
          'id': 1,
          's': {'id': 10},
        },
        {
          'id': 2,
          's': {'id': null},
        },
        {'id': 3, 's': null},
      ]);
    });

    test('a null in a required column is refused', () async {
      final schema = Schema(
        fields: [Field(name: 'a', dataType: DataType.int64, nullable: false)],
      );
      final writer = ParquetWriter('${dir.path}/required.parquet');
      await writer.open(schema).ok;
      // Batches are buffered, so the refusal comes from the batch or the
      // flush at close; either way nothing is written.
      final batch = await writer.writeBatch(
        Table(
          name: 't',
          schema: Schema(
            fields: [Field(name: 'a', dataType: DataType.int64)],
          ),
          columns: [
            NumericColumn.int64(name: 'a', values: [1, null, 3]),
          ],
        ),
      );
      await expectLater(
        Future.value(batch is Err ? batch : await writer.close()),
        failsWith(Kind.invalid, contains('required')),
      );
      expect(File('${dir.path}/required.parquet').existsSync(), isFalse);
    });

    test('large values are split into bounded pages', () async {
      // 6 MiB of text: one page before, which a reader capped at 2 MiB per
      // page refused.
      final path = '${dir.path}/big.parquet';
      final value = 'x' * 2048;
      await ParquetWriter(path)
          .write(
            Table.fromRows('t', [
              for (var i = 0; i < 3000; i++) {'s': '$value$i'},
            ]),
          )
          .ok;
      final back = await ParquetReader(
        path,
        options: const ParquetReaderOptions(maxPageBytes: 2 << 20),
      ).read().ok;
      expect(back.rowCount, 3000);
      expect(back.column('s')![2999], '${value}2999');
    });
  });

  group('Parquet writes are crash-safe', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('quickwins_'));
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

    String parquetPath() => '${dir.path}/data.parquet';

    test('no .tmp left after a successful write', () async {
      final path = parquetPath();
      await ParquetWriter(path).write(sample()).ok;
      expect(File(path).existsSync(), isTrue);
      expect(File('$path.tmp').existsSync(), isFalse);
    });

    test('a stream failure leaves no file', () async {
      final path = '${dir.path}/streamed.parquet';
      Stream<DataFrame> batches() async* {
        yield DataFrame.fromTable(sample());
        throw StateError('boom mid-stream');
      }

      await expectLater(
        DataFrameIO.writeParquetStream(
          path,
          batches(),
          options: const ParquetWriterOptions(rowGroupSize: 10),
        ),
        failsWith(Kind.invalid),
      );
      expect(File(path).existsSync(), isFalse);
      expect(File('$path.tmp').existsSync(), isFalse);
    });
  });

  group('review defects (streaming Parquet)', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('review_defects'));
    tearDown(() => dir.deleteSync(recursive: true));

    group('#7 streaming Parquet rejects schema drift', () {
      test('a drifted second batch throws and leaves no temp', () async {
        final path = '${dir.path}/drift.parquet';
        final a = Table(
          name: 't',
          schema: Schema(
            fields: [Field(name: 'x', dataType: DataType.int64)],
          ),
          columns: [
            NumericColumn.int64(name: 'x', values: [1, 2], nullable: true),
          ],
        );
        final b = Table(
          name: 't',
          schema: Schema(
            fields: [Field(name: 'y', dataType: DataType.int64)],
          ),
          columns: [
            NumericColumn.int64(name: 'y', values: [3], nullable: true),
          ],
        );
        await expectLater(
          ParquetWriter(path).writeBatches(Stream.fromIterable([a, b])),
          failsWith(Kind.schema),
        );
        final orphans = await dir
            .list()
            .where((e) => e.path.endsWith('.tmp'))
            .toList();
        expect(orphans, isEmpty);
      });
    });

    test(
      '#9 a large single batch spanning many row groups round-trips',
      () async {
        const n = 5000;
        final table = Table(
          name: 't',
          schema: Schema(
            fields: [Field(name: 'v', dataType: DataType.int64)],
          ),
          columns: [
            NumericColumn.int64(
              name: 'v',
              values: [for (var i = 0; i < n; i++) i],
              nullable: false,
            ),
          ],
        );
        final path = '${dir.path}/big.parquet';
        final writer = ParquetWriter(
          path,
          options: const ParquetWriterOptions(
            rowGroupSize: 500,
          ), // 10 row groups
        );
        await writer.writeBatch(table).ok;
        await writer.close().ok;
        final back = await ParquetReader(path).read().ok;
        expect(back.rowCount, n);
        expect(back.column('v')!.toList(), [for (var i = 0; i < n; i++) i]);
      },
    );
  });

  group('review findings (Parquet)', () {
    late Directory dir;
    setUp(() async {
      dir = await Directory.systemTemp.createTemp('review_findings');
    });
    tearDown(() async {
      if (await dir.exists()) await dir.delete(recursive: true);
    });

    group('finding 5: invalid sizes throw instead of hanging', () {
      test('ParquetWriterOptions(rowGroupSize: 0) throws', () {
        expect(
          () => ParquetWriterOptions(rowGroupSize: 0),
          throwsA(isA<Object>()), // AssertionError in checked mode
        );
      });
    });

    group('finding 8: no temp orphan on a failed rename', () {
      test(
        'Parquet write to a destination that is a directory leaves no .tmp',
        () async {
          final dest = '${dir.path}/dest.parquet';
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
            ParquetWriter(dest).write(table),
            failsWith(Kind.io),
          );
          final orphans = await dir
              .list()
              .where((e) => e.path.endsWith('.tmp'))
              .toList();
          expect(orphans, isEmpty);
        },
      );
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

    test('Parquet projection of an unknown column throws', () async {
      final path = '${dir.path}/data.parquet';
      await ParquetWriter(path).write(sample()).ok;
      expect(
        DataFrameIO.readParquet(path, columns: ['id', 'ghost']),
        failsWith(Kind.invalid),
      );
    });
  });

  // DELTA_BYTE_ARRAY reuses each value's prefix, so a small page can claim far
  // more output than it holds. Lengths are checked before allocating and the
  // total decoded size is bounded.
  group('DELTA_BYTE_ARRAY rejects hostile lengths', () {
    Uint8List page(List<int> prefixes, List<int> suffixes, [int body = 0]) {
      final encoder = DeltaBinaryPackedEncoder();
      return Uint8List.fromList([
        ...encoder.encodeInts(prefixes),
        ...encoder.encodeInts(suffixes),
        ...List.filled(body, 0x61),
      ]);
    }

    test('a negative suffix length throws FormatException', () {
      expect(
        () => DeltaByteArrayDecoder.decode(page([0], [-1]), 1),
        throwsA(isA<FormatException>()),
      );
    });

    test('a negative prefix length throws FormatException', () {
      expect(
        () => DeltaByteArrayDecoder.decode(page([0, -1], [1, 0], 1), 2),
        throwsA(isA<FormatException>()),
      );
    });

    test('a suffix past the page end throws FormatException', () {
      expect(
        () => DeltaByteArrayDecoder.decode(page([0], [10], 3), 1),
        throwsA(isA<FormatException>()),
      );
    });

    test('prefix reuse past the output limit throws FormatException', () {
      // One 100-byte value, then 19 copies of it via prefix reuse: 2000 bytes
      // of output from about 120 bytes of input.
      final data = page(
        [0, for (var i = 1; i < 20; i++) 100],
        [100, for (var i = 1; i < 20; i++) 0],
        100,
      );
      expect(DeltaByteArrayDecoder.decode(data, 20), hasLength(20));
      expect(
        () => DeltaByteArrayDecoder.decode(data, 20, limit: 1000),
        throwsA(isA<FormatException>()),
      );
    });

    test('a length-array value past the page end throws FormatException', () {
      final data = Uint8List.fromList([
        ...DeltaBinaryPackedEncoder().encodeInts([10]),
        0x61,
      ]);
      expect(
        () => DeltaLengthByteArrayDecoder.decode(data, 1),
        throwsA(isA<FormatException>()),
      );
    });
  });

  // A corrupt/hostile page must yield a domain FormatException, never a raw
  // RangeError from deep inside a sublist. The BYTE_ARRAY length prefix is read
  // SIGNED, so a negative length slipped past the bounds check (`n < -1` is
  // false) and reached `sublist(offset, offset - 1)`. The sibling
  // DeltaLengthByteArrayDecoder already rejects `length < 0`; this path was
  // missed.
  group('PLAIN BYTE_ARRAY rejects a negative length', () {
    Uint8List page(int length, {int trailing = 8}) {
      final b = BytesBuilder();
      final len = Uint8List(4);
      ByteData.view(len.buffer).setInt32(0, length, Endian.little);
      b.add(len);
      b.add(Uint8List(trailing)); // some payload to read past
      return b.toBytes();
    }

    test('-1 throws a FormatException, not a RangeError', () {
      expect(
        () => PlainDecoder(thrift.ParquetType.byteArray).decode(page(-1), 1),
        throwsA(isA<FormatException>()),
      );
    });

    test('int32 min throws a FormatException', () {
      expect(
        () => PlainDecoder(
          thrift.ParquetType.byteArray,
        ).decode(page(-2147483648), 1),
        throwsA(isA<FormatException>()),
      );
    });

    test(
      'a tolerant decoder also refuses it (it is corruption, not truncation)',
      () {
        // `tolerant` exists for legacy pages that end EARLY: a negative length is
        // malformed either way and must not be silently swallowed.
        expect(
          () => PlainDecoder(
            thrift.ParquetType.byteArray,
            tolerant: true,
          ).decode(page(-1), 1),
          throwsA(isA<FormatException>()),
        );
      },
    );

    test('a zero-length value still decodes', () {
      final v = PlainDecoder(thrift.ParquetType.byteArray).decode(page(0), 1);
      expect(v.single, isEmpty);
    });

    test('a valid length still decodes', () {
      final b = BytesBuilder();
      final len = Uint8List(4);
      ByteData.view(len.buffer).setInt32(0, 3, Endian.little);
      b.add(len);
      b.add(Uint8List.fromList([1, 2, 3]));
      final v = PlainDecoder(
        thrift.ParquetType.byteArray,
      ).decode(b.toBytes(), 1);
      expect(v.single, [1, 2, 3]);
    });

    test('a dictionary index bit width over 32 is a FormatException '
        '(audit 7)', () {
      // The first byte of a dictionary-index stream is its bit width; a byte
      // can claim up to 255, and anything over 32 is corrupt. It used to
      // surface as a raw ArgumentError from the RLE decoder, breaking the
      // corrupt-Parquet -> FormatException contract.
      final decoder = DictionaryDecoder(thrift.ParquetType.int32);
      expect(
        () => decoder.decodeIndices(Uint8List.fromList([64, 0, 0]), 1),
        throwsA(isA<FormatException>()),
      );
    });
  });

  group('schema layout built once per file', () {
    thrift.SchemaElement element(
      String name, {
      thrift.ParquetType? type,
      thrift.FieldRepetitionType repetition =
          thrift.FieldRepetitionType.optional,
      int? children,
    }) => thrift.SchemaElement()
      ..name = name
      ..type = type
      ..repetitionType = repetition
      ..numChildren = children;

    // root { a; s { b; l* }; c* { e } }
    final schema = [
      element('root', children: 3),
      element('a', type: thrift.ParquetType.int32),
      element('s', children: 2),
      element(
        'b',
        type: thrift.ParquetType.int64,
        repetition: thrift.FieldRepetitionType.required,
      ),
      element(
        'l',
        type: thrift.ParquetType.int32,
        repetition: thrift.FieldRepetitionType.repeated,
      ),
      element(
        'c',
        repetition: thrift.FieldRepetitionType.repeated,
        children: 1,
      ),
      element('e', type: thrift.ParquetType.double),
    ];

    test('levels match the per-column calculation', () {
      final layout = SchemaLayout(schema);
      for (final index in [1, 3, 4, 6]) {
        final expected = SchemaLevels.calculateLevels(schema, index);
        final levels = layout.levels(index);
        expect(levels.maxDefinition, expected.maxDefinition, reason: '$index');
        expect(levels.maxRepetition, expected.maxRepetition, reason: '$index');
      }
      expect(layout.levels(6).maxDefinition, 2);
      expect(layout.levels(6).maxRepetition, 1);
    });

    test('paths match the leaf paths', () {
      expect(SchemaLayout(schema).paths, SchemaLevels.leafPaths(schema));
      expect(SchemaLayout(schema).paths, ['a', 's.b', 's.l', 'c.e']);
    });
  });

  group('streamed batches match an eager read', () {
    // streaming_v2.parquet: DuckDB, PARQUET_VERSION V2, three row groups of
    // up to 2048 rows. id, n (every seventh null) and ts are
    // DELTA_BINARY_PACKED; s (every fifth null) and k are dictionary; d is
    // PLAIN. strings_v2.parquet: DuckDB, V2, DELTA_LENGTH_BYTE_ARRAY text.
    // strings_pages.parquet: pyarrow, one row group of 3000 rows cut into
    // pages of about 64 rows: PLAIN text (p), dictionary text (d) and int64
    // (v), all with nulls. A batch size of 61 crosses page and row group
    // boundaries.
    for (final name in [
      'streaming_v2.parquet',
      'strings_v2.parquet',
      'strings_pages.parquet',
      'dictionary_numeric.parquet',
      'byte_stream_split_nulls.parquet',
      'booleans_pages.parquet',
      'iris.parquet',
    ]) {
      test(name, () async {
        final path = 'test/fixtures/$name';
        final whole = await ParquetReader(path).read().ok;
        final batches = await ParquetReader(
          path,
        ).readBatches(batchSize: 61).ok.toList();
        expect(batches.length, greaterThan(1));
        for (final column in whole.columns) {
          final parts = [for (final b in batches) b.column(column.name)!];
          expect(
            [for (final part in parts) ...part.toList()],
            column.toList(),
            reason: column.name,
          );
          for (final part in parts) {
            expect(part.dataType, column.dataType, reason: column.name);
            expect(part.runtimeType, column.runtimeType, reason: column.name);
          }
        }
      });
    }
  });

  group('byte stream split', () {
    // byte_stream_split_nulls.parquet: pyarrow, 3000 rows in pages of 512
    // bytes, every eleventh row null, all four columns BYTE_STREAM_SPLIT.
    test('reads typed columns with nulls across pages', () async {
      final table = await ParquetReader(
        'test/fixtures/byte_stream_split_nulls.parquet',
      ).read().ok;
      List<num?> expected(num Function(int) value) => [
        for (var i = 0; i < 3000; i++) i % 11 == 0 ? null : value(i),
      ];
      final columns = {
        'f32': expected((i) => i * 0.5),
        'f64': expected((i) => i * 1.25 - 7),
        'i32': expected((i) => i * 7 - 9000),
        'i64': expected((i) => i * 1000003 - 5),
      };
      for (final MapEntry(:key, :value) in columns.entries) {
        final column = table.column(key)!;
        expect(column, isA<NumericColumn>(), reason: key);
        expect(column.toList(), value, reason: key);
      }
    });

    test('matches the PLAIN twin of each column', () async {
      final table = await ParquetReader(
        'test/fixtures/parquet-testing/data/'
        'byte_stream_split_extended.gzip.parquet',
      ).read().ok;
      for (final name in ['float', 'double', 'int32', 'int64']) {
        expect(
          table.column('${name}_byte_stream_split')!.toList(),
          table.column('${name}_plain')!.toList(),
          reason: name,
        );
      }
    });

    test('a short stream is a FormatException', () {
      expect(
        () => ByteStreamSplitDecoder(
          thrift.ParquetType.double,
        ).decodeInto(Uint8List(15), 2, Float64List(2), 0),
        throwsFormatException,
      );
    });
  });

  group('booleans read as BoolColumn', () {
    // booleans_pages.parquet: pyarrow, V2 pages of 50 values, 3000 rows: b is
    // RLE with every thirteenth row null, r is PLAIN with no nulls.
    test('RLE and PLAIN pages, with nulls', () async {
      final table = await ParquetReader(
        'test/fixtures/booleans_pages.parquet',
      ).read().ok;
      expect(table.column('b'), isA<BoolColumn>());
      expect(table.column('b')!.toList(), [
        for (var i = 0; i < 3000; i++) i % 13 == 0 ? null : i % 3 == 0,
      ]);
      expect(table.column('r'), isA<BoolColumn>());
      expect(table.column('r')!.toList(), [
        for (var i = 0; i < 3000; i++) i % 5 != 0,
      ]);
    });

    test('V1 PLAIN pages written by this library', () async {
      final dir = Directory.systemTemp.createTempSync('booleans_');
      addTearDown(() => dir.deleteSync(recursive: true));
      final values = [
        for (var i = 0; i < 1000; i++) i % 7 == 0 ? null : i.isEven,
      ];
      final path = '${dir.path}/b.parquet';
      await ParquetWriter(path)
          .write(
            Table(
              name: 't',
              columns: [BoolColumn(name: 'flag', values: values)],
              schema: Schema(
                fields: [Field(name: 'flag', dataType: DataType.boolean)],
              ),
            ),
          )
          .ok;
      final column = (await ParquetReader(path).read().ok).column('flag')!;
      expect(column, isA<BoolColumn>());
      expect(column.toList(), values);
    });

    test('a short page is a FormatException', () {
      final decoder = PageDecoder(type: thrift.ParquetType.boolean);
      expect(
        () => decoder.decodeValuesTyped(
          Uint8List(1),
          9,
          thrift.Encoding.plain,
          Uint8List(9),
          0,
        ),
        throwsFormatException,
      );
      expect(
        () => decoder.decodeValuesTyped(
          Uint8List.fromList([9, 0, 0, 0, 0]),
          4,
          thrift.Encoding.rle,
          Uint8List(4),
          0,
        ),
        throwsFormatException,
      );
    });
  });

  group('string columns read as StringColumn', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('strings_'));
    tearDown(() => dir.deleteSync(recursive: true));

    // fixed_utf8.parquet: pyarrow fixed_size_binary(3), every seventh row
    // null, with a UTF8 annotation added to the footer afterwards. DuckDB
    // reads it as VARCHAR.
    test('DuckDB JSON columns read as text', () async {
      final table = await ParquetReader(
        'test/fixtures/duckdb_json.parquet',
      ).read().ok;
      expect(table.schema.field('j')!.dataType, DataType.string);
      expect(table.column('j')!.toList(), ['{"a":0}', '{"a":1}', '{"a":2}']);
      expect(table.column('id')!.toList(), [0, 1, 2]);
    });

    test(
      'DuckDB INTERVAL columns read (INTERVAL is converted type 21)',
      () async {
        // The footer failed with "Invalid ConvertedType value: 21".
        final table = await ParquetReader(
          'test/fixtures/duckdb_interval.parquet',
        ).read().ok;
        expect(table.column('id')!.toList(), [0, 1, 2]);
        expect(table.schema.field('d')!.dataType, DataType.binary);
      },
    );

    test('converted types follow the spec numbering', () {
      expect(
        {
          for (final type in [
            thrift.ConvertedType.int32,
            thrift.ConvertedType.int64,
            thrift.ConvertedType.json,
            thrift.ConvertedType.bson,
            thrift.ConvertedType.interval,
          ])
            type.name: type.value,
        },
        {'int32': 17, 'int64': 18, 'json': 19, 'bson': 20, 'interval': 21},
      );
    });

    test('FIXED_LEN_BYTE_ARRAY with a UTF8 annotation', () async {
      final table = await ParquetReader(
        'test/fixtures/fixed_utf8.parquet',
      ).read().ok;
      final column = table.column('code')!;
      expect(column, isA<StringColumn>());
      expect(column.toList(), [
        for (var i = 0; i < 40; i++)
          i % 7 == 0 ? null : const ['abc', 'xyz', '\u00e9t', 'q12'][i % 4],
      ]);
      final batches = await ParquetReader(
        'test/fixtures/fixed_utf8.parquet',
      ).readBatches(batchSize: 9).ok.toList();
      expect(batches, hasLength(5));
      expect([
        for (final b in batches) ...b.column('code')!.toList(),
      ], column.toList());
    });

    test('dictionary encoded, with nulls', () async {
      final table = await ParquetReader(
        'test/fixtures/streaming_v2.parquet',
      ).read().ok;
      final column = table.column('s')!;
      expect(column, isA<StringColumn>());
      expect(column.toList(), [
        for (var i = 0; i < 6000; i++) i % 5 == 0 ? null : 's${i % 97}',
      ]);
    });

    test('DELTA_LENGTH_BYTE_ARRAY, with nulls and non-ASCII text', () async {
      final table = await ParquetReader(
        'test/fixtures/strings_v2.parquet',
      ).read().ok;
      final column = table.column('s')!;
      expect(column, isA<StringColumn>());
      expect(column.toList(), [
        for (var i = 0; i < 5000; i++)
          i % 6 == 0
              ? null
              : i % 4 == 0
              ? 'n\u00e4me-$i'
              : 'name-${i * 7919}',
      ]);
    });

    test('PLAIN, from our writer', () async {
      final path = '${dir.path}/plain.parquet';
      final values = [
        for (var i = 0; i < 3000; i++)
          i % 7 == 0 ? null : (i.isEven ? 'v$i' : '\u65e5\u672c$i'),
      ];
      await ParquetWriter(path)
          .write(
            Table(
              name: 't',
              columns: [
                ListColumn<String>(
                  name: 's',
                  dataType: DataType.string,
                  data: values,
                ),
              ],
              schema: Schema(
                fields: [Field(name: 's', dataType: DataType.string)],
              ),
            ),
          )
          .ok;
      final column = (await ParquetReader(path).read().ok).column('s')!;
      expect(column, isA<StringColumn>());
      expect(column.toList(), values);
    });

    test('malformed UTF-8 reads as replacement characters', () async {
      final path = '${dir.path}/malformed.parquet';
      await ParquetWriter(
            path,
            options: const ParquetWriterOptions(
              compression: ParquetCompression.none,
              writeChecksums: false,
            ),
          )
          .write(
            Table(
              name: 't',
              columns: [
                ListColumn<String>(
                  name: 's',
                  dataType: DataType.string,
                  data: ['ok', 'QQQQ', null],
                ),
              ],
              schema: Schema(
                fields: [Field(name: 's', dataType: DataType.string)],
              ),
            ),
          )
          .ok;
      // Overwrite every copy of QQQQ (the page and the statistics) with an
      // invalid lead byte followed by ASCII.
      final bytes = File(path).readAsBytesSync();
      final marker = 'QQQQ'.codeUnits;
      for (var i = 0; i + marker.length <= bytes.length; i++) {
        var match = true;
        for (var j = 0; j < marker.length; j++) {
          if (bytes[i + j] != marker[j]) match = false;
        }
        if (match) bytes[i] = 0xff;
      }
      File(path).writeAsBytesSync(bytes);
      final column = (await ParquetReader(path).read().ok).column('s')!;
      expect(column, isA<StringColumn>());
      expect(column.toList(), ['ok', '\ufffdQQQ', null]);
      final batches = await ParquetReader(
        path,
      ).readBatches(batchSize: 2).ok.toList();
      final parts = [for (final b in batches) b.column('s')!];
      expect(parts, everyElement(isA<StringColumn>()));
      expect(
        [for (final part in parts) ...part.toList()],
        ['ok', '\ufffdQQQ', null],
      );
    });
  });

  group('row groups decoded in parallel', () {
    late Directory dir;
    late String path;

    setUpAll(() async {
      dir = Directory.systemTemp.createTempSync('parallel_');
      path = '${dir.path}/groups.parquet';
      // 9 row groups of 1000 rows, with nulls, text and doubles.
      await ParquetWriter(
            path,
            options: const ParquetWriterOptions(rowGroupSize: 1000),
          )
          .write(
            Table.fromRows('t', [
              for (var i = 0; i < 9000; i++)
                {
                  'id': i,
                  'n': i % 7 == 0 ? null : i * 3,
                  's': i % 5 == 0 ? null : 's${i % 113}',
                  'd': i / 8,
                },
            ]),
          )
          .ok;
    });
    tearDownAll(() => dir.deleteSync(recursive: true));

    void same(Table actual, Table expected) {
      expect(actual.rowCount, expected.rowCount);
      expect(
        actual.schema.fields.map((f) => '${f.name} ${f.dataType}'),
        expected.schema.fields.map((f) => '${f.name} ${f.dataType}'),
      );
      for (final column in expected.columns) {
        expect(
          actual.column(column.name)!.toList(),
          column.toList(),
          reason: column.name,
        );
      }
    }

    test('match the serial read in order and content', () async {
      final serial = await ParquetReader(path).read().ok;
      for (final concurrency in [2, 4, 16]) {
        same(
          await ParquetReader(path).read(concurrency: concurrency).ok,
          serial,
        );
      }
    });

    test('honor projection and row-group pruning', () async {
      final options = ParquetReaderOptions(
        columns: ['s', 'id'],
        filter: RowFilter.compare('id', FilterOp.ge, 6500),
      );
      final serial = await ParquetReader(path, options: options).read().ok;
      expect(serial.rowCount, 3000);
      same(
        await ParquetReader(path, options: options).read(concurrency: 4).ok,
        serial,
      );
    });

    test('stop at the row cap like the serial read', () async {
      await expectLater(
        ParquetReader(path).read(maxRows: 2500, concurrency: 4),
        failsWith(Kind.limit),
      );
    });
  });

  group('nested assembly and caps', () {
    test('a nested read over maxBytes is a limit', () async {
      final file = Directory(
        'test/fixtures/delta/rs_nested',
      ).listSync().firstWhere((f) => f.path.endsWith('.parquet')).path;
      await expectLater(
        ParquetReader(file).read(maxBytes: 10),
        failsWith(Kind.limit, contains('maxBytes=10')),
      );
    });

    test('a record that starts mid-list is corrupt', () {
      expect(
        () => ColumnReader.assemble([1], [1], [1], 1, false),
        throwsA(isA<FormatException>()),
      );
      expect(ColumnReader.assemble([1, 2, 3], [0, 1, 0], [1, 1, 1], 1, false), [
        [1, 2],
        [3],
      ]);
    });
  });
}
