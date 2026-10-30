@Tags(['interop'])
library;

import 'dart:convert';
import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:test/test.dart';

import '../support/results.dart';

/// cross-engine validation of nested Parquet WRITES. Prior
/// rounds only self-round-tripped nested output (libbigdata read its own bytes),
/// so a subtly wrong Dremel rep/def computation could pass while pyarrow
/// mis-assembled the data. This reads our output with pyarrow and compares.
/// Skipped when pyarrow isn't installed (e.g. CI without Python).
void main() {
  bool pyarrow() {
    try {
      final r = Process.runSync('python3', ['-c', 'import pyarrow']);
      return r.exitCode == 0;
    } catch (_) {
      return false;
    }
  }

  group(
    'nested Parquet writes read back via pyarrow',
    () {
      late Directory tmp;
      setUp(() => tmp = Directory.systemTemp.createTempSync('pqinterop_'));
      tearDown(() => tmp.deleteSync(recursive: true));

      Table nested() => Table(
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

    test('pyarrow reassembles list / struct / map identically', () async {
      final path = '${tmp.path}/n.parquet';
      await ParquetWriter(path).write(nested()).ok;

        // Read with pyarrow and emit normalized JSON (maps → dict, [] for empty).
        const script = '''
import sys, json
import pyarrow as pa, pyarrow.parquet as pq
t = pq.read_table(sys.argv[1])
maps = {f.name for f in t.schema if pa.types.is_map(f.type)}
rows = t.to_pylist()
def norm(name, v):
    if name in maps and v is not None:
        return {str(k): val for k, val in v}
    return v
print(json.dumps([{k: norm(k, v) for k, v in r.items()} for r in rows]))
''';
        final r = Process.runSync('python3', ['-c', script, path]);
        expect(r.exitCode, 0, reason: r.stderr.toString());
        final rows = (jsonDecode(r.stdout as String) as List)
            .cast<Map<String, dynamic>>();

        expect(rows.map((e) => e['l']), [
          [1, 2, 3],
          [4],
          [],
          [5, 6],
        ]);
        expect(rows[0]['s'], {'a': 1, 'b': 'x'});
        expect(rows[2]['s'], {'a': 3, 'b': null});
        expect(rows.map((e) => e['m']), [
          {'k1': 1, 'k2': 2},
          {},
          {'z': 9},
          {'p': 7},
        ]);
      });

    test('pyarrow reads a map whose key field is nullable', () async {
      // Field defaults to nullable; map keys are still written REQUIRED,
      // since pyarrow refuses optional ones.
      final path = '${tmp.path}/m.parquet';
      final schema = Schema(
        fields: [
          Field(
            name: 'm',
            dataType: DataType.map,
            children: [
              Field(
                name: 'entries',
                dataType: DataType.struct,
                nullable: false,
                children: [
                  Field(name: 'key', dataType: DataType.string),
                  Field(name: 'value', dataType: DataType.int64),
                ],
              ),
            ],
          ),
        ],
      );
      await ParquetWriter(path)
          .write(
            Table(
              name: 't',
              schema: schema,
              columns: [
                ListColumn<Map<String, int>?>(
                  name: 'm',
                  dataType: DataType.map,
                  data: [
                    {'a': 1},
                    null,
                  ],
                ),
              ],
            ),
          )
          .ok;
      final r = Process.runSync('python3', [
        '-c',
        'import sys, pyarrow.parquet as pq; '
            'print(pq.read_table(sys.argv[1]).column("m").to_pylist())',
        path,
      ]);
      expect(r.exitCode, 0, reason: r.stderr.toString());
      expect((r.stdout as String).trim(), "[[('a', 1)], None]");
    });

    test('pyarrow sees the logical types of scalar columns', () async {
      final path = '${tmp.path}/logical.parquet';
      await ParquetWriter(path)
          .write(
            Table(
              name: 't',
              schema: Schema(
                fields: [
                  Field(name: 'ts', dataType: DataType.timestamp),
                  Field(name: 'day', dataType: DataType.date),
                  Field(name: 's', dataType: DataType.string),
                  Field(
                    name: 'd',
                    dataType: DataType.decimal,
                    precision: 10,
                    scale: 2,
                  ),
                ],
              ),
              columns: [
                ListColumn<DateTime>(
                  name: 'ts',
                  dataType: DataType.timestamp,
                  data: [DateTime.utc(2024, 1, 2, 3, 4, 5)],
                ),
                ListColumn<DateTime>(
                  name: 'day',
                  dataType: DataType.date,
                  data: [DateTime.utc(2024, 1, 2)],
                ),
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
              ],
            ),
          )
          .ok;
      // pyarrow derives the Arrow types from the legacy converted types
      // alone; the raw Parquet logical type shows whether the file itself
      // carried one.
      const script = '''
import sys, json
import pyarrow.parquet as pq
schema = pq.read_schema(sys.argv[1])
raw = pq.ParquetFile(sys.argv[1]).metadata.schema
print(json.dumps({
  'types': {f.name: str(f.type) for f in schema},
  'raw': str(raw.column(0).logical_type),
}))
''';
      final r = Process.runSync('python3', ['-c', script, path]);
      expect(r.exitCode, 0, reason: r.stderr.toString());
      final report = jsonDecode(r.stdout as String) as Map<String, dynamic>;
      expect(report['raw'], contains('is_from_converted_type=false'));
      final types = report['types'] as Map<String, dynamic>;
      expect(types['ts'], 'timestamp[us, tz=UTC]');
      expect(types['day'], 'date32[day]');
      expect(types['s'], 'string');
      expect(types['d'], 'decimal128(10, 2)');
    });
  }, skip: pyarrow() ? false : 'pyarrow not installed');
}
