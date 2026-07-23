@Tags(['interop'])
library;

import 'dart:convert';
import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:test/test.dart';

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
        await ParquetWriter(path).write(nested());

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
    },
    skip: pyarrow() ? false : 'pyarrow not installed',
  );
}
