@Tags(['interop'])
library;

import 'dart:io';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/formats/arrow/arrow_reader.dart';
import 'package:test/test.dart';

import '../support/results.dart';

/// Arrow IPC files exchanged with pyarrow in both directions. Tagged
/// `interop` (needs python3 with pyarrow); run with
/// `dart test -t interop --run-skipped`.
void main() {
  bool pyarrow() {
    try {
      return Process.runSync('python3', ['-c', 'import pyarrow']).exitCode == 0;
    } catch (_) {
      return false;
    }
  }

  group('Arrow IPC with pyarrow', () {
    late Directory dir;
    setUp(() => dir = Directory.systemTemp.createTempSync('arrow_interop_'));
    tearDown(() => dir.deleteSync(recursive: true));

    String python(String script, List<String> args) {
      final r = Process.runSync('python3', ['-c', script, ...args]);
      expect(r.exitCode, 0, reason: '${r.stderr}');
      return (r.stdout as String).trim();
    }

    test('pyarrow reads our dates and timestamps', () async {
      final path = '${dir.path}/ours.arrow';
      final engine = SqlEngine.open().ok;
      addTearDown(engine.dispose);
      final table = await engine
          .execute(
            "SELECT DATE '2020-01-01' AS d, "
            "TIMESTAMP '2020-01-01 10:30:05' AS ts UNION ALL "
            "SELECT DATE '1969-12-31' AS d, "
            "TIMESTAMP '1969-12-31 23:59:59' AS ts",
          )
          .ok;
      await DataFrame.fromTable(table).writeArrow(path).ok;
      expect(
        python(
          '''
import sys, pyarrow.ipc as ipc
t = ipc.open_file(sys.argv[1]).read_all()
print(t.schema.field("d").type, [str(v) for v in t.column("d").to_pylist()])
print([v.isoformat() for v in t.column("ts").to_pylist()])
''',
          [path],
        ),
        "date32[day] ['2020-01-01', '1969-12-31']\n"
        "['2020-01-01T10:30:05', '1969-12-31T23:59:59']",
      );
    });

    test('we read pyarrow date64 and every timestamp unit', () async {
      final path = '${dir.path}/theirs.arrow';
      python(
        '''
import sys, datetime, pyarrow as pa, pyarrow.ipc as ipc
days = [datetime.date(2020, 1, 1), datetime.date(1969, 12, 31)]
at = [datetime.datetime(2020, 1, 1, 10, 30, 5), datetime.datetime(1969, 12, 31, 23, 59, 59)]
t = pa.table({
  "d32": pa.array(days, pa.date32()),
  "d64": pa.array(days, pa.date64()),
  "s": pa.array(at, pa.timestamp("s")),
  "ms": pa.array(at, pa.timestamp("ms")),
  "us": pa.array(at, pa.timestamp("us")),
  "ns": pa.array(at, pa.timestamp("ns")),
})
with ipc.new_file(sys.argv[1], t.schema) as w:
  w.write_table(t)
''',
        [path],
      );
      final table = await ArrowReader(path).read();
      final days = [DateTime.utc(2020, 1, 1), DateTime.utc(1969, 12, 31)];
      final at = [
        DateTime.utc(2020, 1, 1, 10, 30, 5),
        DateTime.utc(1969, 12, 31, 23, 59, 59),
      ];
      expect(table.column('d32')!.toList(), days);
      expect(table.column('d64')!.toList(), days);
      for (final unit in ['s', 'ms', 'us', 'ns']) {
        expect(table.column(unit)!.toList(), at, reason: unit);
      }
    });

    test('a dictionary-encoded column is unsupported, not corrupt', () async {
      final path = '${dir.path}/dictionary.arrow';
      python(
        '''
import sys, pyarrow as pa, pyarrow.ipc as ipc
t = pa.table({"c": pa.array(["a", "b", "a"]).dictionary_encode()})
with ipc.new_file(sys.argv[1], t.schema) as w:
  w.write_table(t)
''',
        [path],
      );
      await expectLater(
        DataFrameIO.readArrow(path),
        failsWith(Kind.unsupported, contains('dictionary')),
      );
    });

    test('pyarrow reads our IPC stream, compressed or not', () async {
      final frame = DataFrame.fromTable(
        Table.fromRows('t', [
          for (var i = 0; i < 5; i++) {'id': i, 'name': 'n$i'},
        ]),
      );
      for (final compression in ArrowCompression.values) {
        final path = '${dir.path}/ours.${compression.name}.arrows';
        await frame
            .writeArrowStream(path, batchSize: 2, compression: compression)
            .ok;
        expect(
          python(
            '''
import sys, pyarrow.ipc as ipc
reader = ipc.open_stream(sys.argv[1])
batches = list(reader)
print([b.num_rows for b in batches], sum(b.num_rows for b in batches))
print(batches[-1].column(1).to_pylist())
''',
            [path],
          ),
          "[2, 2, 1] 5\n['n4']",
          reason: compression.name,
        );
      }
    });

    test('we read pyarrow\'s IPC stream, compressed or not', () async {
      for (final compression in ['none', 'lz4', 'zstd']) {
        final path = '${dir.path}/theirs.$compression.arrows';
        python(
          '''
import sys, pyarrow as pa, pyarrow.ipc as ipc
table = pa.table({"id": [1, 2, 3, 4], "s": ["a", None, "c", "d"]})
codec = None if sys.argv[2] == "none" else sys.argv[2]
options = ipc.IpcWriteOptions(compression=codec)
with ipc.new_stream(sys.argv[1], table.schema, options=options) as writer:
    for batch in table.to_batches(max_chunksize=3):
        writer.write_batch(batch)
''',
          [path, compression],
        );
        final frame = await DataFrameIO.readArrowStream(path).ok;
        final rows = (await frame.execute().ok).toRows();
        expect(rows.map((r) => r['id']), [1, 2, 3, 4], reason: compression);
        expect(rows.map((r) => r['s']), ['a', null, 'c', 'd']);
      }
    });
  }, skip: pyarrow() ? false : 'pyarrow not installed');
}
