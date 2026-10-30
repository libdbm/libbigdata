// Dev benchmark tooling; doc comments use generics like List<int> as prose.
// ignore_for_file: unintended_html_in_doc_comment
import 'dart:io';
import 'dart:typed_data';

import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/core/column.dart' show NumericVector;
// CompressionCodec is not re-exported by the barrel.

import '../results.dart';

/// libbigdata read/write Parquet benchmark.
///
/// Read:  dart bench read  <dataset> <path>        <reps>
/// Write: dart bench write <dataset> <csv> <codec> <reps>
///
/// Emits one machine-parseable line per repetition:
///   RESULT libbigdata <op> <dataset> <codec> <ms> <rows> <bytes> <intsum> <floatsum>
Future<void> main(List<String> args) async {
  final op = args[0];
  if (op == 'read') {
    await _read(args[1], args[2], int.parse(args[3]));
  } else if (op == 'write') {
    await _write(args[1], args[2], args[3], int.parse(args[4]));
  } else {
    stderr.writeln('unknown op: $op');
    exit(2);
  }
}

Future<void> _read(String dataset, String path, int reps) async {
  final bytes = File(path).lengthSync();
  for (int r = 0; r < reps; r++) {
    final sw = Stopwatch()..start();
    final table = await ParquetReader(path).read().ok;
    final agg = dataset == 'flights' ? _aggFlights(table) : _aggGen(table);
    sw.stop();
    _emit('read', dataset, 'none', sw, table.rowCount, bytes, agg);
  }
}

Future<void> _write(String dataset, String csv, String codec, int reps) async {
  final out = 'out_libbigdata_$codec.parquet';
  final compression = codec == 'zstd'
      ? ParquetCompression.zstd
      : codec == 'gzip'
      ? ParquetCompression.gzip
      : ParquetCompression.snappy;
  for (int r = 0; r < reps; r++) {
    final sw = Stopwatch()..start();
    final table = await CsvReader().readFile(csv).ok;
    await ParquetWriter(
      out,
      options: ParquetWriterOptions(compression: compression),
    ).write(table).ok;
    sw.stop();
    _emit(
      'write',
      dataset,
      codec,
      sw,
      table.rowCount,
      File(out).lengthSync(),
      const [0, 0],
    );
  }
}

/// Returns [intChecksum, floatChecksum]. Touches every column so the whole
/// file is decoded.
List<num> _aggGen(Table table) {
  final ints =
      _sum(table, 'id') +
      _sum(table, 'quantity') +
      _sum(table, 'ts') +
      _strLen(table, 'category') +
      _strLen(table, 'name') +
      _countTrue(table, 'active') +
      _countNonNull(table, 'score');
  final floats = _sum(table, 'amount') + _sum(table, 'score');
  return [ints, floats];
}

List<num> _aggFlights(Table table) {
  final ints =
      _sum(table, 'DEP_DELAY') +
      _sum(table, 'ARR_DELAY') +
      _sum(table, 'AIR_TIME') +
      _sum(table, 'DISTANCE') +
      _countNonNull(table, 'FL_DATE') +
      table.rowCount;
  final floats = _sum(table, 'DEP_TIME') + _sum(table, 'ARR_TIME');
  return [ints, floats];
}

num _sum(Table table, String name) {
  final col = table.column(name)!;
  // Fast path: iterate the typed buffer directly, skipping nulls, no boxing.
  if (col is NumericVector) {
    final buf = col.raw;
    final bitmap = col.nulls;
    bool present(int i) =>
        bitmap == null || (bitmap[i >> 3] & (1 << (i & 7))) == 0;
    num total = 0;
    if (buf is Int32List) {
      for (int i = 0; i < buf.length; i++) {
        if (present(i)) total += buf[i];
      }
    } else if (buf is Int64List) {
      for (int i = 0; i < buf.length; i++) {
        if (present(i)) total += buf[i];
      }
    } else if (buf is Float32List) {
      for (int i = 0; i < buf.length; i++) {
        if (present(i)) total += buf[i];
      }
    } else if (buf is Float64List) {
      for (int i = 0; i < buf.length; i++) {
        if (present(i)) total += buf[i];
      }
    } else if (buf is Int16List) {
      // libbigdata narrows INT32 to int16 on read when the values fit (an
      // intended representation choice), so the typed fast-path must handle it.
      for (int i = 0; i < buf.length; i++) {
        if (present(i)) total += buf[i];
      }
    } else if (buf is Int8List) {
      for (int i = 0; i < buf.length; i++) {
        if (present(i)) total += buf[i];
      }
    }
    return total;
  }
  num total = 0;
  for (final v in col.toList()) {
    if (v is num) total += v;
  }
  return total;
}

int _strLen(Table table, String name) {
  int total = 0;
  for (final v in table.column(name)!.toList()) {
    if (v is String) total += v.length;
  }
  return total;
}

int _countTrue(Table table, String name) {
  int total = 0;
  for (final v in table.column(name)!.toList()) {
    if (v == true) total++;
  }
  return total;
}

int _countNonNull(Table table, String name) {
  final col = table.column(name)!;
  if (col is NumericVector) {
    final bitmap = col.nulls;
    if (bitmap == null) return col.length;
    int total = 0;
    for (int i = 0; i < col.length; i++) {
      if ((bitmap[i >> 3] & (1 << (i & 7))) == 0) total++;
    }
    return total;
  }
  int total = 0;
  for (final v in col.toList()) {
    if (v != null) total++;
  }
  return total;
}

void _emit(
  String op,
  String dataset,
  String codec,
  Stopwatch sw,
  int rows,
  int bytes,
  List<num> agg,
) {
  final ms = sw.elapsedMicroseconds / 1000.0;
  print(
    'RESULT libbigdata $op $dataset $codec '
    '${ms.toStringAsFixed(3)} $rows $bytes ${agg[0]} ${agg[1]}',
  );
}
