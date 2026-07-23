// Dev tooling; doc comments use generics like List<int> as prose.
// ignore_for_file: unintended_html_in_doc_comment
import 'dart:io';

/// Deterministic CSV generator used as the shared input for the write benchmark
/// and (after conversion to parquet) the read benchmark.
///
/// Usage: dart run benchmark/gen_data.dart <rows> <out.csv>
///
/// A representative mixed-type dataset with realistic (random) distributions so
/// Parquet's dictionary/delta encodings don't collapse it to a few KB:
///   id       int64   random, high-cardinality (no delta benefit)
///   amount   double  random continuous
///   category string  12 distinct (low-cardinality -> dictionary)
///   name     string  ~100k distinct (high-cardinality)
///   quantity int64   random small range
///   active   bool    random
///   ts       int64   random within a ~1y window (not sequential)
///   score    double  ~12% null (exercises null handling)
///
/// All integer aggregate sums stay below 2^53 so they are exact across engines.
void main(List<String> args) {
  final rows = args.isNotEmpty ? int.parse(args[0]) : 1000000;
  final out = args.length > 1 ? args[1] : 'gen.csv';

  const categories = [
    'alpha',
    'bravo',
    'charlie',
    'delta',
    'echo',
    'foxtrot',
    'golf',
    'hotel',
    'india',
    'juliet',
    'kilo',
    'lima',
  ];

  // Seeded xorshift32 -> reproducible files across runs (engines must read the
  // identical dataset). Random()/Date.now() are intentionally avoided.
  int s = 0x2545F491;
  int rnd() {
    s ^= (s << 13) & 0xFFFFFFFF;
    s ^= s >> 17;
    s ^= (s << 5) & 0xFFFFFFFF;
    return s & 0xFFFFFFFF;
  }

  final sink = File(out).openWrite();
  sink.writeln('id,amount,category,name,quantity,active,ts,score');
  final buffer = StringBuffer();
  for (int i = 0; i < rows; i++) {
    final id = 5000000000 + rnd() % 1000000000; // int64, high-card
    final amount = (rnd() % 1000000) / 100.0; // 0.00 .. 9999.99
    final category = categories[rnd() % categories.length];
    final name = 'name_${rnd() % 100000}'; // ~100k distinct
    final quantity = 1 + rnd() % 1000;
    final active = (rnd() & 1) == 0 ? 'true' : 'false';
    final ts = 1600000000 + rnd() % 31536000; // within ~1 year (seconds)
    final score = (rnd() % 100) >= 12
        ? ((rnd() % 10000) / 100.0).toString()
        : '';
    buffer.writeln('$id,$amount,$category,$name,$quantity,$active,$ts,$score');
    if (buffer.length > 1 << 20) {
      sink.write(buffer);
      buffer.clear();
    }
  }
  sink.write(buffer);
  sink.close();
}
