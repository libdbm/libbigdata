import 'limits.dart';
import 'row_filter.dart';
import 'schema.dart';
import 'table.dart';

/// A format-neutral, lazy scan over a file source. The SQL layer depends on this
/// contract instead of concrete format readers, so adding a format means
/// implementing [Scan] (+ registering it in the formats-layer factory) rather
/// than editing query planning.
///
/// Capabilities are best-effort: a reader applies whichever of [projection] /
/// [filter] it supports natively (e.g. Parquet pushes both down; CSV selects
/// columns; JSON does neither) — the query plan still applies the exact filter
/// and projection on top, so correctness never depends on pushdown.
abstract class Scan {
  /// The source's full (unprojected) schema, discovered as cheaply as the format
  /// allows (Parquet footer; a peeked first batch for CSV/JSON). Null when the
  /// source is empty / schema can't be discovered without a full read.
  Future<Schema?> readSchema();

  /// Stream the source one [Table] batch at a time (bounded memory), optionally
  /// pruning by [filter] (stats-based) and reading only [projection] columns.
  Stream<Table> batches({
    int size,
    List<String>? projection,
    RowFilter? filter,
  });

  /// Eagerly read the whole source into one [Table], honoring the format's
  /// native materialization caps ([maxRows] / [maxBytes]; pass null to opt out).
  /// [filter] / [projection] are applied best-effort (Parquet pushes both down);
  /// the caller stays responsible for exact filtering as with [batches].
  Future<Table> read({
    int? maxRows = defaultMaxMaterializedRows,
    int? maxBytes = defaultMaxMaterializedBytes,
    List<String>? projection,
    RowFilter? filter,
  });
}
