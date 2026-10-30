import 'dart:math' as math;

import 'failure.dart';
import 'result.dart';

/// Default safety cap (in rows) applied by the eager, fully-materializing read
/// APIs (`SqlEngine.execute`, `Dataset.read`, `JsonReader.readFile`,
/// `ArrowReader.read`). These methods stop reading and fail with [Kind.limit]
/// once the result would exceed this many rows, so an accidental full load can't OOM the
/// process. Pass `maxRows: null` to opt out (unbounded), or a streaming API
/// (`executeStream` / `readBatches`) for bounded-memory iteration.
const int defaultMaxMaterializedRows = 10000000; // 10M

/// Default safety cap (in bytes) applied by the same eager read APIs. Row counts
/// can't bound memory (a row may be a long string, a binary blob, or a deep
/// nested value), so the readers also stop with [Kind.limit] once the
/// accumulated in-memory size would exceed this. Pass `maxBytes: null` to opt out.
///
/// Byte accounting uses `Table.memoryUsage`, which counts RAW buffer bytes only;
/// it excludes List/Map/String object overhead and the transient duplication
/// while the input batches and the concatenated result briefly coexist. To keep
/// `maxBytes` an honest ceiling of true peak memory, the eager readers compare
/// against `memoryUsage * eagerReadOverheadFactor` rather than the raw figure.
/// Spill/budget decisions deliberately use the RAW `memoryUsage` (unchanged), so
/// this factor does not shift any spill threshold.
const int defaultMaxMaterializedBytes = 1 << 30; // 1 GiB

/// Multiplier applied to `Table.memoryUsage` ONLY at the eager-read byte-cap
/// comparison (see [defaultMaxMaterializedBytes]): ~1.3–1.5× for String/List/Map
/// object overhead composed with ~1.5× for the transient concat duplication,
/// rounded to a conservative 2.0. Applied once, at the cap check, and never
/// folded into `memoryUsage` itself (which would destabilize spill thresholds).
const double eagerReadOverheadFactor = 2.0;

/// Whether an eager read's accumulated raw [bytes] exceeds [cap] once the
/// [eagerReadOverheadFactor] is applied. Single source of truth for every
/// eager-read byte cap, so the factor is applied the same way in every format.
/// [cap] null = no cap.
bool overByteCap(int bytes, int? cap) =>
    cap != null && bytes * eagerReadOverheadFactor > cap;

/// Enforce BOTH eager-read caps in one place: throw a [Failure] of [Kind.limit]
/// once the PROSPECTIVE totals [rows]/[bytes] (including the batch about to be
/// added) would exceed [maxRows] or the [maxBytes] byte cap (via
/// [overByteCap]). [label] names the reader in the message (e.g.
/// `'Parquet result'`, `'dataset'`). Every eager read (Parquet/Arrow/CSV/JSON/
/// Delta/Iceberg/Dataset/SQL) shares this so the row check and both messages
/// can't drift per format.
void enforceMaterializationCaps(
  int rows,
  int bytes,
  int? maxRows,
  int? maxBytes,
  String label,
) {
  if (maxRows != null && rows > maxRows) {
    throw Failure.of(
      Kind.limit,
      '$label exceeds maxRows=$maxRows; pass maxRows: null to allow it, or use a '
      'streaming/batched API (readBatches / executeStream) for bounded memory',
    );
  }
  if (overByteCap(bytes, maxBytes)) {
    throw Failure.of(
      Kind.limit,
      '$label exceeds maxBytes=$maxBytes; pass maxBytes: null to allow it, or use '
      'a streaming/batched API (readBatches / executeStream) for bounded memory',
    );
  }
}

/// The smaller of two caps, null being none.
int? tighter(int? cap, int? other) =>
    cap == null ? other : (other == null ? cap : math.min(cap, other));

/// Ceiling on a file's metadata footer (Parquet FileMetaData / Arrow IPC
/// footer). The footer length is otherwise bounded only by the file size, so a
/// crafted file with a huge body can declare a multi-GB footer that passes the
/// "smaller than the file" check and drives an enormous metadata read + parse.
/// Real footers are tiny; 128 MiB is far above any legitimate schema.
const int defaultMaxFooterBytes = 128 << 20;

/// Ceilings on Arrow IPC schema parsing, checked BEFORE the recursive
/// FlatBuffers walk allocates `Field` objects. The footer byte cap bounds total
/// input, but within a legal footer a deeply-nested schema would StackOverflow
/// the recursive parser and a very wide one would allocate millions of `Field`s
/// before any rejection. Real schemas are shallow and modest; these mirror the
/// Avro decoder's depth guard.
const int defaultMaxSchemaDepth = 100;
const int defaultMaxSchemaFields = 1 << 20; // ~1M fields

/// Ceiling on a single Delta log file (a commit JSON, or one of this library's
/// `NN.checkpoint.json` checkpoints) admitted to replay. A checkpoint compacts
/// the WHOLE table state into one file,
/// so it is legitimately large: 1 GiB bounds a crafted oversize file without
/// rejecting a real checkpoint. Deliberately larger than
/// [defaultMaxTableMetadataBytes]: a Delta checkpoint is a different artifact from
/// an Iceberg metadata.json.
const int defaultMaxDeltaLogBytes = 1 << 30; // 1 GiB

/// Ceiling on an Iceberg `metadata.json`. Unlike a Delta checkpoint it holds only
/// schema + snapshot pointers (never data), so it stays small even for large
/// tables; 128 MiB is far above any real table's metadata while still blocking a
/// metadata bomb before allocation. READ-side only; it does not bound a writer.
const int defaultMaxTableMetadataBytes = 128 << 20; // 128 MiB

/// Ceiling on a single Delta deletion-vector `.bin` payload read into memory.
///
/// The `.bin` header declares a payload length that is otherwise bounded only by
/// the file size, so a genuinely large (or maliciously large-and-materialized)
/// vector would slurp multiple GiB into one buffer BEFORE the Roaring
/// cardinality budget (which caps the DECODED bitmap, not the compressed
/// payload) can help. A real DV over even a huge data file is well under this;
/// 256 MiB blocks the resource-exhaustion case before allocation.
const int defaultMaxDeletionVectorBytes = 256 << 20; // 256 MiB

/// Ceiling on the number of distinct partitions ONE write may create.
///
/// A partitioned write emits one data file and one manifest entry per distinct
/// key, so partitioning on a high-cardinality column (a timestamp, an id) turns a
/// single write into millions of tiny files plus a manifest held whole in memory:
/// the classic small-file catastrophe, and unrecoverable without a rewrite.
/// Almost always a mis-chosen partition column rather than a real intent, so it
/// fails loud and early. Pass `maxPartitions: null` to opt out.
const int defaultMaxPartitionsPerWrite = 10000;
