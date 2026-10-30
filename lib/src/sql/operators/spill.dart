import 'dart:io';
import 'dart:typed_data';

import '../../core/data_type.dart';
import '../../core/keys.dart' show partitionOf;
import '../../core/schema.dart';
import '../../core/table.dart';
import '../../formats/arrow/arrow_reader.dart';
import '../../formats/arrow/arrow_writer.dart';
import '../../formats/parquet/parquet_reader.dart';
import '../../formats/parquet/parquet_writer.dart';
import '../exec/kernels.dart';
import '../execution/exec_core.dart';
import '../../core/ids.dart' as ids;
import '../../core/failure.dart';
import '../../core/result.dart';

/// On-disk spill files for blocking operators. A spill run/partition is a single
/// file, unique-named and deleted when the owning iterator closes.
///
/// Two formats:
/// - [SpillFormat.parquet] (default): lossless for all column types, read back
///   lazily (one row group at a time): required where files are co-read or
///   streamed (sort k-way merge, join probe side).
/// - [SpillFormat.arrow]: cheaper to write (no compression/stats) with zero-copy
///   numeric decode, written incrementally (bounded memory) via [ArrowStreamWriter]
///   and read back one record batch at a time. Used for files that are read
///   fully (aggregate partitions). Falls back to Parquet when the schema has a
///   type Arrow can't encode.
enum SpillFormat { parquet, arrow }

final Set<DataType> _arrowTypes = {
  DataType.boolean,
  DataType.int8,
  DataType.int16,
  DataType.int32,
  DataType.int64,
  DataType.float32,
  DataType.float64,
  DataType.string,
  DataType.binary,
  DataType.date,
  DataType.timestamp,
  DataType.decimal,
  DataType.null_,
};

bool _arrowSupported(Schema schema) =>
    schema.fields.every((f) => _arrowTypes.contains(f.dataType));

/// Prefix of a per-[SqlEngine]-session spill subdirectory. Each engine spills
/// under its own `<base>/<sessionPrefix><token>/` so no engine's sweep can
/// ever touch another live session's runs. (Deliberately does NOT contain the
/// `spill-` file prefix, so a directory listing that greps for leaked `spill-*`
/// files doesn't match the session dir itself.)
const sessionPrefix = 'libbigdata-session-';

/// Where a [SqlEngine] or DataFrame execution with no spill directory of its
/// own keeps its sessions: a dedicated subdirectory of the system temp, so
/// [sweepOrphanedSpills] lists only this library's sessions.
String get defaultSpillBase => '${Directory.systemTemp.path}/libbigdata-spill';

/// A new session directory under [base]. Nothing is created: an operator makes
/// the directory on its first spill, and the marker with it.
String newSpillSession(String base) => '$base/$sessionPrefix${ids.token()}';

/// The per-session live-marker (a sibling of the spill files) whose mtime is
/// kept fresh while a query is active, so a concurrent engine's sweep treats the
/// session as live and never reclaims it.
String _sessionMarker(String spillFilePath) =>
    '${File(spillFilePath).parent.path}/.live';

/// Best-effort refresh of the session marker so a sibling engine's age-checked
/// sweep sees this session as live. RECREATES the marker if it is missing: a
/// prior sweep may have reclaimed the (then-idle) session dir, and an operator
/// re-creates the spill dir on demand WITHOUT the marker; without this a live
/// session that spills again would have no `.live` and be deletable mid-query.
/// Sync + swallow errors (hot path, advisory).
void _touchSessionMarker(String spillFilePath) {
  try {
    final f = File(_sessionMarker(spillFilePath));
    if (f.existsSync()) {
      f.setLastModifiedSync(DateTime.now());
    } else {
      f.createSync(recursive: true); // fresh marker → mtime is now
    }
  } catch (_) {}
}

/// Best-effort reclaim of crash-orphaned spill SESSIONS in [directory].
/// Graceful cleanup runs via operator `close()`/[SpillWriter.abort], but a
/// SIGKILL exits before any `finally`, leaving a whole `<sessionPrefix>*`
/// subdir behind.
///
/// SESSION-SCOPED, not per-file: it only reclaims a session subdir whose `.live`
/// marker is missing or older than [ttl]: a marker an active engine keeps fresh
/// on every spill write/read. So a live sibling engine's runs are never deleted
/// (the P0 that let a >[ttl] query lose a partition to another engine's sweep).
/// Every step is best-effort: a racing writer or a vanished dir never errors.
Future<void> sweepOrphanedSpills(
  String directory, {
  Duration ttl = const Duration(hours: 1),
}) async {
  try {
    final d = Directory(directory);
    if (!await d.exists()) return;
    final now = DateTime.now();
    await for (final e in d.list()) {
      if (e is! Directory) continue;
      final name = e.uri.pathSegments.where((s) => s.isNotEmpty).last;
      if (!name.startsWith(sessionPrefix)) continue;
      try {
        final marker = File('${e.path}/.live');
        final stale =
            !await marker.exists() ||
            now.difference((await marker.stat()).modified) > ttl;
        if (stale) await e.delete(recursive: true);
      } catch (_) {
        // Racing writer / permission / vanished: skip, never break startup.
      }
    }
  } catch (_) {
    // Dir vanished / not listable; nothing to reclaim.
  }
}

/// An opt-in ceiling on the COMBINED on-disk size of live spill files (across
/// every blocking operator sharing it). Disk-full mid-spill otherwise fails the
/// query with a `FileSystemException`; this fails fast and deterministically
/// before the disk fills. A true free-disk probe isn't portable in pure Dart, so
/// this caps what WE write instead. Same reserve/release shape as
/// [MemoryManager]; accounting uses each batch's in-memory size, a conservative
/// over-estimate of the (compressed) on-disk bytes.
///
/// CONTRACT (as with [MemoryManager]): isolate-private and synchronous.
/// [charge]/[release] read-modify-write [_used] with no `await` between, so on
/// Dart's single event loop they can't interleave: no lock needed. Do NOT share
/// one instance across isolates, and do NOT introduce an `await` into the
/// charge/release critical section.
final class SpillBudget {
  /// Max combined live spill bytes.
  final int cap;
  int _used = 0;
  SpillBudget(this.cap);

  /// Bytes currently held by live spill files.
  int get used => _used;

  /// Account [bytes] about to be spilled; throws a [Kind.limit] [Failure]
  /// (charging nothing) if it would exceed [cap].
  void charge(int bytes) {
    if (bytes <= 0) return;
    if (_used + bytes > cap) {
      throw Failure.of(
        Kind.limit,
        'spill exceeded maxSpillBytes=$cap (used $_used, +$bytes); raise '
        'maxSpillBytes or reduce the query',
      );
    }
    _used += bytes;
  }

  /// Return [bytes] when a spill file is deleted. Clamped at zero.
  void release(int bytes) {
    if (bytes <= 0) return;
    _used -= bytes;
    if (_used < 0) _used = 0;
  }

  /// Reconcile a prior charge to the true on-disk size once a file is finalized.
  /// A negative delta refunds the compression saving; a POSITIVE delta (the
  /// finalized file is larger than estimated) is reserved with the cap check and
  /// THROWS if it would cross [cap]: the caller must then delete the run rather
  /// than let the on-disk total silently exceed maxSpillBytes.
  void adjust(int delta) {
    if (delta < 0) {
      release(-delta);
    } else if (delta > 0) {
      charge(delta);
    }
  }
}

/// A finalized spill file; read it back one [RecordBatch] at a time. Both
/// formats read lazily: Parquet a row group at a time, Arrow a record batch at
/// a time.
class SpillRun {
  final String path;
  final Schema schema;

  /// Whether this run finalized an actual file. `false` = a legitimately empty
  /// run (the writer finished with nothing added). When `true` but the file is
  /// gone, the run was deleted after finalization: a corruption we must fail
  /// loud on rather than silently read as zero rows (a dropped partition).
  final bool hasFile;
  final SpillBudget? _budget;
  final int _bytes;
  bool _released = false;
  SpillRun(
    this.path,
    this.schema, {
    this.hasFile = true,
    SpillBudget? budget,
    int bytes = 0,
  }) : _budget = budget,
       _bytes = bytes;

  Stream<RecordBatch> read({int size = 8192}) async* {
    if (!hasFile) return; // legitimately empty run: no file was ever written
    if (!await File(path).exists()) {
      // A finalized run vanished (e.g. another session swept a shared spill
      // dir). Reading it as empty would silently drop a join/agg/sort/set-op
      // partition; fail loud instead.
      throw Failure.of(
        Kind.io,
        'Spill run "$path" was finalized but its file is missing; it was '
        'deleted after finalization (results would be silently wrong). Use a '
        'per-session spill directory.',
      );
    }
    _touchSessionMarker(path);
    final batches = path.endsWith('.arrow')
        ? ArrowReader(path).readBatches()
        : ParquetReader(path).readBatches(batchSize: size).unwrap();
    // The file stores columns under positional names (see SpillWriter.add);
    // give them back the operator's names and qualifiers.
    await for (final table in batches) {
      await pause();
      yield RecordBatch(
        columns: [
          for (final (index, column) in table.columns.indexed)
            column.withName(schema.fields[index].name),
        ],
        schema: schema,
        rowCount: table.rowCount,
      );
    }
  }

  Future<void> delete() async {
    final file = File(path);
    if (await file.exists()) await file.delete();
    if (!_released) {
      _released = true;
      _budget?.release(_bytes);
    }
  }
}

/// Route [batch]'s rows to [writers] by the hash of each row's [key]: rows
/// with equal keys go to the same writer (so each partition can be finished
/// on its own), and [salt] redistributes them on a recursive re-partition.
/// Each gathered batch carries [schema], by default the batch's own. The one
/// router for the aggregate, join and set-operation spills.
Future<void> route(
  RecordBatch batch,
  List<SpillWriter> writers,
  Object? Function(int row) key, {
  int salt = 0,
  Schema? schema,
}) async {
  final buckets = [for (var i = 0; i < writers.length; i++) <int>[]];
  for (var row = 0; row < batch.rowCount; row++) {
    buckets[partitionOf(key(row), writers.length, salt: salt)].add(row);
  }
  for (var part = 0; part < writers.length; part++) {
    if (buckets[part].isEmpty) continue;
    final index = Int32List.fromList(buckets[part]);
    await writers[part].add(
      RecordBatch(
        columns: [for (final c in batch.columns) Kernels.gather(c, index)],
        schema: schema ?? batch.schema,
        rowCount: index.length,
      ),
    );
  }
}

/// Appends record batches to a single spill file, then finalizes it.
class SpillWriter {
  final String path;
  final Schema schema;
  final ParquetWriter? _parquet;
  final ArrowStreamWriter? _arrow;
  final SpillBudget? _budget;
  bool _any = false;
  int _bytes = 0; // charged-but-not-yet-released spill bytes
  bool _owned = true; // false once finish() hands the bytes to a SpillRun

  SpillWriter._(
    this.path,
    this.schema,
    this._parquet,
    this._arrow,
    this._budget,
  );

  /// Construct a writer for a spill file in [directory]. [directory] MUST
  /// already exist: callers create it once (asynchronously) before their
  /// writer loop so this stays off the event loop on the hot spill path.
  /// [budget], when set, caps the combined on-disk spill size and fails fast
  /// when exceeded.
  factory SpillWriter(
    String directory,
    Schema schema, {
    SpillFormat format = SpillFormat.parquet,
    SpillBudget? budget,
  }) {
    // Spilled intermediates may carry nulls in any column across the stream, so
    // the file schema must be all-nullable: a footer frozen from a no-null first
    // batch would mark leaves required and then read back inconsistently.
    final nullable = _nullableSchema(schema);
    final useArrow = format == SpillFormat.arrow && _arrowSupported(nullable);
    final path =
        '$directory/spill-${ids.token()}.${useArrow ? 'arrow' : 'parquet'}';
    return SpillWriter._(
      path,
      nullable,
      useArrow ? null : ParquetWriter(path),
      useArrow ? ArrowStreamWriter(path, _positional(nullable)) : null,
      budget,
    );
  }

  /// [schema] with each field named by its position. A batch can hold two
  /// columns of the same name under different qualifiers (a join's `t.id` and
  /// `u.id`), which a file schema cannot; [SpillRun.read] restores the names.
  static Schema _positional(Schema schema) => Schema(
    fields: [
      for (final (index, field) in schema.fields.indexed)
        Field(
          name: 'c$index',
          dataType: field.dataType,
          nullable: field.nullable,
          precision: field.precision,
          scale: field.scale,
          children: field.children,
        ),
    ],
  );

  /// Widen every field to nullable (spilled data is always potentially-null).
  static Schema _nullableSchema(Schema schema) => Schema(
    fields: [
      for (final f in schema.fields)
        f.nullable ? f : f.copyWith(nullable: true),
    ],
  );

  Future<void> add(RecordBatch batch) async {
    await pause();
    if (batch.rowCount == 0) return;
    _touchSessionMarker(path); // keep this session live for the sweep
    _any = true;
    final table = Table(
      name: 'spill',
      schema: _positional(_nullableSchema(batch.schema)),
      columns: [
        for (final (index, column) in batch.columns.indexed)
          column.withName('c$index'),
      ],
    );
    // Charge the budget BEFORE writing, so an over-cap spill fails fast and
    // never grows the file past the limit. charge() throws without counting if
    // it would exceed the cap.
    final estimate = table.memoryUsage;
    _budget?.charge(estimate);
    _bytes += estimate;
    if (_arrow != null) {
      await _arrow.add(table);
    } else {
      await _parquet!.writeBatch(table).unwrap();
    }
  }

  /// Finalize the file and return a readable run. If nothing was written, an
  /// empty run is returned without creating a file.
  Future<SpillRun> finish() async {
    // Transfer ownership of the charged bytes to the SpillRun ONLY AFTER
    // finalization succeeds. If close() throws, `_owned` stays true so a
    // subsequent abort() releases the budget; otherwise a disk-full / footer
    // failure would permanently consume the engine-wide SpillBudget.
    if (_arrow != null) {
      await _arrow.close(); // no file if nothing was added
      if (_any) await _reconcileToDisk();
      _owned = false;
      return SpillRun(
        path,
        schema,
        hasFile: _any,
        budget: _budget,
        bytes: _bytes,
      );
    }
    if (!_any) {
      await _parquet!.abort();
      _owned = false;
      return SpillRun(
        path,
        schema,
        hasFile: false,
        budget: _budget,
        bytes: _bytes,
      );
    }
    await _parquet!.close().unwrap();
    await _reconcileToDisk();
    _owned = false;
    return SpillRun(
      path,
      schema,
      hasFile: true,
      budget: _budget,
      bytes: _bytes,
    );
  }

  /// After finalization, replace the pre-write uncompressed estimate with the
  /// file's true on-disk size so the budget reflects real disk usage (spilled
  /// files are compressed, so this typically refunds a large over-charge). If
  /// the file is instead LARGER than estimated and reserving the extra would
  /// cross maxSpillBytes, the run is deleted and the query fails: the cap is a
  /// HARD ceiling, not silently crossed.
  Future<void> _reconcileToDisk() async {
    final file = File(path);
    if (!await file.exists()) return;
    final actual = await file.length();
    final delta = actual - _bytes;
    if (delta == 0) return;
    try {
      _budget?.adjust(delta); // throws if a positive delta would exceed the cap
      _bytes = actual;
    } on Failure {
      // The finalized file would push on-disk spill past maxSpillBytes. Release
      // our estimate, delete the run, and let the query fail rather than exceed
      // the limit the caller was promised.
      _budget?.release(_bytes);
      _bytes = 0;
      _owned = false;
      try {
        await file.delete();
      } catch (_) {}
      rethrow;
    }
  }

  /// Release this writer WITHOUT finalizing: close the open handle and delete any
  /// partial file. For cleanup when a query is cancelled or fails mid-build, so a
  /// not-yet-finished spill writer never leaks its file descriptor or temp file.
  /// Safe (best-effort) on an unfinished or never-written writer.
  Future<void> abort() async {
    if (_arrow != null) {
      await _arrow.abort(); // true abort: close the handle + delete the partial
    } else {
      await _parquet!.abort();
    }
    // Release any bytes this (unfinished) writer charged. No-op once finish()
    // handed ownership to a SpillRun.
    if (_owned) {
      _owned = false;
      _budget?.release(_bytes);
    }
  }
}
