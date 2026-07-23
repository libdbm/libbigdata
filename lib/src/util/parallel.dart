import 'dart:async';

/// Run [task] over [items] with at most [concurrency] tasks in flight, returning
/// the results in **input order** (not completion order), so callers get
/// deterministic output regardless of parallelism.
///
/// A worker pool shares a cursor over [items]; because the event loop is
/// single-threaded, `next++` is atomic between awaits. The first task error
/// propagates; once any task throws, sibling workers stop pulling new items so
/// no further work (e.g. isolates) is spawned for the abandoned batch.
///
/// [onEach] runs (in completion order) as each result arrives — a caller that
/// charges a running budget there can THROW to abort the whole run early, so an
/// over-cap load stops after ~[concurrency] more in-flight tasks instead of
/// materializing every item first. The returned list stays in INPUT order.
Future<List<O>> parallelMap<I, O>(
  List<I> items,
  Future<O> Function(I item) task, {
  int concurrency = 1,
  void Function(O result)? onEach,
}) async {
  if (items.isEmpty) return <O>[];
  final workers = concurrency < 1 ? 1 : concurrency;
  final results = List<O?>.filled(items.length, null);
  var next = 0;
  var failed = false;

  Future<void> run() async {
    while (!failed) {
      final i = next++;
      if (i >= items.length) return;
      try {
        final result = await task(items[i]);
        results[i] = result;
        onEach?.call(
          result,
        ); // may throw (e.g. budget overflow) → aborts siblings
      } catch (_) {
        failed = true; // siblings see this and stop pulling new items
        rethrow;
      }
    }
  }

  await Future.wait([
    for (var w = 0; w < workers && w < items.length; w++) run(),
  ]);
  return [for (final r in results) r as O];
}
