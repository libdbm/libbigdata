import 'package:libbigdata/libbigdata.dart';

/// The value a benchmarked call returns; a failure stops the run.
extension Outcome<T> on Future<Result<T, ErrorDetails>> {
  Future<T> get ok async => switch (await this) {
    Ok(:final value) => value,
    Err(:final error) => throw StateError('$error'),
  };
}

/// The batches a benchmarked stream yields; a failure stops the run.
extension Batches<T> on Stream<Result<T, ErrorDetails>> {
  Stream<T> get ok => map(
    (result) => switch (result) {
      Ok(:final value) => value,
      Err(:final error) => throw StateError('$error'),
    },
  );
}

/// The value of a benchmark set-up step; a failure stops the run.
extension Value<T> on Result<T, ErrorDetails> {
  T get ok => switch (this) {
    Ok(:final value) => value,
    Err(:final error) => throw StateError('$error'),
  };
}
