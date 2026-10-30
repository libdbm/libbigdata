import 'package:libbigdata/libbigdata.dart';
import 'package:libbigdata/src/core/failure.dart';
import 'package:test/test.dart';

/// Unwrap a library [Result] in a test, failing it on the other case.
extension Outcome<T> on Future<Result<T, ErrorDetails>> {
  /// The value of an [Ok]; an [Err] fails the test with its details.
  Future<T> get ok async => switch (await this) {
    Ok(:final value) => value,
    Err(:final error) => fail('expected Ok, got $error'),
  };

  /// The details of an [Err]; an [Ok] fails the test.
  Future<ErrorDetails> get err async => switch (await this) {
    Ok(:final value) => fail('expected Err, got Ok($value)'),
    Err(:final error) => error,
  };
}

/// Matches a `Future<Result>` that completes with an [Err] of [kind] whose
/// message matches [message].
Matcher failsWith(Kind kind, [Object? message = anything]) => completion(
  isA<Err<Object?, ErrorDetails>>()
      .having((err) => err.error.kind, 'kind', kind)
      .having((err) => err.error.message, 'message', message),
);

/// Matches a call that still throws, with a [Failure] of [kind] whose message
/// matches [message].
Matcher throwsFailure(Kind kind, [Object? message = anything]) => throwsA(
  isA<Failure>()
      .having((failure) => failure.details.kind, 'kind', kind)
      .having((failure) => failure.details.message, 'message', message),
);

/// Unwrap a library result stream in a test, failing it on an [Err].
extension Batches<T> on Stream<Result<T, ErrorDetails>> {
  /// Each [Ok] batch; an [Err] fails the test with its details.
  Stream<T> get ok => map(
    (result) => switch (result) {
      Ok(:final value) => value,
      Err(:final error) => fail('expected Ok, got $error'),
    },
  );
}

/// Unwrap a library [Result] that is already complete.
extension Value<T> on Result<T, ErrorDetails> {
  /// The value of an [Ok]; an [Err] fails the test with its details.
  T get ok => switch (this) {
    Ok(:final value) => value,
    Err(:final error) => fail('expected Ok, got $error'),
  };
}
