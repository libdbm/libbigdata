import 'dart:async';
import 'dart:io';

import 'package:logging/logging.dart';

import 'result.dart';
import '../util/logging_config.dart' show Loggers;

/// An expected failure raised inside the library, carrying the [ErrorDetails]
/// a public entry point returns as an [Err]. Internal code keeps throwing; the
/// boundary converts once, with [capture].
class Failure implements Exception {
  const Failure(this.details);

  /// A failure of [kind] described by [message].
  Failure.of(Kind kind, String message, [Object? cause])
    : details = ErrorDetails(kind, message, cause);

  final ErrorDetails details;

  @override
  String toString() => details.toString();
}

final Logger _log = Logger(Loggers.root);

/// Run [body] and return its value as [Ok], or whatever it throws as [Err], so
/// nothing escapes a public entry point. A [Failure] keeps its details and
/// standard exceptions map by type (see [expected]). Anything else, an [Error]
/// included, was not expected: it is reported as [fallback] (or
/// [Kind.unsupported] for an [UnsupportedError]) with the error as its cause,
/// and logged with its stack trace. Argument preconditions a caller must meet
/// are checked before [body] runs, so they still throw.
Future<Result<T, ErrorDetails>> capture<T>(
  Future<T> Function() body, {
  Kind fallback = Kind.invalid,
}) async {
  try {
    return Ok(await body());
  } catch (error, stack) {
    return Err(caught(error, stack, fallback));
  }
}

/// As [capture], for a stream: each value of [body] as an [Ok], then at most
/// one [Err], which ends the stream. The returned stream never errors.
Stream<Result<T, ErrorDetails>> captureStream<T>(
  Stream<T> Function() body, {
  Kind fallback = Kind.invalid,
}) async* {
  try {
    // `await for`, not `yield*`: a forwarded stream's error would bypass this
    // catch.
    await for (final value in body()) {
      yield Ok(value);
    }
  } catch (error, stack) {
    yield Err(caught(error, stack, fallback));
  }
}

/// As [capture], for a [body] that already returns a [Result]: one that
/// handles some failures itself and throws the rest.
Future<Result<T, ErrorDetails>> guard<T>(
  Future<Result<T, ErrorDetails>> Function() body, {
  Kind fallback = Kind.invalid,
}) async {
  try {
    return await body();
  } catch (error, stack) {
    return Err(caught(error, stack, fallback));
  }
}

/// The [ErrorDetails] [capture] gives [error]: as [expected] maps it, or, for
/// anything unexpected, [fallback] with the error as its cause, logged with
/// [stack].
ErrorDetails caught(Object error, StackTrace stack, Kind fallback) {
  if (error is Exception) {
    if (expected(error) case final details?) return details;
  }
  _log.warning('Unexpected failure: $error', error, stack);
  return switch (error) {
    UnsupportedError(:final message) => ErrorDetails(
      Kind.unsupported,
      message ?? '$error',
      error,
    ),
    _ => ErrorDetails(fallback, '$error', error),
  };
}

/// The [ErrorDetails] for an expected failure [error], or null when it is not
/// one this library recognises.
ErrorDetails? expected(Exception error) => switch (error) {
  Failure(:final details) => details,
  TimeoutException() => ErrorDetails(Kind.timeout, '$error', error),
  PathNotFoundException() => ErrorDetails(
    Kind.missing,
    _describe(error),
    error,
  ),
  FileSystemException() => ErrorDetails(Kind.io, _describe(error), error),
  FormatException(:final message) => ErrorDetails(Kind.corrupt, message, error),
  _ => null,
};

/// [error] as `message: path (os message)`, without the exception type name.
String _describe(FileSystemException error) => [
  error.message,
  if (error.path != null) ': ${error.path}',
  if (error.osError != null) ' (${error.osError!.message})',
].join();

/// Run [body], returning anything it throws as an [Err]: the boundary of an
/// entry point that evaluates caller-supplied expressions (SQL, DataFrame
/// predicates), which reports every failure rather than throwing one. See
/// [describe] for the mapping. An [Error] is logged to [log] with its stack
/// trace, since the [Err] keeps only the error itself.
Future<Result<T, ErrorDetails>> report<T>(
  Future<Result<T, ErrorDetails>> Function() body,
  Logger log,
) async {
  try {
    return await body();
  } catch (error, stack) {
    if (error is Error) log.warning('Failed with $error', error, stack);
    return Err(describe(error));
  }
}

/// As [report], for a stream: each value of [body] as an [Ok], then at most
/// one [Err], which ends the stream. The returned stream never errors.
Stream<Result<T, ErrorDetails>> reportStream<T>(
  Stream<T> Function() body,
  Logger log,
) async* {
  try {
    // `await for`, not `yield*`: a forwarded stream's error would bypass this
    // catch and reach the consumer as a stream error.
    await for (final value in body()) {
      yield Ok(value);
    }
  } catch (error, stack) {
    if (error is Error) log.warning('Failed with $error', error, stack);
    yield Err(describe(error));
  }
}

/// The [ErrorDetails] [report] gives [error]. A [Failure] keeps its kind. A
/// [FormatException] is the caller's expression's (a data file's is reported
/// as [Kind.corrupt] where the file is read), so it is [Kind.invalid]. Other
/// expected exceptions map as [expected] maps them, an unsupported feature is
/// [Kind.unsupported], and anything else is [Kind.invalid] with the error as
/// its cause.
ErrorDetails describe(Object error) => switch (error) {
  FormatException(:final message) => ErrorDetails(Kind.invalid, message, error),
  Exception() => expected(error) ?? ErrorDetails(Kind.invalid, '$error', error),
  UnsupportedError(:final message) => ErrorDetails(
    Kind.unsupported,
    message ?? '$error',
    error,
  ),
  _ => ErrorDetails(Kind.invalid, '$error', error),
};

/// For library code that consumes a public [Result] inside a call chain that
/// still throws: the [Ok] value, or the [Err] thrown as its [Failure] for the
/// chain's own boundary to report.
extension Unwrap<T> on Result<T, ErrorDetails> {
  T unwrap() => switch (this) {
    Ok(:final value) => value,
    Err(:final error) => throw Failure(error),
  };
}

/// As [Unwrap], for a pending result.
extension UnwrapFuture<T> on Future<Result<T, ErrorDetails>> {
  Future<T> unwrap() async => (await this).unwrap();
}

/// As [Unwrap], for a result stream.
extension UnwrapStream<T> on Stream<Result<T, ErrorDetails>> {
  Stream<T> unwrap() => map((result) => result.unwrap());
}
