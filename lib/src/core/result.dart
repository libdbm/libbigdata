/// The outcome of an operation that can fail in an expected way: [Ok] with a
/// value, or [Err] with an error (in this library, an [ErrorDetails]). Public
/// entry points return `Result<T, ErrorDetails>` for outcomes such as a parse
/// error, a denied path or a commit conflict, and keep throwing only for
/// programming errors.
///
/// It is sealed, so a `switch` over it must handle both cases:
///
/// ```dart
/// switch (await engine.execute(query)) {
///   case Ok(:final value):
///     print(value);
///   case Err(:final error):
///     print('${error.kind}: ${error.message}');
/// }
/// ```
sealed class Result<T, E> {
  const Result();

  /// This result with [transform] applied to an [Ok] value; an [Err] passes
  /// through unchanged.
  Result<U, E> map<U>(U Function(T value) transform) => switch (this) {
    Ok(:final value) => Ok(transform(value)),
    Err(:final error) => Err(error),
  };
}

/// A successful [Result] holding [value]. Equal to another [Ok] with an
/// equal value.
final class Ok<T, E> extends Result<T, E> {
  const Ok(this.value);

  final T value;

  @override
  bool operator ==(Object other) => other is Ok && other.value == value;

  @override
  int get hashCode => Object.hash(Ok, value);

  @override
  String toString() => 'Ok($value)';
}

/// A failed [Result] holding [error]. Equal to another [Err] with an equal
/// error.
final class Err<T, E> extends Result<T, E> {
  const Err(this.error);

  final E error;

  @override
  bool operator ==(Object other) => other is Err && other.error == error;

  @override
  int get hashCode => Object.hash(Err, error);

  @override
  String toString() => 'Err($error)';
}

/// What kind of expected failure an [ErrorDetails] reports.
enum Kind {
  /// Another writer committed first; retrying may succeed.
  conflict,

  /// The operation may or may not have taken effect (a commit whose outcome
  /// could not be confirmed); staged files must be kept.
  indeterminate,

  /// A configured cap was reached: rows, bytes, partitions, retries.
  limit,

  /// The data does not match the expected schema.
  schema,

  /// Input is malformed: a corrupt file or table metadata.
  corrupt,

  /// Access was refused: a path outside the sandbox, a write to a read-only
  /// engine or table.
  denied,

  /// An argument or request is not valid (an unknown column, a bad option,
  /// SQL that does not parse).
  invalid,

  /// What was asked for does not exist: a table, file, version or snapshot.
  missing,

  /// Valid input this library cannot handle: a table feature, format version
  /// or file format it does not implement.
  unsupported,

  /// The file system or another resource failed.
  io,

  /// A deadline passed.
  timeout,
}

/// Why an operation failed: its [kind], a human-readable [message], and the
/// underlying [cause] (often the exception that was converted), if any.
final class ErrorDetails {
  const ErrorDetails(this.kind, this.message, [this.cause]);

  final Kind kind;
  final String message;
  final Object? cause;

  @override
  String toString() => '${kind.name}: $message';
}
