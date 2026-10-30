import 'package:meta/meta.dart';

import '../../core/schema.dart';

/// Options for CSV reading and writing
@immutable
final class CsvOptions {
  /// Field delimiter character
  final String delimiter;

  /// Quote character for escaping fields
  final String quote;

  /// Escape character within quoted fields
  final String escape;

  /// Line terminator
  final String lineTerminator;

  /// Whether the first row contains headers
  final bool hasHeader;

  /// Column names (if hasHeader is false)
  final List<String>? columnNames;

  /// Strings to recognize as null values
  final Set<String> nullValues;

  /// Explicit null representation for LOSSLESS round-trips. RFC 4180 has no null,
  /// so by default (null) the writer emits an empty field for both null and the
  /// empty string and the two can't be told apart on read. When set (e.g. `\N`):
  /// the writer emits this token for null and force-quotes empty strings as `""`
  /// (so a single-column null/empty row is never an ambiguous blank line), and
  /// the reader maps an UNQUOTED occurrence of this token back to null (a quoted
  /// `""` stays the empty string). Recognized in addition to [nullValues].
  final String? nullOutput;

  /// Whether [value] should read as null: a configured [nullValues] token or the
  /// [nullOutput] sentinel. Only applied to UNQUOTED fields by the reader.
  bool isNullToken(String value) =>
      nullValues.contains(value) || value == nullOutput;

  /// Validate the structural options both the reader and writer assume, throwing
  /// [ArgumentError] on violation: `delimiter`/`quote`/`escape` must be exactly
  /// one character (the parser reads only the first code unit, so a multi-char
  /// value silently mismatches the writer and an empty one throws a RangeError),
  /// and a `nullOutput` sentinel must be non-empty and never need quoting.
  void validate() {
    void single(String name, String value) {
      if (value.length != 1) {
        throw ArgumentError(
          'CsvOptions.$name must be exactly one character (got "$value")',
        );
      }
    }

    single('delimiter', delimiter);
    single('quote', quote);
    single('escape', escape);
    final sentinel = nullOutput;
    if (sentinel != null &&
        (sentinel.isEmpty ||
            [delimiter, quote, escape, '\n', '\r'].any(sentinel.contains))) {
      throw ArgumentError(
        'CsvOptions.nullOutput "$sentinel" must be non-empty and contain no '
        'delimiter, quote, escape, or newline character',
      );
    }
  }

  /// Whether to skip empty lines
  final bool skipEmptyLines;

  /// Whether to trim whitespace from fields
  final bool trimFields;

  /// Maximum number of rows to read (null for all)
  final int? maxRows;

  /// Cap on the size (in characters) of a single logical record while merging a
  /// quote-spanning field across physical lines. Guards against an unclosed
  /// quote accumulating the rest of the file into memory before erroring at EOF.
  final int maxRecordBytes;

  /// Optional ceiling (in bytes) on a file read eagerly by `readFile`, which
  /// loads the whole file into memory. `null` (default) keeps the prior
  /// behavior; set it to fail loud on oversized files and steer callers to the
  /// streaming `readBatches` API. Does not affect `readBatches`.
  final int? maxFileBytes;

  /// Number of rows to skip at the beginning
  final int skipRows;

  /// Whether to infer data types
  final bool inferTypes;

  /// Number of rows to sample for type inference
  final int typeInferenceSampleSize;

  /// Encoding for the CSV file
  final String encoding;

  /// Comment character (lines starting with this are ignored)
  final String? comment;

  /// Whether to allow variable number of fields
  final bool allowVariableFields;

  /// Date format for parsing dates
  final String? dateFormat;

  /// Timestamp format for parsing timestamps
  final String? timestampFormat;

  /// Decimal separator for numbers
  final String decimalSeparator;

  /// Thousands separator for numbers
  final String? thousandsSeparator;

  /// Explicit column schema. When provided, type inference is skipped and the
  /// file is read in a single pass with these column types (positionally
  /// aligned to the CSV columns).
  final Schema? schema;

  /// Fail loud on a cell that cannot be parsed to its column type (default).
  /// A configured null token (see [nullValues]) is still accepted; only genuine
  /// parse failures (e.g. `12O` in a numeric column, `tru` in a boolean column)
  /// throw. Set `false` for permissive ingestion, where an unparseable cell
  /// becomes null in a nullable column (and still errors in a non-nullable one).
  final bool strict;

  const CsvOptions({
    this.delimiter = ',',
    this.quote = '"',
    this.escape = '"',
    this.lineTerminator = '\n',
    this.hasHeader = true,
    this.columnNames,
    this.nullValues = const {
      'NULL',
      'null',
      'NA',
      'N/A',
      'n/a',
      'NaN',
      'nan',
    }, // RFC 4180: empty strings are not null
    this.nullOutput,
    this.skipEmptyLines = true,
    this.trimFields = false,
    this.maxRows,
    this.maxRecordBytes = 16 << 20, // 16 MB
    this.maxFileBytes,
    this.skipRows = 0,
    this.inferTypes = true,
    this.typeInferenceSampleSize = 1000,
    this.encoding = 'utf-8',
    this.comment,
    this.allowVariableFields = false,
    this.dateFormat,
    this.timestampFormat,
    this.decimalSeparator = '.',
    this.thousandsSeparator,
    this.schema,
    this.strict = true,
  });

  /// Options with TYPE inference and field trimming enabled. Despite the name,
  /// this does NOT sniff the delimiter/quote/terminator; there is no dialect
  /// detection; pass [delimiter] etc. explicitly for non-default dialects.
  /// (Kept under this name for compatibility.)
  factory CsvOptions.autoDetect() {
    return const CsvOptions(inferTypes: true, trimFields: true);
  }

  /// Create options for TSV files
  factory CsvOptions.tsv() {
    return const CsvOptions(delimiter: '\t', inferTypes: true);
  }

  /// Create options for pipe-delimited files
  factory CsvOptions.pipe() {
    return const CsvOptions(delimiter: '|', inferTypes: true);
  }

  /// Create options for European-style CSV (semicolon delimiter, comma decimal)
  factory CsvOptions.european() {
    return const CsvOptions(
      delimiter: ';',
      decimalSeparator: ',',
      thousandsSeparator: '.',
      inferTypes: true,
    );
  }

  /// Sentinel distinguishing "argument omitted" (keep the current value) from an
  /// explicit `null` (clear it) for the nullable options: `x ?? this.x` can't
  /// tell those apart, so passing `maxRows: null` / `schema: null` etc. would
  /// silently retain the old value (a stale schema or a still-active safety cap).
  static const Object _keep = Object();

  /// Copy with modifications. Non-nullable options use `?? current`; the nullable
  /// ones take the [_keep] sentinel so they can be explicitly cleared with `null`.
  CsvOptions copyWith({
    String? delimiter,
    String? quote,
    String? escape,
    String? lineTerminator,
    bool? hasHeader,
    Object? columnNames = _keep,
    Set<String>? nullValues,
    Object? nullOutput = _keep,
    bool? skipEmptyLines,
    bool? trimFields,
    Object? maxRows = _keep,
    int? maxRecordBytes,
    Object? maxFileBytes = _keep,
    int? skipRows,
    bool? inferTypes,
    int? typeInferenceSampleSize,
    String? encoding,
    Object? comment = _keep,
    bool? allowVariableFields,
    Object? dateFormat = _keep,
    Object? timestampFormat = _keep,
    String? decimalSeparator,
    Object? thousandsSeparator = _keep,
    Object? schema = _keep,
    bool? strict,
  }) {
    return CsvOptions(
      delimiter: delimiter ?? this.delimiter,
      quote: quote ?? this.quote,
      escape: escape ?? this.escape,
      lineTerminator: lineTerminator ?? this.lineTerminator,
      hasHeader: hasHeader ?? this.hasHeader,
      columnNames: identical(columnNames, _keep)
          ? this.columnNames
          : columnNames as List<String>?,
      nullValues: nullValues ?? this.nullValues,
      nullOutput: identical(nullOutput, _keep)
          ? this.nullOutput
          : nullOutput as String?,
      skipEmptyLines: skipEmptyLines ?? this.skipEmptyLines,
      trimFields: trimFields ?? this.trimFields,
      maxRows: identical(maxRows, _keep) ? this.maxRows : maxRows as int?,
      maxRecordBytes: maxRecordBytes ?? this.maxRecordBytes,
      maxFileBytes: identical(maxFileBytes, _keep)
          ? this.maxFileBytes
          : maxFileBytes as int?,
      skipRows: skipRows ?? this.skipRows,
      inferTypes: inferTypes ?? this.inferTypes,
      typeInferenceSampleSize:
          typeInferenceSampleSize ?? this.typeInferenceSampleSize,
      encoding: encoding ?? this.encoding,
      comment: identical(comment, _keep) ? this.comment : comment as String?,
      allowVariableFields: allowVariableFields ?? this.allowVariableFields,
      dateFormat: identical(dateFormat, _keep)
          ? this.dateFormat
          : dateFormat as String?,
      timestampFormat: identical(timestampFormat, _keep)
          ? this.timestampFormat
          : timestampFormat as String?,
      decimalSeparator: decimalSeparator ?? this.decimalSeparator,
      thousandsSeparator: identical(thousandsSeparator, _keep)
          ? this.thousandsSeparator
          : thousandsSeparator as String?,
      schema: identical(schema, _keep) ? this.schema : schema as Schema?,
      strict: strict ?? this.strict,
    );
  }
}
