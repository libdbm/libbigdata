import 'column.dart';
import 'conversions.dart';
import 'decimal.dart';
import 'data_type.dart';
import 'schema.dart';

/// Shared building blocks for the columnar containers ([Table] and [DataChunk]),
/// so the two can't drift: a fix (float coercion, width promotion, the null_
/// validation rule) applies to both.

/// [value] as an unscaled BigInt at [scale]. Padding with zeros is exact;
/// DROPPING digits is not, so a value needing more scale than the field declares
/// is rejected rather than silently rounded.
BigInt _rescale(Decimal value, int scale, String column) {
  final shift = scale - value.scale;
  if (shift >= 0) return value.unscaled * BigInt.from(10).pow(shift);
  final normalized = value.normalized;
  final shrink = scale - normalized.scale;
  if (shrink >= 0) return normalized.unscaled * BigInt.from(10).pow(shrink);
  throw ArgumentError(
    'Column "$column" has scale $scale but the value $value needs '
    '${normalized.scale} (storing it would drop digits)',
  );
}

/// Build a typed [Column] for [field] from boxed [data]. Numeric fields get the
/// matching typed buffer (int8/16/32/64, float32/64); integer-literal source
/// values are COERCED to double for a float field (a plain `cast<double?>()`
/// throws on an int); string fields are a [StringColumn] and boolean fields a
/// [BoolColumn]; every other type is a boxed [ListColumn].
Column columnFor(Field field, List<dynamic> data) {
  final type = field.dataType;
  if (type.isNumeric) {
    if (type == DataType.int8) {
      return NumericColumn.int8(
        name: field.name,
        values: data.cast<int?>(),
        nullable: field.nullable,
      );
    }
    if (type == DataType.int16) {
      return NumericColumn.int16(
        name: field.name,
        values: data.cast<int?>(),
        nullable: field.nullable,
      );
    }
    if (type == DataType.int32) {
      return NumericColumn.int32(
        name: field.name,
        values: data.cast<int?>(),
        nullable: field.nullable,
      );
    }
    if (type == DataType.int64) {
      return NumericColumn.int64(
        name: field.name,
        values: data.cast<int?>(),
        nullable: field.nullable,
      );
    }
    if (type == DataType.float32 || type == DataType.float64) {
      // Source values are frequently integer literals (e.g. JSON `10` then
      // `10.5`); coerce rather than `cast<double?>()`, which throws on an int.
      final doubles = [
        for (final v in data) v == null ? null : (v as num).toDouble(),
      ];
      return type == DataType.float32
          ? NumericColumn.float32(
              name: field.name,
              values: doubles,
              nullable: field.nullable,
            )
          : NumericColumn.float64(
              name: field.name,
              values: doubles,
              nullable: field.nullable,
            );
    }
    if (type == DataType.decimal) {
      // A decimal promoted from a mixed int/float column holds raw int/double
      // values (from JSON/row literals) that must become unscaled BigInt at the
      // field's scale; a [Decimal] arrives already exact but possibly at a
      // DIFFERENT scale (it is what Table.row hands out, so rows round-trip).
      // An ALREADY-decimal column holds pre-scaled BigInt and passes straight
      // through (converting it would double-scale).
      if (data.any((v) => v is int || v is double || v is Decimal)) {
        final scale = field.scale ?? 0;
        return ListColumn<BigInt>(
          name: field.name,
          dataType: DataType.decimal,
          data: [
            // Per value: an already-unscaled BigInt passes through; a Decimal is
            // re-scaled to the field; a raw int/double is scaled to the field's
            // scale (toUnscaled throws if a fractional value needs more scale
            // than the field declares).
            for (final v in data)
              if (v == null)
                null
              else if (v is BigInt)
                v
              else if (v is Decimal)
                _rescale(v, scale, field.name)
              else
                toUnscaled(v as num, scale),
          ],
          nullable: field.nullable,
        );
      }
    }
    // decimal-as-BigInt / any other numeric is boxed: falls through to ListColumn.
  }
  // A string field inferred from mixed-type rows (JSON `1` then `"a"` →
  // commonType string) still holds the raw non-string values; coerce them to
  // their string form so the column's data matches its declared dtype. Only the
  // non-strict (inferred-schema) path reaches here with a non-string: the strict
  // path rejects it in _check first, so an explicit-schema caller still
  // fails loud rather than getting a silent stringification.
  final coerced =
      type == DataType.string && data.any((v) => v != null && v is! String)
      ? [
          for (final v in data)
            if (v == null) null else v.toString(),
        ]
      : data;
  if (type == DataType.string) {
    return StringColumn(
      name: field.name,
      values: coerced.cast<String?>(),
      nullable: field.nullable,
    );
  }
  if (type == DataType.boolean &&
      coerced.every((v) => v == null || v is bool)) {
    return BoolColumn(
      name: field.name,
      values: coerced.cast<bool?>(),
      nullable: field.nullable,
    );
  }
  return ListColumn(
    name: field.name,
    dataType: type,
    data: coerced,
    nullable: field.nullable,
  );
}

/// Build a [Column] named [name] of [type] from boxed [values]: a numeric,
/// string or boolean column gets its typed buffers when every value matches,
/// otherwise a boxed [ListColumn]. Schema-free columnar construction: lives
/// in core so format readers (CSV/Delta/Iceberg/Dataset) build columns without
/// depending on the SQL kernel layer.
Column columnFromValues(String name, DataType type, List<Object?> values) {
  final nullable = values.contains(null);
  switch (type) {
    case DataType.int8:
      if (values.every((v) => v == null || v is int)) {
        return NumericColumn.int8(
          name: name,
          values: values.cast<int?>(),
          nullable: nullable,
        );
      }
      break;
    case DataType.int16:
      if (values.every((v) => v == null || v is int)) {
        return NumericColumn.int16(
          name: name,
          values: values.cast<int?>(),
          nullable: nullable,
        );
      }
      break;
    case DataType.int32:
      if (values.every((v) => v == null || v is int)) {
        return NumericColumn.int32(
          name: name,
          values: values.cast<int?>(),
          nullable: nullable,
        );
      }
      break;
    case DataType.int64:
      if (values.every((v) => v == null || v is int)) {
        return NumericColumn.int64(
          name: name,
          values: values.cast<int?>(),
          nullable: nullable,
        );
      }
      break;
    case DataType.float32:
      if (values.every((v) => v == null || v is double)) {
        return NumericColumn.float32(
          name: name,
          values: values.cast<double?>(),
          nullable: nullable,
        );
      }
      break;
    case DataType.float64:
      if (values.every((v) => v == null || v is double)) {
        return NumericColumn.float64(
          name: name,
          values: values.cast<double?>(),
          nullable: nullable,
        );
      }
      break;
    case DataType.boolean:
      if (values.every((v) => v == null || v is bool)) {
        return BoolColumn(
          name: name,
          values: values.cast<bool?>(),
          nullable: nullable,
        );
      }
      break;
    case DataType.string:
      if (values.every((v) => v == null || v is String)) {
        return StringColumn(
          name: name,
          values: values.cast<String?>(),
          nullable: nullable,
        );
      }
      break;
    default:
      break;
  }
  return ListColumn(
    name: name,
    dataType: type,
    data: values,
    nullable: nullable,
  );
}

/// Build one typed column per field of [schema] from row maps. When [strict]
/// (explicit-schema construction), a row key that is NOT a schema field is
/// rejected rather than silently dropped, so a typo'd column can't vanish (and,
/// e.g., a Delta update to a mistyped column can't commit a silent no-op).
List<Column> columnsFromRows(
  List<Map<String, dynamic>> rows,
  Schema schema, {
  bool strict = false,
}) {
  final data = <String, List<dynamic>>{
    for (final field in schema.fields) field.name: [],
  };
  final names = strict ? {for (final f in schema.fields) f.name} : null;
  for (final row in rows) {
    if (names != null) {
      for (final key in row.keys) {
        if (!names.contains(key)) {
          throw ArgumentError(
            'Row has field "$key" not in the schema '
            '(${names.join(', ')}); explicit-schema construction is strict',
          );
        }
      }
    }
    for (final field in schema.fields) {
      final value = row[field.name];
      if (strict) _check(field, value);
      data[field.name]!.add(value);
    }
  }
  return [
    for (final field in schema.fields) columnFor(field, data[field.name]!),
  ];
}

/// Reject a value whose runtime type contradicts [field]'s dtype.
///
/// Only for the strict (explicit-schema) path: an inferred schema is derived
/// FROM the values, so they always fit. This is not redundant with
/// [validateColumns], which checks a column's DECLARED dtype and never inspects
/// values: a boxed [ListColumn] would otherwise happily hold an int under a
/// `string` field, leaving the table asserting a type its data contradicts.
/// Numeric fields would eventually throw inside [columnFor]'s cast, but with a
/// bare `String is not a subtype of int?` naming neither column nor value.
void _check(Field field, Object? value) {
  if (value == null) return; // nullability is enforced by validateColumns
  final type = field.dataType;
  final bool ok;
  if (type == DataType.decimal) {
    // Stored unscaled; columnFor scales a raw int/double to the field's scale,
    // and a Decimal (what Table.row emits) back to it.
    ok = value is BigInt || value is num || value is Decimal;
  } else if (type == DataType.float32 || type == DataType.float64) {
    ok = value is num; // an int source value is coerced, by design
  } else if (type.isNumeric) {
    ok = value is int;
  } else if (type == DataType.null_) {
    ok = false; // a null_ field admits nothing but nulls
  } else {
    // string/boolean/binary/temporal/nested map cleanly onto their Dart type.
    ok = switch (type.name) {
      'string' => value is String,
      'boolean' => value is bool,
      'binary' => value is List<int>,
      'date' || 'timestamp' || 'time' => value is DateTime || value is int,
      'list' => value is List,
      'map' || 'struct' => value is Map,
      _ => true, // unknown type: leave it to columnFor
    };
  }
  if (!ok) {
    throw ArgumentError(
      'Column "${field.name}" is $type but the value $value is a '
      '${value.runtimeType}',
    );
  }
}

/// An empty column per field of [schema].
List<Column> emptyColumns(Schema schema) => [
  for (final field in schema.fields)
    ListColumn(
      name: field.name,
      dataType: field.dataType,
      data: [],
      nullable: field.nullable,
    ),
];

/// The [names] columns, in that order, found with [lookup]; a name it does not
/// find is an [ArgumentError].
List<Column> selectColumns(
  List<String> names,
  Column? Function(String name) lookup,
) => [
  for (final name in names)
    lookup(name) ?? (throw ArgumentError('Column not found: $name')),
];

/// Validate that [columns] match [schema] in count, per-column name, equal
/// length, and exact dtype: the ONE exception being an all-null (`null_`)
/// COLUMN, which fits any concrete field (but NOT the reverse: a concrete column
/// under a `null_` field is a schema lie and is rejected). A non-nullable field
/// must not carry nulls. Throws [ArgumentError] on any violation.
void validateColumns(List<Column> columns, Schema schema) {
  if (columns.length != schema.fields.length) {
    throw ArgumentError(
      'Column count (${columns.length}) does not match schema field count '
      '(${schema.fields.length})',
    );
  }
  if (columns.isEmpty) return;
  final expected = columns.first.length;
  for (int i = 0; i < columns.length; i++) {
    if (columns[i].length != expected) {
      throw ArgumentError(
        'Column ${columns[i].name} has length ${columns[i].length}, '
        'expected $expected',
      );
    }
    if (columns[i].name != schema.fields[i].name) {
      throw ArgumentError(
        'Column name ${columns[i].name} does not match schema field '
        '${schema.fields[i].name}',
      );
    }
    final actual = columns[i].dataType;
    final declared = schema.fields[i].dataType;
    if (actual != declared && actual != DataType.null_) {
      throw ArgumentError(
        'Column ${columns[i].name} has type $actual but schema field '
        'declares $declared',
      );
    }
    if (!schema.fields[i].nullable && columns[i].hasNulls) {
      throw ArgumentError(
        'Column ${columns[i].name} contains nulls but schema field '
        '${schema.fields[i].name} is declared non-nullable',
      );
    }
  }
}

/// The schema of [base] with each field's dtype reconciled to the dtype of the
/// corresponding [combined] column, so a width promotion during a concat/append
/// (e.g. int32 + int64 → int64) is reflected and field/column dtypes agree.
Schema promotedSchema(Schema base, List<Column> combined) => Schema(
  fields: [
    for (int i = 0; i < combined.length; i++)
      base.fields[i].dataType == combined[i].dataType
          ? base.fields[i]
          : base.fields[i].copyWith(dataType: combined[i].dataType),
  ],
  metadata: base.metadata,
);
