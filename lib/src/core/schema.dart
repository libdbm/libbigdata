import 'package:meta/meta.dart';
import 'data_type.dart';
import 'conversions.dart';

/// Represents a field in a schema
@immutable
class Field {
  final String name;
  final DataType dataType;
  final bool nullable;
  final Map<String, dynamic> metadata;

  /// Optional table qualifier (alias) used to disambiguate same-named columns
  /// from different inputs in a join. Excluded from equality/hashCode (like
  /// [metadata]) so schema-equality and concatenation are unaffected.
  final String? qualifier;

  /// Child fields for nested types ([DataType.list] = 1 element field;
  /// [DataType.struct] = its members; [DataType.map] = key, value). Empty for
  /// scalars. Excluded from equality/hashCode (carried metadata, like
  /// [qualifier]) so spill schema-equality / concatenation stay unaffected.
  final List<Field> children;

  /// Decimal precision/scale (only set for [DataType.decimal]). Also excluded
  /// from equality/hashCode.
  final int? precision;
  final int? scale;

  /// Optional column field-id, emitted into the Parquet SchemaElement when set
  /// (Iceberg/Spark-Iceberg use file-level field-ids for schema evolution and
  /// column reordering). Carried metadata — excluded from equality/hashCode.
  final int? id;

  // Not const: defensively wrap the caller's [children]/[metadata] so a mutation
  // after construction can't invalidate this @immutable field (empty stays the
  // shared const instance, so scalar fields — the hot case — allocate nothing).
  Field({
    required this.name,
    required this.dataType,
    this.nullable = true,
    Map<String, dynamic> metadata = const {},
    this.qualifier,
    List<Field> children = const [],
    this.precision,
    this.scale,
    this.id,
  }) : metadata = metadata.isEmpty ? const {} : Map.unmodifiable(metadata),
       children = children.isEmpty ? const [] : List.unmodifiable(children);

  Field copyWith({
    String? name,
    DataType? dataType,
    bool? nullable,
    Map<String, dynamic>? metadata,
    String? qualifier,
    List<Field>? children,
    int? precision,
    int? scale,
    int? id,
  }) {
    return Field(
      name: name ?? this.name,
      dataType: dataType ?? this.dataType,
      nullable: nullable ?? this.nullable,
      metadata: metadata ?? this.metadata,
      qualifier: qualifier ?? this.qualifier,
      children: children ?? this.children,
      precision: precision ?? this.precision,
      scale: scale ?? this.scale,
      id: id ?? this.id,
    );
  }

  /// Structural compatibility for append/concat/spill: same name, a common data
  /// type, matching decimal scale, and — for nested types — recursively
  /// compatible [children]. Distinct from [==], which is deliberately loose (it
  /// ignores children/precision/scale so carried metadata doesn't block
  /// concatenation); this guards the axes whose mismatch would corrupt data.
  bool isCompatibleWith(Field other) {
    if (name != other.name) return false;
    if (dataType != other.dataType &&
        DataType.commonType(dataType, other.dataType) == null) {
      return false;
    }
    // Merging unscaled BigInts of different decimal scales corrupts values;
    // precision may differ (same-scale unscaled ints concatenate safely).
    if (dataType == DataType.decimal &&
        other.dataType == DataType.decimal &&
        scale != other.scale) {
      return false;
    }
    // Nested types (list/struct/map) carry their shape in [children], which
    // [==] ignores — so struct<a,b> and struct<a,c> would otherwise look
    // compatible and mis-pair column data on concat.
    if (children.length != other.children.length) return false;
    for (int i = 0; i < children.length; i++) {
      if (!children[i].isCompatibleWith(other.children[i])) return false;
    }
    return true;
  }

  /// EXACT encoding compatibility for a STREAMING write against an
  /// already-frozen physical schema: same name, the SAME `dataType` (NO widening
  /// — an int64 batch under an int32 column would be encoded positionally and
  /// silently truncated), matching decimal precision/scale, and recursively-exact
  /// nested [children]. Distinct from [isCompatibleWith], which permits width
  /// promotion (safe only for IN-MEMORY append/concat, never for encoding under a
  /// fixed physical type).
  bool encodesAs(Field other) {
    if (name != other.name || dataType != other.dataType) return false;
    if (dataType == DataType.decimal &&
        (scale != other.scale || precision != other.precision)) {
      return false;
    }
    if (children.length != other.children.length) return false;
    for (int i = 0; i < children.length; i++) {
      if (!children[i].encodesAs(other.children[i])) return false;
    }
    return true;
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is Field &&
          runtimeType == other.runtimeType &&
          name == other.name &&
          dataType == other.dataType &&
          nullable == other.nullable;

  @override
  int get hashCode => Object.hash(name, dataType, nullable);

  @override
  String toString() => 'Field($name: ${dataType.name}${nullable ? "?" : ""})';

  Map<String, dynamic> toJson() => {
    'name': name,
    'type': dataType.name,
    'nullable': nullable,
    if (metadata.isNotEmpty) 'metadata': metadata,
    if (children.isNotEmpty)
      'children': children.map((c) => c.toJson()).toList(),
    if (precision != null) 'precision': precision,
    if (scale != null) 'scale': scale,
    if (id != null) 'id': id,
  };

  factory Field.fromJson(Map<String, dynamic> json) {
    return Field(
      name: json['name'] as String,
      dataType: _parseDataType(json['type'] as String),
      nullable: json['nullable'] as bool? ?? true,
      metadata: (json['metadata'] as Map<String, dynamic>?) ?? {},
      children: ((json['children'] as List<dynamic>?) ?? const [])
          .map((c) => Field.fromJson(c as Map<String, dynamic>))
          .toList(),
      precision: json['precision'] as int?,
      scale: json['scale'] as int?,
      id: json['id'] as int?,
    );
  }

  static DataType _parseDataType(String typeName) {
    switch (typeName) {
      case 'boolean':
        return DataType.boolean;
      case 'int8':
        return DataType.int8;
      case 'int16':
        return DataType.int16;
      case 'int32':
        return DataType.int32;
      case 'int64':
        return DataType.int64;
      case 'float32':
        return DataType.float32;
      case 'float64':
        return DataType.float64;
      case 'string':
        return DataType.string;
      case 'binary':
        return DataType.binary;
      case 'date':
        return DataType.date;
      case 'timestamp':
        return DataType.timestamp;
      case 'decimal':
        return DataType.decimal;
      case 'list':
        return DataType.list;
      case 'map':
        return DataType.map;
      case 'struct':
        return DataType.struct;
      case 'null':
        return DataType.null_;
      default:
        throw ArgumentError('Unknown data type: $typeName');
    }
  }
}

/// Represents the schema of a table or DataFrame
@immutable
class Schema {
  final List<Field> fields;
  final Map<String, dynamic> metadata;

  // Defensively copy into an unmodifiable list so a caller mutating the passed
  // list after construction can't silently invalidate column order/type against
  // the table's buffers (the name→index maps below are memoized once).
  Schema({
    required List<Field> fields,
    Map<String, dynamic> metadata = const {},
  }) : fields = List.unmodifiable(fields),
       metadata = metadata.isEmpty ? const {} : Map.unmodifiable(metadata) {
    _validateFieldNames();
  }

  /// Lazily-built name → first-index map so [field]/[fieldIndex] are O(1)
  /// rather than O(n); name-keyed projection over wide tables was O(cols²).
  /// First occurrence wins, matching the old linear scan.
  late final Map<String, int> _firstIndex = {
    for (int i = fields.length - 1; i >= 0; i--) fields[i].name: i,
  };

  /// Names that occur on more than one field — a bare (unqualified) reference to
  /// one is ambiguous. Built once so [resolve] is O(1) instead of O(cols).
  late final Set<String> _ambiguousNames = () {
    final seen = <String>{};
    final dup = <String>{};
    for (final f in fields) {
      if (!seen.add(f.name)) dup.add(f.name);
    }
    return dup;
  }();

  void _validateFieldNames() {
    final names = <String>{};
    for (final field in fields) {
      // Uniqueness is on the (qualifier, name) pair, so two joined inputs may
      // each contribute a `name` column as long as they carry distinct aliases.
      final key = field.qualifier == null
          ? field.name
          : '${field.qualifier}.${field.name}';
      if (!names.add(key)) {
        throw ArgumentError('Duplicate field name: $key');
      }
    }
  }

  /// Index of the field named [name]; if [qualifier] is given it must also match.
  /// A bare lookup that matches more than one field throws (ambiguous); returns
  /// -1 when absent.
  ///
  /// [strict] controls what an UNKNOWN qualifier (one no field carries) does. The
  /// default falls back to a bare match — required at RUNTIME, where a spilled
  /// batch re-read through [ColumnListSchema.schemaOf] has lost its qualifiers
  /// (columns don't carry them), so an operator resolving a join key by its
  /// original qualifier must still find it by name. `strict: true` is for the
  /// PLAN-TIME binding gate, where the schema still has qualifiers: there an
  /// unknown qualifier is a real user error (a typo'd alias), NOT a fall-back —
  /// otherwise `typo.n` silently binds to an unrelated column.
  int resolve(String name, {String? qualifier, bool strict = false}) {
    if (qualifier != null) {
      var known = false;
      for (int i = 0; i < fields.length; i++) {
        if (fields[i].qualifier == qualifier) {
          known = true;
          if (fields[i].name == name) return i;
        }
      }
      return (known || strict) ? -1 : resolve(name);
    }
    if (_ambiguousNames.contains(name)) {
      throw StateError(
        'Ambiguous column "$name" (qualify it, e.g. alias.$name)',
      );
    }
    return _firstIndex[name] ?? -1;
  }

  /// A copy with every field tagged with table [alias] as its qualifier.
  Schema qualify(String alias) => Schema(
    fields: [for (final f in fields) f.copyWith(qualifier: alias)],
    metadata: metadata,
  );

  /// Get field by name
  Field? field(String name) {
    final i = _firstIndex[name];
    return i == null ? null : fields[i];
  }

  /// Get field index by name
  int fieldIndex(String name) => _firstIndex[name] ?? -1;

  /// Check if schema contains a field
  bool hasField(String name) => field(name) != null;

  /// Get all field names
  List<String> get fieldNames => fields.map((f) => f.name).toList();

  /// Get number of fields
  int get length => fields.length;

  /// Select subset of fields
  Schema select(List<String> fieldNames) {
    final selectedFields = <Field>[];
    for (final name in fieldNames) {
      final field = this.field(name);
      if (field == null) {
        throw ArgumentError('Field not found: $name');
      }
      selectedFields.add(field);
    }
    return Schema(fields: selectedFields, metadata: metadata);
  }

  /// Rename fields
  Schema rename(Map<String, String> renames) {
    final renamedFields = fields.map((field) {
      final newName = renames[field.name];
      return newName != null ? field.copyWith(name: newName) : field;
    }).toList();
    return Schema(fields: renamedFields, metadata: metadata);
  }

  /// Add a field to the schema
  Schema addField(Field field) {
    if (hasField(field.name)) {
      throw ArgumentError('Field already exists: ${field.name}');
    }
    return Schema(fields: [...fields, field], metadata: metadata);
  }

  /// Remove a field from the schema
  Schema removeField(String name) {
    if (!hasField(name)) {
      throw ArgumentError('Field not found: $name');
    }
    return Schema(
      fields: fields.where((f) => f.name != name).toList(),
      metadata: metadata,
    );
  }

  /// Merge with another schema
  Schema merge(Schema other, {bool overwrite = false}) {
    final mergedFields = <Field>[];
    final usedNames = <String>{};

    for (final field in fields) {
      mergedFields.add(field);
      usedNames.add(field.name);
    }

    for (final field in other.fields) {
      if (usedNames.contains(field.name)) {
        if (overwrite) {
          final index = mergedFields.indexWhere((f) => f.name == field.name);
          mergedFields[index] = field;
        }
      } else {
        mergedFields.add(field);
        usedNames.add(field.name);
      }
    }

    return Schema(
      fields: mergedFields,
      metadata: {...metadata, ...other.metadata},
    );
  }

  /// Check if schemas are compatible (per-field [Field.isCompatibleWith], which
  /// guards decimal scale and nested-children shape that [Field.==] ignores).
  bool isCompatibleWith(Schema other) {
    if (fields.length != other.fields.length) return false;
    for (int i = 0; i < fields.length; i++) {
      if (!fields[i].isCompatibleWith(other.fields[i])) return false;
    }
    return true;
  }

  /// EXACT encoding compatibility (see [Field.encodesAs]): same arity and every
  /// field exactly encodable — used by streaming writers whose physical schema is
  /// frozen, so a widened batch is rejected rather than silently truncated.
  bool encodesAs(Schema other) {
    if (fields.length != other.fields.length) return false;
    for (int i = 0; i < fields.length; i++) {
      if (!fields[i].encodesAs(other.fields[i])) return false;
    }
    return true;
  }

  /// Infer schema from a list of records
  static Schema infer(
    List<Map<String, dynamic>> records, {
    bool nullable = true,
    int sampleSize = 100,
  }) {
    if (records.isEmpty) {
      return Schema(fields: []);
    }

    final fieldTypes = <String, DataType>{};
    final sample = records.take(sampleSize);

    for (final record in sample) {
      for (final entry in record.entries) {
        final key = entry.key;
        final value = entry.value;

        if (value == null) {
          continue; // nullability is derived exhaustively below
        }

        final valueType = DataType.fromValue(value);

        if (!fieldTypes.containsKey(key)) {
          fieldTypes[key] = valueType;
        } else {
          final currentType = fieldTypes[key]!;
          if (currentType != valueType) {
            // inferredCommon (not commonType) so a large int mixed with a float
            // promotes to a lossless decimal instead of a rounding float64.
            final common = DataType.inferredCommon(currentType, valueType);
            if (common != null) {
              fieldTypes[key] = common;
            } else {
              fieldTypes[key] = DataType.string;
            }
          }
        }
      }
    }

    // Keys and nullability are derived from ALL records (a cheap key scan), not
    // just the sampled prefix — otherwise a field that first appears (or is first
    // null) beyond [sampleSize] would be silently dropped or mis-typed as
    // non-null. Type inference stays bounded by the sample (the cost contract); a
    // field seen only beyond the sample defaults to string (pass a larger
    // sampleSize for exact typing).
    final allKeys = <String>{};
    for (final record in records) {
      allKeys.addAll(record.keys);
    }
    // A field is non-null only if present AND non-null in EVERY record
    // (`record[k] == null` is true for both an absent key and a null value).
    final required = <String>{...allKeys};
    for (final record in records) {
      required.removeWhere((k) => record[k] == null);
    }

    final fields = <Field>[];
    for (final key in allKeys) {
      var type = fieldTypes[key] ?? DataType.string;
      int? precision;
      int? scale;
      if (type == DataType.decimal) {
        // A decimal promoted from a mixed int/float column: derive the exact
        // scale + precision by scanning ALL values (so no fractional digit is
        // truncated). A non-finite value can't be a decimal — fall back to
        // float64 rather than throwing.
        final meta = inferDecimalMeta([
          for (final record in records) record[key],
        ]);
        if (meta == null) {
          type = DataType.float64;
        } else {
          scale = meta.scale;
          precision = meta.precision;
        }
      }
      fields.add(
        Field(
          name: key,
          dataType: type,
          nullable: nullable || !required.contains(key),
          precision: precision,
          scale: scale,
        ),
      );
    }

    return Schema(fields: fields);
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is Schema &&
          runtimeType == other.runtimeType &&
          _fieldsEqual(fields, other.fields);

  static bool _fieldsEqual(List<Field> a, List<Field> b) {
    if (a.length != b.length) return false;
    for (int i = 0; i < a.length; i++) {
      if (a[i] != b[i]) return false;
    }
    return true;
  }

  @override
  int get hashCode => Object.hashAll(fields);

  @override
  String toString() {
    final buffer = StringBuffer('Schema(\n');
    for (final field in fields) {
      buffer.writeln('  $field');
    }
    buffer.write(')');
    return buffer.toString();
  }

  Map<String, dynamic> toJson() => {
    'fields': fields.map((f) => f.toJson()).toList(),
    if (metadata.isNotEmpty) 'metadata': metadata,
  };

  factory Schema.fromJson(Map<String, dynamic> json) {
    final fieldsList = (json['fields'] as List<dynamic>)
        .map((f) => Field.fromJson(f as Map<String, dynamic>))
        .toList();

    return Schema(
      fields: fieldsList,
      metadata: (json['metadata'] as Map<String, dynamic>?) ?? {},
    );
  }
}
