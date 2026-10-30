import 'dart:convert';

import '../../core/conversions.dart' show decimalTypeParams;
import '../../core/data_type.dart';
import '../../core/schema.dart';

/// Mapping between libbigdata [DataType]s and Delta/Spark schema type names, and
/// (de)serialization of the Delta `schemaString` (a Spark StructType JSON).
class DeltaSchema {
  static String typeName(DataType type, {int? precision, int? scale}) {
    switch (type) {
      case DataType.int8:
        return 'byte';
      case DataType.int16:
        return 'short';
      case DataType.int32:
        return 'integer';
      case DataType.int64:
        return 'long';
      case DataType.float32:
        return 'float';
      case DataType.float64:
        return 'double';
      case DataType.boolean:
        return 'boolean';
      case DataType.string:
        return 'string';
      case DataType.binary:
        return 'binary';
      case DataType.date:
        return 'date';
      case DataType.timestamp:
        return 'timestamp';
      case DataType.decimal:
        // Spark StructType represents decimal as "decimal(P,S)".
        return 'decimal(${precision ?? 38},${scale ?? 0})';
      default:
        return 'string';
    }
  }

  static DataType parseType(Object? type) {
    if (type is Map) {
      switch (type['type']) {
        case 'struct':
          return DataType.struct;
        case 'array':
          return DataType.list;
        case 'map':
          return DataType.map;
      }
      throw FormatException('Unknown Delta type ${jsonEncode(type)}');
    }
    if (type is! String) return DataType.string;
    if (type.startsWith('decimal')) return DataType.decimal;
    switch (type) {
      case 'byte':
      case 'tinyint':
        return DataType.int8;
      case 'short':
      case 'smallint':
        return DataType.int16;
      case 'integer':
      case 'int':
        return DataType.int32;
      case 'long':
      case 'bigint':
        return DataType.int64;
      case 'float':
        return DataType.float32;
      case 'double':
        return DataType.float64;
      case 'boolean':
        return DataType.boolean;
      case 'binary':
        return DataType.binary;
      case 'date':
        return DataType.date;
      case 'timestamp':
      case 'timestamp_ntz':
        return DataType.timestamp;
      case 'string':
      default:
        return DataType.string;
    }
  }

  /// Serialize a [Schema] to a Delta `schemaString` (Spark StructType JSON).
  static String toJson(Schema schema) => jsonEncode(_struct(schema.fields));

  static Map<String, dynamic> _struct(List<Field> fields) => {
    'type': 'struct',
    'fields': [
      for (final field in fields)
        {
          'name': field.name,
          'type': _type(field),
          'nullable': field.nullable,
          // Round-trip whatever another engine attached (invariants,
          // generation expressions, comments). Emitting an empty map here
          // would silently STRIP them on every overwrite, and would also make
          // an append's schemaString comparison spuriously mismatch.
          'metadata': field.metadata,
        },
    ],
  };

  /// The Spark type of [field]: a name for a scalar, an object for a struct,
  /// array (one `element` child) or map (one `key_value` struct child holding
  /// `key` and `value`).
  static Object _type(Field field) {
    switch (field.dataType) {
      case DataType.struct:
        return _struct(field.children);
      case DataType.list:
        final element = field.children.single;
        return {
          'type': 'array',
          'elementType': _type(element),
          'containsNull': element.nullable,
        };
      case DataType.map:
        final [key, value] = field.children.single.children;
        return {
          'type': 'map',
          'keyType': _type(key),
          'valueType': _type(value),
          'valueContainsNull': value.nullable,
        };
      default:
        return typeName(
          field.dataType,
          precision: field.precision,
          scale: field.scale,
        );
    }
  }

  /// Whether two Delta `schemaString`s describe the same DATA, ignoring field
  /// metadata at every level.
  ///
  /// Metadata carries invariants, generation expressions and comments another
  /// engine attached (not the shape of the data) and a [Schema] built in Dart
  /// carries none of it. Comparing the raw strings therefore made an append to
  /// ANY metadata-carrying table (i.e. any Spark-authored one) fail with a
  /// "schema mismatch" whose two blobs differed only inside `metadata`.
  /// Structure (including nested fields, order, type and nullability) is still
  /// compared exactly.
  static bool sameStructure(String a, String b) =>
      jsonEncode(_withoutMetadata(jsonDecode(a))) ==
      jsonEncode(_withoutMetadata(jsonDecode(b)));

  /// [incoming] with each top-level field that carries no metadata of its
  /// own taking the metadata of [previous]'s field of the same name and
  /// structure, so a rewrite keeps the invariants, generation expressions and
  /// comments another engine attached to a column it did not change. A schema
  /// built in Dart carries none of them.
  static String carry(String previous, String incoming) {
    final prior = {
      for (final field in (jsonDecode(previous)['fields'] as List).cast<Map>())
        field['name']: field,
    };
    final root = jsonDecode(incoming) as Map<String, dynamic>;
    root['fields'] = [
      for (final field in (root['fields'] as List).cast<Map<String, dynamic>>())
        if (prior[field['name']] case final old?
            when (field['metadata'] as Map?)?.isEmpty != false &&
                jsonEncode(_withoutMetadata(old)) ==
                    jsonEncode(_withoutMetadata(field)))
          {...field, 'metadata': old['metadata']}
        else
          field,
    ];
    return jsonEncode(root);
  }

  static Object? _withoutMetadata(Object? node) {
    if (node is Map) {
      return {
        for (final entry in node.entries)
          if (entry.key != 'metadata') entry.key: _withoutMetadata(entry.value),
      };
    }
    if (node is List) return [for (final item in node) _withoutMetadata(item)];
    return node;
  }

  /// Parse a Delta `schemaString` into a [Schema].
  static Schema fromJson(String schemaString) {
    final root = jsonDecode(schemaString) as Map<String, dynamic>;
    final fields = (root['fields'] as List).cast<Map<String, dynamic>>();
    return Schema(
      fields: [
        for (final field in fields)
          _field(
            field['name'] as String,
            field['type'],
            field['nullable'] as bool? ?? true,
            (field['metadata'] as Map?)?.cast<String, dynamic>() ?? const {},
          ),
      ],
    );
  }

  /// Build a [Field], attaching precision/scale when the type is decimal(p,s)
  /// and children when it is a struct, array or map.
  static Field _field(
    String name,
    Object? type,
    bool nullable,
    Map<String, dynamic> metadata,
  ) {
    final kind = parseType(type);
    if (type is Map) {
      final children = switch (kind) {
        DataType.struct => [
          for (final field in (type['fields'] as List).cast<Map>())
            _field(
              field['name'] as String,
              field['type'],
              field['nullable'] as bool? ?? true,
              (field['metadata'] as Map?)?.cast<String, dynamic>() ?? const {},
            ),
        ],
        DataType.list => [
          _field(
            'element',
            type['elementType'],
            type['containsNull'] as bool? ?? true,
            const {},
          ),
        ],
        _ => [
          Field(
            name: 'key_value',
            dataType: DataType.struct,
            nullable: false,
            children: [
              _field('key', type['keyType'], false, const {}),
              _field(
                'value',
                type['valueType'],
                type['valueContainsNull'] as bool? ?? true,
                const {},
              ),
            ],
          ),
        ],
      };
      return Field(
        name: name,
        dataType: kind,
        nullable: nullable,
        metadata: metadata,
        children: children,
      );
    }
    if (kind == DataType.decimal && type is String) {
      final decimal = decimalTypeParams(type);
      return Field(
        name: name,
        dataType: kind,
        nullable: nullable,
        precision: decimal.precision,
        scale: decimal.scale,
        metadata: metadata,
      );
    }
    return Field(
      name: name,
      dataType: kind,
      nullable: nullable,
      metadata: metadata,
    );
  }
}
