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

  /// Extract (precision, scale) from a Spark "decimal(p,s)" type string.
  static ({int precision, int scale}) decimalParams(String type) =>
      decimalTypeParams(type);

  static DataType parseType(Object? type) {
    if (type is! String) return DataType.string; // nested types -> fallback
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
  static String toJson(Schema schema) {
    return jsonEncode({
      'type': 'struct',
      'fields': [
        for (final f in schema.fields)
          {
            'name': f.name,
            'type': typeName(
              f.dataType,
              precision: f.precision,
              scale: f.scale,
            ),
            'nullable': f.nullable,
            // Round-trip whatever another engine attached (invariants,
            // generation expressions, comments). Emitting an empty map here
            // would silently STRIP them on every overwrite, and would also make
            // an append's schemaString comparison spuriously mismatch.
            'metadata': f.metadata,
          },
      ],
    });
  }

  /// Whether two Delta `schemaString`s describe the same DATA, ignoring field
  /// metadata at every level.
  ///
  /// Metadata carries invariants, generation expressions and comments another
  /// engine attached — not the shape of the data — and a [Schema] built in Dart
  /// carries none of it. Comparing the raw strings therefore made an append to
  /// ANY metadata-carrying table (i.e. any Spark-authored one) fail with a
  /// "schema mismatch" whose two blobs differed only inside `metadata`.
  /// Structure (including nested fields, order, type and nullability) is still
  /// compared exactly.
  static bool sameStructure(String a, String b) =>
      jsonEncode(_withoutMetadata(jsonDecode(a))) ==
      jsonEncode(_withoutMetadata(jsonDecode(b)));

  static Object? _withoutMetadata(Object? node) {
    if (node is Map) {
      return {
        for (final entry in node.entries)
          if (entry.key != 'metadata') entry.key: _withoutMetadata(entry.value),
      };
    }
    if (node is List) return [for (final e in node) _withoutMetadata(e)];
    return node;
  }

  /// Parse a Delta `schemaString` into a [Schema].
  static Schema fromJson(String schemaString) {
    final root = jsonDecode(schemaString) as Map<String, dynamic>;
    final fields = (root['fields'] as List).cast<Map<String, dynamic>>();
    return Schema(
      fields: [
        for (final f in fields)
          _field(
            f['name'] as String,
            f['type'],
            f['nullable'] as bool? ?? true,
            (f['metadata'] as Map?)?.cast<String, dynamic>() ?? const {},
          ),
      ],
    );
  }

  /// Build a [Field], attaching precision/scale when the type is decimal(p,s).
  static Field _field(
    String name,
    Object? type,
    bool nullable,
    Map<String, dynamic> metadata,
  ) {
    final dt = parseType(type);
    if (dt == DataType.decimal && type is String) {
      final p = decimalParams(type);
      return Field(
        name: name,
        dataType: dt,
        nullable: nullable,
        precision: p.precision,
        scale: p.scale,
        metadata: metadata,
      );
    }
    return Field(
      name: name,
      dataType: dt,
      nullable: nullable,
      metadata: metadata,
    );
  }
}
