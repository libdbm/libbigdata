import '../../core/conversions.dart' show decimalTypeParams;
import '../../core/data_type.dart';
import '../../core/schema.dart';

/// Maps an Iceberg table schema (`{"type":"struct","fields":[{id,name,type,
/// required}]}`) to/from a libbigdata [Schema]. Iceberg uses field IDs and a
/// `required` flag (the inverse of nullable).
class IcebergSchema {
  static DataType parseType(Object? type) {
    if (type is! String) return DataType.string; // struct/list/map -> fallback
    if (type.startsWith('decimal')) return DataType.decimal;
    if (type.startsWith('fixed')) return DataType.binary;
    switch (type) {
      case 'boolean':
        return DataType.boolean;
      case 'int':
        return DataType.int32;
      case 'long':
        return DataType.int64;
      case 'float':
        return DataType.float32;
      case 'double':
        return DataType.float64;
      case 'date':
        return DataType.date;
      case 'timestamp':
      case 'timestamptz':
      case 'timestamp_ns':
      case 'timestamptz_ns':
        return DataType.timestamp;
      case 'binary':
        return DataType.binary;
      case 'string':
      case 'uuid':
      case 'time':
      default:
        return DataType.string;
    }
  }

  static Schema fromJson(Map<String, dynamic> json) {
    final fields = (json['fields'] as List).cast<Map<String, dynamic>>();
    return Schema(fields: [for (final field in fields) _field(field)]);
  }

  /// Build a [Field] from an Iceberg field map, attaching precision/scale for
  /// decimal(p,s) and recursing into nested type objects (struct/list/map).
  static Field _field(Map<String, dynamic> json) {
    final nullable = !((json['required'] as bool?) ?? false);
    // Carry the Iceberg field-id so the reader can resolve a data-file column by
    // id (a renamed column keeps its id but changes its physical name).
    return _fieldOfType(
      json['name'] as String,
      json['type'],
      nullable,
      json['id'] as int?,
    );
  }

  /// Build a [Field] named [name] of nullability [nullable] and field-[id] from
  /// an Iceberg type (a string for scalars, or an object for struct/list/map).
  static Field _fieldOfType(
    String name,
    Object? type,
    bool nullable, [
    int? id,
  ]) {
    if (type is Map) {
      final kind = type['type'];
      if (kind == 'struct') {
        return Field(
          name: name,
          dataType: DataType.struct,
          nullable: nullable,
          id: id,
          children: [
            for (final child in (type['fields'] as List).cast<Map>())
              _fieldOfType(
                child['name'] as String,
                child['type'],
                !((child['required'] as bool?) ?? false),
                child['id'] as int?,
              ),
          ],
        );
      }
      if (kind == 'list') {
        return Field(
          name: name,
          dataType: DataType.list,
          nullable: nullable,
          id: id,
          children: [
            _fieldOfType(
              'element',
              type['element'],
              !((type['element-required'] as bool?) ?? false),
              type['element-id'] as int?,
            ),
          ],
        );
      }
      if (kind == 'map') {
        return Field(
          name: name,
          dataType: DataType.map,
          nullable: nullable,
          id: id,
          children: [
            Field(
              name: 'key_value',
              dataType: DataType.struct,
              nullable: false,
              children: [
                _fieldOfType('key', type['key'], false, type['key-id'] as int?),
                _fieldOfType(
                  'value',
                  type['value'],
                  !((type['value-required'] as bool?) ?? false),
                  type['value-id'] as int?,
                ),
              ],
            ),
          ],
        );
      }
    }
    final parsed = parseType(type);
    if (parsed == DataType.decimal && type is String) {
      final decimal = decimalTypeParams(type);
      return Field(
        name: name,
        dataType: parsed,
        nullable: nullable,
        precision: decimal.precision,
        scale: decimal.scale,
        id: id,
      );
    }
    return Field(name: name, dataType: parsed, nullable: nullable, id: id);
  }

  static String typeName(DataType type, {int? precision, int? scale}) {
    switch (type) {
      case DataType.int8:
      case DataType.int16:
      case DataType.int32:
        return 'int';
      case DataType.int64:
        return 'long';
      case DataType.float32:
        return 'float';
      case DataType.float64:
        return 'double';
      case DataType.boolean:
        return 'boolean';
      case DataType.binary:
        return 'binary';
      case DataType.date:
        return 'date';
      case DataType.timestamp:
        return 'timestamp';
      case DataType.decimal:
        // Iceberg represents decimal as the string "decimal(P,S)".
        return 'decimal(${precision ?? 38},${scale ?? 0})';
      default:
        return 'string';
    }
  }

  /// Serialize a [Schema] to an Iceberg schema object (field IDs 1..N, nested
  /// types fully described).
  static Map<String, dynamic> toJson(
    Schema schema, {
    int schemaId = 0,
    Map<String, int>? reuseIds,
    int lastId = 0,
  }) => build(
    schema,
    schemaId: schemaId,
    reuseIds: reuseIds,
    lastId: lastId,
  ).json;

  /// Assign Iceberg field-ids and produce BOTH the schema JSON and an id-stamped
  /// [Schema] (used to tag the Parquet data file). The two share one id counter
  /// so the metadata ids and the data-file field-ids agree (depth-first: all
  /// top-level fields first, then nested ids).
  static ({Map<String, dynamic> json, Schema idSchema}) build(
    Schema schema, {
    int schemaId = 0,
    Map<String, int>? reuseIds,
    int lastId = 0,
  }) {
    // On append and overwrite we REUSE the table's existing field-ids (Iceberg
    // projects by id; regenerating 1..N here while the metadata keeps the old
    // ids makes external engines bind the wrong columns). [reuseIds] maps a
    // dotted field PATH (`col`, `col.child`, `col.element`, `col.key`,
    // `col.value`) to its id; a field with no entry (a genuinely new column)
    // gets the next fresh id, above both [reuseIds] and [lastId] (the table's
    // `last-column-id`, which also covers ids of dropped columns).
    var next = lastId + 1;
    for (final id in reuseIds?.values ?? const <int>[]) {
      if (id >= next) next = id + 1;
    }
    int idFor(String path) => reuseIds?[path] ?? next++;
    final jsons = <Map<String, dynamic>>[];
    final stamped = <Field>[];
    for (final field in schema.fields) {
      final id = idFor(field.name);
      final emitted = _emit(field, id, idFor, field.name);
      jsons.add({
        'id': id,
        'name': field.name,
        'required': !field.nullable,
        'type': emitted.type,
      });
      stamped.add(emitted.field);
    }
    return (
      json: {'type': 'struct', 'schema-id': schemaId, 'fields': jsons},
      idSchema: Schema(fields: stamped),
    );
  }

  /// The ids of [current] (the table's schema JSON) that [schema] may keep on
  /// an overwrite: those of each top-level column whose name and type are
  /// unchanged, nested ids included. A column whose type changed is a new
  /// column to Iceberg and gets fresh ids, since reusing an id across an
  /// incompatible type breaks every reader that binds old files by id.
  static Map<String, int> reuseFor(
    Map<String, dynamic> current,
    Schema schema,
  ) {
    final ids = idMap(current);
    final old = {
      for (final field
          in (current['fields'] as List).cast<Map<String, dynamic>>())
        field['name'] as String: '${field['type']}',
    };
    final built = (build(schema, reuseIds: ids).json['fields'] as List)
        .cast<Map<String, dynamic>>();
    for (final field in built) {
      final name = field['name'] as String;
      if (old[name] != '${field['type']}') {
        ids.removeWhere((path, _) => path == name || path.startsWith('$name.'));
      }
    }
    return ids;
  }

  /// A dotted-path → field-id map extracted from an Iceberg schema JSON (the
  /// table's current schema), so an append can pass it as [build]'s `reuseIds`
  /// and keep the data-file field-ids in lock-step with the metadata ids.
  static Map<String, int> idMap(Map<String, dynamic> json) {
    final map = <String, int>{};
    late final void Function(List<Map<String, dynamic>>, String) walkFields;
    void walkType(Object? type, String path) {
      if (type is! Map) return;
      switch (type['type']) {
        case 'struct':
          walkFields(
            (type['fields'] as List).cast<Map<String, dynamic>>(),
            path,
          );
        case 'list':
          map['$path.element'] = type['element-id'] as int;
          walkType(type['element'], '$path.element');
        case 'map':
          map['$path.key'] = type['key-id'] as int;
          map['$path.value'] = type['value-id'] as int;
          walkType(type['key'], '$path.key');
          walkType(type['value'], '$path.value');
      }
    }

    walkFields = (fields, prefix) {
      for (final field in fields) {
        final path = prefix.isEmpty
            ? field['name'] as String
            : '$prefix.${field['name']}';
        map[path] = field['id'] as int;
        walkType(field['type'], path);
      }
    };

    walkFields((json['fields'] as List).cast<Map<String, dynamic>>(), '');
    return map;
  }

  /// Emit one field's Iceberg `type` JSON (a string for scalars, an object for
  /// nested) plus the id-stamped [Field]. [id] is already assigned to this field.
  static ({Object type, Field field}) _emit(
    Field field,
    int id,
    int Function(String) idFor,
    String path,
  ) {
    switch (field.dataType) {
      case DataType.struct:
        final fields = <Map<String, dynamic>>[];
        final children = <Field>[];
        for (final child in field.children) {
          final nested = '$path.${child.name}';
          final assigned = idFor(nested);
          final emitted = _emit(child, assigned, idFor, nested);
          fields.add({
            'id': assigned,
            'name': child.name,
            'required': !child.nullable,
            'type': emitted.type,
          });
          children.add(emitted.field);
        }
        return (
          type: {'type': 'struct', 'fields': fields},
          field: field.copyWith(id: id, children: children),
        );
      case DataType.list:
        final element = field.children.first;
        final assigned = idFor('$path.element');
        final emitted = _emit(element, assigned, idFor, '$path.element');
        return (
          type: {
            'type': 'list',
            'element-id': assigned,
            'element-required': !element.nullable,
            'element': emitted.type,
          },
          field: field.copyWith(id: id, children: [emitted.field]),
        );
      case DataType.map:
        final entries = field.children.first; // key_value struct(key, value)
        final keyId = idFor('$path.key'), valueId = idFor('$path.value');
        final key = _emit(entries.children[0], keyId, idFor, '$path.key');
        final value = _emit(entries.children[1], valueId, idFor, '$path.value');
        return (
          type: {
            'type': 'map',
            'key-id': keyId,
            'key': key.type,
            'value-id': valueId,
            'value-required': !entries.children[1].nullable,
            'value': value.type,
          },
          // The key_value wrapper itself carries no Iceberg id.
          field: field.copyWith(
            id: id,
            children: [
              entries.copyWith(children: [key.field, value.field]),
            ],
          ),
        );
      default:
        return (
          type: typeName(
            field.dataType,
            precision: field.precision,
            scale: field.scale,
          ),
          field: field.copyWith(id: id),
        );
    }
  }
}
