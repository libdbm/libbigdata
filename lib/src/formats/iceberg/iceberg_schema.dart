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

  /// Extract (precision, scale) from an Iceberg "decimal(p,s)" type string.
  static ({int precision, int scale}) decimalParams(String type) =>
      decimalTypeParams(type);

  static Schema fromJson(Map<String, dynamic> schemaJson) {
    final fields = (schemaJson['fields'] as List).cast<Map<String, dynamic>>();
    return Schema(fields: [for (final f in fields) _field(f)]);
  }

  /// Build a [Field] from an Iceberg field map, attaching precision/scale for
  /// decimal(p,s) and recursing into nested type objects (struct/list/map).
  static Field _field(Map<String, dynamic> f) {
    final nullable = !((f['required'] as bool?) ?? false);
    // Carry the Iceberg field-id so the reader can resolve a data-file column by
    // id (a renamed column keeps its id but changes its physical name).
    return _fieldOfType(
      f['name'] as String,
      f['type'],
      nullable,
      f['id'] as int?,
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
      final t = type['type'];
      if (t == 'struct') {
        return Field(
          name: name,
          dataType: DataType.struct,
          nullable: nullable,
          id: id,
          children: [
            for (final c in (type['fields'] as List).cast<Map>())
              _fieldOfType(
                c['name'] as String,
                c['type'],
                !((c['required'] as bool?) ?? false),
                c['id'] as int?,
              ),
          ],
        );
      }
      if (t == 'list') {
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
      if (t == 'map') {
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
    final dt = parseType(type);
    if (dt == DataType.decimal && type is String) {
      final p = decimalParams(type);
      return Field(
        name: name,
        dataType: dt,
        nullable: nullable,
        precision: p.precision,
        scale: p.scale,
        id: id,
      );
    }
    return Field(name: name, dataType: dt, nullable: nullable, id: id);
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
  static Map<String, dynamic> toJson(Schema schema, {int schemaId = 0}) =>
      build(schema, schemaId: schemaId).json;

  /// Assign Iceberg field-ids and produce BOTH the schema JSON and an id-stamped
  /// [Schema] (used to tag the Parquet data file). The two share one id counter
  /// so the metadata ids and the data-file field-ids agree (depth-first: all
  /// top-level fields first, then nested ids).
  static ({Map<String, dynamic> json, Schema idSchema}) build(
    Schema schema, {
    int schemaId = 0,
    Map<String, int>? reuseIds,
  }) {
    // On append we REUSE the table's existing field-ids (Iceberg projects by id;
    // regenerating 1..N here while the metadata keeps the old ids makes external
    // engines bind the wrong columns). [reuseIds] maps a dotted field PATH
    // (`col`, `col.child`, `col.element`, `col.key`, `col.value`) to its id; a
    // field with no entry (a genuinely new column) gets the next fresh id.
    var next = reuseIds == null || reuseIds.isEmpty
        ? 1
        : reuseIds.values.reduce((a, b) => a > b ? a : b) + 1;
    int idFor(String path) => reuseIds?[path] ?? next++;
    final jsons = <Map<String, dynamic>>[];
    final idFields = <Field>[];
    for (final f in schema.fields) {
      final id = idFor(f.name);
      final r = _emit(f, id, idFor, f.name);
      jsons.add({
        'id': id,
        'name': f.name,
        'required': !f.nullable,
        'type': r.type,
      });
      idFields.add(r.field);
    }
    return (
      json: {'type': 'struct', 'schema-id': schemaId, 'fields': jsons},
      idSchema: Schema(fields: idFields),
    );
  }

  /// A dotted-path → field-id map extracted from an Iceberg schema JSON (the
  /// table's current schema), so an append can pass it as [build]'s `reuseIds`
  /// and keep the data-file field-ids in lock-step with the metadata ids.
  static Map<String, int> idMap(Map<String, dynamic> schemaJson) {
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
      for (final f in fields) {
        final path = prefix.isEmpty
            ? f['name'] as String
            : '$prefix.${f['name']}';
        map[path] = f['id'] as int;
        walkType(f['type'], path);
      }
    };

    walkFields((schemaJson['fields'] as List).cast<Map<String, dynamic>>(), '');
    return map;
  }

  /// Emit one field's Iceberg `type` JSON (a string for scalars, an object for
  /// nested) plus the id-stamped [Field]. [id] is already assigned to this field.
  static ({Object type, Field field}) _emit(
    Field f,
    int id,
    int Function(String) idFor,
    String path,
  ) {
    switch (f.dataType) {
      case DataType.struct:
        final childJsons = <Map<String, dynamic>>[];
        final childFields = <Field>[];
        for (final c in f.children) {
          final cpath = '$path.${c.name}';
          final cid = idFor(cpath);
          final r = _emit(c, cid, idFor, cpath);
          childJsons.add({
            'id': cid,
            'name': c.name,
            'required': !c.nullable,
            'type': r.type,
          });
          childFields.add(r.field);
        }
        return (
          type: {'type': 'struct', 'fields': childJsons},
          field: f.copyWith(id: id, children: childFields),
        );
      case DataType.list:
        final element = f.children.first;
        final eid = idFor('$path.element');
        final r = _emit(element, eid, idFor, '$path.element');
        return (
          type: {
            'type': 'list',
            'element-id': eid,
            'element-required': !element.nullable,
            'element': r.type,
          },
          field: f.copyWith(id: id, children: [r.field]),
        );
      case DataType.map:
        final entries = f.children.first; // key_value struct(key, value)
        final kid = idFor('$path.key'), vid = idFor('$path.value');
        final kr = _emit(entries.children[0], kid, idFor, '$path.key');
        final vr = _emit(entries.children[1], vid, idFor, '$path.value');
        return (
          type: {
            'type': 'map',
            'key-id': kid,
            'key': kr.type,
            'value-id': vid,
            'value-required': !entries.children[1].nullable,
            'value': vr.type,
          },
          // The key_value wrapper itself carries no Iceberg id.
          field: f.copyWith(
            id: id,
            children: [
              entries.copyWith(children: [kr.field, vr.field]),
            ],
          ),
        );
      default:
        return (
          type: typeName(f.dataType, precision: f.precision, scale: f.scale),
          field: f.copyWith(id: id),
        );
    }
  }
}
