import '../../../core/column.dart';
import '../../../core/data_type.dart';
import '../../../core/schema.dart';
import '../schema/schema_levels.dart';
import '../thrift/parquet_thrift.dart' as thrift;

/// Reassembles the flat (dotted-path) leaf columns a Parquet row group decodes
/// into nested `struct`/`list`/`map` columns, driven by the schema tree.
///
/// Works for single-level nesting (struct-of-scalars, list-of-scalars,
/// list-of-struct, map-of-scalars) by combining the already-assembled per-row
/// leaf columns; shapes it can't safely reassemble (e.g. list-of-list,
/// map-of-map) fall back to the original flat leaf columns so reads never break.
class NestedAssembler {
  final List<thrift.SchemaElement> schema;
  final Map<String, Field> _fields = {};
  final Map<String, Column> _columns = {};

  NestedAssembler(this.schema, List<Field> fields, List<Column> columns) {
    for (int i = 0; i < fields.length; i++) {
      _fields[fields[i].name] = fields[i];
      _columns[fields[i].name] = columns[i];
    }
  }

  /// Returns the reconstructed top-level (fields, columns). When nothing nests,
  /// this is the original flat list unchanged.
  (List<Field>, List<Column>) run() {
    final tree = SchemaLevels.buildSchemaTree(schema);
    final fields = <Field>[];
    final columns = <Column>[];
    for (final node in tree.children) {
      // Any reconstruction failure falls back to the flat leaves so reads of
      // unusual/odd-shaped nested files never crash.
      (Field, Column)? built;
      try {
        built = _build(node, node.element.name ?? '');
      } catch (_) {
        built = null;
      }
      if (built != null) {
        fields.add(built.$1);
        columns.add(built.$2);
      } else {
        // Fall back to the flat leaves under this subtree.
        for (final path in _leafPaths(node, node.element.name ?? '')) {
          final field = _fields[path];
          final column = _columns[path];
          if (field != null && column != null) {
            fields.add(field);
            columns.add(column);
          }
        }
      }
    }
    return (fields, columns);
  }

  /// Build one nested (or scalar) column for [node] at dotted [path], or null if
  /// it can't be safely reassembled.
  (Field, Column)? _build(SchemaNode node, String path) {
    if (node.isLeaf) {
      final field = _fields[path];
      final column = _columns[path];
      if (field == null || column == null) return null;
      return (field.copyWith(name: node.element.name), column);
    }
    final kind = _classify(node);
    switch (kind) {
      case _Nesting.struct:
        return _buildStruct(node, path);
      case _Nesting.list:
        return _buildList(node, path);
      case _Nesting.map:
        return _buildMap(node, path);
    }
  }

  (Field, Column)? _buildStruct(SchemaNode node, String path) {
    final fields = <Field>[];
    final columns = <Column>[];
    for (final child in node.children) {
      final built = _build(child, '$path.${child.element.name}');
      if (built == null) return null;
      fields.add(built.$1);
      columns.add(built.$2);
    }
    if (columns.isEmpty) return null;
    final count = columns.first.length;
    final names = [for (final child in fields) child.name];
    final lists = [for (final column in columns) column.toList()];
    final data = [
      for (int r = 0; r < count; r++)
        {for (int j = 0; j < names.length; j++) names[j]: lists[j][r]},
    ];
    final field = Field(
      name: node.element.name ?? '',
      dataType: DataType.struct,
      nullable: true,
      children: fields,
    );
    return (
      field,
      ListColumn(
        name: field.name,
        dataType: DataType.struct,
        data: data,
        nullable: true,
      ),
    );
  }

  (Field, Column)? _buildList(SchemaNode node, String path) {
    // LIST -> repeated group -> element.
    if (node.children.length != 1) return null;
    final repeated = node.children.first;
    if (repeated.children.length != 1) return null;
    final element = repeated.children.first;
    final elementPath =
        '$path.${repeated.element.name}.${element.element.name}';

    if (element.isLeaf) {
      // The leaf column is already assembled into one list per row.
      final column = _columns[elementPath];
      final leaf = _fields[elementPath];
      if (column == null || leaf == null) return null;
      final field = Field(
        name: node.element.name ?? '',
        dataType: DataType.list,
        nullable: true,
        children: [leaf.copyWith(name: element.element.name)],
      );
      return (
        field,
        ListColumn(
          name: field.name,
          dataType: DataType.list,
          data: column.toList(),
          nullable: true,
        ),
      );
    }

    // list-of-struct: each struct leaf is a list per row; zip per row.
    if (_classify(element) != _Nesting.struct) return null;
    final memberFields = <Field>[];
    final memberLists = <List<dynamic>>[];
    for (final member in element.children) {
      if (!member.isLeaf) return null; // deeper nesting -> fall back
      final memberPath = '$elementPath.${member.element.name}';
      final column = _columns[memberPath];
      final leaf = _fields[memberPath];
      if (column == null || leaf == null) return null;
      memberFields.add(leaf.copyWith(name: member.element.name));
      memberLists.add(column.toList());
    }
    final count = memberLists.isEmpty ? 0 : memberLists.first.length;
    final names = [for (final member in memberFields) member.name];
    final data = <dynamic>[];
    for (int r = 0; r < count; r++) {
      final lists = [for (final list in memberLists) list[r] as List?];
      if (lists.any((list) => list == null)) {
        data.add(null);
        continue;
      }
      final length = lists.first!.length;
      data.add([
        for (int i = 0; i < length; i++)
          {for (int j = 0; j < names.length; j++) names[j]: lists[j]![i]},
      ]);
    }
    final field = Field(
      name: node.element.name ?? '',
      dataType: DataType.list,
      nullable: true,
      children: [
        Field(
          name: element.element.name ?? 'element',
          dataType: DataType.struct,
          nullable: true,
          children: memberFields,
        ),
      ],
    );
    return (
      field,
      ListColumn(
        name: field.name,
        dataType: DataType.list,
        data: data,
        nullable: true,
      ),
    );
  }

  (Field, Column)? _buildMap(SchemaNode node, String path) {
    // MAP -> key_value (repeated) -> key, value.
    if (node.children.length != 1) return null;
    final entry = node.children.first;
    if (entry.children.length != 2) return null;
    final keyNode = entry.children[0];
    final valueNode = entry.children[1];
    if (!keyNode.isLeaf || !valueNode.isLeaf) {
      return null; // map-of-nested -> fall back
    }
    final keyPath = '$path.${entry.element.name}.${keyNode.element.name}';
    final valuePath = '$path.${entry.element.name}.${valueNode.element.name}';
    final keyColumn = _columns[keyPath];
    final valueColumn = _columns[valuePath];
    final keyField = _fields[keyPath];
    final valueField = _fields[valuePath];
    if (keyColumn == null ||
        valueColumn == null ||
        keyField == null ||
        valueField == null) {
      return null;
    }
    final keys = keyColumn.toList();
    final values = valueColumn.toList();
    final count = keys.length;
    final data = <dynamic>[];
    for (int r = 0; r < count; r++) {
      final entryKeys = keys[r] as List?;
      final entryValues = values[r] as List?;
      if (entryKeys == null || entryValues == null) {
        data.add(null);
        continue;
      }
      data.add({
        for (int i = 0; i < entryKeys.length; i++) entryKeys[i]: entryValues[i],
      });
    }
    final field = Field(
      name: node.element.name ?? '',
      dataType: DataType.map,
      nullable: true,
      children: [
        Field(
          name: entry.element.name ?? 'key_value',
          dataType: DataType.struct,
          nullable: false,
          children: [
            keyField.copyWith(name: keyNode.element.name),
            valueField.copyWith(name: valueNode.element.name),
          ],
        ),
      ],
    );
    return (
      field,
      ListColumn(
        name: field.name,
        dataType: DataType.map,
        data: data,
        nullable: true,
      ),
    );
  }

  _Nesting _classify(SchemaNode node) {
    final converted = node.element.convertedType;
    if (converted == thrift.ConvertedType.list ||
        node.element.logicalType?.list != null) {
      return _Nesting.list;
    }
    if (converted == thrift.ConvertedType.map ||
        converted == thrift.ConvertedType.mapKeyValue ||
        node.element.logicalType?.map != null) {
      return _Nesting.map;
    }
    return _Nesting.struct;
  }

  Iterable<String> _leafPaths(SchemaNode node, String path) sync* {
    if (node.isLeaf) {
      yield path;
      return;
    }
    for (final child in node.children) {
      yield* _leafPaths(child, '$path.${child.element.name}');
    }
  }
}

enum _Nesting { struct, list, map }
