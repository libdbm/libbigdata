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
  final Map<String, Field> _leafField = {};
  final Map<String, Column> _leafColumn = {};

  NestedAssembler(
    this.schema,
    List<Field> leafFields,
    List<Column> leafColumns,
  ) {
    for (int i = 0; i < leafFields.length; i++) {
      _leafField[leafFields[i].name] = leafFields[i];
      _leafColumn[leafFields[i].name] = leafColumns[i];
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
          final f = _leafField[path];
          final c = _leafColumn[path];
          if (f != null && c != null) {
            fields.add(f);
            columns.add(c);
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
      final field = _leafField[path];
      final column = _leafColumn[path];
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
    final childFields = <Field>[];
    final childCols = <Column>[];
    for (final child in node.children) {
      final built = _build(child, '$path.${child.element.name}');
      if (built == null) return null;
      childFields.add(built.$1);
      childCols.add(built.$2);
    }
    if (childCols.isEmpty) return null;
    final n = childCols.first.length;
    final names = [for (final f in childFields) f.name];
    final lists = [for (final c in childCols) c.toList()];
    final data = [
      for (int r = 0; r < n; r++)
        {for (int j = 0; j < names.length; j++) names[j]: lists[j][r]},
    ];
    final field = Field(
      name: node.element.name ?? '',
      dataType: DataType.struct,
      nullable: true,
      children: childFields,
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
    final elemPath = '$path.${repeated.element.name}.${element.element.name}';

    if (element.isLeaf) {
      // The leaf column is already assembled into one list per row.
      final column = _leafColumn[elemPath];
      final lf = _leafField[elemPath];
      if (column == null || lf == null) return null;
      final field = Field(
        name: node.element.name ?? '',
        dataType: DataType.list,
        nullable: true,
        children: [lf.copyWith(name: element.element.name)],
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
    for (final m in element.children) {
      if (!m.isLeaf) return null; // deeper nesting -> fall back
      final mp = '$elemPath.${m.element.name}';
      final column = _leafColumn[mp];
      final lf = _leafField[mp];
      if (column == null || lf == null) return null;
      memberFields.add(lf.copyWith(name: m.element.name));
      memberLists.add(column.toList());
    }
    final n = memberLists.isEmpty ? 0 : memberLists.first.length;
    final names = [for (final f in memberFields) f.name];
    final data = <dynamic>[];
    for (int r = 0; r < n; r++) {
      final rowLists = [for (final ml in memberLists) ml[r] as List?];
      if (rowLists.any((l) => l == null)) {
        data.add(null);
        continue;
      }
      final length = rowLists.first!.length;
      data.add([
        for (int i = 0; i < length; i++)
          {for (int j = 0; j < names.length; j++) names[j]: rowLists[j]![i]},
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
    final kv = node.children.first;
    if (kv.children.length != 2) return null;
    final keyNode = kv.children[0];
    final valNode = kv.children[1];
    if (!keyNode.isLeaf || !valNode.isLeaf) {
      return null; // map-of-nested -> fall back
    }
    final keyPath = '$path.${kv.element.name}.${keyNode.element.name}';
    final valPath = '$path.${kv.element.name}.${valNode.element.name}';
    final keyCol = _leafColumn[keyPath];
    final valCol = _leafColumn[valPath];
    final keyF = _leafField[keyPath];
    final valF = _leafField[valPath];
    if (keyCol == null || valCol == null || keyF == null || valF == null) {
      return null;
    }
    final keys = keyCol.toList();
    final vals = valCol.toList();
    final n = keys.length;
    final data = <dynamic>[];
    for (int r = 0; r < n; r++) {
      final ks = keys[r] as List?;
      final vs = vals[r] as List?;
      if (ks == null || vs == null) {
        data.add(null);
        continue;
      }
      data.add({for (int i = 0; i < ks.length; i++) ks[i]: vs[i]});
    }
    final field = Field(
      name: node.element.name ?? '',
      dataType: DataType.map,
      nullable: true,
      children: [
        Field(
          name: kv.element.name ?? 'key_value',
          dataType: DataType.struct,
          nullable: false,
          children: [
            keyF.copyWith(name: keyNode.element.name),
            valF.copyWith(name: valNode.element.name),
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
    final ct = node.element.convertedType;
    if (ct == thrift.ConvertedType.list ||
        node.element.logicalType?.list != null) {
      return _Nesting.list;
    }
    if (ct == thrift.ConvertedType.map ||
        ct == thrift.ConvertedType.mapKeyValue ||
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
