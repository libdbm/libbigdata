import '../../../core/column.dart';
import '../../../core/data_type.dart';
import '../../../core/schema.dart' as core;
import '../encoding/rle_bitpacked.dart' show levelBitWidth;
import '../schema/schema_converter.dart';
import '../thrift/parquet_thrift.dart' as thrift;

/// One shredded leaf column: physical values (slot-aligned, nulls included — the
/// value encoder skips them, the levels carry presence) plus repetition and
/// definition levels.
class LeafColumn {
  final List<String> path;
  final thrift.ParquetType physicalType;
  final thrift.ConvertedType? convertedType;
  final int? typeLength;
  final int? precision;
  final int? scale;
  final List<dynamic> values;
  final List<int>? repLevels;
  final List<int>? defLevels;
  final int repBitWidth;
  final int defBitWidth;

  LeafColumn({
    required this.path,
    required this.physicalType,
    this.convertedType,
    this.typeLength,
    this.precision,
    this.scale,
    required this.values,
    this.repLevels,
    this.defLevels,
    this.repBitWidth = 0,
    this.defBitWidth = 0,
  });
}

/// Whether [field] needs Dremel shredding (a nested type the writer flattens
/// into multiple leaf columns).
bool isNestedField(core.Field field) =>
    field.dataType == DataType.list ||
    field.dataType == DataType.struct ||
    field.dataType == DataType.map;

({
  thrift.ParquetType type,
  thrift.ConvertedType? converted,
  int? typeLength,
  int? precision,
  int? scale,
})
_leafPhysical(core.Field f) {
  if (f.dataType == DataType.decimal) {
    final precision = f.precision ?? 38;
    final layout = decimalLayout(precision);
    return (
      type: layout.type,
      converted: thrift.ConvertedType.decimal,
      typeLength: layout.typeLength,
      precision: precision,
      scale: f.scale ?? 0,
    );
  }
  final converted = switch (f.dataType) {
    DataType.string => thrift.ConvertedType.utf8,
    DataType.date => thrift.ConvertedType.date,
    DataType.timestamp => thrift.ConvertedType.timestampMicros,
    _ => null,
  };
  return (
    type: SchemaConverter().toParquetType(f.dataType),
    converted: converted,
    typeLength: null,
    precision: null,
    scale: null,
  );
}

dynamic _physical(core.Field f, dynamic v) =>
    toPhysicalValue(f.dataType, f.precision ?? 38, v);

/// A mutable per-leaf accumulator collected during striping.
class _Leaf {
  final List<String> path;
  final core.Field
  field; // the scalar leaf field (physical encoding + finalize)
  final bool opt; // the leaf's own optionality
  final int maxRep;
  final int maxDef;
  final List<dynamic> values = [];
  final List<int> reps = [];
  final List<int> defs = [];
  _Leaf(this.path, this.field, this.opt, this.maxRep, this.maxDef);
}

/// A node in the shred tree (mirrors the Parquet group structure).
sealed class _Node {
  final List<_Leaf> leaves;
  _Node(this.leaves);
}

class _LeafNode extends _Node {
  final _Leaf leaf;
  _LeafNode(this.leaf) : super([leaf]);
}

class _StructNode extends _Node {
  final bool opt;
  final List<({String name, _Node node})> members;
  _StructNode(this.opt, this.members, super.leaves);
}

class _ListNode extends _Node {
  final bool opt;
  final _Node element;
  _ListNode(this.opt, this.element) : super(element.leaves);
}

class _MapNode extends _Node {
  final bool opt;
  final _Node key;
  final _Node value;
  _MapNode(this.opt, this.key, this.value, super.leaves);
}

/// Build the shred tree for [field]. [rep]/[def] are the repetition/definition
/// levels at which this node's PARENT is present (what `dissect` will pass in).
_Node _build(core.Field field, List<String> path, int rep, int def) {
  switch (field.dataType) {
    case DataType.struct:
      final opt = field.nullable;
      final d = def + (opt ? 1 : 0);
      final members = [
        for (final m in field.children)
          (name: m.name, node: _build(m, [...path, m.name], rep, d)),
      ];
      return _StructNode(opt, members, [
        for (final m in members) ...m.node.leaves,
      ]);
    case DataType.list:
      if (field.children.length != 1) {
        throw ArgumentError(
          'list field "${field.name}" must have exactly one element child; '
          'got ${field.children.length}',
        );
      }
      final opt = field.nullable;
      final d = def + (opt ? 1 : 0);
      // The synthetic 'list' level is repeated: +1 rep AND +1 def.
      final element = _build(
        field.children.first,
        [...path, 'list', 'element'],
        rep + 1,
        d + 1,
      );
      return _ListNode(opt, element);
    case DataType.map:
      if (field.children.length != 1 ||
          field.children.first.children.length < 2) {
        throw ArgumentError(
          'map field "${field.name}" must have a single key_value struct child '
          'whose children are [key, value]; got a malformed shape',
        );
      }
      final opt = field.nullable;
      final d = def + (opt ? 1 : 0);
      final entries = field.children.first;
      // The synthetic 'key_value' level is repeated: +1 rep AND +1 def.
      final key = _build(
        entries.children[0],
        [...path, 'key_value', 'key'],
        rep + 1,
        d + 1,
      );
      final value = _build(
        entries.children[1],
        [...path, 'key_value', 'value'],
        rep + 1,
        d + 1,
      );
      return _MapNode(opt, key, value, [...key.leaves, ...value.leaves]);
    default:
      final opt = field.nullable;
      return _LeafNode(_Leaf(path, field, opt, rep, def + (opt ? 1 : 0)));
  }
}

/// Append one null slot (at [rep]/[def]) to EVERY leaf under [node] — used when
/// a container is null or a present-but-empty collection.
void _emitNulls(_Node node, int rep, int def) {
  for (final leaf in node.leaves) {
    leaf.values.add(null);
    leaf.reps.add(rep);
    leaf.defs.add(def);
  }
}

/// Dremel striping: descend [value] through [node], emitting (value, rep, def)
/// to the relevant leaves. [lvl] = number of repeated ancestors entered so far.
void _dissect(_Node node, dynamic value, int rep, int def, int lvl) {
  switch (node) {
    case final _LeafNode n:
      n.leaf.values.add(value == null ? null : _physical(n.leaf.field, value));
      n.leaf.reps.add(rep);
      n.leaf.defs.add(value == null ? def : def + (n.leaf.opt ? 1 : 0));
    case final _StructNode n:
      if (n.opt && value == null) {
        _emitNulls(n, rep, def);
        return;
      }
      final d = def + (n.opt ? 1 : 0);
      final map = value as Map?;
      for (final m in n.members) {
        _dissect(m.node, map == null ? null : map[m.name], rep, d, lvl);
      }
    case final _ListNode n:
      if (n.opt && value == null) {
        _emitNulls(n, rep, def);
        return;
      }
      final d = def + (n.opt ? 1 : 0);
      final list = (value as List?) ?? const [];
      if (list.isEmpty) {
        _emitNulls(n.element, rep, d); // present-but-empty list
        return;
      }
      final myRep = lvl + 1;
      for (int i = 0; i < list.length; i++) {
        _dissect(n.element, list[i], i == 0 ? rep : myRep, d + 1, myRep);
      }
    case final _MapNode n:
      if (n.opt && value == null) {
        _emitNulls(n, rep, def);
        return;
      }
      final d = def + (n.opt ? 1 : 0);
      final map = (value as Map?) ?? const {};
      if (map.isEmpty) {
        _emitNulls(n.key, rep, d); // present-but-empty map
        _emitNulls(n.value, rep, d);
        return;
      }
      final myRep = lvl + 1;
      var i = 0;
      for (final e in map.entries) {
        final cr = i == 0 ? rep : myRep;
        _dissect(n.key, e.key, cr, d + 1, myRep);
        _dissect(n.value, e.value, cr, d + 1, myRep);
        i++;
      }
  }
}

LeafColumn _toLeafColumn(_Leaf leaf) {
  final phys = _leafPhysical(leaf.field);
  return LeafColumn(
    path: leaf.path,
    physicalType: phys.type,
    convertedType: phys.converted,
    typeLength: phys.typeLength,
    precision: phys.precision,
    scale: phys.scale,
    values: leaf.values,
    repLevels: leaf.maxRep > 0 ? leaf.reps : null,
    // Gated exactly like repLevels: a leaf that is REQUIRED all the way to the
    // root has maxDef 0 and therefore no definition levels to carry. Passing
    // them anyway made the encoder emit a zero-width level section (a 4-byte
    // length plus an RLE run) that the decoder — which reads levels only when
    // defLevelBitWidth > 0 — never consumes, so those bytes were decoded as
    // VALUES and every value shifted. pyarrow computes the same maxDef 0 and
    // breaks identically.
    defLevels: leaf.maxDef > 0 ? leaf.defs : null,
    repBitWidth: levelBitWidth(leaf.maxRep),
    defBitWidth: levelBitWidth(leaf.maxDef),
  );
}

/// Shred a top-level nested [field]/[column] into its leaf columns, to ANY depth
/// (list/struct/map composed arbitrarily), via the Dremel striping algorithm.
List<LeafColumn> shred(core.Field field, Column column) {
  if (!isNestedField(field)) {
    throw UnsupportedError('shred called on non-nested ${field.dataType.name}');
  }
  final node = _build(field, [field.name], 0, 0);
  // Index access avoids the whole-column List allocation `toList()` makes (a
  // nested column is boxed already, so per-element access is free of extra copy).
  final n = column.length;
  for (int i = 0; i < n; i++) {
    _dissect(node, column[i], 0, 0, 0);
  }
  return [for (final leaf in node.leaves) _toLeafColumn(leaf)];
}
