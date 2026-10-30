import '../../../core/column.dart';
import '../../../core/data_type.dart';
import '../../../core/schema.dart' as core;
import '../encoding/rle_bitpacked.dart' show levelBitWidth;
import '../schema/schema_converter.dart';
import '../thrift/parquet_thrift.dart' as thrift;
import '../../../core/failure.dart';
import '../../../core/result.dart';

/// One shredded leaf column: physical values (slot-aligned, nulls included; the
/// value encoder skips them, the levels carry presence) plus repetition and
/// definition levels.
class LeafColumn {
  final List<String> path;
  final thrift.ParquetType physicalType;
  final List<dynamic> values;
  final List<int>? repLevels;
  final List<int>? defLevels;
  final int repBitWidth;
  final int defBitWidth;

  LeafColumn({
    required this.path,
    required this.physicalType,
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

thrift.ParquetType _leafPhysical(core.Field field) =>
    field.dataType == DataType.decimal
    ? decimalLayout(field.precision ?? 38).type
    : SchemaConverter().toParquetType(field.dataType);

dynamic _physical(core.Field field, dynamic value) =>
    toPhysicalValue(field.dataType, field.precision ?? 38, value);

/// A mutable per-leaf accumulator collected during striping.
class _Leaf {
  final List<String> path;
  final core.Field
  field; // the scalar leaf field (physical encoding + finalize)
  final bool optional; // the leaf's own optionality
  final int maxRep;
  final int maxDef;
  final List<dynamic> values = [];
  final List<int> reps = [];
  final List<int> defs = [];
  _Leaf(this.path, this.field, this.optional, this.maxRep, this.maxDef);
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
  final bool optional;
  final List<({String name, _Node node})> members;
  _StructNode(this.optional, this.members, super.leaves);
}

class _ListNode extends _Node {
  final bool optional;
  final _Node element;
  _ListNode(this.optional, this.element) : super(element.leaves);
}

class _MapNode extends _Node {
  final bool optional;
  final _Node key;
  final _Node value;
  _MapNode(this.optional, this.key, this.value, super.leaves);
}

/// Build the shred tree for [field]. [repetition]/[definition] are the repetition/definition
/// levels at which this node's PARENT is present (what `dissect` will pass in).
_Node _build(
  core.Field field,
  List<String> path,
  int repetition,
  int definition,
) {
  switch (field.dataType) {
    case DataType.struct:
      final optional = field.nullable;
      final present = definition + (optional ? 1 : 0);
      final members = [
        for (final member in field.children)
          (
            name: member.name,
            node: _build(member, [...path, member.name], repetition, present),
          ),
      ];
      return _StructNode(optional, members, [
        for (final member in members) ...member.node.leaves,
      ]);
    case DataType.list:
      if (field.children.length != 1) {
        throw Failure.of(
          Kind.invalid,
          'list field "${field.name}" must have exactly one element child; '
          'got ${field.children.length}',
        );
      }
      final optional = field.nullable;
      final present = definition + (optional ? 1 : 0);
      // The synthetic 'list' level is repeated: +1 rep AND +1 def.
      final element = _build(
        field.children.first,
        [...path, 'list', 'element'],
        repetition + 1,
        present + 1,
      );
      return _ListNode(optional, element);
    case DataType.map:
      if (field.children.length != 1 ||
          field.children.first.children.length < 2) {
        throw Failure.of(
          Kind.invalid,
          'map field "${field.name}" must have a single key_value struct child '
          'whose children are [key, value]; got a malformed shape',
        );
      }
      final optional = field.nullable;
      final present = definition + (optional ? 1 : 0);
      final entries = field.children.first;
      // The synthetic 'key_value' level is repeated: +1 rep AND +1 def. Keys
      // are required (the spec, and pyarrow refuses optional ones).
      final key = _build(
        entries.children[0].copyWith(nullable: false),
        [...path, 'key_value', 'key'],
        repetition + 1,
        present + 1,
      );
      final value = _build(
        entries.children[1],
        [...path, 'key_value', 'value'],
        repetition + 1,
        present + 1,
      );
      return _MapNode(optional, key, value, [...key.leaves, ...value.leaves]);
    default:
      final optional = field.nullable;
      return _LeafNode(
        _Leaf(
          path,
          field,
          optional,
          repetition,
          definition + (optional ? 1 : 0),
        ),
      );
  }
}

/// Append one null slot (at [repetition]/[definition]) to EVERY leaf under [node]: used when
/// a container is null or a present-but-empty collection.
void _emitNulls(_Node node, int repetition, int definition) {
  for (final leaf in node.leaves) {
    leaf.values.add(null);
    leaf.reps.add(repetition);
    leaf.defs.add(definition);
  }
}

/// Dremel striping: descend [value] through [node], emitting (value, repetition, definition)
/// to the relevant leaves. [depth] = number of repeated ancestors entered so far.
void _dissect(
  _Node node,
  dynamic value,
  int repetition,
  int definition,
  int depth,
) {
  switch (node) {
    case final _LeafNode branch:
      if (value == null && !branch.leaf.optional) {
        throw Failure.of(
          Kind.invalid,
          'Field ${branch.leaf.path.join('.')} is required but holds a null',
        );
      }
      branch.leaf.values.add(
        value == null ? null : _physical(branch.leaf.field, value),
      );
      branch.leaf.reps.add(repetition);
      branch.leaf.defs.add(
        value == null
            ? definition
            : definition + (branch.leaf.optional ? 1 : 0),
      );
    case final _StructNode branch:
      if (branch.optional && value == null) {
        _emitNulls(branch, repetition, definition);
        return;
      }
      final present = definition + (branch.optional ? 1 : 0);
      final map = value as Map?;
      for (final member in branch.members) {
        _dissect(
          member.node,
          map == null ? null : map[member.name],
          repetition,
          present,
          depth,
        );
      }
    case final _ListNode branch:
      if (branch.optional && value == null) {
        _emitNulls(branch, repetition, definition);
        return;
      }
      final present = definition + (branch.optional ? 1 : 0);
      final list = (value as List?) ?? const [];
      if (list.isEmpty) {
        _emitNulls(
          branch.element,
          repetition,
          present,
        ); // present-but-empty list
        return;
      }
      final level = depth + 1;
      for (int i = 0; i < list.length; i++) {
        _dissect(
          branch.element,
          list[i],
          i == 0 ? repetition : level,
          present + 1,
          level,
        );
      }
    case final _MapNode branch:
      if (branch.optional && value == null) {
        _emitNulls(branch, repetition, definition);
        return;
      }
      final present = definition + (branch.optional ? 1 : 0);
      final map = (value as Map?) ?? const {};
      if (map.isEmpty) {
        _emitNulls(branch.key, repetition, present); // present-but-empty map
        _emitNulls(branch.value, repetition, present);
        return;
      }
      final level = depth + 1;
      var i = 0;
      for (final entry in map.entries) {
        final current = i == 0 ? repetition : level;
        _dissect(branch.key, entry.key, current, present + 1, level);
        _dissect(branch.value, entry.value, current, present + 1, level);
        i++;
      }
  }
}

LeafColumn _toLeafColumn(_Leaf leaf) {
  return LeafColumn(
    path: leaf.path,
    physicalType: _leafPhysical(leaf.field),
    values: leaf.values,
    repLevels: leaf.maxRep > 0 ? leaf.reps : null,
    // Conditional, exactly like repLevels: a leaf that is REQUIRED all the way to the
    // root has maxDef 0 and therefore no definition levels to carry. Passing
    // them anyway made the encoder emit a zero-width level section (a 4-byte
    // length plus an RLE run) that the decoder, which reads levels only when
    // defLevelBitWidth > 0, never consumes, so those bytes were decoded as
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
  final count = column.length;
  for (int i = 0; i < count; i++) {
    _dissect(node, column[i], 0, 0, 0);
  }
  return [for (final leaf in node.leaves) _toLeafColumn(leaf)];
}
