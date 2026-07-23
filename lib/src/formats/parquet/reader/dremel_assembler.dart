import '../../../core/column.dart';
import '../../../core/data_type.dart';
import '../../../core/schema.dart';
import '../schema/schema_converter.dart';
import '../schema/schema_levels.dart';
import '../thrift/parquet_thrift.dart' as thrift;

/// Raw page-decoded leaf data (values already logically converted; no record
/// assembly) — the input to [DremelAssembler].
class RawLeaf {
  final List<dynamic> values;
  final List<int> reps;
  final List<int> defs;
  final int maxDef;
  final int maxRep;
  final DataType dataType;
  RawLeaf({
    required this.values,
    required this.reps,
    required this.defs,
    required this.maxDef,
    required this.maxRep,
    required this.dataType,
  });

  /// Rough decoded footprint of this leaf — the rep/def level lists plus the
  /// actual value payloads. Drives incremental byte-cap enforcement during
  /// row-group accumulation. A flat 8 bytes/value badly undercounts a leaf of
  /// long strings or binary blobs (the payload dominates), letting such a file
  /// slip past the cap and exhaust memory during decode/assembly — so charge the
  /// real content length for String / binary values (a guard, not exact).
  int get bytes {
    var payload = 0;
    for (final v in values) {
      if (v is String) {
        payload += v.length; // ~UTF-8 magnitude; O(1) and proportional
      } else if (v is List<int>) {
        payload += v.length;
      } else {
        payload += 8;
      }
    }
    return (reps.length + defs.length) * 8 + payload;
  }

  /// Concatenate same-leaf raw pieces across row groups (each piece starts a new
  /// record at rep 0, so concatenation is correct).
  static RawLeaf concat(List<RawLeaf> pieces) {
    if (pieces.length == 1) return pieces.first;
    final f = pieces.first;
    return RawLeaf(
      values: [for (final p in pieces) ...p.values],
      reps: [for (final p in pieces) ...p.reps],
      defs: [for (final p in pieces) ...p.defs],
      maxDef: f.maxDef,
      maxRep: f.maxRep,
      dataType: f.dataType,
    );
  }
}

/// Reassembles arbitrary-depth nested columns from the raw per-leaf
/// (values, repLevels, defLevels) triples using the standard Dremel record
/// assembly — the exact inverse of the write shredder. Round-trips
/// list/struct/map composed to any depth.
class DremelAssembler {
  final List<thrift.SchemaElement> schema;
  final Map<String, RawLeaf> leaves; // keyed by dotted leaf path

  late final SchemaNode _root;
  final Map<SchemaNode, int> _maxDef = {};
  final Map<SchemaNode, int> _maxRep = {};
  final Map<SchemaNode, String> _leafPath = {};
  final Map<String, int> _pos = {}; // leaf-path -> cursor

  DremelAssembler(this.schema, this.leaves) {
    _root = SchemaLevels.buildSchemaTree(schema);
    _index(_root, [], 0, 0);
  }

  /// Whether [schema] uses ONLY the WELL-FORMED modern nesting this assembler
  /// reconstructs: non-repeated struct groups, 3-level LIST (annotated, one
  /// child), and key_value MAP (annotated, one child with exactly key+value).
  /// Anything else — a legacy repeated primitive leaf, a repeated group without
  /// a LIST/MAP annotation, a malformed map — keeps the older single-level
  /// assembly path (which tolerates odd shapes via flat fallback).
  static bool hasNested(List<thrift.SchemaElement> schema) {
    final root = SchemaLevels.buildSchemaTree(schema);
    var anyGroup = false;
    var ok = true;
    // Check a LOGICAL field node, descending THROUGH the synthetic 'list' /
    // 'key_value' repeated levels (list -> element, map -> key+value).
    void check(SchemaNode node) {
      if (node.isLeaf) {
        if (node.element.repetitionType ==
            thrift.FieldRepetitionType.repeated) {
          ok = false; // legacy repeated primitive (no LIST wrapper)
        }
        return;
      }
      final ct = node.element.convertedType;
      if (ct == thrift.ConvertedType.list) {
        anyGroup = true;
        if (node.children.length != 1 ||
            node.children.first.children.length != 1) {
          ok = false;
          return;
        }
        check(node.children.first.children.first); // element
      } else if (ct == thrift.ConvertedType.map ||
          ct == thrift.ConvertedType.mapKeyValue) {
        anyGroup = true;
        if (node.children.length != 1 ||
            node.children.first.children.length != 2) {
          ok = false;
          return;
        }
        check(node.children.first.children[0]); // key
        check(node.children.first.children[1]); // value
      } else if (node.element.repetitionType ==
          thrift.FieldRepetitionType.repeated) {
        ok =
            false; // a repeated group that is NOT an annotated LIST/MAP (legacy)
      } else {
        anyGroup = true; // plain struct group
        for (final c in node.children) {
          check(c);
        }
      }
    }

    for (final top in root.children) {
      check(top);
    }
    return anyGroup && ok;
  }

  void _index(SchemaNode node, List<String> path, int rep, int def) {
    for (final child in node.children) {
      final p = [...path, child.element.name ?? ''];
      final opt =
          child.element.repetitionType == thrift.FieldRepetitionType.optional;
      final repeated =
          child.element.repetitionType == thrift.FieldRepetitionType.repeated;
      final cRep = rep + (repeated ? 1 : 0);
      final cDef = def + (opt || repeated ? 1 : 0);
      _maxDef[child] = cDef;
      _maxRep[child] = cRep;
      if (child.isLeaf) {
        _leafPath[child] = p.join('.');
      } else {
        _index(child, p, cRep, cDef);
      }
    }
  }

  /// Total top-level records in this row group (each begins at rep-level 0 in the
  /// driver leaf). Computed once for windowed assembly.
  late final int _recordCount = _root.children.isEmpty
      ? 0
      : leaves[_leafPath[_firstLeaf(_root.children.first)]!]!.reps
            .where((r) => r == 0)
            .length;
  int _emitted = 0;

  /// True while [assembleWindow] has records left to emit.
  bool get hasMore => _emitted < _recordCount;

  /// Assemble the NEXT up-to-[size] records into (fields, columns), advancing
  /// persistent per-leaf cursors so repeated calls stream a row group in bounded
  /// batches (honoring readBatches's `size`) instead of materializing the whole
  /// assembled row group at once. Do NOT mix with [run] on the same instance
  /// (run resets the cursors).
  (List<Field>, List<Column>) assembleWindow(int size) {
    final remaining = _recordCount - _emitted;
    final count = size < remaining ? size : remaining;
    final fields = <Field>[];
    final columns = <Column>[];
    for (final node in _root.children) {
      final rows = <dynamic>[];
      for (var i = 0; i < count; i++) {
        rows.add(_value(node)); // each call consumes exactly one record
      }
      fields.add(_field(node));
      columns.add(
        ListColumn(
          name: node.element.name ?? '',
          dataType: _dataType(node),
          data: rows,
          nullable: true,
        ),
      );
    }
    _emitted += count;
    return (fields, columns);
  }

  /// Build (fields, columns) — one per top-level field.
  (List<Field>, List<Column>) run() {
    final fields = <Field>[];
    final columns = <Column>[];
    for (final node in _root.children) {
      // Each top field has independent leaf cursors; reset them.
      for (final leaf in _leavesUnder(node)) {
        _pos[_leafPath[leaf]!] = 0;
      }
      final drvPath = _leafPath[_firstLeaf(node)]!;
      final total = leaves[drvPath]!.reps.length;
      final rows = <dynamic>[];
      // Each _value(node) consumes exactly one record (the driver returns to a
      // rep==0 boundary), so iterate until the driver cursor is exhausted.
      while ((_pos[drvPath] ?? 0) < total) {
        rows.add(_value(node));
      }
      fields.add(_field(node));
      columns.add(
        ListColumn(
          name: node.element.name ?? '',
          dataType: _dataType(node),
          data: rows,
          nullable: true,
        ),
      );
    }
    return (fields, columns);
  }

  // ---- assembly state machine -------------------------------------------

  dynamic _value(SchemaNode node) {
    if (node.isLeaf) {
      final path = _leafPath[node]!;
      final raw = leaves[path]!;
      final p = _pos[path] ?? 0;
      final v = raw.values[p];
      final d = raw.defs[p];
      _pos[path] = p + 1;
      return d == raw.maxDef ? v : null;
    }
    switch (_kind(node)) {
      case _Kind.struct:
        final drv = _firstLeaf(node);
        if (_peekDef(drv) < _maxDef[node]!) {
          _consumeOne(node);
          return null;
        }
        return {
          for (final m in node.children) (m.element.name ?? ''): _value(m),
        };
      case _Kind.list:
        final repeated = node.children.first;
        final element = repeated.children.first;
        final drv = _firstLeaf(node);
        final d = _peekDef(drv);
        if (d < _maxDef[node]!) {
          _consumeOne(node);
          return null;
        }
        if (d == _maxDef[node]!) {
          _consumeOne(node);
          return <dynamic>[];
        }
        final myRep = _maxRep[repeated]!;
        final out = <dynamic>[];
        do {
          out.add(_value(element));
        } while (!_atEnd(drv) && _peekRep(drv) == myRep);
        return out;
      case _Kind.map:
        final repeated = node.children.first;
        final keyNode = repeated.children[0];
        final valNode = repeated.children[1];
        final drv = _firstLeaf(node);
        final d = _peekDef(drv);
        if (d < _maxDef[node]!) {
          _consumeOne(node);
          return null;
        }
        if (d == _maxDef[node]!) {
          _consumeOne(node);
          return <dynamic, dynamic>{};
        }
        final myRep = _maxRep[repeated]!;
        final out = <dynamic, dynamic>{};
        do {
          final k = _value(keyNode);
          final v = _value(valNode);
          out[k] = v;
        } while (!_atEnd(drv) && _peekRep(drv) == myRep);
        return out;
    }
  }

  // ---- cursors / helpers ------------------------------------------------

  SchemaNode _firstLeaf(SchemaNode node) {
    var n = node;
    while (!n.isLeaf) {
      n = n.children.first;
    }
    return n;
  }

  void _consumeOne(SchemaNode node) {
    for (final leaf in _leavesUnder(node)) {
      final path = _leafPath[leaf]!;
      _pos[path] = (_pos[path] ?? 0) + 1;
    }
  }

  Iterable<SchemaNode> _leavesUnder(SchemaNode node) sync* {
    if (node.isLeaf) {
      yield node;
    } else {
      for (final c in node.children) {
        yield* _leavesUnder(c);
      }
    }
  }

  int _peekDef(SchemaNode leaf) {
    final path = _leafPath[leaf]!;
    final raw = leaves[path]!;
    final p = _pos[path] ?? 0;
    return p < raw.defs.length ? raw.defs[p] : -1;
  }

  int _peekRep(SchemaNode leaf) {
    final path = _leafPath[leaf]!;
    final raw = leaves[path]!;
    final p = _pos[path] ?? 0;
    return p < raw.reps.length ? raw.reps[p] : 0;
  }

  bool _atEnd(SchemaNode leaf) {
    final path = _leafPath[leaf]!;
    final raw = leaves[path]!;
    return (_pos[path] ?? 0) >= raw.reps.length;
  }

  static _Kind _kind(SchemaNode node) {
    final ct = node.element.convertedType;
    if (ct == thrift.ConvertedType.list) return _Kind.list;
    if (ct == thrift.ConvertedType.map ||
        ct == thrift.ConvertedType.mapKeyValue) {
      return _Kind.map;
    }
    return _Kind.struct;
  }

  DataType _dataType(SchemaNode node) {
    if (node.isLeaf) return leaves[_leafPath[node]!]!.dataType;
    return switch (_kind(node)) {
      _Kind.list => DataType.list,
      _Kind.map => DataType.map,
      _Kind.struct => DataType.struct,
    };
  }

  Field _field(SchemaNode node) => _fieldFor(
    node,
    (leaf, name, nullable) =>
        Field(name: name, dataType: _dataType(leaf), nullable: nullable),
  );

  /// The one Dremel field walk, shared by the data-assembly path ([_field])
  /// and the metadata-only [fieldsOf]. [leafField] supplies each LEAF's field
  /// (its type source differs: decoded [RawLeaf] vs the schema converter).
  static Field _fieldFor(
    SchemaNode node,
    Field Function(SchemaNode leaf, String name, bool nullable) leafField,
  ) {
    final name = node.element.name ?? '';
    final nullable =
        node.element.repetitionType != thrift.FieldRepetitionType.required;
    if (node.isLeaf) {
      return leafField(node, name, nullable);
    }
    switch (_kind(node)) {
      case _Kind.struct:
        return Field(
          name: name,
          dataType: DataType.struct,
          nullable: nullable,
          children: [for (final c in node.children) _fieldFor(c, leafField)],
        );
      case _Kind.list:
        final element = node.children.first.children.first;
        return Field(
          name: name,
          dataType: DataType.list,
          nullable: nullable,
          children: [_fieldFor(element, leafField)],
        );
      case _Kind.map:
        final kv = node.children.first;
        return Field(
          name: name,
          dataType: DataType.map,
          nullable: nullable,
          children: [
            Field(
              name: 'key_value',
              dataType: DataType.struct,
              nullable: false,
              children: [
                _fieldFor(kv.children[0], leafField),
                _fieldFor(kv.children[1], leafField),
              ],
            ),
          ],
        );
    }
  }

  /// The top-level logical fields the ASSEMBLY paths emit for [schema],
  /// derived from the schema tree alone (no data). `ParquetReader.schema()`
  /// must return THESE for a nested file — it used to return the flattened
  /// dotted-leaf fields instead, so a metadata-first consumer (the Dataset
  /// pre-pass) unified names like `info.a` that no streamed batch carries and
  /// silently null-filled every nested column. Leaf types/precision/scale/id
  /// come from the same [SchemaConverter] the readers decode with.
  static List<Field> fieldsOf(List<thrift.SchemaElement> schema) {
    final root = SchemaLevels.buildSchemaTree(schema);
    final flat = SchemaConverter().fromParquetSchema(schema);
    final byPath = {for (final f in flat.fields) f.name: f};
    final paths = <SchemaNode, String>{};
    void index(SchemaNode node, String prefix) {
      final name = node.element.name ?? '';
      final path = prefix.isEmpty ? name : '$prefix.$name';
      paths[node] = path;
      for (final c in node.children) {
        index(c, path);
      }
    }

    for (final c in root.children) {
      index(c, '');
    }
    return [
      for (final c in root.children)
        _fieldFor(c, (leaf, name, nullable) {
          final f = byPath[paths[leaf]];
          if (f == null) {
            throw StateError(
              'Parquet leaf "${paths[leaf]}" missing from the flattened '
              'schema (corrupt or unsupported schema shape)',
            );
          }
          return Field(
            name: name,
            dataType: f.dataType,
            nullable: nullable,
            precision: f.precision,
            scale: f.scale,
            id: f.id,
          );
        }),
    ];
  }
}

enum _Kind { struct, list, map }
