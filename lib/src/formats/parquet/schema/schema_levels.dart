import '../thrift/parquet_thrift.dart' as thrift;

/// Calculates max definition and repetition levels for schema elements
class SchemaLevels {
  /// Schema node in tree structure
  static SchemaNode buildSchemaTree(List<thrift.SchemaElement> elements) {
    if (elements.isEmpty) {
      throw ArgumentError('Schema cannot be empty');
    }

    final root = SchemaNode(elements[0], 0, null);
    _buildTree(elements, 1, root);
    return root;
  }

  /// Build tree recursively
  static int _buildTree(
    List<thrift.SchemaElement> elements,
    int index,
    SchemaNode parent,
  ) {
    final count = parent.element.numChildren ?? 0;

    for (int i = 0; i < count; i++) {
      if (index >= elements.length) break;

      final element = elements[index];
      final node = SchemaNode(element, index, parent);
      parent.children.add(node);

      index++;

      // Recursively add children
      if (element.numChildren != null && element.numChildren! > 0) {
        index = _buildTree(elements, index, node);
      }
    }

    return index;
  }

  /// Calculate levels for a column
  static LevelInfo calculateLevels(
    List<thrift.SchemaElement> elements,
    int columnIndex,
  ) {
    final tree = buildSchemaTree(elements);
    final node = _findNode(tree, columnIndex);

    if (node == null) {
      throw ArgumentError('Column not found at index $columnIndex');
    }

    return _calculateNodeLevels(node);
  }

  /// Find node by index
  static SchemaNode? _findNode(SchemaNode node, int index) {
    if (node.index == index) return node;

    for (final child in node.children) {
      final found = _findNode(child, index);
      if (found != null) return found;
    }

    return null;
  }

  /// Calculate levels for a node
  static LevelInfo _calculateNodeLevels(SchemaNode node) {
    int definition = 0;
    int repetition = 0;

    // Walk up the tree from leaf to root
    SchemaNode? current = node;
    while (current != null && current.parent != null) {
      final element = current.element;

      // Increment definition level for optional fields
      if (element.repetitionType == thrift.FieldRepetitionType.optional) {
        definition++;
      }

      // Increment repetition level for repeated fields
      if (element.repetitionType == thrift.FieldRepetitionType.repeated) {
        repetition++;
        // Repeated fields also add to definition level
        definition++;
      }

      current = current.parent;
    }

    return LevelInfo(maxDefinition: definition, maxRepetition: repetition);
  }

  /// Ordered dotted paths of every leaf (primitive) column, in document order.
  static List<String> leafPaths(List<thrift.SchemaElement> elements) {
    final tree = buildSchemaTree(elements);
    final paths = <String>[];
    void walk(SchemaNode node, List<String> prefix) {
      for (final child in node.children) {
        final path = [...prefix, child.element.name ?? ''];
        if (child.isLeaf) {
          paths.add(path.join('.'));
        } else {
          walk(child, path);
        }
      }
    }

    walk(tree, []);
    return paths;
  }
}

/// Each leaf column's dotted path and levels, from one walk of the schema
/// tree. A reader builds this once per file: looking levels up per column with
/// [SchemaLevels.calculateLevels] rebuilds and searches the tree each time,
/// which is quadratic in the column count.
class SchemaLayout {
  /// Dotted paths of every leaf column, in document order.
  final List<String> paths = [];
  final Map<int, LevelInfo> _levels = {};

  SchemaLayout(List<thrift.SchemaElement> elements) {
    void walk(SchemaNode node, String prefix, int definition, int repetition) {
      for (final child in node.children) {
        final name = child.element.name ?? '';
        final path = prefix.isEmpty ? name : '$prefix.$name';
        final repeated =
            child.element.repetitionType == thrift.FieldRepetitionType.repeated;
        final optional =
            child.element.repetitionType == thrift.FieldRepetitionType.optional;
        final maxDefinition = definition + (repeated || optional ? 1 : 0);
        final maxRepetition = repetition + (repeated ? 1 : 0);
        if (child.isLeaf) {
          paths.add(path);
          _levels[child.index] = LevelInfo(
            maxDefinition: maxDefinition,
            maxRepetition: maxRepetition,
          );
        } else {
          walk(child, path, maxDefinition, maxRepetition);
        }
      }
    }

    walk(SchemaLevels.buildSchemaTree(elements), '', 0, 0);
  }

  /// Levels of the leaf at schema element [index].
  LevelInfo levels(int index) {
    final found = _levels[index];
    if (found == null) {
      throw ArgumentError('Column not found at index $index');
    }
    return found;
  }
}

/// Schema node in tree structure
class SchemaNode {
  final thrift.SchemaElement element;
  final int index;
  final SchemaNode? parent;
  final List<SchemaNode> children = [];

  SchemaNode(this.element, this.index, this.parent);

  bool get isLeaf => children.isEmpty;

  @override
  String toString() {
    final indent = '  ' * depth;
    final buffer = StringBuffer();
    buffer.writeln(
      '$indent${element.name} (${element.type}, ${element.repetitionType})',
    );
    for (final child in children) {
      buffer.write(child.toString());
    }
    return buffer.toString();
  }

  int get depth {
    int count = 0;
    SchemaNode? current = parent;
    while (current != null) {
      count++;
      current = current.parent;
    }
    return count;
  }
}

/// Level information for a column
class LevelInfo {
  final int maxDefinition;
  final int maxRepetition;

  LevelInfo({required this.maxDefinition, required this.maxRepetition});

  @override
  String toString() =>
      'LevelInfo(maxDef: $maxDefinition, maxRep: $maxRepetition)';
}
