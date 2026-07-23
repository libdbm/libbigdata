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
    final numChildren = parent.element.numChildren ?? 0;

    for (int i = 0; i < numChildren; i++) {
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
    final columnNode = _findNode(tree, columnIndex);

    if (columnNode == null) {
      throw ArgumentError('Column not found at index $columnIndex');
    }

    return _calculateNodeLevels(columnNode);
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
    int maxDef = 0;
    int maxRep = 0;

    // Walk up the tree from leaf to root
    SchemaNode? current = node;
    while (current != null && current.parent != null) {
      final element = current.element;

      // Increment definition level for optional fields
      if (element.repetitionType == thrift.FieldRepetitionType.optional) {
        maxDef++;
      }

      // Increment repetition level for repeated fields
      if (element.repetitionType == thrift.FieldRepetitionType.repeated) {
        maxRep++;
        // Repeated fields also add to definition level
        maxDef++;
      }

      current = current.parent;
    }

    return LevelInfo(maxDefinition: maxDef, maxRepetition: maxRep);
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

  /// Get the leaf column path for nested types
  static List<String> getColumnPath(
    List<thrift.SchemaElement> elements,
    int columnIndex,
  ) {
    final tree = buildSchemaTree(elements);
    final node = _findNode(tree, columnIndex);

    if (node == null) return [];

    final path = <String>[];
    SchemaNode? current = node;

    while (current != null && current.parent != null) {
      path.insert(0, current.element.name ?? '');
      current = current.parent;
    }

    return path;
  }

  /// Check if schema element is part of a LIST
  static bool isListElement(List<thrift.SchemaElement> elements, int index) {
    if (index <= 0) return false;

    // Check parent
    for (int i = index - 1; i >= 0; i--) {
      final element = elements[i];
      if (element.convertedType == thrift.ConvertedType.list) {
        // Check if this element is a descendant
        final numChildren = element.numChildren ?? 0;
        if (i + numChildren >= index) {
          return true;
        }
      }
    }

    return false;
  }

  /// Check if schema element is part of a MAP
  static bool isMapElement(List<thrift.SchemaElement> elements, int index) {
    if (index <= 0) return false;

    // Check parent
    for (int i = index - 1; i >= 0; i--) {
      final element = elements[i];
      if (element.convertedType == thrift.ConvertedType.map ||
          element.convertedType == thrift.ConvertedType.mapKeyValue) {
        // Check if this element is a descendant
        final numChildren = element.numChildren ?? 0;
        if (i + numChildren >= index) {
          return true;
        }
      }
    }

    return false;
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
    int d = 0;
    SchemaNode? current = parent;
    while (current != null) {
      d++;
      current = current.parent;
    }
    return d;
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
