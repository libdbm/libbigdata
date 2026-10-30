import 'dart:typed_data';
import '../../../core/limits.dart';
import '../thrift/compact_protocol.dart';
import '../thrift/parquet_thrift.dart' as thrift;
import '../compression.dart';
import 'file_reader.dart';

/// Reads Parquet file metadata
class MetadataReader {
  final FileReader reader;

  MetadataReader(this.reader);

  /// Read file metadata from the footer
  Future<thrift.FileMetaData> readMetadata() async {
    // Read footer length (4 bytes before magic)
    final length = await reader.readFooterLength();

    if (length <= 0 ||
        length > reader.length - 8 ||
        length > defaultMaxFooterBytes) {
      throw FormatException('Invalid footer length: $length');
    }

    // Read footer bytes
    final start = reader.length - 8 - length;
    final bytes = await reader.readBytes(start, length);

    // Deserialize metadata
    return deserialize(bytes);
  }

  /// Deserialize metadata from bytes
  thrift.FileMetaData deserialize(Uint8List bytes) {
    final protocol = CompactReader(bytes);

    final metadata = thrift.FileMetaData();
    metadata.read(protocol);

    validate(metadata);

    return metadata;
  }

  /// Validate metadata
  void validate(thrift.FileMetaData metadata) {
    if (metadata.version != 1 && metadata.version != 2) {
      throw FormatException('Unsupported Parquet version: ${metadata.version}');
    }

    if (metadata.schema.isEmpty) {
      throw FormatException('Empty schema in metadata');
    }

    if (metadata.rowGroups.isEmpty && metadata.numRows > 0) {
      throw FormatException('No row groups but numRows is ${metadata.numRows}');
    }

    // Validate schema has root element
    final root = metadata.schema.first;
    if (root.numChildren == null || root.numChildren! <= 0) {
      throw FormatException('Invalid root schema element');
    }
    _checkSchemaTree(metadata.schema);

    // Per-column-chunk checks: reject unsupported codecs, and sanity-check
    // offsets/sizes against the actual file length so a corrupt/adversarial
    // footer fails loud here instead of driving an out-of-file read or a huge
    // allocation downstream. (numValues magnitude can't be bounded from the
    // footer alone (RLE/dictionary pack many values per byte) so the decoder
    // allocates incrementally as pages are read; see column_reader.)
    final length = reader.length;
    for (final group in metadata.rowGroups) {
      for (final column in group.columns) {
        // Readers dereference the chunk metadata unconditionally, and chunks
        // stored in a separate file are not supported.
        final meta =
            column.metaData ??
            (throw const FormatException(
              'Parquet column chunk without metadata',
            ));
        if (!supported(meta.codec)) {
          throw FormatException(
            'Unsupported Parquet codec: ${meta.codec.name}',
          );
        }
        final data = meta.dataPageOffset;
        final dictionary = meta.dictionaryPageOffset;
        final size = meta.totalCompressedSize;
        if (data < 0 || data > length) {
          throw FormatException(
            'Parquet column "${meta.pathInSchema.join('.')}": data page offset '
            '$data outside file (length $length)',
          );
        }
        if (dictionary != null && (dictionary < 0 || dictionary > length)) {
          throw FormatException(
            'Parquet column dictionary page offset $dictionary outside file '
            '(length $length)',
          );
        }
        final start = dictionary ?? data;
        if (size < 0 || start + size > length) {
          throw FormatException(
            'Parquet column chunk (offset $start + size $size) exceeds file '
            'length $length',
          );
        }
      }
    }
  }

  /// Walk the flat, depth-first [schema] list as a tree without recursion.
  /// Every reader downstream walks it recursively, so its depth and size are
  /// bounded here, and its child counts must exactly cover the list.
  static void _checkSchemaTree(List<thrift.SchemaElement> schema) {
    if (schema.length > defaultMaxSchemaFields) {
      throw FormatException(
        'Parquet schema has ${schema.length} elements '
        '(limit $defaultMaxSchemaFields)',
      );
    }
    final pending = <int>[]; // children still expected by each open group
    for (var i = 0; i < schema.length; i++) {
      if (i > 0) {
        if (pending.isEmpty) {
          throw FormatException(
            'Parquet schema element $i is outside the tree',
          );
        }
        pending.last--;
      }
      final children = schema[i].numChildren ?? 0;
      if (children < 0) {
        throw FormatException(
          'Parquet schema element $i has $children children',
        );
      }
      if (children > 0) {
        pending.add(children);
        if (pending.length > defaultMaxSchemaDepth) {
          throw FormatException(
            'Parquet schema nesting exceeds depth $defaultMaxSchemaDepth',
          );
        }
      }
      while (pending.isNotEmpty && pending.last == 0) {
        pending.removeLast();
      }
    }
    if (pending.isNotEmpty) {
      throw const FormatException(
        'Parquet schema declares more children than it lists',
      );
    }
  }
}
