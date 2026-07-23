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

    // Per-column-chunk checks: reject unsupported codecs, and sanity-check
    // offsets/sizes against the actual file length so a corrupt/adversarial
    // footer fails loud here instead of driving an out-of-file read or a huge
    // allocation downstream. (numValues magnitude can't be bounded from the
    // footer alone — RLE/dictionary pack many values per byte — so the decoder
    // allocates incrementally as pages are read; see column_reader.)
    final fileLength = reader.length;
    for (final group in metadata.rowGroups) {
      for (final column in group.columns) {
        final meta = column.metaData;
        if (meta == null) continue;
        if (!supported(meta.codec)) {
          throw FormatException(
            'Unsupported Parquet codec: ${meta.codec.name}',
          );
        }
        final dataOff = meta.dataPageOffset;
        final dictOff = meta.dictionaryPageOffset;
        final size = meta.totalCompressedSize;
        if (dataOff < 0 || dataOff > fileLength) {
          throw FormatException(
            'Parquet column "${meta.pathInSchema.join('.')}": data page offset '
            '$dataOff outside file (length $fileLength)',
          );
        }
        if (dictOff != null && (dictOff < 0 || dictOff > fileLength)) {
          throw FormatException(
            'Parquet column dictionary page offset $dictOff outside file '
            '(length $fileLength)',
          );
        }
        final start = dictOff ?? dataOff;
        if (size < 0 || start + size > fileLength) {
          throw FormatException(
            'Parquet column chunk (offset $start + size $size) exceeds file '
            'length $fileLength',
          );
        }
      }
    }
  }
}
