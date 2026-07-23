import 'dart:typed_data';
import '../thrift/compact_protocol.dart';
import '../thrift/parquet_thrift.dart' as thrift;
import 'file_writer.dart';

/// Writes Parquet file metadata
class MetadataWriter {
  final FileWriter writer;

  MetadataWriter(this.writer);

  /// Write metadata to file
  Future<void> writeMetadata(thrift.FileMetaData metadata) async {
    // Serialize metadata
    final bytes = serialize(metadata);

    // Write metadata
    await writer.write(bytes);

    // Write footer (metadata length + magic)
    final footer = Uint8List(8);
    final view = ByteData.view(footer.buffer);
    view.setInt32(0, bytes.length, Endian.little);
    footer.setAll(4, 'PAR1'.codeUnits);

    await writer.write(footer);
  }

  /// Serialize metadata to bytes
  Uint8List serialize(thrift.FileMetaData metadata) {
    final protocol = CompactWriter();
    metadata.write(protocol);
    return protocol.takeBytes();
  }
}
