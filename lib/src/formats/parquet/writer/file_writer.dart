import 'dart:io';
import 'dart:typed_data';

/// Low-level file writer for Parquet files
class FileWriter {
  final String path;
  late RandomAccessFile file;
  int position = 0;

  FileWriter(this.path);

  /// Open file for writing
  Future<void> open() async {
    file = await File(path).open(mode: FileMode.write);
    position = 0;
  }

  /// Flush to stable media, then close.
  ///
  /// `close()` alone only hands the bytes to the OS — it is not an fsync, so a
  /// power loss can leave a published file whose content never reached disk.
  /// That matters here because the commit layers (Delta/Iceberg) DO fsync their
  /// metadata, so without this a durable commit could reference a data file that
  /// isn't. `RandomAccessFile.flush()` is fsync.
  ///
  /// This covers the file's CONTENT. Durably committing the directory entry
  /// created by the subsequent rename needs an fsync on the parent directory,
  /// which `dart:io` cannot express (it would require FFI, and this package has
  /// no native dependencies) — so a crash in that window can still lose a
  /// rename that already returned.
  Future<void> close() async {
    await file.flush();
    await file.close();
  }

  /// Write bytes
  Future<int> write(Uint8List bytes) async {
    await file.writeFrom(bytes);
    position += bytes.length;
    return bytes.length;
  }

  /// Write magic bytes
  Future<void> writeMagic() async {
    await write(Uint8List.fromList('PAR1'.codeUnits));
  }
}
