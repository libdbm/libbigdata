import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

/// Low-level file reader for Parquet files
/// Handles file I/O operations and provides methods for reading at specific offsets
class FileReader {
  final String filePath;
  late RandomAccessFile _file;
  late int _length;
  bool _opened = false;

  // Serializes the seek+read pair against the single shared file cursor. Without
  // it, two concurrent reads (e.g. a future Future.wait over columns sharing one
  // FileReader) would interleave setPosition/read and read each other's bytes.
  // The offset-based [readBytes] is the concurrency-safe primitive; all readers
  // use it.
  Future<void> _lock = Future<void>.value();

  FileReader(this.filePath);

  Future<T> _locked<T>(Future<T> Function() body) {
    final previous = _lock;
    final done = Completer<void>();
    _lock = done.future;
    return previous.then((_) => body()).whenComplete(done.complete);
  }

  /// Open the file for reading
  Future<void> open() async {
    final file = File(filePath);
    if (!await file.exists()) {
      throw PathNotFoundException(
        filePath,
        const OSError('No such file or directory', 2),
        'File not found',
      );
    }

    _file = await file.open(mode: FileMode.read);
    // Take ownership of the handle IMMEDIATELY: if length() below throws (an I/O
    // error or a file-race), _opened is already true so close() releases the fd
    // instead of leaking it (close() no-ops when _opened is false).
    _opened = true;
    try {
      _length = await _file.length();
    } catch (_) {
      // Self-clean: every caller awaits open() BEFORE entering its own
      // try/finally, so a throw here would skip their close() and leak the fd
      // (one per transient I/O race, until "too many open files").
      await close();
      rethrow;
    }
  }

  /// Close the file. A no-op if [open] never succeeded (so a cleanup `close()`
  /// after a failed open doesn't throw a LateInitializationError).
  Future<void> close() async {
    if (!_opened) return;
    _opened = false;
    await _file.close();
  }

  /// Get file length
  int get length => _length;

  /// Read [length] bytes at absolute [offset]. The seek+read is atomic under the
  /// internal lock, so this is the concurrency-safe read primitive.
  Future<Uint8List> readBytes(int offset, int length) {
    if (offset < 0 || offset >= _length) {
      throw RangeError('Offset $offset is out of range [0, $_length)');
    }
    final count = offset + length > _length ? _length - offset : length;
    return _locked(() async {
      await _file.setPosition(offset);
      return _file.read(count);
    });
  }

  /// Validate Parquet magic bytes
  Future<bool> validateMagic() async {
    if (_length < 8) {
      return false;
    }
    final head = await readBytes(0, 4);
    if (String.fromCharCodes(head) != 'PAR1') {
      return false;
    }
    final tail = await readBytes(_length - 4, 4);
    return String.fromCharCodes(tail) == 'PAR1';
  }

  /// Read footer length
  Future<int> readFooterLength() async {
    final bytes = await readBytes(_length - 8, 4);
    return ByteData.view(
      bytes.buffer,
      bytes.offsetInBytes,
    ).getInt32(0, Endian.little);
  }
}
