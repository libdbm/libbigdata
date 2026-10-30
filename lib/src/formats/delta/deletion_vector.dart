import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:libcompress/libcompress.dart' show Crc32;

import '../../core/failure.dart';
import '../../core/result.dart';
import '../../core/limits.dart';
import '../../core/paths.dart';
import 'actions.dart';
import 'roaring_bitmap.dart';

/// Reads and writes Delta deletion vectors (the set of positionally-deleted row
/// indices for a data file). Supports reading inline (`i`, Z85), UUID-relative
/// (`u`) and absolute-path (`p`) `.bin` files; writes the `.bin` file format
/// (version byte, big-endian length, little-endian magic, portable Roaring
/// bitmap, big-endian CRC-32) and returns a `u` descriptor.
class DeletionVector {
  static const int magic = 1681511377; // ROARING_BITMAP_PORTABLE_MAGIC

  /// Load the deleted row indices described by [descriptor] as a compressed
  /// [RoaringBitmap] (bits/element, not a ~40-50 B/element `Set<int>`, so a table
  /// with hundreds of millions of deletes doesn't OOM before any row is read).
  /// [tablePath] is the table root used to resolve `u`/relative paths.
  static Future<RoaringBitmap> load(
    DeletionVectorDescriptor descriptor,
    String tablePath, {
    bool allowExternalPaths = false,
  }) async {
    String confined(String path) =>
        allowExternalPaths ? path : confine(tablePath, path);
    switch (descriptor.storageType) {
      case 'i':
        final bytes = _z85Decode(descriptor.pathOrInlineDv);
        return _fromPayload(bytes, 0, bytes.length, descriptor.cardinality);
      case 'p':
        return _fromFile(
          confined(descriptor.pathOrInlineDv),
          descriptor.offset ?? 1,
          descriptor.sizeInBytes,
          descriptor.cardinality,
        );
      case 'u':
        return _fromFile(
          confined(_resolveUuid(tablePath, descriptor.pathOrInlineDv)),
          descriptor.offset ?? 1,
          descriptor.sizeInBytes,
          descriptor.cardinality,
        );
      default:
        throw UnsupportedError('DV storageType "${descriptor.storageType}"');
    }
  }

  static Future<RoaringBitmap> _fromFile(
    String path,
    int offset,
    int? sizeInBytes,
    int cardinality,
  ) async {
    // A `.bin` DV is `[u32 BE payloadLen][payload][u32 BE crc32]` at [offset].
    // A single `.bin` can hold MANY vectors, each with its own offset; read only
    // THIS vector's declared range via a ranged RandomAccessFile rather than
    // slurping the whole (potentially multi-GB) file into memory.
    final file = await File(path).open();
    try {
      final size = await file.length();
      if (offset < 0 || offset + 4 > size) {
        throw FormatException(
          'Deletion vector truncated: offset $offset beyond $size bytes',
        );
      }
      await file.setPosition(offset);
      final header = await file.read(4);
      if (header.length < 4) {
        throw const FormatException('Deletion vector truncated: length header');
      }
      final length = ByteData.sublistView(header).getUint32(0, Endian.big);
      // Cross-check the on-disk length against the descriptor's sizeInBytes (when
      // present) so a tampered header can't drive an over-read; then bound it
      // against the file before allocating the payload buffer.
      if (sizeInBytes != null && length != sizeInBytes) {
        throw FormatException(
          'Deletion vector length $length disagrees with descriptor '
          'sizeInBytes $sizeInBytes',
        );
      }
      // Cap the DECLARED length before ANY allocation: a length otherwise
      // bounded only by the file size lets a large `.bin` (or a small file with
      // an inflated header) drive a huge single-buffer read before the Roaring
      // cardinality budget (which caps the decoded bitmap, not the payload) can
      // help. Checked ahead of the file-size check so a malicious header is
      // rejected as over-cap regardless of the actual file length.
      if (length < 0 || length > defaultMaxDeletionVectorBytes) {
        throw FormatException(
          'Deletion vector payload declares $length bytes, over the '
          '$defaultMaxDeletionVectorBytes-byte cap',
        );
      }
      if (offset + 4 + length + 4 > size) {
        throw FormatException(
          'Deletion vector truncated: declared length $length exceeds the '
          '$size-byte file',
        );
      }
      final rest = await file.read(length + 4); // payload + trailing CRC-32
      if (rest.length < length + 4) {
        throw const FormatException('Deletion vector truncated: payload');
      }
      final payload = Uint8List.sublistView(rest, 0, length);
      final want = ByteData.sublistView(rest).getUint32(length, Endian.big);
      final got = _crc32(payload);
      if (want != got) {
        throw const FormatException(
          'Deletion vector CRC-32 mismatch (corrupt)',
        );
      }
      return _fromPayload(payload, 0, length, cardinality);
    } finally {
      await file.close();
    }
  }

  /// [bytes] from [magicAt]: `[u32 LE magic][treemap...]` up to [end].
  /// [cardinality] is the descriptor's declared delete count, cross-checked
  /// against what the bitmap decodes to (a CRC-valid but content-inflated bomb).
  static RoaringBitmap _fromPayload(
    Uint8List bytes,
    int magicAt,
    int end,
    int cardinality,
  ) {
    if (magicAt + 4 > end || end > bytes.length) {
      throw FormatException('Deletion vector payload truncated');
    }
    final value = ByteData.sublistView(bytes).getUint32(magicAt, Endian.little);
    if (value != magic) {
      throw FormatException('bad deletion-vector magic: $value');
    }
    return Roaring.deserializeToBitmap(
      Uint8List.sublistView(bytes, magicAt + 4, end),
      0,
      cardinality,
    );
  }

  /// Write a single-DV `.bin` file under [tablePath] using the standard
  /// UUID-relative (`u`) storage that delta-kernel/Spark expect:
  /// `deletion_vector_<uuid>.bin`, with `pathOrInlineDv` = Z85(uuid bytes).
  /// Returns the descriptor and the absolute `.bin` path (for cleanup on a
  /// failed commit).
  static Future<({DeletionVectorDescriptor dv, String path})> write(
    String tablePath,
    Iterable<int> rows,
  ) async {
    final uuid = _randomUuid();
    final path = '$tablePath/deletion_vector_${_formatUuid(uuid)}.bin';
    final cardinality = rows.length;
    final size = await _writeBin(path, rows);
    return (
      dv: DeletionVectorDescriptor(
        storageType: 'u',
        pathOrInlineDv: _z85Encode(uuid),
        offset: 1,
        sizeInBytes: size,
        cardinality: cardinality,
      ),
      path: path,
    );
  }

  /// Write the `.bin` framing (version byte, BE length, LE magic, portable
  /// Roaring bitmap, BE CRC-32) and return the payload size (== `sizeInBytes`).
  static Future<int> _writeBin(String path, Iterable<int> rows) async {
    final payload = BytesBuilder()
      ..add(
        (ByteData(4)..setUint32(0, magic, Endian.little)).buffer.asUint8List(),
      )
      ..add(Roaring.serializeTreemap(rows));
    final bytes = payload.toBytes();

    final out = BytesBuilder()..addByte(1); // format version
    out.add(
      (ByteData(
        4,
      )..setUint32(0, bytes.length, Endian.big)).buffer.asUint8List(),
    );
    out.add(bytes);
    out.add(
      (ByteData(
        4,
      )..setUint32(0, _crc32(bytes), Endian.big)).buffer.asUint8List(),
    );
    await File(path).writeAsBytes(out.toBytes());
    return bytes.length;
  }

  // Secure entropy so concurrent writers can't collide on a v4 UUID via
  // identical clock-seeded Random() in the same millisecond.
  static final _secure = Random.secure();

  static Uint8List _randomUuid() {
    final bytes = Uint8List(16);
    for (int i = 0; i < 16; i++) {
      bytes[i] = _secure.nextInt(256);
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant
    return bytes;
  }

  /// The absolute `.bin` path a path-backed deletion vector resolves to, or null
  /// for an inline (`i`) DV (which has no file). Used by orphan vacuuming to know
  /// which `deletion_vector_*.bin` files are still referenced.
  static String? absolutePath(
    String tablePath,
    DeletionVectorDescriptor descriptor,
  ) => switch (descriptor.storageType) {
    'p' => descriptor.pathOrInlineDv,
    'u' => _resolveUuid(tablePath, descriptor.pathOrInlineDv),
    _ => null,
  };

  static String _resolveUuid(String tablePath, String pathOrInline) {
    if (pathOrInline.length < 20) {
      throw Failure.of(
        Kind.corrupt,
        'Delta deletion vector "$pathOrInline" is shorter than its 20-character '
        'encoded UUID',
      );
    }
    final encoded = pathOrInline.substring(pathOrInline.length - 20);
    final prefix = pathOrInline.substring(0, pathOrInline.length - 20);
    final uuid = _formatUuid(_z85Decode(encoded));
    final directory = prefix.isEmpty ? tablePath : '$tablePath/$prefix';
    return '$directory/deletion_vector_$uuid.bin';
  }

  static String _formatUuid(Uint8List bytes) {
    String hex(int start, int end) => [
      for (int i = start; i < end; i++)
        bytes[i].toRadixString(16).padLeft(2, '0'),
    ].join();
    return '${hex(0, 4)}-${hex(4, 6)}-${hex(6, 8)}-${hex(8, 10)}-${hex(10, 16)}';
  }

  // --- Z85 ----------------------------------------------------------------
  static const String _alphabet =
      '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%\$#';
  static final List<int> _lookup = _buildLookup();
  static List<int> _buildLookup() {
    final lookup = List<int>.filled(128, -1);
    for (int i = 0; i < _alphabet.length; i++) {
      lookup[_alphabet.codeUnitAt(i)] = i;
    }
    return lookup;
  }

  static String _z85Encode(Uint8List data) {
    assert(data.length % 4 == 0);
    final buffer = StringBuffer();
    for (int i = 0; i < data.length; i += 4) {
      int value =
          ((data[i] << 24) |
              (data[i + 1] << 16) |
              (data[i + 2] << 8) |
              data[i + 3]) &
          0xFFFFFFFF;
      final chars = List<int>.filled(5, 0);
      for (int j = 4; j >= 0; j--) {
        chars[j] = value % 85;
        value = value ~/ 85;
      }
      for (final digit in chars) {
        buffer.writeCharCode(_alphabet.codeUnitAt(digit));
      }
    }
    return buffer.toString();
  }

  static Uint8List _z85Decode(String text) {
    if (text.length % 5 != 0) {
      throw FormatException('Z85 length ${text.length} not a multiple of 5');
    }
    final out = Uint8List(text.length ~/ 5 * 4);
    int offset = 0;
    for (int i = 0; i < text.length; i += 5) {
      int value = 0;
      for (int j = 0; j < 5; j++) {
        final unit = text.codeUnitAt(i + j);
        // Fail closed on any non-Z85 character (a bad external Delta log) rather
        // than indexing _lookup out of range or accepting its -1 sentinel, which
        // would silently decode to a bogus path/UUID.
        if (unit >= 128 || _lookup[unit] < 0) {
          throw FormatException('invalid Z85 character (code unit $unit)');
        }
        value = value * 85 + _lookup[unit];
      }
      out[offset++] = (value >> 24) & 0xFF;
      out[offset++] = (value >> 16) & 0xFF;
      out[offset++] = (value >> 8) & 0xFF;
      out[offset++] = value & 0xFF;
    }
    return out;
  }

  /// CRC-32/ISO-HDLC, the checksum a `.bin` deletion vector carries.
  static int _crc32(Uint8List bytes) => Crc32.hash(bytes);
}
