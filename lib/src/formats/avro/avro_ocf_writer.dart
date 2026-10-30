import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'avro_decoder.dart';
import 'avro_encoder.dart';

/// Writes an Avro Object Container File: magic, file metadata
/// (`avro.schema`/`avro.codec`), sync marker, then a single data block.
/// Supports the `null` and `deflate` (raw) codecs.
class AvroOcfWriter {
  static void writePath(
    String path,
    String schemaJson,
    List<Map<String, dynamic>> records, {
    String codec = 'deflate',
  }) => File(path).writeAsBytesSync(
    write(schemaJson, records, codec: codec),
    // Force the bytes to stable media. Iceberg publishes a metadata pointer
    // (fsynced + atomically renamed) that references these manifest/manifest-list
    // files; without this flush the content could still be in the page cache when
    // the pointer goes live, so a crash leaves a committed snapshot pointing at a
    // missing/truncated manifest. Matches every other side-file writer in the repo.
    flush: true,
  );

  /// Async variant of [writePath]: doesn't block the event loop on the write.
  static Future<void> writePathAsync(
    String path,
    String schemaJson,
    List<Map<String, dynamic>> records, {
    String codec = 'deflate',
  }) => File(
    path,
  ).writeAsBytes(write(schemaJson, records, codec: codec), flush: true);

  static Uint8List write(
    String schemaJson,
    List<Map<String, dynamic>> records, {
    String codec = 'deflate',
  }) {
    final schema = AvroSchema.parse(schemaJson);
    final sync = _randomSync();
    final out = BytesBuilder();
    out.add([0x4F, 0x62, 0x6A, 0x01]); // magic "Obj\x01"

    // File metadata: map<string,bytes> with avro.schema and avro.codec.
    final meta = AvroWriter();
    meta.writeLong(2);
    meta.writeString('avro.schema');
    meta.writeBytes(utf8.encode(schemaJson));
    meta.writeString('avro.codec');
    meta.writeBytes(utf8.encode(codec));
    meta.writeLong(0);
    out.add(meta.toBytes());
    out.add(sync);

    if (records.isNotEmpty) {
      final body = AvroWriter();
      for (final record in records) {
        schema.encode(body, record);
      }
      final compressed = _compress(codec, body.toBytes());
      final header = AvroWriter()
        ..writeLong(records.length)
        ..writeLong(compressed.length);
      out.add(header.toBytes());
      out.add(compressed);
      out.add(sync);
    }
    return out.toBytes();
  }

  static Uint8List _compress(String codec, Uint8List raw) {
    switch (codec) {
      case 'null':
        return raw;
      case 'deflate':
        return Uint8List.fromList(ZLibCodec(raw: true).encode(raw));
      default:
        throw FormatException('Unsupported Avro write codec: $codec');
    }
  }

  // Secure entropy: the sync marker must be unpredictable and unique per file
  // (a clock-seeded Random() repeats across same-millisecond writers).
  static final _secure = Random.secure();

  static Uint8List _randomSync() =>
      Uint8List.fromList([for (int i = 0; i < 16; i++) _secure.nextInt(256)]);
}
