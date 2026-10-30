/// A line of text and the terminator that ended it: `\n`, `\r\n`, `\r`, or
/// empty for a last line with none.
typedef Line = ({String text, String end});

/// Splits a decoded character [chunks] stream into lines on `\n`, `\r\n` or a
/// lone `\r` (classic Mac and Excel "CSV (Macintosh)" files), keeping each
/// terminator, and throws a [FormatException] as soon as an in-progress line
/// exceeds [maxBytes], BEFORE the whole oversized line is buffered, so a
/// pathological single multi-GB line can't OOM the process (unlike
/// `LineSplitter`, which buffers a full physical line unbounded). The bound
/// counts UTF-16 code units, which is what a Dart string's memory grows with
/// (the options document it as characters); a non-ASCII line is up to three
/// times as many UTF-8 bytes. [label] names the record kind in the error
/// message.
Stream<Line> splitLines(
  Stream<String> chunks,
  int maxBytes, {
  String label = 'record',
}) async* {
  final rest = StringBuffer();
  // A `\r` that ended a chunk: the line it ends is held until the next chunk
  // shows whether a `\n` follows it.
  String? held;
  void guard() {
    if (rest.length > maxBytes) {
      throw FormatException('$label exceeds maxRecordBytes ($maxBytes)');
    }
  }

  await for (final chunk in chunks) {
    var start = 0;
    if (held != null) {
      final crlf = chunk.startsWith('\n');
      yield (text: held, end: crlf ? '\r\n' : '\r');
      held = null;
      if (crlf) start = 1;
    }
    // The next `\n` and `\r` at or after start, each searched only once it
    // has been passed (most files have no `\r` at all).
    var feed = chunk.indexOf('\n', start);
    var carriage = chunk.indexOf('\r', start);
    while (feed >= 0 || carriage >= 0) {
      final at = carriage < 0 || (feed >= 0 && feed < carriage)
          ? feed
          : carriage;
      rest.write(chunk.substring(start, at));
      guard();
      final text = rest.toString();
      rest.clear();
      if (at == feed) {
        yield (text: text, end: '\n');
        start = at + 1;
      } else if (at + 1 == chunk.length) {
        held = text;
        start = at + 1;
      } else if (chunk.codeUnitAt(at + 1) == 0x0a) {
        yield (text: text, end: '\r\n');
        start = at + 2;
      } else {
        yield (text: text, end: '\r');
        start = at + 1;
      }
      if (feed >= 0 && feed < start) feed = chunk.indexOf('\n', start);
      if (carriage >= 0 && carriage < start) {
        carriage = chunk.indexOf('\r', start);
      }
    }
    if (start < chunk.length) {
      rest.write(chunk.substring(start));
      guard(); // bound the not-yet-terminated line
    }
  }
  if (held != null) yield (text: held, end: '\r');
  if (rest.isNotEmpty) yield (text: rest.toString(), end: '');
}

/// [splitLines] without the terminators.
Stream<String> cappedLines(
  Stream<String> chunks,
  int maxBytes, {
  String label = 'record',
}) => splitLines(chunks, maxBytes, label: label).map((line) => line.text);
