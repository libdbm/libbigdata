/// Splits a decoded character [chunks] stream into lines on `\n` (stripping a
/// trailing `\r`), throwing a [FormatException] as soon as an in-progress line
/// exceeds [maxBytes] — BEFORE the whole oversized line is buffered, so a
/// pathological single multi-GB line can't OOM the process (unlike `LineSplitter`,
/// which buffers a full physical line unbounded). Only `\n` / `\r\n` terminators
/// are recognised. [label] names the record kind in the error message.
Stream<String> cappedLines(
  Stream<String> chunks,
  int maxBytes, {
  String label = 'record',
}) async* {
  final rem = StringBuffer();
  String strip(String s) => s.endsWith('\r') ? s.substring(0, s.length - 1) : s;
  void guard() {
    if (rem.length > maxBytes) {
      throw FormatException('$label exceeds maxRecordBytes ($maxBytes)');
    }
  }

  await for (final chunk in chunks) {
    var start = 0;
    while (true) {
      final nl = chunk.indexOf('\n', start);
      if (nl < 0) {
        rem.write(chunk.substring(start));
        guard(); // bound the not-yet-terminated line
        break;
      }
      rem.write(chunk.substring(start, nl));
      guard();
      yield strip(rem.toString());
      rem.clear();
      start = nl + 1;
    }
  }
  if (rem.isNotEmpty) yield strip(rem.toString());
}
