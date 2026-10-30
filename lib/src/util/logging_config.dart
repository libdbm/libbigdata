import 'dart:async';
import 'dart:io';
import 'package:logging/logging.dart';

/// Centralized logging configuration for libbigdata.
///
/// The library logs SPARINGLY and only where a silent decision would otherwise
/// be undebuggable; it is not a trace facility, and configuring it will not
/// narrate a query. What is instrumented today:
///
/// - Delta commit contention and conflict retries ([Loggers.delta]; a writer
///   backing off for seconds is otherwise indistinguishable from a hang).
/// - Delta cleanup and checkpoint failures, which are deliberately swallowed so
///   they cannot fail an already-durable commit: the log is the only signal
///   they happened at all.
/// - JSON records skipped as malformed in permissive mode.
///
/// Everything else fails loud instead, which is the deliberate house style: an
/// error the caller must handle beats a warning nobody reads.
final class LoggingConfig {
  static bool _initialized = false;
  static IOSink? _sink;
  static StreamSubscription<LogRecord>? _console;
  static StreamSubscription<LogRecord>? _file;

  /// Cancel the log subscriptions and flush/close the file sink for a clean
  /// shutdown. The file subscription is cancelled BEFORE its sink is closed so a
  /// late record can't write to a closed sink; both are cancelled so a later
  /// re-[initialize] doesn't stack duplicate listeners.
  static Future<void> shutdown() async {
    await _file?.cancel();
    _file = null;
    await _console?.cancel();
    _console = null;
    await _sink?.flush();
    await _sink?.close();
    _sink = null;
    _initialized = false;
  }

  /// Initialize logging with the specified level. Calling it again replaces
  /// the previous configuration.
  ///
  /// [level] - The minimum logging level (default: INFO)
  /// [enableColors] - Whether to use colored output (default: true for terminals)
  /// [logToFile] - Optional file path to also log to a file
  static void initialize({
    Level level = Level.INFO,
    bool? enableColors,
    String? logToFile,
  }) {
    if (_initialized) {
      // Stop the previous listeners at once (a cancelled subscription gets no
      // further records) and let the old file finish in the background.
      unawaited(_console?.cancel());
      unawaited(_file?.cancel());
      final sink = _sink;
      if (sink != null) unawaited(sink.flush().then((_) => sink.close()));
      _console = null;
      _file = null;
      _sink = null;
    }

    // Enable hierarchical logging
    hierarchicalLoggingEnabled = true;

    // Set root logger level
    Logger.root.level = level;

    // Auto-detect color support if not specified
    final colors = enableColors ?? stdout.hasTerminal;

    // Configure console handler
    _console = Logger.root.onRecord.listen((record) {
      _handle(record, colors: colors);
    });

    // Configure file handler if specified
    if (logToFile != null) {
      _open(logToFile);
    }

    _initialized = true;
  }

  /// Get a logger for the specified name
  static Logger getLogger(String name) {
    return Logger(name);
  }

  /// Handle a log record with optional color formatting
  static void _handle(LogRecord record, {required bool colors}) {
    final timestamp = record.time.toIso8601String();
    final level = record.level.name.padRight(7);
    final logger = record.loggerName.isEmpty ? 'ROOT' : record.loggerName;
    final message = record.message;

    String line;

    if (colors) {
      final color = _color(record.level);
      final reset = '\x1B[0m';
      line = '$color$timestamp [$level] $logger: $message$reset';
    } else {
      line = '$timestamp [$level] $logger: $message';
    }

    // Print to appropriate stream
    if (record.level >= Level.SEVERE) {
      stderr.writeln(line);
    } else {
      stdout.writeln(line);
    }

    // Print exception and stack trace if present
    if (record.error != null) {
      if (colors) {
        stderr.writeln('\x1B[31mError: ${record.error}\x1B[0m');
      } else {
        stderr.writeln('Error: ${record.error}');
      }
    }

    if (record.stackTrace != null) {
      if (colors) {
        stderr.writeln('\x1B[90m${record.stackTrace}\x1B[0m');
      } else {
        stderr.writeln('${record.stackTrace}');
      }
    }
  }

  /// Setup file logging
  static void _open(String path) {
    final file = File(path);
    final sink = file.openWrite(mode: FileMode.append);
    _sink = sink; // tracked so LoggingConfig.shutdown() can flush + close it
    // An unwritable file reports its error on [IOSink.done]; unhandled, it
    // crashed the process. Stop logging to the file and say so instead.
    sink.done.catchError((Object error) {
      _file?.cancel();
      _file = null;
      if (identical(_sink, sink)) _sink = null;
      stderr.writeln('Logging to $path stopped: $error');
    });

    _file = Logger.root.onRecord.listen((record) {
      final timestamp = record.time.toIso8601String();
      final level = record.level.name.padRight(7);
      final logger = record.loggerName.isEmpty ? 'ROOT' : record.loggerName;
      final message = record.message;

      sink.writeln('$timestamp [$level] $logger: $message');

      if (record.error != null) {
        sink.writeln('Error: ${record.error}');
      }

      if (record.stackTrace != null) {
        sink.writeln('${record.stackTrace}');
      }
    });
  }

  /// Get ANSI color code for log level
  static String _color(Level level) {
    switch (level) {
      case Level.SEVERE:
      case Level.SHOUT:
        return '\x1B[31m'; // Red
      case Level.WARNING:
        return '\x1B[33m'; // Yellow
      case Level.INFO:
        return '\x1B[32m'; // Green
      case Level.CONFIG:
        return '\x1B[36m'; // Cyan
      case Level.FINE:
      case Level.FINER:
      case Level.FINEST:
        return '\x1B[90m'; // Gray
      default:
        return '\x1B[0m'; // Reset
    }
  }
}

/// The names the library logs under, one hierarchy below [root], so
/// `Logger(Loggers.delta).level = Level.FINE` turns up one subsystem (with
/// `hierarchicalLoggingEnabled`, which [LoggingConfig.initialize] sets).
final class Loggers {
  /// Unexpected errors converted to `Err` results.
  static const String root = 'libbigdata';

  /// DataFrame execution and its readers and writers.
  static const String dataframe = 'libbigdata.dataframe';

  /// Delta commits, conflict retries, cleanup and checkpoints.
  static const String delta = 'libbigdata.delta';

  /// JSON records skipped in permissive mode.
  static const String json = 'libbigdata.json';

  /// The SQL engine.
  static const String sql = 'libbigdata.sql';
}
