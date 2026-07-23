import 'dart:async';
import 'dart:io';
import 'package:logging/logging.dart';

/// Centralized logging configuration for libbigdata.
///
/// The library logs SPARINGLY and only where a silent decision would otherwise
/// be undebuggable — it is not a trace facility, and configuring it will not
/// narrate a query. What is instrumented today:
///
/// - Delta commit contention and conflict retries ([Loggers.storage] territory;
///   a writer backing off for seconds is otherwise indistinguishable from a
///   hang).
/// - Delta cleanup and checkpoint failures, which are deliberately swallowed so
///   they cannot fail an already-durable commit — the log is the only signal
///   they happened at all.
/// - JSON records skipped as malformed in permissive mode.
///
/// Everything else fails loud instead, which is the deliberate house style: an
/// error the caller must handle beats a warning nobody reads.
class LoggingConfig {
  static bool _initialized = false;
  static IOSink? _fileSink;
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
    await _fileSink?.flush();
    await _fileSink?.close();
    _fileSink = null;
    _initialized = false;
  }

  /// Initialize logging with the specified level
  ///
  /// [level] - The minimum logging level (default: INFO)
  /// [enableColors] - Whether to use colored output (default: true for terminals)
  /// [logToFile] - Optional file path to also log to a file
  static void initialize({
    Level level = Level.INFO,
    bool? enableColors,
    String? logToFile,
  }) {
    if (_initialized) return;

    // Enable hierarchical logging
    hierarchicalLoggingEnabled = true;

    // Set root logger level
    Logger.root.level = level;

    // Auto-detect color support if not specified
    final colors = enableColors ?? stdout.hasTerminal;

    // Configure console handler
    _console = Logger.root.onRecord.listen((record) {
      _handleLogRecord(record, enableColors: colors);
    });

    // Configure file handler if specified
    if (logToFile != null) {
      _setupFileLogging(logToFile);
    }

    _initialized = true;
  }

  /// Get a logger for the specified name
  static Logger getLogger(String name) {
    return Logger(name);
  }

  /// Handle a log record with optional color formatting
  static void _handleLogRecord(LogRecord record, {required bool enableColors}) {
    final timestamp = record.time.toIso8601String();
    final level = record.level.name.padRight(7);
    final logger = record.loggerName.isEmpty ? 'ROOT' : record.loggerName;
    final message = record.message;

    String formattedMessage;

    if (enableColors) {
      final colorCode = _getColorCode(record.level);
      final resetCode = '\x1B[0m';
      formattedMessage =
          '$colorCode$timestamp [$level] $logger: $message$resetCode';
    } else {
      formattedMessage = '$timestamp [$level] $logger: $message';
    }

    // Print to appropriate stream
    if (record.level >= Level.SEVERE) {
      stderr.writeln(formattedMessage);
    } else {
      stdout.writeln(formattedMessage);
    }

    // Print exception and stack trace if present
    if (record.error != null) {
      if (enableColors) {
        stderr.writeln('\x1B[31mError: ${record.error}\x1B[0m');
      } else {
        stderr.writeln('Error: ${record.error}');
      }
    }

    if (record.stackTrace != null) {
      if (enableColors) {
        stderr.writeln('\x1B[90m${record.stackTrace}\x1B[0m');
      } else {
        stderr.writeln('${record.stackTrace}');
      }
    }
  }

  /// Setup file logging
  static void _setupFileLogging(String filePath) {
    final file = File(filePath);
    final sink = file.openWrite(mode: FileMode.append);
    _fileSink =
        sink; // tracked so LoggingConfig.shutdown() can flush + close it

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
  static String _getColorCode(Level level) {
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

/// Common logger names used throughout the library
class Loggers {
  static const String parquet = 'libbigdata.parquet';
  static const String thrift = 'libbigdata.thrift';
  static const String dataframe = 'libbigdata.dataframe';
  static const String query = 'libbigdata.query';
  static const String storage = 'libbigdata.storage';
  static const String formats = 'libbigdata.formats';
  static const String test = 'libbigdata.test';
}
