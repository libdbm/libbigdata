import 'dart:io';
import 'package:logging/logging.dart';
import 'package:libbigdata/libbigdata.dart';

void main() async {
  // Example 1: Basic logging setup
  print('=== Basic Logging Setup ===');
  LoggingConfig.initialize(level: Level.INFO);

  final logger = LoggingConfig.getLogger('example');
  logger.info('Starting Parquet processing example');

  // Example 2: Custom logging levels
  print('\n=== Custom Logging Levels ===');
  LoggingConfig.initialize(level: Level.FINE, enableColors: true);

  final parquetLogger = LoggingConfig.getLogger(Loggers.parquet);
  parquetLogger.fine('Processing Parquet metadata...');
  parquetLogger.info('File parsed successfully');
  parquetLogger.warning('Unknown encoding detected');

  // Example 3: File logging
  print('\n=== File Logging ===');
  final logFile = 'logs/libbigdata.log';
  await Directory('logs').create(recursive: true);

  LoggingConfig.initialize(level: Level.ALL, logToFile: logFile);

  final testLogger = LoggingConfig.getLogger('test');
  testLogger.info('This message goes to both console and file');
  testLogger.severe('Error processing file', 'Simulated error');

  // Example 4: Different logger categories
  print('\n=== Logger Categories ===');
  final dataframeLogger = LoggingConfig.getLogger(Loggers.dataframe);
  final queryLogger = LoggingConfig.getLogger(Loggers.query);
  final storageLogger = LoggingConfig.getLogger(Loggers.storage);

  dataframeLogger.info('DataFrame operation started');
  queryLogger.config('SQL query parsed');
  storageLogger.fine('Reading from storage layer');

  // Example 5: Hierarchical logging
  print('\n=== Hierarchical Logging ===');
  final parentLogger = LoggingConfig.getLogger('myapp');
  final childLogger = LoggingConfig.getLogger('myapp.module');
  final grandchildLogger = LoggingConfig.getLogger('myapp.module.component');

  parentLogger.level = Level.WARNING; // Only warnings and above

  parentLogger.info('This will not be shown');
  parentLogger.warning('This will be shown');
  childLogger.severe('Child logger inherits parent level');
  grandchildLogger.shout('Grandchild logger also inherits');

  // Example 6: Production vs Development logging
  print('\n=== Environment-based Logging ===');
  final isDevelopment = Platform.environment['ENVIRONMENT'] != 'production';

  LoggingConfig.initialize(
    level: isDevelopment ? Level.FINE : Level.WARNING,
    enableColors: isDevelopment,
    logToFile: isDevelopment ? null : 'production.log',
  );

  final appLogger = LoggingConfig.getLogger('app');
  appLogger.fine('Debug info (development only)');
  appLogger.info('General information');
  appLogger.warning('Warning message (always shown)');

  print('\nLogging examples completed. Check logs/ directory for file output.');
}
