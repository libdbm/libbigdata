#!/usr/bin/env dart

import 'dart:io';
import 'dart:convert';
import 'package:args/args.dart';
import 'package:libbigdata/libbigdata.dart';

const String version = '1.0.0';

void main(List<String> arguments) async {
  final parser = ArgParser()
    ..addFlag('help', abbr: 'h', help: 'Show this help message')
    ..addFlag('version', abbr: 'v', help: 'Show version information')
    ..addFlag(
      'interactive',
      abbr: 'i',
      help: 'Start interactive REPL mode',
      defaultsTo: false,
    )
    ..addOption('file', abbr: 'f', help: 'Execute SQL from file')
    ..addOption('execute', abbr: 'e', help: 'Execute SQL statement directly')
    ..addOption(
      'csv',
      abbr: 'c',
      help: 'Load CSV file as table (format: name:path)',
    )
    ..addOption(
      'parquet',
      abbr: 'p',
      help: 'Load Parquet file as table (format: name:path)',
    )
    ..addOption(
      'output',
      abbr: 'o',
      help: 'Output format',
      allowed: ['table', 'json', 'csv'],
      defaultsTo: 'table',
    )
    ..addFlag(
      'timing',
      abbr: 't',
      help: 'Show query execution time',
      defaultsTo: false,
    )
    ..addFlag(
      'verbose',
      help: 'Show verbose output including query plan',
      defaultsTo: false,
    );

  try {
    final results = parser.parse(arguments);

    if (results['help']) {
      printUsage(parser);
      exit(0);
    }

    if (results['version']) {
      print('BigData SQL CLI v$version');
      print('A pure Dart SQL engine for data analysis');
      exit(0);
    }

    // Initialize SQL engine. Own its lifetime in try/finally so the per-session
    // spill directory is always cleaned up (dispose), even on an error or exit.
    final engine = SqlEngine();
    try {
      final verbose = results['verbose'] as bool;
      final timing = results['timing'] as bool;
      final outputFormat = results['output'] as String;

      // Load data files if specified
      if (results['csv'] != null) {
        await loadCsvFile(engine, results['csv'] as String, verbose);
      }

      if (results['parquet'] != null) {
        await loadParquetFile(engine, results['parquet'] as String, verbose);
      }

      // Execute SQL
      if (results['execute'] != null) {
        // Execute single statement
        final sql = results['execute'] as String;
        await executeQuery(engine, sql, outputFormat, timing, verbose);
      } else if (results['file'] != null) {
        // Execute from file
        final filePath = results['file'] as String;
        final sql = await File(filePath).readAsString();
        await executeSqlScript(engine, sql, outputFormat, timing, verbose);
      } else if (results['interactive'] ||
          (results.rest.isEmpty &&
              results['csv'] == null &&
              results['parquet'] == null)) {
        // Start interactive mode
        await runInteractiveMode(engine, outputFormat, timing, verbose);
      } else if (results.rest.isNotEmpty) {
        // Treat remaining arguments as SQL
        final sql = results.rest.join(' ');
        await executeQuery(engine, sql, outputFormat, timing, verbose);
      }
    } finally {
      await engine.dispose();
    }
  } catch (e) {
    stderr.writeln('Error: $e');
    exit(1);
  }
}

void printUsage(ArgParser parser) {
  print('''
BigData SQL CLI - A pure Dart SQL engine for data analysis

Usage: sql [options] [SQL statement]

Options:
${parser.usage}

Examples:
  # Execute a single query
  sql -e "SELECT * FROM employees WHERE age > 30"
  
  # Load CSV and query it
  sql -c employees:data.csv -e "SELECT COUNT(*) FROM employees"
  
  # Load Parquet file
  sql -p sales:sales.parquet -e "SELECT SUM(amount) FROM sales"
  
  # Execute SQL script
  sql -f queries.sql
  
  # Interactive mode
  sql -i
  
  # With timing and verbose output
  sql -tv -e "SELECT * FROM employees ORDER BY salary DESC"
  
  # Output as JSON
  sql -o json -e "SELECT * FROM employees"

Interactive Mode Commands:
  .help          Show help
  .tables        List loaded tables
  .schema TABLE  Show table schema
  .load CSV      Load CSV file (format: name:path)
  .load PARQUET  Load Parquet file (format: name:path)
  .output FORMAT Set output format (table, json, csv)
  .timing ON/OFF Toggle query timing
  .verbose ON/OFF Toggle verbose output
  .quit          Exit
''');
}

Future<void> loadCsvFile(SqlEngine engine, String spec, bool verbose) async {
  final parts = spec.split(':');
  if (parts.length != 2) {
    throw ArgumentError('CSV spec must be in format name:path');
  }

  final tableName = parts[0];
  final filePath = parts[1];

  if (verbose) print('Loading CSV file: $filePath as table "$tableName"...');

  await engine.registerFile(tableName, filePath);

  if (verbose) {
    print('Successfully loaded CSV file as table "$tableName"');
  }
}

Future<void> loadParquetFile(
  SqlEngine engine,
  String spec,
  bool verbose,
) async {
  final parts = spec.split(':');
  if (parts.length != 2) {
    throw ArgumentError('Parquet spec must be in format name:path');
  }

  final tableName = parts[0];
  final filePath = parts[1];

  if (verbose) {
    print('Loading Parquet file: $filePath as table "$tableName"...');
  }

  await engine.registerFile(tableName, filePath);

  if (verbose) {
    print('Successfully loaded Parquet file as table "$tableName"');
  }
}

Future<void> executeQuery(
  SqlEngine engine,
  String sql,
  String outputFormat,
  bool timing,
  bool verbose,
) async {
  if (verbose) {
    print('Executing: $sql');
    print('-' * 60);
  }

  final stopwatch = timing ? (Stopwatch()..start()) : null;

  try {
    final result = await engine.execute(sql);

    if (stopwatch != null) {
      stopwatch.stop();
      print('Execution time: ${stopwatch.elapsedMilliseconds}ms');
      print('');
    }

    // Convert Table to DataChunk for output
    final chunks = result.toChunks();
    if (chunks.isEmpty) {
      print('(0 rows)');
    } else {
      // Merge all chunks into a single DataChunk for output
      final allColumns = <Column>[];
      for (int i = 0; i < result.schema.fields.length; i++) {
        final columnData = <dynamic>[];
        for (final chunk in chunks) {
          if (i < chunk.columns.length) {
            columnData.addAll(chunk.columns[i].toList());
          }
        }
        allColumns.add(
          ListColumn(
            name: result.schema.fields[i].name,
            data: columnData,
            dataType: result.schema.fields[i].dataType,
          ),
        );
      }
      final mergedChunk = DataChunk(columns: allColumns, schema: result.schema);
      outputResult(mergedChunk, outputFormat);
    }
  } catch (e) {
    stderr.writeln('Query failed: $e');
  }
}

Future<void> executeSqlScript(
  SqlEngine engine,
  String script,
  String outputFormat,
  bool timing,
  bool verbose,
) async {
  // Split script into individual statements (simple split on semicolon)
  final statements = script
      .split(';')
      .map((s) => s.trim())
      .where((s) => s.isNotEmpty)
      .toList();

  for (final sql in statements) {
    if (verbose) {
      print(
        '\nExecuting statement ${statements.indexOf(sql) + 1}/${statements.length}',
      );
    }
    await executeQuery(engine, sql, outputFormat, timing, verbose);
  }
}

Future<void> runInteractiveMode(
  SqlEngine engine,
  String outputFormat,
  bool timing,
  bool verbose,
) async {
  print('BigData SQL Interactive Mode v$version');
  print('Type ".help" for help, ".quit" to exit\n');

  var currentOutputFormat = outputFormat;
  var currentTiming = timing;
  var currentVerbose = verbose;

  while (true) {
    stdout.write('sql> ');
    final input = stdin.readLineSync();

    if (input == null || input.trim().isEmpty) continue;

    final trimmed = input.trim();

    // Handle special commands
    if (trimmed.startsWith('.')) {
      await handleCommand(
        trimmed,
        engine,
        (format) => currentOutputFormat = format,
        (value) => currentTiming = value,
        (value) => currentVerbose = value,
      );
      continue;
    }

    // Handle multi-line SQL
    var sql = trimmed;
    while (!sql.endsWith(';') && !sql.startsWith('.')) {
      stdout.write('   > ');
      final nextLine = stdin.readLineSync();
      if (nextLine == null) break;
      sql += '\n$nextLine';
    }

    if (sql.endsWith(';')) {
      sql = sql.substring(0, sql.length - 1);
    }

    await executeQuery(
      engine,
      sql,
      currentOutputFormat,
      currentTiming,
      currentVerbose,
    );
  }
}

Future<void> handleCommand(
  String command,
  SqlEngine engine,
  Function(String) setOutputFormat,
  Function(bool) setTiming,
  Function(bool) setVerbose,
) async {
  final parts = command.split(' ');
  final cmd = parts[0].toLowerCase();

  switch (cmd) {
    case '.help':
      print('''
Interactive Mode Commands:
  .help              Show this help
  .tables            List loaded tables
  .schema TABLE      Show schema of a table
  .load csv NAME:PATH    Load CSV file as table
  .load parquet NAME:PATH Load Parquet file as table
  .output FORMAT     Set output format (table, json, csv)
  .timing on/off     Toggle query timing
  .verbose on/off    Toggle verbose output
  .quit              Exit
''');
      break;

    case '.tables':
      final tables = engine.getTables().keys.toList()..sort();
      if (tables.isEmpty) {
        print('No tables loaded');
      } else {
        print('Tables:');
        for (final table in tables) {
          final t = engine.getTables()[table]!;
          print('  $table (${t.rowCount} rows)');
        }
      }
      break;

    case '.schema':
      if (parts.length < 2) {
        print('Usage: .schema TABLE');
        return;
      }
      final tableName = parts[1];
      final table = engine.getTables()[tableName];
      if (table == null) {
        print('Table "$tableName" not found');
        return;
      }
      print('Schema for $tableName:');
      for (final field in table.schema.fields) {
        print('  ${field.name}: ${field.dataType}');
      }
      break;

    case '.load':
      if (parts.length < 3) {
        print('Usage: .load csv|parquet NAME:PATH');
        return;
      }
      final type = parts[1].toLowerCase();
      final spec = parts.sublist(2).join(' ');
      try {
        if (type == 'csv') {
          await loadCsvFile(engine, spec, true);
        } else if (type == 'parquet') {
          await loadParquetFile(engine, spec, true);
        } else {
          print('Unknown file type: $type');
        }
      } catch (e) {
        print('Failed to load file: $e');
      }
      break;

    case '.output':
      if (parts.length < 2) {
        print('Current output format: $setOutputFormat');
        return;
      }
      final format = parts[1].toLowerCase();
      if (['table', 'json', 'csv'].contains(format)) {
        setOutputFormat(format);
        print('Output format set to: $format');
      } else {
        print('Invalid format. Use: table, json, or csv');
      }
      break;

    case '.timing':
      if (parts.length < 2) {
        print('Usage: .timing on|off');
        return;
      }
      final value = parts[1].toLowerCase() == 'on';
      setTiming(value);
      print('Timing ${value ? "enabled" : "disabled"}');
      break;

    case '.verbose':
      if (parts.length < 2) {
        print('Usage: .verbose on|off');
        return;
      }
      final value = parts[1].toLowerCase() == 'on';
      setVerbose(value);
      print('Verbose output ${value ? "enabled" : "disabled"}');
      break;

    case '.quit':
    case '.exit':
      print('Goodbye!');
      exit(0);

    default:
      print('Unknown command: $cmd');
      print('Type .help for available commands');
  }
}

void outputResult(DataChunk result, String format) {
  switch (format) {
    case 'json':
      outputJson(result);
      break;
    case 'csv':
      outputCsv(result);
      break;
    case 'table':
    default:
      outputTable(result);
      break;
  }
}

void outputTable(DataChunk result) {
  if (result.rowCount == 0) {
    print('(0 rows)');
    return;
  }

  // Check if output is being piped (not a terminal)
  final isInteractive = stdout.hasTerminal;

  // Calculate column widths
  final widths = <int>[];
  for (final column in result.columns) {
    var maxWidth = column.name.length;
    for (int i = 0; i < result.rowCount && i < 100; i++) {
      final value = column[i]?.toString() ?? 'NULL';
      maxWidth = maxWidth > value.length ? maxWidth : value.length;
    }
    widths.add(maxWidth.clamp(3, 50));
  }

  // Print header
  final header = result.columns
      .asMap()
      .entries
      .map((e) => e.value.name.padRight(widths[e.key]))
      .join(' | ');
  print(header);
  print('-' * (header.length + (result.columns.length - 1) * 3));

  // Print rows - limit to 1000 for interactive display, all rows when piped
  final maxRows = isInteractive && result.rowCount > 1000
      ? 1000
      : result.rowCount;
  for (int row = 0; row < maxRows; row++) {
    final values = <String>[];
    for (int col = 0; col < result.columns.length; col++) {
      final value = result.columns[col][row];
      final str = value?.toString() ?? 'NULL';
      values.add(str.padRight(widths[col]));
    }
    print(values.join(' | '));
  }

  if (isInteractive && result.rowCount > maxRows) {
    print('... (${result.rowCount - maxRows} more rows)');
  }
  print('\n(${result.rowCount} rows)');
}

void outputJson(DataChunk result) {
  final rows = <Map<String, dynamic>>[];

  for (int row = 0; row < result.rowCount; row++) {
    final rowMap = <String, dynamic>{};
    for (final column in result.columns) {
      rowMap[column.name] = column[row];
    }
    rows.add(rowMap);
  }

  print(JsonEncoder.withIndent('  ').convert(rows));
  print('\n(${result.rowCount} rows)');
}

void outputCsv(DataChunk result) {
  // Print header
  print(result.columns.map((c) => c.name).join(','));

  // Print rows
  for (int row = 0; row < result.rowCount; row++) {
    final values = result.columns
        .map((c) {
          final value = c[row];
          if (value == null) return '';
          if (value is String &&
              (value.contains(',') ||
                  value.contains('"') ||
                  value.contains('\n'))) {
            return '"${value.replaceAll('"', '""')}"';
          }
          return value.toString();
        })
        .join(',');
    print(values);
  }
}
