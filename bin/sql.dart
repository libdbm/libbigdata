#!/usr/bin/env dart

import 'dart:io';
import 'dart:convert';
import 'package:args/args.dart';
import 'package:libbigdata/libbigdata.dart';

/// Must match `version:` in pubspec.yaml (test/cli_test.dart checks).
const String version = '2.0.0';

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
      'root',
      help:
          'Directory SQL and loaded files may read from, by absolute or '
          'relative path (default: the current directory, relative paths only)',
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
      help: 'Show each statement and load as it runs',
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
    // --root widens the path sandbox to a chosen directory. The CLI's user
    // is the operator, so writes stay allowed as they are without it.
    final root = results['root'] as String?;
    if (root != null && !Directory(root).existsSync()) {
      stderr.writeln('Error: --root "$root" is not a directory');
      exit(1);
    }
    final SqlEngine engine;
    switch (SqlEngine.open(sandboxRoot: root, readOnly: false)) {
      case Ok(:final value):
        engine = value;
      case Err(:final error):
        stderr.writeln('Error: $error');
        exit(1);
    }
    try {
      final verbose = results['verbose'] as bool;
      final timing = results['timing'] as bool;
      final format = results['output'] as String;

      // Load data files if specified. A failed load prints its kind and
      // message and exits non-zero (after the finally below disposes).
      for (final (option, load) in [
        ('csv', loadCsvFile),
        ('parquet', loadParquetFile),
      ]) {
        if (results[option] == null) continue;
        final loaded = await load(engine, results[option] as String, verbose);
        if (loaded case Err(:final error)) {
          stderr.writeln('Error: $error');
          exitCode = 1;
          return;
        }
      }

      // Execute SQL. A failed statement sets a non-zero exit code; exitCode
      // (not exit()) lets the finally below dispose the engine first.
      if (results['execute'] != null) {
        // Execute single statement
        final sql = results['execute'] as String;
        if (!await executeQuery(engine, sql, format, timing, verbose)) {
          exitCode = 1;
        }
      } else if (results['file'] != null) {
        // Execute from file
        final path = results['file'] as String;
        final sql = await File(path).readAsString();
        if (!await executeSqlScript(engine, sql, format, timing, verbose)) {
          exitCode = 1;
        }
      } else if (results['interactive'] || results.rest.isEmpty) {
        // No SQL to run, so open the REPL (on the loaded files, if any).
        await runInteractiveMode(engine, format, timing, verbose);
      } else if (results.rest.isNotEmpty) {
        // Treat remaining arguments as SQL
        final sql = results.rest.join(' ');
        if (!await executeQuery(engine, sql, format, timing, verbose)) {
          exitCode = 1;
        }
      }
    } finally {
      await engine.dispose();
    }
  } catch (error) {
    stderr.writeln('Error: $error');
    exit(1);
  }
}

void printUsage(ArgParser parser) {
  print('''
BigData SQL CLI - A pure Dart SQL engine for data analysis

Usage: libbigdata [options] [SQL statement]

Options:
${parser.usage}

Examples:
  # Execute a single query
  libbigdata -e "SELECT * FROM employees WHERE age > 30"
  
  # Load CSV and query it
  libbigdata -c employees:data.csv -e "SELECT COUNT(*) FROM employees"
  
  # Load Parquet file
  libbigdata -p sales:sales.parquet -e "SELECT SUM(amount) FROM sales"

  # Read files under /data by absolute path
  libbigdata --root /data -e "SELECT COUNT(*) FROM '/data/sales.parquet'"
  
  # Execute SQL script
  libbigdata -f queries.sql
  
  # Interactive mode
  libbigdata -i
  
  # With timing and verbose output
  libbigdata -t --verbose -e "SELECT * FROM employees ORDER BY salary DESC"
  
  # Output as JSON
  libbigdata -o json -e "SELECT * FROM employees"

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

/// Register the CSV file named by [spec] (`name:path`) on [engine].
Future<Result<void, ErrorDetails>> loadCsvFile(
  SqlEngine engine,
  String spec,
  bool verbose,
) => _load(engine, spec, 'CSV', '.csv', verbose);

/// Register the Parquet file named by [spec] (`name:path`) on [engine].
Future<Result<void, ErrorDetails>> loadParquetFile(
  SqlEngine engine,
  String spec,
  bool verbose,
) => _load(engine, spec, 'Parquet', '.parquet', verbose);

/// Register the [format] file named by [spec] on [engine]. The name ends at
/// the FIRST colon, so a path may hold more (`C:\data.csv`), and the path
/// must have the format's [extension]: the engine reads by extension, so a
/// `--csv` of a Parquet file was read as Parquet.
Future<Result<void, ErrorDetails>> _load(
  SqlEngine engine,
  String spec,
  String format,
  String extension,
  bool verbose,
) async {
  final colon = spec.indexOf(':');
  if (colon <= 0 || colon == spec.length - 1) {
    return Err(
      ErrorDetails(Kind.invalid, '$format spec must be in format name:path'),
    );
  }

  final name = spec.substring(0, colon);
  final path = spec.substring(colon + 1);
  if (!path.toLowerCase().endsWith(extension)) {
    return Err(
      ErrorDetails(
        Kind.invalid,
        '$path is not a $format file (expected a $extension extension)',
      ),
    );
  }

  if (verbose) {
    print('Loading $format file: $path as table "$name"...');
  }

  final loaded = await engine.registerFile(name, path);
  if (verbose && loaded is Ok) {
    print('Successfully loaded $format file as table "$name"');
  }
  return loaded;
}

/// Run [sql] and print its result; false when the query failed.
Future<bool> executeQuery(
  SqlEngine engine,
  String sql,
  String format,
  bool timing,
  bool verbose,
) async {
  if (verbose) {
    print('Executing: $sql');
    print('-' * 60);
  }

  final stopwatch = timing ? (Stopwatch()..start()) : null;

  switch (await engine.execute(sql)) {
    case Ok(:final value):
      if (stopwatch != null) {
        stopwatch.stop();
        print('Execution time: ${stopwatch.elapsedMilliseconds}ms');
        print('');
      }
      outputResult(value, format);
      return true;
    case Err(:final error):
      stderr.writeln('Query failed: $error');
      return false;
  }
}

/// Run each statement in [script]; false when any of them failed.
Future<bool> executeSqlScript(
  SqlEngine engine,
  String script,
  String format,
  bool timing,
  bool verbose,
) async {
  final list = statements(script);

  var succeeded = true;
  for (final (index, sql) in list.indexed) {
    if (verbose) {
      print('\nExecuting statement ${index + 1}/${list.length}');
    }
    if (!await executeQuery(engine, sql, format, timing, verbose)) {
      succeeded = false;
    }
  }
  return succeeded;
}

/// The statements of [script], split on semicolons outside quoted strings,
/// quoted identifiers and comments (a plain split cut `'a;b'` in two). A
/// piece holding only comments is dropped.
List<String> statements(String script) {
  final out = <String>[];
  final current = StringBuffer();
  var code = false; // whether [current] holds more than comments and spaces
  void end() {
    if (code) out.add(current.toString().trim());
    current.clear();
    code = false;
  }

  var i = 0;
  while (i < script.length) {
    final char = script[i];
    var stop = i + 1;
    if (char == "'" || char == '"') {
      // A doubled quote is an escaped one.
      while (stop < script.length &&
          (script[stop] != char ||
              (stop + 1 < script.length && script[stop + 1] == char))) {
        stop += script[stop] == char ? 2 : 1;
      }
      stop = stop < script.length ? stop + 1 : stop;
      code = true;
    } else if (script.startsWith('--', i)) {
      final line = script.indexOf('\n', i);
      stop = line < 0 ? script.length : line;
    } else if (script.startsWith('/*', i)) {
      final close = script.indexOf('*/', i + 2);
      stop = close < 0 ? script.length : close + 2;
    } else if (char == ';') {
      end();
      i = stop;
      continue;
    } else if (char.trim().isNotEmpty) {
      code = true;
    }
    current.write(script.substring(i, stop));
    i = stop;
  }
  end();
  return out;
}

Future<void> runInteractiveMode(
  SqlEngine engine,
  String format,
  bool timing,
  bool verbose,
) async {
  print('BigData SQL Interactive Mode v$version');
  print('Type ".help" for help, ".quit" to exit\n');

  var output = format;
  var timed = timing;
  var detailed = verbose;

  while (true) {
    stdout.write('sql> ');
    final input = stdin.readLineSync();

    if (input == null) break; // end of input: leave cleanly
    if (input.trim().isEmpty) continue;

    final trimmed = input.trim();

    // Handle special commands
    if (trimmed.startsWith('.')) {
      final quit = await handleCommand(
        trimmed,
        engine,
        output,
        (value) => output = value,
        (value) => timed = value,
        (value) => detailed = value,
      );
      if (quit) break; // return to main, which disposes the engine
      continue;
    }

    // Handle multi-line SQL
    var sql = trimmed;
    while (!sql.endsWith(';') && !sql.startsWith('.')) {
      stdout.write('   > ');
      final line = stdin.readLineSync();
      if (line == null) break;
      sql += '\n$line';
    }

    if (sql.endsWith(';')) {
      sql = sql.substring(0, sql.length - 1);
    }

    await executeQuery(engine, sql, output, timed, detailed);
  }
}

/// Run one `.command`; true when it asks the REPL to stop.
Future<bool> handleCommand(
  String command,
  SqlEngine engine,
  String current,
  Function(String) setOutputFormat,
  Function(bool) setTiming,
  Function(bool) setVerbose,
) async {
  final parts = command.split(' ');
  final keyword = parts[0].toLowerCase();

  switch (keyword) {
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
      final tables = engine.tables.keys.toList()..sort();
      if (tables.isEmpty) {
        print('No tables loaded');
      } else {
        print('Tables:');
        // A loaded file is read lazily, so its row count is not known here;
        // the catalog's placeholder reported 0 rows.
        for (final table in tables) {
          final loaded = engine.tables[table]!;
          print('  $table (${loaded.schema.fields.length} columns)');
        }
      }
      break;

    case '.schema':
      if (parts.length < 2) {
        print('Usage: .schema TABLE');
        return false;
      }
      final name = parts[1];
      final table = engine.tables[name];
      if (table == null) {
        print('Table "$name" not found');
        return false;
      }
      print('Schema for $name:');
      for (final field in table.schema.fields) {
        print('  ${field.name}: ${field.dataType}');
      }
      break;

    case '.load':
      if (parts.length < 3) {
        print('Usage: .load csv|parquet NAME:PATH');
        return false;
      }
      final type = parts[1].toLowerCase();
      final spec = parts.sublist(2).join(' ');
      final loaded = switch (type) {
        'csv' => await loadCsvFile(engine, spec, true),
        'parquet' => await loadParquetFile(engine, spec, true),
        _ => null,
      };
      if (loaded == null) {
        print('Unknown file type: $type');
      } else if (loaded case Err(:final error)) {
        print('Failed to load file: $error');
      }
      break;

    case '.output':
      if (parts.length < 2) {
        print('Current output format: $current');
        return false;
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
        return false;
      }
      final value = parts[1].toLowerCase() == 'on';
      setTiming(value);
      print('Timing ${value ? "enabled" : "disabled"}');
      break;

    case '.verbose':
      if (parts.length < 2) {
        print('Usage: .verbose on|off');
        return false;
      }
      final value = parts[1].toLowerCase() == 'on';
      setVerbose(value);
      print('Verbose output ${value ? "enabled" : "disabled"}');
      break;

    case '.quit':
    case '.exit':
      print('Goodbye!');
      return true;

    default:
      print('Unknown command: $keyword');
      print('Type .help for available commands');
  }
  return false;
}

/// Print [result] in [format]. Rows come from `Table.row`, which presents
/// a decimal as a scaled `Decimal` (the column itself holds unscaled
/// integers), so every format prints `1.50` rather than `150`.
void outputResult(Table result, String format) {
  final names = [for (final field in result.schema.fields) field.name];
  final rows = result.toRows();
  switch (format) {
    case 'json':
      outputJson(rows);
      break;
    case 'csv':
      // The library's writer: it quotes headers, carriage returns and every
      // value type as reading the file back expects.
      print(CsvWriter().writeTableToString(result));
      break;
    case 'table':
    default:
      outputTable(names, rows);
      break;
  }
}

void outputTable(List<String> names, List<Map<String, dynamic>> rows) {
  if (rows.isEmpty) {
    print('(0 rows)');
    return;
  }

  // Check if output is being piped (not a terminal)
  final interactive = stdout.hasTerminal;

  // Calculate column widths
  final widths = <int>[];
  for (final name in names) {
    var width = name.length;
    for (int i = 0; i < rows.length && i < 100; i++) {
      final value = rows[i][name]?.toString() ?? 'NULL';
      width = width > value.length ? width : value.length;
    }
    widths.add(width.clamp(3, 50));
  }

  // Print header
  final header = names
      .asMap()
      .entries
      .map((entry) => entry.value.padRight(widths[entry.key]))
      .join(' | ');
  print(header);
  print('-' * header.length);

  // Print rows - limit to 1000 for interactive display, all rows when piped
  final limit = interactive && rows.length > 1000 ? 1000 : rows.length;
  for (int row = 0; row < limit; row++) {
    final values = <String>[];
    for (int column = 0; column < names.length; column++) {
      final text = rows[row][names[column]]?.toString() ?? 'NULL';
      values.add(text.padRight(widths[column]));
    }
    print(values.join(' | '));
  }

  if (interactive && rows.length > limit) {
    print('... (${rows.length - limit} more rows)');
  }
  print('\n(${rows.length} rows)');
}

void outputJson(List<Map<String, dynamic>> rows) {
  // Dates print as ISO-8601 and decimals (and BigInt) as their exact text;
  // JSON numbers would round them through a double.
  final encoder = JsonEncoder.withIndent(
    '  ',
    (value) => value is DateTime ? value.toIso8601String() : value.toString(),
  );
  print(encoder.convert(rows));
  print('\n(${rows.length} rows)');
}
