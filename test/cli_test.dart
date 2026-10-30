import 'dart:convert';
import 'dart:io';

import 'package:test/test.dart';

/// Runs `bin/sql.dart` as a process, compiled once to a kernel file.
void main() {
  late Directory dir;
  late String kernel;

  setUpAll(() async {
    dir = Directory.systemTemp.createTempSync('cli_');
    kernel = '${dir.path}/sql.dill';
    final compiled = await Process.run(Platform.resolvedExecutable, [
      'compile',
      'kernel',
      'bin/sql.dart',
      '-o',
      kernel,
    ]);
    expect(compiled.exitCode, 0, reason: '${compiled.stderr}');
  });
  tearDownAll(() => dir.deleteSync(recursive: true));

  /// Run the CLI with [arguments], feeding [input] to stdin. TMPDIR points
  /// into [temp] so the engine's spill directory can be inspected.
  Future<ProcessResult> run(
    List<String> arguments, {
    String input = '',
    Directory? temp,
  }) async {
    final process = await Process.start(
      Platform.resolvedExecutable,
      [kernel, ...arguments],
      environment: {if (temp != null) 'TMPDIR': temp.path},
    );
    process.stdin.write(input);
    await process.stdin.close();
    final out = process.stdout.transform(utf8.decoder).join();
    final err = process.stderr.transform(utf8.decoder).join();
    final code = await process.exitCode.timeout(
      const Duration(seconds: 20),
      onTimeout: () {
        process.kill();
        return -1; // a REPL that never ends
      },
    );
    return ProcessResult(process.pid, code, await out, await err);
  }

  const decimal = 'SELECT CAST(1.5 AS DECIMAL(10,2)) AS d';

  test('a decimal prints at its scale in table, CSV and JSON', () async {
    final table = await run(['-e', decimal]);
    expect(table.stdout, contains('1.50'));
    final csv = await run(['-o', 'csv', '-e', decimal]);
    expect(csv.stdout, contains('1.50'));
    final json = await run(['-o', 'json', '-e', decimal]);
    expect(json.exitCode, 0, reason: '${json.stderr}');
    expect(json.stdout, contains('"1.50"'));
  });

  test('a date encodes in JSON', () async {
    final json = await run([
      '-o',
      'json',
      '-e',
      "SELECT DATE '2024-01-02' AS d",
    ]);
    expect(json.exitCode, 0, reason: '${json.stderr}');
    expect(json.stdout, contains('2024-01-02'));
  });

  test('a failing query exits with code 1', () async {
    final result = await run(['-e', 'SELECT nope FROM nowhere']);
    expect(result.exitCode, 1);
    expect(result.stderr, contains('Query failed'));
  });

  test('an empty stdin ends the REPL with code 0', () async {
    final result = await run(['-i']);
    expect(result.exitCode, 0);
  });

  test('.quit disposes the engine and leaves no spill directory', () async {
    final temp = Directory('${dir.path}/tmp')..createSync();
    final result = await run(['-i'], input: '.quit\n', temp: temp);
    expect(result.exitCode, 0);
    final spill = Directory('${temp.path}/libbigdata-spill');
    final sessions = spill.existsSync()
        ? spill.listSync().whereType<Directory>().toList()
        : const <Directory>[];
    expect(sessions, isEmpty);
  });

  test('.output with no argument names the current format', () async {
    final result = await run(['-i', '-o', 'csv'], input: '.output\n.quit\n');
    expect(result.stdout, contains('Current output format: csv'));
  });

  test('--version matches pubspec.yaml', () async {
    final pubspec = File('pubspec.yaml').readAsStringSync();
    final declared = RegExp(
      r'^version:\s*(\S+)',
      multiLine: true,
    ).firstMatch(pubspec)!.group(1)!;
    final result = await run(['--version']);
    expect(result.stdout, contains('v$declared'));
  });

  test('the help example for timing and verbose output is valid', () async {
    final result = await run(['--help']);
    expect(result.stdout, isNot(contains('-tv')));
    expect(result.stdout, contains('-t --verbose'));
  });

  group('--root', () {
    late String root;
    late String file;
    late String outside;

    setUp(() {
      root = dir.createTempSync('root_').path;
      file = '$root/people.csv';
      File(file).writeAsStringSync('name,age\nann,30\nbob,40\n');
      outside = '${dir.createTempSync('outside_').path}/x.csv';
      File(outside).writeAsStringSync('a\n1\n');
    });

    test('a query reads an absolute path under the root', () async {
      final result = await run([
        '--root',
        root,
        '-o',
        'csv',
        '-e',
        "SELECT COUNT(*) AS n FROM '$file'",
      ]);
      expect(result.exitCode, 0, reason: '${result.stderr}');
      expect(result.stdout, contains('2'));
    });

    test('without --root an absolute path is refused', () async {
      final result = await run(['-e', "SELECT COUNT(*) FROM '$file'"]);
      expect(result.exitCode, 1);
      expect(result.stderr, contains('denied'));
    });

    test('--csv loads an absolute path under the root', () async {
      final result = await run([
        '--root',
        root,
        '-o',
        'csv',
        '-c',
        'people:$file',
        '-e',
        'SELECT MAX(age) AS m FROM people',
      ]);
      expect(result.exitCode, 0, reason: '${result.stderr}');
      expect(result.stdout, contains('40'));
    });

    test('a path outside the root is refused', () async {
      final query = await run([
        '--root',
        root,
        '-e',
        "SELECT * FROM '$outside'",
      ]);
      expect(query.exitCode, 1);
      expect(query.stderr, contains('denied'));
      final load = await run([
        '--root',
        root,
        '-c',
        'x:$outside',
        '-e',
        'SELECT 1',
      ]);
      expect(load.exitCode, 1);
      expect(load.stderr, contains('denied'));
    });

    test('a relative path resolves against the root', () async {
      final result = await run([
        '--root',
        root,
        '-o',
        'csv',
        '-e',
        "SELECT COUNT(*) AS n FROM 'people.csv'",
      ]);
      expect(result.exitCode, 0, reason: '${result.stderr}');
      expect(result.stdout, contains('2'));
    });

    test('the root still allows writes such as CREATE VIEW', () async {
      final result = await run(
        ['--root', root, '-i', '-o', 'csv'],
        input:
            "CREATE VIEW v AS (SELECT * FROM 'people.csv');\n"
            'SELECT COUNT(*) AS n FROM v;\n'
            '.quit\n',
      );
      expect(result.exitCode, 0, reason: '${result.stderr}');
      expect('${result.stdout}${result.stderr}', isNot(contains('failed')));
      expect(result.stdout, contains('2'));
    });

    test('a missing root exits before any query', () async {
      final result = await run([
        '--root',
        '$root/missing',
        '-e',
        'SELECT 1 AS one',
      ]);
      expect(result.exitCode, 1);
      expect(result.stderr, contains('is not a directory'));
      expect(result.stdout, isNot(contains('one')));
    });
  });

  group('correctness', () {
    late String root;
    setUp(() => root = dir.createTempSync('correct_').path);

    test('a script splits on semicolons outside strings and comments', () async {
      final script = '$root/script.sql';
      File(script).writeAsStringSync(
        "SELECT 'a;b' AS s; -- c;d\n"
        'SELECT 2 AS n /* ; */;\n',
      );
      final result = await run(['--root', root, '-o', 'csv', '-f', script]);
      expect(result.exitCode, 0, reason: '${result.stderr}');
      expect(result.stdout, contains('a;b'));
      expect(result.stdout, contains('n\n2'));
    });

    test('a load path may contain a colon', () async {
      final folder = Directory('$root/x:y')..createSync();
      File('${folder.path}/t.csv').writeAsStringSync('a,b\n1,2\n');
      final result = await run([
        '--root',
        root,
        '-o',
        'csv',
        '-c',
        't:${folder.path}/t.csv',
        '-e',
        'SELECT a FROM t',
      ]);
      expect(result.exitCode, 0, reason: '${result.stderr}');
      expect(result.stdout, contains('a\n1'));
    });

    test('loading without a query opens the REPL; .tables does not guess '
        'rows', () async {
      File('$root/t.csv').writeAsStringSync('a,b\n1,2\n');
      final result = await run(
        ['--root', root, '-c', 't:$root/t.csv'],
        input: '.tables\n',
      );
      expect(result.exitCode, 0, reason: '${result.stderr}');
      expect(result.stdout, contains('t (2 columns)'));
      expect(result.stdout, isNot(contains('0 rows')));
    });

    test('-o csv quotes headers, carriage returns and every value type', () async {
      File('$root/t.csv').writeAsStringSync('s\n"x\ry"\n');
      final result = await run([
        '--root',
        root,
        '-o',
        'csv',
        '-c',
        't:$root/t.csv',
        '-e',
        'SELECT s AS "a,b" FROM t',
      ]);
      expect(result.exitCode, 0, reason: '${result.stderr}');
      expect(result.stdout, startsWith('"a,b"\n"x\ry"\n'));
    });

    test('a CSV load refuses a Parquet file', () async {
      File('$root/t.parquet').writeAsStringSync('');
      final result = await run([
        '--root',
        root,
        '-c',
        't:$root/t.parquet',
        '-e',
        'SELECT 1',
      ]);
      expect(result.exitCode, 1);
      expect(result.stderr, contains('CSV'));
    });

    test('the rule under the header is as wide as the header', () async {
      final result = await run(['-e', 'SELECT 1 AS a, 2 AS b']);
      final lines = '${result.stdout}'.split('\n');
      expect(lines[1], '-' * lines[0].length);
    });

    test('help names the installed command and does not promise a plan', () async {
      final result = await run(['--help']);
      expect(result.stdout, contains('Usage: libbigdata'));
      expect(result.stdout, isNot(contains('query plan')));
    });
  });
}
