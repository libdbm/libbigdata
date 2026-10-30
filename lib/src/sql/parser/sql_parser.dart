import 'package:petitparser/petitparser.dart';
import '../../core/conversions.dart' show parseUtc;
import '../ast/ast.dart';
import 'limits.dart';

/// SQL Parser using PetitParser
class SqlParser {
  late final Parser<Statement> _parser;
  late final Parser<Expression> _expression;

  SqlParser() {
    _parser = _buildParser();
    final grammar = SqlGrammarDefinition();
    _expression = grammar
        .buildFrom(grammar.expression().end())
        .cast<Expression>();
  }

  /// Cap on raw statement length and parenthesis nesting depth. The grammar is
  /// recursive-descent, so deeply nested parens would overflow the stack. Both
  /// are bounded before parsing so untrusted SQL is not a DoS surface; a
  /// [StackOverflowError] the depth scan does not model is still caught and
  /// reported as a [FormatException].
  static const _maxSqlLength = 1 << 20; // 1 MiB of SQL text
  static const _maxNestingDepth = 256;

  /// Parse [text] as one complete expression, such as a constraint predicate.
  /// Nothing may follow it, so text that closes a surrounding parenthesis is
  /// rejected with a [FormatException].
  Expression parseExpression(String text) => _run(text, _expression);

  /// Parse SQL string into AST
  Statement parse(String sql) => _run(sql, _parser);

  T _run<T>(String sql, Parser<T> parser) {
    if (sql.length > _maxSqlLength) {
      throw FormatException(
        'SQL statement too large: ${sql.length} chars '
        '(limit $_maxSqlLength)',
      );
    }
    final text = _blankComments(sql);
    _guardDepth(text);

    final Result<T> result;
    try {
      result = parser.parse(text);
    } on StackOverflowError {
      // Backstop for nesting the depth scan does not model.
      throw FormatException('SQL nesting too deep', sql);
    } on FormatException catch (e) {
      // A numeric literal that overflows 64 bits is rejected by int.parse inside
      // the grammar; surface it in the normal parse-error shape rather than a
      // raw FormatException leaking from deep in the parser.
      throw FormatException('SQL Parse Error: ${e.message}', sql);
    }
    if (result is Success<T>) {
      checkLimits(result.value as Object);
      return result.value;
    }

    // Improved error reporting
    final failure = result as Failure;
    final location = _getLineAndColumn(sql, failure.position);
    throw FormatException(
      'SQL Parse Error at line ${location.line}, column ${location.column}: ${failure.message}',
      sql,
      failure.position,
    );
  }

  Parser<Statement> _buildParser() {
    final builder = SqlGrammarDefinition();
    return builder.build().cast<Statement>();
  }

  /// [sql] with each `--` line comment and `/* */` block comment replaced by
  /// spaces (newlines kept), so the grammar sees whitespace and every position
  /// in an error message still points into the original text. Quoted text is
  /// left alone.
  static String _blankComments(String sql) {
    final out = StringBuffer();
    var quote = 0; // 0 = not in quoted text; else the opening quote code unit
    var i = 0;
    while (i < sql.length) {
      final c = sql.codeUnitAt(i);
      final next = i + 1 < sql.length ? sql.codeUnitAt(i + 1) : 0;
      if (quote != 0) {
        if (c == quote) quote = 0; // a doubled quote re-opens next iteration
      } else if (c == 0x27 || c == 0x22 || c == 0x60) {
        quote = c; // ' " `
      } else if (c == 0x2D && next == 0x2D) {
        // -- to the end of the line
        while (i < sql.length && sql.codeUnitAt(i) != 0x0A) {
          out.write(' ');
          i++;
        }
        continue;
      } else if (c == 0x2F && next == 0x2A) {
        // /* to the next */
        final end = sql.indexOf('*/', i + 2);
        if (end < 0) {
          throw FormatException('Unterminated /* comment', sql, i);
        }
        for (var k = i; k < end + 2; k++) {
          out.write(sql.codeUnitAt(k) == 0x0A ? '\n' : ' ');
        }
        i = end + 2;
        continue;
      }
      out.writeCharCode(c);
      i++;
    }
    return out.toString();
  }

  /// Reject pathologically nested parentheses and CASE ... END blocks before
  /// recursive-descent parsing overflows the stack. All three grammar
  /// string/identifier quotes (`'`, `"`, backtick) are skipped so text inside
  /// them doesn't trip the guard.
  void _guardDepth(String sql) {
    int depth = 0, max = 0;
    int quote = 0; // 0 = not in a string; else the opening quote code unit
    int word = -1; // start of the current bare word, or -1
    for (int i = 0; i <= sql.length; i++) {
      final c = i < sql.length ? sql.codeUnitAt(i) : 0x20;
      if (quote != 0) {
        // closing quote (a doubled quote re-opens next iter)
        if (c == quote) quote = 0;
        continue;
      }
      if (_isWordChar(c)) {
        if (word < 0) word = i;
        continue;
      }
      if (word >= 0) {
        final token = sql.substring(word, i).toUpperCase();
        word = -1;
        if (token == 'CASE') {
          depth++;
          if (depth > max) max = depth;
        } else if (token == 'END' && depth > 0) {
          depth--;
        }
      }
      if (c == 0x27 || c == 0x22 || c == 0x60) {
        quote = c; // opening '  "  or  `
      } else if (c == 0x28) {
        depth++; // (
        if (depth > max) max = depth;
      } else if (c == 0x29 && depth > 0) {
        depth--; // )
      }
    }
    if (max > _maxNestingDepth) {
      throw FormatException('SQL nesting too deep: $max levels', sql);
    }
  }

  static bool _isWordChar(int c) =>
      (c >= 0x30 && c <= 0x39) || // 0-9
      (c >= 0x41 && c <= 0x5A) || // A-Z
      (c >= 0x61 && c <= 0x7A) || // a-z
      c == 0x5F; // _

  /// Get line and column from position
  ({int line, int column}) _getLineAndColumn(String text, int position) {
    int line = 1;
    int column = 1;

    for (int i = 0; i < position && i < text.length; i++) {
      if (text[i] == '\n') {
        line++;
        column = 1;
      } else {
        column++;
      }
    }

    return (line: line, column: column);
  }
}

/// SQL Grammar Definition
class SqlGrammarDefinition extends GrammarDefinition {
  @override
  Parser start() => ref0(statement).end();

  // Main statement parser: DML/DDL first, else a query.
  Parser<Statement> statement() =>
      (ref0(updateStatement) |
              ref0(insertStatement) |
              ref0(mergeStatement) |
              ref0(createViewStatement) |
              ref0(dropViewStatement) |
              ref0(queryStatement))
          .cast<Statement>();

  // ---- DDL: CREATE VIEW / DROP VIEW (a view is a persisted named query) ----
  Parser<Statement> createViewStatement() =>
      (ref1(keyword, 'CREATE') &
              (ref1(keyword, 'OR') & ref1(keyword, 'REPLACE')).optional() &
              ref1(keyword, 'VIEW') &
              (ref1(keyword, 'IF') &
                      ref1(keyword, 'NOT') &
                      ref1(keyword, 'EXISTS'))
                  .optional() &
              ref0(identifier) &
              (char('(').trim() &
                      ref0(
                        identifier,
                      ).plusSeparated(char(',').trim()).map((l) => l.elements) &
                      char(')').trim())
                  .optional() &
              ref1(keyword, 'AS') &
              // The body may be parenthesised or bare, as in other engines.
              ((char('(').trim() & ref0(selectStatement) & char(')').trim())
                      .pick(1) |
                  ref0(selectStatement)))
          .map((values) {
            final orReplace = values[1] != null;
            final ifNotExists = values[3] != null;
            final name = values[4] as String;
            final columns = values[5] != null
                ? (values[5][1] as List).cast<String>()
                : <String>[];
            final query = values[7] as SelectStatement;
            return CreateViewStatement(
              name: name,
              columns: columns,
              query: query,
              orReplace: orReplace,
              ifNotExists: ifNotExists,
            );
          });

  Parser<Statement> dropViewStatement() =>
      (ref1(keyword, 'DROP') &
              ref1(keyword, 'VIEW') &
              (ref1(keyword, 'IF') & ref1(keyword, 'EXISTS')).optional() &
              ref0(identifier))
          .map(
            (values) => DropViewStatement(
              name: values[3] as String,
              ifExists: values[2] != null,
            ),
          );

  // ---- DML: UPDATE / MERGE on a Delta table path ----
  Parser<Statement> updateStatement() =>
      (ref1(keyword, 'UPDATE') &
              ref0(filePathLiteral) &
              ref1(keyword, 'SET') &
              ref0(
                assignment,
              ).plusSeparated(char(',').trim()).map((l) => l.elements) &
              ref0(whereClause).optional())
          .map(
            (values) => UpdateStatement(
              targetPath: values[1] as String,
              assignments: (values[3] as List).cast<UpdateAssignment>(),
              where: values[4] as Expression?,
            ),
          );

  Parser<UpdateAssignment> assignment() =>
      (ref0(identifier) & char('=').trim() & ref0(expression)).map(
        (values) =>
            UpdateAssignment(values[0] as String, values[2] as Expression),
      );

  // `INSERT INTO '<delta path>' [(c1, c2)] VALUES (…), (…)`. Reuses the same
  // row production as the VALUES table.
  Parser<Statement> insertStatement() =>
      (ref1(keyword, 'INSERT') &
              ref1(keyword, 'INTO') &
              ref0(filePathLiteral) &
              (char('(').trim() &
                      ref0(
                        identifier,
                      ).plusSeparated(char(',').trim()).map((l) => l.elements) &
                      char(')').trim())
                  .optional() &
              ref1(keyword, 'VALUES') &
              ref0(
                valuesList,
              ).plusSeparated(char(',').trim()).map((l) => l.elements))
          .map((values) {
            final columns = values[3] as List?;
            return InsertStatement(
              targetPath: values[2] as String,
              columns: columns == null
                  ? const <String>[]
                  : (columns[1] as List).cast<String>(),
              values: (values[5] as List).cast<List<Expression>>(),
            );
          });

  Parser<Statement> mergeStatement() =>
      (ref1(keyword, 'MERGE') &
              ref1(keyword, 'INTO') &
              ref0(filePathLiteral) &
              ref0(mergeAlias).optional() &
              ref1(keyword, 'USING') &
              ref0(mergeSource) &
              ref0(mergeAlias).optional() &
              ref1(keyword, 'ON') &
              ref0(mergeCondition) &
              ref1(keyword, 'WHEN') &
              ref1(keyword, 'MATCHED') &
              ref1(keyword, 'THEN') &
              ref0(matchedAction) &
              (ref1(keyword, 'WHEN') &
                      ref1(keyword, 'NOT') &
                      ref1(keyword, 'MATCHED') &
                      ref1(keyword, 'THEN') &
                      ref1(keyword, 'INSERT') &
                      char('*').trim())
                  .optional())
          .map((values) {
            final path = values[2] as String;
            final source = values[5];
            final action = values[12] as String;
            // Each ON pair is (target, source): a pair written the other way
            // round names the source's qualifier first, or the target's last.
            final target = _mergeName(values[3]) ?? _basename(path);
            final named = _mergeName(values[6]) ?? _sourceName(source);
            final pairs = [
              for (final (left, right)
                  in (values[8] as List)
                      .cast<((String?, String), (String?, String))>())
                left.$1 == named || (right.$1 == target && left.$1 != target)
                    ? (right.$2, left.$2)
                    : (left.$2, right.$2),
            ];
            return MergeStatement(
              targetPath: path,
              sourcePath: source is String ? source : null,
              sourceQuery: source is SelectStatement ? source : null,
              onKeys: [for (final pair in pairs) pair.$1],
              sourceKeys: [for (final pair in pairs) pair.$2],
              matchedUpdateAll: action == 'update',
              matchedDelete: action == 'delete',
              notMatchedInsert: values[13] != null,
            );
          });

  Parser mergeAlias() => ref1(keyword, 'AS').optional() & ref0(identifier);

  /// A MERGE source: a file path, a parenthesized SELECT, or a table name
  /// (read as `SELECT * FROM name`, as in the SQL.md example).
  Parser mergeSource() =>
      (ref0(filePathLiteral) |
      (char('(').trim() & ref0(selectStatement) & char(')').trim()).map(
        (v) => v[1],
      ) |
      ref0(identifier).map(
        (name) => SelectStatement(
          selectList: [StarSelectItem()],
          from: [SimpleTable(table: name as String)],
        ),
      ));

  Parser<List<((String?, String), (String?, String))>> mergeCondition() =>
      ref0(mergeEquality)
          .plusSeparated(ref1(keyword, 'AND'))
          .map(
            (pairs) =>
                pairs.elements.cast<((String?, String), (String?, String))>(),
          );

  /// One ON equality, both sides kept: `t.id = s.key` merges on two names.
  Parser<((String?, String), (String?, String))> mergeEquality() =>
      (ref0(qualifiedColumn) &
              (string('==') | char('=')).trim() &
              ref0(qualifiedColumn))
          .map(
            (values) => (
              values[0] as (String?, String),
              values[2] as (String?, String),
            ),
          );

  Parser<(String?, String)> qualifiedColumn() =>
      (ref0(identifier) & (char('.') & ref0(identifier)).optional()).map(
        (values) => values[1] != null
            ? (values[0] as String?, (values[1] as List)[1] as String)
            : (null, values[0] as String),
      );

  /// The alias a [mergeAlias] parse holds, or null when absent.
  static String? _mergeName(Object? alias) =>
      alias is List ? alias.last as String : null;

  /// A MERGE source's default qualifier: a path's basename or a table's name.
  static String? _sourceName(Object? source) => switch (source) {
    final String path => _basename(path),
    SelectStatement(from: [SimpleTable(:final table, isFilePath: false)]) =>
      table,
    _ => null,
  };

  /// The basename of [path] without its extension (`data/a.csv` is `a`).
  static String _basename(String path) {
    final name = path.split(RegExp(r'[/\\]')).last;
    final dot = name.lastIndexOf('.');
    return dot > 0 ? name.substring(0, dot) : name;
  }

  Parser<String> matchedAction() =>
      ((ref1(keyword, 'UPDATE') & ref1(keyword, 'SET') & char('*').trim()).map(
                (_) => 'update',
              ) |
              ref1(keyword, 'DELETE').map((_) => 'delete'))
          .cast<String>();

  // WITH clause support
  Parser<Statement> queryStatement() =>
      (ref0(withClause).optional() & ref0(selectStatement)).map((values) {
        final ctes = values[0] as List<CommonTableExpression>?;
        final body = values[1] as SelectStatement;

        return ctes != null ? QueryStatement(ctes: ctes, select: body) : body;
      });

  Parser<List<CommonTableExpression>> withClause() =>
      (ref1(keyword, 'WITH') &
              ref1(keyword, 'RECURSIVE').optional() &
              ref0(
                commonTableExpression,
              ).plusSeparated(char(',').trim()).map((l) => l.elements))
          .map((values) {
            final recursive = values[1] != null;
            final ctes = (values[2] as List).cast<CommonTableExpression>();
            if (!recursive) return ctes;
            // RECURSIVE applies to the whole WITH; tag each CTE so the engine
            // can reject it (recursive evaluation is unsupported).
            return [
              for (final c in ctes)
                CommonTableExpression(
                  name: c.name,
                  columns: c.columns,
                  query: c.query,
                  recursive: true,
                ),
            ];
          });

  Parser<CommonTableExpression> commonTableExpression() =>
      (ref0(identifier) &
              (char('(').trim() &
                      ref0(
                        identifier,
                      ).plusSeparated(char(',').trim()).map((l) => l.elements) &
                      char(')').trim())
                  .optional() &
              ref1(keyword, 'AS') &
              char('(').trim() &
              ref0(selectStatement) &
              char(')').trim())
          .map((values) {
            final name = values[0] as String;
            final columns = values[1] != null
                ? (values[1][1] as List).cast<String>()
                : <String>[];
            final query = values[4] as SelectStatement;

            return CommonTableExpression(
              name: name,
              columns: columns,
              query: query,
            );
          });

  // A single SELECT with no trailing ORDER BY / LIMIT and no set-operation tail.
  // The building block of a set-operation chain: `A UNION B ORDER BY c` is one
  // chain of two cores with ONE trailing ORDER BY over the whole result, not two
  // selects each with their own clauses (which bound ORDER BY to the last branch
  // and nested the operators right-associatively).
  Parser<SelectStatement> selectCore() =>
      (ref1(keyword, 'SELECT') &
              ref1(keyword, 'DISTINCT').optional() &
              ref0(selectList) &
              ref0(fromClause).optional() &
              ref0(whereClause).optional() &
              ref0(groupByClause).optional() &
              ref0(havingClause).optional())
          .map((values) {
            final fromClause = values[3] as List<TableReference>?;
            return SelectStatement(
              distinct: values[1] != null,
              selectList: (values[2] as List).cast<SelectItem>(),
              from: fromClause ?? [],
              where: values[4] as Expression?,
              groupBy: (values[5] as List<Expression>?) ?? [],
              having: values[6] as Expression?,
            );
          });

  // SELECT statement: a set-operation chain (one or more cores) with an optional
  // trailing ORDER BY / LIMIT that applies to the WHOLE result. The tails are a
  // flat left-to-right list; the planner folds them left-associatively with
  // INTERSECT binding tighter than UNION/EXCEPT.
  Parser<SelectStatement> selectStatement() =>
      (ref0(selectCore) &
              ref0(setOperationTail).star() &
              ref0(orderByClause).optional() &
              ref0(limitClause).optional())
          .map((values) {
            final core = values[0] as SelectStatement;
            final tails = (values[1] as List).cast<SetOperation>();
            final orderBy = values[2] as List<OrderByItem>?;
            final limitOffset = values[3] as List<int?>?;
            if (tails.isEmpty && orderBy == null && limitOffset == null) {
              return core;
            }
            return SelectStatement(
              distinct: core.distinct,
              selectList: core.selectList,
              from: core.from,
              where: core.where,
              groupBy: core.groupBy,
              having: core.having,
              orderBy: orderBy ?? const [],
              limit: limitOffset?[0],
              offset: limitOffset?[1],
              operations: tails,
            );
          });

  // SELECT list
  Parser<List<SelectItem>> selectList() =>
      (char('*').trim().map((_) => <SelectItem>[StarSelectItem()]) |
              ref0(selectItem)
                  .plusSeparated(char(',').trim())
                  .map((list) => list.elements.cast<SelectItem>()))
          .cast<List<SelectItem>>();

  Parser<SelectItem> selectItem() =>
      (ref0(expression) & (ref1(keyword, 'AS') & ref0(identifier)).optional())
          .map((values) {
            final term = values[0] as Expression;
            final alias = values[1]?[1] as String?;
            return SelectItem(expression: term, alias: alias);
          });

  // FROM clause
  Parser<List<TableReference>> fromClause() =>
      (ref1(keyword, 'FROM') &
              ref0(
                tableReference,
              ).plusSeparated(char(',').trim()).map((l) => l.elements))
          .map((values) => (values[1] as List).cast<TableReference>());

  // One table followed by any joins. Trying "table plus joins" before "table"
  // re-parsed the table on backtrack, doubling the work per nesting level.
  Parser<TableReference> tableReference() =>
      (ref0(simpleTable) & ref0(joinClause).star()).map((values) {
        final left = values[0] as TableReference;
        final joins = (values[1] as List).cast<JoinClause>();
        return joins.isEmpty ? left : JoinedTable(left: left, joins: joins);
      });

  Parser<TableReference> simpleTable() =>
      (ref0(filePath).cast<TableReference>() |
              ref0(tableName).cast<TableReference>() |
              ref0(subqueryTable).cast<TableReference>() |
              ref0(valuesTable).cast<TableReference>())
          .cast<TableReference>();

  // Parse file path as table reference (e.g., 'data.parquet' or "data.csv")
  Parser<SimpleTable> filePath() =>
      (ref0(filePathLiteral) & ref0(tableAlias).optional()).map((values) {
        final path = values[0] as String;
        final suffix = values[1] as List?;

        return SimpleTable(
          table: path,
          alias: suffix?[0] as String?,
          isFilePath: true,
          columns: suffix?[1] as List<String>?,
        );
      });

  // Parse string literal as file path
  Parser<String> filePathLiteral() =>
      (_quoted("'") | _quoted('"')).cast<String>();

  Parser<SimpleTable> tableName() =>
      (ref0(identifier) & ref0(tableAlias).optional()).map((values) {
        final name = values[0] as String;
        final suffix = values[1] as List?;

        return SimpleTable(
          table: name,
          alias: suffix?[0] as String?,
          columns: suffix?[1] as List<String>?,
        );
      });

  Parser<List> tableAlias() =>
      ((ref1(keyword, 'AS').optional() & ref0(identifier)) &
              (char('(').trim() &
                      ref0(
                        identifier,
                      ).plusSeparated(char(',').trim()).map((l) => l.elements) &
                      char(')').trim())
                  .optional())
          .map((values) {
            if (values[1] == null) return [null, null];

            final alias = values[1] as String;
            final columns =
                values[2] != null && values[2] is List && values[2].length > 1
                ? (values[2][1] as List).cast<String>()
                : null;
            return [alias, columns];
          });

  Parser<SubqueryTable> subqueryTable() =>
      (char('(').trim() &
              ref0(selectStatement) &
              char(')').trim() &
              ref0(tableAlias))
          .map((values) {
            final subquery = values[1] as SelectStatement;
            final suffix = values[3] as List;

            return SubqueryTable(
              subquery: subquery,
              alias: suffix[0] as String,
              columns: suffix[1] as List<String>?,
            );
          });

  Parser<ValuesTable> valuesTable() =>
      (ref1(keyword, 'VALUES') &
              ref0(
                valuesList,
              ).plusSeparated(char(',').trim()).map((l) => l.elements) &
              ref0(tableAlias).optional())
          .map((values) {
            final rows = (values[1] as List).cast<List<Expression>>();
            final suffix = values[2] as List?;

            return ValuesTable(
              values: rows,
              alias: suffix?[0] as String?,
              columnAliases: suffix?[1] as List<String>?,
            );
          });

  Parser<List<Expression>> valuesList() =>
      (char('(').trim() &
              ref0(
                expression,
              ).plusSeparated(char(',').trim()).map((l) => l.elements) &
              char(')').trim())
          .map((values) => (values[1] as List).cast<Expression>());

  // JOIN support
  Parser<JoinClause> joinClause() =>
      (ref0(joinType) &
              ref1(keyword, 'JOIN') &
              ref0(simpleTable) &
              ref0(joinCondition).optional())
          .map((values) {
            final type = values[0] as JoinType;
            final condition = values[3] as JoinCondition?;
            // Only CROSS JOIN takes no condition, as in PostgreSQL and DuckDB;
            // any other join without one is an error, never a silent cross
            // join.
            if (condition == null && type != JoinType.cross) {
              throw FormatException(
                '${type.name.toUpperCase()} JOIN needs ON or USING '
                '(write CROSS JOIN for every pairing of rows)',
              );
            }
            return JoinClause(
              type: type,
              right: values[2] as TableReference,
              condition: condition,
            );
          });

  Parser<JoinType> joinType() =>
      (ref1(keyword, 'INNER') |
              ref1(keyword, 'LEFT').seq(ref1(keyword, 'OUTER').optional()) |
              ref1(keyword, 'RIGHT').seq(ref1(keyword, 'OUTER').optional()) |
              ref1(keyword, 'FULL').seq(ref1(keyword, 'OUTER').optional()) |
              ref1(keyword, 'CROSS') |
              epsilon())
          .flatten()
          .trim()
          .map((value) {
            // Collapse interior whitespace: the flatten spans keyword()'s own
            // trims, so `LEFT  OUTER` (two spaces, or a newline) arrived as
            // 'LEFT  OUTER', missed every case, and silently became INNER.
            final upper = value.trim().toUpperCase().replaceAll(
              RegExp(r'\s+'),
              ' ',
            );
            return switch (upper) {
              'INNER' => JoinType.inner,
              'LEFT' || 'LEFT OUTER' => JoinType.left,
              'RIGHT' || 'RIGHT OUTER' => JoinType.right,
              'FULL' || 'FULL OUTER' => JoinType.full,
              'CROSS' => JoinType.cross,
              _ => JoinType.inner,
            };
          });

  Parser<JoinCondition> joinCondition() =>
      (ref0(onCondition).cast<JoinCondition>() |
              ref0(usingCondition).cast<JoinCondition>())
          .cast<JoinCondition>();

  Parser<OnCondition> onCondition() => (ref1(keyword, 'ON') & ref0(expression))
      .map((values) => OnCondition(values[1] as Expression));

  Parser<UsingCondition> usingCondition() =>
      (ref1(keyword, 'USING') &
              char('(').trim() &
              ref0(
                identifier,
              ).plusSeparated(char(',').trim()).map((l) => l.elements) &
              char(')').trim())
          .map((values) => UsingCondition((values[2] as List).cast<String>()));

  // WHERE clause
  Parser<Expression> whereClause() =>
      (ref1(keyword, 'WHERE') & ref0(expression)).map(
        (values) => values[1] as Expression,
      );

  // GROUP BY clause
  Parser<List<Expression>> groupByClause() =>
      (ref1(keyword, 'GROUP') &
              ref1(keyword, 'BY') &
              ref0(
                expression,
              ).plusSeparated(char(',').trim()).map((l) => l.elements))
          .map((values) => (values[2] as List).cast<Expression>());

  // HAVING clause
  Parser<Expression> havingClause() =>
      (ref1(keyword, 'HAVING') & ref0(expression)).map(
        (values) => values[1] as Expression,
      );

  // ORDER BY clause
  Parser<List<OrderByItem>> orderByClause() =>
      (ref1(keyword, 'ORDER') &
              ref1(keyword, 'BY') &
              ref0(
                orderByItem,
              ).plusSeparated(char(',').trim()).map((l) => l.elements))
          .map((values) => (values[2] as List).cast<OrderByItem>());

  Parser<OrderByItem> orderByItem() =>
      (ref0(expression) &
              (ref1(keyword, 'ASC') | ref1(keyword, 'DESC')).optional() &
              (ref1(keyword, 'NULLS') &
                      (ref1(keyword, 'FIRST') | ref1(keyword, 'LAST')))
                  .optional())
          .map((values) {
            final term = values[0] as Expression;
            // keyword() flattens the ORIGINAL (case-preserved) text, so a
            // lowercase `asc` would fail an `== 'ASC'` check and sort DESCENDING.
            final direction = (values[1] as String?)?.toUpperCase();
            final ascending = direction == null || direction == 'ASC';
            final nulls = (values[2] as List?)?[1] as String?;
            // Explicit NULLS FIRST/LAST wins; otherwise NULLS LAST in both
            // directions, as in DuckDB (the default was NULLS FIRST for DESC,
            // which is Postgres's).
            final nullsFirst = nulls?.toUpperCase() == 'FIRST';
            return OrderByItem(
              expression: term,
              ascending: ascending,
              nullsFirst: nullsFirst,
            );
          });

  // LIMIT clause
  Parser<List<int?>> limitClause() =>
      (ref1(keyword, 'LIMIT') &
              ref0(integer) &
              (ref1(keyword, 'OFFSET') & ref0(integer)).optional())
          .map((values) {
            final limit = values[1] as int;
            final offset = values[2]?[1] as int?;
            return [limit, offset];
          });

  // One tail of a set-operation chain: an operator and the following core. The
  // right operand is a [selectCore] (not a full statement) so a chain's ORDER BY
  // / LIMIT and further tails stay at the top level rather than being swallowed
  // by the last branch.
  Parser<SetOperation> setOperationTail() =>
      ((ref1(keyword, 'UNION').seq(ref1(keyword, 'ALL').optional()) |
                      ref1(keyword, 'INTERSECT') |
                      ref1(keyword, 'EXCEPT'))
                  .flatten()
                  .trim() &
              ref0(selectCore))
          .map((values) {
            // keyword() flattens the ORIGINAL (case-preserved) text, so without
            // normalizing, a lowercase `union`/`intersect` fails every branch and
            // falls to the `except` default: silently returning wrong results.
            // Collapse interior whitespace too: `UNION  ALL` (two spaces, or a
            // newline) failed startsWith('UNION ALL') and silently DEDUPED as
            // plain UNION.
            final operator = (values[0] as String)
                .trim()
                .toUpperCase()
                .replaceAll(RegExp(r'\s+'), ' ');
            final core = values[1] as SelectStatement;

            final kind = switch (operator) {
              final String s when s.startsWith('UNION ALL') =>
                SetOperationType.unionAll,
              final String s when s.startsWith('UNION') =>
                SetOperationType.union,
              'INTERSECT' => SetOperationType.intersect,
              _ => SetOperationType.except,
            };

            return SetOperation(type: kind, right: core);
          });

  // Expressions
  Parser<Expression> expression() => ref0(orExpression).cast<Expression>();

  Parser<Expression> orExpression() =>
      ref0(andExpression).plusSeparated(ref1(keyword, 'OR')).map((values) {
        final operands = values.elements.cast<Expression>();
        return operands.reduce(
          (left, right) =>
              BinaryExpression(left: left, operator: 'OR', right: right),
        );
      });

  Parser<Expression> andExpression() =>
      ref0(notExpression).plusSeparated(ref1(keyword, 'AND')).map((values) {
        final operands = values.elements.cast<Expression>();
        return operands.reduce(
          (left, right) =>
              BinaryExpression(left: left, operator: 'AND', right: right),
        );
      });

  Parser<Expression> notExpression() =>
      (ref1(keyword, 'NOT').optional() & ref0(comparisonExpression)).map((
        values,
      ) {
        final not = values[0] != null;
        final operand = values[1] as Expression;
        return not
            ? UnaryExpression(operator: 'NOT', operand: operand)
            : operand;
      });

  Parser comparisonExpression() =>
      (ref0(additiveExpression) &
              (ref0(betweenPredicate) |
                      ref0(inPredicate) |
                      ref0(comparisonOperator))
                  .optional())
          .map((values) {
            final left = values[0] as Expression;
            final tail = values[1] as List?;
            if (tail == null) return left;
            final tag = tail[0] as String;

            // BETWEEN is DESUGARED into a comparison + AND/OR tree, so it
            // reuses the fully-tested evaluator (and preserves SQL three-valued
            // logic) instead of a dedicated node. IN keeps its own node: the
            // equivalent OR-chain re-scans the batch once per list element.
            switch (tag) {
              case 'BETWEEN': // x >= lo AND x <= hi
                return BinaryExpression(
                  left: BinaryExpression(
                    left: left,
                    operator: '>=',
                    right: tail[1] as Expression,
                  ),
                  operator: 'AND',
                  right: BinaryExpression(
                    left: left,
                    operator: '<=',
                    right: tail[2] as Expression,
                  ),
                );
              case 'NOT BETWEEN': // x < lo OR x > hi
                return BinaryExpression(
                  left: BinaryExpression(
                    left: left,
                    operator: '<',
                    right: tail[1] as Expression,
                  ),
                  operator: 'OR',
                  right: BinaryExpression(
                    left: left,
                    operator: '>',
                    right: tail[2] as Expression,
                  ),
                );
              case 'IN_LIST':
              case 'NOT_IN_LIST':
                return InExpression(
                  expression: left,
                  values: (tail[1] as List).cast<Expression>(),
                  negated: tag == 'NOT_IN_LIST',
                );
              case 'IN':
              case 'NOT IN':
                return InExpression(
                  expression: left,
                  subquery: tail[1] as SelectStatement,
                  negated: tag == 'NOT IN',
                );
              default: // comparisonOperator: [op, right]
                return BinaryExpression(
                  left: left,
                  operator: tag,
                  right: tail[1] as Expression,
                );
            }
          });

  // `x [NOT] BETWEEN lo AND hi`: bounds are additive (so the `AND` isn't eaten
  // by the enclosing AND-expression). Normalized to [tag, lo, hi].
  Parser betweenPredicate() =>
      (((ref1(keyword, 'NOT') & ref1(keyword, 'BETWEEN')).map(
                    (_) => 'NOT BETWEEN',
                  ) |
                  ref1(keyword, 'BETWEEN').map((_) => 'BETWEEN')) &
              ref0(additiveExpression) &
              ref1(keyword, 'AND') &
              ref0(additiveExpression))
          .map((v) => [v[0], v[1], v[3]]);

  // `x [NOT] IN (SELECT …)` or `x [NOT] IN (v1, v2, …)`. The parenthesis is
  // consumed once and the subquery tried before the list, so a malformed
  // list is not re-parsed as a subquery (nested lists were exponential).
  // Normalized to [tag, SelectStatement] or [tag, List<Expression>].
  Parser inPredicate() =>
      (((ref1(keyword, 'NOT') & ref1(keyword, 'IN')).map((_) => 'NOT') |
                  ref1(keyword, 'IN').map((_) => '')) &
              char('(').trim() &
              (ref0(selectStatement).map((query) => ['IN', query]) |
                  ref0(expression)
                      .plusSeparated(char(',').trim())
                      .map((l) => ['IN_LIST', l.elements])) &
              char(')').trim())
          .map((v) {
            final body = v[2] as List;
            final tag = v[0] == 'NOT'
                ? (body[0] == 'IN' ? 'NOT IN' : 'NOT_IN_LIST')
                : body[0] as String;
            return [tag, body[1]];
          });

  Parser comparisonOperator() =>
      ((string('<=') |
                  string('>=') |
                  string('<>') |
                  string('!=') |
                  string('==').map((_) => '=') |
                  char('<') |
                  char('>') |
                  char('=')) |
              ((ref1(keyword, 'NOT') & ref1(keyword, 'LIKE')).map(
                (_) => 'NOT LIKE',
              )) |
              (ref1(keyword, 'LIKE').map((_) => 'LIKE')) |
              ((ref1(keyword, 'IS') & ref1(keyword, 'NOT')).map(
                (_) => 'IS NOT',
              )) |
              (ref1(keyword, 'IS').map((_) => 'IS')))
          .trim() &
      ref0(additiveExpression);

  /// Folds `a OP b OP c` left (`(a OP b) OP c`) taking each operator from the
  /// separator it was parsed from. `foldLeft` returns a lone element untouched,
  /// so a single operand yields no [BinaryExpression].
  Expression _foldLeft(SeparatedList<Expression, String> chain) =>
      chain.foldLeft(
        (left, operator, right) =>
            BinaryExpression(left: left, operator: operator, right: right),
      );

  Parser additiveExpression() => ref0(multiplicativeExpression)
      .cast<Expression>()
      .plusSeparated((char('+') | char('-')).trim().cast<String>())
      .map(_foldLeft);

  Parser multiplicativeExpression() => ref0(unaryExpression)
      .cast<Expression>()
      .plusSeparated((char('*') | char('/') | char('%')).trim().cast<String>())
      .map(_foldLeft);

  Parser unaryExpression() =>
      ((char('+') | char('-')).trim().optional() & ref0(primaryExpression)).map(
        (values) {
          final sign = values[0] as String?;
          final operand = values[1] as Expression;

          if (sign == '-') {
            return UnaryExpression(operator: '-', operand: operand);
          } else if (sign == '+') {
            return UnaryExpression(operator: '+', operand: operand);
          }
          return operand;
        },
      );

  Parser primaryExpression() =>
      (ref0(literal) |
      ref0(functionCall) |
      ref0(caseExpression) |
      ref0(castExpression) |
      ref0(existsExpression) |
      ref0(columnReference) |
      ref0(starExpression) |
      ref0(subqueryExpression) |
      ref0(parenthesizedExpression));

  // `[NOT] EXISTS (SELECT …)`. NOT is bound here (not left to the enclosing
  // NOT-expression) so the negation reaches the ExistsExpression directly.
  Parser existsExpression() =>
      (((ref1(keyword, 'NOT') & ref1(keyword, 'EXISTS')).map((_) => true) |
                  ref1(keyword, 'EXISTS').map((_) => false)) &
              char('(').trim() &
              ref0(selectStatement) &
              char(')').trim())
          .map(
            (v) => ExistsExpression(
              subquery: v[2] as SelectStatement,
              negated: v[0] as bool,
            ),
          );

  Parser parenthesizedExpression() =>
      (char('(').trim() & ref0(expression) & char(')').trim()).map(
        (values) => values[1] as Expression,
      );

  Parser subqueryExpression() =>
      (char('(').trim() & ref0(selectStatement) & char(')').trim()).map(
        (values) => SubqueryExpression(subquery: values[1] as SelectStatement),
      );

  // Column reference
  Parser columnReference() =>
      (ref0(identifier) & (char('.').trim() & ref0(identifier)).optional()).map(
        (values) {
          if (values[1] != null) {
            // table.column
            return ColumnExpression(
              column: values[1][1] as String,
              table: values[0] as String,
            );
          } else {
            // just column
            return ColumnExpression(column: values[0] as String);
          }
        },
      );

  // Star expression
  Parser starExpression() =>
      ((ref0(identifier) & char('.').trim()).optional() & char('*').trim()).map(
        (values) {
          if (values[0] != null) {
            return QualifiedStarExpression(table: values[0][0] as String);
          } else {
            return StarExpression();
          }
        },
      );

  // Function call
  Parser functionCall() =>
      (ref0(identifier) &
              char('(').trim() &
              (ref1(keyword, 'DISTINCT').optional() &
                      (char('*').trim().map((_) => <Expression>[]) |
                          ref0(expression)
                              .plusSeparated(char(',').trim())
                              .map((l) => l.elements)))
                  .optional() &
              char(')').trim())
          .map((values) {
            final name = values[0] as String;
            final parts = values[2] as List?;

            if (parts == null) {
              return FunctionExpression(name: name, arguments: []);
            }

            final distinct = parts[0] != null;
            final arguments = parts[1] is List
                ? (parts[1] as List).cast<Expression>()
                : <Expression>[];

            return FunctionExpression(
              name: name,
              arguments: arguments,
              distinct: distinct,
            );
          });

  // CASE expression
  Parser caseExpression() =>
      (ref1(keyword, 'CASE') &
              ref0(expression).optional() &
              ref0(whenClause).plus() &
              ref0(elseClause).optional() &
              ref1(keyword, 'END'))
          .map((values) {
            final subject = values[1] as Expression?;
            // `.plus()` yields a List<dynamic>; a direct `as List<WhenClause>`
            // is a failing downcast, so build a properly-typed view.
            final whenClauses = (values[2] as List).cast<WhenClause>();
            final fallback = values[3] as Expression?;

            return CaseExpression(
              expression: subject,
              whenClauses: whenClauses,
              elseExpression: fallback,
            );
          });

  Parser whenClause() =>
      (ref1(keyword, 'WHEN') &
              ref0(expression) &
              ref1(keyword, 'THEN') &
              ref0(expression))
          .map(
            (values) => WhenClause(
              condition: values[1] as Expression,
              result: values[3] as Expression,
            ),
          );

  Parser elseClause() => (ref1(keyword, 'ELSE') & ref0(expression)).map(
    (values) => values[1] as Expression,
  );

  // CAST expression
  Parser castExpression() =>
      (ref1(keyword, 'CAST') &
              char('(').trim() &
              ref0(expression) &
              ref1(keyword, 'AS') &
              ref0(dataType) &
              char(')').trim())
          .map((values) {
            final operand = values[2] as Expression;
            final type = values[4] as String;
            return CastExpression(expression: operand, targetType: type);
          });

  // Literals
  Parser literal() =>
      (ref0(nullLiteral) |
      ref0(booleanLiteral) |
      ref0(temporalLiteral) |
      ref0(numericLiteral) |
      ref0(stringLiteral));

  // `DATE '2024-01-01'` / `TIMESTAMP '2024-01-01 10:00'`: a CAST of the text,
  // checked here so a malformed literal is a parse error rather than NULL.
  Parser temporalLiteral() =>
      ((ref1(keyword, 'DATE') | ref1(keyword, 'TIMESTAMP')) &
              _quoted("'").trim())
          .map((values) {
            final type = (values[0] as String).toUpperCase();
            final text = values[1] as String;
            if (parseUtc(text) == null) {
              throw FormatException("Invalid $type literal '$text'");
            }
            return CastExpression(
              expression: LiteralExpression(text),
              targetType: type,
            );
          });

  Parser nullLiteral() =>
      ref1(keyword, 'NULL').map((_) => LiteralExpression(null));

  // No .flatten() here: keyword() trims surrounding whitespace, and a flatten
  // OVER it captures that whitespace too: `'TRUE '.toUpperCase() == 'TRUE'`
  // is false, so `SELECT true` literally returned false whenever the literal
  // was followed by a space. keyword() already yields the bare token text.
  Parser booleanLiteral() =>
      (ref1(keyword, 'TRUE') | ref1(keyword, 'FALSE')).map(
        (value) => LiteralExpression((value as String).toUpperCase() == 'TRUE'),
      );

  Parser numericLiteral() =>
      (ref0(decimalLiteral) |
      (ref0(floatingPoint) | ref0(integer)).map(
        (value) => LiteralExpression(value),
      ));

  /// A number with a point and no exponent is an exact DECIMAL, as in DuckDB:
  /// `1.50` is DECIMAL(3,2) and `0.1 + 0.2` is exactly 0.3 (it was a double).
  /// Precision is the digit count without leading zeros, scale the digits
  /// after the point. Past 38 digits it stays a double, as in DuckDB.
  Parser decimalLiteral() =>
      (((digit().plus() & char('.') & digit().star()) |
                      (char('.') & digit().plus()))
                  .flatten() &
              anyOf('eE').not())
          .pick(0)
          .trim()
          .map((text) {
            final value = text as String;
            final point = value.indexOf('.');
            final whole = value
                .substring(0, point)
                .replaceFirst(RegExp('^0+'), '');
            final scale = value.length - point - 1;
            final precision = whole.length + scale;
            if (precision > 38) return LiteralExpression(double.parse(value));
            return CastExpression(
              expression: LiteralExpression(value),
              targetType: 'DECIMAL(${precision < 1 ? 1 : precision},$scale)',
            );
          });

  /// Match a [q]-quoted string in which a doubled quote (`''` / `""`) is an
  /// escaped literal quote (standard SQL); yields the unescaped content.
  Parser<String> _quoted(String q) =>
      (char(q) &
              (string('$q$q').map((_) => q) | char(q).neg()).star() &
              char(q))
          .map((v) => (v[1] as List).cast<String>().join());

  // Single quotes only: `"..."` is a quoted identifier (standard SQL).
  Parser stringLiteral() => _quoted("'").map((s) => LiteralExpression(s));

  // Reserved keywords that cannot be used as identifiers
  static const reservedKeywords = {
    'SELECT',
    'FROM',
    'WHERE',
    'GROUP',
    'BY',
    'HAVING',
    'ORDER',
    'LIMIT',
    'OFFSET',
    'AS',
    'JOIN',
    'ON',
    'LEFT',
    'RIGHT',
    'INNER',
    'OUTER',
    'FULL',
    'CROSS',
    'USING',
    'NATURAL',
    'AND',
    'OR',
    'NOT',
    'IN',
    'EXISTS',
    'BETWEEN',
    'LIKE',
    'IS',
    'NULL',
    'TRUE',
    'FALSE',
    'CASE',
    'WHEN',
    'THEN',
    'ELSE',
    'END',
    'WITH',
    'RECURSIVE',
    'DISTINCT',
    'ALL',
    'UNION',
    'INTERSECT',
    'EXCEPT',
    'VALUES',
    'INSERT',
    'INTO',
    'CREATE',
    'TABLE',
    'VIEW',
    'DROP',
    'ALTER',
    'DELETE',
    'UPDATE',
    'SET',
    'ASC',
    'DESC',
    'NULLS',
    'FIRST',
    'LAST',
  };

  // Basic tokens
  Parser identifier() =>
      (
          // Quoted identifiers can be any string including keywords; a doubled
          // quote is an escaped literal quote.
          _quoted('"') |
              _quoted('`') |
              // Unquoted identifiers must not be reserved keywords
              (letter() & word().star()).flatten().where((value) {
                return !reservedKeywords.contains((value).toUpperCase());
              }))
          .trim()
          .map((value) => value as String);

  Parser dataType() =>
      ((ref1(keyword, 'INTEGER') |
                  ref1(keyword, 'INT') |
                  ref1(keyword, 'BIGINT') |
                  ref1(keyword, 'SMALLINT') |
                  ref1(keyword, 'TINYINT') |
                  ref1(keyword, 'FLOAT') |
                  ref1(keyword, 'DOUBLE') |
                  ref1(keyword, 'REAL') |
                  ref1(keyword, 'DECIMAL') |
                  ref1(keyword, 'NUMERIC') |
                  ref1(keyword, 'VARCHAR') |
                  ref1(keyword, 'TEXT') |
                  ref1(keyword, 'BOOLEAN') |
                  ref1(keyword, 'BOOL') |
                  ref1(keyword, 'DATETIME') |
                  ref1(keyword, 'DATE') |
                  ref1(keyword, 'TIMESTAMP') |
                  ref1(keyword, 'TIME')) &
              // Optional `(p[,s])`: DECIMAL(10,2), VARCHAR(30), ...
              (char('(').trim() &
                      ref0(integer) &
                      (char(',').trim() & ref0(integer)).optional() &
                      char(')').trim())
                  .optional())
          // No .flatten() over keyword(): it would also capture whitespace
          // keyword() consumed, so `CAST(x AS INTEGER )` carried 'INTEGER '
          // into _parseDataType and silently fell to its string default.
          .map((values) {
            final name = values[0] as String;
            final params = values[1] as List?;
            if (params == null) return name;
            final p = params[1] as int;
            final s = params[2] == null ? null : (params[2][1] as int);
            return s == null ? '$name($p)' : '$name($p,$s)';
          });

  Parser integer() =>
      digit().plus().flatten().trim().map((value) => int.parse(value));

  // A float has a decimal point (digits on at least one side: `1.5`, `.5`, `1.`)
  // and/or an exponent (`1e10`, `1.5E-3`). A bare integer has neither, so it
  // still falls through to [integer].
  Parser floatingPoint() {
    final digits = digit().plus();
    final exponent = anyOf('eE') & anyOf('+-').optional() & digits;
    final fractional =
        ((digits & char('.') & digit().star()) | (char('.') & digits)) &
        exponent.optional();
    final scientific = digits & exponent;
    return (fractional | scientific).flatten().trim().map(
      (value) => double.parse(value),
    );
  }

  // Keywords (case-insensitive) - must not be followed by word characters
  Parser keyword(String keyword) =>
      (string(keyword, ignoreCase: true) & word().not())
          .pick(0)
          .flatten()
          .trim();
}
