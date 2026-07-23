import 'package:petitparser/petitparser.dart';
import '../ast/ast.dart';

/// SQL Parser using PetitParser
class SqlParser {
  late final Parser<Statement> _parser;

  SqlParser() {
    _parser = _buildParser();
  }

  /// Cap on raw statement length and parenthesis nesting depth. The grammar is
  /// recursive-descent, so deeply nested parens would overflow the native stack
  /// (an uncatchable crash) — bound them before parsing turns untrusted SQL into
  /// a DoS surface.
  static const _maxSqlLength = 1 << 20; // 1 MiB of SQL text
  static const _maxNestingDepth = 256;

  /// Parse SQL string into AST
  Statement parse(String sql) {
    if (sql.length > _maxSqlLength) {
      throw FormatException(
        'SQL statement too large: ${sql.length} chars '
        '(limit $_maxSqlLength)',
      );
    }
    _guardDepth(sql);

    final Result<Statement> result;
    try {
      result = _parser.parse(sql);
    } on FormatException catch (e) {
      // A numeric literal that overflows 64 bits is rejected by int.parse inside
      // the grammar; surface it in the normal parse-error shape rather than a
      // raw FormatException leaking from deep in the parser.
      throw FormatException('SQL Parse Error: ${e.message}', sql);
    }
    if (result is Success<Statement>) {
      return result.value;
    }

    // Improved error reporting
    final failure = result as Failure;
    final lineAndCol = _getLineAndColumn(sql, failure.position);
    throw FormatException(
      'SQL Parse Error at line ${lineAndCol.line}, column ${lineAndCol.column}: ${failure.message}',
      sql,
      failure.position,
    );
  }

  Parser<Statement> _buildParser() {
    final builder = SqlGrammarDefinition();
    return builder.build().cast<Statement>();
  }

  /// Reject pathologically nested parentheses before recursive-descent parsing
  /// overflows the stack. All three grammar string/identifier quotes (`'`, `"`,
  /// backtick) are skipped so parens inside quoted text don't trip the guard.
  void _guardDepth(String sql) {
    int depth = 0, max = 0;
    int quote = 0; // 0 = not in a string; else the opening quote code unit
    for (int i = 0; i < sql.length; i++) {
      final c = sql.codeUnitAt(i);
      if (quote != 0) {
        // closing quote (a doubled quote re-opens next iter)
        if (c == quote) quote = 0;
        continue;
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

  // Main statement parser — DML/DDL first, else a query.
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
              char('(').trim() &
              ref0(selectStatement) &
              char(')').trim())
          .map((values) {
            final orReplace = values[1] != null;
            final ifNotExists = values[3] != null;
            final name = values[4] as String;
            final columns = values[5] != null
                ? (values[5][1] as List).cast<String>()
                : <String>[];
            final query = values[8] as SelectStatement;
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
            final source = values[5];
            final action = values[12] as String;
            return MergeStatement(
              targetPath: values[2] as String,
              sourcePath: source is String ? source : null,
              sourceQuery: source is SelectStatement ? source : null,
              onKeys: (values[8] as List).cast<String>(),
              matchedUpdateAll: action == 'update',
              matchedDelete: action == 'delete',
              notMatchedInsert: values[13] != null,
            );
          });

  Parser mergeAlias() => ref1(keyword, 'AS').optional() & ref0(identifier);

  Parser mergeSource() =>
      (ref0(filePathLiteral) |
      (char('(').trim() & ref0(selectStatement) & char(')').trim()).map(
        (v) => v[1],
      ));

  Parser<List<String>> mergeCondition() => ref0(mergeEquality)
      .plusSeparated(ref1(keyword, 'AND'))
      .map((eqs) => eqs.elements.cast<String>());

  Parser<String> mergeEquality() =>
      (ref0(qualifiedColumn) & char('=').trim() & ref0(qualifiedColumn)).map(
        (values) => values[0] as String,
      );

  Parser<String> qualifiedColumn() =>
      (ref0(identifier) & (char('.') & ref0(identifier)).optional()).map(
        (values) => values[1] != null
            ? (values[1] as List)[1] as String
            : values[0] as String,
      );

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
              setOps: tails,
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
            final expr = values[0] as Expression;
            final alias = values[1]?[1] as String?;
            return SelectItem(expression: expr, alias: alias);
          });

  // FROM clause
  Parser<List<TableReference>> fromClause() =>
      (ref1(keyword, 'FROM') &
              ref0(
                tableReference,
              ).plusSeparated(char(',').trim()).map((l) => l.elements))
          .map((values) => (values[1] as List).cast<TableReference>());

  Parser<TableReference> tableReference() =>
      (ref0(joinedTable).cast<TableReference>() |
              ref0(simpleTable).cast<TableReference>())
          .cast<TableReference>();

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
        final aliasInfo = values[1] as List?;

        return SimpleTable(
          table: path,
          alias: aliasInfo?[0] as String?,
          isFilePath: true,
        );
      });

  // Parse string literal as file path
  Parser<String> filePathLiteral() =>
      (_quoted("'") | _quoted('"')).cast<String>();

  Parser<SimpleTable> tableName() =>
      (ref0(identifier) & ref0(tableAlias).optional()).map((values) {
        final name = values[0] as String;
        final aliasInfo = values[1] as List?;

        return SimpleTable(table: name, alias: aliasInfo?[0] as String?);
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
            final aliasInfo = values[3] as List;

            return SubqueryTable(
              subquery: subquery,
              alias: aliasInfo[0] as String,
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
            final aliasInfo = values[2] as List?;

            return ValuesTable(
              values: rows,
              alias: aliasInfo?[0] as String?,
              columnAliases: aliasInfo?[1] as List<String>?,
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
  Parser<JoinedTable> joinedTable() =>
      (ref0(simpleTable) & ref0(joinClause).plus()).map((values) {
        final left = values[0] as TableReference;
        final joins = (values[1] as List).cast<JoinClause>();

        return JoinedTable(left: left, joins: joins);
      });

  Parser<JoinClause> joinClause() =>
      (ref0(joinType) &
              ref1(keyword, 'JOIN') &
              ref0(simpleTable) &
              ref0(joinCondition).optional())
          .map(
            (values) => JoinClause(
              type: values[0] as JoinType,
              right: values[2] as TableReference,
              condition: values[3] as JoinCondition?,
            ),
          );

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
            final expr = values[0] as Expression;
            // keyword() flattens the ORIGINAL (case-preserved) text, so a
            // lowercase `asc` would fail an `== 'ASC'` check and sort DESCENDING.
            final dir = (values[1] as String?)?.toUpperCase();
            final ascending = dir == null || dir == 'ASC';
            final nulls = (values[2] as List?)?[1] as String?;
            // Explicit NULLS FIRST/LAST wins; otherwise default to the SQL
            // convention (NULLS FIRST for DESC, NULLS LAST for ASC), matching
            // Postgres/DuckDB.
            final nullsFirst = nulls != null
                ? nulls.toUpperCase() == 'FIRST'
                : !ascending;
            return OrderByItem(
              expression: expr,
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
            // falls to the `except` default — silently returning wrong results.
            // Collapse interior whitespace too: `UNION  ALL` (two spaces, or a
            // newline) failed startsWith('UNION ALL') and silently DEDUPED as
            // plain UNION.
            final operator = (values[0] as String)
                .trim()
                .toUpperCase()
                .replaceAll(RegExp(r'\s+'), ' ');
            final core = values[1] as SelectStatement;

            final setOpType = switch (operator) {
              final String s when s.startsWith('UNION ALL') =>
                SetOperationType.unionAll,
              final String s when s.startsWith('UNION') =>
                SetOperationType.union,
              'INTERSECT' => SetOperationType.intersect,
              _ => SetOperationType.except,
            };

            return SetOperation(type: setOpType, right: core);
          });

  // Expressions
  Parser<Expression> expression() => ref0(orExpression).cast<Expression>();

  Parser<Expression> orExpression() =>
      ref0(andExpression).plusSeparated(ref1(keyword, 'OR')).map((values) {
        final exprs = values.elements.cast<Expression>();
        return exprs.reduce(
          (left, right) =>
              BinaryExpression(left: left, operator: 'OR', right: right),
        );
      });

  Parser<Expression> andExpression() =>
      ref0(notExpression).plusSeparated(ref1(keyword, 'AND')).map((values) {
        final exprs = values.elements.cast<Expression>();
        return exprs.reduce(
          (left, right) =>
              BinaryExpression(left: left, operator: 'AND', right: right),
        );
      });

  Parser<Expression> notExpression() =>
      (ref1(keyword, 'NOT').optional() & ref0(comparisonExpression)).map((
        values,
      ) {
        final not = values[0] != null;
        final expr = values[1] as Expression;
        return not ? UnaryExpression(operator: 'NOT', operand: expr) : expr;
      });

  Parser comparisonExpression() =>
      (ref0(additiveExpression) &
              (ref0(betweenPredicate) |
                      ref0(inListPredicate) |
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
                // The value-list form is taken by `inListPredicate` above, so
                // reaching here means a subquery: `x IN (SELECT …)`.
                return InExpression(
                  expression: left,
                  subquery: tail[1] is SubqueryExpression
                      ? (tail[1] as SubqueryExpression).subquery
                      : null,
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

  // `x [NOT] BETWEEN lo AND hi` — bounds are additive (so the `AND` isn't eaten
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

  // `x [NOT] IN (v1, v2, …)` — a VALUE list (a subquery `IN (SELECT …)` fails the
  // `expression` element and falls through to the subquery IN in
  // [comparisonOperator]). Normalized to [tag, List<Expression>].
  Parser inListPredicate() =>
      (((ref1(keyword, 'NOT') & ref1(keyword, 'IN')).map((_) => 'NOT_IN_LIST') |
                  ref1(keyword, 'IN').map((_) => 'IN_LIST')) &
              char('(').trim() &
              ref0(
                expression,
              ).plusSeparated(char(',').trim()).map((l) => l.elements) &
              char(')').trim())
          .map((v) => [v[0], (v[2] as List).cast<Expression>()]);

  Parser comparisonOperator() =>
      ((string('<=') |
                  string('>=') |
                  string('<>') |
                  string('!=') |
                  char('<') |
                  char('>') |
                  char('=')) |
              ((ref1(keyword, 'NOT') & ref1(keyword, 'LIKE')).map(
                (_) => 'NOT LIKE',
              )) |
              (ref1(keyword, 'LIKE').map((_) => 'LIKE')) |
              ((ref1(keyword, 'NOT') & ref1(keyword, 'IN')).map(
                (_) => 'NOT IN',
              )) |
              (ref1(keyword, 'IN').map((_) => 'IN')) |
              ((ref1(keyword, 'IS') & ref1(keyword, 'NOT')).map(
                (_) => 'IS NOT',
              )) |
              (ref1(keyword, 'IS').map((_) => 'IS')))
          .trim() &
      ref0(additiveExpression);

  /// Folds `a OP b OP c` left — `(a OP b) OP c` — taking each operator from the
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
          final expr = values[1] as Expression;

          if (sign == '-') {
            return UnaryExpression(operator: '-', operand: expr);
          } else if (sign == '+') {
            return UnaryExpression(operator: '+', operand: expr);
          }
          return expr;
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
            final argsInfo = values[2] as List?;

            if (argsInfo == null) {
              return FunctionExpression(name: name, arguments: []);
            }

            final distinct = argsInfo[0] != null;
            final args = argsInfo[1] is List
                ? (argsInfo[1] as List).cast<Expression>()
                : <Expression>[];

            return FunctionExpression(
              name: name,
              arguments: args,
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
            final expr = values[1] as Expression?;
            // `.plus()` yields a List<dynamic>; a direct `as List<WhenClause>`
            // is a failing downcast, so build a properly-typed view.
            final whenClauses = (values[2] as List).cast<WhenClause>();
            final elseExpr = values[3] as Expression?;

            return CaseExpression(
              expression: expr,
              whenClauses: whenClauses,
              elseExpression: elseExpr,
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
            final expr = values[2] as Expression;
            final type = values[4] as String;
            return CastExpression(expression: expr, targetType: type);
          });

  // Literals
  Parser literal() =>
      (ref0(nullLiteral) |
      ref0(booleanLiteral) |
      ref0(numericLiteral) |
      ref0(stringLiteral));

  Parser nullLiteral() =>
      ref1(keyword, 'NULL').map((_) => LiteralExpression(null));

  // No .flatten() here: keyword() trims surrounding whitespace, and a flatten
  // OVER it captures that whitespace too — `'TRUE '.toUpperCase() == 'TRUE'`
  // is false, so `SELECT true` literally returned false whenever the literal
  // was followed by a space. keyword() already yields the bare token text.
  Parser booleanLiteral() =>
      (ref1(keyword, 'TRUE') | ref1(keyword, 'FALSE')).map(
        (value) => LiteralExpression((value as String).toUpperCase() == 'TRUE'),
      );

  Parser numericLiteral() => (ref0(floatingPoint) | ref0(integer)).map(
    (value) => LiteralExpression(value),
  );

  /// Match a [q]-quoted string in which a doubled quote (`''` / `""`) is an
  /// escaped literal quote (standard SQL); yields the unescaped content.
  Parser<String> _quoted(String q) =>
      (char(q) &
              (string('$q$q').map((_) => q) | char(q).neg()).star() &
              char(q))
          .map((v) => (v[1] as List).cast<String>().join());

  Parser stringLiteral() =>
      (_quoted("'") | _quoted('"')).map((s) => LiteralExpression(s));

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
                  ref1(keyword, 'FLOAT') |
                  ref1(keyword, 'DOUBLE') |
                  ref1(keyword, 'DECIMAL') |
                  ref1(keyword, 'NUMERIC') |
                  ref1(keyword, 'VARCHAR') |
                  ref1(keyword, 'TEXT') |
                  ref1(keyword, 'BOOLEAN') |
                  ref1(keyword, 'DATE') |
                  ref1(keyword, 'TIME') |
                  ref1(keyword, 'TIMESTAMP')) &
              // Optional `(p[,s])` — DECIMAL(10,2), VARCHAR(30), ...
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
    final exp = anyOf('eE') & anyOf('+-').optional() & digits;
    final withDot =
        ((digits & char('.') & digit().star()) | (char('.') & digits)) &
        exp.optional();
    final intWithExp = digits & exp;
    return (withDot | intWithExp).flatten().trim().map(
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
