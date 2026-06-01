package sqala.ast.table

import sqala.ast.expr.{SqlExpr, SqlJsonPassing}
import sqala.ast.statement.{SqlQuery, SqlSelectItem}

/**
 * A table reference used in `FROM` clauses.
 */
enum SqlTable:
    /**
     * A table identified by name, optionally with temporal, alias,
     * match-recognize, and sample clauses.
     *
     * Renders as `name [alias] [FOR SYSTEM_TIME ...] [MATCH_RECOGNIZE ...] [TABLESAMPLE ...]`.
     *
     * @param name the table name.
     * @param period optional temporal period specification.
     * @param alias optional table alias.
     * @param matchRecognize optional `MATCH_RECOGNIZE` clause.
     * @param sample optional `TABLESAMPLE` clause.
     */
    case Ident(
        name: String,
        period: Option[SqlTablePeriodForMode],
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize],
        sample: Option[SqlTableSample]
    )

    /**
     * A table-valued function.
     *
     * Renders as `[LATERAL] name(arg, ...) [WITH ORDINALITY] [alias] [MATCH_RECOGNIZE ...]`.
     *
     * @param lateral when `true`, adds `LATERAL` keyword.
     * @param name the function name.
     * @param args the function arguments.
     * @param withOrd when `true`, adds `WITH ORDINALITY`.
     * @param alias optional table alias.
     * @param matchRecognize optional `MATCH_RECOGNIZE` clause.
     */
    case Func(
        lateral: Boolean,
        name: String,
        args: List[SqlExpr],
        withOrd: Boolean,
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize]
    )

    /**
     * A subquery used as a table source.
     *
     * Renders as `[LATERAL] (SELECT ...) [alias] [MATCH_RECOGNIZE ...]`.
     *
     * @param lateral when `true`, adds `LATERAL` keyword.
     * @param query the nested `SELECT` query.
     * @param alias optional table alias.
     * @param matchRecognize optional `MATCH_RECOGNIZE` clause.
     */
    case Subquery(
        lateral: Boolean,
        query: SqlQuery,
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize]
    )

    /**
     * A `JSON_TABLE` expression.
     *
     * Renders as
     * `[LATERAL] JSON_TABLE(expr, path
     *   [AS alias]
     *   [PASSING expr AS alias, ...]
     *   COLUMNS(columns)
     *   [ERROR|EMPTY|EMPTY ARRAY ON ERROR])
     *   [alias]
     *   [MATCH_RECOGNIZE ...]`.
     *
     * @param lateral when `true`, adds `LATERAL` keyword.
     * @param expr the JSON source expression.
     * @param path the JSON path expression.
     * @param pathAlias optional alias for the path.
     * @param passingItems variables passed to the path expression.
     * @param columns the column definitions.
     * @param onError error behavior mode.
     * @param alias optional table alias.
     * @param matchRecognize optional `MATCH_RECOGNIZE` clause.
     */
    case Json(
        lateral: Boolean,
        expr: SqlExpr,
        path: SqlExpr,
        pathAlias: Option[String],
        passingItems: List[SqlJsonPassing],
        columns: List[SqlJsonColumn],
        onError: Option[SqlJsonErrorBehavior],
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize]
    )

    /**
     * A `GRAPH_TABLE` expression for graph queries.
     *
     * Renders as
     * `[LATERAL] GRAPH_TABLE(name
     *   MATCH pattern, ...
     *   [WHERE condition]
     *   [COLUMNS(columns)]
     *   [ROWS ...]
     *   [ALIAS ...])
     *   [alias]
     *   [MATCH_RECOGNIZE ...]`.
     *
     * @param lateral when `true`, adds `LATERAL` keyword.
     * @param name the graph name.
     * @param `match` optional match mode.
     * @param patterns the graph patterns.
     * @param where optional filter condition.
     * @param rows optional rows mode.
     * @param columns the column definitions.
     * @param `export` optional export mode.
     * @param alias optional table alias.
     * @param matchRecognize optional `MATCH_RECOGNIZE` clause.
     */
    case Graph(
        lateral: Boolean,
        name: String,
        `match`: Option[SqlGraphMatchMode],
        patterns: List[SqlGraphPattern],
        where: Option[SqlExpr],
        rows: Option[SqlGraphRowsMode],
        columns: List[SqlSelectItem],
        `export`: Option[SqlGraphExportMode],
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize]
    )

    /**
     * A join between two tables.
     *
     * Renders as `left [INNER|LEFT|RIGHT|FULL|CROSS] JOIN right [ON condition|USING(column, ...)]`.
     *
     * @param left the left table.
     * @param joinType the join type.
     * @param right the right table.
     * @param condition optional join condition.
     */
    case Join(
        left: SqlTable,
        joinType: SqlJoinType,
        right: SqlTable,
        condition: Option[SqlJoinCondition]
    )

/**
 * A table alias with optional column aliases.
 *
 * Renders as `[AS] alias [(columnAlias, ...)]`.
 *
 * @param alias the table alias.
 * @param columnAliases optional column aliases.
 */
case class SqlTableAlias(alias: String, columnAliases: List[String])

/**
 * Temporal period specification for system-versioned tables.
 */
enum SqlTablePeriodForMode:
    /**
     * `FOR SYSTEM_TIME AS OF` point-in-time query.
     *
     * Renders as `FOR SYSTEM_TIME AS OF expr`.
     *
     * @param expr the time expression.
     */
    case SystemTimeAsOf(expr: SqlExpr)

    /**
     * `FOR SYSTEM_TIME BETWEEN ... AND ...` range query.
     *
     * Renders as `FOR SYSTEM_TIME BETWEEN [ASYMMETRIC|SYMMETRIC] start AND end`.
     *
     * @param mode optional BETWEEN mode (ASYMMETRIC or SYMMETRIC).
     * @param start the start expression.
     * @param end the end expression.
     */
    case SystemTimeBetween(
        mode: Option[SqlTablePeriodBetweenMode],
        start: SqlExpr,
        end: SqlExpr
    )

    /**
     * `FOR SYSTEM_TIME FROM ... TO ...` range query.
     *
     * Renders as `FOR SYSTEM_TIME FROM from TO to`.
     *
     * @param from the start time expression.
     * @param to the end time expression.
     */
    case SystemTimeFrom(from: SqlExpr, to: SqlExpr)

/**
 * Between mode for temporal period queries.
 */
enum SqlTablePeriodBetweenMode:
    /**
     * Asymmetric between.
     *
     * Renders as `ASYMMETRIC`.
     */
    case Asymmetric

    /**
     * Symmetric between.
     *
     * Renders as `SYMMETRIC`.
     */
    case Symmetric

/**
 * A `TABLESAMPLE` clause for sampling rows.
 *
 * Renders as `TABLESAMPLE mode(percentage) [REPEATABLE(repeatable)]`.
 *
 * @param mode the sampling mode.
 * @param percentage the sampling percentage expression.
 * @param repeatable optional seed for repeatable sampling.
 */
case class SqlTableSample(mode: SqlTableSampleMode, percentage: SqlExpr, repeatable: Option[SqlExpr])

/**
 * Sampling mode for `TABLESAMPLE`.
 */
enum SqlTableSampleMode:
    /**
     * Bernoulli sampling.
     *
     * Renders as `BERNOULLI`.
     */
    case Bernoulli

    /**
     * System sampling.
     *
     * Renders as `SYSTEM`.
     */
    case System

    /**
     * A custom sampling mode with a free-form name.
     *
     * Renders as the given `mode` string directly.
     *
     * @param mode the sampling mode text.
     */
    case Custom(mode: String)