package sqala.ast.table

import sqala.ast.expr.{SqlExpr, SqlJsonPassing}
import sqala.ast.statement.{SqlQuery, SqlSelectItem}

/**
 * A table reference used in `FROM` clause.
 */
enum SqlTable:
    /**
     * A table identified by name.
     *
     * Renders as `"name" [[AS] "alias" [("column_alias" [, ...])]] [period] [match_recognize] [sample]`.
     */
    case Ident(
        name: String,
        alias: Option[SqlTableAlias],
        periodForMode: Option[SqlTablePeriodForMode],
        matchRecognize: Option[SqlMatchRecognize],
        sample: Option[SqlTableSample]
    )

    /**
     * A table-valued function.
     *
     * Renders as `[LATERAL] name(expr [, ...]) [WITH ORDINALITY] [[AS] "alias" [("column_alias" [, ...])]] [match_recognize]`.
     */
    case Func(
        lateral: Boolean,
        name: String,
        args: List[SqlExpr],
        withOrdinality: Boolean,
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize]
    )

    /**
     * A subquery used as a table source.
     *
     * Renders as `[LATERAL] (query) [[AS] "alias" [("column_alias" [, ...])]] [match_recognize]`.
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
     * `[LATERAL] JSON_TABLE(expr, expr [AS "alias"]
     *   [PASSING expr AS "alias" [, ...]]
     *   COLUMNS(column [, ...])
     *   [ERROR|EMPTY|EMPTY ARRAY ON ERROR])
     *   [[AS] "alias" [("column_alias" [, ...])]]
     *   [match_recognize]`.
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
     * `[LATERAL] GRAPH_TABLE("name"
     *   MATCH pattern [, ...]
     *   [WHERE expr]
     *   [rows_mode]
     *   COLUMNS(column [, ...])
     *   [EXPORT ALL SINGLETONS EXCEPT (pattern [, ...])|EXPORT SINGLETONS (pattern [, ...])|EXPORT NO SINGLETONS])
     *   [[AS] "alias" [("column_alias" [, ...])]]
     *   [match_recognize]`.
     */
    case Graph(
        lateral: Boolean,
        name: String,
        matchMode: Option[SqlGraphMatchMode],
        patterns: List[SqlGraphPattern],
        where: Option[SqlExpr],
        rowsMode: Option[SqlGraphRowsMode],
        columns: List[SqlSelectItem],
        exportMode: Option[SqlGraphExportMode],
        alias: Option[SqlTableAlias],
        matchRecognize: Option[SqlMatchRecognize]
    )

    /**
     * A join between two tables.
     *
     * Renders as `table [INNER|LEFT|RIGHT|FULL|CROSS] JOIN table [ON condition|USING("column" [, ...])]`.
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
 * Renders as `[AS] "alias" [("column_alias" [, ...])]`.
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
     */
    case SystemTimeAsOf(expr: SqlExpr)

    /**
     * `FOR SYSTEM_TIME BETWEEN ... AND ...` range query.
     *
     * Renders as `FOR SYSTEM_TIME BETWEEN [ASYMMETRIC|SYMMETRIC] start AND end`.
     */
    case SystemTimeBetween(
        mode: Option[SqlTablePeriodBetweenMode],
        start: SqlExpr,
        end: SqlExpr
    )

    /**
     * `FOR SYSTEM_TIME FROM ... TO ...` range query.
     *
     * Renders as `FOR SYSTEM_TIME FROM expr TO expr`.
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
 * Renders as `TABLESAMPLE [BERNOULLI|SYSTEM](expr) [REPEATABLE(expr)]`.
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
     */
    case Custom(mode: String)