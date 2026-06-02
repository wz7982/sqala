package sqala.ast.table

import sqala.ast.expr.*

/**
 * Error behavior for JSON table functions.
 */
enum SqlJsonErrorBehavior:
    /**
     * Raise an error.
     *
     * Renders as `ERROR`.
     */
    case Error

    /**
     * Return an empty result.
     *
     * Renders as `EMPTY`.
     */
    case Empty

    /**
     * Return an empty array.
     *
     * Renders as `EMPTY ARRAY`.
     */
    case EmptyArray

/**
 * A column definition within a `JSON_TABLE` expression.
 */
enum SqlJsonColumn:
    /**
     * An ordinality column that returns the row number.
     *
     * Renders as `"name" FOR ORDINALITY`.
     */
    case Ordinality(name: String)

    /**
     * A regular column extracting a JSON value.
     *
     * Renders as
     * `"name" type
     *   [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]
     *   [PATH expr]
     *   [WITH|WITHOUT [CONDITIONAL|UNCONDITIONAL] [ARRAY] WRAPPER]
     *   [KEEP|OMIT QUOTES [ON SCALAR STRING]]
     *   [ERROR|NULL|EMPTY OBJECT|EMPTY ARRAY|DEFAULT expr ON EMPTY]
     *   [ERROR|NULL|EMPTY OBJECT|EMPTY ARRAY|DEFAULT expr ON ERROR]`.
     */
    case Column(
        name: String,
        `type`: SqlType,
        format: Option[SqlJsonOutputFormat],
        path: Option[SqlExpr],
        wrapper: Option[SqlJsonQueryWrapperBehavior],
        quotes: Option[SqlJsonQueryQuotesBehavior],
        onEmpty: Option[SqlJsonQueryEmptyBehavior],
        onError: Option[SqlJsonQueryErrorBehavior]
    )

    /**
     * An `EXISTS` column that tests whether a JSON path exists.
     *
     * Renders as
     * `"name" type EXISTS [PATH expr] [ERROR|TRUE|FALSE|UNKNOWN ON ERROR]`.
     */
    case Exists(
        name: String,
        `type`: SqlType,
        path: Option[SqlExpr],
        onError: Option[SqlJsonExistsErrorBehavior]
    )

    /**
     * A `NESTED PATH` clause for nested JSON structures.
     *
     * Renders as
     * `NESTED PATH expr [AS "alias"] COLUMNS(column [, ...])`.
     */
    case Nested(
        path: SqlExpr,
        pathAlias: Option[String],
        columns: List[SqlJsonColumn]
    )