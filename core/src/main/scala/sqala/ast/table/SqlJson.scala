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
     * Renders as `name FOR ORDINALITY`.
     *
     * @param name the column name.
     */
    case Ordinality(name: String)

    /**
     * A regular column extracting a JSON value.
     *
     * Renders as
     * `name type
     *   [FORMAT JSON [ENCODING UTF8|UTF16|UTF32]]
     *   [PATH path]
     *   [WITH|WITHOUT [CONDITIONAL|UNCONDITIONAL] [ARRAY] WRAPPER]
     *   [KEEP|OMIT QUOTES [ON SCALAR STRING]]
     *   [ERROR|NULL|EMPTY OBJECT|EMPTY ARRAY|DEFAULT expr ON EMPTY]
     *   [ERROR|NULL|EMPTY OBJECT|EMPTY ARRAY|DEFAULT expr ON ERROR]`.
     *
     * @param name the column name.
     * @param `type` the column type.
     * @param format optional output format clause.
     * @param path optional JSON path expression.
     * @param wrapper optional wrapper behavior.
     * @param quotes optional quotes behavior.
     * @param onEmpty behavior when the path yields an empty result.
     * @param onError behavior when a JSON structural error occurs.
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
     * `name type EXISTS [PATH path] [ERROR|TRUE|FALSE|UNKNOWN ON ERROR]`.
     *
     * @param name the column name.
     * @param `type` the column type.
     * @param path optional JSON path expression.
     * @param onError behavior on structural error.
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
     * `NESTED PATH path [AS alias] COLUMNS(columns)`.
     *
     * @param path the JSON path to the nested structure.
     * @param pathAlias optional alias for the nested path.
     * @param columns the nested column definitions.
     */
    case Nested(
        path: SqlExpr,
        pathAlias: Option[String],
        columns: List[SqlJsonColumn]
    )