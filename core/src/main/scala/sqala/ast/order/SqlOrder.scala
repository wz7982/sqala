package sqala.ast.order

import sqala.ast.expr.SqlExpr

/**
 * An `ORDER BY` item.
 *
 * Renders as `expr [ASC|DESC] [NULLS FIRST|NULLS LAST]`.
 *
 * @param expr the ordering expression.
 * @param ordering optional sort direction (ASC or DESC).
 * @param nullsOrdering optional nulls ordering (FIRST or LAST).
 */
case class SqlOrderingItem(
    expr: SqlExpr,
    ordering: Option[SqlOrdering],
    nullsOrdering: Option[SqlNullsOrdering]
)

/**
 * Sort direction.
 */
enum SqlOrdering:
    /**
     * Ascending order.
     *
     * Renders as `ASC`.
     */
    case Asc

    /**
     * Descending order.
     *
     * Renders as `DESC`.
     */
    case Desc

/**
 * Nulls ordering option.
 */
enum SqlNullsOrdering:
    /**
     * Nulls first.
     *
     * Renders as `NULLS FIRST`.
     */
    case First

    /**
     * Nulls last.
     *
     * Renders as `NULLS LAST`.
     */
    case Last