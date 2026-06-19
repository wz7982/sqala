package sqala.ast.group

import sqala.ast.expr.SqlExpr
import sqala.ast.quantifier.SqlQuantifier
import sqala.util.NonEmptyList

/**
 * A `GROUP BY` clause.
 *
 * Renders as `GROUP BY [DISTINCT|ALL] grouping_item [, ...]`.
 */
case class SqlGroup(quantifier: Option[SqlQuantifier], items: NonEmptyList[SqlGroupingItem])

/**
 * A grouping item within a `GROUP BY` clause.
 */
enum SqlGroupingItem:
    /**
     * An empty grouping item.
     *
     * Renders as `()`.
     */
    case EmptyGroup
    /**
     * A single expression to group by.
     *
     * Renders as `expr`.
     */
    case Expr(item: SqlExpr)

    /**
     * A `CUBE` grouping set over the given expressions.
     *
     * Renders as `CUBE(expr [, ...])`.
     */
    case Cube(items: NonEmptyList[SqlExpr])

    /**
     * A `ROLLUP` grouping set over the given expressions.
     *
     * Renders as `ROLLUP(expr [, ...])`.
     */
    case Rollup(items: NonEmptyList[SqlExpr])

    /**
     * An explicit `GROUPING SETS` clause.
     *
     * Renders as `GROUPING SETS(grouping_item [, ...])`.
     */
    case GroupingSets(items: NonEmptyList[SqlGroupingItem])