package sqala.ast.group

import sqala.ast.expr.SqlExpr
import sqala.ast.quantifier.SqlQuantifier

/**
 * A `GROUP BY` clause.
 *
 * Renders as `GROUP BY [DISTINCT|ALL] grouping_item [, ...]`.
 *
 * @param quantifier optional `DISTINCT` or `ALL`.
 * @param items the grouping items.
 */
case class SqlGroupBy(quantifier: Option[SqlQuantifier], items: List[SqlGroupingItem])

/**
 * A grouping item within a `GROUP BY` clause.
 */
enum SqlGroupingItem:
    /**
     * A single expression to group by.
     *
     * Renders as `expr`.
     *
     * @param item the grouping expression.
     */
    case Expr(item: SqlExpr)

    /**
     * A `CUBE` grouping set over the given expressions.
     *
     * Renders as `CUBE(expr [, ...])`.
     *
     * @param items the expressions to cube.
     */
    case Cube(items: List[SqlExpr])

    /**
     * A `ROLLUP` grouping set over the given expressions.
     *
     * Renders as `ROLLUP(expr [, ...])`.
     *
     * @param items the expressions to roll up.
     */
    case Rollup(items: List[SqlExpr])

    /**
     * An explicit `GROUPING SETS` clause.
     *
     * Renders as `GROUPING SETS(grouping_item [, ...])`.
     *
     * @param items the grouping set expressions.
     */
    case GroupingSets(items: List[SqlExpr])