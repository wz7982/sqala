package sqala.ast.expr

import sqala.ast.order.SqlOrderingItem

/**
 * A window specification for window function `OVER` clauses.
 *
 * Renders as `[PARTITION BY expr, ...] [ORDER BY order, ...] [ROWS|RANGE|GROUPS frame]`.
 *
 * @param partitionBy the `PARTITION BY` expressions.
 * @param orderBy the `ORDER BY` items.
 * @param frame optional window frame clause.
 */
case class SqlWindow(
    partitionBy: List[SqlExpr],
    orderBy: List[SqlOrderingItem],
    frame: Option[SqlWindowFrame]
)

/**
 * Window frame specification.
 *
 * Defines the set of rows within a partition for window function calculation.
 */
enum SqlWindowFrame:
    /**
     * A frame starting at `start` and extending to the current row.
     *
     * Renders as `unit BETWEEN start AND CURRENT ROW [EXCLUDE CURRENT ROW|GROUP|TIES|NO OTHERS]`.
     *
     * @param unit the frame unit (`ROWS`, `RANGE`, or `GROUPS`).
     * @param start the lower bound.
     * @param exclude optional exclusion mode.
     */
    case Start(
        unit: SqlWindowFrameUnit,
        start: SqlWindowFrameBound,
        exclude: Option[SqlWindowFrameExcludeMode]
    )

    /**
     * A frame between `start` and `end` bounds.
     *
     * Renders as `unit BETWEEN start AND end [EXCLUDE CURRENT ROW|GROUP|TIES|NO OTHERS]`.
     *
     * @param unit the frame unit (`ROWS`, `RANGE`, or `GROUPS`).
     * @param start the lower bound.
     * @param end the upper bound.
     * @param exclude optional exclusion mode.
     */
    case Between(
        unit: SqlWindowFrameUnit,
        start: SqlWindowFrameBound,
        end: SqlWindowFrameBound,
        exclude: Option[SqlWindowFrameExcludeMode]
    )

/**
 * Window frame boundary.
 */
enum SqlWindowFrameBound:
    /**
     * A bound representing the current row.
     *
     * Renders as `CURRENT ROW`.
     */
    case CurrentRow

    /**
     * Unbounded preceding (the first row of the partition).
     *
     * Renders as `UNBOUNDED PRECEDING`.
     */
    case UnboundedPreceding

    /**
     * A number of rows preceding the current row.
     *
     * Renders as `n PRECEDING`.
     *
     * @param n the offset expression.
     */
    case Preceding(n: SqlExpr)

    /**
     * Unbounded following (the last row of the partition).
     *
     * Renders as `UNBOUNDED FOLLOWING`.
     */
    case UnboundedFollowing

    /**
     * A number of rows following the current row.
     *
     * Renders as `n FOLLOWING`.
     *
     * @param n the offset expression.
     */
    case Following(n: SqlExpr)

/**
 * Window frame unit type.
 */
enum SqlWindowFrameUnit:
    /**
     * Physical rows.
     *
     * Renders as `ROWS`.
     */
    case Rows

    /**
     * Logical range based on `ORDER BY` values.
     *
     * Renders as `RANGE`.
     */
    case Range

    /**
     * Peer groups based on `ORDER BY` equality.
     *
     * Renders as `GROUPS`.
     */
    case Groups

/**
 * Window frame exclusion mode.
 */
enum SqlWindowFrameExcludeMode:
    /**
     * Exclude the current row from the window frame.
     *
     * Renders as `EXCLUDE CURRENT ROW`.
     */
    case CurrentRow

    /**
     * Exclude the current row and all its peers (rows with equal `ORDER BY` values) from the window frame.
     *
     * Renders as `EXCLUDE GROUP`.
     */
    case Group

    /**
     * Exclude all peers of the current row from the window frame,
     * but keep the current row itself.
     *
     * Renders as `EXCLUDE TIES`.
     */
    case Ties

    /**
     * No rows are excluded from the window frame (default behavior).
     *
     * Renders as `EXCLUDE NO OTHERS`.
     */
    case NoOthers