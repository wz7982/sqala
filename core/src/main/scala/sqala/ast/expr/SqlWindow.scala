package sqala.ast.expr

import sqala.ast.order.SqlOrderingItem

/**
 * A window specification for window function `OVER` clause.
 *
 * Renders as `[PARTITION BY expr [, ...]] [ORDER BY ordering_item [, ...]] [ROWS|RANGE|GROUPS frame]`.
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
     * Renders as
     * `[ROWS|RANGE|GROUPS]
     *   CURRENT ROW|UNBOUNDED PRECEDING|n PRECEDING|UNBOUNDED FOLLOWING|n FOLLOWING
     *   [EXCLUDE CURRENT ROW|GROUP|TIES|NO OTHERS]`.
     */
    case Start(
        unit: SqlWindowFrameUnit,
        start: SqlWindowFrameBound,
        exclude: Option[SqlWindowFrameExcludeMode]
    )

    /**
     * A frame between `start` and `end` bounds.
     *
     * Renders as
     * `[ROWS|RANGE|GROUPS] BETWEEN
     *   CURRENT ROW|UNBOUNDED PRECEDING|n PRECEDING|UNBOUNDED FOLLOWING|n FOLLOWING AND
     *   CURRENT ROW|UNBOUNDED PRECEDING|n PRECEDING|UNBOUNDED FOLLOWING|n FOLLOWING
     *   [EXCLUDE CURRENT ROW|GROUP|TIES|NO OTHERS]`.
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
     * No rows are excluded from the window frame.
     *
     * Renders as `EXCLUDE NO OTHERS`.
     */
    case NoOthers