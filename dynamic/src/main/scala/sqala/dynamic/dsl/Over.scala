package sqala.dynamic.dsl

import sqala.ast.expr.*
import sqala.ast.order.SqlOrderingItem
import sqala.ast.window.{SqlWindowFrame, SqlWindowFrameBound, SqlWindowFrameExcludeMode, SqlWindowFrameUnit}

/**
 * A window frame bound, such as `CURRENT ROW`, `UNBOUNDED PRECEDING`, or
 * `n FOLLOWING`. Created with `currentRow`, `unboundedPreceding`,
 * `unboundedFollowing`, or the `preceding` / `following` helpers on
 * Expr, and used with the frame methods on Over.
 */
final class FrameBound(private[sqala] val bound: SqlWindowFrameBound)

/**
 * A window specification (`OVER` clause). Built with `partitionBy`,
 * `orderBy`, and the frame methods (`rows`, `range`, `groups`, and their
 * `...Between` variants), then applied to an expression with
 * `Expr.over`.
 *
 * {{{
 * sum(column("score")).over(
 *     partitionBy(column("author"))
 *         .orderBy(column("id").asc)
 *         .rows(unboundedPreceding)
 * )
 * }}}
 */
final class Over(
    private[sqala] val partitionBy: List[SqlExpr] = Nil,
    private[sqala] val orderBy: List[SqlOrderingItem] = Nil,
    private[sqala] val frame: Option[SqlWindowFrame] = None
):
    /**
     * Returns a copy of this specification with the given frame exclusion
     * mode applied. Ignored when no frame is set.
     */
    private def setExclude(exclude: SqlWindowFrameExcludeMode): Option[SqlWindowFrame] =
        frame.map:
            case s: SqlWindowFrame.Start =>
                s.copy(excludeMode = Some(exclude))
            case b: SqlWindowFrame.Between =>
                b.copy(excludeMode = Some(exclude))

    /**
     * Appends one or more `ORDER BY` items to the window specification.
     *
     * {{{
     * partitionBy(column("author")).orderBy(column("id").asc, column("name").desc)
     * }}}
     */
    def orderBy(orders: Order*): Over =
        Over(partitionBy, orderBy ++ orders.toList.map(_.order), frame)

    /**
     * List-based overload of `orderBy`.
     *
     * {{{
     * partitionBy(column("author")).orderBy(List(column("id").asc))
     * }}}
     */
    def orderBy(orders: List[Order]): Over =
        orderBy(orders*)

    /**
     * Sets a `ROWS` frame starting at `start`. Maps to
     * `ROWS start`.
     *
     * {{{
     * orderBy(column("id").asc).rows(unboundedPreceding)
     * }}}
     */
    def rows(start: FrameBound): Over =
        Over(partitionBy, orderBy, Some(SqlWindowFrame.Start(Nil, SqlWindowFrameUnit.Rows, start.bound, None, None)))

    /**
     * Sets a `ROWS BETWEEN start AND end` frame. Maps to
     * `ROWS BETWEEN start AND end`.
     *
     * {{{
     * orderBy(column("id").asc).rowsBetween(unboundedPreceding, currentRow)
     * }}}
     */
    def rowsBetween(start: FrameBound, end: FrameBound): Over =
        Over(partitionBy, orderBy, Some(SqlWindowFrame.Between(Nil, SqlWindowFrameUnit.Rows, start.bound, end.bound, None, None)))

    /**
     * Sets a `RANGE` frame starting at `start`. Maps to
     * `RANGE start`.
     *
     * {{{
     * orderBy(column("score").asc).range(unboundedPreceding)
     * }}}
     */
    def range(start: FrameBound): Over =
        Over(partitionBy, orderBy, Some(SqlWindowFrame.Start(Nil, SqlWindowFrameUnit.Range, start.bound, None, None)))

    /**
     * Sets a `RANGE BETWEEN start AND end` frame. Maps to
     * `RANGE BETWEEN start AND end`.
     *
     * {{{
     * orderBy(column("score").asc).rangeBetween(unboundedPreceding, currentRow)
     * }}}
     */
    def rangeBetween(start: FrameBound, end: FrameBound): Over =
        Over(partitionBy, orderBy, Some(SqlWindowFrame.Between(Nil, SqlWindowFrameUnit.Range, start.bound, end.bound, None, None)))

    /**
     * Sets a `GROUPS` frame starting at `start`. Maps to
     * `GROUPS start`.
     *
     * {{{
     * orderBy(column("score").asc).groups(unboundedPreceding)
     * }}}
     */
    def groups(start: FrameBound): Over =
        Over(partitionBy, orderBy, Some(SqlWindowFrame.Start(Nil, SqlWindowFrameUnit.Groups, start.bound, None, None)))

    /**
     * Sets a `GROUPS BETWEEN start AND end` frame. Maps to
     * `GROUPS BETWEEN start AND end`.
     *
     * {{{
     * orderBy(column("score").asc).groupsBetween(unboundedPreceding, currentRow)
     * }}}
     */
    def groupsBetween(start: FrameBound, end: FrameBound): Over =
        Over(partitionBy, orderBy, Some(SqlWindowFrame.Between(Nil, SqlWindowFrameUnit.Groups, start.bound, end.bound, None, None)))

    /**
     * Adds `EXCLUDE CURRENT ROW` to the current frame. Requires a frame
     * to have been set.
     *
     * {{{
     * orderBy(column("id").asc).rowsBetween(unboundedPreceding, currentRow).excludeCurrentRow
     * }}}
     */
    def excludeCurrentRow: Over =
        Over(partitionBy, orderBy, setExclude(SqlWindowFrameExcludeMode.CurrentRow))

    /**
     * Adds `EXCLUDE TIES` to the current frame. Requires a frame to have
     * been set.
     *
     * {{{
     * orderBy(column("id").asc).rowsBetween(unboundedPreceding, currentRow).excludeTies
     * }}}
     */
    def excludeTies: Over =
        Over(partitionBy, orderBy, setExclude(SqlWindowFrameExcludeMode.Ties))

    /**
     * Adds `EXCLUDE GROUP` to the current frame. Requires a frame to
     * have been set.
     *
     * {{{
     * orderBy(column("id").asc).rowsBetween(unboundedPreceding, currentRow).excludeGroup
     * }}}
     */
    def excludeGroup: Over =
        Over(partitionBy, orderBy, setExclude(SqlWindowFrameExcludeMode.Group))