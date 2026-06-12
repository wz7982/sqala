package sqala.static.dsl

import sqala.ast.expr.{SqlWindowFrame, SqlWindowFrameBound, SqlWindowFrameExcludeMode, SqlWindowFrameUnit}

/**
 * A window frame bound, produced by `currentRow`,
 * `unboundedPreceding`, `unboundedFollowing`, `n.preceding`,
 * and `n.following`.
 */
final case class FrameBound[T](private[sqala] val bound: SqlWindowFrameBound)

/**
 * An intermediate window specification used in `over` clause.
 * Holds the `partitionBy`, `sortBy`, and optional frame
 * specification.
 */
sealed class Over[KS <: Tuple](
    private[sqala] val partitionBy: List[Expr[?, ?]] = Nil,
    private[sqala] val sortBy: List[Sort[?, ?]] = Nil,
    private[sqala] val frame: Option[SqlWindowFrame] = None
)(using OverContext)

/**
 * A window specification after `partitionBy`, supporting `sortBy`, 
 * and unbounded frame methods.
 */
final case class PartitionedOver[KS <: Tuple](
    override private[sqala] val partitionBy: List[Expr[?, ?]] = Nil,
    override private[sqala] val sortBy: List[Sort[?, ?]] = Nil,
    override private[sqala] val frame: Option[SqlWindowFrame] = None
)(using OverContext) extends Over[KS](partitionBy, sortBy, frame):
    /**
     * Specifies `ORDER BY` for the window.
     *
     * {{{
     * .sortBy(p.viewCount.desc)
     * }}}
     */
    def sortBy[T, CL <: Int](sortValue: T)(using
        qc: QueryContext[CL],
        a: AsOverSort[T, CL],
        c: CombineKindTuple[KS, a.KS]
    ): SortedOver[a.R, c.R] =
        SortedOver(partitionBy, a.asSorts(sortValue), frame)

    /**
     * Alias of `sortBy`, provided for users familiar with `ORDER BY`.
     * {{{
     * .orderBy(p.viewCount.desc)
     * }}}
     */
    def orderBy[T, CL <: Int](sortValue: T)(using
        qc: QueryContext[CL],
        a: AsOverSort[T, CL],
        c: CombineKindTuple[KS, a.KS]
    ): SortedOver[a.R, c.R] =
        sortBy(sortValue)

    /**
     * Sets the window frame to `ROWS start`.
     * 
     * {{{
     * .rows(currentRow)
     * }}}
     */
    def rows[CL <: Int](start: FrameBound[Nothing])(using qc: QueryContext[CL]): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Start(SqlWindowFrameUnit.Rows, start.bound, None))

    /**
     * Sets the window frame to `ROWS BETWEEN start AND end`.
     * 
     * {{{
     * .rowsBetween(1.preceding, currentRow)
     * }}}
     */
    def rowsBetween[CL <: Int](start: FrameBound[Nothing], end: FrameBound[Nothing])(using qc: QueryContext[CL]): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Between(SqlWindowFrameUnit.Rows, start.bound, end.bound, None))

/**
 * A window specification after `sortBy`, supporting window frame
 * methods (`rows`, `range`, `groups` and their `between` variants).
 */
final case class SortedOver[T, KS <: Tuple](
    override private[sqala] val partitionBy: List[Expr[?, ?]] = Nil,
    override private[sqala] val sortBy: List[Sort[?, ?]] = Nil,
    override private[sqala] val frame: Option[SqlWindowFrame] = None
)(using OverContext) extends Over[KS](partitionBy, sortBy, frame):
    /**
     * Sets the window frame to `ROWS start`.
     * 
     * {{{
     * .rows(currentRow)
     * }}}
     */
    def rows[S, CL <: Int](start: FrameBound[S])(using
        QueryContext[CL],
        CanInRowsOrGroupsFrame[S]
    ): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Start(SqlWindowFrameUnit.Rows, start.bound, None))

    /**
     * Sets the window frame to `ROWS BETWEEN start AND end`.
     * 
     * {{{
     * .rowsBetween(1.preceding, currentRow)
     * }}}
     */
    def rowsBetween[S, E, CL <: Int](start: FrameBound[S], end: FrameBound[E])(using
        QueryContext[CL],
        CanInRowsOrGroupsFrame[S],
        CanInRowsOrGroupsFrame[E]
    ): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Between(SqlWindowFrameUnit.Rows, start.bound, end.bound, None))

    /**
     * Sets the window frame to `RANGE start`. 
     * The bound must be compatible with the
     * sort key type.
     * 
     * {{{
     * .range(currentRow)
     * }}}
     */
    def range[S, CL <: Int](start: FrameBound[S])(using
        QueryContext[CL],
        CanInRangeFrame[T, S]
    ): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Start(SqlWindowFrameUnit.Range, start.bound, None))

    /**
     * Sets the window frame to `RANGE BETWEEN start AND end`.
     * The bound must be compatible with the
     * sort key type.
     * 
     * {{{
     * .rangeBetween(1.preceding, currentRow)
     * }}}
     */
    def rangeBetween[S, E, CL <: Int](start: FrameBound[S], end: FrameBound[E])(using
        QueryContext[CL],
        CanInRangeFrame[T, S],
        CanInRangeFrame[T, E]
    ): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Between(SqlWindowFrameUnit.Range, start.bound, end.bound, None))

    /**
     * Sets the window frame to `GROUPS start`.
     * 
     * {{{
     * .groups(currentRow)
     * }}}
     */
    def groups[S, CL <: Int](start: FrameBound[S])(using
        QueryContext[CL],
        CanInRowsOrGroupsFrame[S]
    ): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Start(SqlWindowFrameUnit.Groups, start.bound, None))

    /**
     * Sets the window frame to `GROUPS BETWEEN start AND end`.
     * 
     * {{{
     * .groupsBetween(1.preceding, currentRow)
     * }}}
     */
    def groupsBetween[S, E, CL <: Int](start: FrameBound[S], end: FrameBound[E])(using
        QueryContext[CL],
        CanInRowsOrGroupsFrame[S],
        CanInRowsOrGroupsFrame[E]
    ): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Between(SqlWindowFrameUnit.Groups, start.bound, end.bound, None))

/**
 * A window specification after the frame has been set, supporting
 * `exclude` methods.
 */
final case class BoundedOver[KS <: Tuple](
    override private[sqala] val partitionBy: List[Expr[?, ?]] = Nil,
    override private[sqala] val sortBy: List[Sort[?, ?]] = Nil,
    private[sqala] val boundedFrame: SqlWindowFrame
)(using OverContext) extends Over[KS](partitionBy, sortBy, Some(boundedFrame)):
    /**
     * Copies the frame with a new exclusion mode.
     */
    private def setExclude(exclude: SqlWindowFrameExcludeMode): SqlWindowFrame =
        boundedFrame match
            case s: SqlWindowFrame.Start =>
                s.copy(excludeMode = Some(exclude))
            case b: SqlWindowFrame.Between =>
                b.copy(excludeMode = Some(exclude))

    /**
     * Excludes the current row from the window frame. Maps to
     * `EXCLUDE CURRENT ROW`.
     * 
     * {{{
     * .excludeCurrentRow
     * }}}
     */
    def excludeCurrentRow[CL <: Int](using QueryContext[CL]): Over[KS] =
        Over(partitionBy, sortBy, Some(setExclude(SqlWindowFrameExcludeMode.CurrentRow)))

    /**
     * Excludes ties from the window frame. Maps to
     * `EXCLUDE TIES`.
     * 
     * {{{
     * .excludeTies
     * }}}
     */
    def excludeTies[CL <: Int](using QueryContext[CL]): Over[KS] =
        Over(partitionBy, sortBy, Some(setExclude(SqlWindowFrameExcludeMode.Ties)))

    /**
     * Excludes the current group from the window frame. Maps to
     * `EXCLUDE GROUP`.
     * 
     * {{{
     * .excludeGroup
     * }}}
     */
    def excludeGroup[CL <: Int](using QueryContext[CL]): Over[KS] =
        Over(partitionBy, sortBy, Some(setExclude(SqlWindowFrameExcludeMode.Group)))