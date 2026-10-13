package sqala.static.dsl

import sqala.ast.window.{SqlWindowFrame, SqlWindowFrameBound, SqlWindowFrameExcludeMode, SqlWindowFrameUnit}
import sqala.ir.IrWindowFrameBound
import sqala.ir.IrWindowFrame

/**
 * A window frame bound, produced by `currentRow`,
 * `unboundedPreceding`, `unboundedFollowing`, `n.preceding`,
 * and `n.following`.
 */
final case class FrameBound[T, S <: Int, N <: String](private[sqala] val bound: IrWindowFrameBound)

/**
 * An intermediate window specification used in `over` clause.
 * Holds the `partitionBy`, `sortBy`, and optional frame
 * specification.
 */
sealed class Over[KS <: Tuple](
    private[sqala] val partitionBy: List[Expr[?, ?]] = Nil,
    private[sqala] val sortBy: List[Sort[?, ?]] = Nil,
    private[sqala] val frame: Option[IrWindowFrame] = None
)(using OverContext)

/**
 * A window specification after `partitionBy`, supporting `sortBy`, 
 * and unbounded frame methods.
 */
final case class PartitionedOver[KS <: Tuple](
    override private[sqala] val partitionBy: List[Expr[?, ?]] = Nil,
    override private[sqala] val sortBy: List[Sort[?, ?]] = Nil,
    override private[sqala] val frame: Option[IrWindowFrame] = None
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
    def rows[ST, SS <: Int, SN <: String, CL <: Int](start: FrameBound[ST, SS, SN])(using
        QueryContext[CL],
        CanInRowsOrGroupsFrame[ST],
        ValidateFrame[SS, SN, 3, "current row"]
    ): BoundedOver[KS] =
        BoundedOver(
            partitionBy, 
            sortBy, 
            IrWindowFrame.Start(SqlWindowFrameUnit.Rows, start.bound, None)
        )

    /**
     * Sets the window frame to `ROWS BETWEEN start AND end`.
     * 
     * {{{
     * .rowsBetween(1.preceding, currentRow)
     * }}}
     */
    def rowsBetween[ST, SS <: Int, SN <: String, ET, ES <: Int, EN <: String, CL <: Int](
        start: FrameBound[ST, SS, SN], 
        end: FrameBound[ET, ES, EN]
    )(using 
        QueryContext[CL],
        CanInRowsOrGroupsFrame[ST],
        CanInRowsOrGroupsFrame[ET],
        ValidateFrame[SS, SN, ES, EN]
    ): BoundedOver[KS] =
        BoundedOver(
            partitionBy, 
            sortBy, 
            IrWindowFrame.Between(SqlWindowFrameUnit.Rows, start.bound, end.bound, None)
        )

/**
 * A window specification after `sortBy`, supporting window frame
 * methods (`rows`, `range`, `groups` and their `between` variants).
 */
final case class SortedOver[T, KS <: Tuple](
    override private[sqala] val partitionBy: List[Expr[?, ?]] = Nil,
    override private[sqala] val sortBy: List[Sort[?, ?]] = Nil,
    override private[sqala] val frame: Option[IrWindowFrame] = None
)(using OverContext) extends Over[KS](partitionBy, sortBy, frame):
    /**
     * Sets the window frame to `ROWS start`.
     * 
     * {{{
     * .rows(currentRow)
     * }}}
     */
    def rows[ST, SS <: Int, SN <: String, CL <: Int](start: FrameBound[ST, SS, SN])(using
        QueryContext[CL],
        CanInRowsOrGroupsFrame[ST],
        ValidateFrame[SS, SN, 3, "current row"]
    ): BoundedOver[KS] =
        BoundedOver(
            partitionBy, 
            sortBy, 
            IrWindowFrame.Start(SqlWindowFrameUnit.Rows, start.bound, None)
        )

    /**
     * Sets the window frame to `ROWS BETWEEN start AND end`.
     * 
     * {{{
     * .rowsBetween(1.preceding, currentRow)
     * }}}
     */
    def rowsBetween[ST, SS <: Int, SN <: String, ET, ES <: Int, EN <: String, CL <: Int](
        start: FrameBound[ST, SS, SN], 
        end: FrameBound[ET, ES, EN]
    )(using
        QueryContext[CL],
        CanInRowsOrGroupsFrame[ST],
        CanInRowsOrGroupsFrame[ET],
        ValidateFrame[SS, SN, ES, EN]
    ): BoundedOver[KS] =
        BoundedOver(
            partitionBy, 
            sortBy, 
            IrWindowFrame.Between(SqlWindowFrameUnit.Rows, start.bound, end.bound, None)
        )

    /**
     * Sets the window frame to `RANGE start`. 
     * The bound must be compatible with the
     * sort key type.
     * 
     * {{{
     * .range(currentRow)
     * }}}
     */
    def range[ST, SS <: Int, SN <: String, CL <: Int](start: FrameBound[ST, SS, SN])(using
        QueryContext[CL],
        CanInRangeFrame[T, ST],
        ValidateFrame[SS, SN, 3, "current row"]
    ): BoundedOver[KS] =
        BoundedOver(
            partitionBy, 
            sortBy, 
            IrWindowFrame.Start(SqlWindowFrameUnit.Range, start.bound, None)
        )

    /**
     * Sets the window frame to `RANGE BETWEEN start AND end`.
     * The bound must be compatible with the
     * sort key type.
     * 
     * {{{
     * .rangeBetween(1.preceding, currentRow)
     * }}}
     */
    def rangeBetween[ST, SS <: Int, SN <: String, ET, ES <: Int, EN <: String, CL <: Int](
        start: FrameBound[ST, SS, SN], 
        end: FrameBound[ET, ES, EN]
    )(using
        QueryContext[CL],
        CanInRangeFrame[T, ST],
        CanInRangeFrame[T, ET],
        ValidateFrame[SS, SN, ES, EN]
    ): BoundedOver[KS] =
        BoundedOver(
            partitionBy, 
            sortBy, 
            IrWindowFrame.Between(SqlWindowFrameUnit.Range, start.bound, end.bound, None)
        )

    /**
     * Sets the window frame to `GROUPS start`.
     * 
     * {{{
     * .groups(currentRow)
     * }}}
     */
    def groups[ST, SS <: Int, SN <: String, CL <: Int](start: FrameBound[ST, SS, SN])(using
        QueryContext[CL],
        CanInRowsOrGroupsFrame[ST],
        ValidateFrame[SS, SN, 3, "current row"]
    ): BoundedOver[KS] =
        BoundedOver(
            partitionBy, 
            sortBy, 
            IrWindowFrame.Start(SqlWindowFrameUnit.Groups, start.bound, None)
        )

    /**
     * Sets the window frame to `GROUPS BETWEEN start AND end`.
     * 
     * {{{
     * .groupsBetween(1.preceding, currentRow)
     * }}}
     */
    def groupsBetween[ST, SS <: Int, SN <: String, ET, ES <: Int, EN <: String, CL <: Int](
        start: FrameBound[ST, SS, SN], 
        end: FrameBound[ET, ES, EN]
    )(using
        QueryContext[CL],
        CanInRowsOrGroupsFrame[ST],
        CanInRowsOrGroupsFrame[ET],
        ValidateFrame[SS, SN, ES, EN]
    ): BoundedOver[KS] =
        BoundedOver(
            partitionBy, 
            sortBy, 
            IrWindowFrame.Between(SqlWindowFrameUnit.Groups, start.bound, end.bound, None)
        )

/**
 * A window specification after the frame has been set, supporting
 * `exclude` methods.
 */
final case class BoundedOver[KS <: Tuple](
    override private[sqala] val partitionBy: List[Expr[?, ?]] = Nil,
    override private[sqala] val sortBy: List[Sort[?, ?]] = Nil,
    private[sqala] val boundedFrame: IrWindowFrame
)(using OverContext) extends Over[KS](partitionBy, sortBy, Some(boundedFrame)):
    /**
     * Copies the frame with a new exclusion mode.
     */
    private def setExclude(exclude: SqlWindowFrameExcludeMode): IrWindowFrame =
        boundedFrame match
            case s: IrWindowFrame.Start =>
                s.copy(excludeMode = Some(exclude))
            case b: IrWindowFrame.Between =>
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