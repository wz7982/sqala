package sqala.static.dsl

import sqala.ast.expr.{SqlWindowFrame, SqlWindowFrameBound, SqlWindowFrameExcludeMode, SqlWindowFrameUnit}

final case class FrameBound[T](private[sqala] val bound: SqlWindowFrameBound)

sealed class Over[KS <: Tuple](
    private[sqala] val partitionBy: List[Expr[?, ?]] = Nil,
    private[sqala] val sortBy: List[Sort[?, ?]] = Nil,
    private[sqala] val frame: Option[SqlWindowFrame] = None
)(using OverContext)

final case class PartitionedOver[KS <: Tuple](
    override private[sqala] val partitionBy: List[Expr[?, ?]] = Nil,
    override private[sqala] val sortBy: List[Sort[?, ?]] = Nil,
    override private[sqala] val frame: Option[SqlWindowFrame] = None
)(using OverContext) extends Over[KS](partitionBy, sortBy, frame):
    def sortBy[T, CL <: Int](sortValue: T)(using
        qc: QueryContext[CL],
        a: AsOverSort[T, CL],
        c: CombineKindTuple[KS, a.KS]
    ): SortedOver[a.R, c.R] =
        SortedOver(partitionBy, a.asSorts(sortValue), frame)

    def orderBy[T, CL <: Int](sortValue: T)(using
        qc: QueryContext[CL],
        a: AsOverSort[T, CL],
        c: CombineKindTuple[KS, a.KS]
    ): SortedOver[a.R, c.R] =
        sortBy(sortValue)

    def rows[CL <: Int](start: FrameBound[Nothing])(using qc: QueryContext[CL]): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Start(SqlWindowFrameUnit.Rows, start.bound, None))

    def rowsBetween[CL <: Int](start: FrameBound[Nothing], end: FrameBound[Nothing])(using qc: QueryContext[CL]): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Between(SqlWindowFrameUnit.Rows, start.bound, end.bound, None))

final case class SortedOver[T, KS <: Tuple](
    override private[sqala] val partitionBy: List[Expr[?, ?]] = Nil,
    override private[sqala] val sortBy: List[Sort[?, ?]] = Nil,
    override private[sqala] val frame: Option[SqlWindowFrame] = None
)(using OverContext) extends Over[KS](partitionBy, sortBy, frame):
    def rows[S, CL <: Int](start: FrameBound[S])(using
        QueryContext[CL],
        CanInRowsOrGroupsFrame[S]
    ): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Start(SqlWindowFrameUnit.Rows, start.bound, None))

    def rowsBetween[S, E, CL <: Int](start: FrameBound[S], end: FrameBound[E])(using
        QueryContext[CL],
        CanInRowsOrGroupsFrame[S],
        CanInRowsOrGroupsFrame[E]
    ): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Between(SqlWindowFrameUnit.Rows, start.bound, end.bound, None))

    def range[S, CL <: Int](start: FrameBound[S])(using
        QueryContext[CL],
        CanInRangeFrame[T, S]
    ): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Start(SqlWindowFrameUnit.Range, start.bound, None))

    def rangeBetween[S, E, CL <: Int](start: FrameBound[S], end: FrameBound[E])(using
        QueryContext[CL],
        CanInRangeFrame[T, S],
        CanInRangeFrame[T, E]
    ): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Between(SqlWindowFrameUnit.Range, start.bound, end.bound, None))

    def groups[S, CL <: Int](start: FrameBound[S])(using
        QueryContext[CL],
        CanInRowsOrGroupsFrame[S]
    ): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Start(SqlWindowFrameUnit.Groups, start.bound, None))

    def groupsBetween[S, E, CL <: Int](start: FrameBound[S], end: FrameBound[E])(using
        QueryContext[CL],
        CanInRowsOrGroupsFrame[S],
        CanInRowsOrGroupsFrame[E]
    ): BoundedOver[KS] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Between(SqlWindowFrameUnit.Groups, start.bound, end.bound, None))

final case class BoundedOver[KS <: Tuple](
    override private[sqala] val partitionBy: List[Expr[?, ?]] = Nil,
    override private[sqala] val sortBy: List[Sort[?, ?]] = Nil,
    private[sqala] val boundedFrame: SqlWindowFrame
)(using OverContext) extends Over[KS](partitionBy, sortBy, Some(boundedFrame)):
    private def setExclude(exclude: SqlWindowFrameExcludeMode): SqlWindowFrame =
        boundedFrame match
            case s: SqlWindowFrame.Start =>
                s.copy(exclude = Some(exclude))
            case b: SqlWindowFrame.Between =>
                b.copy(exclude = Some(exclude))

    def excludeCurrentRow[CL <: Int](using QueryContext[CL]): Over[KS] =
        Over(partitionBy, sortBy, Some(setExclude(SqlWindowFrameExcludeMode.CurrentRow)))

    def excludeTies[CL <: Int](using QueryContext[CL]): Over[KS] =
        Over(partitionBy, sortBy, Some(setExclude(SqlWindowFrameExcludeMode.Ties)))

    def excludeGroup[CL <: Int](using QueryContext[CL]): Over[KS] =
        Over(partitionBy, sortBy, Some(setExclude(SqlWindowFrameExcludeMode.Group)))