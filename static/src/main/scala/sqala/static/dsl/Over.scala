package sqala.static.dsl

import sqala.ast.expr.{SqlWindowFrame, SqlWindowFrameBound, SqlWindowFrameExcludeMode, SqlWindowFrameUnit}

class FrameBound[T](private[sqala] val bound: SqlWindowFrameBound)

class Over[K <: ExprKind](
    private[sqala] val partitionBy: List[Expr[?, ?]] = Nil,
    private[sqala] val sortBy: List[Sort[?, ?]] = Nil,
    private[sqala] val frame: Option[SqlWindowFrame] = None
)(using OverContext)

class PartitionedOver[K <: ExprKind](
    private[sqala] override val partitionBy: List[Expr[?, ?]] = Nil,
    private[sqala] override val sortBy: List[Sort[?, ?]] = Nil,
    private[sqala] override val frame: Option[SqlWindowFrame] = None
)(using OverContext) extends Over[K](partitionBy, sortBy, frame):
    def sortBy[T](sortValue: T)(using
        a: AsOverSort[T],
        o: KindOperation[K, a.K],
        c: QueryContext
    ): SortedOver[a.R, a.K] =
        SortedOver(partitionBy, a.asSorts(sortValue), frame)

    def orderBy[T](sortValue: T)(using
        a: AsOverSort[T],
        o: KindOperation[K, a.K],
        c: QueryContext
    ): SortedOver[a.R, a.K] =
        sortBy(sortValue)

    def rows(start: FrameBound[Nothing])(using QueryContext): BoundedOver[K] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Start(SqlWindowFrameUnit.Rows, start.bound, None))

    def rowsBetween(start: FrameBound[Nothing], end: FrameBound[Nothing])(using QueryContext): BoundedOver[K] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Between(SqlWindowFrameUnit.Rows, start.bound, end.bound, None))

class SortedOver[T, K <: ExprKind](
    private[sqala] override val partitionBy: List[Expr[?, ?]] = Nil,
    private[sqala] override val sortBy: List[Sort[?, ?]] = Nil,
    private[sqala] override val frame: Option[SqlWindowFrame] = None
)(using OverContext) extends Over[K](partitionBy, sortBy, frame):
    def rows[S](start: FrameBound[S])(using
        CanInRowsOrGroupsFrame[S],
        QueryContext
    ): BoundedOver[K] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Start(SqlWindowFrameUnit.Rows, start.bound, None))

    def rowsBetween[S, E](start: FrameBound[S], end: FrameBound[E])(using
        CanInRowsOrGroupsFrame[S],
        CanInRowsOrGroupsFrame[E],
        QueryContext
    ): BoundedOver[K] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Between(SqlWindowFrameUnit.Rows, start.bound, end.bound, None))

    def range[S](start: FrameBound[S])(using
        CanInRangeFrame[T, S],
        QueryContext
    ): BoundedOver[K] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Start(SqlWindowFrameUnit.Range, start.bound, None))

    def rangeBetween[S, E](start: FrameBound[S], end: FrameBound[E])(using
        CanInRangeFrame[T, S],
        CanInRangeFrame[T, E],
        QueryContext
    ): BoundedOver[K] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Between(SqlWindowFrameUnit.Range, start.bound, end.bound, None))

    def groups[S](start: FrameBound[S])(using
        CanInRowsOrGroupsFrame[S],
        QueryContext
    ): BoundedOver[K] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Start(SqlWindowFrameUnit.Groups, start.bound, None))

    def groupsBetween[S, E](start: FrameBound[S], end: FrameBound[E])(using
        CanInRowsOrGroupsFrame[S],
        CanInRowsOrGroupsFrame[E],
        QueryContext
    ): BoundedOver[K] =
        BoundedOver(partitionBy, sortBy, SqlWindowFrame.Between(SqlWindowFrameUnit.Groups, start.bound, end.bound, None))

class BoundedOver[K <: ExprKind](
    private[sqala] override val partitionBy: List[Expr[?, ?]] = Nil,
    private[sqala] override val sortBy: List[Sort[?, ?]] = Nil,
    private[sqala] val boundedFrame: SqlWindowFrame
)(using OverContext) extends Over[K](partitionBy, sortBy, Some(boundedFrame)):
    private def setExclude(exclude: SqlWindowFrameExcludeMode): SqlWindowFrame =
        boundedFrame match
            case s: SqlWindowFrame.Start =>
                s.copy(exclude = Some(exclude))
            case b: SqlWindowFrame.Between =>
                b.copy(exclude = Some(exclude))

    def excludeCurrentRow(using QueryContext): Over[K] =
        Over(partitionBy, sortBy, Some(setExclude(SqlWindowFrameExcludeMode.CurrentRow)))

    def excludeTies(using QueryContext): Over[K] =
        Over(partitionBy, sortBy, Some(setExclude(SqlWindowFrameExcludeMode.Ties)))

    def excludeGroup(using QueryContext): Over[K] =
        Over(partitionBy, sortBy, Some(setExclude(SqlWindowFrameExcludeMode.Group)))