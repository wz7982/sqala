package sqala.static.dsl

import sqala.ast.expr.{SqlWindowFrame, SqlWindowFrameBound, SqlWindowFrameUnit}

case class Over[K <: ExprKind](
    private[sqala] val partitionBy: List[Expr[?, ?]] = Nil,
    private[sqala] val sortBy: List[Sort[?, ?]] = Nil,
    private[sqala] val frame: Option[SqlWindowFrame] = None
):
    def sortBy[T](sortValue: T)(using
        a: AsOverSort[T],
        o: KindOperation[K, a.K],
        c: QueryContext,
        oc: OverContext
    ): Over[a.K] =
        copy(sortBy = sortBy ++ a.asSorts(sortValue))

    def orderBy[T](sortValue: T)(using
        a: AsOverSort[T],
        o: KindOperation[K, a.K],
        c: QueryContext,
        oc: OverContext
    ): Over[a.K] =
        sortBy(sortValue)

    def rows(start: SqlWindowFrameBound)(using QueryContext, OverContext): Over[K] =
        copy(frame = Some(SqlWindowFrame.Start(SqlWindowFrameUnit.Rows, start, None)))

    def rowsBetween(start: SqlWindowFrameBound, end: SqlWindowFrameBound)(using
        QueryContext,
        OverContext
    ): Over[K] =
        copy(frame = Some(SqlWindowFrame.Between(SqlWindowFrameUnit.Rows, start, end, None)))

    def range(start: SqlWindowFrameBound)(using QueryContext, OverContext): Over[K] =
        copy(frame = Some(SqlWindowFrame.Start(SqlWindowFrameUnit.Range, start, None)))

    def rangeBetween(start: SqlWindowFrameBound, end: SqlWindowFrameBound)(using
        QueryContext,
        OverContext
    ): Over[K] =
        copy(frame = Some(SqlWindowFrame.Between(SqlWindowFrameUnit.Range, start, end, None)))

    def groups(start: SqlWindowFrameBound)(using QueryContext, OverContext): Over[K] =
        copy(frame = Some(SqlWindowFrame.Start(SqlWindowFrameUnit.Groups, start, None)))

    def groupsBetween(start: SqlWindowFrameBound, end: SqlWindowFrameBound)(using
        QueryContext,
        OverContext
    ): Over[K] =
        copy(frame = Some(SqlWindowFrame.Between(SqlWindowFrameUnit.Groups, start, end, None)))