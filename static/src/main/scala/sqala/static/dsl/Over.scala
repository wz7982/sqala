package sqala.static.dsl

import sqala.ast.expr.{SqlWindowFrame, SqlWindowFrameBound, SqlWindowFrameUnit}

case class Over(
    private[sqala] val partitionBy: List[Expr[?]] = Nil,
    private[sqala] val sortBy: List[Sort] = Nil,
    private[sqala] val frame: Option[SqlWindowFrame] = None
):
    def sortBy[T: AsSort as a](sortValue: T)(using QueryContext, OverContext): Over =
        copy(sortBy = sortBy ++ a.asSort(sortValue))

    def orderBy[T: AsSort as a](sortValue: T)(using QueryContext, OverContext): Over =
        sortBy(sortValue)

    def rows(start: SqlWindowFrameBound)(using QueryContext, OverContext): Over =
        copy(frame = Some(SqlWindowFrame.Start(SqlWindowFrameUnit.Rows, start, None)))

    def rowsBetween(start: SqlWindowFrameBound, end: SqlWindowFrameBound)(using 
        QueryContext, 
        OverContext
    ): Over =
        copy(frame = Some(SqlWindowFrame.Between(SqlWindowFrameUnit.Rows, start, end, None)))

    def range(start: SqlWindowFrameBound)(using QueryContext, OverContext): Over =
        copy(frame = Some(SqlWindowFrame.Start(SqlWindowFrameUnit.Range, start, None)))

    def rangeBetween(start: SqlWindowFrameBound, end: SqlWindowFrameBound)(using 
        QueryContext, 
        OverContext
    ): Over =
        copy(frame = Some(SqlWindowFrame.Between(SqlWindowFrameUnit.Range, start, end, None)))

    def groups(start: SqlWindowFrameBound)(using QueryContext, OverContext): Over =
        copy(frame = Some(SqlWindowFrame.Start(SqlWindowFrameUnit.Groups, start, None)))

    def groupsBetween(start: SqlWindowFrameBound, end: SqlWindowFrameBound)(using 
        QueryContext, 
        OverContext
    ): Over =
        copy(frame = Some(SqlWindowFrame.Between(SqlWindowFrameUnit.Groups, start, end, None)))