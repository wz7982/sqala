package sqala.static.dsl

import sqala.ast.expr.{SqlWindowFrame, SqlWindowFrameBound, SqlWindowFrameUnit}

case class Over(
    private[sqala] val partitionBy: List[Expr[?]] = Nil,
    private[sqala] val sortBy: List[Sort[?]] = Nil,
    private[sqala] val frame: Option[SqlWindowFrame] = None
):
    infix def sortBy[T: AsSort as a](sortValue: T): Over =
        copy(sortBy = a.asSort(sortValue))

    infix def orderBy[T: AsSort as a](sortValue: T): Over =
        copy(sortBy = a.asSort(sortValue))

    infix def rows(start: SqlWindowFrameBound): Over =
        copy(frame = Some(SqlWindowFrame.Start(SqlWindowFrameUnit.Rows, start)))

    infix def rowsBetween(start: SqlWindowFrameBound, end: SqlWindowFrameBound): Over =
        copy(frame = Some(SqlWindowFrame.Between(SqlWindowFrameUnit.Rows, start, end)))

    infix def range(start: SqlWindowFrameBound): Over =
        copy(frame = Some(SqlWindowFrame.Start(SqlWindowFrameUnit.Range, start)))

    infix def rangeBetween(start: SqlWindowFrameBound, end: SqlWindowFrameBound): Over =
        copy(frame = Some(SqlWindowFrame.Between(SqlWindowFrameUnit.Range, start, end)))

    infix def groups(start: SqlWindowFrameBound): Over =
        copy(frame = Some(SqlWindowFrame.Start(SqlWindowFrameUnit.Groups, start)))

    infix def groupsBetween(start: SqlWindowFrameBound, end: SqlWindowFrameBound): Over =
        copy(frame = Some(SqlWindowFrame.Between(SqlWindowFrameUnit.Groups, start, end)))