package sqala.static.dsl

import sqala.ast.expr.{SqlWindowFrame, SqlWindowFrameOption}

case class Over(
    private[sqala] val partitionBy: List[Expr[?]] = Nil,
    private[sqala] val sortBy: List[Sort[?]] = Nil,
    private[sqala] val frame: Option[SqlWindowFrame] = None
):
    infix def sortBy[T: AsSort as a](sortValue: T): Over =
        copy(sortBy = a.asSort(sortValue))

    infix def orderBy[T: AsSort as a](sortValue: T): Over =
        copy(sortBy = a.asSort(sortValue))

    infix def rowsBetween(start: SqlWindowFrameOption, end: SqlWindowFrameOption): Over =
        copy(frame = Some(SqlWindowFrame.Rows(start, end)))

    infix def rangeBetween(start: SqlWindowFrameOption, end: SqlWindowFrameOption): Over =
        copy(frame = Some(SqlWindowFrame.Range(start, end)))

    infix def groupsBetween(start: SqlWindowFrameOption, end: SqlWindowFrameOption): Over =
        copy(frame = Some(SqlWindowFrame.Groups(start, end)))