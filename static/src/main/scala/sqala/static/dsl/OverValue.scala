package sqala.static.dsl

import sqala.ast.expr.*

case class OverValue(
    private[sqala] val partitionBy: List[Expr[?]] = Nil,
    private[sqala] val sortBy: List[Sort[?]] = Nil,
    private[sqala] val frame: Option[SqlWindowFrame] = None
):
    infix def sortBy(sortValue: Sort[?]*): OverValue =
        copy(sortBy = sortValue.toList)

    infix def rowsBetween(start: SqlWindowFrameOption, end: SqlWindowFrameOption): OverValue =
        copy(frame = Some(SqlWindowFrame.Rows(start, end)))

    infix def rangeBetween(start: SqlWindowFrameOption, end: SqlWindowFrameOption): OverValue =
        copy(frame = Some(SqlWindowFrame.Range(start, end)))

    infix def groupsBetween(start: SqlWindowFrameOption, end: SqlWindowFrameOption): OverValue =
        copy(frame = Some(SqlWindowFrame.Groups(start, end)))