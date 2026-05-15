package sqala.dynamic.dsl

import sqala.ast.expr.*
import sqala.ast.order.SqlOrderingItem

final case class FrameBound(private[sqala] val bound: SqlWindowFrameBound)

final case class Over(
    private[sqala] val partitionBy: List[SqlExpr] = Nil,
    private[sqala] val orderBy: List[SqlOrderingItem] = Nil,
    private[sqala] val frame: Option[SqlWindowFrame] = None
):
    private def setExclude(exclude: SqlWindowFrameExcludeMode): Option[SqlWindowFrame] =
        frame.map:
            case s: SqlWindowFrame.Start =>
                s.copy(exclude = Some(exclude))
            case b: SqlWindowFrame.Between =>
                b.copy(exclude = Some(exclude))

    def orderBy(orders: Order*): Over =
        copy(orderBy = orderBy ++ orders.toList.map(_.order))

    def orderBy(orders: List[Order]): Over =
        orderBy(orders*)

    def rows(start: FrameBound): Over =
        copy(frame = Some(SqlWindowFrame.Start(SqlWindowFrameUnit.Rows, start.bound, None)))

    def rowsBetween(start: FrameBound, end: FrameBound): Over =
        copy(frame = Some(SqlWindowFrame.Between(SqlWindowFrameUnit.Rows, start.bound, end.bound, None)))

    def range(start: FrameBound): Over =
        copy(frame = Some(SqlWindowFrame.Start(SqlWindowFrameUnit.Range, start.bound, None)))

    def rangeBetween(start: FrameBound, end: FrameBound): Over =
        copy(frame = Some(SqlWindowFrame.Between(SqlWindowFrameUnit.Range, start.bound, end.bound, None)))

    def groups(start: FrameBound): Over =
        copy(frame = Some(SqlWindowFrame.Start(SqlWindowFrameUnit.Groups, start.bound, None)))

    def groupsBetween(start: FrameBound, end: FrameBound): Over =
        copy(frame = Some(SqlWindowFrame.Between(SqlWindowFrameUnit.Groups, start.bound, end.bound, None)))

    def excludeCurrentRow: Over =
        copy(frame = setExclude(SqlWindowFrameExcludeMode.CurrentRow))

    def excludeTies: Over =
        copy(frame = setExclude(SqlWindowFrameExcludeMode.Ties))

    def excludeGroup: Over =
        copy(frame = setExclude(SqlWindowFrameExcludeMode.Group))