package sqala.ast.expr

import sqala.ast.order.SqlOrderingItem

case class SqlWindow(
    partitionBy: List[SqlExpr], 
    orderBy: List[SqlOrderingItem], 
    frame: Option[SqlWindowFrame]
)