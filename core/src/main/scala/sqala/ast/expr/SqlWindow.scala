package sqala.ast.expr

import sqala.ast.order.SqlOrderingItem

case class SqlWindow(
    partitionBy: List[SqlExpr], 
    orderBy: List[SqlOrderingItem], 
    frame: Option[SqlWindowFrame]
)

enum SqlWindowFrame:
    case Start(
        unit: SqlWindowFrameUnit, 
        start: SqlWindowFrameBound,
        exclude: Option[SqlWindowFrameExcludeMode]
    )
    case Between(
        unit: SqlWindowFrameUnit, 
        start: SqlWindowFrameBound, 
        end: SqlWindowFrameBound,
        exclude: Option[SqlWindowFrameExcludeMode]
    )

enum SqlWindowFrameBound:
    case CurrentRow
    case UnboundedPreceding
    case Preceding(n: SqlExpr)
    case UnboundedFollowing
    case Following(n: SqlExpr)

enum SqlWindowFrameUnit:
    case Rows
    case Range
    case Groups

enum SqlWindowFrameExcludeMode:
    case CurrentRow
    case Group
    case Ties
    case NoOthers