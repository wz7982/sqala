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
    case CurrentRow extends SqlWindowFrameBound
    case UnboundedPreceding extends SqlWindowFrameBound
    case Preceding(n: SqlExpr) extends SqlWindowFrameBound
    case UnboundedFollowing extends SqlWindowFrameBound
    case Following(n: SqlExpr) extends SqlWindowFrameBound

enum SqlWindowFrameUnit(val unit: String):
    case Rows extends SqlWindowFrameUnit("ROWS")
    case Range extends SqlWindowFrameUnit("RANGE")
    case Groups extends SqlWindowFrameUnit("GROUPS")

enum SqlWindowFrameExcludeMode(val mode: String):
    case CurrentRow extends SqlWindowFrameExcludeMode("EXCLUDE CURRENT ROW")
    case Group extends SqlWindowFrameExcludeMode("EXCLUDE GROUP")
    case Ties extends SqlWindowFrameExcludeMode("EXCLUDE TIES")
    case NoOthers extends SqlWindowFrameExcludeMode("EXCLUDE NO OTHERS")