package sqala.ir

import sqala.util.NonEmptyList
import sqala.ast.order.SqlOrdering
import sqala.ast.order.SqlNullsOrdering
import sqala.ast.window.SqlWindowFrameUnit
import sqala.ast.window.SqlWindowFrameExcludeMode
import sqala.ast.limit.SqlFetchUnit
import sqala.ast.limit.SqlFetchMode
import sqala.ast.quantifier.SqlQuantifier

enum IrInRightOperand:
    case Values(items: List[IrExpr])
    case Subquery(query: IrQuery)

case class IrCaseBranch(when: IrExpr, `then`: IrExpr)

enum IrWindowFrame(
    val unit: SqlWindowFrameUnit,
    val excludeMode: Option[SqlWindowFrameExcludeMode]
):
    case Start(
        override val unit: SqlWindowFrameUnit,
        start: IrWindowFrameBound,
        override val excludeMode: Option[SqlWindowFrameExcludeMode]
    ) extends IrWindowFrame(unit, excludeMode)
    case Between(
        override val unit: SqlWindowFrameUnit,
        start: IrWindowFrameBound,
        end: IrWindowFrameBound,
        override val excludeMode: Option[SqlWindowFrameExcludeMode]
    ) extends IrWindowFrame(unit, excludeMode)

enum IrWindowFrameBound:
    case CurrentRow
    case UnboundedPreceding
    case Preceding(n: IrExpr)
    case UnboundedFollowing
    case Following(n: IrExpr)

case class IrOrderingItem(
    expr: IrExpr,
    ordering: SqlOrdering,
    nullsOrdering: Option[SqlNullsOrdering]
)

case class IrSelectItem(expr: IrExpr, alias: String)

case class IrGroup(quantifier: Option[SqlQuantifier], items: NonEmptyList[IrGroupingItem])

enum IrGroupingItem:
    case EmptyGroup
    case Expr(item: IrExpr)
    case Cube(items: NonEmptyList[IrExpr])
    case Rollup(items: NonEmptyList[IrExpr])
    case GroupingSets(items: NonEmptyList[IrGroupingItem])

case class IrLimit(offset: Option[IrExpr], fetch: Option[IrFetch])

case class IrFetch(limit: IrExpr, unit: SqlFetchUnit, mode: SqlFetchMode)

enum IrLock(val waitMode: Option[IrLockWaitMode]):
    case Update(override val waitMode: Option[IrLockWaitMode]) extends IrLock(waitMode)
    case Share(override val waitMode: Option[IrLockWaitMode]) extends IrLock(waitMode)

enum IrLockWaitMode:
    case NoWait
    case SkipLocked

case class IrWithItem(columns: List[String], query: IrQuery)