package sqala.ir

import sqala.ast.limit.{SqlFetchMode, SqlFetchUnit}
import sqala.ast.order.{SqlNullsOrdering, SqlOrdering}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.window.{SqlWindowFrameExcludeMode, SqlWindowFrameUnit}
import sqala.util.NonEmptyList

/**
 * ⚠️ A unsafe custom token that can be used in queries.
 * ⚠️ Do not pass user input directly!
 */
enum IrUnsafeToken:
    case Keyword(keyword: String)
    case Expr(expr: IrExpr)

/**
 * The right-hand operand of an `IN` predicate.
 */
enum IrInRightOperand:
    case Values(items: List[IrExpr])
    case Subquery(query: IrQuery)

/**
 * A single `WHEN ... THEN ...` branch of a `CASE` expression.
 */
case class IrCaseBranch(when: IrExpr, `then`: IrExpr)

/**
 * A window frame, either starting at a bound (`Start`) or spanning two
 * bounds (`Between`).
 */
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

/**
 * A window frame bound, such as `CURRENT ROW`, `UNBOUNDED PRECEDING`, or
 * `n FOLLOWING`.
 */
enum IrWindowFrameBound:
    case CurrentRow
    case UnboundedPreceding
    case Preceding(n: IrExpr)
    case UnboundedFollowing
    case Following(n: IrExpr)

/**
 * A key/value pair of a `JSON_OBJECT` construction.
 */
case class IrJsonObjectItem(key: IrExpr, value: IrExpr)

/**
 * A single `ORDER BY` item.
 */
case class IrOrderingItem(
    expr: IrExpr,
    ordering: SqlOrdering,
    nullsOrdering: Option[SqlNullsOrdering]
)

/**
 * A single `SELECT` projection item.
 */
case class IrSelectItem(expr: IrExpr, alias: String)

/**
 * A `GROUP BY` clause.
 */
case class IrGroup(quantifier: Option[SqlQuantifier], items: NonEmptyList[IrGroupingItem])

/**
 * A grouping item in a `GROUP BY` clause, including the multidimensional
 * `CUBE`, `ROLLUP`, and `GROUPING SETS` forms.
 */
enum IrGroupingItem:
    case EmptyGroup
    case Expr(item: IrExpr)
    case Cube(items: NonEmptyList[IrExpr])
    case Rollup(items: NonEmptyList[IrExpr])
    case GroupingSets(items: NonEmptyList[IrGroupingItem])

/**
 * A `LIMIT`/`OFFSET` clause.
 */
case class IrLimit(offset: Option[IrExpr], fetch: Option[IrFetch])

/**
 * A `FETCH FIRST|NEXT ... ROWS ONLY|WITH TIES` clause.
 */
case class IrFetch(limit: IrExpr, unit: SqlFetchUnit, mode: SqlFetchMode)

/**
 * A row-level lock mode (`FOR UPDATE` or `FOR SHARE`).
 */
enum IrLock(val waitMode: Option[IrLockWaitMode]):
    case Update(override val waitMode: Option[IrLockWaitMode]) extends IrLock(waitMode)
    case Share(override val waitMode: Option[IrLockWaitMode]) extends IrLock(waitMode)

/**
 * The wait behavior applied to a row lock (`NOWAIT` or `SKIP LOCKED`).
 */
enum IrLockWaitMode:
    case NoWait
    case SkipLocked

/**
 * A common table expression (CTE) item.
 */
case class IrWithItem(columns: List[String], query: IrQuery)