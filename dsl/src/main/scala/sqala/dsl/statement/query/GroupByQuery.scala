package sqala.dsl.statement.query

import sqala.ast.statement.SqlQuery
import sqala.dsl.*

import scala.compiletime.{erasedValue, error}

class GroupByQuery[T](
    private[sqala] val items: T,
    private[sqala] val ast: SqlQuery.Select
)(using QueryContext):
    inline def having[K <: ExprKind](f: QueryContext ?=> T => Expr[Boolean, K]): GroupByQuery[T] =
        inline erasedValue[K] match
            case _: AggKind =>
            case _: AggOperationKind =>
            case _: GroupKind =>
            case _: WindowKind => error("Window functions are not allowed in HAVING.")
            case _ =>
                error("Column must appear in the GROUP BY clause or be used in an aggregate function.")
        val sqlCondition = f(items).asSqlExpr
        GroupByQuery(items, ast.addHaving(sqlCondition))

    inline def sortBy[O, K <: ExprKind](f: QueryContext ?=> T => OrderBy[O, K]): GroupByQuery[T] =
        inline erasedValue[K] match
            case _: AggKind =>
            case _: AggOperationKind =>
            case _: GroupKind =>
            case _: ValueKind =>
                error("Constants are not allowed in ORDER BY.")
            case _ =>
                error("Column must appear in the GROUP BY clause or be used in an aggregate function.")
        val orderBy = f(items)
        val sqlOrderBy = orderBy.asSqlOrderBy
        new GroupByQuery(items, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

    def map[R](f: QueryContext ?=> T => R)(using s: SelectItem[R], a: SelectItemAsExpr[R], i: IsAggOrGroup[R], ck: CheckGroupMapKind[i.R]): ProjectionQuery[R, ResultSize.ManyRows] =
        val mappedItems = f(items)
        val selectItems = s.selectItems(mappedItems, 0)
        ProjectionQuery(mappedItems, ast.copy(select = selectItems))