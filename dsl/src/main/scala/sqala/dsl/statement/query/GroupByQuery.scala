package sqala.dsl.statement.query

import sqala.ast.statement.SqlQuery
import sqala.dsl.*

import scala.compiletime.{erasedValue, error}

class GroupByQuery[T](
    private[sqala] val items: T,
    private[sqala] val ast: SqlQuery.Select
)(using QueryContext):
    inline def having[K <: ExprKind](f: T => Expr[Boolean, K]): GroupByQuery[T] =
        inline erasedValue[K] match
            case _: WindowKind => error("Window functions are not allowed in HAVING.")
            case _ =>
        val sqlCondition = f(items).asSqlExpr
        GroupByQuery(items, ast.addHaving(sqlCondition))

    inline def sortBy[O, K <: ExprKind](f: T => OrderBy[O, K]): GroupByQuery[T] =
        inline erasedValue[K] match
            case _: AggKind | AggOperationKind =>
            case _ =>
                error("Column must appear in the GROUP BY clause or be used in an aggregate function.")
        val orderBy = f(items)
        val sqlOrderBy = orderBy.asSqlOrderBy
        new GroupByQuery(items, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

    def map[R](f: T => R)(using s: SelectItem[R], a: IsAggOrGroupKind[R], ck: CheckGroupMapKind[a.R], c: ChangeKind[R, ColumnKind]): ProjectionQuery[c.R, ResultSize.Many.type] =
        val mappedItems = f(items)
        val selectItems = s.selectItems(mappedItems, 0)
        ProjectionQuery(c.changeKind(mappedItems), ast.copy(select = selectItems))