package sqala.dsl.statement.query

import sqala.ast.statement.SqlQuery
import sqala.dsl.*

class GroupByQuery[T](
    private[sqala] val items: T,
    private[sqala] val ast: SqlQuery.Select
)(using QueryContext):
    def having[K <: ExprKind](f: T => Expr[Boolean, K]): GroupByQuery[T] =
        val sqlCondition = f(items).asSqlExpr
        GroupByQuery(items, ast.addHaving(sqlCondition))

    def sortBy(f: T => OrderBy[AggKind]): GroupByQuery[T] =
        val orderBy = f(items)
        val sqlOrderBy = orderBy.asSqlOrderBy
        new GroupByQuery(items, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

    def map[R](f: T => R)(using s: SelectItem[R], a: IsAggOrWindowKind[R], t: a.R =:= true, c: ChangeKind[R, ColumnKind]): SelectQuery[c.R] =
        val mappedItems = f(items)
        val selectItems = s.selectItems(mappedItems, 0)
        SelectQuery(c.changeKind(mappedItems), ast.copy(select = selectItems))