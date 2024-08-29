package sqala.dsl.statement.query

import sqala.ast.statement.SqlQuery
import sqala.dsl.*

class GroupByQuery[T](
    private[sqala] val tables: T,
    private[sqala] val ast: SqlQuery.Select
)(using QueryContext):
    def having[K <: ExprKind](f: T => Expr[Boolean, K]): GroupByQuery[T] =
        val sqlCondition = f(tables).asSqlExpr
        GroupByQuery(tables, ast.addHaving(sqlCondition))

    def sortBy(f: T => OrderBy[AggKind]): GroupByQuery[T] =
        val orderBy = f(tables)
        val sqlOrderBy = orderBy.asSqlOrderBy
        new GroupByQuery(tables, ast.copy(orderBy = ast.orderBy :+ sqlOrderBy))

    def map[R](f: T => R)(using s: SelectItem[R], a: IsAggOrWindowKind[R], t: a.R =:= true, c: ChangeKind[R, ColumnKind]): SelectQuery[c.R] =
        val mappedItems = f(tables)
        val selectItems = s.selectItems(mappedItems, 0)
        SelectQuery(c.changeKind(mappedItems), ast.copy(select = selectItems))