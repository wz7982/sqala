package sqala.dsl.statement.query

import sqala.ast.statement.SqlQuery
import sqala.dsl.Expr

class GroupByQuery[T](
    private[sqala] val tables: T,
    private[sqala] val ast: SqlQuery.Select
)(using QueryContext):
    def map[R](f: T => R)(using s: SelectItem[R]): SelectQuery[R] =
        val mappedItems = f(tables)
        val selectItems = s.selectItems(mappedItems, 0)
        SelectQuery(mappedItems, ast.copy(select = selectItems))

    def having(f: T => Expr[Boolean]): GroupByQuery[T] =
        val sqlCondition = f(tables).asSqlExpr
        GroupByQuery(tables, ast.addHaving(sqlCondition))