package sqala.dsl.statement.query

import sqala.ast.statement.SqlQuery
import sqala.dsl.Expr

import scala.NamedTuple.*
import scala.annotation.targetName

class GroupByQuery[T](
    private[sqala] val tables: T,
    private[sqala] val ast: SqlQuery.Select
)(using QueryContext):
    def map[N <: Tuple, V <: Tuple](f: T => NamedTuple[N, V])(using s: SelectItem[NamedTuple[N, V]]): SelectQuery[NamedTupleWrapper[N, V]] =
        val mappedItems = f(tables)
        val selectItems = s.selectItems(mappedItems, 0)
        SelectQuery(NamedTupleWrapper(mappedItems.toTuple), ast.copy(select = selectItems))

    @targetName("mapAny")
    def map[R](f: T => R)(using s: SelectItem[R]): SelectQuery[R] =
        val mappedItems = f(tables)
        val selectItems = s.selectItems(mappedItems, 0)
        SelectQuery(mappedItems, ast.copy(select = selectItems))

    def having(f: T => Expr[Boolean]): GroupByQuery[T] =
        val sqlCondition = f(tables).asSqlExpr
        GroupByQuery(tables, ast.addHaving(sqlCondition))
