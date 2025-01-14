package sqala.static.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlQuery
import sqala.static.dsl.*

import scala.NamedTuple.NamedTuple

class Grouping[T](
    private[sqala] val queryParam: T,
    val ast: SqlQuery.Select
)(using 
    private[sqala] val context: QueryContext
):
    def having(f: QueryContext ?=> T => Expr[Boolean]): Grouping[T] =
        val cond = f(queryParam)
        Grouping(queryParam, ast.addHaving(cond.asSqlExpr))

    def sortBy[S](f: QueryContext ?=> T => S)(using s: AsSort[S]): Grouping[T] =
        val sort = f(queryParam)
        val sqlOrderBy = s.asSort(sort)
        Grouping(queryParam, ast.copy(orderBy = ast.orderBy ++ sqlOrderBy))

    def map[M](f: QueryContext ?=> T => M)(using s: AsSelect[M]): Query[s.R] =
        val mapped = f(queryParam)
        val sqlSelect = s.selectItems(mapped, 1)
        Query(s.transform(mapped), ast.copy(select = sqlSelect))

    def pivot[N <: Tuple, V <: Tuple : AsExpr as a](f: T => NamedTuple[N, V]): PivotQuery[T, N, V] =
        val functions = a.asExprs(f(queryParam).toTuple)
            .map(e => e.asSqlExpr.asInstanceOf[SqlExpr.Func])
        PivotQuery[T, N, V](queryParam, functions, ast)