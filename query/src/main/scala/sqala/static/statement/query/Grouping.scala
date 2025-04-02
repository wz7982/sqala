package sqala.static.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlQuery
import sqala.static.dsl.*

import scala.NamedTuple.*
import scala.compiletime.constValue

class Grouping[T](
    private[sqala] val queryParam: T,
    val ast: SqlQuery.Select
)(using
    private[sqala] val context: QueryContext
):
    def having[F: AsExpr as a](f: QueryContext ?=> T => F)(using a.R =:= Boolean): Grouping[T] =
        val cond = a.asExpr(f(queryParam))
        Grouping(queryParam, ast.addHaving(cond.asSqlExpr))

    def sortBy[S](f: QueryContext ?=> T => S)(using s: AsSort[S]): Grouping[T] =
        val sort = f(queryParam)
        val sqlOrderBy = s.asSort(sort).map(_.asSqlOrderBy)
        Grouping(queryParam, ast.copy(orderBy = ast.orderBy ++ sqlOrderBy))

    def orderBy[S](f: QueryContext ?=> T => S)(using s: AsSort[S]): Grouping[T] =
        sortBy(f)

    def map[M](f: QueryContext ?=> T => M)(using s: AsSelect[M]): Query[s.R] =
        val mapped = f(queryParam)
        val sqlSelect = s.selectItems(mapped, 1)
        Query(s.transform(mapped), ast.copy(select = sqlSelect))

    def select[M](f: QueryContext ?=> T => M)(using s: AsSelect[M]): Query[s.R] =
        map(f)

    def pivot[N <: Tuple, V <: Tuple : AsExpr as a](f: QueryContext ?=> T => NamedTuple[N, V]): Pivot[T, N, V] =
        val functions = a.exprs(f(queryParam).toTuple)
            .map(e => e.asSqlExpr.asInstanceOf[SqlExpr.Func])
        Pivot[T, N, V](queryParam, functions, ast)