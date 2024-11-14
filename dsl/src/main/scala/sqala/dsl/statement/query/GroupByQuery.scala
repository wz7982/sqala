package sqala.dsl.statement.query

import sqala.ast.statement.SqlQuery
import sqala.dsl.*
import sqala.dsl.macros.AnalysisMacro

import scala.NamedTuple.NamedTuple
import scala.util.TupledFunction

class GroupByQuery[T](
    private[sqala] val queryItems: T,
    private[sqala] val ast: SqlQuery.Select
)(using QueryContext):
    inline def having[F](using
        t: TupledFunction[F, T => Expr[Boolean]]
    )(inline f: QueryContext ?=> F): GroupByQuery[T] =
        AnalysisMacro.analysisHaving(f)
        val func = t.tupled(f)
        val sqlCondition = func(queryItems).asSqlExpr
        GroupByQuery(queryItems, ast.addHaving(sqlCondition))

    inline def map[F, N <: Tuple, V <: Tuple](using
        t: TupledFunction[F, T => NamedTuple[N, V]]
    )(inline f: QueryContext ?=> F)(using
        s: SelectItem[V],
        a: SelectItemAsExpr[V]
    ): GroupedProjectionQuery[N, a.R, ManyRows] =
        AnalysisMacro.analysisGroupedSelect(f)
        val func = t.tupled(f)
        val mappedItems = func(queryItems)
        val selectItems = s.selectItems(mappedItems, 0)
        GroupedProjectionQuery(a.asExpr(mappedItems), ast.copy(select = selectItems))