package sqala.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlQuery
import sqala.ast.table.{SqlJoinCondition, SqlTable}
import sqala.dsl.*
import sqala.dsl.macros.AnalysisMacro

import scala.util.TupledFunction

class JoinQuery[T](
    private[sqala] val tables: T,
    private[sqala] val table: Option[SqlTable.JoinTable],
    private[sqala] val ast: SqlQuery.Select
)(using QueryContext):
    inline def on[F](using 
        t: TupledFunction[F, T => Expr[Boolean]]
    )(inline f: QueryContext ?=> F): SelectQuery[T] =
        AnalysisMacro.analysisFilter(f)
        val func = t.tupled(f)
        val sqlCondition = func(tables).asSqlExpr
        val sqlTable = table.map(_.copy(condition = Some(SqlJoinCondition.On(sqlCondition))))
        SelectQuery(tables, ast.copy(from = sqlTable.toList))

    inline def apply[F](using 
        TupledFunction[F, T => Expr[Boolean]]
    )(inline f: QueryContext ?=> F): SelectQuery[T] = on(f)