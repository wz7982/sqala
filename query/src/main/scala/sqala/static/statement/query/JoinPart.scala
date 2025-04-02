package sqala.static.statement.query

import sqala.ast.statement.SqlQuery
import sqala.ast.table.*
import sqala.static.dsl.{AsExpr, Expr}

class JoinPart[T](
    private[sqala] val queryParam: T,
    private[sqala] val joinTable: SqlTable.Join,
    val ast: SqlQuery.Select
)(using 
    private[sqala] val context: QueryContext
):
    def on[F: AsExpr as a](f: QueryContext ?=> T => F)(using a.R =:= Boolean): JoinQuery[T] =
        val cond = a.asExpr(f(queryParam))
        val newTable = joinTable.copy(condition = Some(SqlJoinCondition.On(cond.asSqlExpr)))
        JoinQuery(queryParam, ast.copy(from = newTable :: Nil))