package sqala.static.statement.query

import sqala.ast.statement.SqlQuery
import sqala.ast.table.*
import sqala.static.dsl.Expr

class JoinPart[T](
    private[sqala] val queryParam: T,
    private[sqala] val joinTable: SqlTable.Join,
    val ast: SqlQuery.Select
)(using 
    private[sqala] val context: QueryContext
):
    def on(f: QueryContext ?=> T => Expr[Boolean]): JoinQuery[T] =
        val cond = f(queryParam)
        val newTable = joinTable.copy(condition = Some(SqlJoinCondition.On(cond.asSqlExpr)))
        JoinQuery(queryParam, ast.copy(from = newTable :: Nil))