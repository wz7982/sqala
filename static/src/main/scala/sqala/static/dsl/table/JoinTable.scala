package sqala.static.dsl.table

import sqala.ast.table.{SqlJoinCondition, SqlTable}
import sqala.static.dsl.AsExpr
import sqala.static.metadata.SqlBoolean

case class JoinTable[T](
    private[sqala] val params: T,
    private[sqala] val sqlTable: SqlTable.Join
)

case class JoinPart[T](
    private[sqala] val params: T,
    private[sqala] val sqlTable: SqlTable.Join
):
    infix def on[F: AsExpr as a](f: T => F)(using SqlBoolean[a.R]): JoinTable[T] =
        val cond = a.asExpr(f(params))
        JoinTable(
            params,
            sqlTable.copy(condition = Some(SqlJoinCondition.On(cond.asSqlExpr)))
        )