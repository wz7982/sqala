package sqala.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlQuery
import sqala.ast.table.{SqlJoinCondition, SqlTable}
import sqala.dsl.{Expr, SimpleKind}

class JoinQuery[T](
    private[sqala] val tables: T,
    private[sqala] val table: Option[SqlTable.JoinTable],
    private[sqala] val ast: SqlQuery.Select
)(using QueryContext):
    def on[K <: SimpleKind](f: T => Expr[Boolean, K]): SelectQuery[T] =
        val sqlCondition = f(tables).asSqlExpr
        val sqlTable = table.map(_.copy(condition = Some(SqlJoinCondition.On(sqlCondition))))
        SelectQuery(tables, ast.copy(from = sqlTable.toList))