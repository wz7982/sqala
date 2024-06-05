package sqala.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlQuery
import sqala.ast.table.{SqlJoinCondition, SqlTable}
import sqala.dsl.{Column, Expr}

class JoinQuery[T](
    private[sqala] val tables: T,
    private[sqala] val table: Option[SqlTable.JoinTable],
    private[sqala] val ast: SqlQuery.Select
)(using QueryContext):
    def on(f: T => Expr[Boolean]): SelectQuery[T] =
        val sqlCondition = f(tables).asSqlExpr
        val sqlTable = table.map(_.copy(condition = Some(SqlJoinCondition.On(sqlCondition))))
        SelectQuery(tables, ast.copy(from = sqlTable.toList))

    def using[E](f: T => Column[E]): SelectQuery[T] =
        val sqlCondition = f(tables).asSqlExpr.asInstanceOf[SqlExpr.Column].copy(tableName = None)
        val sqlTable = table.map(_.copy(condition = Some(SqlJoinCondition.Using(sqlCondition))))
        SelectQuery(tables, ast.copy(from = sqlTable.toList))