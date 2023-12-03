package sqala.compiletime.statement.dml

import sqala.ast.statement.SqlStatement
import sqala.compiletime.{Expr, Table}

class Delete[T <: Table[?, ?]](val ast: SqlStatement.Delete, private[sqala] val table: T) extends Dml:
    infix def where(f: T => Expr[Boolean, ?]): Delete[T] =
        val expr = f(table)
        new Delete(ast.addWhere(expr.toSqlExpr), table)

object Delete:
    def apply(table: Table[?, ?]): Delete[table.type] = 
        new Delete(SqlStatement.Delete(table.toSqlTable, None), table)