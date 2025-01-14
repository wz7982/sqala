package sqala.static.statement.dml

import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.static.dsl.{Expr, Table}
import sqala.static.macros.TableMacro

class Delete[T](
    private[sqala] val table: T,
    val ast: SqlStatement.Delete
):
    inline def where(f: T => Expr[Boolean]): Delete[T] =
        val condition = f(table).asSqlExpr
        new Delete(table, ast.addWhere(condition))

object Delete:
    inline def apply[T <: Product]: Delete[Table[T]] =
        val tableName = TableMacro.tableName[T]
        val metaData = TableMacro.tableMetaData[T]
        val table = Table[T](tableName, tableName, metaData)
        val ast: SqlStatement.Delete = SqlStatement.Delete(SqlTable.Range(tableName, None), None)
        new Delete(table, ast)