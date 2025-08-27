package sqala.static.dsl.statement.dml

import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.metadata.TableMacro
import sqala.static.dsl.{AsExpr, QueryContext, Table}

class Delete[T](
    private[sqala] val table: Table[T],
    val tree: SqlStatement.Delete
):
    def where[F: AsExpr as a](f: QueryContext ?=> Table[T] => F)(using 
        a.R <:< (Boolean | Option[Boolean])
    ): Delete[T] =
        given QueryContext = QueryContext(0)
        val condition = a.asExpr(f(table)).asSqlExpr
        new Delete(table, tree.addWhere(condition))

object Delete:
    inline def apply[T <: Product]: Delete[T] =
        val tableName = TableMacro.tableName[T]
        val metaData = TableMacro.tableMetaData[T]
        val table = Table[T](tableName, tableName, metaData)
        val tree: SqlStatement.Delete = SqlStatement.Delete(SqlTable.Standard(tableName, None), None)
        new Delete(table, tree)