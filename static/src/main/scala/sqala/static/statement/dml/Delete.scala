package sqala.static.statement.dml

import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.static.common.*
import sqala.static.macros.*

class Delete[T](
    private[sqala] val tableName: String,
    override val ast: SqlStatement.Delete
) extends Dml(ast):
    inline def where(inline f: T => Boolean): Delete[T] =
        val condition = ClauseMacro.fetchFilter(f, false, false, tableName :: Nil, new QueryContext)
        new Delete(tableName, ast.addWhere(condition))

object Delete:
    inline def apply[T <: Product]: Delete[Table[T]] =
        val tableName = TableMacro.tableName[T]
        val ast: SqlStatement.Delete = SqlStatement.Delete(SqlTable.IdentTable(tableName, None), None)
        new Delete(tableName, ast)