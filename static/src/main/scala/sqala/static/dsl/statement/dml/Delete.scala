package sqala.static.dsl.statement.dml

import sqala.ast.statement.SqlStatement
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.static.dsl.table.Table
import sqala.static.dsl.{AsExpr, QueryContext}
import sqala.static.metadata.{SqlBoolean, TableMacro}

class Delete[T](
    private[sqala] val table: Table[T],
    val tree: SqlStatement.Delete
):
    def where[F: AsExpr as a](f: QueryContext ?=> Table[T] => F)(using 
        SqlBoolean[a.R]
    ): Delete[T] =
        given QueryContext = QueryContext(0)
        val condition = a.asExpr(f(table)).asSqlExpr
        new Delete(table, tree.addWhere(condition))

object Delete:
    inline def apply[T <: Product](using c: QueryContext): Delete[T] =
        val metaData = TableMacro.tableMetaData[T]
        val alias = c.fetchAlias
        val sqlTable: SqlTable.Standard = SqlTable.Standard(
            metaData.tableName,
            None,
            Some(SqlTableAlias(alias, Nil)),
            None,
            None
        )
        val table = Table[T](Some(alias), metaData, sqlTable)
        val tree: SqlStatement.Delete = SqlStatement.Delete(sqlTable, None)
        new Delete(table, tree)