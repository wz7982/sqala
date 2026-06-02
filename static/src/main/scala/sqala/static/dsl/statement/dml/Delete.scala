package sqala.static.dsl.statement.dml

import sqala.ast.statement.SqlStatement
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.metadata.{SqlBoolean, TableMacro}
import sqala.static.dsl.*
import sqala.static.dsl.table.Table

class Delete[T](
    private[sqala] val table: Table[T, Column, 1],
    private[sqala] val tree: SqlStatement.Delete
)(using private[sqala] val qc: QueryContext[1]):
    def where[F](f: QueryContext[1] ?=> Table[T, Column, 1] => F)(using
        a: AsExpr[F, 1],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R]
    ): Delete[T] =
        val condition = a.asExpr(f(table)).asSqlExpr
        new Delete(table, tree.addWhere(condition))

object Delete:
    inline def apply[T <: Product]: Delete[T] =
        given qc: QueryContext[1] = QueryContext(0)
        val metaData = TableMacro.tableMetaData[T]
        val alias = qc.fetchAlias
        val sqlTable: SqlTable.Ident = SqlTable.Ident(
            metaData.tableName,
            Some(SqlTableAlias(alias, Nil)),
            None,
            None,
            None
        )
        val table = Table[T, Column, 1](Some(alias), metaData, sqlTable)
        val tree: SqlStatement.Delete = SqlStatement.Delete(sqlTable, None)
        new Delete(table, tree)