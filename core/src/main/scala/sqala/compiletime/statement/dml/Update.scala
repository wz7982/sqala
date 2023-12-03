package sqala.compiletime.statement.dml

import sqala.ast.expr.*
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.compiletime.*

class Update[T <: Table[?, ?]](val ast: SqlStatement.Update, private[sqala] val table: T) extends Dml:
    infix def set(f: T => UpdatePair): Update[T] =
        val pair = f(table)
        val updatePair = SqlExpr.Column(None, pair.column.columnName) -> pair.updateExpr.toSqlExpr
        new Update(ast.copy(setList = ast.setList :+ updatePair), table)

    infix def where(f: T => Expr[Boolean, ?]): Update[T] =
        val expr = f(table)
        new Update(ast.addWhere(expr.toSqlExpr), table)

object Update:
    def apply[T <: Table[?, ?]](table: T): Update[T] =
        new Update(SqlStatement.Update(table.toSqlTable, Nil, None), table)

    def apply[T <: Product](entity: T, skipNone: Boolean = false)(using e: Entity[T]): Update[?] =
        val updateMetaData = e.updateMeta(entity)
        val tableName = updateMetaData._1
        val updateColumns = (if skipNone then updateMetaData._2.filter(_._2 != SqlExpr.Null) else updateMetaData._2).map((c, v) => (SqlExpr.Column(None, c), v))
        val condition = updateMetaData._3.map((c, v) => SqlExpr.Binary(SqlExpr.Column(None, c), SqlBinaryOperator.Equal, v)).reduce((x, y) => SqlExpr.Binary(x, SqlBinaryOperator.And, y))
        new Update(SqlStatement.Update(SqlTable.IdentTable(tableName, None), updateColumns, Some(condition)), new Table(tableName, None, Nil))

case class UpdatePair(column: Column[?, ?, ?], updateExpr: Expr[?, ?])