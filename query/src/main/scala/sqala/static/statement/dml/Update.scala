package sqala.static.statement.dml

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.common.AsSqlExpr
import sqala.macros.TableMacro
import sqala.static.dsl.{Expr, Table}

import scala.deriving.Mirror

class Update[T, S <: UpdateState](
    private[sqala] val items: T,
    val ast: SqlStatement.Update
):
    inline def set(f: T => UpdatePair)(using S =:= UpdateTable): Update[T, UpdateTable] =
        val pair = f(items)
        val expr = SqlExpr.Column(None, pair.columnName)
        val updateExpr = pair.updateExpr
        new Update(items, ast.copy(setList = ast.setList :+ (expr, updateExpr)))

    inline def where(f: T => Expr[Boolean])(using S =:= UpdateTable): Update[T, UpdateTable] =
        val condition = f(items)
        new Update(items, ast.addWhere(condition.asSqlExpr))

object Update:
    inline def apply[T <: Product]: Update[Table[T], UpdateTable] =
        val tableName = TableMacro.tableName[T]
        val metaData = TableMacro.tableMetaData[T]
        val table = Table[T](tableName, tableName, metaData)
        val ast: SqlStatement.Update = SqlStatement.Update(SqlTable.Range(tableName, None), Nil, None)
        new Update(table, ast)

    inline def apply[T <: Product](entity: T, skipNone: Boolean = false)(using p: Mirror.ProductOf[T]): Update[Table[T], UpdateEntity] =
        val tableName = TableMacro.tableName[T]
        val metaData = TableMacro.tableMetaData[T]
        val table = Table[T](tableName, tableName, metaData)
        val instances = AsSqlExpr.summonInstances[p.MirroredElemTypes]
        val updateMetaData = metaData.fieldNames
            .zip(metaData.columnNames)
            .zip(instances.map(_.asInstanceOf[AsSqlExpr[Any]]))
            .zip(entity.productIterator.toList)
            .map(i => (i._1._1._1, i._1._1._2, i._1._2, i._2))
        val updateColumns = updateMetaData
            .filterNot((c, _, _, _) => metaData.primaryKeyFields.contains(c))
            .filter: (_, _, _, field) =>
                (field, skipNone) match
                    case (None, true) => false
                    case _ => true
            .map((_, column, instance, field) => (SqlExpr.Column(None, column), instance.asSqlExpr(field)))
        val conditions = updateMetaData
            .filter((c, _, _, _) => metaData.primaryKeyFields.contains(c))
            .map((_, column, instance, field) => SqlExpr.Binary(SqlExpr.Column(None, column), SqlBinaryOperator.Equal, instance.asSqlExpr(field)))
        val condition = if conditions.isEmpty then None else Some(conditions.reduce((x, y) => SqlExpr.Binary(x, SqlBinaryOperator.And, y)))
        val ast: SqlStatement.Update = SqlStatement.Update(SqlTable.Range(tableName, None), updateColumns, condition)
        new Update(table, ast)