package sqala.dsl.statement.dml

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.dsl.`macro`.{tableMetaDataMacro, tableNameMacro}
import sqala.dsl.{AsSqlExpr, Column, Expr, Table}

import scala.deriving.Mirror

class Update[T, S <: UpdateState](
    private[sqala] val items: T,
    val ast: SqlStatement.Update
):
    inline def set(f: T => UpdatePair)(using S =:= UpdateState.Table.type): Update[T, UpdateState.Table.type] =
        val pair = f(items)
        val expr = SqlExpr.Column(None, pair.expr.columnName)
        val updateExpr = pair.updateExpr.asSqlExpr
        new Update(items, ast.copy(setList = ast.setList :+ (expr, updateExpr)))

    inline def where(f: T => Expr[Boolean])(using S =:= UpdateState.Table.type): Update[T, UpdateState.Table.type] =
        val condition = f(items)
        new Update(items, ast.addWhere(condition.asSqlExpr))

object Update:
    inline def apply[T]: Update[Table[T], UpdateState.Table.type] =
        val tableName = tableNameMacro[T]
        val metaData = tableMetaDataMacro[T]
        val table = Table[T](tableName, tableName, metaData, 0)
        val ast: SqlStatement.Update = SqlStatement.Update(SqlTable.IdentTable(tableName, None), Nil, None)
        new Update(table, ast)

    inline def apply[T <: Product](entity: T, skipNone: Boolean = false)(using p: Mirror.ProductOf[T]): Update[Table[T], UpdateState.Entity.type] =
        val tableName = tableNameMacro[T]
        val metaData = tableMetaDataMacro[T]
        val table = Table[T](tableName, tableName, metaData, 0)
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
        val ast: SqlStatement.Update = SqlStatement.Update(SqlTable.IdentTable(tableName, None), updateColumns, condition)
        new Update(table, ast)

enum UpdateState:
    case Table
    case Entity

case class UpdatePair(expr: Column[?], updateExpr: Expr[?])