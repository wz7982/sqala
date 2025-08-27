package sqala.static.dsl.statement.dml

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.metadata.{AsSqlExpr, TableMacro}
import sqala.static.dsl.{AsExpr, QueryContext, Table}

import scala.deriving.Mirror

case class UpdatePair(columnName: String, updateExpr: SqlExpr)

enum UpdateState:
    case Table
    case Entity

type UpdateTable = UpdateState.Table.type

type UpdateEntity = UpdateState.Entity.type

class Update[T, S <: UpdateState](
    private[sqala] val table: Table[T],
    val tree: SqlStatement.Update
):
    def set(f: Table[T] => UpdatePair)(using S =:= UpdateTable): Update[T, UpdateTable] =
        val pair = f(table)
        val expr = SqlExpr.Column(None, pair.columnName)
        val updateExpr = pair.updateExpr
        new Update(table, tree.copy(setList = tree.setList :+ (expr, updateExpr)))

    def where[F: AsExpr as a](f: QueryContext ?=> Table[T] => F)(using 
        S =:= UpdateTable, 
        a.R <:< (Boolean | Option[Boolean])
    ): Update[T, UpdateTable] =
        given QueryContext = QueryContext(0)
        val condition = a.asExpr(f(table))
        new Update(table, tree.addWhere(condition.asSqlExpr))

object Update:
    inline def apply[T <: Product]: Update[T, UpdateTable] =
        val tableName = TableMacro.tableName[T]
        val metaData = TableMacro.tableMetaData[T]
        val table = Table[T](tableName, tableName, metaData)
        val tree: SqlStatement.Update = SqlStatement.Update(SqlTable.Standard(tableName, None), Nil, None)
        new Update(table, tree)

    inline def apply[T <: Product](entity: T, skipNone: Boolean = false)(using 
        p: Mirror.ProductOf[T]
    ): Update[T, UpdateEntity] =
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
        val condition = 
            if conditions.isEmpty then None 
            else Some(conditions.reduce((x, y) => SqlExpr.Binary(x, SqlBinaryOperator.And, y)))
        val tree: SqlStatement.Update = SqlStatement.Update(SqlTable.Standard(tableName, None), updateColumns, condition)
        new Update(table, tree)