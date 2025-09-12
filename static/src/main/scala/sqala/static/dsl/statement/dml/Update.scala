package sqala.static.dsl.statement.dml

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.statement.SqlStatement
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.static.dsl.{AsExpr, QueryContext, Table}
import sqala.static.metadata.{AsSqlExpr, SqlBoolean, TableMacro}

import scala.deriving.Mirror

case class UpdatePair(columnName: String, updateExpr: SqlExpr)

enum UpdateState:
    case Table
    case Entity

type UpdateTable = UpdateState.Table.type

type UpdateEntity = UpdateState.Entity.type

class UpdateSetContext

class Update[T, S <: UpdateState](
    private[sqala] val table: Table[T],
    val tree: SqlStatement.Update
):
    def set(f: UpdateSetContext ?=> Table[T] => UpdatePair)(using 
        S =:= UpdateTable
    ): Update[T, UpdateTable] =
        given UpdateSetContext = new UpdateSetContext
        val pair = f(table)
        val expr = SqlExpr.Column(None, pair.columnName)
        val updateExpr = pair.updateExpr
        new Update(table, tree.copy(setList = tree.setList :+ (expr, updateExpr)))

    def where[F: AsExpr as a](f: QueryContext ?=> Table[T] => F)(using 
        S =:= UpdateTable, 
        SqlBoolean[a.R]
    ): Update[T, UpdateTable] =
        given QueryContext = QueryContext(0)
        val condition = a.asExpr(f(table))
        new Update(table, tree.addWhere(condition.asSqlExpr))

object Update:
    inline def apply[T <: Product](using c: QueryContext): Update[T, UpdateTable] =
        val metaData = TableMacro.tableMetaData[T]
        c.tableIndex += 1
        val alias = s"t${c.tableIndex}"
        val sqlTable: SqlTable.Standard = SqlTable.Standard(
            metaData.tableName,
            Some(SqlTableAlias(alias, Nil)),
            None,
            None
        )
        val table = Table[T](Some(alias), metaData, sqlTable)
        val tree: SqlStatement.Update = SqlStatement.Update(sqlTable, Nil, None)
        new Update(table, tree)

    inline def updateByEntity[T <: Product](entity: T, skipNone: Boolean = false)(using 
        p: Mirror.ProductOf[T]
    ): Update[T, UpdateEntity] =
        val metaData = TableMacro.tableMetaData[T]
        val sqlTable: SqlTable.Standard = SqlTable.Standard(
            metaData.tableName,
            None,
            None,
            None
        )
        val table = Table[T](None, metaData, sqlTable)
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
            .map: (_, column, instance, field) => 
                SqlExpr.Binary(SqlExpr.Column(None, column), SqlBinaryOperator.Equal, instance.asSqlExpr(field))
        val condition = 
            if conditions.isEmpty then None 
            else Some(conditions.reduce((x, y) => SqlExpr.Binary(x, SqlBinaryOperator.And, y)))
        val tree: SqlStatement.Update = SqlStatement.Update(sqlTable, updateColumns, condition)
        new Update(table, tree)