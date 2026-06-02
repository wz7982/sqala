package sqala.static.dsl.statement.dml

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.statement.{SqlStatement, SqlUpdateSetPair}
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.metadata.{AsSqlExpr, SqlBoolean, TableMacro}
import sqala.static.dsl.*
import sqala.static.dsl.table.Table

import scala.deriving.Mirror

case class UpdatePair(private[sqala] val columnName: String, private[sqala] val updateExpr: SqlExpr)

enum UpdateState:
    case Table
    case Entity

type UpdateTable = UpdateState.Table.type

type UpdateEntity = UpdateState.Entity.type

class UpdateSetContext

class Update[T, S <: UpdateState](
    private[sqala] val table: Table[T, Column, 1],
    private[sqala] val tree: SqlStatement.Update
)(using private[sqala] val qc: QueryContext[1]):
    def set(f: UpdateSetContext ?=> Table[T, Column, 1] => UpdatePair)(using
        S =:= UpdateTable
    ): Update[T, UpdateTable] =
        given UpdateSetContext = new UpdateSetContext
        val pair = f(table)
        val updateExpr = pair.updateExpr
        new Update(table, tree.copy(setList = tree.setList :+ SqlUpdateSetPair(pair.columnName, updateExpr)))

    def where[F](f: QueryContext[1] ?=> Table[T, Column, 1] => F)(using
        a: AsExpr[F, 1],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        refl: S =:= UpdateTable,
    ): Update[T, UpdateTable] =
        val condition = a.asExpr(f(table))
        new Update(table, tree.addWhere(condition.asSqlExpr))

object Update:
    given qc: QueryContext[1] = QueryContext(0)

    inline def apply[T <: Product]: Update[T, UpdateTable] =
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
        val tree: SqlStatement.Update = SqlStatement.Update(sqlTable, Nil, None)
        new Update(table, tree)

    inline def updateByEntity[T <: Product](entity: T, skipNone: Boolean = false)(using
        p: Mirror.ProductOf[T]
    ): Update[T, UpdateEntity] =
        val metaData = TableMacro.tableMetaData[T]
        val sqlTable: SqlTable.Ident = SqlTable.Ident(
            metaData.tableName,
            None,
            None,
            None,
            None
        )
        val table = Table[T, Column, 1](None, metaData, sqlTable)
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
            .map: (_, column, instance, field) =>
                SqlUpdateSetPair(column, instance.asSqlExpr(field))
        val conditions = updateMetaData
            .filter((c, _, _, _) => metaData.primaryKeyFields.contains(c))
            .map: (_, column, instance, field) =>
                SqlExpr.Binary(SqlExpr.Column(None, column), SqlBinaryOperator.Equal, instance.asSqlExpr(field))
        val condition =
            if conditions.isEmpty then None
            else Some(conditions.reduce((x, y) => SqlExpr.Binary(x, SqlBinaryOperator.And, y)))
        val tree: SqlStatement.Update = SqlStatement.Update(sqlTable, updateColumns, condition)
        new Update(table, tree)