package sqala.static.statement.dml

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.static.macros.*
import sqala.static.common.*

import scala.deriving.Mirror

class Update[T, S <: UpdateState](
    private[sqala] val tableName: String,
    override val ast: SqlStatement.Update
) extends Dml(ast):
    inline def set(inline f: T => UpdatePair)(using S =:= UpdateTable): Update[T, UpdateTable] =
        val setExpr = ClauseMacro.fetchSet(f, tableName :: Nil, new QueryContext)
        new Update(tableName, ast.copy(setList = ast.setList :+ setExpr))

    inline def where(inline f: T => Boolean)(using S =:= UpdateTable): Update[T, UpdateTable] =
        val condition = ClauseMacro.fetchFilter(f, false, false, tableName :: Nil, new QueryContext)
        new Update(tableName, ast.addWhere(condition))

object Update:
    inline def apply[T <: Product]: Update[Table[T], UpdateTable] =
        val tableName = TableMacro.tableName[T]
        val ast: SqlStatement.Update = SqlStatement.Update(SqlTable.Range(tableName, None), Nil, None)
        new Update(tableName, ast)

    inline def apply[T <: Product](entity: T, skipNone: Boolean = false)(using p: Mirror.ProductOf[T]): Update[Table[T], UpdateEntity] =
        val tableName = TableMacro.tableName[T]
        val metaData = TableMacro.tableMetaData[T]
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
        new Update(tableName, ast)