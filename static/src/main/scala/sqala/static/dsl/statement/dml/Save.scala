package sqala.static.dsl.statement.dml

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.static.metadata.{AsSqlExpr, TableMacro}

import scala.deriving.Mirror

class Save(val tree: SqlStatement.Upsert)

object Save:
    inline def saveByEntity[T <: Product](entity: T)(using m: Mirror.ProductOf[T]): Save =
        val tableName = TableMacro.tableName[T]
        val metaData = TableMacro.tableMetaData[T]
        val columns = metaData.columnNames.map(c => SqlExpr.Column(None, c))
        val instances = 
            AsSqlExpr.summonInstances[m.MirroredElemTypes].map(_.asInstanceOf[AsSqlExpr[Any]])
        val values = entity.productIterator.toList.zip(instances).map((f, i) => i.asSqlExpr(f))
        val primaryKeys = metaData.columnNames
            .zip(metaData.fieldNames)
            .filter((_, f) => metaData.primaryKeyFields.contains(f))
            .map((c, _) => SqlExpr.Column(None, c))
        val updateColumns = metaData.columnNames
            .zip(metaData.fieldNames)
            .filterNot((_, f) => metaData.primaryKeyFields.contains(f))
            .map((c, _) => SqlExpr.Column(None, c))
        val tree: SqlStatement.Upsert = 
            SqlStatement.Upsert(
                SqlTable.Standard(tableName, None, None, None, None), columns, values, primaryKeys, updateColumns
            )
        new Save(tree)