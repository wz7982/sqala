package sqala.dsl.statement.dml

import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.dsl.`macro`.{tableMetaDataMacro, tableNameMacro}
import sqala.ast.expr.SqlExpr
import sqala.dsl.AsSqlExpr

class Save(val ast: SqlStatement.Upsert)

object Save:
    inline def apply[T <: Product](entity: T): Save =
        val tableName = tableNameMacro[T]
        val metaData = tableMetaDataMacro[T]
        val columns = metaData.columnNames.map(c => SqlExpr.Column(None, c))
        val instances = AsSqlExpr.summonInstances[T].map(_.asInstanceOf[AsSqlExpr[Any]])
        val values = entity.productIterator.toList.zip(instances).map((f, i) => i.asSqlExpr(f))
        val primaryKeys = metaData.columnNames
            .zip(metaData.fieldNames)
            .filter((_, f) => metaData.primaryKeyFields.contains(f))
            .map((c, _) => SqlExpr.Column(None, c))
        val updateColumns = metaData.columnNames
            .zip(metaData.fieldNames)
            .filterNot((_, f) => metaData.primaryKeyFields.contains(f))
            .map((c, _) => SqlExpr.Column(None, c))
        val ast: SqlStatement.Upsert = 
            SqlStatement.Upsert(SqlTable.IdentTable(tableName, None), columns, values, primaryKeys, updateColumns)
        new Save(ast)