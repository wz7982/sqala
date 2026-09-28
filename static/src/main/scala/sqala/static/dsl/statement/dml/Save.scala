package sqala.static.dsl.statement.dml

import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.metadata.{AsSqlExprs, TableMacro}
import sqala.util.NonEmptyList.toNonEmptyList

import scala.compiletime.summonInline
import scala.deriving.Mirror

/**
 * Wraps an upsert statement.
 */
final class Save(private[sqala] val tree: SqlStatement.Upsert)

object Save:
    /**
     * Creates an upsert statement from an entity, determining
     * primary key and update columns from table metadata.
     */
    inline def saveByEntity[T <: Product](entity: T)(using m: Mirror.ProductOf[T]): Save =
        val tableName = TableMacro.tableName[T]
        val metaData = TableMacro.tableMetaData[T]
        val columns = metaData.columnNames
        val data: m.MirroredElemTypes = Tuple.fromProductTyped(entity)
        val values = summonInline[AsSqlExprs[m.MirroredElemTypes]].asSqlExprs(data)
        val primaryKeys = metaData.columnNames
            .zip(metaData.fieldNames)
            .filter((_, f) => metaData.primaryKeyFields.contains(f))
            .map((c, _) => c)
        val updateColumns = metaData.columnNames
            .zip(metaData.fieldNames)
            .filterNot((_, f) => metaData.primaryKeyFields.contains(f))
            .map((c, _) => c)
        val tree: SqlStatement.Upsert =
            SqlStatement.Upsert(
                SqlTable.Ident(tableName, None, None, None, None),
                columns.toNonEmptyList,
                values.toNonEmptyList,
                primaryKeys.toNonEmptyList,
                updateColumns.toNonEmptyList
            )
        new Save(tree)