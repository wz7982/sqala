package sqala.static.dsl.statement.dml

import sqala.dialect.Dialect
import sqala.ir.{IrExpr, IrStatement, IrTable, ToAst}
import sqala.metadata.{AsSqlExprs, TableMacro}
import sqala.util.{NonEmptyList, statementToString}

import scala.compiletime.summonInline
import scala.deriving.Mirror

/**
 * Wraps an upsert statement.
 */
final class Save(private[sqala] val tree: IrStatement.Upsert):
    /**
     * Generates the SQL string for the given dialect.
     *
     * `standardEscapeStrings` controls string literal escaping: `true` treats
     * backslashes literally (standard behavior, e.g. PostgreSQL, Oracle);
     * `false` uses backslashes as escape characters (e.g. MySQL).
     *
     * {{{
     * val sql = q.sql(PostgresqlDialect, true)
     * }}}
     */
    def sql(dialect: Dialect, standardEscapeStrings: Boolean = true): String =
        statementToString(ToAst().toAst(tree), dialect, standardEscapeStrings)

object Save:
    /**
     * Creates an upsert statement from an entity, determining
     * primary key and update columns from table metadata.
     */
    inline def saveByEntity[T <: Product](entity: T)(using m: Mirror.ProductOf[T]): Save =
        val tableName = TableMacro.tableName[T]
        val metaData = TableMacro.tableMetaData[T]
        val columns = metaData.columns
        val data: m.MirroredElemTypes = Tuple.fromProductTyped(entity)
        val values = summonInline[AsSqlExprs[m.MirroredElemTypes]]
            .asSqlExprs(data, false)
            .map(e => IrExpr.Native(e))
        val primaryKeys = metaData.columns.toList
            .zip(metaData.fields.toList)
            .filter((_, f) => metaData.primaryKeyFields.contains(f))
            .map((c, _) => c)
        require(primaryKeys.nonEmpty, "Save table must have at least one primary key column.")
        val updateColumns = metaData.columns.toList
            .zip(metaData.fields.toList)
            .filterNot((_, f) => metaData.primaryKeyFields.contains(f))
            .map((c, _) => c)
        require(updateColumns.nonEmpty, "Save table must have at least one update column.")
        val tree: IrStatement.Upsert =
            IrStatement.Upsert(
                IrTable.Ident(tableName, Nil),
                columns,
                NonEmptyList(values.head, values.tail),
                NonEmptyList(primaryKeys.head, primaryKeys.tail),
                NonEmptyList(updateColumns.head, updateColumns.tail)
            )
        new Save(tree)