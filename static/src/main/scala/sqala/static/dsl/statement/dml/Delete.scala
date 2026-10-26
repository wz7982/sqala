package sqala.static.dsl.statement.dml

import sqala.ast.statement.SqlStatement
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.metadata.{SqlBoolean, TableMacro}
import sqala.static.dsl.*
import sqala.static.dsl.table.Table
import sqala.ir.IrStatement
import sqala.ir.IrTable
import sqala.metadata.Dialect
import sqala.util.statementToString
import sqala.ir.ToAst

/**
 * A `DELETE` statement builder, created by `delete[T]`.
 */
final class Delete[T](
    private[sqala] val table: Table[T, Column, 1],
    private[sqala] val tree: IrStatement.Delete
)(using private[sqala] val qc: QueryContext[1]):
    /**
     * Adds a `WHERE` clause to the `DELETE` statement. The condition
     * must be a valid filter expression — aggregate functions, window
     * functions, and other expressions not allowed in `where` are
     * rejected at compile time.
     *
     * {{{
     * delete[User].where(_.id === 1)
     * }}}
     */
    def where[F](f: QueryContext[1] ?=> Table[T, Column, 1] => F)(using
        a: AsExpr[F, 1],
        ba: ValidateArgType[SqlBoolean, a.R, "boolean"],
        kt: KindToTuple[a.K],
        ci: CanInSimpleClause[kt.R, "where"]
    ): Delete[T] =
        val condition = a.asExpr(f(table)).expr
        new Delete(table, tree.addWhere(condition))

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

object Delete:
    inline def apply[T <: Product]: Delete[T] =
        given qc: QueryContext[1] = QueryContext()
        val metaData = TableMacro.tableMetaData[T]
        val irTable: IrTable.Ident =
            IrTable.Ident(
                metaData.tableName,
                Nil
            )
        val table = Table[T, Column, 1](irTable, metaData)
        val tree: IrStatement.Delete = IrStatement.Delete(irTable, None)
        new Delete(table, tree)