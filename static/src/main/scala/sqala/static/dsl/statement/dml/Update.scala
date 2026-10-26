package sqala.static.dsl.statement.dml

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.statement.{SqlStatement, SqlUpdateSetPair}
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.metadata.{AsSqlExprs, SqlBoolean, TableMacro}
import sqala.static.dsl.*
import sqala.static.dsl.table.Table

import scala.compiletime.summonInline
import scala.deriving.Mirror
import sqala.ir.IrTable
import sqala.ir.IrUpdateSetPair
import sqala.ir.IrExpr
import sqala.ir.IrStatement
import sqala.util.NonEmptyList
import sqala.metadata.Dialect
import sqala.util.statementToString
import sqala.ir.ToAst

/**
 * A column name and update expression pair produced by `:=`.
 */
final case class UpdatePair(private[sqala] val columnName: String, private[sqala] val updateExpr: IrExpr)

/**
 * Tracks the state of an `UPDATE` builder, restricting which
 * methods are available at each stage.
 */
enum UpdateState:
    case Table
    case Entity

type UpdateTable = UpdateState.Table.type

type UpdateEntity = UpdateState.Entity.type

/**
 * Context for `:=`.
 */
final class UpdateSetContext

/**
  * Represents an `UPDATE` statement.
  */
final case class UpdateTree(
    private[sqala] val table: IrTable.Ident,
    private[sqala] val setPairs: List[IrUpdateSetPair],
    private[sqala] val where: Option[IrExpr]
):
    /**
     * Returns a copy with the given condition added to the `WHERE` clause
     * via `AND`.
     */
    def addWhere(condition: IrExpr): UpdateTree =
        copy(where = where.map(IrExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))


/**
 * An `UPDATE` statement builder, created by `update[T]`.
 */
final class Update[T, S <: UpdateState](
    private[sqala] val table: Table[T, Column, 1],
    private[sqala] val tree: UpdateTree
)(using private[sqala] val qc: QueryContext[1]):
    /**
     * Adds `SET column = value` assignments. Multiple calls
     * accumulate. The right-hand side can be a value, expression,
     * or subquery.
     *
     * {{{
     * update[User].set(u => u.name := "Alice")
     * }}}
     */
    def set(f: UpdateSetContext ?=> Table[T, Column, 1] => UpdatePair)(using
        S =:= UpdateTable
    ): Update[T, UpdateTable] =
        given UpdateSetContext = new UpdateSetContext
        val pair = f(table)
        val updateExpr = pair.updateExpr
        new Update(
            table,
            tree.copy(setPairs = (tree.setPairs :+ IrUpdateSetPair(pair.columnName, updateExpr)))
        )

    /**
     * Adds a `WHERE` clause to the `UPDATE` statement. The condition
     * must be a valid filter expression — aggregate functions, window
     * functions, and other expressions not allowed in `where` are
     * rejected at compile time.
     *
     * {{{
     * update[User].set(u => u.name := "Alice").where(_.id == 1)
     * }}}
     */
    def where[F](f: QueryContext[1] ?=> Table[T, Column, 1] => F)(using
        a: AsExpr[F, 1],
        ba: ValidateArgType[SqlBoolean, a.R, "boolean"],
        kt: KindToTuple[a.K],
        ci: CanInSimpleClause[kt.R, "where"]
    ): Update[T, UpdateTable] =
        val condition = a.asExpr(f(table)).expr
        new Update(table, tree.addWhere(condition))

    /**
     * Returns an `UPDATE` statement.
     */
    private[sqala] def toSqlStatement: IrStatement.Update =
        require(tree.setPairs.nonEmpty, "Update table must have at least one set column.")
        IrStatement.Update(
            tree.table,
            NonEmptyList(tree.setPairs.head, tree.setPairs.tail),
            tree.where
        )

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
        statementToString(ToAst().toAst(toSqlStatement), dialect, standardEscapeStrings)

object Update:
    given qc: QueryContext[1] = QueryContext()

    inline def apply[T <: Product]: Update[T, UpdateTable] =
        val metaData = TableMacro.tableMetaData[T]
        val irTable: IrTable.Ident = IrTable.Ident(
            metaData.tableName,
            Nil
        )
        val table = Table[T, Column, 1](irTable, metaData)
        val tree = UpdateTree(irTable, Nil, None)
        new Update(table, tree)

    /**
     * Creates an `UPDATE` statement from an entity object. The
     * primary key fields are used to construct the `WHERE` clause,
     * and the remaining fields become the `SET` assignments.
     *
     * When `skipNone` is `true`, fields with `None` values are
     * omitted from the update.
     */
    inline def updateByEntity[T <: Product](entity: T, skipNone: Boolean = false)(using
        p: Mirror.ProductOf[T]
    ): Update[T, UpdateEntity] =
        val metaData = TableMacro.tableMetaData[T]
        val irTable: IrTable.Ident =
            IrTable.Ident(
                metaData.tableName,
                Nil
            )
        val table = Table[T, Column, 1](irTable, metaData)
        val data: p.MirroredElemTypes = Tuple.fromProductTyped(entity)
        val irExprs = summonInline[AsSqlExprs[p.MirroredElemTypes]]
            .asSqlExprs(data)
            .map(e => IrExpr.Native(e))
        val updateMetaData = metaData.fields.toList
            .zip(metaData.columns.toList)
            .zip(irExprs)
            .map(i => (i._1._1, i._1._2, i._2))
        val updateColumns = updateMetaData
            .filterNot((f, _, _) => metaData.primaryKeyFields.contains(f))
            .filter: (_, _, expr) =>
                (expr, skipNone) match
                    case (SqlExpr.Cast(SqlExpr.NullLiteral, _), true) => false
                    case _ => true
            .map: (_, column, expr) =>
                IrUpdateSetPair(column, expr)
        val conditions = updateMetaData
            .filter((f, _, _) => metaData.primaryKeyFields.contains(f))
            .map: (_, column, expr) =>
                IrExpr.Binary(IrExpr.Native(SqlExpr.Column(None, column)), SqlBinaryOperator.Equal, expr)
        val condition =
            if conditions.isEmpty then None
            else Some(conditions.reduce((x, y) => IrExpr.Binary(x, SqlBinaryOperator.And, y)))
        val tree = UpdateTree(irTable, updateColumns, condition)
        new Update(table, tree)