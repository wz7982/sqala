package sqala.static.dsl.statement.dml

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.statement.{SqlStatement, SqlUpdateSetPair}
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.metadata.{AsSqlExprs, SqlBoolean, TableMacro}
import sqala.static.dsl.*
import sqala.static.dsl.table.Table
import sqala.util.NonEmptyList.toNonEmptyList

import scala.compiletime.summonInline
import scala.deriving.Mirror

/**
 * A column name and update expression pair produced by `:=`.
 */
final case class UpdatePair(private[sqala] val columnName: String, private[sqala] val updateExpr: SqlExpr)

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
    private[sqala] val table: SqlTable.Ident,
    private[sqala] val setPairs: List[SqlUpdateSetPair],
    private[sqala] val where: Option[SqlExpr]
):
    /**
     * Returns a copy with the given condition added to the `WHERE` clause
     * via `AND`.
     */
    def addWhere(condition: SqlExpr): UpdateTree =
        copy(where = where.map(SqlExpr.Binary(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))


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
            tree.copy(setPairs = (tree.setPairs :+ SqlUpdateSetPair(pair.columnName, updateExpr)))
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
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        refl: S =:= UpdateTable,
    ): Update[T, UpdateTable] =
        val condition = a.asExpr(f(table))
        new Update(table, tree.addWhere(condition.asSqlExpr))

    /**
     * Returns an `UPDATE` statement.
     */
    private[sqala] def toSqlStatement: SqlStatement.Update =
        SqlStatement.Update(
            tree.table,
            tree.setPairs.toNonEmptyList,
            tree.where
        )

object Update:
    given qc: QueryContext[1] = QueryContext(TableIndexRef(0))

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
        val table = Table[T, Column, 1](alias, metaData)
        val tree = UpdateTree(sqlTable, Nil, None)
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
        val sqlTable: SqlTable.Ident =
            SqlTable.Ident(
                metaData.tableName,
                None,
                None,
                None,
                None
            )
        val table = Table[T, Column, 1](metaData.tableName, metaData)
        val data: p.MirroredElemTypes = Tuple.fromProductTyped(entity)
        val sqlExprs = summonInline[AsSqlExprs[p.MirroredElemTypes]].asSqlExprs(data)
        val updateMetaData = metaData.fieldNames
            .zip(metaData.columnNames)
            .zip(sqlExprs)
            .map(i => (i._1._1, i._1._2, i._2))
        val updateColumns = updateMetaData
            .filterNot((f, _, _) => metaData.primaryKeyFields.contains(f))
            .filter: (_, _, expr) =>
                (expr, skipNone) match
                    case (SqlExpr.Cast(SqlExpr.NullLiteral, _), true) => false
                    case _ => true
            .map: (_, column, expr) =>
                SqlUpdateSetPair(column, expr)
        val conditions = updateMetaData
            .filter((f, _, _) => metaData.primaryKeyFields.contains(f))
            .map: (_, column, expr) =>
                SqlExpr.Binary(SqlExpr.Column(None, column), SqlBinaryOperator.Equal, expr)
        val condition =
            if conditions.isEmpty then None
            else Some(conditions.reduce((x, y) => SqlExpr.Binary(x, SqlBinaryOperator.And, y)))
        val tree = UpdateTree(sqlTable, updateColumns, condition)
        new Update(table, tree)