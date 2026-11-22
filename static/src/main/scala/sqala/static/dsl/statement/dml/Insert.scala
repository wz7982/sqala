package sqala.static.dsl.statement.dml

import sqala.dialect.Dialect
import sqala.ir.*
import sqala.metadata.{AsSqlExprs, TableMacro}
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.Query
import sqala.static.dsl.table.Table
import sqala.util.{NonEmptyList, statementToString}

import scala.compiletime.summonInline
import scala.deriving.Mirror

/**
 * Tracks the state of an `INSERT` builder, restricting which
 * methods are available at each stage.
 */
enum InsertState:
    case Entity
    case Table
    case Values
    case Query

type InsertEntity = InsertState.Entity.type

type InsertTable = InsertState.Table.type

type InsertValues = InsertState.Values.type

type InsertQuery = InsertState.Query.type

/**
 * Represents an `INSERT` statement.
 */
case class InsertTree(
    private[sqala] val table: IrTable.Ident,
    private[sqala] val columns: List[String],
    private[sqala] val insert: List[List[IrExpr]] | IrQuery
)

/**
 * Restricts the target of `insert[T]` to columns.
 *
 * Only a `ColumnExpr` or a tuple of `ColumnExpr`s is accepted. Values,
 * computed expressions, aggregates, and subqueries are rejected at compile
 * time, so an invalid insert column list never reaches the printer.
 */
trait AsInsertColumns[T]:
    /**
     * The tuple type of the selected columns' values.
     */
    type R

    /**
     * Returns the names of the selected columns, in selection order.
     */
    def columnNames(x: T): List[String]

object AsInsertColumns:
    type Aux[T, O] = AsInsertColumns[T]:
        type R = O

    given column[T, L <: Int]: Aux[ColumnExpr[T, L], T] =
        new AsInsertColumns[ColumnExpr[T, L]]:
            type R = T

            def columnNames(x: ColumnExpr[T, L]): List[String] =
                List(x.expr.name)

    given tuple[H, T <: Tuple](using
        ah: AsInsertColumns[H],
        at: AsInsertColumns[T],
        tt: ToTuple[at.R]
    ): Aux[H *: T, ah.R *: tt.R] =
        new AsInsertColumns[H *: T]:
            type R = ah.R *: tt.R

            def columnNames(x: H *: T): List[String] =
                ah.columnNames(x.head) ++ at.columnNames(x.tail)

    given tuple1[H](using ah: AsInsertColumns[H]): Aux[H *: EmptyTuple, ah.R] =
        new AsInsertColumns[H *: EmptyTuple]:
            type R = ah.R

            def columnNames(x: H *: EmptyTuple): List[String] =
                ah.columnNames(x.head)

/**
 * An `INSERT` statement builder, created by `insert[T]`.
 */
final class Insert[T, S <: InsertState](
    private[sqala] val tree: InsertTree
):
    /**
     * Selects columns to insert.
     *
     * {{{
     * insert[User](u => (u.id, u.name))
     * }}}
     */
    inline def apply[I](f: Table[T, Column, 1] => I)(using a: AsInsertColumns[I]): Insert[a.R, InsertTable] =
        val tableName = TableMacro.tableName[T]
        val metaData = TableMacro.tableMetaData[T]
        val irTable: IrTable.Ident = IrTable.Ident(tableName, Nil)
        val table = Table[T, Column, 1](irTable, metaData)
        val insertItems = a.columnNames(f(table))
        val tree = InsertTree(irTable, insertItems, Nil)
        new Insert(tree)

    /**
     * Provides multiple rows of values for the `INSERT`. Maps to
     * `VALUES (...), (...)`.
     *
     * {{{
     * insert[User](u => (u.id, u.name)).values(List((1, "Alice"), (2, "Bob")))
     * }}}
     */
    def values(rows: Seq[T])(using
        a: AsSqlExprs[T],
        refl: S =:= InsertTable
    ): Insert[T, InsertValues] =
        val insertValues = rows.toList.map: row =>
            a.asSqlExprs(row, false).map(e => IrExpr.Native(e))
        new Insert(tree.copy(insert = insertValues))

    /**
     * Provides a single row of values for the `INSERT`. Maps to
     * `VALUES (...)`.
     *
     * {{{
     * insert[User](u => (u.id, u.name)).values((1, "Alice"))
     * }}}
     */
    inline def values(row: T)(using
        AsSqlExprs[T],
        S =:= InsertTable
    ): Insert[T, InsertValues] =
        values(row :: Nil)

    /**
      * Provides a query for the `INSERT`. Maps to
      * `(SELECT ...)`
      *
      * {{{
     * insert[User](u => (u.id, u.name)).query(from(User).filter(u => u.id == 1).map(u => (u.id, u.name)))
     * }}}
      */
    def query[QT](query: Query[QT, ?, ?, ?])(using
        r: Result[QT],
        rs: r.R =:= T,
        refl: S =:= InsertTable
    ): Insert[T, InsertQuery] =
        new Insert(tree.copy(insert = query.tree))

    /**
     * Returns an `INSERT` statement.
     */
    private[sqala] def toSqlStatement: IrStatement.Insert =
        val insertData =
            tree.insert match
                case values: List[List[IrExpr]] =>
                    require(values.nonEmpty, "Insert table must have at least one row.")
                    val rows = values.map: row =>
                        require(row.nonEmpty, "Insert table must have at least one insert column.")
                        NonEmptyList(row.head, row.tail)
                    IrInsertMode.Values(NonEmptyList(rows.head, rows.tail))
                case query: IrQuery =>
                    IrInsertMode.Subquery(query)
        IrStatement.Insert(
            tree.table,
            tree.columns,
            insertData
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

object Insert:
    inline def apply[T <: Product]: Insert[T, InsertTable] =
        val tableName = TableMacro.tableName[T]
        val tree =
            InsertTree(
                IrTable.Ident(tableName, Nil), Nil, Nil
            )
        new Insert(tree)

    /**
     * Creates an `INSERT` statement from entity objects, excluding
     * auto-increment columns.
     */
    inline def insertByEntities[T <: Product](entities: Seq[T])(using
        p: Mirror.ProductOf[T]
    ): Insert[T, InsertEntity] =
        val tableName = TableMacro.tableName[T]
        val metaData = TableMacro.tableMetaData[T]
        val columns = metaData.columns.toList
            .zip(metaData.fields.toList)
            .filterNot((_, field) => metaData.incrementField.contains(field))
            .map((c, _) => c)
        val values = entities.map: entity =>
            val data: p.MirroredElemTypes = Tuple.fromProductTyped(entity)
            val irExprs = summonInline[AsSqlExprs[p.MirroredElemTypes]]
                .asSqlExprs(data, false)
                .map(e => IrExpr.Native(e))
            metaData.fields.toList.zip(irExprs)
                .filterNot((field, _) => metaData.incrementField.contains(field))
                .map((_, expr) => expr)
        new Insert(
            InsertTree(
                IrTable.Ident(tableName, Nil),
                columns,
                values.toList
            )
        )