package sqala.static.dsl.statement.dml

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.{SqlInsertMode, SqlQuery, SqlStatement}
import sqala.ast.table.SqlTable
import sqala.metadata.{AsSqlExpr, TableMacro}
import sqala.static.dsl.{AsExpr, Column, Expr}
import sqala.static.dsl.statement.query.Query
import sqala.static.dsl.table.Table
import sqala.util.NonEmptyList.toNonEmptyList

import scala.deriving.Mirror
import sqala.static.dsl.Result

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
final case class InsertTree(
    private[sqala] val table: SqlTable.Ident, 
    private[sqala] val columns: List[String],
    private[sqala] val values: List[List[SqlExpr]],
    private[sqala] val query: Option[SqlQuery]
)

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
    inline def apply[I](f: Table[T, Column, 1] => I)(using a: AsExpr[I, 1]): Insert[a.R, InsertTable] =
        val tableName = TableMacro.tableName[T]
        val metaData = TableMacro.tableMetaData[T]
        val sqlTable: SqlTable.Ident = SqlTable.Ident(tableName, None, None, None, None)
        val table = Table[T, Column, 1](None, metaData, sqlTable)
        val insertItems = a.asExprs(f(table))
        val columns = insertItems.map: i =>
            i match
                case Expr(SqlExpr.Column(_, c)) => c
                case _ => throw MatchError(i)
        val tree =
            InsertTree(sqlTable, columns, Nil, None)
        new Insert(tree)

    /**
     * Provides multiple rows of values for the `INSERT`. Maps to
     * `VALUES (...), (...)`.
     *
     * {{{
     * insert[User](u => (u.id, u.name)).values(List((1, "Alice"), (2, "Bob")))
     * }}}
     */
    inline def values(rows: Seq[T])(using
        S =:= InsertTable
    ): Insert[T, InsertValues] =
        val instances = AsSqlExpr.summonInstances[T]
        val insertValues = rows.toList.map: row =>
            val data: List[Any] = inline row match
                case t: Tuple => t.toList
                case x => x :: Nil
            data.zip(instances).map: (datum, instance) =>
                instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(datum)
        new Insert(tree.copy(values = insertValues))

    /**
     * Provides a single row of values for the `INSERT`. Maps to
     * `VALUES (...)`.
     *
     * {{{
     * insert[User](u => (u.id, u.name)).values((1, "Alice"))
     * }}}
     */
    inline def values(row: T)(using
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
        new Insert(tree.copy(query = Some(query.tree)))

    /**
     * Returns an `INSERT` statement.
     */
    private[sqala] def toSqlStatement: SqlStatement.Insert =
        val insertData =
            if tree.query.isDefined then
                SqlInsertMode.Subquery(tree.query.get)
            else
                SqlInsertMode.Values(tree.values.map(_.toNonEmptyList).toNonEmptyList)

        SqlStatement.Insert(
            tree.table, 
            tree.columns, 
            insertData
        )

object Insert:
    inline def apply[T <: Product]: Insert[T, InsertTable] =
        val tableName = TableMacro.tableName[T]
        val tree =
            InsertTree(
                SqlTable.Ident(tableName, None, None, None, None), Nil, Nil, None
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
        val columns = metaData.columnNames
            .zip(metaData.fieldNames)
            .filterNot((_, field) => metaData.incrementField.contains(field))
            .map((c, _) => c)
        val instances = AsSqlExpr.summonInstances[p.MirroredElemTypes]
        val values = entities.map: entity =>
            val data = entity.productIterator.toList
                .zip(instances)
                .zip(metaData.fieldNames)
                .filterNot((_, field) => metaData.incrementField.contains(field))
                .map(_._1)
            data.map: (datum, instance) =>
                instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(datum)
        new Insert(
            InsertTree(
                SqlTable.Ident(tableName, None, None, None, None), 
                columns, 
                values.toList,
                None
            )
        )