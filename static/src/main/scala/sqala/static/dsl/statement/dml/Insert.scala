package sqala.static.dsl.statement.dml

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.metadata.{AsSqlExpr, TableMacro}
import sqala.static.dsl.{AsExpr, Expr, QueryContext, Table}
import sqala.static.dsl.statement.query.Query

import scala.deriving.Mirror

enum InsertState:
    case New
    case Entity
    case Table
    case Values
    case Query

type InsertEntity = InsertState.Entity.type

type InsertTable = InsertState.Table.type

type InsertValues = InsertState.Values.type

type InsertQuery = InsertState.Query.type

class Insert[T, S <: InsertState](
    val tree: SqlStatement.Insert
):
    inline def apply[I: AsExpr as a](f: Table[T] => I): Insert[a.R, InsertTable] =
        val tableName = TableMacro.tableName[T]
        val metaData = TableMacro.tableMetaData[T]
        val table = Table[T](tableName, tableName, metaData)
        val insertItems = a.exprs(f(table))
        val columns = insertItems.map: i =>
            i match
                case Expr(SqlExpr.Column(_, c)) => SqlExpr.Column(None, c)
                case _ => throw MatchError(i)
        val tree: SqlStatement.Insert = 
            SqlStatement.Insert(SqlTable.Range(tableName, None), columns, Nil, None)
        new Insert(tree)

    inline infix def values(rows: List[T])(using 
        S =:= InsertTable
    ): Insert[T, InsertValues] =
        val instances = AsSqlExpr.summonInstances[T]
        val insertValues = rows.map: row =>
            val data = inline row match
                case t: Tuple => t.toList
                case x => x :: Nil
            data.zip(instances).map: (datum, instance) =>
                instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(datum)
        new Insert(tree.copy(values = insertValues))

    inline infix def values(row: T)(using 
        S =:= InsertTable
    ): Insert[T, InsertValues] = 
        values(row :: Nil)

    infix def select[V <: Tuple](query: QueryContext ?=> Query[T])(using 
        S =:= InsertTable, V =:= T
    ): Insert[T, InsertQuery] =
        given QueryContext = QueryContext(0)
        new Insert(tree.copy(query = Some(query.tree)))

object Insert:
    inline def apply[T <: Product]: Insert[T, InsertTable] =
        val tableName = TableMacro.tableName[T]
        val tree: SqlStatement.Insert = 
            SqlStatement.Insert(SqlTable.Range(tableName, None), Nil, Nil, None)
        new Insert(tree)

    inline def apply[T <: Product](entities: List[T])(using 
        p: Mirror.ProductOf[T]
    ): Insert[T, InsertEntity] =
        val tableName = TableMacro.tableName[T]
        val metaData = TableMacro.tableMetaData[T]
        val columns = metaData.columnNames
            .zip(metaData.fieldNames)
            .filterNot((_, field) => metaData.incrementField.contains(field))
            .map((c, _) => SqlExpr.Column(None, c))
        val instances = AsSqlExpr.summonInstances[p.MirroredElemTypes]
        val values = entities.map: entity =>
            val data = entity.productIterator.toList
                .zip(instances)
                .zip(metaData.fieldNames)
                .filterNot((_, field) => metaData.incrementField.contains(field))
                .map(_._1)
            data.map: (datum, instance) =>
                instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(datum)
        new Insert(SqlStatement.Insert(SqlTable.Range(tableName, None), columns, values, None))