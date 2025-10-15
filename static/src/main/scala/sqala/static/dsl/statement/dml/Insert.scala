package sqala.static.dsl.statement.dml

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.static.dsl.table.Table
import sqala.static.dsl.{AsExpr, Expr}
import sqala.static.metadata.{AsSqlExpr, TableMacro}

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
        val sqlTable: SqlTable.Ident = SqlTable.Ident(tableName, None, None, None, None)
        val table = Table[T](None, metaData, sqlTable)
        val insertItems = a.exprs(f(table))
        val columns = insertItems.map: i =>
            i match
                case Expr(SqlExpr.Column(_, c)) => SqlExpr.Column(None, c)
                case _ => throw MatchError(i)
        val tree: SqlStatement.Insert = 
            SqlStatement.Insert(sqlTable, columns, Nil, None)
        new Insert(tree)

    inline def values(rows: List[T])(using 
        S =:= InsertTable
    ): Insert[T, InsertValues] =
        val instances = AsSqlExpr.summonInstances[T]
        val insertValues = rows.map: row =>
            val data: List[Any] = inline row match
                case t: Tuple => t.toList
                case x => x :: Nil
            data.zip(instances).map: (datum, instance) =>
                instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(datum)
        new Insert(tree.copy(values = insertValues))

    inline def values(row: T)(using 
        S =:= InsertTable
    ): Insert[T, InsertValues] = 
        values(row :: Nil)

object Insert:
    inline def apply[T <: Product]: Insert[T, InsertTable] =
        val tableName = TableMacro.tableName[T]
        val tree: SqlStatement.Insert = 
            SqlStatement.Insert(SqlTable.Ident(tableName, None, None, None, None), Nil, Nil, None)
        new Insert(tree)

    inline def insertByEntities[T <: Product](entities: Seq[T])(using 
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
        new Insert(
            SqlStatement.Insert(
                SqlTable.Ident(tableName, None, None, None, None), columns, values.toList, None
            )
        )