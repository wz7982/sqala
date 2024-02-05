package sqala.dsl.statement.dml

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.dsl.*
import sqala.dsl.`macro`.{tableMetaDataMacro, tableNameMacro}
import sqala.dsl.statement.query.Query

import scala.deriving.Mirror

class Insert[T, S <: InsertState](
    private[sqala] val items: T,
    val ast: SqlStatement.Insert
):
    inline def apply[I: AsExpr](f: T => I)(using S =:= InsertState.New.type): Insert[I, InsertState.Table.type] =
        val insertItems = f(items)
        val columns = summon[AsExpr[I]].asExprs(insertItems).map:
            case Column(_, c) => SqlExpr.Column(None, c)
            case _ => SqlExpr.Null
        new Insert(insertItems, ast.copy(columns = columns))

    inline infix def values(rows: List[InverseMap[T, Expr]])(using S =:= InsertState.Table.type): Insert[T, InsertState.Values.type] =
        val instances = AsSqlExpr.summonInstances[InverseMap[T, Expr]]
        val insertValues = rows.map: row =>
            val data = inline row match
                case t: Tuple => t.toList
                case x => x :: Nil
            data.zip(instances).map: (datum, instance) =>
                instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(datum)
        new Insert(items, ast.copy(values = insertValues))

    inline infix def values(row: InverseMap[T, Expr])(using S =:= InsertState.Table.type): Insert[T, InsertState.Values.type] =
        values(row :: Nil)

    inline infix def select(query: Query[T])(using S =:= InsertState.Table.type): Insert[T, InsertState.Query.type] =
        new Insert(items, ast.copy(query = Some(query.ast)))

object Insert:
    inline def apply[T]: Insert[Table[T], InsertState.New.type] =
        val tableName = tableNameMacro[T]
        val metaData = tableMetaDataMacro[T]
        val table = Table[T](tableName, tableName, metaData, 0)
        val ast: SqlStatement.Insert = SqlStatement.Insert(SqlTable.IdentTable(tableName, None), Nil, Nil, None)
        new Insert(table, ast)

    inline def apply[T <: Product](entities: List[T])(using p: Mirror.ProductOf[T]): Insert[Table[T], InsertState.Entity.type] =
        val tableName = tableNameMacro[T]
        val metaData = tableMetaDataMacro[T]
        val table = Table[T](tableName, tableName, metaData, 0)
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
        new Insert(table, SqlStatement.Insert(SqlTable.IdentTable(tableName, None), columns, values, None))

enum InsertState:
    case New
    case Entity
    case Table
    case Values
    case Query