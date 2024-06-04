package sqala.dsl.statement.dml

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.dsl.*
import sqala.dsl.macros.{tableMetaDataMacro, tableNameMacro}
import sqala.dsl.statement.query.{NamedTupleWrapper, Query}

import scala.deriving.Mirror

class Insert[T, S <: InsertState](
    private[sqala] val items: T,
    val ast: SqlStatement.Insert
):
    inline def apply[I: AsExpr](f: T => I)(using S =:= InsertNew): Insert[I, InsertTable] =
        val insertItems = f(items)
        val columns = summon[AsExpr[I]].asExprs(insertItems).map:
            case Column(_, c) => SqlExpr.Column(None, c)
            case _ => SqlExpr.Null
        new Insert(insertItems, ast.copy(columns = columns))

    inline infix def values(rows: List[InverseMap[T, Expr]])(using S =:= InsertTable): Insert[T, InsertValues] =
        val instances = AsSqlExpr.summonInstances[InverseMap[T, Expr]]
        val insertValues = rows.map: row =>
            val data = inline row match
                case t: Tuple => t.toList
                case x => x :: Nil
            data.zip(instances).map: (datum, instance) =>
                instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(datum)
        new Insert(items, ast.copy(values = insertValues))

    inline infix def values(row: InverseMap[T, Expr])(using S =:= InsertTable): Insert[T, InsertValues] = values(row :: Nil)

    inline infix def select[N <: Tuple, V <: Tuple](query: Query[NamedTupleWrapper[N, V]])(using S =:= InsertTable, V =:= T): Insert[T, InsertQuery] =
        new Insert(items, ast.copy(query = Some(query.ast)))

object Insert:
    inline def apply[T <: Product]: Insert[Table[T], InsertNew] =
        val tableName = tableNameMacro[T]
        val metaData = tableMetaDataMacro[T]
        val table = Table[T](tableName, tableName, metaData)
        val ast: SqlStatement.Insert = SqlStatement.Insert(SqlTable.IdentTable(tableName, None), Nil, Nil, None)
        new Insert(table, ast)

    inline def apply[T <: Product](entities: List[T])(using p: Mirror.ProductOf[T]): Insert[Table[T], InsertEntity] =
        val tableName = tableNameMacro[T]
        val metaData = tableMetaDataMacro[T]
        val table = Table[T](tableName, tableName, metaData)
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