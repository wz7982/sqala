package sqala.static.statement.dml

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.static.common.*
import sqala.static.macros.*
import sqala.static.statement.query.Query

import scala.deriving.Mirror

class Insert[T, S <: InsertState](
    private[sqala] val tableName: String,
    override val ast: SqlStatement.Insert
) extends Dml(ast):
    inline def apply[I: AsExpr](inline f: T => I)(using S =:= InsertNew): Insert[I, InsertTable] =
        val insertList = ClauseMacro.fetchInsert(f, tableName :: Nil)
        new Insert(tableName, ast.copy(columns = insertList))

    inline infix def values(rows: List[T])(using S =:= InsertTable): Insert[T, InsertValues] =
        val instances = AsSqlExpr.summonInstances[T]
        val insertValues = rows.map: row =>
            val data = inline row match
                case t: Tuple => t.toList
                case x => x :: Nil
            data.zip(instances).map: (datum, instance) =>
                instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(datum)
        new Insert(tableName, ast.copy(values = insertValues))

    inline infix def values(row: T)(using S =:= InsertTable): Insert[T, InsertValues] = 
        values(row :: Nil)

    inline infix def select(query: Query[T, ?])(using S =:= InsertTable): Insert[T, InsertQuery] =
        new Insert(tableName, ast.copy(query = Some(query.ast)))

object Insert:
    inline def apply[T <: Product]: Insert[Table[T], InsertNew] =
        val tableName = TableMacro.tableName[T]
        val ast: SqlStatement.Insert = SqlStatement.Insert(SqlTable.Range(tableName, None), Nil, Nil, None)
        new Insert(tableName, ast)

    inline def apply[T <: Product](entities: List[T])(using p: Mirror.ProductOf[T]): Insert[Table[T], InsertEntity] =
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
        new Insert(tableName, SqlStatement.Insert(SqlTable.Range(tableName, None), columns, values, None))