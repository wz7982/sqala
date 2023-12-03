package sqala.compiletime.statement.dml

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.compiletime.*
import sqala.compiletime.statement.query.Query

class Insert[T <: Tuple, State <: InsertState](val ast: SqlStatement.Insert) extends Dml:
    inline infix def values(rows: List[T])(using State =:= InsertState.InsertTable): Insert[T, InsertState.Values] =
        val instances = AsSqlExpr.summonInstances[T]
        val insertValues = rows.map: row =>
            row.toList.zip(instances).map: (datum, instance) =>
                instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(datum)
        new Insert(ast.copy(values = insertValues))

    inline infix def values(row: T)(using State =:= InsertState.InsertTable): Insert[T, InsertState.Values] =
        values(row :: Nil)

    infix def select(q: Query[T, ?]): Insert[T, InsertState.Query] =
        new Insert(ast.copy(query = Some(q.ast)))

object Insert:
    def apply[T <: Tuple](insertColumns: (Table[?, ?], T)): Insert[Tuple.InverseMap[T, [t] =>> Expr[t, ?]], InsertState.InsertTable] =
        val columns = insertColumns._2.toList.map:
            case Column(_, columnName, _) => SqlExpr.Column(None, columnName)
            case PrimaryKey(_, columnName, _) => SqlExpr.Column(None, columnName)
        new Insert(SqlStatement.Insert(insertColumns._1.toSqlTable, columns, Nil, None))   

    def apply[T <: Product](entities: List[T])(using e: Entity[T]): Insert[Tuple1[T], InsertState.InsertEntity] =
        val insertMetaData = e.insertMeta(entities.head)
        val tableName = insertMetaData._1
        val columns = insertMetaData._2.map(c => SqlExpr.Column(None, c._1))
        val values = entities.map: entity =>
            e.insertMeta(entity)._2.map(_._2)
        new Insert(SqlStatement.Insert(SqlTable.IdentTable(tableName, None), columns, values, None))

enum InsertState:
    case InsertEntity()
    case InsertTable()
    case Values()
    case Query()