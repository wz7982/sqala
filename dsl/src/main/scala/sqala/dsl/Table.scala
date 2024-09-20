package sqala.dsl

import scala.NamedTuple.NamedTuple
import scala.annotation.nowarn

case class Table[T](
    private[sqala] __tableName__ : String, 
    private[sqala] __aliasName__ : String,
    private[sqala] __metaData__ : TableMetaData
) extends Selectable:
    def selectDynamic(name: String): Expr[?, ?] =
        val columnMap = __metaData__.fieldNames.zip(__metaData__.columnNames).toMap
        Expr.Column(__aliasName__, columnMap(name))
    
object Table:
    extension [T](table: Table[T])(using t: NamedTable[T])
        transparent inline def * : t.Fields =
            val columns = table.__metaData__.columnNames
                .map(n => Expr.Column(table.__aliasName__, n))
            val columnTuple = Tuple.fromArray(columns.toArray)
            NamedTuple(columnTuple).asInstanceOf[t.Fields]

trait NamedTable[T]:
    type Fields

object NamedTable:
    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given table[T]: NamedTable[T] =
        new NamedTable[T]:
            type Fields =
                NamedTuple[
                    NamedTuple.Names[NamedTuple.From[Unwrap[T, Option]]],
                    Tuple.Map[
                        NamedTuple.DropNames[NamedTuple.From[Unwrap[T, Option]]],
                        [t] =>> MapField[t, T]
                    ]
                ]