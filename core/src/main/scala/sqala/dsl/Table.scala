package sqala.dsl

import scala.annotation.targetName
import scala.deriving.Mirror
import scala.language.dynamics

case class Table[T](
    private[sqala] __tableName__ : String, 
    private[sqala] __aliasName__ : String,
    private[sqala] __metaData__ : TableMetaData,
    private[sqala] __depth__ : Int
) extends Dynamic

object Table:
    extension [T](table: Table[T])
        inline def selectDynamic(name: String)(using p: Mirror.ProductOf[T]): Column[Element[p.MirroredElemLabels, p.MirroredElemTypes, name.type, T]] =
            val columnMap = table.__metaData__.fieldNames.zip(table.__metaData__.columnNames).toMap
            Column(table.__aliasName__, columnMap(name))

    extension [T](table: Table[Option[T]])
        @targetName("selectDynamicOption")
        inline def selectDynamic(name: String)(using p: Mirror.ProductOf[T]): Column[Element[p.MirroredElemLabels, p.MirroredElemTypes, name.type, Option[T]]] =
            val columnMap = table.__metaData__.fieldNames.zip(table.__metaData__.columnNames).toMap
            Column(table.__aliasName__, columnMap(name))

case class TableMetaData(
    tableName: String,
    primaryKeyFields: List[String],
    incrementField: Option[String],
    columnNames: List[String],
    fieldNames: List[String]
)