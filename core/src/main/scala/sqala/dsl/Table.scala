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
        inline def selectDynamic(name: String)(using p: Mirror.ProductOf[T]): Column[Element[p.MirroredElemLabels, p.MirroredElemTypes, name.type]] =
            val columnMap = table.__metaData__.fieldNames.zip(table.__metaData__.columnNames).toMap
            Column(table.__aliasName__, columnMap(name))

        inline def *(using p: Mirror.ProductOf[T]): Map[p.MirroredElemTypes, Expr] =
            val items = table.__metaData__.columnNames.map(c => Column(table.__aliasName__, c))
            Tuple.fromArray(items.toArray).asInstanceOf[Map[p.MirroredElemTypes, Expr]]

    extension [T](table: Table[Option[T]])
        @targetName("selectDynamicOption")
        inline def selectDynamic(name: String)(using p: Mirror.ProductOf[T]): Column[Wrap[Element[p.MirroredElemLabels, p.MirroredElemTypes, name.type], Option]] =
            val columnMap = table.__metaData__.fieldNames.zip(table.__metaData__.columnNames).toMap
            Column(table.__aliasName__, columnMap(name))

        @targetName("allColumnsOption")
        inline def *(using p: Mirror.ProductOf[T]): Map[p.MirroredElemTypes, [i] =>> Expr[Wrap[i, Option]]] =
            val items = table.__metaData__.columnNames.map(c => Column(table.__aliasName__, c))
            Tuple.fromArray(items.toArray).asInstanceOf[Map[p.MirroredElemTypes, [i] =>> Expr[Wrap[i, Option]]]]

case class TableMetaData(
    tableName: String,
    primaryKeyFields: List[String],
    incrementField: Option[String],
    columnNames: List[String],
    fieldNames: List[String]
)
