package sqala.compiletime

import sqala.ast.expr.SqlExpr
import sqala.compiletime.macros.tableMetaDataMacro

import java.sql.ResultSet
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import scala.compiletime.*
import scala.deriving.Mirror

trait AsSqlExpr[T]:
    def asSqlExpr(x: T): SqlExpr

object AsSqlExpr:
    inline def summonInstances[T <: Tuple]: List[AsSqlExpr[?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[AsSqlExpr[t]] :: summonInstances[ts]

    given intAsSqlExpr: AsSqlExpr[Int] with
        override def asSqlExpr(x: Int): SqlExpr = SqlExpr.NumberLiteral(x)

    given longAsSqlExpr: AsSqlExpr[Long] with
        override def asSqlExpr(x: Long): SqlExpr = SqlExpr.NumberLiteral(x)

    given floatAsSqlExpr: AsSqlExpr[Float] with
        override def asSqlExpr(x: Float): SqlExpr = SqlExpr.NumberLiteral(x)

    given doubleAsSqlExpr: AsSqlExpr[Double] with
        override def asSqlExpr(x: Double): SqlExpr = SqlExpr.NumberLiteral(x)

    given decimalAsSqlExpr: AsSqlExpr[BigDecimal] with
        override def asSqlExpr(x: BigDecimal): SqlExpr = SqlExpr.NumberLiteral(x)

    given stringAsSqlExpr: AsSqlExpr[String] with
        override def asSqlExpr(x: String): SqlExpr = SqlExpr.StringLiteral(x)
    
    given dateAsSqlExpr: AsSqlExpr[Date] with
        override def asSqlExpr(x: Date): SqlExpr = SqlExpr.DateLiteral(x)

    given localDateAsSqlExpr : AsSqlExpr[LocalDate] with
        override def asSqlExpr(x: LocalDate): SqlExpr =
            SqlExpr.DateLiteral(Date.from(x.atStartOfDay(ZoneId.systemDefault()).nn.toInstant()).nn)

    given localDateTimeAsSqlExpr : AsSqlExpr[LocalDateTime] with
        override def asSqlExpr(x: LocalDateTime): SqlExpr =
            SqlExpr.DateLiteral(Date.from(x.atZone(ZoneId.systemDefault()).nn.toInstant()).nn)
    
    given booleanAsSqlExpr: AsSqlExpr[Boolean] with
        override def asSqlExpr(x: Boolean): SqlExpr = SqlExpr.BooleanLiteral(x)

    given optionAsSqlExpr[T](using a: AsSqlExpr[T]): AsSqlExpr[Option[T]] with
        override def asSqlExpr(x: Option[T]): SqlExpr = x match
            case Some(value) => a.asSqlExpr(value)
            case None => SqlExpr.Null

    given listAsSqlExpr[T](using a: AsSqlExpr[T]): AsSqlExpr[List[T]] with
        override def asSqlExpr(x: List[T]): SqlExpr =  SqlExpr.Vector(x.map(i => a.asSqlExpr(i)))

trait CustomField[T, R](using a: AsSqlExpr[R], d: Decoder[R]) extends AsSqlExpr[T], Decoder[T]:
    def toValue(x: T): R

    def fromValue(x: R): T
        
    override def asSqlExpr(x: T): SqlExpr = a.asSqlExpr(toValue(x))

    override def decode(data: ResultSet, cursor: Int): T = fromValue(d.decode(data, cursor))

    override def offset: Int = d.offset

trait Decoder[T]:
    def offset: Int

    def decode(data: ResultSet, cursor: Int): T

object Decoder:
    inline def summonInstances[T <: Tuple]: List[Decoder[?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[Decoder[t]] :: summonInstances[ts]

    given intDecoder: Decoder[Int] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): Int = data.getInt(cursor)

    given longDecoder: Decoder[Long] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): Long = data.getLong(cursor)

    given floatDecoder: Decoder[Float] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): Float = data.getFloat(cursor)

    given doubleDecoder: Decoder[Double] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): Double = data.getDouble(cursor)

    given decimalDecoder: Decoder[BigDecimal] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): BigDecimal = data.getBigDecimal(cursor).nn

    given booleanDecoder: Decoder[Boolean] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): Boolean = data.getBoolean(cursor)

    given stringDecoder: Decoder[String] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): String = data.getString(cursor).nn

    given dateDecoder: Decoder[Date] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): Date = data.getTimestamp(cursor).nn

    given localDateDecoder: Decoder[LocalDate] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): LocalDate = data.getTimestamp(cursor).nn.toInstant().nn.atZone(ZoneId.systemDefault()).nn.toLocalDate().nn

    given localDateTimeDecoder: Decoder[LocalDateTime] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): LocalDateTime = LocalDateTime.ofInstant(data.getTimestamp(cursor).nn.toInstant(), ZoneId.systemDefault()).nn

    given optionFieldDecoder[T](using d: Decoder[T]): Decoder[Option[T]] with
        override def offset: Int = d.offset
        
        override def decode(data: ResultSet, cursor: Int): Option[T] =
            val slice = for i <- (cursor until cursor + offset) yield
                data.getObject(i)
            if slice.map(_ == null).reduce((x, y) => x && y) then None else Some(d.decode(data, cursor))

    given tupleDecoder[H, T <: Tuple](using headDecoder: Decoder[H], tailDecoder: Decoder[T]): Decoder[H *: T] with
        override def offset: Int = headDecoder.offset

        override def decode(data: ResultSet, cursor: Int): H *: T =
            headDecoder.decode(data, cursor) *: tailDecoder.decode(data, cursor + offset)

    given emptyTupleDecoder: Decoder[EmptyTuple] with
        override def offset: Int = 0
        
        override def decode(data: ResultSet, cursor: Int): EmptyTuple = EmptyTuple  

trait Entity[T] extends Decoder[T]:
    def insertMeta(entity: T): (String, List[(String, SqlExpr)])

    def updateMeta(entity: T): (String, List[(String, SqlExpr)], List[(String, SqlExpr)])

    def tableMeta: TableMetaData

object Entity:
    inline given derived[T <: Product](using m: Mirror.ProductOf[T]): Entity[T] =
        val elemNames = summonNames[m.MirroredElemLabels]
        val elemInstances = AsSqlExpr.summonInstances[m.MirroredElemTypes]
        val entityInfo = elemNames.zip(elemInstances)
        val tableInfo = tableMetaDataMacro[T]
        val columnMap = elemNames.zip(tableInfo.columnNames).toMap
        val decodeInstances = Decoder.summonInstances[m.MirroredElemTypes]

        new Entity[T]:
            override def insertMeta(entity: T): (String, List[(String, SqlExpr)]) =
                val fields = entity.asInstanceOf[Product].productIterator.toList
                val columns = fields
                    .zip(entityInfo)
                    .filter((_, info) => !tableInfo.incrementKeyField.contains(info(0)))
                    .map((value, info) => (columnMap(info(0)), info(1).asInstanceOf[AsSqlExpr[Any]].asSqlExpr(value)))
                (tableInfo.tableName, columns)

            override def updateMeta(entity: T): (String, List[(String, SqlExpr)], List[(String, SqlExpr)]) =
                val fields = entity.asInstanceOf[Product].productIterator.toList
                val updateColumns = fields
                    .zip(entityInfo)
                    .filter((_, info) => !tableInfo.primaryKeyFields.contains(info(0)))
                    .map((value, info) => (columnMap(info(0)), info(1).asInstanceOf[AsSqlExpr[Any]].asSqlExpr(value)))
                val primaryKeys = fields
                    .zip(entityInfo)
                    .filter((_, info) => tableInfo.primaryKeyFields.contains(info(0)))
                    .map((value, info) => (columnMap(info(0)), info(1).asInstanceOf[AsSqlExpr[Any]].asSqlExpr(value)))
                (tableInfo.tableName, updateColumns, primaryKeys)

            override def tableMeta: TableMetaData = tableInfo

            override def offset: Int = decodeInstances.size

            override def decode(data: ResultSet, cursor: Int): T =
                var tempCursor = cursor
                val dataArray = ArrayBuffer[Any]()
                decodeInstances.foreach: instance =>
                    dataArray.addOne(instance.asInstanceOf[Decoder[Any]].decode(data, tempCursor))
                    tempCursor = tempCursor + instance.offset
                val dataTup = Tuple.fromArray(dataArray.toArray)
                m.fromProduct(dataTup)

    inline def summonNames[T <: Tuple]: List[String] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => constValue[t].asInstanceOf[String] :: summonNames[ts]