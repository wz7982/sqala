package sqala.jdbc

import sqala.dsl.CustomField

import java.sql.ResultSet
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror

trait JdbcDecoder[T]:
    def offset: Int

    def decode(data: ResultSet, cursor: Int): T

object JdbcDecoder:
    inline def summonInstances[T <: Tuple]: List[JdbcDecoder[?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[JdbcDecoder[t]] :: summonInstances[ts]

    given intDecoder: JdbcDecoder[Int] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): Int = data.getInt(cursor)

    given longDecoder: JdbcDecoder[Long] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): Long = data.getLong(cursor)

    given floatDecoder: JdbcDecoder[Float] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): Float = data.getFloat(cursor)

    given doubleDecoder: JdbcDecoder[Double] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): Double = data.getDouble(cursor)

    given decimalDecoder: JdbcDecoder[BigDecimal] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): BigDecimal = data.getBigDecimal(cursor)

    given booleanDecoder: JdbcDecoder[Boolean] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): Boolean = data.getBoolean(cursor)

    given stringDecoder: JdbcDecoder[String] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): String = data.getString(cursor)

    given dateDecoder: JdbcDecoder[Date] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): Date = data.getTimestamp(cursor)

    given localDateDecoder: JdbcDecoder[LocalDate] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): LocalDate =
            data.getTimestamp(cursor).toInstant().atZone(ZoneId.systemDefault()).toLocalDate()

    given localDateTimeDecoder: JdbcDecoder[LocalDateTime] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): LocalDateTime =
            LocalDateTime.ofInstant(data.getTimestamp(cursor).toInstant(), ZoneId.systemDefault())

    given optionDecoder[T](using d: JdbcDecoder[T]): JdbcDecoder[Option[T]] with
        override def offset: Int = d.offset

        override def decode(data: ResultSet, cursor: Int): Option[T] =
            val slice = for i <- (cursor until cursor + offset) yield
                data.getObject(i)
            if slice.map(_ == null).reduce((x, y) => x && y) then None else Some(d.decode(data, cursor))

    given customFieldDecoder[T, R](using c: CustomField[T, R], d: JdbcDecoder[R]): JdbcDecoder[T] with
        override def offset: Int = d.offset

        override def decode(data: ResultSet, cursor: Int): T = c.fromValue(d.decode(data, cursor))

    given tupleDecoder[H, T <: Tuple](using headDecoder: JdbcDecoder[H], tailDecoder: JdbcDecoder[T]): JdbcDecoder[H *: T] with
        override def offset: Int = headDecoder.offset

        override def decode(data: ResultSet, cursor: Int): H *: T =
            headDecoder.decode(data, cursor) *: tailDecoder.decode(data, cursor + offset)

    given emptyTupleDecoder: JdbcDecoder[EmptyTuple] with
        override def offset: Int = 0

        override def decode(data: ResultSet, cursor: Int): EmptyTuple = EmptyTuple

    given namedTupleDecoder[N <: Tuple, V <: Tuple](using d: JdbcDecoder[V]): JdbcDecoder[NamedTuple.NamedTuple[N, V]] with
        override def offset: Int = d.offset

        override def decode(data: ResultSet, cursor: Int): NamedTuple.NamedTuple[N, V] =
            NamedTuple(d.decode(data, cursor))

    private def newDecoder[T](instances: List[JdbcDecoder[?]])(using m: Mirror.ProductOf[T]): JdbcDecoder[T] =
        new JdbcDecoder[T]:
            override def offset: Int = instances.size

            override def decode(data: ResultSet, cursor: Int): T =
                var tempCursor = cursor
                val dataArray = ArrayBuffer[Any]()
                instances.foreach: instance =>
                    dataArray.addOne(instance.asInstanceOf[JdbcDecoder[Any]].decode(data, tempCursor))
                    tempCursor = tempCursor + instance.offset
                val dataTup = Tuple.fromArray(dataArray.toArray)
                m.fromProduct(dataTup)

    inline given derived[T <: Product](using m: Mirror.ProductOf[T]): JdbcDecoder[T] =
        val instances = summonInstances[m.MirroredElemTypes]
        newDecoder[T](instances)
        
        