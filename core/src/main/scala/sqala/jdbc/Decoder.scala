package sqala.jdbc

import java.sql.ResultSet
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.Date
import scala.compiletime.{erasedValue, summonInline}

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

        override def decode(data: ResultSet, cursor: Int): LocalDate =
            data.getTimestamp(cursor).nn.toInstant().nn.atZone(ZoneId.systemDefault()).nn.toLocalDate().nn

    given localDateTimeDecoder: Decoder[LocalDateTime] with
        override def offset: Int = 1

        override def decode(data: ResultSet, cursor: Int): LocalDateTime =
            LocalDateTime.ofInstant(data.getTimestamp(cursor).nn.toInstant(), ZoneId.systemDefault()).nn

    given optionDecoder[T](using d: Decoder[T]): Decoder[Option[T]] with
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

    inline given derived[T <: Product](using m: Mirror.ProductOf[T]): Decoder[T] =
        val decodeInstances = Decoder.summonInstances[m.MirroredElemTypes]
        
        new Decoder:
            override def offset: Int = decodeInstances.size

            override def decode(data: ResultSet, cursor: Int): T =
                var tempCursor = cursor
                val dataArray = ArrayBuffer[Any]()
                decodeInstances.foreach: instance =>
                    dataArray.addOne(instance.asInstanceOf[Decoder[Any]].decode(data, tempCursor))
                    tempCursor = tempCursor + instance.offset
                val dataTup = Tuple.fromArray(dataArray.toArray)
                m.fromProduct(dataTup)
