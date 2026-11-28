package sqala.jdbc

import sqala.metadata.Json

import java.sql.{PreparedStatement, Types}
import java.time.*
import scala.annotation.{implicitAmbiguous, implicitNotFound}

/**
 * Type class for encoding a Scala value as a JDBC parameter.
 *
 * An encoder is selected at compile time from the static type of the
 * value, so binding does not need to inspect the value at runtime.
 */
@implicitAmbiguous("This value cannot be converted to a JDBC parameter.")
@implicitNotFound("This value cannot be converted to a JDBC parameter.")
trait JdbcEncoder[T]:
    /**
     * Encodes `value` into the `index`‑th parameter of `stmt`.
     */
    def encode(stmt: PreparedStatement, index: Int, value: T): Unit

object JdbcEncoder:
    given int: JdbcEncoder[Int] with
        inline def encode(stmt: PreparedStatement, index: Int, value: Int): Unit =
            stmt.setInt(index, value)

    given long: JdbcEncoder[Long] with
        inline def encode(stmt: PreparedStatement, index: Int, value: Long): Unit =
            stmt.setLong(index, value)

    given float: JdbcEncoder[Float] with
        inline def encode(stmt: PreparedStatement, index: Int, value: Float): Unit =
            stmt.setFloat(index, value)

    given double: JdbcEncoder[Double] with
        inline def encode(stmt: PreparedStatement, index: Int, value: Double): Unit =
            stmt.setDouble(index, value)

    given bigDecimal: JdbcEncoder[BigDecimal] with
        inline def encode(stmt: PreparedStatement, index: Int, value: BigDecimal): Unit =
            stmt.setBigDecimal(index, value.bigDecimal)

    given boolean: JdbcEncoder[Boolean] with
        inline def encode(stmt: PreparedStatement, index: Int, value: Boolean): Unit =
            stmt.setBoolean(index, value)

    given string: JdbcEncoder[String] with
        inline def encode(stmt: PreparedStatement, index: Int, value: String): Unit =
            stmt.setString(index, value)

    given localDate: JdbcEncoder[LocalDate] with
        inline def encode(stmt: PreparedStatement, index: Int, value: LocalDate): Unit =
            stmt.setObject(index, value)

    given localDateTime: JdbcEncoder[LocalDateTime] with
        inline def encode(stmt: PreparedStatement, index: Int, value: LocalDateTime): Unit =
            stmt.setObject(index, value)

    given localTime: JdbcEncoder[LocalTime] with
        inline def encode(stmt: PreparedStatement, index: Int, value: LocalTime): Unit =
            stmt.setObject(index, value)

    given offsetDateTime: JdbcEncoder[OffsetDateTime] with
        inline def encode(stmt: PreparedStatement, index: Int, value: OffsetDateTime): Unit =
            stmt.setObject(index, value)

    given offsetTime: JdbcEncoder[OffsetTime] with
        inline def encode(stmt: PreparedStatement, index: Int, value: OffsetTime): Unit =
            stmt.setObject(index, value)

    given option[T](using e: JdbcEncoder[T]): JdbcEncoder[Option[T]] with
        inline def encode(stmt: PreparedStatement, index: Int, value: Option[T]): Unit =
            value match
                case Some(v) =>
                    e.encode(stmt, index, v)
                case None =>
                    stmt.setNull(index, Types.NULL)

    given json: JdbcEncoder[Json] with
        inline def encode(stmt: PreparedStatement, index: Int, value: Json): Unit =
            stmt.setString(index, value.value)

    given array[T](using e: JdbcArrayItemEncoder[T]): JdbcEncoder[Array[T]] with
        inline def encode(stmt: PreparedStatement, index: Int, value: Array[T]): Unit =
            val elements = value.map(item => e.toJdbcValue(item).asInstanceOf[AnyRef])
            val array = stmt.getConnection.createArrayOf(e.typeName, elements)
            stmt.setArray(index, array)

/**
 * Encodes the elements of a SQL array parameter.
 *
 * The type name is passed to `Connection.createArrayOf`, so it should be the
 * database's name for the element type. Support for JDBC arrays depends on
 * the driver; databases without a native array type cannot bind `Array[T]`.
 */
@implicitAmbiguous("This value cannot be converted to a JDBC array element.")
@implicitNotFound("This value cannot be converted to a JDBC array element.")
trait JdbcArrayItemEncoder[T]:
    /**
     * The database type name of one array element.
     */
    def typeName: String

    /**
     * Converts an element to the value expected by `createArrayOf`.
     */
    def toJdbcValue(value: T): Any

object JdbcArrayItemEncoder:
    given int: JdbcArrayItemEncoder[Int] with
        def typeName: String =
            "INTEGER"

        def toJdbcValue(value: Int): Any =
            value

    given long: JdbcArrayItemEncoder[Long] with
        def typeName: String =
            "BIGINT"

        def toJdbcValue(value: Long): Any =
            value

    given float: JdbcArrayItemEncoder[Float] with
        def typeName: String =
            "REAL"

        def toJdbcValue(value: Float): Any =
            value

    given double: JdbcArrayItemEncoder[Double] with
        def typeName: String =
            "DOUBLE PRECISION"

        def toJdbcValue(value: Double): Any =
            value

    given bigDecimal: JdbcArrayItemEncoder[BigDecimal] with
        def typeName: String =
            "DECIMAL"

        def toJdbcValue(value: BigDecimal): Any =
            value.bigDecimal

    given boolean: JdbcArrayItemEncoder[Boolean] with
        def typeName: String =
            "BOOLEAN"

        def toJdbcValue(value: Boolean): Any =
            value

    given string: JdbcArrayItemEncoder[String] with
        def typeName: String =
            "VARCHAR"

        def toJdbcValue(value: String): Any =
            value

    given localDate: JdbcArrayItemEncoder[LocalDate] with
        def typeName: String =
            "DATE"

        def toJdbcValue(value: LocalDate): Any =
            value

    given localDateTime: JdbcArrayItemEncoder[LocalDateTime] with
        def typeName: String =
            "TIMESTAMP"

        def toJdbcValue(value: LocalDateTime): Any =
            value

    given localTime: JdbcArrayItemEncoder[LocalTime] with
        def typeName: String =
            "TIME"

        def toJdbcValue(value: LocalTime): Any =
            value

    given offsetDateTime: JdbcArrayItemEncoder[OffsetDateTime] with
        def typeName: String =
            "TIMESTAMP WITH TIME ZONE"

        def toJdbcValue(value: OffsetDateTime): Any =
            value

    given offsetTime: JdbcArrayItemEncoder[OffsetTime] with
        def typeName: String =
            "TIME WITH TIME ZONE"

        def toJdbcValue(value: OffsetTime): Any =
            value

    given json: JdbcArrayItemEncoder[Json] with
        def typeName: String =
            "JSON"

        def toJdbcValue(value: Json): Any =
            value.toString

    given option[T](using e: JdbcArrayItemEncoder[T]): JdbcArrayItemEncoder[Option[T]] with
        def typeName: String =
            e.typeName

        def toJdbcValue(value: Option[T]): Any =
            value match
                case Some(v) =>
                    e.toJdbcValue(v)
                case None =>
                    null