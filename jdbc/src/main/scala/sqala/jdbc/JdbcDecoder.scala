package sqala.jdbc

import sqala.metadata.{CustomField, Json}

import java.sql.ResultSet
import java.time.*
import scala.NamedTuple.NamedTuple
import scala.deriving.Mirror
import scala.quoted.{Expr, Quotes, Type}
import scala.reflect.ClassTag

/**
 * Type class for decoding a `ResultSet` row into a typed value.
 * Each instance declares how many columns it consumes (`offset`)
 * and how to decode the value starting from a cursor position.
 */
trait JdbcDecoder[T]:
    /**
     * The number of columns consumed by this decoder.
     */
    def offset: Int

    /**
     * Decodes the value from the `ResultSet` starting at `cursor`
     * (1‑based column index).
     */
    def decode(data: ResultSet, cursor: Int): T

    /**
     * Decodes the value as an `Option` without building an intermediate
     * collection. Scalar decoders override this with a typed getter and
     * `ResultSet.wasNull`, which avoids fetching the same column twice.
     */
    def decodeOption(data: ResultSet, cursor: Int): Option[T] =
        var index = cursor
        val end = cursor + offset
        var nonNull = false
        while index < end && !nonNull do
            data.getObject(index)
            if !data.wasNull() then nonNull = true
            index += 1
        if nonNull then Some(decode(data, cursor)) else None

object JdbcDecoder:
    given int: JdbcDecoder[Int] with
        inline def offset: Int =
            1

        inline def decode(data: ResultSet, cursor: Int): Int =
            data.getInt(cursor)

        override inline def decodeOption(data: ResultSet, cursor: Int): Option[Int] =
            val value = data.getInt(cursor)
            if data.wasNull() then None else Some(value)

    given long: JdbcDecoder[Long] with
        inline def offset: Int =
            1

        inline def decode(data: ResultSet, cursor: Int): Long =
            data.getLong(cursor)

        override inline def decodeOption(data: ResultSet, cursor: Int): Option[Long] =
            val value = data.getLong(cursor)
            if data.wasNull() then None else Some(value)

    given float: JdbcDecoder[Float] with
        inline def offset: Int =
            1

        inline def decode(data: ResultSet, cursor: Int): Float =
            data.getFloat(cursor)

        override inline def decodeOption(data: ResultSet, cursor: Int): Option[Float] =
            val value = data.getFloat(cursor)
            if data.wasNull() then None else Some(value)

    given double: JdbcDecoder[Double] with
        inline def offset: Int =
            1

        inline def decode(data: ResultSet, cursor: Int): Double =
            data.getDouble(cursor)

        override inline def decodeOption(data: ResultSet, cursor: Int): Option[Double] =
            val value = data.getDouble(cursor)
            if data.wasNull() then None else Some(value)

    given decimal: JdbcDecoder[BigDecimal] with
        inline def offset: Int =
            1

        inline def decode(data: ResultSet, cursor: Int): BigDecimal =
            data.getBigDecimal(cursor)

        override inline def decodeOption(data: ResultSet, cursor: Int): Option[BigDecimal] =
            val value = data.getBigDecimal(cursor)
            if data.wasNull() then None else Some(BigDecimal(value))

    given boolean: JdbcDecoder[Boolean] with
        inline def offset: Int =
            1

        inline def decode(data: ResultSet, cursor: Int): Boolean =
            data.getBoolean(cursor)

        override inline def decodeOption(data: ResultSet, cursor: Int): Option[Boolean] =
            val value = data.getBoolean(cursor)
            if data.wasNull() then None else Some(value)

    given string: JdbcDecoder[String] with
        inline def offset: Int =
            1

        inline def decode(data: ResultSet, cursor: Int): String =
            data.getString(cursor)

        override inline def decodeOption(data: ResultSet, cursor: Int): Option[String] =
            val value = data.getString(cursor)
            if data.wasNull() then None else Some(value)

    given localDate: JdbcDecoder[LocalDate] with
        inline def offset: Int =
            1

        inline def decode(data: ResultSet, cursor: Int): LocalDate =
            data.getObject(cursor, classOf[LocalDate])

        override inline def decodeOption(data: ResultSet, cursor: Int): Option[LocalDate] =
            val value = data.getObject(cursor, classOf[LocalDate])
            if data.wasNull() then None else Some(value)

    given localDateTime: JdbcDecoder[LocalDateTime] with
        inline def offset: Int =
            1

        inline def decode(data: ResultSet, cursor: Int): LocalDateTime =
            data.getObject(cursor, classOf[LocalDateTime])

        override inline def decodeOption(data: ResultSet, cursor: Int): Option[LocalDateTime] =
            val value = data.getObject(cursor, classOf[LocalDateTime])
            if data.wasNull() then None else Some(value)

    given localTime: JdbcDecoder[LocalTime] with
        inline def offset: Int =
            1

        inline def decode(data: ResultSet, cursor: Int): LocalTime =
            data.getObject(cursor, classOf[LocalTime])

        override inline def decodeOption(data: ResultSet, cursor: Int): Option[LocalTime] =
            val value = data.getObject(cursor, classOf[LocalTime])
            if data.wasNull() then None else Some(value)

    given offsetDateTime: JdbcDecoder[OffsetDateTime] with
        inline def offset: Int =
            1

        inline def decode(data: ResultSet, cursor: Int): OffsetDateTime =
            data.getObject(cursor, classOf[OffsetDateTime])

        override inline def decodeOption(data: ResultSet, cursor: Int): Option[OffsetDateTime] =
            val value = data.getObject(cursor, classOf[OffsetDateTime])
            if data.wasNull() then None else Some(value)

    given offsetTime: JdbcDecoder[OffsetTime] with
        inline def offset: Int =
            1

        inline def decode(data: ResultSet, cursor: Int): OffsetTime =
            data.getObject(cursor, classOf[OffsetTime])

        override inline def decodeOption(data: ResultSet, cursor: Int): Option[OffsetTime] =
            val value = data.getObject(cursor, classOf[OffsetTime])
            if data.wasNull() then None else Some(value)

    given json: JdbcDecoder[Json] with
        inline def offset: Int =
            1

        inline def decode(data: ResultSet, cursor: Int): Json =
            Json(data.getString(cursor))

        override inline def decodeOption(data: ResultSet, cursor: Int): Option[Json] =
            val value = data.getString(cursor)
            if data.wasNull() then None else Some(Json(value))

    given option[T](using d: JdbcDecoder[T]): JdbcDecoder[Option[T]] with
        inline def offset: Int =
            d.offset

        inline def decode(data: ResultSet, cursor: Int): Option[T] =
            d.decodeOption(data, cursor)

    given array[T](using d: JdbcArrayItemDecoder[T], c: ClassTag[T]): JdbcDecoder[Array[T]] with
        inline def offset: Int =
            1

        inline def decode(data: ResultSet, cursor: Int): Array[T] =
            data
                .getArray(cursor)
                .getArray
                .asInstanceOf[Array[?]]
                .map(i => d.decode(i))

        override inline def decodeOption(data: ResultSet, cursor: Int): Option[Array[T]] =
            val value = data.getArray(cursor)
            if data.wasNull() then None
            else Some(value.getArray.asInstanceOf[Array[?]].map(i => d.decode(i)))

    given customField[T, R](using c: CustomField[T, R], d: JdbcDecoder[R]): JdbcDecoder[T] with
        inline def offset: Int =
            d.offset

        inline def decode(data: ResultSet, cursor: Int): T =
            c.fromValue(d.decode(data, cursor))

        override inline def decodeOption(data: ResultSet, cursor: Int): Option[T] =
            d.decodeOption(data, cursor).map(c.fromValue)

    given tuple[H, T <: Tuple](using headDecoder: JdbcDecoder[H], tailDecoder: JdbcDecoder[T]): JdbcDecoder[H *: T] with
        inline def offset: Int =
            headDecoder.offset + tailDecoder.offset

        inline def decode(data: ResultSet, cursor: Int): H *: T =
            headDecoder.decode(data, cursor) *: tailDecoder.decode(data, cursor + headDecoder.offset)

    given tuple1[H](using headDecoder: JdbcDecoder[H]): JdbcDecoder[H *: EmptyTuple] with
        inline def offset: Int =
            headDecoder.offset

        inline def decode(data: ResultSet, cursor: Int): H *: EmptyTuple =
            headDecoder.decode(data, cursor) *: EmptyTuple

    given namedTuple[N <: Tuple, V <: Tuple](using d: JdbcDecoder[V]): JdbcDecoder[NamedTuple[N, V]] with
        inline def offset: Int =
            d.offset

        inline def decode(data: ResultSet, cursor: Int): NamedTuple[N, V] =
            NamedTuple(d.decode(data, cursor))

    inline given derived[T <: Product](using m: Mirror.ProductOf[T]): JdbcDecoder[T] =
        ${ productDecoderMacro[T] }

    /**
     * Macro-generated decoder for product types, summoning
     * field decoders at compile time and constructing the
     * entity via its primary constructor.
     */
    private def productDecoderMacro[T <: Product : Type](using q: Quotes): Expr[JdbcDecoder[T]] =
        import q.reflect.*

        val tpr = TypeRepr.of[T]
        val typeArgs = tpr match
            case AppliedType(_, args) => args
            case _ => Nil
        val symbol = tpr.typeSymbol
        val ctor = symbol.primaryConstructor
        val types = symbol.caseFields.map: i =>
            i.tree match
                case ValDef(_, _, _) =>
                    tpr.memberType(i).asType
        val sizeExpr = Expr(types.size)

        def fetchExprs(data: Expr[ResultSet], cursor: Expr[Int]): List[Expr[Any]] =
            var tmpCursor = cursor
            types.map:
                case '[t] =>
                    val decoderExpr = Expr.summon[JdbcDecoder[t]].get
                    val resultExpr = '{ $decoderExpr.decode($data, $tmpCursor) }
                    tmpCursor = '{ $tmpCursor + $decoderExpr.offset }
                    resultExpr

        '{
            new JdbcDecoder[T]:
                def decode(data: ResultSet, cursor: Int): T =
                    ${
                        val exprs = fetchExprs('data, 'cursor)
                        val entity = if typeArgs.nonEmpty then
                            New(Inferred(tpr)).select(ctor).appliedToTypes(typeArgs).appliedToArgs(exprs.map(_.asTerm))
                        else
                            New(Inferred(tpr)).select(ctor).appliedToArgs(exprs.map(_.asTerm))
                        entity.asExprOf[T]
                    }

                def offset: Int = $sizeExpr
        }

/**
 * Type class for decoding individual array items from a JDBC
 * `java.sql.Array`. Used internally by `JdbcDecoder.array`.
 */
trait JdbcArrayItemDecoder[T]:
    /**
     * Decodes a raw JDBC array element into the target type.
     */
    def decode(x: Any): T

object JdbcArrayItemDecoder:
    given int: JdbcArrayItemDecoder[Int] with
        inline def decode(x: Any): Int =
            x.asInstanceOf[java.lang.Integer].intValue

    given long: JdbcArrayItemDecoder[Long] with
        inline def decode(x: Any): Long =
            x.asInstanceOf[java.lang.Long].longValue

    given float: JdbcArrayItemDecoder[Float] with
        inline def decode(x: Any): Float =
            x.asInstanceOf[java.lang.Float].floatValue

    given double: JdbcArrayItemDecoder[Double] with
        inline def decode(x: Any): Double =
            x.asInstanceOf[java.lang.Double].doubleValue

    given decimal: JdbcArrayItemDecoder[BigDecimal] with
        inline def decode(x: Any): BigDecimal =
            BigDecimal(x.asInstanceOf[java.math.BigDecimal])

    given bool: JdbcArrayItemDecoder[Boolean] with
        inline def decode(x: Any): Boolean =
            x.asInstanceOf[java.lang.Boolean].booleanValue

    given string: JdbcArrayItemDecoder[String] with
        inline def decode(x: Any): String =
            x.asInstanceOf[java.lang.String]

    given option[T: JdbcArrayItemDecoder as d]: JdbcArrayItemDecoder[Option[T]] with
        inline def decode(x: Any): Option[T] =
            x match
                case null => None
                case v => Some(d.decode(v))

    given array[T: JdbcArrayItemDecoder as d](using ClassTag[T]): JdbcArrayItemDecoder[Array[T]] with
        inline def decode(x: Any): Array[T] =
            x.asInstanceOf[Array[?]].map(i => d.decode(i))