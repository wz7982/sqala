package sqala.jdbc

import sqala.metadata.{CustomField, Json, Vector}

import java.sql.ResultSet
import java.time.{LocalDate, LocalDateTime, ZoneId}
import scala.NamedTuple.NamedTuple
import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror
import scala.quoted.{Expr, Quotes, Type}

trait JdbcDecoder[T]:
    def offset: Int

    def decode(data: ResultSet, cursor: Int): T

object JdbcDecoder:
    inline def summonInstances[T <: Tuple]: List[JdbcDecoder[?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[JdbcDecoder[t]] :: summonInstances[ts]

    given intDecoder: JdbcDecoder[Int] with
        override inline def offset: Int = 1

        override inline def decode(data: ResultSet, cursor: Int): Int = data.getInt(cursor)

    given longDecoder: JdbcDecoder[Long] with
        override inline def offset: Int = 1

        override inline def decode(data: ResultSet, cursor: Int): Long = data.getLong(cursor)

    given floatDecoder: JdbcDecoder[Float] with
        override inline def offset: Int = 1

        override inline def decode(data: ResultSet, cursor: Int): Float = data.getFloat(cursor)

    given doubleDecoder: JdbcDecoder[Double] with
        override inline def offset: Int = 1

        override inline def decode(data: ResultSet, cursor: Int): Double = data.getDouble(cursor)

    given decimalDecoder: JdbcDecoder[BigDecimal] with
        override inline def offset: Int = 1

        override inline def decode(data: ResultSet, cursor: Int): BigDecimal = data.getBigDecimal(cursor)

    given booleanDecoder: JdbcDecoder[Boolean] with
        override inline def offset: Int = 1

        override inline def decode(data: ResultSet, cursor: Int): Boolean = data.getBoolean(cursor)

    given stringDecoder: JdbcDecoder[String] with
        override inline def offset: Int = 1

        override inline def decode(data: ResultSet, cursor: Int): String = data.getString(cursor)

    given localDateDecoder: JdbcDecoder[LocalDate] with
        override inline def offset: Int = 1

        override inline def decode(data: ResultSet, cursor: Int): LocalDate =
            LocalDate.ofInstant(data.getTimestamp(cursor).toInstant(), ZoneId.systemDefault())

    given localDateTimeDecoder: JdbcDecoder[LocalDateTime] with
        override inline def offset: Int = 1

        override inline def decode(data: ResultSet, cursor: Int): LocalDateTime =
            LocalDateTime.ofInstant(data.getTimestamp(cursor).toInstant(), ZoneId.systemDefault())

    given jsonDecoder: JdbcDecoder[Json] with
        override inline def offset: Int = 1

        override inline def decode(data: ResultSet, cursor: Int): Json =
            Json(data.getString(cursor))
        
    given vectorDecoder: JdbcDecoder[Vector] with
        override inline def offset: Int = 1

        override inline def decode(data: ResultSet, cursor: Int): Vector =
            Vector(data.getString(cursor))

    given optionDecoder[T](using d: JdbcDecoder[T]): JdbcDecoder[Option[T]] with
        override inline def offset: Int = d.offset

        override inline def decode(data: ResultSet, cursor: Int): Option[T] =
            val slice = for i <- (cursor until cursor + offset) yield
                data.getObject(i)
            if slice.map(_ == null).reduce((x, y) => x && y) then None else Some(d.decode(data, cursor))

    given someDecoder[T](using d: JdbcDecoder[T]): JdbcDecoder[Some[T]] with
        override inline def offset: Int = d.offset

        override inline def decode(data: ResultSet, cursor: Int): Some[T] =
            Some(d.decode(data, cursor))

    given noneDecoder[T]: JdbcDecoder[None.type] with
        override inline def offset: Int = 1

        override inline def decode(data: ResultSet, cursor: Int): None.type =
            None

    given customFieldDecoder[T, R](using c: CustomField[T, R], d: JdbcDecoder[R]): JdbcDecoder[T] with
        override inline def offset: Int = d.offset

        override inline def decode(data: ResultSet, cursor: Int): T = c.fromValue(d.decode(data, cursor))

    given tupleDecoder[H, T <: Tuple](using headDecoder: JdbcDecoder[H], tailDecoder: JdbcDecoder[T]): JdbcDecoder[H *: T] with
        override inline def offset: Int = headDecoder.offset + tailDecoder.offset

        override inline def decode(data: ResultSet, cursor: Int): H *: T =
            headDecoder.decode(data, cursor) *: tailDecoder.decode(data, cursor + headDecoder.offset)

    given tuple1Decoder[H](using headDecoder: JdbcDecoder[H]): JdbcDecoder[H *: EmptyTuple] with
        override inline def offset: Int = headDecoder.offset

        override inline def decode(data: ResultSet, cursor: Int): H *: EmptyTuple =
            headDecoder.decode(data, cursor) *: EmptyTuple

    given namedTupleDecoder[N <: Tuple, V <: Tuple](using d: JdbcDecoder[V]): JdbcDecoder[NamedTuple[N, V]] with
        override inline def offset: Int = d.offset

        override inline def decode(data: ResultSet, cursor: Int): NamedTuple[N, V] =
            NamedTuple(d.decode(data, cursor))

    inline given derived[T <: Product](using m: Mirror.ProductOf[T]): JdbcDecoder[T] =
        ${ productDecoderMacro[T] }

    private def productDecoderMacro[T <: Product : Type](using q: Quotes): Expr[JdbcDecoder[T]] =
        import q.reflect.*

        val tpr = TypeRepr.of[T]
        val symbol = tpr.typeSymbol
        val ctor = symbol.primaryConstructor
        val types = symbol.declaredFields.map: i =>
            i.tree match
                case ValDef(_, typeTree, _) => typeTree.tpe.asType
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
                override def decode(data: ResultSet, cursor: Int): T =
                    ${
                        val exprs = fetchExprs('data, 'cursor)
                        New(Inferred(tpr)).select(ctor).appliedToArgs(exprs.map(_.asTerm)).asExprOf[T]
                    }

                override def offset: Int = $sizeExpr
        }