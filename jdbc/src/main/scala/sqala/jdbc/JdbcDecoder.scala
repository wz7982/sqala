package sqala.jdbc

import sqala.static.metadata.*

import java.sql.ResultSet
import java.time.{LocalDate, LocalDateTime, ZoneId}
import scala.NamedTuple.NamedTuple
import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror
import scala.quoted.{Expr, Quotes, Type}
import scala.reflect.ClassTag

trait JdbcDecoder[T]:
    def offset: Int

    def decode(data: ResultSet, cursor: Int): T

object JdbcDecoder:
    inline def summonInstances[T <: Tuple]: List[JdbcDecoder[?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[JdbcDecoder[t]] :: summonInstances[ts]

    given int: JdbcDecoder[Int] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): Int = data.getInt(cursor)

    given long: JdbcDecoder[Long] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): Long = data.getLong(cursor)

    given float: JdbcDecoder[Float] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): Float = data.getFloat(cursor)

    given double: JdbcDecoder[Double] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): Double = data.getDouble(cursor)

    given decimal: JdbcDecoder[BigDecimal] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): BigDecimal = data.getBigDecimal(cursor)

    given boolean: JdbcDecoder[Boolean] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): Boolean = data.getBoolean(cursor)

    given string: JdbcDecoder[String] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): String = data.getString(cursor)

    given localDate: JdbcDecoder[LocalDate] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): LocalDate =
            LocalDate.ofInstant(data.getTimestamp(cursor).toInstant(), ZoneId.systemDefault())

    given localDateTime: JdbcDecoder[LocalDateTime] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): LocalDateTime =
            LocalDateTime.ofInstant(data.getTimestamp(cursor).toInstant(), ZoneId.systemDefault())

    given json: JdbcDecoder[Json] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): Json =
            Json(data.getString(cursor))
        
    given vector: JdbcDecoder[Vector] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): Vector =
            Vector(data.getString(cursor))

    given point: JdbcDecoder[Point] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): Point =
            Point(data.getString(cursor))

    given lineString: JdbcDecoder[LineString] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): LineString =
            LineString(data.getString(cursor))

    given polygon: JdbcDecoder[Polygon] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): Polygon =
            Polygon(data.getString(cursor))

    given multiPoint: JdbcDecoder[MultiPoint] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): MultiPoint =
            MultiPoint(data.getString(cursor))

    given multiLineString: JdbcDecoder[MultiLineString] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): MultiLineString =
            MultiLineString(data.getString(cursor))

    given multiPolygon: JdbcDecoder[MultiPolygon] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): MultiPolygon =
            MultiPolygon(data.getString(cursor))

    given geometryCollection: JdbcDecoder[GeometryCollection] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): GeometryCollection =
            GeometryCollection(data.getString(cursor))

    given option[T](using d: JdbcDecoder[T]): JdbcDecoder[Option[T]] with
        inline def offset: Int = d.offset

        inline def decode(data: ResultSet, cursor: Int): Option[T] =
            val slice = for i <- (cursor until cursor + offset) yield
                data.getObject(i)
            if slice.map(_ == null).reduce((x, y) => x && y) then None else Some(d.decode(data, cursor))

    given array[T](using d: JdbcArrayItemDecoder[T], c: ClassTag[T]): JdbcDecoder[Array[T]] with
        inline def offset: Int = 1

        inline def decode(data: ResultSet, cursor: Int): Array[T] =
            data
                .getArray(cursor)
                .getArray
                .asInstanceOf[Array[?]]
                .map(i => d.decode(i))

    given customField[T, R](using c: CustomField[T, R], d: JdbcDecoder[R]): JdbcDecoder[T] with
        inline def offset: Int = d.offset

        inline def decode(data: ResultSet, cursor: Int): T = c.fromValue(d.decode(data, cursor))

    given tuple[H, T <: Tuple](using headDecoder: JdbcDecoder[H], tailDecoder: JdbcDecoder[T]): JdbcDecoder[H *: T] with
        inline def offset: Int = headDecoder.offset + tailDecoder.offset

        inline def decode(data: ResultSet, cursor: Int): H *: T =
            headDecoder.decode(data, cursor) *: tailDecoder.decode(data, cursor + headDecoder.offset)

    given tuple1[H](using headDecoder: JdbcDecoder[H]): JdbcDecoder[H *: EmptyTuple] with
        inline def offset: Int = headDecoder.offset

        inline def decode(data: ResultSet, cursor: Int): H *: EmptyTuple =
            headDecoder.decode(data, cursor) *: EmptyTuple

    given namedTuple[N <: Tuple, V <: Tuple](using d: JdbcDecoder[V]): JdbcDecoder[NamedTuple[N, V]] with
        inline def offset: Int = d.offset

        inline def decode(data: ResultSet, cursor: Int): NamedTuple[N, V] =
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
                def decode(data: ResultSet, cursor: Int): T =
                    ${
                        val exprs = fetchExprs('data, 'cursor)
                        New(Inferred(tpr)).select(ctor).appliedToArgs(exprs.map(_.asTerm)).asExprOf[T]
                    }

                def offset: Int = $sizeExpr
        }

trait JdbcArrayItemDecoder[T]:
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