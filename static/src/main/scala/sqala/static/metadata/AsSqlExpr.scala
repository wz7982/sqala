package sqala.static.metadata

import sqala.ast.expr.{SqlExpr, SqlTimeLiteralUnit, SqlTimeZoneMode, SqlType}

import java.time.*
import scala.annotation.implicitNotFound
import scala.compiletime.{erasedValue, summonInline}

@implicitNotFound("The type ${T} cannot be converted to SQL expression.")
trait AsSqlExpr[T]:
    def sqlType: SqlType

    def asSqlExpr(x: T): SqlExpr

object AsSqlExpr:
    inline def summonInstances[T]: List[AsSqlExpr[?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[AsSqlExpr[t]] :: summonInstances[ts]
            case _ => summonInline[AsSqlExpr[T]] :: Nil

    given int: AsSqlExpr[Int] with
        def sqlType: SqlType = SqlType.Int

        def asSqlExpr(x: Int): SqlExpr = 
            SqlExpr.NumberLiteral(x)

    given long: AsSqlExpr[Long] with
        def sqlType: SqlType = SqlType.Long

        def asSqlExpr(x: Long): SqlExpr = 
            SqlExpr.NumberLiteral(x)

    given float: AsSqlExpr[Float] with
        def sqlType: SqlType = SqlType.Float

        def asSqlExpr(x: Float): SqlExpr = 
            SqlExpr.NumberLiteral(x)

    given double: AsSqlExpr[Double] with
        def sqlType: SqlType = SqlType.Double

        def asSqlExpr(x: Double): SqlExpr = 
            SqlExpr.NumberLiteral(x)

    given decimal: AsSqlExpr[BigDecimal] with
        def sqlType: SqlType = SqlType.Decimal(None)

        def asSqlExpr(x: BigDecimal): SqlExpr = 
            SqlExpr.NumberLiteral(x)

    given string: AsSqlExpr[String] with
        def sqlType: SqlType = SqlType.Varchar(None)

        def asSqlExpr(x: String): SqlExpr = 
            SqlExpr.StringLiteral(x)

    given boolean: AsSqlExpr[Boolean] with
        def sqlType: SqlType = SqlType.Boolean

        def asSqlExpr(x: Boolean): SqlExpr = 
            SqlExpr.BooleanLiteral(x)

    given localDate: AsSqlExpr[LocalDate] with
        def sqlType: SqlType = SqlType.Date

        def asSqlExpr(x: LocalDate): SqlExpr =
            SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Date, x.toString)

    given localDateTime: AsSqlExpr[LocalDateTime] with
        def sqlType: SqlType = SqlType.Timestamp(None)

        def asSqlExpr(x: LocalDateTime): SqlExpr =
            SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Timestamp, x.toString)

    given localTime: AsSqlExpr[LocalTime] with
        def sqlType: SqlType = SqlType.Time(None)

        def asSqlExpr(x: LocalTime): SqlExpr =
            SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Time, x.toString)

    given offsetDateTime: AsSqlExpr[OffsetDateTime] with
        def sqlType: SqlType = SqlType.Timestamp(Some(SqlTimeZoneMode.With))

        def asSqlExpr(x: OffsetDateTime): SqlExpr =
            SqlExpr.Cast(SqlExpr.StringLiteral(x.toString), SqlType.Timestamp(Some(SqlTimeZoneMode.With)))

    given offsetTime: AsSqlExpr[OffsetTime] with
        def sqlType: SqlType = SqlType.Time(Some(SqlTimeZoneMode.With))

        def asSqlExpr(x: OffsetTime): SqlExpr =
            SqlExpr.Cast(SqlExpr.StringLiteral(x.toString), SqlType.Time(Some(SqlTimeZoneMode.With)))

    given json: AsSqlExpr[Json] with
        def sqlType: SqlType = SqlType.Json

        def asSqlExpr(x: Json): SqlExpr = 
            SqlExpr.Cast(SqlExpr.StringLiteral(x.toString), SqlType.Json)

    given vector: AsSqlExpr[Vector] with
        def sqlType: SqlType = SqlType.Vector

        def asSqlExpr(x: Vector): SqlExpr = 
            SqlExpr.StringLiteral(x.toString)

    given point: AsSqlExpr[Point] with
        def sqlType: SqlType = SqlType.Point

        def asSqlExpr(x: Point): SqlExpr =
            SqlExpr.StandardFunc(
                "ST_GeomFromText",
                SqlExpr.StringLiteral(x.toString) :: Nil,
                None,
                Nil,
                Nil,
                None
            )

    given lineString: AsSqlExpr[LineString] with
        def sqlType: SqlType = SqlType.LineString

        def asSqlExpr(x: LineString): SqlExpr =
            SqlExpr.StandardFunc(
                "ST_GeomFromText",
                SqlExpr.StringLiteral(x.toString) :: Nil,
                None,
                Nil,
                Nil,
                None
            )

    given polygon: AsSqlExpr[Polygon] with
        def sqlType: SqlType = SqlType.Polygon

        def asSqlExpr(x: Polygon): SqlExpr =
            SqlExpr.StandardFunc(
                "ST_GeomFromText",
                SqlExpr.StringLiteral(x.toString) :: Nil,
                None,
                Nil,
                Nil,
                None
            )

    given multiPoint: AsSqlExpr[MultiPoint] with
        def sqlType: SqlType = SqlType.MultiPoint

        def asSqlExpr(x: MultiPoint): SqlExpr =
            SqlExpr.StandardFunc(
                "ST_GeomFromText",
                SqlExpr.StringLiteral(x.toString) :: Nil,
                None,
                Nil,
                Nil,
                None
            )

    given multiLineString: AsSqlExpr[MultiLineString] with
        def sqlType: SqlType = SqlType.MultiLineString

        def asSqlExpr(x: MultiLineString): SqlExpr =
            SqlExpr.StandardFunc(
                "ST_GeomFromText",
                SqlExpr.StringLiteral(x.toString) :: Nil,
                None,
                Nil,
                Nil,
                None
            )

    given multiPolygon: AsSqlExpr[MultiPolygon] with
        def sqlType: SqlType = SqlType.MultiPolygon

        def asSqlExpr(x: MultiPolygon): SqlExpr =
            SqlExpr.StandardFunc(
                "ST_GeomFromText",
                SqlExpr.StringLiteral(x.toString) :: Nil,
                None,
                Nil,
                Nil,
                None
            )

    given geometryCollection: AsSqlExpr[GeometryCollection] with
        def sqlType: SqlType = SqlType.GeometryCollection

        def asSqlExpr(x: GeometryCollection): SqlExpr =
            SqlExpr.StandardFunc(
                "ST_GeomFromText",
                SqlExpr.StringLiteral(x.toString) :: Nil,
                None,
                Nil,
                Nil,
                None
            )

    given option[T: AsSqlExpr as a]: AsSqlExpr[Option[T]] with
        def sqlType: SqlType = a.sqlType

        def asSqlExpr(x: Option[T]): SqlExpr = x match
            case None => SqlExpr.Cast(SqlExpr.NullLiteral, sqlType)
            case Some(v) => a.asSqlExpr(v)

    given array[T: AsSqlExpr as a]: AsSqlExpr[Array[T]] with
        def sqlType: SqlType = SqlType.Array(a.sqlType)

        def asSqlExpr(x: Array[T]): SqlExpr =
            if x.isEmpty then
                SqlExpr.Cast(SqlExpr.Array(Nil), sqlType)
            else
                SqlExpr.Array(x.toList.map(i => a.asSqlExpr(i)))