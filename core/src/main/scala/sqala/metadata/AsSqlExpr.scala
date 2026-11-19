package sqala.metadata

import sqala.ast.expr.*

import java.time.*
import java.time.format.DateTimeFormatter

/**
 * Type class that converts a value into a `SqlExpr` literal.
 *
 * Also provides the corresponding `SqlType` for the value.
 */
trait AsSqlExpr[T]:
    /**
     * The SQL type corresponding to this type.
     */
    def sqlType: SqlType

    /**
     * Converts the given value to a `SqlExpr` literal node.
     */
    def asSqlExpr(x: T, inlined: Boolean): SqlExpr

object AsSqlExpr:
    given int: AsSqlExpr[Int] with
        def sqlType: SqlType =
            SqlType.Int

        def asSqlExpr(x: Int, inlined: Boolean): SqlExpr =
            val expr: SqlExpr.NumberLiteral = SqlExpr.NumberLiteral(BigDecimal(x.toString))
            if inlined then
                SqlExpr.InlinedLiteral(expr)
            else
                expr

    given long: AsSqlExpr[Long] with
        def sqlType: SqlType =
            SqlType.Long

        def asSqlExpr(x: Long, inlined: Boolean): SqlExpr =
            val expr: SqlExpr.NumberLiteral = SqlExpr.NumberLiteral(BigDecimal(x.toString))
            if inlined then
                SqlExpr.InlinedLiteral(expr)
            else
                expr

    given float: AsSqlExpr[Float] with
        def sqlType: SqlType =
            SqlType.Float

        def asSqlExpr(x: Float, inlined: Boolean): SqlExpr =
            val expr: SqlExpr.NumberLiteral = SqlExpr.NumberLiteral(BigDecimal(x.toString))
            if inlined then
                SqlExpr.InlinedLiteral(expr)
            else
                expr

    given double: AsSqlExpr[Double] with
        def sqlType: SqlType =
            SqlType.Double

        def asSqlExpr(x: Double, inlined: Boolean): SqlExpr =
            val expr: SqlExpr.NumberLiteral = SqlExpr.NumberLiteral(BigDecimal(x.toString))
            if inlined then
                SqlExpr.InlinedLiteral(expr)
            else
                expr

    given decimal: AsSqlExpr[BigDecimal] with
        def sqlType: SqlType =
            SqlType.Decimal(Some(38, 10))

        def asSqlExpr(x: BigDecimal, inlined: Boolean): SqlExpr =
            val expr: SqlExpr.NumberLiteral = SqlExpr.NumberLiteral(BigDecimal(x.toString))
            if inlined then
                SqlExpr.InlinedLiteral(expr)
            else
                expr

    given string: AsSqlExpr[String] with
        def sqlType: SqlType =
            SqlType.Varchar(None)

        def asSqlExpr(x: String, inlined: Boolean): SqlExpr =
            val expr: SqlExpr.StringLiteral = SqlExpr.StringLiteral(x)
            if inlined then
                SqlExpr.InlinedLiteral(expr)
            else
                expr

    given boolean: AsSqlExpr[Boolean] with
        def sqlType: SqlType =
            SqlType.Boolean

        def asSqlExpr(x: Boolean, inlined: Boolean): SqlExpr =
            val expr: SqlExpr.BooleanLiteral = SqlExpr.BooleanLiteral(x)
            if inlined then
                SqlExpr.InlinedLiteral(expr)
            else
                expr

    given localDate: AsSqlExpr[LocalDate] with
        def sqlType: SqlType =
            SqlType.Date

        def asSqlExpr(x: LocalDate, inlined: Boolean): SqlExpr =
            val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            val expr: SqlExpr.TimeLiteral = SqlExpr.TimeLiteral(SqlTimeType.Date, formatter.format(x))
            if inlined then
                SqlExpr.InlinedLiteral(expr)
            else
                expr

    given localDateTime: AsSqlExpr[LocalDateTime] with
        def sqlType: SqlType =
            SqlType.Timestamp(None)

        def asSqlExpr(x: LocalDateTime, inlined: Boolean): SqlExpr =
            val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn")
            val expr: SqlExpr.TimeLiteral = SqlExpr.TimeLiteral(SqlTimeType.Timestamp(None), formatter.format(x))
            if inlined then
                SqlExpr.InlinedLiteral(expr)
            else
                expr

    given localTime: AsSqlExpr[LocalTime] with
        def sqlType: SqlType =
            SqlType.Time(None)

        def asSqlExpr(x: LocalTime, inlined: Boolean): SqlExpr =
            val formatter = DateTimeFormatter.ofPattern("HH:mm:ss")
            val expr: SqlExpr.TimeLiteral = SqlExpr.TimeLiteral(SqlTimeType.Time(None), formatter.format(x))
            if inlined then
                SqlExpr.InlinedLiteral(expr)
            else
                expr

    given offsetDateTime: AsSqlExpr[OffsetDateTime] with
        def sqlType: SqlType =
            SqlType.Timestamp(Some(SqlTimeZoneMode.With))

        def asSqlExpr(x: OffsetDateTime, inlined: Boolean): SqlExpr =
            val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn XXX")
            val expr: SqlExpr.TimeLiteral = SqlExpr.TimeLiteral(SqlTimeType.Timestamp(Some(SqlTimeZoneMode.With)), formatter.format(x))
            if inlined then
                SqlExpr.InlinedLiteral(expr)
            else
                expr

    given offsetTime: AsSqlExpr[OffsetTime] with
        def sqlType: SqlType =
            SqlType.Time(Some(SqlTimeZoneMode.With))

        def asSqlExpr(x: OffsetTime, inlined: Boolean): SqlExpr =
            val formatter = DateTimeFormatter.ofPattern("HH:mm:ss XXX")
            val expr: SqlExpr.TimeLiteral = SqlExpr.TimeLiteral(SqlTimeType.Time(Some(SqlTimeZoneMode.With)), formatter.format(x))
            if inlined then
                SqlExpr.InlinedLiteral(expr)
            else
                expr

    given json: AsSqlExpr[Json] with
        def sqlType: SqlType =
            SqlType.Json

        def asSqlExpr(x: Json, inlined: Boolean): SqlExpr =
            if inlined then
                SqlExpr.Cast(SqlExpr.InlinedLiteral(SqlExpr.StringLiteral(x.toString)), SqlType.Json)
            else
                SqlExpr.Cast(SqlExpr.StringLiteral(x.toString), SqlType.Json)

    given interval: AsSqlExpr[Interval] with
        def sqlType: SqlType =
            SqlType.Interval

        def asSqlExpr(x: Interval, inlined: Boolean): SqlExpr =
            val expr: SqlExpr.IntervalLiteral = SqlExpr.IntervalLiteral(x.value, SqlIntervalField.Single(x.unit))
            if inlined then
                SqlExpr.InlinedLiteral(expr)
            else
                expr

    given geometry: AsSqlExpr[Geometry] with
        def sqlType: SqlType =
            SqlType.Geometry

        def asSqlExpr(x: Geometry, inlined: Boolean): SqlExpr =
            val valueExpr: SqlExpr.StringLiteral = SqlExpr.StringLiteral(x.value)
            val sridExpr: SqlExpr.NumberLiteral = SqlExpr.NumberLiteral(x.srid)
            val argExprs = 
                if inlined then
                    SqlExpr.InlinedLiteral(valueExpr) :: SqlExpr.InlinedLiteral(sridExpr) :: Nil
                else
                    valueExpr :: sridExpr :: Nil
            SqlExpr.GeneralFunc(
                None, 
                "ST_GeomFromText", 
                argExprs, 
                Nil, 
                Nil, 
                None
            )

    given point: AsSqlExpr[Point] with
        def sqlType: SqlType =
            SqlType.Point

        def asSqlExpr(x: Point, inlined: Boolean): SqlExpr =
            val valueExpr: SqlExpr.StringLiteral = SqlExpr.StringLiteral(x.value)
            val sridExpr: SqlExpr.NumberLiteral = SqlExpr.NumberLiteral(x.srid)
            val argExprs = 
                if inlined then
                    SqlExpr.InlinedLiteral(valueExpr) :: SqlExpr.InlinedLiteral(sridExpr) :: Nil
                else
                    valueExpr :: sridExpr :: Nil
            SqlExpr.GeneralFunc(
                None, 
                "ST_GeomFromText", 
                argExprs, 
                Nil, 
                Nil, 
                None
            )

    given lineString: AsSqlExpr[LineString] with
        def sqlType: SqlType =
            SqlType.LineString

        def asSqlExpr(x: LineString, inlined: Boolean): SqlExpr =
            val valueExpr: SqlExpr.StringLiteral = SqlExpr.StringLiteral(x.value)
            val sridExpr: SqlExpr.NumberLiteral = SqlExpr.NumberLiteral(x.srid)
            val argExprs = 
                if inlined then
                    SqlExpr.InlinedLiteral(valueExpr) :: SqlExpr.InlinedLiteral(sridExpr) :: Nil
                else
                    valueExpr :: sridExpr :: Nil
            SqlExpr.GeneralFunc(
                None, 
                "ST_GeomFromText", 
                argExprs, 
                Nil, 
                Nil, 
                None
            )

    given polygon: AsSqlExpr[Polygon] with
        def sqlType: SqlType =
            SqlType.Polygon

        def asSqlExpr(x: Polygon, inlined: Boolean): SqlExpr =
            val valueExpr: SqlExpr.StringLiteral = SqlExpr.StringLiteral(x.value)
            val sridExpr: SqlExpr.NumberLiteral = SqlExpr.NumberLiteral(x.srid)
            val argExprs = 
                if inlined then
                    SqlExpr.InlinedLiteral(valueExpr) :: SqlExpr.InlinedLiteral(sridExpr) :: Nil
                else
                    valueExpr :: sridExpr :: Nil
            SqlExpr.GeneralFunc(
                None, 
                "ST_GeomFromText", 
                argExprs, 
                Nil, 
                Nil, 
                None
            )

    given multiPoint: AsSqlExpr[MultiPoint] with
        def sqlType: SqlType =
            SqlType.MultiPoint

        def asSqlExpr(x: MultiPoint, inlined: Boolean): SqlExpr =
            val valueExpr: SqlExpr.StringLiteral = SqlExpr.StringLiteral(x.value)
            val sridExpr: SqlExpr.NumberLiteral = SqlExpr.NumberLiteral(x.srid)
            val argExprs = 
                if inlined then
                    SqlExpr.InlinedLiteral(valueExpr) :: SqlExpr.InlinedLiteral(sridExpr) :: Nil
                else
                    valueExpr :: sridExpr :: Nil
            SqlExpr.GeneralFunc(
                None, 
                "ST_GeomFromText", 
                argExprs, 
                Nil, 
                Nil, 
                None
            )

    given multiLineString: AsSqlExpr[MultiLineString] with
        def sqlType: SqlType =
            SqlType.MultiLineString

        def asSqlExpr(x: MultiLineString, inlined: Boolean): SqlExpr =
            val valueExpr: SqlExpr.StringLiteral = SqlExpr.StringLiteral(x.value)
            val sridExpr: SqlExpr.NumberLiteral = SqlExpr.NumberLiteral(x.srid)
            val argExprs = 
                if inlined then
                    SqlExpr.InlinedLiteral(valueExpr) :: SqlExpr.InlinedLiteral(sridExpr) :: Nil
                else
                    valueExpr :: sridExpr :: Nil
            SqlExpr.GeneralFunc(
                None, 
                "ST_GeomFromText", 
                argExprs, 
                Nil, 
                Nil, 
                None
            )

    given multiPolygon: AsSqlExpr[MultiPolygon] with
        def sqlType: SqlType =
            SqlType.MultiPolygon

        def asSqlExpr(x: MultiPolygon, inlined: Boolean): SqlExpr =
            val valueExpr: SqlExpr.StringLiteral = SqlExpr.StringLiteral(x.value)
            val sridExpr: SqlExpr.NumberLiteral = SqlExpr.NumberLiteral(x.srid)
            val argExprs = 
                if inlined then
                    SqlExpr.InlinedLiteral(valueExpr) :: SqlExpr.InlinedLiteral(sridExpr) :: Nil
                else
                    valueExpr :: sridExpr :: Nil
            SqlExpr.GeneralFunc(
                None, 
                "ST_GeomFromText", 
                argExprs, 
                Nil, 
                Nil, 
                None
            )

    given geometryCollection: AsSqlExpr[GeometryCollection] with
        def sqlType: SqlType =
            SqlType.GeometryCollection

        def asSqlExpr(x: GeometryCollection, inlined: Boolean): SqlExpr =
            val valueExpr: SqlExpr.StringLiteral = SqlExpr.StringLiteral(x.value)
            val sridExpr: SqlExpr.NumberLiteral = SqlExpr.NumberLiteral(x.srid)
            val argExprs = 
                if inlined then
                    SqlExpr.InlinedLiteral(valueExpr) :: SqlExpr.InlinedLiteral(sridExpr) :: Nil
                else
                    valueExpr :: sridExpr :: Nil
            SqlExpr.GeneralFunc(
                None, 
                "ST_GeomFromText", 
                argExprs, 
                Nil, 
                Nil, 
                None
            )

    given option[T: AsSqlExpr as a]: AsSqlExpr[Option[T]] with
        def sqlType: SqlType =
            a.sqlType

        def asSqlExpr(x: Option[T], inlined: Boolean): SqlExpr =
            x match
                case None => 
                    SqlExpr.Cast(SqlExpr.NullLiteral, sqlType)
                case Some(v) => 
                    a.asSqlExpr(v, inlined)

    given array[T: AsSqlExpr as a]: AsSqlExpr[Array[T]] with
        def sqlType: SqlType =
            SqlType.Array(a.sqlType)

        def asSqlExpr(x: Array[T], inlined: Boolean): SqlExpr =
            if x.isEmpty then 
                SqlExpr.Cast(SqlExpr.Array(Nil), sqlType)
            else 
                SqlExpr.Array(x.toList.map(i => a.asSqlExpr(i, inlined)))

/**
 * Type class that converts a tuple of values into a list of `SqlExpr` literals.
 */
trait AsSqlExprs[T]:
    /**
     * Converts the given tuple of values to a list of `SqlExpr` literal nodes.
     */
    def asSqlExprs(values: T, inlined: Boolean): List[SqlExpr]

object AsSqlExprs:
    given singleton[T: AsSqlExpr as a]: AsSqlExprs[T] with
        def asSqlExprs(values: T, inlined: Boolean): List[SqlExpr] =
            List(a.asSqlExpr(values, inlined))

    given tuple[H, T <: Tuple](using h: AsSqlExpr[H], t: AsSqlExprs[T]): AsSqlExprs[H *: T] with
        def asSqlExprs(values: H *: T, inlined: Boolean): List[SqlExpr] =
            h.asSqlExpr(values.head, inlined) :: t.asSqlExprs(values.tail, inlined)

    given emptyTuple: AsSqlExprs[EmptyTuple] with
        def asSqlExprs(values: EmptyTuple, inlined: Boolean): List[SqlExpr] =
            Nil