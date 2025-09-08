package sqala.metadata

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

    given intAsSqlExpr: AsSqlExpr[Int] with
        def sqlType: SqlType = SqlType.Int

        def asSqlExpr(x: Int): SqlExpr = 
            SqlExpr.NumberLiteral(x)

    given longAsSqlExpr: AsSqlExpr[Long] with
        def sqlType: SqlType = SqlType.Long

        def asSqlExpr(x: Long): SqlExpr = 
            SqlExpr.NumberLiteral(x)

    given floatAsSqlExpr: AsSqlExpr[Float] with
        def sqlType: SqlType = SqlType.Float

        def asSqlExpr(x: Float): SqlExpr = 
            SqlExpr.NumberLiteral(x)

    given doubleAsSqlExpr: AsSqlExpr[Double] with
        def sqlType: SqlType = SqlType.Double

        def asSqlExpr(x: Double): SqlExpr = 
            SqlExpr.NumberLiteral(x)

    given decimalAsSqlExpr: AsSqlExpr[BigDecimal] with
        def sqlType: SqlType = SqlType.Decimal(None)

        def asSqlExpr(x: BigDecimal): SqlExpr = 
            SqlExpr.NumberLiteral(x)

    given stringAsSqlExpr: AsSqlExpr[String] with
        def sqlType: SqlType = SqlType.Varchar(None)

        def asSqlExpr(x: String): SqlExpr = 
            SqlExpr.StringLiteral(x)

    given booleanAsSqlExpr: AsSqlExpr[Boolean] with
        def sqlType: SqlType = SqlType.Boolean

        def asSqlExpr(x: Boolean): SqlExpr = 
            SqlExpr.BooleanLiteral(x)

    given localDateAsSqlExpr: AsSqlExpr[LocalDate] with
        def sqlType: SqlType = SqlType.Date

        def asSqlExpr(x: LocalDate): SqlExpr =
            SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Date, x.toString)

    given localDateTimeAsSqlExpr: AsSqlExpr[LocalDateTime] with
        def sqlType: SqlType = SqlType.Timestamp(None)

        def asSqlExpr(x: LocalDateTime): SqlExpr =
            SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Timestamp, x.toString)

    given localTimeAsSqlExpr: AsSqlExpr[LocalDateTime] with
        def sqlType: SqlType = SqlType.Time(None)

        def asSqlExpr(x: LocalDateTime): SqlExpr =
            SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Time, x.toString)

    given offsetDateTimeAsSqlExpr: AsSqlExpr[OffsetDateTime] with
        def sqlType: SqlType = SqlType.Timestamp(Some(SqlTimeZoneMode.With))

        def asSqlExpr(x: OffsetDateTime): SqlExpr =
            SqlExpr.Cast(SqlExpr.StringLiteral(x.toString), SqlType.Timestamp(Some(SqlTimeZoneMode.With)))

    given offsetTimeAsSqlExpr: AsSqlExpr[OffsetTime] with
        def sqlType: SqlType = SqlType.Time(Some(SqlTimeZoneMode.With))

        def asSqlExpr(x: OffsetTime): SqlExpr =
            SqlExpr.Cast(SqlExpr.StringLiteral(x.toString), SqlType.Time(Some(SqlTimeZoneMode.With)))

    given jsonAsSqlExpr: AsSqlExpr[Json] with
        def sqlType: SqlType = SqlType.Json

        def asSqlExpr(x: Json): SqlExpr = 
            SqlExpr.Cast(SqlExpr.StringLiteral(x.toString), SqlType.Json)

    given vectorAsSqlExpr: AsSqlExpr[Vector] with
        def sqlType: SqlType = SqlType.Vector

        def asSqlExpr(x: Vector): SqlExpr = 
            SqlExpr.Vector(SqlExpr.StringLiteral(x.toString))

    given optionAsSqlExpr[T: AsSqlExpr as a]: AsSqlExpr[Option[T]] with
        def sqlType: SqlType = a.sqlType

        def asSqlExpr(x: Option[T]): SqlExpr = x match
            case None => SqlExpr.Cast(SqlExpr.NullLiteral, sqlType)
            case Some(v) => a.asSqlExpr(v)

    given arrayAsSqlExpr[T: AsSqlExpr as a]: AsSqlExpr[Array[T]] with
        def sqlType: SqlType = SqlType.Array(a.sqlType)

        def asSqlExpr(x: Array[T]): SqlExpr =
            if x.isEmpty then
                SqlExpr.Cast(SqlExpr.Array(Nil), sqlType)
            else
                SqlExpr.Array(x.toList.map(i => a.asSqlExpr(i)))