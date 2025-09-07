package sqala.metadata

import sqala.ast.expr.{SqlExpr, SqlTimeLiteralUnit, SqlTimeZoneMode, SqlType}

import java.time.*
import scala.annotation.implicitNotFound
import scala.compiletime.{erasedValue, summonInline}

@implicitNotFound("The type ${T} cannot be converted to SQL expression.")
trait AsSqlExpr[T]:
    def asSqlExpr(x: T): SqlExpr

object AsSqlExpr:
    inline def summonInstances[T]: List[AsSqlExpr[?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[AsSqlExpr[t]] :: summonInstances[ts]
            case _ => summonInline[AsSqlExpr[T]] :: Nil

    given intAsSqlExpr: AsSqlExpr[Int] with
        def asSqlExpr(x: Int): SqlExpr = 
            SqlExpr.NumberLiteral(x)

    given longAsSqlExpr: AsSqlExpr[Long] with
        def asSqlExpr(x: Long): SqlExpr = 
            SqlExpr.NumberLiteral(x)

    given floatAsSqlExpr: AsSqlExpr[Float] with
        def asSqlExpr(x: Float): SqlExpr = 
            SqlExpr.NumberLiteral(x)

    given doubleAsSqlExpr: AsSqlExpr[Double] with
        def asSqlExpr(x: Double): SqlExpr = 
            SqlExpr.NumberLiteral(x)

    given decimalAsSqlExpr: AsSqlExpr[BigDecimal] with
        def asSqlExpr(x: BigDecimal): SqlExpr = 
            SqlExpr.NumberLiteral(x)

    given stringAsSqlExpr: AsSqlExpr[String] with
        def asSqlExpr(x: String): SqlExpr = 
            SqlExpr.StringLiteral(x)

    given booleanAsSqlExpr: AsSqlExpr[Boolean] with
        def asSqlExpr(x: Boolean): SqlExpr = 
            SqlExpr.BooleanLiteral(x)

    given localDateAsSqlExpr: AsSqlExpr[LocalDate] with
        def asSqlExpr(x: LocalDate): SqlExpr =
            SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Date, x.toString)

    given localDateTimeAsSqlExpr: AsSqlExpr[LocalDateTime] with
        def asSqlExpr(x: LocalDateTime): SqlExpr =
            SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Timestamp, x.toString)

    given localTimeAsSqlExpr: AsSqlExpr[LocalDateTime] with
        def asSqlExpr(x: LocalDateTime): SqlExpr =
            SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Time, x.toString)

    given offsetDateTimeAsSqlExpr: AsSqlExpr[OffsetDateTime] with
        def asSqlExpr(x: OffsetDateTime): SqlExpr =
            SqlExpr.Cast(SqlExpr.StringLiteral(x.toString), SqlType.Timestamp(Some(SqlTimeZoneMode.With)))

    given offsetTimeAsSqlExpr: AsSqlExpr[OffsetTime] with
        def asSqlExpr(x: OffsetTime): SqlExpr =
            SqlExpr.Cast(SqlExpr.StringLiteral(x.toString), SqlType.Time(Some(SqlTimeZoneMode.With)))

    given jsonAsSqlExpr: AsSqlExpr[Json] with
        def asSqlExpr(x: Json): SqlExpr = 
            SqlExpr.Cast(SqlExpr.StringLiteral(x.toString), SqlType.Json)

    given vectorAsSqlExpr: AsSqlExpr[Vector] with
        def asSqlExpr(x: Vector): SqlExpr = 
            SqlExpr.Vector(SqlExpr.StringLiteral(x.toString))

    given optionAsSqlExpr[T](using a: AsSqlExpr[T]): AsSqlExpr[Option[T]] with
        def asSqlExpr(x: Option[T]): SqlExpr = x match
            case None => SqlExpr.NullLiteral
            case Some(v) => a.asSqlExpr(v)

    given someAsSqlExpr[T](using a: AsSqlExpr[T]): AsSqlExpr[Some[T]] with
        def asSqlExpr(x: Some[T]): SqlExpr =
            a.asSqlExpr(x.value)

    given noneAsSqlExpr: AsSqlExpr[None.type] with
        def asSqlExpr(x: None.type): SqlExpr =
            SqlExpr.NullLiteral