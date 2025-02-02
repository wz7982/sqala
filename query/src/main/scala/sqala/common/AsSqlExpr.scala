package sqala.common

import sqala.ast.expr.*

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import scala.annotation.implicitNotFound
import scala.compiletime.{erasedValue, summonInline}

@implicitNotFound("The type ${T} cannot be converted to SQL expression.")
trait AsSqlExpr[T]:
    def asSqlExpr(x: T): SqlExpr

    def asPreparedSqlExpr(x: T): SqlExpr

object AsSqlExpr:
    inline def summonInstances[T]: List[AsSqlExpr[?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[AsSqlExpr[t]] :: summonInstances[ts]
            case _ => summonInline[AsSqlExpr[T]] :: Nil

    given intAsSqlExpr: AsSqlExpr[Int] with
        def asSqlExpr(x: Int): SqlExpr = SqlExpr.NumberLiteral(x)

        def asPreparedSqlExpr(x: Int): SqlExpr = SqlExpr.PreparedParam(x)

    given longAsSqlExpr: AsSqlExpr[Long] with
        def asSqlExpr(x: Long): SqlExpr = SqlExpr.NumberLiteral(x)

        def asPreparedSqlExpr(x: Long): SqlExpr = SqlExpr.PreparedParam(x)

    given floatAsSqlExpr: AsSqlExpr[Float] with
        def asSqlExpr(x: Float): SqlExpr = SqlExpr.NumberLiteral(x)

        def asPreparedSqlExpr(x: Float): SqlExpr = SqlExpr.PreparedParam(x)

    given doubleAsSqlExpr: AsSqlExpr[Double] with
        def asSqlExpr(x: Double): SqlExpr = SqlExpr.NumberLiteral(x)

        def asPreparedSqlExpr(x: Double): SqlExpr = SqlExpr.PreparedParam(x)

    given decimalAsSqlExpr: AsSqlExpr[BigDecimal] with
        def asSqlExpr(x: BigDecimal): SqlExpr = SqlExpr.NumberLiteral(x)

        def asPreparedSqlExpr(x: BigDecimal): SqlExpr = SqlExpr.PreparedParam(x)

    given stringAsSqlExpr: AsSqlExpr[String] with
        def asSqlExpr(x: String): SqlExpr = SqlExpr.StringLiteral(x)

        def asPreparedSqlExpr(x: String): SqlExpr = SqlExpr.PreparedParam(x)

    given booleanAsSqlExpr: AsSqlExpr[Boolean] with
        def asSqlExpr(x: Boolean): SqlExpr = SqlExpr.BooleanLiteral(x)

        def asPreparedSqlExpr(x: Boolean): SqlExpr = SqlExpr.PreparedParam(x)

    given localDateAsSqlExpr: AsSqlExpr[LocalDate] with
        def asSqlExpr(x: LocalDate): SqlExpr =
            val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Date, formatter.format(x))

        def asPreparedSqlExpr(x: LocalDate): SqlExpr =
            val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            SqlExpr.PreparedParam(formatter.format(x))

    given localDateTimeAsSqlExpr: AsSqlExpr[LocalDateTime] with
        def asSqlExpr(x: LocalDateTime): SqlExpr =
            val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Timestamp, formatter.format(x))

        def asPreparedSqlExpr(x: LocalDateTime): SqlExpr =
            val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            SqlExpr.PreparedParam(formatter.format(x))

    given jsonAsSqlExpr: AsSqlExpr[Json] with
        def asSqlExpr(x: Json): SqlExpr = SqlExpr.StringLiteral(x.toString)

        def asPreparedSqlExpr(x: Json): SqlExpr = SqlExpr.PreparedParam(x.toString)

    given optionAsSqlExpr[T](using a: AsSqlExpr[T]): AsSqlExpr[Option[T]] with
        def asSqlExpr(x: Option[T]): SqlExpr = x match
            case None => SqlExpr.Null
            case Some(v) => a.asSqlExpr(v)

        def asPreparedSqlExpr(x: Option[T]): SqlExpr = x match
            case None => SqlExpr.Null
            case Some(v) => a.asPreparedSqlExpr(v)

    given someAsSqlExpr[T](using a: AsSqlExpr[T]): AsSqlExpr[Some[T]] with
        def asSqlExpr(x: Some[T]): SqlExpr =
            a.asSqlExpr(x.value)

        def asPreparedSqlExpr(x: Some[T]): SqlExpr =
            a.asPreparedSqlExpr(x.value)

    given noneAsSqlExpr: AsSqlExpr[None.type] with
        def asSqlExpr(x: None.type): SqlExpr =
            SqlExpr.Null

        def asPreparedSqlExpr(x: None.type): SqlExpr =
            SqlExpr.Null