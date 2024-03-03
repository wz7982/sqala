package sqala.dynamic

import sqala.ast.expr.SqlExpr

import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.Date

trait AsSqlExpr[T]:
    def asSqlExpr(x: T): SqlExpr

object AsSqlExpr:
    given intAsSqlExpr: AsSqlExpr[Int] with
        override def asSqlExpr(x: Int): SqlExpr = SqlExpr.NumberLiteral(x)

    given longAsSqlExpr: AsSqlExpr[Long] with
        override def asSqlExpr(x: Long): SqlExpr = SqlExpr.NumberLiteral(x)

    given floatAsSqlExpr: AsSqlExpr[Float] with
        override def asSqlExpr(x: Float): SqlExpr = SqlExpr.NumberLiteral(x)

    given doubleAsSqlExpr: AsSqlExpr[Double] with
        override def asSqlExpr(x: Double): SqlExpr = SqlExpr.NumberLiteral(x)

    given decimalAsSqlExpr: AsSqlExpr[BigDecimal] with
        override def asSqlExpr(x: BigDecimal): SqlExpr = SqlExpr.NumberLiteral(x)

    given stringAsSqlExpr: AsSqlExpr[String] with
        override def asSqlExpr(x: String): SqlExpr = SqlExpr.StringLiteral(x)

    given booleanAsSqlExpr: AsSqlExpr[Boolean] with
        override def asSqlExpr(x: Boolean): SqlExpr = SqlExpr.BooleanLiteral(x)

    given dateAsSqlExpr: AsSqlExpr[Date] with
        override def asSqlExpr(x: Date): SqlExpr = SqlExpr.DateLiteral(x)

    given localDateAsSqlExpr: AsSqlExpr[LocalDate] with
        override def asSqlExpr(x: LocalDate): SqlExpr =
            SqlExpr.DateLiteral(Date.from(x.atStartOfDay(ZoneId.systemDefault()).nn.toInstant()).nn)

    given localDateTimeAsSqlExpr: AsSqlExpr[LocalDateTime] with
        override def asSqlExpr(x: LocalDateTime): SqlExpr =
            SqlExpr.DateLiteral(Date.from(x.atZone(ZoneId.systemDefault()).nn.toInstant()).nn)

    given optionAsSqlExpr[T](using a: AsSqlExpr[T]): AsSqlExpr[Option[T]] with
        override def asSqlExpr(x: Option[T]): SqlExpr = x match
            case None => SqlExpr.Null
            case Some(v) => a.asSqlExpr(v)