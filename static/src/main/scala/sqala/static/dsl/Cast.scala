package sqala.static.dsl

import sqala.ast.expr.{SqlTimeZoneMode, SqlType}
import sqala.metadata.{AsSqlExpr, Json, Number}

import java.time.{LocalDateTime, OffsetDateTime}
import scala.annotation.implicitNotFound

@implicitNotFound("Cannot cast type ${T} to ${R}.")
trait Cast[T, R]:
    def castType: SqlType

object Cast:
    given castString[T: AsSqlExpr]: Cast[T, String] with
        def castType: SqlType = SqlType.Varchar(None)

    given castInt[T: Number]: Cast[T, Int] with
        def castType: SqlType = SqlType.Int

    given castLong[T: Number]: Cast[T, Long] with
        def castType: SqlType = SqlType.Long

    given castFloat[T: Number]: Cast[T, Float] with
        def castType: SqlType = SqlType.Float

    given castDouble[T: Number]: Cast[T, Double] with
        def castType: SqlType = SqlType.Double

    given castDecimal[T: Number]: Cast[T, Double] with
        def castType: SqlType = SqlType.Decimal(None)

    given castJson[T <: String | Option[String]]: Cast[T, Json] with
        def castType: SqlType = SqlType.Json

    given castLocalDateTime[T <: String | Option[String]]: Cast[T, LocalDateTime] with
        def castType: SqlType = SqlType.Timestamp(None)

    given castOffsetDateTime[T <: String | Option[String]]: Cast[T, OffsetDateTime] with
        def castType: SqlType = SqlType.Timestamp(Some(SqlTimeZoneMode.With))