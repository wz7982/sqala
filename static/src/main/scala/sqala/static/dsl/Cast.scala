package sqala.static.dsl

import sqala.ast.expr.{SqlTimeZoneMode, SqlType}
import sqala.static.metadata.{AsSqlExpr, Json, SqlNumber, SqlString}

import java.time.{LocalDateTime, OffsetDateTime}
import scala.annotation.implicitNotFound

@implicitNotFound("Cannot cast type ${T} to ${R}.")
trait Cast[T, R]:
    def castType: SqlType

object Cast:
    given castString[T: AsSqlExpr]: Cast[T, String] with
        def castType: SqlType = SqlType.Varchar(None)

    given castInt[T: SqlNumber]: Cast[T, Int] with
        def castType: SqlType = SqlType.Int

    given castLong[T: SqlNumber]: Cast[T, Long] with
        def castType: SqlType = SqlType.Long

    given castFloat[T: SqlNumber]: Cast[T, Float] with
        def castType: SqlType = SqlType.Float

    given castDouble[T: SqlNumber]: Cast[T, Double] with
        def castType: SqlType = SqlType.Double

    given castDecimal[T: SqlNumber]: Cast[T, Double] with
        def castType: SqlType = SqlType.Decimal(None)

    given castJson[T: SqlString]: Cast[T, Json] with
        def castType: SqlType = SqlType.Json

    given castLocalDateTime[T: SqlString]: Cast[T, LocalDateTime] with
        def castType: SqlType = SqlType.Timestamp(None)

    given castOffsetDateTime[T: SqlString]: Cast[T, OffsetDateTime] with
        def castType: SqlType = SqlType.Timestamp(Some(SqlTimeZoneMode.With))