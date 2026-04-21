package sqala.static.dsl

import sqala.ast.expr.{SqlTimeZoneMode, SqlType}
import sqala.static.metadata.*

import java.time.{LocalDateTime, OffsetDateTime}

trait Cast[T, R]:
    def castType: SqlType

object Cast:
    given string[T: AsSqlExpr]: Cast[T, String] with
        def castType: SqlType = SqlType.Varchar(None)

    given int[T: SqlNumber]: Cast[T, Int] with
        def castType: SqlType = SqlType.Int

    given long[T: SqlNumber]: Cast[T, Long] with
        def castType: SqlType = SqlType.Long

    given float[T: SqlNumber]: Cast[T, Float] with
        def castType: SqlType = SqlType.Float

    given double[T: SqlNumber]: Cast[T, Double] with
        def castType: SqlType = SqlType.Double

    given decimal[T: SqlNumber]: Cast[T, Double] with
        def castType: SqlType = SqlType.Decimal(None)

    given json[T: SqlString]: Cast[T, Json] with
        def castType: SqlType = SqlType.Json

    given localDateTime[T: SqlString]: Cast[T, LocalDateTime] with
        def castType: SqlType = SqlType.Timestamp(None)

    given offsetDateTime[T: SqlString]: Cast[T, OffsetDateTime] with
        def castType: SqlType = SqlType.Timestamp(Some(SqlTimeZoneMode.With))

    given vector[T: SqlString]: Cast[T, Vector] with
        def castType: SqlType = SqlType.Vector