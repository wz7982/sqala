package sqala.static.dsl

import sqala.ast.expr.SqlCastType
import sqala.static.common.*

import java.time.LocalDateTime
import scala.annotation.implicitNotFound

@implicitNotFound("Cannot cast type ${T} to ${R}.")
trait Cast[T, R]:
    def castType: SqlCastType

object Cast:
    given castString[T: AsSqlExpr]: Cast[T, String] with
        def castType: SqlCastType = SqlCastType.Varchar

    given castInt[T: Number]: Cast[T, Int] with
        def castType: SqlCastType = SqlCastType.Int4

    given castLong[T: Number]: Cast[T, Long] with
        def castType: SqlCastType = SqlCastType.Int8

    given castFloat[T: Number]: Cast[T, Float] with
        def castType: SqlCastType = SqlCastType.Float4

    given castDouble[T: Number]: Cast[T, Double] with
        def castType: SqlCastType = SqlCastType.Float8

    given castJson[T <: String | Option[String]]: Cast[T, Json] with
        def castType: SqlCastType = SqlCastType.Json

    given castDate[T <: String | Option[String]]: Cast[T, LocalDateTime] with
        def castType: SqlCastType = SqlCastType.DateTime