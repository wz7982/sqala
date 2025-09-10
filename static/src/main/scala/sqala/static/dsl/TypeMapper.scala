package sqala.static.dsl

import java.time.*
import scala.compiletime.ops.int.S

type Wrap[T, F[_]] = T match
    case F[t] => T
    case _ => F[T]

type Unwrap[T, F[_]] = T match
    case F[t] => t
    case _ => T

type UnnestFlatten[T] = T match
    case Option[t] => UnnestFlatten[t]
    case Array[t] => UnnestFlatten[t]
    case _ => T

type MapField[X, T] = T match
    case Option[_] => Expr[Wrap[X, Option]]
    case _ => Expr[X]

type Index[T <: Tuple, X, N <: Int] <: Int = T match
    case X *: xs => N
    case x *: xs => Index[xs, X, S[N]]

type IsOption[T] <: Boolean = T match
    case Option[t] => true
    case _ => false

type NumericResult[L, R, N <: Boolean] = (L, R, N) match
    case (BigDecimal, _, true) => Option[BigDecimal]
    case (BigDecimal, _, false) => BigDecimal
    case (_, BigDecimal, true) => Option[BigDecimal]
    case (_, BigDecimal, false) => BigDecimal
    case (Double, _, true) => Option[Double]
    case (Double, _, false) => Double
    case (_, Double, true) => Option[Double]
    case (_, Double, false) => Double
    case (Float, _, true) => Option[Float]
    case (Float, _, false) => Float
    case (_, Float, true) => Option[Float]
    case (_, Float, false) => Float
    case (Long, _, true) => Option[Long]
    case (Long, _, false) => Long
    case (_, Long, true) => Option[Long]
    case (_, Long, false) => Long
    case (Int, _, true) => Option[Int]
    case (Int, _, false) => Int
    case (_, Int, true) => Option[Int]
    case (_, Int, false) => Int

type DateTimeResult[L, R, N <: Boolean] = (L, R, N) match
    case (OffsetDateTime, _, true) => Option[OffsetDateTime]
    case (OffsetDateTime, _, false) => OffsetDateTime
    case (_, OffsetDateTime, true) => Option[OffsetDateTime]
    case (_, OffsetDateTime, false) => OffsetDateTime
    case (LocalDateTime, _, true) => Option[LocalDateTime]
    case (LocalDateTime, _, false) => LocalDateTime
    case (_, LocalDateTime, true) => Option[LocalDateTime]
    case (_, LocalDateTime, false) => LocalDateTime
    case (LocalDate, _, true) => Option[LocalDate]
    case (LocalDate, _, false) => LocalDate
    case (_, LocalDate, true) => Option[LocalDate]
    case (_, LocalDate, false) => LocalDate

type TimeResult[L, R, N <: Boolean] = (L, R, N) match
    case (OffsetTime, _, true) => Option[OffsetTime]
    case (OffsetTime, _, false) => OffsetTime
    case (_, OffsetTime, true) => Option[OffsetTime]
    case (_, OffsetTime, false) => OffsetTime
    case (LocalTime, _, true) => Option[LocalTime]
    case (LocalTime, _, false) => LocalTime
    case (_, LocalTime, true) => Option[LocalTime]
    case (_, LocalTime, false) => LocalTime