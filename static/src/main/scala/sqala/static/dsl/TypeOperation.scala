package sqala.static.dsl

import sqala.static.dsl.table.*

import java.time.*
import scala.compiletime.ops.boolean.||
import scala.compiletime.ops.int.S

/**
 * Wraps a type with constructor if not already wrapped.
 */
type Wrap[T, F[_]] =
    T match
        case F[t] => T
        case _ => F[T]

/**
 * Conditionally wraps a type constructor.
 */
type WrapIf[T, C <: Boolean, F[_]] =
    C match
        case true => Wrap[T, F]
        case _ => T

/**
 * Unwraps nested type constructor.
 */
type Unwrap[T, F[_]] =
    T match
        case F[t] => Unwrap[t, F]
        case _ => T

/**
 * Flattens nested `Option` and `Array` wrappers for `unnest`.
 */
type FlattenUnnest[T] =
    T match
        case Option[t] => FlattenUnnest[t]
        case Array[t] => FlattenUnnest[t]
        case _ => T

/**
 * Maps an entity field to its expression type, wrapping with
 * `Option` when the entity field is optional.
 */
type MapField[X, T, K[_ <: Int] <: ExprKind, L <: Int] =
    T match
        case Option[_] => Expr[Wrap[X, Option], K[L]]
        case _ => Expr[X, K[L]]

/**
 * Finds the index of a type in a tuple.
 */
type Index[T <: Tuple, X, N <: Int] <: Int =
    T match
        case X *: xs => N
        case x *: xs => Index[xs, X, S[N]]

/**
 * Tests whether a type is `Option`.
 */
type IsOption[T] <: Boolean =
    T match
        case Option[t] => true
        case _ => false

/**
 * Removes duplicate types from a tuple.
 */
type TupleDistinct[T <: Tuple] <: Tuple =
    T match
        case x *: xs => x *: TupleDistinct[TupleFilterNot[x, xs]]
        case EmptyTuple => EmptyTuple

/**
 * Filters out all occurrences of a type from a tuple.
 */
type TupleFilterNot[H, T <: Tuple] <: Tuple =
    T match
        case H *: xs => TupleFilterNot[H, xs]
        case x *: xs => x *: TupleFilterNot[H, xs]
        case EmptyTuple => EmptyTuple

/**
 * Computes the wider numeric result type, wrapping with `Option`
 * if either input is optional.
 */
type NumericResult[A, B] =
    (Unwrap[A, Option], Unwrap[B, Option], IsOption[A] || IsOption[B]) match
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

/**
 * Computes the wider datetime result type, wrapping with `Option`
 * if either input is optional.
 */
type DateTimeResult[A, B] =
    (Unwrap[A, Option], Unwrap[B, Option], IsOption[A] || IsOption[B]) match
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

/**
 * Computes the wider time result type, wrapping with `Option` if
 * either input is optional.
 */
type TimeResult[A, B] =
    (Unwrap[A, Option], Unwrap[B, Option], IsOption[A] || IsOption[B]) match
        case (OffsetTime, _, true) => Option[OffsetTime]
        case (OffsetTime, _, false) => OffsetTime
        case (_, OffsetTime, true) => Option[OffsetTime]
        case (_, OffsetTime, false) => OffsetTime
        case (LocalTime, _, true) => Option[LocalTime]
        case (LocalTime, _, false) => LocalTime
        case (_, LocalTime, true) => Option[LocalTime]
        case (_, LocalTime, false) => LocalTime

/**
 * Flattens nested JSON column names into a flat tuple.
 */
type JsonColumnNameFlatten[N <: Tuple, V <: Tuple] <: Tuple =
    (N, V) match
        case (hn *: tn, hv *: tv) =>
            hv match
                case JsonNestedColumns[n, v] =>
                    Tuple.Concat[JsonColumnNameFlatten[n, v], JsonColumnNameFlatten[tn, tv]]
                case _ =>
                    hn *: JsonColumnNameFlatten[tn, tv]
        case (EmptyTuple, EmptyTuple) => EmptyTuple

/**
 * Flattens nested JSON column types into flat column expressions.
 */
type JsonColumnFlatten[V <: Tuple, L <: Int] <: Tuple =
    V match
        case h *: t =>
            h match
                case JsonNestedColumns[_, v] =>
                    Tuple.Concat[JsonColumnFlatten[v, L], JsonColumnFlatten[t, L]]
                case JsonOrdinalColumn =>
                    Expr[Int, Column[L]] *: JsonColumnFlatten[t, L]
                case JsonPathColumn[pt] =>
                    Expr[Wrap[pt, Option], Column[L]] *: JsonColumnFlatten[t, L]
                case JsonExistsColumn =>
                    Expr[Option[Boolean], Column[L]] *: JsonColumnFlatten[t, L]
        case EmptyTuple => EmptyTuple