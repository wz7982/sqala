package sqala.dsl

import scala.compiletime.ops.int.S

type Wrap[T, F[_]] = T match
    case F[t] => T
    case _ => F[T]

type Unwrap[T, F[_]] = T match
    case F[t] => t
    case _ => T

type InverseMap[T, F[_]] = T match
    case x *: xs => Tuple.InverseMap[x *: xs, F]
    case F[x] => x

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
    case (Float, _, true) => Option[Double]
    case (Float, _, false) => Double
    case (_, Float, true) => Option[Double]
    case (_, Float, false) => Double
    case (Long, _, true) => Option[Long]
    case (Long, _, false) => Long
    case (_, Long, true) => Option[Long]
    case (_, Long, false) => Long
    case (Int, _, true) => Option[Int]
    case (Int, _, false) => Int
    case (_, Int, true) => Option[Int]
    case (_, Int, false) => Int