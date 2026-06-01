package sqala.static.dsl

import sqala.static.dsl.table.*

import java.time.*
import scala.compiletime.ops.any.ToString
import scala.compiletime.ops.boolean.{!, ||}
import scala.compiletime.ops.int.S
import scala.compiletime.ops.string.{+, Length, Substring}

type Wrap[T, F[_]] = T match
    case F[t] => T
    case _ => F[T]

type WrapIf[T, C <: Boolean, F[_]] = C match
    case true => Wrap[T, F]
    case _ => T

type Unwrap[T, F[_]] = T match
    case F[t] => Unwrap[t, F]
    case _ => T

type TupleMap[T <: Tuple, F[_]] <: Tuple =
    T match
        case x *: xs => F[x] *: TupleMap[xs, F]
        case EmptyTuple => EmptyTuple

type FlattenUnnest[T] = T match
    case Option[t] => FlattenUnnest[t]
    case Array[t] => FlattenUnnest[t]
    case _ => T

type MapField[X, T, K[_ <: Int] <: ExprKind, L <: Int] = T match
    case Option[_] => Expr[Wrap[X, Option], K[L]]
    case _ => Expr[X, K[L]]

type Index[T <: Tuple, X, N <: Int] <: Int = T match
    case X *: xs => N
    case x *: xs => Index[xs, X, S[N]]

type IsOption[T] <: Boolean = T match
    case Option[t] => true
    case _ => false

type TupleDistinct[T <: Tuple] <: Tuple = T match
    case x *: xs => x *: TupleDistinct[TupleFilterNot[x, xs]]
    case EmptyTuple => EmptyTuple

type TupleFilterNot[H, T <: Tuple] <: Tuple = T match
    case H *: xs => TupleFilterNot[H, xs]
    case x *: xs => x *: TupleFilterNot[H, xs]
    case EmptyTuple => EmptyTuple

type NumericResult[A, B] = (Unwrap[A, Option], Unwrap[B, Option], IsOption[A] || IsOption[B]) match
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

type DateTimeResult[A, B] = (Unwrap[A, Option], Unwrap[B, Option], IsOption[A] || IsOption[B]) match
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

type TimeResult[A, B] = (Unwrap[A, Option], Unwrap[B, Option], IsOption[A] || IsOption[B]) match
    case (OffsetTime, _, true) => Option[OffsetTime]
    case (OffsetTime, _, false) => OffsetTime
    case (_, OffsetTime, true) => Option[OffsetTime]
    case (_, OffsetTime, false) => OffsetTime
    case (LocalTime, _, true) => Option[LocalTime]
    case (LocalTime, _, false) => LocalTime
    case (_, LocalTime, true) => Option[LocalTime]
    case (_, LocalTime, false) => LocalTime

type JsonColumnNameFlatten[N <: Tuple, V <: Tuple] <: Tuple = (N, V) match
    case (hn *: tn, hv *: tv) =>
        hv match
            case JsonNestedColumns[n, v] =>
                Tuple.Concat[JsonColumnNameFlatten[n, v], JsonColumnNameFlatten[tn, tv]]
            case _ =>
                hn *: JsonColumnNameFlatten[tn, tv]
    case (EmptyTuple, EmptyTuple) => EmptyTuple

type JsonColumnFlatten[V <: Tuple, L <: Int] <: Tuple = V match
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

type UpperCase[S <: String] =
    S match
        case "a" => "A"
        case "b" => "B"
        case "c" => "C"
        case "d" => "D"
        case "e" => "E"
        case "f" => "F"
        case "g" => "G"
        case "h" => "H"
        case "i" => "I"
        case "j" => "J"
        case "k" => "K"
        case "l" => "L"
        case "m" => "M"
        case "n" => "N"
        case "o" => "O"
        case "p" => "P"
        case "q" => "Q"
        case "r" => "R"
        case "s" => "S"
        case "t" => "T"
        case "u" => "U"
        case "v" => "V"
        case "w" => "W"
        case "x" => "X"
        case "y" => "Y"
        case "z" => "Z"
        case _ => S

type CombinePivotNames[A <: Tuple, F <: Tuple] =
    Tuple.FlatMap[
        A,
        [i] =>> Tuple.Map[
            CombinePivotForNames[F],
            [ii] =>> ToString[i] + UpperCase[Substring[ToString[ii], 0, 1]] + Substring[ToString[ii], 1, Length[ToString[ii]]]
        ]
    ]

type CombinePivotTypes[T <: Tuple, F <: Tuple] =
    Tuple.FlatMap[
        T,
        [i] =>> Tuple.Map[
            CombinePivotForNames[F],
            [ii] =>> i
        ]
    ]

type CombinePivotForNames[F <: Tuple] <: Tuple =
    F match
        case EmptyTuple => EmptyTuple
        case (x *: xs) *: EmptyTuple => x *: xs
        case x *: xs =>
            Tuple.FlatMap[
                x,
                [i] =>> Tuple.Map[
                    CombinePivotForNames[xs],
                    [ii] =>> i + UpperCase[Substring[ii, 0, 1]] + Substring[ii, 1, Length[ii]]
                ]
            ]

type InTuple[X, T <: Tuple] <: Boolean =
    T match
        case X *: xs => true
        case x *: xs => InTuple[X, xs]
        case EmptyTuple => false

type NameFilter[N <: Tuple, P[_] <: Boolean] <: Tuple =
    N match
        case n *: ns =>
            P[n] match
                case true => n *: NameFilter[ns, P]
                case false => NameFilter[ns, P]
        case EmptyTuple => EmptyTuple

type ValueFilter[N <: Tuple, V <: Tuple, P[_] <: Boolean] <: Tuple =
    (N, V) match
        case (n *: ns, v *: vs) =>
            P[n] match
                case true => v *: ValueFilter[ns, vs, P]
                case false => ValueFilter[ns, vs, P]
        case (EmptyTuple, EmptyTuple) => EmptyTuple

type ExcludeName[EN <: Tuple, N <: Tuple] =
    NameFilter[N, [x] =>> ![InTuple[x, EN]]]

type ExcludeValue[EN <: Tuple, N <: Tuple, V <: Tuple] =
    ValueFilter[N, V, [x] =>> ![InTuple[x, EN]]]