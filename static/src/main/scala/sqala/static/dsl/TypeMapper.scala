package sqala.static.dsl

import sqala.static.dsl.table.{JsonTableExistsColumn, JsonTableNestedColumns, JsonTableOrdinalColumn, JsonTablePathColumn}

import java.time.*
import scala.compiletime.ops.any.ToString
import scala.compiletime.ops.int.S
import scala.compiletime.ops.string.+

type Wrap[T, F[_]] = T match
    case F[t] => T
    case _ => F[T]

type Unwrap[T, F[_]] = T match
    case F[t] => t
    case _ => T

type TupleMap[T <: Tuple, F[_]] <: Tuple =
    T match
        case x *: xs => F[x] *: TupleMap[xs, F]
        case EmptyTuple => EmptyTuple

type UnnestFlattenImpl[T] = T match
    case Option[t] => UnnestFlattenImpl[t]
    case Array[t] => UnnestFlattenImpl[t]
    case _ => T

trait UnnestFlatten[T]:
    type R

object UnnestFlatten:
    type Aux[T, O] = UnnestFlatten[T]:
        type R = O

    given flatten[T]: Aux[T, UnnestFlattenImpl[T]] =
        new UnnestFlatten[T]:
            type R = UnnestFlattenImpl[T]

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

type JsonTableColumnNameFlatten[N <: Tuple, V <: Tuple] <: Tuple = (N, V) match
    case (hn *: tn, hv *: tv) =>
        hv match
            case JsonTableNestedColumns[n, v] =>
                Tuple.Concat[JsonTableColumnNameFlatten[n, v], JsonTableColumnNameFlatten[tn, tv]]
            case _ => 
                hn *: JsonTableColumnNameFlatten[tn, tv]
    case (EmptyTuple, EmptyTuple) => 
        EmptyTuple

type JsonTableColumnFlatten[V <: Tuple] <: Tuple = V match
    case h *: t =>
        h match
            case JsonTableNestedColumns[_, v] =>
                Tuple.Concat[JsonTableColumnFlatten[v], JsonTableColumnFlatten[t]]
            case JsonTableOrdinalColumn => 
                Expr[Int] *: JsonTableColumnFlatten[t]
            case JsonTablePathColumn[pt] =>
                Expr[Wrap[pt, Option]] *: JsonTableColumnFlatten[t]
            case JsonTableExistsColumn =>
                Expr[Option[Boolean]] *: JsonTableColumnFlatten[t]
    case EmptyTuple => 
        EmptyTuple

type CombinePivotNames[A <: Tuple, F <: Tuple] =
    Tuple.FlatMap[
        A,
        [i] =>> Tuple.Map[
            CombinePivotForNames[F],
            [ii] =>> ToString[i] + "_" + ToString[ii]
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
                    [ii] =>> i + "_" + ii
                ]
            ]

type KindResult[L <: ExprKind, R <: ExprKind] =
    (L, R) match
        case (Field, Field) => Operation
        case (Field, Operation) => Operation
        case (Field, Value) => Operation
        case (Field, ValueOperation) => Operation
        case (Field, WindowOver) => WindowOver
        case (Field, WindowOverEmpty) => WindowOver
        case (Field, Grouped) => Operation
        case (Field, WithoutGrouping) => WithoutGrouping

        case (Operation, Field) => Operation
        case (Operation, Operation) => Operation
        case (Operation, Value) => Operation
        case (Operation, ValueOperation) => Operation
        case (Operation, WindowOver) => WindowOver
        case (Operation, WindowOverEmpty) => WindowOver
        case (Operation, Grouped) => Operation
        case (Operation, WithoutGrouping) => WithoutGrouping

        case (Value, Field) => Operation
        case (Value, Operation) => Operation
        case (Value, Value) => ValueOperation
        case (Value, ValueOperation) => ValueOperation
        case (Value, Agg) => AggOperation
        case (Value, AggOperation) => AggOperation
        case (Value, WindowOver) => WindowOver
        case (Value, WindowOverEmpty) => WindowOverEmpty
        case (Value, WindowOverAgg) => WindowOverAgg
        case (Value, Grouped) => Grouped
        case (Value, WithoutGrouping) => WithoutGrouping
        
        case (ValueOperation, Field) => Operation
        case (ValueOperation, Operation) => Operation
        case (ValueOperation, Value) => ValueOperation
        case (ValueOperation, ValueOperation) => ValueOperation
        case (ValueOperation, Agg) => AggOperation
        case (ValueOperation, AggOperation) => AggOperation
        case (ValueOperation, WindowOver) => WindowOver
        case (ValueOperation, WindowOverEmpty) => WindowOverEmpty
        case (ValueOperation, WindowOverAgg) => WindowOverAgg
        case (ValueOperation, Grouped) => Grouped
        case (ValueOperation, WithoutGrouping) => WithoutGrouping

        case (Agg, Value) => AggOperation
        case (Agg, ValueOperation) => AggOperation
        case (Agg, Agg) => AggOperation
        case (Agg, AggOperation) => AggOperation
        case (Agg, WindowOverEmpty) => AggOperation
        case (Agg, WindowOverAgg) => AggOperation
        case (Agg, Grouped) => AggOperation

        case (AggOperation, Value) => AggOperation
        case (AggOperation, ValueOperation) => AggOperation
        case (AggOperation, Agg) => AggOperation
        case (AggOperation, AggOperation) => AggOperation
        case (AggOperation, WindowOverEmpty) => AggOperation
        case (AggOperation, WindowOverAgg) => AggOperation
        case (AggOperation, Grouped) => AggOperation

        case (WindowOver, Field) => WindowOver
        case (WindowOver, Operation) => WindowOver
        case (WindowOver, Value) => WindowOver
        case (WindowOver, ValueOperation) => WindowOver
        case (WindowOver, WindowOver) => WindowOver
        case (WindowOver, WindowOverEmpty) => WindowOver
        case (WindowOver, Grouped) => WindowOver

        case (WindowOverEmpty, Field) => WindowOver
        case (WindowOverEmpty, Operation) => WindowOver
        case (WindowOverEmpty, Value) => WindowOverEmpty
        case (WindowOverEmpty, ValueOperation) => WindowOverEmpty
        case (WindowOverEmpty, Agg) => AggOperation
        case (WindowOverEmpty, AggOperation) => AggOperation
        case (WindowOverEmpty, WindowOver) => WindowOver
        case (WindowOverEmpty, WindowOverEmpty) => WindowOverEmpty
        case (WindowOverEmpty, WindowOverAgg) => WindowOverAgg
        case (WindowOverEmpty, Grouped) => WindowOverEmpty

        case (WindowOverAgg, Value) => WindowOverAgg
        case (WindowOverAgg, ValueOperation) => WindowOverAgg
        case (WindowOverAgg, Agg) => AggOperation
        case (WindowOverAgg, AggOperation) => AggOperation
        case (WindowOverAgg, WindowOverEmpty) => WindowOverAgg
        case (WindowOverAgg, WindowOverAgg) => WindowOverAgg
        case (WindowOverAgg, Grouped) => WindowOverAgg

        case (Grouped, Field) => Operation
        case (Grouped, Operation) => Operation
        case (Grouped, Value) => Grouped
        case (Grouped, ValueOperation) => Grouped
        case (Grouped, Agg) => AggOperation
        case (Grouped, AggOperation) => AggOperation
        case (Grouped, WindowOver) => WindowOver
        case (Grouped, WindowOverEmpty) => WindowOverEmpty
        case (Grouped, WindowOverAgg) => WindowOverAgg
        case (Grouped, Grouped) => Grouped
        case (Grouped, WithoutGrouping) => WithoutGrouping

        case (WithoutGrouping, Field) => WithoutGrouping
        case (WithoutGrouping, Operation) => WithoutGrouping
        case (WithoutGrouping, Value) => WithoutGrouping
        case (WithoutGrouping, ValueOperation) => WithoutGrouping
        case (WithoutGrouping, Grouped) => WithoutGrouping
        case (WithoutGrouping, WithoutGrouping) => WithoutGrouping