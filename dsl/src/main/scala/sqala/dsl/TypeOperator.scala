package sqala.dsl

import sqala.dsl.statement.query.ResultSize

import scala.compiletime.ops.boolean.&&

type Wrap[T, F[_]] = T match
    case F[t] => T
    case _ => F[T]

type Unwrap[T, F[_]] = T match
    case F[t] => t
    case _ => T

type ToTuple[T] <: Tuple = T match
    case h *: t => h *: t
    case EmptyTuple => EmptyTuple
    case _ => Tuple1[T]

type InverseMap[T, F[_]] = T match
    case x *: xs => Tuple.InverseMap[x *: xs, F]
    case F[x] => x

type CheckOverPartition[T] <: Boolean = T match
    case x *: xs => CheckOverPartition[x] && CheckOverPartition[xs]
    case EmptyTuple => true
    case Expr[_, k] => k match
        case SimpleKind => true
        case _ => false

type CheckOverOrder[T] <: Boolean = T match
    case x *: xs => CheckOverOrder[x] && CheckOverOrder[xs]
    case EmptyTuple => true
    case OrderBy[_, k] => k match
        case ColumnKind | CommonKind => true
        case _ => false

type CheckGrouping[T] <: Boolean = T match
    case x *: xs => CheckGrouping[x] && CheckGrouping[xs]
    case EmptyTuple => true
    case Expr[_, k] => k match
        case GroupKind => true
        case _ => false

type QuerySize[N <: Int] <: ResultSize = N match
    case 1 => ResultSize.OneRow
    case _ => ResultSize.ManyRows

type ProjectionSize[IsAgg <: Boolean] <: ResultSize = IsAgg match
    case true => ResultSize.OneRow
    case _ => ResultSize.ManyRows