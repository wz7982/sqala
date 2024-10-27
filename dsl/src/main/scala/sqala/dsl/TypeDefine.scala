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
    case Option[_] => Expr[Wrap[X, Option], ColumnKind]
    case _ => Expr[X, ColumnKind]

type Index[T <: Tuple, X, N <: Int] <: Int = T match
    case X *: xs => N
    case x *: xs => Index[xs, X, S[N]]

type SimpleKind = ColumnKind | CommonKind | ValueKind

type CompositeKind = CommonKind | AggOperationKind | WindowKind

type SortKind = ColumnKind | CommonKind | WindowKind

type FuncKind = CommonKind | AggKind | AggOperationKind | WindowKind

type ResultKind[L <: ExprKind, R <: ExprKind] <: CompositeKind = (L, R) match
    case (WindowKind, r) => WindowKind
    case (l, WindowKind) => WindowKind
    case (AggKind | AggOperationKind | GroupKind, r) => AggOperationKind
    case (l, AggKind | AggOperationKind | GroupKind) => AggOperationKind
    case (l, r) => CommonKind