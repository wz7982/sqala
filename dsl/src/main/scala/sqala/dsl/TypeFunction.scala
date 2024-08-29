package sqala.dsl

import sqala.dsl.statement.query.NamedQuery

import scala.Tuple.Append
import scala.compiletime.ops.int.S

type Wrap[T, F[_]] = T match
    case F[t] => T
    case _ => F[T]

type Unwrap[T, F[_]] = T match
    case F[t] => t
    case _ => T

type Operation[T] = T | Wrap[T, Option] | Unwrap[T, Option]

type ToTuple[T] <: Tuple = T match
    case h *: t => h *: t
    case EmptyTuple => EmptyTuple
    case _ => Tuple1[T]

type SimpleKind = ColumnKind | CommonKind | ValueKind

type CompositeKind = CommonKind | AggKind | WindowKind

type SortKind = ColumnKind | CommonKind | WindowKind

type OperationKind[T <: ExprKind] <: ExprKind = T match
    case ValueKind => ExprKind
    case CommonKind | ColumnKind | WindowKind => 
        CommonKind | ColumnKind | WindowKind | ValueKind
    case AggKind => AggKind | ValueKind

type ResultKind[L <: ExprKind, R <: ExprKind] <: CompositeKind = (L, R) match
    case (WindowKind, r) => WindowKind
    case (l, WindowKind) => WindowKind
    case (AggKind, r) => AggKind
    case (l, AggKind) => AggKind
    case (l, r) => CommonKind

type CastKind[E <: Expr[?, ?]] <: CompositeKind = E match
    case Expr[_, k] => ResultKind[k, ValueKind]

type InverseMap[T, F[_]] = T match
    case x *: xs => Tuple.InverseMap[x *: xs, F]
    case F[x] => x

type MapField[X, T] = T match
    case Option[_] => Expr[Wrap[X, Option], ColumnKind]
    case _ => Expr[X, ColumnKind]

type Index[T <: Tuple, X, N <: Int] <: Int = T match
    case X *: xs => N
    case x *: xs => Index[xs, X, S[N]]

type MapOption[T] = T match
    case Table[t] => Table[Wrap[t, Option]]
    case Expr[t, k] => Expr[Wrap[t, Option], k]
    case NamedQuery[n, v] => NamedQuery[n, MapOption[v]]
    case x *: xs => TupleMapOption[x *: xs]

type TupleMapOption[T <: Tuple] <: Tuple = T match
    case x *: xs => MapOption[x] *: TupleMapOption[xs]
    case EmptyTuple => EmptyTuple

type InnerJoin[L, R] <: Tuple = L match
    case x *: xs => Append[x *: xs, Table[R]]
    case _ => (L, Table[R])

type InnerJoinQuery[L, R, N] <: Tuple = L match
    case x *: xs => Append[x *: xs, NamedQuery[N, R]]
    case _ => (L, NamedQuery[N, R])

type LeftJoin[L, R] <: Tuple = L match
    case x *: xs => Append[x *: xs, Table[Option[R]]]
    case _ => (L, Table[Option[R]])

type LeftJoinQuery[L, R, N] <: Tuple = L match
    case x *: xs => Append[x *: xs, NamedQuery[N, MapOption[R]]]
    case _ => (L, NamedQuery[N, MapOption[R]])

type RightJoin[L, R] <: Tuple = L match
    case x *: xs => Append[TupleMapOption[x *: xs], Table[R]]
    case _ => (MapOption[L], Table[R])

type RightJoinQuery[L, R, N] <: Tuple = L match
    case x *: xs => Append[TupleMapOption[x *: xs], NamedQuery[N, R]]
    case _ => (MapOption[L], NamedQuery[N, R])

type Union[A <: Tuple, B <: Tuple] <: Tuple = (A, B) match
    case (Expr[a, k] *: at, Expr[b, _] *: bt) => Expr[UnionTo[a, b], k] *: Union[at, bt]
    case (EmptyTuple, EmptyTuple) => EmptyTuple

type UnionTo[A, B] = A match
    case B => B
    case Option[B] => A
    case Unwrap[B, Option] => B