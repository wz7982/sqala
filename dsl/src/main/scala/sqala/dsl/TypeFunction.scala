package sqala.dsl

import sqala.dsl.statement.query.{NamedQuery, NamedTupleWrapper}

import scala.NamedTuple.NamedTuple
import scala.Tuple.Append
import scala.compiletime.ops.int.S

type Wrap[T, F[_]] = T match
    case F[t] => T
    case _ => F[T]

type Unwrap[T, F[_]] = T match
    case F[t] => t
    case _ => T

type Operation[T] = T | Wrap[T, Option] | Unwrap[T, Option]

type MapField[X, T] = T match
    case Option[_] => Column[Wrap[X, Option]]
    case _ => Column[X]

type InverseMap[T, F[_]] = T match
    case x *: xs => Tuple.InverseMap[x *: xs, F]
    case F[x] => x

type Result[T] = T match
    case Expr[t] => t
    case Table[t] => t
    case h *: t => TupleResult[h *: t]
    case NamedQuery[n, v] => NamedTuple[n, Result[v]]
    case NamedTupleWrapper[n, v] => NamedTuple[n, Result[v]]
    
type TupleResult[T <: Tuple] = T match
    case EmptyTuple => EmptyTuple
    case x *: xs => Result[x] *: TupleResult[xs]

type Index[T <: Tuple, X, N <: Int] <: Int = T match
    case X *: xs => N
    case x *: xs => Index[xs, X, S[N]]

type MapOption[T] = T match
    case Table[t] => Table[Wrap[t, Option]]
    case Expr[t] => Expr[Wrap[t, Option]]
    case NamedQuery[n, v] => NamedQuery[n, MapOption[v]]
    case x *: xs => TupleMapOption[x *: xs]

type TupleMapOption[T <: Tuple] <: Tuple = T match
    case x *: xs => MapOption[x] *: TupleMapOption[xs]
    case EmptyTuple => EmptyTuple

type NamedTupleMapOption[T <: NamedTupleWrapper[?, ?]] = T match
    case NamedTupleWrapper[n, v] => NamedTupleWrapper[n, TupleMapOption[v]]

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

type FullJoin[L, R] <: Tuple = L match
    case x *: xs => Append[TupleMapOption[x *: xs], Table[Option[R]]]
    case _ => (MapOption[L], Table[Option[R]])

type FullJoinQuery[L, R, N] <: Tuple = L match
    case x *: xs => Append[TupleMapOption[x *: xs], NamedQuery[N, MapOption[R]]]
    case _ => (MapOption[L], NamedQuery[N, MapOption[R]])

type Union[A <: Tuple, B <: Tuple] <: Tuple = (A, B) match
    case (Expr[a] *: at, Expr[b] *: bt) => Expr[UnionTo[a, b]] *: Union[at, bt]
    case (EmptyTuple, EmptyTuple) => EmptyTuple

type UnionTo[A, B] = A match
    case B => B
    case Option[B] => A
    case Unwrap[B, Option] => B