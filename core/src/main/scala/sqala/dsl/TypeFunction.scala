package sqala.dsl

import sqala.dsl.statement.query.NamedQuery

import scala.Tuple.Concat

type Wrap[T, F[_]] = T match
    case F[t] => T
    case _ => F[T]

type Unwrap[T, F[_]] = T match
    case F[t] => t
    case _ => T

type Operation[T] = T | Wrap[T, Option] | Unwrap[T, Option]

type Map[T, F[_]] = T match
    case x *: xs => F[x] *: Map[xs, F]
    case EmptyTuple => EmptyTuple
    case _ => F[T]

type InverseMap[T, F[_]] = T match
    case F[x] *: xs => x *: InverseMap[xs, F]
    case EmptyTuple => EmptyTuple
    case F[x] => x

type Element[Names <: Tuple, Types <: Tuple, Name <: String] = (Names, Types) match
    case (Name *: _, x *: _) => x
    case (_ *: ns, _ *: xs) => Element[ns, xs, Name]

type Result[R] = R match
    case Expr[t] => t
    case Table[t] => t
    case NamedQuery[t] => Result[t]
    case x *: xs => Result[x] *: Result[xs]
    case EmptyTuple => EmptyTuple

type MapOption[T] = T match
    case Table[t] => Table[Wrap[t, Option]]
    case Expr[t] => Expr[Wrap[t, Option]]
    case NamedQuery[t] => NamedQuery[MapOption[t]]
    case x *: xs => TupleMapOption[x *: xs]

type TupleMapOption[T <: Tuple] <: Tuple = T match
    case x *: xs => MapOption[x] *: TupleMapOption[xs]
    case EmptyTuple => EmptyTuple

type AsTuple[T] <: Tuple = T match
    case x *: xs => x *: xs
    case _ => Tuple1[T]

type InnerJoin[L, R] <: Tuple = L match
    case x *: xs => Concat[x *: xs, Tuple1[Table[R]]]
    case _ => (L, Table[R])

type InnerJoinQuery[L, R] <: Tuple = L match
    case x *: xs => Concat[x *: xs, Tuple1[NamedQuery[R]]]
    case _ => (L, NamedQuery[R])

type LeftJoin[L, R] <: Tuple = L match
    case x *: xs => Concat[x *: xs, Tuple1[Table[Option[R]]]]
    case _ => (L, Table[Option[R]])

type LeftJoinQuery[L, R] <: Tuple = L match
    case x *: xs => Concat[x *: xs, Tuple1[NamedQuery[MapOption[R]]]]
    case _ => (L, NamedQuery[MapOption[R]])

type RightJoin[L, R] <: Tuple = L match
    case x *: xs => Concat[TupleMapOption[x *: xs], Tuple1[Table[R]]]
    case _ => (MapOption[L], Table[R])

type RightJoinQuery[L, R] <: Tuple = L match
    case x *: xs => Concat[TupleMapOption[x *: xs], Tuple1[NamedQuery[R]]]
    case _ => (MapOption[L], NamedQuery[R])

type FullJoin[L, R] <: Tuple = L match
    case x *: xs => Concat[TupleMapOption[x *: xs], Tuple1[Table[Option[R]]]]
    case _ => (MapOption[L], Table[Option[R]])

type FullJoinQuery[L, R] <: Tuple = L match
    case x *: xs => Concat[TupleMapOption[x *: xs], Tuple1[NamedQuery[MapOption[R]]]]
    case _ => (MapOption[L], NamedQuery[MapOption[R]])

type Union[A, B] = (A, B) match
    case (a *: at, b *: bt) => UnionTuple[a *: at, b *: bt]
    case _ => Expr[UnionTo[A, B]]

type UnionTuple[A <: Tuple, B <: Tuple] <: Tuple = (A, B) match
    case (Expr[a] *: at, Expr[b] *: bt) => Expr[UnionTo[a, b]] *: UnionTuple[at, bt]
    case (EmptyTuple, EmptyTuple) => EmptyTuple

type UnionTo[A, B] = A match
    case B => B
    case Option[B] => A
    case Unwrap[B, Option] => B

type QueryMap[T] = T match
    case Expr[t] => Expr[t]
    case Table[t] => Table[t]
    case NamedQuery[t] => NamedQuery[t]
    case x *: xs => QueryMap[x] *: QueryMap[xs]
    case EmptyTuple => EmptyTuple
