package sqala.dsl

import sqala.dsl.statement.query.NamedQuery

import scala.NamedTuple.NamedTuple

trait Result[T]:
    type R

object Result:
    type Aux[T, O] = Result[T] { type R = O }

    given tableResult[T]: Aux[Table[T], T] = new Result[Table[T]]:
        type R = T

    given selectTableResult[T <: Tuple, N <: Tuple]: Aux[SelectTable[T, N], UnwarpTuple1[SelectTableResult[T]]] = new Result[SelectTable[T, N]]:
        type R = UnwarpTuple1[SelectTableResult[T]]

    given exprResult[T, K <: ExprKind]: Aux[Expr[T, K], T] = new Result[Expr[T, K]]:
        type R = T

    given tupleResult[H, T <: Tuple](using hr: Result[H], tr: Result[T]): Aux[H *: T, hr.R *: ToTuple[tr.R]] = 
        new Result[H *: T]:
            type R = hr.R *: ToTuple[tr.R]

    given emptyTupleResult: Aux[EmptyTuple, EmptyTuple] = new Result[EmptyTuple]:
        type R = EmptyTuple

    given namedTupleResult[N <: Tuple, V <: Tuple](using r: Result[V]): Aux[NamedTuple[N, V], NamedTuple[N, ToTuple[r.R]]] =
        new Result[NamedTuple[N, V]]:
            type R = NamedTuple[N, ToTuple[r.R]]

    given namedQueryResult[N <: Tuple, V <: Tuple](using r: Result[NamedTuple[N, V]]): Aux[NamedQuery[N, V], r.R] =
        new Result[NamedQuery[N, V]]:
            type R = r.R