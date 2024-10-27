package sqala.dsl

import sqala.dsl.statement.query.SubQuery

import scala.NamedTuple.NamedTuple

trait Result[T]:
    type R

object Result:
    type Aux[T, O] = Result[T]:
        type R = O

    given tableResult[X, T <: Table[X]]: Aux[T, X] = new Result[T]:
        type R = X

    given exprResult[T, K <: ExprKind]: Aux[Expr[T, K], T] = new Result[Expr[T, K]]:
        type R = T

    given tupleResult[H, T <: Tuple](using hr: Result[H], tr: Result[T]): Aux[H *: T, hr.R *: ToTuple[tr.R]] = 
        new Result[H *: T]:
            type R = hr.R *: ToTuple[tr.R]

    given tuple1Result[H](using hr: Result[H]): Aux[H *: EmptyTuple, hr.R *: EmptyTuple] = 
        new Result[H *: EmptyTuple]:
            type R = hr.R *: EmptyTuple

    given namedTupleResult[N <: Tuple, V <: Tuple](using r: Result[V]): Aux[NamedTuple[N, V], NamedTuple[N, ToTuple[r.R]]] =
        new Result[NamedTuple[N, V]]:
            type R = NamedTuple[N, ToTuple[r.R]]

    given subQueryResult[N <: Tuple, V <: Tuple](using r: Result[V]): Aux[SubQuery[N, V], NamedTuple[N, ToTuple[r.R]]] =
        new Result[SubQuery[N, V]]:
            type R = NamedTuple[N, ToTuple[r.R]]