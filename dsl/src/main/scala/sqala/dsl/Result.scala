package sqala.dsl

import sqala.dsl.statement.query.{Group, SubQuery}

import scala.NamedTuple.NamedTuple

trait Result[T]:
    type R

object Result:
    type Aux[T, O] = Result[T]:
        type R = O

    given tableResult[T]: Aux[Table[T], T] = new Result[Table[T]]:
        type R = T

    given exprResult[T]: Aux[Expr[T], T] = new Result[Expr[T]]:
        type R = T

    given subQueryResult[N <: Tuple, V <: Tuple](using
        r: Result[NamedTuple[N, V]]
    ): Aux[SubQuery[N, V], r.R] =
        new Result[SubQuery[N, V]]:
            type R = r.R

    given groupResult[N <: Tuple, V <: Tuple](using
        r: Result[NamedTuple[N, V]]
    ): Aux[Group[N, V], r.R] =
        new Result[Group[N, V]]:
            type R = r.R

    given tupleResult[H, T <: Tuple](using
        hr: Result[H],
        tr: Result[T],
        tt: ToTuple[tr.R]
    ): Aux[H *: T, hr.R *: tt.R] =
        new Result[H *: T]:
            type R = hr.R *: tt.R

    given tuple1Result[H](using hr: Result[H]): Aux[H *: EmptyTuple, hr.R *: EmptyTuple] =
        new Result[H *: EmptyTuple]:
            type R = hr.R *: EmptyTuple

    given unnamedTupleResult[V <: Tuple](using 
        r: Result[V],
        tt: ToTuple[r.R]
    ): Aux[NamedTuple[Tuple, V], tt.R] =
        new Result[NamedTuple[Tuple, V]]:
            type R = tt.R

    given singletonResult[V](using 
        r: Result[V]
    ): Aux[NamedTuple[Tuple, Tuple1[V]], r.R] =
        new Result[NamedTuple[Tuple, Tuple1[V]]]:
            type R = r.R

    given namedTupleResult[N <: Tuple, V <: Tuple](using
        r: Result[V],
        tt: ToTuple[r.R]
    ): Aux[NamedTuple[N, V], NamedTuple[N, tt.R]] =
        new Result[NamedTuple[N, V]]:
            type R = NamedTuple[N, tt.R]