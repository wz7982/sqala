package sqala.dsl

import sqala.dsl.statement.query.SubQuery

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

    given namedTupleResult[N <: Tuple, V <: Tuple](using
        r: Result[V],
        tt: ToTuple[r.R]
    ): Aux[NamedTuple[N, V], NamedTuple[N, tt.R]] =
        new Result[NamedTuple[N, V]]:
            type R = NamedTuple[N, tt.R]

    given subQueryResult[N <: Tuple, V <: Tuple](using
        r: Result[V],
        tt: ToTuple[r.R]
    ): Aux[SubQuery[N, V], NamedTuple[N, tt.R]] =
        new Result[SubQuery[N, V]]:
            type R = NamedTuple[N, tt.R]