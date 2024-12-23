package sqala.static.statement.query

import sqala.static.common.*

import scala.NamedTuple.NamedTuple

trait Result[T]:
    type R

object Result:
    type Aux[T, O] = Result[T]:
        type R = O

    given table[T]: Aux[Table[T], T] = new Result[Table[T]]:
        type R = T

    given expr[T: AsSqlExpr]: Aux[T, T] = new Result[T]:
        type R = T

    given scalarQuery[T: AsSqlExpr]: Aux[Query[T, OneRow], T] = new Result[Query[T, OneRow]]:
        type R = T

    given subQuery[N <: Tuple, V <: Tuple](using
        r: Result[NamedTuple[N, V]]
    ): Aux[SubQuery[N, V], r.R] =
        new Result[SubQuery[N, V]]:
            type R = r.R

    given tuple[H, T <: Tuple](using
        hr: Result[H],
        tr: Result[T],
        tt: ToTuple[tr.R]
    ): Aux[H *: T, hr.R *: tt.R] =
        new Result[H *: T]:
            type R = hr.R *: tt.R

    given tuple1[H](using hr: Result[H]): Aux[H *: EmptyTuple, hr.R *: EmptyTuple] =
        new Result[H *: EmptyTuple]:
            type R = hr.R *: EmptyTuple

    given namedTuple[N <: Tuple, V <: Tuple](using
        r: Result[V],
        tt: ToTuple[r.R]
    ): Aux[NamedTuple[N, V], NamedTuple[N, tt.R]] =
        new Result[NamedTuple[N, V]]:
            type R = NamedTuple[N, tt.R]