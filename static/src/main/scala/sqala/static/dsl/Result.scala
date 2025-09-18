package sqala.static.dsl

import sqala.static.dsl.table.*

import scala.NamedTuple.NamedTuple

trait Result[T]:
    type R

object Result:
    type Aux[T, O] = Result[T]:
        type R = O

    given expr[T]: Aux[Expr[T], T] = new Result[Expr[T]]:
        type R = T

    given table[T]: Aux[Table[T], T] = new Result[Table[T]]:
        type R = T

    given funcTable[T]: Aux[FuncTable[T], T] = new Result[FuncTable[T]]:
        type R = T

    given subQueryTable[N <: Tuple, V <: Tuple](using
        r: Result[NamedTuple[N, V]]
    ): Aux[SubQueryTable[N, V], r.R] =
        new Result[SubQueryTable[N, V]]:
            type R = r.R

    given jsonTable[N <: Tuple, V <: Tuple](using
        r: Result[NamedTuple[N, V]]
    ): Aux[JsonTable[N, V], r.R] =
        new Result[JsonTable[N, V]]:
            type R = r.R

    given graphTable[N <: Tuple, V <: Tuple](using
        r: Result[NamedTuple[N, V]]
    ): Aux[GraphTable[N, V], r.R] =
        new Result[GraphTable[N, V]]:
            type R = r.R

    given recognizeTable[N <: Tuple, V <: Tuple](using
        r: Result[NamedTuple[N, V]]
    ): Aux[RecognizeTable[N, V], r.R] =
        new Result[RecognizeTable[N, V]]:
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