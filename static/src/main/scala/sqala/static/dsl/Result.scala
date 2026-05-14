package sqala.static.dsl

import sqala.static.dsl.table.*

import scala.NamedTuple.NamedTuple

trait Result[T]:
    type R

object Result:
    type Aux[T, O] = Result[T]:
        type R = O

    given expr[T, K <: ExprKind]: Aux[Expr[T, K], T] = new Result[Expr[T, K]]:
        type R = T

    given table[T, K[_ <: Int] <: ExprKind, L <: Int]: Aux[Table[T, K, L], T] = new Result[Table[T, K, L]]:
        type R = T

    given excludedTable[N <: Tuple, V <: Tuple, L <: Int](using
        r: Result[NamedTuple[N, V]]
    ): Aux[ExcludedTable[N, V, L], r.R] =
        new Result[ExcludedTable[N, V, L]]:
            type R = r.R

    given funcTable[T, K[_ <: Int] <: ExprKind, L <: Int]: Aux[FuncTable[T, K, L], T] = new Result[FuncTable[T, K, L]]:
        type R = T

    given subqueryTable[N <: Tuple, V <: Tuple, L <: Int](using
        r: Result[NamedTuple[N, V]]
    ): Aux[SubqueryTable[N, V, L], r.R] =
        new Result[SubqueryTable[N, V, L]]:
            type R = r.R

    given jsonTable[N <: Tuple, V <: Tuple, L <: Int](using
        r: Result[NamedTuple[N, V]]
    ): Aux[JsonTable[N, V, L], r.R] =
        new Result[JsonTable[N, V, L]]:
            type R = r.R

    given graphTable[N <: Tuple, V <: Tuple, L <: Int](using
        r: Result[NamedTuple[N, V]]
    ): Aux[GraphTable[N, V, L], r.R] =
        new Result[GraphTable[N, V, L]]:
            type R = r.R

    given recursiveTable[N <: Tuple, V <: Tuple, L <: Int](using
        r: Result[NamedTuple[N, V]]
    ): Aux[RecursiveTable[N, V, L], r.R] =
        new Result[RecursiveTable[N, V, L]]:
            type R = r.R

    given recognizeTable[N <: Tuple, V <: Tuple, L <: Int](using
        r: Result[NamedTuple[N, V]]
    ): Aux[RecognizeTable[N, V, L], r.R] =
        new Result[RecognizeTable[N, V, L]]:
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