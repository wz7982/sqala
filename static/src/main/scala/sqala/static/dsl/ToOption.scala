package sqala.static.dsl

import sqala.static.dsl.table.*

import scala.NamedTuple.NamedTuple

trait ToOption[T]:
    type R

    def toOption(x: T): R

object ToOption:
    type Aux[T, O] = ToOption[T]:
        type R = O

    given expr[T, K <: ExprKind]: Aux[Expr[T, K], Expr[Wrap[T, Option], K]] =
        new ToOption[Expr[T, K]]:
            type R = Expr[Wrap[T, Option], K]

            def toOption(x: Expr[T, K]): R =
                Expr(x.asSqlExpr)

    given windowFunc[T, KS <: Tuple]: Aux[WindowFunc[T, KS], WindowFunc[Wrap[T, Option], KS]] =
        new ToOption[WindowFunc[T, KS]]:
            type R = WindowFunc[Wrap[T, Option], KS]

            def toOption(x: WindowFunc[T, KS]): R =
                WindowFunc(x.asSqlExpr)

    given table[T, K[_ <: Int] <: ExprKind, L <: Int]: Aux[Table[T, K, L], Table[Wrap[T, Option], K, L]] =
        new ToOption[Table[T, K, L]]:
            type R = Table[Wrap[T, Option], K, L]

            def toOption(x: Table[T, K, L]): R =
                x.copy[Wrap[T, Option], K, L]()

    given excludedTable[N <: Tuple, V <: Tuple, L <: Int](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[ExcludedTable[N, V, L], ExcludedTable[N, tt.R, L]] =
        new ToOption[ExcludedTable[N, V, L]]:
            type R = ExcludedTable[N, tt.R, L]

            def toOption(x: ExcludedTable[N, V, L]): R =
                x.copy[N, tt.R, L](__items__ = tt.toTuple(t.toOption(x.__items__.asInstanceOf[V])))

    given funcTable[T, K[_ <: Int] <: ExprKind, L <: Int]: Aux[FuncTable[T, K, L], FuncTable[Wrap[T, Option], K, L]] =
        new ToOption[FuncTable[T, K, L]]:
            type R = FuncTable[Wrap[T, Option], K, L]

            def toOption(x: FuncTable[T, K, L]): R =
                x.copy[Wrap[T, Option], K, L]()

    given subqueryTable[N <: Tuple, V <: Tuple, L <: Int](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[SubqueryTable[N, V, L], SubqueryTable[N, tt.R, L]] =
        new ToOption[SubqueryTable[N, V, L]]:
            type R = SubqueryTable[N, tt.R, L]

            def toOption(x: SubqueryTable[N, V, L]): R =
                x.copy[N, tt.R, L](__items__ = tt.toTuple(t.toOption(x.__items__)))

    given jsonTable[N <: Tuple, V <: Tuple, L <: Int](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[JsonTable[N, V, L], JsonTable[N, tt.R, L]] =
        new ToOption[JsonTable[N, V, L]]:
            type R = JsonTable[N, tt.R, L]

            def toOption(x: JsonTable[N, V, L]): R =
                x.copy[N, tt.R, L](__items__ = tt.toTuple(t.toOption(x.__items__)))

    given graphTable[N <: Tuple, V <: Tuple, L <: Int](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[GraphTable[N, V, L], GraphTable[N, tt.R, L]] =
        new ToOption[GraphTable[N, V, L]]:
            type R = GraphTable[N, tt.R, L]

            def toOption(x: GraphTable[N, V, L]): R =
                x.copy[N, tt.R, L](__items__ = tt.toTuple(t.toOption(x.__items__)))

    given recursiveTable[N <: Tuple, V <: Tuple, L <: Int](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[RecursiveTable[N, V, L], RecursiveTable[N, tt.R, L]] =
        new ToOption[RecursiveTable[N, V, L]]:
            type R = RecursiveTable[N, tt.R, L]

            def toOption(x: RecursiveTable[N, V, L]): R =
                x.copy[N, tt.R, L](__items__ = tt.toTuple(t.toOption(x.__items__)))

    given recognizeTable[N <: Tuple, V <: Tuple, L <: Int](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[RecognizeTable[N, V, L], RecognizeTable[N, tt.R, L]] =
        new ToOption[RecognizeTable[N, V, L]]:
            type R = RecognizeTable[N, tt.R, L]

            def toOption(x: RecognizeTable[N, V, L]): R =
                x.copy[N, tt.R, L](__items__ = tt.toTuple(t.toOption(x.__items__)))

    given tuple[H, T <: Tuple](using
        h: ToOption[H],
        t: ToOption[T],
        tt: ToTuple[t.R]
    ): Aux[H *: T, h.R *: tt.R] =
        new ToOption[H *: T]:
            type R = h.R *: tt.R

            def toOption(x: H *: T): R =
                h.toOption(x.head) *: tt.toTuple(t.toOption(x.tail))

    given tuple1[H](using
        h: ToOption[H]
    ): Aux[H *: EmptyTuple, h.R *: EmptyTuple] =
        new ToOption[H *: EmptyTuple]:
            type R = h.R *: EmptyTuple

            def toOption(x: H *: EmptyTuple): R =
                h.toOption(x.head) *: EmptyTuple

    given namedTuple[N <: Tuple, V <: Tuple](using
        v: ToOption[V],
        tt: ToTuple[v.R]
    ): Aux[NamedTuple[N, V], NamedTuple[N, tt.R]] =
        new ToOption[NamedTuple[N, V]]:
            type R = NamedTuple[N, tt.R]

            def toOption(x: NamedTuple[N, V]): R =
                tt.toTuple(v.toOption(x.toTuple))