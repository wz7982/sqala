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

    given table[T, K <: ExprKind, F <: InFrom]: Aux[Table[T, K, F], Table[Wrap[T, Option], K, F]] =
        new ToOption[Table[T, K, F]]:
            type R = Table[Wrap[T, Option], K, F]

            def toOption(x: Table[T, K, F]): R =
                x.copy[Wrap[T, Option], K, F]()

    given excludedTable[N <: Tuple, V <: Tuple, F <: InFrom](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[ExcludedTable[N, V, F], ExcludedTable[N, tt.R, F]] =
        new ToOption[ExcludedTable[N, V, F]]:
            type R = ExcludedTable[N, tt.R, F]

            def toOption(x: ExcludedTable[N, V, F]): R =
                x.copy[N, tt.R, F](__items__ = tt.toTuple(t.toOption(x.__items__.asInstanceOf[V])))

    given funcTable[T, K <: ExprKind, F <: InFrom]: Aux[FuncTable[T, K, F], FuncTable[Wrap[T, Option], K, F]] =
        new ToOption[FuncTable[T, K, F]]:
            type R = FuncTable[Wrap[T, Option], K, F]

            def toOption(x: FuncTable[T, K, F]): R =
                x.copy[Wrap[T, Option], K, F]()

    given subQueryTable[N <: Tuple, V <: Tuple, F <: InFrom](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[SubQueryTable[N, V, F], SubQueryTable[N, tt.R, F]] =
        new ToOption[SubQueryTable[N, V, F]]:
            type R = SubQueryTable[N, tt.R, F]

            def toOption(x: SubQueryTable[N, V, F]): R =
                x.copy[N, tt.R, F](__items__ = tt.toTuple(t.toOption(x.__items__)))

    given jsonTable[N <: Tuple, V <: Tuple, F <: InFrom](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[JsonTable[N, V, F], JsonTable[N, tt.R, F]] =
        new ToOption[JsonTable[N, V, F]]:
            type R = JsonTable[N, tt.R, F]

            def toOption(x: JsonTable[N, V, F]): R =
                x.copy[N, tt.R, F](__items__ = tt.toTuple(t.toOption(x.__items__)))

    given graphTable[N <: Tuple, V <: Tuple, F <: InFrom](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[GraphTable[N, V, F], GraphTable[N, tt.R, F]] =
        new ToOption[GraphTable[N, V, F]]:
            type R = GraphTable[N, tt.R, F]

            def toOption(x: GraphTable[N, V, F]): R =
                x.copy[N, tt.R, F](__items__ = tt.toTuple(t.toOption(x.__items__)))

    given recursiveTable[N <: Tuple, V <: Tuple](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[RecursiveTable[N, V], RecursiveTable[N, tt.R]] =
        new ToOption[RecursiveTable[N, V]]:
            type R = RecursiveTable[N, tt.R]

            def toOption(x: RecursiveTable[N, V]): R =
                x.copy[N, tt.R](__items__ = tt.toTuple(t.toOption(x.__items__)))

    given recognizeTable[N <: Tuple, V <: Tuple, F <: InFrom](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[RecognizeTable[N, V, F], RecognizeTable[N, tt.R, F]] =
        new ToOption[RecognizeTable[N, V, F]]:
            type R = RecognizeTable[N, tt.R, F]

            def toOption(x: RecognizeTable[N, V, F]): R =
                x.copy[N, tt.R, F](__items__ = tt.toTuple(t.toOption(x.__items__)))

    given tuple[H, T <: Tuple](using
        h: ToOption[H],
        t: ToOption[T],
        tt: ToTuple[t.R]
    ): Aux[H *: T, h.R *: tt.R] =
        new ToOption[H *: T]:
            type R = h.R *: tt.R

            def toOption(x: H *: T): R =
                h.toOption(x.head) *: tt.toTuple(t.toOption(x.tail))

    given tuple1[H](using h: ToOption[H]): Aux[H *: EmptyTuple, h.R *: EmptyTuple] =
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