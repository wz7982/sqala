package sqala.static.dsl

import sqala.static.dsl.table.*

import scala.NamedTuple.NamedTuple

trait ToOption[T]:
    type R

    def toOption(x: T): R

object ToOption:
    type Aux[T, O] = ToOption[T]:
        type R = O

    given expr[T]: Aux[Expr[T], Expr[Wrap[T, Option]]] =
        new ToOption[Expr[T]]:
            type R = Expr[Wrap[T, Option]]

            def toOption(x: Expr[T]): R =
                Expr(x.asSqlExpr)

    given table[T]: Aux[Table[T], Table[Wrap[T, Option]]] =
        new ToOption[Table[T]]:
            type R = Table[Wrap[T, Option]]

            def toOption(x: Table[T]): R =
                x.copy[Wrap[T, Option]]()

    given funcTable[T]: Aux[FuncTable[T], FuncTable[Wrap[T, Option]]] =
        new ToOption[FuncTable[T]]:
            type R = FuncTable[Wrap[T, Option]]

            def toOption(x: FuncTable[T]): R =
                x.copy[Wrap[T, Option]]()

    given subQueryTable[N <: Tuple, V <: Tuple](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[SubQueryTable[N, V], SubQueryTable[N, tt.R]] =
        new ToOption[SubQueryTable[N, V]]:
            type R = SubQueryTable[N, tt.R]

            def toOption(x: SubQueryTable[N, V]): R =
                x.copy[N, tt.R](__items__ = tt.toTuple(t.toOption(x.__items__)))

    given jsonTable[N <: Tuple, V <: Tuple](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[JsonTable[N, V], JsonTable[N, tt.R]] =
        new ToOption[JsonTable[N, V]]:
            type R = JsonTable[N, tt.R]

            def toOption(x: JsonTable[N, V]): R =
                x.copy[N, tt.R](__items__ = tt.toTuple(t.toOption(x.__items__)))

    given graphTable[N <: Tuple, V <: Tuple](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[GraphTable[N, V], GraphTable[N, tt.R]] =
        new ToOption[GraphTable[N, V]]:
            type R = GraphTable[N, tt.R]

            def toOption(x: GraphTable[N, V]): R =
                x.copy[N, tt.R](__items__ = tt.toTuple(t.toOption(x.__items__)))

    given recursiveTable[N <: Tuple, V <: Tuple](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[RecursiveTable[N, V], RecursiveTable[N, tt.R]] =
        new ToOption[RecursiveTable[N, V]]:
            type R = RecursiveTable[N, tt.R]

            def toOption(x: RecursiveTable[N, V]): R =
                x.copy[N, tt.R](__items__ = tt.toTuple(t.toOption(x.__items__)))

    given recognizeTable[N <: Tuple, V <: Tuple](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[RecognizeTable[N, V], RecognizeTable[N, tt.R]] =
        new ToOption[RecognizeTable[N, V]]:
            type R = RecognizeTable[N, tt.R]

            def toOption(x: RecognizeTable[N, V]): R =
                x.copy[N, tt.R](__items__ = tt.toTuple(t.toOption(x.__items__)))

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