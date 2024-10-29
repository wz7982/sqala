package sqala.dsl

import sqala.dsl.statement.query.SubQuery

import scala.NamedTuple.NamedTuple

trait ToOption[T]:
    type R

    def toOption(x: T): R

object ToOption:
    type Aux[T, O] = ToOption[T]:
        type R = O

    given exprToOption[T, K <: GroupKind | DistinctKind | CompositeKind]: Aux[Expr[T, K], Expr[Wrap[T, Option], K]] =
        new ToOption[Expr[T, K]]:
            type R = Expr[Wrap[T, Option], K]

            def toOption(x: Expr[T, K]): R =
                Expr.Ref(x)

    given tableToOption[T]: Aux[Table[T], Table[Wrap[T, Option]]] = 
        new ToOption[Table[T]]:
            type R = Table[Wrap[T, Option]]

            def toOption(x: Table[T]): R = 
                new Table(x.__tableName__, x.__aliasName__, x.__metaData__)

    given subQueryToOption[N <: Tuple, V <: Tuple](using 
        t: ToOption[V], 
        tt: ToTuple[t.R]
    ): Aux[SubQuery[N, V], SubQuery[N, tt.R]] =
        new ToOption[SubQuery[N, V]]:
            type R = SubQuery[N, tt.R]

            def toOption(x: SubQuery[N, V]): R =
                new SubQuery(x.__alias__, x.__columnSize__)(using x.qc)

    given tupleToOption[H, T <: Tuple](using 
        h: ToOption[H], 
        t: ToOption[T], 
        tt: ToTuple[t.R]
    ): Aux[H *: T, h.R *: tt.R] = 
        new ToOption[H *: T]:
            type R = h.R *: tt.R

            def toOption(x: H *: T): R =
                h.toOption(x.head) *: tt.toTuple(t.toOption(x.tail))

    given tuple1ToOption[H](using h: ToOption[H]): Aux[H *: EmptyTuple, h.R *: EmptyTuple] = 
        new ToOption[H *: EmptyTuple]:
            type R = h.R *: EmptyTuple

            def toOption(x: H *: EmptyTuple): R =
                h.toOption(x.head) *: EmptyTuple

    given namedTupleToOption[N <: Tuple, V <: Tuple](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[NamedTuple[N, V], NamedTuple[N, tt.R]] =
        new ToOption[NamedTuple[N, V]]:
            type R = NamedTuple[N, tt.R]

            def toOption(x: NamedTuple[N, V]): R =
                tt.toTuple(t.toOption(x.toTuple))