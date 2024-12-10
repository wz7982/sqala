package sqala.static.statement.query

import sqala.static.common.*

trait ToOption[T]:
    type R

    def toOption(x: T): R

object ToOption:
    type Aux[T, O] = ToOption[T]:
        type R = O

    given exprToOption[T: AsSqlExpr]: Aux[T, Wrap[T, Option]] =
        new ToOption[T]:
            type R = Wrap[T, Option]

            def toOption(x: T): R = compileTimeOnly

    given tableToOption[T]: Aux[Table[T], Table[Wrap[T, Option]]] =
        new ToOption[Table[T]]:
            type R = Table[Wrap[T, Option]]

            def toOption(x: Table[T]): R =
                new Table(x.__metaData__)

    given subQueryToOption[N <: Tuple, V <: Tuple](using
        t: ToOption[V],
        tt: ToTuple[t.R]
    ): Aux[SubQuery[N, V], SubQuery[N, tt.R]] =
        new ToOption[SubQuery[N, V]]:
            type R = SubQuery[N, tt.R]

            def toOption(x: SubQuery[N, V]): R =
                new SubQuery(x.__columns__)

    given tableSubQueryToOption[T]: Aux[TableSubQuery[T], TableSubQuery[Wrap[T, Option]]] =
        new ToOption[TableSubQuery[T]]:
            type R = TableSubQuery[Wrap[T, Option]]

            def toOption(x: TableSubQuery[T]): R =
                new TableSubQuery(x.__metaData__)

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