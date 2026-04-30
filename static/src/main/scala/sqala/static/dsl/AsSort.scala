package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.order.SqlOrdering
import sqala.static.dsl.statement.query.Query
import sqala.static.metadata.AsSqlExpr

trait AsSortItem[T, S <: QuerySize]:
    def asSort(x: T): Sort[?, ?]

object AsSortItem:
    given expr[T: AsSqlExpr, K <: ExprKind, S <: QuerySize](using CanInSort[K, S]): AsSortItem[Expr[T, K], S] with
        def asSort(x: Expr[T, K]): Sort[?, ?] =
            Sort(x, SqlOrdering.Asc, None)

    given sort[T, K <: ExprKind, S <: QuerySize](using CanInSort[K, S]): AsSortItem[Sort[T, K], S] with
        def asSort(x: Sort[T, K]): Sort[?, ?] =
            x

    given query[T: AsSqlExpr, K <: ExprKind, Q <: Query[Expr[T, K], OneRow], S <: QuerySize]: AsSortItem[Q, S] with
        def asSort(x: Q): Sort[?, ?] =
            Sort(Expr(SqlExpr.SubQuery(x.tree)), SqlOrdering.Asc, None)

trait AsSort[T, S <: QuerySize]:
    def asSorts(x: T): List[Sort[?, ?]]

object AsSort:
    given sort[T, S <: QuerySize](using a: AsSortItem[T, S]): AsSort[T, S] with
        def asSorts(x: T): List[Sort[?, ?]] =
            a.asSort(x) :: Nil

    given tuple[H, T <: Tuple, S <: QuerySize](using
        h: AsSortItem[H, S],
        t: AsSort[T, S]
    ): AsSort[H *: T, S] with
        def asSorts(x: H *: T): List[Sort[?, ?]] =
            h.asSort(x.head) :: t.asSorts(x.tail)

    given emptyTuple[S <: QuerySize]: AsSort[EmptyTuple, S] with
        def asSorts(x: EmptyTuple): List[Sort[?, ?]] =
            Nil

trait AsGroupedSortItem[T]:
    def asSort(x: T): Sort[?, ?]

object AsGroupedSortItem:
    given expr[T: AsSqlExpr, K <: ExprKind](using CanInGroupedSort[K]): AsGroupedSortItem[Expr[T, K]] with
        def asSort(x: Expr[T, K]): Sort[?, ?] =
            Sort(x, SqlOrdering.Asc, None)

    given sort[T, K <: ExprKind](using CanInGroupedSort[K]): AsGroupedSortItem[Sort[T, K]] with
        def asSort(x: Sort[T, K]): Sort[?, ?] =
            x

    given query[T: AsSqlExpr, K <: ExprKind, Q <: Query[Expr[T, K], OneRow]]: AsGroupedSortItem[Q] with
        def asSort(x: Q): Sort[?, ?] =
            Sort(Expr(SqlExpr.SubQuery(x.tree)), SqlOrdering.Asc, None)

trait AsGroupedSort[T]:
    def asSorts(x: T): List[Sort[?, ?]]

object AsGroupedSort:
    given sort[T](using a: AsGroupedSortItem[T]): AsGroupedSort[T] with
        def asSorts(x: T): List[Sort[?, ?]] =
            a.asSort(x) :: Nil

    given tuple[H, T <: Tuple](using
        h: AsGroupedSortItem[H],
        t: AsGroupedSort[T]
    ): AsGroupedSort[H *: T] with
        def asSorts(x: H *: T): List[Sort[?, ?]] =
            h.asSort(x.head) :: t.asSorts(x.tail)

    given emptyTuple: AsGroupedSort[EmptyTuple] with
        def asSorts(x: EmptyTuple): List[Sort[?, ?]] =
            Nil

trait AsDistinctSortItem[T]:
    def asSort(x: T): Sort[?, ?]

object AsDistinctSortItem:
    given expr[T: AsSqlExpr]: AsDistinctSortItem[Expr[T, Distinct]] with
        def asSort(x: Expr[T, Distinct]): Sort[?, ?] =
            Sort(x, SqlOrdering.Asc, None)

    given sort[T]: AsDistinctSortItem[Sort[T, Distinct]] with
        def asSort(x: Sort[T, Distinct]): Sort[?, ?] =
            x

trait AsDistinctSort[T]:
    def asSorts(x: T): List[Sort[?, ?]]

object AsDistinctSort:
    given sort[T](using a: AsDistinctSortItem[T]): AsDistinctSort[T] with
        def asSorts(x: T): List[Sort[?, ?]] =
            a.asSort(x) :: Nil

    given tuple[H, T <: Tuple](using
        h: AsDistinctSortItem[H],
        t: AsDistinctSort[T]
    ): AsDistinctSort[H *: T] with
        def asSorts(x: H *: T): List[Sort[?, ?]] =
            h.asSort(x.head) :: t.asSorts(x.tail)

    given emptyTuple: AsDistinctSort[EmptyTuple] with
        def asSorts(x: EmptyTuple): List[Sort[?, ?]] =
            Nil

trait AsOverSortItem[T]:
    type R

    type K <: ExprKind

    def asSort(x: T): Sort[?, ?]

object AsOverSortItem:
    type Aux[T, O, OK <: ExprKind] = AsOverSortItem[T]:
        type R = O

        type K = OK

    given expr[T: AsSqlExpr, EK <: ExprKind](using CanInOver[EK]): Aux[Expr[T, EK], T, EK] =
        new AsOverSortItem[Expr[T, EK]]:
            type R = T

            type K = EK

            def asSort(x: Expr[T, EK]): Sort[?, ?] =
                Sort(x, SqlOrdering.Asc, None)

    given sort[T, EK <: ExprKind](using CanInOver[EK]): Aux[Sort[T, EK], T, EK] =
        new AsOverSortItem[Sort[T, EK]]:
            type R = T

            type K = EK

            def asSort(x: Sort[T, EK]): Sort[?, ?] =
                x

    given query[T: AsSqlExpr, K <: ExprKind, Q <: Query[Expr[T, K], OneRow]]: Aux[Q, T, ValueOperation] =
        new AsOverSortItem[Q]:
            type R = T

            type K = ValueOperation

            def asSort(x: Q): Sort[?, ?] =
                Sort(Expr(SqlExpr.SubQuery(x.tree)), SqlOrdering.Asc, None)

trait AsOverSort[T]:
    type R

    type K <: ExprKind

    def asSorts(x: T): List[Sort[?, ?]]

object AsOverSort:
    type Aux[T, O, OK <: ExprKind] = AsOverSort[T]:
        type R = O

        type K = OK

    given sort[T](using a: AsOverSortItem[T]): Aux[T, a.R, a.K] =
        new AsOverSort[T]:
            type R = a.R

            type K = a.K

            def asSorts(x: T): List[Sort[?, ?]] =
                a.asSort(x) :: Nil

    given tuple[H, T <: Tuple](using
        h: AsOverSortItem[H],
        t: AsOverSort[T],
        tt: ToTuple[t.R],
        o: KindOperation[h.K, t.K]
    ): Aux[H *: T, h.R *: tt.R, o.R] =
        new AsOverSort[H *: T]:
            type R = h.R *: tt.R

            type K = o.R

            def asSorts(x: H *: T): List[Sort[?, ?]] =
                h.asSort(x.head) :: t.asSorts(x.tail)

    given emptyTuple: Aux[EmptyTuple, EmptyTuple, ValueOperation] =
        new AsOverSort[EmptyTuple]:
            type R = EmptyTuple

            type K = ValueOperation

            def asSorts(x: EmptyTuple): List[Sort[?, ?]] =
                Nil

trait AsColumnSortItem[T]:
    def asSort(x: T): Sort[?, ?]

object AsColumnSortItem:
    given expr[T]: AsColumnSortItem[Expr[T, Column]] with
        def asSort(x: Expr[T, Column]): Sort[?, ?] =
            Sort(x, SqlOrdering.Asc, None)

    given sort[T]: AsColumnSortItem[Sort[T, Column]] with
        def asSort(x: Sort[T, Column]): Sort[?, ?] =
            x

trait AsColumnSort[T]:
    def asSorts(x: T): List[Sort[?, ?]]

object AsColumnSort:
    given sort[T](using a: AsColumnSortItem[T]): AsColumnSort[T] with
        def asSorts(x: T): List[Sort[?, ?]] =
            a.asSort(x) :: Nil

    given tuple[H, T <: Tuple](using
        h: AsColumnSortItem[H],
        t: AsColumnSort[T]
    ): AsColumnSort[H *: T] with
        def asSorts(x: H *: T): List[Sort[?, ?]] =
            h.asSort(x.head) :: t.asSorts(x.tail)

    given emptyTuple: AsColumnSort[EmptyTuple] with
        def asSorts(x: EmptyTuple): List[Sort[?, ?]] =
            Nil