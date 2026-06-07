package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.order.SqlOrdering
import sqala.metadata.AsSqlExpr
import sqala.static.dsl.statement.query.Query

import scala.compiletime.ops.int.>

/**
 * Lifts values, expressions, and subqueries for `sortBy` in
 * queries. `S` is the query size. `CL` is the current query
 * context level.
 */
trait AsSortItem[T, S <: QuerySize, CL <: Int]:
    /**
     * The expression kind tuple of the sort item.
     */
    type KS <: Tuple

    /**
     * Converts the value to a `Sort`.
     */
    def asSort(x: T): Sort[?, ?]

object AsSortItem:
    type Aux[T, S <: QuerySize, CL <: Int, OKS <: Tuple] =
        AsSortItem[T, S, CL]:
            type KS = OKS

    given expr[T: AsSqlExpr, EK <: ExprKind, S <: QuerySize, CL <: Int](using
        kt: KindToTuple[EK],
        i: CanInSort[kt.R, S],
        iv: IsKind[EK, Value],
        nv: iv.R =:= false
    ): Aux[Expr[T, EK], S, CL, kt.R] =
        new AsSortItem[Expr[T, EK], S, CL]:
            type KS = kt.R

            def asSort(x: Expr[T, EK]): Sort[?, ?] =
                Sort(x, SqlOrdering.Asc, None)

    given sort[T: AsSqlExpr, EK <: ExprKind, S <: QuerySize, CL <: Int](using
        kt: KindToTuple[EK],
        i: CanInSort[kt.R, S],
        iv: IsKind[EK, Value],
        nv: iv.R =:= false
    ): Aux[Sort[T, EK], S, CL, kt.R] =
        new AsSortItem[Sort[T, EK], S, CL]:
            type KS = kt.R

            def asSort(x: Sort[T, EK]): Sort[?, ?] =
                x

    given query[T, OKS <: Tuple, L <: Int, Q <: Query[T, OKS, L, OneRow], S <: QuerySize, CL <: Int](using
        a: AsExpr[T, CL],
        as: AsSqlExpr[a.R],
        i: CanInSort[OKS, S],
        refl: L > CL =:= true
    ): Aux[Q, S, CL, OKS] =
        new AsSortItem[Q, S, CL]:
            type KS = OKS

            def asSort(x: Q): Sort[?, ?] =
                Sort(Expr(SqlExpr.Subquery(None, x.tree)), SqlOrdering.Asc, None)

/**
 * Collects sort items for `sortBy` in queries. `S` is the query
 * size. `CL` is the current query context level.
 */
trait AsSort[T, S <: QuerySize, CL <: Int]:
    /**
     * The combined kind tuple of the sort clause.
     */
    type KS <: Tuple

    /**
     * Converts the value to a list of `Sort` values.
     */
    def asSorts(x: T): List[Sort[?, ?]]

object AsSort:
    type Aux[T, S <: QuerySize, CL <: Int, OKS <: Tuple] = AsSort[T, S, CL]:
        type KS = OKS

    given item[T, S <: QuerySize, CL <: Int](using a: AsSortItem[T, S, CL]): Aux[T, S, CL, a.KS] =
        new AsSort[T, S, CL]:
            type KS = a.KS

            def asSorts(x: T): List[Sort[?, ?]] =
                a.asSort(x) :: Nil

    given tuple[H, T <: Tuple, S <: QuerySize, CL <: Int](using
        h: AsSortItem[H, S, CL],
        t: AsSort[T, S, CL],
        c: CombineKindTuple[h.KS, t.KS]
    ): Aux[H *: T, S, CL, c.R] =
        new AsSort[H *: T, S, CL]:
            type KS = c.R

            def asSorts(x: H *: T): List[Sort[?, ?]] =
                h.asSort(x.head) :: t.asSorts(x.tail)

    given tuple1[H, S <: QuerySize, CL <: Int](using
        h: AsSortItem[H, S, CL],
        c: CombineKindTuple[h.KS, EmptyTuple]
    ): Aux[H *: EmptyTuple, S, CL, c.R] =
        new AsSort[H *: EmptyTuple, S, CL]:
            type KS = c.R

            def asSorts(x: H *: EmptyTuple): List[Sort[?, ?]] =
                h.asSort(x.head) :: Nil

/**
 * Lifts values, expressions, and subqueries for `sortBy` after
 * `groupBy`. `CL` is the current query context level.
 */
trait AsGroupedSortItem[T, CL <: Int]:
    /**
     * The expression kind tuple of the sort item.
     */
    type KS <: Tuple

    /**
     * Converts the value to a `Sort`.
     */
    def asSort(x: T): Sort[?, ?]

object AsGroupedSortItem:
    type Aux[T, CL <: Int, OKS <: Tuple] = AsGroupedSortItem[T, CL]:
        type KS = OKS

    given expr[T: AsSqlExpr, EK <: ExprKind, CL <: Int](using
        kt: KindToTuple[EK],
        i: CanInGroupedSort[kt.R],
        iv: IsKind[EK, Value],
        nv: iv.R =:= false
    ): Aux[Expr[T, EK], CL, kt.R] =
        new AsGroupedSortItem[Expr[T, EK], CL]:
            type KS = kt.R

            def asSort(x: Expr[T, EK]): Sort[?, ?] =
                Sort(x, SqlOrdering.Asc, None)

    given sort[T: AsSqlExpr, EK <: ExprKind, CL <: Int](using
        kt: KindToTuple[EK],
        i: CanInGroupedSort[kt.R],
        iv: IsKind[EK, Value],
        nv: iv.R =:= false
    ): Aux[Sort[T, EK], CL, kt.R] =
        new AsGroupedSortItem[Sort[T, EK], CL]:
            type KS = kt.R

            def asSort(x: Sort[T, EK]): Sort[?, ?] =
                x

    given query[T, OKS <: Tuple, L <: Int, Q <: Query[T, OKS, L, OneRow], CL <: Int](using
        a: AsExpr[T, CL],
        as: AsSqlExpr[a.R],
        i: CanInGroupedSort[OKS],
        refl: L > CL =:= true
    ): Aux[Q, CL, OKS] =
        new AsGroupedSortItem[Q, CL]:
            type KS = OKS

            def asSort(x: Q): Sort[?, ?] =
                Sort(Expr(SqlExpr.Subquery(None, x.tree)), SqlOrdering.Asc, None)

/**
 * Collects sort items for `sortBy` after `groupBy`. `CL` is the
 * current query context level.
 */
trait AsGroupedSort[T, CL <: Int]:
    /**
     * The combined kind tuple of the sort clause.
     */
    type KS <: Tuple

    /**
     * Converts the value to a list of `Sort` values.
     */
    def asSorts(x: T): List[Sort[?, ?]]

object AsGroupedSort:
    type Aux[T, CL <: Int, OKS <: Tuple] = AsGroupedSort[T, CL]:
        type KS = OKS

    given item[T, CL <: Int](using a: AsGroupedSortItem[T, CL]): Aux[T, CL, a.KS] =
        new AsGroupedSort[T, CL]:
            type KS = a.KS

            def asSorts(x: T): List[Sort[?, ?]] =
                a.asSort(x) :: Nil

    given tuple[H, T <: Tuple, CL <: Int](using
        h: AsGroupedSortItem[H, CL],
        t: AsGroupedSort[T, CL],
        c: CombineKindTuple[h.KS, t.KS]
    ): Aux[H *: T, CL, c.R] =
        new AsGroupedSort[H *: T, CL]:
            type KS = c.R

            def asSorts(x: H *: T): List[Sort[?, ?]] =
                h.asSort(x.head) :: t.asSorts(x.tail)

    given tuple1[H, CL <: Int](using
        h: AsGroupedSortItem[H, CL],
        c: CombineKindTuple[h.KS, EmptyTuple]
    ): Aux[H *: EmptyTuple, CL, c.R] =
        new AsGroupedSort[H *: EmptyTuple, CL]:
            type KS = c.R

            def asSorts(x: H *: EmptyTuple): List[Sort[?, ?]] =
                h.asSort(x.head) :: Nil

/**
 * Collects distinct sort items for `sortBy` after `mapDistinct`.
 * `CL` is the current query context level.
 */
trait AsDistinctSortItem[T, CL <: Int]:
    /**
     * Converts the value to a list of `DistinctSort` values.
     */
    def asSort(x: T): DistinctSort[?, ?]

object AsDistinctSortItem:
    given expr[T: AsSqlExpr, EL <: Int, CL <: Int](using EL =:= CL): AsDistinctSortItem[Distinct[T, EL], CL] with
        def asSort(x: Distinct[T, EL]): DistinctSort[?, ?] =
            DistinctSort(Expr(x.expr), SqlOrdering.Asc, None)

    given sort[T: AsSqlExpr, EL <: Int, CL <: Int](using EL =:= CL): AsDistinctSortItem[DistinctSort[T, EL], CL] with
        def asSort(x: DistinctSort[T, EL]): DistinctSort[?, ?] =
            x

/**
 * Collects distinct sort items for `sortBy` after `mapDistinct`.
 * `CL` is the current query context level.
 */
trait AsDistinctSort[T, CL <: Int]:
    /**
     * Converts the value to a list of `DistinctSort` values.
     */
    def asSorts(x: T): List[DistinctSort[?, ?]]

object AsDistinctSort:
    given item[T, CL <: Int](using a: AsDistinctSortItem[T, CL]): AsDistinctSort[T, CL] with
        def asSorts(x: T): List[DistinctSort[?, ?]] =
            a.asSort(x) :: Nil

    given tuple[H, T <: Tuple, CL <: Int](using
        h: AsDistinctSortItem[H, CL],
        t: AsDistinctSort[T, CL]
    ): AsDistinctSort[H *: T, CL] with
        def asSorts(x: H *: T): List[DistinctSort[?, ?]] =
            h.asSort(x.head) :: t.asSorts(x.tail)

    given tuple1[H, CL <: Int](using
        h: AsDistinctSortItem[H, CL]
    ): AsDistinctSort[H *: EmptyTuple, CL] with
        def asSorts(x: H *: EmptyTuple): List[DistinctSort[?, ?]] =
            h.asSort(x.head) :: Nil

/**
 * Lifts values, expressions, and subqueries for `sortBy` in a
 * window `over` clause. `CL` is the current query context level.
 */
trait AsOverSortItem[T, CL <: Int]:
    /**
     * The result type.
     */
    type R

    /**
     * The expression kind tuple of the sort item.
     */
    type KS <: Tuple

    /**
     * Converts the value to a `Sort`.
     */
    def asSort(x: T): Sort[?, ?]

object AsOverSortItem:
    type Aux[T, CL <: Int, O, OKS <: Tuple] = AsOverSortItem[T, CL]:
        type R = O

        type KS = OKS

    given expr[T: AsSqlExpr, EK <: ExprKind, CL <: Int](using
        kt: KindToTuple[EK],
        i: CanInWindow[kt.R]
    ): Aux[Expr[T, EK], CL, T, kt.R] =
        new AsOverSortItem[Expr[T, EK], CL]:
            type R = T

            type KS = kt.R

            def asSort(x: Expr[T, EK]): Sort[?, ?] =
                Sort(x, SqlOrdering.Asc, None)

    given sort[T: AsSqlExpr, EK <: ExprKind, CL <: Int](using
        kt: KindToTuple[EK],
        i: CanInWindow[kt.R]
    ): Aux[Sort[T, EK], CL, T, kt.R] =
        new AsOverSortItem[Sort[T, EK], CL]:
            type R = T

            type KS = kt.R

            def asSort(x: Sort[T, EK]): Sort[?, ?] =
                x

    given query[T, OKS <: Tuple, L <: Int, Q <: Query[T, OKS, L, OneRow], CL <: Int](using
        a: AsExpr[T, CL],
        as: AsSqlExpr[a.R],
        i: CanInWindow[OKS],
        refl: L > CL =:= true
    ): Aux[Q, CL, a.R, OKS] =
        new AsOverSortItem[Q, CL]:
            type R = a.R

            type KS = OKS

            def asSort(x: Q): Sort[?, ?] =
                Sort(Expr(SqlExpr.Subquery(None, x.tree)), SqlOrdering.Asc, None)

/**
 * Collects sort items for `sortBy` in a window `over` clause.
 * `CL` is the current query context level.
 */
trait AsOverSort[T, CL <: Int]:
    /**
     * The result type.
     */
    type R

    /**
     * The combined kind tuple of the sort clause.
     */
    type KS <: Tuple

    /**
     * Converts the value to a list of `Sort` values.
     */
    def asSorts(x: T): List[Sort[?, ?]]

object AsOverSort:
    type Aux[T, CL <: Int, O, OKS <: Tuple] = AsOverSort[T, CL]:
        type R = O

        type KS = OKS

    given item[T, CL <: Int](using a: AsOverSortItem[T, CL]): Aux[T, CL, a.R, a.KS] =
        new AsOverSort[T, CL]:
            type R = a.R

            type KS = a.KS

            def asSorts(x: T): List[Sort[?, ?]] =
                a.asSort(x) :: Nil

    given tuple[H, T <: Tuple, CL <: Int](using
        h: AsOverSortItem[H, CL],
        t: AsOverSort[T, CL],
        tt: ToTuple[t.R],
        c: CombineKindTuple[h.KS, t.KS]
    ): Aux[H *: T, CL, h.R *: tt.R, c.R] =
        new AsOverSort[H *: T, CL]:
            type R = h.R *: tt.R

            type KS = c.R

            def asSorts(x: H *: T): List[Sort[?, ?]] =
                h.asSort(x.head) :: t.asSorts(x.tail)

    given tuple1[H, CL <: Int](using
        h: AsOverSortItem[H, CL],
        c: CombineKindTuple[h.KS, EmptyTuple]
    ): Aux[H *: EmptyTuple, CL, h.R *: EmptyTuple, c.R] =
        new AsOverSort[H *: EmptyTuple, CL]:
            type R = h.R *: EmptyTuple

            type KS = c.R

            def asSorts(x: H *: EmptyTuple): List[Sort[?, ?]] =
                h.asSort(x.head) :: Nil

/**
 * Lifts column expressions for `sortBy` in contexts requiring
 * column-level validation `CL` is the current query context level.
 */
trait AsColumnSortItem[T, CL <: Int]:
    /**
     * Converts the value to a `Sort`.
     */
    def asSort(x: T): Sort[?, ?]

object AsColumnSortItem:
    given expr[T: AsSqlExpr, EL <: Int, CL <: Int](using EL =:= CL): AsColumnSortItem[Expr[T, Column[EL]], CL] with
        def asSort(x: Expr[T, Column[EL]]): Sort[?, ?] =
            Sort(x, SqlOrdering.Asc, None)

    given sort[T: AsSqlExpr, EL <: Int, CL <: Int](using EL =:= CL): AsColumnSortItem[Sort[T, Column[EL]], CL] with
        def asSort(x: Sort[T, Column[EL]]): Sort[?, ?] =
            x

/**
 * Collects column sort items for `sortBy` in column-level
 * contexts.
 */
trait AsColumnSort[T, CL <: Int]:
    /**
     * Converts the value to a list of `Sort` values.
     */
    def asSorts(x: T): List[Sort[?, ?]]

object AsColumnSort:
    given item[T, CL <: Int](using a: AsColumnSortItem[T, CL]): AsColumnSort[T, CL] with
        def asSorts(x: T): List[Sort[?, ?]] =
            a.asSort(x) :: Nil

    given tuple[H, T <: Tuple, CL <: Int](using
        h: AsColumnSortItem[H, CL],
        t: AsColumnSort[T, CL]
    ): AsColumnSort[H *: T, CL] with
        def asSorts(x: H *: T): List[Sort[?, ?]] =
            h.asSort(x.head) :: t.asSorts(x.tail)

    given tuple1[H, CL <: Int](using
        h: AsColumnSortItem[H, CL]
    ): AsColumnSort[H *: EmptyTuple, CL] with
        def asSorts(x: H *: EmptyTuple): List[Sort[?, ?]] =
            h.asSort(x.head) :: Nil