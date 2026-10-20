package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.order.SqlOrdering
import sqala.metadata.AsSqlExpr
import sqala.static.dsl.statement.query.Query

import scala.compiletime.ops.int.>
import sqala.ir.IrExpr
import scala.quoted.Type
import scala.quoted.Quotes
import scala.util.NotGiven

// TODO =:=的要改一下 fallback
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

    given expr[T, S <: QuerySize, CL <: Int](using
        a: AsExpr[T, CL],
        as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
        kt: KindToTuple[a.K],
        i: CanInSort[kt.R, S],
        nv: SortByNotAllowedValue[a.K]
    ): Aux[T, S, CL, kt.R] =
        new AsSortItem[T, S, CL]:
            type KS = kt.R

            def asSort(x: T): Sort[?, ?] =
                Sort(a.asExpr(x), SqlOrdering.Asc, None)

    given sort[T, EK <: ExprKind, S <: QuerySize, CL <: Int](using
        as: ValidateArgType[AsSqlExpr, T, "scalar"],
        kt: KindToTuple[EK],
        i: CanInSort[kt.R, S],
        nv: SortByNotAllowedValue[EK]
    ): Aux[Sort[T, EK], S, CL, kt.R] =
        new AsSortItem[Sort[T, EK], S, CL]:
            type KS = kt.R

            def asSort(x: Sort[T, EK]): Sort[?, ?] =
                x

    inline given fallback[T, S <: QuerySize, CL <: Int](using
        NotGiven[AsSortItem[T, S, CL]]
    ): Aux[T, S, CL, EmptyTuple] =
        ${ errorMacro[T] }

    def errorMacro[T: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val t = Type.show[T]

        report.errorAndAbort(s"Cannot convert type $t for the SQL ordering list.")

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

    given expr[T, CL <: Int](using
        a: AsExpr[T, CL],
        as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
        kt: KindToTuple[a.K],
        i: CanInGroupedSort[kt.R],
        nv: SortByNotAllowedValue[a.K]
    ): Aux[T, CL, kt.R] =
        new AsGroupedSortItem[T, CL]:
            type KS = kt.R

            def asSort(x: T): Sort[?, ?] =
                Sort(a.asExpr(x), SqlOrdering.Asc, None)

    given sort[T, EK <: ExprKind, CL <: Int](using
        as: ValidateArgType[AsSqlExpr, T, "scalar"],
        kt: KindToTuple[EK],
        i: CanInGroupedSort[kt.R],
        nv: SortByNotAllowedValue[EK]
    ): Aux[Sort[T, EK], CL, kt.R] =
        new AsGroupedSortItem[Sort[T, EK], CL]:
            type KS = kt.R

            def asSort(x: Sort[T, EK]): Sort[?, ?] =
                x

    inline given fallback[T, CL <: Int](using
        NotGiven[AsGroupedSortItem[T, CL]]
    ): Aux[T, CL, EmptyTuple] =
        ${ errorMacro[T] }

    def errorMacro[T: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val t = Type.show[T]

        report.errorAndAbort(s"Cannot convert type $t for the SQL ordering list.")

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
    given expr[T, EL <: Int, CL <: Int](using
        ValidateArgType[AsSqlExpr, T, "scalar"],
        CanInDistinctSort[Distinct[T, EL], CL]
    ): AsDistinctSortItem[Distinct[T, EL], CL] with
        def asSort(x: Distinct[T, EL]): DistinctSort[?, ?] =
            DistinctSort(Expr(x.expr), SqlOrdering.Asc, None)

    given sort[T, EL <: Int, CL <: Int](using
        ValidateArgType[AsSqlExpr, T, "scalar"],
        CanInDistinctSort[DistinctSort[T, EL], CL]
    ): AsDistinctSortItem[DistinctSort[T, EL], CL] with
        def asSort(x: DistinctSort[T, EL]): DistinctSort[?, ?] =
            x

    given wrongExpr[T, CL <: Int](using
        a: AsExpr[T, CL],
        as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
        i: CanInDistinctSort[T, CL]
    ): AsDistinctSortItem[T, CL] with
        def asSort(x: T): DistinctSort[?, ?] =
            DistinctSort(a.asExpr(x), SqlOrdering.Asc, None)

    given wrongSort[T, K <: ExprKind, CL <: Int](using
        ValidateArgType[AsSqlExpr, T, "scalar"],
        CanInDistinctSort[Sort[T, K], CL]
    ): AsDistinctSortItem[Sort[T, K], CL] with
        def asSort(x: Sort[T, K]): DistinctSort[?, ?] =
            DistinctSort(x.expr, x.ordering, x.nullsOrdering)

    inline given fallback[T, CL <: Int](using
        NotGiven[AsDistinctSortItem[T, CL]]
    ): AsDistinctSortItem[T, CL] =
        ${ errorMacro[T] }

    def errorMacro[T: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val t = Type.show[T]

        report.errorAndAbort(s"Cannot convert type $t for the SQL ordering list.")

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

    given expr[T, CL <: Int](using
        a: AsExpr[T, CL],
        as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
        kt: KindToTuple[a.K],
        i: CanInWindow[kt.R]
    ): Aux[T, CL, a.R, kt.R] =
        new AsOverSortItem[T, CL]:
            type R = a.R

            type KS = kt.R

            def asSort(x: T): Sort[?, ?] =
                Sort(a.asExpr(x), SqlOrdering.Asc, None)

    given sort[T, EK <: ExprKind, CL <: Int](using
        as: ValidateArgType[AsSqlExpr, T, "scalar"],
        kt: KindToTuple[EK],
        i: CanInWindow[kt.R]
    ): Aux[Sort[T, EK], CL, T, kt.R] =
        new AsOverSortItem[Sort[T, EK], CL]:
            type R = T

            type KS = kt.R

            def asSort(x: Sort[T, EK]): Sort[?, ?] =
                x

    inline given fallback[T, CL <: Int](using
        NotGiven[AsOverSortItem[T, CL]]
    ): Aux[T, CL, T, EmptyTuple] =
        ${ errorMacro[T] }

    def errorMacro[T: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val t = Type.show[T]

        report.errorAndAbort(s"Cannot convert type $t for the SQL ordering list.")

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