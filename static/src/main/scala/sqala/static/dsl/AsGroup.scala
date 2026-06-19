package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.metadata.AsSqlExpr
import sqala.static.dsl.statement.query.Query

import scala.NamedTuple.NamedTuple
import scala.compiletime.ops.int.>

/**
 * Lifts a single expression or subquery into a grouping item.
 * `CL` is the current query context level.
 * `KS` is the kind tuple for semantic validation.
 */
trait AsGroupItem[T, CL <: Int]:
    /**
     * The result type of the grouping item.
     */
    type R

    /**
     * The expression kind tuple of the grouping item.
     */
    type KS <: Tuple

    /**
     * Converts the value to a grouping item.
     */
    def asGroup(x: T): R

    /**
     * Converts the value to a list of expressions.
     */
    def asExprs(x: T): List[Expr[?, ?]]

object AsGroupItem:
    type Aux[T, CL <: Int, O, OKS <: Tuple] = AsGroupItem[T, CL]:
        type R = O

        type KS = OKS

    given expr[T: AsSqlExpr, EK <: ExprKind, CL <: Int](using
        kt: KindToTuple[EK],
        i: CanInGroup[kt.R],
        iv: IsKind[EK, Value],
        nv: iv.R =:= false
    ): Aux[Expr[T, EK], CL, Expr[T, Grouped[kt.R]], kt.R] =
        new AsGroupItem[Expr[T, EK], CL]:
            type R = Expr[T, Grouped[kt.R]]

            type KS = kt.R

            def asGroup(x: Expr[T, EK]): R =
                Expr(x.asSqlExpr)

            def asExprs(x: Expr[T, EK]): List[Expr[?, ?]] =
                x :: Nil

    given query[T, OKS <: Tuple, L <: Int, Q <: Query[T, OKS, L, OneRow], S <: QuerySize, CL <: Int](using
        a: AsExpr[T, CL],
        as: AsSqlExpr[a.R],
        i: CanInSort[OKS, S],
        refl: L > CL =:= true
    ): Aux[Q, CL, Expr[a.R, Grouped[OKS]], OKS] =
        new AsGroupItem[Q, CL]:
            type R = Expr[a.R, Grouped[OKS]]

            type KS = OKS

            def asGroup(x: Q): R =
                Expr(SqlExpr.Subquery(x.tree))

            def asExprs(x: Q): List[Expr[?, ?]] =
                Expr(SqlExpr.Subquery(x.tree)) :: Nil

/**
 * Lifts expressions, subqueries, tuples, and named tuples into grouping
 * items for the `groupBy` clause. `CL` is the current query context level.
 */
trait AsGroup[T, CL <: Int]:
    /**
     * The result type of the grouping clause.
     */
    type R

    /**
     * The expression kind tuple of the grouping clause.
     */
    type KS <: Tuple

    /**
     * Converts the value to a grouping clause.
     */
    def asGroup(x: T): R

    /**
     * Converts the value to a list of expressions.
     */
    def asExprs(x: T): List[Expr[?, ?]]

object AsGroup:
    type Aux[T, CL <: Int, O, OKS <: Tuple] = AsGroup[T, CL]:
        type R = O

        type KS = OKS

    given item[T, CL <: Int](using
        a: AsGroupItem[T, CL]
    ): Aux[T, CL, a.R, a.KS] =
        new AsGroup[T, CL]:
            type R = a.R

            type KS = a.KS

            def asGroup(x: T): R =
                a.asGroup(x)

            def asExprs(x: T): List[Expr[?, ?]] =
                a.asExprs(x)

    given tuple[H, T <: Tuple, CL <: Int](using
        h: AsGroup[H, CL],
        t: AsGroup[T, CL],
        tt: ToTuple[t.R],
        c: CombineKindTuple[h.KS, t.KS]
    ): Aux[H *: T, CL, h.R *: tt.R, c.R] =
        new AsGroup[H *: T, CL]:
            type R = h.R *: tt.R

            type KS = c.R

            def asGroup(x: H *: T): R =
                h.asGroup(x.head) *: tt.toTuple(t.asGroup(x.tail))

            def asExprs(x: H *: T): List[Expr[?, ?]] =
                h.asExprs(x.head) ++ t.asExprs(x.tail)

    given tuple1[H, CL <: Int](using
        h: AsGroup[H, CL],
        c: CombineKindTuple[h.KS, EmptyTuple]
    ): Aux[H *: EmptyTuple, CL, h.R *: EmptyTuple, c.R] =
        new AsGroup[H *: EmptyTuple, CL]:
            type R = h.R *: EmptyTuple

            type KS = c.R

            def asGroup(x: H *: EmptyTuple): R =
                h.asGroup(x.head) *: EmptyTuple

            def asExprs(x: H *: EmptyTuple): List[Expr[?, ?]] =
                h.asExprs(x.head)

    given namedTuple[N <: Tuple, V <: Tuple, CL <: Int](using
        a: AsGroup[V, CL],
        tt: ToTuple[a.R]
    ): Aux[NamedTuple[N, V], CL, NamedTuple[N, tt.R], a.KS] =
        new AsGroup[NamedTuple[N, V], CL]:
            type R = NamedTuple[N, tt.R]

            type KS = a.KS

            def asGroup(x: NamedTuple[N, V]): R =
                NamedTuple(tt.toTuple(a.asGroup(x.toTuple)))

            def asExprs(x: NamedTuple[N, V]): List[Expr[?, ?]] =
                a.asExprs(x.toTuple)