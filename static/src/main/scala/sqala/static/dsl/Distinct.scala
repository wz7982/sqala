package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.order.{SqlNullsOrdering, SqlOrdering, SqlOrderingItem}

import scala.NamedTuple.NamedTuple

/**
 * Wraps an expression after `mapDistinct`, used to validate the
 * semantic legality of `sortBy` following a `DISTINCT` operation.
 * Sort methods (`asc`, `desc`, etc.) can be chained for ordering.
 */
final case class Distinct[T, L <: Int](private[sqala] val expr: SqlExpr):
    /**
     * Ascending sort order. Maps to SQL `ASC`. Used in `sortBy` clause after `mapDistinct`.
     *
     * {{{
     * from(User).mapDistinct(u => u.nickname).sortBy(n => n.asc)
     * }}}
     */
    def asc: DistinctSort[T, L] =
        DistinctSort(Expr(expr), SqlOrdering.Asc, None)

    /**
     * Ascending sort order with nulls first. Maps to SQL
     * `ASC NULLS FIRST`. Used in `sortBy` clause after `mapDistinct`.
     *
     * {{{
     * from(User).mapDistinct(u => u.nickname).sortBy(n => n.ascNullsFirst)
     * }}}
     */
    def ascNullsFirst: DistinctSort[T, L] =
        DistinctSort(Expr(expr), SqlOrdering.Asc, Some(SqlNullsOrdering.First))

    /**
     * Ascending sort order with nulls last. Maps to SQL
     * `ASC NULLS LAST`. Used in `sortBy` clause clause after `mapDistinct`.
     *
     * {{{
     * from(User).mapDistinct(u => u.nickname).sortBy(n => n.ascNullsLast)
     * }}}
     */
    def ascNullsLast: DistinctSort[T, L] =
        DistinctSort(Expr(expr), SqlOrdering.Asc, Some(SqlNullsOrdering.Last))

    /**
     * Descending sort order. Maps to SQL `DESC`. Used in `sortBy` clause after `mapDistinct`.
     *
     * {{{
     * from(User).mapDistinct(u => u.nickname).sortBy(n => n.desc)
     * }}}
     */
    def desc: DistinctSort[T, L] =
        DistinctSort(Expr(expr), SqlOrdering.Desc, None)

    /**
     * Descending sort order with nulls first. Maps to SQL
     * `DESC NULLS FIRST`. Used in `sortBy` clause after `mapDistinct`.
     *
     * {{{
     * from(User).mapDistinct(u => u.nickname).sortBy(n => n.descNullsFirst)
     * }}}
     */
    def descNullsFirst: DistinctSort[T, L] =
        DistinctSort(Expr(expr), SqlOrdering.Desc, Some(SqlNullsOrdering.First))

    /**
     * Descending sort order with nulls last. Maps to SQL
     * `DESC NULLS LAST`. Used in `sortBy` clause after `mapDistinct`.
     *
     * {{{
     * from(User).mapDistinct(u => u.nickname).sortBy(n => n.descNullsLast)
     * }}}
     */
    def descNullsLast: DistinctSort[T, L] =
        DistinctSort(Expr(expr), SqlOrdering.Desc, Some(SqlNullsOrdering.Last))

/**
 * A sort specification for a `Distinct` expression.
 */
final case class DistinctSort[T, L <: Int](
    private[sqala] val expr: Expr[?, ?],
    private[sqala] val ordering: SqlOrdering,
    private[sqala] val nullsOrdering: Option[SqlNullsOrdering]
):
    private[sqala] def asSqlOrderBy: SqlOrderingItem =
        SqlOrderingItem(expr.asSqlExpr, Some(ordering), nullsOrdering)

/**
 * Lifts expressions and tuples into `Distinct` for validating
 * the semantic legality of `sortBy` after `mapDistinct`. `CL` is the
 * current query context level.
 */
trait ToDistinct[T, CL <: Int]:
    /**
     * The transformed type.
     */
    type R

    /**
     * Converts the value to a `Distinct` type.
     */
    def toDistinct(x: T): R

object ToDistinct:
    type Aux[T, CL <: Int, O] = ToDistinct[T, CL]:
        type R = O

    given expr[T, EK <: ExprKind, CL <: Int]: Aux[Expr[T, EK], CL, Distinct[T, CL]] =
        new ToDistinct[Expr[T, EK], CL]:
            type R = Distinct[T, CL]

            def toDistinct(x: Expr[T, EK]): R =
                Distinct(x.asSqlExpr)

    given tuple[H, T <: Tuple, CL <: Int](using
        h: ToDistinct[H, CL],
        t: ToDistinct[T, CL],
        tt: ToTuple[t.R]
    ): Aux[H *: T, CL, h.R *: tt.R] =
        new ToDistinct[H *: T, CL]:
            type R = h.R *: tt.R

            def toDistinct(x: H *: T): R =
                h.toDistinct(x.head) *: tt.toTuple(t.toDistinct(x.tail))

    given tuple1[H, CL <: Int](using
        h: ToDistinct[H, CL]
    ): Aux[H *: EmptyTuple, CL, h.R *: EmptyTuple] =
        new ToDistinct[H *: EmptyTuple, CL]:
            type R = h.R *: EmptyTuple

            def toDistinct(x: H *: EmptyTuple): R =
                h.toDistinct(x.head) *: EmptyTuple

    given nameTuple[N <: Tuple, V <: Tuple, CL <: Int](using
        td: ToDistinct[V, CL],
        tt: ToTuple[td.R]
    ): Aux[NamedTuple[N, V], CL, NamedTuple[N, tt.R]] =
        new ToDistinct[NamedTuple[N, V], CL]:
            type R = NamedTuple[N, tt.R]

            def toDistinct(x: NamedTuple[N, V]): NamedTuple[N, tt.R] =
                NamedTuple(tt.toTuple(td.toDistinct(x.toTuple)))