package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.order.{SqlNullsOrdering, SqlOrdering, SqlOrderingItem}

import scala.NamedTuple.NamedTuple

final case class Distinct[T, L <: Int](private[sqala] val expr: SqlExpr):
    def asc: DistinctSort[T, L] =
        DistinctSort(Expr(expr), SqlOrdering.Asc, None)

    def ascNullsFirst: DistinctSort[T, L] =
        DistinctSort(Expr(expr), SqlOrdering.Asc, Some(SqlNullsOrdering.First))

    def ascNullsLast: DistinctSort[T, L] =
        DistinctSort(Expr(expr), SqlOrdering.Asc, Some(SqlNullsOrdering.Last))

    def desc: DistinctSort[T, L] =
        DistinctSort(Expr(expr), SqlOrdering.Desc, None)

    def descNullsFirst: DistinctSort[T, L] =
        DistinctSort(Expr(expr), SqlOrdering.Desc, Some(SqlNullsOrdering.First))

    def descNullsLast: DistinctSort[T, L] =
        DistinctSort(Expr(expr), SqlOrdering.Desc, Some(SqlNullsOrdering.Last))

final case class DistinctSort[T, L <: Int](
    private[sqala] val expr: Expr[?, ?],
    private[sqala] val ordering: SqlOrdering,
    private[sqala] val nullsOrdering: Option[SqlNullsOrdering]
):
    private[sqala] def asSqlOrderBy: SqlOrderingItem =
        SqlOrderingItem(expr.asSqlExpr, Some(ordering), nullsOrdering)

trait ToDistinct[T, CL <: Int]:
    type R

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