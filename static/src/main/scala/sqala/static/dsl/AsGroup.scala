package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.static.dsl.statement.query.Query
import sqala.static.metadata.AsSqlExpr

import scala.NamedTuple.NamedTuple

trait AsGroup[T]:
    type R

    def asGroup(x: T): R

    def asExprs(x: T): List[Expr[?, ?]]

object AsGroup:
    type Aux[T, O] = AsGroup[T]:
        type R = O

    given expr[T: AsSqlExpr, K <: ExprKind : CanInGroup]: Aux[Expr[T, K], Expr[T, Grouped]] =
        new AsGroup[Expr[T, K]]:
            type R = Expr[T, Grouped]

            def asGroup(x: Expr[T, K]): R =
                Expr(x.asSqlExpr)

            def asExprs(x: Expr[T, K]): List[Expr[?, ?]] =
                x :: Nil

    given query[T: AsSqlExpr, K <: ExprKind, Q <: Query[Expr[T, K], OneRow]]: Aux[Q, Expr[K, Grouped]] =
        new AsGroup[Q]:
            type R = Expr[K, Grouped]

            def asGroup(x: Q): R =
                Expr(SqlExpr.SubQuery(x.tree))

            def asExprs(x: Q): List[Expr[?, ?]] =
                Expr(SqlExpr.SubQuery(x.tree)) :: Nil

    given tuple[H, T <: Tuple](using
        h: AsGroup[H],
        t: AsGroup[T],
        e: AsExpr[H],
        a: AsSqlExpr[e.R],
        tt: ToTuple[t.R]
    ): Aux[H *: T, h.R *: tt.R] =
        new AsGroup[H *: T]:
            type R = h.R *: tt.R

            def asGroup(x: H *: T): R =
                h.asGroup(x.head) *: tt.toTuple(t.asGroup(x.tail))

            def asExprs(x: H *: T): List[Expr[?, ?]] =
                h.asExprs(x.head) ++ t.asExprs(x.tail)

    given tuple1[H](using
        h: AsGroup[H],
        e: AsExpr[H],
        a: AsSqlExpr[e.R]
    ): Aux[H *: EmptyTuple, h.R *: EmptyTuple] =
        new AsGroup[H *: EmptyTuple]:
            type R = h.R *: EmptyTuple

            def asGroup(x: H *: EmptyTuple): R =
                h.asGroup(x.head) *: EmptyTuple

            def asExprs(x: H *: EmptyTuple): List[Expr[?, ?]] =
                h.asExprs(x.head)

    given namedTuple[N <: Tuple, V <: Tuple : AsGroup as a](using
        tt: ToTuple[a.R]
    ): Aux[NamedTuple[N, V], NamedTuple[N, tt.R]] =
        new AsGroup[NamedTuple[N, V]]:
            type R = NamedTuple[N, tt.R]

            def asGroup(x: NamedTuple[N, V]): R =
                NamedTuple(tt.toTuple(a.asGroup(x.toTuple)))

            def asExprs(x: NamedTuple[N, V]): List[Expr[?, ?]] =
                a.asExprs(x.toTuple)