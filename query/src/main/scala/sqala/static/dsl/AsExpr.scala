package sqala.static.dsl

import sqala.common.AsSqlExpr
import sqala.static.statement.query.Query

import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SQL expressions.")
trait AsExpr[T]:
    type R

    def exprs(x: T): List[Expr[?]]

    def asExpr(x: T): Expr[R] =
        val exprList = exprs(x)
        if exprList.size == 1 then
            Expr.Ref(exprList.head.asSqlExpr)
        else
            Expr.Tuple(exprList)

object AsExpr:
    type Aux[T, O] = AsExpr[T]:
        type R = O

    given value[T: AsSqlExpr as a]: Aux[T, T] =
        new AsExpr[T]:
            type R = T

            def exprs(x: T): List[Expr[?]] =
                Expr.Literal(x, a) :: Nil

    given expr[T]: Aux[Expr[T], T] =
        new AsExpr[Expr[T]]:
            type R = T

            def exprs(x: Expr[T]): List[Expr[?]] =
                x :: Nil

    given query[Q](using m: AsExpr[Q]): Aux[Query[Q], m.R] =
        new AsExpr[Query[Q]]:
            type R = m.R

            def exprs(x: Query[Q]): List[Expr[?]] =
                Expr.SubQuery(x.ast) :: Nil

    given tuple[H, T <: Tuple](using
        h: AsExpr[H],
        t: AsExpr[T],
        tt: ToTuple[t.R]
    ): Aux[H *: T, h.R *: tt.R] =
        new AsExpr[H *: T]:
            type R = h.R *: tt.R

            def exprs(x: H *: T): List[Expr[?]] =
                h.asExpr(x.head) :: t.exprs(x.tail)

    given tuple1[H](using h: AsExpr[H]): Aux[H *: EmptyTuple, h.R] =
        new AsExpr[H *: EmptyTuple]:
            type R = h.R

            def exprs(x: H *: EmptyTuple): List[Expr[?]] =
                h.asExpr(x.head) :: Nil