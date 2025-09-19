package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.static.dsl.statement.query.Query
import sqala.static.metadata.AsSqlExpr

import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SQL expressions.")
trait AsExpr[T]:
    type R

    def exprs(x: T): List[Expr[?]]

    def asExpr(x: T): Expr[R] =
        val exprList = exprs(x)
        if exprList.size == 1 then
            Expr(exprList.head.asSqlExpr)
        else
            Expr(SqlExpr.Tuple(exprList.map(_.asSqlExpr)))

object AsExpr:
    type Aux[T, O] = AsExpr[T]:
        type R = O

    given value[T: AsSqlExpr as a]: Aux[T, T] =
        new AsExpr[T]:
            type R = T

            def exprs(x: T): List[Expr[?]] =
                Expr(a.asSqlExpr(x)) :: Nil

    given expr[T: AsSqlExpr]: Aux[Expr[T], T] =
        new AsExpr[Expr[T]]:
            type R = T

            def exprs(x: Expr[T]): List[Expr[?]] =
                x :: Nil

    given query[T, Q <: Query[T]](using a: AsExpr[T]): Aux[Q, a.R] =
        new AsExpr[Q]:
            type R = a.R

            def exprs(x: Q): List[Expr[?]] =
                Expr(SqlExpr.SubQuery(x.tree)) :: Nil

    given tuple[H, T <: Tuple](using
        h: AsExpr[H],
        t: AsExpr[T],
        tt: ToTuple[t.R],
        a: AsSqlExpr[h.R]
    ): Aux[H *: T, h.R *: tt.R] =
        new AsExpr[H *: T]:
            type R = h.R *: tt.R

            def exprs(x: H *: T): List[Expr[?]] =
                h.asExpr(x.head) :: t.exprs(x.tail)

    given tuple1[H](using 
        h: AsExpr[H],
        a: AsSqlExpr[h.R]
    ): Aux[H *: EmptyTuple, h.R] =
        new AsExpr[H *: EmptyTuple]:
            type R = h.R

            def exprs(x: H *: EmptyTuple): List[Expr[?]] =
                h.asExpr(x.head) :: Nil