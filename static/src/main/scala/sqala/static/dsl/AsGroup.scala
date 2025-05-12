package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.metadata.AsSqlExpr
import sqala.static.dsl.statement.query.Query

import scala.annotation.implicitNotFound

@implicitNotFound("Type ${T} cannot be converted to SQL expressions.")
trait AsGroup[T]:
    type R

    def exprs(x: T): List[Expr[?]]

    def asExpr(x: T): Expr[R] =
        val exprList = exprs(x)
        if exprList.size == 1 then
            Expr(exprList.head.asSqlExpr)
        else
            Expr(SqlExpr.Tuple(exprList.map(_.asSqlExpr)))

object AsGroup:
    type Aux[T, O] = AsGroup[T]:
        type R = O

    given expr[T: AsSqlExpr]: Aux[Expr[T], T] =
        new AsGroup[Expr[T]]:
            type R = T

            def exprs(x: Expr[T]): List[Expr[?]] =
                x :: Nil

    given query[T: AsSqlExpr, Q <: Query[Expr[T]]]: Aux[Q, T] =
        new AsGroup[Q]:
            type R = T

            def exprs(x: Q): List[Expr[?]] =
                Expr(SqlExpr.SubQuery(x.tree)) :: Nil

    given tuple[H, T <: Tuple](using
        h: AsGroup[H],
        t: AsGroup[T],
        tt: ToTuple[t.R],
        a: AsSqlExpr[h.R]
    ): Aux[H *: T, h.R *: tt.R] =
        new AsGroup[H *: T]:
            type R = h.R *: tt.R

            def exprs(x: H *: T): List[Expr[?]] =
                h.asExpr(x.head) :: t.exprs(x.tail)

    given tuple1[H](using 
        h: AsGroup[H],
        a: AsSqlExpr[h.R]
    ): Aux[H *: EmptyTuple, h.R] =
        new AsGroup[H *: EmptyTuple]:
            type R = h.R

            def exprs(x: H *: EmptyTuple): List[Expr[?]] =
                h.asExpr(x.head) :: Nil