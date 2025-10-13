package sqala.static.dsl

import sqala.ast.expr.SqlExpr

trait AsRightOperand[T]:
    type R

    def asExpr(x: T): Expr[R]

object AsRightOperand:
    type Aux[T, O] = AsRightOperand[T]:
        type R = O

    given expr[T: AsExpr as a]: Aux[T, a.R] =
        new AsRightOperand[T]:
            type R = a.R

            def asExpr(x: T): Expr[R] =
                a.asExpr(x)

    given subLink[T]: Aux[SubLink[T], T] =
        new AsRightOperand[SubLink[T]]:
            type R = T

            def asExpr(x: SubLink[T]): Expr[T] =
                Expr(SqlExpr.SubLink(x.quantifier, x.tree))