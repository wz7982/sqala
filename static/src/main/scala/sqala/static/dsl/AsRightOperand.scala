package sqala.static.dsl

import sqala.ast.expr.SqlExpr

trait AsRightOperand[T]:
    type R

    type K <: ExprKind

    def asExpr(x: T): Expr[R, K]

object AsRightOperand:
    type Aux[T, O, OK <: ExprKind] = AsRightOperand[T]:
        type R = O

        type K = OK

    given expr[T: AsExpr as a]: Aux[T, a.R, a.K] =
        new AsRightOperand[T]:
            type R = a.R

            type K = a.K

            def asExpr(x: T): Expr[R, K] =
                a.asExpr(x)

    given subLink[T]: Aux[SubLink[T], T, Value] =
        new AsRightOperand[SubLink[T]]:
            type R = T

            type K = Value

            def asExpr(x: SubLink[T]): Expr[T, K] =
                Expr(SqlExpr.SubLink(x.quantifier, x.tree))