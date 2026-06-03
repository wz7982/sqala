package sqala.static.dsl

import sqala.ast.expr.SqlExpr

import scala.compiletime.ops.int.>

/**
 * Lifts expressions, and quantified subqueries to the right-hand
 * side of comparison operators.
 */
trait AsRightOperand[T, CL <: Int]:
    /**
     * The result type of the operand.
     */
    type R

    /**
     * The expression kind of the operand.
     */
    type K <: ExprKind

    /**
     * Converts the value to an expression.
     */
    def asExpr(x: T): Expr[R, K]

object AsRightOperand:
    type Aux[T, CL <: Int, O, OK <: ExprKind] = AsRightOperand[T, CL]:
        type R = O

        type K = OK

    given expr[T, CL <: Int](using a: AsExpr[T, CL]): Aux[T, CL, a.R, a.K] =
        new AsRightOperand[T, CL]:
            type R = a.R

            type K = a.K

            def asExpr(x: T): Expr[R, K] =
                a.asExpr(x)

    given quantifiedSubquery[T, OKS <: Tuple, L <: Int, CL <: Int](using
        refl: L > CL =:= true
    ): Aux[QuantifiedSubquery[T, OKS, L], CL, T, Composite[OKS]] =
        new AsRightOperand[QuantifiedSubquery[T, OKS, L], CL]:
            type R = T

            type K = Composite[OKS]

            def asExpr(x: QuantifiedSubquery[T, OKS, L]): Expr[R, K] =
                Expr(SqlExpr.Subquery(Some(x.quantifier), x.tree))