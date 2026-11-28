package sqala.static.dsl

import sqala.ast.expr.{SqlBinaryOperator, SqlQuantifiedComparisonOperator}
import sqala.ir.IrExpr

import scala.annotation.{implicitAmbiguous, implicitNotFound}

/**
 * Comparison operators.
 */
enum ComparisonOperator:
    case Equal
    case NotEqual
    case GreaterThan
    case GreaterThanEqual
    case LessThan
    case LessThanEqual

/**
 * Lifts expressions, and quantified subqueries to the right-hand
 * side of comparison operators.
 */
@implicitAmbiguous("This value cannot be converted to a comparison operand.")
@implicitNotFound("This value cannot be converted to a comparison operand.")
trait AsComparison[T, CL <: Int]:
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
    def asIrExpr(expr: IrExpr, operator: ComparisonOperator, x: T): IrExpr

object AsComparison:
    type Aux[T, CL <: Int, O, OK <: ExprKind] = AsComparison[T, CL]:
        type R = O

        type K = OK

    given expr[T, CL <: Int](using a: AsExpr[T, CL]): Aux[T, CL, a.R, a.K] =
        new AsComparison[T, CL]:
            type R = a.R

            type K = a.K

            def asIrExpr(expr: IrExpr, operator: ComparisonOperator, x: T): IrExpr =
                val sqlOperator =
                    operator match
                        case ComparisonOperator.Equal =>
                            SqlBinaryOperator.Equal
                        case ComparisonOperator.NotEqual =>
                            SqlBinaryOperator.NotEqual
                        case ComparisonOperator.GreaterThan =>
                            SqlBinaryOperator.GreaterThan
                        case ComparisonOperator.GreaterThanEqual =>
                            SqlBinaryOperator.GreaterThanEqual
                        case ComparisonOperator.LessThan =>
                            SqlBinaryOperator.LessThan
                        case ComparisonOperator.LessThanEqual =>
                            SqlBinaryOperator.LessThanEqual
                IrExpr.Binary(expr, sqlOperator, a.asExpr(x).expr)

    given quantifiedSubquery[T, OKS <: Tuple, L <: Int, CL <: Int](using
        vss: ValidateSubqueryScope[L, CL]
    ): Aux[QuantifiedSubquery[T, OKS, L], CL, T, Composite[OKS]] =
        new AsComparison[QuantifiedSubquery[T, OKS, L], CL]:
            type R = T

            type K = Composite[OKS]

            def asIrExpr(expr: IrExpr, operator: ComparisonOperator, x: QuantifiedSubquery[T, OKS, L]): IrExpr =
                val sqlOperator =
                    operator match
                        case ComparisonOperator.Equal =>
                            SqlQuantifiedComparisonOperator.Equal
                        case ComparisonOperator.NotEqual =>
                            SqlQuantifiedComparisonOperator.NotEqual
                        case ComparisonOperator.GreaterThan =>
                            SqlQuantifiedComparisonOperator.GreaterThan
                        case ComparisonOperator.GreaterThanEqual =>
                            SqlQuantifiedComparisonOperator.GreaterThanEqual
                        case ComparisonOperator.LessThan =>
                            SqlQuantifiedComparisonOperator.LessThan
                        case ComparisonOperator.LessThanEqual =>
                            SqlQuantifiedComparisonOperator.LessThanEqual
                IrExpr.QuantifiedComparisonPredicate(expr, sqlOperator, x.quantifier, x.tree)