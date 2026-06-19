package sqala.static.dsl

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlQuantifiedComparisonOperator}

import scala.compiletime.ops.int.>

/**
 * Comparison operators.
 */
enum ComparisonOperator:
    /**
     * Equality comparison.
     */
    case Equal
    /**
     * Not-equal comparison.
     */
    case NotEqual
    /**
     * Greater-than comparison.
     */
    case GreaterThan
    /**
     * Greater-than-or-equal comparison.
     */
    case GreaterThanEqual
    /**
     * Less-than comparison.
     */
    case LessThan
    /**
     * Less-than-or-equal comparison.
     */
    case LessThanEqual

/**
 * Lifts expressions, and quantified subqueries to the right-hand
 * side of comparison operators.
 */
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
    def asSqlExpr(expr: SqlExpr, operator: ComparisonOperator, x: T): SqlExpr

object AsComparison:
    type Aux[T, CL <: Int, O, OK <: ExprKind] = AsComparison[T, CL]:
        type R = O

        type K = OK

    given expr[T, CL <: Int](using a: AsExpr[T, CL]): Aux[T, CL, a.R, a.K] =
        new AsComparison[T, CL]:
            type R = a.R

            type K = a.K

            def asSqlExpr(expr: SqlExpr, operator: ComparisonOperator, x: T): SqlExpr =
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
                SqlExpr.Binary(expr, sqlOperator, a.asExpr(x).asSqlExpr)

    given quantifiedSubquery[T, OKS <: Tuple, L <: Int, CL <: Int](using
        refl: L > CL =:= true
    ): Aux[QuantifiedSubquery[T, OKS, L], CL, T, Composite[OKS]] =
        new AsComparison[QuantifiedSubquery[T, OKS, L], CL]:
            type R = T

            type K = Composite[OKS]

            def asSqlExpr(expr: SqlExpr, operator: ComparisonOperator, x: QuantifiedSubquery[T, OKS, L]): SqlExpr =
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
                SqlExpr.QuantifiedComparisonPredicate(expr, sqlOperator, x.quantifier, x.tree)