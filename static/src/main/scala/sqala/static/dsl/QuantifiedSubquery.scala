package sqala.static.dsl

import sqala.ast.expr.SqlSubqueryQuantifier
import sqala.ir.IrQuery

/**
 * A subquery qualified with `ANY`, `ALL`, used as the
 * right-hand side of comparison operators.
 */
final class QuantifiedSubquery[T, OKS <: Tuple, L <: Int](
    private[sqala] val quantifier: SqlSubqueryQuantifier,
    private[sqala] val tree: IrQuery
)