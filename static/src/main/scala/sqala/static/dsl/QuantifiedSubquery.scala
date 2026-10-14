package sqala.static.dsl

import sqala.ir.IrQuery
import sqala.ast.expr.SqlSubqueryQuantifier

/**
 * A subquery qualified with `ANY`, `ALL`, used as the
 * right-hand side of comparison operators.
 */
final case class QuantifiedSubquery[T, OKS <: Tuple, L <: Int](
    private[sqala] val quantifier: SqlSubqueryQuantifier,
    private[sqala] val tree: IrQuery
)