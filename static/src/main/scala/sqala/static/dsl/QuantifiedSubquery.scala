package sqala.static.dsl

import sqala.ast.expr.SqlSubqueryQuantifier
import sqala.ast.statement.SqlQuery

final case class QuantifiedSubquery[T, OKS <: Tuple, L <: Int](
    private[sqala] val quantifier: SqlSubqueryQuantifier,
    private[sqala] val tree: SqlQuery
)