package sqala.dynamic.dsl

import sqala.ast.expr.SqlSubqueryQuantifier
import sqala.ast.statement.SqlQuery

final case class QuantifiedSubquery(
    private[sqala] val quantifier: SqlSubqueryQuantifier,
    private[sqala] val query: SqlQuery
)