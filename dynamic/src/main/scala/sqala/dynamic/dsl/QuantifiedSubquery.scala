package sqala.dynamic.dsl

import sqala.ast.expr.SqlSubqueryQuantifier
import sqala.ast.statement.SqlQuery

/**
 * A quantified subquery used on the right-hand side of a comparison,
 * i.e. `[ANY|ALL] (query)`.
 *
 * Instances are created with the `any` and `all` helpers and consumed by
 * the comparison operators on [[Expr]] (`===`, `<>`, `>`, `>=`, `<`,
 * `<=`).
 *
 * {{{
 * column("id") === any(select(column("user_id").as("id")))
 * }}}
 */
final class QuantifiedSubquery(
    private[sqala] val quantifier: SqlSubqueryQuantifier,
    private[sqala] val query: SqlQuery
)