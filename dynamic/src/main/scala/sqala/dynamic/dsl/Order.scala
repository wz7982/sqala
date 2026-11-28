package sqala.dynamic.dsl

import sqala.ast.order.SqlOrderingItem

/**
 * A dynamic SQL ordering item (`expr [ASC|DESC] [NULLS FIRST|LAST]`),
 * built via the ordering helpers on Expr such as `asc`, `desc`,
 * or `ascNullsFirst`.
 *
 * It is used by `orderBy` for window specifications and by query
 * `orderBy` clauses.
 *
 * {{{
 * column("id").descNullsLast
 * }}}
 */
final class Order(private[sqala] val order: SqlOrderingItem):
    /**
     * Returns the underlying AST ordering item.
     *
     * {{{
     * column("id").desc.ast
     * }}}
     */
    def ast: SqlOrderingItem =
        order