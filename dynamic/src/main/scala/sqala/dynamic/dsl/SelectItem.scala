package sqala.dynamic.dsl

import sqala.ast.statement.SqlSelectItem

/**
 * A single projection item of a dynamic `SELECT` query. It is usually an
 * aliased expression, created with `Expr.as`:
 *
 * {{{
 * select(column("id").as("id"), column("name").as("name"))
 * }}}
 */
final class SelectItem(private[sqala] val item: SqlSelectItem):
    /**
     * Returns the underlying AST select item.
     *
     * {{{
     * column("id").as("id").ast
     * }}}
     */
    def ast: SqlSelectItem =
        item