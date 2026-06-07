package sqala.static.dsl

import sqala.ast.order.{SqlNullsOrdering, SqlOrdering, SqlOrderingItem}

/**
 * An intermediate sort specification used by `sortBy`,
 * and window ordering clause, combining an expression with an
 * ordering direction and nulls handling.
 */
final case class Sort[T, K <: ExprKind](
    private[sqala] val expr: Expr[?, ?],
    private[sqala] val ordering: SqlOrdering,
    private[sqala] val nullsOrdering: Option[SqlNullsOrdering]
):
    /**
     * Converts this sort specification to a `SqlOrderingItem` AST node.
     */
    private[sqala] def asSqlOrderingItem: SqlOrderingItem =
        SqlOrderingItem(expr.asSqlExpr, Some(ordering), nullsOrdering)