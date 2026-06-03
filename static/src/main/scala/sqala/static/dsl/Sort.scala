package sqala.static.dsl

import sqala.ast.order.{SqlNullsOrdering, SqlOrdering, SqlOrderingItem}

/**
 * An intermediate sort specification combining an expression with an
 * ordering direction and nulls handling.
 */
final case class Sort[T, K <: ExprKind](
    private[sqala] val expr: Expr[?, ?],
    private[sqala] val ordering: SqlOrdering,
    private[sqala] val nullsOrdering: Option[SqlNullsOrdering]
):
    private[sqala] def asSqlOrderBy: SqlOrderingItem =
        SqlOrderingItem(expr.asSqlExpr, Some(ordering), nullsOrdering)