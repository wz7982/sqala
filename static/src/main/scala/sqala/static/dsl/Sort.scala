package sqala.static.dsl

import sqala.ast.order.{SqlNullsOrdering, SqlOrderItem, SqlOrdering}

class Sort[T](
    private[sqala] val expr: Expr[?],
    private[sqala] val ordering: SqlOrdering,
    private[sqala] val nullsOrdering: Option[SqlNullsOrdering]
):
    private[sqala] def asSqlOrderBy: SqlOrderItem =
        SqlOrderItem(expr.asSqlExpr, Some(ordering), nullsOrdering)