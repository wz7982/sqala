package sqala.static.dsl

import sqala.ast.order.{SqlNullsOrdering, SqlOrdering, SqlOrderingItem}

class Sort(
    private[sqala] val expr: Expr[?],
    private[sqala] val ordering: SqlOrdering,
    private[sqala] val nullsOrdering: Option[SqlNullsOrdering]
):
    private[sqala] def asSqlOrderBy: SqlOrderingItem =
        SqlOrderingItem(expr.asSqlExpr, Some(ordering), nullsOrdering)