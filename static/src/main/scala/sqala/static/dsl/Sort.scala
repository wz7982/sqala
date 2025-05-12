package sqala.static.dsl

import sqala.ast.order.{SqlOrderItem, SqlOrderNullsOption, SqlOrderOption}

class Sort[T](
    private[sqala] val expr: Expr[?],
    private[sqala] val order: SqlOrderOption,
    private[sqala] val nullsOrder: Option[SqlOrderNullsOption]
):
    private[sqala] def asSqlOrderBy: SqlOrderItem =
        SqlOrderItem(expr.asSqlExpr, Some(order), nullsOrder)