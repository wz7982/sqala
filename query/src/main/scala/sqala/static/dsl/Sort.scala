package sqala.static.dsl

import sqala.ast.order.*

case class Sort[T](
    private[sqala] val expr: Expr[?],
    private[sqala] val order: SqlOrderByOption,
    private[sqala] val nullsOrder: Option[SqlOrderByNullsOption]
):
    private[sqala] def asSqlOrderBy: SqlOrderBy = 
        SqlOrderBy(expr.asSqlExpr, Some(order), nullsOrder)