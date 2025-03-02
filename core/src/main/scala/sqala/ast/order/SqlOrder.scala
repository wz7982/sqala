package sqala.ast.order

import sqala.ast.expr.SqlExpr

case class SqlOrderItem(expr: SqlExpr, order: Option[SqlOrderOption], nullsOrder: Option[SqlOrderNullsOption])

enum SqlOrderOption(val order: String):
    case Asc extends SqlOrderOption("ASC")
    case Desc extends SqlOrderOption("DESC")

enum SqlOrderNullsOption(val order: String):
    case First extends SqlOrderNullsOption("NULLS FIRST")
    case Last extends SqlOrderNullsOption("NULLS LAST")