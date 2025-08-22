package sqala.ast.order

import sqala.ast.expr.SqlExpr

case class SqlOrderItem(
    expr: SqlExpr, 
    ordering: Option[SqlOrdering], 
    nullsOrdering: Option[SqlNullsOrdering]
)

enum SqlOrdering(val order: String):
    case Asc extends SqlOrdering("ASC")
    case Desc extends SqlOrdering("DESC")

enum SqlNullsOrdering(val order: String):
    case First extends SqlNullsOrdering("NULLS FIRST")
    case Last extends SqlNullsOrdering("NULLS LAST")