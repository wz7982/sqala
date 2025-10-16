package sqala.ast.order

import sqala.ast.expr.SqlExpr

case class SqlOrderingItem(
    expr: SqlExpr, 
    ordering: Option[SqlOrdering], 
    nullsOrdering: Option[SqlNullsOrdering]
)

enum SqlOrdering:
    case Asc
    case Desc

enum SqlNullsOrdering:
    case First
    case Last