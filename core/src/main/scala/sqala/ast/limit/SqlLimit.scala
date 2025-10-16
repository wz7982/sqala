package sqala.ast.limit

import sqala.ast.expr.SqlExpr

case class SqlLimit(offset: Option[SqlExpr], fetch: Option[SqlFetch])

enum SqlFetchUnit:
    case RowCount
    case Percentage

enum SqlFetchMode:
    case Only
    case WithTies

case class SqlFetch(limit: SqlExpr, unit: SqlFetchUnit, mode: SqlFetchMode)