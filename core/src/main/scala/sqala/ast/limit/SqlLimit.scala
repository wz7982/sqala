package sqala.ast.limit

import sqala.ast.expr.SqlExpr

case class SqlLimit(offset: Option[SqlExpr], fetch: Option[SqlFetch])

enum SqlFetchUnit(val unit: String):
    case RowCount extends SqlFetchUnit("")
    case Percentage extends SqlFetchUnit("PERCENT")

enum SqlFetchMode(val mode: String):
    case Only extends SqlFetchMode("ONLY")
    case WithTies extends SqlFetchMode("WITH TIES")

case class SqlFetch(limit: SqlExpr, unit: SqlFetchUnit, mode: SqlFetchMode)