package sqala.ast.limit

import sqala.ast.expr.SqlExpr

case class SqlLimit(limit: SqlExpr, offset: SqlExpr)