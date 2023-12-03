package sqala.ast.order

import sqala.ast.expr.SqlExpr

case class SqlOrderBy(expr: SqlExpr, order: Option[SqlOrderByOption])