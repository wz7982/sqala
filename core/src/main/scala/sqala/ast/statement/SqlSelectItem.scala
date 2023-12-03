package sqala.ast.statement

import sqala.ast.expr.SqlExpr

case class SqlSelectItem(expr: SqlExpr, alias: Option[String])