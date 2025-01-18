package sqala.static.statement.dml

import sqala.ast.expr.SqlExpr

case class UpdatePair(columnName: String, updateExpr: SqlExpr)