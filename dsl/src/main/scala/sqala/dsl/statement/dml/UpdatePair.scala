package sqala.dsl.statement.dml

import sqala.dsl.Expr

case class UpdatePair(columnName: String, updateExpr: Expr[?, ?])