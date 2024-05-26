package sqala.dsl.statement.dml

import sqala.dsl.{Column, Expr}

case class UpdatePair(expr: Column[?], updateExpr: Expr[?])