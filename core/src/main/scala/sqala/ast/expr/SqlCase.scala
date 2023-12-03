package sqala.ast.expr

final case class SqlCase(expr: SqlExpr, thenExpr: SqlExpr)