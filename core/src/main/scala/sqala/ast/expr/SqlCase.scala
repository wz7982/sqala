package sqala.ast.expr

case class SqlCase(expr: SqlExpr, thenExpr: SqlExpr)