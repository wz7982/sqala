package sqala.ast.expr

case class SqlCase(whenExpr: SqlExpr, thenExpr: SqlExpr)