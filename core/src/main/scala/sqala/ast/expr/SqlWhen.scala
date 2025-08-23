package sqala.ast.expr

case class SqlWhen(whenExpr: SqlExpr, thenExpr: SqlExpr)