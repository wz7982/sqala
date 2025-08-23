package sqala.ast.expr

case class SqlWhen(when: SqlExpr, `then`: SqlExpr)