package sqala.ast.expr

case class SqlCaseBranch(when: SqlExpr, `then`: SqlExpr)