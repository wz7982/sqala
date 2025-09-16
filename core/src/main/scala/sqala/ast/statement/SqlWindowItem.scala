package sqala.ast.statement

import sqala.ast.expr.SqlWindow

case class SqlWindowItem(name: String, window: SqlWindow)