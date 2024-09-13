package sqala.dsl.statement.query

case class QueryContext(var tableIndex: Int)

case class WithContext(alias: String)