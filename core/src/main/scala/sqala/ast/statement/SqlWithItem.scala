package sqala.ast.statement

case class SqlWithItem(name: String, query: SqlQuery, columnNames: List[String])