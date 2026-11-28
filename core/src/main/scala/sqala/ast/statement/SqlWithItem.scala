package sqala.ast.statement

/**
 * A common table expression (CTE) item in a `WITH` clause.
 *
 * Renders as `"name" [("column" [, ...])] AS (query)`.
 */
case class SqlWithItem(name: String, columnNames: List[String], query: SqlQuery)