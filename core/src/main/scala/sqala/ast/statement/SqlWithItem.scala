package sqala.ast.statement

/**
 * A common table expression (CTE) item in a `WITH` clause.
 *
 * Renders as `"name" [("column" [, ...])] AS (query)`.
 *
 * @param name the CTE name.
 * @param columnNames optional column name list.
 * @param query the CTE query.
 */
case class SqlWithItem(name: String, columnNames: List[String], query: SqlQuery)