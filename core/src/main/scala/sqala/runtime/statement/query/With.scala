package sqala.runtime.statement.query

import sqala.ast.statement.{SqlQuery, SqlWithItem}
import sqala.runtime.SubQueryTable

class With(val ast: SqlQuery.Cte) extends Query:
    def recursive: With = new With(ast.copy(recursive = true))

    infix def select(query: Query): With = new With(ast.copy(query = query.ast))

object With:
    def apply(items: SubQueryTable*): With =
        val queries = items.toList.map: i =>
            SqlWithItem(i.alias, i.query.ast, Nil)
        new With(ast = SqlQuery.Cte(queries, false, queries.head.query))