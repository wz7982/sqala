package sqala.dsl.statement.query

import sqala.ast.statement.{SqlQuery, SqlWithItem}
import sqala.ast.table.SqlTable

class WithRecursive[T](val ast: SqlQuery.Cte)

object WithRecursive:
    def apply[T](query: Query[T])(f: Option[WithRecursiveContext] ?=> Query[T] => Query[T])(using s: SelectItem[NamedQuery[T]]): WithRecursive[T] =
        val aliasName = "cte"
        given Option[WithRecursiveContext] = Some(WithRecursiveContext(aliasName))
        val namedQuery = NamedQuery(query, aliasName)
        val cteQuery = f(query)
        val ast: SqlQuery.Cte = SqlQuery.Cte(
            SqlWithItem(aliasName, cteQuery.ast, s.selectItems(namedQuery, 0).map(_.alias.get)) :: Nil, 
            true, 
            SqlQuery.Select(
                select = s.selectItems(namedQuery, 0),
                from = SqlTable.IdentTable(aliasName, None) :: Nil
            )
        )
        new WithRecursive(ast)

case class WithRecursiveContext(alias: String)