package sqala.dsl.statement.query

import sqala.ast.statement.{SqlQuery, SqlWithItem}
import sqala.ast.table.SqlTable

class WithRecursive[T](val ast: SqlQuery.Cte)

object WithRecursive:
    def apply[N <: Tuple, WN <: Tuple, V <: Tuple](query: Query[NamedTupleWrapper[N, V]])(f: Option[WithContext] ?=> Query[NamedTupleWrapper[N, V]] => Query[NamedTupleWrapper[WN, V]])(using s: SelectItem[NamedQuery[N, V]]): WithRecursive[NamedTupleWrapper[N, V]] =
        val aliasName = "cte"
        given Option[WithContext] = Some(WithContext(aliasName))
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