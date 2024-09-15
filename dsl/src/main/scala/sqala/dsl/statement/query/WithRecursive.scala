package sqala.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.*
import sqala.ast.table.SqlTable
import sqala.printer.Dialect
import sqala.util.queryToString

import scala.collection.mutable.ListBuffer

class WithRecursive[T](val ast: SqlQuery.Cte):
    def sql(dialect: Dialect, prepare: Boolean = true): (String, Array[Any]) =
        queryToString(ast, dialect, prepare)

object WithRecursive:
    def apply[Q, V <: Tuple](query: Query[Q, ?])(f: Option[WithContext] ?=> Query[Q, ?] => Query[Q, ?])(using s: SelectItem[Q]): WithRecursive[Q] =
        val aliasName = "cte"
        given Option[WithContext] = Some(WithContext(aliasName))
        val selectItems = s.selectItems(query.queryItems, 0)
        val cteQuery = f(query)
        var tmpCursor = 0
        val tmpItems = ListBuffer[SqlSelectItem.Item]()
        for field <- selectItems.map(_.alias.get) do
            tmpItems.addOne(SqlSelectItem.Item(SqlExpr.Column(Some(aliasName), field), Some(s"c${tmpCursor}")))
            tmpCursor += 1
        val queryItems = tmpItems.toList
        val ast: SqlQuery.Cte = SqlQuery.Cte(
            SqlWithItem(aliasName, cteQuery.ast, selectItems.map(_.alias.get)) :: Nil, 
            true, 
            SqlQuery.Select(
                select = queryItems,
                from = SqlTable.IdentTable(aliasName, None) :: Nil
            )
        )
        new WithRecursive(ast)