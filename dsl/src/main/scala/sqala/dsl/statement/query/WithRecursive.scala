package sqala.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.*
import sqala.ast.table.SqlTable
import sqala.printer.Dialect
import sqala.util.queryToString

class WithRecursive[T](val ast: SqlQuery.Cte):
    def sql(dialect: Dialect, prepare: Boolean = true): (String, Array[Any]) =
        queryToString(ast, dialect, prepare)

object WithRecursive:
    def apply[Q](query: Query[Q, ?])(f: Query[Q, ?] => Query[Q, ?])(using s: SelectItem[Q]): WithRecursive[Q] =
        val aliasName = "cte"
        val items = s.subQueryItems(query.queryItems, 0, aliasName)
        val selectItems = s.subQuerySelectItems(items, 0)
        val cteQuery = f(query)

        def transformTable(table: SqlTable): SqlTable = table match
            case SqlTable.SubQueryTable(query.ast, false, alias) =>
                SqlTable.IdentTable(aliasName, Some(alias))
            case SqlTable.JoinTable(left, joinType, right, condition) =>
                SqlTable.JoinTable(transformTable(left), joinType, transformTable(right), condition)
            case _ => table

        def transformAst(originalAst: SqlQuery): SqlQuery = originalAst match
            case s: SqlQuery.Select =>
                s.copy(from = s.from.map(transformTable))
            case u: SqlQuery.Union =>
                u.copy(left = transformAst(u.left), right = transformAst(u.right))
            case _ => originalAst

        val ast: SqlQuery.Cte = SqlQuery.Cte(
            SqlWithItem(aliasName, transformAst(cteQuery.ast), selectItems.map(_.alias.get)) :: Nil,
            true,
            SqlQuery.Select(
                select = selectItems,
                from = SqlTable.IdentTable(aliasName, None) :: Nil
            )
        )
        new WithRecursive(ast)