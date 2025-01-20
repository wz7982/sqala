package sqala.static.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.*
import sqala.ast.table.SqlTable
import sqala.static.dsl.tableCte

import scala.NamedTuple.NamedTuple

object WithRecursive:
    def apply[N <: Tuple, WN <: Tuple, V <: Tuple](
        query: Query[NamedTuple[N, V]]
    )(f: Query[NamedTuple[N, V]] => Query[NamedTuple[WN, V]])(using 
        s: AsSelect[SubQuery[N, V]],
        sq: AsSubQuery[V]
    ): Query[NamedTuple[N, V]] =
        given QueryContext = query.context
        val aliasName = tableCte
        val subQuery = SubQuery[N, V](query.queryParam, aliasName)
        val selectItems = s.selectItems(subQuery, 1)
        val cteQuery = f(query)

        def transformTable(table: SqlTable): SqlTable = table match
            case SqlTable.SubQuery(query.ast, false, alias) =>
                SqlTable.Range(aliasName, alias)
            case SqlTable.Join(left, joinType, right, condition, alias) =>
                SqlTable.Join(transformTable(left), joinType, transformTable(right), condition, alias)
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
                from = SqlTable.Range(aliasName, None) :: Nil
            )
        )

        Query(subQuery.__items__, ast)