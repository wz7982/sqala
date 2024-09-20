package sqala.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.*
import sqala.ast.table.SqlTable
import sqala.printer.Dialect
import sqala.util.queryToString

import scala.NamedTuple.NamedTuple

class WithRecursive[T](val ast: SqlQuery.Cte):
    def sql(dialect: Dialect, prepare: Boolean = true): (String, Array[Any]) =
        queryToString(ast, dialect, prepare)

object WithRecursive:
    def apply[N <: Tuple, WN <: Tuple, V <: Tuple](query: Query[NamedTuple[N, V], ?])(f: Query[NamedTuple[N, V], ?] => Query[NamedTuple[WN, V], ?])(using sq: SelectItem[SubQuery[N, V]], s: SelectItem[V], qc: QueryContext): WithRecursive[NamedTuple[N, V]] =
        val aliasName = "cte"
        val subQuery = SubQuery[N, V](aliasName, s.offset(query.queryItems.toTuple))
        val selectItems = sq.selectItems(subQuery, 0)
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