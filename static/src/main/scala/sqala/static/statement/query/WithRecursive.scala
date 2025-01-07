package sqala.static.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.*
import sqala.ast.table.SqlTable
import sqala.printer.Dialect
import sqala.static.common.*
import sqala.util.queryToString

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValueTuple

class WithRecursive[T](val ast: SqlQuery.Cte):
    def sql(dialect: Dialect, prepare: Boolean = true, indent: Int = 4): (String, Array[Any]) =
        queryToString(ast, dialect, prepare, indent)

object WithRecursive:
    inline def apply[N <: Tuple, WN <: Tuple, V <: Tuple](
        query: Query[NamedTuple[N, V], ?]
    )(f: Query[NamedTuple[N, V], ?] => Query[NamedTuple[WN, V], ?])(using
        sq: SelectItem[SubQuery[N, V]],
        qc: QueryContext
    ): Query[NamedTuple[N, V], ManyRows] =
        val alias = tableCte
        val columns = constValueTuple[N].toList.map(_.asInstanceOf[String])
        val subQuery = SubQuery[N, V](columns)
        val selectItems = sq.selectItems(subQuery, alias :: Nil)
        val cteQuery = f(query)

        def transformTable(table: SqlTable): SqlTable = table match
            case SqlTable.SubQuery(query.ast, false, a) =>
                SqlTable.Range(alias, a)
            case SqlTable.Join(left, joinType, right, condition, _) =>
                SqlTable.Join(transformTable(left), joinType, transformTable(right), condition, None)
            case _ => table

        def transformAst(originalAst: SqlQuery): SqlQuery = originalAst match
            case s: SqlQuery.Select =>
                s.copy(from = s.from.map(transformTable))
            case u: SqlQuery.Union =>
                u.copy(left = transformAst(u.left), right = transformAst(u.right))
            case _ => originalAst

        val ast: SqlQuery.Cte = SqlQuery.Cte(
            SqlWithItem(alias, transformAst(cteQuery.ast), columns) :: Nil,
            true,
            SqlQuery.Select(
                select = selectItems,
                from = SqlTable.Range(alias, None) :: Nil
            )
        )
        Query(ast)