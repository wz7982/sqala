package sqala.static.statement.query

import sqala.ast.statement.SqlQuery
import sqala.ast.table.SqlTable
import sqala.printer.Dialect
import sqala.static.common.*
import sqala.static.macros.*
import sqala.util.queryToString

import scala.util.TupledFunction


sealed class Query[T](val ast: SqlQuery):
    def sql(dialect: Dialect, prepare: Boolean = true, indent: Int = 4): (String, Array[Any]) =
        queryToString(ast, dialect, prepare, indent)

class TableQuery[T](
    private val tableNames: List[String],
    override val ast: SqlQuery.Select
) extends Query[T](ast):
    private def replaceTableName(tableNames: List[String]): SqlQuery.Select =
        (tableNames, ast.from.head) match
            case (p :: Nil, SqlTable.IdentTable(t, a)) =>
                val newTable = SqlTable.IdentTable(
                    t, 
                    a.map(_.copy(tableAlias = p))
                )
                ast.copy(from = newTable :: Nil)
            case _ => ast

    inline def filter[F](using
        tt: ToTuple[T],
        t: TupledFunction[F, tt.R => Boolean]
    )(inline f: F): TableQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParams = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(newParams)
        val cond = ClauseMacro.fetchExpr(f, newParams)
        TableQuery(newParams, newAst.addWhere(cond))