package sqala.static.statement.query

import sqala.ast.statement.SqlQuery
import sqala.ast.table.{SqlJoinCondition, SqlTable}
import sqala.static.common.QueryContext
import sqala.static.macros.ClauseMacro

import scala.util.TupledFunction

class JoinPart[T](
    private[sqala] val tables: T,
    private[sqala] val tableNames: List[String],
    private[sqala] val ast: SqlQuery.Select
)(using
    val queryContext: QueryContext
):
    inline def on[F](using
        TupledFunction[F, T => Boolean],
        SelectItem[T]
    )(inline f: F): JoinQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames.appended(args.last)
        val newAst = replaceTableName(tables, newParam, ast)
        val cond = ClauseMacro.fetchFilter(f, false, false, newParam, queryContext)
        val newTable = newAst.from.head match
            case t: SqlTable.Join =>
                t.copy(condition = Some(SqlJoinCondition.On(cond)))
            case t => t
        JoinQuery(tables, newParam, newAst.copy(from = newTable :: Nil))

    inline def apply[F](using
        TupledFunction[F, T => Boolean],
        SelectItem[T]
    )(inline f: F): JoinQuery[T] =
        on(f)