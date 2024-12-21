package sqala.static.statement.query

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.static.common.*
import sqala.static.macros.*

import scala.util.TupledFunction

class GroupByQuery[T](
    private[sqala] val groups: List[SqlExpr],
    private[sqala] val tableNames: List[String],
    private[sqala] val ast: SqlQuery.Select,
)(using val queryContext: QueryContext):
    inline def having[F](using
        TupledFunction[F, T => Boolean]
    )(inline f: F): GroupByQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        queryContext.groups.prepend((args.head, groups))
        val having = 
            ClauseMacro.fetchFilter(f, true, tableNames.prepended(args.head), queryContext)
        GroupByQuery(groups, tableNames, ast.addHaving(having))

    inline def sortBy[F, S: AsSort](using
        TupledFunction[F, T => S]
    )(inline f: F): GroupByQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        queryContext.groups.prepend((args.head, groups))
        val sortBy = 
            ClauseMacro.fetchSortBy(f, tableNames.prepended(args.head), queryContext)
        GroupByQuery(groups, tableNames, ast.copy(orderBy = ast.orderBy ++ sortBy))

    inline def map[F, M: AsSelectItem](using
        TupledFunction[F, T => M]
    )(inline f: F): Query[M, ManyRows] =
        val args = ClauseMacro.fetchArgNames(f)
        queryContext.groups.prepend((args.head, groups))
        val selectItems = 
            ClauseMacro.fetchGroupedMap(f, tableNames.prepended(args.head), queryContext)
        Query(ast.copy(select = selectItems))