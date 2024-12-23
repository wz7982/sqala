package sqala.static.statement.query

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.static.common.*
import sqala.static.macros.*

import scala.util.TupledFunction

class DistinctOnQuery[T](
    private[sqala] val groups: List[SqlExpr],
    private[sqala] val tableNames: List[String],
    private[sqala] val ast: SqlQuery.Select,
)(using val queryContext: QueryContext):
    inline def sortBy[F, S: AsSort](using
        TupledFunction[F, T => S]
    )(inline f: F): DistinctOnQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        queryContext.groups.prepend((args.head, groups))
        val sortBy =
            ClauseMacro.fetchSortBy(f, true, tableNames.prepended(args.head), queryContext)
        DistinctOnQuery(groups, tableNames, ast.copy(orderBy = ast.orderBy ++ sortBy))

    inline def map[F, M: AsSelectItem](using
        TupledFunction[F, T => M]
    )(inline f: F): Query[M, ManyRows] =
        val args = ClauseMacro.fetchArgNames(f)
        queryContext.groups.prepend((args.head, groups))
        val selectItems =
            ClauseMacro.fetchGroupedMap(f, true, tableNames.prepended(args.head), queryContext)
        Query(ast.copy(select = selectItems))