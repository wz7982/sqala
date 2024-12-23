package sqala.static.statement.query

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.static.common.*
import sqala.static.macros.*

import scala.NamedTuple.NamedTuple
import scala.util.TupledFunction

class GroupByQuery[T](
    private[sqala] val groups: List[SqlExpr],
    private[sqala] val tableNames: List[String],
    private[sqala] val ast: SqlQuery.Select
)(using val queryContext: QueryContext):
    inline def having[F](using
        TupledFunction[F, T => Boolean]
    )(inline f: F): GroupByQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        queryContext.groups.prepend((args.head, groups))
        val having =
            ClauseMacro.fetchFilter(f, true, false, tableNames.prepended(args.head), queryContext)
        GroupByQuery(groups, tableNames, ast.addHaving(having))

    inline def sortBy[F, S: AsSort](using
        TupledFunction[F, T => S]
    )(inline f: F): GroupByQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        queryContext.groups.prepend((args.head, groups))
        val sortBy =
            ClauseMacro.fetchSortBy(f, false, tableNames.prepended(args.head), queryContext)
        GroupByQuery(groups, tableNames, ast.copy(orderBy = ast.orderBy ++ sortBy))

    inline def map[F, M: AsSelectItem](using
        TupledFunction[F, T => M]
    )(inline f: F): Query[M, ManyRows] =
        val args = ClauseMacro.fetchArgNames(f)
        queryContext.groups.prepend((args.head, groups))
        val selectItems =
            ClauseMacro.fetchGroupedMap(f, false, tableNames.prepended(args.head), queryContext)
        Query(ast.copy(select = selectItems))

object GroupByQuery:
    extension [GN <: Tuple, GV <: Tuple, T <: Tuple](query: GroupByQuery[Group[GN, GV] *: T])
        inline def pivot[F, N <: Tuple, V <: Tuple](using
            tf: TupledFunction[F, Group[GN, GV] *: T => NamedTuple[N, V]]
        )(inline f: F): GroupedPivotQuery[GN, GV, Group[GN, GV] *: T, N, V] =
            val args = ClauseMacro.fetchArgNames(f)
            query.queryContext.groups.prepend((args.head, query.groups))
            val functions = ClauseMacro.fetchPivot(f, query.tableNames.prepended(args.head), query.queryContext)
            GroupedPivotQuery(query.groups, query.tableNames, functions, query.ast)(using query.queryContext)