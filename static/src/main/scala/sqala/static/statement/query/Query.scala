package sqala.static.statement.query

import sqala.ast.expr.*
import sqala.ast.group.SqlGroupItem
import sqala.ast.statement.*
import sqala.ast.table.SqlTable
import sqala.printer.Dialect
import sqala.static.common.*
import sqala.static.macros.*
import sqala.util.queryToString

import scala.NamedTuple.NamedTuple
import scala.compiletime.summonInline
import scala.util.TupledFunction

sealed class Query[T](val ast: SqlQuery):
    def sql(dialect: Dialect, prepare: Boolean = true, indent: Int = 4): (String, Array[Any]) =
        queryToString(ast, dialect, prepare, indent)

// TODO union

class GroupByQuery[T](
    private[sqala] val groups: List[SqlExpr],
    val ast: SqlQuery.Select,
)(using val queryContext: QueryContext):
    inline def having[F](using
        TupledFunction[F, T => Boolean]
    )(inline f: F): GroupByQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        queryContext.groups.prepend((args.head, groups))
        val having = ClauseMacro.fetchExpr(f, args, queryContext)
        GroupByQuery(groups, ast.addHaving(having))

    inline def sortBy[F, S: AsSort](using
        TupledFunction[F, T => S]
    )(inline f: F): GroupByQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        queryContext.groups.prepend((args.head, groups))
        val sortBy = ClauseMacro.fetchSortBy(f, args, queryContext)
        GroupByQuery(groups, ast.copy(orderBy = ast.orderBy ++ sortBy))

    inline def map[F, M: AsSelectItem](using
        TupledFunction[F, T => M]
    )(inline f: F): Query[M] =
        val args = ClauseMacro.fetchArgNames(f)
        queryContext.groups.prepend((args.head, groups))
        val selectItems = ClauseMacro.fetchMap(f, args, queryContext)
        Query(ast.copy(select = selectItems))

class SelectQuery[T <: Tuple : SelectItem](
    private[sqala] val tables: T,
    private[sqala] val tableNames: List[String],
    override val ast: SqlQuery.Select
)(using val queryContext: QueryContext) extends Query[Table[T]](ast):
    private inline def replaceTableName(tableNames: List[String]): SqlQuery.Select =
        // TODO 测试values查询
        def from(tableNames: List[String], table: SqlTable): SqlTable = (tableNames, table) match
            case (x :: _, SqlTable.IdentTable(t, a)) =>
                SqlTable.IdentTable(
                    t,
                    a.map(_.copy(tableAlias = x))
                )
            case (x :: _, SqlTable.SubQueryTable(q, l, a)) =>
                SqlTable.SubQueryTable(q, l, a.copy(tableAlias = x))
            case (x :: xs, SqlTable.JoinTable(l, j, r, c)) =>
                ???
                // SqlTable.JoinTable(from(x :: Nil, l), j, )
                // TODO
            case _ => table
            
        val select = summon[SelectItem[T]].selectItems(tables, tableNames)

        ast.copy(select = select, from = from(tableNames, ast.from.head) :: Nil)

    inline def filter[F](using
        TupledFunction[F, T => Boolean]
    )(inline f: F): SelectQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(newParam)
        val cond = ClauseMacro.fetchExpr(f, newParam, queryContext)
        SelectQuery(tables, newParam, newAst.addWhere(cond))

    inline def withFilter[F](using
        TupledFunction[F, T => Boolean]
    )(inline f: F): SelectQuery[T] =
        filter(f)

    inline def filterIf[F](test: => Boolean)(using
        TupledFunction[F, T => Boolean]
    )(inline f: F): SelectQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(newParam)
        val cond = ClauseMacro.fetchExpr(f, newParam, queryContext)
        SelectQuery(tables, newParam, if test then newAst.addWhere(cond) else newAst)

    inline def sortBy[F, S: AsSort](using 
        TupledFunction[F, T => S]
    )(inline f: F): SelectQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(newParam)
        val sortBy = ClauseMacro.fetchSortBy(f, newParam, queryContext)
        SelectQuery(tables, newParam, newAst.copy(orderBy = newAst.orderBy ++ sortBy))

    inline def groupBy[F, N <: Tuple, V <: Tuple : AsExpr](using
        TupledFunction[F, T => NamedTuple[N, V]]
    )(inline f: F): GroupByQuery[Group[N, V] *: T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(newParam)
        val groupBy = ClauseMacro.fetchGroupBy(f, newParam, queryContext)
        GroupByQuery(
            groupBy,
            newAst.copy(groupBy = groupBy.map(g => SqlGroupItem.Singleton(g)))
        )

    inline def map[F, M: AsSelectItem](using
        TupledFunction[F, T => M]
    )(inline f: F): Query[M] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = if tableNames.isEmpty then args else tableNames
        val newAst = replaceTableName(newParam)
        val selectItems = ClauseMacro.fetchMap(f, newParam, queryContext)
        Query(newAst.copy(select = selectItems))

class TableQuery[T](
    private[sqala] val table: Table[T],
    override val ast: SqlQuery.Select
)(using 
    override val queryContext: QueryContext
) extends SelectQuery[Tuple1[Table[T]]](Tuple1(table), Nil, ast)
// TODO join query 的join 除了第一次两表join，每次只添加on里最后一个表的别名