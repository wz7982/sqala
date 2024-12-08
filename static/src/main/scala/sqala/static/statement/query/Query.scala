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
import scala.util.TupledFunction

sealed class Query[T](val ast: SqlQuery):
    def sql(dialect: Dialect, prepare: Boolean = true, indent: Int = 4): (String, Array[Any]) =
        queryToString(ast, dialect, prepare, indent)

// 加一个select query
// 加一个join query，join query调用filter 和 sortBy也变成select query
// join query 的join 每次只添加on里最后一个表的别名
// 只有table query和join query 有join方法
// 其他查询调用groupBy 变成group query
// 加一个group query 有having sort map
// map后变成Query

// Table query 和 join query 继承 Select query

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

class TableQuery[T](
    private[sqala] val tableName: Option[String],
    override val ast: SqlQuery.Select
)(using val queryContext: QueryContext) extends Query[Table[T]](ast):
    private inline def replaceTableName(tableName: String): SqlQuery.Select =
        ast.from.head match
            case SqlTable.IdentTable(t, a) =>
                val newTable = SqlTable.IdentTable(
                    t, 
                    a.map(_.copy(tableAlias = tableName))
                )
                val metaData = TableMacro.tableMetaData[T]
                val selectItems = metaData.fieldNames.map: n =>
                    SqlSelectItem.Item(SqlExpr.Column(Some(tableName), n), None)
                ast.copy(select = selectItems, from = newTable :: Nil)
            case _ => ast

    inline def filter(inline f: Table[T] => Boolean): TableQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = tableName.getOrElse(args.head)
        val newAst = replaceTableName(newParam)
        val cond = ClauseMacro.fetchExpr(f, newParam :: Nil, queryContext)
        TableQuery(Some(newParam), newAst.addWhere(cond))

    inline def withFilter(inline f: Table[T] => Boolean): TableQuery[T] =
        filter(f)

    inline def filterIf(test: => Boolean)(inline f: Table[T] => Boolean): TableQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = tableName.getOrElse(args.head)
        val newAst = replaceTableName(newParam)
        val cond = ClauseMacro.fetchExpr(f, newParam :: Nil, queryContext)
        TableQuery(Some(newParam), if test then newAst.addWhere(cond) else newAst)

    inline def sortBy[S: AsSort](inline f: Table[T] => S): TableQuery[T] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = tableName.getOrElse(args.head)
        val newAst = replaceTableName(newParam)
        val sortBy = ClauseMacro.fetchSortBy(f, newParam :: Nil, queryContext)
        TableQuery(Some(newParam), newAst.copy(orderBy = newAst.orderBy ++ sortBy))

    inline def groupBy[N <: Tuple, V <: Tuple : AsExpr](
        inline f: Table[T] => NamedTuple[N, V]
    ): GroupByQuery[(Group[N, V], Table[T])] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = tableName.getOrElse(args.head)
        val newAst = replaceTableName(newParam)
        val groupBy = ClauseMacro.fetchGroupBy(f, newParam :: Nil, queryContext)
        GroupByQuery(
            groupBy,
            newAst.copy(groupBy = groupBy.map(g => SqlGroupItem.Singleton(g)))
        )

    inline def map[M: AsSelectItem](inline f: Table[T] => M): Query[M] =
        val args = ClauseMacro.fetchArgNames(f)
        val newParam = tableName.getOrElse(args.head)
        val newAst = replaceTableName(newParam)
        val selectItems = ClauseMacro.fetchMap(f, newParam :: Nil, queryContext)
        Query(newAst.copy(select = selectItems))