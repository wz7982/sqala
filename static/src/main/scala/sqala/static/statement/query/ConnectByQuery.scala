package sqala.static.statement.query

import sqala.ast.statement.*
import sqala.ast.table.SqlTable
import sqala.static.common.*
import sqala.static.macros.*

class ConnectByQuery[T](
    private[sqala] val table: Table[T],
    private[sqala] val tableName: String,
    private[sqala] val baseAst: SqlQuery.Select,
    private[sqala] val connectByAst: SqlQuery.Select,
    private[sqala] val startWithAst: SqlQuery.Select,
    private[sqala] val mapAst: SqlQuery.Select =
        SqlQuery.Select(select = Nil, from = SqlTable.IdentTable("__cte__", None) :: Nil)
)(using val queryContext: QueryContext):
    inline def startWith(inline f: Table[T] => Boolean): ConnectByQuery[T] =
        val cond = ClauseMacro.fetchFilter(f, false, tableName :: Nil, queryContext)
        ConnectByQuery(table, tableName, baseAst, connectByAst, baseAst.addWhere(cond))

    inline def sortBy[S: AsSort](inline f: Table[T] => S): ConnectByQuery[T] =
        val sortBy = ClauseMacro.fetchSortBy(f, "__cte__" :: Nil, queryContext)
        ConnectByQuery(table, tableName, baseAst, connectByAst, startWithAst, mapAst.copy(orderBy = mapAst.orderBy ++ sortBy))

    inline def sortSiblingsBy[S: AsSort](inline f: Table[T] => S): ConnectByQuery[T] =
        val sortBy = ClauseMacro.fetchSortBy(f, tableName :: Nil, queryContext)
        ConnectByQuery(
            table,
            tableName,
            baseAst,
            connectByAst.copy(orderBy = connectByAst.orderBy ++ sortBy),
            startWithAst
        )

    inline def map[M: AsSelectItem](inline f: Table[T] => M): Query[M, ManyRows] =
        val mapQuery = ClauseMacro.fetchMap(f, "__cte__" :: Nil, mapAst, queryContext)
        val metaData = TableMacro.tableMetaData[T]
        val unionQuery = SqlQuery.Union(startWithAst, SqlUnionType.UnionAll, connectByAst)
        val withItem = SqlWithItem("__cte__", unionQuery, metaData.columnNames)
        val cteAst = SqlQuery.Cte(withItem :: Nil, true, mapQuery.ast)
        Query(cteAst)