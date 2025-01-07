package sqala.static.statement.query

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.ast.table.SqlTable
import sqala.static.common.*
import sqala.static.macros.*

class ConnectByQuery[T](
    private[sqala] val table: Table[T],
    private[sqala] val tableName: String,
    private[sqala] val connectByAst: SqlQuery.Select,
    private[sqala] val startWithAst: SqlQuery.Select,
    private[sqala] val mapAst: SqlQuery.Select =
        SqlQuery.Select(select = Nil, from = SqlTable.Range(tableCte, None) :: Nil)
)(using val queryContext: QueryContext):
    inline def startWith(inline f: Table[T] => Boolean): ConnectByQuery[T] =
        val cond = ClauseMacro.fetchFilter(f, false, false, tableName :: Nil, queryContext)
        ConnectByQuery(table, tableName, connectByAst, startWithAst.addWhere(cond))

    inline def sortBy[S: AsSort](inline f: Table[T] => S): ConnectByQuery[T] =
        val sortBy = ClauseMacro.fetchSortBy(f, true, false, tableCte :: Nil, queryContext)
        ConnectByQuery(table, tableName, connectByAst, startWithAst, mapAst.copy(orderBy = mapAst.orderBy ++ sortBy))

    def maxDepth(n: Int): ConnectByQuery[T] =
        val cond =
            SqlExpr.Binary(SqlExpr.Column(Some(tableCte), columnPseudoLevel), SqlBinaryOperator.LessThan, SqlExpr.NumberLiteral(n))
        ConnectByQuery(table, tableName, connectByAst.addWhere(cond), startWithAst)

    inline def sortSiblingsBy[S: AsSort](inline f: Table[T] => S): ConnectByQuery[T] =
        val sortBy = ClauseMacro.fetchSortBy(f, true, false, tableName :: Nil, queryContext)
        ConnectByQuery(
            table,
            tableName,
            connectByAst.copy(orderBy = connectByAst.orderBy ++ sortBy),
            startWithAst
        )

    transparent inline def map[M: AsSelectItem](inline f: Table[T] => M): Query[M, ManyRows] =
        val mapQuery = ClauseMacro.fetchMap(f, true, tableCte :: Nil, mapAst, queryContext)
        val metaData = TableMacro.tableMetaData[T]
        val unionQuery = SqlQuery.Union(startWithAst, SqlUnionType.UnionAll, connectByAst)
        val withItem = SqlWithItem(tableCte, unionQuery, metaData.columnNames :+ columnPseudoLevel)
        val cteAst = SqlQuery.Cte(withItem :: Nil, true, mapQuery.ast)
        Query(cteAst)