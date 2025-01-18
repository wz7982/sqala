package sqala.static.statement.query

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.ast.table.SqlTable
import sqala.static.dsl.*
import sqala.static.macros.TableMacro

case class ConnectBy[T](
    private[sqala] val table: Table[T],
    private[sqala] val connectByAst: SqlQuery.Select,
    private[sqala] val startWithAst: SqlQuery.Select,
    private[sqala] val mapAst: SqlQuery.Select =
        SqlQuery.Select(select = Nil, from = SqlTable.Range(tableCte, None) :: Nil)
)(using private[sqala] val context: QueryContext):
    def startWith(f: QueryContext ?=> Table[T] => Expr[Boolean]): ConnectBy[T] =
        val cond = f(table)
        copy(startWithAst = startWithAst.addWhere(cond.asSqlExpr))

    def sortBy[S](f: QueryContext ?=> Table[T] => S)(using s: AsSort[S]): ConnectBy[T] =
        val sort = f(table)
        val sqlOrderBy = s.asSort(sort)
        copy(mapAst = mapAst.copy(orderBy = mapAst.orderBy ++ sqlOrderBy))

    def orderBy[S](f: QueryContext ?=> Table[T] => S)(using s: AsSort[S]): ConnectBy[T] =
        sortBy(f)

    def maxDepth(n: Int): ConnectBy[T] =
        val cond = SqlExpr.Binary(
            SqlExpr.Column(Some(tableCte), columnPseudoLevel), 
            SqlBinaryOperator.LessThan, 
            SqlExpr.NumberLiteral(n)
        )
        copy(connectByAst = connectByAst.addWhere(cond))

    def sortSiblingsBy[S](f: QueryContext ?=> Table[T] => S)(using s: AsSort[S]): ConnectBy[T] =
        val sort = f(table)
        val sqlOrderBy = s.asSort(sort)
        copy(connectByAst = connectByAst.copy(orderBy = connectByAst.orderBy ++ sqlOrderBy))

    def orderSiblingsBy[S](f: QueryContext ?=> Table[T] => S)(using s: AsSort[S]): ConnectBy[T] =
        sortSiblingsBy(f)

    def map[M](f: QueryContext ?=> Table[T] => M)(using s: AsSelect[M]): Query[s.R] =
        val mapped = f(table.copy(__aliasName__ = tableCte))
        val sqlSelect = s.selectItems(mapped, 1)
        val metaData = TableMacro.tableMetaData[T]
        val unionQuery = SqlQuery.Union(startWithAst, SqlUnionType.UnionAll, connectByAst)
        val withItem = SqlWithItem(tableCte, unionQuery, metaData.columnNames :+ columnPseudoLevel)
        val cteAst = SqlQuery.Cte(withItem :: Nil, true, mapAst.copy(select = sqlSelect))
        Query(s.transform(mapped), cteAst)

    def select[M](f: QueryContext ?=> Table[T] => M)(using s: AsSelect[M]): Query[s.R] =
        map(f)