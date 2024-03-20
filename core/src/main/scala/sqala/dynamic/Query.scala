package sqala.dynamic

import sqala.ast.expr.SqlExpr
import sqala.ast.limit.SqlLimit
import sqala.ast.order.SqlOrderBy
import sqala.ast.statement.*
import sqala.jdbc.Dialect

sealed trait Query:
    def ast: SqlQuery

    infix def as(name: String): SubQueryTable = SubQueryTable(this, name, false)

    def asExpr: Expr = Expr(SqlExpr.SubQuery(ast))

    def sql(dialect: Dialect): (String, Array[Any]) =
        val printer = dialect.printer(true)
        printer.printQuery(ast)
        printer.sql -> printer.args.toArray

    infix def union(query: Query): Union = Union(this, SqlUnionType.Union, query)

    infix def unionAll(query: Query): Union = Union(this, SqlUnionType.UnionAll, query)

    infix def except(query: Query): Union = Union(this, SqlUnionType.Except, query)

    infix def exceptAll(query: Query): Union = Union(this, SqlUnionType.ExceptAll, query)

    infix def intersect(query: Query): Union = Union(this, SqlUnionType.Intersect, query)

    infix def intersectAll(query: Query): Union = Union(this, SqlUnionType.IntersectAll, query)

case class LateralQuery(query: Query):
    infix def as(name: String): SubQueryTable = SubQueryTable(query, name, true)

class Select(val ast: SqlQuery.Select) extends Query:
    infix def from(table: AnyTable): Select = new Select(ast.copy(from = table.toSqlTable :: Nil))

    infix def select(items: List[SelectItem]): Select =
        val selectItems = items.map: s =>
            SqlSelectItem(s.expr.sqlExpr, s.alias)
        new Select(ast.copy(select = ast.select ++ selectItems))

    infix def where(expr: Expr): Select = new Select(ast.addWhere(expr.sqlExpr))

    infix def orderBy(items: List[OrderBy]): Select =
        val orderByItems = items.map: o =>
            SqlOrderBy(o.expr.sqlExpr, Some(o.order))
        new Select(ast.copy(orderBy = ast.orderBy ++ orderByItems))

    infix def groupBy(items: List[Expr]): Select =
        val groupByItems = items.map(_.sqlExpr)
        new Select(ast.copy(groupBy = ast.groupBy ++ groupByItems))

    infix def having(expr: Expr): Select = new Select(ast.addHaving(expr.sqlExpr))

    infix def limit(n: Int): Select =
        val sqlLimit = ast.limit.map(l => SqlLimit(SqlExpr.NumberLiteral(n), l.offset)).orElse(Some(SqlLimit(SqlExpr.NumberLiteral(n), SqlExpr.NumberLiteral(0))))
        new Select(ast.copy(limit = sqlLimit))

    infix def offset(n: Int): Select =
        val sqlLimit = ast.limit.map(l => SqlLimit(l.limit, SqlExpr.NumberLiteral(n))).orElse(Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(n))))
        new Select(ast.copy(limit = sqlLimit))

    def distinct: Select = new Select(ast.copy(param = Some(SqlSelectParam.Distinct)))

object Select:
    def apply(): Select = new Select(SqlQuery.Select(select = Nil, from = Nil))

class Union(
    private[sqala] val left: Query,
    private[sqala] val unionType: SqlUnionType,
    private[sqala] val right: Query
) extends Query:
    override def ast: SqlQuery = SqlQuery.Union(left.ast, unionType, right.ast)

class With(val ast: SqlQuery.Cte) extends Query:
    def recursive: With = new With(ast.copy(recursive = true))

    infix def select(query: Query): With = new With(ast.copy(query = query.ast))

object With:
    def apply(items: List[SubQueryTable]): With =
        val queries = items.map: i =>
            SqlWithItem(i.alias, i.query.ast, Nil)
        new With(ast = SqlQuery.Cte(queries, false, queries.head.query))