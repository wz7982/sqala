package sqala.dynamic.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.group.SqlGroupingItem
import sqala.ast.limit.SqlLimit
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.{SqlQuery, SqlSelectItem, SqlSetOperator, SqlWithItem}
import sqala.dynamic.parser.SqlParser
import sqala.printer.Dialect
import sqala.util.queryToString

sealed trait DynamicQuery:
    def tree: SqlQuery

    infix def as(name: String): SubQueryTable =
        SqlParser.parseIdent(name)
        SubQueryTable(this, name, false)

    def asExpr: Expr = Expr(SqlExpr.SubQuery(tree))

    def sql(dialect: Dialect, enableJdbcPrepare: Boolean): (String, Array[Any]) =
        queryToString(tree, dialect, enableJdbcPrepare)

    infix def union(query: DynamicQuery): Union = Union(this, SqlSetOperator.Union(None), query)

    infix def unionAll(query: DynamicQuery): Union = Union(this, SqlSetOperator.Union(Some(SqlQuantifier.All)), query)

    infix def except(query: DynamicQuery): Union = Union(this, SqlSetOperator.Except(None), query)

    infix def exceptAll(query: DynamicQuery): Union = Union(this, SqlSetOperator.Except(Some(SqlQuantifier.All)), query)

    infix def intersect(query: DynamicQuery): Union = Union(this, SqlSetOperator.Intersect(None), query)

    infix def intersectAll(query: DynamicQuery): Union = Union(this, SqlSetOperator.Intersect(Some(SqlQuantifier.All)), query)

case class LateralQuery(query: DynamicQuery):
    infix def as(name: String): SubQueryTable =
        SqlParser.parseIdent(name)
        SubQueryTable(query, name, true)

class Select(val tree: SqlQuery.Select) extends DynamicQuery:
    infix def from(table: AnyTable): Select = new Select(tree.copy(from = table.toSqlTable :: Nil))

    infix def select(items: List[SelectItem]): Select =
        val selectItems = items.map: s =>
            SqlSelectItem.Expr(s.expr.sqlExpr, s.alias)
        new Select(tree.copy(select = tree.select ++ selectItems))

    infix def select(items: SelectItem*): Select =
        select(items.toList)

    infix def where(expr: Expr): Select = new Select(tree.addWhere(expr.sqlExpr))

    infix def orderBy(items: List[OrderBy]): Select =
        val orderByItems = items.map: o =>
            o.asSqlOrderBy
        new Select(tree.copy(orderBy = tree.orderBy ++ orderByItems))

    infix def groupBy(items: List[Expr]): Select =
        val groupByItems = items.map(i => SqlGroupingItem.Expr(i.sqlExpr))
        new Select(tree.copy(groupBy = tree.groupBy.map(g => g.copy(items = g.items ++ groupByItems))))

    infix def having(expr: Expr): Select = new Select(tree.addHaving(expr.sqlExpr))

    infix def limit(n: Int): Select =
        val sqlLimit = tree.limit.map(l => SqlLimit(SqlExpr.NumberLiteral(n), l.offset)).orElse(Some(SqlLimit(SqlExpr.NumberLiteral(n), SqlExpr.NumberLiteral(0))))
        new Select(tree.copy(limit = sqlLimit))

    infix def offset(n: Int): Select =
        val sqlLimit = tree.limit.map(l => SqlLimit(l.limit, SqlExpr.NumberLiteral(n))).orElse(Some(SqlLimit(SqlExpr.NumberLiteral(Long.MaxValue), SqlExpr.NumberLiteral(n))))
        new Select(tree.copy(limit = sqlLimit))

    def distinct: Select = new Select(tree.copy(quantifier = Some(SqlQuantifier.Distinct)))

object Select:
    def apply(): Select = new Select(SqlQuery.Select(select = Nil, from = Nil))

class Union(
    private[sqala] val left: DynamicQuery,
    private[sqala] val operator: SqlSetOperator,
    private[sqala] val right: DynamicQuery
) extends DynamicQuery:
    override def tree: SqlQuery = SqlQuery.Set(left.tree, operator, right.tree)

class With(val tree: SqlQuery.Cte) extends DynamicQuery:
    def recursive: With = new With(tree.copy(recursive = true))

    infix def select(query: DynamicQuery): With = new With(tree.copy(query = query.tree))

object With:
    def apply(items: List[SubQueryTable]): With =
        val queries = items.map: i =>
            SqlWithItem(i.__alias__, i.__query__.tree, Nil)
        new With(tree = SqlQuery.Cte(queries, false, queries.head.query))