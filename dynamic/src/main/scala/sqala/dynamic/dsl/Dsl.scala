package sqala.dynamic.dsl

import sqala.ast.expr.*
import sqala.ast.statement.SqlQuery
import sqala.metadata.{AsSqlExpr, Interval}

def column(tableName: String, columnName: String): Expr =
    Expr(SqlExpr.Column(Some(tableName), columnName))

def column(columnName: String): Expr =
    Expr(SqlExpr.Column(None, columnName))

def value[V: AsSqlExpr as a](v: V): Expr =
    Expr(a.asSqlExpr(v))

def subquery(query: Query): Expr =
    Expr(SqlExpr.Subquery(None, query.tree))

def any(query: Query): Expr =
    Expr(SqlExpr.Subquery(Some(SqlSubqueryQuantifier.Any), query.tree))

def all(query: Query): Expr =
    Expr(SqlExpr.Subquery(Some(SqlSubqueryQuantifier.All), query.tree))

def exists(query: Query): Expr =
    Expr(SqlExpr.Subquery(Some(SqlSubqueryQuantifier.Exists), query.tree))

def table(name: String): Table =
    Table(name, None)

def subqueryTable(query: Query): SubqueryTable =
    SubqueryTable(query.tree, None, false)

def from(tables: AnyTable*): SelectQuery =
    SelectQuery(SqlQuery.Select(None, Nil, tables.toList.map(_.asSqlTable), None, None, None, Nil, None, None))

def from(tables: List[AnyTable]): SelectQuery =
    from(tables*)

def caseWhen(exprs: Expr*): Expr =
    val caseBranches =
        exprs.grouped(2).toList.map(i => (i(0), i(1)))
    if caseBranches.length * 2 == exprs.length then
        Expr(SqlExpr.Case(caseBranches.map((w, t) => SqlCaseBranch(w.asSqlExpr, t.asSqlExpr)), None))
    else
        Expr(SqlExpr.Case(caseBranches.map((w, t) => SqlCaseBranch(w.asSqlExpr, t.asSqlExpr)), Some(exprs.last.asSqlExpr)))

def caseWhen(exprs: List[Expr]): Expr =
    caseWhen(exprs*)

extension (n: Int)
    def year: Interval =
        Interval(n.toString, SqlTimeUnit.Year)

    def month: Interval =
        Interval(n.toString, SqlTimeUnit.Month)

    def day: Interval =
        Interval(n.toString, SqlTimeUnit.Day)

    def hour: Interval =
        Interval(n.toString, SqlTimeUnit.Hour)

    def minute: Interval =
        Interval(n.toString, SqlTimeUnit.Minute)

    def second: Interval =
        Interval(n.toString, SqlTimeUnit.Second)

def coalesce(x: Expr, y: Expr): Expr =
    Expr(SqlExpr.Coalesce(x.asSqlExpr :: y.asSqlExpr :: Nil))

def ifNull(x: Expr, y: Expr): Expr =
    coalesce(x, y)

def nullIf(x: Expr, y: Expr): Expr =
    Expr(SqlExpr.NullIf(x.asSqlExpr, y.asSqlExpr))

def grouping(x: Expr): Expr =
    Expr(SqlExpr.Grouping(x.asSqlExpr :: Nil))

def currentRow: FrameBound =
    FrameBound(SqlWindowFrameBound.CurrentRow)

def unboundedPreceding: FrameBound =
    FrameBound(SqlWindowFrameBound.UnboundedPreceding)

def unboundedFollowing: FrameBound =
    FrameBound(SqlWindowFrameBound.UnboundedFollowing)

def partitionBy(exprs: Expr*): Over =
    Over(exprs.toList.map(_.asSqlExpr), Nil, None)

def partitionBy(exprs: List[Expr]): Over =
    partitionBy(exprs*)

def orderBy(orders: Order*): Over =
    Over(Nil, orders.toList.map(_.order), None)

def orderBy(orders: List[Order]): Over =
    orderBy(orders*)