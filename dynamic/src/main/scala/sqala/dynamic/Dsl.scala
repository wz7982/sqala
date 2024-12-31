package sqala.dynamic

import sqala.ast.expr.{SqlCase, SqlExpr, SqlSubLinkType}
import sqala.parser.SqlParser

def column(name: String): Expr = Expr(SqlParser().parseColumn(name))

def unsafeExpr(snippet: String): Expr = Expr(SqlParser().parseExpr(snippet))

extension [T](value: T)(using a: AsSqlExpr[T])
    def asExpr: Expr = Expr(a.asSqlExpr(value))

def table(name: String): Table = Table(name, None)

def lateral(query: Query): LateralQuery = LateralQuery(query)

def select(items: List[SelectItem]): Select = Select().select(items)

def `with`(items: List[SubQueryTable]): With = With(items)

def `case`(branches: List[(Expr, Expr)], default: Expr): Expr =
    Expr(SqlExpr.Case(branches.toList.map(b => SqlCase(b._1.sqlExpr, b._2.sqlExpr)), default.sqlExpr))

def count(): Expr = Expr(SqlExpr.Func("COUNT", Nil))

def count(expr: Expr): Expr = Expr(SqlExpr.Func("COUNT", expr.sqlExpr :: Nil))

def sum(expr: Expr): Expr = Expr(SqlExpr.Func("SUM", expr.sqlExpr :: Nil))

def avg(expr: Expr): Expr = Expr(SqlExpr.Func("AVG", expr.sqlExpr :: Nil))

def max(expr: Expr): Expr = Expr(SqlExpr.Func("MAX", expr.sqlExpr :: Nil))

def min(expr: Expr): Expr = Expr(SqlExpr.Func("MIN", expr.sqlExpr :: Nil))

def rank(): Expr = Expr(SqlExpr.Func("RANK", Nil))

def denseRank(): Expr = Expr(SqlExpr.Func("DENSE_RANK", Nil))

def rowNumber(): Expr = Expr(SqlExpr.Func("ROW_NUMBER", Nil))

def any(query: Query): Expr = Expr(SqlExpr.SubLink(query.ast, SqlSubLinkType.Any))

def all(query: Query): Expr = Expr(SqlExpr.SubLink(query.ast, SqlSubLinkType.All))

def some(query: Query): Expr = Expr(SqlExpr.SubLink(query.ast, SqlSubLinkType.Some))

def exists(query: Query): Expr = Expr(SqlExpr.SubLink(query.ast, SqlSubLinkType.Exists))

def notExists(query: Query): Expr = Expr(SqlExpr.SubLink(query.ast, SqlSubLinkType.NotExists))