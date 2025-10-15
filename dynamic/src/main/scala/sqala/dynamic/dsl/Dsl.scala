package sqala.dynamic.dsl

import sqala.ast.expr.{SqlExpr, SqlSubLinkQuantifier, SqlWhen}
import sqala.dynamic.parser.SqlParser

def column(name: String): Expr = Expr(new SqlParser().parseColumn(name))

def unsafeExpr(snippet: String): Expr = Expr(new SqlParser().parseExpr(snippet))

def value[N: Numeric](n: N): Expr = Expr(SqlExpr.NumberLiteral(n))

def value(s: String): Expr = Expr(SqlExpr.StringLiteral(s))

def table(name: String): Table =
    new SqlParser().parseIdent(name)
    Table(name, None)

def lateral(query: DynamicQuery): LateralQuery = LateralQuery(query)

def select(items: List[SelectItem]): Select = Select().select(items)

def select(items: SelectItem*): Select = Select().select(items.toList)

def `with`(items: List[SubQueryTable]): With = With(items)

def `case`(branches: List[(Expr, Expr)], default: Expr): Expr =
    Expr(SqlExpr.Case(branches.toList.map(b => SqlWhen(b._1.sqlExpr, b._2.sqlExpr)), Some(default.sqlExpr)))

def count(): Expr = Expr(SqlExpr.CountAsteriskFunc(None, None))

def count(expr: Expr): Expr = 
    Expr(SqlExpr.GeneralFunc(None, "COUNT", expr.sqlExpr :: Nil, Nil, Nil, None))

def sum(expr: Expr): Expr = 
    Expr(SqlExpr.GeneralFunc(None, "SUM", expr.sqlExpr :: Nil, Nil, Nil, None))

def avg(expr: Expr): Expr = 
    Expr(SqlExpr.GeneralFunc(None, "AVG", expr.sqlExpr :: Nil, Nil, Nil, None))

def max(expr: Expr): Expr = 
    Expr(SqlExpr.GeneralFunc(None, "MAX", expr.sqlExpr :: Nil, Nil, Nil, None))

def min(expr: Expr): Expr = 
    Expr(SqlExpr.GeneralFunc(None, "MIN", expr.sqlExpr :: Nil, Nil, Nil, None))

def rank(): Expr = 
    Expr(SqlExpr.GeneralFunc(None, "RANK", Nil, Nil, Nil, None))

def denseRank(): Expr = 
    Expr(SqlExpr.GeneralFunc(None, "DENSE_RANK", Nil, Nil, Nil, None))

def rowNumber(): Expr = 
    Expr(SqlExpr.GeneralFunc(None, "ROW_NUMBER", Nil, Nil, Nil, None))

def any(query: DynamicQuery): Expr = Expr(SqlExpr.SubLink(SqlSubLinkQuantifier.Any, query.tree))

def all(query: DynamicQuery): Expr = Expr(SqlExpr.SubLink(SqlSubLinkQuantifier.All, query.tree))

def exists(query: DynamicQuery): Expr = Expr(SqlExpr.SubLink(SqlSubLinkQuantifier.Exists, query.tree))