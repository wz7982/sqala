package sqala.dynamic

import sqala.ast.expr.{SqlCase, SqlExpr, SqlSubQueryPredicate}
import sqala.parser.{ParseException, SqlParser}

import scala.language.experimental.saferExceptions

def column(name: String): Expr throws ParseException = Expr(SqlParser().parseColumn(name))

def unsafeExpr(snippet: String): Expr throws ParseException = Expr(SqlParser().parse(snippet))

extension [T](value: T)(using a: AsSqlExpr[T])
    def asExpr: Expr = Expr(a.asSqlExpr(value))

def table(name: String): Table = Table(name, None)

def lateral(query: Query): LateralQuery = LateralQuery(query)

def select(items: List[SelectItem]): Select = Select().select(items)

def `with`(items: List[SubQueryTable]): With = With(items)

def `case`(branches: List[(Expr, Expr)], default: Expr): Expr =
    Expr(SqlExpr.Case(branches.toList.map(b => SqlCase(b._1.sqlExpr, b._2.sqlExpr)), default.sqlExpr))

def count(): Expr = Expr(SqlExpr.Agg("COUNT", Nil, false, Map(), Nil))

def count(expr: Expr): Expr = Expr(SqlExpr.Agg("COUNT", expr.sqlExpr :: Nil, false, Map(), Nil))

def sum(expr: Expr): Expr = Expr(SqlExpr.Agg("SUM", expr.sqlExpr :: Nil, false, Map(), Nil))

def avg(expr: Expr): Expr = Expr(SqlExpr.Agg("AVG", expr.sqlExpr :: Nil, false, Map(), Nil))

def max(expr: Expr): Expr = Expr(SqlExpr.Agg("MAX", expr.sqlExpr :: Nil, false, Map(), Nil))

def min(expr: Expr): Expr = Expr(SqlExpr.Agg("MIN", expr.sqlExpr :: Nil, false, Map(), Nil))

def rank(): Expr = Expr(SqlExpr.Agg("RANK", Nil, false, Map(), Nil))

def denseRank(): Expr = Expr(SqlExpr.Agg("DENSE_RANK", Nil, false, Map(), Nil))

def rowNumber(): Expr = Expr(SqlExpr.Agg("ROW_NUMBER", Nil, false, Map(), Nil))

def any(query: Query): Expr = Expr(SqlExpr.SubQueryPredicate(query.ast, SqlSubQueryPredicate.Any))

def all(query: Query): Expr = Expr(SqlExpr.SubQueryPredicate(query.ast, SqlSubQueryPredicate.All))

def some(query: Query): Expr = Expr(SqlExpr.SubQueryPredicate(query.ast, SqlSubQueryPredicate.Some))

def exists(query: Query): Expr = Expr(SqlExpr.SubQueryPredicate(query.ast, SqlSubQueryPredicate.Exists))

def notExists(query: Query): Expr = Expr(SqlExpr.SubQueryPredicate(query.ast, SqlSubQueryPredicate.NotExists))