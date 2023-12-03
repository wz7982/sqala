package sqala.runtime

import sqala.ast.expr.*
import sqala.runtime.parser.SqlParser
import sqala.runtime.statement.query.*

import java.util.Date

def col(snippet: String): Expr = Expr(SqlParser().parse(snippet))

def value(v: Any): Expr = v match
    case null => Expr(SqlExpr.Null)
    case s: String => Expr(SqlExpr.StringLiteral(s))
    case d: Date => Expr(SqlExpr.DateLiteral(d))
    case Some(x) => value(x)
    case None => Expr(SqlExpr.Null)
    case Expr => v
    case _ => Expr(SqlParser().parse(v.toString))

def table(name: String): Table = Table(name, None)

def lateral(query: Query): LateralQuery = LateralQuery(query)

def select(items: (Expr | SelectItem)*): Select = Select().select(items)

def `with`(items: SubQueryTable*): With = With(items*)

def `case`(branches: CaseBranch*): Expr = 
    Expr(SqlExpr.Case(branches.toList.map(b => SqlCase(b.expr.sqlExpr, b.thenExpr.sqlExpr)), SqlExpr.Null))

def count: Expr = Expr(SqlExpr.Agg("COUNT", Nil, false, Map(), Nil))

def count(expr: Expr): Expr = Expr(SqlExpr.Agg("COUNT", expr.sqlExpr :: Nil, false, Map(), Nil))

def sum(expr: Expr): Expr = Expr(SqlExpr.Agg("SUM", expr.sqlExpr :: Nil, false, Map(), Nil))

def avg(expr: Expr): Expr = Expr(SqlExpr.Agg("AVG", expr.sqlExpr :: Nil, false, Map(), Nil))

def max(expr: Expr): Expr = Expr(SqlExpr.Agg("MAX", expr.sqlExpr :: Nil, false, Map(), Nil))

def min(expr: Expr): Expr = Expr(SqlExpr.Agg("MIN", expr.sqlExpr :: Nil, false, Map(), Nil))

def rank: Expr = Expr(SqlExpr.Agg("RANK", Nil, false, Map(), Nil))

def denseRank: Expr = Expr(SqlExpr.Agg("DENSE_RANK", Nil, false, Map(), Nil))

def rowNumber: Expr = Expr(SqlExpr.Agg("ROW_NUMBER", Nil, false, Map(), Nil))

def any(query: Query): Expr = Expr(SqlExpr.SubQueryPredicate(query.ast, SqlSubQueryPredicate.Any))

def all(query: Query): Expr = Expr(SqlExpr.SubQueryPredicate(query.ast, SqlSubQueryPredicate.All))

def some(query: Query): Expr = Expr(SqlExpr.SubQueryPredicate(query.ast, SqlSubQueryPredicate.Some))

def exists(query: Query): Expr = Expr(SqlExpr.SubQueryPredicate(query.ast, SqlSubQueryPredicate.Exists))

def notExists(query: Query): Expr = Expr(SqlExpr.SubQueryPredicate(query.ast, SqlSubQueryPredicate.NotExists))