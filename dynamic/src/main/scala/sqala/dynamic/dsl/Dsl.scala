package sqala.dynamic.dsl

import sqala.ast.expr.{SqlExpr, SqlSubLinkQuantifier, SqlWhen}
import sqala.dynamic.parser.SqlParser
import sqala.metadata.{AsSqlExpr, TableMacro}

def dynamicColumn(name: String): Expr = Expr(new SqlParser().parseColumn(name))

def unsafeExpr(snippet: String): Expr = Expr(new SqlParser().parseExpr(snippet))

extension [T](value: T)(using a: AsSqlExpr[T])
    def asExpr: Expr = Expr(a.asSqlExpr(value))

def dynamicTable(name: String): Table =
    new SqlParser().parseIdent(name)
    Table(name, None)

inline def asTable[T]: EntityTable =
    val tableName = TableMacro.tableName[T]
    val metaData = TableMacro.tableMetaData[T]
    EntityTable(tableName, tableName, metaData)

def lateral(query: DynamicQuery): LateralQuery = LateralQuery(query)

def select(items: List[SelectItem]): Select = Select().select(items)

def select(items: SelectItem*): Select = Select().select(items.toList)

def `with`(items: List[SubQueryTable]): With = With(items)

def `case`(branches: List[(Expr, Expr)], default: Expr): Expr =
    Expr(SqlExpr.Case(branches.toList.map(b => SqlWhen(b._1.sqlExpr, b._2.sqlExpr)), Some(default.sqlExpr)))

def count(): Expr = Expr(SqlExpr.Func("COUNT", Nil))

def count(expr: Expr): Expr = Expr(SqlExpr.Func("COUNT", expr.sqlExpr :: Nil))

def sum(expr: Expr): Expr = Expr(SqlExpr.Func("SUM", expr.sqlExpr :: Nil))

def avg(expr: Expr): Expr = Expr(SqlExpr.Func("AVG", expr.sqlExpr :: Nil))

def max(expr: Expr): Expr = Expr(SqlExpr.Func("MAX", expr.sqlExpr :: Nil))

def min(expr: Expr): Expr = Expr(SqlExpr.Func("MIN", expr.sqlExpr :: Nil))

def rank(): Expr = Expr(SqlExpr.Func("RANK", Nil))

def denseRank(): Expr = Expr(SqlExpr.Func("DENSE_RANK", Nil))

def rowNumber(): Expr = Expr(SqlExpr.Func("ROW_NUMBER", Nil))

def any(query: DynamicQuery): Expr = Expr(SqlExpr.SubLink(query.tree, SqlSubLinkQuantifier.Any))

def all(query: DynamicQuery): Expr = Expr(SqlExpr.SubLink(query.tree, SqlSubLinkQuantifier.All))

def exists(query: DynamicQuery): Expr = Expr(SqlExpr.SubLink(query.tree, SqlSubLinkQuantifier.Exists))