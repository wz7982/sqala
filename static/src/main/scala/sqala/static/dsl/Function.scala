package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.quantifier.SqlQuantifier
import sqala.metadata.{AsSqlExpr, Number, agg, function, window}

import java.time.LocalDateTime

@agg("GROUPING")
def grouping[T: AsExpr as a](items: T)(using QueryContext): Expr[Int] =
    Expr(SqlExpr.Func("GROUPING", a.exprs(items).map(_.asSqlExpr)))

@agg("COUNT")
def count()(using QueryContext): Expr[Long] = 
    Expr(SqlExpr.Func("COUNT", Nil))

@agg("COUNT")
def count[T: AsExpr as a](expr: T)(using QueryContext): Expr[Long] =
    Expr(SqlExpr.Func("COUNT", a.asExpr(expr).asSqlExpr :: Nil))

@agg("COUNT")
def count[T: AsExpr as a](expr: T, distinct: Boolean)(using QueryContext): Expr[Long] =
    Expr(SqlExpr.Func("COUNT", a.asExpr(expr).asSqlExpr :: Nil, if distinct then Some(SqlQuantifier.Distinct) else None))

@agg("SUM")
def sum[T: AsExpr as a](expr: T)(using n: Number[a.R], to: ToOption[Expr[a.R]], c: QueryContext): to.R =
    to.toOption(Expr(SqlExpr.Func("SUM", a.asExpr(expr).asSqlExpr :: Nil)))

@agg("AVG")
def avg[T: AsExpr as a](expr: T)(using Number[a.R], QueryContext): Expr[Option[BigDecimal]] =
    Expr(SqlExpr.Func("AVG", a.asExpr(expr).asSqlExpr :: Nil))

@agg("MAX")
def max[T: AsExpr as a](expr: T)(using to: ToOption[Expr[a.R]], c: QueryContext): to.R =
    to.toOption(Expr(SqlExpr.Func("MAX", a.asExpr(expr).asSqlExpr :: Nil)))

@agg("MIN")
def min[T: AsExpr as a](expr: T)(using to: ToOption[Expr[a.R]], c: QueryContext): to.R =
    to.toOption(Expr(SqlExpr.Func("MIN", a.asExpr(expr).asSqlExpr :: Nil)))

@agg("ANY_VALUE")
def angValue[T: AsExpr as a](expr: T)(using to: ToOption[Expr[a.R]], c: QueryContext): to.R =
    to.toOption(Expr(SqlExpr.Func("ANY_VALUE", a.asExpr(expr).asSqlExpr :: Nil)))

@agg("PERCENTILE_CONT")
def percentileCont[N: Number](
    n: Double,
    withinGroup: Sort[N]
)(using QueryContext): Expr[Option[BigDecimal]] =
    Expr(SqlExpr.Func("PERCENTILE_CONT", n.asExpr.asSqlExpr :: Nil, withinGroup = withinGroup.asSqlOrderBy :: Nil))

@agg("PERCENTILE_DISC")
def percentileDisc[N: Number](
    n: Double,
    withinGroup: Sort[N]
)(using QueryContext): Expr[Option[BigDecimal]] =
    Expr(SqlExpr.Func("PERCENTILE_DISC", n.asExpr.asSqlExpr :: Nil, withinGroup = withinGroup.asSqlOrderBy :: Nil))

@agg("STRING_AGG")
def stringAgg[T <: String | Option[String]](
    expr: Expr[T],
    separator: String,
    sortBy: Sort[?]
)(using QueryContext): Expr[Option[String]] =
    Expr(
        SqlExpr.Func(
            "STRING_AGG", 
            expr.asSqlExpr :: separator.asExpr.asSqlExpr :: Nil, 
            orderBy = sortBy.asSqlOrderBy :: Nil
        )
    )

@agg("STRING_AGG")
def groupConcat[T <: String | Option[String]](
    expr: Expr[T],
    separator: String,
    sortBy: Sort[?]
)(using QueryContext): Expr[Option[String]] =
    Expr(
        SqlExpr.Func(
            "STRING_AGG", 
            expr.asSqlExpr :: separator.asExpr.asSqlExpr :: Nil, 
            orderBy = sortBy.asSqlOrderBy :: Nil
        )
    )

@agg("STRING_AGG")
def listAgg[T <: String | Option[String]](
    expr: Expr[T],
    separator: String,
    sortBy: Sort[?]
)(using QueryContext): Expr[Option[String]] =
    Expr(
        SqlExpr.Func(
            "STRING_AGG", 
            expr.asSqlExpr :: separator.asExpr.asSqlExpr :: Nil, 
            orderBy = sortBy.asSqlOrderBy :: Nil
        )
    )

@window("RANK")
def rank()(using QueryContext): Expr[Long] =
    Expr(SqlExpr.Func("RANK", Nil))

@window("DENSE_RANK")
def denseRank()(using QueryContext): Expr[Long] =
    Expr(SqlExpr.Func("DENSE_RANK", Nil))

@window("ROW_NUMBER")
def rowNumber()(using QueryContext): Expr[Long] =
    Expr(SqlExpr.Func("ROW_NUMBER", Nil))

@window("LAG")
def lag[T](
    expr: Expr[T],
    offset: Int,
    default: Wrap[T, Option]
)(using 
    a: AsSqlExpr[Wrap[T, Option]],
    c: QueryContext
): Expr[Wrap[T, Option]] =
    val defaultExpr = a.asSqlExpr(default)
    Expr(SqlExpr.Func("LAG", expr.asSqlExpr :: offset.asExpr.asSqlExpr :: defaultExpr :: Nil))

@window("LEAD")
def lead[T](
    expr: Expr[T],
    offset: Int,
    default: Wrap[T, Option]
)(using 
    a: AsSqlExpr[Wrap[T, Option]],
    c: QueryContext
): Expr[Wrap[T, Option]] =
    val defaultExpr = a.asSqlExpr(default)
    Expr(SqlExpr.Func("LEAD", expr.asSqlExpr :: offset.asExpr.asSqlExpr :: defaultExpr :: Nil))

@window("NTILE")
def ntile(n: Int)(using QueryContext): Expr[Long] =
    Expr(SqlExpr.Func("NTILE", n.asExpr.asSqlExpr :: Nil))

@window("FIRST_VALUE")
def firstValue[T](expr: Expr[T])(using QueryContext): Expr[Wrap[T, Option]] =
    Expr(SqlExpr.Func("FIRST_VALUE", expr.asSqlExpr :: Nil))

@window("LAST_VALUE")
def lastValue[T](expr: Expr[T])(using QueryContext): Expr[Wrap[T, Option]] =
    Expr(SqlExpr.Func("LAST_VALUE", expr.asSqlExpr :: Nil))

@window("NTH_VALUE")
def nthValue[T](expr: Expr[T], n: Int)(using QueryContext): Expr[Wrap[T, Option]] =
    Expr(SqlExpr.Func("NTH_VALUE", expr.asSqlExpr :: n.asExpr.asSqlExpr :: Nil))

@window("CUME_DIST")
def cumeDist()(using QueryContext): Expr[BigDecimal] =
    Expr(SqlExpr.Func("CUME_DIST", Nil))

@window("PERCENT_RANK")
def percentRank()(using QueryContext): Expr[BigDecimal] =
    Expr(SqlExpr.Func("PERCENT_RANK", Nil))

@function("COALESCE")
def coalesce[T](expr: Expr[Option[T]], value: T)(using
    a: AsSqlExpr[T],
    c: QueryContext
): Expr[T] =
    Expr(SqlExpr.Func("COALESCE", expr.asSqlExpr :: a.asSqlExpr(value) :: Nil))

@function("COALESCE")
def ifNull[T](expr: Expr[Option[T]], value: T)(using
    a: AsSqlExpr[T],
    c: QueryContext
): Expr[T] =
    Expr(SqlExpr.Func("COALESCE", expr.asSqlExpr :: a.asSqlExpr(value) :: Nil))

@function("NULLIF")
def nullIf[T](expr: Expr[T], value: T)(using
    a: AsSqlExpr[T],
    c: QueryContext
): Expr[Wrap[T, Option]] =
    Expr(SqlExpr.Func("NULLIF", expr.asSqlExpr :: a.asSqlExpr(value) :: Nil))

@function("ABS")
def abs[T: Number](expr: Expr[T])(using QueryContext): Expr[T] =
    Expr(SqlExpr.Func("ABS", expr.asSqlExpr :: Nil))

@function("CEIL")
def ceil[T: Number](expr: Expr[T])(using QueryContext): Expr[Option[Long]] =
    Expr(SqlExpr.Func("CEIL", expr.asSqlExpr :: Nil))

@function("FLOOR")
def floor[T: Number](expr: Expr[T])(using QueryContext): Expr[Option[Long]] =
    Expr(SqlExpr.Func("FLOOR", expr.asSqlExpr :: Nil))

@function("ROUND")
def round[T: Number](expr: Expr[T], n: Int)(using QueryContext): Expr[Option[BigDecimal]] =
    Expr(SqlExpr.Func("ROUND", expr.asSqlExpr :: n.asExpr.asSqlExpr :: Nil))

@function("POWER")
def power[T: Number](expr: Expr[T], n: Double)(using QueryContext): Expr[Option[BigDecimal]] =
    Expr(SqlExpr.Func("POWER", expr.asSqlExpr :: n.asExpr.asSqlExpr :: Nil))

@function("CONCAT")
def concat[T: AsExpr as a](exprs: T)(using QueryContext): Expr[Option[String]] =
    Expr(SqlExpr.Func("CONCAT", a.exprs(exprs).map(_.asSqlExpr)))

@function("SUBSTRING")
def substring[T <: String | Option[String]](
    expr: Expr[T],
    start: Int,
    end: Int
)(using QueryContext): Expr[Option[String]] =
    Expr(SqlExpr.Func("SUBSTRING", expr.asSqlExpr :: start.asExpr.asSqlExpr :: end.asExpr.asSqlExpr :: Nil))

@function("REPLACE")
def replace[T <: String | Option[String]](
    expr: Expr[T],
    oldString: String,
    newString: String
)(using QueryContext): Expr[T] =
    Expr(SqlExpr.Func("REPLACE", expr.asSqlExpr :: oldString.asExpr.asSqlExpr :: newString.asExpr.asSqlExpr :: Nil))

@function("LENGTH")
def length[T <: String | Option[String]](expr: Expr[T])(using QueryContext): Expr[Option[Long]] =
    Expr(SqlExpr.Func("LENGTH", expr.asSqlExpr :: Nil))

@function("REPEAT")
def repeat[T <: String | Option[String]](expr: Expr[T], n: Int)(using QueryContext): Expr[T] =
    Expr(SqlExpr.Func("REPEAT", expr.asSqlExpr :: n.asExpr.asSqlExpr :: Nil))

@function("TRIM")
def trim[T <: String | Option[String]](expr: Expr[T])(using QueryContext): Expr[T] =
    Expr(SqlExpr.Func("TRIM", expr.asSqlExpr :: Nil))

@function("UPPER")
def upper[T <: String | Option[String]](expr: Expr[T])(using QueryContext): Expr[T] =
    Expr(SqlExpr.Func("UPPER", expr.asSqlExpr :: Nil))

@function("LOWER")
def lower[T <: String | Option[String]](expr: Expr[T])(using QueryContext): Expr[T] =
    Expr(SqlExpr.Func("LOWER", expr.asSqlExpr :: Nil))

@function("NOW")
def now()(using QueryContext): Expr[Option[LocalDateTime]] =
    Expr(SqlExpr.Func("NOW", Nil))