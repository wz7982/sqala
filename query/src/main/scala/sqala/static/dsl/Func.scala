package sqala.static.dsl

import sqala.common.*
import sqala.metadata.*

import java.time.LocalDateTime

def grouping[T: AsExpr as a](items: T): Expr[Int] =
    Expr.Func("GROUPING", a.exprs(items))

@agg
def count(): Expr[Long] = Expr.Func("COUNT", Nil)

@agg
def count[T: AsExpr as a](expr: T): Expr[Long] =
    Expr.Func("COUNT", a.asExpr(expr) :: Nil, false)

@agg
def count[T: AsExpr as a](expr: T, distinct: Boolean): Expr[Long] =
    Expr.Func("COUNT", a.asExpr(expr) :: Nil, distinct)

@agg
def sum[T: AsExpr as a](expr: T)(using n: Number[a.R], to: ToOption[Expr[a.R]]): to.R =
    to.toOption(Expr.Func("SUM", a.asExpr(expr) :: Nil))

@agg
def avg[T: AsExpr as a](expr: T)(using Number[a.R]): Expr[Option[BigDecimal]] =
    Expr.Func("AVG", a.asExpr(expr) :: Nil)

@agg
def max[T: AsExpr as a](expr: T)(using to: ToOption[Expr[a.R]]): to.R =
    to.toOption(Expr.Func("MAX", a.asExpr(expr) :: Nil))

@agg
def min[T: AsExpr as a](expr: T)(using to: ToOption[Expr[a.R]]): to.R =
    to.toOption(Expr.Func("MIN", a.asExpr(expr) :: Nil))

@agg
def anyValue[T: AsExpr as a](expr: T)(using to: ToOption[Expr[a.R]]): to.R =
    to.toOption(Expr.Func("ANY_VALUE", a.asExpr(expr) :: Nil))

@agg
def percentileCont[N: Number](
    n: Double,
    withinGroup: Sort[N]
): Expr[Option[BigDecimal]] =
    Expr.Func("PERCENTILE_CONT", n.asExpr :: Nil, withinGroup = withinGroup :: Nil)

@agg
def percentileDisc[N: Number](
    n: Double,
    withinGroup: Sort[N]
): Expr[Option[BigDecimal]] =
    Expr.Func("PERCENTILE_DISC", n.asExpr :: Nil, withinGroup = withinGroup :: Nil)

@agg
def stringAgg[T <: String | Option[String]](
    expr: Expr[T],
    separator: String,
    sortBy: Sort[?]*
): Expr[Option[String]] =
    Expr.Func("STRING_AGG", expr :: separator.asExpr :: Nil, false, sortBy.toList)

@agg
def groupConcat[T <: String | Option[String]](
    expr: Expr[T],
    separator: String,
    sortBy: Sort[?]*
): Expr[Option[String]] =
    Expr.Func("STRING_AGG", expr :: separator.asExpr :: Nil, false, sortBy.toList)

@agg
def listAgg[T <: String | Option[String]](
    expr: Expr[T],
    separator: String,
    sortBy: Sort[?]*
): Expr[Option[String]] =
    Expr.Func("STRING_AGG", expr :: separator.asExpr :: Nil, false, sortBy.toList)

@window
def rank(): Expr[Long] = Expr.Func("RANK", Nil)

@window
def denseRank(): Expr[Long] = Expr.Func("DENSE_RANK", Nil)

@window
def rowNumber(): Expr[Long] = Expr.Func("ROW_NUMBER", Nil)

@window
def lag[T](
    expr: Expr[T],
    offset: Int = 1,
    default: Option[Unwrap[T, Option]] = None
)(using a: AsSqlExpr[Option[Unwrap[T, Option]]]): Expr[Wrap[T, Option]] =
    val defaultExpr = Expr.Literal(default, a)
    Expr.Func("LAG", expr :: Expr.Literal(offset, summon[AsSqlExpr[Int]]) :: defaultExpr :: Nil)

@window
def lead[T](
    expr: Expr[T],
    offset: Int = 1,
    default: Option[Unwrap[T, Option]] = None
)(using a: AsSqlExpr[Option[Unwrap[T, Option]]]): Expr[Wrap[T, Option]] =
    val defaultExpr = Expr.Literal(default, a)
    Expr.Func("LEAD", expr :: Expr.Literal(offset, summon[AsSqlExpr[Int]]) :: defaultExpr :: Nil)

@window
def ntile(n: Int): Expr[Int] =
    Expr.Func("NTILE", n.asExpr :: Nil)

@window
def firstValue[T](expr: Expr[T]): Expr[Wrap[T, Option]] =
    Expr.Func("FIRST_VALUE", expr :: Nil)

@window
def lastValue[T](expr: Expr[T]): Expr[Wrap[T, Option]] =
    Expr.Func("LAST_VALUE", expr :: Nil)

@window
def nthValue[T](expr: Expr[T], n: Int): Expr[Wrap[T, Option]] =
    Expr.Func("NTH_VALUE", expr :: n.asExpr :: Nil)

@window
def cumeDist(): Expr[BigDecimal] =
    Expr.Func("CUME_DIST", Nil)

@window
def percentRank(): Expr[BigDecimal] =
    Expr.Func("PERCENT_RANK", Nil)

@function
def coalesce[T](expr: Expr[Option[T]], value: T)(using
    a: AsSqlExpr[T]
): Expr[T] =
    Expr.Func("COALESCE", expr :: Expr.Literal(value, a) :: Nil)

@function
def ifNull[T](expr: Expr[Option[T]], value: T)(using
    AsSqlExpr[T]
): Expr[T] =
    coalesce(expr, value)

@function
def nullIf[T](expr: Expr[T], value: T)(using
    a: AsSqlExpr[T]
): Expr[Wrap[T, Option]] =
    Expr.Func("NULLIF", expr :: Expr.Literal(value, a) :: Nil)

@function
def abs[T: Number](expr: Expr[T]): Expr[T] =
    Expr.Func("ABS", expr :: Nil)

@function
def ceil[T: Number](expr: Expr[T]): Expr[Option[Long]] =
    Expr.Func("CEIL", expr :: Nil)

@function
def floor[T: Number](expr: Expr[T]): Expr[Option[Long]] =
    Expr.Func("FLOOR", expr :: Nil)

@function
def round[T: Number](expr: Expr[T], n: Int): Expr[Option[BigDecimal]] =
    Expr.Func("ROUND", expr :: n.asExpr :: Nil)

@function
def power[T: Number](expr: Expr[T], n: Double): Expr[Option[BigDecimal]] =
    Expr.Func("POWER", expr :: n.asExpr :: Nil)

@function
def concat[T: AsExpr as a](exprs: T): Expr[Option[String]] =
    Expr.Func("CONCAT", a.exprs(exprs))

@function
def substring[T <: String | Option[String]](
    expr: Expr[T],
    start: Int,
    end: Int
): Expr[Option[String]] =
    Expr.Func("SUBSTRING", expr :: start.asExpr :: end.asExpr :: Nil)

@function
def replace[T <: String | Option[String]](
    expr: Expr[T],
    oldString: String,
    newString: String
): Expr[T] =
    Expr.Func("REPLACE", expr :: oldString.asExpr :: newString.asExpr :: Nil)

@function
def length[T <: String | Option[String]](expr: Expr[T]): Expr[Option[Long]] =
    Expr.Func("LENGTH", expr :: Nil)

@function
def repeat[T <: String | Option[String]](expr: Expr[T], n: Int): Expr[T] =
    Expr.Func("REPEAT", expr :: n.asExpr :: Nil)

@function
def trim[T <: String | Option[String]](expr: Expr[T]): Expr[T] =
    Expr.Func("TRIM", expr :: Nil)

@function
def upper[T <: String | Option[String]](expr: Expr[T]): Expr[T] =
    Expr.Func("UPPER", expr :: Nil)

@function
def lower[T <: String | Option[String]](expr: Expr[T]): Expr[T] =
    Expr.Func("LOWER", expr :: Nil)

@function
def now(): Expr[Option[LocalDateTime]] =
    Expr.Func("NOW", Nil)