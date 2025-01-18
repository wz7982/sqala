package sqala.static.dsl

import sqala.common.*

import java.time.LocalDateTime

def grouping(items: Expr[?]*): Expr[Int] =
    Expr.Func("GROUPING", items.toList)

def count(): Expr[Long] = Expr.Func("COUNT", Nil)

def count[T](expr: Expr[T]): Expr[Long] =
    Expr.Func("COUNT", expr :: Nil, false)

def countDistinct[T](expr: Expr[T]): Expr[Long] =
    Expr.Func("COUNT", expr :: Nil, true)

def sum[T: Number](expr: Expr[T]): Expr[T] =
    Expr.Func("SUM", expr :: Nil)

def avg[T: Number](expr: Expr[T]): Expr[Option[BigDecimal]] =
    Expr.Func("AVG", expr :: Nil)

def max[T](expr: Expr[T]): Expr[Wrap[T, Option]] =
    Expr.Func("MAX", expr :: Nil)

def min[T](expr: Expr[T]): Expr[Wrap[T, Option]] =
    Expr.Func("MIN", expr :: Nil)

def anyValue[T](expr: Expr[T]): Expr[Wrap[T, Option]] =
    Expr.Func("ANY_VALUE", expr :: Nil)

def percentileCont[N: Number](
    n: Double,
    withinGroup: Sort[N]
): Expr[Option[BigDecimal]] =
    Expr.Func("PERCENTILE_CONT", n.asExpr :: Nil, withinGroup = withinGroup :: Nil)

def percentileDisc[N: Number](
    n: Double,
    withinGroup: Sort[N]
): Expr[Option[BigDecimal]] =
    Expr.Func("PERCENTILE_DISC", n.asExpr :: Nil, withinGroup = withinGroup :: Nil)

def stringAgg[T <: String | Option[String]](
    expr: Expr[T],
    separator: String,
    sortBy: Sort[?]*
): Expr[Option[String]] =
    Expr.Func("STRING_AGG", expr :: separator.asExpr :: Nil, false, sortBy.toList)

def rank(): Expr[Long] = Expr.Func("RANK", Nil)

def denseRank(): Expr[Long] = Expr.Func("DENSE_RANK", Nil)

def rowNumber(): Expr[Long] = Expr.Func("ROW_NUMBER", Nil)

def lag[T](
    expr: Expr[T],
    offset: Int = 1,
    default: Option[Unwrap[T, Option]] = None
)(using a: AsSqlExpr[Option[Unwrap[T, Option]]]): Expr[Wrap[T, Option]] =
    val defaultExpr = Expr.Literal(default, a)
    Expr.Func("LAG", expr :: Expr.Literal(offset, summon[AsSqlExpr[Int]]) :: defaultExpr :: Nil)

def lead[T](
    expr: Expr[T],
    offset: Int = 1,
    default: Option[Unwrap[T, Option]] = None
)(using a: AsSqlExpr[Option[Unwrap[T, Option]]]): Expr[Wrap[T, Option]] =
    val defaultExpr = Expr.Literal(default, a)
    Expr.Func("LEAD", expr :: Expr.Literal(offset, summon[AsSqlExpr[Int]]) :: defaultExpr :: Nil)

def ntile(n: Int): Expr[Int] =
    Expr.Func("NTILE", n.asExpr :: Nil)

def firstValue[T](expr: Expr[T]): Expr[Wrap[T, Option]] =
    Expr.Func("FIRST_VALUE", expr :: Nil)

def lastValue[T](expr: Expr[T]): Expr[Wrap[T, Option]] =
    Expr.Func("LAST_VALUE", expr :: Nil)

def nthValue[T](expr: Expr[T], n: Int): Expr[Wrap[T, Option]] =
    Expr.Func("NTH_VALUE", expr :: n.asExpr :: Nil)

def cumeDist(): Expr[BigDecimal] =
    Expr.Func("CUME_DIST", Nil)

def percentRank(): Expr[BigDecimal] =
    Expr.Func("PERCENT_RANK", Nil)

def coalesce[T](expr: Expr[Option[T]], value: T)(using
    a: AsSqlExpr[T]
): Expr[T] =
    Expr.Func("COALESCE", expr :: Expr.Literal(value, a) :: Nil)

def ifnull[T](expr: Expr[Option[T]], value: T)(using
    AsSqlExpr[T]
): Expr[T] =
    coalesce(expr, value)

def nullif[T](expr: Expr[T], value: T)(using
    a: AsSqlExpr[T]
): Expr[Wrap[T, Option]] =
    Expr.Func("NULLIF", expr :: Expr.Literal(value, a) :: Nil)

def abs[T: Number](expr: Expr[T]): Expr[T] =
    Expr.Func("ABS", expr :: Nil)

def ceil[T: Number](expr: Expr[T]): Expr[Option[Long]] =
    Expr.Func("CEIL", expr :: Nil)

def floor[T: Number](expr: Expr[T]): Expr[Option[Long]] =
    Expr.Func("FLOOR", expr :: Nil)

def round[T: Number](expr: Expr[T], n: Int): Expr[Option[BigDecimal]] =
    Expr.Func("ROUND", expr :: n.asExpr :: Nil)

def power[T: Number](expr: Expr[T], n: Double): Expr[Option[BigDecimal]] =
    Expr.Func("POWER", expr :: n.asExpr :: Nil)

def concat(
    expr: (Expr[String] | Expr[Option[String]] | String)*
): Expr[Option[String]] =
    val args = expr.toList.map:
        case s: String => s.asExpr
        case e: Expr[?] => e
    Expr.Func("CONCAT", args)

def substring[T <: String | Option[String]](
    expr: Expr[T],
    start: Int,
    end: Int
): Expr[Option[String]] =
    Expr.Func("SUBSTRING", expr :: start.asExpr :: end.asExpr :: Nil)

def replace[T <: String | Option[String]](
    expr: Expr[T],
    oldString: String,
    newString: String
): Expr[T] =
    Expr.Func("REPLACE", expr :: oldString.asExpr :: newString.asExpr :: Nil)

def length[T <: String | Option[String]](expr: Expr[T]): Expr[Option[Long]] =
    Expr.Func("LENGTH", expr :: Nil)

def repeat[T <: String | Option[String]](expr: Expr[T], n: Int): Expr[T] =
    Expr.Func("REPEAT", expr :: n.asExpr :: Nil)

def trim[T <: String | Option[String]](expr: Expr[T]): Expr[T] =
    Expr.Func("TRIM", expr :: Nil)

def upper[T <: String | Option[String]](expr: Expr[T]): Expr[T] =
    Expr.Func("UPPER", expr :: Nil)

def lower[T <: String | Option[String]](expr: Expr[T]): Expr[T] =
    Expr.Func("LOWER", expr :: Nil)

def now(): Expr[Option[LocalDateTime]] =
    Expr.Func("NOW", Nil)