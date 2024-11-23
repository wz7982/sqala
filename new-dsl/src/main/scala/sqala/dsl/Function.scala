package sqala.dsl

import sqala.annotation.*

import java.time.LocalDateTime
import scala.compiletime.ops.boolean.*
import scala.compiletime.ops.double.*

@sqlAgg("GROUPING")
def grouping(items: Any*)(using QueryContext): Int =
    compileTimeOnly

@sqlAgg("COUNT")
def count()(using QueryContext): Long =
    compileTimeOnly

@sqlAgg("COUNT")
def count[T](x: T, distinct: Boolean = false)(using AsSqlExpr[T], QueryContext): Long =
    compileTimeOnly

@sqlAgg("SUM")
def sum[T](x: T)(using Numeric[T], QueryContext): Option[BigDecimal] =
    compileTimeOnly

@sqlAgg("AVG")
def avg[T](x: T)(using Numeric[T], QueryContext): Option[BigDecimal] =
    compileTimeOnly

@sqlAgg("MAX")
def max[T](x: T)(using AsSqlExpr[T], QueryContext): Wrap[T, Option] =
    compileTimeOnly

@sqlAgg("MIN")
def min[T](x: T)(using AsSqlExpr[T], QueryContext): Wrap[T, Option] =
    compileTimeOnly

@sqlAgg("ANY_VALUE")
def anyValue[T](x: T)(using AsSqlExpr[T], QueryContext): Wrap[T, Option] =
    compileTimeOnly

@sqlAgg("PERCENTILE_CONT")
def percentileCont[T](
    n: Double,
    withinGroup: SortOption[T]
)(using
    Numeric[T],
    Validate[n.type >= 0D && n.type <= 1D, "The percentage must be between 0 and 1."],
    QueryContext
): Wrap[T, Option] =
    compileTimeOnly

@sqlAgg("PERCENTILE_DISC")
def percentileDisc[T](
    n: Double,
    withinGroup: SortOption[T]
)(using
    Numeric[T],
    Validate[n.type >= 0D && n.type <= 1D, "The percentage must be between 0 and 1."],
    QueryContext
): Wrap[T, Option] =
    compileTimeOnly

@sqlAgg("STRING_AGG")
def stringAgg(
    x: String | Option[String],
    separator: String,
    sortBy: SortOption[?]*
)(using QueryContext): Option[String] =
    compileTimeOnly

@sqlAgg("STRING_AGG")
def groupConcat(
    x: String | Option[String],
    separator: String,
    sortBy: SortOption[?]*
)(using QueryContext): Option[String] =
    compileTimeOnly

@sqlWindow("RANK")
def rank()(using QueryContext): Long =
    compileTimeOnly

@sqlWindow("DENSE_RANK")
def denseRank()(using QueryContext): Long =
    compileTimeOnly

@sqlWindow("PERCENT_RANK")
def percentRank()(using QueryContext): BigDecimal =
    compileTimeOnly

@sqlWindow("ROW_NUMBER")
def rowNumber()(using QueryContext): Long =
    compileTimeOnly

@sqlWindow("LAG")
def lag[T](x: T, offset: Int)(using AsSqlExpr[T], QueryContext): Wrap[T, Option] =
    compileTimeOnly

@sqlWindow("LAG")
def lag[T](x: T)(using AsSqlExpr[T], QueryContext): Wrap[T, Option] =
    compileTimeOnly

@sqlWindow("LAG")
def lag[T](x: T, offset: Int, default: Wrap[T, Option])(using AsSqlExpr[T], QueryContext): Wrap[T, Option] =
    compileTimeOnly

@sqlWindow("LEAD")
def lead[T](x: T)(using AsSqlExpr[T], QueryContext): Wrap[T, Option] =
    compileTimeOnly

@sqlWindow("LEAD")
def lead[T](x: T, offset: Int)(using AsSqlExpr[T], QueryContext): Wrap[T, Option] =
    compileTimeOnly

@sqlWindow("LEAD")
def lead[T](x: T, offset: Int, default: Wrap[T, Option])(using AsSqlExpr[T], QueryContext): Wrap[T, Option] =
    compileTimeOnly

@sqlWindow("NTILE")
def ntile(n: Int)(using QueryContext): Long =
    compileTimeOnly

@sqlWindow("FIRST_VALUE")
def firstValue[T](expr: T)(using AsSqlExpr[T], QueryContext): Wrap[T, Option] =
    compileTimeOnly

@sqlWindow("LAST_VALUE")
def lastValue[T](expr: T)(using AsSqlExpr[T], QueryContext): Wrap[T, Option] =
    compileTimeOnly

@sqlWindow("NTH_VALUE")
def nthValue[T](expr: T, n: Int)(using AsSqlExpr[T], QueryContext): Wrap[T, Option] =
    compileTimeOnly

@sqlWindow("CUME_DIST")
def cumeDist()(using QueryContext): BigDecimal =
    compileTimeOnly

@sqlFunction("COALESCE")
def coalesce[T](x: Option[T], value: T)(using AsSqlExpr[T], QueryContext): T =
    compileTimeOnly

@sqlFunction("COALESCE")
def ifNull[T](x: Option[T], value: T)(using AsSqlExpr[T], QueryContext): T =
    compileTimeOnly

@sqlFunction("NULLIF")
def nullIf[T](x: T, value: T)(using AsSqlExpr[T], QueryContext): Wrap[T, Option] =
    compileTimeOnly

@sqlFunction("ABS")
def abs[T](x: T)(using Numeric[T], QueryContext): T =
    compileTimeOnly

@sqlFunction("CEIL")
def ceil[T](x: T)(using Numeric[T], QueryContext): Option[Long] =
    compileTimeOnly

@sqlFunction("FLOOR")
def floor[T](x: T)(using Numeric[T], QueryContext): Option[Long] =
    compileTimeOnly

@sqlFunction("ROUND")
def round[T](x: T, n: Int)(using Numeric[T], QueryContext): Option[BigDecimal] =
    compileTimeOnly

@sqlFunction("POWER")
def power[T](x: T, n: Double)(using Numeric[T], QueryContext): Option[BigDecimal] =
    compileTimeOnly

@sqlFunction("CONCAT")
def concat(items: (String | Option[String])*)(using QueryContext): Option[String] = compileTimeOnly

@sqlFunction("SUBSTRING")
def subString(x: String | Option[String], start: Int, end: Int)(using QueryContext): Option[String] =
    compileTimeOnly

@sqlFunction("REPLACE")
def replace(
    x: String | Option[String],
    oldString: String,
    newString: String
)(using QueryContext): Option[String] =
    compileTimeOnly

@sqlFunction("LENGTH")
def length(x: String | Option[String])(using QueryContext): Option[Long] =
    compileTimeOnly

@sqlFunction("TRIM")
def trim(x: String | Option[String])(using QueryContext): Option[String] =
    compileTimeOnly

@sqlFunction("UPPER")
def upper(x: String | Option[String])(using QueryContext): Option[String] =
    compileTimeOnly

@sqlFunction("LOWER")
def lower(x: String | Option[String])(using QueryContext): Option[String] =
    compileTimeOnly

@sqlFunction("NOW")
def now()(using QueryContext): Option[LocalDateTime] =
    compileTimeOnly