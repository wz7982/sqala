package sqala.dsl

import sqala.ast.expr.*

import java.util.Date
import java.time.{LocalDate, LocalDateTime}
import scala.annotation.targetName

// TODO query、subLink
given valueAndTuple1Equal[L, R](using CanEqual[L, R], QueryContext): CanEqual[L, Tuple1[R]] =
    CanEqual.derived

given tuple1AndValueEqual[L, R](using CanEqual[L, R], QueryContext): CanEqual[Tuple1[L], R] =
    CanEqual.derived

given valueAndOptionEqual[L, R](using CanEqual[L, R], QueryContext): CanEqual[L, Option[R]] =
    CanEqual.derived

given optionAndValueEqual[L, R](using CanEqual[L, R], QueryContext): CanEqual[Option[L], R] =
    CanEqual.derived

given numericEqual[L, R](using Numeric[L], Numeric[R], QueryContext): CanEqual[L, R] =
    CanEqual.derived

given timeEqual[L, R](using DateTime[L], DateTime[R], QueryContext): CanEqual[L, R] =
    CanEqual.derived

given timeStringEqual[L, R <: String | Option[String]](using DateTime[L], QueryContext): CanEqual[L, R] =
    CanEqual.derived

given stringTimeEqual[L <: String | Option[String], R](using DateTime[R], QueryContext): CanEqual[L, R] =
    CanEqual.derived

given nothingEqual[L](using QueryContext): CanEqual[L, Nothing] =
    CanEqual.derived

extension [X](x: X)
    def asc(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def ascNullsFirst(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def ascNullsLast(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def desc(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def descNullsFirst(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def descNullsLast(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def as[Y](using AsSqlExpr[X], QueryContext, Cast[X, Y]): Option[Y] = compileTimeOnly

extension [X](x: X)
    @targetName("gt")
    def >[Y](y: Y)(using QueryContext, CanEqual[X, Y]): Boolean =
        compileTimeOnly

    @targetName("ge")
    def >=[Y](y: Y)(using QueryContext, CanEqual[X, Y]): Boolean =
        compileTimeOnly

    @targetName("lt")
    def <[Y](y: Y)(using QueryContext, CanEqual[X, Y]): Boolean =
        compileTimeOnly

    @targetName("le")
    def <=[Y](y: Y)(using QueryContext, CanEqual[X, Y]): Boolean =
        compileTimeOnly

    def between[S, E](s: S, e: E)(using QueryContext, CanEqual[X, S], CanEqual[X, E]): Boolean =
        compileTimeOnly

    def in[I <: Tuple](items: I)(using QueryContext, CanIn[X, I]): Boolean =
        compileTimeOnly

extension (x: Json | Option[Json])
    def ->(n: Int)(using QueryContext): Option[Json] = compileTimeOnly

    def ->(k: String)(using QueryContext): Option[Json] = compileTimeOnly

    def ->>(n: Int)(using QueryContext): Option[String] = compileTimeOnly

    def ->>(k: String)(using QueryContext): Option[String] = compileTimeOnly

extension [X: Numeric](x: X)
    @targetName("plus")
    def +[Y: Numeric](y: Y)(using QueryContext): NumericOperationResult[X, Y] = compileTimeOnly

    @targetName("plus")
    def +(y: String | Option[String])(using QueryContext): Option[String] = compileTimeOnly

    @targetName("minus")
    def -[Y: Numeric](y: Y)(using QueryContext): NumericOperationResult[X, Y] = compileTimeOnly

    @targetName("times")
    def *[Y: Numeric](y: Y)(using QueryContext): NumericOperationResult[X, Y] = compileTimeOnly

    @targetName("div")
    def /[Y: Numeric](y: Y)(using QueryContext): NumericOperationResult[X, Y] = compileTimeOnly

    @targetName("mod")
    def %[Y: Numeric](y: Y)(using QueryContext): NumericOperationResult[X, Y] = compileTimeOnly

extension (x: String)
    def like(y: String | Option[String])(using QueryContext): Boolean = compileTimeOnly

    def startsWith(y: Option[String])(using QueryContext): Boolean = compileTimeOnly

    def endsWith(y: Option[String])(using QueryContext): Boolean = compileTimeOnly

    def contains(y: Option[String])(using QueryContext): Boolean = compileTimeOnly

extension (x: Option[String])
    @targetName("plus")
    def +[Y: AsSqlExpr](y: Y)(using QueryContext): Option[String] = compileTimeOnly

    def like(y: String | Option[String])(using QueryContext): Boolean = compileTimeOnly

    def startsWith(y: String | Option[String])(using QueryContext): Boolean = compileTimeOnly

    def endsWith(y: String | Option[String])(using QueryContext): Boolean = compileTimeOnly

    def contains(y: String | Option[String])(using QueryContext): Boolean = compileTimeOnly

// TODO 与exists查询的&& || exists的!

case class Interval(n: Double, unit: SqlTimeUnit)

extension (n: Double)
    def year: Interval = Interval(n, SqlTimeUnit.Year)

    def month: Interval = Interval(n, SqlTimeUnit.Month)

    def week: Interval = Interval(n, SqlTimeUnit.Week)

    def day: Interval = Interval(n, SqlTimeUnit.Day)

    def hour: Interval = Interval(n, SqlTimeUnit.Hour)

    def minute: Interval = Interval(n, SqlTimeUnit.Minute)

    def second: Interval = Interval(n, SqlTimeUnit.Second)

def interval(value: Interval): Interval = value

case class ExtractValue[T](unit: SqlTimeUnit, expr: T)

enum TimeUnit(val unit: SqlTimeUnit):
    case Year extends TimeUnit(SqlTimeUnit.Year)
    case Month extends TimeUnit(SqlTimeUnit.Month)
    case Week extends TimeUnit(SqlTimeUnit.Week)
    case Day extends TimeUnit(SqlTimeUnit.Day)
    case Hour extends TimeUnit(SqlTimeUnit.Hour)
    case Minute extends TimeUnit(SqlTimeUnit.Minute)
    case Second extends TimeUnit(SqlTimeUnit.Second)

    infix def from[T](expr: T): ExtractValue[T] =
        ExtractValue(unit, expr)

def year: TimeUnit = TimeUnit.Year

def month: TimeUnit = TimeUnit.Month

def week: TimeUnit = TimeUnit.Week

def day: TimeUnit = TimeUnit.Day

def hour: TimeUnit = TimeUnit.Hour

def minute: TimeUnit = TimeUnit.Minute

def second: TimeUnit = TimeUnit.Second

def extract[T: DateTime](value: ExtractValue[T]): Option[BigDecimal] =
    compileTimeOnly

extension [X: DateTime](x: X)
    @targetName("plus")
    def +(interval: Interval)(using QueryContext): X = compileTimeOnly

    @targetName("minus")
    def -(interval: Interval)(using QueryContext): X = compileTimeOnly

    @targetName("minus")
    def -(y: Date)(using QueryContext): X = compileTimeOnly

    @targetName("minusOptionDate")
    def -(y: Option[Date])(using QueryContext): X = compileTimeOnly

    @targetName("minus")
    def -(y: LocalDate)(using QueryContext): X = compileTimeOnly

    @targetName("minusOptionLocalDate")
    def -(y: Option[LocalDate])(using QueryContext): X = compileTimeOnly

    @targetName("minus")
    def -(y: LocalDateTime)(using QueryContext): X = compileTimeOnly

    @targetName("minusOptionLocalDateTime")
    def -(y: Option[LocalDateTime])(using QueryContext): X = compileTimeOnly