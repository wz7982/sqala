package sqala.static.dsl

import sqala.ast.expr.*
import sqala.static.common.*
import sqala.static.statement.query.Query
import sqala.static.statement.dml.*

import java.time.{LocalDate, LocalDateTime}

export sqala.printer.{
    DB2Dialect,
    MssqlDialect,
    MysqlDialect,
    OracleDialect,
    PostgresqlDialect,
    SqliteDialect
}

export sqala.static.annotation.{
    autoInc, 
    column, 
    primaryKey, 
    sqlAgg, 
    sqlFunction, 
    sqlWindow, 
    table
}

export sqala.static.common.compileTimeOnly

export sqala.static.common.{
    CustomField, 
    QueryContext, 
    Table, 
    Validate
}

def exists(query: Query[?, ?])(using QueryContext): Boolean = compileTimeOnly

def all[T](query: Query[T, ?])(using QueryContext): SubLink[T] = compileTimeOnly

def any[T](query: Query[T, ?])(using QueryContext): SubLink[T] = compileTimeOnly

class Window[T]

type OverResult[T] = T match
    case Window[t] => t
    case _ => T

extension [X](x: X)
    def asc(using QueryContext): Sort[X] = compileTimeOnly

    def ascNullsFirst(using QueryContext): Sort[X] = compileTimeOnly

    def ascNullsLast(using QueryContext): Sort[X] = compileTimeOnly

    def desc(using QueryContext): Sort[X] = compileTimeOnly

    def descNullsFirst(using QueryContext): Sort[X] = compileTimeOnly

    def descNullsLast(using QueryContext): Sort[X] = compileTimeOnly

    def as[Y](using QueryContext, Cast[X, Y]): Option[Y] = compileTimeOnly

    def :=[Y](value: Y)(using CanEqual[X, Y]): UpdatePair = compileTimeOnly

    infix def over(value: OverValue)(using QueryContext): OverResult[X] = compileTimeOnly

    infix def over(value: Unit)(using QueryContext): OverResult[X] = compileTimeOnly

case class TimeInterval(n: Double, unit: SqlTimeUnit)

extension (n: Double)
    def year: TimeInterval = TimeInterval(n, SqlTimeUnit.Year)

    def month: TimeInterval = TimeInterval(n, SqlTimeUnit.Month)

    def week: TimeInterval = TimeInterval(n, SqlTimeUnit.Week)

    def day: TimeInterval = TimeInterval(n, SqlTimeUnit.Day)

    def hour: TimeInterval = TimeInterval(n, SqlTimeUnit.Hour)

    def minute: TimeInterval = TimeInterval(n, SqlTimeUnit.Minute)

    def second: TimeInterval = TimeInterval(n, SqlTimeUnit.Second)

def interval(value: TimeInterval): TimeInterval = value

class ExtractValue[T]

extension [X: DateTime](x: X)
    def year: ExtractValue[X] = ExtractValue()

    def month: ExtractValue[X] = ExtractValue()

    def week: ExtractValue[X] = ExtractValue()

    def day: ExtractValue[X] = ExtractValue()

    def hour: ExtractValue[X] = ExtractValue()

    def minute: ExtractValue[X] = ExtractValue()

    def second: ExtractValue[X] = ExtractValue()

extension [X <: Interval | Option[Interval]](x: X)
    def year: ExtractValue[X] = ExtractValue()

    def month: ExtractValue[X] = ExtractValue()

    def week: ExtractValue[X] = ExtractValue()

    def day: ExtractValue[X] = ExtractValue()

    def hour: ExtractValue[X] = ExtractValue()

    def minute: ExtractValue[X] = ExtractValue()

    def second: ExtractValue[X] = ExtractValue()

def extract[T: DateTime](value: ExtractValue[T])(using QueryContext): Option[BigDecimal] =
    compileTimeOnly

def extract[T <: Interval | Option[Interval]](value: ExtractValue[T])(using QueryContext): Option[BigDecimal] =
    compileTimeOnly

extension (s: StringContext)
    def timestamp()(using QueryContext): LocalDateTime = compileTimeOnly

    def date()(using QueryContext): LocalDate = compileTimeOnly

def partitionBy(value: Any*)(using QueryContext): OverValue =
    compileTimeOnly

def sortBy(value: Sort[?]*)(using QueryContext): OverValue =
    compileTimeOnly

def currentRow: SqlWindowFrameOption = SqlWindowFrameOption.CurrentRow

def unboundedPreceding: SqlWindowFrameOption = SqlWindowFrameOption.UnboundedPreceding

def unboundedFollowing: SqlWindowFrameOption = SqlWindowFrameOption.UnboundedFollowing

extension (n: Int)
    def preceding: SqlWindowFrameOption = SqlWindowFrameOption.Preceding(n)

    def following: SqlWindowFrameOption = SqlWindowFrameOption.Following(n)

class OverValue:
    infix def sortBy(value: Sort[?]*)(using QueryContext): OverValue =
        compileTimeOnly

    infix def rowsBetween(s: SqlWindowFrameOption, e: SqlWindowFrameOption)(using QueryContext): OverValue =
        compileTimeOnly

    infix def rangeBetween(s: SqlWindowFrameOption, e: SqlWindowFrameOption)(using QueryContext): OverValue =
        compileTimeOnly

    infix def groupsBetween(s: SqlWindowFrameOption, e: SqlWindowFrameOption)(using QueryContext): OverValue =
        compileTimeOnly