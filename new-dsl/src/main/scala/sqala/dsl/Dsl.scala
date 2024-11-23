package sqala.dsl

import sqala.ast.expr.*

class SortOption[T]

extension [X](x: X)
    def asc(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def ascNullsFirst(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def ascNullsLast(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def desc(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def descNullsFirst(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def descNullsLast(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def as[Y](using AsSqlExpr[X], QueryContext, Cast[X, Y]): Option[Y] = compileTimeOnly

    infix def over(value: OverValue): X = compileTimeOnly

    infix def over(value: Unit): X = compileTimeOnly

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

extension [X: DateTime](x: X)
    def year: ExtractValue[X] = ExtractValue(SqlTimeUnit.Year, x)

    def month: ExtractValue[X] = ExtractValue(SqlTimeUnit.Month, x)

    def week: ExtractValue[X] = ExtractValue(SqlTimeUnit.Week, x)

    def day: ExtractValue[X] = ExtractValue(SqlTimeUnit.Day, x)

    def hour: ExtractValue[X] = ExtractValue(SqlTimeUnit.Hour, x)

    def minute: ExtractValue[X] = ExtractValue(SqlTimeUnit.Minute, x)

    def second: ExtractValue[X] = ExtractValue(SqlTimeUnit.Second, x)

def extract[T: DateTime](value: ExtractValue[T])(using QueryContext): Option[BigDecimal] =
    compileTimeOnly

class OverValue:
    infix def sortBy(sort: SortOption[?]*)(using QueryContext): OverValue =
        compileTimeOnly