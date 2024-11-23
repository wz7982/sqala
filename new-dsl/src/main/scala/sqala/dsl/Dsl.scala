package sqala.dsl

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.ast.table.*
import sqala.dsl.macros.TableMacro

import scala.deriving.Mirror

class SortOption[T]

extension [X](x: X)
    def asc(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def ascNullsFirst(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def ascNullsLast(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def desc(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def descNullsFirst(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def descNullsLast(using AsSqlExpr[X], QueryContext): SortOption[X] = compileTimeOnly

    def as[Y](using AsSqlExpr[X], QueryContext, Cast[X, Y]): Option[Y] = compileTimeOnly

    infix def over(value: OverValue)(using QueryContext): X = compileTimeOnly

    infix def over(value: Unit)(using QueryContext): X = compileTimeOnly

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

def partitionBy(value: Any*)(using QueryContext): OverValue =
    compileTimeOnly

def sortBy(value: SortOption[?]*)(using QueryContext): OverValue =
    compileTimeOnly

def currentRow: SqlWindowFrameOption = SqlWindowFrameOption.CurrentRow

def unboundedPreceding: SqlWindowFrameOption = SqlWindowFrameOption.UnboundedPreceding

def unboundedFollowing: SqlWindowFrameOption = SqlWindowFrameOption.UnboundedFollowing

extension (n: Int)
    def preceding: SqlWindowFrameOption = SqlWindowFrameOption.Preceding(n)

    def following: SqlWindowFrameOption = SqlWindowFrameOption.Following(n)

class OverValue:
    infix def sortBy(value: SortOption[?]*)(using QueryContext): OverValue =
        compileTimeOnly

    infix def rowsBetween(s: SqlWindowFrameOption, e: SqlWindowFrameOption)(using QueryContext): OverValue =
        compileTimeOnly

    infix def rangeBetween(s: SqlWindowFrameOption, e: SqlWindowFrameOption)(using QueryContext): OverValue =
        compileTimeOnly

    infix def groupsBetween(s: SqlWindowFrameOption, e: SqlWindowFrameOption)(using QueryContext): OverValue =
        compileTimeOnly

inline def query[T](using
    qc: QueryContext = QueryContext(-1, Nil),
    p: Mirror.ProductOf[T],
    s: SelectItem[Table[T]]
): TableQuery[Table[T]] =
    AsSqlExpr.summonInstances[p.MirroredElemTypes]
    val tableName = TableMacro.tableName[T]
    val newContext = QueryContext(qc.tableIndex + 1, qc.outerContainers)
    val aliasName = s"t${newContext.tableIndex}"
    val table = Table[T](tableName, aliasName, TableMacro.tableMetaData[T])
    val ast = SqlQuery.Select(
        select = s.selectItems(table, 0),
        from = SqlTable.IdentTable(tableName, Some(SqlTableAlias(aliasName))) :: Nil
    )
    TableQuery(table :: Nil, ast)(using newContext)