package sqala.dsl

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.ast.table.*
import sqala.dsl.annotation.*
import sqala.dsl.macros.TableMacro
import sqala.dsl.statement.dml.*
import sqala.dsl.statement.query.*

import java.util.Date
import scala.NamedTuple.NamedTuple
import scala.deriving.Mirror
import scala.compiletime.ops.boolean.*
import scala.compiletime.ops.double.*

extension [T: AsSqlExpr](value: T)
    def asExpr: Expr[T] = Expr.Literal(value, summon[AsSqlExpr[T]])

class If[T](private[sqala] val exprs: List[Expr[?]]):
    infix def `then`[E: AsSqlExpr](expr: Expr[E])(using
        o: ResultOperation[T, E]
    ): IfThen[o.R] =
        IfThen(exprs :+ expr)

    infix def `then`[E](value: E)(using
        a: AsSqlExpr[E],
        o: ResultOperation[T, E]
    ): IfThen[o.R] =
        IfThen(exprs :+ Expr.Literal(value, a))

class IfThen[T](private[sqala] val exprs: List[Expr[?]]):
    infix def `else`[E: AsSqlExpr](expr: Expr[E])(using
        o: ResultOperation[T, E]
    ): Expr[o.R] =
        val caseBranches =
            exprs.grouped(2).toList.map(i => (i(0), i(1)))
        Expr.Case(caseBranches, expr)

    infix def `else`[E](value: E)(using
        a: AsSqlExpr[E],
        o: ResultOperation[T, E]
    ): Expr[o.R] =
        val caseBranches =
            exprs.grouped(2).toList.map(i => (i(0), i(1)))
        Expr.Case(caseBranches, Expr.Literal(value, a))

    infix def `else if`(expr: Expr[Boolean]): If[T] =
        If(exprs :+ expr)

def `if`(expr: Expr[Boolean]): If[Nothing] = If(expr :: Nil)

def exists[T, S <: ResultSize](query: Query[T, S]): Expr[Boolean] =
    Expr.SubLink(query.ast, SqlSubLinkType.Exists)

def all[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using 
    m: Merge[V]
): SubLinkItem[m.R] =
    SubLinkItem(query.ast, SqlSubLinkType.All)

def any[N <: Tuple, V <: Tuple, S <: ResultSize](query: Query[NamedTuple[N, V], S])(using 
    m: Merge[V]
): SubLinkItem[m.R] =
    SubLinkItem(query.ast, SqlSubLinkType.Any)

def grouping(items: Expr[?]*): Expr[Int] =
    Expr.Func("GROUPING", items.toList)

@sqlAgg
def count(): Expr[Long] = Expr.Func("COUNT", Nil)

@sqlAgg
def count[T](expr: Expr[T]): Expr[Long] =
    Expr.Func("COUNT", expr :: Nil, false)

@sqlAgg
def countDistinct[T](expr: Expr[T]): Expr[Long] =
    Expr.Func("COUNT", expr :: Nil, true)

@sqlAgg
def sum[T: Number](expr: Expr[T]): Expr[Option[BigDecimal]] =
    Expr.Func("SUM", expr :: Nil)

@sqlAgg
def avg[T: Number](expr: Expr[T]): Expr[Option[BigDecimal]] =
    Expr.Func("AVG", expr :: Nil)

@sqlAgg
def max[T](expr: Expr[T]): Expr[Wrap[T, Option]] =
    Expr.Func("MAX", expr :: Nil)

@sqlAgg
def min[T](expr: Expr[T]): Expr[Wrap[T, Option]] =
    Expr.Func("MIN", expr :: Nil)

@sqlAgg
def anyValue[T](expr: Expr[T]): Expr[Wrap[T, Option]] =
    Expr.Func("ANY_VALUE", expr :: Nil)

@sqlAgg
def percentileCont[N: Number](
    n: Double,
    withinGroup: SortBy[N]
)(using Validate[n.type >= 0D && n.type <= 1D, "The percentage must be between 0 and 1."]): Expr[Option[BigDecimal]] =
    Expr.Func("PERCENTILE_CONT", n.asExpr :: Nil, withinGroup = withinGroup :: Nil)

@sqlAgg
def percentileDisc[N: Number](
    n: Double,
    withinGroup: SortBy[N]
)(using Validate[n.type >= 0D && n.type <= 1D, "The percentage must be between 0 and 1."]): Expr[Option[BigDecimal]] =
    Expr.Func("PERCENTILE_DISC", n.asExpr :: Nil, withinGroup = withinGroup :: Nil)

@sqlAgg
def stringAgg[T <: String | Option[String]](
    expr: Expr[T],
    separator: String,
    sortBy: SortBy[?]*
): Expr[Option[String]] =
    Expr.Func("STRING_AGG", expr :: separator.asExpr :: Nil, false, sortBy.toList)

@sqlWindow
def rank(): WindowFunc[Long] = WindowFunc("RANK", Nil)

@sqlWindow
def denseRank(): WindowFunc[Long] = WindowFunc("DENSE_RANK", Nil)

@sqlWindow
def rowNumber(): WindowFunc[Long] = WindowFunc("ROW_NUMBER", Nil)

@sqlWindow
def lag[T](
    expr: Expr[T],
    offset: Int = 1,
    default: Option[Unwrap[T, Option]] = None
)(using a: AsSqlExpr[Option[Unwrap[T, Option]]]): WindowFunc[Wrap[T, Option]] =
    val defaultExpr = Expr.Literal(default, a)
    WindowFunc("LAG", expr :: Expr.Literal(offset, summon[AsSqlExpr[Int]]) :: defaultExpr :: Nil)

@sqlWindow
def lead[T](
    expr: Expr[T],
    offset: Int = 1,
    default: Option[Unwrap[T, Option]] = None
)(using a: AsSqlExpr[Option[Unwrap[T, Option]]]): WindowFunc[Wrap[T, Option]] =
    val defaultExpr = Expr.Literal(default, a)
    WindowFunc("LEAD", expr :: Expr.Literal(offset, summon[AsSqlExpr[Int]]) :: defaultExpr :: Nil)

@sqlWindow
def ntile(n: Int): WindowFunc[Int] =
    WindowFunc("NTILE", n.asExpr :: Nil)

@sqlWindow
def firstValue[T](expr: Expr[T]): WindowFunc[Wrap[T, Option]] =
    WindowFunc("FIRST_VALUE", expr :: Nil)

@sqlWindow
def lastValue[T](expr: Expr[T]): WindowFunc[Wrap[T, Option]] =
    WindowFunc("LAST_VALUE", expr :: Nil)

@sqlWindow
def nthValue[T](expr: Expr[T], n: Int): WindowFunc[Wrap[T, Option]] =
    WindowFunc("NTH_VALUE", expr :: n.asExpr :: Nil)

@sqlWindow
def cumeDist(): WindowFunc[BigDecimal] =
    WindowFunc("CUME_DIST", Nil)

@sqlWindow
def percentRank(): WindowFunc[BigDecimal] =
    WindowFunc("PERCENT_RANK", Nil)

@sqlFunction
def coalesce[T](expr: Expr[Option[T]], value: T)(using
    a: AsSqlExpr[T]
): Expr[T] =
    Expr.Func("COALESCE", expr :: Expr.Literal(value, a) :: Nil)

@sqlFunction
def ifnull[T](expr: Expr[Option[T]], value: T)(using
    AsSqlExpr[T]
): Expr[T] =
    coalesce(expr, value)

@sqlFunction
def nullif[T](expr: Expr[T], value: T)(using
    a: AsSqlExpr[T]
): Expr[Wrap[T, Option]] =
    Expr.Func("NULLIF", expr :: Expr.Literal(value, a) :: Nil)

@sqlFunction
def abs[T: Number](expr: Expr[T]): Expr[T] =
    Expr.Func("ABS", expr :: Nil)

@sqlFunction
def ceil[T: Number](expr: Expr[T]): Expr[Option[Long]] =
    Expr.Func("CEIL", expr :: Nil)

@sqlFunction
def floor[T: Number](expr: Expr[T]): Expr[Option[Long]] =
    Expr.Func("FLOOR", expr :: Nil)

@sqlFunction
def round[T: Number](expr: Expr[T], n: Int): Expr[Option[BigDecimal]] =
    Expr.Func("ROUND", expr :: n.asExpr :: Nil)

@sqlFunction
def power[T: Number](expr: Expr[T], n: Double): Expr[Option[BigDecimal]] =
    Expr.Func("POWER", expr :: n.asExpr :: Nil)

@sqlFunction
def concat(
    expr: (Expr[String] | Expr[Option[String]] | String)*
): Expr[Option[String]] =
    val args = expr.toList.map:
        case s: String => s.asExpr
        case e: Expr[?] => e
    Expr.Func("CONCAT", args)

@sqlFunction
def substring[T <: String | Option[String]](
    expr: Expr[T],
    start: Int,
    end: Int
): Expr[Option[String]] =
    Expr.Func("SUBSTRING", expr :: start.asExpr :: end.asExpr :: Nil)

@sqlFunction
def replace[T <: String | Option[String]](
    expr: Expr[T],
    oldString: String,
    newString: String
): Expr[T] =
    Expr.Func("REPLACE", expr :: oldString.asExpr :: newString.asExpr :: Nil)

@sqlFunction
def length[T <: String | Option[String]](expr: Expr[T]): Expr[Option[Long]] =
    Expr.Func("LENGTH", expr :: Nil)

@sqlFunction
def repeat[T <: String | Option[String]](expr: Expr[T], n: Int): Expr[T] =
    Expr.Func("REPEAT", expr :: n.asExpr :: Nil)

@sqlFunction
def trim[T <: String | Option[String]](expr: Expr[T]): Expr[T] =
    Expr.Func("TRIM", expr :: Nil)

@sqlFunction
def upper[T <: String | Option[String]](expr: Expr[T]): Expr[T] =
    Expr.Func("UPPER", expr :: Nil)

@sqlFunction
def lower[T <: String | Option[String]](expr: Expr[T]): Expr[T] =
    Expr.Func("LOWER", expr :: Nil)

@sqlFunction
def now(): Expr[Option[Date]] =
    Expr.Func("NOW", Nil)

def createFunc[T](
    name: String,
    args: List[Expr[?]],
    distinct: Boolean = false,
    sortBy: List[SortBy[?]] = Nil,
    withinGroup: List[SortBy[?]] = Nil
): Expr[T] =
    Expr.Func(name, args, distinct, sortBy, withinGroup)

def createWindowFunc[T](
    name: String,
    args: List[Expr[?]]
): WindowFunc[T] =
    WindowFunc(name, args)

def createBinaryOperator[T](left: Expr[?], op: String, right: Expr[?]): Expr[T] =
    Expr.Binary(left, SqlBinaryOperator.Custom(op), right)

case class IntervalValue(n: Double, unit: SqlTimeUnit)

extension (n: Double)
    def year: IntervalValue = IntervalValue(n, SqlTimeUnit.Year)

    def month: IntervalValue = IntervalValue(n, SqlTimeUnit.Month)

    def week: IntervalValue = IntervalValue(n, SqlTimeUnit.Week)

    def day: IntervalValue = IntervalValue(n, SqlTimeUnit.Day)

    def hour: IntervalValue = IntervalValue(n, SqlTimeUnit.Hour)

    def minute: IntervalValue = IntervalValue(n, SqlTimeUnit.Minute)

    def second: IntervalValue = IntervalValue(n, SqlTimeUnit.Second)

def interval(value: IntervalValue): TimeInterval =
    TimeInterval(value.n, value.unit)

case class ExtractValue[T](unit: SqlTimeUnit, expr: Expr[?])

private enum TimeUnit(val unit: SqlTimeUnit):
    case Year extends TimeUnit(SqlTimeUnit.Year)
    case Month extends TimeUnit(SqlTimeUnit.Month)
    case Week extends TimeUnit(SqlTimeUnit.Week)
    case Day extends TimeUnit(SqlTimeUnit.Day)
    case Hour extends TimeUnit(SqlTimeUnit.Hour)
    case Minute extends TimeUnit(SqlTimeUnit.Minute)
    case Second extends TimeUnit(SqlTimeUnit.Second)

    infix def from[T](expr: Expr[T]): ExtractValue[T] =
        ExtractValue(unit, expr)

def year: TimeUnit = TimeUnit.Year

def month: TimeUnit = TimeUnit.Month

def week: TimeUnit = TimeUnit.Week

def day: TimeUnit = TimeUnit.Day

def hour: TimeUnit = TimeUnit.Hour

def minute: TimeUnit = TimeUnit.Minute

def second: TimeUnit = TimeUnit.Second

def extract[T: DateTime](
    value: ExtractValue[T]
): Expr[Option[BigDecimal]] =
    Expr.Extract(value.unit, value.expr)

extension [T](expr: Expr[T])
    infix def as[R](using cast: Cast[T, R]): Expr[Option[R]] =
        Expr.Cast(expr, cast.castType)

def currentRow: SqlWindowFrameOption = SqlWindowFrameOption.CurrentRow

def unboundedPreceding: SqlWindowFrameOption = SqlWindowFrameOption.UnboundedPreceding

def unboundedFollowing: SqlWindowFrameOption = SqlWindowFrameOption.UnboundedFollowing

extension (n: Int)
    def preceding: SqlWindowFrameOption = SqlWindowFrameOption.Preceding(n)

    def following: SqlWindowFrameOption = SqlWindowFrameOption.Following(n)

def partitionBy(partitionValue: Expr[?]*): OverValue =
    OverValue(partitionBy = partitionValue.toList)

def sortBy(sortValue: SortBy[?]*): OverValue =
    OverValue(sortBy = sortValue.toList)

def queryContext[T](v: QueryContext ?=> T): T =
    given QueryContext = QueryContext(-1)
    v

inline def query[T](using
    qc: QueryContext = QueryContext(-1),
    p: Mirror.ProductOf[T],
    s: SelectItem[Table[T]]
): SelectQuery[Table[T]] =
    AsSqlExpr.summonInstances[p.MirroredElemTypes]
    val tableName = TableMacro.tableName[T]
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val table = Table[T](tableName, aliasName, TableMacro.tableMetaData[T])
    val ast = SqlQuery.Select(
        select = s.selectItems(table, 0),
        from = SqlTable.IdentTable(tableName, Some(SqlTableAlias(aliasName))) :: Nil
    )
    SelectQuery(table, ast)

inline def subquery[N <: Tuple, V <: Tuple, S <: ResultSize](
    q: QueryContext ?=> Query[NamedTuple[N, V], S]
)(using
    qc: QueryContext = QueryContext(-1),
    s: SelectItem[SubQuery[N, V]],
    sq: SelectItem[V]
): SelectQuery[SubQuery[N, V]] =
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val query = q
    val innerQuery = SubQuery[N, V](aliasName, sq.offset(query.queryItems))
    val ast = SqlQuery.Select(
        select = s.selectItems(innerQuery, 0),
        from = SqlTable.SubQueryTable(query.ast, false, SqlTableAlias(aliasName, Nil)) :: Nil
    )
    SelectQuery(innerQuery, ast)

inline def values[T <: Product](list: List[T])(using
    qc: QueryContext = QueryContext(-1),
    p: Mirror.ProductOf[T],
    s: SelectItem[Table[T]]
): SelectQuery[Table[T]] =
    val instances = AsSqlExpr.summonInstances[p.MirroredElemTypes]
    val tableName = TableMacro.tableName[T]
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val table = Table[T](tableName, aliasName, TableMacro.tableMetaData[T])
    val selectItems: List[SqlSelectItem.Item] = s.selectItems(table, 0).map: i =>
        SqlSelectItem.Item(SqlExpr.Column(Some(aliasName), i.alias.get), i.alias)
    val exprList = list.map: datum =>
        instances.zip(datum.productIterator).map: (i, v) =>
            i.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(v)
    val sqlValues = SqlQuery.Values(exprList)
    val tableAlias = SqlTableAlias(aliasName, selectItems.map(_.alias.get))
    val ast = SqlQuery.Select(
        select = selectItems,
        from = SqlTable.SubQueryTable(sqlValues, false, tableAlias) :: Nil
    )
    val newTable = Table[T](
        aliasName,
        aliasName,
        TableMacro.tableMetaData[T].copy(columnNames = selectItems.map(_.alias.get))
    )
    SelectQuery(newTable, ast)

inline def withRecursive[N <: Tuple, WN <: Tuple, V <: Tuple](
    query: Query[NamedTuple[N, V], ?]
)(f: Query[NamedTuple[N, V], ?] => Query[NamedTuple[WN, V], ?])(using
    sq: SelectItem[SubQuery[N, V]],
    s: SelectItem[V],
    qc: QueryContext
): WithRecursive[NamedTuple[N, V]] =
    WithRecursive(query)(f)

inline def insert[T <: Product]: Insert[Table[T], InsertNew] = Insert[T]

inline def insert[T <: Product](entity: T)(using Mirror.ProductOf[T]): Insert[Table[T], InsertEntity] =
    Insert[T](entity :: Nil)

inline def insert[T <: Product](entities: List[T])(using Mirror.ProductOf[T]): Insert[Table[T], InsertEntity] =
    Insert[T](entities)

inline def update[T <: Product]: Update[Table[T], UpdateTable] = Update[T]

inline def update[T <: Product](entity: T, skipNone: Boolean = false)(using
    Mirror.ProductOf[T]
): Update[Table[T], UpdateEntity] =
    Update[T](entity, skipNone)

inline def delete[T <: Product]: Delete[Table[T]] = Delete[T]

inline def save[T <: Product](entity: T)(using Mirror.ProductOf[T]): Save = Save[T](entity)