package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.statement.SqlQuery
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.metadata.{AsSqlExpr, DateTime, Interval, TableMacro}
import sqala.static.dsl.statement.dml.*
import sqala.static.dsl.statement.query.*

import java.time.{LocalDate, LocalDateTime}
import scala.NamedTuple.NamedTuple
import scala.compiletime.ops.boolean.||
import scala.deriving.Mirror

inline def query[T](inline q: QueryContext ?=> T): T =
    given QueryContext = QueryContext(0)
    q

inline def from[T](using 
    s: AsSelect[Table[T]],
    c: QueryContext
): TableQuery[T] =
    val tableName = TableMacro.tableName[T]
    c.tableIndex += 1
    val aliasName = s"t${c.tableIndex}"
    val metaData = TableMacro.tableMetaData[T]
    val table = Table[T](tableName, aliasName, metaData)
    val tree = SqlQuery.Select(
        select = s.selectItems(table, 1),
        from = SqlTable.Range(tableName, Some(SqlTableAlias(aliasName))) :: Nil
    )
    TableQuery(table, tree)

inline def from[N <: Tuple, V <: Tuple](query: Query[NamedTuple[N, V]])(using
    c: QueryContext,
    s: AsSelect[SubQuery[N, V]],
    sq: AsSubQuery[V]
): JoinQuery[SubQuery[N, V]] =
    val innerQuery = SubQuery[N, V](query.params)
    val tree = SqlQuery.Select(
        select = s.selectItems(innerQuery, 1),
        from = SqlTable.SubQuery(query.tree, false, Some(SqlTableAlias(innerQuery.__alias__, Nil))) :: Nil
    )
    JoinQuery(innerQuery, tree)

inline def from[T <: Product](data: Seq[T])(using
    c: QueryContext,
    p: Mirror.ProductOf[T],
    s: AsSelect[Table[T]]
): TableQuery[T] =
    val instances = AsSqlExpr.summonInstances[p.MirroredElemTypes]
    val tableName = TableMacro.tableName[T]
    val metaData = TableMacro.tableMetaData[T]
    c.tableIndex += 1
    val aliasName = s"t${c.tableIndex}"
    val table = Table[T](tableName, aliasName, metaData)
    val selectItems = s.selectItems(table, 1)
    val exprList = data.toList.map: datum =>
        instances.zip(datum.productIterator).map: (i, v) =>
            i.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(v)
    val sqlValues = SqlQuery.Values(exprList)
    val tableAlias = SqlTableAlias(aliasName, metaData.columnNames)
    val tree = SqlQuery.Select(
        select = selectItems,
        from = SqlTable.SubQuery(sqlValues, false, Some(tableAlias)) :: Nil
    )
    val newTable = Table[T](
        aliasName,
        aliasName,
        metaData
    )
    TableQuery(newTable, tree)

inline def insert[T <: Product, I: AsExpr as a](f: Table[T] => I): Insert[a.R, InsertTable] =
    Insert[T, I](f)

inline def insert[T <: Product](entity: T)(using 
    Mirror.ProductOf[T]
): Insert[T, InsertEntity] =
    Insert[T](entity :: Nil)

inline def insert[T <: Product](entities: List[T])(using 
    Mirror.ProductOf[T]
): Insert[T, InsertEntity] =
    Insert[T](entities)

inline def update[T <: Product]: Update[T, UpdateTable] = 
    Update[T]

inline def update[T <: Product](entity: T, skipNone: Boolean = false)(using
    Mirror.ProductOf[T]
): Update[T, UpdateEntity] =
    Update[T](entity, skipNone)

inline def delete[T <: Product]: Delete[T] = 
    Delete[T]

inline def save[T <: Product](entity: T)(using Mirror.ProductOf[T]): Save = 
    Save[T](entity)

extension [T: AsExpr as a](x: T)
    def asExpr: Expr[a.R] = a.asExpr(x)

def timestamp(s: String)(using QueryContext): Expr[LocalDateTime] =
    Expr(SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Timestamp, s))

def date(s: String)(using QueryContext): Expr[LocalDate] =
    Expr(SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Date, s))

private[sqala] val tableCte = "__cte__"

private[sqala] val columnPseudoLevel = "__pseudo__level__"

def prior[T](expr: Expr[T])(using QueryContext): Expr[T] =
    expr match
        case Expr(SqlExpr.Column(_, n)) => 
            Expr(SqlExpr.Column(Some(tableCte), n))
        case _ => throw MatchError(expr)

def level()(using QueryContext): Expr[Int] =
    Expr(SqlExpr.Column(Some(tableCte), columnPseudoLevel))

class EmptyIf(private[sqala] val exprs: List[Expr[?]]):
    infix def `then`[E: AsExpr as a](expr: E)(using QueryContext): IfThen[a.R] =
        IfThen(exprs :+ a.asExpr(expr))

class If[T](private[sqala] val exprs: List[Expr[?]]):
    infix def `then`[R: AsExpr as a](expr: R)(using
        o: Return[Unwrap[T, Option], Unwrap[a.R, Option], IsOption[T] || IsOption[R]],
        c: QueryContext
    ): IfThen[o.R] =
        IfThen(exprs :+ a.asExpr(expr))

class IfThen[T](private[sqala] val exprs: List[Expr[?]]):
    infix def `else`[R: AsExpr as a](expr: R)(using
        o: Return[Unwrap[T, Option], Unwrap[a.R, Option], IsOption[T] || IsOption[R]],
        c: QueryContext
    ): Expr[o.R] =
        val caseBranches =
            exprs.grouped(2).toList.map(i => (i(0), i(1)))
        Expr(
            SqlExpr.Case(
                caseBranches.map((i, t) => SqlCase(i.asSqlExpr, t.asSqlExpr)), 
                a.asExpr(expr).asSqlExpr
            )
        )

    infix def `else if`[E: AsExpr as a](expr: E)(using 
        a.R <:< (Boolean | Option[Boolean]),
        QueryContext
    ): If[T] =
        If(exprs :+ a.asExpr(expr))

def `if`[E: AsExpr as a](expr: E)(using 
    a.R <:< (Boolean | Option[Boolean]),
    QueryContext
): EmptyIf = 
    EmptyIf(a.asExpr(expr) :: Nil)

def exists[T](query: Query[T])(using QueryContext): Expr[Option[Boolean]] =
    Expr(SqlExpr.SubLink(query.tree, SqlSubLinkType.Exists))

def any[T](query: Query[T])(using
    a: AsExpr[T],
    c: QueryContext
): SubLink[a.R] =
    SubLink(query.tree, SqlSubLinkType.Any)

def all[T](query: Query[T])(using
    a: AsExpr[T],
    c: QueryContext
): SubLink[a.R] =
    SubLink(query.tree, SqlSubLinkType.All)

case class IntervalValue(n: Double, unit: SqlTimeUnit)

extension (n: Double)
    def year(using QueryContext): IntervalValue = 
        IntervalValue(n, SqlTimeUnit.Year)

    def month(using QueryContext): IntervalValue = 
        IntervalValue(n, SqlTimeUnit.Month)

    def week(using QueryContext): IntervalValue = 
        IntervalValue(n, SqlTimeUnit.Week)

    def day(using QueryContext): IntervalValue = 
        IntervalValue(n, SqlTimeUnit.Day)

    def hour(using QueryContext): IntervalValue = 
        IntervalValue(n, SqlTimeUnit.Hour)

    def minute(using QueryContext): IntervalValue = 
        IntervalValue(n, SqlTimeUnit.Minute)

    def second(using QueryContext): IntervalValue = 
        IntervalValue(n, SqlTimeUnit.Second)

def interval(value: IntervalValue)(using QueryContext): TimeInterval =
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

    infix def from[T: AsExpr as a](expr: T)(using QueryContext): ExtractValue[a.R] =
        ExtractValue(unit, a.asExpr(expr))

def year(using QueryContext): TimeUnit = 
    TimeUnit.Year

def month(using QueryContext): TimeUnit = 
    TimeUnit.Month

def week(using QueryContext): TimeUnit = 
    TimeUnit.Week

def day(using QueryContext): TimeUnit = 
    TimeUnit.Day

def hour(using QueryContext): TimeUnit = 
    TimeUnit.Hour

def minute(using QueryContext): TimeUnit = 
    TimeUnit.Minute

def second(using QueryContext): TimeUnit = 
    TimeUnit.Second

def extract[T: DateTime](
    value: ExtractValue[T]
)(using QueryContext): Expr[Option[BigDecimal]] =
    Expr(SqlExpr.Extract(value.expr.asSqlExpr, value.unit))

def extract[T <: Interval | Option[Interval]](
    value: ExtractValue[T]
)(using QueryContext): Expr[Option[BigDecimal]] =
    Expr(SqlExpr.Extract(value.expr.asSqlExpr, value.unit))

extension [T: AsExpr as a](expr: T)
    def as[R](using cast: Cast[a.R, R], c: QueryContext): Expr[Option[R]] =
        Expr(SqlExpr.Cast(a.asExpr(expr).asSqlExpr, cast.castType))

def currentRow(using QueryContext): SqlWindowFrameOption = 
    SqlWindowFrameOption.CurrentRow

def unboundedPreceding(using QueryContext): SqlWindowFrameOption = 
    SqlWindowFrameOption.UnboundedPreceding

def unboundedFollowing(using QueryContext): SqlWindowFrameOption = 
    SqlWindowFrameOption.UnboundedFollowing

extension (n: Int)
    def preceding(using QueryContext): SqlWindowFrameOption = 
        SqlWindowFrameOption.Preceding(n)

    def following(using QueryContext): SqlWindowFrameOption = 
        SqlWindowFrameOption.Following(n)

def partitionBy[T: AsGroup as a](partitionValue: T)(using QueryContext): Over =
    Over(partitionBy = a.exprs(partitionValue))

def sortBy[T: AsSort as a](sortValue: T)(using QueryContext): Over =
    Over(sortBy = a.asSort(sortValue))

def orderBy[T: AsSort as a](sortValue: T)(using QueryContext): Over =
    Over(sortBy = a.asSort(sortValue))