package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.ast.table.*
import sqala.common.*
import sqala.macros.*
import sqala.static.statement.query.*
import sqala.static.statement.dml.*

import java.time.*
import scala.NamedTuple.NamedTuple
import scala.compiletime.ops.boolean.*
import scala.deriving.Mirror

extension [T](exprs: T)(using a: AsExpr[T])
    def asExpr: Expr[a.R] = a.asExpr(exprs)

given asExpr[T: AsExpr as a]: Conversion[T, Expr[a.R]] =
    a.asExpr(_)

def timestamp(s: String): Expr[LocalDateTime] =
    Expr.Ref(SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Timestamp, s))

def date(s: String): Expr[LocalDate] =
    Expr.Ref(SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Date, s))

extension [T: AsSqlExpr](expr: Expr[T])
    def within[N <: Tuple, V <: Tuple : AsExpr](pivot: NamedTuple[N, V])(using CanEqual[T, V]): PivotPair[T, N, V] =
        PivotPair(expr, pivot)

private[sqala] val tableCte = "__cte__"

private[sqala] val columnPseudoLevel = "__pseudo__level__"

def prior[T](expr: Expr[T]): Expr[T] =
    expr match
        case Expr.Column(_, n) => Expr.Column(tableCte, n)
        case _ => throw MatchError(expr)

def level(): Expr[Int] =
    Expr.Column(tableCte, columnPseudoLevel)

class EmptyIf(private[sqala] val exprs: List[Expr[?]]):
    infix def `then`[E : AsExpr as a](expr: E): IfThen[a.R] =
        IfThen(exprs :+ a.asExpr(expr))

class If[T](private[sqala] val exprs: List[Expr[?]]):
    infix def `then`[R : AsExpr as a](expr: R)(using
        o: ResultOperation[Unwrap[T, Option], Unwrap[a.R, Option], IsOption[T] || IsOption[R]]
    ): IfThen[o.R] =
        IfThen(exprs :+ a.asExpr(expr))

class IfThen[T](private[sqala] val exprs: List[Expr[?]]):
    infix def `else`[R : AsExpr as a](expr: R)(using
        o: ResultOperation[Unwrap[T, Option], Unwrap[a.R, Option], IsOption[T] || IsOption[R]]
    ): Expr[o.R] =
        val caseBranches =
            exprs.grouped(2).toList.map(i => (i(0), i(1)))
        Expr.Case(caseBranches, a.asExpr(expr))

    infix def `else if`(expr: Expr[Boolean]): If[T] =
        If(exprs :+ expr)

def `if`(expr: Expr[Boolean]): EmptyIf = EmptyIf(expr :: Nil)

def exists[T](query: Query[T]): Expr[Boolean] =
    Expr.SubLink(query.ast, SqlSubLinkType.Exists)

def any[T](query: Query[T])(using
    a: AsExpr[T]
): SubLinkItem[a.R] =
    SubLinkItem(query.ast, SqlSubLinkType.Any)

def all[T](query: Query[T])(using
    a: AsExpr[T]
): SubLinkItem[a.R] =
    SubLinkItem(query.ast, SqlSubLinkType.All)

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

def extract[T <: Interval | Option[Interval]](
    value: ExtractValue[T]
): Expr[Option[BigDecimal]] =
    Expr.Extract(value.unit, value.expr)

extension [T](expr: Expr[T])
    def as[R](using cast: Cast[T, R]): Expr[Option[R]] =
        Expr.Cast(expr, cast.castType)

def currentRow: SqlWindowFrameOption = SqlWindowFrameOption.CurrentRow

def unboundedPreceding: SqlWindowFrameOption = SqlWindowFrameOption.UnboundedPreceding

def unboundedFollowing: SqlWindowFrameOption = SqlWindowFrameOption.UnboundedFollowing

extension (n: Int)
    def preceding: SqlWindowFrameOption = SqlWindowFrameOption.Preceding(n)

    def following: SqlWindowFrameOption = SqlWindowFrameOption.Following(n)

def partitionBy(partitionValue: Expr[?]*): OverValue =
    OverValue(partitionBy = partitionValue.toList)

def sortBy(sortValue: Sort[?]*): OverValue =
    OverValue(sortBy = sortValue.toList)

def orderBy(sortValue: Sort[?]*): OverValue =
    OverValue(sortBy = sortValue.toList)

def queryContext[T](v: QueryContext ?=> T): T =
    given QueryContext = QueryContext(0)
    v

inline def analysisContext[T](inline v: QueryContext ?=> T): T =
    given QueryContext = QueryContext(0)
    AnalysisClauseMacro.analysis(v)
    v

inline def from[T](using
    qc: QueryContext = QueryContext(0),
    p: Mirror.ProductOf[T],
    s: AsSelect[Table[T]]
): TableQuery[T] =
    AsSqlExpr.summonInstances[p.MirroredElemTypes]
    val tableName = TableMacro.tableName[T]
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val table = Table[T](tableName, aliasName, TableMacro.tableMetaData[T])
    val ast = SqlQuery.Select(
        select = s.selectItems(table, 1),
        from = SqlTable.Range(tableName, Some(SqlTableAlias(aliasName))) :: Nil
    )
    TableQuery(table, ast)

inline def fromQuery[N <: Tuple, V <: Tuple](query: QueryContext ?=> Query[NamedTuple[N, V]])(using
    qc: QueryContext = QueryContext(0),
    sq: AsSubQuery[V],
    s: AsSelect[SubQuery[N, V]]
): SelectQuery[SubQuery[N, V]] =
    val q = query
    val innerQuery = SubQuery[N, V](q.queryParam)
    val ast = SqlQuery.Select(
        select = s.selectItems(innerQuery, 1),
        from = SqlTable.SubQuery(q.ast, false, Some(SqlTableAlias(innerQuery.__alias__, Nil))) :: Nil
    )
    SelectQuery(innerQuery, ast)

inline def fromValues[T <: Product](data: Seq[T])(using
    qc: QueryContext = QueryContext(0),
    p: Mirror.ProductOf[T],
    s: AsSelect[Table[T]]
): TableQuery[T] =
    val instances = AsSqlExpr.summonInstances[p.MirroredElemTypes]
    val tableName = TableMacro.tableName[T]
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val table = Table[T](tableName, aliasName, TableMacro.tableMetaData[T])
    val selectItems: List[SqlSelectItem.Item] = s.selectItems(table, 1).map: i =>
        SqlSelectItem.Item(SqlExpr.Column(Some(aliasName), i.alias.get), i.alias)
    val exprList = data.toList.map: datum =>
        instances.zip(datum.productIterator).map: (i, v) =>
            i.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(v)
    val sqlValues = SqlQuery.Values(exprList)
    val tableAlias = SqlTableAlias(aliasName, selectItems.map(_.alias.get))
    val ast = SqlQuery.Select(
        select = selectItems,
        from = SqlTable.SubQuery(sqlValues, false, Some(tableAlias)) :: Nil
    )
    val newTable = Table[T](
        aliasName,
        aliasName,
        TableMacro.tableMetaData[T].copy(columnNames = selectItems.map(_.alias.get))
    )
    TableQuery(newTable, ast)

inline def fromFunction[T](function: FunctionTable[T])(using
    qc: QueryContext = QueryContext(0),
    p: Mirror.ProductOf[T],
    s: AsSelect[Table[T]]
): TableQuery[T] =
    AsSqlExpr.summonInstances[p.MirroredElemTypes]
    val tableName = TableMacro.tableName[T]
    val metaData = TableMacro.tableMetaData[T]
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val table = Table[T](tableName, aliasName, metaData)
    val ast = SqlQuery.Select(
        select = s.selectItems(table, 1),
        from = SqlTable.Func(
            tableName,
            function.args.map(_.asSqlExpr),
            Some(SqlTableAlias(aliasName, metaData.columnNames))
        ) :: Nil
    )
    TableQuery(table, ast)

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

def withRecursive[N <: Tuple, WN <: Tuple, V <: Tuple](
    query: Query[NamedTuple[N, V]]
)(f: Query[NamedTuple[N, V]] => Query[NamedTuple[WN, V]])(using
    AsSelect[SubQuery[N, V]],
    AsSubQuery[V]
): Query[NamedTuple[N, V]] =
    WithRecursive(query)(f)