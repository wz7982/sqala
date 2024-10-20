package sqala.dsl

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.ast.table.*
import sqala.dsl.macros.TableMacro
import sqala.dsl.statement.dml.*
import sqala.dsl.statement.query.*

import java.util.Date
import scala.NamedTuple.NamedTuple
import scala.annotation.targetName
import scala.compiletime.ops.boolean.&&
import scala.compiletime.ops.double.{<=, >=}
import scala.compiletime.ops.int.>
import scala.compiletime.{erasedValue, error}
import scala.deriving.Mirror

extension [T: AsSqlExpr](value: T)
    def asExpr: Expr[T, ValueKind] = Expr.Literal(value, summon[AsSqlExpr[T]])

extension [T](x: T)(using m: Merge[T])
    def merge: Expr[m.R, m.K] = m.asExpr(x)

enum CaseState:
    case Init
    case When

type CaseInit = CaseState.Init.type

type CaseWhen = CaseState.When.type

class Case[T, K <: ExprKind, S <: CaseState](val exprs: List[Expr[?, ?]]):
    infix def when[WK <: ExprKind](expr: Expr[Boolean, WK])(using S =:= CaseInit, KindOperation[K, WK]): Case[T, ResultKind[K, WK], CaseWhen] =
        new Case(exprs :+ expr)

    infix def `then`[E, TK <: ExprKind](expr: Expr[E, TK])(using p: S =:= CaseWhen, o: ResultOperation[T, E], k: KindOperation[K, TK]): Case[o.R, ResultKind[K, TK], CaseInit] =
        new Case(exprs :+ expr)

    infix def `then`[E](value: E)(using p: S =:= CaseWhen, a: AsSqlExpr[E], o: ResultOperation[T, E]): Case[o.R, ResultKind[K, ValueKind], CaseInit] =
        new Case(exprs :+ Expr.Literal(value, a))

    infix def `else`[E, TK <: ExprKind](expr: Expr[E, TK])(using p: S =:= CaseInit, o: ResultOperation[T, E], k: KindOperation[K, TK]): Expr[o.R, ResultKind[K, ValueKind]] =
        val caseBranches =
            exprs.grouped(2).toList.map(i => (i.head, i(1)))
        Expr.Case(caseBranches, expr)

    infix def `else`[E](value: E)(using p: S =:= CaseInit, a: AsSqlExpr[E], o: ResultOperation[T, E]): Expr[o.R, ResultKind[K, ValueKind]] =
        val caseBranches =
            exprs.grouped(2).toList.map(i => (i.head, i(1)))
        Expr.Case(caseBranches, Expr.Literal(value, a))

def `case`: Case[Nothing, ValueKind, CaseInit] = new Case(Nil)

def exists[T, S <: ResultSize](query: Query[T, S]): Expr[Boolean, CommonKind] =
    Expr.SubLink(query.ast, SqlSubLinkType.Exists)

def notExists[T, S <: ResultSize](query: Query[T, S]): Expr[Boolean, CommonKind] =
    Expr.SubLink(query.ast, SqlSubLinkType.NotExists)

def all[Q, S <: ResultSize](query: Query[Q, S])(using m: Merge[Q]): SubLinkItem[m.R] =
    SubLinkItem(query.ast, SqlSubLinkType.All)

def any[Q, S <: ResultSize](query: Query[Q, S])(using m: Merge[Q]): SubLinkItem[m.R] =
    SubLinkItem(query.ast, SqlSubLinkType.Any)

def some[Q, S <: ResultSize](query: Query[Q, S])(using m: Merge[Q]): SubLinkItem[m.R] =
    SubLinkItem(query.ast, SqlSubLinkType.Some)

private inline def aggregate[T, K <: ExprKind](name: String, expr: Expr[?, K], distinct: Boolean = false): Expr[T, AggKind] =
    inline erasedValue[K] match
        case _: AggKind =>
            error("Aggregate function calls cannot be nested.")
        case _: AggOperationKind =>
            error("Aggregate function calls cannot be nested.")
        case _: WindowKind =>
            error("Aggregate function calls cannot contain window function calls.")
        case _ => Expr.Func(name, expr :: Nil, distinct)

def count(): Expr[Long, AggKind] = Expr.Func("COUNT", Nil)

inline def count[T, K <: ExprKind](expr: Expr[T, K]): Expr[Long, AggKind] =
    aggregate("COUNT", expr)

inline def countDistinct[T, K <: ExprKind](expr: Expr[T, K]): Expr[Long, AggKind] =
    aggregate("COUNT", expr, true)

inline def sum[T: Number, K <: ExprKind](expr: Expr[T, K]): Expr[Option[BigDecimal], AggKind] =
    aggregate("SUM", expr)

inline def avg[T: Number, K <: ExprKind](expr: Expr[T, K]): Expr[Option[BigDecimal], AggKind] =
    aggregate("AVG", expr)

inline def max[T, K <: ExprKind](expr: Expr[T, K]): Expr[Wrap[T, Option], AggKind] =
    aggregate("MAX", expr)

inline def min[T, K <: ExprKind](expr: Expr[T, K]): Expr[Wrap[T, Option], AggKind] =
    aggregate("MIN", expr)

inline def percentileCont[N: Number, K <: ExprKind](n: Double, withinGroup: OrderBy[N, K]): Expr[Option[BigDecimal], AggKind] =
    inline erasedValue[K] match
        case _: AggKind =>
            error("Aggregate function calls cannot be nested.")
        case _: AggOperationKind =>
            error("Aggregate function calls cannot be nested.")
        case _: WindowKind =>
            error("Aggregate function calls cannot contain window function calls.")
        case _ => inline erasedValue[n.type >= 0.0 && n.type <= 1.0] match
            case _: false => error("The percentage value is not between 0 and 1.")
            case _ =>
                Expr.Func("PERCENTILE_CONT", n.asExpr :: Nil, withinGroup = withinGroup :: Nil)

inline def percentileDisc[N: Number, K <: ExprKind](n: Double, withinGroup: OrderBy[N, K]): Expr[Option[BigDecimal], AggKind] =
    inline erasedValue[K] match
        case _: AggKind =>
            error("Aggregate function calls cannot be nested.")
        case _: AggOperationKind =>
            error("Aggregate function calls cannot be nested.")
        case _: WindowKind =>
            error("Aggregate function calls cannot contain window function calls.")
        case _ => inline erasedValue[n.type >= 0.0 && n.type <= 1.0] match
            case _: false => error("The percentage value is not between 0 and 1.")
            case _ =>
                Expr.Func("PERCENTILE_DISC", n.asExpr :: Nil, withinGroup = withinGroup :: Nil)

def rank(): WindowFunc[Option[Long]] = WindowFunc("RANK", Nil)

def denseRank(): WindowFunc[Option[Long]] = WindowFunc("DENSE_RANK", Nil)

def rowNumber(): WindowFunc[Option[Long]] = WindowFunc("ROW_NUMBER", Nil)

inline def lag[T, K <: ExprKind](expr: Expr[T, K], offset: Int = 1, default: Option[Unwrap[T, Option]] = None)(using a: AsSqlExpr[Option[Unwrap[T, Option]]]): WindowFunc[Wrap[T, Option]] =
    inline erasedValue[K] match
        case _: AggKind =>
            error("Aggregate function calls cannot be nested.")
        case _: AggOperationKind =>
            error("Aggregate function calls cannot be nested.")
        case _: WindowKind =>
            error("Aggregate function calls cannot contain window function calls.")
        case _ =>
            val defaultExpr = Expr.Literal(default, a)
            WindowFunc("LAG", expr :: Expr.Literal(offset, summon[AsSqlExpr[Int]]) :: defaultExpr :: Nil)

inline def lead[T, K <: ExprKind](expr: Expr[T, K], offset: Int = 1, default: Option[Unwrap[T, Option]] = None)(using a: AsSqlExpr[Option[Unwrap[T, Option]]]): WindowFunc[Wrap[T, Option]] =
    inline erasedValue[K] match
        case _: AggKind =>
            error("Aggregate function calls cannot be nested.")
        case _: AggOperationKind =>
            error("Aggregate function calls cannot be nested.")
        case _: WindowKind =>
            error("Aggregate function calls cannot contain window function calls.")
        case _ =>
            val defaultExpr = Expr.Literal(default, a)
            WindowFunc("LEAD", expr :: Expr.Literal(offset, summon[AsSqlExpr[Int]]) :: defaultExpr :: Nil)

inline def ntile(n: Int): WindowFunc[Int] =
    inline erasedValue[n.type > 0] match
        case _: false =>
            error("The parameter of NTILE must be greater than 0.")
        case _ =>
            WindowFunc("NTILE", n.asExpr :: Nil)

inline def firstValue[T, K <: ExprKind](expr: Expr[T, K]): WindowFunc[Wrap[T, Option]] =
    inline erasedValue[K] match
        case _: AggKind =>
            error("Aggregate function calls cannot be nested.")
        case _: AggOperationKind =>
            error("Aggregate function calls cannot be nested.")
        case _: WindowKind =>
            error("Aggregate function calls cannot contain window function calls.")
        case _ => WindowFunc("FIRST_VALUE", expr :: Nil)

inline def lastValue[T, K <: ExprKind](expr: Expr[T, K]): WindowFunc[Wrap[T, Option]] =
    inline erasedValue[K] match
        case _: AggKind =>
            error("Aggregate function calls cannot be nested.")
        case _: AggOperationKind =>
            error("Aggregate function calls cannot be nested.")
        case _: WindowKind =>
            error("Aggregate function calls cannot contain window function calls.")
        case _ => WindowFunc("LAST_VALUE", expr :: Nil)

def coalesce[T, K <: ExprKind](expr: Expr[Option[T], K], value: T)(using a: AsSqlExpr[T]): Expr[T, ResultKind[K, ValueKind]] =
    Expr.Func("COALESCE", expr :: Expr.Literal(value, a) :: Nil)

def ifnull[T, K <: ExprKind](expr: Expr[Option[T], K], value: T)(using AsSqlExpr[T]): Expr[T, ResultKind[K, ValueKind]] =
    coalesce(expr, value)

def abs[T: Number, K <: ExprKind](expr: Expr[T, K]): Expr[T, ResultKind[K, ValueKind]] =
    Expr.Func("ABS", expr :: Nil)

def ceil[T: Number, K <: ExprKind](expr: Expr[T, K]): Expr[Option[Long], ResultKind[K, ValueKind]] =
    Expr.Func("CEIL", expr :: Nil)

def floor[T: Number, K <: ExprKind](expr: Expr[T, K]): Expr[Option[Long], ResultKind[K, ValueKind]] =
    Expr.Func("FLOOR", expr :: Nil)

def round[T: Number, K <: ExprKind](expr: Expr[T, K], n: Int): Expr[Option[BigDecimal], ResultKind[K, ValueKind]] =
    Expr.Func("ROUND", expr :: n.asExpr :: Nil)

def power[T: Number, K <: ExprKind](expr: Expr[T, K], n: Double): Expr[Option[BigDecimal], ResultKind[K, ValueKind]] =
    Expr.Func("POWER", expr :: n.asExpr :: Nil)

@targetName("concatAgg")
def concat(expr: (Expr[String, AggKind] | Expr[Option[String], AggKind] | String)*): Expr[Option[String], AggOperationKind] =
    val args = expr.toList.map:
        case s: String => s.asExpr
        case e: Expr[?, ?] => e
    Expr.Func("CONCAT", args)

def concat(expr: (Expr[String, ?] | Expr[Option[String], ?] | String)*): Expr[Option[String], CommonKind] =
    val args = expr.toList.map:
        case s: String => s.asExpr
        case e: Expr[?, ?] => e
    Expr.Func("CONCAT", args)

def substring[T <: String | Option[String], K <: ExprKind](expr: Expr[T, K], start: Int, end: Int): Expr[Option[String], ResultKind[K, ValueKind]] =
    Expr.Func("SUBSTRING", expr :: start.asExpr :: end.asExpr :: Nil)

def replace[T <: String | Option[String], K <: ExprKind](expr: Expr[T, K], oldString: String, newString: String): Expr[T, ResultKind[K, ValueKind]] =
    Expr.Func("REPLACE", expr :: oldString.asExpr :: newString.asExpr :: Nil)

def length[T <: String | Option[String], K <: ExprKind](expr: Expr[T, K]): Expr[Option[Long], ResultKind[K, ValueKind]] =
    Expr.Func("LENGTH", expr :: Nil)

def repeat[T <: String | Option[String], K <: ExprKind](expr: Expr[T, K], n: Int): Expr[T, ResultKind[K, ValueKind]] =
    Expr.Func("REPEAT", expr :: n.asExpr :: Nil)

def trim[T <: String | Option[String], K <: ExprKind](expr: Expr[T, K]): Expr[T, ResultKind[K, ValueKind]] =
    Expr.Func("TRIM", expr :: Nil)

def upper[T <: String | Option[String], K <: ExprKind](expr: Expr[T, K]): Expr[T, ResultKind[K, ValueKind]] =
    Expr.Func("UPPER", expr :: Nil)

def lower[T <: String | Option[String], K <: ExprKind](expr: Expr[T, K]): Expr[T, ResultKind[K, ValueKind]] =
    Expr.Func("LOWER", expr :: Nil)

def now(): Expr[Option[Date], CommonKind] =
    Expr.Func("NOW", Nil)

inline def grouping[G](items: G): Expr[Int, AggOperationKind] =
    inline erasedValue[CheckGrouping[items.type]] match
        case _: false =>
            error("The parameter of GROUPING must be a grouping expression.")
        case _ =>
            val groupingItems = items match
                case t: Tuple => t.toList.map(_.asInstanceOf[Expr[?, ?]])
                case e: Expr[?, ?] => e :: Nil
            Expr.Grouping(groupingItems)

def createFunc[T](name: String, args: List[Expr[?, ?]]): Expr[T, CommonKind] =
    Expr.Func(name, args)

def createAggFunc[T](name: String, args: List[Expr[?, ?]], distinct: Boolean = false, orderBy: List[OrderBy[?, ?]] = Nil): Expr[T, AggKind] =
    Expr.Func(name, args, distinct, orderBy)

def createWindowFunc[T](name: String, args: List[Expr[?, ?]], distinct: Boolean = false, orderBy: List[OrderBy[?, ?]] = Nil): WindowFunc[T] =
    WindowFunc(name, args, distinct, orderBy)

def createBinaryOperator[T](left: Expr[?, ?], op: String, right: Expr[?, ?]): Expr[T, CommonKind] =
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

case class ExtractValue[T, K <: ExprKind](unit: SqlTimeUnit, expr: Expr[?, ?])

private enum TimeUnit(val unit: SqlTimeUnit):
    case Year extends TimeUnit(SqlTimeUnit.Year)
    case Month extends TimeUnit(SqlTimeUnit.Month)
    case Week extends TimeUnit(SqlTimeUnit.Week)
    case Day extends TimeUnit(SqlTimeUnit.Day)
    case Hour extends TimeUnit(SqlTimeUnit.Hour)
    case Minute extends TimeUnit(SqlTimeUnit.Minute)
    case Second extends TimeUnit(SqlTimeUnit.Second)

    infix def from[T, K <: ExprKind](expr: Expr[T, K]): ExtractValue[T, K] =
        ExtractValue(unit, expr)

def year: TimeUnit = TimeUnit.Year

def month: TimeUnit = TimeUnit.Month

def week: TimeUnit = TimeUnit.Week

def day: TimeUnit = TimeUnit.Day

def hour: TimeUnit = TimeUnit.Hour

def minute: TimeUnit = TimeUnit.Minute

def second: TimeUnit = TimeUnit.Second

def extract[T: DateTime, K <: ExprKind](value: ExtractValue[T, K]): Expr[Option[BigDecimal], ResultKind[K, ValueKind]] =
    Expr.Extract(value.unit, value.expr)

def cast[T](expr: Expr[?, ?], castType: String): Expr[Wrap[T, Option], CastKind[expr.type]] =
    Expr.Cast(expr, castType)

def currentRow: SqlWindowFrameOption = SqlWindowFrameOption.CurrentRow

def unboundedPreceding: SqlWindowFrameOption = SqlWindowFrameOption.UnboundedPreceding

def unboundedFollowing: SqlWindowFrameOption = SqlWindowFrameOption.UnboundedFollowing

extension (n: Int)
    def preceding: SqlWindowFrameOption = SqlWindowFrameOption.Preceding(n)

    def following: SqlWindowFrameOption = SqlWindowFrameOption.Following(n)

inline def partitionBy[P](partitionValue: P): OverValue =
    inline erasedValue[CheckOverPartition[P]] match
        case _: false =>
            error("The parameters for PARTITION BY cannot contain aggregate functions or window functions.")
        case _ =>
    val partition = partitionValue match
        case e: Expr[?, ?] => e :: Nil
        case t: Tuple => t.toList.map(_.asInstanceOf[Expr[?, ?]])
    OverValue(partitionBy = partition)

def queryContext[T](v: QueryContext ?=> T): T =
    given QueryContext = QueryContext(-1)
    v

inline def query[T](using qc: QueryContext = QueryContext(-1), p: Mirror.ProductOf[T], a: AsTable[T], s: SelectItem[Table[T]]): SelectQuery[a.R] =
    AsSqlExpr.summonInstances[p.MirroredElemTypes]
    val tableName = TableMacro.tableName[T]
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val table = Table[T](tableName, aliasName, TableMacro.tableMetaData[T])
    val ast = SqlQuery.Select(select = s.selectItems(table, 0), from = SqlTable.IdentTable(tableName, Some(SqlTableAlias(aliasName))) :: Nil)
    SelectQuery(table.asInstanceOf[a.R], ast)

inline def query[N <: Tuple, V <: Tuple, S <: ResultSize](q: Query[NamedTuple[N, V], S])(using t: TransformKind[V, ColumnKind])(using s: SelectItem[SubQuery[N, ToTuple[t.R]]], sq: SelectItem[NamedTuple[N, V]]): SelectQuery[SubQuery[N, ToTuple[t.R]]] =
    given qc: QueryContext = q.qc
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val innerQuery = SubQuery[N, ToTuple[t.R]](aliasName, sq.offset(q.queryItems))
    val ast = SqlQuery.Select(
        select = s.selectItems(innerQuery, 0),
        from = SqlTable.SubQueryTable(q.ast, false, SqlTableAlias(aliasName, Nil)) :: Nil
    )
    SelectQuery(innerQuery, ast)

inline def values[T <: Product](list: List[T])(using qc: QueryContext = QueryContext(-1), p: Mirror.ProductOf[T], a: AsTable[T], s: SelectItem[Table[T]]): SelectQuery[a.R] =
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
    val ast = SqlQuery.Select(select = selectItems, from = SqlTable.SubQueryTable(sqlValues, false, tableAlias) :: Nil)
    val newTable = Table[T](aliasName, aliasName, TableMacro.tableMetaData[T].copy(columnNames = selectItems.map(_.alias.get)))
    SelectQuery(newTable.asInstanceOf[a.R], ast)

def withRecursive[N <: Tuple, WN <: Tuple, V <: Tuple](query: Query[NamedTuple[N, V], ?])(f: Query[NamedTuple[N, V], ?] => Query[NamedTuple[WN, V], ?])(using sq: SelectItem[SubQuery[N, V]], s: SelectItem[V], qc: QueryContext): WithRecursive[NamedTuple[N, V]] =
    WithRecursive(query)(f)

inline def insert[T <: Product]: Insert[Table[T], InsertNew] = Insert[T]

inline def insert[T <: Product](entity: T)(using Mirror.ProductOf[T]): Insert[Table[T], InsertEntity] =
    Insert[T](entity :: Nil)

inline def insert[T <: Product](entities: List[T])(using Mirror.ProductOf[T]): Insert[Table[T], InsertEntity] =
    Insert[T](entities)

inline def update[T <: Product]: Update[Table[T], UpdateTable] = Update[T]

inline def update[T <: Product](entity: T, skipNone: Boolean = false)(using Mirror.ProductOf[T]): Update[Table[T], UpdateEntity] =
    Update[T](entity, skipNone)

inline def delete[T <: Product]: Delete[Table[T]] = Delete[T]

inline def save[T <: Product](entity: T)(using Mirror.ProductOf[T]): Save = Save[T](entity)