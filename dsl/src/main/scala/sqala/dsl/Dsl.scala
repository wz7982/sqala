package sqala.dsl

import sqala.ast.expr.*
import sqala.ast.statement.SqlQuery
import sqala.ast.table.*
import sqala.dsl.macros.TableMacro
import sqala.dsl.statement.dml.*
import sqala.dsl.statement.query.*

import scala.NamedTuple.NamedTuple
import scala.annotation.targetName
import scala.compiletime.ops.boolean.&&
import scala.compiletime.ops.double.{>=, <=}
import scala.deriving.Mirror
import java.util.Date

extension [T: AsSqlExpr](value: T)
    def asExpr: Expr[T, ValueKind] = Expr.Literal(value, summon[AsSqlExpr[T]])

enum CaseState:
    case Init
    case When

type CaseInit = CaseState.Init.type

type CaseWhen = CaseState.When.type

class Case[T, K <: ExprKind, S <: CaseState](val exprs: List[Expr[?, ?]]):
    infix def when[T, WK <: OperationKind[K]](expr: Expr[Boolean, WK])(using S =:= CaseInit): Case[T, ResultKind[K, WK], CaseWhen] =
        new Case(exprs :+ expr)

    infix def `then`[E <: Operation[T], WK <: OperationKind[K]](expr: Expr[E, WK])(using S =:= CaseWhen): Case[T, ResultKind[K, WK], CaseInit] =
        new Case(exprs :+ expr)

    infix def `then`[E <: Operation[T]](value: E)(using p: S =:= CaseWhen, a: AsSqlExpr[E]): Case[T, ResultKind[K, ValueKind], CaseInit] =
        new Case(exprs :+ Expr.Literal(value, a))

    infix def `else`[E <: Operation[T]](value: E)(using p: S =:= CaseInit, a: AsSqlExpr[E]): Expr[E, ResultKind[K, ValueKind]] =
        val caseBranches =
            exprs.grouped(2).toList.map(i => (i.head, i(1)))
        Expr.Case(caseBranches, Expr.Literal(value, a))

def `case`: Case[Any, ValueKind, CaseInit] = new Case(Nil)

def exists[T, S <: ResultSize](query: Query[T, S]): SubLinkItem[Boolean] =
    SubLinkItem(query.ast, SqlSubLinkType.Exists)

def notExists[T, S <: ResultSize](query: Query[T, S]): SubLinkItem[Boolean] =
    SubLinkItem(query.ast, SqlSubLinkType.NotExists)

def all[T, K <: ExprKind, S <: ResultSize](query: Query[Expr[T, K], S]): SubLinkItem[Wrap[T, Option]] =
    SubLinkItem(query.ast, SqlSubLinkType.All)

def any[T, K <: ExprKind, S <: ResultSize](query: Query[Expr[T, K], S]): SubLinkItem[Wrap[T, Option]] =
    SubLinkItem(query.ast, SqlSubLinkType.Any)

def some[T, K <: ExprKind, S <: ResultSize](query: Query[Expr[T, K], S]): SubLinkItem[Wrap[T, Option]] =
    SubLinkItem(query.ast, SqlSubLinkType.Some)

def count(): Expr[Long, AggKind] = Expr.Func("COUNT", Nil)

def count[K <: SimpleKind](expr: Expr[?, K]): Expr[Long, AggKind] =
    Expr.Func("COUNT", expr :: Nil)

def countDistinct[K <: SimpleKind](expr: Expr[?, K]): Expr[Long, AggKind] =
    Expr.Func("COUNT", expr :: Nil, true)

def sum[T: Number, K <: SimpleKind](expr: Expr[T, K]): Expr[Option[BigDecimal], AggKind] =
    Expr.Func("SUM", expr :: Nil)

def avg[T: Number, K <: SimpleKind](expr: Expr[T, K]): Expr[Option[BigDecimal], AggKind] =
    Expr.Func("AVG", expr :: Nil)

def max[T, K <: SimpleKind](expr: Expr[T, K]): Expr[Wrap[T, Option], AggKind] =
    Expr.Func("MAX", expr :: Nil)

def min[T, K <: SimpleKind](expr: Expr[T, K]): Expr[Wrap[T, Option], AggKind] =
    Expr.Func("MIN", expr :: Nil)

def percentileCont[N: Number, K <: SimpleKind](n: Double, withinGroup: OrderBy[N, K])(using (n.type >= 0.0 && n.type <= 1.0) =:= true): Expr[Option[BigDecimal], AggKind] =
    Expr.Func("PERCENTILE_CONT", n.asExpr :: Nil, withinGroupOrderBy = withinGroup :: Nil)

def percentileDisc[N: Number, K <: SimpleKind](n: Double, withinGroup: OrderBy[N, K])(using (n.type >= 0.0 && n.type <= 1.0) =:= true): Expr[Option[BigDecimal], AggKind] =
    Expr.Func("PERCENTILE_DISC", n.asExpr :: Nil, withinGroupOrderBy = withinGroup :: Nil)

def rank(): Expr[Option[Long], AggKind] = Expr.Func("RANK", Nil)

def denseRank(): Expr[Option[Long], AggKind] = Expr.Func("DENSE_RANK", Nil)

def rowNumber(): Expr[Option[Long], AggKind] = Expr.Func("ROW_NUMBER", Nil)

def lag[T, K <: SimpleKind](expr: Expr[T, K], offset: Int = 1, default: Option[Unwrap[T, Option]] = None)(using a: AsSqlExpr[Unwrap[T, Option]]): Expr[Wrap[T, Option], AggKind] =
    val defaultExpr = default match
        case Some(v) => Expr.Literal(v, a)
        case _ => Expr.Null
    Expr.Func("LAG", expr :: Expr.Literal(offset, summon[AsSqlExpr[Int]]) :: defaultExpr :: Nil)

def lead[T, K <: SimpleKind](expr: Expr[T, K], offset: Int = 1, default: Option[Unwrap[T, Option]] = None)(using a: AsSqlExpr[Unwrap[T, Option]]): Expr[Wrap[T, Option], AggKind] =
    val defaultExpr = default match
        case Some(v) => Expr.Literal(v, a)
        case _ => Expr.Null
    Expr.Func("LEAD", expr :: Expr.Literal(offset, summon[AsSqlExpr[Int]]) :: defaultExpr :: Nil)

def ntile(n: Int): Expr[Int, AggKind] =
    Expr.Func("NTILE", n.asExpr :: Nil)

def firstValue[T, K <: SimpleKind](expr: Expr[T, K]): Expr[Wrap[T, Option], AggKind] =
    Expr.Func("FIRST_VALUE", expr :: Nil)

def lastValue[T, K <: SimpleKind](expr: Expr[T, K]): Expr[Wrap[T, Option], AggKind] =
    Expr.Func("LAST_VALUE", expr :: Nil)

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
def concat(expr: (Expr[String, AggKind] | Expr[Option[String], AggKind] | String)*): Expr[Option[String], AggKind] =
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

def now(): Expr[Option[Date], ValueKind] =
    Expr.Func("NOW", Nil)

def grouping(items: Expr[?, AggKind]*): Expr[Int, AggKind] =
    Expr.Grouping(items.toList)

sealed trait TimeUnit(val unit: SqlTimeUnit)
case object Year extends TimeUnit(SqlTimeUnit.Year)
case object Month extends TimeUnit(SqlTimeUnit.Month)
case object Week extends TimeUnit(SqlTimeUnit.Week)
case object Day extends TimeUnit(SqlTimeUnit.Day)
case object Hour extends TimeUnit(SqlTimeUnit.Hour)
case object Minute extends TimeUnit(SqlTimeUnit.Minute)
case object Second extends TimeUnit(SqlTimeUnit.Second)

def interval(value: Double, unit: TimeUnit): TimeInterval =
    TimeInterval(value, unit.unit)

def extract[T: DateTime, K <: ExprKind](unit: TimeUnit, expr: Expr[T, K]): Expr[Option[BigDecimal], ResultKind[K, ValueKind]] =
    Expr.Extract(unit.unit, expr)

def cast[T](expr: Expr[?, ?], castType: String): Expr[Wrap[T, Option], CastKind[expr.type]] =
    Expr.Cast(expr, castType)

def currentRow: SqlWindowFrameOption = SqlWindowFrameOption.CurrentRow

def unboundedPreceding: SqlWindowFrameOption = SqlWindowFrameOption.UnboundedPreceding

def unboundedFollowing: SqlWindowFrameOption = SqlWindowFrameOption.UnboundedFollowing

def preceding(n: Int): SqlWindowFrameOption = SqlWindowFrameOption.Preceding(n)

def following(n: Int): SqlWindowFrameOption = SqlWindowFrameOption.Following(n)

def rowsBetween(start: SqlWindowFrameOption, end: SqlWindowFrameOption): SqlWindowFrame =
    SqlWindowFrame.Rows(start, end)

def rangeBetween(start: SqlWindowFrameOption, end: SqlWindowFrameOption): SqlWindowFrame =
    SqlWindowFrame.Range(start, end)

def groupsBetween(start: SqlWindowFrameOption, end: SqlWindowFrameOption): SqlWindowFrame =
    SqlWindowFrame.Groups(start, end)

def queryContext[T](v: QueryContext ?=> T): T =
    given QueryContext = QueryContext(-1)
    v

inline def query[T](using qc: QueryContext = QueryContext(-1), p: Mirror.ProductOf[T], s: SelectItem[Table[T]]): SelectQuery[Table[T]] =
    AsSqlExpr.summonInstances[p.MirroredElemTypes]
    val tableName = TableMacro.tableName[T]
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val table = Table[T](tableName, aliasName, TableMacro.tableMetaData[T])
    val ast = SqlQuery.Select(select = s.selectItems(table, 0), from = SqlTable.IdentTable(tableName, Some(SqlTableAlias(aliasName))) :: Nil)
    SelectQuery(table, ast)

inline def query[N <: Tuple, V <: Tuple, S <: ResultSize](q: Query[NamedTuple[N, V], S])(using qc: QueryContext, s: SelectItem[NamedQuery[N, V]]): SelectQuery[NamedQuery[N, V]] =
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val innerQuery = NamedQuery(q, aliasName)
    val ast = SqlQuery.Select(
        select = s.selectItems(innerQuery, 0),
        from = SqlTable.SubQueryTable(q.ast, false, SqlTableAlias(aliasName, Nil)) :: Nil
    )
    SelectQuery(innerQuery, ast)

def withRecursive[N <: Tuple, WN <: Tuple, V <: Tuple](query: Query[NamedTuple[N, V], ?])(f: Option[WithContext] ?=> Query[NamedTuple[N, V], ?] => Query[NamedTuple[WN, V], ?])(using s: SelectItem[NamedQuery[N, V]]): WithRecursive[NamedTuple[N, V]] =
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