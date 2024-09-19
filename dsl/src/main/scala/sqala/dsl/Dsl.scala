package sqala.dsl

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.ast.table.*
import sqala.dsl.macros.TableMacro
import sqala.dsl.statement.dml.*
import sqala.dsl.statement.query.*

import java.util.Date
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

def exists[T, S <: ResultSize](query: Query[T, S]): SubLinkItem[Boolean] =
    SubLinkItem(query.ast, SqlSubLinkType.Exists)

def notExists[T, S <: ResultSize](query: Query[T, S]): SubLinkItem[Boolean] =
    SubLinkItem(query.ast, SqlSubLinkType.NotExists)

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

inline def max[T: Number, K <: ExprKind](expr: Expr[T, K]): Expr[Option[BigDecimal], AggKind] =
    aggregate("MAX", expr)

inline def min[T: Number, K <: ExprKind](expr: Expr[T, K]): Expr[Option[BigDecimal], AggKind] =
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
                Expr.Func("PERCENTILE_CONT", n.asExpr :: Nil, withinGroupOrderBy = withinGroup :: Nil)

inline def percentileDist[N: Number, K <: ExprKind](n: Double, withinGroup: OrderBy[N, K]): Expr[Option[BigDecimal], AggKind] =
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
                Expr.Func("PERCENTILE_DIST", n.asExpr :: Nil, withinGroupOrderBy = withinGroup :: Nil)

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
            val groupingItems = inline items match
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

inline def query[T](using qc: QueryContext = QueryContext(-1), p: Mirror.ProductOf[T], a: AsTable[T]): SelectQuery[a.R] =
    AsSqlExpr.summonInstances[p.MirroredElemTypes]
    val tableName = TableMacro.tableName[T]
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val table = Table[T](tableName, aliasName, TableMacro.tableMetaData[T])
    val ast = SqlQuery.Select(select = table.__selectItems__(0), from = SqlTable.IdentTable(tableName, Some(SqlTableAlias(aliasName))) :: Nil)
    SelectQuery(table.asInstanceOf[a.R], table.__offset__, ast)

inline def query[Q, S <: ResultSize](q: Query[Q, S])(using qc: QueryContext, s: SelectItem[Q]): SelectQuery[s.R] =
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val subQueryItems = s.subQueryItems(q.queryItems, 0, aliasName)
    val subQuerySelectItems = s.subQuerySelectItems(subQueryItems, 0)
    val ast = SqlQuery.Select(
        select = subQuerySelectItems,
        from = SqlTable.SubQueryTable(q.ast, false, SqlTableAlias(aliasName, Nil)) :: Nil
    )
    SelectQuery(subQueryItems, s.offset(q.queryItems), ast)

inline def values[T <: Product](list: List[T])(using qc: QueryContext = QueryContext(-1), p: Mirror.ProductOf[T], a: AsTable[T]): SelectQuery[a.R] =
    val instances = AsSqlExpr.summonInstances[p.MirroredElemTypes]
    val tableName = TableMacro.tableName[T]
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val table = Table[T](tableName, aliasName, TableMacro.tableMetaData[T])
    val selectItems: List[SqlSelectItem.Item] = table.__selectItems__(0).map: i =>
        SqlSelectItem.Item(SqlExpr.Column(Some(aliasName), i.alias.get), i.alias)
    val exprList = list.map: datum =>
        instances.zip(datum.productIterator).map: (i, v) =>
            i.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(v)
    val sqlValues = SqlQuery.Values(exprList)
    val tableAlias = SqlTableAlias(aliasName, selectItems.map(_.alias.get))
    val ast = SqlQuery.Select(select = selectItems, from = SqlTable.SubQueryTable(sqlValues, false, tableAlias) :: Nil)
    val newTable = Table[T](aliasName, aliasName, TableMacro.tableMetaData[T].copy(columnNames = selectItems.map(_.alias.get)))
    SelectQuery(newTable.asInstanceOf[a.R], newTable.__offset__, ast)

def withRecursive[Q](query: Query[Q, ?])(f: Query[Q, ?] => Query[Q, ?])(using SelectItem[Q]): WithRecursive[Q] =
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