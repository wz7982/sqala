package sqala.dsl

import sqala.ast.expr.*
import sqala.ast.statement.SqlQuery
import sqala.ast.table.*
import sqala.dsl.macros.TableMacro
import sqala.dsl.statement.dml.*
import sqala.dsl.statement.native.NativeSql
import sqala.dsl.statement.query.*
import sqala.dsl.statement.select.TableQuery

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValue
import scala.deriving.Mirror
import scala.language.experimental.erasedDefinitions
import java.util.Date

inline def asTable[T <: Product](using m: Mirror.ProductOf[T]): EntityTable[T, m.MirroredLabel] =
    AsSqlExpr.summonInstances[m.MirroredElemTypes]
    val aliasName = constValue[m.MirroredLabel]
    val metaData = TableMacro.tableMetaData[T]
    EntityTable(metaData.tableName, aliasName, metaData)

extension [T: AsSqlExpr](value: T)
    def asExpr: Expr[T, ValueKind] = Expr.Literal(value, summon[AsSqlExpr[T]])

enum CaseState:
    case Init
    case When

type CaseInit = CaseState.Init.type

type CaseWhen = CaseState.When.type

class Case[T, K <: ExprKind, S <: CaseState](val exprs: List[Expr[?, ?]]):
    infix def when[T, WK <: OperationKind[K]](expr: Expr[Boolean, WK])(using erased S =:= CaseInit): Case[T, ResultKind[K, WK], CaseWhen] =
        new Case(exprs :+ expr)

    infix def `then`[E <: Operation[T], WK <: OperationKind[K]](expr: Expr[E, WK])(using erased S =:= CaseWhen): Case[T, ResultKind[K, WK], CaseInit] =
        new Case(exprs :+ expr)

    infix def `then`[E <: Operation[T]](value: E)(using erased p: S =:= CaseWhen, a: AsSqlExpr[E]): Case[T, ResultKind[K, ValueKind], CaseInit] =
        new Case(exprs :+ Expr.Literal(value, a))

    infix def `else`[E <: Operation[T]](value: E)(using erased p: S =:= CaseInit, a: AsSqlExpr[E]): Expr[E, ResultKind[K, ValueKind]] =
        val caseBranches =
            exprs.grouped(2).toList.map(i => (i.head, i(1)))
        Expr.Case(caseBranches, Expr.Literal(value, a))

def `case`: Case[Any, ValueKind, CaseInit] = new Case(Nil)

def exists[T](query: Queryable[T]): Expr[Boolean, CommonKind] =
    Expr.SubQueryPredicate(query.ast, SqlSubQueryPredicate.Exists)

def notExists[T](query: Queryable[T]): Expr[Boolean, CommonKind] =
    Expr.SubQueryPredicate(query.ast, SqlSubQueryPredicate.NotExists)

def all[T, K <: ExprKind](query: Queryable[Expr[T, K]]): Expr[Wrap[T, Option], CommonKind] =
    Expr.SubQueryPredicate(query.ast, SqlSubQueryPredicate.All)

def any[T, K <: ExprKind](query: Queryable[Expr[T, K]]): Expr[Wrap[T, Option], CommonKind] =
    Expr.SubQueryPredicate(query.ast, SqlSubQueryPredicate.Any)

def some[T, K <: ExprKind](query: Queryable[Expr[T, K]]): Expr[Wrap[T, Option], CommonKind] =
    Expr.SubQueryPredicate(query.ast, SqlSubQueryPredicate.Some)

def count(): Expr[Long, AggKind] = Expr.Agg("COUNT", Nil, false, Nil)

def count[K <: SimpleKind](expr: Expr[?, K]): Expr[Long, AggKind] =
    Expr.Agg("COUNT", expr :: Nil, false, Nil)

def countDistinct[K <: SimpleKind](expr: Expr[?, K]): Expr[Long, AggKind] =
    Expr.Agg("COUNT", expr :: Nil, true, Nil)

def sum[T: Number, K <: SimpleKind](expr: Expr[T, K]): Expr[Option[BigDecimal], AggKind] =
    Expr.Agg("SUM", expr :: Nil, false, Nil)

def avg[T: Number, K <: SimpleKind](expr: Expr[T, K]): Expr[Option[BigDecimal], AggKind] =
    Expr.Agg("AVG", expr :: Nil, false, Nil)

def max[T, K <: SimpleKind](expr: Expr[T, K]): Expr[Wrap[T, Option], AggKind] =
    Expr.Agg("MAX", expr :: Nil, false, Nil)

def min[T, K <: SimpleKind](expr: Expr[T, K]): Expr[Wrap[T, Option], AggKind] =
    Expr.Agg("MIN", expr :: Nil, false, Nil)

def rank(): Expr[Option[Long], AggKind] = Expr.Agg("RANK", Nil, false, Nil)

def denseRank(): Expr[Option[Long], AggKind] = Expr.Agg("DENSE_RANK", Nil, false, Nil)

def rowNumber(): Expr[Option[Long], AggKind] = Expr.Agg("ROW_NUMBER", Nil, false, Nil)

def lag[T, K <: SimpleKind](expr: Expr[T, K], offset: Int = 1, default: Option[Unwrap[T, Option]] = None)(using a: AsSqlExpr[Unwrap[T, Option]]): Expr[Wrap[T, Option], AggKind] =
    val defaultExpr = default match
        case Some(v) => Expr.Literal(v, a)
        case _ => Expr.Null
    Expr.Agg("LAG", expr :: Expr.Literal(offset, summon[AsSqlExpr[Int]]) :: defaultExpr :: Nil, false, Nil)

def lead[T, K <: SimpleKind](expr: Expr[T, K], offset: Int = 1, default: Option[Unwrap[T, Option]] = None)(using a: AsSqlExpr[Unwrap[T, Option]]): Expr[Wrap[T, Option], AggKind] =
    val defaultExpr = default match
        case Some(v) => Expr.Literal(v, a)
        case _ => Expr.Null
    Expr.Agg("LEAD", expr :: Expr.Literal(offset, summon[AsSqlExpr[Int]]) :: defaultExpr :: Nil, false, Nil)

def coalesce[T, K <: SimpleKind](expr: Expr[Option[T], K], value: T)(using a: AsSqlExpr[T]): Expr[T, CommonKind] =
    Expr.Func("COALESCE", expr :: Expr.Literal(value, a) :: Nil)

def ifnull[T, K <: SimpleKind](expr: Expr[Option[T], K], value: T)(using AsSqlExpr[T]): Expr[T, CommonKind] =
    coalesce(expr, value)

def abs[T: Number, K <: SimpleKind](expr: Expr[T, K]): Expr[T, CommonKind] =
    Expr.Func("ABS", expr :: Nil)

def ceil[T: Number, K <: SimpleKind](expr: Expr[T, K]): Expr[Option[Long], CommonKind] =
    Expr.Func("CEIL", expr :: Nil)

def floor[T: Number, K <: SimpleKind](expr: Expr[T, K]): Expr[Option[Long], CommonKind] =
    Expr.Func("FLOOR", expr :: Nil)

def round[T: Number, K <: SimpleKind](expr: Expr[T, K], n: Int): Expr[Option[BigDecimal], CommonKind] =
    Expr.Func("ROUND", expr :: n.asExpr :: Nil)

def power[T: Number, K <: SimpleKind](expr: Expr[T, K], n: Double): Expr[Option[BigDecimal], CommonKind] =
    Expr.Func("POWER", expr :: n.asExpr :: Nil)

def concat(expr: (Expr[String, ?] | Expr[Option[String], ?] | String)*): Expr[Option[String], CommonKind] =
    val args = expr.toList.map:
        case s: String => s.asExpr
        case e: Expr[?, ?] => e
    Expr.Func("CONCAT", args)

def substring[T <: String | Option[String], K <: SimpleKind](expr: Expr[T, K], start: Int, end: Int): Expr[Option[String], CommonKind] =
    Expr.Func("SUBSTRING", expr :: start.asExpr :: end.asExpr :: Nil)

def replace[T <: String | Option[String], K <: SimpleKind](expr: Expr[T, K], oldString: String, newString: String): Expr[T, CommonKind] =
    Expr.Func("REPLACE", expr :: oldString.asExpr :: newString.asExpr :: Nil)

def length[T <: String | Option[String], K <: SimpleKind](expr: Expr[T, K]): Expr[Option[Long], CommonKind] =
    Expr.Func("LENGTH", expr :: Nil)

def repeat[T <: String | Option[String], K <: SimpleKind](expr: Expr[T, K], n: Int): Expr[T, CommonKind] =
    Expr.Func("REPEAT", expr :: n.asExpr :: Nil)

def trim[T <: String | Option[String], K <: SimpleKind](expr: Expr[T, K]): Expr[T, CommonKind] =
    Expr.Func("TRIM", expr :: Nil)

def upper[T <: String | Option[String], K <: SimpleKind](expr: Expr[T, K]): Expr[T, CommonKind] =
    Expr.Func("UPPER", expr :: Nil)

def lower[T <: String | Option[String], K <: SimpleKind](expr: Expr[T, K]): Expr[T, CommonKind] =
    Expr.Func("LOWER", expr :: Nil)

def now(): Expr[Option[Date], CommonKind] =
    Expr.Func("NOW", Nil)

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

def lateral[N <: Tuple, V <: Tuple](query: sqala.dsl.statement.select.Query[NamedTuple[N, V]]): LateralSubQuery[N, V] =
    LateralSubQuery(query.ast)

object QueryToken:
    infix def from[T <: Tuple, N <: Tuple](table: SelectTable[T, N]): TableQuery[T, N] =
        val selectItems = table.selectItems(0)
        new TableQuery(SqlQuery.Select(select = selectItems, from = table.asSqlTable :: Nil))

def query = QueryToken

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

inline def query[N <: Tuple, V <: Tuple](q: Query[NamedTuple[N, V]])(using qc: QueryContext, s: SelectItem[NamedQuery[N, V]]): SelectQuery[NamedQuery[N, V]] =
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val innerQuery = NamedQuery(q, aliasName)
    val ast = SqlQuery.Select(
        select = s.selectItems(innerQuery, 0),
        from = SqlTable.SubQueryTable(q.ast, false, SqlTableAlias(aliasName, Nil)) :: Nil
    )
    SelectQuery(innerQuery, ast)

def withRecursive[N <: Tuple, WN <: Tuple, V <: Tuple](query: Query[NamedTuple[N, V]])(f: Option[WithContext] ?=> Query[NamedTuple[N, V]] => Query[NamedTuple[WN, V]])(using s: SelectItem[NamedQuery[N, V]]): WithRecursive[NamedTuple[N, V]] =
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

extension (s: StringContext)
    def sql(args: Any*): NativeSql =
        val strings = s.parts.iterator
        val argArray = args.toArray
        val argIterator = args.iterator
        val builder = new StringBuilder(strings.next())
        while strings.hasNext do
            val arg = argIterator.next()
            arg match
                case l: List[_] => builder.append(l.map(_ => "?").mkString("(", ", ", ")"))
                case _ => builder.append("?")
            builder.append(strings.next())
        NativeSql(builder.toString, argArray)