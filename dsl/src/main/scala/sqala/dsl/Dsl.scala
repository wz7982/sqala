package sqala.dsl

import sqala.ast.expr.SqlSubQueryPredicate
import sqala.ast.statement.SqlQuery
import sqala.ast.table.*
import sqala.dsl.macros.{tableMetaDataMacro, tableNameMacro}
import sqala.dsl.statement.dml.*
import sqala.dsl.statement.native.NativeSql
import sqala.dsl.statement.query.*

import scala.annotation.targetName
import scala.deriving.Mirror
import scala.language.experimental.erasedDefinitions

extension [T: AsSqlExpr](value: T)
    def asExpr: Expr[T] = Literal(value)

enum CaseState:
    case Init
    case When
    case Else

type CaseInit = CaseState.Init.type

type CaseWhen = CaseState.When.type

type CaseElse = CaseState.Else.type

class CaseToken[T, S <: CaseState](val exprs: List[Expr[?]]):
    infix def when(expr: Expr[Boolean])(using erased S =:= CaseInit): CaseToken[T, CaseWhen] =
        new CaseToken(exprs :+ expr)

    infix def `then`[E <: Operation[T]](value: E)(using erased S =:= CaseWhen, AsSqlExpr[E]): CaseToken[E, CaseInit] =
        new CaseToken(exprs :+ Literal(value))

    infix def `then`[E <: Operation[T]](expr: Expr[E])(using erased S =:= CaseWhen): CaseToken[E, CaseInit] =
        new CaseToken(exprs :+ expr)

    infix def `else`[E <: Operation[T]](value: E)(using erased S =:= CaseInit, AsSqlExpr[E]): CaseToken[E, CaseElse] =
        new CaseToken(exprs :+ Literal(value))

    infix def `else`[E <: Operation[T]](expr: Expr[E])(using erased S =:= CaseInit): CaseToken[E, CaseElse] =
        new CaseToken(exprs :+ expr)

    def end(using erased S <:< (CaseInit | CaseElse)): Case[Wrap[T, Option]] =
        if exprs.size % 2 == 0 then
            val caseBranches = exprs.grouped(2).toList.map(i => (i.head, i(1)))
            Case(caseBranches, Null)
        else
            val lastExpr = exprs.last
            val caseBranches = exprs.dropRight(1).grouped(2).toList.map(i => (i.head, i(1)))
            Case(caseBranches, lastExpr)

def `case`: CaseToken[Any, CaseInit] = new CaseToken(Nil)

def exists[T](query: Query[T]): Expr[Boolean] = SubQueryPredicate(query, SqlSubQueryPredicate.Exists)

def notExists[T](query: Query[T]): Expr[Boolean] = SubQueryPredicate(query, SqlSubQueryPredicate.NotExists)

def all[T, E <: Expr[T], N <: Tuple](query: Query[NamedTupleWrapper[N, Tuple1[E]]]): Expr[Wrap[T, Option]] =
    SubQueryPredicate(query, SqlSubQueryPredicate.All)

@targetName("allExprQuery")
def all[T, E <: Expr[T]](query: Query[E]): Expr[Wrap[T, Option]] = SubQueryPredicate(query, SqlSubQueryPredicate.All)

def any[T, E <: Expr[T], N <: Tuple](query: Query[NamedTupleWrapper[N, Tuple1[E]]]): Expr[Wrap[T, Option]] =
    SubQueryPredicate(query, SqlSubQueryPredicate.Any)

@targetName("anyExprQuery")
def any[T, E <: Expr[T]](query: Query[E]): Expr[Wrap[T, Option]] = SubQueryPredicate(query, SqlSubQueryPredicate.Any)

def some[T, E <: Expr[T], N <: Tuple](query: Query[NamedTupleWrapper[N, Tuple1[E]]]): Expr[Wrap[T, Option]] =
    SubQueryPredicate(query, SqlSubQueryPredicate.Some)

@targetName("someExprQuery")
def some[T, E <: Expr[T]](query: Query[E]): Expr[Wrap[T, Option]] = SubQueryPredicate(query, SqlSubQueryPredicate.Some)

def count(): Agg[Long] = Agg("COUNT", Nil, false, Nil)

def count(expr: Expr[?]): Agg[Long] = Agg("COUNT", expr :: Nil, false, Nil)

def countDistinct(expr: Expr[?]): Agg[Long] = Agg("COUNT", expr :: Nil, true, Nil)

def sum[T: Number](expr: Expr[T]): Agg[Option[BigDecimal]] = Agg("SUM", expr :: Nil, false, Nil)

def avg[T: Number](expr: Expr[T]): Agg[Option[BigDecimal]] = Agg("AVG", expr :: Nil, false, Nil)

def max[T](expr: Expr[T]): Agg[Wrap[T, Option]] = Agg("MAX", expr :: Nil, false, Nil)

def min[T](expr: Expr[T]): Agg[Wrap[T, Option]] = Agg("MIN", expr :: Nil, false, Nil)

def rank(): Agg[Option[Long]] = Agg("RANK", Nil, false, Nil)

def denseRank(): Agg[Option[Long]] = Agg("DENSE_RANK", Nil, false, Nil)

def rowNumber(): Agg[Option[Long]] = Agg("ROW_NUMBER", Nil, false, Nil)

def lag[T](expr: Expr[T], offset: Int = 1, default: Option[Unwrap[T, Option]] = None)(using AsSqlExpr[Unwrap[T, Option]]): Agg[Wrap[T, Option]] =
    val defaultExpr = default match
        case Some(v) => Literal(v)
        case _ => Null
    Agg("LAG", expr :: Literal(offset) :: defaultExpr :: Nil, false, Nil)

def lead[T](expr: Expr[T], offset: Int = 1, default: Option[Unwrap[T, Option]] = None)(using AsSqlExpr[Unwrap[T, Option]]): Agg[Wrap[T, Option]] =
    val defaultExpr = default match
        case Some(v) => Literal(v)
        case _ => Null
    Agg("LEAD", expr :: Literal(offset) :: defaultExpr :: Nil, false, Nil)

def queryContext[T](v: QueryContext ?=> T): T =
    given QueryContext = QueryContext(-1)
    v
    
inline def query[T](using qc: QueryContext, p: Mirror.ProductOf[T], s: SelectItem[Table[T]]): SelectQuery[Table[T]] =
    val tableName = tableNameMacro[T]
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val table = Table[T](tableName, aliasName, tableMetaDataMacro[T])
    val ast = SqlQuery.Select(select = s.selectItems(table, 0), from = SqlTable.IdentTable(tableName, Some(aliasName)) :: Nil)
    SelectQuery(table, ast)

inline def query[N <: Tuple, V <: Tuple](q: Query[NamedTupleWrapper[N, V]])(using qc: QueryContext, s: SelectItem[NamedQuery[N, V]]): SelectQuery[NamedQuery[N, V]] =
    qc.tableIndex += 1
    val aliasName = s"t${qc.tableIndex}"
    val innerQuery = NamedQuery(q, aliasName)
    val ast = SqlQuery.Select(
        select = s.selectItems(innerQuery, 0),
        from = SqlTable.SubQueryTable(q.ast, false, SqlSubQueryAlias(aliasName, Nil)) :: Nil
    )
    SelectQuery(innerQuery, ast)

def withRecursive[N <: Tuple, WN <: Tuple, V <: Tuple](query: Query[NamedTupleWrapper[N, V]])(f: Option[WithContext] ?=> Query[NamedTupleWrapper[N, V]] => Query[NamedTupleWrapper[WN, V]])(using s: SelectItem[NamedQuery[N, V]]): WithRecursive[NamedTupleWrapper[N, V]] =
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