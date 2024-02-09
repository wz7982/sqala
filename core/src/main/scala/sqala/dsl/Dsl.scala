package sqala.dsl

import sqala.ast.expr.SqlSubQueryPredicate
import sqala.ast.statement.SqlQuery
import sqala.ast.table.SqlTable
import sqala.dsl.`macro`.{tableMetaDataMacro, tableNameMacro}
import sqala.dsl.statement.dml.*
import sqala.dsl.statement.query.{Depth, Query, SelectItem, SelectQuery}

import scala.deriving.Mirror
import sqala.dsl.statement.query.NamedQuery
import sqala.ast.table.SqlSubQueryAlias

extension [T: AsSqlExpr](value: T)
    def asExpr: Expr[T] = Literal(value)

inline def query[T](using depth: Depth = Depth(0))(using p: Mirror.ProductOf[T], s: SelectItem[Table[T]]): SelectQuery[Table[T]] =
    val tableName = tableNameMacro[T]
    val aliasName = s"d${depth.asInt}_t0"
    val table = Table[T](tableName, aliasName, tableMetaDataMacro[T], depth.asInt)
    val ast = SqlQuery.Select(select = s.selectItems(table, 0), from = SqlTable.IdentTable(tableName, Some(aliasName)) :: Nil)
    SelectQuery(depth.asInt, 0, table, ast)

inline def subQuery[T](query: Depth ?=> Query[T])(using depth: Depth = Depth(0))(using s: SelectItem[NamedQuery[T]]): SelectQuery[NamedQuery[T]] =
    given d: Depth = Depth(depth.asInt + 1)
    val aliasName = s"d${depth.asInt}_t0"
    val innerQuery = NamedQuery(query, aliasName)
    val ast = SqlQuery.Select(
        select = s.selectItems(innerQuery, 0), 
        from = SqlTable.SubQueryTable(query.ast, false, SqlSubQueryAlias(aliasName, Nil)) :: Nil
    )
    SelectQuery(d.asInt, 0, innerQuery, ast)

enum CaseState:
    case Case
    case When
    case Else

class CaseToken[T, S <: CaseState](val exprs: List[Expr[?]]):
    infix def when(expr: Expr[Boolean])(using S =:= CaseState.Case.type): CaseToken[T, CaseState.When.type] =
        new CaseToken(exprs :+ expr)

    infix def `then`[E <: Operation[T]](value: E)(using S =:= CaseState.When.type, AsSqlExpr[E]): CaseToken[E, CaseState.Case.type] =
        new CaseToken(exprs :+ Literal(value))

    infix def `then`[E <: Operation[T]](expr: Expr[E])(using S =:= CaseState.When.type): CaseToken[E, CaseState.Case.type] =
        new CaseToken(exprs :+ expr)

    infix def `else`[E <: Operation[T]](value: E)(using S =:= CaseState.Case.type, AsSqlExpr[E]): CaseToken[E, CaseState.Else.type] =
        new CaseToken(exprs :+ Literal(value))

    infix def `else`[E <: Operation[T]](expr: Expr[E])(using S =:= CaseState.Case.type): CaseToken[E, CaseState.Else.type] =
        new CaseToken(exprs :+ expr)

    def end(using S <:< (CaseState.Case.type | CaseState.Else.type)): Case[Wrap[T, Option]] =
        if exprs.size % 2 == 0 then
            val caseBranches = exprs.grouped(2).toList.map(i => (i.head, i(1)))
            Case(caseBranches, Null)
        else
            val lastExpr = exprs.last
            val caseBranches = exprs.dropRight(1).grouped(2).toList.map(i => (i.head, i(1)))
            Case(caseBranches, lastExpr)

def exists(query: Query[?]): Expr[Boolean] = SubQueryPredicate(query, SqlSubQueryPredicate.Exists)

def notExists(query: Query[?]): Expr[Boolean] = SubQueryPredicate(query, SqlSubQueryPredicate.NotExists)

def all[T](query: Query[Expr[T]]): Expr[Wrap[T, Option]] = SubQueryPredicate(query, SqlSubQueryPredicate.All)

def any[T](query: Query[Expr[T]]): Expr[Wrap[T, Option]] = SubQueryPredicate(query, SqlSubQueryPredicate.Any)

def some[T](query: Query[Expr[T]]): Expr[Wrap[T, Option]] = SubQueryPredicate(query, SqlSubQueryPredicate.Some)

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

def lag[T: AsSqlExpr](expr: Expr[T], offset: Int = 1, default: Wrap[T, Option] = None): Agg[Wrap[T, Option]] =
    val defaultExpr = default match
        case Some(v) => Literal(v.asInstanceOf[T])
        case _ => Null
    Agg("LAG", expr :: Literal(offset) :: defaultExpr :: Nil, false, Nil)
    
def lead[T: AsSqlExpr](expr: Expr[T], offset: Int = 1, default: Wrap[T, Option] = None): Agg[Wrap[T, Option]] =
    val defaultExpr = default match
        case Some(v) => Literal(v.asInstanceOf[T])
        case _ => Null
    Agg("LEAD", expr :: Literal(offset) :: defaultExpr :: Nil, false, Nil)

inline def insert[T]: Insert[Table[T], InsertState.New.type] = Insert[T]

inline def insert[T <: Product](entity: T)(using Mirror.ProductOf[T]): Insert[Table[T], InsertState.Entity.type] = 
    Insert[T](entity :: Nil)

inline def insert[T <: Product](entities: List[T])(using Mirror.ProductOf[T]): Insert[Table[T], InsertState.Entity.type] = 
    Insert[T](entities)

inline def update[T]: Update[Table[T], UpdateState.Table.type] = Update[T]

inline def update[T <: Product](entity: T, skipNone: Boolean = false)(using Mirror.ProductOf[T]): Update[Table[T], UpdateState.Entity.type] =
    Update[T](entity, skipNone)

inline def delete[T]: Delete[Table[T]] = Delete[T]

inline def save[T <: Product](entity: T): Save = Save[T](entity)

extension [T: AsSqlExpr] (column: Column[T])
   def :=(value: T): UpdatePair = UpdatePair(column, Literal(value))

   def :=(updateExpr: Expr[T]): UpdatePair = UpdatePair(column, updateExpr)