package sqala.compiletime

import sqala.ast.expr.*
import sqala.compiletime.macros.tableInfoMacro
import sqala.compiletime.statement.dml.*
import sqala.compiletime.statement.query.*

def value[T: AsSqlExpr](v: T): Literal[T] = Literal(v)

class CaseToken[T, TableNames <: Tuple, State <: CaseState](val exprs: List[Expr[?, ?]]):
    infix def when[ExprTableNames <: Tuple](expr: Expr[Boolean, ExprTableNames])(using State =:= CaseState.Case): CaseToken[T, Combine[TableNames, ExprTableNames], CaseState.When] =
        new CaseToken(exprs = exprs :+ expr)

    infix def `then`[ExprType <: WrapToOption[T] | UnwrapFromOption[T] | T, ExprTableNames <: Tuple](expr: Expr[ExprType, ExprTableNames])(using State =:= CaseState.When): CaseToken[WrapToOption[ExprType], Combine[TableNames, ExprTableNames], CaseState.Case] =
        new CaseToken(exprs = exprs :+ expr)

    infix def `then`[ExprType <: UnwrapFromOption[T] | T](value: Option[ExprType])(using State =:= CaseState.When, AsSqlExpr[ExprType]): CaseToken[Option[ExprType], TableNames, CaseState.Case] =
        val thenExpr = value match
            case Some(v) => Literal(v)
            case None => Null
        new CaseToken(exprs = exprs :+ thenExpr)

    infix def `else`[ExprType <: WrapToOption[T] | UnwrapFromOption[T], ExprTableNames <: Tuple](expr: Expr[ExprType, ExprTableNames])(using State =:= CaseState.Case): CaseToken[WrapToOption[T], Combine[TableNames, ExprTableNames], CaseState.Else] =
        new CaseToken(exprs = exprs :+ expr)

    infix def `else`[ExprType <: UnwrapFromOption[T]](value: Option[ExprType])(using State =:= CaseState.Case, AsSqlExpr[ExprType]): CaseToken[WrapToOption[T], TableNames, CaseState.Else] =
        val expr = value match
            case Some(v) => Literal(v)
            case None => Null
        new CaseToken(exprs = exprs :+ expr)

    def end(using State <:< (CaseState.Case | CaseState.Else)): Case[T, TableNames] =
        if exprs.size % 2 == 0 then
            val caseBranches = exprs.grouped(2).toList.map(i => CaseBranch(i.head, i(1)))
            Case(caseBranches, Null)
        else
            val lastExpr = exprs.last
            val caseBranches = exprs.dropRight(1).grouped(2).toList.map(i => CaseBranch(i.head, i(1)))
            Case(caseBranches, lastExpr)

enum CaseState:
    case Case()
    case When()
    case Else()

def `case`: CaseToken[Any, EmptyTuple, CaseState.Case] = new CaseToken(Nil)

def exists(query: Query[?, ?]): SubQueryPredicate[Boolean] = SubQueryPredicate(query, SqlSubQueryPredicate.Exists)

def notExists(query: Query[?, ?]): SubQueryPredicate[Boolean] = SubQueryPredicate(query, SqlSubQueryPredicate.NotExists)

def all[T](query: Query[Tuple1[T], ?]): SubQueryPredicate[WrapToOption[T]] = SubQueryPredicate(query, SqlSubQueryPredicate.All)

def any[T](query: Query[Tuple1[T], ?]): SubQueryPredicate[WrapToOption[T]] = SubQueryPredicate(query, SqlSubQueryPredicate.Any)

def some[T](query: Query[Tuple1[T], ?]): SubQueryPredicate[WrapToOption[T]] = SubQueryPredicate(query, SqlSubQueryPredicate.Some)

def cast[T](expr: Expr[?, ?], castType: String): Cast[WrapToOption[T], FetchTableNames[expr.type]] = Cast(expr, castType)

def unboundedPreceding: SqlWindowFrameOption = SqlWindowFrameOption.UnboundedPreceding

def unboundedFollowing: SqlWindowFrameOption = SqlWindowFrameOption.UnboundedFollowing

def currentRow: SqlWindowFrameOption = SqlWindowFrameOption.CurrentRow

extension (n: Int)
    infix def preceding: SqlWindowFrameOption = SqlWindowFrameOption.Preceding(n)

    infix def following: SqlWindowFrameOption = SqlWindowFrameOption.Following(n)

transparent inline def asTable[T <: Product]: Any = tableInfoMacro[T]

class QueryToken:
    def apply[T <: Product, TableName <: String](table: Table[T, TableName]): Select[Tuple1[T], EmptyTuple, Tuple1[TableName], table.type] =
        Select(table)
    
    def apply[T <: Tuple, TableNames <: Tuple, Tables <: Tuple](table: JoinTable[T, TableNames, Tables]): Select[T, EmptyTuple, TableNames, Tables] =
        Select(table)

    def apply[T <: Tuple, TableName <: String, ColumnNames <: Tuple](table: SubQueryTable[T, TableName, ColumnNames]): Select[T, EmptyTuple, Tuple1[TableName], table.type] =
        Select(table)

def query: QueryToken = new QueryToken

class InsertToken:
    def into[T <: Tuple](insertColumns: (Table[?, ?], T)): Insert[Tuple.InverseMap[T, [t] =>> Expr[t, ?]], InsertState.InsertTable] =
        Insert.apply(insertColumns)

    def apply[T <: Product](entities: List[T])(using e: Entity[T]): Insert[Tuple1[T], InsertState.InsertEntity] =
        Insert.apply[T](entities)
    
    def apply[T <: Product](entity: T)(using e: Entity[T]): Insert[Tuple1[T], InsertState.InsertEntity] =
        Insert.apply[T](entity :: Nil)

def insert: InsertToken = new InsertToken

class DeleteToken:
    def from(table: Table[?, ?]): Delete[table.type] = Delete(table)

def delete: DeleteToken = new DeleteToken

class UpdateToken:
    def table[T <: Table[?, ?]](table: T): Update[T] = Update(table)

    def apply[T <: Product](entity: T, skipNone: Boolean = false)(using e: Entity[T]): Update[?] = Update(entity, skipNone)

def update: UpdateToken = new UpdateToken

class WithRecursiveToken:
    def apply[T <: Tuple, QueryName <: String, ColumnNames <: Tuple](
        query: Query[T, ColumnNames], 
        queryName: QueryName
    )(
        f: (Query[T, ColumnNames], SubQueryTable[T, QueryName, ColumnNames]) => Query[T, ?]
    ): WithRecursive[T, T, QueryName, ColumnNames] =
        WithRecursive.apply(query, queryName)(f)

def withRecursive: WithRecursiveToken = new WithRecursiveToken

def namedQuery[P <: Product : Entity](table: Table[P, ?]): NamedQuery[P] = new NamedQuery(table)

extension [T: AsSqlExpr] (column: Column[T, ?, ?])
    def :=(value: T): UpdatePair = UpdatePair(column, Literal(value))

    def :=(updateExpr: Expr[T, ?]): UpdatePair = UpdatePair(column, updateExpr)
