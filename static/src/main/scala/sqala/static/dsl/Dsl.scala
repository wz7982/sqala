package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.statement.SqlQuery
import sqala.ast.table.*
import sqala.static.dsl.statement.dml.*
import sqala.static.dsl.statement.query.{AsSelect, ConnectByContext, GroupingContext, SelectQuery}
import sqala.static.metadata.*

import java.time.{LocalDate, LocalDateTime}
import scala.NamedTuple.NamedTuple
import scala.compiletime.ops.boolean.||

inline def query[T](inline q: QueryContext ?=> T): T =
    given QueryContext = QueryContext(0)
    q

inline def insert[T <: Product]: Insert[T, InsertTable] =
    Insert.apply[T]

inline def update[T <: Product](using c: QueryContext = QueryContext(0)): Update[T, UpdateTable] =
    Update.apply[T]

inline def delete[T <: Product](using c: QueryContext): Delete[T] =
    Delete.apply[T]

extension [A](a: => A)(using c: QueryContext)
    infix def join[B](b: => B)(using ta: AsTable[A], tb: AsTable[B], j: TableJoin[ta.R, tb.R]): JoinPart[j.R] = 
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Inner, rightSqlTable, None))

    infix def joinLateral[B](using ta: AsTable[A])(f: ta.R => B)(using 
        tb: AsLateralTable[B], 
        j: TableJoin[ta.R, tb.R]
    ): JoinPart[j.R] =
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(f(leftTable))
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Inner, rightSqlTable, None))

    infix def crossJoin[B](b: => B)(using ta: AsTable[A], tb: AsTable[B], j: TableJoin[ta.R, tb.R]): JoinTable[j.R] = 
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(b)
        val params = j.join(leftTable, rightTable)
        JoinTable(params, SqlTable.Join(leftSqlTable, SqlJoinType.Cross, rightSqlTable, None))

    infix def crossJoinLateral[B](using ta: AsTable[A])(f: ta.R => B)(using 
        tb: AsLateralTable[B], 
        j: TableJoin[ta.R, tb.R]
    ): JoinTable[j.R] =
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(f(leftTable))
        val params = j.join(leftTable, rightTable)
        JoinTable(params, SqlTable.Join(leftSqlTable, SqlJoinType.Cross, rightSqlTable, None))

    infix def leftJoin[B](b: => B)(using ta: AsTable[A], tb: AsTable[B], j: TableLeftJoin[ta.R, tb.R]): JoinPart[j.R] = 
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Left, rightSqlTable, None))

    infix def leftJoinLateral[B](using ta: AsTable[A])(f: ta.R => B)(using 
        tb: AsLateralTable[B], 
        j: TableLeftJoin[ta.R, tb.R]
    ): JoinPart[j.R] =
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(f(leftTable))
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Left, rightSqlTable, None))

    infix def rightJoin[B](b: => B)(using ta: AsTable[A], tb: AsTable[B], j: TableRightJoin[ta.R, tb.R]): JoinPart[j.R] = 
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Right, rightSqlTable, None))

    infix def fullJoin[B](b: => B)(using ta: AsTable[A], tb: AsTable[B], j: TableFullJoin[ta.R, tb.R]): JoinPart[j.R] = 
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Full, rightSqlTable, None))

case class Unnest[T](x: Option[T])

def unnest[T: AsExpr as a](x: T)(using
    f: UnnestFlatten[a.R],
    c: QueryContext
): FuncTable[Unnest[f.R]] =
    c.tableIndex += 1
    val aliasName = s"t${c.tableIndex}"
    val sqlTable: SqlTable.Func = SqlTable.Func(
        "UNNEST",
        a.asExpr(x).asSqlExpr :: Nil,
        false,
        false,
        Some(SqlTableAlias(aliasName, "x" :: Nil)),
        None
    )
    FuncTable(Some(aliasName), "x" :: Nil, "x" :: Nil, sqlTable)

case class UnnestWithOrdinal[T](x: Option[T], ordinal: Int)

def unnestWithOrdinal[T: AsExpr as a](x: T)(using
    f: UnnestFlatten[a.R],
    c: QueryContext
): FuncTable[UnnestWithOrdinal[f.R]] =
    c.tableIndex += 1
    val aliasName = s"t${c.tableIndex}"
    val sqlTable: SqlTable.Func = SqlTable.Func(
        "UNNEST",
        a.asExpr(x).asSqlExpr :: Nil,
        false,
        true,
        Some(SqlTableAlias(aliasName, "x" :: "ordinal" :: Nil)),
        None
    )
    FuncTable(Some(aliasName), "x" :: "ordinal" :: Nil, "x" :: "ordinal" :: Nil, sqlTable)

def jsonTable[E: AsExpr as ae, P: AsExpr as ap, N <: Tuple, V <: Tuple](
    expr: E,
    path: P,
    columns: JsonTableColumns[N, V]
)(using
    s: SqlString[ap.R],
    p: AsTableParam[JsonTableColumnFlatten[V]],
    c: QueryContext
): JsonTable[JsonTableColumnNameFlatten[N, V], JsonTableColumnFlatten[V]] =
    c.tableIndex += 1
    val aliasName = s"t${c.tableIndex}"
    JsonTable(ae.asExpr(expr).asSqlExpr, ap.asExpr(path).asSqlExpr, Some(aliasName), columns)

class JsonTableColumnContext

def ordinalColumn(using QueryContext, JsonTableColumnContext): JsonTableOrdinalColumn =
    new JsonTableOrdinalColumn

class JsonTablePathColumnPart[T]:
    def apply[P: AsExpr as ap](path: P)(using sp: AsSqlExpr[T]): JsonTablePathColumn[T] =
        JsonTablePathColumn(ap.asExpr(path).asSqlExpr, sp.sqlType)

def pathColumn[T: AsSqlExpr](using QueryContext, JsonTableColumnContext): JsonTablePathColumnPart[T] =
    new JsonTablePathColumnPart

def existsColumn[P: AsExpr as ap](path: P)(using 
    QueryContext, 
    JsonTableColumnContext
): JsonTableExistsColumn =
    JsonTableExistsColumn(ap.asExpr(path).asSqlExpr)

def columns[N <: Tuple, V <: Tuple](c: JsonTableColumnContext ?=> NamedTuple[N, V])(using
    QueryContext
): JsonTableColumns[N, V] =
    given JsonTableColumnContext = new JsonTableColumnContext
    val columnList: List[Any] = c.toList
    val jsonColumns = columnList.map:
        case p: JsonTablePathColumn[?] => p
        case o: JsonTableOrdinalColumn => o
        case e: JsonTableExistsColumn => e
        case n: JsonTableNestedColumns[?, ?] => n
    JsonTableColumns(jsonColumns)

def nestedColumns[P: AsExpr as ap, N <: Tuple, V <: Tuple](
    path: P
)(
    c: JsonTableColumnContext ?=> NamedTuple[N, V]
)(using QueryContext, JsonTableColumnContext): JsonTableNestedColumns[N, V] =
    given JsonTableColumnContext = new JsonTableColumnContext
    val columnList: List[Any] = c.toList
    val jsonColumns = columnList.map:
        case p: JsonTablePathColumn[?] => p
        case o: JsonTableOrdinalColumn => o
        case e: JsonTableExistsColumn => e
        case n: JsonTableNestedColumns[?, ?] => n
    JsonTableNestedColumns(ap.asExpr(path).asSqlExpr, jsonColumns)

def from[T](tables: T)(using
    f: AsTable[T],
    s: AsSelect[f.R],
    c: QueryContext
): SelectQuery[f.R] =
    val (params, fromTable) = f.table(tables)
    val selectItems = s.selectItems(params, 1)
    val tree: SqlQuery.Select = SqlQuery.Select(
        None,
        selectItems,
        fromTable :: Nil,
        None,
        None,
        None,
        Nil,
        Nil,
        None,
        None
    )
    SelectQuery(params, tree)

def grouping[T: AsExpr as a](x: T)(using QueryContext, GroupingContext): Expr[Int] =
    Expr(
        SqlExpr.Grouping(
            a.exprs(x).map(_.asSqlExpr)
        )
    )

def timestamp(s: String)(using QueryContext): Expr[LocalDateTime] =
    Expr(SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Timestamp, s))

def date(s: String)(using QueryContext): Expr[LocalDate] =
    Expr(SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Date, s))

def level()(using QueryContext, ConnectByContext): Expr[Int] =
    Expr(SqlExpr.Column(Some(tableCte), columnPseudoLevel))

sealed class TimeUnit(val unit: SqlTimeUnit)
case object Year extends TimeUnit(SqlTimeUnit.Year)
case object Month extends TimeUnit(SqlTimeUnit.Month)
case object Day extends TimeUnit(SqlTimeUnit.Day)
case object Hour extends TimeUnit(SqlTimeUnit.Hour)
case object Minute extends TimeUnit(SqlTimeUnit.Minute)
case object Second extends TimeUnit(SqlTimeUnit.Second)

def interval(n: Double, unit: TimeUnit)(using QueryContext): TimeInterval =
    TimeInterval(n.toString, unit.unit)

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
                caseBranches.map((i, t) => SqlWhen(i.asSqlExpr, t.asSqlExpr)), 
                Some(a.asExpr(expr).asSqlExpr)
            )
        )

    infix def `else if`[E: AsExpr as a](expr: E)(using 
        SqlBoolean[a.R],
        QueryContext
    ): If[T] =
        If(exprs :+ a.asExpr(expr))

def `if`[E: AsExpr as a](expr: E)(using 
    SqlBoolean[a.R],
    QueryContext
): EmptyIf = 
    EmptyIf(a.asExpr(expr) :: Nil)

def coalesce[A: AsExpr as a, B: AsExpr as b](x: A, y: B)(using 
    o: Return[Unwrap[a.R, Option], Unwrap[b.R, Option], IsOption[b.R]],
    c: QueryContext
): Expr[o.R] =
    Expr(
        SqlExpr.Coalesce(
            a.asExpr(x).asSqlExpr :: b.asExpr(y).asSqlExpr :: Nil
        )
    )

def ifNull[A: AsExpr as a, B: AsExpr as b](x: A, y: B)(using 
    o: Return[Unwrap[a.R, Option], Unwrap[b.R, Option], IsOption[b.R]],
    c: QueryContext
): Expr[o.R] =
    coalesce(x, y)

def nullIf[A: AsExpr as a, B: AsExpr as b](x: A, y: B)(using 
    o: Return[Unwrap[a.R, Option], Unwrap[b.R, Option], false],
    to: ToOption[Expr[a.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NullIf(
                a.asExpr(x).asSqlExpr, b.asExpr(y).asSqlExpr
            )
        )
    )

extension [T: AsExpr as a](expr: T)
    def as[R](using cast: Cast[a.R, R], c: QueryContext): Expr[Option[R]] =
        Expr(SqlExpr.Cast(a.asExpr(expr).asSqlExpr, cast.castType))

def currentRow(using QueryContext): SqlWindowFrameBound = 
    SqlWindowFrameBound.CurrentRow

def unboundedPreceding(using QueryContext): SqlWindowFrameBound = 
    SqlWindowFrameBound.UnboundedPreceding

def unboundedFollowing(using QueryContext): SqlWindowFrameBound = 
    SqlWindowFrameBound.UnboundedFollowing

extension (n: Int)
    def preceding(using QueryContext): SqlWindowFrameBound = 
        SqlWindowFrameBound.Preceding(n)

    def following(using QueryContext): SqlWindowFrameBound = 
        SqlWindowFrameBound.Following(n)

def partitionBy[T: AsGroup as a](partitionValue: T)(using QueryContext, OverContext): Over =
    Over(partitionBy = a.exprs(partitionValue))

def sortBy[T: AsSort as a](sortValue: T)(using QueryContext, OverContext): Over =
    Over(sortBy = a.asSort(sortValue))

def orderBy[T: AsSort as a](sortValue: T)(using QueryContext, OverContext): Over =
    Over(sortBy = a.asSort(sortValue))

def ^(using QueryContext, MatchRecognizeContext): Pattern =
    new Pattern(SqlRowPatternTerm.Circumflex(None))

def $(using QueryContext, MatchRecognizeContext): Pattern =
    new Pattern(SqlRowPatternTerm.Circumflex(None))

def permute(terms: Pattern*)(using QueryContext, MatchRecognizeContext): Pattern =
    new Pattern(SqlRowPatternTerm.Permute(terms.toList.map(_.pattern), None))

def exclusion(term: Pattern)(using QueryContext, MatchRecognizeContext): Pattern =
    new Pattern(SqlRowPatternTerm.Exclusion(term.pattern, None))

def `final`[T: AsExpr as a](x: T)(using QueryContext, MatchRecognizeContext): Expr[a.R] =
    Expr(SqlExpr.MatchPhase(a.asExpr(x).asSqlExpr, SqlMatchPhase.Final))

def running[T: AsExpr as a](x: T)(using QueryContext, MatchRecognizeContext): Expr[a.R] =
    Expr(SqlExpr.MatchPhase(a.asExpr(x).asSqlExpr, SqlMatchPhase.Running))

extension [T](table: T)(using t: AsTable[T], r: AsRecognizeTable[t.R])
    def matchRecognize[N <: Tuple, V <: Tuple](
        f: MatchRecognizeContext ?=> t.R => RecognizeTable[N, V]
    )(using 
        QueryContext
    ): RecognizeTable[N, V] =
        given MatchRecognizeContext = new MatchRecognizeContext
        val initialTable = r.asRecognizeTable(t.table(table)._1)
        f(initialTable)

extension [T](table: T)(using r: AsRecognizeTable[T])
    def partitionBy[P: AsGroup as a](partitionValue: P)(using 
        QueryContext, 
        MatchRecognizeContext
    ): RecognizePredefine[T] =
        RecognizePredefine(r.setPartitionBy(table, a.exprs(partitionValue).map(_.asSqlExpr)))

    def sortBy[S: AsSort as a](sortValue: S)(using 
        QueryContext, 
        MatchRecognizeContext
    ): RecognizePredefine[T] =
        val sort = a.asSort(sortValue).map(_.asSqlOrderBy)
        RecognizePredefine(r.setOrderBy(table, sort))

    def orderBy[S: AsSort as a](sortValue: S)(using 
        QueryContext, 
        MatchRecognizeContext
    ): RecognizePredefine[T] =
        sortBy(sortValue)

    def oneRowPerMatch(using 
        QueryContext, 
        MatchRecognizeContext
    ): RecognizePredefine[T] =
        RecognizePredefine(r.setPerMatch(table, SqlPatternRowsPerMatchMode.OneRow))

    def allRowsPerMatch(using 
        QueryContext, 
        MatchRecognizeContext
    ): RecognizePredefine[T] =
        RecognizePredefine(r.setPerMatch(table, SqlPatternRowsPerMatchMode.AllRows(None)))