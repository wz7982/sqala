package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.{SqlQuery, SqlSetOperator, SqlWithItem}
import sqala.ast.table.*
import sqala.static.dsl.statement.dml.*
import sqala.static.dsl.statement.query.*
import sqala.static.dsl.table.*
import sqala.static.metadata.*

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
    def join[B](b: => B)(using ta: AsTable[A], tb: AsTable[B], j: TableJoin[ta.R, tb.R]): JoinPart[j.R] = 
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Inner, rightSqlTable, None))

    def joinLateral[B](using ta: AsTable[A])(f: ta.R => B)(using 
        tb: AsLateralTable[B], 
        j: TableJoin[ta.R, tb.R]
    ): JoinPart[j.R] =
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(f(leftTable))
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Inner, rightSqlTable, None))

    def crossJoin[B](b: => B)(using ta: AsTable[A], tb: AsTable[B], j: TableJoin[ta.R, tb.R]): JoinTable[j.R] = 
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(b)
        val params = j.join(leftTable, rightTable)
        JoinTable(params, SqlTable.Join(leftSqlTable, SqlJoinType.Cross, rightSqlTable, None))

    def crossJoinLateral[B](using ta: AsTable[A])(f: ta.R => B)(using 
        tb: AsLateralTable[B], 
        j: TableJoin[ta.R, tb.R]
    ): JoinTable[j.R] =
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(f(leftTable))
        val params = j.join(leftTable, rightTable)
        JoinTable(params, SqlTable.Join(leftSqlTable, SqlJoinType.Cross, rightSqlTable, None))

    def leftJoin[B](b: => B)(using ta: AsTable[A], tb: AsTable[B], j: TableLeftJoin[ta.R, tb.R]): JoinPart[j.R] = 
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Left, rightSqlTable, None))

    def leftJoinLateral[B](using ta: AsTable[A])(f: ta.R => B)(using 
        tb: AsLateralTable[B], 
        j: TableLeftJoin[ta.R, tb.R]
    ): JoinPart[j.R] =
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(f(leftTable))
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Left, rightSqlTable, None))

    def rightJoin[B](b: => B)(using ta: AsTable[A], tb: AsTable[B], j: TableRightJoin[ta.R, tb.R]): JoinPart[j.R] = 
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Right, rightSqlTable, None))

    def fullJoin[B](b: => B)(using ta: AsTable[A], tb: AsTable[B], j: TableFullJoin[ta.R, tb.R]): JoinPart[j.R] = 
        val (leftTable, leftSqlTable) = ta.table(a)
        val (rightTable, rightSqlTable) = tb.table(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Full, rightSqlTable, None))

def any[T: AsExpr as a](query: Query[T]): SubLink[a.R] =
    SubLink(query.tree, SqlSubLinkQuantifier.Any)

def all[T: AsExpr as a](query: Query[T]): SubLink[a.R] =
    SubLink(query.tree, SqlSubLinkQuantifier.All)

def exists[T](query: Query[T]): Expr[Option[Boolean]] =
    Expr(
        SqlExpr.SubLink(
            query.tree,
            SqlSubLinkQuantifier.Exists
        )
    )

case class Unnest[T](x: Option[T])

def unnest[T: AsExpr as a](x: T)(using
    f: UnnestFlatten[a.R],
    c: QueryContext
): FuncTable[Unnest[f.R]] =
    val alias = c.fetchAlias
    val sqlTable: SqlTable.Func = SqlTable.Func(
        "UNNEST",
        a.asExpr(x).asSqlExpr :: Nil,
        false,
        false,
        Some(SqlTableAlias(alias, "x" :: Nil)),
        None
    )
    FuncTable(Some(alias), "x" :: Nil, "x" :: Nil, sqlTable)

case class UnnestWithOrdinal[T](x: Option[T], ordinal: Int)

def unnestWithOrdinal[T: AsExpr as a](x: T)(using
    f: UnnestFlatten[a.R],
    c: QueryContext
): FuncTable[UnnestWithOrdinal[f.R]] =
    val alias = c.fetchAlias
    val sqlTable: SqlTable.Func = SqlTable.Func(
        "UNNEST",
        a.asExpr(x).asSqlExpr :: Nil,
        false,
        true,
        Some(SqlTableAlias(alias, "x" :: "ordinal" :: Nil)),
        None
    )
    FuncTable(Some(alias), "x" :: "ordinal" :: Nil, "x" :: "ordinal" :: Nil, sqlTable)

def jsonTable[E: AsExpr as ae, P: AsExpr as ap, N <: Tuple, V <: Tuple](
    expr: E,
    path: P,
    columns: JsonTableColumns[N, V]
)(using
    s: SqlString[ap.R],
    p: AsTableParam[JsonTableColumnFlatten[V]],
    c: QueryContext
): JsonTable[JsonTableColumnNameFlatten[N, V], JsonTableColumnFlatten[V]] =
    val alias = c.fetchAlias
    JsonTable(ae.asExpr(expr).asSqlExpr, ap.asExpr(path).asSqlExpr, Some(alias), columns)

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

extension [T: AsExpr as a](x: T)
    def asExpr: Expr[a.R] = a.asExpr(x)

def level()(using QueryContext, ConnectByContext): Expr[Int] =
    Expr(SqlExpr.Column(Some(tableCte), columnPseudoLevel))

sealed class TimeUnit(val unit: SqlTimeUnit)
case object Year extends TimeUnit(SqlTimeUnit.Year)
case object Month extends TimeUnit(SqlTimeUnit.Month)
case object Day extends TimeUnit(SqlTimeUnit.Day)
case object Hour extends TimeUnit(SqlTimeUnit.Hour)
case object Minute extends TimeUnit(SqlTimeUnit.Minute)
case object Second extends TimeUnit(SqlTimeUnit.Second)

def interval(n: Double, unit: TimeUnit)(using QueryContext): Interval =
    Interval(n.toString, unit.unit)

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

def ^(using QueryContext, MatchRecognizeContext): RecognizePatternTerm =
    new RecognizePatternTerm(SqlRowPatternTerm.Circumflex(None))

def $(using QueryContext, MatchRecognizeContext): RecognizePatternTerm =
    new RecognizePatternTerm(SqlRowPatternTerm.Circumflex(None))

def permute(terms: RecognizePatternTerm*)(using QueryContext, MatchRecognizeContext): RecognizePatternTerm =
    new RecognizePatternTerm(SqlRowPatternTerm.Permute(terms.toList.map(_.pattern), None))

def exclusion(term: RecognizePatternTerm)(using QueryContext, MatchRecognizeContext): RecognizePatternTerm =
    new RecognizePatternTerm(SqlRowPatternTerm.Exclusion(term.pattern, None))

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
        RecognizePredefine(r.setPerMatch(table, SqlRecognizePatternRowsPerMatchMode.OneRow))

    def allRowsPerMatch(using 
        QueryContext, 
        MatchRecognizeContext
    ): RecognizePredefine[T] =
        RecognizePredefine(r.setPerMatch(table, SqlRecognizePatternRowsPerMatchMode.AllRows(None)))

extension [T](x: T)(using at: AsTable[T], ap: AsPivotTable[at.R])
    def pivot[N <: Tuple, V <: Tuple](f: PivotContext ?=> ap.R => SubQueryTable[N, V])(using 
        QueryContext
    ): SubQueryTable[N, V] =
        given PivotContext = new PivotContext
        f(ap.table(at.table(x)._1))

extension [T: AsExpr as a](x: T)
    def within[N <: Tuple, V <: Tuple](items: NamedTuple[N, V])(using
        av: AsExpr[V],
        in: CanIn[a.R, V],
        c: QueryContext,
        pc: PivotContext
    ): PivotWithin[N] =
        PivotWithin[N](a.asExpr(x).asSqlExpr, av.exprs(items.toTuple).map(_.asSqlExpr))

def createFunction[T](name: String, args: List[Expr[?]])(using QueryContext): Expr[T] =
    Expr(
        SqlExpr.StandardFunc(
            name,
            args.map(_.asSqlExpr),
            None,
            Nil,
            Nil,
            None
        )
    )

def createBinaryExpr[T](left: Expr[?], operator: String, right: Expr[?])(using QueryContext): Expr[T] =
    Expr(
        SqlExpr.Binary(
            left.asSqlExpr,
            SqlBinaryOperator.Custom(operator),
            right.asSqlExpr
        )
    )

inline def createTableFunction[T](
    name: String, 
    args: List[Expr[?]],
    withOrdinal: Boolean
)(using
    c: QueryContext
): FuncTable[T] =
    val metaData = TableMacro.tableMetaData[T]
    val alias = c.fetchAlias
    val sqlTable: SqlTable.Func = SqlTable.Func(
        name,
        args.map(_.asSqlExpr),
        false,
        withOrdinal,
        Some(SqlTableAlias(alias, metaData.columnNames)),
        None
    )
    FuncTable(Some(alias), metaData.fieldNames, metaData.columnNames, sqlTable)

inline def vertex[T]: GraphVertex[T] =
    val metaData = TableMacro.tableMetaData[T]
    GraphVertex(None, metaData)

inline def vertex[T](label: String): GraphVertex[T] =
    val metaData = TableMacro.tableMetaData[T]
    GraphVertex(None, metaData.copy(tableName = label))

inline def edge[T]: GraphEdge[T] =
    val metaData = TableMacro.tableMetaData[T]
    GraphEdge(None, metaData, None, None)

inline def edge[T](label: String): GraphEdge[T] =
    val metaData = TableMacro.tableMetaData[T]
    GraphEdge(None, metaData.copy(tableName = label), None, None)

def createGraph[N <: Tuple, V <: Tuple](name: String)(labels: NamedTuple[N, V]): Graph[N, V] =
    Graph(name, labels.toTuple)

extension (s: String)
    infix def is[V](v: GraphVertex[V])(using 
        QueryContext, 
        GraphContext
    ): GraphPattern[Tuple1[s.type], Tuple1[GraphVertex[V]]] =
        GraphPattern(
            Tuple1(v), 
            SqlGraphPatternTerm.Vertex(
                v.__alias__, 
                Some(SqlGraphLabel.Label(v.__metaData__.tableName)), 
                None
            ) :: Nil
        )

    infix def is[V](v: GraphEdge[V])(using 
        QueryContext, 
        GraphContext
    ): GraphPattern[Tuple1[s.type], Tuple1[GraphEdge[V]]] =
        val pattern = 
            v.__quantifier__.map: q =>
                SqlGraphPatternTerm.Quantified(
                    SqlGraphPatternTerm.Edge(
                        SqlGraphSymbol.Dash,
                        v.__alias__,
                        Some(SqlGraphLabel.Label(v.__metaData__.tableName)),
                        v.__where__,
                        SqlGraphSymbol.Dash
                    ),
                    q
                )
            .getOrElse:
                SqlGraphPatternTerm.Edge(
                    SqlGraphSymbol.Dash,
                    v.__alias__,
                    Some(SqlGraphLabel.Label(v.__metaData__.tableName)),
                    v.__where__,
                    SqlGraphSymbol.Dash
                )
        GraphPattern(Tuple1(v), pattern :: Nil)

def graphTable[N <: Tuple, V <: Tuple, TN <: Tuple, TV <: Tuple](
    graph: Graph[N, V]
)(f: GraphContext ?=> Graph[N, V] => GraphTable[TN, TV])(using 
    QueryContext
): GraphTable[TN, TV] =
    given GraphContext = new GraphContext
    f(graph)

def withRecursive[N <: Tuple, V <: Tuple, UN <: Tuple, UV <: Tuple, R](
    baseQuery: Query[NamedTuple[N, V]]
)(
    f: RecursiveTable[N, V] => Query[NamedTuple[UN, UV]]
)(using
    u: Union[V, UV],
    t: ToTuple[u.R]
)(
    g: RecursiveTable[N, t.R] => Query[R]
)(using
    p: AsTableParam[V],
    pt: AsTableParam[t.R],
    m: AsMap[V],
    c: QueryContext
): Query[R] =
    val alias = c.fetchAlias
    val withTable = RecursiveTable[N, V](Some(alias))
    val unionQuery = f(withTable)
    val finalTable = RecursiveTable[N, t.R](Some(tableCte))
    val finalQuery = g(finalTable)
    val columns = m.selectItems(baseQuery.params.toTuple, 1).map(_.alias.get)
    val withTree = SqlQuery.Set(
        baseQuery.tree,
        SqlSetOperator.Union(Some(SqlQuantifier.All)),
        unionQuery.tree,
        Nil,
        None,
        None
    )
    val tree = SqlQuery.Cte(
        SqlWithItem(tableCte, withTree, columns) :: Nil,
        true,
        finalQuery.tree,
        None
    )
    Query(finalQuery.params, tree)