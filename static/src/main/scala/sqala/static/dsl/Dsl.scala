package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.{SqlQuery, SqlSetOperator, SqlWithItem}
import sqala.ast.table.*
import sqala.static.dsl.statement.dml.*
import sqala.static.dsl.statement.query.*
import sqala.static.dsl.table.*
import sqala.static.metadata.*

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}
import scala.annotation.targetName
import scala.compiletime.ops.boolean.||

extension [T](data: List[T])
    def toView[V](using m: ViewMapping[T, V]): List[V] =
        m.mapToList(data)

def query[T](q: QueryContext ?=> T): T =
    given QueryContext = QueryContext(0)
    q

inline def insert[T <: Product]: Insert[T, InsertTable] =
    Insert.apply[T]

inline def update[T <: Product](using c: QueryContext = QueryContext(0)): Update[T, UpdateTable] =
    Update.apply[T]

inline def delete[T <: Product](using c: QueryContext): Delete[T] =
    Delete.apply[T]

extension [T, AT](x: T)(using t: AsTable[T], refl: t.R <:< Table[AT, Column, CanNotInFrom], c: QueryContext)
    def withName(tableName: String): Table[AT, Column, CanInFrom] =
        val (table, _) = t.table(x)
        val baseTable = table.asInstanceOf[Table[AT, Column, CanInFrom]]
        baseTable
            .copy(
                __metaData__ = baseTable.__metaData__.copy(tableName = tableName),
                __sqlTable__ = baseTable.__sqlTable__.copy(name = tableName)
            )

    inline def exclude[N <: Tuple]: ExcludedTable[
        ExcludeName[
            N,
            Names[From[Unwrap[AT, Option]]]
        ],
        ExcludeValue[
            N,
            Names[From[Unwrap[AT, Option]]],
            Tuple.Map[DropNames[From[Unwrap[AT, Option]]], [x] =>> MapField[x, AT, Column]]
        ],
        CanInFrom
    ] =
        val (table, _) = t.table(x)
        ExcludedTable[AT, N](table.asInstanceOf[Table[AT, Column, CanNotInFrom]])

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

def any[T: AsExpr as a, S <: QuerySize](query: Query[T, S])(using QueryContext): SubLink[a.R] =
    SubLink(SqlSubLinkQuantifier.Any, query.tree)

def all[T: AsExpr as a, S <: QuerySize](query: Query[T, S])(using QueryContext): SubLink[a.R] =
    SubLink(SqlSubLinkQuantifier.All, query.tree)

def exists[T, S <: QuerySize](query: Query[T, S])(using QueryContext): Expr[Boolean, Value] =
    Expr(
        SqlExpr.SubLink(
            SqlSubLinkQuantifier.Exists,
            query.tree
        )
    )

case class Unnest[T](x: Option[T])

def unnest[T: AsExpr as a](x: T)(using
    f: UnnestFlatten[a.R],
    c: QueryContext
): FuncTable[Unnest[f.R], Column, CanInFrom] =
    val alias = c.fetchAlias
    val sqlTable: SqlTable.Func = SqlTable.Func(
        false,
        "UNNEST",
        a.asExpr(x).asSqlExpr :: Nil,
        false,
        Some(SqlTableAlias(alias, "x" :: Nil)),
        None
    )
    FuncTable(Some(alias), "x" :: Nil, "x" :: Nil, sqlTable)

case class UnnestWithOrdinal[T](x: Option[T], ordinal: Int)

def unnestWithOrdinal[T: AsExpr as a](x: T)(using
    f: UnnestFlatten[a.R],
    c: QueryContext
): FuncTable[UnnestWithOrdinal[f.R], Column, CanInFrom] =
    val alias = c.fetchAlias
    val sqlTable: SqlTable.Func = SqlTable.Func(
        false,
        "UNNEST",
        a.asExpr(x).asSqlExpr :: Nil,
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
    t: ToTuple[p.R],
    c: QueryContext
): JsonTable[JsonTableColumnNameFlatten[N, V], t.R, CanInFrom] =
    val alias = c.fetchAlias
    JsonTable(ae.asExpr(expr).asSqlExpr, ap.asExpr(path).asSqlExpr, Some(alias), columns)

def ordinalColumn(using QueryContext, JsonContext): JsonTableOrdinalColumn =
    new JsonTableOrdinalColumn

class JsonTablePathColumnPart[T]:
    def apply[P: AsExpr as ap](path: P)(using
        sp: AsSqlExpr[T],
        s: SqlString[ap.R],
        o: KindOperation[ap.K, Column],
        qc: QueryContext,
        jc: JsonContext
    ): JsonTablePathColumn[T] =
        JsonTablePathColumn(ap.asExpr(path).asSqlExpr, sp.sqlType)

def pathColumn[T: AsSqlExpr](using QueryContext, JsonContext): JsonTablePathColumnPart[T] =
    new JsonTablePathColumnPart

def existsColumn[P: AsExpr as ap](path: P)(using
    SqlString[ap.R],
    KindOperation[ap.K, Column],
    QueryContext,
    JsonContext
): JsonTableExistsColumn =
    JsonTableExistsColumn(ap.asExpr(path).asSqlExpr)

def columns[N <: Tuple, V <: Tuple](c: JsonContext ?=> NamedTuple[N, V])(using
    QueryContext
): JsonTableColumns[N, V] =
    given JsonContext = new JsonContext
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
    c: JsonContext ?=> NamedTuple[N, V]
)(using QueryContext, JsonContext): JsonTableNestedColumns[N, V] =
    given JsonContext = new JsonContext
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
    val selectItems = s.asSelectItems(params, 1)
    val tree: SqlQuery.Select = SqlQuery.Select(
        None,
        selectItems,
        fromTable :: Nil,
        None,
        None,
        None,
        Nil,
        None,
        None
    )
    SelectQuery(params, tree)

def grouping[T: AsSqlExpr](x: Expr[T, Grouped])(using QueryContext, GroupingContext): Expr[Int, Agg] =
    Expr(
        SqlExpr.Grouping(
            x.asSqlExpr :: Nil
        )
    )

extension [T: AsExpr as a](x: T)
    def asExpr(using QueryContext): Expr[a.R, a.K] =
        a.asExpr(x)

def level()(using QueryContext, ConnectByContext): Expr[Int, Column] =
    Expr(SqlExpr.Column(Some(tableCte), columnPseudoLevel))

extension (n: Int)(using QueryContext)
    def year: Interval =
        Interval(n.toString, SqlTimeUnit.Year)

    def month: Interval =
        Interval(n.toString, SqlTimeUnit.Month)

    def day: Interval =
        Interval(n.toString, SqlTimeUnit.Day)

    def hour: Interval =
        Interval(n.toString, SqlTimeUnit.Hour)

    def minute: Interval =
        Interval(n.toString, SqlTimeUnit.Minute)

    def second: Interval =
        Interval(n.toString, SqlTimeUnit.Second)

trait ExtractArg[T]

object ExtractArg:
    given dateTime[T: SqlDateTime]: ExtractArg[T]()

    given interval[T: SqlInterval]: ExtractArg[T]()

extension [T: AsExpr as a](x: T)(using ExtractArg[a.R], QueryContext)
    def year: Expr[Option[BigDecimal], a.K] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Year,
                a.asExpr(x).asSqlExpr
            )
        )

    def month: Expr[Option[BigDecimal], a.K] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Month,
                a.asExpr(x).asSqlExpr
            )
        )

    def day: Expr[Option[BigDecimal], a.K] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Day,
                a.asExpr(x).asSqlExpr
            )
        )

    def hour: Expr[Option[BigDecimal], a.K] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Hour,
                a.asExpr(x).asSqlExpr
            )
        )

    def minute: Expr[Option[BigDecimal], a.K] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Minute,
                a.asExpr(x).asSqlExpr
            )
        )

    def second: Expr[Option[BigDecimal], a.K] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Second,
                a.asExpr(x).asSqlExpr
            )
        )

class EmptyIf[K <: ExprKind](private[sqala] val exprs: List[Expr[?, ?]]):
    infix def `then`[E: AsExpr as a](expr: E)(using
        o: KindOperation[K, a.K],
        c: QueryContext
    ): IfThen[a.R, o.R] =
        IfThen(exprs :+ a.asExpr(expr))

class If[T, K <: ExprKind](private[sqala] val exprs: List[Expr[?, ?]]):
    infix def `then`[R: AsExpr as a](expr: R)(using
        r: Return[Unwrap[T, Option], Unwrap[a.R, Option], IsOption[T] || IsOption[R]],
        o: KindOperation[K, a.K],
        c: QueryContext
    ): IfThen[r.R, o.R] =
        IfThen(exprs :+ a.asExpr(expr))

class IfThen[T, K <: ExprKind](private[sqala] val exprs: List[Expr[?, ?]]):
    infix def `else`[R: AsExpr as a](expr: R)(using
        r: Return[Unwrap[T, Option], Unwrap[a.R, Option], IsOption[T] || IsOption[R]],
        o: KindOperation[K, a.K],
        c: QueryContext
    ): Expr[r.R, o.R] =
        val caseBranches =
            exprs.grouped(2).toList.map(i => (i(0), i(1)))
        Expr(
            SqlExpr.Case(
                caseBranches.map((i, t) => SqlWhen(i.asSqlExpr, t.asSqlExpr)),
                Some(a.asExpr(expr).asSqlExpr)
            )
        )

    infix def `else if`[E: AsExpr as a](expr: E)(using
        b: SqlBoolean[a.R],
        o: KindOperation[K, a.K],
        c: QueryContext
    ): If[T, o.R] =
        If(exprs :+ a.asExpr(expr))

def `if`[E: AsExpr as a](expr: E)(using
    b: SqlBoolean[a.R],
    o: KindOperation[a.K, Value],
    c: QueryContext
): EmptyIf[o.R] =
    EmptyIf(a.asExpr(expr) :: Nil)

def coalesce[A: AsExpr as a, B: AsExpr as b](x: A, y: B)(using
    r: Return[Unwrap[a.R, Option], Unwrap[b.R, Option], IsOption[b.R]],
    o: KindOperation[a.K, b.K],
    c: QueryContext
): Expr[r.R, o.R] =
    Expr(
        SqlExpr.Coalesce(
            a.asExpr(x).asSqlExpr :: b.asExpr(y).asSqlExpr :: Nil
        )
    )

def ifNull[A: AsExpr as a, B: AsExpr as b](x: A, y: B)(using
    r: Return[Unwrap[a.R, Option], Unwrap[b.R, Option], IsOption[b.R]],
    o: KindOperation[a.K, b.K],
    c: QueryContext
): Expr[r.R, o.R] =
    coalesce(x, y)

def nullIf[A: AsExpr as a, B: AsExpr as b](x: A, y: B)(using
    r: Return[Unwrap[a.R, Option], Unwrap[b.R, Option], false],
    o: KindOperation[a.K, b.K],
    to: ToOption[Expr[a.R, o.R]],
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
    def as[R](using cast: Cast[a.R, R], c: QueryContext): Expr[Option[R], a.K] =
        Expr(SqlExpr.Cast(a.asExpr(expr).asSqlExpr, cast.castType))

extension [T, K <: ExprKind](expr: Expr[T, K])
    def over()(using CanInvokeOver[K], QueryContext): Expr[T, WindowEmpty] =
        Expr(
            SqlExpr.Window(
                expr.asSqlExpr,
                SqlWindow(
                    Nil,
                    Nil,
                    None
                )
            )
        )

    def over[OK <: ExprKind](over: OverContext ?=> Over[OK])(using
        w: WindowKind[OK],
        c: CanInvokeOver[K],
        q: QueryContext
    ): Expr[T, w.R] =
        given OverContext = new OverContext
        val o = over
        Expr(
            SqlExpr.Window(
                expr.asSqlExpr,
                SqlWindow(
                    o.partitionBy.map(_.asSqlExpr),
                    o.sortBy.map(_.asSqlOrderBy),
                    o.frame
                )
            )
        )

extension [T](expr: Expr[T, Column])
    @targetName("to")
    def :=[R: AsExpr as a](updateExpr: R)(using
        Compare[T, a.R],
        UpdateSetContext
    ): UpdatePair = expr match
        case Expr(SqlExpr.Column(_, columnName)) =>
            UpdatePair(columnName, a.asExpr(updateExpr).asSqlExpr)
        case _ =>
            throw MatchError(expr)

def currentRow(using QueryContext): SqlWindowFrameBound =
    SqlWindowFrameBound.CurrentRow

def unboundedPreceding(using QueryContext): SqlWindowFrameBound =
    SqlWindowFrameBound.UnboundedPreceding

def unboundedFollowing(using QueryContext): SqlWindowFrameBound =
    SqlWindowFrameBound.UnboundedFollowing

extension [T: AsExpr as a](n: T)(using a.K =:= Value)
    def preceding(using QueryContext): SqlWindowFrameBound =
        SqlWindowFrameBound.Preceding(n.asExpr.asSqlExpr)

    def following(using QueryContext): SqlWindowFrameBound =
        SqlWindowFrameBound.Following(n.asExpr.asSqlExpr)

def partitionBy[T](partitionValue: T)(using
    a: AsPartition[T],
    c: QueryContext,
    oc: OverContext
): Over[a.K] =
    Over(partitionBy = a.asExprs(partitionValue))

def sortBy[T](sortValue: T)(using
    a: AsOverSort[T],
    c: QueryContext,
    oc: OverContext
): Over[a.K] =
    Over(sortBy = a.asSorts(sortValue))

def orderBy[T](sortValue: T)(using
    a: AsOverSort[T],
    c: QueryContext,
    oc: OverContext
): Over[a.K] =
    Over(sortBy = a.asSorts(sortValue))

def ^(using QueryContext, MatchRecognizeContext): RecognizePatternTerm =
    new RecognizePatternTerm(SqlRowPatternTerm.Circumflex(None))

def $(using QueryContext, MatchRecognizeContext): RecognizePatternTerm =
    new RecognizePatternTerm(SqlRowPatternTerm.Circumflex(None))

def permute(terms: RecognizePatternTerm*)(using QueryContext, MatchRecognizeContext): RecognizePatternTerm =
    new RecognizePatternTerm(SqlRowPatternTerm.Permute(terms.toList.map(_.pattern), None))

def exclusion(term: RecognizePatternTerm)(using QueryContext, MatchRecognizeContext): RecognizePatternTerm =
    new RecognizePatternTerm(SqlRowPatternTerm.Exclusion(term.pattern, None))

def `final`[T: AsExpr as a](x: T)(using
    KindOperation[a.K, Agg],
    QueryContext,
    MatchRecognizeContext
): Expr[a.R, a.K] =
    Expr(SqlExpr.MatchPhase(SqlMatchPhase.Final, a.asExpr(x).asSqlExpr))

def running[T: AsExpr as a](x: T)(using
    KindOperation[a.K, Agg],
    QueryContext,
    MatchRecognizeContext
): Expr[a.R, a.K] =
    Expr(SqlExpr.MatchPhase(SqlMatchPhase.Running, a.asExpr(x).asSqlExpr))

extension [T](table: T)(using t: AsTable[T], r: AsRecognizeTable[t.R])
    def matchRecognize[N <: Tuple, V <: Tuple](
        f: MatchRecognizeContext ?=> t.R => RecognizeTable[N, V, CanInFrom]
    )(using
        QueryContext
    ): RecognizeTable[N, V, CanInFrom] =
        given MatchRecognizeContext = new MatchRecognizeContext
        val initialTable = r.asRecognizeTable(t.table(table)._1)
        f(initialTable)

extension [T](table: T)(using r: AsRecognizeTable[T])
    def partitionBy[P: AsRecognizePartition as a](partitionValue: P)(using
        QueryContext,
        MatchRecognizeContext
    ): RecognizePredefine[T] =
        RecognizePredefine(r.setPartitionBy(table, a.asExprs(partitionValue).map(_.asSqlExpr)))

    def sortBy[S: AsColumnSort as a](sortValue: S)(using
        QueryContext,
        MatchRecognizeContext
    ): RecognizePredefine[T] =
        val sort = a.asSorts(sortValue).map(_.asSqlOrderBy)
        RecognizePredefine(r.setOrderBy(table, sort))

    def orderBy[S: AsColumnSort as a](sortValue: S)(using
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

extension [T](x: T)(using at: AsTable[T])
    def pivot[N <: Tuple, V <: Tuple](using
        ap: AsPivotTable[at.R],
        qc: QueryContext
    )(f: PivotContext ?=> ap.R => SubQueryTable[N, V, CanInFrom]): SubQueryTable[N, V, CanInFrom] =
        given PivotContext = new PivotContext
        f(ap.table(at.table(x)._1))

extension [T: AsExpr as a](x: T)
    def within[N <: Tuple, V <: Tuple](items: NamedTuple[N, V])(using
        av: AsExpr[V],
        i: CanIn[T, V],
        c: CanInAgg[i.K],
        qc: QueryContext,
        pc: PivotContext
    ): PivotWithin[N] =
        PivotWithin[N](a.asExpr(x).asSqlExpr, av.asExprs(items.toTuple).map(_.asSqlExpr))

def createFunction[T](name: String, args: List[Expr[?, ?]])(using QueryContext): Expr[T, Value] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            name,
            args.map(_.asSqlExpr),
            Nil,
            Nil,
            None
        )
    )

def createBinaryExpr[T](left: Expr[?, ?], operator: String, right: Expr[?, ?])(using QueryContext): Expr[T, Value] =
    Expr(
        SqlExpr.Binary(
            left.asSqlExpr,
            SqlBinaryOperator.Custom(operator),
            right.asSqlExpr
        )
    )

inline def createTableFunction[T](
    name: String,
    args: List[Expr[?, ?]],
    withOrdinal: Boolean
)(using
    c: QueryContext
): FuncTable[T, Column, CanInFrom] =
    val metaData = TableMacro.tableMetaData[T]
    val alias = c.fetchAlias
    val sqlTable: SqlTable.Func = SqlTable.Func(
        false,
        name,
        args.map(_.asSqlExpr),
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
)(f: GraphContext ?=> Graph[N, V] => GraphTable[TN, TV, CanInFrom])(using
    QueryContext
): GraphTable[TN, TV, CanInFrom] =
    given GraphContext = new GraphContext
    f(graph)

def withRecursive[N <: Tuple, V <: Tuple, S <: QuerySize, UN <: Tuple, UV <: Tuple, US <: QuerySize, R, RS <: QuerySize](
    baseQuery: Query[NamedTuple[N, V], S]
)(using
    p: AsTableParam[V],
    tp: ToTuple[p.R]
)(
    f: RecursiveTable[N, tp.R] => Query[NamedTuple[UN, UV], US]
)(using
    u: Union[V, UV],
    tu: ToTuple[u.R],
    up: AsTableParam[tu.R],
    t: ToTuple[up.R]
)(
    g: RecursiveTable[N, t.R] => Query[R, RS]
)(using
    m: AsMap[V],
    c: QueryContext
): Query[R, ManyRows] =
    val alias = c.fetchAlias
    val withTable = RecursiveTable[N, V](Some(alias))
    val unionQuery = f(withTable)
    val finalTable = RecursiveTable[N, tu.R](Some(tableCte))
    val finalQuery = g(finalTable)
    val columns = m.asSelectItems(baseQuery.params.toTuple, 1).map(_.alias.get)
    val withTree = SqlQuery.Set(
        baseQuery.tree,
        SqlSetOperator.Union(Some(SqlQuantifier.All)),
        unionQuery.tree,
        Nil,
        None,
        None
    )
    val tree = SqlQuery.Cte(
        true,
        SqlWithItem(tableCte, withTree, columns) :: Nil,
        finalQuery.tree,
        None
    )
    Query(finalQuery.params, tree)