package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.{SqlQuery, SqlSetOperator, SqlWithItem}
import sqala.ast.table.*
import sqala.metadata.*
import sqala.static.dsl.statement.dml.*
import sqala.static.dsl.statement.query.*
import sqala.static.dsl.table.*

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}
import scala.annotation.targetName
import scala.compiletime.ops.boolean.||
import scala.compiletime.ops.int.{+, >}
import sqala.static.{dsl => a}

def query[T](q: QueryContext[0] ?=> T): T =
    given QueryContext[0] = QueryContext(0)
    q

inline def insert[T <: Product]: Insert[T, InsertTable] =
    Insert.apply[T]

inline def update[T <: Product]: Update[T, UpdateTable] =
    Update.apply[T]

inline def delete[T <: Product]: Delete[T] =
    Delete.apply[T]

private[sqala] val tableCte = "__cte__"

private[sqala] val columnPseudoLevel = "__pseudo__level__"

def from[T, CL <: Int](using qc: QueryContext[CL] = QueryContext[0](0))(
    tables: QueryContext[CL + 1] ?=> T
)(using
    at: AsTable[T, CL + 1],
    as: AsSelect[at.R]
): SelectQuery[at.R, at.OKS, CL + 1] =
    given QueryContext[CL + 1] = qc.asInstanceOf[QueryContext[CL + 1]]
    val (params, fromTable) = at.asTable(tables)
    val selectItems = as.asSelectItems(params, 1)
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

def withRecursive[N <: Tuple, V <: Tuple, S <: QuerySize, UN <: Tuple, UV <: Tuple, US <: QuerySize, R, RS <: QuerySize, CL <: Int](
    baseQuery: Query[NamedTuple[N, V], EmptyTuple, CL, S]
)(using
    qc: QueryContext[CL],
    p: AsTableParam[V, CL],
    tp: ToTuple[p.R]
)(
    f: RecursiveTable[N, tp.R, CL] => Query[NamedTuple[UN, UV], EmptyTuple, CL, US]
)(using
    u: Union[V, UV, CL],
    tu: ToTuple[u.R],
    up: AsTableParam[tu.R, CL],
    t: ToTuple[up.R]
)(
    g: RecursiveTable[N, t.R, CL] => Query[R, EmptyTuple, CL, RS]
)(using
    m: AsMap[V, CL]
): Query[R, EmptyTuple, CL, ManyRows] =
    val alias = qc.fetchAlias
    val withTable = RecursiveTable[N, V, CL](Some(alias))
    val unionQuery = f(withTable)
    val finalTable = RecursiveTable[N, tu.R, CL](Some(tableCte))
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

extension [T, AT, CL <: Int](x: T)(using qc: QueryContext[CL], t: AsTable[T, CL], refl: t.R <:< Table[AT, Column, CL])
    inline def exclude[N <: Tuple]: FromExcluded[
        ExcludeName[
            N,
            Names[From[Unwrap[AT, Option]]]
        ],
        ExcludeValue[
            N,
            Names[From[Unwrap[AT, Option]]],
            Tuple.Map[DropNames[From[Unwrap[AT, Option]]], [x] =>> MapField[x, AT, Column, CL]]
        ],
        CL
    ] =
        val (table, _) = t.asTable(x)
        FromExcluded[AT, N, CL](table.asInstanceOf[Table[AT, Column, CL]])

extension [A, CL <: Int](a: A)(using qc: QueryContext[CL], ta: AsTable[A, CL])
    def join[B](b: B)(using
        tb: AsTable[B, CL],
        j: Join[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL] =
        val (leftTable, leftSqlTable) = ta.asTable(a)
        val (rightTable, rightSqlTable) = tb.asTable(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Inner, rightSqlTable, None))

    def joinLateral[B](f: QueryContext[CL + 1] ?=> ta.R => B)(using
        tb: AsLateralTable[B, CL + 1],
        j: Join[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL] =
        given QueryContext[CL + 1] = qc.asInstanceOf[QueryContext[CL + 1]]
        val (leftTable, leftSqlTable) = ta.asTable(a)
        val (rightTable, rightSqlTable) = tb.asTable(f(leftTable))
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Inner, rightSqlTable, None))

    def crossJoin[B](b: B)(using
        tb: AsTable[B, CL],
        j: Join[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinTable[j.R, c.R, CL] =
        val (leftTable, leftSqlTable) = ta.asTable(a)
        val (rightTable, rightSqlTable) = tb.asTable(b)
        val params = j.join(leftTable, rightTable)
        JoinTable(params, SqlTable.Join(leftSqlTable, SqlJoinType.Cross, rightSqlTable, None))

    def crossJoinLateral[B](f: QueryContext[CL + 1] ?=> ta.R => B)(using
        tb: AsLateralTable[B, CL + 1],
        j: Join[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinTable[j.R, c.R, CL + 1] =
        given QueryContext[CL + 1] = qc.asInstanceOf[QueryContext[CL + 1]]
        val (leftTable, leftSqlTable) = ta.asTable(a)
        val (rightTable, rightSqlTable) = tb.asTable(f(leftTable))
        val params = j.join(leftTable, rightTable)
        JoinTable(params, SqlTable.Join(leftSqlTable, SqlJoinType.Cross, rightSqlTable, None))

    def leftJoin[B](b: B)(using
        tb: AsTable[B, CL],
        j: LeftJoin[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL] =
        val (leftTable, leftSqlTable) = ta.asTable(a)
        val (rightTable, rightSqlTable) = tb.asTable(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Left, rightSqlTable, None))

    def leftJoinLateral[B](f: QueryContext[CL + 1] ?=> ta.R => B)(using
        tb: AsLateralTable[B, CL + 1],
        j: Join[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL + 1] =
        given QueryContext[CL + 1] = qc.asInstanceOf[QueryContext[CL + 1]]
        val (leftTable, leftSqlTable) = ta.asTable(a)
        val (rightTable, rightSqlTable) = tb.asTable(f(leftTable))
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Left, rightSqlTable, None))

    def rightJoin[B](b: B)(using
        tb: AsTable[B, CL],
        j: RightJoin[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL] =
        val (leftTable, leftSqlTable) = ta.asTable(a)
        val (rightTable, rightSqlTable) = tb.asTable(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Right, rightSqlTable, None))

    def fullJoin[B](b: B)(using
        tb: AsTable[B, CL],
        j: FullJoin[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL] =
        val (leftTable, leftSqlTable) = ta.asTable(a)
        val (rightTable, rightSqlTable) = tb.asTable(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Full, rightSqlTable, None))

final case class Unnest[T](x: Option[T])

def unnest[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    r: UnnestReturn[a.R],
    kt: KindToTuple[a.K],
    i: CanInSimpleClause[kt.R],
    e: ExcludeCurrentLevelColumn[kt.R, CL]
): FromFunc[Unnest[r.R], Column, e.R, CL] =
    val alias = qc.fetchAlias
    val sqlTable: SqlTable.Func = SqlTable.Func(
        false,
        "UNNEST",
        a.asExpr(x).asSqlExpr :: Nil,
        false,
        Some(SqlTableAlias(alias, "x" :: Nil)),
        None
    )
    FromFunc(Some(alias), "x" :: Nil, "x" :: Nil, sqlTable)

final case class UnnestWithOrdinal[T](x: Option[T], ordinal: Int)

def unnestWithOrdinal[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    r: UnnestReturn[a.R],
    kt: KindToTuple[a.K],
    i: CanInSimpleClause[kt.R],
    e: ExcludeCurrentLevelColumn[kt.R, CL]
): FromFunc[UnnestWithOrdinal[r.R], Column, e.R, CL] =
    val alias = qc.fetchAlias
    val sqlTable: SqlTable.Func = SqlTable.Func(
        false,
        "UNNEST",
        a.asExpr(x).asSqlExpr :: Nil,
        true,
        Some(SqlTableAlias(alias, "x" :: "ordinal" :: Nil)),
        None
    )
    FromFunc(Some(alias), "x" :: "ordinal" :: Nil, "x" :: "ordinal" :: Nil, sqlTable)

def jsonTable[E, N <: Tuple, V <: Tuple, CL <: Int](
    expr: E,
    path: String,
    columns: JsonContext ?=> JsonTableColumns[N, V]
)(using
    qc: QueryContext[CL],
    a: AsExpr[E, CL],
    p: AsTableParam[JsonTableColumnFlatten[V, CL], CL],
    t: ToTuple[p.R],
    kt: KindToTuple[a.K],
    i: CanInSimpleClause[kt.R],
    e: ExcludeCurrentLevelColumn[kt.R, CL],
    c: CombineKindTuple[EmptyTuple, e.R]
): FromJson[JsonTableColumnNameFlatten[N, V], t.R, c.R, CL] =
    given JsonContext = JsonContext()
    val alias = qc.fetchAlias
    FromJson(a.asExpr(expr).asSqlExpr, path.asExpr.asSqlExpr, Some(alias), columns)

def ordinalColumn[CL <: Int](using QueryContext[CL], JsonContext): JsonTableOrdinalColumn =
    JsonTableOrdinalColumn()

final class JsonTablePathColumnPart[T]:
    def apply[CL <: Int](path: String)(using
        qc: QueryContext[CL],
        jc: JsonContext,
        a: AsSqlExpr[T]
    ): JsonTablePathColumn[T] =
        JsonTablePathColumn(path.asExpr.asSqlExpr, a.sqlType)

def pathColumn[T: AsSqlExpr]: JsonTablePathColumnPart[T] =
    JsonTablePathColumnPart()

def existsColumn[CL <: Int](path: String)(using
    qc: QueryContext[CL],
    jc: JsonContext,
): JsonTableExistsColumn =
    JsonTableExistsColumn(path.asExpr.asSqlExpr)

def nestedColumns[N <: Tuple, V <: Tuple, CL <: Int](
    path: String
)(using
    qc: QueryContext[CL],
    jc: JsonContext
)(
    c: JsonContext ?=> NamedTuple[N, V]
): JsonTableNestedColumns[N, V] =
    val columnList: List[Any] = c.toList
    val jsonColumns = columnList.map:
        case p: JsonTablePathColumn[?] => p
        case o: JsonTableOrdinalColumn => o
        case e: JsonTableExistsColumn => e
        case n: JsonTableNestedColumns[?, ?] => n
    JsonTableNestedColumns(path.asExpr.asSqlExpr, jsonColumns)

def columns[N <: Tuple, V <: Tuple, CL <: Int](c: JsonContext ?=> NamedTuple[N, V])(using
    qc: QueryContext[CL],
    jc: JsonContext
): JsonTableColumns[N, V] =
    val columnList: List[Any] = c.toList
    val jsonColumns = columnList.map:
        case p: JsonTablePathColumn[?] => p
        case o: JsonTableOrdinalColumn => o
        case e: JsonTableExistsColumn => e
        case n: JsonTableNestedColumns[?, ?] => n
    JsonTableColumns(jsonColumns)

inline def vertex[T]: GraphVertexSchema[T] =
    val metaData = TableMacro.tableMetaData[T]
    GraphVertexSchema(None, metaData)

inline def vertex[T](label: String): GraphVertexSchema[T] =
    val metaData = TableMacro.tableMetaData[T]
    GraphVertexSchema(None, metaData.copy(tableName = label))

inline def edge[T]: GraphEdgeSchema[T] =
    val metaData = TableMacro.tableMetaData[T]
    GraphEdgeSchema(None, metaData, None, None)

inline def edge[T](label: String): GraphEdgeSchema[T] =
    val metaData = TableMacro.tableMetaData[T]
    GraphEdgeSchema(None, metaData.copy(tableName = label), None, None)

def createGraph[N <: Tuple, V <: Tuple](name: String)(labels: NamedTuple[N, V]): GraphSchema[N, V] =
    GraphSchema(name, labels.toTuple)

extension [CL <: Int](s: String)(using qc: QueryContext[CL])
    infix def is[V, OKS <: Tuple, T <: GraphPatternTerm[V, OKS, CL]](v: T)(using
        GraphContext
    ): GraphPattern[Tuple1[s.type], Tuple1[T], OKS, CL] =
        GraphPattern(Tuple1(v), v.asTerm :: Nil)

def graphTable[N <: Tuple, V <: Tuple, TN <: Tuple, TV <: Tuple, TOKS <: Tuple, CL <: Int](using
    qc: QueryContext[CL]
)(
    schema: GraphSchema[N, V]
)(using
    t: TransformGraphSchema[V, CL],
    tt: ToTuple[t.R]
)(
    f: GraphContext ?=> Graph[N, tt.R, CL] => FromGraph[TN, TV, TOKS, CL]
): FromGraph[TN, TV, TOKS, CL] =
    given GraphContext = GraphContext()
    val graph =
        Graph[N, tt.R, CL](schema.__name__, tt.toTuple(t.transform(schema.__items__)))
    f(graph)

def ^[CL <: Int](using QueryContext[CL], MatchRecognizeContext): RecognizePatternTerm[CL] =
    RecognizePatternTerm(SqlRowPatternTerm.Circumflex(None))

def $[CL <: Int](using QueryContext[CL], MatchRecognizeContext): RecognizePatternTerm[CL] =
    RecognizePatternTerm(SqlRowPatternTerm.Dollar(None))

def permute[CL <: Int](terms: RecognizePatternTerm[CL]*)(using QueryContext[CL], MatchRecognizeContext): RecognizePatternTerm[CL] =
    RecognizePatternTerm(SqlRowPatternTerm.Permute(terms.toList.map(_.pattern), None))

def exclusion[CL <: Int](term: RecognizePatternTerm[CL])(using QueryContext[CL], MatchRecognizeContext): RecognizePatternTerm[CL] =
    RecognizePatternTerm(SqlRowPatternTerm.Exclusion(term.pattern, None))

def finalized[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext,
    a: AsExpr[T, CL],
    kt: KindToTuple[a.K],
    i: CanInGroupedMap[kt.R]
): Expr[a.R, Agg[kt.R]] =
    Expr(SqlExpr.MatchPhase(SqlMatchPhase.Final, a.asExpr(x).asSqlExpr))

def running[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext,
    a: AsExpr[T, CL],
    kt: KindToTuple[a.K],
    i: CanInGroupedMap[kt.R]
): Expr[a.R, Agg[kt.R]] =
    Expr(SqlExpr.MatchPhase(SqlMatchPhase.Running, a.asExpr(x).asSqlExpr))

extension [T, CL <: Int](table: T)(using qc: QueryContext[CL], t: AsTable[T, CL], r: AsRecognize[t.R])
    def matchRecognize[N <: Tuple, V <: Tuple](
        f: MatchRecognizeContext ?=> t.R => RecognizeMeasures[N, V, CL]
    ): FromRecognize[N, V, t.OKS, CL] =
        given MatchRecognizeContext = MatchRecognizeContext()
        val initialTable = r.asRecognizeTable(t.asTable(table)._1)
        val measures = f(initialTable)
        FromRecognize(measures.__aliasName__, measures.__items__, measures.__sqlTable__)

extension [T, CL <: Int](table: T)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext,
    r: AsRecognize[T]
)
    def partitionBy[P](partitionValue: P)(using
        a: AsRecognizePartition[P, CL]
    ): RecognizePredefine[T, CL] =
        RecognizePredefine(r.setPartitionBy(table, a.asExprs(partitionValue).map(_.asSqlExpr)))

    def sortBy[S](sortValue: S)(using
        a: AsColumnSort[S, CL]
    ): RecognizePredefine[T, CL] =
        val sort = a.asSorts(sortValue).map(_.asSqlOrderBy)
        RecognizePredefine(r.setOrderBy(table, sort))

    def orderBy[S](sortValue: S)(using
        a: AsColumnSort[S, CL]
    ): RecognizePredefine[T, CL] =
        sortBy(sortValue)

    def oneRowPerMatch: RecognizePredefine[T, CL] =
        RecognizePredefine(r.setPerMatch(table, SqlRecognizePatternRowsPerMatchMode.OneRow))

    def allRowsPerMatch: RecognizePredefine[T, CL] =
        RecognizePredefine(r.setPerMatch(table, SqlRecognizePatternRowsPerMatchMode.AllRows(None)))

extension [T, CL <: Int](x: T)(using qc: QueryContext[CL], p: AsPivot[T, CL])
    def pivot[N <: Tuple, V <: Tuple](f: PivotContext ?=> p.R => FromPivot[N, V, p.OKS, CL]): FromPivot[N, V, p.OKS, CL] =
        given PivotContext = PivotContext()
        f(p.asPivot(x))

def grouping[T: AsSqlExpr, K <: Grouped[?], CL <: Int](x: Expr[T, K])(using QueryContext[CL], GroupingContext): Expr[Int, Agg[K *: EmptyTuple]] =
    Expr(
        SqlExpr.Grouping(
            x.asSqlExpr :: Nil
        )
    )

extension [T, CL <: Int](x: T)(using qc: QueryContext[CL], a: AsExpr[T, CL])
    def asExpr: Expr[a.R, a.K] =
        a.asExpr(x)

    def as[R](using c: Cast[a.R, R], kt: KindToTuple[a.K]): Expr[Option[R], Composite[kt.R]] =
        Expr(SqlExpr.Cast(a.asExpr(x).asSqlExpr, c.castType))

    def within[N <: Tuple, V <: Tuple](items: NamedTuple[N, V])(using
        pc: PivotContext,
        av: AsExpr[V, CL],
        i: CanIn[T, V, CL],
        ia: CanInAgg[i.KS],
        e: ExcludeCurrentLevelColumn[i.KS, CL],
        refl: e.R =:= EmptyTuple
    ): PivotWithin[N] =
        PivotWithin[N](a.asExpr(x).asSqlExpr, av.asExprs(items.toTuple).map(_.asSqlExpr))

def level[CL <: Int]()(using QueryContext[CL], ConnectByContext): Expr[Int, Column[CL]] =
    Expr(SqlExpr.Column(Some(tableCte), columnPseudoLevel))

extension [CL <: Int](n: Int)(using QueryContext[CL])
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

    given time[T: SqlTime]: ExtractArg[T]()

    given interval[T: SqlInterval]: ExtractArg[T]()

extension [T, CL <: Int](x: T)(using qc: QueryContext[CL], a: AsExpr[T, CL], kt: KindToTuple[a.K], e: ExtractArg[a.R])
    def year: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Year,
                a.asExpr(x).asSqlExpr
            )
        )

    def month: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Month,
                a.asExpr(x).asSqlExpr
            )
        )

    def day: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Day,
                a.asExpr(x).asSqlExpr
            )
        )

    def hour: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Hour,
                a.asExpr(x).asSqlExpr
            )
        )

    def minute: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Minute,
                a.asExpr(x).asSqlExpr
            )
        )

    def second: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Second,
                a.asExpr(x).asSqlExpr
            )
        )

final case class CaseWhen[T, KS <: Tuple](private[sqala] val exprs: List[Expr[?, ?]]):
    def when[C, R, CL <: Int](cond: C)(result: R)(using
        qc: QueryContext[CL],
        ac: AsExpr[C, CL],
        ar: AsExpr[R, CL],
        b: SqlBoolean[ac.R],
        r: Return[Unwrap[T, Option], Unwrap[ar.R, Option], IsOption[T] || IsOption[ar.R]],
        ktc: KindToTuple[ac.K],
        ktr: KindToTuple[ar.K],
        cc: CombineKindTuple[KS, ktc.R],
        c: CombineKindTuple[cc.R, ktr.R]
    ): CaseWhen[r.R, c.R] =
        CaseWhen(exprs :+ ac.asExpr(cond) :+ ar.asExpr(result))

    def otherwise[R, CL <: Int](result: R)(using
        qc: QueryContext[CL],
        a: AsExpr[R, CL],
        r: Return[Unwrap[T, Option], Unwrap[a.R, Option], IsOption[T] || IsOption[R]],
        kt: KindToTuple[a.K],
        c: CombineKindTuple[KS, kt.R]
    ): Expr[r.R, Composite[c.R]] =
        val caseBranches =
            exprs.grouped(2).toList.map(i => (i(0), i(1)))
        Expr(
            SqlExpr.Case(
                caseBranches.map((i, t) => SqlWhen(i.asSqlExpr, t.asSqlExpr)),
                Some(a.asExpr(result).asSqlExpr)
            )
        )

def caseWhen[C, R, CL <: Int](cond: C)(result: R)(using
    qc: QueryContext[CL],
    ac: AsExpr[C, CL],
    ar: AsExpr[R, CL],
    b: SqlBoolean[ac.R],
    ktc: KindToTuple[ac.K],
    ktr: KindToTuple[ar.K],
    c: CombineKindTuple[ktc.R, ktr.R]
): CaseWhen[ar.R, c.R] =
    CaseWhen(ac.asExpr(cond) :: ar.asExpr(result) :: Nil)

def coalesce[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    r: Return[Unwrap[aa.R, Option], Unwrap[ab.R, Option], IsOption[ab.R]],
    c: CombineKind[aa.K, ab.K]
): Expr[r.R, c.R] =
    Expr(
        SqlExpr.Coalesce(
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil
        )
    )

def ifNull[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    r: Return[Unwrap[aa.R, Option], Unwrap[ab.R, Option], IsOption[ab.R]],
    c: CombineKind[aa.K, ab.K]
): Expr[r.R, c.R] =
    coalesce(x, y)

def nullIf[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    r: Return[Unwrap[aa.R, Option], Unwrap[ab.R, Option], false],
    c: CombineKind[aa.K, ab.K],
    to: ToOption[Expr[r.R, c.R]]
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NullIf(
                aa.asExpr(x).asSqlExpr, ab.asExpr(y).asSqlExpr
            )
        )
    )

extension [T, EK <: ExprKind, CL <: Int](x: Expr[T, EK])(using qc: QueryContext[CL], kt: KindToTuple[EK], co: CanCallOver[EK])
    def over(): Expr[T, Window[kt.R]] =
        Expr(
            SqlExpr.Window(
                x.asSqlExpr,
                SqlWindow(
                    Nil,
                    Nil,
                    None
                )
            )
        )

    def over[OKS <: Tuple](over: OverContext ?=> Over[OKS])(using
        i: CanInWindow[OKS],
        c: CombineKindTuple[kt.R, OKS]
    ): Expr[T, Window[c.R]] =
        given OverContext = OverContext()
        val o = over
        Expr(
            SqlExpr.Window(
                x.asSqlExpr,
                SqlWindow(
                    o.partitionBy.map(_.asSqlExpr),
                    o.sortBy.map(_.asSqlOrderBy),
                    o.frame
                )
            )
        )

def currentRow[CL <: Int](using QueryContext[CL]): FrameBound[Nothing] =
    FrameBound(SqlWindowFrameBound.CurrentRow)

def unboundedPreceding[CL <: Int](using QueryContext[CL]): FrameBound[Nothing] =
    FrameBound(SqlWindowFrameBound.UnboundedPreceding)

def unboundedFollowing[CL <: Int](using QueryContext[CL]): FrameBound[Nothing] =
    FrameBound(SqlWindowFrameBound.UnboundedFollowing)

extension [T, CL <: Int](n: T)(using qc: QueryContext[CL], a: AsExpr[T, CL], kt: KindToTuple[a.K], nv: IsNotVariable[kt.R])
    def preceding: FrameBound[a.R] =
        FrameBound(SqlWindowFrameBound.Preceding(n.asExpr.asSqlExpr))

    def following: FrameBound[a.R] =
        FrameBound(SqlWindowFrameBound.Following(n.asExpr.asSqlExpr))

def partitionBy[T, CL <: Int](partitionValue: T)(using
    qc: QueryContext[CL],
    oc: OverContext,
    a: AsPartition[T, CL]
): PartitionedOver[a.KS] =
    PartitionedOver(partitionBy = a.asExprs(partitionValue))

def sortBy[T, CL <: Int](sortValue: T)(using
    qc: QueryContext[CL],
    oc: OverContext,
    a: AsOverSort[T, CL]
): SortedOver[a.R, a.KS] =
    SortedOver(sortBy = a.asSorts(sortValue))

def orderBy[T, CL <: Int](sortValue: T)(using
    qc: QueryContext[CL],
    oc: OverContext,
    a: AsOverSort[T, CL]
): SortedOver[a.R, a.KS] =
    SortedOver(sortBy = a.asSorts(sortValue))

def any[T, OKS <: Tuple, L <: Int, S <: QuerySize, CL <: Int](query: Query[T, OKS, L, S])(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL]
): QuantifiedSubquery[a.R, OKS, L] =
    QuantifiedSubquery(SqlSubqueryQuantifier.Any, query.tree)

def all[T, OKS <: Tuple, L <: Int, S <: QuerySize, CL <: Int](query: Query[T, OKS, L, S])(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL]
): QuantifiedSubquery[a.R, OKS, L] =
    QuantifiedSubquery(SqlSubqueryQuantifier.All, query.tree)

def exists[T, OKS <: Tuple, L <: Int, S <: QuerySize, CL <: Int](query: Query[T, OKS, L, S])(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    refl: L > CL =:= true
): Expr[Boolean, Composite[OKS]] =
    Expr(
        SqlExpr.Subquery(
            Some(SqlSubqueryQuantifier.Exists),
            query.tree
        )
    )

def createFunction[T, CL <: Int](name: String, args: List[Expr[?, ?]])(using QueryContext[CL]): Expr[T, Composite[EmptyTuple]] =
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

def createBinaryExpr[T, CL <: Int](left: Expr[?, ?], operator: String, right: Expr[?, ?])(using QueryContext[CL]): Expr[T, Composite[EmptyTuple]] =
    Expr(
        SqlExpr.Binary(
            left.asSqlExpr,
            SqlBinaryOperator.Custom(operator),
            right.asSqlExpr
        )
    )

inline def createTableFunction[T, CL <: Int](
    name: String,
    args: List[Expr[?, ?]],
    withOrdinal: Boolean
)(using
    qc: QueryContext[CL]
): FromFunc[T, Column, EmptyTuple, CL] =
    val metaData = TableMacro.tableMetaData[T]
    val alias = qc.fetchAlias
    val sqlTable: SqlTable.Func = SqlTable.Func(
        false,
        name,
        args.map(_.asSqlExpr),
        withOrdinal,
        Some(SqlTableAlias(alias, metaData.columnNames)),
        None
    )
    FromFunc(Some(alias), metaData.fieldNames, metaData.columnNames, sqlTable)

extension (s: StringContext)
    inline def rawExpr[CL <: Int](inline args: Any*)(using QueryContext[CL]): RawExpr[CL] =
        val instances = RawMacro.asSqlInstances[CL](args)
        RawExpr(s.parts.toList, instances, args.toList)

extension [T](expr: Expr[T, Column[1]])
    @targetName("to")
    def :=[R](updateExpr: R)(using
        a: AsExpr[R, 1],
        c: Compare[T, a.R],
        uc: UpdateSetContext
    ): UpdatePair = expr match
        case Expr(SqlExpr.Column(_, columnName)) =>
            UpdatePair(columnName, a.asExpr(updateExpr).asSqlExpr)
        case _ =>
            throw MatchError(expr)