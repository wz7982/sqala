package sqala.static.ext.dsl

import sqala.ast.expr.SqlMatchPhase
import sqala.ir.{IrExpr, IrRowPatternTerm, IrTable}
import sqala.metadata.{AsSqlExpr, TableMacro, TableMetaData}
import sqala.static.dsl.*
import sqala.static.dsl.table.AsTableParam
import sqala.static.ext.dsl.table.*
import sqala.util.NonEmptyList

import scala.NamedTuple.NamedTuple

/**
 * Computes the return type of `unnest`.
 */
trait UnnestReturn[T]:
    /**
     * The result type.
     */
    type R

object UnnestReturn:
    type Aux[T, O] = UnnestReturn[T]:
        type R = O

    given flatten[T]: Aux[T, FlattenUnnest[T]] =
        new UnnestReturn[T]:
            type R = FlattenUnnest[T]

/**
 * An unnested table element, used as the row shape from `unnest`.
 */
case class Unnest[T](x: Option[T])

/**
 * Expands an array into a set of rows. Maps to `UNNEST(expr)`.
 *
 * {{{
 * from(unnest(Array(1, 2, 3))).filter(t => t.x > 1)
 * }}}
 */
def unnest[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    r: UnnestReturn[a.R],
    kt: KindToTuple[a.K],
    i: CanInSimpleClause[kt.R, "unnest"],
    e: ExcludeCurrentLevelColumn[kt.R, CL]
): FromFunc[Unnest[r.R], Column, e.R, CL] =
    val metaData = TableMetaData("UNNEST", Nil, None, NonEmptyList("x", Nil), NonEmptyList("x", Nil))
    val table: IrTable.Func =
        IrTable.Func(
            false,
            "UNNEST",
            a.asExpr(x).expr :: Nil,
            false,
            metaData.columns.toList
        )
    FromFunc(table, metaData)

/**
 * An unnested table element with row number, used as the row shape
 * from `unnestWithOrdinal`.
 */
case class UnnestWithOrdinal[T](x: Option[T], ordinal: Int)

/**
 * Expands an array into a set of rows with ordinal position.
 * Maps to `UNNEST(expr) WITH ORDINALITY`.
 *
 * {{{
 * from(unnestWithOrdinal(Array(1, 2, 3))).filter(t => t.ordinal > 1)
 * }}}
 */
def unnestWithOrdinal[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    r: UnnestReturn[a.R],
    kt: KindToTuple[a.K],
    i: CanInSimpleClause[kt.R, "unnest"],
    e: ExcludeCurrentLevelColumn[kt.R, CL]
): FromFunc[UnnestWithOrdinal[r.R], Column, e.R, CL] =
    val metaData = TableMetaData("UNNEST", Nil, None, NonEmptyList("x", "ordinal" :: Nil), NonEmptyList("x", "ordinal" :: Nil))
    val table: IrTable.Func =
        IrTable.Func(
            false,
            "UNNEST",
            a.asExpr(x).expr :: Nil,
            true,
            metaData.columns.toList
        )
    FromFunc(table, metaData)

/**
 * Creates a JSON table. Maps to `JSON_TABLE`. Columns are defined using
 * `pathColumn`, `existsColumn`, `ordinalColumn`, and `nestedColumns`.
 *
 * {{{
 * from(
 *     jsonTable(
 *         """{"a": 1, "obj": {"b": "abc", "c": 2}}""",
 *         "$",
 *         columns(
 *             o = ordinalColumn,
 *             a = pathColumn[Int]("$.a"),
 *             n = nestedColumns("$.obj")(
 *                 b = pathColumn[String]("$.b"),
 *                 c = existsColumn("$.c")
 *             )
 *         )
 *     )
 * )
 * }}}
 */
def jsonTable[E, N <: Tuple, V <: Tuple, CL <: Int](
    expr: E,
    path: String,
    columns: JsonContext ?=> JsonColumns[N, V]
)(using
    qc: QueryContext[CL],
    a: AsExpr[E, CL],
    f: JsonColumnFlatten[V, CL],
    p: AsTableParam[f.R, CL],
    t: ToTuple[p.R],
    kt: KindToTuple[a.K],
    i: CanInSimpleClause[kt.R, "jsonTable"],
    e: ExcludeCurrentLevelColumn[kt.R, CL],
    c: CombineKindTuple[EmptyTuple, e.R]
): FromJson[JsonColumnNameFlatten[N, V], t.R, c.R, CL] =
    given JsonContext = JsonContext()
    FromJson(a.asExpr(expr).expr, path, columns)

/**
 * Creates an ordinality column for `jsonTable`. Maps to
 * `FOR ORDINALITY`.
 *
 * {{{
 * ordinalColumn
 * }}}
 */
def ordinalColumn[CL <: Int](using QueryContext[CL], JsonContext): JsonOrdinalColumn =
    JsonOrdinalColumn()

/**
 * Intermediate builder for a path column in `jsonTable`.
 */
final class JsonPathColumnPart[T]:
    /**
     * Creates a path column definition for `jsonTable`. Maps to
     * `path type PATH "$.path"`.
     *
     * {{{
     * pathColumn[Int]("$.a")
     * }}}
     */
    def apply[CL <: Int](path: String)(using
        qc: QueryContext[CL],
        jc: JsonContext,
        as: ValidateArgType[AsSqlExpr, T, "scalar"],
        a: AsSqlExpr[T]
    ): JsonPathColumn[T] =
        JsonPathColumn(path, a.sqlType)

/**
 * Creates a path column definition for `jsonTable`. Maps to
 * `type PATH "$.path"`.
 *
 * {{{
 * pathColumn[Int]("$.a")
 * }}}
 */
def pathColumn[T: AsSqlExpr]: JsonPathColumnPart[T] =
    JsonPathColumnPart()

/**
 * Creates an existence check column for `jsonTable`. Maps to
 * `BOOLEAN EXISTS PATH "$.path"`.
 *
 * {{{
 * existsColumn("$.c")
 * }}}
 */
def existsColumn[CL <: Int](path: String)(using
    QueryContext[CL],
    JsonContext,
): JsonExistsColumn =
    JsonExistsColumn(path)

/**
 * Defines nested columns at a JSON path for `jsonTable`. Maps to
 * `NESTED PATH "$.path" COLUMNS(...)`.
 *
 * {{{
 * nestedColumns("$.obj")(
 *     b = pathColumn[String]("$.b"),
 *     c = existsColumn("$.c")
 * )
 * }}}
 */
def nestedColumns[N <: Tuple, V <: Tuple, CL <: Int](
    path: String
)(using
    QueryContext[CL],
    JsonContext
)(
    c: JsonContext ?=> NamedTuple[N, V]
): JsonNestedColumns[N, V] =
    JsonNestedColumns(path, c.toTuple)

/**
 * Creates a column list for `jsonTable`. Maps to
 * `COLUMNS(...)`.
 *
 * {{{
 * from(
 *     jsonTable(
 *         """{"a": 1, "obj": {"b": "abc", "c": 2}}""",
 *         "$",
 *         columns(
 *             o = ordinalColumn,
 *             a = pathColumn[Int]("$.a"),
 *             n = nestedColumns("$.obj")(
 *                 b = pathColumn[String]("$.b"),
 *                 c = existsColumn("$.c")
 *             )
 *         )
 *     )
 * )
 * }}}
 */
def columns[N <: Tuple, V <: Tuple, CL <: Int](c: JsonContext ?=> NamedTuple[N, V])(using
    QueryContext[CL],
    JsonContext
): JsonColumns[N, V] =
    JsonColumns(c.toTuple)

/**
 * Creates a vertex schema for a graph query. The type parameter is
 * the entity class for the vertex.
 *
 * {{{
 * val friendGraph = createGraph(name = "friend_graph")(
 *     person = vertex[Person](label = "person"),
 *     friends = edge[Friendship](label = "friendship")
 * )
 * }}}
 */
inline def vertex[T](label: String): GraphVertexSchema[T] =
    val metaData = TableMacro.tableMetaData[T]
    GraphVertexSchema(metaData.copy(tableName = label))

/**
 * Creates an edge schema for a graph query. The type parameter is
 * the entity class for the edge.
 *
 * {{{
 * val friendGraph = createGraph(name = "friend_graph")(
 *     person = vertex[Person](label = "person"),
 *     friends = edge[Friendship](label = "friendship")
 * )
 * }}}
 */
inline def edge[T](label: String): GraphEdgeSchema[T] =
    val metaData = TableMacro.tableMetaData[T]
    GraphEdgeSchema(metaData.copy(tableName = label))

/**
 * Creates a graph schema combining vertices and edges, used as the
 * input to `graphTable`.
 *
 * {{{
 * val friendGraph = createGraph(name = "friend_graph")(
 *     person = vertex[Person](label = "person"),
 *     friends = edge[Friendship](label = "friendship")
 * )
 * }}}
 */
def createGraph[N <: Tuple, V <: Tuple](name: String)(labels: NamedTuple[N, V]): GraphSchema[N, V] =
    GraphSchema(name, labels.toTuple)

extension [CL <: Int](s: String)(using QueryContext[CL])
    /**
     * Associates a label name with a vertex or edge schema in a
     * graph pattern. Used inside `matching` blocks of `graphTable`.
     *
     * {{{
     * ("a" is g.person)
     * }}}
     */
    infix def is[V, OKS <: Tuple, T <: GraphPatternTerm[V, OKS, CL]](term: T)(using
        GraphContext
    ): GraphPattern[Tuple1[s.type], Tuple1[T], OKS, CL] =
        GraphPattern(Tuple1(term), term.asTerm :: Nil)

/**
 * Creates a graph table from a graph schema. Maps to `GRAPH_TABLE`. Used with `matching`
 * to define graph patterns, `filter` for conditions, and `columns` for the output projection.
 *
 * {{{
 * from(
 *     graphTable(friendGraph)(g =>
 *         g.matching(
 *             ("a" is g.person) |-|
 *             ("f" is g.friends.filter(f => f.personAId > 1)) |->|
 *             ("b" is g.person)
 *         ).filter(p => p.a.id > 1)
 *         .columns( p =>
 *             (
 *                 personA = p.a.name,
 *                 personB = p.b.name,
 *                 meetingDate = p.f.meetingDate
 *             )
 *         )
 *     )
 * )
 * }}}
 */
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
    val graph = Graph[N, tt.R, CL](schema.name, tt.toTuple(t.transform(schema.items)))
    f(graph)

/**
 * Matches the beginning of a partition in `matchRecognize`.
 * Maps to `^`.
 *
 * {{{
 * .pattern(d => ^ ~ d.down.+ ~ d.up.+)
 * }}}
 */
def ^[CL <: Int](using
    QueryContext[CL],
    MatchRecognizeContext
): RecognizePatternTerm[CL] =
    new RecognizePatternTerm[CL](IrRowPatternTerm.Circumflex(None))

/**
 * Matches the end of a partition in `matchRecognize`.
 * Maps to `$`.
 * {{{
 * .pattern(d => d.down.+ ~ d.up.+ ~ $)
 * }}}
 */
def $[CL <: Int](using
    QueryContext[CL],
    MatchRecognizeContext
): RecognizePatternTerm[CL] =
    new RecognizePatternTerm[CL](IrRowPatternTerm.Dollar(None))

/**
 * Permutes a set of patterns in `matchRecognize`. Maps to
 * `PERMUTE(p1, p2, ...)`.
 *
 * {{{
 * .pattern(d => permute(d.a, d.b, d.c))
 * }}}
 */
def permute[CL <: Int](term: RecognizePatternTerm[CL], terms: RecognizePatternTerm[CL]*)(using
    QueryContext[CL],
    MatchRecognizeContext
): RecognizePatternTerm[CL] =
    new RecognizePatternTerm[CL](
        IrRowPatternTerm.Permute(
            NonEmptyList(term.pattern, terms.toList.map(_.pattern)),
            None
        )
    )

/**
 * Excludes a pattern from the output in `matchRecognize`. Maps to
 * `{- pattern -}`.
 *
 * {{{
 * .pattern(d => exclusion(d.a))
 * }}}
 */
def exclusion[CL <: Int](term: RecognizePatternTerm[CL])(using
    QueryContext[CL],
    MatchRecognizeContext
): RecognizePatternTerm[CL] =
    new RecognizePatternTerm[CL](IrRowPatternTerm.Exclusion(term.pattern, None))

/**
 * Returns the final value of an expression across the match.
 * Maps to `FINAL expr`. Used in `measures` of `matchRecognize`.
 *
 * {{{
 * .measures(d => (endTime = finalized(last(d.up.tradeTime))))
 * }}}
 */
def finalized[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext,
    a: AsExpr[T, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInGroupedMap[kt.R]
): Expr[a.R, Composite[kt.R]] =
    Expr(
        IrExpr.MatchPhase(
            SqlMatchPhase.Final,
            a.asExpr(x).expr
        )
    )

/**
 * Returns the running value of an expression during the match.
 * Maps to `RUNNING expr`. Used in `measures` of `matchRecognize`.
 *
 * {{{
 * .measures(d => (runningTotal = running(sum(d.price))))
 * }}}
 */
def running[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext,
    a: AsExpr[T, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInGroupedMap[kt.R]
): Expr[a.R, Composite[kt.R]] =
    Expr(
        IrExpr.MatchPhase(
            SqlMatchPhase.Running,
            a.asExpr(x).expr
        )
    )

extension [T, CL <: Int](table: T)(using
    qc: QueryContext[CL],
    r: AsRecognize[T, CL]
)
    /**
     * Starts a pattern recognition block on a table. Maps to
     * `MATCH_RECOGNIZE(...)`. Supports `partitionBy`, `sortBy`,
     * `predefine`, `define`, `pattern`, `measures`, and rows mode.
     *
     * {{{
     * StockPrice.matchRecognize(s =>
     *     s.partitionBy(s.stockSymbol)
     *         .sortBy(s.tradeTime)
     *         .oneRowPerMatch
     *         .predefine("start", "down", "up")
     *         .define(d => (
     *             start = true,
     *             down = d.down.price < prev(d.down.price),
     *             up = d.up.price > prev(d.up.price)
     *         ))
     *         .pattern(d => d.start ~ d.down.+ ~ d.up.+)
     *         .measures(d => (
     *             startTime = d.start.tradeTime,
     *             endTime = finalized(last(d.up.tradeTime))
     *         ))
     * )
     * }}}
     */
    def matchRecognize[N <: Tuple, V <: Tuple](
        f: MatchRecognizeContext ?=> RecognizeSource[r.R, CL] => FinalRecognize[N, V, CL]
    ): FromRecognize[N, V, r.OKS, CL] =
        given MatchRecognizeContext = new MatchRecognizeContext
        val (initialTable, source) = r.asRecognizeTable(table)
        val recognizeSource = RecognizeSource(initialTable, source)
        val recognize = f(recognizeSource)
        FromRecognize(recognize.table, recognize.items)

/**
 * Creates a custom table function that can be used as a table source
 * in `from`. Maps to `name(args...)`. For non-standard table
 * functions that aren't built into sqala.
 *
 * {{{
 * case class GenerateSeries(x: Int)
 *
 * def generateSeries(start: Int, end: Int) =
 *     createTableFunc[GenerateSeries](
 *         "generate_series",
 *         start.asExpr :: end.asExpr :: Nil,
 *         false
 *     )
 *
 * from(generateSeries(1, 10)).filter(t => t.x > 1)
 * }}}
 */
inline def createTableFunc[T, CL <: Int](
    name: String,
    args: List[Expr[?, ?]],
    withOrdinal: Boolean
)(using
    QueryContext[CL]
): FromFunc[T, Column, EmptyTuple, CL] =
    val metaData = TableMacro.tableMetaData[T]
    val table: IrTable.Func = IrTable.Func(
        false,
        name,
        args.map(_.expr),
        withOrdinal,
        metaData.columns.toList
    )
    FromFunc(table, metaData)