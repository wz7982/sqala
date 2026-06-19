package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.{SqlQuery, SqlSetOperator, SqlWithItem}
import sqala.ast.table.*
import sqala.ast.token.SqlCustomToken
import sqala.metadata.*
import sqala.static.dsl.statement.dml.*
import sqala.static.dsl.statement.query.*
import sqala.static.dsl.table.*
import sqala.util.NonEmptyList.toNonEmptyList

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}
import scala.annotation.targetName
import scala.compiletime.ops.int.{+, >}

/**
 * Creates a query scope with an implicit `QueryContext`, enabling
 * all DSL operations within the block. Usually can be omitted since
 * `from` also provides a query context.
 *
 * {{{
 * val q = query:
 *     from(User).filter(u => u.id == 1)
 * }}}
 */
def query[T](q: QueryContext[0] ?=> T): T =
    given QueryContext[0] = QueryContext(0)
    q

/**
 * Starts an `INSERT` statement builder.
 *
 * {{{
 * insert[User](u => (u.id, u.name)).values((1, "Alice"), (2, "Bob"))
 * }}}
 */
inline def insert[T <: Product]: Insert[T, InsertTable] =
    Insert.apply[T]

/**
 * Starts an `UPDATE` statement builder.
 *
 * {{{
 * update[User].set(u => u.name := "Alice").where(_.id == 1)
 * }}}
 */
inline def update[T <: Product]: Update[T, UpdateTable] =
    Update.apply[T]

/**
 * Starts a `DELETE` statement builder.
 *
 * {{{
 * delete[User].where(_.id == 1)
 * }}}
 */
inline def delete[T <: Product]: Delete[T] =
    Delete.apply[T]

/**
 * Internal alias name used for recursive CTE tables.
 */
private[sqala] val tableCte = "__cte__"

/**
 * Internal column name used for the pseudo `LEVEL` column in
 * hierarchical queries.
 */
private[sqala] val columnPseudoLevel = "__pseudo__level__"

/**
 * Starts a `SELECT` query from one or more table sources.
 * Supports entity companion objects, joins, subqueries, graph
 * tables, pivot tables, and more.
 *
 * {{{
 * // Single table
 * from(User).filter(u => u.id == 1)
 *
 * // Join table
 * from(Channel.join(Post).on((c, p) => c.id == p.channelId))
 *
 * // Subquery (must project to a named tuple via `map`) table
 * from(from(User).map(u => (name = u.name)))
 *
 * // Multi-table tuple
 * from(Channel, Post)
 *     .filter((c, p) => c.id == p.channelId)
 * }}}
 */
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

/**
 * Creates a recursive query using `WITH RECURSIVE`. Takes a base
 * query, a recursive step, and a final projection. The recursive
 * step can reference the base query via its parameter, linked by
 * `UNION ALL`.
 *
 * {{{
 * withRecursive(
 *     from(Comment)
 *         .filter(c => c.parentId.isNull)
 *         .map(c => (id = c.id, content = c.content, level = 1))
 * )(t =>
 *     from(Comment.join(t).on((c, prior) => c.parentId == prior.id))
 *         .map((c, prior) => (id = c.id, content = c.content,level = prior.level + 1))
 * )(t =>
 *     from(t).map(cte => (id = cte.id, content = cte.content, level = cte.level))
 * )
 * }}}
 */
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
    val tree = SqlQuery.With(
        true,
        (SqlWithItem(tableCte, columns, withTree) :: Nil).toNonEmptyList,
        finalQuery.tree,
        None
    )
    Query(finalQuery.params, tree)

extension [T, AT, CL <: Int](x: T)(using qc: QueryContext[CL], t: AsTable[T, CL], refl: t.R <:< Table[AT, Column, CL])
    /**
     * Excludes specific columns from query projection. Excluded
     * columns are removed from the output type at compile time.
     *
     * {{{
     * from(Post.exclude[("title", "createTime")])
     * }}}
     */
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
    /**
     * Inner join. Must be followed by `on`.
     *
     * {{{
     * from(Channel.join(Post).on((c, p) => c.id == p.channelId))
     * }}}
     */
    def join[B](b: B)(using
        tb: AsTable[B, CL],
        j: Join[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL] =
        val (leftTable, leftSqlTable) = ta.asTable(a)
        val (rightTable, rightSqlTable) = tb.asTable(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Inner, rightSqlTable, None))

    /**
     * Lateral inner join. Must be followed by `on`. The lateral
     * table source can reference columns from the left table.
     *
     * {{{
     * from(
     *     Channel.joinLateral(c =>
     *         from(Post)
     *             .filter(p => c.id == p.channelId)
     *             .map(p => (id = p.id, channelId = p.channelId, title = p.title))
     *             .sortBy(p => p.likeCount.desc)
     *             .take(2)
     *     ).on((c, p) => c.id == p.channelId)
     * )
     * }}}
     */
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

    /**
     * Cross join. No `on` condition needed.
     *
     * {{{
     * from(Channel.crossJoin(Post)).filter((c, p) => c.id == p.channelId)
     * }}}
     */
    def crossJoin[B](b: B)(using
        tb: AsTable[B, CL],
        j: Join[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): FromJoin[j.R, c.R, CL] =
        val (leftTable, leftSqlTable) = ta.asTable(a)
        val (rightTable, rightSqlTable) = tb.asTable(b)
        val params = j.join(leftTable, rightTable)
        FromJoin(params, SqlTable.Join(leftSqlTable, SqlJoinType.Cross, rightSqlTable, None))

    /**
     * Lateral cross join. The lateral table source can reference
     * columns from the left table.
     *
     * {{{
     * from(
     *     Channel.crossJoinLateral(c =>
     *         from(Post)
     *             .filter(p => c.id == p.channelId)
     *             .map(p => (id = p.id, channelId = p.channelId, title = p.title))
     *             .sortBy(p => p.likeCount.desc)
     *             .take(2)
     *     )
     * ).filter((c, p) => c.id == p.channelId)
     * }}}
     */
    def crossJoinLateral[B](f: QueryContext[CL + 1] ?=> ta.R => B)(using
        tb: AsLateralTable[B, CL + 1],
        j: Join[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): FromJoin[j.R, c.R, CL + 1] =
        given QueryContext[CL + 1] = qc.asInstanceOf[QueryContext[CL + 1]]
        val (leftTable, leftSqlTable) = ta.asTable(a)
        val (rightTable, rightSqlTable) = tb.asTable(f(leftTable))
        val params = j.join(leftTable, rightTable)
        FromJoin(params, SqlTable.Join(leftSqlTable, SqlJoinType.Cross, rightSqlTable, None))

    /**
     * Left outer join. Must be followed by `on`. The right side
     * columns are wrapped with `Option`.
     *
     * {{{
     * from(Channel.leftJoin(Post).on((c, p) => c.id == p.channelId))
     * }}}
     */
    def leftJoin[B](b: B)(using
        tb: AsTable[B, CL],
        j: LeftJoin[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL] =
        val (leftTable, leftSqlTable) = ta.asTable(a)
        val (rightTable, rightSqlTable) = tb.asTable(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Left, rightSqlTable, None))

    /**
     * Lateral left outer join. Must be followed by `on`. The
     * lateral table source can reference columns from the left
     * table.
     *
     * {{{
     * from(
     *     Channel.leftJoinLateral(c =>
     *         from(Post)
     *             .filter(p => c.id == p.channelId)
     *             .map(p => (id = p.id, channelId = p.channelId, title = p.title))
     *             .sortBy(p => p.likeCount.desc)
     *             .take(2)
     *     ).on((c, p) => c.id == p.channelId)
     * )
     * }}}
     */
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

    /**
     * Right outer join. Must be followed by `on`. The left side
     * columns are wrapped with `Option`.
     *
     * {{{
     * from(Channel.rightJoin(Post).on((c, p) => c.id == p.channelId))
     * }}}
     */
    def rightJoin[B](b: B)(using
        tb: AsTable[B, CL],
        j: RightJoin[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL] =
        val (leftTable, leftSqlTable) = ta.asTable(a)
        val (rightTable, rightSqlTable) = tb.asTable(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Right, rightSqlTable, None))

    /**
     * Full outer join. Must be followed by `on`. Both sides are
     * wrapped with `Option`.
     *
     * {{{
     * from(Channel.fullJoin(Post).on((c, p) => c.id == p.channelId))
     * }}}
     */
    def fullJoin[B](b: B)(using
        tb: AsTable[B, CL],
        j: FullJoin[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL] =
        val (leftTable, leftSqlTable) = ta.asTable(a)
        val (rightTable, rightSqlTable) = tb.asTable(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, SqlTable.Join(leftSqlTable, SqlJoinType.Full, rightSqlTable, None))

/**
 * An unnested table element, used as the row shape from `unnest`.
 */
final case class Unnest[T](x: Option[T])

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

/**
 * An unnested table element with row number, used as the row shape
 * from `unnestWithOrdinal`.
 */
final case class UnnestWithOrdinal[T](x: Option[T], ordinal: Int)

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
    p: AsTableParam[JsonColumnFlatten[V, CL], CL],
    t: ToTuple[p.R],
    kt: KindToTuple[a.K],
    i: CanInSimpleClause[kt.R],
    e: ExcludeCurrentLevelColumn[kt.R, CL],
    c: CombineKindTuple[EmptyTuple, e.R]
): FromJson[JsonColumnNameFlatten[N, V], t.R, c.R, CL] =
    given JsonContext = JsonContext()
    val alias = qc.fetchAlias
    FromJson(a.asExpr(expr).asSqlExpr, path.asExpr.asSqlExpr, Some(alias), columns)

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
        a: AsSqlExpr[T]
    ): JsonPathColumn[T] =
        JsonPathColumn(path.asExpr.asSqlExpr, a.sqlType)

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
    qc: QueryContext[CL],
    jc: JsonContext,
): JsonExistsColumn =
    JsonExistsColumn(path.asExpr.asSqlExpr)

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
    qc: QueryContext[CL],
    jc: JsonContext
)(
    c: JsonContext ?=> NamedTuple[N, V]
): JsonNestedColumns[N, V] =
    val columnList: List[Any] = c.toList
    val jsonColumns = columnList.map:
        case p: JsonPathColumn[?] => p
        case o: JsonOrdinalColumn => o
        case e: JsonExistsColumn => e
        case n: JsonNestedColumns[?, ?] => n
    JsonNestedColumns(path.asExpr.asSqlExpr, jsonColumns)

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
    qc: QueryContext[CL],
    jc: JsonContext
): JsonColumns[N, V] =
    val columnList: List[Any] = c.toList
    val jsonColumns = columnList.map:
        case p: JsonPathColumn[?] => p
        case o: JsonOrdinalColumn => o
        case e: JsonExistsColumn => e
        case n: JsonNestedColumns[?, ?] => n
    JsonColumns(jsonColumns)

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
    GraphVertexSchema(None, metaData.copy(tableName = label))

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
    GraphEdgeSchema(None, metaData.copy(tableName = label), None, None)

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

extension [CL <: Int](s: String)(using qc: QueryContext[CL])
    /**
     * Associates a label name with a vertex or edge schema in a
     * graph pattern. Used inside `matching` blocks of `graphTable`.
     *
     * {{{
     * ("a" is g.person)
     * }}}
     */
    infix def is[V, OKS <: Tuple, T <: GraphPatternTerm[V, OKS, CL]](v: T)(using
        GraphContext
    ): GraphPattern[Tuple1[s.type], Tuple1[T], OKS, CL] =
        GraphPattern(Tuple1(v), v.asTerm :: Nil)

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
    val graph =
        Graph[N, tt.R, CL](schema.__name__, tt.toTuple(t.transform(schema.__items__)))
    f(graph)

/**
 * Matches the beginning of a partition in `matchRecognize`.
 * Maps to `^`.
 *
 * {{{
 * .pattern(d => ^ ~ d.down.+ ~ d.up.+)
 * }}}
 */
def ^[CL <: Int](using QueryContext[CL], MatchRecognizeContext): RecognizePatternTerm[CL] =
    RecognizePatternTerm(SqlRowPatternTerm.Circumflex(None))

/**
 * Matches the end of a partition in `matchRecognize`.
 * Maps to `$`.
 * {{{
 * .pattern(d => d.down.+ ~ d.up.+ ~ $)
 * }}}
 */
def $[CL <: Int](using QueryContext[CL], MatchRecognizeContext): RecognizePatternTerm[CL] =
    RecognizePatternTerm(SqlRowPatternTerm.Dollar(None))

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
    RecognizePatternTerm(SqlRowPatternTerm.Permute((term.pattern :: terms.toList.map(_.pattern)).toNonEmptyList, None))

/**
 * Excludes a pattern from the output in `matchRecognize`. Maps to
 * `{- pattern -}`.
 *
 * {{{
 * .pattern(d => exclusion(d.a))
 * }}}
 */
def exclusion[CL <: Int](term: RecognizePatternTerm[CL])(using QueryContext[CL], MatchRecognizeContext): RecognizePatternTerm[CL] =
    RecognizePatternTerm(SqlRowPatternTerm.Exclusion(term.pattern, None))

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
): Expr[a.R, Agg[kt.R]] =
    Expr(SqlExpr.MatchPhase(SqlMatchPhase.Final, a.asExpr(x).asSqlExpr))

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
): Expr[a.R, Agg[kt.R]] =
    Expr(SqlExpr.MatchPhase(SqlMatchPhase.Running, a.asExpr(x).asSqlExpr))

extension [T, CL <: Int](table: T)(using qc: QueryContext[CL], t: AsTable[T, CL], r: AsRecognize[t.R])
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
     *         .predefine[("start", "down", "up")]
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
    /**
     * Specifies `PARTITION BY` for `matchRecognize`.
     *
     * {{{
     * s.partitionBy(s.stockSymbol)
     * }}}
     */
    def partitionBy[P](partitionValue: P)(using
        a: AsRecognizePartition[P, CL]
    ): RecognizePredefine[T, CL] =
        RecognizePredefine(r.setPartitionBy(table, a.asExprs(partitionValue).map(_.asSqlExpr)))

    /**
     * Specifies `ORDER BY` for `matchRecognize`.
     *
     * {{{
     * s.sortBy(s.tradeTime)
     * }}}
     */
    def sortBy[S](sortValue: S)(using
        a: AsColumnSort[S, CL]
    ): RecognizePredefine[T, CL] =
        val sort = a.asSorts(sortValue).map(_.asSqlOrderingItem)
        RecognizePredefine(r.setOrderBy(table, sort))

    /**
     * Alias of `sortBy` for `matchRecognize`, provided for users familiar with `ORDER BY`.
     *
     * {{{
     * s.orderBy(s.tradeTime)
     * }}}
     */
    def orderBy[S](sortValue: S)(using
        a: AsColumnSort[S, CL]
    ): RecognizePredefine[T, CL] =
        sortBy(sortValue)

    /**
     * Returns one row per match. Maps to `ONE ROW PER MATCH`.
     *
     * {{{
     * s.oneRowPerMatch
     * }}}
     */
    def oneRowPerMatch: RecognizePredefine[T, CL] =
        RecognizePredefine(r.setPerMatch(table, SqlRecognizePatternRowsMode.OneRow))

    /**
     * Returns all rows per match. Maps to `ALL ROWS PER MATCH`.
     *
     * {{{
     * s.allRowsPerMatch
     * }}}
     */
    def allRowsPerMatch: RecognizePredefine[T, CL] =
        RecognizePredefine(r.setPerMatch(table, SqlRecognizePatternRowsMode.AllRows(None)))

extension [T, CL <: Int](x: T)(using qc: QueryContext[CL], p: AsPivot[T, CL])
    /**
     * Creates a pivot table, transforming rows into columns. `agg`
     * specifies the aggregations, and `by` defines the within-conditions
     * whose values are combined as a Cartesian product to generate
     * column names.
     *
     * {{{
     * from(
     *     City.pivot(c =>
     *         c.agg(sum = sum(c.population), count = count())
     *         .by(
     *             c.country.within(cn = "CN", us = "US"),
     *             c.year.within(`2024` = 2024, `2025` = 2025)
     *         )
     *     )
     * )
     * }}}
     */
    def pivot[N <: Tuple, V <: Tuple](f: PivotContext ?=> p.R => FromPivot[N, V, p.OKS, CL]): FromPivot[N, V, p.OKS, CL] =
        given PivotContext = PivotContext()
        f(p.asPivot(x))

/**
 * Returns 1 if the column is not part of the current grouping set
 * (aggregated), 0 if it is. Maps to `GROUPING(expr)`. Used after
 * `groupBy` clause.
 *
 * {{{
 * from(Post)
 *     .groupBy(p => (p.authorId, p.channelId))
 *     .map((g, p) => (author = p.authorId, count = count(), groupingAuthorId = grouping(p.authorId)))
 * }}}
 */
def grouping[T: AsSqlExpr, K <: Grouped[?], CL <: Int](x: Expr[T, K])(using 
    QueryContext[CL], 
    GroupingContext
): Expr[Int, Agg[K *: EmptyTuple]] =
    Expr(
        SqlExpr.Grouping(
            (x.asSqlExpr :: Nil).toNonEmptyList
        )
    )

extension [T, CL <: Int](x: T)(using qc: QueryContext[CL], a: AsExpr[T, CL])
    /**
     * Explicitly lifts a value, expression, or subquery to an
     * expression. Values are normally lifted automatically, but this
     * can be useful in custom function or operator contexts.
     *
     * {{{
     * from(Post).map(p => p.likeCount + 1.asExpr)
     * }}}
     */
    def asExpr: Expr[a.R, a.K] =
        a.asExpr(x)

    /**
     * Casts an expression to a target type. Maps to
     * `CAST(expr AS type)`.
     *
     * {{{
     * from(Post).map(p => p.id.as[String])
     * }}}
     */
    def as[R](using c: Cast[a.R, R], kt: KindToTuple[a.K]): Expr[Option[R], Composite[kt.R]] =
        Expr(SqlExpr.Cast(a.asExpr(x).asSqlExpr, c.castType))

    /**
     * Defines the value mappings for a pivot column. Each named
     * argument pairs a column name with a filter value, and the
     * names are used to generate the output column names.
     *
     * {{{
     * c.country.within(cn = "CN", us = "US")
     * }}}
     */
    def within[N <: Tuple, V <: Tuple](items: NamedTuple[N, V])(using
        pc: PivotContext,
        av: AsExpr[V, CL],
        as: AsSqlExpr[a.R],
        asv: AsSqlExpr[av.R],
        i: In[T, V, CL],
        ia: CanInAgg[i.KS],
        e: ExcludeCurrentLevelColumn[i.KS, CL],
        refl: e.R =:= EmptyTuple
    ): PivotWithin[N] =
        PivotWithin[N](a.asExpr(x).asSqlExpr, av.asExprs(items.toTuple).map(_.asSqlExpr))

/**
 * Returns the hierarchical level in a `connectBy` recursive query,
 * starting from 1. Only available inside a `connectBy` context.
 *
 * {{{
 * from(Comment)
 *     .connectBy((c, prior) => c.parentId == prior.id)
 *     .startWith(c => c.parentId.isNull)
 *     .map(c => (id = c.id, content = c.content, level = level()))
 * }}}
 */
def level[CL <: Int]()(using QueryContext[CL], ConnectByContext): Expr[Int, Column[CL]] =
    Expr(SqlExpr.Column(Some(tableCte), columnPseudoLevel))

extension [CL <: Int](n: Int)(using QueryContext[CL])
    /**
     * Creates an interval of years. Maps to `INTERVAL 'n' YEAR`.
     *
     * {{{
     * from(Post).map(p => p.createTime + 1.year)
     * }}}
     */
    def year: Interval =
        Interval(n.toString, SqlTimeUnit.Year)

    /**
     * Creates an interval of months. Maps to `INTERVAL 'n' MONTH`.
     *
     * {{{
     * from(Post).map(p => p.createTime + 2.month)
     * }}}
     */
    def month: Interval =
        Interval(n.toString, SqlTimeUnit.Month)

    /**
     * Creates an interval of days. Maps to `INTERVAL 'n' DAY`.
     *
     * {{{
     * from(Post).map(p => p.createTime + 3.day)
     * }}}
     */
    def day: Interval =
        Interval(n.toString, SqlTimeUnit.Day)

    /**
     * Creates an interval of hours. Maps to `INTERVAL 'n' HOUR`.
     *
     * {{{
     * from(Post).map(p => p.createTime + 4.hour)
     * }}}
     */
    def hour: Interval =
        Interval(n.toString, SqlTimeUnit.Hour)

    /**
     * Creates an interval of minutes. Maps to `INTERVAL 'n' MINUTE`.
     *
     * {{{
     * from(Post).map(p => p.createTime + 5.minute)
     * }}}
     */
    def minute: Interval =
        Interval(n.toString, SqlTimeUnit.Minute)

    /**
     * Creates an interval of seconds. Maps to `INTERVAL 'n' SECOND`.
     *
     * {{{
     * from(Post).map(p => p.createTime + 6.second)
     * }}}
     */
    def second: Interval =
        Interval(n.toString, SqlTimeUnit.Second)

/**
 * Constrains the types that can be used with `year`, `month`, etc.
 * extract functions.
 */
trait ExtractArg[T]

object ExtractArg:
    given dateTime[T: SqlDateTime]: ExtractArg[T]()

    given time[T: SqlTime]: ExtractArg[T]()

    given interval[T: SqlInterval]: ExtractArg[T]()

extension [T, CL <: Int](x: T)(using qc: QueryContext[CL], a: AsExpr[T, CL], kt: KindToTuple[a.K], e: ExtractArg[a.R])
    /**
     * Extracts the year from a datetime expression. Maps to
     * `EXTRACT(YEAR FROM expr)`.
     *
     * {{{
     * from(Post).map(p => p.createTime.year)
     * }}}
     */
    def year: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Year,
                a.asExpr(x).asSqlExpr
            )
        )

    /**
     * Extracts the month from a datetime expression. Maps to
     * `EXTRACT(MONTH FROM expr)`.
     *
     * {{{
     * from(Post).map(p => p.createTime.month)
     * }}}
     */
    def month: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Month,
                a.asExpr(x).asSqlExpr
            )
        )

    /**
     * Extracts the day from a datetime expression. Maps to
     * `EXTRACT(DAY FROM expr)`.
     *
     * {{{
     * from(Post).map(p => p.createTime.day)
     * }}}
     */
    def day: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Day,
                a.asExpr(x).asSqlExpr
            )
        )

    /**
     * Extracts the hour from a datetime expression. Maps to
     * `EXTRACT(HOUR FROM expr)`.
     *
     * {{{
     * from(Post).map(p => p.createTime.hour)
     * }}}
     */
    def hour: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Hour,
                a.asExpr(x).asSqlExpr
            )
        )

    /**
     * Extracts the minute from a datetime expression. Maps to
     * `EXTRACT(MINUTE FROM expr)`.
     *
     * {{{
     * from(Post).map(p => p.createTime.minute)
     * }}}
     */
    def minute: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Minute,
                a.asExpr(x).asSqlExpr
            )
        )

    /**
     * Extracts the second from a datetime expression. Maps to
     * `EXTRACT(SECOND FROM expr)`.
     *
     * {{{
     * from(Post).map(p => p.createTime.second)
     * }}}
     */
    def second: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            SqlExpr.ExtractFunc(
                SqlTimeUnit.Second,
                a.asExpr(x).asSqlExpr
            )
        )

/**
 * An intermediate builder for `CASE WHEN` chains. Use `caseWhen`
 * to start, add branches with `when`, and finish with `otherwise`.
 */
final case class CaseWhen[T, KS <: Tuple](private[sqala] val exprs: List[Expr[?, ?]]):
    /**
     * Adds a `WHEN condition THEN result` branch.
     *
     * {{{
     * from(Post).map(p => caseWhen(p.viewCount > 100)(p.title).when(p.likeCount > 10)("Hot").otherwise("N/A"))
     * }}}
     */
    def when[C, R, CL <: Int](cond: C)(result: R)(using
        qc: QueryContext[CL],
        ac: AsExpr[C, CL],
        ar: AsExpr[R, CL],
        b: SqlBoolean[ac.R],
        r: Return[T, ar.R],
        ktc: KindToTuple[ac.K],
        ktr: KindToTuple[ar.K],
        cc: CombineKindTuple[KS, ktc.R],
        c: CombineKindTuple[cc.R, ktr.R]
    ): CaseWhen[r.R, c.R] =
        CaseWhen(exprs :+ ac.asExpr(cond) :+ ar.asExpr(result))

    /**
     * Ends the `CASE WHEN` chain with an `ELSE` default.
     *
     * {{{
     * from(Post).map(p => caseWhen(p.viewCount > 100)(p.title).otherwise("N/A"))
     * }}}
     */
    def otherwise[R, CL <: Int](result: R)(using
        qc: QueryContext[CL],
        a: AsExpr[R, CL],
        r: Return[T, a.R],
        kt: KindToTuple[a.K],
        c: CombineKindTuple[KS, kt.R]
    ): Expr[r.R, Composite[c.R]] =
        val caseBranches =
            exprs.grouped(2).toList.map(i => (i(0), i(1))).map((w, t) => SqlCaseBranch(w.asSqlExpr, t.asSqlExpr))
        Expr(
            SqlExpr.Case(
                caseBranches.toNonEmptyList,
                Some(a.asExpr(result).asSqlExpr)
            )
        )

/**
 * Starts a `CASE WHEN` chain. Maps to `CASE WHEN cond THEN result`.
 *
 * {{{
 * from(Post).map(p => caseWhen(p.viewCount > 100)(p.title).otherwise("N/A"))
 * }}}
 */
def caseWhen[C, R, CL <: Int](cond: C)(result: R)(using
    qc: QueryContext[CL],
    ac: AsExpr[C, CL],
    ar: AsExpr[R, CL],
    b: SqlBoolean[ac.R],
    as: AsSqlExpr[ar.R],
    ktc: KindToTuple[ac.K],
    ktr: KindToTuple[ar.K],
    c: CombineKindTuple[ktc.R, ktr.R]
): CaseWhen[ar.R, c.R] =
    CaseWhen(ac.asExpr(cond) :: ar.asExpr(result) :: Nil)

/**
 * Returns the first non-null value from two expressions.
 * Maps to `COALESCE(expr1, expr2)`. Both arguments must be
 * compatible types.
 *
 * {{{
 * from(Post).map(p => coalesce(p.title, "Untitled"))
 * }}}
 */
def coalesce[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    asa: AsSqlExpr[aa.R],
    asb: AsSqlExpr[ab.R],
    r: Return[aa.R, ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[r.R, c.R] =
    Expr(
        SqlExpr.Coalesce(
            (aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil).toNonEmptyList
        )
    )

/**
 * Alias of `coalesce`, provided for users familiar with `IFNULL`.
 *
 * {{{
 * from(Post).map(p => ifNull(p.title, "Untitled"))
 * }}}
 */
def ifNull[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    asa: AsSqlExpr[aa.R],
    asb: AsSqlExpr[ab.R],
    r: Return[aa.R, ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[r.R, c.R] =
    coalesce(x, y)

/**
 * Returns null if two expressions are equal, otherwise returns the
 * first expression. Maps to `NULLIF(expr1, expr2)`. Both arguments
 * must be compatible types.
 *
 * {{{
 * from(Post).map(p => nullIf(p.title, ""))
 * }}}
 */
def nullIf[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    asa: AsSqlExpr[aa.R],
    asb: AsSqlExpr[ab.R],
    r: Return[aa.R, ab.R],
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
    /**
     * Applies a window specification to the expression. Maps to
     * `OVER ()`.
     *
     * {{{
     * from(Post).map(p => rank().over())
     * }}}
     */
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

    /**
     * Applies a window specification with `partitionBy` and
     * `orderBy` clause. Maps to `OVER (PARTITION BY ... ORDER BY ...)`.
     *
     * {{{
     * from(Post).map(p => sum(p.likeCount).over(
     *     partitionBy(p.authorId).orderBy(p.id.desc)
     * ))
     * }}}
     */
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
                    o.sortBy.map(_.asSqlOrderingItem),
                    o.frame
                )
            )
        )

/**
 * References the current row in a window frame. Maps to
 * `CURRENT ROW`. Used with `rows`, `range`, or `groups`.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount).over(
 *     partitionBy(p.authorId).orderBy(p.id.desc).rows(currentRow)
 * ))
 * }}}
 */
def currentRow[CL <: Int](using QueryContext[CL], OverContext): FrameBound[Nothing] =
    FrameBound(SqlWindowFrameBound.CurrentRow)

/**
 * References the first row in a window frame. Maps to
 * `UNBOUNDED PRECEDING`. Used with `rows`, `range`, or `groups`.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount).over(
 *     partitionBy(p.authorId).orderBy(p.id.desc).rows(unboundedPreceding)
 * ))
 * }}}
 */
def unboundedPreceding[CL <: Int](using QueryContext[CL], OverContext): FrameBound[Nothing] =
    FrameBound(SqlWindowFrameBound.UnboundedPreceding)

/**
 * References the last row in a window frame. Maps to
 * `UNBOUNDED FOLLOWING`. Used with `rows`, `range`, or `groups`.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount).over(
 *     partitionBy(p.authorId).orderBy(p.id.desc).rows(unboundedFollowing)
 * ))
 * }}}
 */
def unboundedFollowing[CL <: Int](using QueryContext[CL], OverContext): FrameBound[Nothing] =
    FrameBound(SqlWindowFrameBound.UnboundedFollowing)

extension [T, CL <: Int](n: T)(using qc: QueryContext[CL], oc: OverContext, a: AsExpr[T, CL], kt: KindToTuple[a.K], nv: IsNotVariable[kt.R])
    /**
     * References N rows before the current row in a window frame.
     * Maps to `n PRECEDING`. Used with `rows`, `range`, or `groups`.
     *
     * {{{
     * from(Post).map(p => sum(p.likeCount).over(
     *     partitionBy(p.authorId).orderBy(p.id.desc).rows(1.preceding)
     * ))
     * }}}
     */
    def preceding: FrameBound[a.R] =
        FrameBound(SqlWindowFrameBound.Preceding(n.asExpr.asSqlExpr))

    /**
     * References N rows after the current row in a window frame.
     * Maps to `n FOLLOWING`. Used with `rows`, `range`, or `groups`.
     *
     * {{{
     * from(Post).map(p => sum(p.likeCount).over(
     *     partitionBy(p.authorId).orderBy(p.id.desc).rows(1.following)
     * ))
     * }}}
     */
    def following: FrameBound[a.R] =
        FrameBound(SqlWindowFrameBound.Following(n.asExpr.asSqlExpr))

/**
 * Specifies the `PARTITION BY` clause in a window function.
 * Partitions the rows into groups for the window calculation.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount).over(
 *     partitionBy(p.authorId)
 * ))
 * }}}
 */
def partitionBy[T, CL <: Int](partitionValue: T)(using
    qc: QueryContext[CL],
    oc: OverContext,
    a: AsPartition[T, CL]
): PartitionedOver[a.KS] =
    PartitionedOver(partitionBy = a.asExprs(partitionValue))

/**
 * Specifies the `ORDER BY` clause in a window function.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount).over(
 *     sortBy(p.id.desc)
 * ))
 * }}}
 */
def sortBy[T, CL <: Int](sortValue: T)(using
    qc: QueryContext[CL],
    oc: OverContext,
    a: AsOverSort[T, CL]
): SortedOver[a.R, a.KS] =
    SortedOver(sortBy = a.asSorts(sortValue))

/**
 * Alias of `sortBy`, provided for users familiar with `ORDER BY`.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount).over(
 *     orderBy(p.id.desc)
 * ))
 * }}}
 */
def orderBy[T, CL <: Int](sortValue: T)(using
    qc: QueryContext[CL],
    oc: OverContext,
    a: AsOverSort[T, CL]
): SortedOver[a.R, a.KS] =
    SortedOver(sortBy = a.asSorts(sortValue))

/**
 * Tests whether a condition holds for any row in a subquery.
 * Maps to `ANY (subquery)`. Typically used in `filter` with
 * comparison operators.
 *
 * {{{
 * from(Post).filter(p => p.id == any(from(Comment).map(c => c.postId)))
 * }}}
 */
def any[T, OKS <: Tuple, L <: Int, S <: QuerySize, CL <: Int](query: Query[T, OKS, L, S])(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL]
): QuantifiedSubquery[a.R, OKS, L] =
    QuantifiedSubquery(SqlSubqueryQuantifier.Any, query.tree)

/**
 * Tests whether a condition holds for all rows in a subquery.
 * Maps to `ALL (subquery)`. Typically used in `filter` with
 * comparison operators.
 *
 * {{{
 * from(Post).filter(p => p.id == all(from(Comment).map(c => c.postId)))
 * }}}
 */
def all[T, OKS <: Tuple, L <: Int, S <: QuerySize, CL <: Int](query: Query[T, OKS, L, S])(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL]
): QuantifiedSubquery[a.R, OKS, L] =
    QuantifiedSubquery(SqlSubqueryQuantifier.All, query.tree)

/**
 * Tests whether a subquery returns at least one row. Maps to
 * `EXISTS (subquery)`. Typically used in `filter`.
 *
 * {{{
 * from(User).filter(u => exists(from(User).filter(uu => uu.name == u.name && uu.id != u.id)))
 * }}}
 */
def exists[T, OKS <: Tuple, L <: Int, S <: QuerySize, CL <: Int](query: Query[T, OKS, L, S])(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    refl: L > CL =:= true
): Expr[Boolean, Composite[OKS]] =
    Expr(
        SqlExpr.ExistsPredicate(
            query.tree
        )
    )

/**
  * Creates a `CUBE` clause. Maps to `CUBE(exprs)`.
  * 
  * {{{
  * from(Post).groupBy(p => (id = p.id, title = p.title))(g => cube(g.id, g.title)).map((g, p) => count())
  * }}}
  */
def cube[T, CL <: Int](x: T)(using 
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    kt: KindToTuple[a.K],
    ak: AllIsKind[kt.R, Grouped[?]],
    refl: ak.R =:= true
): Cube =
    Cube(a.asExprs(x).map(_.asSqlExpr))

/**
  * Creates a `ROLLUP` clause. Maps to `ROLLUP(exprs)`.
  * 
  * {{{
  * from(Post).groupBy(p => (id = p.id, title = p.title))(g => rollup(g.id, g.title)).map((g, p) => count())
  * }}}
  */
def rollup[T, CL <: Int](x: T)(using 
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    kt: KindToTuple[a.K],
    ak: AllIsKind[kt.R, Grouped[?]],
    refl: ak.R =:= true
): Rollup =
    Rollup(a.asExprs(x).map(_.asSqlExpr))

/**
  * Creates a `GROUPING SETS` clause. Maps to `GROUPING SETS(exprs)`.
  * 
  * {{{
  * from(Post).groupBy(p => (id = p.id, title = p.title))(g => groupingSets(g.id, g.title, ())).map((g, p) => count())
  * }}}
  */
def groupingSets[T, CL <: Int](x: T)(using 
    qc: QueryContext[CL],
    a: AsMultidimensionalGrouping[T]
): GroupingSets =
    GroupingSets(a.asSqlGroupingItems(x))

/**
 * Creates a custom function call expression. Maps to `name(args...)`.
 * Used to wrap non-standard functions that aren't built into sqala.
 *
 * {{{
 * def left(x: Expr[String, ?], n: Int) =
 *     createFunc[Option[String]]("LEFT", x :: n.asExpr :: Nil)
 *
 * from(Post).map(p => left(p.title, 10))
 * }}}
 */
def createFunc[T, CL <: Int](name: String, args: List[Expr[?, ?]])(using QueryContext[CL]): Expr[T, Composite[EmptyTuple]] =
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

/**
 * Creates a custom binary expression with a raw operator string.
 * For non-standard operators that aren't built into sqala.
 *
 * {{{
 * extension (x: Expr[Json, ?])
 *     def ->>(k: String) =
 *         createBinaryExpr[Boolean](x, "->>", k.asExpr)
 *
 * from(Post).map(p => p.data ->> "key")
 * }}}
 */
def createBinaryExpr[T, CL <: Int](left: Expr[?, ?], operator: String, right: Expr[?, ?])(using QueryContext[CL]): Expr[T, Composite[EmptyTuple]] =
    Expr(
        SqlExpr.Binary(
            left.asSqlExpr,
            SqlBinaryOperator.Custom(SqlCustomToken.Keyword(operator) :: Nil),
            right.asSqlExpr
        )
    )

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

/**
 * Creates a raw expression from a string interpolation, supporting
 * values, expressions, and subqueries as arguments. Values are
 * automatically escaped. Must be followed by `as`.
 *
 * {{{
 * from(Post).filter(p => rawExpr"MATCH(${p.title}) AGAINST(${word})".as[Boolean])
 * }}}
 */
extension (s: StringContext)
    inline def rawExpr[CL <: Int](inline args: Any*)(using QueryContext[CL]): RawExpr[CL] =
        val instances = RawMacro.asExprInstances[CL](args)
        RawExpr(s.parts.toList.map(_.trim), instances, args.toList)

extension [T](expr: Expr[T, Column[1]])
    /**
     * Assigns a value to a column in an `UPDATE` statement.
     *
     * {{{
     * update[User].set(u => u.name := "Alice")
     * }}}
     */
    @targetName("to")
    def :=[R](updateExpr: R)(using
        a: AsExpr[R, 1],
        c: CanCompare[T, a.R],
        uc: UpdateSetContext
    ): UpdatePair =
        expr match
            case Expr(SqlExpr.Column(_, columnName)) =>
                UpdatePair(columnName, a.asExpr(updateExpr).asSqlExpr)
            case _ =>
                throw MatchError(expr)