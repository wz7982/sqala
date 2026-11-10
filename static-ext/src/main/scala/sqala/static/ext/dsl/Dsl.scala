package sqala.static.ext.dsl

import sqala.static.dsl.QueryContext
import sqala.static.dsl.AsExpr
import sqala.static.ext.dsl.table.FromFunc
import sqala.static.dsl.Column
import sqala.static.dsl.KindToTuple
import sqala.static.dsl.ExcludeCurrentLevelColumn
import sqala.static.dsl.CanInSimpleClause
import sqala.static.dsl.FlattenUnnest
import sqala.metadata.TableMetaData
import sqala.ir.IrTable
import sqala.util.NonEmptyList
import sqala.static.ext.dsl.table.JsonColumns
import sqala.static.dsl.table.AsTableParam
import sqala.static.ext.dsl.table.JsonColumnFlatten
import sqala.static.dsl.ToTuple
import sqala.static.dsl.CombineKindTuple
import sqala.static.ext.dsl.table.JsonColumnNameFlatten
import sqala.static.ext.dsl.table.FromJson
import sqala.static.ext.dsl.table.JsonOrdinalColumn
import sqala.metadata.AsSqlExpr
import sqala.static.dsl.ValidateArgType
import sqala.static.ext.dsl.table.JsonPathColumn
import sqala.static.dsl.asExpr
import sqala.static.ext.dsl.table.JsonExistsColumn
import scala.NamedTuple.NamedTuple
import sqala.static.ext.dsl.table.JsonNestedColumns
import sqala.static.ext.dsl.table.GraphVertexSchema
import sqala.metadata.TableMacro
import sqala.static.ext.dsl.table.GraphEdgeSchema
import sqala.static.ext.dsl.table.GraphSchema
import sqala.static.ext.dsl.table.GraphPatternTerm
import sqala.static.ext.dsl.table.GraphPattern
import scala.compiletime.constValueTuple
import sqala.static.ext.dsl.table.TransformGraphSchema
import sqala.static.ext.dsl.table.FromGraph
import sqala.static.ext.dsl.table.Graph

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
    FromJson(a.asExpr(expr).expr, path.asExpr.expr, columns)

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
        JsonPathColumn(path.asExpr.expr, a.sqlType)

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
    JsonExistsColumn(path.asExpr.expr)

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
    JsonNestedColumns(path.asExpr.expr, c.toTuple)

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
    inline infix def is[V, OKS <: Tuple, T <: GraphPatternTerm[V, OKS, CL]](term: T)(using
        GraphContext
    ): GraphPattern[Tuple1[s.type], Tuple1[T], OKS, CL] =
        val fields = constValueTuple[Tuple1[s.type]].toArray.toList.map(_.toString)
        GraphPattern(Tuple1(term), term.asTerm :: Nil, fields)

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
inline def graphTable[N <: Tuple, V <: Tuple, TN <: Tuple, TV <: Tuple, TOKS <: Tuple, CL <: Int](using
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
    val fields = constValueTuple[N].toArray.toList.map(_.toString)
    val graph =
        Graph[N, tt.R, CL](schema.__name__, tt.toTuple(t.transform(schema.__items__)), fields)
    f(graph)