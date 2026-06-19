package sqala.static.dsl.table

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.statement.SqlSelectItem
import sqala.ast.table.*
import sqala.metadata.{SqlBoolean, SqlNumber, TableMetaData}
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.AsMap
import sqala.util.NonEmptyList.toNonEmptyList

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}
import scala.compiletime.constValue

/**
 * A graph schema definition created by `createGraph`, combining
 * vertex and edge schemas into a reusable graph descriptor.
 */
final case class GraphSchema[N <: Tuple, V <: Tuple](
    private[sqala] val __name__ : String,
    private[sqala] val __items__ : V
)

/**
 * A vertex schema created by `vertex[T]`.
 */
final case class GraphVertexSchema[T](
    private[sqala] val __alias__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData
)

/**
 * An edge schema created by `edge[T]`.
 */
final case class GraphEdgeSchema[T](
    private[sqala] val __alias__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData,
    private[sqala] val __where__ : Option[SqlExpr],
    private[sqala] val __quantifier__ : Option[SqlGraphQuantifier]
)

/**
 * Transforms graph schema definitions into runtime graph elements,
 * assigning query context and aliases. `CL` is the current query context level.
 */
trait TransformGraphSchema[T, CL <: Int]:
    /**
     * The transformed type.
     */
    type R

    /**
     * Transforms the schema item into a graph element.
     */
    def transform(x: T)(using qc: QueryContext[CL]): R

object TransformGraphSchema:
    type Aux[T, CL <: Int, O] = TransformGraphSchema[T, CL]:
        type R = O

    given vertex[T, CL <: Int]: Aux[GraphVertexSchema[T], CL, GraphVertex[T, CL]] = new
        TransformGraphSchema[GraphVertexSchema[T], CL]:
            type R = GraphVertex[T, CL]

            def transform(x: GraphVertexSchema[T])(using qc: QueryContext[CL]): R =
                GraphVertex(x.__alias__, x.__metaData__)

    given edge[T, CL <: Int]: Aux[GraphEdgeSchema[T], CL, GraphEdge[T, EmptyTuple, CL]] = new
        TransformGraphSchema[GraphEdgeSchema[T], CL]:
            type R = GraphEdge[T, EmptyTuple, CL]

            def transform(x: GraphEdgeSchema[T])(using qc: QueryContext[CL]): R =
                GraphEdge(x.__alias__, x.__metaData__, x.__where__, x.__quantifier__)

    given tuple[H, T <: Tuple, CL <: Int](using
        sh: TransformGraphSchema[H, CL],
        st: TransformGraphSchema[T, CL],
        tt: ToTuple[st.R]
    ): Aux[H *: T, CL, sh.R *: tt.R] =
        new TransformGraphSchema[H *: T, CL]:
            type R = sh.R *: tt.R

            def transform(x: H *: T)(using qc: QueryContext[CL]): R =
                sh.transform(x.head) *: tt.toTuple(st.transform(x.tail))

    given emptyTuple[CL <: Int]: Aux[EmptyTuple, CL, EmptyTuple] = new
        TransformGraphSchema[EmptyTuple, CL]:
            type R = EmptyTuple

            def transform(x: EmptyTuple)(using qc: QueryContext[CL]): R =
                EmptyTuple

/**
 * A graph context within a `graphTable` block.
 */
final case class Graph[N <: Tuple, V <: Tuple, L <: Int](
    private[sqala] val __name__ : String,
    private[sqala] val __items__ : V
)(using private[sqala] val qc: QueryContext[L]) extends Selectable:
    /**
     * The structural type declaring available vertex and edge labels
     * as a named tuple. Required by `Selectable`.
     */
    type Fields = NamedTuple[N, V]

    /**
     * Resolves a vertex or edge label by name, assigning a fresh
     * alias for use in the graph pattern. Required by `Selectable`.
     */
    inline def selectDynamic(name: String): Any =
        val alias = qc.fetchAlias
        val index = constValue[Index[N, name.type, 0]]
        val node = __items__.toList(index)
        node match
            case GraphVertex(_, metaData) =>
                GraphVertex(Some(alias), metaData)
            case GraphEdge(_, metaData, where, quantifier) =>
                GraphEdge(Some(alias), metaData, where, quantifier)

    /**
     * Starts a graph pattern match. The pattern is built from
     * vertex and edge terms connected by graph symbols (`|-|`,
     * `|->|`, `|~|`, etc.). Maps to `MATCH` in `GRAPH_TABLE`.
     *
     * {{{
     * g.matching(
     *     ("a" is g.person) |-|
     *     ("f" is g.friends.filter(f => f.personAId > 1)) |->|
     *     ("b" is g.person)
     * )
     * }}}
     */
    def matching[PN <: Tuple, PV <: Tuple, POKS <: Tuple](pattern: GraphPattern[PN, PV, POKS, L])(using
        GraphContext
    ): GraphMatch[GraphPattern[PN, PV, POKS, L], POKS, L] =
        GraphMatch(__name__, pattern, pattern.__patterns__, None, None)

/**
 * A term in a graph pattern, representing either a vertex or
 * an edge.
 */
sealed trait GraphPatternTerm[V, OKS <: Tuple, L <: Int]:
    /**
     * Converts this graph pattern term to its SQL AST representation.
     */
    private[sqala] def asTerm: SqlGraphPatternTerm

/**
 * A vertex in a graph pattern. Created by `vertex[T]` and
 * accessed via the `is` method in `matching` blocks.
 */
final case class GraphVertex[T, L <: Int](
    private[sqala] val __alias__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData
)(using private[sqala] val qc: QueryContext[L]) extends Selectable with GraphPatternTerm[T, EmptyTuple, L]:
    /**
     * The structural type declaring available columns as a named tuple.
     * Required by `Selectable`.
     */
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T, Column, L]]
        ]

    /**
     * Converts this vertex to a `VERTEX` term in the graph pattern AST,
     * carrying the assigned alias and label name.
     */
    override private[sqala] def asTerm: SqlGraphPatternTerm =
        SqlGraphPatternTerm.Vertex(
            __alias__,
            Some(SqlGraphLabel.Label(__metaData__.tableName)),
            None
        )

    /**
     * Runtime column accessor. Required by `Selectable`.
     */
    def selectDynamic(name: String): Any =
        val index = __metaData__.fieldNames.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__alias__, __metaData__.columnNames(index)))

/**
 * An edge in a graph pattern. Created by `edge[T]` and accessed
 * via the `is` method in `matching` blocks.
 */
final case class GraphEdge[T, OKS <: Tuple, L <: Int](
    private[sqala] val __alias__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData,
    private[sqala] val __where__ : Option[SqlExpr],
    private[sqala] val __quantifier__ : Option[SqlGraphQuantifier]
)(using private[sqala] val qc: QueryContext[L]) extends Selectable with GraphPatternTerm[T, OKS, L]:
    /**
     * The structural type declaring available columns as a named tuple.
     * Required by `Selectable`.
     */
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T, Column, L]]
        ]

    /**
     * Converts this edge to an `EDGE` term in the graph pattern AST,
     * carrying the assigned alias, label, optional `WHERE` condition,
     * and quantifier.
     */
    override private[sqala] def asTerm: SqlGraphPatternTerm =
        __quantifier__.map: q =>
            SqlGraphPatternTerm.Quantified(
                SqlGraphPatternTerm.Edge(
                    SqlGraphSymbol.Dash,
                    __alias__,
                    Some(SqlGraphLabel.Label(__metaData__.tableName)),
                    __where__,
                    SqlGraphSymbol.Dash
                ),
                q
            )
        .getOrElse:
            SqlGraphPatternTerm.Edge(
                SqlGraphSymbol.Dash,
                __alias__,
                Some(SqlGraphLabel.Label(__metaData__.tableName)),
                __where__,
                SqlGraphSymbol.Dash
            )

    /**
     * Runtime column accessor. Required by `Selectable`.
     */
    def selectDynamic(name: String): Any =
        val index = __metaData__.fieldNames.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__alias__, __metaData__.columnNames(index)))

    /**
     * Adds a `WHERE` condition to the edge. Maps to the `WHERE`
     * clause inside the edge pattern in `GRAPH_TABLE`.
     *
     * {{{
     * ("f" is g.friends.filter(f => f.personAId > 1))
     * }}}
     */
    def filter[F](f: Table[T, Column, L] => F)(using
        gc: GraphContext,
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): GraphEdge[T, c.R, L] =
        val table = Table[T, Column, L](
            __alias__,
            __metaData__,
            SqlTable.Ident(__alias__.get, None, None, None, None)
        )
        val cond = a.asExpr(f(table)).asSqlExpr
        copy(__where__ = __where__.map(SqlExpr.Binary(_, SqlBinaryOperator.And, cond)).orElse(Some(cond)))

    /**
     * Alias of `filter`, provided for users familiar with `WHERE`.
     *
     * {{{
     * ("f" is g.friends.where(f => f.personAId > 1))
     * }}}
     */
    def where[F](f: Table[T, Column, L] => F)(using
        gc: GraphContext,
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): GraphEdge[T, c.R, L] =
        filter(f)

    /**
     * Zero or more edge repetitions. Maps to `*`.
     *
     * {{{
     * g.friends.*
     * }}}
     */
    def *(using GraphContext): GraphEdge[T, OKS, L] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Asterisk
            )
        )

    /**
     * One or more edge repetitions. Maps to `+`.
     *
     * {{{
     * g.friends.+
     * }}}
     */
    def +(using GraphContext): GraphEdge[T, OKS, L] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Plus
            )
        )

    /**
     * Zero or one edge repetition. Maps to `?`.
     *
     * {{{
     * g.friends.?
     * }}}
     */
    def ?(using GraphContext): GraphEdge[T, OKS, L] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Question
            )
        )

    /**
     * Between `m` and `n` edge repetitions. Maps to `{m, n}`.
     *
     * {{{
     * g.friends.between(1, 10)
     * }}}
     */
    def between[S, E](start: S, end: E)(using
        gc: GraphContext,
        as: AsExpr[S, L],
        ae: AsExpr[E, L],
        ns: SqlNumber[as.R],
        ne: SqlNumber[ae.R],
        kts: KindToTuple[as.K],
        kte: KindToTuple[ae.K],
        is: CanInSimpleClause[kts.R],
        ie: CanInSimpleClause[kte.R],
        es: ExcludeCurrentLevelColumn[kts.R, L],
        ee: ExcludeCurrentLevelColumn[kte.R, L],
        cs: CombineKindTuple[OKS, es.R],
        c: CombineKindTuple[cs.R, ee.R]
    ): GraphEdge[T, c.R, L] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Between(
                    Some(as.asExpr(start).asSqlExpr),
                    Some(ae.asExpr(end).asSqlExpr)
                )
            )
        )

    /**
     * At least `n` edge repetitions. Maps to `{m,}`.
     *
     * {{{
     * g.friends.least(3)
     * }}}
     */
    def least[T](x: T)(using
        gc: GraphContext,
        a: AsExpr[T, L],
        n: SqlNumber[a.R],
        kt: KindToTuple[a.K],
        i: CanInSimpleClause[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): GraphEdge[T, c.R, L] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Between(
                    Some(a.asExpr(x).asSqlExpr),
                    None
                )
            )
        )

    /**
     * At most `n` edge repetitions. Maps to `{,n}`.
     *
     * {{{
     * g.friends.most(10)
     * }}}
     */
    def most[T](x: T)(using
        gc: GraphContext,
        a: AsExpr[T, L],
        n: SqlNumber[a.R],
        kt: KindToTuple[a.K],
        i: CanInSimpleClause[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): GraphEdge[T, c.R, L] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Between(
                    None,
                    Some(a.asExpr(x).asSqlExpr)
                )
            )
        )

    /**
     * Exactly `n` edge repetitions. Maps to `{n}`.
     *
     * {{{
     * g.friends.at(3)
     * }}}
     */
    def at[T](x: T)(using
        gc: GraphContext,
        a: AsExpr[T, L],
        n: SqlNumber[a.R],
        kt: KindToTuple[a.K],
        i: CanInSimpleClause[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): GraphEdge[T, c.R, L] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Quantity(
                    a.asExpr(x).asSqlExpr
                )
            )
        )

/**
 * An intermediate graph pattern after an edge has been connected,
 * waiting for a vertex on the right side.
 */
final case class GraphPatternPart[N <: Tuple, V <: Tuple, OKS <: Tuple, L <: Int](
    private[sqala] val __items__ : V,
    private[sqala] val __patterns__ : List[SqlGraphPatternTerm],
    private[sqala] val __leftSymbol__ : SqlGraphSymbol
)(using private[sqala] val qc: QueryContext[L]):
    /**
     * Connects to a vertex with a dash symbol, completing the
     * edge-to-vertex connection. Maps to `-` in `GRAPH_TABLE`.
     *
     * {{{
     * ... |-| ("b" is g.person)
     * }}}
     */
    def |-|[PN, PV, POKS <: Tuple](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphVertex[PV, L]], POKS, L])(using
        gc: GraphContext,
        c: CombineKindTuple[OKS, POKS]
    ): GraphPattern[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphVertex[PV, L]]], c.R, L] =
        val rightSymbol = SqlGraphSymbol.Dash
        val last = __patterns__.last match
            case e: SqlGraphPatternTerm.Edge =>
                e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol)
            case SqlGraphPatternTerm.Quantified(e: SqlGraphPatternTerm.Edge, q) =>
                SqlGraphPatternTerm.Quantified(e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol), q)
            case p => p
        GraphPattern(
            __items__ ++ pattern.__items__,
            __patterns__.dropRight(1) ++ (last :: pattern.__patterns__)
        )

    /**
     * Connects to a vertex with a tilde symbol, completing the
     * edge-to-vertex connection. Maps to `~` in `GRAPH_TABLE`.
     *
     * {{{
     * ... |~| ("b" is g.person)
     * }}}
     */
    def |~|[PN, PV, POKS <: Tuple](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphVertex[PV, L]], POKS, L])(using
        gc: GraphContext,
        c: CombineKindTuple[OKS, POKS]
    ): GraphPattern[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphVertex[PV, L]]], c.R, L] =
        val rightSymbol = SqlGraphSymbol.Tilde
        val last = __patterns__.last match
            case e: SqlGraphPatternTerm.Edge =>
                e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol)
            case SqlGraphPatternTerm.Quantified(e: SqlGraphPatternTerm.Edge, q) =>
                SqlGraphPatternTerm.Quantified(e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol), q)
            case p => p
        GraphPattern(
            __items__ ++ pattern.__items__,
            __patterns__.dropRight(1) ++ (last :: pattern.__patterns__)
        )

    /**
     * Connects to a vertex with a right-arrow symbol, completing
     * the edge-to-vertex connection. Maps to `->` in `GRAPH_TABLE`.
     *
     * {{{
     * ... |->| ("b" is g.person)
     * }}}
     */
    def |->|[PN, PV, POKS <: Tuple](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphVertex[PV, L]], POKS, L])(using
        gc: GraphContext,
        c: CombineKindTuple[OKS, POKS]
    ): GraphPattern[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphVertex[PV, L]]], c.R, L] =
        val rightSymbol = SqlGraphSymbol.RightArrow
        val last = __patterns__.last match
            case e: SqlGraphPatternTerm.Edge =>
                e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol)
            case SqlGraphPatternTerm.Quantified(e: SqlGraphPatternTerm.Edge, q) =>
                SqlGraphPatternTerm.Quantified(e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol), q)
            case p => p
        GraphPattern(
            __items__ ++ pattern.__items__,
            __patterns__.dropRight(1) ++ (last :: pattern.__patterns__)
        )

    /**
     * Connects to a vertex with a right-tilde-arrow symbol,
     * completing the edge-to-vertex connection. Maps to `~>`
     * in `GRAPH_TABLE`.
     *
     * {{{
     * ... |~>| ("b" is g.person)
     * }}}
     */
    def |~>|[PN, PV, POKS <: Tuple](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphVertex[PV, L]], POKS, L])(using
        gc: GraphContext,
        c: CombineKindTuple[OKS, POKS]
    ): GraphPattern[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphVertex[PV, L]]], c.R, L] =
        val rightSymbol = SqlGraphSymbol.RightTildeArrow
        val last = __patterns__.last match
            case e: SqlGraphPatternTerm.Edge =>
                e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol)
            case SqlGraphPatternTerm.Quantified(e: SqlGraphPatternTerm.Edge, q) =>
                SqlGraphPatternTerm.Quantified(e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol), q)
            case p => p
        GraphPattern(
            __items__ ++ pattern.__items__,
            __patterns__.dropRight(1) ++ (last :: pattern.__patterns__)
        )

/**
 * A graph pattern at a vertex, ready to connect to an edge.
 */
final case class GraphPattern[N <: Tuple, V <: Tuple, OKS <: Tuple, L <: Int](
    private[sqala] val __items__ : V,
    private[sqala] val __patterns__ : List[SqlGraphPatternTerm]
)(using private[sqala] val qc: QueryContext[L]) extends Selectable:
    /**
     * The structural type declaring matched pattern aliases as a
     * named tuple. Required by `Selectable`.
     */
    type Fields = NamedTuple[N, V]

    /**
     * Runtime pattern alias accessor. Required by `Selectable`.
     */
    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

    /**
     * Connects to an edge with a dash symbol. Maps to `-` in
     * `GRAPH_TABLE`.
     *
     * {{{
     * ("a" is g.person) |-| ...
     * }}}
     */
    def |-|[PN, PV, POKS <: Tuple](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphEdge[PV, POKS, L]], POKS, L])(using
        gc: GraphContext,
        c: CombineKindTuple[OKS, POKS]
    ): GraphPatternPart[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphEdge[PV, POKS, L]]], c.R, L] =
        val leftSymbol = SqlGraphSymbol.Dash
        GraphPatternPart(
            __items__ ++ pattern.__items__,
            __patterns__ ++ pattern.__patterns__,
            leftSymbol
        )

    /**
     * Connects to an edge with a tilde symbol. Maps to `~` in
     * `GRAPH_TABLE`.
     *
     * {{{
     * ("a" is g.person) |~| ...
     * }}}
     */
    def |~|[PN, PV, POKS <: Tuple](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphEdge[PV, POKS, L]], POKS, L])(using
        gc: GraphContext,
        c: CombineKindTuple[OKS, POKS]
    ): GraphPatternPart[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphEdge[PV, POKS, L]]], c.R, L] =
        val leftSymbol = SqlGraphSymbol.Tilde
        GraphPatternPart(
            __items__ ++ pattern.__items__,
            __patterns__ ++ pattern.__patterns__,
            leftSymbol
        )

    /**
     * Connects to an edge with a left-arrow symbol. Maps to `<-`
     * in `GRAPH_TABLE`.
     *
     * {{{
     * ("a" is g.person) |<-| ...
     * }}}
     */
    def |<-|[PN, PV, POKS <: Tuple](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphEdge[PV, POKS, L]], POKS, L])(using
        gc: GraphContext,
        c: CombineKindTuple[OKS, POKS]
    ): GraphPatternPart[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphEdge[PV, POKS, L]]], c.R, L] =
        val leftSymbol = SqlGraphSymbol.LeftArrow
        GraphPatternPart(
            __items__ ++ pattern.__items__,
            __patterns__ ++ pattern.__patterns__,
            leftSymbol
        )

    /**
     * Connects to an edge with a left-tilde-arrow symbol.
     * Maps to `<~` in `GRAPH_TABLE`.
     *
     * {{{
     * ("a" is g.person) |<~| ...
     * }}}
     */
    def |<~|[PN, PV, POKS <: Tuple](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphEdge[PV, POKS, L]], POKS, L])(using
        gc: GraphContext,
        c: CombineKindTuple[OKS, POKS]
    ): GraphPatternPart[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphEdge[PV, POKS, L]]], c.R, L] =
        val leftSymbol = SqlGraphSymbol.LeftTildeArrow
        GraphPatternPart(
            __items__ ++ pattern.__items__,
            __patterns__ ++ pattern.__patterns__,
            leftSymbol
        )

/**
 * A graph match after `matching`.
 */
final case class GraphMatch[T, OKS <: Tuple, L <: Int](
    private[sqala] val __name__ : String,
    private[sqala] val __pattern__ : T,
    private[sqala] val __patterns__ : List[SqlGraphPatternTerm],
    private[sqala] val __where__ : Option[SqlExpr],
    private[sqala] val __rows__ : Option[SqlGraphRowsMode]
)(using private[sqala] val qc: QueryContext[L]):
    /**
     * Defines the output columns of the graph query. Accepts a
     * named tuple where each field becomes a result column.
     * The function's parameter provides typed access to all
     * matched pattern aliases.
     *
     * {{{
     * .columns(p => (
     *     personA = p.a.name,
     *     personB = p.b.name,
     *     meetingDate = p.f.meetingDate
     * ))
     * }}}
     */
    def columns[MN <: Tuple, MV <: Tuple](f: T => NamedTuple[MN, MV])(using
        gc: GraphContext,
        m: AsMap[MV, L],
        p: AsTableParam[m.R, L],
        tt: ToTuple[p.R],
        i: CanInSimpleClause[m.KS],
        e: ExcludeCurrentLevelColumn[m.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): FromGraph[MN, tt.R, c.R, L] =
        val alias = qc.fetchAlias
        val columnsValue = f(__pattern__).toTuple
        val columnItems = m.asSelectItems(columnsValue, 1)
        FromGraph(
            __name__,
            SqlGraphPattern(
                None,
                __patterns__.reduce((x, y) => SqlGraphPatternTerm.And(x, y))
            ) :: Nil,
            __where__,
            __rows__,
            tt.toTuple(p.asTableParam(Some(alias), 1)),
            columnItems,
            false,
            Some(alias)
        )

    /**
     * Adds a `WHERE` condition to the graph match.
     *
     * {{{
     * .filter(p => p.a.id > 1)
     * }}}
     */
    def filter[F](f: T => F)(using
        gc: GraphContext,
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): GraphMatch[T, c.R, L] =
        val cond = a.asExpr(f(__pattern__)).asSqlExpr
        GraphMatch(
            __name__,
            __pattern__,
            __patterns__,
            __where__.map(SqlExpr.Binary(_, SqlBinaryOperator.And, cond)).orElse(Some(cond)),
            __rows__
        )

    /**
     * Alias of `filter`, provided for users familiar with `WHERE`.
     *
     * {{{
     * .where(p => p.a.id > 1)
     * }}}
     */
    def where[F](f: T => F)(using
        gc: GraphContext,
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): GraphMatch[T, c.R, L] =
        filter(f)

/**
 * A graph table source produced by `columns`, ready to be passed
 * to `from`.
 */
final case class FromGraph[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Graph
) extends AnyTable

object FromGraph:
    def apply[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](
        name: String,
        patterns: List[SqlGraphPattern],
        where: Option[SqlExpr],
        rows: Option[SqlGraphRowsMode],
        items: V,
        columns: List[SqlSelectItem],
        lateral: Boolean,
        alias: Option[String]
    ): FromGraph[N, V, OKS, CL] =
        new FromGraph(
            alias,
            items,
            SqlTable.Graph(
                lateral,
                name,
                None,
                patterns.toNonEmptyList,
                where,
                rows,
                columns.toNonEmptyList,
                None,
                alias.map(SqlTableAlias(_, Nil)),
                None
            )
        )

/**
 * A table reference produced by `from` when a `FromGraph` is
 * passed, enabling typed column access via `selectDynamic`.
 */
final case class GraphTable[N <: Tuple, V <: Tuple, L <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Graph
) extends Selectable with AnyTable:
    /**
     * The structural type declaring available columns as a named tuple.
     * Required by `Selectable`.
     */
    type Fields = NamedTuple[N, V]

    /**
     * Runtime column accessor. Required by `Selectable`.
     */
    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)
