package sqala.static.ext.dsl.table

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.table.{SqlGraphLabel, SqlGraphRowsMode, SqlGraphSymbol}
import sqala.ir.*
import sqala.metadata.{SqlBoolean, TableMetaData}
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.AsMap
import sqala.static.dsl.table.*
import sqala.static.ext.dsl.GraphContext
import sqala.util.NonEmptyList

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}
import scala.compiletime.ops.int.-

/**
 * A graph schema definition created by `createGraph`, combining
 * vertex and edge schemas into a reusable graph descriptor.
 */
final class GraphSchema[N <: Tuple, V <: Tuple](
    private[sqala] val name: String,
    private[sqala] val items: V
)

/**
 * A vertex schema created by `vertex[T]`.
 */
final class GraphVertexSchema[T](
    private[sqala] val metaData: TableMetaData
)

/**
 * An edge schema created by `edge[T]`.
 */
final class GraphEdgeSchema[T](
    private[sqala] val metaData: TableMetaData
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
    def transform(x: T): R

object TransformGraphSchema:
    type Aux[T, CL <: Int, O] = TransformGraphSchema[T, CL]:
        type R = O

    given vertex[T, CL <: Int]: Aux[GraphVertexSchema[T], CL, GraphVertex[T, CL]] = new
        TransformGraphSchema[GraphVertexSchema[T], CL]:
            type R = GraphVertex[T, CL]

            def transform(x: GraphVertexSchema[T]): R =
                val table: IrTable.Ident = IrTable.Ident(x.metaData.tableName, Nil)
                GraphVertex(table, x.metaData)

    given edge[T, CL <: Int]: Aux[GraphEdgeSchema[T], CL, GraphEdge[T, EmptyTuple, CL]] = new
        TransformGraphSchema[GraphEdgeSchema[T], CL]:
            type R = GraphEdge[T, EmptyTuple, CL]

            def transform(x: GraphEdgeSchema[T]): R =
                val table: IrTable.Ident = IrTable.Ident(x.metaData.tableName, Nil)
                GraphEdge(table, x.metaData, None, None)

    given tuple[H, T <: Tuple, CL <: Int](using
        sh: TransformGraphSchema[H, CL],
        st: TransformGraphSchema[T, CL],
        tt: ToTuple[st.R]
    ): Aux[H *: T, CL, sh.R *: tt.R] =
        new TransformGraphSchema[H *: T, CL]:
            type R = sh.R *: tt.R

            def transform(x: H *: T): R =
                sh.transform(x.head) *: tt.toTuple(st.transform(x.tail))

    given emptyTuple[CL <: Int]: Aux[EmptyTuple, CL, EmptyTuple] = new
        TransformGraphSchema[EmptyTuple, CL]:
            type R = EmptyTuple

            def transform(x: EmptyTuple): R =
                EmptyTuple

/**
 * A graph context within a `graphTable` block.
 */
final class Graph[N <: Tuple, V <: Tuple, L <: Int](
    private[sqala] val __name__ : String,
    private[sqala] val __items__ : V
) extends Selectable:
    /**
     * The structural type declaring available vertex and edge labels
     * as a named tuple. Required by `Selectable`.
     */
    type Fields = NamedTuple[N, V]

    private[sqala] def asPatternTerm(item: Any): Any =
        item match
            case v: GraphVertex[?, ?] =>
                v.createNewPatternTerm
            case e: GraphEdge[?, ?, ?] =>
                e.createNewPatternTerm
            case other =>
                other

    /**
     * Compile-time label accessor. Required by `Selectable`.
     */
    inline def selectDynamic(name: String): Any =
        asPatternTerm(TableSelectMacro.selectMapped[N](__items__, name))

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
    private[sqala] def asTerm: IrGraphPatternTerm

/**
 * A vertex in a graph pattern. Created by `vertex[T]` and
 * accessed via the `is` method in `matching` blocks.
 */
final class GraphVertex[T, L <: Int](
    private[sqala] val __table__ : IrTable.Ident,
    private[sqala] val __metaData__ : TableMetaData
) extends GraphPatternTerm[T, EmptyTuple, L] with Selectable:
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
    override private[sqala] def asTerm: IrGraphPatternTerm =
        IrGraphPatternTerm.Vertex(
            __table__,
            Some(SqlGraphLabel.Label(__metaData__.tableName)),
            None
        )

    /**
     * Creates a new graph pattern term with the same label name.
     */
    private[sqala] def createNewPatternTerm: GraphVertex[T, L] =
        GraphVertex(__table__.copy(), __metaData__)

    /**
     * Compile-time column accessor. Required by `Selectable`.
     */
    inline def selectDynamic(name: String): Any =
        TableSelectMacro.select[Unwrap[T, Option], Column[L]](__table__, true, __metaData__, name)

/**
 * An edge in a graph pattern. Created by `edge[T]` and accessed
 * via the `is` method in `matching` blocks.
 */
final class GraphEdge[T, OKS <: Tuple, L <: Int](
    private[sqala] val __table__ : IrTable.Ident,
    private[sqala] val __metaData__ : TableMetaData,
    private[sqala] val __where__ : Option[IrExpr],
    private[sqala] val __quantifier__ : Option[IrGraphQuantifier]
) extends GraphPatternTerm[T, OKS, L] with Selectable:
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
    override private[sqala] def asTerm: IrGraphPatternTerm =
        __quantifier__.map: q =>
            IrGraphPatternTerm.Quantified(
                IrGraphPatternTerm.Edge(
                    SqlGraphSymbol.Dash,
                    __table__,
                    Some(SqlGraphLabel.Label(__metaData__.tableName)),
                    __where__,
                    SqlGraphSymbol.Dash
                ),
                q
            )
        .getOrElse:
            IrGraphPatternTerm.Edge(
                SqlGraphSymbol.Dash,
                __table__,
                Some(SqlGraphLabel.Label(__metaData__.tableName)),
                __where__,
                SqlGraphSymbol.Dash
            )

    /**
     * Creates a new graph pattern term with the same label name.
     */
    private[sqala] def createNewPatternTerm: GraphEdge[T, OKS, L] =
        GraphEdge(__table__.copy(), __metaData__, __where__, __quantifier__)

    /**
     * Compile-time column accessor. Required by `Selectable`.
     */
    inline def selectDynamic(name: String): Any =
        TableSelectMacro.select[Unwrap[T, Option], Column[L]](__table__, true, __metaData__, name)

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
        b: ValidateArgType[SqlBoolean, a.R, "boolean"],
        kt: KindToTuple[a.K],
        i: CanInSimpleClause[kt.R, "filter"],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): GraphEdge[T, c.R, L] =
        val table = Table[T, Column, L](
            __table__,
            __metaData__
        )
        val cond = a.asExpr(f(table)).expr
        GraphEdge(
            __table__,
            __metaData__,
            __where__.map(IrExpr.Binary(_, SqlBinaryOperator.And, cond)).orElse(Some(cond)),
            __quantifier__
        )

    /**
     * Zero or more edge repetitions. Maps to `*`.
     *
     * {{{
     * g.friends.*
     * }}}
     */
    def *(using GraphContext): GraphEdge[T, OKS, L] =
        GraphEdge(
            __table__,
            __metaData__,
            __where__,
            Some(
                IrGraphQuantifier.Asterisk
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
        GraphEdge(
            __table__,
            __metaData__,
            __where__,
            Some(
                IrGraphQuantifier.Plus
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
        GraphEdge(
            __table__,
            __metaData__,
            __where__,
            Some(
                IrGraphQuantifier.Question
            )
        )

    /**
     * Between `m` and `n` edge repetitions. Maps to `{m, n}`.
     *
     * {{{
     * g.friends.between(1, 10)
     * }}}
     */
    def between(start: Int, end: Int): GraphEdge[T, OKS, L] =
        GraphEdge(
            __table__,
            __metaData__,
            __where__,
            Some(
                IrGraphQuantifier.Between(
                    Some(IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.NumberLiteral(BigDecimal(start))))),
                    Some(IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.NumberLiteral(BigDecimal(end)))))
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
    def least(x: Int): GraphEdge[T, OKS, L] =
        GraphEdge(
            __table__,
            __metaData__,
            __where__,
            Some(
                IrGraphQuantifier.Between(
                    Some(IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.NumberLiteral(BigDecimal(x))))),
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
    def most(x: Int): GraphEdge[T, OKS, L] =
        GraphEdge(
            __table__,
            __metaData__,
            __where__,
            Some(
                IrGraphQuantifier.Between(
                    None,
                    Some(IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.NumberLiteral(BigDecimal(x)))))
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
    def at(x: Int): GraphEdge[T, OKS, L] =
        GraphEdge(
            __table__,
            __metaData__,
            __where__,
            Some(
                IrGraphQuantifier.Quantity(
                    IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.NumberLiteral(BigDecimal(x))))
                )
            )
        )

/**
 * An intermediate graph pattern after an edge has been connected,
 * waiting for a vertex on the right side.
 */
final class GraphPatternPart[N <: Tuple, V <: Tuple, OKS <: Tuple, L <: Int](
    private[sqala] val items: V,
    private[sqala] val patterns: List[IrGraphPatternTerm],
    private[sqala] val leftSymbol: SqlGraphSymbol
):
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
        val last = patterns.last match
            case e: IrGraphPatternTerm.Edge =>
                e.copy(leftSymbol = leftSymbol, rightSymbol = rightSymbol)
            case IrGraphPatternTerm.Quantified(e: IrGraphPatternTerm.Edge, q) =>
                IrGraphPatternTerm.Quantified(e.copy(leftSymbol = leftSymbol, rightSymbol = rightSymbol), q)
            case p => p
        GraphPattern(
            items ++ pattern.__items__,
            patterns.dropRight(1) ++ (last :: pattern.__patterns__)
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
        val last = patterns.last match
            case e: IrGraphPatternTerm.Edge =>
                e.copy(leftSymbol = leftSymbol, rightSymbol = rightSymbol)
            case IrGraphPatternTerm.Quantified(e: IrGraphPatternTerm.Edge, q) =>
                IrGraphPatternTerm.Quantified(e.copy(leftSymbol = leftSymbol, rightSymbol = rightSymbol), q)
            case p => p
        GraphPattern(
            items ++ pattern.__items__,
            patterns.dropRight(1) ++ (last :: pattern.__patterns__)
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
        val last = patterns.last match
            case e: IrGraphPatternTerm.Edge =>
                e.copy(leftSymbol = leftSymbol, rightSymbol = rightSymbol)
            case IrGraphPatternTerm.Quantified(e: IrGraphPatternTerm.Edge, q) =>
                IrGraphPatternTerm.Quantified(e.copy(leftSymbol = leftSymbol, rightSymbol = rightSymbol), q)
            case p => p
        GraphPattern(
            items ++ pattern.__items__,
            patterns.dropRight(1) ++ (last :: pattern.__patterns__)
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
        val last = patterns.last match
            case e: IrGraphPatternTerm.Edge =>
                e.copy(leftSymbol = leftSymbol, rightSymbol = rightSymbol)
            case IrGraphPatternTerm.Quantified(e: IrGraphPatternTerm.Edge, q) =>
                IrGraphPatternTerm.Quantified(e.copy(leftSymbol = leftSymbol, rightSymbol = rightSymbol), q)
            case p => p
        GraphPattern(
            items ++ pattern.__items__,
            patterns.dropRight(1) ++ (last :: pattern.__patterns__)
        )

/**
 * A graph pattern at a vertex, ready to connect to an edge.
 */
final class GraphPattern[N <: Tuple, V <: Tuple, OKS <: Tuple, L <: Int](
    private[sqala] val __items__ : V,
    private[sqala] val __patterns__ : List[IrGraphPatternTerm]
) extends Selectable:
    /**
     * The structural type declaring matched pattern aliases as a
     * named tuple. Required by `Selectable`.
     */
    type Fields = NamedTuple[N, V]

    /**
     * Compile-time pattern alias accessor. Required by `Selectable`.
     */
    inline def selectDynamic(name: String): Any =
        TableSelectMacro.selectMapped[N](__items__, name)

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
final class GraphMatch[T, OKS <: Tuple, L <: Int](
    private[sqala] val name: String,
    private[sqala] val pattern: T,
    private[sqala] val patterns: List[IrGraphPatternTerm],
    private[sqala] val where: Option[IrExpr],
    private[sqala] val rows: Option[SqlGraphRowsMode]
):
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
        i: CanInSimpleClause[m.KS, "columns"],
        e: ExcludeCurrentLevelColumn[m.KS, L],
        c: CombineKindTuple[OKS, e.R]
    ): FromGraph[MN, tt.R, c.R, L] =
        val columnsValue = f(pattern).toTuple
        val columnItems = m.asSelectItems(columnsValue, 1)
        val table: IrTable.Graph = IrTable.Graph(
            false,
            name,
            None,
            NonEmptyList(
                IrGraphPattern(
                    None,
                    patterns.reduce((x, y) => IrGraphPatternTerm.And(x, y))
                ),
                Nil
            ),
            where,
            rows,
            columnItems,
            None,
            Nil
        )
        FromGraph(
            table,
            tt.toTuple(p.asTableParam(table, 1))
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
        b: ValidateArgType[SqlBoolean, a.R, "boolean"],
        kt: KindToTuple[a.K],
        i: CanInSimpleClause[kt.R, "filter"],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): GraphMatch[T, c.R, L] =
        val cond = a.asExpr(f(pattern)).expr
        GraphMatch(
            name,
            pattern,
            patterns,
            where.map(IrExpr.Binary(_, SqlBinaryOperator.And, cond)).orElse(Some(cond)),
            rows
        )

/**
 * A graph table source produced by `columns`, ready to be passed
 * to `from`.
 */
final class FromGraph[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](
    private[sqala] val table: IrTable.Graph,
    private[sqala] val items: V
) extends AnyTable

object FromGraph:
    given asTable[N <: Tuple, V <: Tuple, TOKS <: Tuple, CL <: Int]: AsTable.Aux[FromGraph[N, V, TOKS, CL], CL, MappedTable[N, V, false, CL], TOKS] =
        new AsTable[FromGraph[N, V, TOKS, CL], CL]:
            type R = MappedTable[N, V, false, CL]

            type OKS = TOKS

            def asTable(x: FromGraph[N, V, TOKS, CL]): (R, IrTable) =
                (new MappedTable[N, V, false, CL](x.items), x.table)

    given asLateralTable[N <: Tuple, V <: Tuple, TOKS <: Tuple, CL <: Int](using
        e: ExcludeCurrentLevelColumn[TOKS, CL - 1]
    ): AsLateralTable.Aux[FromGraph[N, V, TOKS, CL], CL, MappedTable[N, V, false, CL - 1], e.R] =
        new AsLateralTable[FromGraph[N, V, TOKS, CL], CL]:
            type R = MappedTable[N, V, false, CL - 1]

            type OKS = e.R

            def asTable(x: FromGraph[N, V, TOKS, CL]): (R, IrTable) =
                val table: IrTable.Graph = x.table.copy(withLateral = true)
                (new MappedTable[N, V, false, CL - 1](x.items), table)