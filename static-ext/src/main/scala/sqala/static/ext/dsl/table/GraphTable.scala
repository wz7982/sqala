package sqala.static.ext.dsl.table

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}
import scala.compiletime.constValue
import sqala.metadata.TableMetaData
import sqala.ir.IrExpr
import sqala.ir.IrGraphQuantifier
import sqala.static.dsl.ToTuple
import sqala.static.ext.dsl.GraphContext
import sqala.ir.IrGraphPatternTerm
import sqala.static.dsl.Unwrap
import sqala.static.dsl.MapField
import sqala.ast.table.SqlGraphLabel
import sqala.ast.table.SqlGraphSymbol
import sqala.static.dsl.table.Table
import sqala.static.dsl.Column
import sqala.ast.expr.SqlBinaryOperator
import sqala.static.dsl.CombineKindTuple
import sqala.ast.table.SqlGraphRowsMode
import sqala.ir.IrGraphPattern
import sqala.util.NonEmptyList
import sqala.ir.IrSelectItem
import sqala.static.dsl.table.AnyTable
import sqala.ir.IrTable
import sqala.static.dsl.Expr
import sqala.static.dsl.AsExpr
import sqala.static.dsl.KindToTuple
import sqala.static.dsl.CanInSimpleClause
import sqala.static.dsl.ExcludeCurrentLevelColumn
import sqala.static.dsl.table.AsTableParam
import sqala.static.dsl.ColumnExpr
import scala.compiletime.constValueTuple
import sqala.static.dsl.statement.query.AsMap
import sqala.static.dsl.ValidateArgType
import sqala.metadata.SqlBoolean
import sqala.metadata.SqlNumber
import sqala.static.dsl.table.AsTable
import sqala.static.dsl.table.MappedTable
import sqala.static.dsl.table.AsLateralTable
import scala.compiletime.ops.int.-

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
    private[sqala] val __metaData__ : TableMetaData
)

/**
 * An edge schema created by `edge[T]`.
 */
final case class GraphEdgeSchema[T](
    private[sqala] val __metaData__ : TableMetaData
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
                val table: IrTable.Ident = IrTable.Ident(x.__metaData__.tableName, Nil)
                GraphVertex(table, x.__metaData__)

    given edge[T, CL <: Int]: Aux[GraphEdgeSchema[T], CL, GraphEdge[T, EmptyTuple, CL]] = new
        TransformGraphSchema[GraphEdgeSchema[T], CL]:
            type R = GraphEdge[T, EmptyTuple, CL]

            def transform(x: GraphEdgeSchema[T]): R =
                val table: IrTable.Ident = IrTable.Ident(x.__metaData__.tableName, Nil)
                GraphEdge(table, x.__metaData__, None, None)

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
final case class Graph[N <: Tuple, V <: Tuple, L <: Int](
    private[sqala] val __name__ : String,
    private[sqala] val __items__ : V,
    private[sqala] val __fields__ : List[String]
) extends Selectable:
    /**
     * The structural type declaring available vertex and edge labels
     * as a named tuple. Required by `Selectable`.
     */
    type Fields = NamedTuple[N, V]

    /**
     * Resolves a vertex or edge label by name, assigning a fresh
     * alias for use in the graph pattern. Required by `Selectable`.
     */
    def selectDynamic(name: String): Any =
        val index = __fields__.indexOf(name)
        val item = __items__.toList(index)
        item match
            case v: GraphVertex[?, ?] =>
                v.createNewPatternTerm
            case e: GraphEdge[?, ?, ?] =>
                e.createNewPatternTerm
            case _ =>
                item

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
case class GraphVertex[T, L <: Int](
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
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T, L]]
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
     * Runtime column accessor. Required by `Selectable`.
     */
    def selectDynamic(name: String): Any =
        val index = __metaData__.fields.toList.indexOf(name)
        ColumnExpr(IrExpr.Column(Some(__table__), __metaData__.columns.toList(index)))

/**
 * An edge in a graph pattern. Created by `edge[T]` and accessed
 * via the `is` method in `matching` blocks.
 */
final case class GraphEdge[T, OKS <: Tuple, L <: Int](
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
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T, L]]
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
     * Runtime column accessor. Required by `Selectable`.
     */
    def selectDynamic(name: String): Any =
        val index = __metaData__.fields.toList.indexOf(name)
        ColumnExpr(IrExpr.Column(Some(__table__), __metaData__.columns.toList(index)))

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
        copy(__where__ = __where__.map(IrExpr.Binary(_, SqlBinaryOperator.And, cond)).orElse(Some(cond)))

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
        copy(
            __quantifier__ = Some(
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
        copy(
            __quantifier__ = Some(
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
    def between[S, E](start: S, end: E)(using
        gc: GraphContext,
        as: AsExpr[S, L],
        ae: AsExpr[E, L],
        ns: ValidateArgType[SqlNumber, as.R, "numeric"],
        ne: ValidateArgType[SqlNumber, ae.R, "numeric"],
        kts: KindToTuple[as.K],
        kte: KindToTuple[ae.K],
        is: CanInSimpleClause[kts.R, "quantifier clause"],
        ie: CanInSimpleClause[kte.R, "quantifier clause"],
        es: ExcludeCurrentLevelColumn[kts.R, L],
        ee: ExcludeCurrentLevelColumn[kte.R, L],
        cs: CombineKindTuple[OKS, es.R],
        c: CombineKindTuple[cs.R, ee.R]
    ): GraphEdge[T, c.R, L] =
        copy(
            __quantifier__ = Some(
                IrGraphQuantifier.Between(
                    Some(as.asExpr(start).expr),
                    Some(ae.asExpr(end).expr)
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
        n: ValidateArgType[SqlNumber, a.R, "numeric"],
        kt: KindToTuple[a.K],
        i: CanInSimpleClause[kt.R, "quantifier clause"],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): GraphEdge[T, c.R, L] =
        copy(
            __quantifier__ = Some(
                IrGraphQuantifier.Between(
                    Some(a.asExpr(x).expr),
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
        n: ValidateArgType[SqlNumber, a.R, "numeric"],
        kt: KindToTuple[a.K],
        i: CanInSimpleClause[kt.R, "quantifier clause"],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): GraphEdge[T, c.R, L] =
        copy(
            __quantifier__ = Some(
                IrGraphQuantifier.Between(
                    None,
                    Some(a.asExpr(x).expr)
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
        n: ValidateArgType[SqlNumber, a.R, "numeric"],
        kt: KindToTuple[a.K],
        i: CanInSimpleClause[kt.R, "quantifier clause"],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): GraphEdge[T, c.R, L] =
        copy(
            __quantifier__ = Some(
                IrGraphQuantifier.Quantity(
                    a.asExpr(x).expr
                )
            )
        )

/**
 * An intermediate graph pattern after an edge has been connected,
 * waiting for a vertex on the right side.
 */
final case class GraphPatternPart[N <: Tuple, V <: Tuple, OKS <: Tuple, L <: Int](
    private[sqala] val __items__ : V,
    private[sqala] val __patterns__ : List[IrGraphPatternTerm],
    private[sqala] val __leftSymbol__ : SqlGraphSymbol
):
    /**
     * Connects to a vertex with a dash symbol, completing the
     * edge-to-vertex connection. Maps to `-` in `GRAPH_TABLE`.
     *
     * {{{
     * ... |-| ("b" is g.person)
     * }}}
     */
    inline def |-|[PN, PV, POKS <: Tuple](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphVertex[PV, L]], POKS, L])(using
        gc: GraphContext,
        c: CombineKindTuple[OKS, POKS]
    ): GraphPattern[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphVertex[PV, L]]], c.R, L] =
        val rightSymbol = SqlGraphSymbol.Dash
        val last = __patterns__.last match
            case e: IrGraphPatternTerm.Edge =>
                e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol)
            case IrGraphPatternTerm.Quantified(e: IrGraphPatternTerm.Edge, q) =>
                IrGraphPatternTerm.Quantified(e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol), q)
            case p => p
        val fields = constValueTuple[Tuple.Append[N, PN]].toArray.toList.map(_.toString)
        GraphPattern(
            __items__ ++ pattern.__items__,
            __patterns__.dropRight(1) ++ (last :: pattern.__patterns__),
            fields
        )

    /**
     * Connects to a vertex with a tilde symbol, completing the
     * edge-to-vertex connection. Maps to `~` in `GRAPH_TABLE`.
     *
     * {{{
     * ... |~| ("b" is g.person)
     * }}}
     */
    inline def |~|[PN, PV, POKS <: Tuple](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphVertex[PV, L]], POKS, L])(using
        gc: GraphContext,
        c: CombineKindTuple[OKS, POKS]
    ): GraphPattern[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphVertex[PV, L]]], c.R, L] =
        val rightSymbol = SqlGraphSymbol.Tilde
        val last = __patterns__.last match
            case e: IrGraphPatternTerm.Edge =>
                e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol)
            case IrGraphPatternTerm.Quantified(e: IrGraphPatternTerm.Edge, q) =>
                IrGraphPatternTerm.Quantified(e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol), q)
            case p => p
        val fields = constValueTuple[Tuple.Append[N, PN]].toArray.toList.map(_.toString)
        GraphPattern(
            __items__ ++ pattern.__items__,
            __patterns__.dropRight(1) ++ (last :: pattern.__patterns__),
            fields
        )

    /**
     * Connects to a vertex with a right-arrow symbol, completing
     * the edge-to-vertex connection. Maps to `->` in `GRAPH_TABLE`.
     *
     * {{{
     * ... |->| ("b" is g.person)
     * }}}
     */
    inline def |->|[PN, PV, POKS <: Tuple](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphVertex[PV, L]], POKS, L])(using
        gc: GraphContext,
        c: CombineKindTuple[OKS, POKS]
    ): GraphPattern[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphVertex[PV, L]]], c.R, L] =
        val rightSymbol = SqlGraphSymbol.RightArrow
        val last = __patterns__.last match
            case e: IrGraphPatternTerm.Edge =>
                e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol)
            case IrGraphPatternTerm.Quantified(e: IrGraphPatternTerm.Edge, q) =>
                IrGraphPatternTerm.Quantified(e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol), q)
            case p => p
        val fields = constValueTuple[Tuple.Append[N, PN]].toArray.toList.map(_.toString)
        GraphPattern(
            __items__ ++ pattern.__items__,
            __patterns__.dropRight(1) ++ (last :: pattern.__patterns__),
            fields
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
    inline def |~>|[PN, PV, POKS <: Tuple](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphVertex[PV, L]], POKS, L])(using
        gc: GraphContext,
        c: CombineKindTuple[OKS, POKS]
    ): GraphPattern[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphVertex[PV, L]]], c.R, L] =
        val rightSymbol = SqlGraphSymbol.RightTildeArrow
        val last = __patterns__.last match
            case e: IrGraphPatternTerm.Edge =>
                e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol)
            case IrGraphPatternTerm.Quantified(e: IrGraphPatternTerm.Edge, q) =>
                IrGraphPatternTerm.Quantified(e.copy(leftSymbol = __leftSymbol__, rightSymbol = rightSymbol), q)
            case p => p
        val fields = constValueTuple[Tuple.Append[N, PN]].toArray.toList.map(_.toString)
        GraphPattern(
            __items__ ++ pattern.__items__,
            __patterns__.dropRight(1) ++ (last :: pattern.__patterns__),
            fields
        )

/**
 * A graph pattern at a vertex, ready to connect to an edge.
 */
final case class GraphPattern[N <: Tuple, V <: Tuple, OKS <: Tuple, L <: Int](
    private[sqala] val __items__ : V,
    private[sqala] val __patterns__ : List[IrGraphPatternTerm],
    private[sqala] val __fields__ : List[String]
) extends Selectable:
    /**
     * The structural type declaring matched pattern aliases as a
     * named tuple. Required by `Selectable`.
     */
    type Fields = NamedTuple[N, V]

    /**
     * Runtime pattern alias accessor. Required by `Selectable`.
     */
    inline def selectDynamic(name: String): Any =
        val index = __fields__.indexOf(name)
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
    private[sqala] val __patterns__ : List[IrGraphPatternTerm],
    private[sqala] val __where__ : Option[IrExpr],
    private[sqala] val __rows__ : Option[SqlGraphRowsMode]
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
        val columnsValue = f(__pattern__).toTuple
        val columnItems = m.asSelectItems(columnsValue, 1)
        val table: IrTable.Graph = IrTable.Graph(
            false,
            __name__,
            None,
            NonEmptyList(
                IrGraphPattern(
                    None,
                    __patterns__.reduce((x, y) => IrGraphPatternTerm.And(x, y))
                ),
                Nil
            ),
            __where__,
            __rows__,
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
        val cond = a.asExpr(f(__pattern__)).expr
        GraphMatch(
            __name__,
            __pattern__,
            __patterns__,
            __where__.map(IrExpr.Binary(_, SqlBinaryOperator.And, cond)).orElse(Some(cond)),
            __rows__
        )

/**
 * A graph table source produced by `columns`, ready to be passed
 * to `from`.
 */
final case class FromGraph[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](
    private[sqala] val __table__ : IrTable.Graph,
    private[sqala] val __items__ : V
) extends AnyTable

object FromGraph:
    inline given asTable[N <: Tuple, V <: Tuple, TOKS <: Tuple, CL <: Int]: AsTable.Aux[FromGraph[N, V, TOKS, CL], CL, MappedTable[N, V, false, CL], TOKS] =
        new AsTable[FromGraph[N, V, TOKS, CL], CL]:
            type R = MappedTable[N, V, false, CL]

            type OKS = TOKS

            def asTable(x: FromGraph[N, V, TOKS, CL]): (R, IrTable) =
                (MappedTable(x.__table__, x.__items__), x.__table__)

    inline given asLateralTable[N <: Tuple, V <: Tuple, TOKS <: Tuple, CL <: Int](using
        e: ExcludeCurrentLevelColumn[TOKS, CL - 1]
    ): AsLateralTable.Aux[FromGraph[N, V, TOKS, CL], CL, MappedTable[N, V, false, CL - 1], e.R] =
        new AsLateralTable[FromGraph[N, V, TOKS, CL], CL]:
            type R = MappedTable[N, V, false, CL - 1]

            type OKS = e.R

            def asTable(x: FromGraph[N, V, TOKS, CL]): (R, IrTable) =
                val table: IrTable.Graph = x.__table__.copy(withLateral = true)
                (MappedTable(table, x.__items__), table)