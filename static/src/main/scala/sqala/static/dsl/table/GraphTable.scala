package sqala.static.dsl.table

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.statement.SqlSelectItem
import sqala.ast.table.*
import sqala.metadata.{SqlBoolean, SqlNumber, TableMetaData}
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.AsMap

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}
import scala.compiletime.constValue

final case class GraphSchema[N <: Tuple, V <: Tuple](
    private[sqala] val __name__ : String,
    private[sqala] val __items__ : V
)

final case class GraphVertexSchema[T](
    private[sqala] val __alias__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData
)

final case class GraphEdgeSchema[T](
    private[sqala] val __alias__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData,
    private[sqala] val __where__ : Option[SqlExpr],
    private[sqala] val __quantifier__ : Option[SqlGraphQuantifier]
)

trait TransformGraphSchema[T, L <: Int]:
    type R

    def transform(x: T)(using qc: QueryContext[L]): R

object TransformGraphSchema:
    type Aux[T, L <: Int, O] = TransformGraphSchema[T, L]:
        type R = O

    given vertex[T, L <: Int]: Aux[GraphVertexSchema[T], L, GraphVertex[T, L]] = new
        TransformGraphSchema[GraphVertexSchema[T], L]:
            type R = GraphVertex[T, L]

            def transform(x: GraphVertexSchema[T])(using qc: QueryContext[L]): R =
                GraphVertex(x.__alias__, x.__metaData__)

    given edge[T, L <: Int]: Aux[GraphEdgeSchema[T], L, GraphEdge[T, EmptyTuple, L]] = new
        TransformGraphSchema[GraphEdgeSchema[T], L]:
            type R = GraphEdge[T, EmptyTuple, L]

            def transform(x: GraphEdgeSchema[T])(using qc: QueryContext[L]): R =
                GraphEdge(x.__alias__, x.__metaData__, x.__where__, x.__quantifier__)

    given tuple[H, T <: Tuple, L <: Int](using
        sh: TransformGraphSchema[H, L],
        st: TransformGraphSchema[T, L],
        tt: ToTuple[st.R]
    ): Aux[H *: T, L, sh.R *: tt.R] =
        new TransformGraphSchema[H *: T, L]:
            type R = sh.R *: tt.R

            def transform(x: H *: T)(using qc: QueryContext[L]): R =
                sh.transform(x.head) *: tt.toTuple(st.transform(x.tail))

    given emptyTuple[L <: Int]: Aux[EmptyTuple, L, EmptyTuple] = new
        TransformGraphSchema[EmptyTuple, L]:
            type R = EmptyTuple

            def transform(x: EmptyTuple)(using qc: QueryContext[L]): R =
                EmptyTuple

final case class Graph[N <: Tuple, V <: Tuple, L <: Int](
    private[sqala] val __name__ : String,
    private[sqala] val __items__ : V
)(using private[sqala] val qc: QueryContext[L]) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val alias = qc.fetchAlias
        val index = constValue[Index[N, name.type, 0]]
        val node = __items__.toList(index)
        node match
            case GraphVertex(_, metaData) =>
                GraphVertex(Some(alias), metaData)
            case GraphEdge(_, metaData, where, quantifier) =>
                GraphEdge(Some(alias), metaData, where, quantifier)

    def `match`[PN <: Tuple, PV <: Tuple, POKS <: Tuple](pattern: GraphPattern[PN, PV, POKS, L])(using
        GraphContext
    ): GraphMatch[GraphPattern[PN, PV, POKS, L], POKS, L] =
        GraphMatch(__name__, pattern, pattern.__patterns__, None, None)

sealed trait GraphPatternTerm[V, OKS <: Tuple, L <: Int]:
    private[sqala] def asTerm: SqlGraphPatternTerm

final case class GraphVertex[T, L <: Int](
    private[sqala] val __alias__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData
)(using private[sqala] val qc: QueryContext[L]) extends Selectable with GraphPatternTerm[T, EmptyTuple, L]:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T, Column, L]]
        ]

    override private[sqala] def asTerm: SqlGraphPatternTerm =
        SqlGraphPatternTerm.Vertex(
            __alias__,
            Some(SqlGraphLabel.Label(__metaData__.tableName)),
            None
        )

    def selectDynamic(name: String): Any =
        val index = __metaData__.fieldNames.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__alias__, __metaData__.columnNames(index)))

final case class GraphEdge[T, OKS <: Tuple, L <: Int](
    private[sqala] val __alias__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData,
    private[sqala] val __where__ : Option[SqlExpr],
    private[sqala] val __quantifier__ : Option[SqlGraphQuantifier]
)(using private[sqala] val qc: QueryContext[L]) extends Selectable with GraphPatternTerm[T, OKS, L]:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T, Column, L]]
        ]

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

    def selectDynamic(name: String): Any =
        val index = __metaData__.fieldNames.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__alias__, __metaData__.columnNames(index)))

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

    def *(using GraphContext): GraphEdge[T, OKS, L] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Asterisk
            )
        )

    def +(using GraphContext): GraphEdge[T, OKS, L] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Plus
            )
        )

    def ?(using GraphContext): GraphEdge[T, OKS, L] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Question
            )
        )

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

final case class GraphPatternPart[N <: Tuple, V <: Tuple, OKS <: Tuple, L <: Int](
    private[sqala] val __items__ : V,
    private[sqala] val __patterns__ : List[SqlGraphPatternTerm],
    private[sqala] val __leftSymbol__ : SqlGraphSymbol
)(using private[sqala] val qc: QueryContext[L]):
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

final case class GraphPattern[N <: Tuple, V <: Tuple, OKS <: Tuple, L <: Int](
    private[sqala] val __items__ : V,
    private[sqala] val __patterns__ : List[SqlGraphPatternTerm]
)(using private[sqala] val qc: QueryContext[L]) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

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

final case class GraphMatch[T, OKS <: Tuple, L <: Int](
    private[sqala] val __name__ : String,
    private[sqala] val __pattern__ : T,
    private[sqala] val __patterns__ : List[SqlGraphPatternTerm],
    private[sqala] val __where__ : Option[SqlExpr],
    private[sqala] val __rows__ : Option[SqlGraphRowsMode]
)(using private[sqala] val qc: QueryContext[L]):
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
                patterns,
                where,
                rows,
                columns,
                None,
                alias.map(SqlTableAlias(_, Nil)),
                None
            )
        )

final case class GraphTable[N <: Tuple, V <: Tuple, L <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Graph
) extends Selectable with AnyTable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)