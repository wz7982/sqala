package sqala.static.dsl.table

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.statement.SqlSelectItem
import sqala.ast.table.*
import sqala.static.dsl.statement.query.AsMap
import sqala.static.dsl.*
import sqala.static.metadata.{SqlBoolean, SqlNumber, TableMetaData}

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}
import scala.compiletime.constValue

class GraphContext

case class Graph[N <: Tuple, V <: Tuple](
    private[sqala] val __name__ : String,
    private[sqala] val __items__ : V
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String)(using c: QueryContext): Any =
        val alias = c.fetchAlias
        val index = constValue[Index[N, name.type, 0]]
        val node = __items__.toList(index)
        node match
            case GraphVertex(_, metaData) => 
                GraphVertex(Some(alias), metaData)
            case GraphEdge(_, metaData, where, quantifier) => 
                GraphEdge(Some(alias), metaData, where, quantifier)

    def `match`[PN <: Tuple, PV <: Tuple](pattern: GraphPattern[PN, PV])(using 
        GraphContext
    ): GraphMatch[GraphPattern[PN, PV]] =
        GraphMatch(__name__, pattern, pattern.__patterns__, None, None)

case class GraphVertex[T](
    private[sqala] val __alias__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    def selectDynamic(name: String): Expr[?] =
        val index = __metaData__.fieldNames.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__alias__, __metaData__.columnNames(index)))

case class GraphEdge[T](
    private[sqala] val __alias__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData,
    private[sqala] val __where__ : Option[SqlExpr],
    private[sqala] val __quantifier__ : Option[SqlGraphQuantifier]
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    def selectDynamic(name: String): Expr[?] =
        val index = __metaData__.fieldNames.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__alias__, __metaData__.columnNames(index)))

    def filter[A: AsExpr as a](f: Table[T] => A)(using
        SqlBoolean[a.R],
        QueryContext,
        GraphContext
    ): GraphEdge[T] =
        val table = Table[T](
            __alias__,
            __metaData__,
            SqlTable.Standard(__alias__.get, None, None, None, None)
        )
        val cond = a.asExpr(f(table)).asSqlExpr
        copy(__where__ = __where__.map(SqlExpr.Binary(_, SqlBinaryOperator.And, cond)).orElse(Some(cond)))

    def where[A: AsExpr as a](f: Table[T] => A)(using
        SqlBoolean[a.R],
        QueryContext,
        GraphContext
    ): GraphEdge[T] =
        filter(f)

    def *(using QueryContext, GraphContext): GraphEdge[T] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Asterisk
            )
        )

    def +(using QueryContext, GraphContext): GraphEdge[T] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Plus
            )
        )

    def ?(using QueryContext, GraphContext): GraphEdge[T] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Question
            )
        )

    def between[S: AsExpr as s, E: AsExpr as e](start: S, end: E)(using
        SqlNumber[s.R],
        SqlNumber[e.R],
        QueryContext,
        GraphContext
    ): GraphEdge[T] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Between(
                    Some(s.asExpr(start).asSqlExpr),
                    Some(e.asExpr(end).asSqlExpr)
                )
            )
        )

    def least[T: AsExpr as a](x: T)(using
        SqlNumber[a.R],
        QueryContext, 
        MatchRecognizeContext
    ): GraphEdge[T] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Between(
                    Some(a.asExpr(x).asSqlExpr),
                    None
                )
            )
        )

    def most[T: AsExpr as a](x: T)(using
        SqlNumber[a.R],
        QueryContext, 
        MatchRecognizeContext
    ): GraphEdge[T] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Between(
                    None,
                    Some(a.asExpr(x).asSqlExpr)
                )
            )
        )

    def at[T: AsExpr as a](x: T)(using
        SqlNumber[a.R],
        QueryContext, 
        MatchRecognizeContext
    ): GraphEdge[T] =
        copy(
            __quantifier__ = Some(
                SqlGraphQuantifier.Quantity(
                    a.asExpr(x).asSqlExpr
                )
            )
        )

case class GraphPatternPart[N <: Tuple, V <: Tuple](
    private[sqala] val __items__ : V,
    private[sqala] val __patterns__ : List[SqlGraphPatternTerm],
    private[sqala] val __leftSymbol__ : SqlGraphSymbol
):
    def |-|[PN, PV](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphVertex[PV]]])(using
        QueryContext, 
        GraphContext
    ): GraphPattern[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphVertex[PV]]]] =
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

    def |~|[PN, PV](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphVertex[PV]]])(using
        QueryContext, 
        GraphContext
    ): GraphPattern[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphVertex[PV]]]] =
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

    def |->|[PN, PV](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphVertex[PV]]])(using
        QueryContext, 
        GraphContext
    ): GraphPattern[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphVertex[PV]]]] =
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

    def |~>|[PN, PV](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphVertex[PV]]])(using
        QueryContext, 
        GraphContext
    ): GraphPattern[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphVertex[PV]]]] =
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

case class GraphPattern[N <: Tuple, V <: Tuple](
    private[sqala] val __items__ : V,
    private[sqala] val __patterns__ : List[SqlGraphPatternTerm]
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String)(using c: QueryContext): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)
    
    def |-|[PN, PV](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphEdge[PV]]])(using
        QueryContext, 
        GraphContext
    ): GraphPatternPart[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphEdge[PV]]]] =
        val leftSymbol = SqlGraphSymbol.Dash
        GraphPatternPart(
            __items__ ++ pattern.__items__,
            __patterns__ ++ pattern.__patterns__,
            leftSymbol
        )

    def |~|[PN, PV](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphEdge[PV]]])(using
        QueryContext, 
        GraphContext
    ): GraphPatternPart[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphEdge[PV]]]] =
        val leftSymbol = SqlGraphSymbol.Tilde
        GraphPatternPart(
            __items__ ++ pattern.__items__,
            __patterns__ ++ pattern.__patterns__,
            leftSymbol
        )

    def |<-|[PN, PV](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphEdge[PV]]])(using
        QueryContext, 
        GraphContext
    ): GraphPatternPart[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphEdge[PV]]]] =
        val leftSymbol = SqlGraphSymbol.LeftArrow
        GraphPatternPart(
            __items__ ++ pattern.__items__,
            __patterns__ ++ pattern.__patterns__,
            leftSymbol
        )

    def |<~|[PN, PV](pattern: GraphPattern[Tuple1[PN], Tuple1[GraphEdge[PV]]])(using
        QueryContext, 
        GraphContext
    ): GraphPatternPart[Tuple.Append[N, PN], Tuple.Concat[V, Tuple1[GraphEdge[PV]]]] =
        val leftSymbol = SqlGraphSymbol.LeftTildeArrow
        GraphPatternPart(
            __items__ ++ pattern.__items__,
            __patterns__ ++ pattern.__patterns__,
            leftSymbol
        )

case class GraphMatch[T](
    private[sqala] val __name__ : String,
    private[sqala] val __pattern__ : T,
    private[sqala] val __patterns__ : List[SqlGraphPatternTerm],
    private[sqala] val __where__ : Option[SqlExpr],
    private[sqala] val __rows__ : Option[SqlGraphRowsMode]
):
    def columns[MN <: Tuple, MV <: Tuple](f: T => NamedTuple[MN, MV])(using
        m: AsMap[MV],
        t: ToTuple[m.R],
        p: AsTableParam[m.R],
        c: QueryContext,
        gc: GraphContext
    ): GraphTable[MN, t.R] =
        val alias = c.fetchAlias
        val columnsValue = f(__pattern__).toTuple
        val columnItems = m.selectItems(columnsValue, 1)
        GraphTable(
            __name__,
            SqlGraphPattern(
                None, 
                __patterns__.reduce((x, y) => SqlGraphPatternTerm.And(x, y))
            ) :: Nil,
            __where__,
            __rows__,
            t.toTuple(p.asTableParam(Some(alias), 1)),
            columnItems,
            false,
            Some(alias)
        )

    def filter[A: AsExpr as a](f: T => A)(using
        SqlBoolean[a.R],
        QueryContext,
        GraphContext
    ): GraphMatch[T] =
        val cond = a.asExpr(f(__pattern__)).asSqlExpr
        copy(__where__ = __where__.map(SqlExpr.Binary(_, SqlBinaryOperator.And, cond)).orElse(Some(cond)))

    def where[A: AsExpr as a](f: T => A)(using
        SqlBoolean[a.R],
        QueryContext,
        GraphContext
    ): GraphMatch[T] =
        filter(f)

case class GraphTable[N <: Tuple, V <: Tuple](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Graph
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

object GraphTable:
    def apply[N <: Tuple, V <: Tuple](
        name: String,
        patterns: List[SqlGraphPattern],
        where: Option[SqlExpr],
        rows: Option[SqlGraphRowsMode],
        items: V,
        columns: List[SqlSelectItem],
        lateral: Boolean,
        alias: Option[String]
    ): GraphTable[N, V] =
        new GraphTable(
            alias,
            items,
            SqlTable.Graph(
                name,
                None,
                patterns,
                where,
                rows,
                columns,
                None,
                lateral,
                alias.map(SqlTableAlias(_, Nil)),
                None
            )
        )

case class UngroupedGraphTable[N <: Tuple, V <: Tuple](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Graph
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)