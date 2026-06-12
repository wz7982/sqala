package sqala.static.dsl.table

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.group.{SqlGroup, SqlGroupingItem}
import sqala.ast.statement.{SqlQuery, SqlSelectItem}
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.{AsMap, Query}

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValue

/**
 * A within-item for `pivot`, specifying a column and its matching
 * conditions.
 */
final case class PivotWithin[N](private[sqala] val expr: SqlExpr, private[sqala] val conditions: List[SqlExpr])

/**
 * A pivot table source constructed by `pivot`, supporting optional
 * `groupBy` and `agg` before finalizing.
 */
final case class Pivot[N <: Tuple, V <: Tuple, OKS <: Tuple, L <: Int](
    private[sqala] val __items__ : V,
    private[sqala] val __sqlQuery__ : SqlQuery.Select
)(using private[sqala] val qc: QueryContext[L]) extends Selectable:
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

    /**
     * Adds a `groupBy` clause to the pivot.
     *
     * {{{
     * City.pivot(c => c
     *     .groupBy((country = c.country))
     *     .agg((sum = sum(c.population)))
     *     .by(c.year.within(`2024` = 2024))
     * )
     * }}}
     */
    def groupBy[GN <: Tuple, GV <: Tuple](grouping: NamedTuple[GN, GV])(using
        pc: PivotContext,
        g: AsGroup[GV, L]
    ): PivotGroupBy[N, V, GN, GV, OKS, L] =
        val group = g.asExprs(grouping.toTuple).map(_.asSqlExpr)
        val newQuery =
            __sqlQuery__.copy(
                groupBy = Some(
                    SqlGroup(None, group.map(g => SqlGroupingItem.Expr(g)))
                )
            )
        PivotGroupBy[N, V, GN, GV, OKS, L](__items__, group, newQuery)

    /**
     * Adds aggregations to the pivot.
     *
     * {{{
     * City.pivot(c => c
     *     .agg(sum = sum(c.population), count = count())
     *     .by(c.year.within(`2024` = 2024))
     * )
     * }}}
     */
    def agg[AN <: Tuple, AV <: Tuple](aggregations: NamedTuple[AN, AV])(using
        pc: PivotContext,
        m: AsMap[AV, L],
        tt: ToTuple[m.R],
        i: AllIsKind[m.KS, Agg[?]],
        e: ExcludeCurrentLevelColumn[m.KS, L],
        refl: e.R =:= EmptyTuple
    ): PivotAgg[N, V, EmptyTuple, EmptyTuple, AN, tt.R, OKS, L] =
        val aggItems = m.asSelectItems(aggregations.toTuple, 1).map(_.expr)
        PivotAgg[N, V, EmptyTuple, EmptyTuple, AN, tt.R, OKS, L](__items__, Nil, aggItems, __sqlQuery__)

/**
 * A pivot table after `groupBy` has been applied.
 */
final case class PivotGroupBy[N <: Tuple, V <: Tuple, GN <: Tuple, GV <: Tuple, OKS <: Tuple, L <: Int](
    private[sqala] val __items__ : V,
    private[sqala] val __group__ : List[SqlExpr],
    private[sqala] val __sqlQuery__ : SqlQuery.Select
)(using private[sqala] val qc: QueryContext[L]) extends Selectable:
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

    /**
     * Adds aggregations to the pivot.
     *
     * {{{
     * City.pivot(c => c
     *     .groupBy((country = c.country))
     *     .agg(sum = sum(c.population), count = count())
     *     .by(c.year.within(`2024` = 2024))
     * )
     * }}}
     */
    def agg[AN <: Tuple, AV <: Tuple](aggregations: NamedTuple[AN, AV])(using
        pc: PivotContext,
        m: AsMap[AV, L],
        tt: ToTuple[m.R],
        i: AllIsKind[m.KS, Agg[?]],
        e: ExcludeCurrentLevelColumn[m.KS, L],
        refl: e.R =:= EmptyTuple
    ): PivotAgg[N, V, GN, GV, AN, tt.R, OKS, L] =
        val aggItems = m.asSelectItems(aggregations.toTuple, 1).map(_.expr)
        PivotAgg[N, V, GN, GV, AN, tt.R, OKS, L](__items__, __group__, aggItems, __sqlQuery__)

/**
 * A pivot table after aggregations have been defined, ready for
 * `by` to specify the within-conditions.
 */
final case class PivotAgg[N <: Tuple, V <: Tuple, GN <: Tuple, GV <: Tuple, AN <: Tuple, AV <: Tuple, OKS <: Tuple, L <: Int](
    private[sqala] val __items__ : V,
    private[sqala] val __group__ : List[SqlExpr],
    private[sqala] val __aggregations__ : List[SqlExpr],
    private[sqala] val __sqlQuery__ : SqlQuery.Select
)(using private[sqala] val qc: QueryContext[L]) extends Selectable:
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

    /**
     * Attaches a `FILTER` clause to an aggregation expression.
     */
    private def addFilter(expr: SqlExpr, filter: SqlExpr): SqlExpr =
        expr match
            case f: SqlExpr.GeneralFunc => f.copy(filter = Some(filter))
            case f: SqlExpr.CountAsteriskFunc => f.copy(filter = Some(filter))
            case f: SqlExpr.ListAggFunc => f.copy(filter = Some(filter))
            case f: SqlExpr.JsonObjectAggFunc => f.copy(filter = Some(filter))
            case f: SqlExpr.JsonArrayAggFunc => f.copy(filter = Some(filter))
            case _ => throw MatchError(expr)

    /**
     * Specifies the within-conditions for multiple pivot columns.
     * Column names are derived from the Cartesian product of the
     * `groupBy`, `agg`, and `by` specifications, using camelCase
     * formatting.
     *
     * {{{
     * City.pivot(c => c
     *     .agg((sum = sum(c.population)))
     *     .by(
     *         c.country.within(cn = "CN", us = "US"),
     *         c.year.within(`2024` = 2024)
     *     )
     * )
     * }}}
     */
    def by[WS <: Tuple](items: WS)(using
        pc: PivotContext,
        f: PivotFor[WS, GN, GV, AN, AV],
        m: AsMap[f.V, L],
        t: ToTuple[m.R],
        a: AsTableParam[t.R, L],
        tt: ToTuple[a.R]
    ): FromPivot[f.N, tt.R, OKS, L] =
        def combineAll(withinList: List[List[SqlExpr]]): List[SqlExpr] =
            withinList match
                case Nil => Nil
                case head :: Nil => head
                case head :: tail =>
                    for
                        x <- head
                        y <- combineAll(tail)
                    yield
                        SqlExpr.Binary(x, SqlBinaryOperator.And, y)

        val withinList = items.toList.map(_.asInstanceOf[PivotWithin[?]])
        val conditions = combineAll:
            withinList.map: i =>
                i.conditions.map(c => SqlExpr.Binary(i.expr, SqlBinaryOperator.Equal, c))
        val selectAggregations =
            for
                agg <- __aggregations__
                c <- conditions
            yield
                addFilter(agg, c)
        val selectItems =
            (__group__ ++ selectAggregations).zipWithIndex.map: (e, i) =>
                SqlSelectItem.Expr(
                    e, Some(s"c${i + 1}")
                )
        val newQuery = __sqlQuery__.copy(select = selectItems)
        val alias = qc.fetchAlias
        FromPivot(newQuery, false, Some(alias))

    /**
     * Specifies the within-condition for a single pivot column.
     * Column names are derived from the Cartesian product of the
     * `groupBy`, `agg`, and `by` specifications, using camelCase
     * formatting.
     *
     * {{{
     * City.pivot(c => c
     *     .agg((sum = sum(c.population)))
     *     .by(c.year.within(`2024` = 2024))
     * )
     * }}}
     */
    def by[WN <: Tuple](item: PivotWithin[WN])(using
        pc: PivotContext,
        f: PivotFor[PivotWithin[WN], GN, GV, AN, AV],
        m: AsMap[f.V, L],
        t: ToTuple[m.R],
        a: AsTableParam[t.R, L],
        tt: ToTuple[a.R]
    ): FromPivot[f.N, tt.R, OKS, L] =
        val conditions = item.conditions.map(c => SqlExpr.Binary(item.expr, SqlBinaryOperator.Equal, c))
        val selectAggregations =
            for
                agg <- __aggregations__
                c <- conditions
            yield
                addFilter(agg, c)
        val selectItems =
            (__group__ ++ selectAggregations).zipWithIndex.map: (e, i) =>
                SqlSelectItem.Expr(
                    e, Some(s"c${i + 1}")
                )
        val newQuery = __sqlQuery__.copy(select = selectItems)
        val alias = qc.fetchAlias
        FromPivot(newQuery, false, Some(alias))

/**
 * Computes the result column names and types of a pivot.
 * `GN`/`GV` are the group-by name and type tuples,
 * `AN`/`AV` are the aggregation name and type tuples.
 * `N` and `V` are the combined result name and type tuples.
 */
trait PivotFor[T, GN <: Tuple, GV <: Tuple, AN <: Tuple, AV <: Tuple]:
    /**
     * The result column name tuple.
     */
    type N <: Tuple

    /**
     * The result column type tuple.
     */
    type V <: Tuple

object PivotFor:
    type Aux[T, GN <: Tuple, GV <: Tuple, AN <: Tuple, AV <: Tuple, PN <: Tuple, PV <: Tuple] =
        PivotFor[T, GN, GV, AN, AV]:
            type N = PN

            type V = PV

    given multiple[WS <: Tuple, GN <: Tuple, GV <: Tuple, AN <: Tuple, AV <: Tuple]: Aux[
        WS,
        GN,
        GV,
        AN,
        AV,
        Tuple.Concat[GN, CombinePivotNames[AN, TupleMap[WS, [x] =>> Unwrap[x, PivotWithin]]]],
        Tuple.Concat[GV, CombinePivotTypes[AV, TupleMap[WS, [x] =>> Unwrap[x, PivotWithin]]]]
    ] =
        new PivotFor[WS, GN, GV, AN, AV]:
            type N = Tuple.Concat[GN, CombinePivotNames[AN, TupleMap[WS, [x] =>> Unwrap[x, PivotWithin]]]]

            type V = Tuple.Concat[GV, CombinePivotTypes[AV, TupleMap[WS, [x] =>> Unwrap[x, PivotWithin]]]]

    given single[WN <: Tuple, GN <: Tuple, GV <: Tuple, AN <: Tuple, AV <: Tuple]: Aux[
        PivotWithin[WN],
        GN,
        GV,
        AN,
        AV,
        Tuple.Concat[GN, CombinePivotNames[AN, WN *: EmptyTuple]],
        Tuple.Concat[GV, CombinePivotTypes[AV, WN *: EmptyTuple]]
    ] =
        new PivotFor[PivotWithin[WN], GN, GV, AN, AV]:
            type N = Tuple.Concat[GN, CombinePivotNames[AN, WN *: EmptyTuple]]

            type V = Tuple.Concat[GV, CombinePivotTypes[AV, WN *: EmptyTuple]]

/**
 * A pivot table source, produced by `by` as the final step before
 * `from`.
 */
final case class FromPivot[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Subquery
) extends AnyTable

object FromPivot:
    def apply[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](query: SqlQuery, lateral: Boolean, alias: Option[String])(using
        p: AsTableParam[V, CL],
        t: ToTuple[p.R]
    ): FromPivot[N, t.R, OKS, CL] =
        new FromPivot(
            alias,
            t.toTuple(p.asTableParam(alias, 1)),
            SqlTable.Subquery(
                lateral,
                query,
                alias.map(SqlTableAlias(_, Nil)),
                None
            )
        )

    def apply[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](query: Query[?, ?, ?, ?], lateral: Boolean, alias: Option[String])(using
        p: AsTableParam[V, CL],
        t: ToTuple[p.R]
    ): FromPivot[N, t.R, OKS, CL] =
        apply(query.tree, lateral, alias)