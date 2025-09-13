package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.group.{SqlGroupBy, SqlGroupingItem}
import sqala.ast.statement.SqlQuery
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.AsMap

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValue
import sqala.ast.expr.SqlBinaryOperator
import sqala.ast.statement.SqlSelectItem

class PivotContext

case class PivotWithin[N](expr: SqlExpr, conditions: List[SqlExpr])

case class PivotTable[N <: Tuple, V <: Tuple](
    private[sqala] val __items__ : V,
    private[sqala] val __sqlQuery__ : SqlQuery.Select
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

    def groupBy[GN <: Tuple, GV <: Tuple](grouping: NamedTuple[GN, GV])(using
        g: AsGroup[GV],
        tt: ToTuple[g.R],
        c: QueryContext, 
        pc: PivotContext
    ): PivotGroupBy[N, V, GN, tt.R] =
        val group = g.exprs(grouping.toTuple).map(_.asSqlExpr)
        val newQuery = 
            __sqlQuery__.copy(
                groupBy = Some(
                    SqlGroupBy(group.map(g => SqlGroupingItem.Expr(g)), None)
                )
            )
        PivotGroupBy[N, V, GN, tt.R](__items__, group, newQuery)

case class PivotGroupBy[N <: Tuple, V <: Tuple, GN <: Tuple, GV <: Tuple](
    private[sqala] val __items__ : V,
    private[sqala] val __group__ : List[SqlExpr],
    private[sqala] val __sqlQuery__ : SqlQuery.Select
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

    def agg[AN <: Tuple, AV <: Tuple](aggregations: NamedTuple[AN, AV])(using
        m: AsMap[AV],
        tt: ToTuple[m.R],
        c: QueryContext,
        pc: PivotContext
    ): PivotAgg[N, V, GN, GV, AN, tt.R] =
        val aggItems = m.selectItems(aggregations.toTuple, 1).map(_.expr)
        PivotAgg[N, V, GN, GV, AN, tt.R](__items__, __group__, aggItems, __sqlQuery__)

case class PivotAgg[N <: Tuple, V <: Tuple, GN <: Tuple, GV <: Tuple, AN <: Tuple, AV <: Tuple](
    private[sqala] val __items__ : V,
    private[sqala] val __group__ : List[SqlExpr],
    private[sqala] val __aggregations__ : List[SqlExpr],
    private[sqala] val __sqlQuery__ : SqlQuery.Select
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

    private def addFilter(expr: SqlExpr, filter: SqlExpr): SqlExpr =
        expr match
            case f: SqlExpr.StandardFunc => f.copy(filter = Some(filter))
            case f: SqlExpr.CountAsteriskFunc => f.copy(filter = Some(filter))
            case f: SqlExpr.ListAggFunc => f.copy(filter = Some(filter))
            case f: SqlExpr.JsonObjectAggFunc => f.copy(filter = Some(filter))
            case f: SqlExpr.JsonArrayAggFunc => f.copy(filter = Some(filter))
            case _ => throw MatchError("AGG clause only supports aggregate function expressions.")

    def `for`[WS <: Tuple](items: WS)(using
        f: PivotFor[WS, GN, GV, AN, AV],
        m: AsMap[f.V],
        tt: ToTuple[m.R],
        a: AsTableParam[tt.R],
        c: QueryContext,
        pc: PivotContext
    ): SubQueryTable[f.N, tt.R] =
        def combineAll(withinList: List[List[SqlExpr]]): List[SqlExpr] = withinList match
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
        c.tableIndex += 1
        val alias = s"t${c.tableIndex}"
        SubQueryTable(newQuery, false, Some(alias))

    def `for`[WN <: Tuple](item: PivotWithin[WN])(using c: QueryContext, pc: PivotContext): SubQueryTable[
        Tuple.Concat[GN, CombinePivotNames[AN, WN *: EmptyTuple]],
        Tuple.Concat[GV, CombinePivotTypes[AV, WN *: EmptyTuple]]
    ] =
        new SubQueryTable(???, ???, ???)

trait PivotFor[T, GN <: Tuple, GV <: Tuple, AN <: Tuple, AV <: Tuple]:
    type N <: Tuple

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