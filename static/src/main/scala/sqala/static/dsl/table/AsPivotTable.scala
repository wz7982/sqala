package sqala.static.dsl.table

import sqala.static.dsl.statement.query.AsMap
import sqala.static.dsl.{QueryContext, ToTuple, from}

import scala.deriving.Mirror
import sqala.ast.expr.SqlExpr
import sqala.static.dsl.Expr

trait AsPivotTable[T]:
    type R

    def table(x: T)(using QueryContext): R

object AsPivotTable:
    type Aux[T, O] = AsPivotTable[T]:
        type R = O

    given table[T](using
        p: Mirror.ProductOf[T],
        m: AsMap[p.MirroredElemTypes],
        tt: ToTuple[m.R],
        ap: AsTableParam[tt.R]
    ): Aux[Table[T], PivotTable[p.MirroredElemLabels, tt.R]] =
        new AsPivotTable[Table[T]]:
            type R = PivotTable[p.MirroredElemLabels, tt.R]

            def table(x: Table[T])(using c: QueryContext): R =
                val query = from(x)
                PivotTable(
                    Tuple.fromArray(
                        x.__metaData__.columnNames.map(n => Expr(SqlExpr.Column(x.__aliasName__, n))).toArray
                    ).asInstanceOf[tt.R], 
                    query.tree
                )

    given subQuery[N <: Tuple, V <: Tuple](using
        s: AsMap[V],
        tt: ToTuple[s.R],
        ap: AsTableParam[tt.R]
    ): Aux[SubQueryTable[N, V], PivotTable[N, tt.R]] =
        new AsPivotTable[SubQueryTable[N, V]]:
            type R = PivotTable[N, tt.R]

            def table(x: SubQueryTable[N, V])(using c: QueryContext): R =
                val query = from(x)
                PivotTable(ap.asTableParam(x.__aliasName__, 1), query.tree)