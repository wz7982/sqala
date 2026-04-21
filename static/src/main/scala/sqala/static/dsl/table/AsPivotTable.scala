package sqala.static.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.AsMap

import scala.deriving.Mirror

trait AsPivotTable[T]:
    type R

    def table(x: T)(using QueryContext): R

object AsPivotTable:
    type Aux[T, O] = AsPivotTable[T]:
        type R = O

    given table[T](using
        p: Mirror.ProductOf[T],
        m: AsMap[p.MirroredElemTypes],
        ap: AsTableParam[m.R],
        tt: ToTuple[ap.R],
    ): Aux[Table[T, Column, CanNotInFrom], PivotTable[p.MirroredElemLabels, tt.R]] =
        new AsPivotTable[Table[T, Column, CanNotInFrom]]:
            type R = PivotTable[p.MirroredElemLabels, tt.R]

            def table(x: Table[T, Column, CanNotInFrom])(using c: QueryContext): R =
                val query = from(x.asInstanceOf[Table[T, Column, CanInFrom]])
                PivotTable(
                    Tuple.fromArray(
                        x.__metaData__.columnNames.map(n => Expr(SqlExpr.Column(x.__aliasName__, n))).toArray
                    ).asInstanceOf[tt.R],
                    query.tree
                )

    given subQuery[N <: Tuple, V <: Tuple](using
        s: AsMap[V],
        ap: AsTableParam[s.R],
        tt: ToTuple[ap.R],
    ): Aux[SubQueryTable[N, V, CanNotInFrom], PivotTable[N, tt.R]] =
        new AsPivotTable[SubQueryTable[N, V, CanNotInFrom]]:
            type R = PivotTable[N, tt.R]

            def table(x: SubQueryTable[N, V, CanNotInFrom])(using c: QueryContext): R =
                val query = from(x.asInstanceOf[SubQueryTable[N, V, CanInFrom]])
                PivotTable(tt.toTuple(ap.asTableParam(x.__aliasName__, 1)), query.tree)