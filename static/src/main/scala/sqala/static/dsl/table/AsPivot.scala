package sqala.static.dsl.table

import sqala.ast.statement.SqlQuery
import sqala.ast.expr.SqlExpr
import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.metadata.FetchCompanion
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.{AsMap, AsSelect, Query}

import scala.NamedTuple.NamedTuple
import scala.deriving.Mirror
import scala.util.NotGiven
import scala.compiletime.ops.int.>

trait AsPivot[T, CL <: Int]:
    type R

    type OKS <: Tuple

    def asPivot(x: T)(using QueryContext[CL]): R

object AsPivot:
    type Aux[T, CL <: Int, O, OOKS <: Tuple] = AsPivot[T, CL]:
        type R = O

        type OKS = OOKS

    given entity[O, CL <: Int](using
        na: NotGiven[O <:< AnyTable],
        nt: NotGiven[O <:< Tuple],
        nq: NotGiven[O <:< Query[?, ?, ?, ?]],
        ns: NotGiven[O <:< Seq[?]],
        fc: FetchCompanion[O],
        p: Mirror.ProductOf[fc.R],
        am: AsMap[p.MirroredElemTypes, CL],
        ap: AsTableParam[am.R, CL],
        as: AsSelect[Table[fc.R, Column, CL]],
        tt: ToTuple[ap.R]
    ): Aux[O, CL, Pivot[p.MirroredElemLabels, tt.R, EmptyTuple, CL], EmptyTuple] =
        new AsPivot[O, CL]:
            type R = Pivot[p.MirroredElemLabels, tt.R, EmptyTuple, CL]

            type OKS = EmptyTuple

            def asPivot(x: O)(using qc: QueryContext[CL]): R =
                val metaData = fc.metaData
                val alias = qc.fetchAlias
                val table = Table[fc.R, Column, CL](
                    Some(alias),
                    metaData,
                    SqlTable.Ident(
                        metaData.tableName,
                        Some(SqlTableAlias(alias, Nil)),
                        None,
                        None,
                        None
                    )
                )
                val selectItems = as.asSelectItems(table, 1)
                val tree: SqlQuery.Select = SqlQuery.Select(
                    None,
                    selectItems,
                    table.__sqlTable__ :: Nil,
                    None,
                    None,
                    None,
                    Nil,
                    None,
                    None
                )
                Pivot(
                    Tuple.fromArray(
                        metaData.columnNames.map(n => Expr(SqlExpr.Column(Some(alias), n))).toArray
                    ).asInstanceOf[tt.R],
                    tree
                )

    given subquery[N <: Tuple, V <: Tuple, TOKS <: Tuple, L <: Int, S <: QuerySize, Q <: Query[NamedTuple[N, V], TOKS, L, S], CL <: Int](using
        ap: AsTableParam[V, CL],
        tt: ToTuple[ap.R],
        as: AsSelect[SubqueryTable[N, tt.R, CL]],
        refl: L > CL =:= true
    ): Aux[Q, CL, Pivot[N, tt.R, TOKS, CL], TOKS] =
        new AsPivot[Q, CL]:
            type R = Pivot[N, tt.R, TOKS, CL]

            type OKS = TOKS

            def asPivot(x: Q)(using qc: QueryContext[CL]): R =
                val alias = qc.fetchAlias
                val subquery = SubqueryTable[N, V, CL](x, false, Some(alias))
                val selectItems = as.asSelectItems(subquery, 1)
                val tree: SqlQuery.Select = SqlQuery.Select(
                    None,
                    selectItems,
                    subquery.__sqlTable__ :: Nil,
                    None,
                    None,
                    None,
                    Nil,
                    None,
                    None
                )
                Pivot(subquery.__items__, tree)