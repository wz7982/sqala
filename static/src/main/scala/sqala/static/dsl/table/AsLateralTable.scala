package sqala.static.dsl.table

import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.static.dsl.statement.query.Query
import sqala.static.dsl.*

import scala.NamedTuple.NamedTuple
import scala.compiletime.ops.int.{-, >}
import sqala.ir.IrQuery
import sqala.ir.IrTable

/**
 * Lifts various DSL types into a lateral table reference, setting
 * `lateral = true`. `CL` is the context level after `joinLateral`
 * incremented it by one; the level is restored here to verify that
 * the subquery correctly references columns from the left table.
 */
trait AsLateralTable[T, CL <: Int]:
    /**
     * The table reference type.
     */
    type R

    /**
     * The kind tuple of the outer query.
     */
    type OKS <: Tuple

    /**
     * Converts the value to a lateral table reference and its SQL
     * table AST.
     */
    def asTable(x: T)(using QueryContext[CL]): (R, IrTable)

object AsLateralTable:
    type Aux[T, CL <: Int, O, OOKS <: Tuple] = AsLateralTable[T, CL]:
        type R = O

        type OKS = OOKS

    inline given subquery[N <: Tuple, V <: Tuple, QOKS <: Tuple, S <: QuerySize, L <: Int, Q <: Query[NamedTuple[N, V], QOKS, L, S], CL <: Int](using
        p: AsTableParam[V, CL - 1],
        tt: ToTuple[p.R],
        e: ExcludeCurrentLevelColumn[QOKS, CL - 1],
        i: IncludeCurrentLevelColumn[QOKS, CL - 1],
        c: CanInSimpleClause[i.R],
        vss: ValidateSubqueryScope[L, CL]
    ): Aux[Q, CL, MappedTable[N, tt.R, tt.R, false, CL - 1], e.R] =
        new AsLateralTable[Q, CL]:
            type R = MappedTable[N, tt.R, tt.R, false, CL - 1]

            type OKS = e.R

            def asTable(x: Q)(using qc: QueryContext[CL]): (R, IrTable) =
                val irTable =
                    IrTable.Subquery(
                        true,
                        x.tree,
                        Nil
                    )
                val table = MappedTable[N, V, tt.R, false, CL - 1](irTable)
                (table, irTable)