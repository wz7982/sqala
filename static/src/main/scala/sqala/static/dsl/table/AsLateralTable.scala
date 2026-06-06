package sqala.static.dsl.table

import sqala.ast.table.SqlTable
import sqala.static.dsl.statement.query.Query
import sqala.static.dsl.*

import scala.NamedTuple.NamedTuple
import scala.compiletime.ops.int.{-, >}

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
    def asTable(x: T)(using QueryContext[CL]): (R, SqlTable)

object AsLateralTable:
    type Aux[T, CL <: Int, O, OOKS <: Tuple] = AsLateralTable[T, CL]:
        type R = O

        type OKS = OOKS

    given func[T, TOKS <: Tuple, CL <: Int](using
        e: ExcludeCurrentLevelColumn[TOKS, CL - 1]
    ): Aux[FromFunc[T, Column, TOKS, CL], CL, FuncTable[T, Column, CL - 1], e.R] =
        new AsLateralTable[FromFunc[T, Column, TOKS, CL], CL]:
            type R = FuncTable[T, Column, CL - 1]

            type OKS = e.R

            def asTable(x: FromFunc[T, Column, TOKS, CL])(using QueryContext[CL]): (R, SqlTable) =
                val sqlTable: SqlTable.Func = x.__sqlTable__.copy(lateral = true)
                val table =
                    FuncTable[T, Column, CL - 1](x.__aliasName__, x.__fieldNames__, x.__columnNames__, sqlTable)
                (table, sqlTable)

    given json[N <: Tuple, V <: Tuple, TOKS <: Tuple, CL <: Int](using
        a: AsTableParam[V, CL - 1],
        tt: ToTuple[a.R],
        e: ExcludeCurrentLevelColumn[TOKS, CL - 1]
    ): Aux[FromJson[N, V, TOKS, CL], CL, JsonTable[N, tt.R, CL - 1], e.R] =
        new AsLateralTable[FromJson[N, V, TOKS, CL], CL]:
            type R = JsonTable[N, tt.R, CL - 1]

            type OKS = e.R

            def asTable(x: FromJson[N, V, TOKS, CL])(using QueryContext[CL]): (R, SqlTable) =
                val sqlTable: SqlTable.Json = x.__sqlTable__.copy(lateral = true)
                val table =
                    JsonTable[N, tt.R, CL - 1](x.__aliasName__, tt.toTuple(a.asTableParam(x.__aliasName__, 1)), sqlTable)
                (table, sqlTable)

    given graph[N <: Tuple, V <: Tuple, TOKS <: Tuple, CL <: Int](using
        a: AsTableParam[V, CL - 1],
        tt: ToTuple[a.R],
        e: ExcludeCurrentLevelColumn[TOKS, CL - 1]
    ): Aux[FromGraph[N, V, TOKS, CL], CL, GraphTable[N, tt.R, CL - 1], e.R] =
        new AsLateralTable[FromGraph[N, V, TOKS, CL], CL]:
            type R = GraphTable[N, tt.R, CL - 1]

            type OKS = e.R

            def asTable(x: FromGraph[N, V, TOKS, CL])(using QueryContext[CL]): (R, SqlTable) =
                val sqlTable: SqlTable.Graph = x.__sqlTable__.copy(lateral = true)
                val table = GraphTable[N, tt.R, CL - 1](x.__aliasName__, tt.toTuple(a.asTableParam(x.__aliasName__, 1)), sqlTable)
                (table, sqlTable)

    given subquery[N <: Tuple, V <: Tuple, QOKS <: Tuple, S <: QuerySize, L <: Int, Q <: Query[NamedTuple[N, V], QOKS, L, S], CL <: Int](using
        p: AsTableParam[V, CL - 1],
        tt: ToTuple[p.R],
        e: ExcludeCurrentLevelColumn[QOKS, CL - 1],
        i: IncludeCurrentLevelColumn[QOKS, CL - 1],
        c: CanInSimpleClause[i.R],
        refl: L > CL =:= true
    ): Aux[Q, CL, SubqueryTable[N, tt.R, CL - 1], e.R] =
        new AsLateralTable[Q, CL]:
            type R = SubqueryTable[N, tt.R, CL - 1]

            type OKS = e.R

            def asTable(x: Q)(using qc: QueryContext[CL]): (R, SqlTable) =
                val alias = qc.fetchAlias
                val table = SubqueryTable[N, V, CL - 1](x, true, Some(alias))
                (table, table.__sqlTable__)