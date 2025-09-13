package sqala.static.dsl.table

import sqala.ast.table.SqlTable
import sqala.static.dsl.QueryContext
import sqala.static.dsl.statement.query.Query

import scala.NamedTuple.NamedTuple

trait AsLateralTable[T]:
    type R

    def table(x: T)(using QueryContext): (R, SqlTable)

object AsLateralTable:
    type Aux[T, O] = AsLateralTable[T]:
        type R = O

    given funcTable[T]: Aux[FuncTable[T], FuncTable[T]] =
        new AsLateralTable[FuncTable[T]]:
            type R = FuncTable[T]

            def table(x: FuncTable[T])(using QueryContext): (R, SqlTable) =
                val sqlTable: SqlTable.Func = x.__sqlTable__.copy(lateral = true)
                val table = x.copy[T](__sqlTable__ = sqlTable)
                (table, sqlTable)

    given jsonTable[N <: Tuple, V <: Tuple]: Aux[JsonTable[N, V], JsonTable[N, V]] =
        new AsLateralTable[JsonTable[N, V]]:
            type R = JsonTable[N, V]

            def table(x: JsonTable[N, V])(using QueryContext): (R, SqlTable) =
                val sqlTable: SqlTable.Json = x.__sqlTable__.copy(lateral = true)
                val table = x.copy[N, V](__sqlTable__ = sqlTable)
                (table, sqlTable)

    given subQueryTable[N <: Tuple, V <: Tuple, Q <: Query[NamedTuple[N, V]]](using 
        AsTableParam[V]
    ): Aux[Q, SubQueryTable[N, V]] =
        new AsLateralTable[Q]:
            type R = SubQueryTable[N, V]

            def table(x: Q)(using c: QueryContext): (R, SqlTable) =
                val alias = c.fetchAlias
                val subQuery = SubQueryTable[N, V](x, true, Some(alias))
                (subQuery, subQuery.__sqlTable__)