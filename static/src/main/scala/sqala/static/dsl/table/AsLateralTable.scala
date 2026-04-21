package sqala.static.dsl.table

import sqala.ast.table.SqlTable
import sqala.static.dsl.statement.query.Query
import sqala.static.dsl.{ExprKind, QueryContext, QuerySize, ToTuple}

import scala.NamedTuple.NamedTuple

trait AsLateralTable[T]:
    type R

    def table(x: T)(using QueryContext): (R, SqlTable)

object AsLateralTable:
    type Aux[T, O] = AsLateralTable[T]:
        type R = O

    given func[T, K <: ExprKind]: Aux[FuncTable[T, K, CanInFrom], FuncTable[T, K, CanNotInFrom]] =
        new AsLateralTable[FuncTable[T, K, CanInFrom]]:
            type R = FuncTable[T, K, CanNotInFrom]

            def table(x: FuncTable[T, K, CanInFrom])(using QueryContext): (R, SqlTable) =
                val sqlTable: SqlTable.Func = x.__sqlTable__.copy(lateral = true)
                val table = x.copy[T, K, CanNotInFrom](__sqlTable__ = sqlTable)
                (table, sqlTable)

    given json[N <: Tuple, V <: Tuple]: Aux[JsonTable[N, V, CanInFrom], JsonTable[N, V, CanNotInFrom]] =
        new AsLateralTable[JsonTable[N, V, CanInFrom]]:
            type R = JsonTable[N, V, CanNotInFrom]

            def table(x: JsonTable[N, V, CanInFrom])(using QueryContext): (R, SqlTable) =
                val sqlTable: SqlTable.Json = x.__sqlTable__.copy(lateral = true)
                val table = x.copy[N, V, CanNotInFrom](__sqlTable__ = sqlTable)
                (table, sqlTable)

    given graph[N <: Tuple, V <: Tuple]: Aux[GraphTable[N, V, CanInFrom], GraphTable[N, V, CanNotInFrom]] =
        new AsLateralTable[GraphTable[N, V, CanInFrom]]:
            type R = GraphTable[N, V, CanNotInFrom]

            def table(x: GraphTable[N, V, CanInFrom])(using QueryContext): (R, SqlTable) =
                val sqlTable: SqlTable.Graph = x.__sqlTable__.copy(lateral = true)
                val table = x.copy[N, V, CanNotInFrom](__sqlTable__ = sqlTable)
                (table, sqlTable)

    given subQuery[N <: Tuple, V <: Tuple, S <: QuerySize, Q <: Query[NamedTuple[N, V], S]](using
        p: AsTableParam[V],
        t: ToTuple[p.R]
    ): Aux[Q, SubQueryTable[N, t.R, CanNotInFrom]] =
        new AsLateralTable[Q]:
            type R = SubQueryTable[N, t.R, CanNotInFrom]

            def table(x: Q)(using c: QueryContext): (R, SqlTable) =
                val alias = c.fetchAlias
                val subQuery = SubQueryTable[N, V](x, true, Some(alias))
                (subQuery.copy(), subQuery.__sqlTable__)