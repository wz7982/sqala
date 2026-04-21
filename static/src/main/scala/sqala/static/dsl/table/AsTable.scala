package sqala.static.dsl.table

import sqala.ast.statement.SqlQuery
import sqala.ast.table.{SqlJoinType, SqlTable, SqlTableAlias}
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.Query
import sqala.static.metadata.{AsSqlExpr, FetchCompanion, TableMacro, TableMetaData}

import scala.NamedTuple.NamedTuple
import scala.deriving.Mirror
import scala.util.NotGiven

trait AsTable[T]:
    type R

    def table(x: T)(using QueryContext): (R, SqlTable)

object AsTable:
    type Aux[T, O] = AsTable[T]:
        type R = O

    given entity[O](using
        fc: FetchCompanion[O],
        na: NotGiven[O <:< AnyTable],
        nt: NotGiven[O <:< Tuple],
        nq: NotGiven[O <:< Query[?, ?]],
        ns: NotGiven[O <:< Seq[?]]
    ): Aux[O, Table[fc.R, Column, CanNotInFrom]] =
        new AsTable[O]:
            type R = Table[fc.R, Column, CanNotInFrom]

            def table(x: O)(using c: QueryContext): (R, SqlTable) =
                val metaData = fc.metaData
                val alias = c.fetchAlias
                val table = Table[fc.R, Column, CanNotInFrom](
                    Some(alias),
                    metaData,
                    SqlTable.Ident(
                        metaData.tableName,
                        None,
                        Some(SqlTableAlias(alias, Nil)),
                        None,
                        None
                    )
                )
                (table, table.__sqlTable__)

    given table[T]: Aux[Table[T, Column, CanInFrom], Table[T, Column, CanNotInFrom]] =
        new AsTable[Table[T, Column, CanInFrom]]:
            type R = Table[T, Column, CanNotInFrom]

            def table(x: Table[T, Column, CanInFrom])(using QueryContext): (R, SqlTable) =
                (x.copy(), x.__sqlTable__)

    given excluded[N <: Tuple, V <: Tuple]: Aux[ExcludedTable[N, V, CanInFrom], ExcludedTable[N, V, CanNotInFrom]] =
        new AsTable[ExcludedTable[N, V, CanInFrom]]:
            type R = ExcludedTable[N, V, CanNotInFrom]

            def table(x: ExcludedTable[N, V, CanInFrom])(using QueryContext): (R, SqlTable) =
                (x.copy(), x.__sqlTable__)

    given func[T]: Aux[FuncTable[T, Column, CanInFrom], FuncTable[T, Column, CanNotInFrom]] =
        new AsTable[FuncTable[T, Column, CanInFrom]]:
            type R = FuncTable[T, Column, CanNotInFrom]

            def table(x: FuncTable[T, Column, CanInFrom])(using QueryContext): (R, SqlTable) =
                (x.copy(), x.__sqlTable__)

    given json[N <: Tuple, V <: Tuple]: Aux[JsonTable[N, V, CanInFrom], JsonTable[N, V, CanNotInFrom]] =
        new AsTable[JsonTable[N, V, CanInFrom]]:
            type R = JsonTable[N, V, CanNotInFrom]

            def table(x: JsonTable[N, V, CanInFrom])(using QueryContext): (R, SqlTable) =
                (x.copy(), x.__sqlTable__)

    given subQuery[N <: Tuple, V <: Tuple, S <: QuerySize, Q <: Query[NamedTuple[N, V], S]](using
        p: AsTableParam[V],
        t: ToTuple[p.R]
    ): Aux[Q, SubQueryTable[N, t.R, CanNotInFrom]] =
        new AsTable[Q]:
            type R = SubQueryTable[N, t.R, CanNotInFrom]

            def table(x: Q)(using c: QueryContext): (R, SqlTable) =
                val alias = c.fetchAlias
                val subQuery = SubQueryTable[N, V](x, false, Some(alias))
                (subQuery.copy(), subQuery.__sqlTable__)

    given subQueryTable[N <: Tuple, V <: Tuple]: Aux[SubQueryTable[N, V, CanInFrom], SubQueryTable[N, V, CanNotInFrom]] =
        new AsTable[SubQueryTable[N, V, CanInFrom]]:
            type R = SubQueryTable[N, V, CanNotInFrom]

            def table(x: SubQueryTable[N, V, CanInFrom])(using QueryContext): (R, SqlTable) =
                (x.copy(), x.__sqlTable__)

    given recognize[N <: Tuple, V <: Tuple]: Aux[RecognizeTable[N, V, CanInFrom], RecognizeTable[N, V, CanNotInFrom]] =
        new AsTable[RecognizeTable[N, V, CanInFrom]]:
            type R = RecognizeTable[N, V, CanNotInFrom]

            def table(x: RecognizeTable[N, V, CanInFrom])(using QueryContext): (R, SqlTable) =
                (x.copy(), x.__sqlTable__)

    given graph[N <: Tuple, V <: Tuple]: Aux[GraphTable[N, V, CanInFrom], GraphTable[N, V, CanNotInFrom]] =
        new AsTable[GraphTable[N, V, CanInFrom]]:
            type R = GraphTable[N, V, CanNotInFrom]

            def table(x: GraphTable[N, V, CanInFrom])(using QueryContext): (R, SqlTable) =
                (x.copy(), x.__sqlTable__)

    given recursive[N <: Tuple, V <: Tuple]: Aux[RecursiveTable[N, V], RecursiveTable[N, V]] =
        new AsTable[RecursiveTable[N, V]]:
            type R = RecursiveTable[N, V]

            def table(x: RecursiveTable[N, V])(using QueryContext): (R, SqlTable) =
                (x, x.__sqlTable__)

    inline given values[T <: Product, S <: Seq[T]](using
        p: Mirror.ProductOf[T]
    ): Aux[S, Table[T, Column, CanNotInFrom]] =
        val metaData = TableMacro.tableMetaData[T]
        val instances = AsSqlExpr.summonInstances[p.MirroredElemTypes]
        createValues[T, S](metaData, instances)

    private[sqala] def createValues[T <: Product, S <: Seq[T]](
        metaData: TableMetaData,
        instances: List[AsSqlExpr[?]]
    ): Aux[S, Table[T, Column, CanNotInFrom]] =
        new AsTable[S]:
            type R = Table[T, Column, CanNotInFrom]

            def table(x: S)(using c: QueryContext): (R, SqlTable) =
                val alias = c.fetchAlias
                val tableAlias = SqlTableAlias(alias, metaData.columnNames)
                val table = Table[T, Column, CanNotInFrom](
                    Some(alias),
                    metaData,
                    SqlTable.Ident(
                        metaData.tableName,
                        None,
                        Some(tableAlias),
                        None,
                        None
                    )
                )
                val exprList = x.toList.map: datum =>
                    instances.zip(datum.productIterator).map: (i, v) =>
                        i.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(v)
                val sqlValues = SqlQuery.Values(exprList, None)
                (table, SqlTable.SubQuery(false, sqlValues, Some(tableAlias), None))

    given join[T]: Aux[JoinTable[T], T] =
        new AsTable[JoinTable[T]]:
            type R = T

            def table(x: JoinTable[T])(using QueryContext): (R, SqlTable) =
                (x.params, x.sqlTable)

    given tuple[H: AsTable as ah, T <: Tuple : AsTable as at](using
        t: ToTuple[at.R]
    ): Aux[H *: T, ah.R *: t.R] =
        new AsTable[H *: T]:
            type R = ah.R *: t.R

            def table(x: H *: T)(using QueryContext): (R, SqlTable) =
                val (headParam, headTable) = ah.table(x.head)
                val (tailParam, tailTable) = at.table(x.tail)
                val param = headParam *: t.toTuple(tailParam)
                val table = SqlTable.Join(headTable, SqlJoinType.Cross, tailTable, None)
                (param, table)

    given tuple1[H: AsTable as ah]: Aux[H *: EmptyTuple, ah.R] =
        new AsTable[H *: EmptyTuple]:
            type R = ah.R

            def table(x: H *: EmptyTuple)(using QueryContext): (R, SqlTable) =
                ah.table(x.head)