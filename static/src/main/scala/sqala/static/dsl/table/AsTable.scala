package sqala.static.dsl.table

import sqala.ast.statement.SqlQuery
import sqala.ast.table.{SqlJoinType, SqlTable, SqlTableAlias}
import sqala.static.dsl.statement.query.Query
import sqala.static.dsl.{QueryContext, ToTuple}
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
        nt: NotGiven[O <:< Tuple],
        nq: NotGiven[O <:< Query[?]],
        ns: NotGiven[O <:< Seq[?]]
    ): Aux[O, Table[fc.R]] =
        new AsTable[O]:
            type R = Table[fc.R]

            def table(x: O)(using c: QueryContext): (R, SqlTable) =
                val metaData = fc.metaData
                val alias = c.fetchAlias
                val table = Table[fc.R](
                    Some(alias),
                    metaData,
                    SqlTable.Standard(
                        metaData.tableName,
                        None,
                        Some(SqlTableAlias(alias, Nil)),
                        None,
                        None
                    )
                )
                (table, table.__sqlTable__)

    given table[T]: Aux[Table[T], Table[T]] =
        new AsTable[Table[T]]:
            type R = Table[T]

            def table(x: Table[T])(using QueryContext): (R, SqlTable) =
                (x, x.__sqlTable__)

    given funcTable[T]: Aux[FuncTable[T], FuncTable[T]] =
        new AsTable[FuncTable[T]]:
            type R = FuncTable[T]

            def table(x: FuncTable[T])(using QueryContext): (R, SqlTable) =
                (x, x.__sqlTable__)

    given jsonTable[N <: Tuple, V <: Tuple]: Aux[JsonTable[N, V], JsonTable[N, V]] =
        new AsTable[JsonTable[N, V]]:
            type R = JsonTable[N, V]

            def table(x: JsonTable[N, V])(using QueryContext): (R, SqlTable) =
                (x, x.__sqlTable__)

    given subQuery[N <: Tuple, V <: Tuple, Q <: Query[NamedTuple[N, V]]](using 
        AsTableParam[V]
    ): Aux[Q, SubQueryTable[N, V]] =
        new AsTable[Q]:
            type R = SubQueryTable[N, V]

            def table(x: Q)(using c: QueryContext): (R, SqlTable) =
                val alias = c.fetchAlias
                val subQuery = SubQueryTable[N, V](x, false, Some(alias))
                (subQuery, subQuery.__sqlTable__)

    given subQueryTable[N <: Tuple, V <: Tuple]: Aux[SubQueryTable[N, V], SubQueryTable[N, V]] =
        new AsTable[SubQueryTable[N, V]]:
            type R = SubQueryTable[N, V]

            def table(x: SubQueryTable[N, V])(using QueryContext): (R, SqlTable) =
                (x, x.__sqlTable__)

    given recognizeTable[N <: Tuple, V <: Tuple]: Aux[RecognizeTable[N, V], RecognizeTable[N, V]] =
        new AsTable[RecognizeTable[N, V]]:
            type R = RecognizeTable[N, V]

            def table(x: RecognizeTable[N, V])(using QueryContext): (R, SqlTable) =
                (x, x.__sqlTable__)

    given graphTable[N <: Tuple, V <: Tuple]: Aux[GraphTable[N, V], GraphTable[N, V]] =
        new AsTable[GraphTable[N, V]]:
            type R = GraphTable[N, V]

            def table(x: GraphTable[N, V])(using QueryContext): (R, SqlTable) =
                (x, x.__sqlTable__)

    given recursiveTable[N <: Tuple, V <: Tuple]: Aux[RecursiveTable[N, V], RecursiveTable[N, V]] =
        new AsTable[RecursiveTable[N, V]]:
            type R = RecursiveTable[N, V]

            def table(x: RecursiveTable[N, V])(using QueryContext): (R, SqlTable) =
                (x, x.__sqlTable__)

    inline given values[T <: Product, S <: Seq[T]](using 
        p: Mirror.ProductOf[T]
    ): Aux[S, Table[T]] =
        val metaData = TableMacro.tableMetaData[T]
        val instances = AsSqlExpr.summonInstances[p.MirroredElemTypes]
        createValues[T, S](metaData, instances)

    private[sqala] def createValues[T <: Product, S <: Seq[T]](
        metaData: TableMetaData,
        instances: List[AsSqlExpr[?]]
    ): Aux[S, Table[T]] =
        new AsTable[S]:
            type R = Table[T]

            def table(x: S)(using c: QueryContext): (R, SqlTable) =
                val alias = c.fetchAlias
                val tableAlias = SqlTableAlias(alias, metaData.columnNames)
                val table = Table[T](
                    Some(alias),
                    metaData,
                    SqlTable.Standard(
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