package sqala.static.dsl.table

import sqala.ast.statement.SqlQuery
import sqala.ast.table.{SqlJoinType, SqlTable, SqlTableAlias}
import sqala.metadata.{AsSqlExpr, FetchCompanion, TableMacro, TableMetaData}
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.Query

import scala.NamedTuple.NamedTuple
import scala.deriving.Mirror
import scala.util.NotGiven
import scala.compiletime.ops.int.>

trait AsTable[T, CL <: Int]:
    type R

    type OKS <: Tuple

    def asTable(x: T)(using QueryContext[CL]): (R, SqlTable)

object AsTable:
    type Aux[T, CL <: Int, O, OOKS <: Tuple] = AsTable[T, CL]:
        type R = O

        type OKS = OOKS

    given entity[O, CL <: Int](using
        na: NotGiven[O <:< AnyTable],
        nt: NotGiven[O <:< Tuple],
        nq: NotGiven[O <:< Query[?, ?, ?, ?]],
        ns: NotGiven[O <:< Seq[?]],
        fc: FetchCompanion[O]
    ): Aux[O, CL, Table[fc.R, Column, CL], EmptyTuple] =
        new AsTable[O, CL]:
            type R = Table[fc.R, Column, CL]

            type OKS = EmptyTuple

            def asTable(x: O)(using qc: QueryContext[CL]): (R, SqlTable) =
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
                (table, table.__sqlTable__)

    given excludedTable[N <: Tuple, V <: Tuple, CL <: Int]: Aux[FromExcluded[N, V, CL], CL, ExcludedTable[N, V, CL], EmptyTuple] =
        new AsTable[FromExcluded[N, V, CL], CL]:
            type R = ExcludedTable[N, V, CL]

            type OKS = EmptyTuple

            def asTable(x: FromExcluded[N, V, CL])(using QueryContext[CL]): (R, SqlTable) =
                (ExcludedTable(x.__aliasName__, x.__items__, x.__sqlTable__), x.__sqlTable__)

    given funcTable[T, TOKS <: Tuple, CL <: Int]: Aux[FromFunc[T, Column, TOKS, CL], CL, FuncTable[T, Column, CL], TOKS] =
        new AsTable[FromFunc[T, Column, TOKS, CL], CL]:
            type R = FuncTable[T, Column, CL]

            type OKS = TOKS

            def asTable(x: FromFunc[T, Column, TOKS, CL])(using QueryContext[CL]): (R, SqlTable) =
                (FuncTable(x.__aliasName__, x.__fieldNames__, x.__columnNames__, x.__sqlTable__), x.__sqlTable__)

    given jsonTable[N <: Tuple, V <: Tuple, TOKS <: Tuple, CL <: Int]: Aux[FromJson[N, V, TOKS, CL], CL, JsonTable[N, V, CL], TOKS] =
        new AsTable[FromJson[N, V, TOKS, CL], CL]:
            type R = JsonTable[N, V, CL]

            type OKS = TOKS

            def asTable(x: FromJson[N, V, TOKS, CL])(using QueryContext[CL]): (R, SqlTable) =
                (JsonTable(x.__aliasName__, x.__items__, x.__sqlTable__), x.__sqlTable__)

    given pivotTable[N <: Tuple, V <: Tuple, TOKS <: Tuple, CL <: Int]: Aux[FromPivot[N, V, TOKS, CL], CL, SubqueryTable[N, V, CL], TOKS] =
        new AsTable[FromPivot[N, V, TOKS, CL], CL]:
            type R = SubqueryTable[N, V, CL]

            type OKS = TOKS

            def asTable(x: FromPivot[N, V, TOKS, CL])(using QueryContext[CL]): (R, SqlTable) =
                (SubqueryTable(x.__aliasName__, x.__items__, x.__sqlTable__), x.__sqlTable__)

    given subquery[N <: Tuple, V <: Tuple, TOKS <: Tuple, L <: Int, S <: QuerySize, Q <: Query[NamedTuple[N, V], TOKS, L, S], CL <: Int](using
        p: AsTableParam[V, CL],
        tt: ToTuple[p.R],
        refl: L > CL =:= true
    ): Aux[Q, CL, SubqueryTable[N, tt.R, CL], TOKS] =
        new AsTable[Q, CL]:
            type R = SubqueryTable[N, tt.R, CL]

            type OKS = TOKS

            def asTable(x: Q)(using qc: QueryContext[CL]): (R, SqlTable) =
                val alias = qc.fetchAlias
                val subquery = SubqueryTable[N, V, CL](x, false, Some(alias))
                (subquery, subquery.__sqlTable__)

    given recognizeTable[N <: Tuple, V <: Tuple, TOKS <: Tuple, CL <: Int]: Aux[FromRecognize[N, V, TOKS, CL], CL, RecognizeTable[N, V, CL], TOKS] =
        new AsTable[FromRecognize[N, V, TOKS, CL], CL]:
            type R = RecognizeTable[N, V, CL]

            type OKS = TOKS

            def asTable(x: FromRecognize[N, V, TOKS, CL])(using QueryContext[CL]): (R, SqlTable) =
                (RecognizeTable(x.__aliasName__, x.__items__, x.__sqlTable__), x.__sqlTable__)

    given graphTable[N <: Tuple, V <: Tuple, TOKS <: Tuple, CL <: Int]: Aux[FromGraph[N, V, TOKS, CL], CL, GraphTable[N, V, CL], TOKS] =
        new AsTable[FromGraph[N, V, TOKS, CL], CL]:
            type R = GraphTable[N, V, CL]

            type OKS = TOKS

            def asTable(x: FromGraph[N, V, TOKS, CL])(using QueryContext[CL]): (R, SqlTable) =
                (GraphTable(x.__aliasName__, x.__items__, x.__sqlTable__), x.__sqlTable__)

    given recursiveTable[N <: Tuple, V <: Tuple, CL <: Int]: Aux[RecursiveTable[N, V, CL], CL, RecursiveTable[N, V, CL], EmptyTuple] =
        new AsTable[RecursiveTable[N, V, CL], CL]:
            type R = RecursiveTable[N, V, CL]

            type OKS = EmptyTuple

            def asTable(x: RecursiveTable[N, V, CL])(using QueryContext[CL]): (R, SqlTable) =
                (x, x.__sqlTable__)

    inline given values[T <: Product, S <: Seq[T], CL <: Int](using
        p: Mirror.ProductOf[T]
    ): Aux[S, CL, Table[T, Column, CL], EmptyTuple] =
        val metaData = TableMacro.tableMetaData[T]
        val instances = AsSqlExpr.summonInstances[p.MirroredElemTypes]
        createValues[T, S, CL](metaData, instances)

    private[sqala] def createValues[T <: Product, S <: Seq[T], CL <: Int](
        metaData: TableMetaData,
        instances: List[AsSqlExpr[?]]
    ): Aux[S, CL, Table[T, Column, CL], EmptyTuple] =
        new AsTable[S, CL]:
            type R = Table[T, Column, CL]

            type OKS = EmptyTuple

            def asTable(x: S)(using qc: QueryContext[CL]): (R, SqlTable) =
                val alias = qc.fetchAlias
                val tableAlias = SqlTableAlias(alias, metaData.columnNames)
                val table = Table[T, Column, CL](
                    Some(alias),
                    metaData,
                    SqlTable.Ident(
                        metaData.tableName,
                        Some(tableAlias),
                        None,
                        None,
                        None
                    )
                )
                val exprList = x.toList.map: datum =>
                    instances.zip(datum.productIterator).map: (i, v) =>
                        i.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(v)
                val sqlValues = SqlQuery.Values(exprList, None)
                (table, SqlTable.Subquery(false, sqlValues, Some(tableAlias), None))

    given joinTable[T, TOKS <: Tuple, CL <: Int]: Aux[FromJoin[T, TOKS, CL], CL, T, TOKS] =
        new AsTable[FromJoin[T, TOKS, CL], CL]:
            type R = T

            type OKS = TOKS

            def asTable(x: FromJoin[T, TOKS, CL])(using QueryContext[CL]): (R, SqlTable) =
                (x.params, x.sqlTable)

    given tuple[H, T <: Tuple, CL <: Int](using
        ah: AsTable[H, CL],
        at: AsTable[T, CL],
        t: ToTuple[at.R],
        c: CombineKindTuple[ah.OKS, at.OKS]
    ): Aux[H *: T, CL, ah.R *: t.R, c.R] =
        new AsTable[H *: T, CL]:
            type R = ah.R *: t.R

            type OKS = c.R

            def asTable(x: H *: T)(using QueryContext[CL]): (R, SqlTable) =
                val (headParam, headTable) = ah.asTable(x.head)
                val (tailParam, tailTable) = at.asTable(x.tail)
                val param = headParam *: t.toTuple(tailParam)
                val table = SqlTable.Join(headTable, SqlJoinType.Cross, tailTable, None)
                (param, table)

    given tuple1[H, CL <: Int](using
        ah: AsTable[H, CL]
    ): Aux[H *: EmptyTuple, CL, ah.R, ah.OKS] =
        new AsTable[H *: EmptyTuple, CL]:
            type R = ah.R

            type OKS = ah.OKS

            def asTable(x: H *: EmptyTuple)(using QueryContext[CL]): (R, SqlTable) =
                ah.asTable(x.head)