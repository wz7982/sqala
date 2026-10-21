package sqala.static.dsl.table

import sqala.metadata.{AsSqlExprs, FetchCompanion, TableMacro, TableMetaData}
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.Query

import scala.NamedTuple.NamedTuple
import scala.deriving.Mirror
import scala.util.NotGiven
import scala.compiletime.summonInline
import scala.compiletime.ops.int.>
import sqala.ir.IrTable
import sqala.ast.table.SqlJoinType
import scala.quoted.Type
import scala.quoted.Quotes
import scala.compiletime.error
import sqala.ast.expr.SqlExpr
import sqala.ir.IrQuery
import sqala.util.NonEmptyList
import sqala.ir.IrExpr

/**
 * Used by `from` to lift various DSL types into a table reference
 * and its AST node. `CL` is the current query context level, used for
 * scope validation.
 */
trait AsTable[T, CL <: Int]:
    /**
     * The table reference type.
     */
    type R

    /**
     * The kind tuple of the outer query.
     */
    type OKS <: Tuple

    /**
     * Converts the value to a table reference and its AST.
     */
    def asTable(x: T): (R, IrTable)

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

            def asTable(x: O): (R, IrTable) =
                val metaData = fc.metaData
                val irTable = IrTable.Ident(metaData.tableName, Nil)
                val table = Table[fc.R, Column, CL](
                    irTable,
                    metaData
                )
                (table, irTable)

    inline given subquery[N <: Tuple, V <: Tuple, TOKS <: Tuple, L <: Int, S <: QuerySize, Q <: Query[NamedTuple[N, V], TOKS, L, S], CL <: Int](using
        p: AsTableParam[V, CL],
        tt: ToTuple[p.R],
        vss: ValidateSubqueryScope[L, CL]
    ): Aux[Q, CL, MappedTable[N, tt.R, CL], TOKS] =
        new AsTable[Q, CL]:
            type R = MappedTable[N, tt.R, CL]

            type OKS = TOKS

            def asTable(x: Q): (R, IrTable) =
                val sqlTable =
                    IrTable.Subquery(
                        false,
                        x.tree,
                        Nil
                    )
                val subquery = MappedTable[N, V, CL](sqlTable)
                (subquery, sqlTable)

    // TODO
    // given recursiveTable[N <: Tuple, V <: Tuple, CL <: Int]: Aux[RecursiveTable[N, V, CL], CL, RecursiveTable[N, V, CL], EmptyTuple] =
    //     new AsTable[RecursiveTable[N, V, CL], CL]:
    //         type R = RecursiveTable[N, V, CL]

    //         type OKS = EmptyTuple

    //         def asTable(x: RecursiveTable[N, V, CL]): (R, SqlTable) =
    //             (x, x.__sqlTable__)

    inline given values[T <: Product, S <: Seq[T], CL <: Int](using p: Mirror.ProductOf[T]): Aux[S, CL, Table[T, Column, CL], EmptyTuple] =
        val metaData = TableMacro.tableMetaData[T]
        val instances = summonInline[AsSqlExprs[p.MirroredElemTypes]]
        createValues[T, S, CL](
            metaData,
            (datum: T) => instances.asSqlExprs(Tuple.fromProductTyped(datum))
        )

    private def createValues[T <: Product, S <: Seq[T], CL <: Int](
        metaData: TableMetaData,
        asExprs: T => List[SqlExpr]
    ): Aux[S, CL, Table[T, Column, CL], EmptyTuple] =
        new AsTable[S, CL]:
            type R = Table[T, Column, CL]

            type OKS = EmptyTuple

            def asTable(x: S): (R, IrTable) =
                require(x.nonEmpty, "Values table must have at least one row.")
                val exprList = x.toList.map: datum =>
                    val row = asExprs(datum).map(e => IrExpr.Native(e))
                    require(row.nonEmpty, "Values table must have at least one column.")
                    NonEmptyList(row.head, row.tail)
                val values = IrQuery.Values(NonEmptyList(exprList.head, exprList.tail))
                val table = IrTable.Subquery(false, values, metaData.columns.toList)
                (Table[T, Column, CL](table, metaData), table)

    inline given joinPart[T, TOKS <: Tuple, CL <: Int]: Aux[JoinPart[T, TOKS, CL], CL, T, TOKS] =
        error("Join table must have an on clause.")

    given joinTable[T, TOKS <: Tuple, CL <: Int]: Aux[FromJoin[T, TOKS, CL], CL, T, TOKS] =
        new AsTable[FromJoin[T, TOKS, CL], CL]:
            type R = T

            type OKS = TOKS

            def asTable(x: FromJoin[T, TOKS, CL]): (R, IrTable) =
                (x.params, x.table)

    given tuple[H, T <: Tuple, CL <: Int](using
        ah: AsTable[H, CL],
        at: AsTable[T, CL],
        t: ToTuple[at.R],
        c: CombineKindTuple[ah.OKS, at.OKS]
    ): Aux[H *: T, CL, ah.R *: t.R, c.R] =
        new AsTable[H *: T, CL]:
            type R = ah.R *: t.R

            type OKS = c.R

            def asTable(x: H *: T): (R, IrTable) =
                val (headParam, headTable) = ah.asTable(x.head)
                val (tailParam, tailTable) = at.asTable(x.tail)
                val param = headParam *: t.toTuple(tailParam)
                val table = IrTable.Join(headTable, SqlJoinType.Cross, tailTable, None)
                (param, table)

    given tuple1[H, CL <: Int](using
        ah: AsTable[H, CL]
    ): Aux[H *: EmptyTuple, CL, ah.R, ah.OKS] =
        new AsTable[H *: EmptyTuple, CL]:
            type R = ah.R

            type OKS = ah.OKS

            def asTable(x: H *: EmptyTuple): (R, IrTable) =
                ah.asTable(x.head)

    inline given fallback[T, CL <: Int](using
        NotGiven[AsTable[T, CL]]
    ): Aux[T, CL, T, EmptyTuple] =
        ${ errorMacro[T] }

    def errorMacro[T: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val t = Type.show[T]

        report.errorAndAbort(s"Cannot convert type $t into a SQL table.")