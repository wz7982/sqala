package sqala.static.ext.dsl.table

import sqala.ast.order.SqlOrdering
import sqala.ir.{IrExpr, IrOrderingItem, IrTable}
import sqala.metadata.{AsSqlExpr, FetchCompanion}
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.Query
import sqala.static.dsl.table.{AnyTable, AsTableParam, MappedTable, Table}
import sqala.static.ext.dsl.{CanInRecognizePartition, CanInRecognizeSort}

import scala.NamedTuple.NamedTuple
import scala.util.NotGiven

/**
 * Applies `matchRecognize` configuration to a table. Each method
 * returns an immutable copy with the updated value. `CL` is the
 * current query context level, used for scope validation.
 */
trait AsRecognize[T, CL <: Int]:
    /**
     * The table reference type.
     */
    type R

    /**
     * The kind tuple of the outer query.
     */
    type OKS <: Tuple

    /**
     * Initializes the `matchRecognize` clause on the table.
     */
    def asRecognizeTable(x: T): (R, IrTable.MatchRecognizable)

object AsRecognize:
    type Aux[T, CL <: Int, O, OOKS <: Tuple] = AsRecognize[T, CL]:
        type R = O

        type OKS = OOKS

    given entity[O, CL <: Int](using
        na: NotGiven[O <:< AnyTable],
        nt: NotGiven[O <:< Tuple],
        nq: NotGiven[O <:< Query[?, ?, ?, ?]],
        ns: NotGiven[O <:< Seq[?]],
        fc: FetchCompanion[O]
    ): Aux[O, CL, Table[fc.R, Column, CL], EmptyTuple] =
        new AsRecognize[O, CL]:
            type R = Table[fc.R, Column, CL]

            type OKS = EmptyTuple

            def asRecognizeTable(x: O): (R, IrTable.MatchRecognizable) =
                val metaData = fc.metaData
                val source: IrTable.Ident = IrTable.Ident(metaData.tableName, Nil)
                val table = Table[fc.R, Column, CL](
                    source,
                    metaData
                )
                (table, source)

    given subquery[N <: Tuple, V <: Tuple, TOKS <: Tuple, L <: Int, S <: QuerySize, Q <: Query[NamedTuple[N, V], TOKS, L, S], CL <: Int](using
        p: AsTableParam[V, CL],
        vss: ValidateSubqueryScope[L, CL]
    ): Aux[Q, CL, MappedTable[N, WrapToTuple[p.R], false, CL], TOKS] =
        new AsRecognize[Q, CL]:
            type R = MappedTable[N, WrapToTuple[p.R], false, CL]

            type OKS = TOKS

            def asRecognizeTable(x: Q): (R, IrTable.MatchRecognizable) =
                val source: IrTable.Subquery = IrTable.Subquery(false, x.tree, Nil)
                val table = MappedTable[N, V, false, CL](source)
                (table, source)

/**
 * Sets the table alias.
 */
trait AliasRecognize[T]:
    /**
     * Sets the table alias.
     */
    def alias(x: T, name: String): T

object AliasRecognize:
    given table[T, K[_ <: Int] <: ExprKind, L <: Int]: AliasRecognize[Table[T, K, L]] with
        def alias(x: Table[T, K, L], name: String): Table[T, K, L] =
            Table(
                IrTable.PatternVariable(name),
                x.__metaData__,
                true
            )

    given subquery[N <: Tuple, V <: Tuple, O <: Boolean, L <: Int](using
        r: ReplaceTarget[V]
    ): AliasRecognize[MappedTable[N, V, O, L]] with
        def alias(x: MappedTable[N, V, O, L], name: String): MappedTable[N, V, O, L] =
            new MappedTable(
                r.replaceTarget(x.__items__, IrTable.PatternVariable(name))
            )

/**
 * Replaces the table that a mapped table's item column references point to.
 *
 * Used when aliasing a `matchRecognize` derived table: the alias becomes a
 * pattern variable, and every column reference must be retargeted at it.
 */
trait ReplaceTarget[T]:
    /**
     * Returns a copy of `x` whose column references target `table`.
     */
    def replaceTarget(x: T, table: IrTable): T

object ReplaceTarget:
    given columnExpr[T, L <: Int]: ReplaceTarget[ColumnExpr[T, L]] with
        def replaceTarget(x: ColumnExpr[T, L], table: IrTable): ColumnExpr[T, L] =
            ColumnExpr(IrExpr.Column(Some(table), x.expr.name))

    given tuple[H, T <: Tuple](using
        h: ReplaceTarget[H],
        t: ReplaceTarget[T]
    ): ReplaceTarget[H *: T] with
        def replaceTarget(x: H *: T, table: IrTable): H *: T =
            h.replaceTarget(x.head, table) *: t.replaceTarget(x.tail, table)

    given tuple1[H](using h: ReplaceTarget[H]): ReplaceTarget[H *: EmptyTuple] with
        def replaceTarget(x: H *: EmptyTuple, table: IrTable): H *: EmptyTuple =
            h.replaceTarget(x.head, table) *: EmptyTuple

/**
 * Lifts a column expression for `partitionBy` in `matchRecognize`.
 * `CL` is the current query context level.
 */
trait AsRecognizePartitionItem[T, CL <: Int]:
    /**
     * Converts the value to an expression.
     */
    def asExpr(x: T): Expr[?, ?]

object AsRecognizePartitionItem:
    given expr[T, CL <: Int](using
        a: AsExpr[T, CL],
        as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
        i: CanInRecognizePartition[a.K, CL]
    ): AsRecognizePartitionItem[T, CL] with
        def asExpr(x: T): Expr[?, ?] =
            val expr = a.asExpr(x).expr match
                case IrExpr.Column(_, name) =>
                    IrExpr.Column(None, name)
                case e =>
                    e
            Expr(expr)

/**
 * Collects partition items for `partitionBy` in `matchRecognize`.
 * `CL` is the current query context level.
 */
trait AsRecognizePartition[T, CL <: Int]:
    /**
     * Converts the value to a list of expressions.
     */
    def asExprs(x: T): List[Expr[?, ?]]

object AsRecognizePartition:
    given item[T, CL <: Int](using
        a: AsRecognizePartitionItem[T, CL]
    ): AsRecognizePartition[T, CL] with
        def asExprs(x: T): List[Expr[?, ?]] =
            a.asExpr(x) :: Nil

    given tuple[H, T <: Tuple, CL <: Int](using
        h: AsRecognizePartitionItem[H, CL],
        t: AsRecognizePartition[T, CL]
    ): AsRecognizePartition[H *: T, CL] with
        def asExprs(x: H *: T): List[Expr[?, ?]] =
            h.asExpr(x.head) :: t.asExprs(x.tail)

    given tuple1[H, CL <: Int](using
        h: AsRecognizePartitionItem[H, CL]
    ): AsRecognizePartition[H *: EmptyTuple, CL] with
        def asExprs(x: H *: EmptyTuple): List[Expr[?, ?]] =
            h.asExpr(x.head) :: Nil

/**
 * Lifts a column expression for `sortBy` in `matchRecognize`. `CL` is
 * the current query context level.
 */
trait AsRecognizeSortItem[T, CL <: Int]:
    /**
     * Converts the value to a `Sort`.
     */
    def asSort(x: T): Sort[?, ?]

object AsRecognizeSortItem:
    given expr[T, CL <: Int](using
        a: AsExpr[T, CL],
        as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
        i: CanInRecognizeSort[a.K, CL]
    ): AsRecognizeSortItem[T, CL] with
        def asSort(x: T): Sort[?, ?] =
            val expr = a.asExpr(x).expr match
                case IrExpr.Column(_, name) =>
                    IrExpr.Column(None, name)
                case e =>
                    e
            Sort(IrOrderingItem(expr, SqlOrdering.Asc, None))

    given sort[T, EK <: ExprKind, CL <: Int](using
        as: ValidateArgType[AsSqlExpr, T, "scalar"],
        i: CanInRecognizeSort[EK, CL]
    ): AsRecognizeSortItem[Sort[T, EK], CL] with
        def asSort(x: Sort[T, EK]): Sort[?, ?] =
            val expr = x.ordering.expr match
                case IrExpr.Column(_, name) =>
                    IrExpr.Column(None, name)
                case e =>
                    e
            Sort(IrOrderingItem(expr, x.ordering.ordering, x.ordering.nullsOrdering))

/**
 * Collects sort items for `sortBy` in `matchRecognize`. `CL` is the
 * current query context level.
 */
trait AsRecognizeSort[T, CL <: Int]:
    /**
     * Converts the value to a list of `Sort` values.
     */
    def asSorts(x: T): List[Sort[?, ?]]

object AsRecognizeSort:
    given item[T, CL <: Int](using
        a: AsRecognizeSortItem[T, CL]
    ): AsRecognizeSort[T, CL] with
        def asSorts(x: T): List[Sort[?, ?]] =
            a.asSort(x) :: Nil

    given tuple[H, T <: Tuple, CL <: Int](using
        h: AsRecognizeSortItem[H, CL],
        t: AsRecognizeSort[T, CL]
    ): AsRecognizeSort[H *: T, CL] with
        def asSorts(x: H *: T): List[Sort[?, ?]] =
            h.asSort(x.head) :: t.asSorts(x.tail)

    given tuple1[H, CL <: Int](using
        h: AsRecognizeSortItem[H, CL]
    ): AsRecognizeSort[H *: EmptyTuple, CL] with
        def asSorts(x: H *: EmptyTuple): List[Sort[?, ?]] =
            h.asSort(x.head) :: Nil