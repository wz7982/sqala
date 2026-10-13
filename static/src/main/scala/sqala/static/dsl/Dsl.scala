package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.{SqlQuery, SqlSetOperator, SqlWithItem}
import sqala.ast.table.*
import sqala.ast.token.SqlUnsafeCustomToken
import sqala.ast.window.{SqlWindow, SqlWindowFrameBound}
import sqala.metadata.*
import sqala.static.dsl.statement.query.*
import sqala.static.dsl.table.*
import sqala.util.NonEmptyList

import scala.NamedTuple.NamedTuple
import scala.annotation.targetName
import scala.compiletime.ops.int.{+, >}
import sqala.ir.IrQuery
import sqala.ir.IrTable
import sqala.ir.IrExpr
import sqala.ir.IrWindowFrameBound

/**
 * Starts a `SELECT` query from one or more table sources.
 * Supports entity companion objects, joins, subqueries, graph
 * tables, pivot tables, and more.
 *
 * {{{
 * // Single table
 * from(User).filter(u => u.id == 1)
 *
 * // Join table
 * from(Channel.join(Post).on((c, p) => c.id == p.channelId))
 *
 * // Subquery (must project to a named tuple via `map`) table
 * from(from(User).map(u => (name = u.name)))
 *
 * // Multi-table tuple
 * from(Channel, Post)
 *     .filter((c, p) => c.id == p.channelId)
 * }}}
 */
def from[T, CL <: Int](using qc: QueryContext[CL] = QueryContext[0]())(
    tables: QueryContext[CL + 1] ?=> T
)(using
    at: AsTable[T, CL + 1],
    as: AsSelect[at.R]
): TableQuery[at.R, at.OKS, CL + 1] =
    given QueryContext[CL + 1] = QueryContext()
    val (params, fromTable) = at.asTable(tables)
    val selectItems = as.asSelectItems(params, 1)
    val tree: IrQuery.Select = IrQuery.Select(
        None,
        selectItems,
        fromTable,
        None,
        None,
        None,
        Nil,
        None,
        None
    )
    TableQuery(params, tree)

extension [A, CL <: Int](a: A)(using qc: QueryContext[CL], ta: AsTable[A, CL])
    /**
     * Inner join. Must be followed by `on`.
     *
     * {{{
     * from(Channel.join(Post).on((c, p) => c.id == p.channelId))
     * }}}
     */
    def join[B](b: B)(using
        tb: AsTable[B, CL],
        j: Join[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL] =
        val (leftTable, leftIrTable) = ta.asTable(a)
        val (rightTable, rightIrTable) = tb.asTable(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, IrTable.Join(leftIrTable, SqlJoinType.Inner, rightIrTable, None))

    /**
     * Lateral inner join. Must be followed by `on`. The lateral
     * table source can reference columns from the left table.
     *
     * {{{
     * from(
     *     Channel.joinLateral(c =>
     *         from(Post)
     *             .filter(p => c.id == p.channelId)
     *             .map(p => (id = p.id, channelId = p.channelId, title = p.title))
     *             .sortBy(p => p.likeCount.desc)
     *             .take(2)
     *     ).on((c, p) => c.id == p.channelId)
     * )
     * }}}
     */
    // def joinLateral[B](f: QueryContext[CL + 1] ?=> ta.R => B)(using
    //     tb: AsLateralTable[B, CL + 1],
    //     j: Join[ta.R, tb.R],
    //     c: CombineKindTuple[ta.OKS, tb.OKS]
    // ): JoinPart[j.R, c.R, CL] =
    //     given QueryContext[CL + 1] = QueryContext(qc.tableIndex)
    //     val (leftTable, leftIrTable) = ta.asTable(a)
    //     val (rightTable, rightIrTable) = tb.asTable(f(leftTable))
    //     val params = j.join(leftTable, rightTable)
    //     JoinPart(params, IrTable.Join(leftIrTable, SqlJoinType.Inner, rightIrTable, None))

    /**
     * Cross join. No `on` condition needed.
     *
     * {{{
     * from(Channel.crossJoin(Post)).filter((c, p) => c.id == p.channelId)
     * }}}
     */
    def crossJoin[B](b: B)(using
        tb: AsTable[B, CL],
        j: Join[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): FromJoin[j.R, c.R, CL] =
        val (leftTable, leftIrTable) = ta.asTable(a)
        val (rightTable, rightIrTable) = tb.asTable(b)
        val params = j.join(leftTable, rightTable)
        FromJoin(params, IrTable.Join(leftIrTable, SqlJoinType.Cross, rightIrTable, None))

    /**
     * Lateral cross join. The lateral table source can reference
     * columns from the left table.
     *
     * {{{
     * from(
     *     Channel.crossJoinLateral(c =>
     *         from(Post)
     *             .filter(p => c.id == p.channelId)
     *             .map(p => (id = p.id, channelId = p.channelId, title = p.title))
     *             .sortBy(p => p.likeCount.desc)
     *             .take(2)
     *     )
     * ).filter((c, p) => c.id == p.channelId)
     * }}}
     */
    // def crossJoinLateral[B](f: QueryContext[CL + 1] ?=> ta.R => B)(using
    //     tb: AsLateralTable[B, CL + 1],
    //     j: Join[ta.R, tb.R],
    //     c: CombineKindTuple[ta.OKS, tb.OKS]
    // ): FromJoin[j.R, c.R, CL + 1] =
    //     given QueryContext[CL + 1] = QueryContext(qc.tableIndex)
    //     val (leftTable, leftIrTable) = ta.asTable(a)
    //     val (rightTable, rightIrTable) = tb.asTable(f(leftTable))
    //     val params = j.join(leftTable, rightTable)
    //     FromJoin(params, IrTable.Join(leftIrTable, SqlJoinType.Cross, rightIrTable, None))

    /**
     * Left outer join. Must be followed by `on`. The right side
     * columns are wrapped with `Option`.
     *
     * {{{
     * from(Channel.leftJoin(Post).on((c, p) => c.id == p.channelId))
     * }}}
     */
    def leftJoin[B](b: B)(using
        tb: AsTable[B, CL],
        j: LeftJoin[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL] =
        val (leftTable, leftIrTable) = ta.asTable(a)
        val (rightTable, rightIrTable) = tb.asTable(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, IrTable.Join(leftIrTable, SqlJoinType.Left, rightIrTable, None))

    /**
     * Lateral left outer join. Must be followed by `on`. The
     * lateral table source can reference columns from the left
     * table.
     *
     * {{{
     * from(
     *     Channel.leftJoinLateral(c =>
     *         from(Post)
     *             .filter(p => c.id == p.channelId)
     *             .map(p => (id = p.id, channelId = p.channelId, title = p.title))
     *             .sortBy(p => p.likeCount.desc)
     *             .take(2)
     *     ).on((c, p) => c.id == p.channelId)
     * )
     * }}}
     */
    // def leftJoinLateral[B](f: QueryContext[CL + 1] ?=> ta.R => B)(using
    //     tb: AsLateralTable[B, CL + 1],
    //     j: Join[ta.R, tb.R],
    //     c: CombineKindTuple[ta.OKS, tb.OKS]
    // ): JoinPart[j.R, c.R, CL + 1] =
    //     given QueryContext[CL + 1] = QueryContext(qc.tableIndex)
    //     val (leftTable, leftIrTable) = ta.asTable(a)
    //     val (rightTable, rightIrTable) = tb.asTable(f(leftTable))
    //     val params = j.join(leftTable, rightTable)
    //     JoinPart(params, IrTable.Join(leftIrTable, SqlJoinType.Left, rightIrTable, None))

    /**
     * Right outer join. Must be followed by `on`. The left side
     * columns are wrapped with `Option`.
     *
     * {{{
     * from(Channel.rightJoin(Post).on((c, p) => c.id == p.channelId))
     * }}}
     */
    def rightJoin[B](b: B)(using
        tb: AsTable[B, CL],
        j: RightJoin[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL] =
        val (leftTable, leftIrTable) = ta.asTable(a)
        val (rightTable, rightIrTable) = tb.asTable(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, IrTable.Join(leftIrTable, SqlJoinType.Right, rightIrTable, None))

    /**
     * Full outer join. Must be followed by `on`. Both sides are
     * wrapped with `Option`.
     *
     * {{{
     * from(Channel.fullJoin(Post).on((c, p) => c.id == p.channelId))
     * }}}
     */
    def fullJoin[B](b: B)(using
        tb: AsTable[B, CL],
        j: FullJoin[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL] =
        val (leftTable, leftIrTable) = ta.asTable(a)
        val (rightTable, rightIrTable) = tb.asTable(b)
        val params = j.join(leftTable, rightTable)
        JoinPart(params, IrTable.Join(leftIrTable, SqlJoinType.Full, rightIrTable, None))

extension [T, CL <: Int](x: T)(using qc: QueryContext[CL], a: AsExpr[T, CL])
    /**
     * Explicitly lifts a value, expression, or subquery to an
     * expression. Values are normally lifted automatically, but this
     * can be useful in custom function or operator contexts.
     *
     * {{{
     * from(Post).map(p => p.likeCount + 1.asExpr)
     * }}}
     */
    def asExpr: Expr[a.R, a.K] =
        a.asExpr(x)

    /**
     * Casts an expression to a target type. Maps to
     * `CAST(expr AS type)`.
     *
     * {{{
     * from(Post).map(p => p.id.as[String])
     * }}}
     */
    def as[R](using c: Cast[a.R, R], kt: KindToTuple[a.K]): Expr[Option[R], Composite[kt.R]] =
        Expr(IrExpr.Cast(a.asExpr(x).asIrExpr, c.castType))

extension [T, CL <: Int](x: T)(using qc: QueryContext[CL])
    def filter[F](f: F)(using
        a: AsExpr[F, CL],
        kt: KindToTuple[a.K],
        ci: CanInFilter[a.R, kt.R],
        c: CallAggFilter[T, kt.R]
    ): c.R =
        c.call(x, a.asExpr(f).asIrExpr)

/**
 * Specifies the `PARTITION BY` clause in a window function.
 * Partitions the rows into groups for the window calculation.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount).over(
 *     partitionBy(p.authorId)
 * ))
 * }}}
 */
def partitionBy[T, CL <: Int](partitionValue: T)(using
    qc: QueryContext[CL],
    oc: OverContext,
    a: AsPartition[T, CL]
): PartitionedOver[a.KS] =
    PartitionedOver(partitionBy = a.asExprs(partitionValue))

/**
 * Specifies the `ORDER BY` clause in a window function.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount).over(
 *     sortBy(p.id.desc)
 * ))
 * }}}
 */
def sortBy[T, CL <: Int](sortValue: T)(using
    qc: QueryContext[CL],
    oc: OverContext,
    a: AsOverSort[T, CL]
): SortedOver[a.R, a.KS] =
    SortedOver(sortBy = a.asSorts(sortValue))

/**
 * Alias of `sortBy`, provided for users familiar with `ORDER BY`.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount).over(
 *     orderBy(p.id.desc)
 * ))
 * }}}
 */
def orderBy[T, CL <: Int](sortValue: T)(using
    qc: QueryContext[CL],
    oc: OverContext,
    a: AsOverSort[T, CL]
): SortedOver[a.R, a.KS] =
    SortedOver(sortBy = a.asSorts(sortValue))

extension [T, CL <: Int](x: T)(using qc: QueryContext[CL], co: CanCallOver[T])
    /**
     * Applies a window specification to the function, promoting it
     * to an `Expr`. Maps to SQL `OVER ()`.
     *
     * {{{
     * from(Post).map(p => rowNumber().over())
     * }}}
     */
    def over(): Expr[co.R, Window[co.KS]] =
        Expr(
            IrExpr.Window(
                co.asIrExpr(x),
                Nil,
                Nil,
                None
            )
        )

    /**
     * Applies a window specification with `partitionBy` and `orderBy`
     * clause. Maps to SQL `OVER (PARTITION BY ... ORDER BY ...)`.
     *
     * {{{
     * from(Post).map(p => rowNumber().over(
     *     partitionBy(p.channelId).sortBy(p.viewCount.desc)
     * ))
     * }}}
     */
    def over[OKS <: Tuple](over: OverContext ?=> Over[OKS])(using
        i: CanInWindow[OKS],
        c: CombineKindTuple[co.KS, OKS]
    ): Expr[co.R, Window[c.R]] =
        given OverContext = OverContext()
        val o = over
        Expr(
            IrExpr.Window(
                co.asIrExpr(x),
                o.partitionBy.map(_.asIrExpr),
                o.sortBy.map(_.asIrOrderingItem),
                o.frame
            )
        )

/**
 * References the current row in a window frame. Maps to
 * `CURRENT ROW`. Used with `rows`, `range`, or `groups`.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount).over(
 *     partitionBy(p.authorId).orderBy(p.id.desc).rows(currentRow)
 * ))
 * }}}
 */
def currentRow[CL <: Int](using QueryContext[CL], OverContext): FrameBound[Nothing, 3, "current row"] =
    FrameBound(IrWindowFrameBound.CurrentRow)

/**
 * References the first row in a window frame. Maps to
 * `UNBOUNDED PRECEDING`. Used with `rows`, `range`, or `groups`.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount).over(
 *     partitionBy(p.authorId).orderBy(p.id.desc).rows(unboundedPreceding)
 * ))
 * }}}
 */
def unboundedPreceding[CL <: Int](using QueryContext[CL], OverContext): FrameBound[Nothing, 1, "unbounded preceding"] =
    FrameBound(IrWindowFrameBound.UnboundedPreceding)

/**
 * References the last row in a window frame. Maps to
 * `UNBOUNDED FOLLOWING`. Used with `rows`, `range`, or `groups`.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount).over(
 *     partitionBy(p.authorId).orderBy(p.id.desc).rows(unboundedFollowing)
 * ))
 * }}}
 */
def unboundedFollowing[CL <: Int](using QueryContext[CL], OverContext): FrameBound[Nothing, 5, "unbounded following"] =
    FrameBound(IrWindowFrameBound.UnboundedFollowing)

extension [T, CL <: Int](n: T)(using 
    qc: QueryContext[CL], 
    oc: OverContext, 
    a: AsExpr[T, CL], 
    kt: KindToTuple[a.K], 
    nv: IsNotVariable[kt.R]
)
    /**
     * References N rows before the current row in a window frame.
     * Maps to `n PRECEDING`. Used with `rows`, `range`, or `groups`.
     *
     * {{{
     * from(Post).map(p => sum(p.likeCount).over(
     *     partitionBy(p.authorId).orderBy(p.id.desc).rows(1.preceding)
     * ))
     * }}}
     */
    def preceding: FrameBound[a.R, 2, "preceding row"] =
        FrameBound(IrWindowFrameBound.Preceding(n.asExpr.asIrExpr))

    /**
     * References N rows after the current row in a window frame.
     * Maps to `n FOLLOWING`. Used with `rows`, `range`, or `groups`.
     *
     * {{{
     * from(Post).map(p => sum(p.likeCount).over(
     *     partitionBy(p.authorId).orderBy(p.id.desc).rows(1.following)
     * ))
     * }}}
     */
    def following: FrameBound[a.R, 4, "following row"] =
        FrameBound(IrWindowFrameBound.Following(n.asExpr.asIrExpr))