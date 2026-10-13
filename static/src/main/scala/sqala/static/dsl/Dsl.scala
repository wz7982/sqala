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