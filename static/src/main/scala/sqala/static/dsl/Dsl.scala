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
import sqala.ir.IrCaseBranch
import sqala.static.dsl.statement.dml.Insert
import sqala.static.dsl.statement.dml.InsertTable
import sqala.static.dsl.statement.dml.Update
import sqala.static.dsl.statement.dml.UpdateTable
import sqala.static.dsl.statement.dml.Delete
import sqala.static.dsl.statement.dml.UpdatePair
import sqala.static.dsl.statement.dml.UpdateSetContext

/**
 * Starts an `INSERT` statement builder.
 *
 * {{{
 * insert[User](u => (u.id, u.name)).values((1, "Alice"), (2, "Bob"))
 * }}}
 */
inline def insert[T <: Product]: Insert[T, InsertTable] =
    Insert.apply[T]

/**
 * Starts an `UPDATE` statement builder.
 *
 * {{{
 * update[User].set(u => u.name := "Alice").where(_.id == 1)
 * }}}
 */
inline def update[T <: Product]: Update[T, UpdateTable] =
    Update.apply[T]

/**
 * Starts a `DELETE` statement builder.
 *
 * {{{
 * delete[User].where(_.id == 1)
 * }}}
 */
inline def delete[T <: Product]: Delete[T] =
    Delete.apply[T]

extension [T](expr: ColumnExpr[T, 1])
    /**
     * Assigns a value to a column in an `UPDATE` statement.
     *
     * {{{
     * update[User].set(u => u.name := "Alice")
     * }}}
     */
    @targetName("to")
    def :=[R](updateExpr: R)(using
        a: AsExpr[R, 1],
        c: CanCompare[T, a.R],
        uc: UpdateSetContext
    ): UpdatePair =
        UpdatePair(expr.expr.name, a.asExpr(updateExpr).expr)

/**
 * Starts a `SELECT` query from one or more table sources.
 * Supports entity companion objects, joins, subqueries, graph
 * tables, pivot tables, and more.
 *
 * {{{
 * // Single table
 * from(User).filter(u => u.id === 1)
 *
 * // Join table
 * from(Channel.join(Post).on((c, p) => c.id === p.channelId))
 *
 * // Subquery (must project to a named tuple via `map`) table
 * from(from(User).map(u => (name = u.name)))
 *
 * // Multi-table tuple
 * from(Channel, Post)
 *     .filter((c, p) => c.id === p.channelId)
 * }}}
 */
def from[T, CL <: Int](using qc: QueryContext[CL] = QueryContext[0]())(
    tables: QueryContext[CL + 1] ?=> T
)(using
    at: AsTable[T, CL + 1],
    am: AsMap[at.R, CL + 1]
): TableQuery[at.R, at.OKS, CL + 1] =
    given QueryContext[CL + 1] = QueryContext()
    val (params, fromTable) = at.asTable(tables)
    val selectItems = am.asSelectItems(params, 1)
    val tree: IrQuery.Select = IrQuery.Select(
        None,
        selectItems,
        Some(fromTable),
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
     * from(Channel.join(Post).on((c, p) => c.id === p.channelId))
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
     *             .filter(p => c.id === p.channelId)
     *             .map(p => (id = p.id, channelId = p.channelId, title = p.title))
     *             .sortBy(p => p.likeCount.desc)
     *             .take(2)
     *     ).on((c, p) => c.id === p.channelId)
     * )
     * }}}
     */
    def joinLateral[B](f: QueryContext[CL + 1] ?=> ta.R => B)(using
        tb: AsLateralTable[B, CL + 1],
        j: Join[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL] =
        given QueryContext[CL + 1] = QueryContext()
        val (leftTable, leftIrTable) = ta.asTable(a)
        val (rightTable, rightIrTable) = tb.asTable(f(leftTable))
        val params = j.join(leftTable, rightTable)
        JoinPart(params, IrTable.Join(leftIrTable, SqlJoinType.Inner, rightIrTable, None))

    /**
     * Cross join. No `on` condition needed.
     *
     * {{{
     * from(Channel.crossJoin(Post)).filter((c, p) => c.id === p.channelId)
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
     *             .filter(p => c.id === p.channelId)
     *             .map(p => (id = p.id, channelId = p.channelId, title = p.title))
     *             .sortBy(p => p.likeCount.desc)
     *             .take(2)
     *     )
     * ).filter((c, p) => c.id === p.channelId)
     * }}}
     */
    def crossJoinLateral[B](f: QueryContext[CL + 1] ?=> ta.R => B)(using
        tb: AsLateralTable[B, CL + 1],
        j: Join[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): FromJoin[j.R, c.R, CL + 1] =
        given QueryContext[CL + 1] = QueryContext()
        val (leftTable, leftIrTable) = ta.asTable(a)
        val (rightTable, rightIrTable) = tb.asTable(f(leftTable))
        val params = j.join(leftTable, rightTable)
        FromJoin(params, IrTable.Join(leftIrTable, SqlJoinType.Cross, rightIrTable, None))

    /**
     * Left outer join. Must be followed by `on`. The right side
     * columns are wrapped with `Option`.
     *
     * {{{
     * from(Channel.leftJoin(Post).on((c, p) => c.id === p.channelId))
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
     *             .filter(p => c.id === p.channelId)
     *             .map(p => (id = p.id, channelId = p.channelId, title = p.title))
     *             .sortBy(p => p.likeCount.desc)
     *             .take(2)
     *     ).on((c, p) => c.id === p.channelId)
     * )
     * }}}
     */
    def leftJoinLateral[B](f: QueryContext[CL + 1] ?=> ta.R => B)(using
        tb: AsLateralTable[B, CL + 1],
        j: Join[ta.R, tb.R],
        c: CombineKindTuple[ta.OKS, tb.OKS]
    ): JoinPart[j.R, c.R, CL + 1] =
        given QueryContext[CL + 1] = QueryContext()
        val (leftTable, leftIrTable) = ta.asTable(a)
        val (rightTable, rightIrTable) = tb.asTable(f(leftTable))
        val params = j.join(leftTable, rightTable)
        JoinPart(params, IrTable.Join(leftIrTable, SqlJoinType.Left, rightIrTable, None))

    /**
     * Right outer join. Must be followed by `on`. The left side
     * columns are wrapped with `Option`.
     *
     * {{{
     * from(Channel.rightJoin(Post).on((c, p) => c.id === p.channelId))
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
     * from(Channel.fullJoin(Post).on((c, p) => c.id === p.channelId))
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
        Expr(IrExpr.Cast(a.asExpr(x).expr, c.castType))

extension [CL <: Int](n: Int)(using QueryContext[CL])
    /**
     * Creates an interval of years. Maps to `INTERVAL 'n' YEAR`.
     *
     * {{{
     * from(Post).map(p => p.createTime + 1.year)
     * }}}
     */
    def year: Interval =
        Interval(n.toString, SqlTimeUnit.Year)

    /**
     * Creates an interval of months. Maps to `INTERVAL 'n' MONTH`.
     *
     * {{{
     * from(Post).map(p => p.createTime + 2.month)
     * }}}
     */
    def month: Interval =
        Interval(n.toString, SqlTimeUnit.Month)

    /**
     * Creates an interval of days. Maps to `INTERVAL 'n' DAY`.
     *
     * {{{
     * from(Post).map(p => p.createTime + 3.day)
     * }}}
     */
    def day: Interval =
        Interval(n.toString, SqlTimeUnit.Day)

    /**
     * Creates an interval of hours. Maps to `INTERVAL 'n' HOUR`.
     *
     * {{{
     * from(Post).map(p => p.createTime + 4.hour)
     * }}}
     */
    def hour: Interval =
        Interval(n.toString, SqlTimeUnit.Hour)

    /**
     * Creates an interval of minutes. Maps to `INTERVAL 'n' MINUTE`.
     *
     * {{{
     * from(Post).map(p => p.createTime + 5.minute)
     * }}}
     */
    def minute: Interval =
        Interval(n.toString, SqlTimeUnit.Minute)

    /**
     * Creates an interval of seconds. Maps to `INTERVAL 'n' SECOND`.
     *
     * {{{
     * from(Post).map(p => p.createTime + 6.second)
     * }}}
     */
    def second: Interval =
        Interval(n.toString, SqlTimeUnit.Second)

// TODO 移动到类型操作里 fallback
/**
 * Constrains the types that can be used with `year`, `month`, etc.
 * extract functions.
 */
trait ExtractArg[T]

object ExtractArg:
    given dateTime[T: SqlDateTime]: ExtractArg[T]()

    given time[T: SqlTime]: ExtractArg[T]()

    given interval[T: SqlInterval]: ExtractArg[T]()

extension [T, CL <: Int](x: T)(using qc: QueryContext[CL], a: AsExpr[T, CL], kt: KindToTuple[a.K], e: ExtractArg[a.R])
    /**
     * Extracts the year from a datetime expression. Maps to
     * `EXTRACT(YEAR FROM expr)`.
     *
     * {{{
     * from(Post).map(p => p.createTime.year)
     * }}}
     */
    def year: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            IrExpr.ExtractFunc(
                SqlTimeUnit.Year,
                a.asExpr(x).expr
            )
        )

    /**
     * Extracts the month from a datetime expression. Maps to
     * `EXTRACT(MONTH FROM expr)`.
     *
     * {{{
     * from(Post).map(p => p.createTime.month)
     * }}}
     */
    def month: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            IrExpr.ExtractFunc(
                SqlTimeUnit.Month,
                a.asExpr(x).expr
            )
        )

    /**
     * Extracts the day from a datetime expression. Maps to
     * `EXTRACT(DAY FROM expr)`.
     *
     * {{{
     * from(Post).map(p => p.createTime.day)
     * }}}
     */
    def day: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            IrExpr.ExtractFunc(
                SqlTimeUnit.Day,
                a.asExpr(x).expr
            )
        )

    /**
     * Extracts the hour from a datetime expression. Maps to
     * `EXTRACT(HOUR FROM expr)`.
     *
     * {{{
     * from(Post).map(p => p.createTime.hour)
     * }}}
     */
    def hour: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            IrExpr.ExtractFunc(
                SqlTimeUnit.Hour,
                a.asExpr(x).expr
            )
        )

    /**
     * Extracts the minute from a datetime expression. Maps to
     * `EXTRACT(MINUTE FROM expr)`.
     *
     * {{{
     * from(Post).map(p => p.createTime.minute)
     * }}}
     */
    def minute: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            IrExpr.ExtractFunc(
                SqlTimeUnit.Minute,
                a.asExpr(x).expr
            )
        )

    /**
     * Extracts the second from a datetime expression. Maps to
     * `EXTRACT(SECOND FROM expr)`.
     *
     * {{{
     * from(Post).map(p => p.createTime.second)
     * }}}
     */
    def second: Expr[Option[BigDecimal], Composite[kt.R]] =
        Expr(
            IrExpr.ExtractFunc(
                SqlTimeUnit.Second,
                a.asExpr(x).expr
            )
        )

/**
 * An intermediate builder for `CASE WHEN` chains. Use `caseWhen`
 * to start, add branches with `when`, and finish with `otherwise`.
 */
final case class CaseWhen[T, KS <: Tuple](private[sqala] val exprs: NonEmptyList[Expr[?, ?]]):
    /**
     * Adds a `WHEN condition THEN result` branch.
     *
     * {{{
     * from(Post).map(p => caseWhen(p.viewCount > 100)(p.title).when(p.likeCount > 10)("Hot").otherwise("N/A"))
     * }}}
     */
    def when[C, R, CL <: Int](cond: C)(result: R)(using
        qc: QueryContext[CL],
        ac: AsExpr[C, CL],
        ar: AsExpr[R, CL],
        b: ValidateArgType[SqlBoolean, ac.R, "boolean"],
        r: Return[T, ar.R],
        ktc: KindToTuple[ac.K],
        ktr: KindToTuple[ar.K],
        cc: CombineKindTuple[KS, ktc.R],
        c: CombineKindTuple[cc.R, ktr.R]
    ): CaseWhen[r.R, c.R] =
        CaseWhen(exprs :+ ac.asExpr(cond) :+ ar.asExpr(result))

    /**
     * Ends the `CASE WHEN` chain with an `ELSE` default.
     *
     * {{{
     * from(Post).map(p => caseWhen(p.viewCount > 100)(p.title).otherwise("N/A"))
     * }}}
     */
    def otherwise[R, CL <: Int](result: R)(using
        qc: QueryContext[CL],
        a: AsExpr[R, CL],
        r: Return[T, a.R],
        kt: KindToTuple[a.K],
        c: CombineKindTuple[KS, kt.R]
    ): Expr[r.R, Composite[c.R]] =
        val caseBranches =
            exprs
                .grouped(2)
                .map(i => (i.toList(0), i.toList(1)))
                .map((w, t) => IrCaseBranch(w.expr, t.expr))
        Expr(
            IrExpr.Case(
                caseBranches,
                Some(a.asExpr(result).expr)
            )
        )

/**
 * Starts a `CASE WHEN` chain. Maps to `CASE WHEN cond THEN result`.
 *
 * {{{
 * from(Post).map(p => caseWhen(p.viewCount > 100)(p.title).otherwise("N/A"))
 * }}}
 */
def caseWhen[C, R, CL <: Int](cond: C)(result: R)(using
    qc: QueryContext[CL],
    ac: AsExpr[C, CL],
    ar: AsExpr[R, CL],
    b: ValidateArgType[SqlBoolean, ac.R, "boolean"],
    as: ValidateArgType[AsSqlExpr, ar.R, "scalar"],
    ktc: KindToTuple[ac.K],
    ktr: KindToTuple[ar.K],
    c: CombineKindTuple[ktc.R, ktr.R]
): CaseWhen[ar.R, c.R] =
    CaseWhen(NonEmptyList(ac.asExpr(cond), ar.asExpr(result) :: Nil))

/**
 * Returns the first non-null value from two expressions.
 * Maps to `COALESCE(expr1, expr2)`. Both arguments must be
 * compatible types.
 *
 * {{{
 * from(Post).map(p => coalesce(p.title, "Untitled"))
 * }}}
 */
def coalesce[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    asb: ValidateArgType[AsSqlExpr, ab.R, "scalar"],
    r: CoalesceReturn[aa.R, ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[r.R, c.R] =
    Expr(
        IrExpr.Coalesce(
            NonEmptyList(aa.asExpr(x).expr, ab.asExpr(y).expr :: Nil)
        )
    )

/**
 * Returns null if two expressions are equal, otherwise returns the
 * first expression. Maps to `NULLIF(expr1, expr2)`. Both arguments
 * must be compatible types.
 *
 * {{{
 * from(Post).map(p => nullIf(p.title, ""))
 * }}}
 */
def nullIf[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    asb: ValidateArgType[AsSqlExpr, ab.R, "scalar"],
    r: Return[aa.R, ab.R],
    c: CombineKind[aa.K, ab.K],
    to: ToOption[Expr[r.R, c.R]]
): to.R =
    to.toOption(
        Expr(
            IrExpr.NullIf(
                aa.asExpr(x).expr, ab.asExpr(y).expr
            )
        )
    )

extension [T, CL <: Int](x: T)(using qc: QueryContext[CL])
    // TODO 注释
    def filter[F](f: F)(using
        a: AsExpr[F, CL],
        ba: ValidateArgType[SqlBoolean, a.R, "boolean"],
        kt: KindToTuple[a.K],
        ci: CanInSimpleClause[kt.R, "filter"],
        c: CallAggFilter[T, kt.R]
    ): c.R =
        val cond = a.asExpr(f)
        c.call(x, cond.expr)

/**
 * Returns 1 if the column is not part of the current grouping set
 * (aggregated), 0 if it is. Maps to `GROUPING(expr)`. Used after
 * `groupBy` clause.
 *
 * {{{
 * from(Post)
 *     .groupBy(p => (author = p.authorId, channel = p.channelId))
 *     .map((g, p) => (author = g.author, count = count(), groupingAuthorId = grouping(g.author)))
 * }}}
 */
def grouping[T, K <: ExprKind, CL <: Int](x: Expr[T, K])(using
    qc: QueryContext[CL],
    gc: GroupingContext,
    as: ValidateArgType[AsSqlExpr, T, "scalar"],
    kt: KindToTuple[K],
    ig: IsGrouped[kt.R]
): Expr[Int, Agg[K *: EmptyTuple]] =
    Expr(
        IrExpr.Grouping(
            NonEmptyList(x.expr, Nil)
        )
    )

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
     * Applies a window specification with `partitionBy` and `sortBy`
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
                o.partitionBy.map(_.expr),
                o.sortBy.map(_.ordering),
                o.frame
            )
        )

/**
 * References the first row in a window frame. Maps to
 * `UNBOUNDED PRECEDING`. Used with `rows`, `range`, or `groups`.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount).over(
 *     partitionBy(p.authorId).sortBy(p.id.desc).rows(unboundedPreceding)
 * ))
 * }}}
 */
def unboundedPreceding[CL <: Int](using QueryContext[CL], OverContext): FrameBound[Nothing, 1, "unbounded preceding"] =
    FrameBound(IrWindowFrameBound.UnboundedPreceding)

/**
 * References the current row in a window frame. Maps to
 * `CURRENT ROW`. Used with `rows`, `range`, or `groups`.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount).over(
 *     partitionBy(p.authorId).sortBy(p.id.desc).rows(currentRow)
 * ))
 * }}}
 */
def currentRow[CL <: Int](using QueryContext[CL], OverContext): FrameBound[Nothing, 3, "current row"] =
    FrameBound(IrWindowFrameBound.CurrentRow)

/**
 * References the last row in a window frame. Maps to
 * `UNBOUNDED FOLLOWING`. Used with `rows`, `range`, or `groups`.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount).over(
 *     partitionBy(p.authorId).sortBy(p.id.desc).rows(unboundedFollowing)
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
     *     partitionBy(p.authorId).sortBy(p.id.desc).rows(1.preceding)
     * ))
     * }}}
     */
    def preceding: FrameBound[a.R, 2, "preceding row"] =
        FrameBound(IrWindowFrameBound.Preceding(n.asExpr.expr))

    /**
     * References N rows after the current row in a window frame.
     * Maps to `n FOLLOWING`. Used with `rows`, `range`, or `groups`.
     *
     * {{{
     * from(Post).map(p => sum(p.likeCount).over(
     *     partitionBy(p.authorId).sortBy(p.id.desc).rows(1.following)
     * ))
     * }}}
     */
    def following: FrameBound[a.R, 4, "following row"] =
        FrameBound(IrWindowFrameBound.Following(n.asExpr.expr))

/**
 * Tests whether a condition holds for any row in a subquery.
 * Maps to `ANY (subquery)`. Typically used in `filter` with
 * comparison operators.
 *
 * {{{
 * from(Post).filter(p => p.id === any(from(Comment).map(c => c.postId)))
 * }}}
 */
def any[T, OKS <: Tuple, L <: Int, S <: QuerySize, CL <: Int](query: Query[T, OKS, L, S])(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL]
): QuantifiedSubquery[a.R, OKS, L] =
    QuantifiedSubquery(SqlSubqueryQuantifier.Any, query.tree)

/**
 * Tests whether a condition holds for all rows in a subquery.
 * Maps to `ALL (subquery)`. Typically used in `filter` with
 * comparison operators.
 *
 * {{{
 * from(Post).filter(p => p.id === all(from(Comment).map(c => c.postId)))
 * }}}
 */
def all[T, OKS <: Tuple, L <: Int, S <: QuerySize, CL <: Int](query: Query[T, OKS, L, S])(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL]
): QuantifiedSubquery[a.R, OKS, L] =
    QuantifiedSubquery(SqlSubqueryQuantifier.All, query.tree)

/**
 * Tests whether a subquery returns at least one row. Maps to
 * `EXISTS (subquery)`. Typically used in `filter`.
 *
 * {{{
 * from(User).filter(u => exists(from(User).filter(uu => uu.name === u.name && uu.id <> u.id)))
 * }}}
 */
def exists[T, OKS <: Tuple, L <: Int, S <: QuerySize, CL <: Int](query: Query[T, OKS, L, S])(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    vss: ValidateSubqueryScope[L, CL]
): Expr[Boolean, Composite[OKS]] =
    Expr(
        IrExpr.ExistsPredicate(
            query.tree
        )
    )

/**
 * Creates a `CUBE` clause. Maps to `CUBE(exprs)`.
 *
 * {{{
 * from(Post).groupBySets(p => (id = p.id, title = p.title))(g => cube(g.id, g.title)).map((g, p) => count())
 * }}}
 */
def cube[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    kt: KindToTuple[a.K],
    ig: IsGrouped[kt.R]
): Cube =
    Cube(a.asExprs(x).map(_.expr))

/**
 * Creates a `ROLLUP` clause. Maps to `ROLLUP(exprs)`.
 *
 * {{{
 * from(Post).groupBySets(p => (id = p.id, title = p.title))(g => rollup(g.id, g.title)).map((g, p) => count())
 * }}}
 */
def rollup[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    kt: KindToTuple[a.K],
    ig: IsGrouped[kt.R]
): Rollup =
    Rollup(a.asExprs(x).map(_.expr))

/**
 * Creates a `GROUPING SETS` clause. Maps to `GROUPING SETS(exprs)`.
 *
 * {{{
 * from(Post).groupBySets(p => (id = p.id, title = p.title))(g => groupingSets(g.id, g.title, ())).map((g, p) => count())
 * }}}
 */
def groupingSets[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsMultidimensionalGrouping[T]
): GroupingSets =
    GroupingSets(a.asIrGroupingItems(x))

/**
 * Creates a raw expression from a string interpolation, supporting
 * values, expressions, and subqueries as arguments. Values are
 * automatically escaped. Must be followed by `as`.
 *
 * {{{
 * from(Post).filter(p => rawExpr"MATCH(${p.title}) AGAINST(${word})".as[Boolean])
 * }}}
 */
extension (s: StringContext)
    inline def rawExpr[CL <: Int](inline args: Any*)(using QueryContext[CL]): RawExpr[CL] =
        val sqlExprs = RawMacro.asIrExprs[CL](args)
        RawExpr(s.parts.toList.map(_.trim), sqlExprs)

/**
 * Creates a custom function call expression. Maps to `name(args...)`.
 * Used to wrap non-standard functions that aren't built into sqala.
 *
 * {{{
 * def left(x: Expr[String, ?], n: Int) =
 *     createFunc[Option[String]]("LEFT", x :: n.asExpr :: Nil)
 *
 * from(Post).map(p => left(p.title, 10))
 * }}}
 */
def createFunc[T, CL <: Int](name: String, args: List[Expr[?, ?]])(using QueryContext[CL]): Expr[T, Composite[EmptyTuple]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            name,
            args.map(_.expr),
            Nil,
            Nil,
            None
        )
    )

/**
 * Creates a custom binary expression with a raw operator string.
 * For non-standard operators that aren't built into sqala.
 *
 * {{{
 * extension (x: Expr[Json, ?])
 *     def ->>(k: String) =
 *         createBinaryExpr[Boolean](x, "->>", k.asExpr)
 *
 * from(Post).map(p => p.data ->> "key")
 * }}}
 */
def createBinaryExpr[T, CL <: Int](left: Expr[?, ?], operator: String, right: Expr[?, ?])(using QueryContext[CL]): Expr[T, Composite[EmptyTuple]] =
    Expr(
        IrExpr.Binary(
            left.expr,
            SqlBinaryOperator.UnsafeCustom(SqlUnsafeCustomToken.Keyword(operator) :: Nil),
            right.expr
        )
    )