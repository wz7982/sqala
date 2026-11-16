package sqala.dynamic.dsl

import sqala.ast.expr.*
import sqala.ast.statement.SqlQuery
import sqala.ast.window.SqlWindowFrameBound
import sqala.metadata.{AsSqlExpr, Interval}
import sqala.util.NonEmptyList

/**
 * References a column qualified by its table name.
 *
 * Maps to `"table"."column"`.
 *
 * {{{
 * column("user", "id")
 * }}}
 */
def column(tableName: String, columnName: String): Expr =
    Expr(SqlExpr.Column(Some(tableName), columnName))

/**
 * References an unqualified column by name.
 *
 * Maps to `"column"`.
 *
 * {{{
 * column("id")
 * }}}
 */
def column(columnName: String): Expr =
    Expr(SqlExpr.Column(None, columnName))

/**
 * Lifts a Scala value into an expression through the `AsSqlExpr` type
 * class. Literal values are escaped automatically when the SQL is
 * generated.
 *
 * {{{
 * value(1)
 * value("Alice")
 * }}}
 */
def value[V: AsSqlExpr as a](v: V): Expr =
    Expr(a.asSqlExpr(v))

/**
 * Embeds a query as a scalar subquery expression.
 *
 * Maps to `(query)`.
 *
 * {{{
 * subquery(select(value(1).as("one")))
 * }}}
 */
def subquery(query: Query): Expr =
    Expr(SqlExpr.Subquery(query.tree))

/**
 * Creates an `ANY (subquery)` quantified subquery, usable on the
 * right-hand side of a comparison operator such as `===` or `>`.
 *
 * {{{
 * column("id") === any(select(column("user_id").as("id")))
 * }}}
 */
def any(query: Query): QuantifiedSubquery =
    QuantifiedSubquery(SqlSubqueryQuantifier.Any, query.tree)

/**
 * Creates an `ALL (subquery)` quantified subquery, usable on the
 * right-hand side of a comparison operator such as `===` or `>`.
 *
 * {{{
 * column("id") === all(select(column("user_id").as("id")))
 * }}}
 */
def all(query: Query): QuantifiedSubquery =
    QuantifiedSubquery(SqlSubqueryQuantifier.All, query.tree)

/**
 * Tests whether a subquery returns at least one row.
 *
 * Maps to `EXISTS (subquery)`.
 *
 * {{{
 * exists(select(column("id").as("id")))
 * }}}
 */
def exists(query: Query): Expr =
    Expr(SqlExpr.ExistsPredicate(query.tree))

/**
 * References a table by name.
 *
 * {{{
 * table("user").as("u")
 * }}}
 */
def table(name: String): Table =
    Table(name, None)

/**
 * Wraps a query as a derived (subquery) table source.
 *
 * {{{
 * table(select(column("id").as("id"))).as("sub")
 * }}}
 */
def table(query: Query): SubqueryTable =
    SubqueryTable(query.tree, None, false)

/**
 * Starts a `SELECT` query with the given select items. At least one
 * item is required.
 *
 * {{{
 * select(column("id").as("id"), value("Alice").as("name"))
 * }}}
 */
def select(items: SelectItem*): SelectQuery =
    require(items.nonEmpty, "Select requires at least one column")
    val selectItems = items.toList.map(_.item)
    SelectQuery(SqlQuery.Select(None, NonEmptyList(selectItems.head, selectItems.tail), Nil, None, None, None, Nil, None, Nil, None, None))

/**
 * List-based overload of `select`.
 *
 * {{{
 * select(List(column("id").as("id"), value("Alice").as("name")))
 * }}}
 */
def select(items: List[SelectItem]): SelectQuery =
    select(items*)

/**
 * Intermediate builder for a searched `CASE WHEN` chain. Create it with
 * `caseWhen` and finish it with `otherwise`.
 */
final class CaseWhen(private[sqala] val branches: NonEmptyList[(Expr, Expr)]):
    /**
     * Ends the `CASE WHEN` chain with an `ELSE` result.
     *
     * Maps to `CASE WHEN ... THEN ... ELSE expr END`.
     *
     * {{{
     * caseWhen(column("score") > value(100))(value("hot")).otherwise(value("normal"))
     * }}}
     */
    def otherwise(result: Expr): Expr =
        Expr(
            SqlExpr.Case(
                branches.map((w, t) => SqlCaseBranch(w.asSqlExpr, t.asSqlExpr)),
                Some(result.asSqlExpr)
            )
        )

/**
 * Starts a searched `CASE WHEN` chain from `(condition, result)` pairs.
 * At least one pair is required; finish the chain with `otherwise`.
 *
 * {{{
 * caseWhen(
 *     column("score") > value(100) -> value("hot"),
 *     column("score") > value(50) -> value("warm")
 * ).otherwise(value("cold"))
 * }}}
 */
def caseWhen(branches: (Expr, Expr)*): CaseWhen =
    require(branches.nonEmpty, "CaseWhen requires at least one (condition, result) pair.")
    CaseWhen(NonEmptyList(branches.head, branches.tail.toList))

/**
 * List-based overload of `caseWhen`.
 *
 * {{{
 * caseWhen(List(column("score") > value(100) -> value("hot"))).otherwise(value("cold"))
 * }}}
 */
def caseWhen(branches: List[(Expr, Expr)]): CaseWhen =
    caseWhen(branches*)

extension (n: Int)
    /**
     * Creates an interval of `n` years.
     *
     * Maps to `INTERVAL 'n' YEAR`.
     *
     * {{{
     * column("create_time") + 1.year
     * }}}
     */
    def year: Interval =
        Interval(n.toString, SqlTimeUnit.Year)

     /**
     * Creates an interval of `n` months.
     *
     * Maps to `INTERVAL 'n' MONTH`.
     *
     * {{{
     * column("create_time") + 2.month
     * }}}
     */
    def month: Interval =
        Interval(n.toString, SqlTimeUnit.Month)

    /**
     * Creates an interval of `n` days.
     *
     * Maps to `INTERVAL 'n' DAY`.
     *
     * {{{
     * column("create_time") + 3.day
     * }}}
     */
    def day: Interval =
        Interval(n.toString, SqlTimeUnit.Day)

    /**
     * Creates an interval of `n` hours.
     *
     * Maps to `INTERVAL 'n' HOUR`.
     *
     * {{{
     * column("create_time") + 4.hour
     * }}}
     */
    def hour: Interval =
        Interval(n.toString, SqlTimeUnit.Hour)

    /**
     * Creates an interval of `n` minutes.
     *
     * Maps to `INTERVAL 'n' MINUTE`.
     *
     * {{{
     * column("create_time") + 5.minute
     * }}}
     */
    def minute: Interval =
        Interval(n.toString, SqlTimeUnit.Minute)

    /**
     * Creates an interval of `n` seconds.
     *
     * Maps to `INTERVAL 'n' SECOND`.
     *
     * {{{
     * column("create_time") + 6.second
     * }}}
     */
    def second: Interval =
        Interval(n.toString, SqlTimeUnit.Second)

/**
 * Returns the first non-null operand.
 *
 * Maps to `COALESCE(x, y)`.
 *
 * {{{
 * coalesce(column("title"), value("Untitled"))
 * }}}
 */
def coalesce(x: Expr, y: Expr): Expr =
    Expr(SqlExpr.Coalesce(NonEmptyList(x.asSqlExpr, y.asSqlExpr :: Nil)))

/**
 * Alias of `coalesce`, provided for users familiar with `IFNULL`.
 *
 * Maps to `COALESCE(x, y)`.
 *
 * {{{
 * ifNull(column("title"), value("Untitled"))
 * }}}
 */
def ifNull(x: Expr, y: Expr): Expr =
    coalesce(x, y)

/**
 * Returns `null` when both operands are equal, otherwise returns the
 * first operand.
 *
 * Maps to `NULLIF(x, y)`.
 *
 * {{{
 * nullIf(column("title"), value(""))
 * }}}
 */
def nullIf(x: Expr, y: Expr): Expr =
    Expr(SqlExpr.NullIf(x.asSqlExpr, y.asSqlExpr))

/**
 * Returns `1` when a column is aggregated (that is, not part of the
 * current grouping set) and `0` otherwise.
 *
 * Maps to `GROUPING(expr)`.
 *
 * {{{
 * grouping(column("author"))
 * }}}
 */
def grouping(x: Expr): Expr =
    Expr(SqlExpr.Grouping(NonEmptyList(x.asSqlExpr, Nil)))

/**
 * References the current row of a window frame. Used with `rows`,
 * `range`, or `groups`.
 *
 * Maps to `CURRENT ROW`.
 *
 * {{{
 * partitionBy(column("author")).rows(currentRow)
 * }}}
 */
def currentRow: FrameBound =
    FrameBound(SqlWindowFrameBound.CurrentRow)

/**
 * References the first row of a window frame. Used with `rows`,
 * `range`, or `groups`.
 *
 * Maps to `UNBOUNDED PRECEDING`.
 *
 * {{{
 * partitionBy(column("author")).rows(unboundedPreceding)
 * }}}
 */
def unboundedPreceding: FrameBound =
    FrameBound(SqlWindowFrameBound.UnboundedPreceding)

/**
 * References the last row of a window frame. Used with `rows`, `range`,
 * or `groups`.
 *
 * Maps to `UNBOUNDED FOLLOWING`.
 *
 * {{{
 * partitionBy(column("author")).rowsBetween(unboundedPreceding, unboundedFollowing)
 * }}}
 */
def unboundedFollowing: FrameBound =
    FrameBound(SqlWindowFrameBound.UnboundedFollowing)

/**
 * Starts a window `PARTITION BY` specification.
 *
 * Maps to `PARTITION BY expr [, ...]`.
 *
 * {{{
 * column("score").over(partitionBy(column("author")))
 * }}}
 */
def partitionBy(exprs: Expr*): Over =
    Over(exprs.toList.map(_.asSqlExpr), Nil, None)

/**
 * List-based overload of `partitionBy`.
 *
 * {{{
 * column("score").over(partitionBy(List(column("author"))))
 * }}}
 */
def partitionBy(exprs: List[Expr]): Over =
    partitionBy(exprs*)

/**
 * Starts a window `ORDER BY` specification.
 *
 * Maps to `ORDER BY ordering [, ...]`.
 *
 * {{{
 * column("score").over(orderBy(column("id").desc))
 * }}}
 */
def orderBy(orders: Order*): Over =
    Over(Nil, orders.toList.map(_.order), None)

/**
 * List-based overload of `orderBy`.
 *
 * {{{
 * column("score").over(orderBy(List(column("id").desc)))
 * }}}
 */
def orderBy(orders: List[Order]): Over =
    orderBy(orders*)