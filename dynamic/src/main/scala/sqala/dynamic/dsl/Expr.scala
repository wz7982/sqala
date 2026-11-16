package sqala.dynamic.dsl

import sqala.ast.expr.*
import sqala.ast.order.{SqlNullsOrdering, SqlOrdering, SqlOrderingItem}
import sqala.ast.statement.SqlSelectItem
import sqala.ast.window.{SqlWindow, SqlWindowFrameBound}
import sqala.util.NonEmptyList

import scala.annotation.targetName

/**
 * A dynamic SQL expression wrapper.
 *
 * Unlike the static DSL, the dynamic DSL performs no type checking:
 * expressions are built at runtime and can combine any operands. Results
 * are always of type `Expr`, and validation is left to the database.
 *
 * Instances are typically created with the helpers in `Dsl`, such as
 * `column`, `value`, or `caseWhen`.
 */
final class Expr(private[sqala] val expr: SqlExpr):
    /**
     * Returns the underlying AST node, after applying safe
     * simplifications (see `asSqlExpr`).
     *
     * {{{
     * column("id").ast
     * }}}
     */
    def ast: SqlExpr =
        asSqlExpr

    /**
     * Equality comparison. Maps to `expr = expr`.
     *
     * {{{
     * column("id") === value(1)
     * }}}
     */
    @targetName("eq")
    def ===(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Equal, expr.asSqlExpr))

    /**
     * Equality comparison against a quantified subquery. Maps to
     * `expr = [ANY|ALL] (query)`.
     *
     * {{{
     * column("id") === any(select(column("user_id").as("id")))
     * }}}
     */
    @targetName("eq")
    def ===(query: QuantifiedSubquery): Expr =
        Expr(
            SqlExpr.QuantifiedComparisonPredicate(
                asSqlExpr,
                SqlQuantifiedComparisonOperator.Equal,
                query.quantifier,
                query.query
            )
        )

    /**
     * Inequality comparison. Maps to `expr <> expr`.
     *
     * {{{
     * column("id") <> value(1)
     * }}}
     */
    @targetName("ne")
    def <>(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.NotEqual, expr.asSqlExpr))

    /**
     * Inequality comparison against a quantified subquery. Maps to
     * `expr <> [ANY|ALL] (query)`.
     *
     * {{{
     * column("id") <> all(select(column("user_id").as("id")))
     * }}}
     */
    @targetName("ne")
    def <>(query: QuantifiedSubquery): Expr =
        Expr(
            SqlExpr.QuantifiedComparisonPredicate(
                asSqlExpr,
                SqlQuantifiedComparisonOperator.NotEqual,
                query.quantifier,
                query.query
            )
        )

    /**
     * Null-safe equality comparison. Maps to
     * `expr IS DISTINCT FROM expr`.
     *
     * Returns `true` when both operands differ, including when one of
     * them is `NULL`.
     *
     * {{{
     * column("email") <=> value(null)
     * }}}
     */
    @targetName("eqIgnoreNulls")
    def <=>(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.IsDistinctFrom(true), expr.asSqlExpr))

    /**
     * Tests whether the expression is `NULL`. Maps to `expr IS NULL`.
     *
     * {{{
     * column("deleted_at").isNull
     * }}}
     */
    def isNull: Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Is(false), SqlExpr.NullLiteral))

    /**
     * Greater-than comparison. Maps to `expr > expr`.
     *
     * {{{
     * column("age") > value(18)
     * }}}
     */
    @targetName("gt")
    def >(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.GreaterThan, expr.asSqlExpr))

    /**
     * Greater-than comparison against a quantified subquery. Maps to
     * `expr > [ANY|ALL] (query)`.
     *
     * {{{
     * column("age") > any(select(column("min_age").as("min_age")))
     * }}}
     */
    @targetName("gt")
    def >(query: QuantifiedSubquery): Expr =
        Expr(
            SqlExpr.QuantifiedComparisonPredicate(
                asSqlExpr,
                SqlQuantifiedComparisonOperator.GreaterThan,
                query.quantifier,
                query.query
            )
        )

    /**
     * Greater-than-or-equal comparison. Maps to `expr >= expr`.
     *
     * {{{
     * column("age") >= value(18)
     * }}}
     */
    @targetName("ge")
    def >=(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.GreaterThanEqual, expr.asSqlExpr))

    /**
     * Greater-than-or-equal comparison against a quantified subquery.
     * Maps to `expr >= [ANY|ALL] (query)`.
     *
     * {{{
     * column("age") >= all(select(column("min_age").as("min_age")))
     * }}}
     */
    @targetName("ge")
    def >=(query: QuantifiedSubquery): Expr =
        Expr(
            SqlExpr.QuantifiedComparisonPredicate(
                asSqlExpr,
                SqlQuantifiedComparisonOperator.GreaterThanEqual,
                query.quantifier,
                query.query
            )
        )

    /**
     * Less-than comparison. Maps to `expr < expr`.
     *
     * {{{
     * column("age") < value(18)
     * }}}
     */
    @targetName("lt")
    def <(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.LessThan, expr.asSqlExpr))

    /**
     * Less-than comparison against a quantified subquery. Maps to
     * `expr < [ANY|ALL] (query)`.
     *
     * {{{
     * column("age") < any(select(column("max_age").as("max_age")))
     * }}}
     */
    @targetName("lt")
    def <(query: QuantifiedSubquery): Expr =
        Expr(
            SqlExpr.QuantifiedComparisonPredicate(
                asSqlExpr,
                SqlQuantifiedComparisonOperator.LessThan,
                query.quantifier,
                query.query
            )
        )

    /**
     * Less-than-or-equal comparison. Maps to `expr <= expr`.
     *
     * {{{
     * column("age") <= value(18)
     * }}}
     */
    @targetName("le")
    def <=(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.LessThanEqual, expr.asSqlExpr))

    /**
     * Less-than-or-equal comparison against a quantified subquery. Maps
     * to `expr <= [ANY|ALL] (query)`.
     *
     * {{{
     * column("age") <= all(select(column("max_age").as("max_age")))
     * }}}
     */
    @targetName("le")
    def <=(query: QuantifiedSubquery): Expr =
        Expr(
            SqlExpr.QuantifiedComparisonPredicate(
                asSqlExpr,
                SqlQuantifiedComparisonOperator.LessThanEqual,
                query.quantifier,
                query.query
            )
        )

    /**
     * Tests membership in a list of values. Maps to
     * `expr IN (expr [, ...])`.
     *
     * An empty list produces a always-false condition (`1 = 0`).
     *
     * {{{
     * column("status").in(value("active"), value("pending"))
     * }}}
     */
    def in(items: Expr*): Expr =
        items.toList.map(_.asSqlExpr) match
            case head :: tail =>
                Expr(
                    SqlExpr.In(
                        asSqlExpr,
                        SqlInRightOperand.Values(NonEmptyList(head, tail)),
                        false
                    )
                )
            case Nil =>
                Expr(
                    SqlExpr.Binary(
                        SqlExpr.NumberLiteral(1),
                        SqlBinaryOperator.Equal,
                        SqlExpr.NumberLiteral(0)
                    )
                )

    /**
     * List-based overload of `in`.
     *
     * {{{
     * column("status").in(List(value("active"), value("pending")))
     * }}}
     */
    def in(items: List[Expr]): Expr =
        in(items*)

    /**
     * Tests membership in the result of a subquery. Maps to
     * `expr IN (query)`.
     *
     * {{{
     * column("id").in(select(column("user_id").as("user_id")))
     * }}}
     */
    def in(query: Query): Expr =
        Expr(
            SqlExpr.In(asSqlExpr, SqlInRightOperand.Subquery(query.tree), false)
        )

    /**
     * Tests whether the expression falls in a closed range. Maps to
     * `expr BETWEEN start AND end`.
     *
     * {{{
     * column("age").between(value(18), value(30))
     * }}}
     */
    def between(start: Expr, end: Expr): Expr =
        Expr(SqlExpr.Between(asSqlExpr, start.asSqlExpr, end.asSqlExpr, false))

    /**
     * Addition. Maps to `expr + expr`.
     *
     * {{{
     * column("price") + column("tax")
     * }}}
     */
    @targetName("plus")
    def +(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Plus, expr.asSqlExpr))

    /**
     * Subtraction. Maps to `expr - expr`.
     *
     * {{{
     * column("price") - column("discount")
     * }}}
     */
    @targetName("minus")
    def -(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Minus, expr.asSqlExpr))

    /**
     * Multiplication. Maps to `expr * expr`.
     *
     * {{{
     * column("price") * value(2)
     * }}}
     */
    @targetName("times")
    def *(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Times, expr.asSqlExpr))

    /**
     * Division. Maps to `expr / expr`.
     *
     * {{{
     * column("price") / value(2)
     * }}}
     */
    @targetName("div")
    def /(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Div, expr.asSqlExpr))

    /**
     * Unary plus. Maps to `+(expr)`.
     *
     * {{{
     * +column("price")
     * }}}
     */
    @targetName("positive")
    def unary_+ : Expr =
        Expr(SqlExpr.Unary(SqlUnaryOperator.Positive, asSqlExpr))

    /**
     * Unary negation. Maps to `-(expr)`.
     *
     * {{{
     * -column("price")
     * }}}
     */
    @targetName("negative")
    def unary_- : Expr =
        Expr(SqlExpr.Unary(SqlUnaryOperator.Negative, asSqlExpr))

    /**
     * Logical conjunction. Maps to `expr AND expr`.
     *
     * {{{
     * (column("age") > value(18)) && (column("active") === value(true))
     * }}}
     */
    @targetName("and")
    def &&(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.And, expr.asSqlExpr))

    /**
     * Logical disjunction. Maps to `expr OR expr`.
     *
     * {{{
     * (column("age") > value(60)) || (column("age") < value(18))
     * }}}
     */
    @targetName("or")
    def ||(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Or, expr.asSqlExpr))

    /**
     * Logical negation. Maps to `NOT expr`. Predicate negation is
     * simplified where possible, e.g. `NOT (a = b)` is left as is but
     * `NOT (a IN (...))` becomes `a NOT IN (...)`.
     *
     * {{{
     * !(column("deleted") === value(true))
     * }}}
     */
    @targetName("not")
    def unary_! : Expr =
        Expr(SqlExpr.Unary(SqlUnaryOperator.Not, asSqlExpr))

    /**
     * Marks the expression as a window function with an empty `OVER`
     * clause. Maps to `expr OVER ()`.
     *
     * {{{
     * count().over()
     * }}}
     */
    def over(): Expr =
        Expr(SqlExpr.Window(asSqlExpr, SqlWindow.Inlined(None, Nil, Nil, None)))

    /**
     * Marks the expression as a window function with the given `OVER`
     * specification. Maps to
     * `expr OVER (PARTITION BY ... ORDER BY ... frame)`.
     *
     * {{{
     * sum(column("score")).over(
     *     partitionBy(column("author")).orderBy(column("id").desc)
     * )
     * }}}
     */
    def over(over: Over): Expr =
        Expr(SqlExpr.Window(asSqlExpr, SqlWindow.Inlined(None, over.partitionBy, over.orderBy, over.frame)))

    /**
     * Orders the expression in ascending order. Maps to `expr ASC`.
     *
     * {{{
     * column("id").asc
     * }}}
     */
    def asc: Order =
        Order(SqlOrderingItem(asSqlExpr, Some(SqlOrdering.Asc), None))

    /**
     * Orders the expression in ascending order with `NULLS FIRST`.
     * Maps to `expr ASC NULLS FIRST`.
     *
     * {{{
     * column("id").ascNullsFirst
     * }}}
     */
    def ascNullsFirst: Order =
        Order(SqlOrderingItem(asSqlExpr, Some(SqlOrdering.Asc), Some(SqlNullsOrdering.First)))

    /**
     * Orders the expression in ascending order with `NULLS LAST`.
     * Maps to `expr ASC NULLS LAST`.
     *
     * {{{
     * column("id").ascNullsLast
     * }}}
     */
    def ascNullsLast: Order =
        Order(SqlOrderingItem(asSqlExpr, Some(SqlOrdering.Asc), Some(SqlNullsOrdering.Last)))

    /**
     * Orders the expression in descending order. Maps to `expr DESC`.
     *
     * {{{
     * column("id").desc
     * }}}
     */
    def desc: Order =
        Order(SqlOrderingItem(asSqlExpr, Some(SqlOrdering.Desc), None))

    /**
     * Orders the expression in descending order with `NULLS FIRST`.
     * Maps to `expr DESC NULLS FIRST`.
     *
     * {{{
     * column("id").descNullsFirst
     * }}}
     */
    def descNullsFirst: Order =
        Order(SqlOrderingItem(asSqlExpr, Some(SqlOrdering.Desc), Some(SqlNullsOrdering.First)))

    /**
     * Orders the expression in descending order with `NULLS LAST`.
     * Maps to `expr DESC NULLS LAST`.
     *
     * {{{
     * column("id").descNullsLast
     * }}}
     */
    def descNullsLast: Order =
        Order(SqlOrderingItem(asSqlExpr, Some(SqlOrdering.Desc), Some(SqlNullsOrdering.Last)))

    /**
     * References `N PRECEDING` in a window frame. Used as a frame bound
     * with `rows`, `range`, or `groups`.
     *
     * {{{
     * partitionBy(column("author")).orderBy(column("id").asc).rows(value(1).preceding)
     * }}}
     */
    def preceding: FrameBound =
        FrameBound(SqlWindowFrameBound.Preceding(asSqlExpr))

    /**
     * References `N FOLLOWING` in a window frame. Used as a frame bound
     * with `rows`, `range`, or `groups`.
     *
     * {{{
     * partitionBy(column("author")).orderBy(column("id").asc).rowsBetween(currentRow, value(1).following)
     * }}}
     */
    def following: FrameBound =
        FrameBound(SqlWindowFrameBound.Following(asSqlExpr))

    /**
     * Extracts the year from a datetime expression. Maps to
     * `EXTRACT(YEAR FROM expr)`.
     *
     * {{{
     * column("create_time").year
     * }}}
     */
    def year: Expr =
        Expr(SqlExpr.ExtractFunc(SqlTimeUnit.Year, asSqlExpr))

    /**
     * Extracts the month from a datetime expression. Maps to
     * `EXTRACT(MONTH FROM expr)`.
     *
     * {{{
     * column("create_time").month
     * }}}
     */
    def month: Expr =
        Expr(SqlExpr.ExtractFunc(SqlTimeUnit.Month, asSqlExpr))

    /**
     * Extracts the day from a datetime expression. Maps to
     * `EXTRACT(DAY FROM expr)`.
     *
     * {{{
     * column("create_time").day
     * }}}
     */
    def day: Expr =
        Expr(SqlExpr.ExtractFunc(SqlTimeUnit.Day, asSqlExpr))

    /**
     * Extracts the hour from a datetime expression. Maps to
     * `EXTRACT(HOUR FROM expr)`.
     *
     * {{{
     * column("create_time").hour
     * }}}
     */
    def hour: Expr =
        Expr(SqlExpr.ExtractFunc(SqlTimeUnit.Hour, asSqlExpr))

    /**
     * Extracts the minute from a datetime expression. Maps to
     * `EXTRACT(MINUTE FROM expr)`.
     *
     * {{{
     * column("create_time").minute
     * }}}
     */
    def minute: Expr =
        Expr(SqlExpr.ExtractFunc(SqlTimeUnit.Minute, asSqlExpr))

    /**
     * Extracts the second from a datetime expression. Maps to
     * `EXTRACT(SECOND FROM expr)`.
     *
     * {{{
     * column("create_time").second
     * }}}
     */
    def second: Expr =
        Expr(SqlExpr.ExtractFunc(SqlTimeUnit.Second, asSqlExpr))

    /**
     * Assigns a column alias to the expression, producing a select item.
     * Maps to `expr AS "alias"`.
     *
     * {{{
     * select(column("id").as("id"))
     * }}}
     */
    def as(alias: String): SelectItem =
        SelectItem(SqlSelectItem.Expr(asSqlExpr, Some(alias)))

    /**
     * Returns the underlying AST node after applying safe, semantics-
     * preserving simplifications:
     *
     *   - `NOT TRUE` becomes `FALSE` and vice versa;
     *   - `NOT LIKE`, `NOT SIMILAR TO`, `NOT IN`, `NOT BETWEEN`,
     *     `IS NOT DISTINCT FROM`, and `IS NOT` flip the underlying
     *     predicate flag instead of nesting a `NOT`.
     *
     * Other expressions are returned unchanged.
     */
    private[sqala] def asSqlExpr: SqlExpr =
        import SqlExpr.*

        expr match
            case Unary(SqlUnaryOperator.Not, e) =>
                e match
                    case BooleanLiteral(boolean) =>
                        BooleanLiteral(!boolean)
                    case Like(expr, pattern, escape, not) =>
                        Like(expr, pattern, escape, !not)
                    case SimilarTo(expr, pattern, escape, not) =>
                        SimilarTo(expr, pattern, escape, !not)
                    case In(left, right, not) =>
                        In(left, right, !not)
                    case Binary(left, SqlBinaryOperator.IsDistinctFrom(not), right) =>
                        Binary(left, SqlBinaryOperator.IsDistinctFrom(!not), right)
                    case Binary(left, SqlBinaryOperator.Is(not), right) =>
                        Binary(left, SqlBinaryOperator.Is(!not), right)
                    case Between(expr, s, e, n) =>
                        Between(expr, s, e, !n)
                    case _ => SqlExpr.Unary(SqlUnaryOperator.Not, e)
            case _ => expr