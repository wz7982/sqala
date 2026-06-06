package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.order.{SqlNullsOrdering, SqlOrdering}
import sqala.metadata.*

import scala.annotation.targetName

extension [A, CL <: Int](self: A)(using qc: QueryContext[CL], aa: AsExpr[A, CL])
    /**
     * Type-safe equality comparison. Typically used in `filter`, `on`, and
     * `having` clause. Maps to SQL `=`. Unlike raw `==` on `Expr`, this
     * operates on any object that can be lifted to an expression. Subqueries
     * must return at most one row, enforced at compile time.
     *
     * {{{
     * // Compare with a value
     * from(User).filter(u => u.id === 1)
     *
     * // Compare with a tuple
     * from(User).filter(u => (u.id, u.nickname) === (1, "John"))
     *
     * // Compare with another expression
     * from(Post.join(Comment).on((p, c) => p.id === c.postId))
     *
     * // Compare with a subquery
     * from(Post).filter(p => p.viewCount === from(Post).map(p => max(p.viewCount)))
     * }}}
     */
    @targetName("equal")
    def ===[B](that: B)(using
        ab: AsRightOperand[B, CL],
        r: Relation[aa.R, ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.Equal,
                ab.asExpr(that).asSqlExpr
            )
        )

    /**
     * Type-safe inequality comparison. Typically used in `filter`, `on`, and
     * `having` clause. Maps to SQL `<>`. Unlike raw `!=` on `Expr`, this
     * operates on any object that can be lifted to an expression. Subqueries
     * must return at most one row, enforced at compile time.
     *
     * {{{
     * // Compare with a value
     * from(User).filter(u => u.id <> 1)
     *
     * // Compare with a tuple
     * from(User).filter(u => (u.id, u.nickname) <> (1, "John"))
     *
     * // Compare with another expression
     * from(Post.join(Comment).on((p, c) => p.id <> c.postId))
     *
     * // Compare with a subquery
     * from(Post).filter(p => p.viewCount <> from(Post).map(p => max(p.viewCount)))
     * }}}
     */
    @targetName("notEqual")
    def <>[B](that: B)(using
        ab: AsRightOperand[B, CL],
        r: Relation[aa.R, ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.NotEqual,
                ab.asExpr(that).asSqlExpr
            )
        )

    /**
     * `IS NULL` test. Typically used in `filter`, `on`, and `having`
     * clause.
     *
     * {{{
     * from(User).filter(u => u.email.isNull)
     * }}}
     */
    def isNull(using t: KindToTuple[aa.K]): Expr[Boolean, Composite[t.R]] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.Is,
                SqlExpr.NullLiteral
            )
        )

    /**
     * Type-safe null-safe equality comparison. Typically used in `filter`,
     * `on`, and `having` clause. Maps to SQL `IS NOT DISTINCT FROM`. Unlike
     * `===` which yields `NULL` when either side is `NULL`, this treats two
     * `NULL` values as equal. Subqueries must return at most one row,
     * enforced at compile time.
     *
     * {{{
     * from(User).filter(u => u.nickname <=> Option.empty[String])
     * }}}
     */
    @targetName("eqIgnoreNulls")
    def <=>[B](that: B)(using
        ab: AsRightOperand[B, CL],
        r: Relation[aa.R, ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[Boolean, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.IsNotDistinctFrom,
                ab.asExpr(that).asSqlExpr
            )
        )

    /**
     * Type-safe greater-than comparison. Typically used in `filter`, `on`,
     * and `having` clause. Maps to SQL `>`. Operates on any object that can
     * be lifted to an expression. Subqueries must return at most one row,
     * enforced at compile time.
     *
     * {{{
     * // Compare with a value
     * from(User).filter(u => u.id > 1)
     *
     * // Compare with a tuple
     * from(User).filter(u => (u.id, u.nickname) > (1, "John"))
     *
     * // Compare with another expression
     * from(Post.join(Comment).on((p, c) => p.id > c.postId))
     *
     * // Compare with a subquery
     * from(Post).filter(p => p.viewCount > from(Post).map(p => max(p.viewCount)))
     * }}}
     */
    @targetName("gt")
    def >[B](that: B)(using
        ab: AsRightOperand[B, CL],
        r: Relation[aa.R, ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.GreaterThan,
                ab.asExpr(that).asSqlExpr
            )
        )

    /**
     * Type-safe greater-than-or-equal comparison. Typically used in
     * `filter`, `on`, and `having` clause. Maps to SQL `>=`. Operates on
     * any object that can be lifted to an expression. Subqueries must return
     * at most one row, enforced at compile time.
     *
     * {{{
     * // Compare with a value
     * from(User).filter(u => u.id >= 1)
     *
     * // Compare with a tuple
     * from(User).filter(u => (u.id, u.nickname) >= (1, "John"))
     *
     * // Compare with another expression
     * from(Post.join(Comment).on((p, c) => p.id >= c.postId))
     *
     * // Compare with a subquery
     * from(Post).filter(p => p.viewCount >= from(Post).map(p => max(p.viewCount)))
     * }}}
     */
    @targetName("ge")
    def >=[B](that: B)(using
        ab: AsRightOperand[B, CL],
        r: Relation[aa.R, ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.GreaterThanEqual,
                ab.asExpr(that).asSqlExpr
            )
        )

    /**
     * Type-safe less-than comparison. Typically used in `filter`, `on`, and
     * `having` clause. Maps to SQL `<`. Operates on any object that can be
     * lifted to an expression. Subqueries must return at most one row,
     * enforced at compile time.
     *
     * {{{
     * // Compare with a value
     * from(User).filter(u => u.id < 1)
     *
     * // Compare with a tuple
     * from(User).filter(u => (u.id, u.nickname) < (1, "John"))
     *
     * // Compare with another expression
     * from(Post.join(Comment).on((p, c) => p.id < c.postId))
     *
     * // Compare with a subquery
     * from(Post).filter(p => p.viewCount < from(Post).map(p => max(p.viewCount)))
     * }}}
     */
    @targetName("lt")
    def <[B](that: B)(using
        ab: AsRightOperand[B, CL],
        r: Relation[aa.R, ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.LessThan,
                ab.asExpr(that).asSqlExpr
            )
        )

    /**
     * Type-safe less-than-or-equal comparison. Typically used in `filter`,
     * `on`, and `having` clause. Maps to SQL `<=`. Operates on any object
     * that can be lifted to an expression. Subqueries must return at most one
     * row, enforced at compile time.
     *
     * {{{
     * // Compare with a value
     * from(User).filter(u => u.id <= 1)
     *
     * // Compare with a tuple
     * from(User).filter(u => (u.id, u.nickname) <= (1, "John"))
     *
     * // Compare with another expression
     * from(Post.join(Comment).on((p, c) => p.id <= c.postId))
     *
     * // Compare with a subquery
     * from(Post).filter(p => p.viewCount <= from(Post).map(p => max(p.viewCount)))
     * }}}
     */
    @targetName("le")
    def <=[B](that: B)(using
        ab: AsRightOperand[B, CL],
        r: Relation[aa.R, ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.LessThanEqual,
                ab.asExpr(that).asSqlExpr
            )
        )

    /**
     * Type-safe `IN` membership test. Typically used in `filter`, `on`, and
     * `having` clause. Maps to SQL `IN`. The right-hand side can be a list
     * of values, a subquery, or a tuple of values, expressions, and
     * subqueries. When used as part of a
     * tuple, subquery elements must return at most one row, enforced at
     * compile time. Empty collections are optimized to `FALSE`.
     *
     * {{{
     * // Compare with a list of values
     * from(User).filter(u => u.id.in(List(1, 2, 3)))
     *
     * // Compare with a subquery
     * from(User).filter(u => u.id.in(from(Post).filter(p => p.viewCount > 100).map(p => p.authorId)))
     *
     * // Compare with a tuple of values, expressions, and subqueries
     * from(Post).filter(p => p.id.in(1, u.id, from(Comment).filter(c => c.postId == p.id).map(c => c.postId).take(1)))
     * }}}
     */
    def in[B](exprs: B)(using
        i: CanIn[A, B, CL],
        kt: KindToTuple[aa.K],
        c: CombineKindTuple[kt.R, i.KS]
    ): Expr[i.R, Composite[c.R]] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.In,
                i.asExpr(exprs).asSqlExpr
            )
        )

    /**
     * Type-safe `BETWEEN` range test. Typically used in `filter`, `on`, and
     * `having` clause. Maps to SQL `BETWEEN start AND end`. The bounds are
     * inclusive. Operates on any object that can be lifted to an expression.
     * Subqueries must return at most one row, enforced at compile time.
     *
     * {{{
     * // Compare with values
     * from(Post).filter(p => p.viewCount.between(100, 1000))
     *
     * // Compare with expressions
     * from(Post).filter(p => p.viewCount.between(p.likeCount, p.likeCount + 1000))
     *
     * // Compare with subqueries
     * from(Post).filter(p => p.viewCount.between(from(Post).map(p => min(p.viewCount)), from(Post).map(p => max(p.viewCount))))
     * }}}
     */
    def between[S, E](start: S, end: E)(using
        as: AsExpr[S, CL],
        ae: AsExpr[E, CL],
        rs: Relation[aa.R, as.R],
        r: Relation[rs.R, ae.R],
        cs: CombineKind[aa.K, as.K],
        c: CombineKind[cs.R, ae.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Between(
                aa.asExpr(self).asSqlExpr,
                as.asExpr(start).asSqlExpr,
                ae.asExpr(end).asSqlExpr,
                false
            )
        )

    /**
     * Type-safe addition. Supports number + number, datetime + interval,
     * time + interval, and string + string. Strings concatenate via
     * SQL `||`; all other pairs map to SQL `+`. Operates on any object that can be lifted to an expression.
     * Subqueries must return at most one row, enforced at compile time.
     *
     * {{{
     * // Number + number
     * from(Post).map(p => p.likeCount + 1)
     *
     * // Datetime + interval
     * from(Post).map(p => p.createTime + 1.year)
     *
     * // String + string
     * from(User).map(u => u.firstName + u.lastName)
     * }}}
     */
    @targetName("plus")
    def +[B](that: B)(using
        ab: AsExpr[B, CL],
        r: Plus[aa.R, ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(r.plus(aa.asExpr(self).asSqlExpr, ab.asExpr(that).asSqlExpr))

    /**
     * Type-safe subtraction. Supports number - number, datetime - datetime,
     * time - time, datetime - interval, and time - interval. Maps to SQL
     * `-`. Operates on any object that can be lifted to an expression.
     * Subqueries must return at most one row, enforced at compile time.
     *
     * {{{
     * // Number - number
     * from(Post).map(p => p.viewCount - p.likeCount)
     *
     * // Datetime - datetime
     * from(Post).map(p => currentTimestamp() - p.createTime)
     *
     * // Datetime - interval
     * from(Post).map(p => p.createTime - 1.year)
     * }}}
     */
    @targetName("minus")
    def -[B](that: B)(using
        ab: AsExpr[B, CL],
        r: Minus[aa.R, ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.Minus,
                ab.asExpr(that).asSqlExpr
            )
        )

    /**
     * Type-safe multiplication. Supports number * number. Maps to SQL
     * `*`. Operates on any object that can be lifted to an expression.
     * Subqueries must return at most one row, enforced at compile time.
     *
     * {{{
     * // Number * number
     * from(Post).map(p => p.likeCount * 10)
     * }}}
     */
    @targetName("times")
    def *[B](that: B)(using
        ab: AsExpr[B, CL],
        na: SqlNumber[aa.R],
        nb: SqlNumber[ab.R],
        r: Return[aa.R, ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.Times,
                ab.asExpr(that).asSqlExpr
            )
        )

    /**
     * Type-safe division. Supports number / number. Maps to SQL `/`.
     * Operates on any object that can be lifted to an expression.
     * Subqueries must return at most one row, enforced at compile time.
     *
     * {{{
     * // Number / number
     * from(Post).map(p => p.likeCount / p.viewCount)
     * }}}
     */
    @targetName("div")
    def /[B](that: B)(using
        ab: AsExpr[B, CL],
        na: SqlNumber[aa.R],
        nb: SqlNumber[ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[Option[BigDecimal], c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.Div,
                ab.asExpr(that).asSqlExpr
            )
        )

    /**
     * Type-safe modulo. Supports number % number. Maps to SQL
     * `MOD(left, right)`. Operates on any object that can be lifted to an
     * expression. Subqueries must return at most one row, enforced at
     * compile time.
     *
     * {{{
     * // Number % number
     * from(Post).map(p => p.viewCount % 10)
     * }}}
     */
    @targetName("mod")
    def %[B](that: B)(using
        ab: AsExpr[B, CL],
        na: SqlNumber[aa.R],
        nb: SqlNumber[ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[Option[BigDecimal], c.R] =
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "MOD",
                aa.asExpr(self).asSqlExpr :: ab.asExpr(that).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )

    /**
     * Unary plus. Maps to SQL `+expr`. The expression must be a numeric
     * type.
     *
     * {{{
     * from(Post).map(p => +p.viewCount)
     * }}}
     */
    @targetName("positive")
    def unary_+(using
        n: SqlNumber[aa.R],
        kt: KindToTuple[aa.K]
    ): Expr[aa.R, Composite[kt.R]] =
        Expr(SqlExpr.Unary(SqlUnaryOperator.Positive, aa.asExpr(self).asSqlExpr))

    /**
     * Unary minus (negation). Maps to SQL `-expr`. The expression must be
     * a numeric type.
     *
     * {{{
     * from(Account).map(a => -a.balance)
     * }}}
     */
    @targetName("negative")
    def unary_-(using
        n: SqlNumber[aa.R],
        kt: KindToTuple[aa.K]
    ): Expr[aa.R, Composite[kt.R]] =
        Expr(SqlExpr.Unary(SqlUnaryOperator.Negative, aa.asExpr(self).asSqlExpr))

    /**
     * Logical AND. Typically used in `filter`, `on`, and
     * `having` clause to combine conditions. Maps to SQL `AND`. Both
     * sides must be boolean expressions.
     *
     * {{{
     * from(Post).filter(p => p.viewCount > 100 && p.likeCount > 10)
     * }}}
     */
    @targetName("and")
    def &&[B](that: B)(using
        ab: AsExpr[B, CL],
        ba: SqlBoolean[aa.R],
        bb: SqlBoolean[ab.R],
        r: Relation[aa.R, ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.And,
                ab.asExpr(that).asSqlExpr
            )
        )

    /**
     * Logical OR. Typically used in `filter`, `on`, and
     * `having` clause to combine conditions. Maps to SQL `OR`. Both
     * sides must be boolean expressions.
     *
     * {{{
     * from(Post).filter(p => p.viewCount > 100 || p.likeCount > 10)
     * }}}
     */
    @targetName("or")
    def ||[B](that: B)(using
        ab: AsExpr[B, CL],
        ba: SqlBoolean[aa.R],
        bb: SqlBoolean[ab.R],
        r: Relation[aa.R, ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.Or,
                ab.asExpr(that).asSqlExpr
            )
        )

    /**
     * Logical NOT. Maps to SQL `NOT expr`. The expression must be boolean.
     *
     * {{{
     * from(Post).filter(p => !(p.viewCount > 100 || p.likeCount > 10))
     * }}}
     */
    @targetName("not")
    def unary_!(using
        b: SqlBoolean[aa.R],
        kt: KindToTuple[aa.K]
    ): Expr[aa.R, Composite[kt.R]] =
        Expr(SqlExpr.Unary(SqlUnaryOperator.Not, aa.asExpr(self).asSqlExpr))

    /**
     * Type-safe `LIKE` pattern matching. Typically used in `filter`, `on`,
     * and `having` clause. Maps to SQL `LIKE`. The pattern supports SQL
     * wildcards `%` and `_`. For simple substring matching, use `contains`,
     * `startsWith`, or `endsWith`. Operates on any object that can be
     * lifted to an expression. Subqueries must return at most one row,
     * enforced at compile time.
     *
     * {{{
     * from(User).filter(u => u.nickname.like("%John%"))
     * }}}
     */
    def like[B](that: B)(using
        ab: AsExpr[B, CL],
        sa: SqlString[aa.R],
        sb: SqlString[ab.R],
        r: Relation[aa.R, ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Like(
                aa.asExpr(self).asSqlExpr,
                ab.asExpr(that).asSqlExpr,
                None,
                false
            )
        )

    /**
     * Type-safe substring containment test. Typically used in `filter`,
     * `on`, and `having` clause. Maps to SQL `LIKE '%value%'`.
     * The left-hand side accepts any object that can be
     * lifted to an expression, the right-hand side is a plain string.
     * Subqueries must return at most one row, enforced at compile time.
     *
     * {{{
     * from(User).filter(u => u.nickname.contains("John"))
     * }}}
     */
    def contains(value: String)(using
        s: SqlString[aa.R],
        r: Relation[aa.R, String],
        c: CombineKind[aa.K, Value]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Like(
                aa.asExpr(self).asSqlExpr,
                s"%$value%".asExpr.asSqlExpr,
                None,
                false
            )
        )

    /**
     * Type-safe prefix test. Typically used in `filter`, `on`, and
     * `having` clause. Maps to SQL `LIKE 'value%'`. The left-hand side
     * accepts any object that can be lifted to an expression, the
     * right-hand side is a plain string. Subqueries must return at most
     * one row, enforced at compile time.
     *
     * {{{
     * from(User).filter(u => u.nickname.startsWith("John"))
     * }}}
     */
    def startsWith(value: String)(using
        s: SqlString[aa.R],
        r: Relation[aa.R, String],
        c: CombineKind[aa.K, Value]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Like(
                aa.asExpr(self).asSqlExpr,
                s"$value%".asExpr.asSqlExpr,
                None,
                false
            )
        )

    /**
     * Type-safe suffix test. Typically used in `filter`, `on`, and
     * `having` clause. Maps to SQL `LIKE '%value'`. The left-hand side
     * accepts any object that can be lifted to an expression, the
     * right-hand side is a plain string. Subqueries must return at most
     * one row, enforced at compile time.
     *
     * {{{
     * from(User).filter(u => u.nickname.endsWith("John"))
     * }}}
     */
    def endsWith(value: String)(using
        s: SqlString[aa.R],
        r: Relation[aa.R, String],
        c: CombineKind[aa.K, Value]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Like(
                aa.asExpr(self).asSqlExpr,
                s"%$value".asExpr.asSqlExpr,
                None,
                false
            )
        )

    /**
     * `IS JSON` predicate. Typically used in `filter`, `on`, and
     * `having` clause. Tests whether the string expression is valid JSON.
     *
     * {{{
     * from(Log).filter(l => l.log.isJson)
     * }}}
     */
    def isJson(using
        s: SqlString[aa.R],
        r: Relation[aa.R, String],
        kt: KindToTuple[aa.K]
    ): Expr[r.R, Composite[kt.R]] =
        Expr(
            SqlExpr.JsonTest(aa.asExpr(self).asSqlExpr, None, None, false)
        )

    /**
     * `IS JSON OBJECT` predicate. Typically used in `filter`, `on`, and
     * `having` clause. Tests whether the string expression is a valid JSON
     * object.
     *
     * {{{
     * from(Log).filter(l => l.log.isJsonObject)
     * }}}
     */
    def isJsonObject(using
        s: SqlString[aa.R],
        r: Relation[aa.R, String],
        kt: KindToTuple[aa.K]
    ): Expr[r.R, Composite[kt.R]] =
        Expr(
            SqlExpr.JsonTest(aa.asExpr(self).asSqlExpr, Some(SqlJsonNodeType.Object), None, false)
        )

    /**
     * `IS JSON ARRAY` predicate. Typically used in `filter`, `on`, and
     * `having` clause. Tests whether the string expression is a valid JSON
     * array.
     *
     * {{{
     * from(Log).filter(l => l.log.isJsonArray)
     * }}}
     */
    def isJsonArray(using
        s: SqlString[aa.R],
        r: Relation[aa.R, String],
        kt: KindToTuple[aa.K]
    ): Expr[r.R, Composite[kt.R]] =
        Expr(
            SqlExpr.JsonTest(aa.asExpr(self).asSqlExpr, Some(SqlJsonNodeType.Array), None, false)
        )

    /**
     * `IS JSON SCALAR` predicate. Typically used in `filter`, `on`, and
     * `having` clause. Tests whether the string expression is a valid JSON
     * scalar value (string, number, boolean, or null).
     *
     * {{{
     * from(Log).filter(l => l.log.isJsonScalar)
     * }}}
     */
    def isJsonScalar(using
        s: SqlString[aa.R],
        r: Relation[aa.R, String],
        kt: KindToTuple[aa.K]
    ): Expr[r.R, Composite[kt.R]] =
        Expr(
            SqlExpr.JsonTest(aa.asExpr(self).asSqlExpr, Some(SqlJsonNodeType.Scalar), None, false)
        )

    /**
     * Ascending sort order. Maps to SQL `ASC`. Used in `sortBy` clause.
     *
     * {{{
     * from(User).sortBy(u => u.nickname.asc)
     * }}}
     */
    def asc(using AsSqlExpr[aa.R]): Sort[aa.R, aa.K] =
        Sort(aa.asExpr(self), SqlOrdering.Asc, None)

    /**
     * Ascending sort order with nulls first. Maps to SQL
     * `ASC NULLS FIRST`. Used in `sortBy` clause.
     *
     * {{{
     * from(User).sortBy(u => u.nickname.ascNullsFirst)
     * }}}
     */
    def ascNullsFirst(using AsSqlExpr[aa.R]): Sort[aa.R, aa.K] =
        Sort(aa.asExpr(self), SqlOrdering.Asc, Some(SqlNullsOrdering.First))

    /**
     * Ascending sort order with nulls last. Maps to SQL
     * `ASC NULLS LAST`. Used in `sortBy` clause.
     *
     * {{{
     * from(User).sortBy(u => u.nickname.ascNullsLast)
     * }}}
     */
    def ascNullsLast(using AsSqlExpr[aa.R]): Sort[aa.R, aa.K] =
        Sort(aa.asExpr(self), SqlOrdering.Asc, Some(SqlNullsOrdering.Last))

    /**
     * Descending sort order. Maps to SQL `DESC`. Used in `sortBy` clause.
     *
     * {{{
     * from(User).sortBy(u => u.nickname.desc)
     * }}}
     */
    def desc(using AsSqlExpr[aa.R]): Sort[aa.R, aa.K] =
        Sort(aa.asExpr(self), SqlOrdering.Desc, None)

    /**
     * Descending sort order with nulls first. Maps to SQL
     * `DESC NULLS FIRST`. Used in `sortBy` clause.
     *
     * {{{
     * from(User).sortBy(u => u.nickname.descNullsFirst)
     * }}}
     */
    def descNullsFirst(using AsSqlExpr[aa.R]): Sort[aa.R, aa.K] =
        Sort(aa.asExpr(self), SqlOrdering.Desc, Some(SqlNullsOrdering.First))

    /**
     * Descending sort order with nulls last. Maps to SQL
     * `DESC NULLS LAST`. Used in `sortBy` clause.
     *
     * {{{
     * from(User).sortBy(u => u.nickname.descNullsLast)
     * }}}
     */
    def descNullsLast(using AsSqlExpr[aa.R]): Sort[aa.R, aa.K] =
        Sort(aa.asExpr(self), SqlOrdering.Desc, Some(SqlNullsOrdering.Last))

extension [A, B, CL <: Int](self: (A, B))(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL]
)
    /**
     * Temporal `OVERLAPS` test. Typically used in `filter`, `on`, and
     * `having` clause. Maps to SQL `OVERLAPS`. Both sides must be
     * `(start, end)` tuples of date-time expressions.
     *
     * {{{
     * from(Event).filter(e => (e.startTime, e.endTime).overlaps(time1, time2))
     * }}}
     */
    def overlaps[C, D](that: (C, D))(using
        ac: AsExpr[C, CL],
        ad: AsExpr[D, CL],
        da: SqlDateTime[aa.R],
        db: SqlDateTime[ab.R],
        dc: SqlDateTime[ac.R],
        dd: SqlDateTime[ad.R],
        r1: Relation[aa.R, ab.R],
        r2: Relation[ac.R, ad.R],
        r: Relation[r1.R, r2.R],
        c1: CombineKind[aa.K, ab.K],
        c2: CombineKind[ac.K, ad.K],
        c: CombineKind[c1.R, c2.R]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                SqlExpr.Tuple(
                    aa.asExpr(self._1).asSqlExpr ::
                    ab.asExpr(self._2).asSqlExpr ::
                    Nil
                ),
                SqlBinaryOperator.Overlaps,
                SqlExpr.Tuple(
                    ac.asExpr(that._1).asSqlExpr ::
                    ad.asExpr(that._2).asSqlExpr ::
                    Nil
                )
            )
        )