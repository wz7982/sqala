package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.quantifier.SqlQuantifier
import sqala.metadata.*

import java.time.*

/**
 * Counts all rows. Maps to `COUNT(*)`.
 *
 * {{{
 * from(Post).map(p => count())
 * }}}
 */
def count[CL <: Int]()(using QueryContext[CL]): Expr[Long, Agg[EmptyTuple]] =
    Expr(SqlExpr.CountAsteriskFunc(None, None))

/**
 * Counts non-null values of an expression. Maps to `COUNT(expr)`.
 * Aggregate functions, window functions, and other expressions
 * not allowed in aggregate calls are rejected at compile time.
 *
 * {{{
 * from(Post).map(p => count(p.likeCount))
 * }}}
 */
def count[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R]
): Expr[Long, Agg[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "COUNT",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Counts distinct non-null values of an expression. Maps to
 * `COUNT(DISTINCT expr)`. Aggregate functions, window functions,
 * and other expressions not allowed in aggregate calls are rejected
 * at compile time.
 *
 * {{{
 * from(Post).map(p => countDistinct(p.likeCount))
 * }}}
 */
def countDistinct[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R]
): Expr[Long, Agg[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            Some(SqlQuantifier.Distinct),
            "COUNT",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the sum of a numeric expression. Maps to `SUM(expr)`.
 * Returns an optional result, since `SUM` is `NULL` when all values
 * are null. Aggregate functions, window functions, and other
 * expressions not allowed in aggregate calls are rejected at
 * compile time.
 *
 * {{{
 * from(Post).map(p => sum(p.likeCount))
 * }}}
 */
def sum[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
    to: ToOption[Expr[a.R, Agg[kt.R]]]
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "SUM",
                a.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

/**
 * Computes the average of a numeric expression. Maps to
 * `AVG(expr)`. Aggregate functions, window functions, and other
 * expressions not allowed in aggregate calls are rejected at
 * compile time.
 *
 * {{{
 * from(Post).map(p => avg(p.likeCount))
 * }}}
 */
def avg[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R]
): Expr[Option[BigDecimal], Agg[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "AVG",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the maximum value of an expression. Maps to `MAX(expr)`.
 * Returns an optional result. Aggregate functions, window functions,
 * and other expressions not allowed in aggregate calls are rejected
 * at compile time.
 *
 * {{{
 * from(Post).map(p => max(p.likeCount))
 * }}}
 */
def max[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
    to: ToOption[Expr[a.R, Agg[kt.R]]]
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "MAX",
                a.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

/**
 * Computes the minimum value of an expression. Maps to `MIN(expr)`.
 * Returns an optional result. Aggregate functions, window functions,
 * and other expressions not allowed in aggregate calls are rejected
 * at compile time.
 *
 * {{{
 * from(Post).map(p => min(p.likeCount))
 * }}}
 */
def min[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
    to: ToOption[Expr[a.R, Agg[kt.R]]]
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "MIN",
                a.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

/**
 * Returns an arbitrary non-null value from a group. Maps to
 * `ANY_VALUE(expr)`. Typically used in `map` clause with
 * `groupBy` to pick a representative value. Aggregate functions,
 * window functions, and other expressions not allowed in aggregate
 * calls are rejected at compile time.
 *
 * {{{
 * from(Post).groupBy(p => p.channelId).map((g, p) => anyValue(p.title))
 * }}}
 */
def anyValue[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
    to: ToOption[Expr[a.R, Agg[kt.R]]]
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "ANY_VALUE",
                a.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

/**
 * Computes the population standard deviation of a numeric expression.
 * Maps to `STDDEV_POP(expr)`. Aggregate functions, window functions,
 * and other expressions not allowed in aggregate calls are rejected
 * at compile time.
 *
 * {{{
 * from(Post).map(p => stddevPop(p.likeCount))
 * }}}
 */
def stddevPop[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R]
): Expr[Option[BigDecimal], Agg[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "STDDEV_POP",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the sample standard deviation of a numeric expression.
 * Maps to `STDDEV_SAMP(expr)`. Aggregate functions, window functions,
 * and other expressions not allowed in aggregate calls are rejected
 * at compile time.
 *
 * {{{
 * from(Post).map(p => stddevSamp(p.likeCount))
 * }}}
 */
def stddevSamp[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R]
): Expr[Option[BigDecimal], Agg[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "STDDEV_SAMP",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the population variance of a numeric expression.
 * Maps to `VAR_POP(expr)`. Aggregate functions, window functions,
 * and other expressions not allowed in aggregate calls are rejected
 * at compile time.
 *
 * {{{
 * from(Post).map(p => varPop(p.likeCount))
 * }}}
 */
def varPop[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R]
): Expr[Option[BigDecimal], Agg[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "VAR_POP",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the sample variance of a numeric expression.
 * Maps to `VAR_SAMP(expr)`. Aggregate functions, window functions,
 * and other expressions not allowed in aggregate calls are rejected
 * at compile time.
 *
 * {{{
 * from(Post).map(p => varSamp(p.likeCount))
 * }}}
 */
def varSamp[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R]
): Expr[Option[BigDecimal], Agg[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "VAR_SAMP",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the population covariance of two numeric expressions.
 * Maps to `COVAR_POP(x, y)`. Both arguments must be numeric
 * expressions. Aggregate functions, window functions, and other
 * expressions not allowed in aggregate calls are rejected at
 * compile time.
 *
 * {{{
 * from(Post).map(p => covarPop(p.likeCount, p.viewCount))
 * }}}
 */
def covarPop[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): Expr[Option[BigDecimal], Agg[c.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "COVAR_POP",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the sample covariance of two numeric expressions.
 * Maps to `COVAR_SAMP(x, y)`. Both arguments must be numeric
 * expressions. Aggregate functions, window functions, and other
 * expressions not allowed in aggregate calls are rejected at
 * compile time.
 *
 * {{{
 * from(Post).map(p => covarSamp(p.likeCount, p.viewCount))
 * }}}
 */
def covarSamp[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): Expr[Option[BigDecimal], Agg[c.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "COVAR_SAMP",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the correlation coefficient of two numeric expressions.
 * Maps to `CORR(x, y)`. Both arguments must be numeric expressions.
 * Aggregate functions, window functions, and other expressions not
 * allowed in aggregate calls are rejected at compile time.
 *
 * {{{
 * from(Post).map(p => corr(p.likeCount, p.viewCount))
 * }}}
 */
def corr[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): Expr[Option[BigDecimal], Agg[c.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "CORR",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the slope of the linear regression line for two numeric
 * expressions. Maps to `REGR_SLOPE(x, y)`. Both arguments must be
 * numeric expressions. Aggregate functions, window functions, and
 * other expressions not allowed in aggregate calls are rejected at
 * compile time.
 *
 * {{{
 * from(Post).map(p => regrSlope(p.likeCount, p.viewCount))
 * }}}
 */
def regrSlope[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): Expr[Option[BigDecimal], Agg[c.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_SLOPE",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the intercept of the linear regression line for two
 * numeric expressions. Maps to `REGR_INTERCEPT(x, y)`. Both arguments
 * must be numeric expressions. Aggregate functions, window functions,
 * and other expressions not allowed in aggregate calls are rejected at
 * compile time.
 *
 * {{{
 * from(Post).map(p => regrIntercept(p.likeCount, p.viewCount))
 * }}}
 */
def regrIntercept[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): Expr[Option[BigDecimal], Agg[c.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_INTERCEPT",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the number of non-null pairs for linear regression.
 * Maps to `REGR_COUNT(x, y)`. Both arguments must be numeric
 * expressions. Aggregate functions, window functions, and other
 * expressions not allowed in aggregate calls are rejected at
 * compile time.
 *
 * {{{
 * from(Post).map(p => regrCount(p.likeCount, p.viewCount))
 * }}}
 */
def regrCount[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): Expr[Long, Agg[c.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_COUNT",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the R-squared of the linear regression for two numeric
 * expressions. Maps to `REGR_R2(x, y)`. Both arguments must be
 * numeric expressions. Aggregate functions, window functions, and
 * other expressions not allowed in aggregate calls are rejected at
 * compile time.
 *
 * {{{
 * from(Post).map(p => regrR2(p.likeCount, p.viewCount))
 * }}}
 */
def regrR2[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): Expr[Option[BigDecimal], Agg[c.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_R2",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the average of the independent variable for linear
 * regression. Maps to `REGR_AVGX(x, y)`. Both arguments must be
 * numeric expressions. Aggregate functions, window functions, and
 * other expressions not allowed in aggregate calls are rejected at
 * compile time.
 *
 * {{{
 * from(Post).map(p => regrAvgx(p.likeCount, p.viewCount))
 * }}}
 */
def regrAvgx[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): Expr[Option[BigDecimal], Agg[c.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_AVGX",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the average of the dependent variable for linear
 * regression. Maps to `REGR_AVGY(x, y)`. Both arguments must be
 * numeric expressions. Aggregate functions, window functions, and
 * other expressions not allowed in aggregate calls are rejected at
 * compile time.
 *
 * {{{
 * from(Post).map(p => regrAvgy(p.likeCount, p.viewCount))
 * }}}
 */
def regrAvgy[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): Expr[Option[BigDecimal], Agg[c.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_AVGY",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the sum of squares of the independent variable for linear
 * regression. Maps to `REGR_SXX(x, y)`. Both arguments must be
 * numeric expressions. Aggregate functions, window functions, and
 * other expressions not allowed in aggregate calls are rejected at
 * compile time.
 *
 * {{{
 * from(Post).map(p => regrSxx(p.likeCount, p.viewCount))
 * }}}
 */
def regrSxx[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): Expr[Option[BigDecimal], Agg[c.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_SXX",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the sum of squares of the dependent variable for linear
 * regression. Maps to `REGR_SYY(x, y)`. Both arguments must be
 * numeric expressions. Aggregate functions, window functions, and
 * other expressions not allowed in aggregate calls are rejected at
 * compile time.
 *
 * {{{
 * from(Post).map(p => regrSyy(p.likeCount, p.viewCount))
 * }}}
 */
def regrSyy[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): Expr[Option[BigDecimal], Agg[c.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_SYY",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the sum of cross products for linear regression.
 * Maps to `REGR_SXY(x, y)`. Both arguments must be numeric
 * expressions. Aggregate functions, window functions, and other
 * expressions not allowed in aggregate calls are rejected at
 * compile time.
 *
 * {{{
 * from(Post).map(p => regrSxy(p.likeCount, p.viewCount))
 * }}}
 */
def regrSxy[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): Expr[Option[BigDecimal], Agg[c.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_SXY",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes a continuous percentile over a group. Maps to
 * `PERCENTILE_CONT(expr)`. The first argument is a numeric
 * expression, the second is a sort specification on a numeric
 * expression.
 * Aggregate functions, window functions, and other expressions not
 * allowed in aggregate calls are rejected at compile time.
 *
 * {{{
 * from(Post).map(p => percentileCont(0.5, p.likeCount.desc))
 * }}}
 */
def percentileCont[T, ST, SK <: ExprKind, CL <: Int](x: T, withinGroup: Sort[ST, SK])(using
    qc: QueryContext[CL],
    at: AsExpr[T, CL],
    nt: SqlNumber[at.R],
    ns: SqlNumber[ST],
    kt: KindToTuple[at.K],
    kts: KindToTuple[SK],
    it: CanInAgg[kt.R],
    is: CanInAgg[kts.R],
    c: CombineKindTuple[kt.R, kts.R],
    to: ToOption[Expr[ST, Agg[c.R]]]
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "PERCENTILE_CONT",
                at.asExpr(x).asSqlExpr :: Nil,
                Nil,
                withinGroup.asSqlOrderingItem :: Nil,
                None
            )
        )
    )

/**
 * Computes a discrete percentile over a group. Maps to
 * `PERCENTILE_DISC(expr)`. The first argument is a numeric
 * expression, the second is a sort specification on a numeric
 * expression.
 * Aggregate functions, window functions, and other expressions not
 * allowed in aggregate calls are rejected at compile time.
 *
 * {{{
 * from(Post).map(p => percentileDisc(0.5, p.likeCount.desc))
 * }}}
 */
def percentileDisc[T, ST, SK <: ExprKind, CL <: Int](x: T, withinGroup: Sort[ST, SK])(using
    qc: QueryContext[CL],
    at: AsExpr[T, CL],
    nt: SqlNumber[at.R],
    ns: SqlNumber[ST],
    kt: KindToTuple[at.K],
    kts: KindToTuple[SK],
    it: CanInAgg[kt.R],
    is: CanInAgg[kts.R],
    c: CombineKindTuple[kt.R, kts.R],
    to: ToOption[Expr[ST, Agg[c.R]]]
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "PERCENTILE_DISC",
                at.asExpr(x).asSqlExpr :: Nil,
                Nil,
                withinGroup.asSqlOrderingItem :: Nil,
                None
            )
        )
    )

/**
 * Concatenates string values with a separator, ordered by a sort
 * specification. Maps to `LISTAGG(expr, separator)`. Both arguments
 * must be string expressions, the third is a sort specification.
 * Aggregate functions, window functions, and other expressions not
 * allowed in aggregate calls are rejected at compile time.
 *
 * {{{
 * from(Post).map(p => listAgg(p.title, ", ", p.id.desc))
 * }}}
 */
def listAgg[T, S, ST, SK <: ExprKind, CL <: Int](x: T, separator: S, withinGroup: Sort[ST, SK])(using
    qc: QueryContext[CL],
    at: AsExpr[T, CL],
    as: AsExpr[S, CL],
    st: SqlString[at.R],
    ss: SqlString[as.R],
    kt: KindToTuple[at.K],
    kts: KindToTuple[as.K],
    ktw: KindToTuple[SK],
    it: CanInAgg[kt.R],
    is: CanInAgg[kts.R],
    iw: CanInAgg[ktw.R],
    cs: CombineKindTuple[kt.R, kts.R],
    c: CombineKindTuple[cs.R, ktw.R]
): Expr[Option[String], Agg[c.R]] =
    Expr(
        SqlExpr.ListAggFunc(
            None,
            at.asExpr(x).asSqlExpr,
            as.asExpr(separator).asSqlExpr,
            None,
            withinGroup.asSqlOrderingItem :: Nil,
            None
        )
    )

/**
 * Alias of `listAgg`, provided for users familiar with `STRING_AGG`.
 * {{{
 * from(Post).map(p => stringAgg(p.title, ", ", p.id.desc))
 * }}}
 */
def stringAgg[T, S, ST, SK <: ExprKind, CL <: Int](x: T, separator: S, withinGroup: Sort[ST, SK])(using
    qc: QueryContext[CL],
    at: AsExpr[T, CL],
    as: AsExpr[S, CL],
    st: SqlString[at.R],
    ss: SqlString[as.R],
    kt: KindToTuple[at.K],
    kts: KindToTuple[as.K],
    ktw: KindToTuple[SK],
    it: CanInAgg[kt.R],
    is: CanInAgg[kts.R],
    iw: CanInAgg[ktw.R],
    cs: CombineKindTuple[kt.R, kts.R],
    c: CombineKindTuple[cs.R, ktw.R]
): Expr[Option[String], Agg[c.R]] =
    listAgg(x, separator, withinGroup)

/**
 * Alias of `listAgg`, provided for users familiar with `GROUP_CONCAT`.
 * {{{
 * from(Post).map(p => groupConcat(p.title, ", ", p.id.desc))
 * }}}
 */
def groupConcat[T, S, ST, SK <: ExprKind, CL <: Int](x: T, separator: S, withinGroup: Sort[ST, SK])(using
    qc: QueryContext[CL],
    at: AsExpr[T, CL],
    as: AsExpr[S, CL],
    st: SqlString[at.R],
    ss: SqlString[as.R],
    kt: KindToTuple[at.K],
    kts: KindToTuple[as.K],
    ktw: KindToTuple[SK],
    it: CanInAgg[kt.R],
    is: CanInAgg[kts.R],
    iw: CanInAgg[ktw.R],
    cs: CombineKindTuple[kt.R, kts.R],
    c: CombineKindTuple[cs.R, ktw.R]
): Expr[Option[String], Agg[c.R]] =
    listAgg(x, separator, withinGroup)

/**
 * Aggregates key and value pairs into a JSON object. Maps to
 * `JSON_OBJECTAGG(key VALUE value)`. Aggregate functions, window
 * functions, and other expressions not allowed in aggregate calls
 * are rejected at compile time.
 *
 * {{{
 * from(Post).groupBy(p => p.authorId).map((g, p) => jsonObjectAgg(p.id, p.title))
 * }}}
 */
def jsonObjectAgg[K, V, CL <: Int](key: K, value: V)(using
    qc: QueryContext[CL],
    ak: AsExpr[K, CL],
    av: AsExpr[V, CL],
    ask: AsSqlExpr[ak.R],
    asv: AsSqlExpr[av.R],
    ktk: KindToTuple[ak.K],
    ktv: KindToTuple[av.K],
    ik: CanInAgg[ktk.R],
    iv: CanInAgg[ktv.R],
    c: CombineKindTuple[ktk.R, ktv.R]
): Expr[Option[Json], Agg[c.R]] =
    Expr(
        SqlExpr.JsonObjectAggFunc(
            SqlJsonObjectItem(ak.asExpr(key).asSqlExpr, av.asExpr(value).asSqlExpr),
            None,
            None,
            None,
            None
        )
    )

/**
 * Aggregates values into a JSON array. Maps to `JSON_ARRAYAGG(expr)`.
 * Aggregate functions, window functions, and other expressions not
 * allowed in aggregate calls are rejected at compile time.
 *
 * {{{
 * from(Post).groupBy(p => p.authorId).map((g, p) => jsonArrayAgg(p.title))
 * }}}
 */
def jsonArrayAgg[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R]
): Expr[Option[Json], Agg[kt.R]] =
    Expr(
        SqlExpr.JsonArrayAggFunc(
            SqlJsonArrayItem(a.asExpr(x).asSqlExpr, None),
            Nil,
            None,
            None,
            None
        )
    )

/**
 * Returns the matched pattern name in `matchRecognize`. Maps to
 * `CLASSIFIER()`.
 */
def classifier[CL <: Int]()(using QueryContext[CL], MatchRecognizeContext): Expr[String, Agg[EmptyTuple]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "CLASSIFIER",
            Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the sequential match number in `matchRecognize`. Maps to
 * `MATCH_NUMBER()`.
 */
def matchNumber[CL <: Int]()(using QueryContext[CL], MatchRecognizeContext): Expr[Int, Agg[EmptyTuple]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "MATCL_NUMBER",
            Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the first value of an expression in `matchRecognize`.
 * Maps to `FIRST(expr)`.
 *
 * {{{
 * first(expr)
 * }}}
 */
def first[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext,
    a: AsExpr[A, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
    to: ToOption[Expr[a.R, Agg[kt.R]]]
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "FIRST",
                a.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

/**
 * Returns the first N values of an expression in `matchRecognize`.
 * Maps to `FIRST(expr, n)`.
 *
 * {{{
 * first(expr, n)
 * }}}
 */
def first[A, N, CL <: Int](x: A, n: N)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext,
    aa: AsExpr[A, CL],
    an: AsExpr[N, CL],
    asa: AsSqlExpr[aa.R],
    nn: SqlNumber[an.R],
    kta: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInAgg[kta.R],
    in: CanInAgg[ktn.R],
    c: CombineKindTuple[kta.R, ktn.R],
    to: ToOption[Expr[aa.R, Agg[c.R]]]
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "FIRST",
                aa.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

/**
 * Returns the last value of an expression in `matchRecognize`.
 * Maps to `LAST(expr)`.
 *
 * {{{
 * last(expr)
 * }}}
 */
def last[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext,
    a: AsExpr[A, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
    to: ToOption[Expr[a.R, Agg[kt.R]]]
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "LAST",
                a.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

/**
 * Returns the last N values of an expression in `matchRecognize`.
 * Maps to `LAST(expr, n)`.
 *
 * {{{
 * last(expr, n)
 * }}}
 */
def last[A, N, CL <: Int](x: A, n: N)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext,
    aa: AsExpr[A, CL],
    an: AsExpr[N, CL],
    asa: AsSqlExpr[aa.R],
    nn: SqlNumber[an.R],
    kta: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInAgg[kta.R],
    in: CanInAgg[ktn.R],
    c: CombineKindTuple[kta.R, ktn.R],
    to: ToOption[Expr[aa.R, Agg[c.R]]]
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "LAST",
                aa.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

/**
 * Returns the previous value of an expression in `matchRecognize`.
 * Maps to `PREV(expr)`.
 *
 * {{{
 * prev(expr)
 * }}}
 */
def prev[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext,
    a: AsExpr[A, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
    to: ToOption[Expr[a.R, Agg[kt.R]]]
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "PREV",
                a.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

/**
 * Returns the previous N-th value of an expression in
 * `matchRecognize`. Maps to `PREV(expr, n)`.
 *
 * {{{
 * prev(expr, n)
 * }}}
 */
def prev[A, N, CL <: Int](x: A, n: N)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext,
    aa: AsExpr[A, CL],
    an: AsExpr[N, CL],
    asa: AsSqlExpr[aa.R],
    nn: SqlNumber[an.R],
    kta: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInAgg[kta.R],
    in: CanInAgg[ktn.R],
    c: CombineKindTuple[kta.R, ktn.R],
    to: ToOption[Expr[aa.R, Agg[c.R]]]
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "PREV",
                aa.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

/**
 * Returns the next value of an expression in `matchRecognize`.
 * Maps to `NEXT(expr)`.
 *
 * {{{
 * next(expr)
 * }}}
 */
def next[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext,
    a: AsExpr[A, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
    to: ToOption[Expr[a.R, Agg[kt.R]]]
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "NEXT",
                a.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

/**
 * Returns the next N-th value of an expression in
 * `matchRecognize`. Maps to `NEXT(expr, n)`.
 *
 * {{{
 * next(expr, n)
 * }}}
 */
def next[A, N, CL <: Int](x: A, n: N)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext,
    aa: AsExpr[A, CL],
    an: AsExpr[N, CL],
    asa: AsSqlExpr[aa.R],
    nn: SqlNumber[an.R],
    kta: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInAgg[kta.R],
    in: CanInAgg[ktn.R],
    c: CombineKindTuple[kta.R, ktn.R],
    to: ToOption[Expr[aa.R, Agg[c.R]]]
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "NEXT",
                aa.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

/**
 * Extracts a substring starting from a position. Maps to
 * `SUBSTRING(expr FROM start)`. The first argument must be a string
 * expression, the second must be a numeric expression.
 *
 * {{{
 * from(Post).map(p => substring(p.title, 1))
 * }}}
 */
def substring[T, S, CL <: Int](x: T, start: S)(using
    qc: QueryContext[CL],
    at: AsExpr[T, CL],
    as: AsExpr[S, CL],
    s: SqlString[at.R],
    n: SqlNumber[as.R],
    c: CombineKind[at.K, as.K]
): Expr[Option[String], c.R] =
    Expr(
        SqlExpr.SubstringFunc(
            at.asExpr(x).asSqlExpr,
            as.asExpr(start).asSqlExpr,
            None
        )
    )

/**
 * Extracts a substring of a given length. Maps to
 * `SUBSTRING(expr FROM start FOR end)`. The first argument must be
 * a string expression, the second and third must be numeric
 * expressions.
 *
 * {{{
 * from(Post).map(p => substring(p.title, 1, 5))
 * }}}
 */
def substring[T, S, E, CL <: Int](x: T, start: S, end: E)(using
    qc: QueryContext[CL],
    at: AsExpr[T, CL],
    as: AsExpr[S, CL],
    ae: AsExpr[E, CL],
    s: SqlString[at.R],
    ns: SqlNumber[as.R],
    ne: SqlNumber[ae.R],
    cs: CombineKind[at.K, as.K],
    c: CombineKind[cs.R, ae.K]
): Expr[Option[String], c.R] =
    Expr(
        SqlExpr.SubstringFunc(
            at.asExpr(x).asSqlExpr,
            as.asExpr(start).asSqlExpr,
            Some(ae.asExpr(end).asSqlExpr)
        )
    )

/**
 * Converts a string to uppercase. Maps to `UPPER(expr)`.
 *
 * {{{
 * from(Post).map(p => upper(p.title))
 * }}}
 */
def upper[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    s: SqlString[a.R],
    kt: KindToTuple[a.K]
): Expr[a.R, Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "UPPER",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Converts a string to lowercase. Maps to `LOWER(expr)`.
 *
 * {{{
 * from(Post).map(p => lower(p.title))
 * }}}
 */
def lower[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    s: SqlString[a.R],
    kt: KindToTuple[a.K]
): Expr[a.R, Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LOWER",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Left-pads a string to a given length. Maps to `LPAD(expr, n)`.
 * The first argument must be a string expression, the second must
 * be a numeric expression.
 *
 * {{{
 * from(Post).map(p => lpad(p.title, 10))
 * }}}
 */
def lpad[T, N, CL <: Int](x: T, n: N)(using
    qc: QueryContext[CL],
    at: AsExpr[T, CL],
    an: AsExpr[N, CL],
    s: SqlString[at.R],
    nn: SqlNumber[an.R],
    c: CombineKind[at.K, an.K]
): Expr[Option[String], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LPAD",
            at.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Left-pads a string with a custom pad character. Maps to
 * `LPAD(expr, n, pad)`. The first and third arguments must be
 * string expressions, the second must be a numeric expression.
 *
 * {{{
 * from(Post).map(p => lpad(p.title, 10, " "))
 * }}}
 */
def lpad[T, N, P, CL <: Int](x: T, n: N, pad: P)(using
    qc: QueryContext[CL],
    at: AsExpr[T, CL],
    an: AsExpr[N, CL],
    ap: AsExpr[P, CL],
    s: SqlString[at.R],
    nn: SqlNumber[an.R],
    sp: SqlString[ap.R],
    cn: CombineKind[at.K, an.K],
    c: CombineKind[cn.R, ap.K]
): Expr[Option[String], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LPAD",
            at.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: ap.asExpr(pad).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Right-pads a string to a given length. Maps to `RPAD(expr, n)`.
 * The first argument must be a string expression, the second must
 * be a numeric expression.
 *
 * {{{
 * from(Post).map(p => rpad(p.title, 10))
 * }}}
 */
def rpad[T, N, CL <: Int](x: T, n: N)(using
    qc: QueryContext[CL],
    at: AsExpr[T, CL],
    an: AsExpr[N, CL],
    s: SqlString[at.R],
    nn: SqlNumber[an.R],
    c: CombineKind[at.K, an.K]
): Expr[Option[String], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "RPAD",
            at.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Right-pads a string with a custom pad character. Maps to
 * `RPAD(expr, n, pad)`. The first and third arguments must be
 * string expressions, the second must be a numeric expression.
 *
 * {{{
 * from(Post).map(p => rpad(p.title, 10, " "))
 * }}}
 */
def rpad[T, N, P, CL <: Int](x: T, n: N, pad: P)(using
    qc: QueryContext[CL],
    at: AsExpr[T, CL],
    an: AsExpr[N, CL],
    ap: AsExpr[P, CL],
    s: SqlString[at.R],
    nn: SqlNumber[an.R],
    sp: SqlString[ap.R],
    cn: CombineKind[at.K, an.K],
    c: CombineKind[cn.R, ap.K]
): Expr[Option[String], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "RPAD",
            at.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: ap.asExpr(pad).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Trims whitespace from both sides of a string. Maps to
 * `BTRIM(expr)`.
 *
 * {{{
 * from(Post).map(p => btrim(p.title))
 * }}}
 */
def btrim[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    s: SqlString[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[String], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "BTRIM",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Trims specified characters from both sides of a string. Maps to
 * `BTRIM(expr, chars)`.
 *
 * {{{
 * from(Post).map(p => btrim(p.title, " "))
 * }}}
 */
def btrim[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    sa: SqlString[aa.R],
    sb: SqlString[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[String], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "BTRIM",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Trims whitespace from the left side of a string. Maps to
 * `LTRIM(expr)`.
 *
 * {{{
 * from(Post).map(p => ltrim(p.title))
 * }}}
 */
def ltrim[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    s: SqlString[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[String], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LTRIM",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Trims specified characters from the left side of a string.
 * Maps to `LTRIM(expr, chars)`.
 *
 * {{{
 * from(Post).map(p => ltrim(p.title, " "))
 * }}}
 */
def ltrim[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    sa: SqlString[aa.R],
    sb: SqlString[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[String], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LTRIM",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Trims whitespace from the right side of a string. Maps to
 * `RTRIM(expr)`.
 *
 * {{{
 * from(Post).map(p => rtrim(p.title))
 * }}}
 */
def rtrim[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    s: SqlString[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[String], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "RTRIM",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Trims specified characters from the right side of a string.
 * Maps to `RTRIM(expr, chars)`.
 *
 * {{{
 * from(Post).map(p => rtrim(p.title, " "))
 * }}}
 */
def rtrim[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    sa: SqlString[aa.R],
    sb: SqlString[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[String], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "RTRIM",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Replaces a substring starting from a position. Maps to
 * `OVERLAY(expr PLACING placing FROM start)`. The first two
 * arguments must be string expressions, the third must be a numeric
 * expression.
 *
 * {{{
 * from(Post).map(p => overlay(p.title, "abc", 1))
 * }}}
 */
def overlay[A, B, S, CL <: Int](x: A, y: B, start: S)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    as: AsExpr[S, CL],
    sa: SqlString[aa.R],
    sb: SqlString[ab.R],
    n: SqlNumber[as.R],
    cb: CombineKind[aa.K, ab.K],
    c: CombineKind[cb.R, as.K]
): Expr[Option[String], c.R] =
    Expr(
        SqlExpr.OverlayFunc(
            aa.asExpr(x).asSqlExpr,
            ab.asExpr(y).asSqlExpr,
            as.asExpr(start).asSqlExpr,
            None
        )
    )

/**
 * Replaces a substring of a given length. Maps to
 * `OVERLAY(expr PLACING placing FROM start FOR end)`. The first
 * two arguments must be string expressions, the third and fourth
 * must be numeric expressions.
 *
 * {{{
 * from(Post).map(p => overlay(p.title, "abc", 1, 3))
 * }}}
 */
def overlay[A, B, S, E, CL <: Int](x: A, y: B, start: S, end: E)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    as: AsExpr[S, CL],
    ae: AsExpr[E, CL],
    sa: SqlString[aa.R],
    sb: SqlString[ab.R],
    ns: SqlNumber[as.R],
    ne: SqlNumber[ae.R],
    cb: CombineKind[aa.K, ab.K],
    cs: CombineKind[cb.R, as.K],
    c: CombineKind[cs.R, ae.K]
): Expr[Option[String], c.R] =
    Expr(
        SqlExpr.OverlayFunc(
            aa.asExpr(x).asSqlExpr,
            ab.asExpr(y).asSqlExpr,
            as.asExpr(start).asSqlExpr,
            Some(ae.asExpr(end).asSqlExpr)
        )
    )

/**
 * Tests whether a string matches a regular expression. Maps to
 * `REGEXP_LIKE(expr, pattern)`. Typically used in `filter` clause.
 * Both arguments must be string expressions.
 *
 * {{{
 * from(Post).filter(p => regexpLike(p.title, "^[A-Z]"))
 * }}}
 */
def regexpLike[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    sa: SqlString[aa.R],
    sb: SqlString[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGEXP_LIKE",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Finds the position of a substring. Maps to `POSITION(x IN y)`.
 * Both arguments must be string expressions.
 *
 * {{{
 * from(Post).map(p => position("abc", p.title))
 * }}}
 */
def position[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    sa: SqlString[aa.R],
    sb: SqlString[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Int, c.R] =
    Expr(
        SqlExpr.PositionFunc(
            aa.asExpr(x).asSqlExpr,
            ab.asExpr(y).asSqlExpr
        )
    )

/**
 * Returns the character length of a string. Maps to
 * `CHAR_LENGTH(expr)`.
 *
 * {{{
 * from(Post).map(p => charLength(p.title))
 * }}}
 */
def charLength[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    s: SqlString[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[Int], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "CLAR_LENGTH",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the byte length of a string. Maps to `OCTET_LENGTH(expr)`.
 *
 * {{{
 * from(Post).map(p => octetLength(p.title))
 * }}}
 */
def octetLength[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    s: SqlString[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[Int], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "OCTET_LENGTH",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the absolute value. Maps to `ABS(expr)`.
 *
 * {{{
 * from(Post).map(p => abs(p.likeCount))
 * }}}
 */
def abs[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[a.R, Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ABS",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the remainder of division. Maps to `MOD(x, y)`.
 *
 * {{{
 * from(Post).map(p => mod(p.likeCount, 10))
 * }}}
 */
def mod[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[A],
    nb: SqlNumber[B],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[BigDecimal], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "MOD",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the sine. Maps to `SIN(expr)`.
 *
 * {{{
 * from(Post).map(p => sin(p.likeCount))
 * }}}
 */
def sin[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "SIN",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the cosine. Maps to `COS(expr)`.
 *
 * {{{
 * from(Post).map(p => cos(p.likeCount))
 * }}}
 */
def cos[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "COS",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the tangent. Maps to `TAN(expr)`.
 *
 * {{{
 * from(Post).map(p => tan(p.likeCount))
 * }}}
 */
def tan[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "TAN",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the hyperbolic sine. Maps to `SINH(expr)`.
 *
 * {{{
 * from(Post).map(p => sinh(p.likeCount))
 * }}}
 */
def sinh[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "SINH",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the hyperbolic cosine. Maps to `COSH(expr)`.
 *
 * {{{
 * from(Post).map(p => cosh(p.likeCount))
 * }}}
 */
def cosh[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "COSH",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the hyperbolic tangent. Maps to `TANH(expr)`.
 *
 * {{{
 * from(Post).map(p => tanh(p.likeCount))
 * }}}
 */
def tanh[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "TANH",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the arc sine. Maps to `ASIN(expr)`.
 *
 * {{{
 * from(Post).map(p => asin(p.likeCount))
 * }}}
 */
def asin[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ASIN",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the arc cosine. Maps to `ACOS(expr)`.
 *
 * {{{
 * from(Post).map(p => acos(p.likeCount))
 * }}}
 */
def acos[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ACOS",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the arc tangent. Maps to `ATAN(expr)`.
 *
 * {{{
 * from(Post).map(p => atan(p.likeCount))
 * }}}
 */
def atan[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ATAN",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the logarithm to a given base. Maps to `LOG(base, expr)`.
 *
 * {{{
 * from(Post).map(p => log(10, p.likeCount))
 * }}}
 */
def log[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[BigDecimal], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LOG",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the base-10 logarithm. Maps to `LOG10(expr)`.
 *
 * {{{
 * from(Post).map(p => log10(p.likeCount))
 * }}}
 */
def log10[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LOG10",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the natural logarithm. Maps to `LN(expr)`.
 *
 * {{{
 * from(Post).map(p => ln(p.likeCount))
 * }}}
 */
def ln[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LN",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes e raised to a power. Maps to `EXP(expr)`.
 *
 * {{{
 * from(Post).map(p => exp(p.likeCount))
 * }}}
 */
def exp[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "EXP",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes a value raised to a power. Maps to `POWER(x, y)`.
 *
 * {{{
 * from(Post).map(p => power(p.likeCount, 2))
 * }}}
 */
def power[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[BigDecimal], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "POWER",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the square root. Maps to `SQRT(expr)`.
 *
 * {{{
 * from(Post).map(p => sqrt(p.likeCount))
 * }}}
 */
def sqrt[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "SQRT",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Rounds down to the nearest integer. Maps to `FLOOR(expr)`.
 *
 * {{{
 * from(Post).map(p => floor(p.likeCount))
 * }}}
 */
def floor[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[Long], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "FLOOR",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Rounds up to the nearest integer. Maps to `CEIL(expr)`.
 *
 * {{{
 * from(Post).map(p => ceil(p.likeCount))
 * }}}
 */
def ceil[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[Long], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "CEIL",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Rounds to a given number of decimal places. Maps to
 * `ROUND(expr, n)`.
 *
 * {{{
 * from(Post).map(p => round(p.likeCount, 2))
 * }}}
 */
def round[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[BigDecimal], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ROUND",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the bucket number for a value in a histogram. Maps to
 * `WIDTH_BUCKET(x, min, max, num)`.
 *
 * {{{
 * from(Post).map(p => widthBucket(p.likeCount, 0, 100, 10))
 * }}}
 */
def widthBucket[A, B, C, D, CL <: Int](x: A, min: B, max: C, num: D)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ac: AsExpr[C, CL],
    ad: AsExpr[D, CL],
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    nc: SqlNumber[ac.R],
    nd: SqlNumber[ad.R],
    cb: CombineKind[aa.K, ab.K],
    cc: CombineKind[cb.R, ac.K],
    c: CombineKind[cc.R, ad.K]
): Expr[Option[Int], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "WIDTH_BUCKET",
            aa.asExpr(x).asSqlExpr ::
            ab.asExpr(min).asSqlExpr ::
            ac.asExpr(max).asSqlExpr ::
            ad.asExpr(num).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Parses a string expression as a JSON expression. Maps to `JSON(expr)`.
 *
 * {{{
 * from(Log).map(l => json(l.data))
 * }}}
 */
def json[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    s: SqlString[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[Json], Composite[kt.R]] =
    Expr(
        SqlExpr.JsonParseFunc(
            a.asExpr(x).asSqlExpr,
            None,
            Some(SqlJsonUniquenessMode.With)
        )
    )

/**
 * Extracts a scalar JSON value at a path. Maps to
 * `JSON_VALUE(expr, path)`.
 *
 * {{{
 * from(Log).map(l => jsonValue(json(l.data), "$.name"))
 * }}}
 */
def jsonValue[A, P, CL <: Int](x: A, path: P)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ap: AsExpr[P, CL],
    j: SqlJson[aa.R],
    s: SqlString[ap.R],
    c: CombineKind[aa.K, ap.K]
): Expr[Option[String], c.R] =
    Expr(
        SqlExpr.JsonValueFunc(
            aa.asExpr(x).asSqlExpr,
            ap.asExpr(path).asSqlExpr,
            Nil,
            None,
            None,
            None
        )
    )

/**
 * Extracts a JSON object or array at a path. Maps to
 * `JSON_QUERY(expr, path)`.
 *
 * {{{
 * from(Log).map(l => jsonQuery(json(l.data), "$.items"))
 * }}}
 */
def jsonQuery[A, P, CL <: Int](x: A, path: P)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ap: AsExpr[P, CL],
    j: SqlJson[aa.R],
    s: SqlString[ap.R],
    c: CombineKind[aa.K, ap.K]
): Expr[Option[Json], c.R] =
    Expr(
        SqlExpr.JsonQueryFunc(
            aa.asExpr(x).asSqlExpr,
            ap.asExpr(path).asSqlExpr,
            Nil,
            None,
            None,
            None,
            None,
            None
        )
    )

/**
 * Tests whether a JSON path exists. Maps to `JSON_EXISTS(expr, path)`.
 *
 * {{{
 * from(Log).filter(l => jsonExists(json(l.data), "$.name"))
 * }}}
 */
def jsonExists[A, P, CL <: Int](x: A, path: P)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ap: AsExpr[P, CL],
    j: SqlJson[aa.R],
    s: SqlString[ap.R],
    c: CombineKind[aa.K, ap.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        SqlExpr.JsonExistsFunc(
            aa.asExpr(x).asSqlExpr,
            ap.asExpr(path).asSqlExpr,
            Nil,
            None
        )
    )

/**
 * A key and value pair for `jsonObject`.
 */
final case class JsonObjectPair[KS <: Tuple](key: SqlExpr, value: SqlExpr)

extension [K, CL <: Int](key: K)(using qc: QueryContext[CL], ak: AsExpr[K, CL], ask: AsSqlExpr[ak.R], ktk: KindToTuple[ak.K])
    /**
     * Pairs a key with a value for `jsonObject`. Maps to
     * `key VALUE value`.
     *
     * {{{
     * jsonObject(p.id value p.title)
     * }}}
     */
    infix def value[V](value: V)(using
        av: AsExpr[V, CL],
        asv: AsSqlExpr[av.R],
        ktv: KindToTuple[av.K],
        c: CombineKindTuple[ktk.R, ktv.R]
    ): JsonObjectPair[c.R] =
        JsonObjectPair(ak.asExpr(key).asSqlExpr, av.asExpr(value).asSqlExpr)

/**
 * Converts key and value pairs into the argument list for
 * `jsonObject`.
 */
trait AsJsonObject[T]:
    /**
     * The combined kind tuple of the key and value pairs.
     */
    type KS <: Tuple

    /**
     * Converts the value to a list of `JsonObjectPair`.
     */
    def asJsonObjects(x: T): List[JsonObjectPair[?]]

object AsJsonObject:
    type Aux[T, OKS <: Tuple] = AsJsonObject[T]:
        type KS = OKS

    given pair[EKS <: Tuple]: Aux[JsonObjectPair[EKS], EKS] =
        new AsJsonObject[JsonObjectPair[EKS]]:
            type KS = EKS

            def asJsonObjects(x: JsonObjectPair[EKS]): List[JsonObjectPair[?]] =
                x :: Nil

    given tuple[EKS <: Tuple, T <: Tuple](using
        j: AsJsonObject[T],
        c: CombineKindTuple[EKS, j.KS]
    ): Aux[JsonObjectPair[EKS] *: T, c.R] =
        new AsJsonObject[JsonObjectPair[EKS] *: T]:
            type KS = c.R

            def asJsonObjects(x: JsonObjectPair[EKS] *: T): List[JsonObjectPair[?]] =
                x.head :: j.asJsonObjects(x.tail)

    given tuple1[EKS <: Tuple](using
        c: CombineKindTuple[EKS, EmptyTuple]
    ): Aux[JsonObjectPair[EKS] *: EmptyTuple, c.R] =
        new AsJsonObject[JsonObjectPair[EKS] *: EmptyTuple]:
            type KS = c.R

            def asJsonObjects(x: JsonObjectPair[EKS] *: EmptyTuple): List[JsonObjectPair[?]] =
                x.head :: Nil

/**
 * Constructs a JSON object from key and value pairs. Maps to
 * `JSON_OBJECT(key VALUE value, ...)`.
 *
 * {{{
 * from(Post).map(p => jsonObject(p.id.value(p.title)))
 * }}}
 */
def jsonObject[T, CL <: Int](items: T)(using
    qc: QueryContext[CL],
    j: AsJsonObject[T]
): Expr[Option[Json], Composite[j.KS]] =
    Expr(
        SqlExpr.JsonObjectFunc(
            j.asJsonObjects(items).map(i => SqlJsonObjectItem(i.key, i.value)),
            None,
            None,
            None
        )
    )

/**
 * Constructs a JSON array from elements. Maps to
 * `JSON_ARRAY(expr, ...)`.
 *
 * {{{
 * from(Post).map(p => jsonArray(p.id, p.title))
 * }}}
 */
def jsonArray[A, CL <: Int](items: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[Json], Composite[kt.R]] =
    Expr(
        SqlExpr.JsonArrayFunc(
            a.asExprs(items).map(i => SqlJsonArrayItem(i.asSqlExpr, None)),
            None,
            None
        )
    )

/**
 * Returns the current date. Maps to `CURRENT_DATE`.
 *
 * {{{
 * from(Post).map(p => currentDate())
 * }}}
 */
def currentDate[CL <: Int]()(using QueryContext[CL]): Expr[LocalDate, Composite[EmptyTuple]] =
    Expr(
        SqlExpr.IdentFunc("CURRENT_DATE")
    )

/**
 * Returns the current time with timezone. Maps to `CURRENT_TIME`.
 *
 * {{{
 * from(Post).map(p => currentTime())
 * }}}
 */
def currentTime[CL <: Int]()(using QueryContext[CL]): Expr[OffsetTime, Composite[EmptyTuple]] =
    Expr(
        SqlExpr.IdentFunc("CURRENT_TIME")
    )

/**
 * Returns the current timestamp with timezone. Maps to
 * `CURRENT_TIMESTAMP`.
 *
 * {{{
 * from(Post).map(p => currentTimestamp())
 * }}}
 */
def currentTimestamp[CL <: Int]()(using QueryContext[CL]): Expr[OffsetDateTime, Composite[EmptyTuple]] =
    Expr(
        SqlExpr.IdentFunc("CURRENT_TIMESTAMP")
    )

/**
 * Returns the current local time without timezone. Maps to
 * `LOCALTIME`.
 *
 * {{{
 * from(Post).map(p => localTime())
 * }}}
 */
def localTime[CL <: Int]()(using QueryContext[CL]): Expr[LocalTime, Composite[EmptyTuple]] =
    Expr(
        SqlExpr.IdentFunc("LOCALTIME")
    )

/**
 * Returns the current local timestamp without timezone. Maps to
 * `LOCALTIMESTAMP`.
 *
 * {{{
 * from(Post).map(p => localTimestamp())
 * }}}
 */
def localTimestamp[CL <: Int]()(using QueryContext[CL]): Expr[LocalDateTime, Composite[EmptyTuple]] =
    Expr(
        SqlExpr.IdentFunc("LOCALTIMESTAMP")
    )

/**
 * Constructs a geometry from WKT text with an SRID. Maps to
 * `ST_GeomFromText(text, srid)`. Part of the ISO/IEC 13249
 * spatial standard.
 *
 * {{{
 * from(Place).map(p => stGeomFromText(p.wkt, 4326))
 * }}}
 */
def stGeomFromText[A, S, CL <: Int](x: A, srid: S)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    as: AsExpr[S, CL],
    s: SqlString[aa.R],
    n: SqlNumber[as.R],
    c: CombineKind[aa.K, as.K]
): Expr[Option[Geometry], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_GeomFromText",
            aa.asExpr(x).asSqlExpr :: as.asExpr(srid).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Converts a geometry to WKT text. Maps to `ST_AsText(expr)`.
 * Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stAsText(p.location))
 * }}}
 */
def stAsText[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    g: SqlGeometry[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[String], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_AsText",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Converts a geometry to GeoJSON text. Maps to
 * `ST_AsGeoJson(expr)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stAsGeoJson(p.location))
 * }}}
 */
def stAsGeoJson[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    g: SqlGeometry[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[String], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_AsGeoJson",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the geometry type name. Maps to
 * `ST_GeometryType(expr)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stGeometryType(p.location))
 * }}}
 */
def stGeometryType[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    g: SqlGeometry[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[String], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_GeometryType",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the X coordinate of a point. Maps to `ST_X(expr)`.
 * Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stX(p.location))
 * }}}
 */
def stX[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    g: SqlGeometry[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_X",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the Y coordinate of a point. Maps to `ST_Y(expr)`.
 * Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stY(p.location))
 * }}}
 */
def stY[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    g: SqlGeometry[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Y",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the area of a geometry. Maps to `ST_Area(expr)`.
 * Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stArea(p.location))
 * }}}
 */
def stArea[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    g: SqlGeometry[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Area",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the length of a geometry. Maps to `ST_Length(expr)`.
 * Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stLength(p.location))
 * }}}
 */
def stLength[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    g: SqlGeometry[a.R],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Length",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Tests whether one geometry contains another. Maps to
 * `ST_Contains(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).filter(p => stContains(p.location, p.location))
 * }}}
 */
def stContains[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Contains",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Tests whether one geometry is within another. Maps to
 * `ST_Within(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).filter(p => stWithin(p.location, p.location))
 * }}}
 */
def stWithin[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Within",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Tests whether two geometries intersect. Maps to
 * `ST_Intersects(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).filter(p => stIntersects(p.location, p.location))
 * }}}
 */
def stIntersects[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Intersects",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Tests whether two geometries touch. Maps to
 * `ST_Touches(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).filter(p => stTouches(p.location, p.location))
 * }}}
 */
def stTouches[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Touches",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Tests whether two geometries overlap. Maps to
 * `ST_Overlaps(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).filter(p => stOverlaps(p.location, p.location))
 * }}}
 */
def stOverlaps[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Overlaps",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Tests whether two geometries cross. Maps to
 * `ST_Crosses(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).filter(p => stCrosses(p.location, p.location))
 * }}}
 */
def stCrosses[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Crosses",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Tests whether two geometries are disjoint. Maps to
 * `ST_Disjoint(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).filter(p => stDisjoint(p.location, p.location))
 * }}}
 */
def stDisjoint[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Disjoint",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the intersection of two geometries. Maps to
 * `ST_Intersection(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stIntersection(p.location, p.location))
 * }}}
 */
def stIntersection[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Intersection",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the union of two geometries. Maps to
 * `ST_Union(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stUnion(p.location, p.location))
 * }}}
 */
def stUnion[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Geometry], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Union",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the difference of two geometries. Maps to
 * `ST_Difference(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stDifference(p.location, p.location))
 * }}}
 */
def stDifference[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Geometry], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Difference",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the symmetric difference of two geometries. Maps to
 * `ST_SymDifference(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stSymDifference(p.location, p.location))
 * }}}
 */
def stSymDifference[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Geometry], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_SymDifference",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Computes the distance between two geometries. Maps to
 * `ST_Distance(x, y)`. Part of the ISO/IEC 13249 spatial standard.
 *
 * {{{
 * from(Place).map(p => stDistance(p.location, p.location))
 * }}}
 */
def stDistance[A, B, CL <: Int](x: A, y: B)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL],
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[BigDecimal], c.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Distance",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the rank of each row with gaps. Maps to `RANK()`.
 * Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => rank().over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def rank[CL <: Int]()(using QueryContext[CL]): WindowFunc[Long, EmptyTuple] =
    WindowFunc(
        SqlExpr.GeneralFunc(
            None,
            "RANK",
            Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the rank of each row without gaps. Maps to
 * `DENSE_RANK()`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => denseRank().over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def denseRank[CL <: Int]()(using QueryContext[CL]): WindowFunc[Long, EmptyTuple] =
    WindowFunc(
        SqlExpr.GeneralFunc(
            None,
            "DENSE_RANK",
            Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the percentile rank of each row. Maps to `PERCENT_RANK()`.
 * Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => percentRank().over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def percentRank[CL <: Int]()(using QueryContext[CL]): WindowFunc[BigDecimal, EmptyTuple] =
    WindowFunc(
        SqlExpr.GeneralFunc(
            None,
            "PERCENT_RANK",
            Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the cumulative distribution of each row. Maps to
 * `CUME_DIST()`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => cumeDist().over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def cumeDist[CL <: Int]()(using QueryContext[CL]): WindowFunc[Long, EmptyTuple] =
    WindowFunc(
        SqlExpr.GeneralFunc(
            None,
            "CUME_DIST",
            Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the sequential row number. Maps to `ROW_NUMBER()`.
 * Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => rowNumber().over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def rowNumber[CL <: Int]()(using QueryContext[CL]): WindowFunc[Long, EmptyTuple] =
    WindowFunc(
        SqlExpr.GeneralFunc(
            None,
            "ROW_NUMBER",
            Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Divides rows into a specified number of buckets. Maps to
 * `NTILE(n)`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => ntile(4).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def ntile[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    n: SqlNumber[a.R],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R]
): WindowFunc[Long, kt.R] =
    WindowFunc(
        SqlExpr.GeneralFunc(
            None,
            "NTILE",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the first value in a window frame. Maps to
 * `FIRST_VALUE(expr)`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => firstValue(p.title).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def firstValue[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowFunc[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "FIRST_VALUE",
                a.asExpr(x).asSqlExpr :: Nil,
                None
            )
        )
    )

/**
 * Returns the first non-null value in a window frame. Maps to
 * `FIRST_VALUE(expr) IGNORE NULLS`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => firstValueIgnoreNulls(p.title).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def firstValueIgnoreNulls[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowFunc[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "FIRST_VALUE",
                a.asExpr(x).asSqlExpr :: Nil,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

/**
 * Returns the last value in a window frame. Maps to
 * `LAST_VALUE(expr)`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => lastValue(p.title).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def lastValue[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowFunc[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "LAST_VALUE",
                a.asExpr(x).asSqlExpr :: Nil,
                None
            )
        )
    )

/**
 * Returns the last non-null value in a window frame. Maps to
 * `LAST_VALUE(expr) IGNORE NULLS`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => lastValueIgnoreNulls(p.title).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def lastValueIgnoreNulls[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowFunc[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "LAST_VALUE",
                a.asExpr(x).asSqlExpr :: Nil,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

/**
 * Returns the N-th value in a window frame from the first row.
 * Maps to `NTH_VALUE(expr, n)`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => nthValue(p.title, 2).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def nthValue[A, N, CL <: Int](x: A, n: N)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    an: AsExpr[N, CL],
    asa: AsSqlExpr[aa.R],
    nn: SqlNumber[an.R],
    kt: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInWindow[kt.R],
    in: CanInWindow[ktn.R],
    c: CombineKindTuple[kt.R, ktn.R],
    to: ToOption[WindowFunc[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NthValueFunc(
                aa.asExpr(x).asSqlExpr,
                an.asExpr(n).asSqlExpr,
                None,
                None
            )
        )
    )

/**
 * Returns the N-th non-null value from the first row. Maps to
 * `NTH_VALUE(expr, n) IGNORE NULLS`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => nthValueIgnoreNulls(p.title, 2).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def nthValueIgnoreNulls[A, N, CL <: Int](x: A, n: N)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    an: AsExpr[N, CL],
    asa: AsSqlExpr[aa.R],
    nn: SqlNumber[an.R],
    kt: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInWindow[kt.R],
    in: CanInWindow[ktn.R],
    c: CombineKindTuple[kt.R, ktn.R],
    to: ToOption[WindowFunc[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NthValueFunc(
                aa.asExpr(x).asSqlExpr,
                an.asExpr(n).asSqlExpr,
                None,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

/**
 * Returns the N-th value from the last row. Maps to
 * `NTH_VALUE(expr, n) FROM LAST`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => nthValueFromLast(p.title, 2).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def nthValueFromLast[A, N, CL <: Int](x: A, n: N)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    an: AsExpr[N, CL],
    asa: AsSqlExpr[aa.R],
    nn: SqlNumber[an.R],
    kt: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInWindow[kt.R],
    in: CanInWindow[ktn.R],
    c: CombineKindTuple[kt.R, ktn.R],
    to: ToOption[WindowFunc[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NthValueFunc(
                aa.asExpr(x).asSqlExpr,
                an.asExpr(n).asSqlExpr,
                Some(SqlNthValueFromMode.Last),
                None
            )
        )
    )

/**
 * Returns the N-th non-null value from the last row. Maps to
 * `NTH_VALUE(expr, n) FROM LAST IGNORE NULLS`. Must have an
 * `over` clause.
 *
 * {{{
 * from(Post).map(p => nthValueFromLastIgnoreNulls(p.title, 2).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def nthValueFromLastIgnoreNulls[A, N, CL <: Int](x: A, n: N)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    an: AsExpr[N, CL],
    asa: AsSqlExpr[aa.R],
    nn: SqlNumber[an.R],
    kt: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInWindow[kt.R],
    in: CanInWindow[ktn.R],
    c: CombineKindTuple[kt.R, ktn.R],
    to: ToOption[WindowFunc[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NthValueFunc(
                aa.asExpr(x).asSqlExpr,
                an.asExpr(n).asSqlExpr,
                Some(SqlNthValueFromMode.Last),
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

/**
 * Returns the value of a preceding row. Maps to `LAG(expr)`.
 * Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => lag(p.title).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def lag[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowFunc[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "LAG",
                a.asExpr(x).asSqlExpr :: Nil,
                None
            )
        )
    )

/**
 * Returns the non-null value of a preceding row. Maps to
 * `LAG(expr) IGNORE NULLS`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => lagIgnoreNulls(p.title).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def lagIgnoreNulls[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowFunc[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "LAG",
                a.asExpr(x).asSqlExpr :: Nil,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

/**
 * Returns the value offset rows before. Maps to `LAG(expr, n)`.
 * Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => lag(p.title, 2).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def lag[A, O, CL <: Int](x: A, offset: O)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ao: AsExpr[O, CL],
    asa: AsSqlExpr[aa.R],
    no: SqlNumber[ao.R],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    c: CombineKindTuple[kt.R, kto.R],
    to: ToOption[WindowFunc[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "LAG",
                aa.asExpr(x).asSqlExpr :: ao.asExpr(offset).asSqlExpr :: Nil,
                None
            )
        )
    )

/**
 * Returns the non-null value offset rows before. Maps to
 * `LAG(expr, n) IGNORE NULLS`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => lagIgnoreNulls(p.title, 2).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def lagIgnoreNulls[A, O, CL <: Int](x: A, offset: O)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ao: AsExpr[O, CL],
    asa: AsSqlExpr[aa.R],
    no: SqlNumber[ao.R],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    c: CombineKindTuple[kt.R, kto.R],
    to: ToOption[WindowFunc[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "LAG",
                aa.asExpr(x).asSqlExpr :: ao.asExpr(offset).asSqlExpr :: Nil,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

/**
 * Returns the value offset rows before, with a default for missing
 * rows. Maps to `LAG(expr, n, d)`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => lag(p.title, 2, "N/A").over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def lag[A, O, D, CL <: Int](x: A, offset: O, default: D)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ao: AsExpr[O, CL],
    ad: AsExpr[D, CL],
    asa: AsSqlExpr[aa.R],
    no: SqlNumber[ao.R],
    asd: AsSqlExpr[ad.R],
    r: Return[aa.R, ad.R],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ktd: KindToTuple[ad.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    id: CanInWindow[ktd.R],
    co: CombineKindTuple[kt.R, kto.R],
    c: CombineKindTuple[co.R, ktd.R],
    to: ToOption[WindowFunc[r.R, c.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "LAG",
                aa.asExpr(x).asSqlExpr ::
                    ao.asExpr(offset).asSqlExpr ::
                    ad.asExpr(default).asSqlExpr :: Nil
                ,
                None
            )
        )
    )

/**
 * Returns the non-null value offset rows before, with a default
 * for missing rows. Maps to `LAG(expr, n, d) IGNORE NULLS`.
 * Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => lagIgnoreNulls(p.title, 2, "N/A").over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def lagIgnoreNulls[A, O, D, CL <: Int](x: A, offset: O, default: D)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ao: AsExpr[O, CL],
    ad: AsExpr[D, CL],
    asa: AsSqlExpr[aa.R],
    no: SqlNumber[ao.R],
    asd: AsSqlExpr[ad.R],
    r: Return[aa.R, ad.R],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ktd: KindToTuple[ad.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    id: CanInWindow[ktd.R],
    co: CombineKindTuple[kt.R, kto.R],
    c: CombineKindTuple[co.R, ktd.R],
    to: ToOption[WindowFunc[r.R, c.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "LAG",
                aa.asExpr(x).asSqlExpr ::
                    ao.asExpr(offset).asSqlExpr ::
                    ad.asExpr(default).asSqlExpr :: Nil
                ,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

/**
 * Returns the value of a following row. Maps to `LEAD(expr)`.
 * Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => lead(p.title).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def lead[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowFunc[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "LEAD",
                a.asExpr(x).asSqlExpr :: Nil,
                None
            )
        )
    )

/**
 * Returns the non-null value of a following row. Maps to
 * `LEAD(expr) IGNORE NULLS`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => leadIgnoreNulls(p.title).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def leadIgnoreNulls[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    as: AsSqlExpr[a.R],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowFunc[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "LEAD",
                a.asExpr(x).asSqlExpr :: Nil,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

/**
 * Returns the value offset rows after. Maps to `LEAD(expr, n)`.
 * Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => lead(p.title, 2).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def lead[A, O, CL <: Int](x: A, offset: O)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ao: AsExpr[O, CL],
    asa: AsSqlExpr[aa.R],
    no: SqlNumber[ao.R],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    c: CombineKindTuple[kt.R, kto.R],
    to: ToOption[WindowFunc[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "LEAD",
                aa.asExpr(x).asSqlExpr :: ao.asExpr(offset).asSqlExpr :: Nil,
                None
            )
        )
    )

/**
 * Returns the non-null value offset rows after. Maps to
 * `LEAD(expr, n) IGNORE NULLS`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => leadIgnoreNulls(p.title, 2).over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def leadIgnoreNulls[A, O, CL <: Int](x: A, offset: O)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ao: AsExpr[O, CL],
    asa: AsSqlExpr[aa.R],
    no: SqlNumber[ao.R],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    c: CombineKindTuple[kt.R, kto.R],
    to: ToOption[WindowFunc[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "LEAD",
                aa.asExpr(x).asSqlExpr :: ao.asExpr(offset).asSqlExpr :: Nil,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

/**
 * Returns the value offset rows after, with a default for missing
 * rows. Maps to `LEAD(expr, n, d)`. Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => lead(p.title, 2, "N/A").over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def lead[A, O, D, CL <: Int](x: A, offset: O, default: D)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ao: AsExpr[O, CL],
    ad: AsExpr[D, CL],
    asa: AsSqlExpr[aa.R],
    no: SqlNumber[ao.R],
    asd: AsSqlExpr[ad.R],
    r: Return[aa.R, ad.R],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ktd: KindToTuple[ad.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    id: CanInWindow[ktd.R],
    co: CombineKindTuple[kt.R, kto.R],
    c: CombineKindTuple[co.R, ktd.R],
    to: ToOption[WindowFunc[r.R, c.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "LEAD",
                aa.asExpr(x).asSqlExpr ::
                    ao.asExpr(offset).asSqlExpr ::
                    ad.asExpr(default).asSqlExpr :: Nil
                ,
                None
            )
        )
    )

/**
 * Returns the non-null value offset rows after, with a default
 * for missing rows. Maps to `LEAD(expr, n, d) IGNORE NULLS`.
 * Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => leadIgnoreNulls(p.title, 2, "N/A").over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def leadIgnoreNulls[A, O, D, CL <: Int](x: A, offset: O, default: D)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ao: AsExpr[O, CL],
    ad: AsExpr[D, CL],
    asa: AsSqlExpr[aa.R],
    no: SqlNumber[ao.R],
    asd: AsSqlExpr[ad.R],
    r: Return[aa.R, ad.R],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ktd: KindToTuple[ad.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    id: CanInWindow[ktd.R],
    co: CombineKindTuple[kt.R, kto.R],
    c: CombineKindTuple[co.R, ktd.R],
    to: ToOption[WindowFunc[r.R, c.R]]
): to.R =
    to.toOption(
        WindowFunc(
            SqlExpr.NullsTreatmentFunc(
                "LEAD",
                aa.asExpr(x).asSqlExpr ::
                    ao.asExpr(offset).asSqlExpr ::
                    ad.asExpr(default).asSqlExpr :: Nil
                ,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )