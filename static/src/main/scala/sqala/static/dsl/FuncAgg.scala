package sqala.static.dsl

import sqala.ir.IrExpr
import sqala.ast.quantifier.SqlQuantifier
import sqala.metadata.SqlNumber
import sqala.metadata.AsSqlExpr
import sqala.metadata.SqlString

/**
 * Counts all rows. Maps to `COUNT(*)`.
 *
 * {{{
 * from(Post).map(p => count())
 * }}}
 */
def count[CL <: Int]()(using QueryContext[CL]): AggExpr[Long, EmptyTuple] =
    AggExpr(
        IrExpr.CountAsteriskFunc(None)
    )

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
): AggExpr[Long, kt.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "COUNT",
            a.asExpr(x).asIrExpr :: Nil,
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
): AggExpr[Long, kt.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            Some(SqlQuantifier.Distinct),
            "COUNT",
            a.asExpr(x).asIrExpr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
    to: ToOption[AggExpr[a.R, kt.R]]
): to.R =
    to.toOption(
        AggExpr(
            IrExpr.GeneralFunc(
                None,
                "SUM",
                a.asExpr(x).asIrExpr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R]
): AggExpr[Option[BigDecimal], kt.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "AVG",
            a.asExpr(x).asIrExpr :: Nil,
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
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
    to: ToOption[AggExpr[a.R, kt.R]]
): to.R =
    to.toOption(
        AggExpr(
            IrExpr.GeneralFunc(
                None,
                "MAX",
                a.asExpr(x).asIrExpr :: Nil,
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
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
    to: ToOption[AggExpr[a.R, kt.R]]
): to.R =
    to.toOption(
        AggExpr(
            IrExpr.GeneralFunc(
                None,
                "MIN",
                a.asExpr(x).asIrExpr :: Nil,
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
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
    to: ToOption[AggExpr[a.R, kt.R]]
): to.R =
    to.toOption(
        AggExpr(
            IrExpr.GeneralFunc(
                None,
                "ANY_VALUE",
                a.asExpr(x).asIrExpr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R]
): AggExpr[Option[BigDecimal], kt.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "STDDEV_POP",
            a.asExpr(x).asIrExpr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R]
): AggExpr[Option[BigDecimal], kt.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "STDDEV_SAMP",
            a.asExpr(x).asIrExpr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R]
): AggExpr[Option[BigDecimal], kt.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "VAR_POP",
            a.asExpr(x).asIrExpr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R]
): AggExpr[Option[BigDecimal], kt.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "VAR_SAMP",
            a.asExpr(x).asIrExpr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): AggExpr[Option[BigDecimal], c.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "COVAR_POP",
            aa.asExpr(x).asIrExpr :: ab.asExpr(y).asIrExpr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): AggExpr[Option[BigDecimal], c.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "COVAR_SAMP",
            aa.asExpr(x).asIrExpr :: ab.asExpr(y).asIrExpr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): AggExpr[Option[BigDecimal], c.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "CORR",
            aa.asExpr(x).asIrExpr :: ab.asExpr(y).asIrExpr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): AggExpr[Option[BigDecimal], c.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "REGR_SLOPE",
            aa.asExpr(x).asIrExpr :: ab.asExpr(y).asIrExpr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): AggExpr[Option[BigDecimal], c.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "REGR_INTERCEPT",
            aa.asExpr(x).asIrExpr :: ab.asExpr(y).asIrExpr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): AggExpr[Long, c.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "REGR_COUNT",
            aa.asExpr(x).asIrExpr :: ab.asExpr(y).asIrExpr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): AggExpr[Option[BigDecimal], c.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "REGR_R2",
            aa.asExpr(x).asIrExpr :: ab.asExpr(y).asIrExpr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): AggExpr[Option[BigDecimal], c.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "REGR_AVGX",
            aa.asExpr(x).asIrExpr :: ab.asExpr(y).asIrExpr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): AggExpr[Option[BigDecimal], c.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "REGR_AVGY",
            aa.asExpr(x).asIrExpr :: ab.asExpr(y).asIrExpr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): AggExpr[Option[BigDecimal], c.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "REGR_SXX",
            aa.asExpr(x).asIrExpr :: ab.asExpr(y).asIrExpr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): AggExpr[Option[BigDecimal], c.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "REGR_SYY",
            aa.asExpr(x).asIrExpr :: ab.asExpr(y).asIrExpr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktb: KindToTuple[ab.K],
    ia: CanInAgg[kta.R],
    ib: CanInAgg[ktb.R],
    c: CombineKindTuple[kta.R, ktb.R]
): AggExpr[Option[BigDecimal], c.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "REGR_SXY",
            aa.asExpr(x).asIrExpr :: ab.asExpr(y).asIrExpr :: Nil,
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
    nt: ValidateArgType[SqlNumber, at.R, "numeric"],
    ns: ValidateArgType[SqlNumber, ST, "numeric"],
    kt: KindToTuple[at.K],
    kts: KindToTuple[SK],
    it: CanInAgg[kt.R],
    is: CanInAgg[kts.R],
    c: CombineKindTuple[kt.R, kts.R],
    to: ToOption[AggExpr[ST, c.R]]
): to.R =
    to.toOption(
        AggExpr(
            IrExpr.GeneralFunc(
                None,
                "PERCENTILE_CONT",
                at.asExpr(x).asIrExpr :: Nil,
                Nil,
                withinGroup.asIrOrderingItem :: Nil,
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
    nt: ValidateArgType[SqlNumber, at.R, "numeric"],
    ns: ValidateArgType[SqlNumber, ST, "numeric"],
    kt: KindToTuple[at.K],
    kts: KindToTuple[SK],
    it: CanInAgg[kt.R],
    is: CanInAgg[kts.R],
    c: CombineKindTuple[kt.R, kts.R],
    to: ToOption[AggExpr[ST, c.R]]
): to.R =
    to.toOption(
        AggExpr(
            IrExpr.GeneralFunc(
                None,
                "PERCENTILE_DISC",
                at.asExpr(x).asIrExpr :: Nil,
                Nil,
                withinGroup.asIrOrderingItem :: Nil,
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
    st: ValidateArgType[SqlString, at.R, "string"],
    ss: ValidateArgType[SqlString, as.R, "string"],
    kt: KindToTuple[at.K],
    kts: KindToTuple[as.K],
    ktw: KindToTuple[SK],
    it: CanInAgg[kt.R],
    is: CanInAgg[kts.R],
    iw: CanInAgg[ktw.R],
    cs: CombineKindTuple[kt.R, kts.R],
    c: CombineKindTuple[cs.R, ktw.R]
): AggExpr[Option[String], c.R] =
    AggExpr(
        IrExpr.ListAggFunc(
            None,
            at.asExpr(x).asIrExpr,
            as.asExpr(separator).asIrExpr,
            withinGroup.asIrOrderingItem :: Nil,
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
    st: ValidateArgType[SqlString, at.R, "string"],
    ss: ValidateArgType[SqlString, as.R, "string"],
    kt: KindToTuple[at.K],
    kts: KindToTuple[as.K],
    ktw: KindToTuple[SK],
    it: CanInAgg[kt.R],
    is: CanInAgg[kts.R],
    iw: CanInAgg[ktw.R],
    cs: CombineKindTuple[kt.R, kts.R],
    c: CombineKindTuple[cs.R, ktw.R]
): AggExpr[Option[String], c.R] =
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
    st: ValidateArgType[SqlString, at.R, "string"],
    ss: ValidateArgType[SqlString, as.R, "string"],
    kt: KindToTuple[at.K],
    kts: KindToTuple[as.K],
    ktw: KindToTuple[SK],
    it: CanInAgg[kt.R],
    is: CanInAgg[kts.R],
    iw: CanInAgg[ktw.R],
    cs: CombineKindTuple[kt.R, kts.R],
    c: CombineKindTuple[cs.R, ktw.R]
): AggExpr[Option[String], c.R] =
    listAgg(x, separator, withinGroup)