package sqala.dynamic.dsl

import sqala.ast.expr.*
import sqala.ast.quantifier.SqlQuantifier

/**
 * `COUNT(*)`: counts all rows. Maps to `COUNT(*)`.
 *
 * {{{
 * count()
 * }}}
 */
def count(): Expr =
    Expr(SqlExpr.CountAsteriskFunc(None, None))

/**
 * `COUNT(x)`: counts non-null values of `x`. Maps to `COUNT(x)`.
 *
 * {{{
 * count(column("id"))
 * }}}
 */
def count(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "COUNT",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * `COUNT(DISTINCT x)`: counts distinct non-null values. Maps to
 * `COUNT(DISTINCT x)`.
 *
 * {{{
 * countDistinct(column("author"))
 * }}}
 */
def countDistinct(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            Some(SqlQuantifier.Distinct),
            "COUNT",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Sum of values. Maps to `SUM(x)`.
 *
 * {{{
 * sum(column("amount"))
 * }}}
 */
def sum(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "SUM",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Average of values. Maps to `AVG(x)`.
 *
 * {{{
 * avg(column("amount"))
 * }}}
 */
def avg(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "AVG",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Maximum value. Maps to `MAX(x)`.
 *
 * {{{
 * max(column("created_at"))
 * }}}
 */
def max(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "MAX",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Minimum value. Maps to `MIN(x)`.
 *
 * {{{
 * min(column("created_at"))
 * }}}
 */
def min(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "MIN",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Arbitrary value from the group. Maps to `ANY_VALUE(x)`.
 *
 * {{{
 * anyValue(column("name"))
 * }}}
 */
def anyValue(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ANY_VALUE",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Population standard deviation. Maps to `STDDEV_POP(x)`.
 *
 * {{{
 * stddevPop(column("score"))
 * }}}
 */
def stddevPop(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "STDDEV_POP",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Sample standard deviation. Maps to `STDDEV_SAMP(x)`.
 *
 * {{{
 * stddevSamp(column("score"))
 * }}}
 */
def stddevSamp(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "STDDEV_SAMP",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Population variance. Maps to `VAR_POP(x)`.
 *
 * {{{
 * varPop(column("score"))
 * }}}
 */
def varPop(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "VAR_POP",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Sample variance. Maps to `VAR_SAMP(x)`.
 *
 * {{{
 * varSamp(column("score"))
 * }}}
 */
def varSamp(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "VAR_SAMP",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Population covariance of two expressions. Maps to
 * `COVAR_POP(x, y)`.
 *
 * {{{
 * covarPop(column("x"), column("y"))
 * }}}
 */
def covarPop(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "COVAR_POP",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Sample covariance of two expressions. Maps to `COVAR_SAMP(x, y)`.
 *
 * {{{
 * covarSamp(column("x"), column("y"))
 * }}}
 */
def covarSamp(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "COVAR_SAMP",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Correlation coefficient of two expressions. Maps to `CORR(x, y)`.
 *
 * {{{
 * corr(column("x"), column("y"))
 * }}}
 */
def corr(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "CORR",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Slope of the least-squares regression line. Maps to
 * `REGR_SLOPE(y, x)`.
 *
 * {{{
 * regrSlope(column("y"), column("x"))
 * }}}
 */
def regrSlope(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_SLOPE",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Intercept of the least-squares regression line. Maps to
 * `REGR_INTERCEPT(y, x)`.
 *
 * {{{
 * regrIntercept(column("y"), column("x"))
 * }}}
 */
def regrIntercept(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_INTERCEPT",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Coefficient of determination (R²) of the regression. Maps to
 * `REGR_R2(y, x)`.
 *
 * {{{
 * regrR2(column("y"), column("x"))
 * }}}
 */
def regrR2(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_R2",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Number of non-null pairs used in the regression. Maps to
 * `REGR_COUNT(y, x)`.
 *
 * {{{
 * regrCount(column("y"), column("x"))
 * }}}
 */
def regrCount(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_COUNT",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Average of the independent variable. Maps to `REGR_AVGX(y, x)`.
 *
 * {{{
 * regrAvgx(column("y"), column("x"))
 * }}}
 */
def regrAvgx(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_AVGX",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Average of the dependent variable. Maps to `REGR_AVGY(y, x)`.
 *
 * {{{
 * regrAvgy(column("y"), column("x"))
 * }}}
 */
def regrAvgy(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_AVGY",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Sum of squares of the independent variable. Maps to `REGR_SXX(y, x)`.
 *
 * {{{
 * regrSxx(column("y"), column("x"))
 * }}}
 */
def regrSxx(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_SXX",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Sum of squares of the dependent variable. Maps to `REGR_SYY(y, x)`.
 *
 * {{{
 * regrSyy(column("y"), column("x"))
 * }}}
 */
def regrSyy(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_SYY",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Sum of products of the two variables. Maps to `REGR_SXY(y, x)`.
 *
 * {{{
 * regrSxy(column("y"), column("x"))
 * }}}
 */
def regrSxy(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGR_SXY",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Continuous percentile. Maps to
 * `PERCENTILE_CONT(x) WITHIN GROUP (ORDER BY ...)`.
 *
 * {{{
 * percentileCont(value(0.5), column("score").asc)
 * }}}
 */
def percentileCont(x: Expr, withinGroup: Order): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "PERCENTILE_CONT",
            x.asSqlExpr :: Nil,
            Nil,
            withinGroup.order :: Nil,
            None
        )
    )

/**
 * Discrete percentile. Maps to
 * `PERCENTILE_DISC(x) WITHIN GROUP (ORDER BY ...)`.
 *
 * {{{
 * percentileDisc(value(0.5), column("score").asc)
 * }}}
 */
def percentileDisc(x: Expr, withinGroup: Order): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "PERCENTILE_DISC",
            x.asSqlExpr :: Nil,
            Nil,
            withinGroup.order :: Nil,
            None
        )
    )

/**
 * Concatenates values in order, separated by `separator`. Maps to
 * `LISTAGG(x, separator) WITHIN GROUP (ORDER BY ...)`.
 *
 * {{{
 * listAgg(column("name"), value(","), column("id").asc)
 * }}}
 */
def listAgg(x: Expr, separator: Expr, withinGroup: Order): Expr =
    Expr(
        SqlExpr.ListAggFunc(
            None,
            x.asSqlExpr,
            separator.asSqlExpr,
            None,
            withinGroup.order :: Nil,
            None
        )
    )

/**
 * Alias of `listAgg`, matching PostgreSQL's `string_agg`. Maps to
 * `LISTAGG(...) WITHIN GROUP (ORDER BY ...)`.
 *
 * {{{
 * stringAgg(column("name"), value(","), column("id").asc)
 * }}}
 */
def stringAgg(x: Expr, separator: Expr, withinGroup: Order): Expr =
    listAgg(x, separator, withinGroup)

/**
 * Alias of `listAgg`, matching MySQL's `GROUP_CONCAT`. Maps to
 * `LISTAGG(...) WITHIN GROUP (ORDER BY ...)`.
 *
 * {{{
 * groupConcat(column("name"), value(","), column("id").asc)
 * }}}
 */
def groupConcat(x: Expr, separator: Expr, withinGroup: Order): Expr =
    listAgg(x, separator, withinGroup)

/**
 * Extracts a substring starting at `start`. Maps to
 * `SUBSTRING(x FROM start)`.
 *
 * {{{
 * substring(column("name"), value(2))
 * }}}
 */
def substring(x: Expr, start: Expr): Expr =
    Expr(
        SqlExpr.SubstringFunc(
            x.asSqlExpr,
            start.asSqlExpr,
            None
        )
    )

/**
 * Extracts a substring from `start` through `end`. Maps to
 * `SUBSTRING(x FROM start FOR end)`.
 *
 * {{{
 * substring(column("name"), value(2), value(5))
 * }}}
 */
def substring(x: Expr, start: Expr, end: Expr): Expr =
    Expr(
        SqlExpr.SubstringFunc(
            x.asSqlExpr,
            start.asSqlExpr,
            Some(end.asSqlExpr)
        )
    )

/**
 * Converts a string to upper case. Maps to `UPPER(x)`.
 *
 * {{{
 * upper(column("name"))
 * }}}
 */
def upper(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "UPPER",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Converts a string to lower case. Maps to `LOWER(x)`.
 *
 * {{{
 * lower(column("name"))
 * }}}
 */
def lower(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LOWER",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Left-pads a string to length `n` with spaces. Maps to `LPAD(x, n)`.
 *
 * {{{
 * lpad(column("name"), value(10))
 * }}}
 */
def lpad(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LPAD",
            x.asSqlExpr :: n.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Left-pads a string to length `n` with `pad`. Maps to
 * `LPAD(x, n, pad)`.
 *
 * {{{
 * lpad(column("name"), value(10), value("0"))
 * }}}
 */
def lpad(x: Expr, n: Expr, pad: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LPAD",
            x.asSqlExpr :: n.asSqlExpr :: pad.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Right-pads a string to length `n` with spaces. Maps to `RPAD(x, n)`.
 *
 * {{{
 * rpad(column("name"), value(10))
 * }}}
 */
def rpad(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "RPAD",
            x.asSqlExpr :: n.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Right-pads a string to length `n` with `pad`. Maps to
 * `RPAD(x, n, pad)`.
 *
 * {{{
 * rpad(column("name"), value(10), value("0"))
 * }}}
 */
def rpad(x: Expr, n: Expr, pad: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "RPAD",
            x.asSqlExpr :: n.asSqlExpr :: pad.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Trims spaces from both ends of a string. Maps to `BTRIM(x)`.
 *
 * {{{
 * btrim(column("name"))
 * }}}
 */
def btrim(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "BTRIM",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Trims any characters in `y` from both ends of `x`. Maps to
 * `BTRIM(x, y)`.
 *
 * {{{
 * btrim(column("name"), value(" "))
 * }}}
 */
def btrim(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "BTRIM",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Trims a leading character set from a string. Maps to `LTRIM(x)`.
 *
 * {{{
 * ltrim(column("name"))
 * }}}
 */
def ltrim(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LTRIM",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Trims any characters in `y` from the leading end of `x`. Maps to
 * `LTRIM(x, y)`.
 *
 * {{{
 * ltrim(column("name"), value("0"))
 * }}}
 */
def ltrim(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LTRIM",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Trims a trailing character set from a string. Maps to `RTRIM(x)`.
 *
 * {{{
 * rtrim(column("name"))
 * }}}
 */
def rtrim(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "RTRIM",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Trims any characters in `y` from the trailing end of `x`. Maps to
 * `RTRIM(x, y)`.
 *
 * {{{
 * rtrim(column("name"), value("0"))
 * }}}
 */
def rtrim(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "RTRIM",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Replaces part of a string starting at `start`. Maps to
 * `OVERLAY(x PLACING y FROM start)`.
 *
 * {{{
 * overlay(column("name"), value("AB"), value(2))
 * }}}
 */
def overlay(x: Expr, y: Expr, start: Expr): Expr =
    Expr(
        SqlExpr.OverlayFunc(
            x.asSqlExpr,
            y.asSqlExpr,
            start.asSqlExpr,
            None
        )
    )

/**
 * Replaces part of a string from `start` through `end`. Maps to
 * `OVERLAY(x PLACING y FROM start FOR end)`.
 *
 * {{{
 * overlay(column("name"), value("AB"), value(2), value(4))
 * }}}
 */
def overlay(x: Expr, y: Expr, start: Expr, end: Expr): Expr =
    Expr(
        SqlExpr.OverlayFunc(
            x.asSqlExpr,
            y.asSqlExpr,
            start.asSqlExpr,
            Some(end.asSqlExpr)
        )
    )

/**
 * Tests whether a string matches a regular expression. Maps to
 * `REGEXP_LIKE(x, y)`.
 *
 * {{{
 * regexpLike(column("email"), value("^[a-z]+@"))
 * }}}
 */
def regexpLike(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "REGEXP_LIKE",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Returns the position of `y` within `x`. Maps to `POSITION(y IN x)`.
 *
 * {{{
 * position(column("name"), value("a"))
 * }}}
 */
def position(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.PositionFunc(
            x.asSqlExpr,
            y.asSqlExpr
        )
    )

/**
 * Number of characters in a string. Maps to `CHAR_LENGTH(x)`.
 *
 * {{{
 * charLength(column("name"))
 * }}}
 */
def charLength(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "CHAR_LENGTH",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Number of bytes in a string. Maps to `OCTET_LENGTH(x)`.
 *
 * {{{
 * octetLength(column("name"))
 * }}}
 */
def octetLength(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "OCTET_LENGTH",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Absolute value. Maps to `ABS(x)`.
 *
 * {{{
 * abs(column("amount"))
 * }}}
 */
def abs(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ABS",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Remainder of `x` divided by `y`. Maps to `MOD(x, y)`.
 *
 * {{{
 * mod(column("amount"), value(10))
 * }}}
 */
def mod(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "MOD",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Sine. Maps to `SIN(x)`.
 *
 * {{{
 * sin(column("angle"))
 * }}}
 */
def sin(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "SIN",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Cosine. Maps to `COS(x)`.
 *
 * {{{
 * cos(column("angle"))
 * }}}
 */
def cos(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "COS",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Tangent. Maps to `TAN(x)`.
 *
 * {{{
 * tan(column("angle"))
 * }}}
 */
def tan(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "TAN",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Hyperbolic sine. Maps to `SINH(x)`.
 *
 * {{{
 * sinh(column("x"))
 * }}}
 */
def sinh(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "SINH",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Hyperbolic cosine. Maps to `COSH(x)`.
 *
 * {{{
 * cosh(column("x"))
 * }}}
 */
def cosh(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "COSH",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Hyperbolic tangent. Maps to `TANH(x)`.
 *
 * {{{
 * tanh(column("x"))
 * }}}
 */
def tanh(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "TANH",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Arc sine. Maps to `ASIN(x)`.
 *
 * {{{
 * asin(column("x"))
 * }}}
 */
def asin(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ASIN",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Arc cosine. Maps to `ACOS(x)`.
 *
 * {{{
 * acos(column("x"))
 * }}}
 */
def acos(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ACOS",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Arc tangent. Maps to `ATAN(x)`.
 *
 * {{{
 * atan(column("x"))
 * }}}
 */
def atan(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ATAN",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Logarithm of `x` in base `y`. Maps to `LOG(x, y)`.
 *
 * {{{
 * log(value(2), column("amount"))
 * }}}
 */
def log(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LOG",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Base-10 logarithm. Maps to `LOG10(x)`.
 *
 * {{{
 * log10(column("amount"))
 * }}}
 */
def log10(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LOG10",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Natural logarithm. Maps to `LN(x)`.
 *
 * {{{
 * ln(column("amount"))
 * }}}
 */
def ln(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LN",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Exponential function. Maps to `EXP(x)`.
 *
 * {{{
 * exp(column("x"))
 * }}}
 */
def exp(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "EXP",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Raises `x` to the power `y`. Maps to `POWER(x, y)`.
 *
 * {{{
 * power(column("x"), value(2))
 * }}}
 */
def power(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "POWER",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Square root. Maps to `SQRT(x)`.
 *
 * {{{
 * sqrt(column("x"))
 * }}}
 */
def sqrt(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "SQRT",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Largest integer not greater than `x`. Maps to `FLOOR(x)`.
 *
 * {{{
 * floor(column("amount"))
 * }}}
 */
def floor(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "FLOOR",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Smallest integer not less than `x`. Maps to `CEIL(x)`.
 *
 * {{{
 * ceil(column("amount"))
 * }}}
 */
def ceil(x: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "CEIL",
            x.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Rounds `x` to `y` decimal places. Maps to `ROUND(x, y)`.
 *
 * {{{
 * round(column("amount"), value(2))
 * }}}
 */
def round(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ROUND",
            x.asSqlExpr :: y.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Assigns `x` to one of `num` equal-width buckets spanning
 * `[min, max]`. Maps to `WIDTH_BUCKET(x, min, max, num)`.
 *
 * {{{
 * widthBucket(column("score"), value(0), value(100), value(10))
 * }}}
 */
def widthBucket(x: Expr, min: Expr, max: Expr, num: Expr): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "WIDTH_BUCKET",
            x.asSqlExpr :: min.asSqlExpr :: max.asSqlExpr :: num.asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Current date. Maps to `CURRENT_DATE` (a keyword, no parentheses).
 *
 * {{{
 * currentDate
 * }}}
 */
def currentDate: Expr =
    Expr(
        SqlExpr.IdentFunc("CURRENT_DATE")
    )

/**
 * Current time with time zone. Maps to `CURRENT_TIME` (a keyword, no
 * parentheses).
 *
 * {{{
 * currentTime
 * }}}
 */
def currentTime: Expr =
    Expr(
        SqlExpr.IdentFunc("CURRENT_TIME")
    )

/**
 * Current timestamp with time zone. Maps to `CURRENT_TIMESTAMP` (a
 * keyword, no parentheses).
 *
 * {{{
 * currentTimestamp
 * }}}
 */
def currentTimestamp: Expr =
    Expr(
        SqlExpr.IdentFunc("CURRENT_TIMESTAMP")
    )

/**
 * Current local time. Maps to `LOCALTIME` (a keyword, no parentheses).
 *
 * {{{
 * localTime
 * }}}
 */
def localTime: Expr =
    Expr(
        SqlExpr.IdentFunc("LOCALTIME")
    )

/**
 * Current local timestamp. Maps to `LOCALTIMESTAMP` (a keyword, no
 * parentheses).
 *
 * {{{
 * localTimestamp
 * }}}
 */
def localTimestamp: Expr =
    Expr(
        SqlExpr.IdentFunc("LOCALTIMESTAMP")
    )

/**
 * Rank of a row with gaps in the ordering. Maps to `RANK()`. Used with
 * `over`.
 *
 * {{{
 * rank().over(orderBy(column("score").desc))
 * }}}
 */
def rank(): Expr =
    Expr(
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
 * Rank of a row without gaps. Maps to `DENSE_RANK()`. Used with `over`.
 *
 * {{{
 * denseRank().over(orderBy(column("score").desc))
 * }}}
 */
def denseRank(): Expr =
    Expr(
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
 * Relative rank of a row within its partition, in `(0, 1]`. Maps to
 * `PERCENT_RANK()`. Used with `over`.
 *
 * {{{
 * percentRank().over(orderBy(column("score").desc))
 * }}}
 */
def percentRank(): Expr =
    Expr(
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
 * Cumulative distribution of a row within its partition, in `(0, 1]`.
 * Maps to `CUME_DIST()`. Used with `over`.
 *
 * {{{
 * cumeDist().over(orderBy(column("score").desc))
 * }}}
 */
def cumeDist(): Expr =
    Expr(
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
 * Sequential row number within its partition. Maps to `ROW_NUMBER()`.
 * Used with `over`.
 *
 * {{{
 * rowNumber().over(orderBy(column("id").asc))
 * }}}
 */
def rowNumber(): Expr =
    Expr(
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
 * Divides the partition into buckets. Maps to `NTILE()`. Used with
 * `over`.
 *
 * {{{
 * ntile().over(orderBy(column("id").asc))
 * }}}
 */
def ntile(): Expr =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "NTILE",
            Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * First value in the window frame. Maps to `FIRST_VALUE(x)`. Used with
 * `over`.
 *
 * {{{
 * firstValue(column("score")).over(partitionBy(column("author")))
 * }}}
 */
def firstValue(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "FIRST_VALUE",
            x.asSqlExpr :: Nil,
            None
        )
    )

/**
 * First non-null value in the window frame. Maps to
 * `FIRST_VALUE(x) IGNORE NULLS`. Used with `over`.
 *
 * {{{
 * firstValueIgnoreNulls(column("score")).over(partitionBy(column("author")))
 * }}}
 */
def firstValueIgnoreNulls(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "FIRST_VALUE",
            x.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

/**
 * Last value in the window frame. Maps to `LAST_VALUE(x)`. Used with
 * `over`.
 *
 * {{{
 * lastValue(column("score")).over(partitionBy(column("author")))
 * }}}
 */
def lastValue(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAST_VALUE",
            x.asSqlExpr :: Nil,
            None
        )
    )

/**
 * Last non-null value in the window frame. Maps to
 * `LAST_VALUE(x) IGNORE NULLS`. Used with `over`.
 *
 * {{{
 * lastValueIgnoreNulls(column("score")).over(partitionBy(column("author")))
 * }}}
 */
def lastValueIgnoreNulls(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAST_VALUE",
            x.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

/**
 * `n`-th value in the window frame. Maps to `NTH_VALUE(x, n)`. Used
 * with `over`.
 *
 * {{{
 * nthValue(column("score"), value(2)).over(partitionBy(column("author")))
 * }}}
 */
def nthValue(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NthValueFunc(
            x.asSqlExpr,
            n.asSqlExpr,
            None,
            None
        )
    )

/**
 * `n`-th non-null value in the window frame. Maps to
 * `NTH_VALUE(x, n) IGNORE NULLS`. Used with `over`.
 *
 * {{{
 * nthValueIgnoreNulls(column("score"), value(2)).over(partitionBy(column("author")))
 * }}}
 */
def nthValueIgnoreNulls(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NthValueFunc(
            x.asSqlExpr,
            n.asSqlExpr,
            None,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

/**
 * `n`-th value from the last row of the window frame. Maps to
 * `NTH_VALUE(x, n) FROM LAST`. Used with `over`.
 *
 * {{{
 * nthValueFromLast(column("score"), value(2)).over(partitionBy(column("author")))
 * }}}
 */
def nthValueFromLast(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NthValueFunc(
            x.asSqlExpr,
            n.asSqlExpr,
            Some(SqlNthValueFromMode.Last),
            None
        )
    )

/**
 * `n`-th non-null value from the last row of the window frame. Maps to
 * `NTH_VALUE(x, n) FROM LAST IGNORE NULLS`. Used with `over`.
 *
 * {{{
 * nthValueFromLastIgnoreNulls(column("score"), value(2)).over(partitionBy(column("author")))
 * }}}
 */
def nthValueFromLastIgnoreNulls(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NthValueFunc(
            x.asSqlExpr,
            n.asSqlExpr,
            Some(SqlNthValueFromMode.Last),
            Some(SqlWindowNullsMode.Ignore)
        )
    )

/**
 * Value from the previous row. Maps to `LAG(x)`. Used with `over`.
 *
 * {{{
 * lag(column("score")).over(orderBy(column("id").asc))
 * }}}
 */
def lag(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAG",
            x.asSqlExpr :: Nil,
            None
        )
    )

/**
 * Value from a previous row, ignoring nulls. Maps to
 * `LAG(x) IGNORE NULLS`. Used with `over`.
 *
 * {{{
 * lagIgnoreNulls(column("score")).over(orderBy(column("id").asc))
 * }}}
 */
def lagIgnoreNulls(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAG",
            x.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

/**
 * Value from a row `n` rows before the current row. Maps to
 * `LAG(x, n)`. Used with `over`.
 *
 * {{{
 * lag(column("score"), value(2)).over(orderBy(column("id").asc))
 * }}}
 */
def lag(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAG",
            x.asSqlExpr :: n.asSqlExpr :: Nil,
            None
        )
    )

/**
 * Value from a row `n` rows before the current row, ignoring nulls.
 * Maps to `LAG(x, n) IGNORE NULLS`. Used with `over`.
 *
 * {{{
 * lagIgnoreNulls(column("score"), value(2)).over(orderBy(column("id").asc))
 * }}}
 */
def lagIgnoreNulls(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAG",
            x.asSqlExpr :: n.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

/**
 * Value from a row `n` rows before the current row, with a default
 * value. Maps to `LAG(x, n, offset)`. Used with `over`.
 *
 * {{{
 * lag(column("score"), value(2), value(0)).over(orderBy(column("id").asc))
 * }}}
 */
def lag(x: Expr, n: Expr, offset: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAG",
            x.asSqlExpr :: n.asSqlExpr :: offset.asSqlExpr :: Nil,
            None
        )
    )

/**
 * Value from a row `n` rows before the current row, with a default
 * value, ignoring nulls. Maps to
 * `LAG(x, n, offset) IGNORE NULLS`. Used with `over`.
 *
 * {{{
 * lagIgnoreNulls(column("score"), value(2), value(0)).over(orderBy(column("id").asc))
 * }}}
 */
def lagIgnoreNulls(x: Expr, n: Expr, offset: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAG",
            x.asSqlExpr :: n.asSqlExpr :: offset.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

/**
 * Value from the next row. Maps to `LEAD(x)`. Used with `over`.
 *
 * {{{
 * lead(column("score")).over(orderBy(column("id").asc))
 * }}}
 */
def lead(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LEAD",
            x.asSqlExpr :: Nil,
            None
        )
    )

/**
 * Value from the next row, ignoring nulls. Maps to
 * `LEAD(x) IGNORE NULLS`. Used with `over`.
 *
 * {{{
 * leadIgnoreNulls(column("score")).over(orderBy(column("id").asc))
 * }}}
 */
def leadIgnoreNulls(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LEAD",
            x.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

/**
 * Value from a row `n` rows after the current row. Maps to
 * `LEAD(x, n)`. Used with `over`.
 *
 * {{{
 * lead(column("score"), value(2)).over(orderBy(column("id").asc))
 * }}}
 */
def lead(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LEAD",
            x.asSqlExpr :: n.asSqlExpr :: Nil,
            None
        )
    )

/**
 * Value from a row `n` rows after the current row, ignoring nulls.
 * Maps to `LEAD(x, n) IGNORE NULLS`. Used with `over`.
 *
 * {{{
 * leadIgnoreNulls(column("score"), value(2)).over(orderBy(column("id").asc))
 * }}}
 */
def leadIgnoreNulls(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LEAD",
            x.asSqlExpr :: n.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

/**
 * Value from a row `n` rows after the current row, with a default
 * value. Maps to `LEAD(x, n, offset)`. Used with `over`.
 *
 * {{{
 * lead(column("score"), value(2), value(0)).over(orderBy(column("id").asc))
 * }}}
 */
def lead(x: Expr, n: Expr, offset: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LEAD",
            x.asSqlExpr :: n.asSqlExpr :: offset.asSqlExpr :: Nil,
            None
        )
    )

/**
 * Value from a row `n` rows after the current row, with a default
 * value, ignoring nulls. Maps to
 * `LEAD(x, n, offset) IGNORE NULLS`. Used with `over`.
 *
 * {{{
 * leadIgnoreNulls(column("score"), value(2), value(0)).over(orderBy(column("id").asc))
 * }}}
 */
def leadIgnoreNulls(x: Expr, n: Expr, offset: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LEAD",
            x.asSqlExpr :: n.asSqlExpr :: offset.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )