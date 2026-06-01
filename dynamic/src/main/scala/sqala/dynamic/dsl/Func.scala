package sqala.dynamic.dsl

import sqala.ast.expr.*
import sqala.ast.quantifier.SqlQuantifier

def count(): Expr =
    Expr(SqlExpr.CountAsteriskFunc(None, None))

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

def stringAgg(x: Expr, separator: Expr, withinGroup: Order): Expr =
    listAgg(x, separator, withinGroup)

def groupConcat(x: Expr, separator: Expr, withinGroup: Order): Expr =
    listAgg(x, separator, withinGroup)

def substring(x: Expr, start: Expr): Expr =
    Expr(
        SqlExpr.SubstringFunc(
            x.asSqlExpr,
            start.asSqlExpr,
            None
        )
    )

def substring(x: Expr, start: Expr, end: Expr): Expr =
    Expr(
        SqlExpr.SubstringFunc(
            x.asSqlExpr,
            start.asSqlExpr,
            Some(end.asSqlExpr)
        )
    )

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

def overlay(x: Expr, y: Expr, start: Expr): Expr =
    Expr(
        SqlExpr.OverlayFunc(
            x.asSqlExpr,
            y.asSqlExpr,
            start.asSqlExpr,
            None
        )
    )

def overlay(x: Expr, y: Expr, start: Expr, end: Expr): Expr =
    Expr(
        SqlExpr.OverlayFunc(
            x.asSqlExpr,
            y.asSqlExpr,
            start.asSqlExpr,
            Some(end.asSqlExpr)
        )
    )

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

def position(x: Expr, y: Expr): Expr =
    Expr(
        SqlExpr.PositionFunc(
            x.asSqlExpr,
            y.asSqlExpr
        )
    )

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

def currentDate: Expr =
    Expr(
        SqlExpr.IdentFunc("CURRENT_DATE")
    )

def currentTime: Expr =
    Expr(
        SqlExpr.IdentFunc("CURRENT_TIME")
    )

def currentTimestamp: Expr =
    Expr(
        SqlExpr.IdentFunc("CURRENT_TIMESTAMP")
    )

def localTime: Expr =
    Expr(
        SqlExpr.IdentFunc("LOCALTIME")
    )

def localTimestamp: Expr =
    Expr(
        SqlExpr.IdentFunc("LOCALTIMESTAMP")
    )

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

def firstValue(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "FIRST_VALUE",
            x.asSqlExpr :: Nil,
            None
        )
    )

def firstValueIgnoreNulls(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "FIRST_VALUE",
            x.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

def lastValue(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAST_VALUE",
            x.asSqlExpr :: Nil,
            None
        )
    )

def lastValueIgnoreNulls(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAST_VALUE",
            x.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

def nthValue(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NthValueFunc(
            x.asSqlExpr,
            n.asSqlExpr,
            None,
            None
        )
    )

def nthValueIgnoreNulls(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NthValueFunc(
            x.asSqlExpr,
            n.asSqlExpr,
            None,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

def nthValueFromLast(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NthValueFunc(
            x.asSqlExpr,
            n.asSqlExpr,
            Some(SqlNthValueFromMode.Last),
            None
        )
    )

def nthValueFromLastIgnoreNulls(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NthValueFunc(
            x.asSqlExpr,
            n.asSqlExpr,
            Some(SqlNthValueFromMode.Last),
            Some(SqlWindowNullsMode.Ignore)
        )
    )

def lag(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAG",
            x.asSqlExpr :: Nil,
            None
        )
    )

def lagIgnoreNulls(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAG",
            x.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

def lag(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAG",
            x.asSqlExpr :: n.asSqlExpr :: Nil,
            None
        )
    )

def lagIgnoreNulls(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAG",
            x.asSqlExpr :: n.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

def lag(x: Expr, n: Expr, offset: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAG",
            x.asSqlExpr :: n.asSqlExpr :: offset.asSqlExpr :: Nil,
            None
        )
    )

def lagIgnoreNulls(x: Expr, n: Expr, offset: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAG",
            x.asSqlExpr :: n.asSqlExpr :: offset.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

def lead(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LEAD",
            x.asSqlExpr :: Nil,
            None
        )
    )

def leadIgnoreNulls(x: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LEAD",
            x.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

def lead(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LEAD",
            x.asSqlExpr :: n.asSqlExpr :: Nil,
            None
        )
    )

def leadIgnoreNulls(x: Expr, n: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LEAD",
            x.asSqlExpr :: n.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

def lead(x: Expr, n: Expr, offset: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LEAD",
            x.asSqlExpr :: n.asSqlExpr :: offset.asSqlExpr :: Nil,
            None
        )
    )

def leadIgnoreNulls(x: Expr, n: Expr, offset: Expr): Expr =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LEAD",
            x.asSqlExpr :: n.asSqlExpr :: offset.asSqlExpr :: Nil,
            Some(SqlWindowNullsMode.Ignore)
        )
    )