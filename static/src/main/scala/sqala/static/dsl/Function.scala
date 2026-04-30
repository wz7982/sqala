package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.quantifier.SqlQuantifier
import sqala.static.metadata.*

import java.time.*

def count()(using QueryContext): Expr[Long, Agg] =
    Expr(SqlExpr.CountAsteriskFunc(None, None))

def count[T: AsExpr as a](x: T)(using QueryContext, CanInAgg[a.K]): Expr[Long, Agg] =
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

def countDistinct[T: AsExpr as a](x: T)(using QueryContext, CanInAgg[a.K]): Expr[Long, Agg] =
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

def sum[T: AsExpr as a](x: T)(using
    n: SqlNumber[a.R],
    i: CanInAgg[a.K],
    to: ToOption[Expr[a.R, Agg]],
    c: QueryContext
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

def avg[T: AsExpr as a](x: T)(using
    SqlNumber[a.R],
    CanInAgg[a.K],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def max[T: AsExpr as a](x: T)(using
    i: CanInAgg[a.K],
    to: ToOption[Expr[a.R, Agg]],
    c: QueryContext
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

def min[T: AsExpr as a](x: T)(using
    i: CanInAgg[a.K],
    to: ToOption[Expr[a.R, Agg]],
    c: QueryContext
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

def anyValue[T: AsExpr as a](x: T)(using
    i: CanInAgg[a.K],
    to: ToOption[Expr[a.R, Agg]],
    c: QueryContext
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

def stddevPop[T: AsExpr as a](x: T)(using
    CanInAgg[a.K],
    SqlNumber[a.R],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def stddevSamp[T: AsExpr as a](x: T)(using
    CanInAgg[a.K],
    SqlNumber[a.R],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def varPop[T: AsExpr as a](x: T)(using
    CanInAgg[a.K],
    SqlNumber[a.R],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def varSamp[T: AsExpr as a](x: T)(using
    CanInAgg[a.K],
    SqlNumber[a.R],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def covarPop[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    CanInAgg[aa.K],
    SqlNumber[aa.R],
    CanInAgg[ab.K],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def covarSamp[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    CanInAgg[aa.K],
    SqlNumber[aa.R],
    CanInAgg[ab.K],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def corr[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    CanInAgg[aa.K],
    SqlNumber[aa.R],
    CanInAgg[ab.K],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def regrSlope[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    CanInAgg[aa.K],
    SqlNumber[aa.R],
    CanInAgg[ab.K],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def regrIntercept[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    CanInAgg[aa.K],
    SqlNumber[aa.R],
    CanInAgg[ab.K],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def regrCount[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    CanInAgg[aa.K],
    SqlNumber[aa.R],
    CanInAgg[ab.K],
    SqlNumber[ab.R],
    QueryContext
): Expr[Long, Agg] =
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

def regrR2[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    CanInAgg[aa.K],
    SqlNumber[aa.R],
    CanInAgg[ab.K],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def regrAvgx[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    CanInAgg[aa.K],
    SqlNumber[aa.R],
    CanInAgg[ab.K],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def regrAvgy[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    CanInAgg[aa.K],
    SqlNumber[aa.R],
    CanInAgg[ab.K],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def regrSxx[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    CanInAgg[aa.K],
    SqlNumber[aa.R],
    CanInAgg[ab.K],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def regrSyy[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    CanInAgg[aa.K],
    SqlNumber[aa.R],
    CanInAgg[ab.K],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def regrSxy[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    CanInAgg[aa.K],
    SqlNumber[aa.R],
    CanInAgg[ab.K],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal], Agg] =
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

def percentileCont[T: AsExpr as at, S, SK <: ExprKind](x: T, withinGroup: Sort[S, SK])(using
    cx: CanInAgg[at.K],
    n: SqlNumber[at.R],
    ns: SqlNumber[S],
    cs: CanInAgg[SK],
    to: ToOption[Expr[S, Agg]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "PERCENTILE_CONT",
                at.asExpr(x).asSqlExpr :: Nil,
                Nil,
                withinGroup.asSqlOrderBy :: Nil,
                None
            )
        )
    )

def percentileDisc[T: AsExpr as at, S, SK <: ExprKind](x: T, withinGroup: Sort[S, SK])(using
    cx: CanInAgg[at.K],
    n: SqlNumber[at.R],
    ns: SqlNumber[S],
    cs: CanInAgg[SK],
    to: ToOption[Expr[S, Agg]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "PERCENTILE_DISC",
                at.asExpr(x).asSqlExpr :: Nil,
                Nil,
                withinGroup.asSqlOrderBy :: Nil,
                None
            )
        )
    )

def listAgg[T: AsExpr as at, S: AsExpr as as, ST, SK <: ExprKind](x: T, separator: S, withinGroup: Sort[ST, SK])(using
    CanInAgg[at.K],
    SqlString[at.R],
    CanInAgg[as.K],
    SqlString[as.R],
    CanInAgg[SK],
    QueryContext
): Expr[Option[String], Agg] =
    Expr(
        SqlExpr.ListAggFunc(
            None,
            at.asExpr(x).asSqlExpr,
            as.asExpr(separator).asSqlExpr,
            None,
            withinGroup.asSqlOrderBy :: Nil,
            None
        )
    )

def stringAgg[T: AsExpr as at, S: AsExpr as as, ST, SK <: ExprKind](x: T, separator: S, withinGroup: Sort[ST, SK])(using
    CanInAgg[at.K],
    SqlString[at.R],
    CanInAgg[as.K],
    SqlString[as.R],
    CanInAgg[SK],
    QueryContext
): Expr[Option[String], Agg] =
    listAgg(x, separator, withinGroup)

def groupConcat[T: AsExpr as at, S: AsExpr as as, ST, SK <: ExprKind](x: T, separator: S, withinGroup: Sort[ST, SK])(using
    CanInAgg[at.K],
    SqlString[at.R],
    CanInAgg[as.K],
    SqlString[as.R],
    CanInAgg[SK],
    QueryContext
): Expr[Option[String], Agg] =
    listAgg(x, separator, withinGroup)

def jsonObjectAgg[K: AsExpr as ak, V: AsExpr as av](key: K, value: V)(using
    CanInAgg[ak.K],
    CanInAgg[av.K],
    QueryContext
): Expr[Option[Json], Agg] =
    Expr(
        SqlExpr.JsonObjectAggFunc(
            SqlJsonObjectItem(ak.asExpr(key).asSqlExpr, av.asExpr(value).asSqlExpr),
            None,
            None,
            None,
            None
        )
    )

def jsonArrayAgg[A: AsExpr as aa](x: A)(using
    CanInAgg[aa.K],
    QueryContext
): Expr[Option[Json], Agg] =
    Expr(
        SqlExpr.JsonArrayAggFunc(
            SqlJsonArrayItem(aa.asExpr(x).asSqlExpr, None),
            Nil,
            None,
            None,
            None
        )
    )

def classifier()(using QueryContext, MatchRecognizeContext): Expr[String, Agg] =
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

def matchNumber()(using QueryContext, MatchRecognizeContext): Expr[Int, Agg] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "MATCH_NUMBER",
            Nil,
            Nil,
            Nil,
            None
        )
    )

def first[A: AsExpr as aa](x: A)(using
    cx: CanInAgg[aa.K],
    to: ToOption[Expr[aa.R, Agg]],
    c: QueryContext,
    mc: MatchRecognizeContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "FIRST",
                aa.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

def first[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    cx: CanInAgg[aa.K],
    sn: SqlNumber[an.R],
    cn: CanInAgg[an.K],
    to: ToOption[Expr[aa.R, Agg]],
    c: QueryContext,
    mc: MatchRecognizeContext
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

def last[A: AsExpr as aa](x: A)(using
    cx: CanInAgg[aa.K],
    to: ToOption[Expr[aa.R, Agg]],
    c: QueryContext,
    mc: MatchRecognizeContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "LAST",
                aa.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

def last[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    cx: CanInAgg[aa.K],
    sn: SqlNumber[an.R],
    cn: CanInAgg[an.K],
    to: ToOption[Expr[aa.R, Agg]],
    c: QueryContext,
    mc: MatchRecognizeContext
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

def prev[A: AsExpr as aa](x: A)(using
    cx: CanInAgg[aa.K],
    to: ToOption[Expr[aa.R, Agg]],
    c: QueryContext,
    mc: MatchRecognizeContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "PREV",
                aa.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

def prev[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    cx: CanInAgg[aa.K],
    sn: SqlNumber[an.R],
    cn: CanInAgg[an.K],
    to: ToOption[Expr[aa.R, Agg]],
    c: QueryContext,
    mc: MatchRecognizeContext
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

def next[A: AsExpr as aa](x: A)(using
    cx: CanInAgg[aa.K],
    to: ToOption[Expr[aa.R, Agg]],
    c: QueryContext,
    mc: MatchRecognizeContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "NEXT",
                aa.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

def next[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    cx: CanInAgg[aa.K],
    sn: SqlNumber[an.R],
    cn: CanInAgg[an.K],
    to: ToOption[Expr[aa.R, Agg]],
    c: QueryContext,
    mc: MatchRecognizeContext
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

def substring[T: AsExpr as at, S: AsExpr as as](x: T, start: S)(using
    s: SqlString[at.R],
    n: SqlNumber[as.R],
    o: KindOperation[at.K, as.K],
    c: QueryContext
): Expr[Option[String], o.R] =
    Expr(
        SqlExpr.SubstringFunc(
            at.asExpr(x).asSqlExpr,
            as.asExpr(start).asSqlExpr,
            None
        )
    )

def substring[T: AsExpr as at, S: AsExpr as as, E: AsExpr as ae](x: T, start: S, end: E)(using
    s: SqlString[at.R],
    ns: SqlNumber[as.R],
    ne: SqlNumber[ae.R],
    os: KindOperation[at.K, as.K],
    oe: KindOperation[os.R, ae.K],
    c: QueryContext
): Expr[Option[String], oe.R] =
    Expr(
        SqlExpr.SubstringFunc(
            at.asExpr(x).asSqlExpr,
            as.asExpr(start).asSqlExpr,
            Some(ae.asExpr(end).asSqlExpr)
        )
    )

def upper[T: AsExpr as a](x: T)(using
    s: SqlString[a.R],
    o: KindOperation[a.K, Value],
    c: QueryContext
): Expr[a.R, o.R] =
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

def lower[T: AsExpr as a](x: T)(using
    s: SqlString[a.R],
    o: KindOperation[a.K, Value],
    c: QueryContext
): Expr[a.R, o.R] =
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

def lpad[T: AsExpr as at, N: AsExpr as an](x: T, n: N)(using
    s: SqlString[at.R],
    sn: SqlNumber[an.R],
    o: KindOperation[at.K, an.K],
    c: QueryContext
): Expr[Option[String], o.R] =
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

def lpad[T: AsExpr as at, N: AsExpr as an, P: AsExpr as ap](x: T, n: N, pad: P)(using
    s: SqlString[at.R],
    sn: SqlNumber[an.R],
    sp: SqlString[ap.R],
    on: KindOperation[at.K, an.K],
    op: KindOperation[on.R, ap.K],
    c: QueryContext
): Expr[Option[String], op.R] =
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

def rpad[T: AsExpr as at, N: AsExpr as an](x: T, n: N)(using
    s: SqlString[at.R],
    sn: SqlNumber[an.R],
    o: KindOperation[at.K, an.K],
    c: QueryContext
): Expr[Option[String], o.R] =
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

def rpad[T: AsExpr as at, N: AsExpr as an, P: AsExpr as ap](x: T, n: N, pad: P)(using
    s: SqlString[at.R],
    sn: SqlNumber[an.R],
    sp: SqlString[ap.R],
    on: KindOperation[at.K, an.K],
    op: KindOperation[on.R, ap.K],
    c: QueryContext
): Expr[Option[String], op.R] =
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

def btrim[A: AsExpr as aa](x: A)(using
    s: SqlString[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[String], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "BTRIM",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def btrim[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    sa: SqlString[aa.R],
    sb: SqlString[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[String], o.R] =
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

def ltrim[A: AsExpr as aa](x: A)(using
    s: SqlString[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[String], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LTRIM",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def ltrim[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    sa: SqlString[aa.R],
    sb: SqlString[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[String], o.R] =
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

def rtrim[A: AsExpr as aa](x: A)(using
    s: SqlString[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[String], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "RTRIM",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def rtrim[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    sa: SqlString[aa.R],
    sb: SqlString[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[String], o.R] =
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

def overlay[A: AsExpr as aa, B: AsExpr as ab, S: AsExpr as as](x: A, y: B, start: S)(using
    sa: SqlString[aa.R],
    sb: SqlString[ab.R],
    n: SqlNumber[as.R],
    ob: KindOperation[aa.K, ab.K],
    os: KindOperation[ob.R, as.K],
    c: QueryContext
): Expr[Option[String], os.R] =
    Expr(
        SqlExpr.OverlayFunc(
            aa.asExpr(x).asSqlExpr,
            ab.asExpr(y).asSqlExpr,
            as.asExpr(start).asSqlExpr,
            None
        )
    )

def overlay[A: AsExpr as aa, B: AsExpr as ab, S: AsExpr as as, E: AsExpr as ae](x: A, y: B, start: S, end: E)(using
    sa: SqlString[aa.R],
    sb: SqlString[ab.R],
    ns: SqlNumber[as.R],
    ne: SqlNumber[ae.R],
    ob: KindOperation[aa.K, ab.K],
    os: KindOperation[ob.R, as.K],
    oe: KindOperation[os.R, ae.K],
    c: QueryContext
): Expr[Option[String], oe.R] =
    Expr(
        SqlExpr.OverlayFunc(
            aa.asExpr(x).asSqlExpr,
            ab.asExpr(y).asSqlExpr,
            as.asExpr(start).asSqlExpr,
            Some(ae.asExpr(end).asSqlExpr)
        )
    )

def regexpLike[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    sa: SqlString[aa.R],
    sb: SqlString[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[Boolean], o.R] =
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

def position[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    sa: SqlString[aa.R],
    sb: SqlString[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Int, o.R] =
    Expr(
        SqlExpr.PositionFunc(
            aa.asExpr(x).asSqlExpr,
            ab.asExpr(y).asSqlExpr
        )
    )

def charLength[A: AsExpr as aa](x: A)(using
    s: SqlString[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[Int], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "CHAR_LENGTH",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def octetLength[A: AsExpr as aa](x: A)(using
    s: SqlString[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[Int], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "OCTET_LENGTH",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def abs[A: AsExpr as aa](x: A)(using
    n: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[aa.R, o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ABS",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def mod[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
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

def sin[A: AsExpr as aa](x: A)(using
    n: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "SIN",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def cos[A: AsExpr as aa](x: A)(using
    n: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "COS",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def tan[A: AsExpr as aa](x: A)(using
    n: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "TAN",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def sinh[A: AsExpr as aa](x: A)(using
    n: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "SINH",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def cosh[A: AsExpr as aa](x: A)(using
    n: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "COSH",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def tanh[A: AsExpr as aa](x: A)(using
    n: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "TANH",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def asin[A: AsExpr as aa](x: A)(using
    n: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ASIN",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def acos[A: AsExpr as aa](x: A)(using
    n: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ACOS",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def atan[A: AsExpr as aa](x: A)(using
    n: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ATAN",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def log[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
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

def log10[A: AsExpr as aa](x: A)(using
    n: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LOG10",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def ln[A: AsExpr as aa](x: A)(using
    n: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "LN",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def exp[A: AsExpr as aa](x: A)(using
    n: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "EXP",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def power[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
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

def sqrt[A: AsExpr as aa](x: A)(using
    n: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "SQRT",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def floor[A: AsExpr as aa](x: A)(using
    na: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[Long], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "FLOOR",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def ceil[A: AsExpr as aa](x: A)(using
    na: SqlNumber[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[Long], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "CEIL",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def round[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
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

def widthBucket[A: AsExpr as aa, B: AsExpr as ab, C: AsExpr as ac, D: AsExpr as ad](x: A, min: B, max: C, num: D)(using
    na: SqlNumber[aa.R],
    nb: SqlNumber[ab.R],
    nc: SqlNumber[ac.R],
    nd: SqlNumber[ad.R],
    ob: KindOperation[aa.K, ab.K],
    oc: KindOperation[ob.R, ac.K],
    od: KindOperation[oc.R, ad.K],
    c: QueryContext
): Expr[Option[Int], od.R] =
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

def json[A: AsExpr as aa](x: A)(using
    s: SqlString[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[Json], o.R] =
    Expr(
        SqlExpr.JsonParseFunc(
            aa.asExpr(x).asSqlExpr,
            None,
            Some(SqlJsonUniqueness.With)
        )
    )

def jsonValue[A: AsExpr as aa, P: AsExpr as ap](x: A, path: P)(using
    j: SqlJson[aa.R],
    s: SqlString[ap.R],
    o: KindOperation[aa.K, ap.K],
    c: QueryContext
): Expr[Option[String], o.R] =
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

def jsonQuery[A: AsExpr as aa, P: AsExpr as ap](x: A, path: P)(using
    j: SqlJson[aa.R],
    s: SqlString[ap.R],
    o: KindOperation[aa.K, ap.K],
    c: QueryContext
): Expr[Option[Json], o.R] =
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

def jsonExists[A: AsExpr as aa, P: AsExpr as ap](x: A, path: P)(using
    j: SqlJson[aa.R],
    s: SqlString[ap.R],
    o: KindOperation[aa.K, ap.K],
    c: QueryContext
): Expr[Option[Boolean], o.R] =
    Expr(
        SqlExpr.JsonExistsFunc(
            aa.asExpr(x).asSqlExpr,
            ap.asExpr(path).asSqlExpr,
            Nil,
            None
        )
    )

case class JsonObjectPair[K <: ExprKind](key: SqlExpr, value: SqlExpr)

extension [K: AsExpr as ak](key: K)
    infix def value[V: AsExpr as av](value: V)(using
        o: KindOperation[ak.K, av.K],
        c: QueryContext
    ): JsonObjectPair[o.R] =
        JsonObjectPair(ak.asExpr(key).asSqlExpr, av.asExpr(value).asSqlExpr)

trait AsJsonObject[T]:
    type R <: ExprKind

    def asJsonObjects(x: T): List[JsonObjectPair[?]]

object AsJsonObject:
    type Aux[T, O <: ExprKind] = AsJsonObject[T]:
        type R = O

    given pair[K <: ExprKind]: Aux[JsonObjectPair[K], K] =
        new AsJsonObject[JsonObjectPair[K]]:
            type R = K

            def asJsonObjects(x: JsonObjectPair[K]): List[JsonObjectPair[K]] =
                x :: Nil

    given tuple[K <: ExprKind, T <: Tuple](using
        j: AsJsonObject[T],
        o: KindOperation[K, j.R]
    ): Aux[JsonObjectPair[K] *: T, o.R] =
        new AsJsonObject[JsonObjectPair[K] *: T]:
            type R = o.R

            def asJsonObjects(x: JsonObjectPair[K] *: T): List[JsonObjectPair[?]] =
                x.head :: j.asJsonObjects(x.tail)

    given emptyTuple: Aux[EmptyTuple, Value] =
        new AsJsonObject[EmptyTuple]:
            type R = Value

            def asJsonObjects(x: EmptyTuple): List[JsonObjectPair[?]] =
                Nil

def jsonObject[T](items: T)(using
    j: AsJsonObject[T],
    c: QueryContext
): Expr[Option[Json], j.R] =
    Expr(
        SqlExpr.JsonObjectFunc(
            j.asJsonObjects(items).map(i => SqlJsonObjectItem(i.key, i.value)),
            None,
            None,
            None
        )
    )

def jsonArray[A: AsExpr as aa](items: A)(using
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[Json], o.R] =
    Expr(
        SqlExpr.JsonArrayFunc(
            aa.asExprs(items).map(i => SqlJsonArrayItem(i.asSqlExpr, None)),
            None,
            None
        )
    )

def currentDate()(using QueryContext): Expr[LocalDate, ValueOperation] =
    Expr(
        SqlExpr.IdentFunc("CURRENT_DATE")
    )

def currentTime()(using QueryContext): Expr[OffsetTime, ValueOperation] =
    Expr(
        SqlExpr.IdentFunc("CURRENT_TIME")
    )

def currentTimestamp()(using QueryContext): Expr[OffsetDateTime, ValueOperation] =
    Expr(
        SqlExpr.IdentFunc("CURRENT_TIMESTAMP")
    )

def localTime()(using QueryContext): Expr[LocalTime, ValueOperation] =
    Expr(
        SqlExpr.IdentFunc("LOCALTIME")
    )

def localTimestamp()(using QueryContext): Expr[LocalDateTime, ValueOperation] =
    Expr(
        SqlExpr.IdentFunc("LOCALTIMESTAMP")
    )

def stGeomFromText[A: AsExpr as aa, S: AsExpr as as](x: A, srid: S)(using
    s: SqlString[aa.R],
    n: SqlNumber[as.R],
    o: KindOperation[aa.K, as.K],
    c: QueryContext
): Expr[Option[Geometry], o.R] =
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

def stAsText[A: AsExpr as aa](x: A)(using
    g: SqlGeometry[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[String], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_AsText",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def stAsGeoJson[A: AsExpr as aa](x: A)(using
    g: SqlGeometry[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[String], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_AsGeoJson",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def stGeometryType[A: AsExpr as aa](x: A)(using
    g: SqlGeometry[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[String], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_GeometryType",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def stX[A: AsExpr as aa](x: A)(using
    g: SqlGeometry[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_X",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def stY[A: AsExpr as aa](x: A)(using
    g: SqlGeometry[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Y",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def stArea[A: AsExpr as aa](x: A)(using
    g: SqlGeometry[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Area",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def stLength[A: AsExpr as aa](x: A)(using
    g: SqlGeometry[aa.R],
    o: KindOperation[aa.K, Value],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "ST_Length",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def stContains[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[Boolean], o.R] =
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

def stWithin[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[Boolean], o.R] =
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

def stIntersects[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[Boolean], o.R] =
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

def stTouches[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[Boolean], o.R] =
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

def stOverlaps[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[Boolean], o.R] =
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

def stCrosses[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[Boolean], o.R] =
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

def stDisjoint[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[Boolean], o.R] =
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

def stIntersection[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[Geometry], o.R] =
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

def stUnion[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[Geometry], o.R] =
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

def stDifference[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[Geometry], o.R] =
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

def stSymDifference[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[Geometry], o.R] =
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

def stDistance[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    ga: SqlGeometry[aa.R],
    gb: SqlGeometry[ab.R],
    o: KindOperation[aa.K, ab.K],
    c: QueryContext
): Expr[Option[BigDecimal], o.R] =
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

def rank()(using QueryContext): Expr[Long, WindowWithoutOver] =
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

def denseRank()(using QueryContext): Expr[Long, WindowWithoutOver] =
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

def percentRank()(using QueryContext): Expr[Long, WindowWithoutOver] =
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

def cumeDist()(using QueryContext): Expr[Long, WindowWithoutOver] =
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

def rowNumber()(using QueryContext): Expr[Long, WindowWithoutOver] =
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

def ntile[A: AsExpr as aa](x: A)(using
    SqlNumber[aa.R],
    CanInWindow[aa.K],
    QueryContext
): Expr[Long, WindowWithoutOver] =
    Expr(
        SqlExpr.GeneralFunc(
            None,
            "NTILE",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

def firstValue[A: AsExpr as aa](x: A)(using
    w: CanInWindow[aa.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NullsTreatmentFunc(
                "FIRST_VALUE",
                aa.asExpr(x).asSqlExpr :: Nil,
                None
            )
        )
    )

def firstValueIgnoreNulls[A: AsExpr as aa](x: A)(using
    w: CanInWindow[aa.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NullsTreatmentFunc(
                "FIRST_VALUE",
                aa.asExpr(x).asSqlExpr :: Nil,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

def lastValue[A: AsExpr as aa](x: A)(using
    w: CanInWindow[aa.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NullsTreatmentFunc(
                "LAST_VALUE",
                aa.asExpr(x).asSqlExpr :: Nil,
                None
            )
        )
    )

def lastValueIgnoreNulls[A: AsExpr as aa](x: A)(using
    w: CanInWindow[aa.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NullsTreatmentFunc(
                "LAST_VALUE",
                aa.asExpr(x).asSqlExpr :: Nil,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

def nthValue[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    wa: CanInWindow[aa.K],
    nn: SqlNumber[an.R],
    wn: CanInWindow[an.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NthValueFunc(
                aa.asExpr(x).asSqlExpr,
                an.asExpr(n).asSqlExpr,
                None,
                None
            )
        )
    )

def nthValueIgnoreNulls[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    wa: CanInWindow[aa.K],
    nn: SqlNumber[an.R],
    wn: CanInWindow[an.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NthValueFunc(
                aa.asExpr(x).asSqlExpr,
                an.asExpr(n).asSqlExpr,
                None,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

def nthValueFromLast[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    wa: CanInWindow[aa.K],
    nn: SqlNumber[an.R],
    wn: CanInWindow[an.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NthValueFunc(
                aa.asExpr(x).asSqlExpr,
                an.asExpr(n).asSqlExpr,
                Some(SqlNthValueFromMode.Last),
                None
            )
        )
    )

def nthValueFromLastIgnoreNulls[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    wa: CanInWindow[aa.K],
    nn: SqlNumber[an.R],
    wn: CanInWindow[an.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NthValueFunc(
                aa.asExpr(x).asSqlExpr,
                an.asExpr(n).asSqlExpr,
                Some(SqlNthValueFromMode.Last),
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

def lag[A: AsExpr as aa](x: A)(using
    i: CanInWindow[aa.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NullsTreatmentFunc(
                "LAG",
                aa.asExpr(x).asSqlExpr :: Nil,
                None
            )
        )
    )

def lagIgnoreNulls[A: AsExpr as aa](x: A)(using
    i: CanInWindow[aa.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NullsTreatmentFunc(
                "LAG",
                aa.asExpr(x).asSqlExpr :: Nil,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

def lag[A: AsExpr as aa, O: AsExpr as ao](x: A, offset: O)(using
    ia: CanInWindow[aa.K],
    no: SqlNumber[ao.R],
    io: CanInWindow[ao.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NullsTreatmentFunc(
                "LAG",
                aa.asExpr(x).asSqlExpr :: ao.asExpr(offset).asSqlExpr :: Nil,
                None
            )
        )
    )

def lagIgnoreNulls[A: AsExpr as aa, O: AsExpr as ao](x: A, offset: O)(using
    ia: CanInWindow[aa.K],
    no: SqlNumber[ao.R],
    io: CanInWindow[ao.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NullsTreatmentFunc(
                "LAG",
                aa.asExpr(x).asSqlExpr :: ao.asExpr(offset).asSqlExpr :: Nil,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

def lag[A: AsExpr as aa, O: AsExpr as ao, D: AsExpr as ad](x: A, offset: O, default: D)(using
    wa: CanInWindow[aa.K],
    no: SqlNumber[ao.R],
    wo: CanInWindow[ao.K],
    wd: CanInWindow[ad.K],
    r: Return[Unwrap[aa.R, Option], Unwrap[ad.R, Option], true],
    c: QueryContext
): Expr[r.R, WindowWithoutOver] =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAG",
            aa.asExpr(x).asSqlExpr ::
                ao.asExpr(offset).asSqlExpr ::
                ad.asExpr(default).asSqlExpr :: Nil
            ,
            None
        )
    )

def lagIgnoreNulls[A: AsExpr as aa, O: AsExpr as ao, D: AsExpr as ad](x: A, offset: O, default: D)(using
    wa: CanInWindow[aa.K],
    no: SqlNumber[ao.R],
    wo: CanInWindow[ao.K],
    wd: CanInWindow[ad.K],
    r: Return[Unwrap[aa.R, Option], Unwrap[ad.R, Option], true],
    c: QueryContext
): Expr[r.R, WindowWithoutOver] =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LAG",
            aa.asExpr(x).asSqlExpr ::
                ao.asExpr(offset).asSqlExpr ::
                ad.asExpr(default).asSqlExpr :: Nil
            ,
            Some(SqlWindowNullsMode.Ignore)
        )
    )

def lead[A: AsExpr as aa](x: A)(using
    i: CanInWindow[aa.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NullsTreatmentFunc(
                "LEAD",
                aa.asExpr(x).asSqlExpr :: Nil,
                None
            )
        )
    )

def leadIgnoreNulls[A: AsExpr as aa](x: A)(using
    i: CanInWindow[aa.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NullsTreatmentFunc(
                "LEAD",
                aa.asExpr(x).asSqlExpr :: Nil,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

def lead[A: AsExpr as aa, O: AsExpr as ao](x: A, offset: O)(using
    ia: CanInWindow[aa.K],
    no: SqlNumber[ao.R],
    io: CanInWindow[ao.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NullsTreatmentFunc(
                "LEAD",
                aa.asExpr(x).asSqlExpr :: ao.asExpr(offset).asSqlExpr :: Nil,
                None
            )
        )
    )

def leadIgnoreNulls[A: AsExpr as aa, O: AsExpr as ao](x: A, offset: O)(using
    ia: CanInWindow[aa.K],
    no: SqlNumber[ao.R],
    io: CanInWindow[ao.K],
    to: ToOption[Expr[aa.R, WindowWithoutOver]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.NullsTreatmentFunc(
                "LEAD",
                aa.asExpr(x).asSqlExpr :: ao.asExpr(offset).asSqlExpr :: Nil,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )

def lead[A: AsExpr as aa, O: AsExpr as ao, D: AsExpr as ad](x: A, offset: O, default: D)(using
    wa: CanInWindow[aa.K],
    no: SqlNumber[ao.R],
    wo: CanInWindow[ao.K],
    wd: CanInWindow[ad.K],
    r: Return[Unwrap[aa.R, Option], Unwrap[ad.R, Option], true],
    c: QueryContext
): Expr[r.R, WindowWithoutOver] =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LEAD",
            aa.asExpr(x).asSqlExpr ::
                ao.asExpr(offset).asSqlExpr ::
                ad.asExpr(default).asSqlExpr :: Nil
            ,
            None
        )
    )

def leadIgnoreNulls[A: AsExpr as aa, O: AsExpr as ao, D: AsExpr as ad](x: A, offset: O, default: D)(using
    wa: CanInWindow[aa.K],
    no: SqlNumber[ao.R],
    wo: CanInWindow[ao.K],
    wd: CanInWindow[ad.K],
    r: Return[Unwrap[aa.R, Option], Unwrap[ad.R, Option], true],
    c: QueryContext
): Expr[r.R, WindowWithoutOver] =
    Expr(
        SqlExpr.NullsTreatmentFunc(
            "LEAD",
            aa.asExpr(x).asSqlExpr ::
                ao.asExpr(offset).asSqlExpr ::
                ad.asExpr(default).asSqlExpr :: Nil
            ,
            Some(SqlWindowNullsMode.Ignore)
        )
    )