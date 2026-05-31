package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.quantifier.SqlQuantifier
import sqala.metadata.*

import java.time.*

def count[CL <: Int]()(using QueryContext[CL]): Expr[Long, Agg[EmptyTuple]] =
    Expr(SqlExpr.CountAsteriskFunc(None, None))

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
                withinGroup.asSqlOrderBy :: Nil,
                None
            )
        )
    )

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
                withinGroup.asSqlOrderBy :: Nil,
                None
            )
        )
    )

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
            withinGroup.asSqlOrderBy :: Nil,
            None
        )
    )

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

final case class JsonObjectPair[KS <: Tuple](key: SqlExpr, value: SqlExpr)

extension [K, CL <: Int](key: K)(using qc: QueryContext[CL], ak: AsExpr[K, CL], ask: AsSqlExpr[ak.R], ktk: KindToTuple[ak.K])
    infix def value[V](value: V)(using
        av: AsExpr[V, CL],
        asv: AsSqlExpr[av.R],
        ktv: KindToTuple[av.K],
        c: CombineKindTuple[ktk.R, ktv.R]
    ): JsonObjectPair[c.R] =
        JsonObjectPair(ak.asExpr(key).asSqlExpr, av.asExpr(value).asSqlExpr)

trait AsJsonObject[T]:
    type KS <: Tuple

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

def currentDate[CL <: Int]()(using QueryContext[CL]): Expr[LocalDate, Composite[EmptyTuple]] =
    Expr(
        SqlExpr.IdentFunc("CURRENT_DATE")
    )

def currentTime[CL <: Int]()(using QueryContext[CL]): Expr[OffsetTime, Composite[EmptyTuple]] =
    Expr(
        SqlExpr.IdentFunc("CURRENT_TIME")
    )

def currentTimestamp[CL <: Int]()(using QueryContext[CL]): Expr[OffsetDateTime, Composite[EmptyTuple]] =
    Expr(
        SqlExpr.IdentFunc("CURRENT_TIMESTAMP")
    )

def localTime[CL <: Int]()(using QueryContext[CL]): Expr[LocalTime, Composite[EmptyTuple]] =
    Expr(
        SqlExpr.IdentFunc("LOCALTIME")
    )

def localTimestamp[CL <: Int]()(using QueryContext[CL]): Expr[LocalDateTime, Composite[EmptyTuple]] =
    Expr(
        SqlExpr.IdentFunc("LOCALTIMESTAMP")
    )

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

def percentRank[CL <: Int]()(using QueryContext[CL]): WindowFunc[Long, EmptyTuple] =
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