package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.quantifier.SqlQuantifier
import sqala.static.metadata.*

import java.time.{LocalDate, LocalTime, OffsetDateTime, OffsetTime}
import scala.compiletime.ops.boolean.||

@aggFunction
def count()(using QueryContext): Expr[Long] =
    Expr(SqlExpr.CountAsteriskFunc(None, None))

@aggFunction
def count[T: AsExpr as a](x: T)(using QueryContext): Expr[Long] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "COUNT",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def countDistinct[T: AsExpr as a](x: T)(using QueryContext): Expr[Long] =
    Expr(
        SqlExpr.StandardFunc(
            Some(SqlQuantifier.Distinct),
            "COUNT",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def sum[T: AsExpr as a](x: T)(using 
    n: SqlNumber[a.R],
    to: ToOption[Expr[a.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                None,
                "SUM",
                a.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

@aggFunction
def avg[T: AsExpr as a](x: T)(using 
    SqlNumber[a.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "AVG",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def max[T: AsExpr as a](x: T)(using 
    to: ToOption[Expr[a.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                None,
                "MAX",
                a.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

@aggFunction
def min[T: AsExpr as a](x: T)(using 
    to: ToOption[Expr[a.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                None,
                "MIN",
                a.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

@aggFunction
def anyValue[T: AsExpr as a](x: T)(using 
    to: ToOption[Expr[a.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                None,
                "ANY_VALUE",
                a.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

@aggFunction
def stddevPop[T: AsExpr as a](x: T)(using 
    SqlNumber[a.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "STDDEV_POP",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def stddevSamp[T: AsExpr as a](x: T)(using 
    SqlNumber[a.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "STDDEV_SAMP",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def varPop[T: AsExpr as a](x: T)(using 
    SqlNumber[a.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "VAR_POP",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def varSamp[T: AsExpr as a](x: T)(using 
    SqlNumber[a.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "VAR_SAMP",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def covarPop[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "COVAR_POP",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def covarSamp[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "COVAR_SAMP",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def corr[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "CORR",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def regrSlope[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "REGR_SLOPE",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def regrIntercept[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "REGR_INTERCEPT",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def regrCount[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Long] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "REGR_INTERCEPT",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def regrR2[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "REGR_R2",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def regrAvgx[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "REGR_AVGX",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def regrAvgy[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "REGR_AVGY",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def regrSxx[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "REGR_SXX",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def regrSyy[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "REGR_SYY",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def regrSxy[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "REGR_SXY",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def percentileCont[T: AsExpr as a](x: T, withinGroup: Sort)(using 
    SqlNumber[a.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "PERCENTILE_CONT",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            withinGroup.asSqlOrderBy :: Nil,
            None
        )
    )

@aggFunction
def percentileDisc[T: AsExpr as a](x: T, withinGroup: Sort)(using 
    SqlNumber[a.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "PERCENTILE_DISC",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            withinGroup.asSqlOrderBy :: Nil,
            None
        )
    )

@aggFunction
def listAgg[T: AsExpr as at, S: AsExpr as as](x: T, separator: S, withinGroup: Sort)(using 
    SqlString[at.R],
    SqlString[as.R],
    QueryContext
): Expr[Option[String]] =
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

@aggFunction
def stringAgg[T: AsExpr as at, S: AsExpr as as](x: T, separator: S, withinGroup: Sort)(using 
    SqlString[at.R],
    SqlString[as.R],
    QueryContext
): Expr[Option[String]] =
    listAgg(x, separator, withinGroup)

@aggFunction
def groupConcat[T: AsExpr as at, S: AsExpr as as](x: T, separator: S, withinGroup: Sort)(using 
    SqlString[at.R],
    SqlString[as.R],
    QueryContext
): Expr[Option[String]] =
    listAgg(x, separator, withinGroup)

@aggFunction
def jsonObjectAgg[K: AsExpr as ak, V: AsExpr as av](key: K, value: V)(using 
    QueryContext
): Expr[Option[Json]] =
    Expr(
        SqlExpr.JsonObjectAggFunc(
            SqlJsonObjectElement(ak.asExpr(key).asSqlExpr, av.asExpr(value).asSqlExpr),
            None,
            None,
            None,
            None
        )
    )

@aggFunction
def jsonArrayAgg[A: AsExpr as aa](x: A)(using 
    QueryContext
): Expr[Option[Json]] =
    Expr(
        SqlExpr.JsonArrayAggFunc(
            SqlJsonArrayElement(aa.asExpr(x).asSqlExpr, None),
            Nil,
            None,
            None,
            None
        )
    )

@aggFunction
def classifier()(using QueryContext): Expr[String] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "CLASSIFIER",
            Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def matchNumber()(using QueryContext): Expr[Int] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "MATCH_NUMBER",
            Nil,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def first[A: AsExpr as aa](x: A)(using
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                None,
                "FIRST",
                aa.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

@aggFunction
def first[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    sn: SqlNumber[an.R],
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                None,
                "FIRST",
                aa.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

@aggFunction
def last[A: AsExpr as aa](x: A)(using
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                None,
                "LAST",
                aa.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

@aggFunction
def last[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    sn: SqlNumber[an.R],
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                None,
                "LAST",
                aa.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

@aggFunction
def prev[A: AsExpr as aa](x: A)(using
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                None,
                "PREV",
                aa.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

@aggFunction
def prev[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    sn: SqlNumber[an.R],
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                None,
                "PREV",
                aa.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

@aggFunction
def next[A: AsExpr as aa](x: A)(using
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                None,
                "NEXT",
                aa.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

@aggFunction
def next[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    sn: SqlNumber[an.R],
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                None,
                "NEXT",
                aa.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

@function
def substring[T: AsExpr as at, S: AsExpr as as](x: T, start: S)(using 
    SqlString[at.R],
    SqlNumber[as.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.SubstringFunc(
            at.asExpr(x).asSqlExpr,
            as.asExpr(start).asSqlExpr,
            None
        )
    )

@function
def substring[T: AsExpr as at, S: AsExpr as as, E: AsExpr as ae](x: T, start: S, end: E)(using 
    SqlString[at.R],
    SqlNumber[as.R],
    SqlNumber[ae.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.SubstringFunc(
            at.asExpr(x).asSqlExpr,
            as.asExpr(start).asSqlExpr,
            Some(ae.asExpr(end).asSqlExpr)
        )
    )

@function
def upper[T: AsExpr as a](x: T)(using
    SqlString[a.R],
    QueryContext
): Expr[a.R] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "UPPER",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def lower[T: AsExpr as a](x: T)(using
    SqlString[a.R],
    QueryContext
): Expr[a.R] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "LOWER",
            a.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def lpad[T: AsExpr as at, N: AsExpr as an](x: T, n: N)(using
    SqlString[at.R],
    SqlNumber[an.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "LPAD",
            at.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def lpad[T: AsExpr as at, N: AsExpr as an, P: AsExpr as ap](x: T, n: N, pad: P)(using
    SqlString[at.R],
    SqlNumber[an.R],
    SqlString[ap.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "LPAD",
            at.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: ap.asExpr(pad).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def rpad[T: AsExpr as at, N: AsExpr as an](x: T, n: N)(using
    SqlString[at.R],
    SqlNumber[an.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "RPAD",
            at.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def rpad[T: AsExpr as at, N: AsExpr as an, P: AsExpr as ap](x: T, n: N, pad: P)(using
    SqlString[at.R],
    SqlNumber[an.R],
    SqlString[ap.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "RPAD",
            at.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: ap.asExpr(pad).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def btrim[A: AsExpr as aa](x: A)(using
    SqlString[aa.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "BTRIM",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def btrim[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlString[aa.R],
    SqlString[ab.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "BTRIM",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def ltrim[A: AsExpr as aa](x: A)(using
    SqlString[aa.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "LTRIM",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def ltrim[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlString[aa.R],
    SqlString[ab.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "LTRIM",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def rtrim[A: AsExpr as aa](x: A)(using
    SqlString[aa.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "RTRIM",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def rtrim[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlString[aa.R],
    SqlString[ab.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "RTRIM",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def overlay[A: AsExpr as aa, B: AsExpr as ab, S: AsExpr as as](x: A, y: B, start: S)(using 
    SqlString[aa.R],
    SqlString[ab.R],
    SqlNumber[as.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.OverlayFunc(
            aa.asExpr(x).asSqlExpr,
            ab.asExpr(y).asSqlExpr,
            as.asExpr(start).asSqlExpr,
            None
        )
    )

@function
def overlay[A: AsExpr as aa, B: AsExpr as ab, S: AsExpr as as, E: AsExpr as ae](x: A, y: B, start: S, end: E)(using 
    SqlString[aa.R],
    SqlString[ab.R],
    SqlNumber[as.R],
    SqlNumber[ae.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.OverlayFunc(
            aa.asExpr(x).asSqlExpr,
            ab.asExpr(y).asSqlExpr,
            as.asExpr(start).asSqlExpr,
            Some(ae.asExpr(end).asSqlExpr)
        )
    )

@function
def regexpLike[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlString[aa.R],
    SqlString[ab.R],
): Expr[Option[Boolean]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "REGEXP_LIKE",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def euclideanDistance[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlVector[aa.R],
    SqlVector[ab.R],
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.VectorDistanceFunc(
            aa.asExpr(x).asSqlExpr,
            ab.asExpr(y).asSqlExpr,
            SqlVectorDistanceMode.Euclidean
        )
    )

@function
def cosineDistance[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlVector[aa.R],
    SqlVector[ab.R],
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.VectorDistanceFunc(
            aa.asExpr(x).asSqlExpr,
            ab.asExpr(y).asSqlExpr,
            SqlVectorDistanceMode.Cosine
        )
    )

@function
def dotDistance[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlVector[aa.R],
    SqlVector[ab.R],
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.VectorDistanceFunc(
            aa.asExpr(x).asSqlExpr,
            ab.asExpr(y).asSqlExpr,
            SqlVectorDistanceMode.Dot
        )
    )

@function
def manhattanDistance[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlVector[aa.R],
    SqlVector[ab.R],
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.VectorDistanceFunc(
            aa.asExpr(x).asSqlExpr,
            ab.asExpr(y).asSqlExpr,
            SqlVectorDistanceMode.Manhattan
        )
    )

@function
def position[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlString[aa.R],
    SqlString[aa.R],
    QueryContext
): Expr[Int] =
    Expr(
        SqlExpr.PositionFunc(
            aa.asExpr(x).asSqlExpr,
            ab.asExpr(y).asSqlExpr
        )
    )

trait ExtractArg[T]

object ExtractArg:
    given dateTime[T: SqlDateTime]: ExtractArg[T]()

    given interval[T: SqlInterval]: ExtractArg[T]()

@function
def extract[A: AsExpr as aa](x: A, unit: TimeUnit)(using
    ExtractArg[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.ExtractFunc(
            unit.unit,
            aa.asExpr(x).asSqlExpr
        )
    )

@function
def charLength[A: AsExpr as aa](x: A)(using
    SqlString[aa.R],
    QueryContext
): Expr[Option[Int]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "CHAR_LENGTH",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def octetLength[A: AsExpr as aa](x: A)(using
    SqlString[aa.R],
    QueryContext
): Expr[Option[Int]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "OCTET_LENGTH",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def abs[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[aa.R] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ABS",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def mod[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "MOD",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def sin[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "SIN",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def cos[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "COS",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def tan[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "TAN",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def sinh[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "SINH",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def cosh[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "COSH",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def tanh[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "TANH",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def asin[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ASIN",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def acos[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ACOS",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def atan[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ATAN",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def log[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "LOG",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def log10[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "LOG10",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def ln[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "LN",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def exp[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "EXP",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def power[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "POWER",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def sqrt[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "SQRT",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def floor[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[Option[Long]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "FLOOR",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def ceil[A: AsExpr as aa](x: A)(using 
    SqlNumber[aa.R],
    QueryContext
): Expr[Option[Long]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "CEIL",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def round[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ROUND",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def widthBucket[A: AsExpr as aa, B: AsExpr as ab, C: AsExpr as ac, D: AsExpr as ad](x: A, min: B, max: C, num: D)(using 
    SqlNumber[aa.R],
    SqlNumber[ab.R],
    SqlNumber[ac.R],
    SqlNumber[ad.R],
    QueryContext
): Expr[Option[Int]] =
    Expr(
        SqlExpr.StandardFunc(
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

@function
def json[A: AsExpr as aa](x: A)(using 
    SqlString[aa.R],
    QueryContext
): Expr[Option[Json]] =
    Expr(
        SqlExpr.JsonParseFunc(
            aa.asExpr(x).asSqlExpr,
            None,
            Some(SqlJsonUniqueness.With)
        )
    )

@function
def jsonValue[A: AsExpr as aa, P: AsExpr as ap](x: A, path: P)(using 
    SqlJson[aa.R],
    SqlString[ap.R],
    QueryContext
): Expr[Option[String]] =
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

@function
def jsonQuery[A: AsExpr as aa, P: AsExpr as ap](x: A, path: P)(using 
    SqlJson[aa.R],
    SqlString[ap.R],
    QueryContext
): Expr[Option[Json]] =
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

@function
def jsonExists[A: AsExpr as aa, P: AsExpr as ap](x: A, path: P)(using 
    SqlJson[aa.R],
    SqlString[ap.R],
    QueryContext
): Expr[Option[Boolean]] =
    Expr(
        SqlExpr.JsonExistsFunc(
            aa.asExpr(x).asSqlExpr,
            ap.asExpr(path).asSqlExpr,
            Nil,
            None
        )
    )

case class JsonObjectPair(key: SqlExpr, value: SqlExpr)

extension [K: AsExpr as ak](key: K)
    infix def value[V: AsExpr as av](value: V)(using QueryContext): JsonObjectPair =
        JsonObjectPair(ak.asExpr(key).asSqlExpr, av.asExpr(value).asSqlExpr)

@function
def jsonObject(items: JsonObjectPair*)(using QueryContext): Expr[Option[Json]] =
    Expr(
        SqlExpr.JsonObjectFunc(
            items.toList.map(i => SqlJsonObjectElement(i.key, i.value)),
            None,
            None,
            None
        )
    )

@function
def jsonArray[A: AsExpr as aa](items: A)(using QueryContext): Expr[Option[Json]] =
    Expr(
        SqlExpr.JsonArrayFunc(
            aa.exprs(items).map(i => SqlJsonArrayElement(i.asSqlExpr, None)),
            None,
            None
        )
    )

@function
def currentDate()(using QueryContext): Expr[LocalDate] =
    Expr(
        SqlExpr.IdentFunc("CURRENT_DATE")
    )

@function
def currentTime()(using QueryContext): Expr[OffsetTime] =
    Expr(
        SqlExpr.IdentFunc("CURRENT_TIME")
    )

@function
def currentTimestamp()(using QueryContext): Expr[OffsetDateTime] =
    Expr(
        SqlExpr.IdentFunc("CURRENT_TIMESTAMP")
    )

@function
def localTime()(using QueryContext): Expr[LocalTime] =
    Expr(
        SqlExpr.IdentFunc("LOCALTIME")
    )

@function
def localTimestamp()(using QueryContext): Expr[OffsetDateTime] =
    Expr(
        SqlExpr.IdentFunc("LOCALTIMESTAMP")
    )

@function
def stGeomFromText[A: AsExpr as aa, S: AsExpr as as](x: A, srid: S)(using
    SqlString[aa.R],
    SqlNumber[as.R],
    QueryContext
): Expr[Option[Geometry]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_GeomFromText",
            aa.asExpr(x).asSqlExpr :: as.asExpr(srid).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stAsText[A: AsExpr as aa](x: A)(using
    SqlGeometry[aa.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_AsText",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stAsGeoJson[A: AsExpr as aa](x: A)(using
    SqlGeometry[aa.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_AsGeoJson",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stGeometryType[A: AsExpr as aa](x: A)(using
    SqlGeometry[aa.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_GeometryType",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stX[A: AsExpr as aa](x: A)(using
    SqlGeometry[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_X",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stY[A: AsExpr as aa](x: A)(using
    SqlGeometry[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_Y",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stArea[A: AsExpr as aa](x: A)(using
    SqlGeometry[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_Area",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stLength[A: AsExpr as aa](x: A)(using
    SqlGeometry[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_Length",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stContains[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlGeometry[aa.R],
    SqlGeometry[ab.R],
    QueryContext
): Expr[Option[Boolean]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_Contains",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stWithin[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlGeometry[aa.R],
    SqlGeometry[ab.R],
    QueryContext
): Expr[Option[Boolean]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_Within",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stIntersects[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlGeometry[aa.R],
    SqlGeometry[ab.R],
    QueryContext
): Expr[Option[Boolean]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_Intersects",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stTouches[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlGeometry[aa.R],
    SqlGeometry[ab.R],
    QueryContext
): Expr[Option[Boolean]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_Touches",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stOverlaps[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlGeometry[aa.R],
    SqlGeometry[ab.R],
    QueryContext
): Expr[Option[Boolean]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_Overlaps",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stCrosses[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlGeometry[aa.R],
    SqlGeometry[ab.R],
    QueryContext
): Expr[Option[Boolean]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_Crosses",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stDisjoint[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlGeometry[aa.R],
    SqlGeometry[ab.R],
    QueryContext
): Expr[Option[Boolean]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_Disjoint",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stIntersection[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlGeometry[aa.R],
    SqlGeometry[ab.R],
    QueryContext
): Expr[Option[Geometry]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_Intersection",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stUnion[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlGeometry[aa.R],
    SqlGeometry[ab.R],
    QueryContext
): Expr[Option[Geometry]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_Union",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stDifference[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlGeometry[aa.R],
    SqlGeometry[ab.R],
    QueryContext
): Expr[Option[Geometry]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_Difference",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stSymDifference[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlGeometry[aa.R],
    SqlGeometry[ab.R],
    QueryContext
): Expr[Option[Geometry]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_SymDifference",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@function
def stDistance[A: AsExpr as aa, B: AsExpr as ab](x: A, y: B)(using
    SqlGeometry[aa.R],
    SqlGeometry[ab.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ST_Distance",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def rank()(using QueryContext): Expr[Long] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "RANK",
            Nil,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def denseRank()(using QueryContext): Expr[Long] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "DENSE_RANK",
            Nil,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def percentRank()(using QueryContext): Expr[Long] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "PERCENT_RANK",
            Nil,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def cumeDist()(using QueryContext): Expr[Long] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "CUME_DIST",
            Nil,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def rowNumber()(using QueryContext): Expr[Long] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "ROW_NUMBER",
            Nil,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def ntile[A: AsExpr as aa](x: A)(using
    SqlNumber[aa.R],
    QueryContext
): Expr[Long] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "NTILE",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def firstValue[A: AsExpr as aa](x: A)(using
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                None,
                "FIRST_VALUE",
                aa.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )
    
@windowFunction
def lastValue[A: AsExpr as aa](x: A)(using
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                None,
                "LAST_VALUE",
                aa.asExpr(x).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )

@windowFunction
def nthValue[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    nn: SqlNumber[an.R],
    to: ToOption[Expr[aa.R]],
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

@windowFunction
def lag[A: AsExpr as aa](x: A)(using
    QueryContext
): Expr[aa.R] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "LAG",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def lag[A: AsExpr as aa, O: AsExpr as ao](x: A, offset: O)(using
    SqlNumber[ao.R],
    QueryContext
): Expr[aa.R] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "LAG",
            aa.asExpr(x).asSqlExpr :: ao.asExpr(offset).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def lag[A: AsExpr as aa, O: AsExpr as ao, D: AsExpr as ad](x: A, offset: O, default: D)(using
    no: SqlNumber[ao.R],
    r: Return[Unwrap[aa.R, Option], Unwrap[ad.R, Option], IsOption[aa.R] || IsOption[ad.R]],
    c: QueryContext
): Expr[r.R] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "LAG",
            aa.asExpr(x).asSqlExpr :: 
                ao.asExpr(offset).asSqlExpr ::
                ad.asExpr(default).asSqlExpr :: Nil
            ,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def lead[A: AsExpr as aa](x: A)(using
    QueryContext
): Expr[aa.R] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "LEAD",
            aa.asExpr(x).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def lead[A: AsExpr as aa, O: AsExpr as ao](x: A, offset: O)(using
    SqlNumber[ao.R],
    QueryContext
): Expr[aa.R] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "LEAD",
            aa.asExpr(x).asSqlExpr :: ao.asExpr(offset).asSqlExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def lead[A: AsExpr as aa, O: AsExpr as ao, D: AsExpr as ad](x: A, offset: O, default: D)(using
    no: SqlNumber[ao.R],
    r: Return[Unwrap[aa.R, Option], Unwrap[ad.R, Option], IsOption[aa.R] || IsOption[ad.R]],
    c: QueryContext
): Expr[r.R] =
    Expr(
        SqlExpr.StandardFunc(
            None,
            "LEAD",
            aa.asExpr(x).asSqlExpr :: 
                ao.asExpr(offset).asSqlExpr ::
                ad.asExpr(default).asSqlExpr :: Nil
            ,
            Nil,
            Nil,
            None
        )
    )