package sqala.static.dsl

import sqala.ast.expr.{SqlExpr, SqlJsonUniqueness, SqlVectorDistanceMode}
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
            "COUNT",
            a.asExpr(x).asSqlExpr :: Nil,
            None,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def countDistinct[T: AsExpr as a](x: T)(using QueryContext): Expr[Long] =
    Expr(
        SqlExpr.StandardFunc(
            "COUNT",
            a.asExpr(x).asSqlExpr :: Nil,
            Some(SqlQuantifier.Distinct),
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
                "SUM",
                a.asExpr(x).asSqlExpr :: Nil,
                None,
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
            "AVG",
            a.asExpr(x).asSqlExpr :: Nil,
            None,
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
                "MAX",
                a.asExpr(x).asSqlExpr :: Nil,
                None,
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
                "MIN",
                a.asExpr(x).asSqlExpr :: Nil,
                None,
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
                "ANY_VALUE",
                a.asExpr(x).asSqlExpr :: Nil,
                None,
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
            "STDDEV_POP",
            a.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "STDDEV_SAMP",
            a.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "VAR_POP",
            a.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "VAR_SAMP",
            a.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "COVAR_POP",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "COVAR_SAMP",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "CORR",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "REGR_SLOPE",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "REGR_INTERCEPT",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "REGR_INTERCEPT",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "REGR_R2",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "REGR_AVGX",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "REGR_AVGY",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "REGR_SXX",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "REGR_SYY",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "REGR_SXY",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
            Nil,
            Nil,
            None
        )
    )

@aggFunction
def percentileCont[T: AsExpr as a, W](x: T, withinGroup: Sort[W])(using 
    SqlNumber[a.R],
    SqlNumber[W],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            "PERCENTILE_CONT",
            a.asExpr(x).asSqlExpr :: Nil,
            None,
            Nil,
            withinGroup.asSqlOrderBy :: Nil,
            None
        )
    )

@aggFunction
def percentileDisc[T: AsExpr as a, W](x: T, withinGroup: Sort[W])(using 
    SqlNumber[a.R],
    SqlNumber[W],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.StandardFunc(
            "PERCENTILE_DISC",
            a.asExpr(x).asSqlExpr :: Nil,
            None,
            Nil,
            withinGroup.asSqlOrderBy :: Nil,
            None
        )
    )

@aggFunction
def listagg[T: AsExpr as at, S: AsExpr as as, W](x: T, separator: S, withinGroup: Sort[W])(using 
    SqlString[at.R],
    SqlString[as.R],
    QueryContext
): Expr[Option[String]] =
    Expr(
        SqlExpr.ListAggFunc(
            at.asExpr(x).asSqlExpr,
            as.asExpr(separator).asSqlExpr,
            None,
            None,
            withinGroup.asSqlOrderBy :: Nil,
            None
        )
    )

@aggFunction
def stringAgg[T: AsExpr as at, S: AsExpr as as, W](x: T, separator: S, withinGroup: Sort[W])(using 
    SqlString[at.R],
    SqlString[as.R],
    QueryContext
): Expr[Option[String]] =
    listagg(x, separator, withinGroup)

@aggFunction
def groupConcat[T: AsExpr as at, S: AsExpr as as, W](x: T, separator: S, withinGroup: Sort[W])(using 
    SqlString[at.R],
    SqlString[as.R],
    QueryContext
): Expr[Option[String]] =
    listagg(x, separator, withinGroup)

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
            "UPPER",
            a.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "LOWER",
            a.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "LPAD",
            at.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
            None,
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
            "LPAD",
            at.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: ap.asExpr(pad).asSqlExpr :: Nil,
            None,
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
            "RPAD",
            at.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
            None,
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
            "RPAD",
            at.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: ap.asExpr(pad).asSqlExpr :: Nil,
            None,
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
            "BTRIM",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "BTRIM",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "LTRIM",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "LTRIM",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "RTRIM",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "RTRIM",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "REGEXP_LIKE",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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

@function
def extract[A: AsExpr as aa](x: A, unit: TimeUnit)(using
    SqlDateTime[aa.R],
    QueryContext
): Expr[Option[BigDecimal]] =
    Expr(
        SqlExpr.ExtractFunc(
            aa.asExpr(x).asSqlExpr,
            unit.unit
        )
    )

@function
def charLength[A: AsExpr as aa](x: A)(using
    SqlString[aa.R],
    QueryContext
): Expr[Option[Int]] =
    Expr(
        SqlExpr.StandardFunc(
            "CHAR_LENGTH",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "OCTET_LENGTH",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "ABS",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "MOD",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "SIN",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "COS",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "TAN",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "SINH",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "COSH",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "TANH",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "ASIN",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "ACOS",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "ATAN",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "LOG",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "LOG10",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "LN",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "EXP",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "POWER",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "SQRT",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "FLOOR",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "CEIL",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "ROUND",
            aa.asExpr(x).asSqlExpr :: ab.asExpr(y).asSqlExpr :: Nil,
            None,
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
            "WIDTH_BUCKET",
            aa.asExpr(x).asSqlExpr :: 
            ab.asExpr(min).asSqlExpr ::
            ac.asExpr(max).asSqlExpr ::
            ad.asExpr(num).asSqlExpr :: Nil
            ,
            None,
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

@windowFunction
def rank()(using QueryContext): Expr[Long] =
    Expr(
        SqlExpr.StandardFunc(
            "RANK",
            Nil,
            None,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def denseRank()(using QueryContext): Expr[Long] =
    Expr(
        SqlExpr.StandardFunc(
            "DENSE_RANK",
            Nil,
            None,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def percentRank()(using QueryContext): Expr[Long] =
    Expr(
        SqlExpr.StandardFunc(
            "PERCENT_RANK",
            Nil,
            None,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def cumeDist()(using QueryContext): Expr[Long] =
    Expr(
        SqlExpr.StandardFunc(
            "CUME_DIST",
            Nil,
            None,
            Nil,
            Nil,
            None
        )
    )

@windowFunction
def rowNumber()(using QueryContext): Expr[Long] =
    Expr(
        SqlExpr.StandardFunc(
            "ROW_NUMBER",
            Nil,
            None,
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
            "NTILE",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
                "FIRST_VALUE",
                aa.asExpr(x).asSqlExpr :: Nil,
                None,
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
                "LAST_VALUE",
                aa.asExpr(x).asSqlExpr :: Nil,
                None,
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
            "LAG",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "LAG",
            aa.asExpr(x).asSqlExpr :: ao.asExpr(offset).asSqlExpr :: Nil,
            None,
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
            "LAG",
            aa.asExpr(x).asSqlExpr :: 
                ao.asExpr(offset).asSqlExpr ::
                ad.asExpr(default).asSqlExpr :: Nil
            ,
            None,
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
            "LEAD",
            aa.asExpr(x).asSqlExpr :: Nil,
            None,
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
            "LEAD",
            aa.asExpr(x).asSqlExpr :: ao.asExpr(offset).asSqlExpr :: Nil,
            None,
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
            "LEAD",
            aa.asExpr(x).asSqlExpr :: 
                ao.asExpr(offset).asSqlExpr ::
                ad.asExpr(default).asSqlExpr :: Nil
            ,
            None,
            Nil,
            Nil,
            None
        )
    )

@matchFunction
def classifier()(using QueryContext): Expr[String] =
    Expr(
        SqlExpr.StandardFunc(
            "CLASSIFIER",
            Nil,
            None,
            Nil,
            Nil,
            None
        )
    )

@matchFunction
def matchNumber()(using QueryContext): Expr[Int] =
    Expr(
        SqlExpr.StandardFunc(
            "MATCH_NUMBER",
            Nil,
            None,
            Nil,
            Nil,
            None
        )
    )

@matchFunction
def first[A: AsExpr as aa](x: A)(using
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                "FIRST",
                aa.asExpr(x).asSqlExpr :: Nil,
                None,
                Nil,
                Nil,
                None
            )
        )
    )

@matchFunction
def first[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    sn: SqlNumber[an.R],
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                "FIRST",
                aa.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
                None,
                Nil,
                Nil,
                None
            )
        )
    )

@matchFunction
def last[A: AsExpr as aa](x: A)(using
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                "LAST",
                aa.asExpr(x).asSqlExpr :: Nil,
                None,
                Nil,
                Nil,
                None
            )
        )
    )

@matchFunction
def last[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    sn: SqlNumber[an.R],
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                "LAST",
                aa.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
                None,
                Nil,
                Nil,
                None
            )
        )
    )

@matchFunction
def prev[A: AsExpr as aa](x: A)(using
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                "PREV",
                aa.asExpr(x).asSqlExpr :: Nil,
                None,
                Nil,
                Nil,
                None
            )
        )
    )

@matchFunction
def prev[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    sn: SqlNumber[an.R],
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                "PREV",
                aa.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
                None,
                Nil,
                Nil,
                None
            )
        )
    )

@matchFunction
def next[A: AsExpr as aa](x: A)(using
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                "NEXT",
                aa.asExpr(x).asSqlExpr :: Nil,
                None,
                Nil,
                Nil,
                None
            )
        )
    )

@matchFunction
def next[A: AsExpr as aa, N: AsExpr as an](x: A, n: N)(using
    sn: SqlNumber[an.R],
    to: ToOption[Expr[aa.R]],
    c: QueryContext
): to.R =
    to.toOption(
        Expr(
            SqlExpr.StandardFunc(
                "NEXT",
                aa.asExpr(x).asSqlExpr :: an.asExpr(n).asSqlExpr :: Nil,
                None,
                Nil,
                Nil,
                None
            )
        )
    )