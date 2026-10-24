package sqala.static.dsl

import java.time.OffsetTime
import java.time.OffsetDateTime
import java.time.LocalTime
import java.time.LocalDate
import java.time.LocalDateTime
import sqala.ir.IrExpr
import sqala.metadata.SqlString
import sqala.metadata.SqlNumber

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
    s: ValidateArgType[SqlString, at.R, "string"],
    n: ValidateArgType[SqlNumber, as.R, "numeric"],
    c: CombineKind[at.K, as.K]
): Expr[Option[String], c.R] =
    Expr(
        IrExpr.SubstringFunc(
            at.asExpr(x).expr,
            as.asExpr(start).expr,
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
    s: ValidateArgType[SqlString, at.R, "string"],
    ns: ValidateArgType[SqlNumber, as.R, "numeric"],
    ne: ValidateArgType[SqlNumber, ae.R, "numeric"],
    cs: CombineKind[at.K, as.K],
    c: CombineKind[cs.R, ae.K]
): Expr[Option[String], c.R] =
    Expr(
        IrExpr.SubstringFunc(
            at.asExpr(x).expr,
            as.asExpr(start).expr,
            Some(ae.asExpr(end).expr)
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
    s: ValidateArgType[SqlString, a.R, "string"],
    kt: KindToTuple[a.K]
): Expr[a.R, Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "UPPER",
            a.asExpr(x).expr :: Nil,
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
    s: ValidateArgType[SqlString, a.R, "string"],
    kt: KindToTuple[a.K]
): Expr[a.R, Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "LOWER",
            a.asExpr(x).expr :: Nil,
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
    s: ValidateArgType[SqlString, at.R, "string"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    c: CombineKind[at.K, an.K]
): Expr[Option[String], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "LPAD",
            at.asExpr(x).expr :: an.asExpr(n).expr :: Nil,
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
    s: ValidateArgType[SqlString, at.R, "string"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    sp: ValidateArgType[SqlString, ap.R, "string"],
    cn: CombineKind[at.K, an.K],
    c: CombineKind[cn.R, ap.K]
): Expr[Option[String], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "LPAD",
            at.asExpr(x).expr :: an.asExpr(n).expr :: ap.asExpr(pad).expr :: Nil,
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
    s: ValidateArgType[SqlString, at.R, "string"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    c: CombineKind[at.K, an.K]
): Expr[Option[String], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "RPAD",
            at.asExpr(x).expr :: an.asExpr(n).expr :: Nil,
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
    s: ValidateArgType[SqlString, at.R, "string"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    sp: ValidateArgType[SqlString, ap.R, "string"],
    cn: CombineKind[at.K, an.K],
    c: CombineKind[cn.R, ap.K]
): Expr[Option[String], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "RPAD",
            at.asExpr(x).expr :: an.asExpr(n).expr :: ap.asExpr(pad).expr :: Nil,
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
    s: ValidateArgType[SqlString, a.R, "string"],
    kt: KindToTuple[a.K]
): Expr[Option[String], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "BTRIM",
            a.asExpr(x).expr :: Nil,
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
    sa: ValidateArgType[SqlString, aa.R, "string"],
    sb: ValidateArgType[SqlString, ab.R, "string"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[String], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "BTRIM",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
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
    s: ValidateArgType[SqlString, a.R, "string"],
    kt: KindToTuple[a.K]
): Expr[Option[String], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "LTRIM",
            a.asExpr(x).expr :: Nil,
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
    sa: ValidateArgType[SqlString, aa.R, "string"],
    sb: ValidateArgType[SqlString, ab.R, "string"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[String], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "LTRIM",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
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
    s: ValidateArgType[SqlString, a.R, "string"],
    kt: KindToTuple[a.K]
): Expr[Option[String], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "RTRIM",
            a.asExpr(x).expr :: Nil,
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
    sa: ValidateArgType[SqlString, aa.R, "string"],
    sb: ValidateArgType[SqlString, ab.R, "string"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[String], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "RTRIM",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
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
    sa: ValidateArgType[SqlString, aa.R, "string"],
    sb: ValidateArgType[SqlString, ab.R, "string"],
    n: ValidateArgType[SqlNumber, as.R, "numeric"],
    cb: CombineKind[aa.K, ab.K],
    c: CombineKind[cb.R, as.K]
): Expr[Option[String], c.R] =
    Expr(
        IrExpr.OverlayFunc(
            aa.asExpr(x).expr,
            ab.asExpr(y).expr,
            as.asExpr(start).expr,
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
    sa: ValidateArgType[SqlString, aa.R, "string"],
    sb: ValidateArgType[SqlString, ab.R, "string"],
    ns: ValidateArgType[SqlNumber, as.R, "numeric"],
    ne: ValidateArgType[SqlNumber, ae.R, "numeric"],
    cb: CombineKind[aa.K, ab.K],
    cs: CombineKind[cb.R, as.K],
    c: CombineKind[cs.R, ae.K]
): Expr[Option[String], c.R] =
    Expr(
        IrExpr.OverlayFunc(
            aa.asExpr(x).expr,
            ab.asExpr(y).expr,
            as.asExpr(start).expr,
            Some(ae.asExpr(end).expr)
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
    sa: ValidateArgType[SqlString, aa.R, "string"],
    sb: ValidateArgType[SqlString, ab.R, "string"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "REGEXP_LIKE",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
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
    sa: ValidateArgType[SqlString, aa.R, "string"],
    sb: ValidateArgType[SqlString, ab.R, "string"],
    c: CombineKind[aa.K, ab.K]
): Expr[Int, c.R] =
    Expr(
        IrExpr.PositionFunc(
            aa.asExpr(x).expr,
            ab.asExpr(y).expr
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
    s: ValidateArgType[SqlString, a.R, "string"],
    kt: KindToTuple[a.K]
): Expr[Option[Int], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "CHAR_LENGTH",
            a.asExpr(x).expr :: Nil,
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
    s: ValidateArgType[SqlString, a.R, "string"],
    kt: KindToTuple[a.K]
): Expr[Option[Int], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "OCTET_LENGTH",
            a.asExpr(x).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[a.R, Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ABS",
            a.asExpr(x).expr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[BigDecimal], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "MOD",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "SIN",
            a.asExpr(x).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "COS",
            a.asExpr(x).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "TAN",
            a.asExpr(x).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "SINH",
            a.asExpr(x).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "COSH",
            a.asExpr(x).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "TANH",
            a.asExpr(x).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ASIN",
            a.asExpr(x).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ACOS",
            a.asExpr(x).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ATAN",
            a.asExpr(x).expr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[BigDecimal], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "LOG",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "LOG10",
            a.asExpr(x).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "LN",
            a.asExpr(x).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "EXP",
            a.asExpr(x).expr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[BigDecimal], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "POWER",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[Option[BigDecimal], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "SQRT",
            a.asExpr(x).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[Option[Long], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "FLOOR",
            a.asExpr(x).expr :: Nil,
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K]
): Expr[Option[Long], Composite[kt.R]] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "CEIL",
            a.asExpr(x).expr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    c: CombineKind[aa.K, ab.K]
): Expr[Option[BigDecimal], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "ROUND",
            aa.asExpr(x).expr :: ab.asExpr(y).expr :: Nil,
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
    na: ValidateArgType[SqlNumber, aa.R, "numeric"],
    nb: ValidateArgType[SqlNumber, ab.R, "numeric"],
    nc: ValidateArgType[SqlNumber, ac.R, "numeric"],
    nd: ValidateArgType[SqlNumber, ad.R, "numeric"],
    cb: CombineKind[aa.K, ab.K],
    cc: CombineKind[cb.R, ac.K],
    c: CombineKind[cc.R, ad.K]
): Expr[Option[Int], c.R] =
    Expr(
        IrExpr.GeneralFunc(
            None,
            "WIDTH_BUCKET",
            aa.asExpr(x).expr ::
            ab.asExpr(min).expr ::
            ac.asExpr(max).expr ::
            ad.asExpr(num).expr :: Nil,
            Nil,
            Nil,
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
        IrExpr.IdentFunc("CURRENT_DATE")
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
        IrExpr.IdentFunc("CURRENT_TIME")
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
        IrExpr.IdentFunc("CURRENT_TIMESTAMP")
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
        IrExpr.IdentFunc("LOCALTIME")
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
        IrExpr.IdentFunc("LOCALTIMESTAMP")
    )