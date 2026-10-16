package sqala.static.dsl

import sqala.ir.IrExpr
import sqala.metadata.SqlNumber
import sqala.metadata.AsSqlExpr
import sqala.ast.expr.SqlWindowNullsMode
import sqala.ast.expr.SqlNthValueFromMode

/**
 * Returns the rank of each row with gaps. Maps to `RANK()`.
 * Must have an `over` clause.
 *
 * {{{
 * from(Post).map(p => rank().over(partitionBy(p.channelId).sortBy(p.viewCount.desc)))
 * }}}
 */
def rank[CL <: Int]()(using QueryContext[CL]): WindowPart[Long, EmptyTuple] =
    WindowPart(
        IrExpr.GeneralFunc(
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
def denseRank[CL <: Int]()(using QueryContext[CL]): WindowPart[Long, EmptyTuple] =
    WindowPart(
        IrExpr.GeneralFunc(
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
def percentRank[CL <: Int]()(using QueryContext[CL]): WindowPart[BigDecimal, EmptyTuple] =
    WindowPart(
        IrExpr.GeneralFunc(
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
def cumeDist[CL <: Int]()(using QueryContext[CL]): WindowPart[Long, EmptyTuple] =
    WindowPart(
        IrExpr.GeneralFunc(
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
def rowNumber[CL <: Int]()(using QueryContext[CL]): WindowPart[Long, EmptyTuple] =
    WindowPart(
        IrExpr.GeneralFunc(
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
    n: ValidateArgType[SqlNumber, a.R, "numeric"],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R]
): WindowPart[Long, kt.R] =
    WindowPart(
        IrExpr.GeneralFunc(
            None,
            "NTILE",
            a.asExpr(x).asIrExpr :: Nil,
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
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowPart[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "FIRST_VALUE",
                a.asExpr(x).asIrExpr :: Nil,
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
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowPart[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "FIRST_VALUE",
                a.asExpr(x).asIrExpr :: Nil,
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
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowPart[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "LAST_VALUE",
                a.asExpr(x).asIrExpr :: Nil,
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
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowPart[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "LAST_VALUE",
                a.asExpr(x).asIrExpr :: Nil,
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    kt: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInWindow[kt.R],
    in: CanInWindow[ktn.R],
    c: CombineKindTuple[kt.R, ktn.R],
    to: ToOption[WindowPart[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NthValueFunc(
                aa.asExpr(x).asIrExpr,
                an.asExpr(n).asIrExpr,
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    kt: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInWindow[kt.R],
    in: CanInWindow[ktn.R],
    c: CombineKindTuple[kt.R, ktn.R],
    to: ToOption[WindowPart[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NthValueFunc(
                aa.asExpr(x).asIrExpr,
                an.asExpr(n).asIrExpr,
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    kt: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInWindow[kt.R],
    in: CanInWindow[ktn.R],
    c: CombineKindTuple[kt.R, ktn.R],
    to: ToOption[WindowPart[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NthValueFunc(
                aa.asExpr(x).asIrExpr,
                an.asExpr(n).asIrExpr,
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    kt: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInWindow[kt.R],
    in: CanInWindow[ktn.R],
    c: CombineKindTuple[kt.R, ktn.R],
    to: ToOption[WindowPart[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NthValueFunc(
                aa.asExpr(x).asIrExpr,
                an.asExpr(n).asIrExpr,
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
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowPart[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "LAG",
                a.asExpr(x).asIrExpr :: Nil,
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
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowPart[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "LAG",
                a.asExpr(x).asIrExpr :: Nil,
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    no: ValidateArgType[SqlNumber, ao.R, "numeric"],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    c: CombineKindTuple[kt.R, kto.R],
    to: ToOption[WindowPart[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "LAG",
                aa.asExpr(x).asIrExpr :: ao.asExpr(offset).asIrExpr :: Nil,
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    no: ValidateArgType[SqlNumber, ao.R, "numeric"],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    c: CombineKindTuple[kt.R, kto.R],
    to: ToOption[WindowPart[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "LAG",
                aa.asExpr(x).asIrExpr :: ao.asExpr(offset).asIrExpr :: Nil,
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    no: ValidateArgType[SqlNumber, ao.R, "numeric"],
    asd: ValidateArgType[AsSqlExpr, ad.R, "scalar"],
    r: Return[aa.R, ad.R],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ktd: KindToTuple[ad.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    id: CanInWindow[ktd.R],
    co: CombineKindTuple[kt.R, kto.R],
    c: CombineKindTuple[co.R, ktd.R],
    to: ToOption[WindowPart[r.R, c.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "LAG",
                aa.asExpr(x).asIrExpr ::
                    ao.asExpr(offset).asIrExpr ::
                    ad.asExpr(default).asIrExpr :: Nil
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    no: ValidateArgType[SqlNumber, ao.R, "numeric"],
    asd: ValidateArgType[AsSqlExpr, ad.R, "scalar"],
    r: Return[aa.R, ad.R],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ktd: KindToTuple[ad.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    id: CanInWindow[ktd.R],
    co: CombineKindTuple[kt.R, kto.R],
    c: CombineKindTuple[co.R, ktd.R],
    to: ToOption[WindowPart[r.R, c.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "LAG",
                aa.asExpr(x).asIrExpr ::
                    ao.asExpr(offset).asIrExpr ::
                    ad.asExpr(default).asIrExpr :: Nil
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
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowPart[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "LEAD",
                a.asExpr(x).asIrExpr :: Nil,
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
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInWindow[kt.R],
    to: ToOption[WindowPart[a.R, kt.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "LEAD",
                a.asExpr(x).asIrExpr :: Nil,
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    no: ValidateArgType[SqlNumber, ao.R, "numeric"],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    c: CombineKindTuple[kt.R, kto.R],
    to: ToOption[WindowPart[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "LEAD",
                aa.asExpr(x).asIrExpr :: ao.asExpr(offset).asIrExpr :: Nil,
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    no: ValidateArgType[SqlNumber, ao.R, "numeric"],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    c: CombineKindTuple[kt.R, kto.R],
    to: ToOption[WindowPart[aa.R, c.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "LEAD",
                aa.asExpr(x).asIrExpr :: ao.asExpr(offset).asIrExpr :: Nil,
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    no: ValidateArgType[SqlNumber, ao.R, "numeric"],
    asd: ValidateArgType[AsSqlExpr, ad.R, "scalar"],
    r: Return[aa.R, ad.R],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ktd: KindToTuple[ad.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    id: CanInWindow[ktd.R],
    co: CombineKindTuple[kt.R, kto.R],
    c: CombineKindTuple[co.R, ktd.R],
    to: ToOption[WindowPart[r.R, c.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "LEAD",
                aa.asExpr(x).asIrExpr ::
                    ao.asExpr(offset).asIrExpr ::
                    ad.asExpr(default).asIrExpr :: Nil
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    no: ValidateArgType[SqlNumber, ao.R, "numeric"],
    asd: ValidateArgType[AsSqlExpr, ad.R, "scalar"],
    r: Return[aa.R, ad.R],
    kt: KindToTuple[aa.K],
    kto: KindToTuple[ao.K],
    ktd: KindToTuple[ad.K],
    ia: CanInWindow[kt.R],
    io: CanInWindow[kto.R],
    id: CanInWindow[ktd.R],
    co: CombineKindTuple[kt.R, kto.R],
    c: CombineKindTuple[co.R, ktd.R],
    to: ToOption[WindowPart[r.R, c.R]]
): to.R =
    to.toOption(
        WindowPart(
            IrExpr.NullsTreatmentFunc(
                "LEAD",
                aa.asExpr(x).asIrExpr ::
                    ao.asExpr(offset).asIrExpr ::
                    ad.asExpr(default).asIrExpr :: Nil
                ,
                Some(SqlWindowNullsMode.Ignore)
            )
        )
    )