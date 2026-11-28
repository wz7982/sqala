package sqala.static.ext.dsl

import sqala.ir.{IrExpr, IrTable}
import sqala.metadata.{AsSqlExpr, SqlNumber}
import sqala.static.dsl.*

/**
 * Returns the matched pattern name in `matchRecognize`. Maps to
 * `CLASSIFIER()`.
 */
def classifier[T <: IrTable, CL <: Int]()(using
    QueryContext[CL],
    MatchRecognizeContext
): AggExpr[String, EmptyTuple] =
    AggExpr(
        IrExpr.GeneralFunc(
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
def matchNumber[T <: IrTable, CL <: Int]()(using
    QueryContext[CL],
    MatchRecognizeContext
): AggExpr[Int, EmptyTuple] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "MATCH_NUMBER",
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
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R, CL],
    to: ToOption[AggExpr[a.R, kt.R]]
): to.R =
    to.toOption(
        AggExpr(
            IrExpr.GeneralFunc(
                None,
                "FIRST",
                a.asExpr(x).expr :: Nil,
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInAgg[kta.R, CL],
    in: CanInAgg[ktn.R, CL],
    c: CombineKindTuple[kta.R, ktn.R],
    to: ToOption[AggExpr[aa.R, c.R]]
): to.R =
    to.toOption(
        AggExpr(
            IrExpr.GeneralFunc(
                None,
                "FIRST",
                aa.asExpr(x).expr :: an.asExpr(n).expr :: Nil,
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
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R, CL],
    to: ToOption[AggExpr[a.R, kt.R]]
): to.R =
    to.toOption(
        AggExpr(
            IrExpr.GeneralFunc(
                None,
                "LAST",
                a.asExpr(x).expr :: Nil,
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInAgg[kta.R, CL],
    in: CanInAgg[ktn.R, CL],
    c: CombineKindTuple[kta.R, ktn.R],
    to: ToOption[AggExpr[aa.R, c.R]]
): to.R =
    to.toOption(
        AggExpr(
            IrExpr.GeneralFunc(
                None,
                "LAST",
                aa.asExpr(x).expr :: an.asExpr(n).expr :: Nil,
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
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R, CL],
    to: ToOption[AggExpr[a.R, kt.R]]
): to.R =
    to.toOption(
        AggExpr(
            IrExpr.GeneralFunc(
                None,
                "PREV",
                a.asExpr(x).expr :: Nil,
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInAgg[kta.R, CL],
    in: CanInAgg[ktn.R, CL],
    c: CombineKindTuple[kta.R, ktn.R],
    to: ToOption[AggExpr[aa.R, c.R]]
): to.R =
    to.toOption(
        AggExpr(
            IrExpr.GeneralFunc(
                None,
                "PREV",
                aa.asExpr(x).expr :: an.asExpr(n).expr :: Nil,
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
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R, CL],
    to: ToOption[AggExpr[a.R, kt.R]]
): to.R =
    to.toOption(
        AggExpr(
            IrExpr.GeneralFunc(
                None,
                "NEXT",
                a.asExpr(x).expr :: Nil,
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
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInAgg[kta.R, CL],
    in: CanInAgg[ktn.R, CL],
    c: CombineKindTuple[kta.R, ktn.R],
    to: ToOption[AggExpr[aa.R, c.R]]
): to.R =
    to.toOption(
        AggExpr(
            IrExpr.GeneralFunc(
                None,
                "NEXT",
                aa.asExpr(x).expr :: an.asExpr(n).expr :: Nil,
                Nil,
                Nil,
                None
            )
        )
    )