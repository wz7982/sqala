package sqala.static.ext.dsl

import sqala.ir.IrTable
import sqala.static.dsl.QueryContext
import sqala.static.dsl.AsExpr
import sqala.static.dsl.CombineKindTuple
import sqala.static.dsl.ToOption
import sqala.static.dsl.AggExpr
import sqala.static.dsl.Expr
import sqala.ir.IrExpr
import sqala.static.dsl.ValidateArgType
import sqala.metadata.AsSqlExpr
import sqala.static.dsl.KindToTuple
import sqala.static.dsl.CanInAgg
import sqala.metadata.SqlNumber

/**
 * Returns the first value of an expression in `matchRecognize`.
 * Maps to `FIRST(expr)`.
 *
 * {{{
 * first(expr)
 * }}}
 */
def first[A, T <: IrTable, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext[T],
    a: AsExpr[A, CL],
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
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
def first[A, N, T <: IrTable, CL <: Int](x: A, n: N)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext[T],
    aa: AsExpr[A, CL],
    an: AsExpr[N, CL],
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInAgg[kta.R],
    in: CanInAgg[ktn.R],
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
def last[A, T <: IrTable, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext[T],
    a: AsExpr[A, CL],
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
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
def last[A, N, T <: IrTable, CL <: Int](x: A, n: N)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext[T],
    aa: AsExpr[A, CL],
    an: AsExpr[N, CL],
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInAgg[kta.R],
    in: CanInAgg[ktn.R],
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
def prev[A, T <: IrTable, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext[T],
    a: AsExpr[A, CL],
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
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
def prev[A, N, T <: IrTable, CL <: Int](x: A, n: N)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext[T],
    aa: AsExpr[A, CL],
    an: AsExpr[N, CL],
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInAgg[kta.R],
    in: CanInAgg[ktn.R],
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
def next[A, T <: IrTable, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext[T],
    a: AsExpr[A, CL],
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R],
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
def next[A, N, T <: IrTable, CL <: Int](x: A, n: N)(using
    qc: QueryContext[CL],
    mc: MatchRecognizeContext[T],
    aa: AsExpr[A, CL],
    an: AsExpr[N, CL],
    asa: ValidateArgType[AsSqlExpr, aa.R, "scalar"],
    nn: ValidateArgType[SqlNumber, an.R, "numeric"],
    kta: KindToTuple[aa.K],
    ktn: KindToTuple[an.K],
    ia: CanInAgg[kta.R],
    in: CanInAgg[ktn.R],
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