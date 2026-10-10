package sqala.static.dsl

import sqala.ir.IrExpr
import sqala.ast.quantifier.SqlQuantifier

// TODO 聚合函数换成一个AggExpr
/**
 * Counts all rows. Maps to `COUNT(*)`.
 *
 * {{{
 * from(Post).map(p => count())
 * }}}
 */
def count[CL <: Int]()(using QueryContext[CL]): AggExpr[Long, EmptyTuple] =
    AggExpr(
        IrExpr.CountAsteriskFunc(None)
    )

/**
 * Counts non-null values of an expression. Maps to `COUNT(expr)`.
 * Aggregate functions, window functions, and other expressions
 * not allowed in aggregate calls are rejected at compile time.
 *
 * {{{
 * from(Post).map(p => count(p.likeCount))
 * }}}
 */
def count[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R]
): AggExpr[Long, kt.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            None,
            "COUNT",
            a.asExpr(x).asIrExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )

/**
 * Counts distinct non-null values of an expression. Maps to
 * `COUNT(DISTINCT expr)`. Aggregate functions, window functions,
 * and other expressions not allowed in aggregate calls are rejected
 * at compile time.
 *
 * {{{
 * from(Post).map(p => countDistinct(p.likeCount))
 * }}}
 */
def countDistinct[T, CL <: Int](x: T)(using
    qc: QueryContext[CL],
    a: AsExpr[T, CL],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R]
): AggExpr[Long, kt.R] =
    AggExpr(
        IrExpr.GeneralFunc(
            Some(SqlQuantifier.Distinct),
            "COUNT",
            a.asExpr(x).asIrExpr :: Nil,
            Nil,
            Nil,
            None
        )
    )