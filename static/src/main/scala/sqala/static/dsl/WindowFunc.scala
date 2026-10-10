package sqala.static.dsl

import sqala.ir.IrExpr

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