package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.window.SqlWindow
import sqala.ir.IrExpr

/**
 * An intermediate type produced by window functions (`rank`, `lag`,
 * etc.). A window function cannot be used as an expression until
 * `over` is called, matching the SQL requirement that window
 * functions require an `OVER` clause.
 */
final case class WindowPart[T, KS <: Tuple](private[sqala] val expr: IrExpr):
    // TODO 改成类型类上的扩展方法
    /**
     * Applies a window specification to the function, promoting it
     * to an `Expr`. Maps to SQL `OVER ()`.
     *
     * {{{
     * from(Post).map(p => rowNumber().over())
     * }}}
     */
    def over(): Expr[T, Window[KS]] =
        Expr(
            IrExpr.Window(
                asIrExpr,
                Nil,
                Nil,
                None
            )
        )

    /**
     * Applies a window specification with `partitionBy` and `orderBy`
     * clause. Maps to SQL `OVER (PARTITION BY ... ORDER BY ...)`.
     *
     * {{{
     * from(Post).map(p => rowNumber().over(
     *     partitionBy(p.channelId).sortBy(p.viewCount.desc)
     * ))
     * }}}
     */
    // def over[OKS <: Tuple](over: OverContext ?=> Over[OKS])(using
    //     i: CanInWindow[OKS],
    //     c: CombineKindTuple[KS, OKS]
    // ): Expr[T, Window[c.R]] =
    //     given OverContext = OverContext()
    //     val o = over
    //     Expr(
    //         SqlExpr.Window(
    //             asSqlExpr,
    //             SqlWindow.Inlined(
    //                 None,
    //                 o.partitionBy.map(_.asSqlExpr),
    //                 o.sortBy.map(_.asSqlOrderingItem),
    //                 o.frame
    //             )
    //         )
    //     )

    /**
     * Normalizes the expression via `Expr.asIrExpr`.
     */
    private[sqala] def asIrExpr: IrExpr =
        Expr(expr).asIrExpr