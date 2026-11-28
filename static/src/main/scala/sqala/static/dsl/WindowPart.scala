package sqala.static.dsl

import sqala.ir.IrExpr

/**
 * An intermediate type produced by window functions (`rank`, `lag`,
 * etc.). A window function cannot be used as an expression until
 * `over` is called, matching the SQL requirement that window
 * functions require an `OVER` clause.
 */
final class WindowPart[T, KS <: Tuple](private[sqala] val expr: IrExpr):
    /**
     * Intercepts `==` on a window function that has no `over` clause.
     * Without this overload the comparison would fall back to universal
     * equality and silently yield a Boolean. Summoning `AsExpr` for a
     * `WindowPart` fails at compile time with "Window function requires
     * an over clause.", so call `over` first and compare the resulting
     * expression instead.
     */
    def ==[B, CL <: Int](that: B)(using
        qc: QueryContext[CL],
        aa: AsExpr[WindowPart[T, KS], CL],
        ab: AsComparison[B, CL],
        r: Relation[T, ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[aa.R, aa.K] =
        Expr(ab.asIrExpr(aa.asExpr(this).expr, ComparisonOperator.Equal, that))
 
    /**
     * Intercepts `!=` on a window function that has no `over` clause.
     * Without this overload the comparison would fall back to universal
     * equality and silently yield a Boolean. Summoning `AsExpr` for a
     * `WindowPart` fails at compile time with "Window function requires
     * an over clause.", so call `over` first and compare the resulting
     * expression instead.
     */
    def !=[B, CL <: Int](that: B)(using
        qc: QueryContext[CL],
        aa: AsExpr[WindowPart[T, KS], CL],
        ab: AsComparison[B, CL],
        r: Relation[T, ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[aa.R, aa.K] =
        Expr(ab.asIrExpr(aa.asExpr(this).expr, ComparisonOperator.NotEqual, that))