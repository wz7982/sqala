package sqala.static.dsl

import sqala.ir.IrExpr

/**
 * A typed expression wrapping an IR node. All DSL operations on
 * columns, values, and subqueries produce or consume `Expr` values.
 */
sealed class Expr[T, K <: ExprKind](private[sqala] val expr: IrExpr):
    /**
     * Type-safe equality comparison. Maps to SQL `=`. Equivalent to `===`,
     * offered as the more intuitive operator. Typically used in `filter`,
     * `on`, and `having` clauses. Subqueries must return at most one row,
     * enforced at compile time.
     *
     * The expression must be on the left: `1 == u.id` resolves to universal
     * equality and is rejected by the compiler, so it can never silently
     * produce a constant condition.
     *
     * {{{
     * // Compare with a value
     * from(User).filter(u => u.id == 1)
     *
     * // Compare with another expression
     * from(Post.join(Comment).on((p, c) => p.id == c.postId))
     *
     * // Compare with a subquery
     * from(Post).filter(p => p.viewCount == from(Post).map(p => max(p.viewCount)))
     * }}}
     */
    def ==[B, CL <: Int](that: B)(using
        qc: QueryContext[CL],
        ab: AsComparison[B, CL],
        r: Relation[T, ab.R],
        c: CombineKind[K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(ab.asIrExpr(expr, ComparisonOperator.Equal, that))
 
    /**
     * Type-safe inequality comparison. Maps to SQL `<>`. Equivalent to `<>`,
     * offered as the more intuitive operator. Typically used in `filter`,
     * `on`, and `having` clauses. Subqueries must return at most one row,
     * enforced at compile time.
     *
     * The expression must be on the left: `1 != u.id` resolves to universal
     * equality and is rejected by the compiler, so it can never silently
     * produce a constant condition.
     *
     * {{{
     * // Compare with a value
     * from(User).filter(u => u.id != 1)
     *
     * // Compare with another expression
     * from(Post.join(Comment).on((p, c) => p.id != c.postId))
     *
     * // Compare with a subquery
     * from(Post).filter(p => p.viewCount != from(Post).map(p => max(p.viewCount)))
     * }}}
     */
    def !=[B, CL <: Int](that: B)(using
        qc: QueryContext[CL],
        ab: AsComparison[B, CL],
        r: Relation[T, ab.R],
        c: CombineKind[K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(ab.asIrExpr(expr, ComparisonOperator.NotEqual, that))

object Expr:
    private[sqala] def unapply[T, K <: ExprKind](expr: Expr[T, K]): Option[IrExpr] =
        Some(expr.expr)

/**
 * A typed expression representing a column reference.
 */
final class ColumnExpr[T, L <: Int](
    override val expr: IrExpr.Column
) extends Expr[T, Column[L]](expr)

/**
 * A typed expression representing an aggregate function.
 */
final class AggExpr[T, KS <: Tuple](
    override val expr: IrExpr.CountAsteriskFunc | IrExpr.ListAggFunc | IrExpr.GeneralFunc | IrExpr.JsonObjectAggFunc | IrExpr.JsonArrayAggFunc
) extends Expr[T, Agg[KS]](expr)