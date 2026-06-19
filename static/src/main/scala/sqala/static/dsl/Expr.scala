package sqala.static.dsl

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlUnaryOperator}

import scala.annotation.targetName

/**
 * A typed expression wrapping an AST node. All DSL operations on
 * columns, values, and subqueries produce or consume `Expr` values.
 */
final case class Expr[T, K <: ExprKind](private[sqala] val expr: SqlExpr):
    /**
     * Type-safe equality comparison. Typically used in `filter`, `on`, and `having`
     * clause. Maps to SQL `=`. The right-hand side can be a value, another
     * expression, or a subquery.
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
    @targetName("equal")
    def ==[R, CL <: Int](that: R)(using
        qc: QueryContext[CL],
        ar: AsComparison[R, CL],
        r: Relation[T, ar.R],
        ck: CombineKind[K, ar.K]
    ): Expr[r.R, ck.R] =
        Expr(
            ar.asSqlExpr(asSqlExpr, ComparisonOperator.Equal, that)
        )

    /**
     * Type-safe inequality comparison. Typically used in `filter`, `on`, and `having`
     * clause. Maps to SQL `<>`. The right-hand side can be a value, another
     * expression, or a subquery.
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
    @targetName("notEqual")
    def !=[R, CL <: Int](that: R)(using
        qc: QueryContext[CL],
        ar: AsComparison[R, CL],
        r: Relation[T, ar.R],
        ck: CombineKind[K, ar.K]
    ): Expr[r.R, ck.R] =
        Expr(
            ar.asSqlExpr(asSqlExpr, ComparisonOperator.NotEqual, that)
        )

    /**
     * Normalizes the expression by folding constant sub-expressions
     * (e.g. `NOT TRUE` → `FALSE`, `x IN ()` → `FALSE`, double negation elimination).
     */
    private[sqala] def asSqlExpr: SqlExpr =
        import SqlExpr.*

        expr match
            case Unary(SqlUnaryOperator.Not, e) =>
                e match
                    case BooleanLiteral(boolean) =>
                        BooleanLiteral(!boolean)
                    case Like(expr, pattern, escape, not) =>
                        Like(expr, pattern, escape, !not)
                    case SimilarTo(expr, pattern, escape, not) =>
                        SimilarTo(expr, pattern, escape, !not)
                    case In(left, right, not) =>
                        In(left, right, !not)
                    case Binary(left, SqlBinaryOperator.IsDistinctFrom(not), right) =>
                        Binary(left, SqlBinaryOperator.IsDistinctFrom(!not), right)
                    case Binary(left, SqlBinaryOperator.Is(not), right) =>
                        Binary(left, SqlBinaryOperator.Is(!not), right)
                    case Between(expr, s, e, n) =>
                        Between(expr, s, e, !n)
                    case _ => SqlExpr.Unary(SqlUnaryOperator.Not, e)
            case _ => expr