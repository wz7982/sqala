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
        ar: AsRightOperand[R, CL],
        r: Relation[T, ar.R],
        ck: CombineKind[K, ar.K]
    ): Expr[r.R, ck.R] =
        Expr(
            SqlExpr.Binary(
                asSqlExpr,
                SqlBinaryOperator.Equal,
                ar.asExpr(that).asSqlExpr
            )
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
        ar: AsRightOperand[R, CL],
        r: Relation[T, ar.R],
        ck: CombineKind[K, ar.K]
    ): Expr[r.R, ck.R] =
        Expr(
            SqlExpr.Binary(
                asSqlExpr,
                SqlBinaryOperator.NotEqual,
                ar.asExpr(that).asSqlExpr
            )
        )

    /**
     * Normalizes the expression by folding constant sub-expressions
     * (e.g. `NOT TRUE` → `FALSE`, `x IN ()` → `FALSE`, double negation elimination).
     */
    private[sqala] def asSqlExpr: SqlExpr =
        import SqlExpr.*

        expr match
            case Binary(left, SqlBinaryOperator.In, SqlExpr.Tuple(Nil)) =>
                BooleanLiteral(false)
            case Binary(left, SqlBinaryOperator.NotIn, SqlExpr.Tuple(Nil)) =>
                BooleanLiteral(true)
            case Unary(SqlUnaryOperator.Not, e) =>
                e match
                    case BooleanLiteral(boolean) =>
                        BooleanLiteral(!boolean)
                    case Like(expr, pattern, escape, not) =>
                        Like(expr, pattern, escape, !not)
                    case SimilarTo(expr, pattern, escape, not) =>
                        SimilarTo(expr, pattern, escape, !not)
                    case Binary(left, SqlBinaryOperator.In, right) =>
                        Binary(left, SqlBinaryOperator.NotIn, right)
                    case Binary(left, SqlBinaryOperator.NotIn, right) =>
                        Binary(left, SqlBinaryOperator.In, right)
                    case Binary(left, SqlBinaryOperator.IsDistinctFrom, right) =>
                        Binary(left, SqlBinaryOperator.IsNotDistinctFrom, right)
                    case Binary(left, SqlBinaryOperator.IsNotDistinctFrom, right) =>
                        Binary(left, SqlBinaryOperator.IsDistinctFrom, right)
                    case Binary(left, SqlBinaryOperator.Is, right) =>
                        Binary(left, SqlBinaryOperator.IsNot, right)
                    case Binary(left, SqlBinaryOperator.IsNot, right) =>
                        Binary(left, SqlBinaryOperator.Is, right)
                    case Between(expr, s, e, n) =>
                        Between(expr, s, e, !n)
                    case _ => SqlExpr.Unary(SqlUnaryOperator.Not, e)
            case _ => expr