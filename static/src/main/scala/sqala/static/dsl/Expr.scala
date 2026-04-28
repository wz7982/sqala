package sqala.static.dsl

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlUnaryOperator}

import scala.annotation.targetName
import scala.compiletime.ops.boolean.||

case class Expr[T, K <: ExprKind](private val expr: SqlExpr):
    @targetName("eq")
    def ==[R](that: R)(using
        a: AsRightOperand[R],
        r: Relation[Unwrap[T, Option], Unwrap[a.R, Option], IsOption[T] || IsOption[a.R]],
        o: KindOperation[K, a.K],
        qc: QueryContext
    ): Expr[r.R, o.R] =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Equal, a.asExpr(that).asSqlExpr))

    @targetName("ne")
    def !=[R](that: R)(using
        a: AsRightOperand[R],
        r: Relation[Unwrap[T, Option], Unwrap[a.R, Option], IsOption[T] || IsOption[a.R]],
        o: KindOperation[K, a.K],
        qc: QueryContext
    ): Expr[r.R, o.R] =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.NotEqual, a.asExpr(that).asSqlExpr))

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