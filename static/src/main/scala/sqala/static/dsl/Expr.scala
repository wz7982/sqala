package sqala.static.dsl

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlUnaryOperator, SqlWindow}
import sqala.static.dsl.statement.dml.{UpdatePair, UpdateSetContext}

import scala.annotation.targetName
import scala.compiletime.ops.boolean.||

class OverContext

case class Expr[T](private val expr: SqlExpr):
    @targetName("eq")
    def ==[R](that: R)(using
        a: AsRightOperand[R],
        r: Relation[Unwrap[T, Option], Unwrap[a.R, Option], IsOption[T] || IsOption[a.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Equal, a.asExpr(that).asSqlExpr))

    @targetName("ne")
    def !=[R](that: R)(using
        a: AsRightOperand[R],
        r: Relation[Unwrap[T, Option], Unwrap[a.R, Option], IsOption[T] || IsOption[a.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.NotEqual, a.asExpr(that).asSqlExpr))

    def over(): Expr[T] =
        Expr(
            SqlExpr.Window(
                asSqlExpr,
                SqlWindow(
                    Nil,
                    Nil,
                    None
                )
            )
        )

    def over(over: OverContext ?=> Over)(using QueryContext): Expr[T] =
        given OverContext = new OverContext
        val o = over
        Expr(
            SqlExpr.Window(
                asSqlExpr,
                SqlWindow(
                    o.partitionBy.map(_.asSqlExpr),
                    o.sortBy.map(_.asSqlOrderBy),
                    o.frame
                )
            )
        )

    @targetName("to")
    def :=[R: AsExpr as a](updateExpr: R)(using
        Compare[T, a.R],
        UpdateSetContext
    ): UpdatePair = this match
        case Expr(SqlExpr.Column(_, columnName)) =>
            UpdatePair(columnName, a.asExpr(updateExpr).asSqlExpr)
        case _ => throw MatchError("The left-hand side of a set operation must be a simple field.")

    def asSqlExpr: SqlExpr =
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
                    case Binary(left, SqlBinaryOperator.Like, right) =>
                        Binary(left, SqlBinaryOperator.NotLike, right)
                    case Binary(left, SqlBinaryOperator.In, right) =>
                        Binary(left, SqlBinaryOperator.NotIn, right)
                    case Binary(left, SqlBinaryOperator.NotIn, right) =>
                        Binary(left, SqlBinaryOperator.In, right)
                    case Binary(left, SqlBinaryOperator.IsDistinctFrom, right) =>
                        Binary(left, SqlBinaryOperator.IsNotDistinctFrom, right)
                    case Binary(left, SqlBinaryOperator.IsNotDistinctFrom, right) =>
                        Binary(left, SqlBinaryOperator.IsDistinctFrom, right)
                    case NullTest(ne, not) =>
                         NullTest(ne, !not)
                    case Between(expr, s, e, n) =>
                        Between(expr, s, e, !n)
                    case _ => SqlExpr.Unary(SqlUnaryOperator.Not, e)
            case _ => expr