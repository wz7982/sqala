package sqala.static.dsl

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlUnaryOperator}
import sqala.static.dsl.statement.dml.UpdatePair

import scala.annotation.targetName

case class Expr[T](private[sqala] val expr: SqlExpr):
    @targetName("eq")
    def ==[R](that: R)(using
        a: AsRightOperand[R],
        c: Compare[Unwrap[T, Option], Unwrap[a.R, Option]],
        qc: QueryContext
    ): Expr[Option[Boolean]] =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Equal, a.asExpr(that).asSqlExpr))

    @targetName("ne")
    def !=[R](that: R)(using
        a: AsRightOperand[R],
        c: Compare[Unwrap[T, Option], Unwrap[a.R, Option]],
        qc: QueryContext
    ): Expr[Option[Boolean]] =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.NotEqual, a.asExpr(that).asSqlExpr))

    infix def over(over: Over | Unit)(using QueryContext): Expr[T] =
        over match
            case o: Over =>
                Expr(
                    SqlExpr.Window(
                        asSqlExpr, 
                        o.partitionBy.map(_.asSqlExpr),
                        o.sortBy.map(_.asSqlOrderBy),
                        o.frame
                    )
                )
            case u: Unit =>
                Expr(
                    SqlExpr.Window(
                        asSqlExpr, 
                        Nil,
                        Nil,
                        None
                    )
                )
                
    @targetName("to")
    def :=[R: AsExpr as a](updateExpr: R)(using
        Compare[T, a.R]
    ): UpdatePair = this match
        case Expr(SqlExpr.Column(_, columnName)) =>
            UpdatePair(columnName, a.asExpr(updateExpr).asSqlExpr)
        case _ => throw MatchError(this)

    def asSqlExpr: SqlExpr =
        import SqlExpr.*
        
        expr match
            case Binary(left, SqlBinaryOperator.Equal, SqlExpr.Null) =>
                NullTest(left, false)
            case Binary(left, SqlBinaryOperator.NotEqual, SqlExpr.Null) =>
                NullTest(left, true)
            case Binary(left, SqlBinaryOperator.NotEqual, right) =>
                Binary(
                    Binary(left, SqlBinaryOperator.NotEqual, right),
                    SqlBinaryOperator.Or,
                    NullTest(left, false)
                )
            case Binary(left, SqlBinaryOperator.In, SqlExpr.Tuple(Nil)) =>
                BooleanLiteral(false)
            case Binary(left, SqlBinaryOperator.NotIn, SqlExpr.Tuple(Nil)) =>
                BooleanLiteral(true)
            case Unary(e, SqlUnaryOperator.Not) =>
                e match
                    case BooleanLiteral(boolean) =>
                        BooleanLiteral(!boolean)
                    case Binary(left, SqlBinaryOperator.Like, right) =>
                        Binary(left, SqlBinaryOperator.NotLike, right)
                    case Binary(left, SqlBinaryOperator.In, right) =>
                        Binary(left, SqlBinaryOperator.NotIn, right)
                    case Binary(left, SqlBinaryOperator.NotIn, right) =>
                        Binary(left, SqlBinaryOperator.In, right)
                    case NullTest(ne, not) =>
                         NullTest(ne, !not)
                    case Between(expr, s, e, n) =>
                        Between(expr, s, e, !n)
                    case _ => SqlExpr.Unary(e, SqlUnaryOperator.Not)
            case _ => expr