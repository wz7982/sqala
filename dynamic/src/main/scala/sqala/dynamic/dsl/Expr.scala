package sqala.dynamic.dsl

import sqala.ast.expr.*
import sqala.ast.order.{SqlNullsOrdering, SqlOrdering, SqlOrderingItem}
import sqala.ast.statement.SqlSelectItem

import scala.annotation.targetName

final case class Expr(private[sqala] val expr: SqlExpr):
    def ast: SqlExpr =
        asSqlExpr

    @targetName("eq")
    def ==(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Equal, expr.asSqlExpr))

    @targetName("ne")
    def !=(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.NotEqual, expr.asSqlExpr))

    @targetName("eqIgnoreNulls")
    def <=>(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.IsNotDistinctFrom, expr.asSqlExpr))

    def isNull: Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Is, SqlExpr.NullLiteral))

    @targetName("gt")
    def >(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.GreaterThan, expr.asSqlExpr))

    @targetName("ge")
    def >=(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.GreaterThanEqual, expr.asSqlExpr))

    @targetName("lt")
    def <(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.LessThan, expr.asSqlExpr))

    @targetName("le")
    def <=(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.LessThanEqual, expr.asSqlExpr))

    def in(list: List[Expr]): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.In, SqlExpr.Tuple(list.map(_.asSqlExpr))))

    def between(start: Expr, end: Expr): Expr =
        Expr(SqlExpr.Between(asSqlExpr, start.asSqlExpr, end.asSqlExpr, false))

    @targetName("plus")
    def +(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Plus, expr.asSqlExpr))

    @targetName("minus")
    def -(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Minus, expr.asSqlExpr))

    @targetName("times")
    def *(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Times, expr.asSqlExpr))

    @targetName("div")
    def /(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Div, expr.asSqlExpr))

    @targetName("positive")
    def unary_+ : Expr =
        Expr(SqlExpr.Unary(SqlUnaryOperator.Positive, asSqlExpr))

    @targetName("negative")
    def unary_- : Expr =
        Expr(SqlExpr.Unary(SqlUnaryOperator.Negative, asSqlExpr))

    @targetName("and")
    def &&(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.And, expr.asSqlExpr))

    @targetName("or")
    def ||(expr: Expr): Expr =
        Expr(SqlExpr.Binary(asSqlExpr, SqlBinaryOperator.Or, expr.asSqlExpr))

    @targetName("not")
    def unary_! : Expr =
        Expr(SqlExpr.Unary(SqlUnaryOperator.Not, asSqlExpr))

    def over(): Expr =
        Expr(SqlExpr.Window(asSqlExpr, SqlWindow(Nil, Nil, None)))

    def over(over: Over): Expr =
        Expr(SqlExpr.Window(asSqlExpr, SqlWindow(over.partitionBy, over.orderBy, over.frame)))

    def asc: Order =
        Order(SqlOrderingItem(asSqlExpr, Some(SqlOrdering.Asc), None))

    def ascNullsFirst: Order =
        Order(SqlOrderingItem(asSqlExpr, Some(SqlOrdering.Asc), Some(SqlNullsOrdering.First)))

    def ascNullsLast: Order =
        Order(SqlOrderingItem(asSqlExpr, Some(SqlOrdering.Asc), Some(SqlNullsOrdering.Last)))

    def desc: Order =
        Order(SqlOrderingItem(asSqlExpr, Some(SqlOrdering.Desc), None))

    def descNullsFirst: Order =
        Order(SqlOrderingItem(asSqlExpr, Some(SqlOrdering.Desc), Some(SqlNullsOrdering.First)))

    def descNullsLast: Order =
        Order(SqlOrderingItem(asSqlExpr, Some(SqlOrdering.Desc), Some(SqlNullsOrdering.Last)))

    def preceding: FrameBound =
        FrameBound(SqlWindowFrameBound.Preceding(asSqlExpr))

    def following: FrameBound =
        FrameBound(SqlWindowFrameBound.Following(asSqlExpr))

    def year: Expr =
        Expr(SqlExpr.ExtractFunc(SqlTimeUnit.Year, asSqlExpr))

    def month: Expr =
        Expr(SqlExpr.ExtractFunc(SqlTimeUnit.Month, asSqlExpr))

    def day: Expr =
        Expr(SqlExpr.ExtractFunc(SqlTimeUnit.Day, asSqlExpr))

    def hour: Expr =
        Expr(SqlExpr.ExtractFunc(SqlTimeUnit.Hour, asSqlExpr))

    def minute: Expr =
        Expr(SqlExpr.ExtractFunc(SqlTimeUnit.Minute, asSqlExpr))

    def second: Expr =
        Expr(SqlExpr.ExtractFunc(SqlTimeUnit.Second, asSqlExpr))

    def as(name: String): SelectItem =
        SelectItem(SqlSelectItem.Expr(asSqlExpr, Some(name)))

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