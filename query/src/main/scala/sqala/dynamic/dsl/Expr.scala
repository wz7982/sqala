package sqala.dynamic.dsl

import sqala.ast.expr.SqlBinaryOperator.*
import sqala.ast.expr.SqlExpr
import sqala.ast.expr.SqlUnaryOperator.*
import sqala.ast.order.{SqlOrderItem, SqlOrderNullsOption, SqlOrderOption}
import sqala.ast.statement.SqlSelectItem
import sqala.common.AsSqlExpr
import sqala.parser.SqlParser

import scala.annotation.targetName
import sqala.ast.expr.SqlBinaryOperator

case class Expr(sqlExpr: SqlExpr):
    @targetName("eq")
    def ==(expr: Expr): Expr = expr match
        case Expr(SqlExpr.Null) => Expr(SqlExpr.NullTest(sqlExpr, false))
        case _ => Expr(SqlExpr.Binary(sqlExpr, Equal, expr.sqlExpr))

    @targetName("eq")
    def ==[T: AsSqlExpr as a](value: T): Expr =
        ==(Expr(a.asSqlExpr(value)))

    @targetName("ne")
    def !=(expr: Expr): Expr = expr match
        case Expr(SqlExpr.Null) => Expr(SqlExpr.NullTest(sqlExpr, true))
        case _ => Expr(SqlExpr.Binary(sqlExpr, NotEqual, expr.sqlExpr))

    @targetName("ne")
    def !=[T: AsSqlExpr as a](value: T): Expr =
        !=(Expr(a.asSqlExpr(value)))

    @targetName("gt")
    def >(expr: Expr): Expr = expr match
        case Expr(SqlExpr.Null) => Expr(SqlExpr.BooleanLiteral(false))
        case _ => Expr(SqlExpr.Binary(sqlExpr, GreaterThan, expr.sqlExpr))

    @targetName("ge")
    def >=(expr: Expr): Expr = expr match
        case Expr(SqlExpr.Null) => Expr(SqlExpr.BooleanLiteral(false))
        case _ => Expr(SqlExpr.Binary(sqlExpr, GreaterThanEqual, expr.sqlExpr))

    @targetName("lt")
    def <(expr: Expr): Expr = expr match
        case Expr(SqlExpr.Null) => Expr(SqlExpr.BooleanLiteral(false))
        case _ => Expr(SqlExpr.Binary(sqlExpr, LessThan, expr.sqlExpr))

    @targetName("le")
    def <=(expr: Expr): Expr = expr match
        case Expr(SqlExpr.Null) => Expr(SqlExpr.BooleanLiteral(false))
        case _ => Expr(SqlExpr.Binary(sqlExpr, LessThanEqual, expr.sqlExpr))

    def in(seq: Seq[Expr]): Expr =
        if seq.isEmpty then Expr(SqlExpr.BooleanLiteral(false))
        else Expr(SqlExpr.Binary(sqlExpr, In, SqlExpr.Tuple(seq.toList.map(_.sqlExpr))))

    def between(start: Expr, end: Expr): Expr = Expr(SqlExpr.Between(sqlExpr, start.sqlExpr, end.sqlExpr, false))

    @targetName("plus")
    def +(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, Plus, expr.sqlExpr))

    @targetName("minus")
    def -(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, Minus, expr.sqlExpr))

    @targetName("times")
    def *(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, Times, expr.sqlExpr))

    @targetName("div")
    def /(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, Div, expr.sqlExpr))

    @targetName("mod")
    def %(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, Mod, expr.sqlExpr))

    @targetName("positive")
    def unary_+ : Expr = Expr(SqlExpr.Unary(sqlExpr, Positive))

    @targetName("negative")
    def unary_- : Expr = Expr(SqlExpr.Unary(sqlExpr, Negative))

    @targetName("and")
    def &&(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, And, expr.sqlExpr))

    @targetName("or")
    def ||(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, Or, expr.sqlExpr))

    @targetName("not")
    def unary_! : Expr =
        val expr = sqlExpr match
            case SqlExpr.BooleanLiteral(b) =>
                SqlExpr.BooleanLiteral(!b)
            case SqlExpr.Binary(l, SqlBinaryOperator.Like, r) =>
                SqlExpr.Binary(l, SqlBinaryOperator.NotLike, r)
            case SqlExpr.Binary(l, SqlBinaryOperator.NotLike, r) =>
                SqlExpr.Binary(l, SqlBinaryOperator.Like, r)
            case SqlExpr.Binary(l, SqlBinaryOperator.In, r) =>
                SqlExpr.Binary(l, SqlBinaryOperator.NotIn, r)
            case SqlExpr.Binary(l, SqlBinaryOperator.NotIn, r) =>
                SqlExpr.Binary(l, SqlBinaryOperator.In, r)
            case SqlExpr.Between(expr, start, end, not) =>
                SqlExpr.Between(expr, start, end, !not)
            case _ => SqlExpr.Unary(sqlExpr, Not)
        Expr(expr)

    def like(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, Like, expr.sqlExpr))

    def over: Expr = Expr(SqlExpr.Window(sqlExpr, Nil, Nil, None))

    def partitionBy(items: Expr*): Expr = sqlExpr match
        case SqlExpr.Window(expr, _, orderBy, frame) => Expr(SqlExpr.Window(expr, items.toList.map(_.sqlExpr), orderBy, frame))
        case _ => this

    def orderBy(items: OrderBy*): Expr = sqlExpr match
        case SqlExpr.Window(expr, partitionBy, _, frame) =>
            Expr(SqlExpr.Window(expr, partitionBy, items.toList.map(i => i.asSqlOrderBy), frame))
        case _ => this

    def asc: OrderBy = OrderBy(this, SqlOrderOption.Asc, None)

    def ascNullsFirst: OrderBy = OrderBy(this, SqlOrderOption.Asc, Some(SqlOrderNullsOption.First))

    def ascNullsLast: OrderBy = OrderBy(this, SqlOrderOption.Asc, Some(SqlOrderNullsOption.Last))

    def desc: OrderBy = OrderBy(this, SqlOrderOption.Desc, None)

    def descNullsFirst: OrderBy = OrderBy(this, SqlOrderOption.Desc, Some(SqlOrderNullsOption.First))

    def descNullsLast: OrderBy = OrderBy(this, SqlOrderOption.Desc, Some(SqlOrderNullsOption.Last))

    infix def as(name: String): SelectItem =
        SqlParser.parseIdent(name)
        SelectItem(this, Some(name))

object Expr:
    given valueToExpr[T](using a: AsSqlExpr[T]): Conversion[T, Expr] =
        v => Expr(a.asSqlExpr(v))

    given queryToExpr: Conversion[Query, Expr] =
        q => Expr(SqlExpr.SubQuery(q.ast))

    given listToExprList[T](using a: AsSqlExpr[T]): Conversion[List[T], List[Expr]] =
        l => l.map(i => Expr(a.asSqlExpr(i)))

    given exprToSelectItem: Conversion[Expr, SelectItem] = SelectItem(_, None)

case class OrderBy(expr: Expr, order: SqlOrderOption, nullsOrder: Option[SqlOrderNullsOption]):
    def asSqlOrderBy: SqlOrderItem = SqlOrderItem(expr.sqlExpr, Some(order), nullsOrder)

case class SelectItem(expr: Expr, alias: Option[String]):
    def toSqlSelectItem: SqlSelectItem = SqlSelectItem.Item(expr.sqlExpr, alias)