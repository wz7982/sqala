package sqala.dynamic

import sqala.ast.expr.SqlBinaryOperator.*
import sqala.ast.expr.SqlExpr
import sqala.ast.expr.SqlUnaryOperator.*
import sqala.ast.order.{SqlOrderBy, SqlOrderByOption}
import sqala.ast.statement.SqlSelectItem

import scala.annotation.targetName

case class Expr(sqlExpr: SqlExpr):
    @targetName("eq")
    def ==(expr: Expr): Expr = expr match
        case Expr(SqlExpr.Null) => Expr(SqlExpr.Binary(sqlExpr, Is, SqlExpr.Null))
        case _ => Expr(SqlExpr.Binary(sqlExpr, Equal, expr.sqlExpr))

    @targetName("ne")
    def <>(expr: Expr): Expr = expr match
        case Expr(SqlExpr.Null) => Expr(SqlExpr.Binary(sqlExpr, IsNot, SqlExpr.Null))
        case _ => Expr(SqlExpr.Binary(sqlExpr, NotEqual, expr.sqlExpr))

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

    def in(list: List[Expr]): Expr =
        if list.isEmpty then Expr(SqlExpr.BooleanLiteral(false)) 
        else Expr(SqlExpr.In(sqlExpr, SqlExpr.Vector(list.map(_.sqlExpr)), false))

    def notIn(list: List[Expr]): Expr =
        if list.isEmpty then Expr(SqlExpr.BooleanLiteral(true)) 
        else Expr(SqlExpr.In(sqlExpr, SqlExpr.Vector(list.map(_.sqlExpr)), true))

    def between(start: Expr, end: Expr): Expr = Expr(SqlExpr.Between(sqlExpr, start.sqlExpr, end.sqlExpr, false))

    def notBetween(start: Expr, end: Expr): Expr = Expr(SqlExpr.Between(sqlExpr, start.sqlExpr, end.sqlExpr, true))

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

    @targetName("xor")
    def ^(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, Xor, expr.sqlExpr))

    @targetName("not")
    def unary_! : Expr = Expr(SqlExpr.Unary(sqlExpr, Not))

    def like(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, Like, expr.sqlExpr))

    def notLike(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, NotLike, expr.sqlExpr))

    def over: Expr = Expr(SqlExpr.Window(sqlExpr, Nil, Nil, None))

    def partitionBy(items: Expr*): Expr = sqlExpr match
        case SqlExpr.Window(expr, _, orderBy, frame) => Expr(SqlExpr.Window(expr, items.toList.map(_.sqlExpr), orderBy, frame))
        case _ => this

    def orderBy(items: OrderBy*): Expr = sqlExpr match
        case SqlExpr.Window(expr, partitionBy, _, frame) => 
            Expr(SqlExpr.Window(expr, partitionBy, items.toList.map(i => SqlOrderBy(i.expr.sqlExpr, Some(i.order))), frame))
        case _ => this

    def asc: OrderBy = OrderBy(this, SqlOrderByOption.Asc)

    def desc: OrderBy = OrderBy(this, SqlOrderByOption.Desc)

    infix def as(name: String): SelectItem = SelectItem(this, Some(name))

object Expr:
    given valueToExpr[T](using a: AsSqlExpr[T]): Conversion[T, Expr] =
        v => Expr(a.asSqlExpr(v))

    given queryToExpr: Conversion[Query, Expr] =
        q => Expr(SqlExpr.SubQuery(q.ast))

    given listToExprList[T](using a: AsSqlExpr[T]): Conversion[List[T], List[Expr]] =
        l => l.map(i => Expr(a.asSqlExpr(i)))

    given exprToSelectItem: Conversion[Expr, SelectItem] = SelectItem(_, None)

case class OrderBy(expr: Expr, order: SqlOrderByOption):
    def asSqlOrderBy: SqlOrderBy = SqlOrderBy(expr.sqlExpr, Some(order))

case class SelectItem(expr: Expr, alias: Option[String]):
    def toSqlSelectItem: SqlSelectItem = SqlSelectItem(expr.sqlExpr, alias)