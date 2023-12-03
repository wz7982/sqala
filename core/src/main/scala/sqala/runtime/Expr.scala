package sqala.runtime

import sqala.ast.expr.*
import sqala.ast.order.*
import sqala.ast.statement.*
import sqala.runtime.statement.query.Query

import scala.annotation.targetName

case class Expr(sqlExpr: SqlExpr):
    @targetName("eq")
    def ===(expr: Expr): Expr = expr match
        case Expr(SqlExpr.Null) => Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.Is, SqlExpr.Null))
        case _ => Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.Equal, expr.sqlExpr))

    @targetName("ne")
    def <>(expr: Expr): Expr = expr match
        case Expr(SqlExpr.Null) => Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.IsNot, SqlExpr.Null))
        case _ => Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.NotEqual, expr.sqlExpr))

    @targetName("gt")
    def >(expr: Expr): Expr = expr match
        case Expr(SqlExpr.Null) => Expr(SqlExpr.BooleanLiteral(false))
        case _ => Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.GreaterThan, expr.sqlExpr))

    @targetName("ge")
    def >=(expr: Expr): Expr = expr match
        case Expr(SqlExpr.Null) => Expr(SqlExpr.BooleanLiteral(false))
        case _ => Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.GreaterThanEqual, expr.sqlExpr))

    @targetName("lt")
    def <(expr: Expr): Expr = expr match
        case Expr(SqlExpr.Null) => Expr(SqlExpr.BooleanLiteral(false))
        case _ => Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.LessThan, expr.sqlExpr))

    @targetName("le")
    def <=(expr: Expr): Expr = expr match
        case Expr(SqlExpr.Null) => Expr(SqlExpr.BooleanLiteral(false))
        case _ => Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.LessThanEqual, expr.sqlExpr))

    def in(list: List[Any]): Expr =
        if list.isEmpty then Expr(SqlExpr.BooleanLiteral(false)) 
        else Expr(SqlExpr.In(sqlExpr, SqlExpr.Vector(list.map(value(_).sqlExpr)), false))

    def in(query: Query): Expr = Expr(SqlExpr.In(this.sqlExpr, SqlExpr.SubQuery(query.ast), false))

    def notIn(list: List[Expr]): Expr =
        if list.isEmpty then Expr(SqlExpr.BooleanLiteral(true)) 
        else Expr(SqlExpr.In(sqlExpr, SqlExpr.Vector(list.map(value(_).sqlExpr)), true))

    def notIn(query: Query): Expr = Expr(SqlExpr.In(this.sqlExpr, SqlExpr.SubQuery(query.ast), true))

    def between(start: Expr, end: Expr): Expr = Expr(SqlExpr.Between(sqlExpr, start.sqlExpr, end.sqlExpr, false))

    def notBetween(start: Expr, end: Expr): Expr = Expr(SqlExpr.Between(sqlExpr, start.sqlExpr, end.sqlExpr, true))

    @targetName("plus")
    def +(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.Plus, expr.sqlExpr))

    @targetName("minus")
    def -(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.Minus, expr.sqlExpr))

    @targetName("times")
    def *(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.Times, expr.sqlExpr))

    @targetName("div")
    def /(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.Div, expr.sqlExpr))

    @targetName("mod")
    def %(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.Mod, expr.sqlExpr))

    @targetName("and")
    def &&(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.And, expr.sqlExpr))

    @targetName("or")
    def ||(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.Or, expr.sqlExpr))

    @targetName("xor")
    def ^(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.Xor, expr.sqlExpr))

    def like(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.Like, expr.sqlExpr))

    def notLike(expr: Expr): Expr = Expr(SqlExpr.Binary(sqlExpr, SqlBinaryOperator.NotLike, expr.sqlExpr))

    def over: Expr = Expr(SqlExpr.Window(sqlExpr, Nil, Nil, None))

    def partitionBy(items: Expr*): Expr = sqlExpr match
        case SqlExpr.Window(expr, _, orderBy, frame) => Expr(SqlExpr.Window(expr, items.toList.map(_.sqlExpr), orderBy, frame))
        case _ => this

    def orderBy(items: OrderBy*): Expr = sqlExpr match
        case SqlExpr.Window(expr, partitionBy, _, frame) => 
            Expr(SqlExpr.Window(expr, partitionBy, items.toList.map(i => SqlOrderBy(i.expr.sqlExpr, Some(i.order))), frame))
        case _ => this

    infix def `then`(expr: Expr): CaseBranch = CaseBranch(this, expr)

    infix def `else`(expr: Expr): Expr = sqlExpr match
        case SqlExpr.Case(branches, _) => Expr(SqlExpr.Case(branches, expr.sqlExpr))
        case _ => this

    def asc: OrderBy = OrderBy(this, SqlOrderByOption.Asc)

    def desc: OrderBy = OrderBy(this, SqlOrderByOption.Desc)

    infix def as(name: String): SelectItem = SelectItem(this, name)

object Expr:
    given anyToExpr: Conversion[Any, Expr] = value(_)

    given queryToExpr: Conversion[Query, Expr] = q => Expr(SqlExpr.SubQuery(q.ast))

case class OrderBy(expr: Expr, order: SqlOrderByOption):
    def toSqlOrderBy: SqlOrderBy = SqlOrderBy(expr.sqlExpr, Some(order))

case class CaseBranch(expr: Expr, thenExpr: Expr):
    def toSqlCase: SqlCase = SqlCase(expr.sqlExpr, thenExpr.sqlExpr)

case class SelectItem(expr: Expr, alias: String):
    def toSqlSelectItem: SqlSelectItem = SqlSelectItem(expr.sqlExpr, Some(alias))