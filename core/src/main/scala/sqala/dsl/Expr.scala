package sqala.dsl

import sqala.ast.expr.*
import sqala.ast.expr.SqlBinaryOperator.*
import sqala.ast.expr.SqlUnaryOperator.{Negative, Positive}
import sqala.ast.order.SqlOrderByOption.{Asc, Desc}
import sqala.ast.order.{SqlOrderBy, SqlOrderByOption}
import sqala.dsl.statement.query.Query

import scala.annotation.targetName

sealed trait Expr[T] derives CanEqual:
    def asSqlExpr: SqlExpr

    @targetName("eq")
    def ==(value: T)(using AsSqlExpr[T]): Expr[Boolean] = Binary(this, Equal, Literal(value))

    @targetName("eq")
    def ==[R <: Operation[T]](that: Expr[R]): Expr[Boolean] = Binary(this, Equal, that)

    @targetName("eq")
    def ==[R <: Operation[T]](query: Query[Expr[R]]): Expr[Boolean] = Binary(this, Equal, SubQuery(query))

    @targetName("ne")
    def !=(value: T)(using AsSqlExpr[T]): Expr[Boolean] = Binary(this, NotEqual, Literal(value))

    @targetName("ne")
    def !=[R <: Operation[T]](that: Expr[R]): Expr[Boolean] = Binary(this, NotEqual, that)

    @targetName("ne")
    def !=[R <: Operation[T], E <: Expr[R]](query: Query[E]): Expr[Boolean] = Binary(this, NotEqual, SubQuery(query))

    @targetName("gt")
    def >(value: Unwrap[T, Option])(using AsSqlExpr[Unwrap[T, Option]]): Expr[Boolean] = Binary(this, GreaterThan, Literal(value))

    @targetName("gt")
    def >[R <: Operation[T]](that: Expr[R]): Expr[Boolean] = Binary(this, GreaterThan, that)

    @targetName("gt")
    def >[R <: Operation[T], E <: Expr[R]](query: Query[E]): Expr[Boolean] = Binary(this, GreaterThan, SubQuery(query))

    @targetName("ge")
    def >=(value: Unwrap[T, Option])(using AsSqlExpr[Unwrap[T, Option]]): Expr[Boolean] = Binary(this, GreaterThanEqual, Literal(value))

    @targetName("ge")
    def >=[R <: Operation[T]](that: Expr[R]): Expr[Boolean] = Binary(this, GreaterThanEqual, that)

    @targetName("ge")
    def >=[R <: Operation[T], E <: Expr[R]](query: Query[E]): Expr[Boolean] = Binary(this, GreaterThanEqual, SubQuery(query))

    @targetName("lt")
    def <(value: Unwrap[T, Option])(using AsSqlExpr[Unwrap[T, Option]]): Expr[Boolean] = Binary(this, LessThan, Literal(value))

    @targetName("lt")
    def <[R <: Operation[T]](that: Expr[R]): Expr[Boolean] = Binary(this, LessThan, that)

    @targetName("lt")
    def <[R <: Operation[T], E <: Expr[R]](query: Query[E]): Expr[Boolean] = Binary(this, LessThan, SubQuery(query))

    @targetName("le")
    def <=(value: Unwrap[T, Option])(using AsSqlExpr[Unwrap[T, Option]]): Expr[Boolean] = Binary(this, LessThanEqual, Literal(value))

    @targetName("le")
    def <=[R <: Operation[T]](that: Expr[R]): Expr[Boolean] = Binary(this, LessThanEqual, that)

    @targetName("le")
    def <=[R <: Operation[T], E <: Expr[R]](query: Query[E]): Expr[Boolean] = Binary(this, LessThanEqual, SubQuery(query))

    def in(list: List[T])(using AsSqlExpr[T]): Expr[Boolean] = In(this, Vector(list.map(Literal(_))), false)

    def in[R <: Operation[T], E <: Expr[R]](query: Query[E]): Expr[Boolean] = In(this, SubQuery(query), false)

    def notIn(list: List[T])(using AsSqlExpr[T]): Expr[Boolean] = In(this, Vector(list.map(Literal(_))), true)

    def notIn[R <: Operation[T], E <: Expr[R]](query: Query[E]): Expr[Boolean] = In(this, SubQuery(query), true)

    def between(start: T, end: T)(using AsSqlExpr[T]): Expr[Boolean] = Between(this, Literal(start), Literal(end), false)

    def between[S <: Operation[T], E <: Operation[T]](start: Expr[S], end: Expr[E]): Expr[Boolean] =
        Between(this, start, end, false)

    def notBetween(start: T, end: T)(using AsSqlExpr[T]): Expr[Boolean] = Between(this, Literal(start), Literal(end), true)

    def notBetween[S <: Operation[T], E <: Operation[T]](start: Expr[S], end: Expr[E]): Expr[Boolean] =
        Between(this, start, end, true)

    def asc: OrderBy = OrderBy(this, Asc)

    def desc: OrderBy = OrderBy(this, Desc)

object Expr:
    extension [T <: String | Option[String]](expr: Expr[T])
        def like(value: String): Expr[Boolean] = Binary(expr, Like, Literal(value))

        def like[R <: String | Option[String]](that: Expr[R]): Expr[Boolean] = Binary(expr, Like, that)

        def notLike(value: String): Expr[Boolean] = Binary(expr, NotLike, Literal(value))

        def notLike[R <: String | Option[String]](that: Expr[R]): Expr[Boolean] = Binary(expr, NotLike, that)

        def contains(value: String): Expr[Boolean] = Binary(expr, Like, Literal("%" + value + "%"))

        def startsWith(value: String): Expr[Boolean] = Binary(expr, Like, Literal(value + "%"))

        def endsWith(value: String): Expr[Boolean] = Binary(expr, Like, Literal("%" + value))

        @targetName("json")
        def ->(n: Int): Expr[Option[String]] = Binary(expr, Json, Literal(n))

        @targetName("json")
        def ->(key: String): Expr[Option[String]] = Binary(expr, Json, Literal(key))

        @targetName("jsonText")
        def ->>(n: Int): Expr[Option[String]] = Binary(expr, JsonText, Literal(n))

        @targetName("jsonText")
        def ->>(key: String): Expr[Option[String]] = Binary(expr, JsonText, Literal(key))

    extension (expr: Expr[Boolean])
        @targetName("and")
        def &&(that: Expr[Boolean]): Expr[Boolean] = Binary(expr, And, that)

        @targetName("or")
        def ||(that: Expr[Boolean]): Expr[Boolean] = Binary(expr, Or, that)

        @targetName("xor")
        def ^(that: Expr[Boolean]): Expr[Boolean] = Binary(expr, Xor, that)

        @targetName("not")
        def unary_! : Expr[Boolean] = Func("NOT", expr :: Nil)

    extension [T: Number : AsSqlExpr](expr: Expr[T])
        @targetName("plus")
        def +[R: Number : AsSqlExpr](value: R): Expr[Option[T]] = Binary(expr, Plus, Literal(value))

        @targetName("plus")
        def +[R: Number](that: Expr[R]): Expr[Option[BigDecimal]] = Binary(expr, Plus, that)

        @targetName("minus")
        def -[R: Number : AsSqlExpr](value: R): Expr[Option[T]] = Binary(expr, Minus, Literal(value))

        @targetName("minus")
        def -[R: Number](that: Expr[R]): Expr[Option[BigDecimal]] = Binary(expr, Minus, that)

        @targetName("times")
        def *[R: Number : AsSqlExpr](value: R): Expr[Option[T]] = Binary(expr, Times, Literal(value))

        @targetName("times")
        def *[R: Number](that: Expr[R]): Expr[Option[BigDecimal]] = Binary(expr, Times, that)

        @targetName("div")
        def /[R: Number : AsSqlExpr](value: R): Expr[Option[T]] = Binary(expr, Div, Literal(value))

        @targetName("div")
        def /[R: Number](that: Expr[R]): Expr[Option[BigDecimal]] = Binary(expr, Div, that)

        @targetName("mod")
        def %[R: Number : AsSqlExpr](value: R): Expr[Option[T]] = Binary(expr, Mod, Literal(value))

        @targetName("mod")
        def %[R: Number](that: Expr[R]): Expr[Option[BigDecimal]] = Binary(expr, Mod, that)

        @targetName("positive")
        def unary_+ : Expr[T] = Unary(expr, Positive)

        @targetName("negative")
        def unary_- : Expr[T] = Unary(expr, Negative)

case class Literal[T](value: T)(using a: AsSqlExpr[T]) extends Expr[T]:
    override def asSqlExpr: SqlExpr = a.asSqlExpr(value)

case class Column[T](tableName: String, columnName: String) extends Expr[T]:
    override def asSqlExpr: SqlExpr = SqlExpr.Column(Some(tableName), columnName)

case object Null extends Expr[Nothing]:
    override def asSqlExpr: SqlExpr = SqlExpr.Null

case class Binary[T](left: Expr[?], op: SqlBinaryOperator, right: Expr[?]) extends Expr[T]:
    override def asSqlExpr: SqlExpr = this match
        case Binary(left, Equal, Literal(None)) =>
            SqlExpr.Binary(left.asSqlExpr, Is, SqlExpr.Null)
        case Binary(left, NotEqual, Literal(None)) =>
            SqlExpr.Binary(left.asSqlExpr, IsNot, SqlExpr.Null)
        case Binary(left, NotEqual, right@Literal(Some(v))) =>
            SqlExpr.Binary(SqlExpr.Binary(left.asSqlExpr, NotEqual, right.asSqlExpr), Or, SqlExpr.Binary(left.asSqlExpr, Is, SqlExpr.Null))
        case Binary(_, _, Literal(None)) =>
            SqlExpr.BooleanLiteral(false)
        case Binary(left, op, right) =>
            SqlExpr.Binary(left.asSqlExpr, op, right.asSqlExpr)

case class Unary[T](expr: Expr[?], op: SqlUnaryOperator) extends Expr[T]:
    override def asSqlExpr: SqlExpr = SqlExpr.Unary(expr.asSqlExpr, op)

case class SubQuery[T](query: Query[?]) extends Expr[T]:
    override def asSqlExpr: SqlExpr = SqlExpr.SubQuery(query.ast)

case class Func[T](name: String, args: List[Expr[?]]) extends Expr[T]:
    override def asSqlExpr: SqlExpr = SqlExpr.Func(name, args.map(_.asSqlExpr))

case class Agg[T](name: String, args: List[Expr[?]], distinct: Boolean, orderBy: List[OrderBy]) extends Expr[T]:
    override def asSqlExpr: SqlExpr = SqlExpr.Agg(name, args.map(_.asSqlExpr), distinct, Map(), orderBy.map(_.asSqlOrderBy))

    def over: Window[T] = Window(this, Nil, Nil)

case class Case[T](branches: List[(Expr[?], Expr[?])], default: Expr[?]) extends Expr[T]:
    override def asSqlExpr: SqlExpr = SqlExpr.Case(branches.map((x, y) => SqlCase(x.asSqlExpr, y.asSqlExpr)), default.asSqlExpr)

case class Vector[T](items: List[Expr[?]]) extends Expr[T]:
    override def asSqlExpr: SqlExpr = SqlExpr.Vector(items.map(_.asSqlExpr))

case class In[T](expr: Expr[?], inExpr: Expr[?], not: Boolean) extends Expr[Boolean]:
    override def asSqlExpr: SqlExpr = this match
        case In(_, Vector(Nil), false) => SqlExpr.BooleanLiteral(false)
        case In(_, Vector(Nil), true) => SqlExpr.BooleanLiteral(true)
        case _ => SqlExpr.In(expr.asSqlExpr, inExpr.asSqlExpr, not)

case class Between[T](expr: Expr[?], start: Expr[?], end: Expr[?], not: Boolean) extends Expr[Boolean]:
    override def asSqlExpr: SqlExpr = SqlExpr.Between(expr.asSqlExpr, start.asSqlExpr, end.asSqlExpr, not)

case class Window[T](expr: Agg[?], partitionBy: List[Expr[?]], orderBy: List[OrderBy]) extends Expr[T]:
    override def asSqlExpr: SqlExpr = SqlExpr.Window(expr.asSqlExpr, partitionBy.map(_.asSqlExpr), orderBy.map(_.asSqlOrderBy), None)

    infix def partitionBy(items: Expr[?]*): Window[T] = this.copy(partitionBy = this.partitionBy ++ items.toList)

    infix def orderBy(items: OrderBy*): Window[T] = this.copy(orderBy = this.orderBy ++ items.toList)

case class SubQueryPredicate[T](query: Query[?], predicate: SqlSubQueryPredicate) extends Expr[T]:
    override def asSqlExpr: SqlExpr = SqlExpr.SubQueryPredicate(query.ast, predicate)

case class OrderBy(expr: Expr[?], order: SqlOrderByOption):
    def asSqlOrderBy: SqlOrderBy = SqlOrderBy(expr.asSqlExpr, Some(order))